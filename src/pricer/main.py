# This Redis instance is tuned for durability.
import datetime
import json
import logging
from decimal import Decimal
from typing import Optional
from xmlrpc.client import ResponseError

import aioredis
from aredis_om import JsonModel  # <- Notice, import from aredis_om
from aredis_om import Field, HashModel, NotFoundError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from pydantic import BaseModel, BaseSettings, Field, Json
from pydantic.json import pydantic_encoder
from starlette.requests import Request
from starlette.responses import Response

from scrapper import FundTS, Scrapper

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CVM_URL = "https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CConsolFdo/ResultBuscaParticFdo.aspx?CNPJNome="

# Settings
class Settings(BaseSettings):
    cvm_url: str = Field(env='CVM_URL', default="")
    redis_host: str = Field(env='REDIS_HOST', default="localhost")
    redis_port: int = Field(env='REDIS_PORT', default=15000)

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
    
settings = Settings()
redis_url = f'redis://{settings.redis_host}:{settings.redis_port}'

logger.info(redis_url)

# Redis Connection
redis = aioredis.from_url(
    url=redis_url,
    decode_responses=True
    )

# Redis Structure
class Keys:
    def __init__(self, prefix: str = "PRICER_"):
        self.prefix = prefix
        
    def fund_key(self, document:str) -> str:
        return f'{self.prefix}{document}'
        
    def value_ts(self, document:str) -> str:
        return f'{self.prefix}_value{document}'
        
    def net_worth_ts(self, document:str) -> str:
        return f'{self.prefix}_value{document}'
        
    def owners_ts(self, document:str) -> str:
        return f'{self.prefix}_value{document}'
    
    def cache_key(self) -> str:
        return f'{self.prefix}_cache'

# Models
class FundValueTS(HashModel):
    timestamp: datetime.datetime 
    value: Decimal
    
class FundNetWorthTS(HashModel):
    timestamp: datetime.datetime
    value: Decimal
    
class FundOwnerTS(HashModel):
    timestamp: datetime.datetime
    value: Decimal
    

class Fund(JsonModel):
    document: str = Field(index=True)
    name: str
    fund_id: str
    first_date: datetime.date
    first_query_date: datetime.date
    last_query_date: datetime.date
    active: bool
    value: Optional[FundValueTS]
    net_worth: Optional[FundValueTS]
    owners: Optional[FundValueTS]
    
class RequestQuery(BaseModel):
    document: str
    from_date: Optional[str]
    end_date: Optional[str]
    investment: Optional[float]
    
    
class ResponseQuery(RequestQuery):
    document: str
    quotes: Optional[Decimal]
    fund_name: Optional[str]
    fund_released_on: Optional[datetime.date]
    from_date: Optional[datetime.date]
    active: Optional[bool]
    timeseries: Optional[Json]


# Application
app = application = FastAPI()

async def make_timeseries(key):
    try:
        await redis.execute_command(
            'TS.CREATE', key,
            'DUPLICATE_POLICE', 'first'
        )
    except ResponseError as e:
        logger.info('Could not create timeseries %s, error: %s', key, e)

async def initialize_redis(keys):
    await make_timeseries(keys)
    

def datetime_parser(info: dict):
    for k,v in info.items():
        if isinstance(v, str) and v.endswith('+00:00'):
            try:
                info[k] = datetime.datetime.fromisoformat(v)
            except:
                logger.info('Error while parsing key: %s with value: %s', k, v)
        
    return info

def serialize_dates(v):
    return v.isoformat() if isinstance(v, datetime.date) else v

async def get_cache(document: str):
    key = Keys().fund_key(document=document)
    cached = await redis.get(key)
    if cached:
        return json.loads(cached, object_hook=datetime_parser)
    
async def set_cache(data, key: Optional[str]=None):
    key = key if key is not None else Keys().cache_key()
    await redis.set(
        key,
        json.dumps(data, separators=(",", ":"), default=pydantic_encoder)
        
    )
    
async def add_many_to_timeseries(
    key_pairs: Iterable[Tuple[str, str]],
    data: BitcoinSentiments
):
    """
    Add many samples to a single timeseries key.
    `key_pairs` is an iteratble of tuples containing in the 0th position the
    timestamp key into which to insert entries and the 1th position the name
    of the key within th `data` dict to find the sample.
    """
    partial = functools.partial(redis.execute_command, 'TS.MADD')
    for datapoint in data:
        for timeseries_key, sample_key in key_pairs:
            partial = functools.partial(
                partial, timeseries_key, int(
                    float(datapoint['timestamp']) * 1000,
                ),
                datapoint[sample_key],
            )
    return await partial()


def make_keys():
    return Keys()


async def persist(keys: Keys, data: BitcoinSentiments):
    ts_sentiment_key = keys.timeseries_sentiment_key()
    ts_price_key = keys.timeseries_price_key()
    await add_many_to_timeseries(
        (
            (ts_price_key, 'btc_price'),
            (ts_sentiment_key, 'mean'),
        ), data,
    )
    
# Scrapping
async def scrapping(data: RequestQuery) -> FundTS:
    scrapper = Scrapper(logger=logger)
    result = await scrapper.get_fund_data(**data)  # type: ignore
    
    return result

@app.on_event("startup")
async def startup():
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")  # type: ignore
    keys = Keys()
    await initialize_redis(keys.cache_key())
    # You can set the Redis OM URL using the REDIS_OM_URL environment
    # variable, or by manually creating the connection using your model's
    # Meta object.
    # Customer.Meta.database = redis


@app.post("/refresh", response_model=ResponseQuery)
async def query_funds(query: RequestQuery, background_tasks: BackgroundTasks):
    # We can save the model to Redis by calling `save()`:
    
    
    data: FundTS = await scrapping(query)
    
    if not data:
        raise HTTPException(status_code=400, detail=f"No fund found with document_number {query.document}")
    
    first_date = None
    last_date = None
    value= None
    owners= None
    net_worth= None
    timeseries = data.ts
    if isinstance(timeseries, list):
        first_date = timeseries[0].timestamp
        last_date = timeseries[-1].timestamp
        value = [FundValueTS(timestamp=ts.timestamp, value=ts.value) for ts in timeseries]
        net_worth = [FundNetWorthTS(timestamp=ts.timestamp, value=ts.net_worth) for ts in timeseries]
        owners = [FundOwnerTS(timestamp=ts.timestamp, value=ts.owners) for ts in timeseries]
    
    fund = Fund(
        document=data.doc_number,
        name=data.fund_name,
        fund_id=data.fund_pk,
        first_date=data.released_on,
        active=data.active,
        first_query_date=first_date,
        last_query_date=last_date,
        value=value,
        net_worth=net_worth,
        owners=owners,
    )
    key = Keys().fund_key(document=query.document)
    background_tasks.add_task(set_cache, fund, key)
    response = ResponseQuery(
        document=data.doc_number,
        end_date=last_date,
        active=data.active,
        from_date=first_date,
        fund_released_on=data.released_on,
        fund_name=data.fund_name,
        timeseries=data.ts.json()
        )  # type: ignore
    return response


@app.get("/funds/{document}")
@cache(expire=10)
async def get_customer(document: str, request: Request, response: Response):
    # To retrieve this customer with its primary key, we use `Customer.get()`:
    try:
        return await Fund.get(document)  # <- And, finally, one more await!
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Customer not found")
