# This Redis instance is tuned for durability.
import logging

from aredis_om import NotFoundError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from starlette.requests import Request
from starlette.responses import Response

from .redis.connector import redis, Keys, redis_url, set_cache, initialize_redis, persist
from .redis.funds_models import FundModel
from .schemas.funds import RequestQuery, ResponseQuery
from .scrapper import FundTS, Scrapper
from .settings import Settings

settings = Settings()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

app = application = FastAPI()


# Scrapping
async def scrapping(data: RequestQuery) -> FundTS:
    scrapper = Scrapper(logger=logger)
    result = await scrapper.get_fund_data(**data)  # type: ignore
    
    return result


@app.on_event("startup")
async def startup():
    logger.info(f"Staring Redis connection on: {redis_url}")
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")  # type: ignore
    keys = Keys()
    await initialize_redis(keys.cache_key())
    # You can set the Redis OM URL using the REDIS_OM_URL environment
    # variable, or by manually creating the connection using your model's
    # Meta object.
    # Fund.Meta.database = redis


@app.post("/fund", response_model=ResponseQuery)
async def query_funds(query: RequestQuery, background_tasks: BackgroundTasks):
    # We can save the model to Redis by calling `save()`:
    data: FundTS = await scrapping(query)
    
    if not data:
        raise HTTPException(
            status_code=400, detail=f"No fund found with document_number {query.document}"
        )
    
    first_date = None
    last_date = None
    value_ts = data.value
    if isinstance(value_ts, list):
        first_date = value_ts[0].timestamp
        last_date = value_ts[-1].timestamp
    
    fund = FundModel(
        document=data.doc_number,
        name=data.fund_name,
        fund_id=data.fund_pk,
        first_date=data.released_on,
        active=data.active,
        first_query_date=first_date,
        last_query_date=last_date,
        value=data.value,
        net_worth=data.net_worth,
        owners=data.owners,
    )
    await persist(fund)
    background_tasks.add_task(set_cache, fund)
    response = ResponseQuery(
        document=data.doc_number,
        end_date=last_date,
        active=data.active,
        from_date=first_date,
        fund_released_on=data.released_on,
        fund_name=data.fund_name,
        value=data.value,
        net_worth=data.net_worth,
        owners=data.owners,
        )  # type: ignore
    return response


@app.get("/funds/{document}")
@cache(expire=10)
async def get_customer(document: str, request: Request, response: Response):
    # To retrieve this customer with its primary key, we use `Customer.get()`:
    try:
        return await FundModel.get(document)  # <- And, finally, one more await!
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Fund not found")
