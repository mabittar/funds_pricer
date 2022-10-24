import datetime
import functools
import json
from typing import Optional, Iterable, Tuple, List
from xmlrpc.client import ResponseError

import aioredis
from pydantic.json import pydantic_encoder

from .funds_models import FundModel, FundValueTS, FundNetWorthTS, FundOwnerTS
from src.pricer.scrapper import FundTS
from src.pricer.settings import logger, settings

redis_url = f'redis://{settings.redis_host}:{settings.redis_port}'

epoch = datetime.datetime(1970, 1, 1)

redis = aioredis.from_url(
    url=redis_url,
    decode_responses=True
    )


class Keys:
    def __init__(self, prefix: str = "PRICER_"):
        self.prefix = prefix

    def fund_key(self, document: str) -> str:
        return f'{self.prefix}{document}'

    def value_ts(self, document: str) -> str:
        return f'{self.prefix}value_{document}'

    def net_worth_ts(self, document: str) -> str:
        return f'{self.prefix}networth_{document}'

    def owners_ts(self, document: str) -> str:
        return f'{self.prefix}owners_{document}'

    def cache_key(self) -> str:
        return f'{self.prefix}cache'


class RedisConnector:
    def __init__(self):
        self.redis = aioredis.from_url(
            url=redis_url,
            decode_responses=True,
            encoding="utf-8"
        )

    async def is_redis_available(self):
        # ... get redis connection here, or pass it in. up to you.
        try:
            await self.redis.ping()
        except (aioredis.exceptions.ConnectionError,
                aioredis.exceptions.BusyLoadingError):
            return False
        return True
    def create_model(self, data: FundTS):
        key = Keys().fund_key(data.doc_number)

        self.redis.create(
            key,
            labels={'NAME': data.fund_name,
                    'ACTIVE': data.active,
                    'FUND_ID': data.fund_pk,
                    'RELEASED_ON': data.released_on
                    }
        )


def datetime_parser(info: dict):
    for k,v in info.items():
        if isinstance(v, str) and v.endswith('+00:00'):
            try:
                info[k] = datetime.datetime.fromisoformat(v)
            except Exception:
                logger.info('Error while parsing key: %s with value: %s', k, v)

    return info


def serialize_dates(v):
    return datetime.datetime.fromtimestamp(v) if isinstance(v, float) else v


async def get_cache(document: str):
    key = Keys().fund_key(document=document)
    cached = await redis.get(key)
    if cached:
        return json.loads(cached, object_hook=datetime_parser)


async def set_cache(data, key: Optional[str] = None):
    key = key if key is not None else Keys().fund_key(data.document)
    await redis.set(
        key,
        json.dumps(data, separators=(",", ":"), default=pydantic_encoder)

    )


def make_keys():
    return Keys()


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


async def add_many_to_timeseries(
    key_value_pairs: Tuple,
):
    """
    Add many samples to a single timeseries key.
    `key_pairs` is an iteratble of tuples containing in the 0th position the
    timestamp key into which to insert entries and the 1th position the name
    of the key within th `data` dict to find the sample.
    """
    partial = functools.partial(redis.execute_command, 'TS.MADD')
    for timeseries_key, timeseries in key_value_pairs:
        partial = functools.partial(
            partial,
            timeseries_key,
            timeseries.timestamp.timestamp(),
            timeseries.value
            )
    return await partial()


async def persist(data: FundModel):
    doc_number = data.document
    value_key = Keys().value_ts(doc_number)
    owner_key = Keys().owners_ts(doc_number)
    networth_key = Keys().net_worth_ts(doc_number)
    await add_many_to_timeseries(
        (
            (value_key, data.value),
            (owner_key, data.owners),
            (networth_key, data.net_worth),
        ),
    )
