import datetime
import functools
import json
from decimal import Decimal
from typing import Optional, Tuple
from xmlrpc.client import ResponseError

from aioredis.exceptions import ResponseError
from pydantic.json import pydantic_encoder
from src.pricer.schemas.funds import TimeSeriesModel
from src.pricer.scrapper import FundTS
from src.pricer.settings import logger, settings

import aioredis as redis

redis_url = f'redis://{settings.redis_host}:{settings.redis_port}'


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
        self.logger = logger
        self.redis = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True,
            encoding="utf-8"
        )

    async def is_redis_available(self):
        # ... get redis connection here, or pass it in. up to you.
        try:
            await self.redis.ping()
            self.logger.info(f"Ping successful: {await self.redis.ping()}")
        except (redis.ConnectionError,
                redis.BusyLoadingError):
            return False
        return True

    async def create_model(self, data: FundTS):
        key = Keys().fund_key(data.doc_number)
        try:
            await self.redis.execute_command(
                'CREATE',
                'DUPLICATE_POLICE',
                'first',
                key,
                labels={'NAME': data.fund_name,
                        'ACTIVE': data.active,
                        'FUND_ID': data.fund_pk,
                        'RELEASED_ON': data.released_on,
                        'LAST_QUERY_DATE': data.timeseries[-1].timestamp
                        }
            )
        except ResponseError as e:
            logger.info('Could not create model %s, error: %s', key, e)

    # TODO: adjust and use REDIS-OM encoders/decoders
    @staticmethod
    def datetime_parser(info: dict):
        for k, v in info.items():
            if isinstance(v, str) and v.endswith('+00:00'):
                try:
                    info[k] = datetime.datetime.fromisoformat(v)
                except Exception:
                    logger.info('Error while parsing key: %s with value: %s', k, v)

        return info

    @staticmethod
    def serialize_dates(v):
        return datetime.datetime.fromtimestamp(v) if isinstance(v, float) else v

    async def get_cache_key(self, document: str):
        key = Keys().fund_key(document=document)
        cached = await self.redis.get(key)
        if cached:
            # TODO: transform to FundTS before return
            return json.loads(cached, object_hook=self.serialize_dates)
        else:
            return None

    async def set_cache(self, data, key: Optional[str] = None):
        key = key if key is not None else Keys().fund_key(data.document)
        await self.redis.set(
            key,
            json.dumps(data, separators=(",", ":"), default=pydantic_encoder)

        )

    async def add_many_to_timeseries(
            self, key_value_pairs: Tuple, data: list[TimeSeriesModel]
    ):
        """
        Add many samples to a single timeseries key.
        `key_pairs` is an iteratble of tuples containing in the 0th position the
        timestamp key into which to insert entries and the 1th position the name
        of the key within th `data` dict to find the sample.
        """
        partial = functools.partial(self.redis.execute_command, 'TS.MADD')
        for entry in data:
            for timeseries_key, attr in key_value_pairs:
                value = entry.__getattribute__(attr)
                v = str(value.quantize(Decimal("1.0000"))) if isinstance(value, Decimal) else v
                partial = functools.partial(
                    partial,
                    timeseries_key,
                    int(entry.timestamp.timestamp()),
                    v
                )
        return await partial()

    async def create_ts(self, key):
        try:
            await self.redis.execute_command(
                'TS.CREATE', key,
                'DUPLICATE_POLICY', 'first',
            )
        except ResponseError as e:
            # Time series probably already exists
            logger.info('Could not create timeseries %s, error: %s', key, e)

    async def check_ts_by_key(self, key: str):
        cached = await self.redis.get(key)
        if cached:
            return True
        else:
            return False

    async def create_ts_key(self, key_list):
        for key in key_list:
            existing = await self.check_ts_by_key(key)
            if not existing:
                self.logger.info("Time series not found, will be created!")
                await self.create_ts(key)

    async def persist_timeseries(self, document: str, data: list[TimeSeriesModel]):
        doc_number = document
        value_key = Keys().value_ts(doc_number)
        owner_key = Keys().owners_ts(doc_number)
        networth_key = Keys().net_worth_ts(doc_number)
        await self.create_ts_key([value_key, owner_key, networth_key])
        try:
            await self.add_many_to_timeseries(
                (
                    (value_key, "value"),
                    (owner_key, "owners"),
                    (networth_key, "net_worth"),
                ),
                data
            )
        except ResponseError as e:
            logger.info('Error while writing data series for document: %s', doc_number)
