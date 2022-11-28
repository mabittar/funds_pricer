import dataclasses
import datetime
import functools
import json
from decimal import Decimal

from typing import Optional, Tuple, List
from xmlrpc.client import ResponseError

from aioredis.exceptions import ResponseError
import aioredis as redis

from schemas.funds import TimeSeriesModel
from scrapper_models import FundTS
from settings import Settings, logger

settings = Settings()

redis_url = f'redis://{settings.redis_host}:{settings.redis_port}'


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()

        return super().default(o)


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

    def timeseries_keys(self, document: str) -> List[str]:
        keys_list = [
            self.value_ts(document),
            self.owners_ts(document),
            self.net_worth_ts(document)
        ]
        return keys_list


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
        key = Keys().fund_key(data.document)
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

    @staticmethod
    def serialize_dates(v):
        return datetime.datetime.fromtimestamp(v) if isinstance(v, int) else v

    @staticmethod
    def str_2_timestamp(str_date) -> int:
        return datetime.datetime.strptime(f'01/{str_date}', "%d/%m/%Y").timestamp()

    def convert_date(self, given_date) -> int:
        if isinstance(given_date, str):
            return self.str_2_timestamp(given_date)
        else:
            return given_date.timestamp()

    async def get_timeseries(self, key: str, from_date, to_date):
        cached_ts = await self.redis.execute_command(
            'TS.RANGE',
            key,
            from_date,
            to_date
        )
        if cached_ts:
            return cached_ts
        else:
            return None

    async def get_cached_timeseries(
            self, key_list: list, from_date: str | datetime.datetime = None, to_date: str | datetime.datetime = None
    ):
        """
        Add many samples to several timeseries keys.
        `key_pairs` is an iterable of tuples containing in the 0th position the
        timestamp key into which to insert entries and the 1th position the name
        of the key within th `data` dict to find the sample.
        """

        from_date = self.convert_date(from_date) if from_date is not None else 0
        to_date = self.convert_date(to_date) if to_date is not None else "+"
        ts_cached = {}
        for key in key_list:
            try:
                ts_cached[key] = await self.get_timeseries(key, from_date, to_date)
            except ResponseError:
                self.logger.debug(f'Cached key {key} returning None')
                ts_cached[key] = None

        # threads = [Thread(target=self.get_timeseries, args=(key, from_date, to_date)) for key in key_list]
        # for thread in threads:
        #     thread.start()
        #     thread.join()

        return ts_cached

    async def get_cached_model(self, document: str) -> Optional[dict]:
        key = Keys().fund_key(document)
        cached = await self.redis.get(key)
        if cached:
            return json.loads(cached, object_hook=self.serialize_dates)
        else:
            return None

    async def set_cache(self, data: FundTS, key: Optional[str] = None):
        key = key if key is not None else Keys().fund_key(data.document)
        data.timeseries = None
        await self.redis.set(
            key,
            json.dumps(dataclasses.asdict(data), separators=(",", ":"), cls=EnhancedJSONEncoder)

        )

    async def publish(self, msg):
        try:
            await self.redis.publish(settings.fund_channel, json.dumps(msg))
        except (ResponseError, TimeoutError, ConnectionError):
            self.logger.error(f"Error while publishing: {msg.message_id}")
            pass

    async def add_many_to_timeseries(
            self, key_value_pairs: Tuple, data: list[TimeSeriesModel]
    ):
        """
        Add many samples to several timeseries keys.
        `key_pairs` is an iterable of tuples containing in the 0th position the
        timestamp key into which to insert entries and the 1th position the name
        of the key within th `data` dict to find the sample.
        """
        partial = functools.partial(self.redis.execute_command, 'TS.MADD')
        for entry in data:
            for timeseries_key, attr in key_value_pairs:
                point = entry.__getattribute__(attr)
                v = str(point.quantize(Decimal("1.000000000"))) if isinstance(point, Decimal) else point
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
