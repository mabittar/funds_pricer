# This Redis instance is tuned for durability.
import datetime
from decimal import Decimal
from functools import wraps
from time import perf_counter
from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException

from redis.connector import redis_url, RedisConnector, Keys
from schemas.funds import RequestQuery, ResponseQuery, TimeSeriesModel, StreamingSchema, ResponseQuote
from scrapper import Scrapper
from scrapper_models import TimeSeries, FundTS
from settings import Settings, logger

settings = Settings()
app = application = FastAPI()


# timer
def timeit(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        s = perf_counter()
        try:
            return await func(*args, **kwargs)
        finally:
            elapsed = (perf_counter() - s)
            if elapsed < 1:
                elapsed = elapsed * 1000
                msg = f"{elapsed:0.4f} ms."
            else:
                msg = f"{elapsed:0.4f} s."

            logger.info(f"Method: {func.__name__} executed in {msg}.")

    return wrapper


# Scrapping
@timeit
async def scrapping(data: RequestQuery) -> FundTS:
    scrapper = Scrapper(logger=logger)
    result = await scrapper.get_fund_data(
        document_number=data.document,
        from_date=data.from_date
    )

    return result


@timeit
async def update_fund_data(data: FundTS) -> FundTS | None:
    if data.timeseries is None:
        data.last_query_date = None
    scrapper = Scrapper(logger=logger)
    result: List[TimeSeries] = await scrapper.update_fund_data(
        fund_id=data.fund_pk,
        from_date=data.last_query_date
    )
    if result is None:
        # Sent to queue
        return None
    else:
        last_entry = max(result)
        data.last_query_date = last_entry.timestamp
        if data.timeseries is not None:
            data.timeseries.append(result)
        else:
            data.timeseries = result
        return result


@timeit
async def dict_2_fund_model(cached_data: dict) -> FundTS:
    cached_model = FundTS(
        document=cached_data["document"],
        fund_pk=cached_data["fund_pk"],
        fund_name=cached_data["fund_name"],
        active=cached_data["active"],
        last_query_date=cached_data["last_query_date"],
        released_on=cached_data["released_on"],
        first_query_date=cached_data["first_query_date"],
    )
    return cached_model


@timeit
async def dict_2_timeseries_model(cached_data: dict, key_list: list):
    key_list_length = len(key_list)
    owners_key = True if any("owners_" in key for key in key_list) else None
    networth_key = True if any("networth_" in key for key in key_list) else None
    time_series_list = []
    if cached_data[key_list[0]] is None:
        return None
    length = len(cached_data[key_list[0]])
    for i in range(length):
        if key_list_length == 1:
            time_series = TimeSeriesModel(
                timestamp=datetime.datetime.fromtimestamp(cached_data[key_list[0]][i][0]),
                value=Decimal(cached_data[key_list[0]][i][1]),
                net_worth=0,
                owners=0
            )
            time_series_list.append(time_series)
        elif key_list_length == 2 and owners_key:
            time_series = TimeSeriesModel(
                timestamp=datetime.datetime.fromtimestamp(cached_data[key_list[0]][i][0]),
                value=Decimal(cached_data[key_list[0]][i][1]),
                net_worth=0,
                owners=int(cached_data[key_list[-1]][i][1])
            )
            time_series_list.append(time_series)
        elif key_list_length == 2 and networth_key:
            time_series = TimeSeriesModel(
                timestamp=datetime.datetime.fromtimestamp(cached_data[key_list[0]][i][0]),
                value=cached_data[key_list[0]][i][0],
                net_worth=cached_data[key_list[-1]][i][1],
                owners=0
            )
            time_series_list.append(time_series)
        else:
            time_series = TimeSeriesModel(
                timestamp=datetime.datetime.fromtimestamp(cached_data[key_list[0]][i][0]),
                value=cached_data[key_list[0]][i][0],
                net_worth=cached_data[key_list[-1]][i][1],
                owners=int(cached_data[key_list[1]][i][1])
            )
            time_series_list.append(time_series)
    return time_series_list


@app.on_event("startup")
async def startup():
    logger.info(f"Starting Redis connection on: {redis_url}")
    redis = RedisConnector()
    redis = await redis.is_redis_available()
    if not redis:
        logger.fatal("Redis server not available")
        raise HTTPException(
            status_code=403, detail=f"Redis service is unavailable!"
        )


@app.post("/fund", response_model=ResponseQuery)
async def query_funds(query: RequestQuery, background_tasks: BackgroundTasks):
    logger.debug("Getting fund with CNPJ %s data" % query.document)
    redis = RedisConnector()  # add to fastapi depends
    fund: dict = await redis.get_cached_model(query.document)
    if fund:
        logger.debug("Cached data found, updating")
        fund: FundTS = await dict_2_fund_model(fund)
        keys = Keys().timeseries_keys(fund.document)
        cached_ts = await redis.get_cached_timeseries(keys)
        if cached_ts is not None or any(d for d in cached_ts):
            timeseries = await dict_2_timeseries_model(cached_ts, keys)
        else:
            logger.debug("Data already exists for fund updating")
            timeseries: FundTS = await update_fund_data(fund)
        fund.timeseries = timeseries
    else:
        logger.debug("Fund not fund in database getting all data")
        fund: FundTS = await scrapping(query)
        if not fund:
            raise HTTPException(
                status_code=400, detail=f"Fund not found with document_number {query.document}"
            )

    logger.debug("All data retrieved, creating models")
    first_date = None
    timeseries = None
    value_ts = fund.timeseries
    if isinstance(value_ts, list) and len(value_ts) >= 1:
        first_date = value_ts[0].timestamp
        fund.first_query_date = first_date
        fund.last_query_date = value_ts[-1].timestamp
        timeseries = [
            TimeSeriesModel(
                timestamp=entry.timestamp,
                value=entry.value,
                owners=entry.owners,
                net_worth=entry.net_worth

            ) for entry in value_ts
        ]
        background_tasks.add_task(redis.persist_timeseries(fund.document, timeseries))
        del value_ts
    await redis.set_cache(fund)
    logger.debug("Creating Response")

    response = ResponseQuery(
        document=fund.document,
        active=fund.active,
        from_date=first_date,
        fund_id=fund.fund_pk,
        fund_released_on=fund.released_on,
        fund_name=fund.fund_name,
        timeseries=timeseries,
    )
    logger.debug("Persisting data")
    # await redis.persist_timeseries(fund.document, timeseries)
    return response


@app.get("/funds/quotes/{document}", response_model=ResponseQuote)
async def get_fund(document: str, investment: float = 0, date: str = ''):
    redis = RedisConnector()  # add to depends
    try:
        asked_date = datetime.datetime.strptime(date, "%d/%m/%Y")
    except ValueError:
        logger.error(f"Invalid date field! {date}")
        raise HTTPException(status_code=404, detail=f"date field must be dd/mm/yyyy")

    if investment <= 0:
        logger.error(f"Invalid investment field!, {investment}")
        raise HTTPException(status_code=404, detail=f"Investment must be greater than R$ 0, sent {investment}.")
    try:
        investment = Decimal(investment)
        fund: dict = await redis.get_cached_model(document)

    except Exception:
        raise HTTPException(status_code=404, detail=f"Fund with CNPJ {document} not found")

    if not fund:
        logger.error("No fund found!")
        response = "No fund found!"
    else:
        fund: FundTS = await dict_2_fund_model(fund)
        fund_key_list = [
            Keys().value_ts(fund.document),
            Keys().net_worth_ts(fund.document)
        ]
        logger.debug("Fund found, get timeseries")
        cached_ts = await redis.get_cached_timeseries(fund_key_list, asked_date, asked_date)
        fund.timeseries = await dict_2_timeseries_model(cached_ts, fund_key_list)

    logger.debug("Creating response")
    quote_value = fund.timeseries[0].value
    response = ResponseQuote(
        document=fund.document,
        date=asked_date,
        investment=investment,
        quote_vale_on_date=quote_value,
        number_of_quotes=Decimal(investment / quote_value),
    )
    return response


@app.get("/funds/{document}")
async def get_fund(document: str, owners: bool = False, networth: bool = False):
    redis = RedisConnector()  # add to depends
    try:
        fund: dict = await redis.get_cached_model(document)

    except Exception:
        raise HTTPException(status_code=404, detail=f"Fund with CNPJ {document} not found")

    if not fund:
        response = "No fund found!"
    else:
        fund: FundTS = await dict_2_fund_model(fund)
        fund_key_list = [
            Keys().value_ts(fund.document)
        ]
        if owners:
            fund_key_list.append(Keys().owners_ts(fund.document))
        if networth:
            fund_key_list.append(Keys().net_worth_ts(fund.document))
        cached_ts = await redis.get_cached_timeseries(fund_key_list)
        fund.timeseries = await dict_2_timeseries_model(cached_ts, fund_key_list)
        from_date = (
            datetime.datetime.fromisoformat(fund.first_query_date)
            if fund.first_query_date is not None else None
        )
        response = ResponseQuery(
            document=fund.document,
            active=fund.active,
            from_date=from_date,
            fund_id=fund.fund_pk,
            fund_released_on=fund.released_on,
            fund_name=fund.fund_name,
            timeseries=fund.timeseries,
        )
    return response
