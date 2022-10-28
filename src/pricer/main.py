# This Redis instance is tuned for durability.
from aredis_om import NotFoundError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import Response

from redis.connector import redis_url, RedisConnector
from redis.funds_models import FundModel
from schemas.funds import RequestQuery, ResponseQuery, TimeSeriesModel
from scrapper import FundTS, Scrapper
from settings import Settings, logger

settings = Settings()
app = application = FastAPI()


# Scrapping
async def scrapping(data: RequestQuery) -> FundTS:
    scrapper = Scrapper(logger=logger)
    result = await scrapper.get_fund_data(
        document_number=data.document,
        from_date=data.from_date
    )

    return result


async def update_fund_data(data: FundTS) -> FundTS:
    scrapper = Scrapper(logger=logger)
    result: FundTS = await scrapper.update_fund_data(
        fund_id=data.fund_pk,
        from_date=data.last_query_date
    )
    data.last_query_date = max(result.timeseries)
    data.timeseries.append(result)
    # TODO: update data in DB
    return result


@app.on_event("startup")
async def startup():
    logger.info(f"Starting Redis connection on: {redis_url}")
    redis = RedisConnector()
    redis = await redis.is_redis_available()
    if not redis:
        logger.fatal("Redis server not available")


@app.post("/fund", response_model=ResponseQuery)
async def query_funds(query: RequestQuery, background_tasks: BackgroundTasks):
    logger.debug("Getting fund with CNPJ %s data" % query.document)
    redis = RedisConnector()  # add to depends
    fund = await redis.get_cache_key(query.document)
    if fund:
        logger.debug("Data already exists for fund updating")
        data: FundTS = await update_fund_data(fund)
        # TODO: retrieve others TS
        # TODO: update last query date
    else:
        logger.debug("Fund does not saved in database getting all data")
        data: FundTS = await scrapping(query)
        if not data:
            raise HTTPException(
                status_code=400, detail=f"Fund not found with document_number {query.document}"
            )
        logger.debug("All data retrieved, creating models")

        first_date = None
        value_ts = data.timeseries
        if isinstance(value_ts, list):
            first_date = value_ts[0].timestamp
        await redis.create_model(data)
    logger.debug("Creating Response")
    timeseries = [
        TimeSeriesModel(
            timestamp=entry.timestamp,
            value=entry.value,
            owners=entry.owners,
            net_worth=entry.net_worth

        ) for entry in value_ts
    ]
    response = ResponseQuery(
        document=data.doc_number,
        active=data.active,
        from_date=first_date,
        fund_id=data.fund_pk,
        fund_released_on=data.released_on,
        fund_name=data.fund_name,
        timeseries=timeseries,
    )
    await redis.persist_timeseries(data.doc_number, timeseries)
    # background_tasks.add_task(redis.persist_timeseries(ata.doc_number, value_ts))
    return response


@app.get("/funds/{document}")
async def get_fund(document: str, request: Request, response: Response):
    # To retrieve this customer with its primary key, we use `Customer.get()`:
    try:
        redis = RedisConnector()  # add to depends
        return await redis.get_cache_key(document)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Fund with CNPJ {document} not found")
