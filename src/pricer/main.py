# This Redis instance is tuned for durability.
import datetime
import logging
from decimal import Decimal
from typing import Optional

import aioredis
from aredis_om import JsonModel  # <- Notice, import from aredis_om
from aredis_om import Field, HashModel, NotFoundError, get_redis_connection
from fastapi import FastAPI, HTTPException
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from pydantic import BaseSettings, EmailStr, Field, Json
from redis import Redis
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Settings
class Settings(BaseSettings):
    redis_host: str = Field(env='REDIS_HOST', default="my_redis")
    redis_port: int = Field(env='REDIS_PORT', default=6380)

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        
settings = Settings()

# Redis Connection
redis_conn = get_redis_connection(
    host=settings.redis_host, 
    port=settings.redis_port,
    decode_responses=True)


# Models
class Funds(HashModel):
    timestamp: datetime.date = Field(index=True)
    value: Decimal
    data: Optional[Json]


class Customer(JsonModel):
    first_name: str
    last_name: str
    email: EmailStr
    join_date: datetime.date
    query: Optional[Json]


# Application
app = application = FastAPI()


@app.on_event("startup")
async def startup():
    r = aioredis.from_url("redis://localhost:6380", encoding="utf8",
                          decode_responses=True)
    FastAPICache.init(RedisBackend(r), prefix="fastapi-cache")  # type: ignore

    # You can set the Redis OM URL using the REDIS_OM_URL environment
    # variable, or by manually creating the connection using your model's
    # Meta object.
    Customer.Meta.database = redis_conn


@app.post("/customer")
async def save_customer(customer: Customer):
    # We can save the model to Redis by calling `save()`:
    return await customer.save()  # <- We use await here


@app.get("/customers")
async def list_customers(request: Request, response: Response):
    # To retrieve this customer with its primary key, we use `Customer.get()`:
    return {"customers": await Customer.all_pks()}  # <- We also use await here


@app.get("/customer/{pk}")
@cache(expire=10)
async def get_customer(pk: str, request: Request, response: Response):
    # To retrieve this customer with its primary key, we use `Customer.get()`:
    try:
        return await Customer.get(pk)  # <- And, finally, one more await!
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Customer not found")
