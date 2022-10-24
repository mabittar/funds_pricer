import logging

from dotenv import find_dotenv
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    cvm_url: str = Field(env='CVM_URL', default="")
    cvm_fund_url: str = Field(env='FUND_DETAIL_URL', default="")
    redis_host: str = Field(env='REDIS_HOST', default="localhost")
    redis_port: int = Field(env='REDIS_PORT', default=15000)

    class Config:
        env_file = find_dotenv(filename=".env", usecwd=True)
        env_file_encoding = 'utf-8'


settings = Settings()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
