import logging
from logging.config import dictConfig
from dotenv import find_dotenv
from pydantic import BaseSettings, Field, BaseModel


class Settings(BaseSettings):
    cvm_url: str = Field(env='CVM_URL', default="")
    cvm_fund_url: str = Field(env='FUND_DETAIL_URL', default="")
    redis_host: str = Field(env='REDIS_HOST', default="localhost")
    redis_port: int = Field(env='REDIS_PORT', default=15000)

    class Config:
        env_file = find_dotenv(filename=".env", usecwd=True)
        env_file_encoding = 'utf-8'


settings = Settings()


class LogConfig(BaseModel):
    """Logging configuration to be set for the server"""

    LOGGER_NAME: str = "pricerlog"
    LOG_FORMAT: str = "%(levelprefix)s | %(asctime)s | %(message)s"
    LOG_LEVEL: str = "DEBUG"

    # Logging config
    version = 1
    disable_existing_loggers = False
    formatters = {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": LOG_FORMAT,
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    }
    handlers = {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    }
    loggers = {
        "mycoolapp": {"handlers": ["default"], "level": LOG_LEVEL},
    }


dictConfig(LogConfig().dict())
logger = logging.getLogger("pricerlog")
