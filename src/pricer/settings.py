from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    cvm_url: str = Field(env='CVM_URL', default="")
    redis_host: str = Field(env='REDIS_HOST', default="localhost")
    redis_port: int = Field(env='REDIS_PORT', default=15000)

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
