from typing import Optional

from pydantic import AnyUrl
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    server_url: AnyUrl = "http://localhost:5439"
    server_maturity: str = "development"
    server_location: str = "RENCI"

    postgres_user: str = "postgres"
    postgres_host: str = "shepherd_db"
    postgres_port: int = 5432
    postgres_password: str = "supersecretpassw0rd"

    redis_host: str = "shepherd_broker"
    redis_port: int = 6379
    redis_password: str = "supersecretpassword"

    lookup_timeout: int = 240
    callback_host: AnyUrl = "http://127.0.0.1:5439"
    retriever_url: AnyUrl = "http://localhost:3000/v1/asyncquery"

    class Config:
        env_file = ".env"


settings = Settings()
