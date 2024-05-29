from typing import Optional

from pydantic import BaseSettings, AnyUrl


class Settings(BaseSettings):
    server_url: Optional[AnyUrl]
    server_maturity: str = "development"
    server_location: str = "RENCI"

    lookup_timeout: int = 240
    callback_host: AnyUrl = "http://127.0.0.1:5439"
    retriever_url: AnyUrl = "http://localhost:3000/v1/asyncquery"

    class Config:
        env_file = ".env"


settings = Settings()
