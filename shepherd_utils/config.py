from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    server_url: str = "http://localhost:5439"
    server_maturity: str = "development"
    server_location: str = "RENCI"

    log_level: str = "INFO"

    postgres_user: str = "postgres"
    postgres_host: str = "shepherd_db"
    postgres_port: int = 5432
    postgres_password: str = "supersecretpassw0rd"

    redis_host: str = "shepherd_broker"
    redis_port: int = 6379
    redis_password: str = "supersecretpassword"

    lookup_timeout: int = 240
    callback_host: str = "http://127.0.0.1:5439"
    kg_retrieval_url: str = "https://strider.renci.org/asyncquery"
    sync_kg_retrieval_url: str = "https://strider.renci.org/query"
    omnicorp_url: str = "https://aragorn-ranker.renci.org/omnicorp_overlay"
    arax_url: str = "https://arax.ncats.io/api/arax/v1.4/query"

    otel_enabled: bool = True
    jaeger_host: str = "http://jaeger"
    jaeger_port: int = 4317

    class Config:
        env_file = ".env"


settings = Settings()
