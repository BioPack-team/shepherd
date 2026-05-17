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

    lookup_timeout: int = 210
    callback_host: str = "http://127.0.0.1:5439"
    kg_retrieval_url: str = "https://strider.renci.org/asyncquery"
    sync_kg_retrieval_url: str = "https://strider.renci.org/query"
    default_data_tier: int = 0
    omnicorp_url: str = "https://aragorn-ranker.renci.org/omnicorp_overlay"
    arax_url: str = "https://arax.ncats.io/shepherd/api/arax/v1.4/query"
    node_norm: str = "https://biothings.ci.transltr.io/nodenorm/api/"

    pathfinder_redis_host: str = "host.docker.internal"
    pathfinder_redis_port: int = 6383
    pathfinder_redis_password: str = "supersecretpassword"
    pathfinder_pmid_db: int = 1
    pathfinder_curies_db: int = 2

    omnicorp_curies_lmdb_path: str = "/app/omnicorp_lmdb/curies.lmdb"
    omnicorp_shared_counts_lmdb_path: str = "/app/omnicorp_lmdb/shared_counts.lmdb"

    otel_enabled: bool = True
    jaeger_host: str = "http://jaeger"
    jaeger_port: int = 4317

    # ttl in seconds
    redis_ttl: int = 1210000

    # Reclaim of orphaned Redis Streams messages from dead consumers.
    # ``min_idle`` must be a comfortable buffer above the worst-case legitimate
    # task processing time so an active consumer is never robbed.
    reclaim_min_idle_sec: int = 300
    reclaim_interval_sec: int = 30
    reclaim_max_batch: int = 50

    # Monitor (dashboard) worker
    monitor_port: int = 5440
    monitor_poll_interval_sec: float = 3.0
    # History samples are persisted on a slower cadence than the live UI tick.
    # Default 30s keeps a few days of trend data without ballooning Redis.
    monitor_history_interval_sec: float = 30.0
    monitor_history_days: int = 3
    monitor_alerts_config: str = "/app/monitor_alerts.yaml"
    slack_webhook_url: str = ""
    alert_email_to: str = ""
    alert_email_from: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True

    class Config:
        env_file = ".env"


settings = Settings()
