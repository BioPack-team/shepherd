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
    callback_host: str = "http://host.docker.internal:5439"
    kg_retrieval_url: str = "http://host.docker.internal:8080/asyncquery"
    sync_kg_retrieval_url: str = "http://host.docker.internal:8080/query"
    kg_rehydrate_url: str = "http://host.docker.internal:8080/rehydrate"
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

    pathfinder_embeddings_dir: str = "pathfinder_embeddings"

    otel_enabled: bool = True
    jaeger_host: str = "http://jaeger"
    jaeger_port: int = 4317

    # ttl in seconds
    redis_ttl: int = 1210000

    # Reclaim of orphaned Redis Streams messages from dead consumers.
    # Per-stream overrides live in shepherd_utils/reclaim.PER_STREAM_MIN_IDLE_SEC;
    # this default applies to streams not listed there (fast workers). The
    # whole query has a ~5-minute upstream budget, so reclaim needs to fire
    # quickly enough that a retry has time to finish.
    reclaim_min_idle_sec: int = 30
    reclaim_interval_sec: int = 10
    reclaim_max_batch: int = 50

    # Graceful shutdown. On SIGTERM/SIGINT (Kubernetes sends SIGTERM on every
    # rollout, scale-down and node drain) a worker stops pulling new tasks and
    # waits up to this long for in-flight tasks to finish before exiting, so the
    # recycling the orchestrator already does is clean instead of lossy. Tasks
    # that don't finish in the window are left in the stream for Redis reclaim.
    # Keep this comfortably below the deployment's terminationGracePeriodSeconds.
    worker_drain_timeout_sec: float = 30.0

    # Monitor (dashboard) worker
    monitor_port: int = 5440
    monitor_poll_interval_sec: float = 3.0
    # History samples are persisted on a slower cadence than the live UI tick.
    # Default 30s keeps a few days of trend data without ballooning Redis.
    monitor_history_interval_sec: float = 30.0
    monitor_history_days: int = 3
    # On monitor startup we suppress worker-down alerts for this long so the
    # whole stack coming up at once doesn't spam Slack before workers have
    # had a chance to register their heartbeats.
    monitor_startup_grace_sec: int = 90
    # When several workers go down close together (e.g. a laptop going to
    # sleep), buffer their down-alerts for this long and send ONE combined
    # Slack/email message listing every downed worker instead of one message
    # per worker. The poll loop ticks faster than this, so the buffer flushes
    # on a normal tick without a dedicated timer.
    monitor_down_debounce_sec: float = 5.0
    # A query that never reaches COMPLETED this long after it started is treated
    # as abandoned (the worker driving it almost certainly crashed). The janitor
    # marks it ABANDONED and clears its pending callbacks. Must comfortably
    # exceed the whole-query upstream budget (~5 min) so genuinely in-flight
    # queries are never reaped; the default matches the callback-age alert.
    monitor_abandoned_query_sec: float = 600.0
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
