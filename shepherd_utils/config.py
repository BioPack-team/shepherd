import re

from pydantic_settings import BaseSettings

# Binary suffixes (Ki/Mi/...) are powers of 1024; decimal suffixes (K/M/...) are
# powers of 1000 -- matching Kubernetes resource-quantity semantics.
_SIZE_UNITS = {
    "": 1,
    "k": 10**3,
    "m": 10**6,
    "g": 10**9,
    "t": 10**12,
    "p": 10**15,
    "ki": 2**10,
    "mi": 2**20,
    "gi": 2**30,
    "ti": 2**40,
    "pi": 2**50,
}


def parse_size_to_bytes(value: str) -> int:
    """Parse a size string into bytes.

    Accepts Kubernetes-style quantities (``"100Gi"``, ``"50G"``, ``"2Ti"``,
    ``"100GiB"``) and plain byte counts (``"107374182400"``). Returns 0 for
    empty or unparseable input, which callers treat as "capacity unknown /
    feature disabled".
    """
    if not value:
        return 0
    m = re.fullmatch(
        r"\s*(\d+(?:\.\d+)?)\s*([kmgtp]i?)?b?\s*", str(value), re.IGNORECASE
    )
    if not m:
        return 0
    number, unit = m.group(1), (m.group(2) or "").lower()
    return int(float(number) * _SIZE_UNITS[unit])


class Settings(BaseSettings):
    server_url: str = "http://localhost:5439"
    server_maturity: str = "development"
    server_location: str = "RENCI"

    log_level: str = "INFO"

    postgres_user: str = "postgres"
    postgres_host: str = "shepherd_db"
    postgres_port: int = 5432
    postgres_password: str = "supersecretpassw0rd"
    # How long (seconds) to wait to acquire a connection from the pool before
    # giving up. Kept short so callers fail fast when the DB is unreachable
    # instead of blocking; raise it if you hit pool timeouts under heavy load.
    postgres_pool_timeout: float = 5.0
    # Size of the Postgres data volume, set from the SAME Helm value that sizes
    # the PVC (e.g. "100Gi"). Lets the monitor compute how full the disk is and
    # alert before it fills. Empty disables the db-capacity alert.
    pg_volume_capacity: str = ""

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

    # Retention for the durable query-state table (``shepherd_brain``). Postgres
    # has no native row TTL, so the monitor janitor purges terminal queries
    # (COMPLETED/ABANDONED) -- and any leftover callbacks -- this many days after
    # they finished. This is the durable-table analog of ``redis_ttl``, which
    # already expires the query payloads in Redis. In-flight queries are never
    # touched regardless of age. Set to 0 to disable purging.
    query_retention_days: int = 30

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

    @property
    def pg_volume_capacity_bytes(self) -> int:
        """Configured Postgres volume size in bytes (0 if unset)."""
        return parse_size_to_bytes(self.pg_volume_capacity)

    class Config:
        env_file = ".env"


settings = Settings()
