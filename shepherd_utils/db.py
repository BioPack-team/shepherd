"""Postgres DB Manager."""

import asyncio
import io
import logging
import time
from typing import Any, Dict, List, Union

import orjson
import redis
import redis.asyncio as aioredis
import zstandard
from psycopg import OperationalError
from psycopg_pool import AsyncConnectionPool

from .config import settings
from .logger import get_query_handler, resolve_log_level

PG_RETRIES = 5

# Postgres SQLSTATE 53100 is ``disk_full``. A full data volume surfaces as
# psycopg.errors.DiskFull, which subclasses OperationalError -- so it lands in
# the OperationalError branches of the retry loops below, where retrying can
# never help. We detect it explicitly and emit one stable, greppable marker so
# a cluster-wide outage caused by a full disk is obvious in every worker's logs
# instead of hiding behind generic "connection error" noise.
PG_DISK_FULL_SQLSTATE = "53100"


def is_disk_full_error(exc: BaseException) -> bool:
    """True if *exc* is a Postgres error caused by a full data volume."""
    return getattr(exc, "sqlstate", None) == PG_DISK_FULL_SQLSTATE


def log_pg_disk_full(
    logger: logging.Logger, operation: str, exc: BaseException
) -> None:
    logger.critical(
        f"PG_DISK_FULL operation={operation} sqlstate={getattr(exc, 'sqlstate', None)}: "
        f"{exc} -- Postgres is rejecting writes because its data volume is full"
    )


CONNINFO = (
    f"postgresql://postgres:{settings.postgres_password}@"
    f"{settings.postgres_host}:{settings.postgres_port}/"
    f"postgres"  # Add database name
    f"?keepalives_idle=120"  # Start keepalive after 2 minutes
    f"&keepalives_interval=30"  # Send keepalive every 30 seconds
    f"&keepalives_count=3"  # Mark dead after 3 failed keepalives
    f"&connect_timeout=10"  # Connection timeout
)


async def check_connection(conn):
    """Check if the postgres connection is still alive."""
    await conn.execute("SELECT 1")


pool = AsyncConnectionPool(
    conninfo=CONNINFO,
    timeout=settings.postgres_pool_timeout,
    min_size=settings.postgres_pool_min_size,
    max_size=settings.postgres_pool_max_size,
    max_idle=300,
    max_lifetime=3600,
    check=check_connection,
    # initialize with the connection closed
    open=False,
)

data_db_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=1,
    password=settings.redis_password,
    max_connections=10,
    timeout=30,
    socket_timeout=5,
    socket_connect_timeout=10,
    socket_keepalive=True,
    socket_keepalive_options={},
    health_check_interval=30,
    retry_on_timeout=True,
)

logs_db_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=3,
    password=settings.redis_password,
    max_connections=10,
    timeout=30,
    socket_timeout=5,
    socket_connect_timeout=10,
    socket_keepalive=True,
    socket_keepalive_options={},
    health_check_interval=30,
    retry_on_timeout=True,
)

data_db_client = aioredis.Redis(connection_pool=data_db_pool)
logs_db_client = aioredis.Redis(connection_pool=logs_db_pool)


# ---------------------------------------------------------------------------
# Sync Redis client (for use inside ProcessPoolExecutor workers)
#
# Lazily constructed so that importing this module in a freshly spawned worker
# does not open a connection unless the worker actually performs DB work. The
# client is reused for the lifetime of the worker process.
# ---------------------------------------------------------------------------

_sync_data_db_client: Union[redis.Redis, None] = None


def _get_sync_data_db() -> redis.Redis:
    """Return a process-local sync Redis client for the data db."""
    global _sync_data_db_client
    if _sync_data_db_client is None:
        _sync_data_db_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=1,
            password=settings.redis_password,
            socket_timeout=5,
            socket_connect_timeout=10,
            socket_keepalive=True,
            health_check_interval=30,
        )
    return _sync_data_db_client


# ---------------------------------------------------------------------------
# Codecs
#
# Pure functions, no I/O. Shared by both the async and sync code paths so the
# wire format stays consistent across the process boundary.
# ---------------------------------------------------------------------------


def encode_message(obj: Any) -> bytes:
    """Serialize a message to compressed bytes for storage in Redis."""
    return zstandard.compress(orjson.dumps(obj))


def decode_message(blob: bytes) -> Any:
    """Deserialize a stored message blob back into a Python object."""
    return orjson.loads(zstandard.decompress(blob))


def decompress_zstd(blob: bytes) -> bytes:
    """Decompress a zstd frame into raw bytes.

    Uses a streaming reader so it handles both frames with an embedded content
    size and streaming frames that omit it (unlike the one-shot
    ``zstandard.decompress``).
    """
    return zstandard.ZstdDecompressor().stream_reader(io.BytesIO(blob)).read()


# Idempotent DDL applied on every startup. The image's init_db.sql only runs
# when the Postgres data volume is brand new, so deployments whose volume
# predates a schema addition never pick it up from there; re-running these
# here upgrades them in place. Everything in this list must be safe to re-run
# and effectively free once already applied.
# ``(index name, DDL)``. The name is what the pre-flight check below looks for
# in the catalog, so it must match the index the DDL creates.
_SCHEMA_UPGRADES = (
    (
        "idx_callbacks_callback_id",
        "CREATE INDEX IF NOT EXISTS idx_callbacks_callback_id ON callbacks (callback_id)",
    ),
    (
        "idx_callbacks_query_id",
        "CREATE INDEX IF NOT EXISTS idx_callbacks_query_id ON callbacks (query_id)",
    ),
)


def _ars_schema_statements():
    """The ARS table DDL bundled with shepherd_utils (see ars/schema.sql).

    Statements are split on the blank-line-then-CREATE boundary so each
    executes separately; all are IF NOT EXISTS and safe to re-run. The
    ``idx_ars_message_ref`` index doubles as the pre-flight marker for
    whether this block has been applied.
    """
    import pathlib

    sql = (
        pathlib.Path(__file__).resolve().parent / "ars" / "schema.sql"
    ).read_text()
    # Drop comment lines FIRST: a ';' inside a comment must not split.
    sql = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    return [s.strip() for s in sql.split(";") if s.strip()]


ARS_SCHEMA_MARKER_INDEX = "idx_ars_message_ref"

# Arbitrary-but-fixed advisory lock id serializing the upgrades across the
# whole fleet booting at once: IF NOT EXISTS alone still races when two
# sessions both pass the existence check and try to create the same index.
_SCHEMA_UPGRADE_LOCK_ID = 762_297_531


async def apply_schema_upgrades() -> None:
    """Bring an existing database up to date with init_db.sql additions."""
    async with pool.connection(settings.postgres_pool_timeout) as conn:
        # Pre-flight catalog check, deliberately OUTSIDE the advisory lock.
        # Every container in the stack runs this at boot, and they all boot the
        # instant Postgres reports healthy, so taking the lock unconditionally
        # made ~23 containers queue up on a single lock for work that is a
        # no-op on any volume created since these indexes landed in
        # init_db.sql. Whoever lost that queue blew ``postgres_pool_timeout``
        # and logged a PoolTimeout traceback on an otherwise healthy startup.
        # The check is a single indexed catalog read and does not serialize, so
        # the common "already applied" case now costs one query and no lock.
        marker_names = [name for name, _ in _SCHEMA_UPGRADES] + [
            ARS_SCHEMA_MARKER_INDEX
        ]
        cursor = await conn.execute(
            "SELECT count(*) FROM pg_class WHERE relkind = 'i' AND relname = ANY(%s)",
            (marker_names,),
        )
        row = await cursor.fetchone()
        if row is not None and row[0] == len(marker_names):
            return
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_UPGRADE_LOCK_ID,)
        )
        for _, ddl in _SCHEMA_UPGRADES:
            await conn.execute(ddl)
        # Bring pre-ARS volumes up to date with the ars_* tables. Everything
        # in the bundled DDL is IF NOT EXISTS, so this is free once applied.
        for ddl in _ars_schema_statements():
            await conn.execute(ddl)
        await conn.commit()


async def initialize_db() -> None:
    """Open connection and create db."""
    await pool.open()
    for attempt in range(PG_RETRIES):
        try:
            await apply_schema_upgrades()
            return
        except Exception:
            # Retry with the same backoff the query paths use. Workers boot the
            # moment the DB reports healthy, so a Postgres crash-restart (or
            # any blip in the first seconds) otherwise burned the single
            # attempt and every container logged a traceback at once.
            if attempt == PG_RETRIES - 1:
                break
            await asyncio.sleep(0.1 * (2**attempt))
    # A failed upgrade must never keep a worker from starting: the schema
    # additions are performance aids, and the janitor/next boot retries.
    logging.getLogger("shepherd.db").warning(
        "Failed to apply startup schema upgrades", exc_info=True
    )


async def shutdown_db() -> None:
    """Close the connection to the db."""
    await pool.close()


async def add_query(
    query_id: str,
    response_id: str,
    query: dict[str, Any],
    callback_url: Union[str, None],
    logger: logging.Logger,
    target: Union[str, None] = None,
):
    """
    Add an initial query to the db.

    Args:
        query (Dict): TRAPI query graph
        target: The ARA the query was routed to (stored in ``domain`` for
            per-ARA dashboards).

    Returns:
        query_id: str
    """
    start = time.time()
    try:
        encoded = encode_message(query)
        await data_db_client.set(query_id, encoded, ex=settings.redis_ttl)
        await data_db_client.set(response_id, encoded, ex=settings.redis_ttl)
    except Exception as e:
        # failed to put message in db
        # TODO: do something more severe
        logger.error(f"Failed to save initial query or response: {e}")
        raise Exception("Failed to save initial query or response.")
    try:
        async with pool.connection(settings.postgres_pool_timeout) as conn:
            await conn.execute(
                """
            INSERT INTO shepherd_brain (qid, start_time, response_id, callback_url, state, status, domain) VALUES (
                %s, NOW(), %s, %s, %s, %s, %s
            )
            """,
                (query_id, response_id, callback_url, "QUEUED", "OK", target),
            )
            # await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(query_id)))
            await conn.commit()
    except Exception as e:
        if is_disk_full_error(e):
            log_pg_disk_full(logger, "add_query", e)
        else:
            logger.error(f"Failed to save initial query state to db: {e}")
        raise Exception("Failed to save initial query state.")
    logger.debug(f"Adding query took {time.time() - start} seconds")


async def save_message(
    callback_id: str,
    response: dict[str, Any],
    logger: logging.Logger,
    num_tries: int = 0,
):
    """
    Add a callback response to the db.

    Args:
        callback_id (str): UID for a callback response
        response (dict[str, Any]): A TRAPI message
    """
    start = time.time()
    try:
        start_comp = time.time()
        compressed = encode_message(response)
        logger.info(f"Compression took {time.time() - start_comp}")
        await data_db_client.set(
            callback_id,
            compressed,
            ex=settings.redis_ttl,
        )
        logger.debug(f"Saving message took {time.time() - start} seconds")
    except Exception as e:
        # failed to put message in db
        if num_tries < 4:
            num_tries += 1
            logger.warning(f"Failed to save message {num_tries} times. Trying again...")
            await asyncio.sleep(0.5)
            await save_message(callback_id, response, logger, num_tries)
        else:
            # TODO: do something more severe
            logger.error(f"Failed to save a message into redis: {e}")
            pass


class ResponseTooLargeError(Exception):
    """Raised when a stored response is too large to safely load into memory.

    A plain ``Exception`` on purpose: the shared task lifecycle
    (``run_task_lifecycle``) already catches any ``Exception`` from a worker and
    routes the task to ``finish_query`` with an ERROR status, so raising this
    *before* the memory-expanding load converts what would otherwise be an
    uncatchable OOM SIGKILL into a clean, accounted-for task failure.
    """


async def get_blob_size(message_id: str) -> int:
    """Return the stored (compressed) size of a data-db blob in bytes.

    ``STRLEN`` is an O(1) server-side read that never transfers the payload.
    Returns 0 when the key is missing.
    """
    return int(await data_db_client.strlen(message_id))


async def message_exists(message_id: str) -> bool:
    """True if a data-db blob exists, via a cheap ``EXISTS``.

    Use this for a presence check instead of ``get_message`` when the payload
    itself isn't needed: ``EXISTS`` never transfers, decompresses or parses the
    blob, so it avoids materializing a potentially large message just to learn
    whether it is there.
    """
    return bool(await data_db_client.exists(message_id))


# The zstd frame header (magic + descriptors + content size) is at most 18
# bytes; a small prefix is enough to read the embedded uncompressed size.
_ZSTD_HEADER_PROBE_BYTES = 64


async def get_response_size(message_id: str) -> int:
    """Best-effort *uncompressed* size of a stored response, read cheaply.

    Every blob we store is a one-shot zstd frame (``encode_message``), which
    embeds the original uncompressed content size in its header. We fetch just
    the first few header bytes with ``GETRANGE`` and decode that size -- no full
    fetch, no decompress. Uncompressed size is what actually drives a worker's
    peak memory: decoding parses the JSON into a Python object tree several times
    larger again (~5-6x for TRAPI-shaped data), and that tree briefly coexists
    with the decompressed bytes. Falls back to the compressed ``STRLEN`` if the
    header carries no content size (e.g. a streaming frame); returns 0 when the
    key is missing.
    """
    header = await data_db_client.getrange(message_id, 0, _ZSTD_HEADER_PROBE_BYTES - 1)
    if not header:
        return 0
    if isinstance(header, str):
        header = header.encode("latin-1")
    try:
        size = zstandard.frame_content_size(header)
    except zstandard.ZstdError:
        size = -1
    if size and size > 0:
        return int(size)
    # Unknown content size -> fall back to the compressed length as a proxy.
    return int(await data_db_client.strlen(message_id))


async def enforce_response_size_limit(
    response_id: str,
    logger: logging.Logger,
) -> None:
    """Raise ``ResponseTooLargeError`` if a response exceeds the configured cap.

    The cap (``settings.max_response_size``) is compared against the response's
    *uncompressed* size -- the number you'd see fetching it from ``/response`` --
    read cheaply from the zstd frame header without loading the blob. Decoding
    expands that several-fold in memory, so set the limit well below the pod's
    memory limit (see the config comment for sizing). A cap of 0 disables the
    guard, leaving the delivery-count circuit breaker as the backstop. Called at
    the top of a worker, before ``get_message``.
    """
    max_bytes = settings.max_response_size_bytes
    if max_bytes <= 0:
        return
    size = await get_response_size(response_id)
    if size > max_bytes:
        raise ResponseTooLargeError(
            f"Response {response_id} is {size} uncompressed bytes, over the "
            f"{max_bytes}-byte limit (max_response_size={settings.max_response_size}); "
            "refusing to load it: parsing it into memory would risk an "
            "out-of-memory kill."
        )


async def get_message(
    message_id: str,
    logger: logging.Logger,
    raw: bool = False,
) -> Union[Dict, bytes]:
    """Get the message from db.

    When *raw* is True, return the decompressed JSON bytes without parsing
    into a Python object — useful when the caller will forward the payload
    without inspecting it.
    """
    start = time.time()
    blob = await data_db_client.get(message_id)
    if blob is None:
        raise KeyError(f"Failed to get {message_id} from db")

    if raw:
        result = zstandard.decompress(blob)
        logger.debug(f"Getting raw message took {time.time() - start} seconds")
        return result

    start_decomp = time.time()
    message = decode_message(blob)
    logger.debug(f"Decompression took {time.time() - start_decomp}")
    logger.debug(f"Getting message took {time.time() - start} seconds")
    return message


async def get_query_log_level(
    query_id: str,
    logger: logging.Logger,
    default: Union[int, None] = None,
) -> int:
    """The log level the client asked for, read back from the stored query.

    The stored query is the only record of the requested level once a request
    has been handed off. A TRAPI *response* has no ``log_level`` field, so
    nothing a subservice posts back to ``/callback`` carries it -- everything
    hanging off a callback (the handler's own logs, the merge task it enqueues,
    the retrieval logs that merge folds into the query's log list) has to come
    back here for it.

    Falls back to the server default when the query didn't ask for a level, or
    when it can no longer be read.
    """
    if default is None:
        default = resolve_log_level(settings.log_level)
    try:
        query = await get_message(query_id, logger)
    except Exception as e:
        logger.warning(f"Couldn't read the log level for query {query_id}: {e}")
        return default
    return resolve_log_level(query.get("log_level"), default)


# ---------------------------------------------------------------------------
# Per-query "ready callback" index
#
# The merge_message worker coalesces all of a query's arrived-but-unmerged
# callbacks into a single locked merge. So that one worker can find every ready
# callback for a query without draining (and thus hoarding) the shared merge
# stream, each arriving callback is recorded in a Redis set keyed by the query's
# response_id. The merge worker drains this set under the response_id lock; the
# per-callback stream message is only a wake signal. Set membership implies the
# callback payload is already saved (add_ready_callback is called after
# save_message), and a callback leaves the set only after it has been merged.
# ---------------------------------------------------------------------------

READY_CALLBACKS_PREFIX = "merge_ready:"


def _ready_callbacks_key(response_id: str) -> str:
    return f"{READY_CALLBACKS_PREFIX}{response_id}"


async def add_ready_callback(
    response_id: str,
    callback_id: str,
    logger: logging.Logger,
) -> None:
    """Record an arrived callback as ready to merge into ``response_id``."""
    key = _ready_callbacks_key(response_id)
    try:
        async with data_db_client.pipeline(transaction=True) as pipe:
            pipe.sadd(key, callback_id)
            pipe.expire(key, settings.redis_ttl)
            await pipe.execute()
    except Exception as e:
        logger.error(f"Failed to record ready callback {callback_id}: {e}")


async def get_ready_callbacks(
    response_id: str,
    logger: logging.Logger,
) -> List[str]:
    """Return the callback ids currently ready to merge for ``response_id``."""
    key = _ready_callbacks_key(response_id)
    try:
        members = await data_db_client.smembers(key)
    except Exception as e:
        logger.error(f"Failed to get ready callbacks for {response_id}: {e}")
        return []
    return [m.decode() if isinstance(m, bytes) else m for m in members]


async def is_ready_callback(
    response_id: str,
    callback_id: str,
    logger: logging.Logger,
) -> bool:
    """True if ``callback_id`` is still an unmerged ready callback.

    On error we assume it is still pending so the caller re-enqueues the wake
    task rather than silently dropping a callback.
    """
    key = _ready_callbacks_key(response_id)
    try:
        return bool(await data_db_client.sismember(key, callback_id))
    except Exception as e:
        logger.error(f"Failed to check ready callback {callback_id}: {e}")
        return True


async def clear_ready_callback(
    response_id: str,
    callback_id: str,
    logger: logging.Logger,
) -> None:
    """Remove a merged callback from the ready index.

    Redis deletes the set automatically once its last member is removed, so no
    explicit key cleanup is needed; the TTL covers abandoned queries.
    """
    key = _ready_callbacks_key(response_id)
    try:
        await data_db_client.srem(key, callback_id)
    except Exception as e:
        logger.error(f"Failed to clear ready callback {callback_id}: {e}")


# ---------------------------------------------------------------------------
# Sync variants of get_message / save_message
#
# Intended for use inside ProcessPoolExecutor workers, which cannot drive an
# async event loop without significant overhead. These deliberately do not
# accept a logger argument: loggers do not pickle cleanly across processes,
# and worker logging should be configured via the executor's `initializer=`.
# ---------------------------------------------------------------------------


def get_message_sync(message_id: str) -> Dict:
    """Synchronously fetch and decode a message from the data db."""
    blob = _get_sync_data_db().get(message_id)
    if blob is None:
        raise KeyError(f"Failed to get {message_id} from db")
    return decode_message(blob)


def save_message_sync(message_id: str, message: dict[str, Any]) -> None:
    """Synchronously encode and store a message in the data db."""
    _get_sync_data_db().set(
        message_id,
        encode_message(message),
        ex=settings.redis_ttl,
    )


async def _append_logs(response_id: str, entries: List[dict]) -> None:
    """Append log entries to a query's list and (re)set the key's TTL.

    Both in one round trip, so a crash can't leave the key without an
    expiration.
    """
    pipe = logs_db_client.pipeline()
    pipe.rpush(response_id, *(orjson.dumps(entry) for entry in entries))
    pipe.expire(response_id, settings.redis_ttl)
    await pipe.execute()


async def _convert_legacy_logs(response_id: str) -> None:
    """Rewrite a pre-list logs key as a list of entries.

    Logs used to be stored as one JSON array under this key, rewritten whole on
    every flush. Appending to a list instead is atomic, so concurrent flushes
    no longer clobber each other -- but a query mid-flight across the version
    change still has the old blob. Replace it with the equivalent list; both
    reads and writes fall back here on the type error and then carry on.

    ``GETDEL`` so that concurrent flushes can't each read the blob and push its
    contents back a second time: exactly one of them comes away with it.
    """
    blob = await logs_db_client.getdel(response_id)
    entries = orjson.loads(blob) if blob else []
    if entries:
        await _append_logs(response_id, entries)


async def save_logs(
    response_id: str,
    logger: logging.Logger,
):
    """
    Save logs from a worker to the db.

    The query's logs are a Redis list that every flush appends to. Appending is
    atomic, so the several producers a single query has -- the callback handler,
    each worker stage, one merge task per callback -- can flush concurrently
    without the read-modify-write of a whole-list rewrite dropping or doubling
    anyone's entries.

    Draining the handler is what keeps each record to a single copy: the logger
    behind it is a process-wide singleton shared by every task for this query,
    so anything left in the queue would be written again by the next flush.

    Args:
        response_id (str): UID for a query response
    """
    # Drain before the first await: another flush for this query may interleave
    # here, and each must come away with a disjoint set of records.
    handler = get_query_handler(logger)
    new_logs = handler.drain() if handler is not None else []
    if not new_logs:
        return
    try:
        try:
            await _append_logs(response_id, new_logs)
        except redis.ResponseError:
            # Key still holds the single-JSON-blob format this used to write --
            # a query that was already in flight when this version rolled out.
            # Convert it in place so its earlier logs survive.
            await _convert_legacy_logs(response_id)
            await _append_logs(response_id, new_logs)
    except Exception as e:
        # Put them back so the next flush retries rather than dropping them --
        # they're no longer anywhere else now that the queue has been drained.
        handler.ingest(new_logs)
        logger.error(f"Failed to save logs for response {response_id}: {e}")


async def get_logs(
    response_id: str,
    logger: logging.Logger,
):
    """
    Get the log messages for a given query.

    Args:
        response_id (str): UID for a query response
    """
    try:
        try:
            entries = await logs_db_client.lrange(response_id, 0, -1)
        except redis.ResponseError:
            # Pre-list format (see ``_convert_legacy_logs``).
            await _convert_legacy_logs(response_id)
            entries = await logs_db_client.lrange(response_id, 0, -1)
        if not entries:
            logger.error(f"Failed to get logs for response {response_id}")
            return []
        logs = [orjson.loads(entry) for entry in entries]
        # The stored list reflects the order in which workers *flushed*
        # their logs, not the order events happened: a callback can arrive
        # and be merged (and flushed) before the lookup that dispatched the
        # query finishes and flushes its own logs. Every entry carries an
        # ISO8601 UTC timestamp, so sort by it to present the logs in
        # chronological order. Stable sort keeps same-timestamp entries in
        # their original relative order.
        logs.sort(key=lambda entry: entry.get("timestamp", ""))
        return logs
    except Exception as e:
        logger.error(f"Failed to get logs from query: {e}")
        return []


async def add_callback_id(
    query_id: str,
    callback_id: str,
    otel_trace: str,
    logger: logging.Logger,
):
    """Add a callback->query mapping."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                INSERT INTO callbacks (query_id, callback_id, otel_trace) VALUES (
                    %s, %s, %s
                )
                """,
                    (
                        query_id,
                        callback_id,
                        otel_trace,
                    ),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "add_callback_id", e)
                break
            logger.error(f"Connection error on attempt {attempt}: {e}")
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to save callback: {e}")
            break


async def remove_callback_id(
    callback_id: str,
    logger: logging.Logger,
):
    """Once a callback has been processed, remove it."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                DELETE FROM callbacks WHERE callback_id = %s
                """,
                    (callback_id,),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "remove_callback_id", e)
                break
            logger.error(
                f"Connection error removing callback id after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to remove callback after processing: {e}")
            break


async def get_running_callbacks(
    query_id: str,
    logger: logging.Logger,
) -> List[str]:
    """Get all currently running callbacks for a single query."""
    running_lookups = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT callback_id FROM callbacks WHERE query_id = %s
                """,
                    (query_id,),
                )
                rows = await cursor.fetchall()
                running_lookups = rows
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_running_callbacks", e)
                break
            logger.error(
                f"Connection error getting running callbacks after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get running lookups: {e}")
            raise
    return running_lookups


async def cleanup_callbacks(
    query_id: str,
    logger: logging.Logger,
):
    """Remove any current running callbacks."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                DELETE FROM callbacks WHERE query_id = %s
                """,
                    (query_id,),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "cleanup_callbacks", e)
                break
            logger.error(
                f"Connection error deleting callbacks after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to remove running lookups: {e}")
            break


async def reap_completed_callbacks(logger: logging.Logger) -> int:
    """Delete callback rows whose parent query is already COMPLETED.

    Used by the monitor's janitor to clean up rows orphaned by code paths that
    finished without calling ``cleanup_callbacks``. Returns the number of rows
    deleted on this call.
    """
    deleted = 0
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cur = await conn.execute("""
                    DELETE FROM callbacks
                    WHERE query_id IN (
                        SELECT qid FROM shepherd_brain WHERE state = 'COMPLETED'
                    )
                    """)
                deleted = cur.rowcount or 0
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "reap_completed_callbacks", e)
                break
            logger.warning(
                f"Connection error reaping completed callbacks (attempt {attempt}): {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to reap completed callbacks: {e}")
            break
    return deleted


async def reap_abandoned_queries(
    max_age_sec: float, logger: logging.Logger
) -> List[Dict[str, Any]]:
    """Fail-and-clean queries stuck in a non-terminal state past the budget.

    A query that hasn't reached COMPLETED long after the whole-query upstream
    budget (~5 min) has elapsed is considered abandoned -- usually because the
    worker driving it crashed. We mark it ABANDONED, clear its pending callback
    rows (the rows that otherwise keep ``oldest_callback_age_sec`` climbing and
    re-firing the callback-age alert every cooldown), and return one record per
    query so the caller can alert on it exactly once. The state flip means a
    query is reaped a single time and never rediscovered.
    """
    abandoned: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cur = await conn.execute(
                    """
                    SELECT b.qid,
                           EXTRACT(EPOCH FROM (NOW() - b.start_time)) AS age_sec,
                           COUNT(c.callback_id) AS callbacks
                    FROM shepherd_brain b
                    LEFT JOIN callbacks c ON c.query_id = b.qid
                    WHERE b.state NOT IN ('COMPLETED', 'ABANDONED')
                      AND b.start_time < NOW() - make_interval(secs => %s)
                    GROUP BY b.qid, b.start_time
                    """,
                    (float(max_age_sec),),
                )
                rows = await cur.fetchall()
                if not rows:
                    return []
                qids = [r[0] for r in rows]
                # Clear callbacks first (FK references shepherd_brain), then
                # move the parent query to a terminal ABANDONED state.
                await conn.execute(
                    "DELETE FROM callbacks WHERE query_id = ANY(%s)", (qids,)
                )
                await conn.execute(
                    """
                    UPDATE shepherd_brain
                    SET state = 'ABANDONED', stop_time = NOW(),
                        status = 'Abandoned: no completion within budget'
                    WHERE qid = ANY(%s)
                    """,
                    (qids,),
                )
                await conn.commit()
                abandoned = [
                    {
                        "qid": r[0],
                        "age_sec": float(r[1] or 0),
                        "callbacks_deleted": int(r[2] or 0),
                    }
                    for r in rows
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "reap_abandoned_queries", e)
                break
            logger.warning(
                f"Connection error reaping abandoned queries (attempt {attempt}): {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to reap abandoned queries: {e}")
            break
    return abandoned


async def purge_old_queries(
    retention_days: int, logger: logging.Logger
) -> Dict[str, int]:
    """Delete terminal queries (and their leftover callbacks) past retention.

    Postgres has no row-level TTL, so this is the scheduled equivalent for the
    ``shepherd_brain`` table, which otherwise grows forever (rows only ever flip
    to COMPLETED/ABANDONED, never get removed). Rows in a terminal state whose
    work finished longer ago than ``retention_days`` are deleted; in-flight
    queries are never touched regardless of age -- the abandoned-query reaper is
    what moves a stuck query into a terminal state, after which it becomes
    eligible here. Returns ``{"queries": n, "callbacks": m}``.
    """
    result = {"queries": 0, "callbacks": 0}
    if retention_days <= 0:
        return result
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                # callbacks FK-references shepherd_brain, so clear any leftover
                # rows for the doomed queries first (most are already reaped on
                # completion, but a crash can leave stragglers).
                cur = await conn.execute(
                    """
                    DELETE FROM callbacks WHERE query_id IN (
                        SELECT qid FROM shepherd_brain
                        WHERE state IN ('COMPLETED', 'ABANDONED')
                          AND COALESCE(stop_time, start_time)
                              < NOW() - make_interval(days => %s)
                    )
                    """,
                    (retention_days,),
                )
                result["callbacks"] = cur.rowcount or 0
                cur = await conn.execute(
                    """
                    DELETE FROM shepherd_brain
                    WHERE state IN ('COMPLETED', 'ABANDONED')
                      AND COALESCE(stop_time, start_time)
                          < NOW() - make_interval(days => %s)
                    """,
                    (retention_days,),
                )
                result["queries"] = cur.rowcount or 0
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "purge_old_queries", e)
                break
            logger.warning(
                f"Connection error purging old queries (attempt {attempt}): {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to purge old queries: {e}")
            break
    if result["queries"] or result["callbacks"]:
        logger.info(
            f"Purged {result['queries']} terminal queries and "
            f"{result['callbacks']} leftover callbacks older than {retention_days}d"
        )
    return result


async def get_callback_query_id(
    callback_id: str,
    logger: logging.Logger,
) -> Union[str, None]:
    """Given a callback id, get the associated query id."""
    original_query = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT query_id, otel_trace FROM callbacks WHERE callback_id = %s
                """,
                    (callback_id,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    original_query = row
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_callback_query_id", e)
                break
            logger.error(
                f"Connection error getting query id from callback after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get a query id from callback: {e}")
            break
    return original_query


async def get_query_state(
    query_id: str,
    logger: logging.Logger,
):
    """Get the query state."""
    query_state = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT * FROM shepherd_brain WHERE qid = %s
                """,
                    (query_id,),
                )
                row = await cursor.fetchone()
                query_state = row
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_query_state", e)
                break
            logger.error(
                f"Connection error getting query state after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get query state: {e}")
            break
    return query_state


async def set_query_completed(
    query_id: str,
    status: str,
    logger: logging.Logger,
):
    """This query is done."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                UPDATE shepherd_brain SET stop_time = NOW(), state = 'COMPLETED', status = %s WHERE qid = %s
                """,
                    (
                        status,
                        query_id,
                    ),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "set_query_completed", e)
                break
            logger.error(
                f"Connection error setting query completed after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to successfully complete query in db: {e}")
            break
