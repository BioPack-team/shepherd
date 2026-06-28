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
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from .config import settings

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
    min_size=5,
    max_size=10,
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


async def initialize_db() -> None:
    """Open connection and create db."""
    await pool.open()


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


async def save_logs(
    response_id: str,
    logger: logging.Logger,
):
    """
    Save logs from a worker to the db.

    Args:
        response_id (str): UID for a query response
    """
    try:
        existing_logs = await logs_db_client.get(response_id)
        if existing_logs is None:
            existing_logs = []
        else:
            existing_logs = orjson.loads(existing_logs)
        # get log handler from logger
        handler = next(
            (
                h
                for h in logger.handlers
                if getattr(h, "name", None) == "query_log_handler"
            ),
            None,
        )
        if handler is not None:
            new_logs = list(handler.contents())
            new_logs.reverse()
            existing_logs.extend(new_logs)
        await logs_db_client.set(
            response_id, orjson.dumps(existing_logs), ex=settings.redis_ttl
        )
    except Exception as e:
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
        logs = await logs_db_client.get(response_id)
        if logs is not None:
            logs = orjson.loads(logs)
            return logs
        else:
            logger.error(f"Failed to get logs for response {response_id}")
            return []
    except Exception as e:
        logger.error(f"Failed to get logs from query: {e}")


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
                          AND retain = FALSE
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
                      AND retain = FALSE
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


# ---------------------------------------------------------------------------
# ARS (Autonomous Relay System) helpers
#
# A parent ARS query fans out to several ARAs. Each (parent x ARA) pair gets a
# row in ``ars_children``; the parent is "done" when no child row is left in a
# non-terminal state. These mirror the retry-loop style of the callbacks helpers
# above.
# ---------------------------------------------------------------------------

# Terminal child states. A parent has finished fanning out once every child row
# is in one of these.
ARS_TERMINAL_CHILD_STATES = ("DONE", "ERROR")


async def set_query_ars_parent(
    query_id: str,
    logger: logging.Logger,
):
    """Mark a query as a top-level ARS parent (so /ars/messages can list it)."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    "UPDATE shepherd_brain SET is_ars_parent = TRUE WHERE qid = %s",
                    (query_id,),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "set_query_ars_parent", e)
                break
            logger.error(
                f"Connection error marking ARS parent after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to mark query {query_id} as ARS parent: {e}")
            break


async def add_ars_children(
    parent_qid: str,
    children: List[Dict[str, str]],
    logger: logging.Logger,
):
    """Insert the per-ARA child rows for a parent ARS query.

    ``children`` is a list of dicts with keys ``ara``, ``child_qid``,
    ``child_response_id``, ``ars_callback_id`` and ``otel_trace``. All rows start
    in the QUEUED state.
    """
    params = [
        (
            parent_qid,
            child["ara"],
            child["child_qid"],
            child["child_response_id"],
            child.get("ars_callback_id"),
            child.get("otel_trace"),
            "QUEUED",
        )
        for child in children
    ]
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                for row in params:
                    await conn.execute(
                        """
                    INSERT INTO ars_children
                        (parent_qid, ara, child_qid, child_response_id,
                         ars_callback_id, otel_trace, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                        row,
                    )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "add_ars_children", e)
                break
            logger.error(
                f"Connection error adding ARS children after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to add ARS children for {parent_qid}: {e}")
            raise


async def set_ars_child_status(
    parent_qid: str,
    ara: str,
    status: str,
    logger: logging.Logger,
    child_response_id: Union[str, None] = None,
    code: Union[int, None] = None,
    result_count: Union[int, None] = None,
):
    """Update one ARA child row. Sets ``stop_time`` when the status is terminal.

    Only non-None optional fields are written, so a RUNNING update doesn't clear
    a previously-recorded ``child_response_id``.
    """
    sets = ["status = %s"]
    values: List[Any] = [status]
    if child_response_id is not None:
        sets.append("child_response_id = %s")
        values.append(child_response_id)
    if code is not None:
        sets.append("code = %s")
        values.append(code)
    if result_count is not None:
        sets.append("result_count = %s")
        values.append(result_count)
    if status in ARS_TERMINAL_CHILD_STATES:
        sets.append("stop_time = NOW()")
    values.extend([parent_qid, ara])
    query = (
        f"UPDATE ars_children SET {', '.join(sets)} "
        "WHERE parent_qid = %s AND ara = %s"
    )
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(query, tuple(values))
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "set_ars_child_status", e)
                break
            logger.error(
                f"Connection error setting ARS child status after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(
                f"Failed to set ARS child status for {parent_qid}/{ara}: {e}"
            )
            break


async def get_pending_ars_children(
    parent_qid: str,
    logger: logging.Logger,
) -> List[str]:
    """Return the ARA names whose child rows are not yet terminal.

    An empty list means every ARA has reported back (DONE or ERROR) -- the
    cross-ARA completion gate.
    """
    pending: List[str] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT ara FROM ars_children
                WHERE parent_qid = %s AND status NOT IN ('DONE', 'ERROR')
                """,
                    (parent_qid,),
                )
                rows = await cursor.fetchall()
                pending = [r[0] for r in rows]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_pending_ars_children", e)
                break
            logger.error(
                f"Connection error getting pending ARS children after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get pending ARS children for {parent_qid}: {e}")
            raise
    return pending


async def get_ars_children(
    parent_qid: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Return all child rows for a parent query (for the trace/status API)."""
    children: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT ara, child_qid, child_response_id, status, code,
                       result_count, merged_version, start_time, stop_time
                FROM ars_children WHERE parent_qid = %s ORDER BY ara
                """,
                    (parent_qid,),
                )
                rows = await cursor.fetchall()
                children = [
                    {
                        "ara": r[0],
                        "child_qid": r[1],
                        "child_response_id": r[2],
                        "status": r[3],
                        "code": r[4],
                        "result_count": r[5],
                        "merged_version": r[6],
                        "start_time": r[7].isoformat() if r[7] else None,
                        "stop_time": r[8].isoformat() if r[8] else None,
                    }
                    for r in rows
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_ars_children", e)
                break
            logger.error(
                f"Connection error getting ARS children after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get ARS children for {parent_qid}: {e}")
            raise
    return children


async def get_ars_child_by_callback(
    callback_id: str,
    logger: logging.Logger,
) -> Union[Dict[str, Any], None]:
    """Resolve an ARS callback id to its parent query and ARA.

    The child ARA's ``finish_query`` posts its final response to
    ``/ars/callback/{ars_callback_id}``; this maps that id back to the parent so
    the response can be folded into the right accumulating message. Returns None
    if the callback id is unknown.
    """
    child = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT parent_qid, ara, child_qid, child_response_id, otel_trace
                FROM ars_children WHERE ars_callback_id = %s
                """,
                    (callback_id,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    child = {
                        "parent_qid": row[0],
                        "ara": row[1],
                        "child_qid": row[2],
                        "child_response_id": row[3],
                        "otel_trace": row[4],
                    }
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_ars_child_by_callback", e)
                break
            logger.error(
                f"Connection error getting ARS child by callback after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get ARS child by callback {callback_id}: {e}")
            break
    return child


async def claim_ars_tail(
    parent_qid: str,
    logger: logging.Logger,
) -> bool:
    """Atomically claim the right to launch the post-merge tail workflow.

    Returns True for exactly one caller (the one that flips ``ars_tail_launched``
    from FALSE to TRUE) and False for every other concurrent caller, so the tail
    (node_norm -> ... -> finish_query) is enqueued only once even when several
    child callbacks observe "all done" at the same time.
    """
    claimed = False
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                UPDATE shepherd_brain SET ars_tail_launched = TRUE
                WHERE qid = %s AND ars_tail_launched = FALSE
                RETURNING qid
                """,
                    (parent_qid,),
                )
                row = await cursor.fetchone()
                await conn.commit()
                claimed = row is not None
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "claim_ars_tail", e)
                break
            logger.error(
                f"Connection error claiming ARS tail after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to claim ARS tail for {parent_qid}: {e}")
            raise
    return claimed


async def upsert_actor(
    infores: str,
    url: str,
    channel: str,
    agent_name: str,
    maturity: str,
    logger: logging.Logger,
    active: bool = True,
    inforev: Union[Dict[str, Any], None] = None,
):
    """Insert or update an ARS actor (ARA) in the registry, keyed by infores."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                INSERT INTO ars_actors
                    (infores, url, channel, agent_name, maturity, active, inforev)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (infores) DO UPDATE SET
                    url = EXCLUDED.url,
                    channel = EXCLUDED.channel,
                    agent_name = EXCLUDED.agent_name,
                    maturity = EXCLUDED.maturity,
                    active = EXCLUDED.active,
                    inforev = EXCLUDED.inforev
                """,
                    (
                        infores,
                        url,
                        channel,
                        agent_name,
                        maturity,
                        active,
                        Json(inforev) if inforev is not None else None,
                    ),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "upsert_actor", e)
                break
            logger.error(
                f"Connection error upserting actor after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to upsert actor {infores}: {e}")
            break


async def list_actors(logger: logging.Logger) -> List[Dict[str, Any]]:
    """List all registered ARS actors."""
    actors: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT infores, url, channel, agent_name, maturity, active
                FROM ars_actors ORDER BY infores
                """
                )
                rows = await cursor.fetchall()
                actors = [
                    {
                        "infores": r[0],
                        "url": r[1],
                        "channel": r[2],
                        "agent_name": r[3],
                        "maturity": r[4],
                        "active": r[5],
                    }
                    for r in rows
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "list_actors", e)
                break
            logger.error(f"Connection error listing actors after attempt {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to list actors: {e}")
            raise
    return actors


async def list_actor_field(field: str, logger: logging.Logger) -> List[str]:
    """Return the distinct non-null values of an ``ars_actors`` column.

    ``field`` is validated against a fixed allowlist so it can be interpolated
    into the query safely (column names can't be parameterized).
    """
    allowed = {"agent_name", "channel"}
    if field not in allowed:
        raise ValueError(f"Unsupported actor field: {field}")
    values: List[str] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    f"SELECT DISTINCT {field} FROM ars_actors "
                    f"WHERE {field} IS NOT NULL ORDER BY {field}"
                )
                rows = await cursor.fetchall()
                values = [r[0] for r in rows]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "list_actor_field", e)
                break
            logger.error(
                f"Connection error listing actor field after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to list actor field {field}: {e}")
            raise
    return values


async def add_subscriber(
    parent_qid: str,
    callback_url: str,
    logger: logging.Logger,
    client_id: Union[str, None] = None,
):
    """Register a subscriber callback for status updates on a parent query."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    "INSERT INTO ars_subscribers (parent_qid, callback_url, client_id)"
                    " VALUES (%s, %s, %s)",
                    (parent_qid, callback_url, client_id),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "add_subscriber", e)
                break
            logger.error(
                f"Connection error adding subscriber after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to add subscriber for {parent_qid}: {e}")
            break


async def remove_subscriber(
    parent_qid: str,
    client_id: str,
    logger: logging.Logger,
):
    """Remove a client's subscription to a parent query."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    "DELETE FROM ars_subscribers WHERE parent_qid = %s "
                    "AND client_id = %s",
                    (parent_qid, client_id),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "remove_subscriber", e)
                break
            logger.error(f"Connection error removing subscriber after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to remove subscriber {client_id}/{parent_qid}: {e}")
            break


async def list_subscribers(
    parent_qid: str,
    logger: logging.Logger,
) -> List[str]:
    """Return the callback urls subscribed to a parent query."""
    urls: List[str] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    "SELECT callback_url FROM ars_subscribers WHERE parent_qid = %s",
                    (parent_qid,),
                )
                rows = await cursor.fetchall()
                urls = [r[0] for r in rows]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "list_subscribers", e)
                break
            logger.error(
                f"Connection error listing subscribers after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to list subscribers for {parent_qid}: {e}")
            raise
    return urls


async def upsert_agent(
    name: str,
    logger: logging.Logger,
    description: str = "",
    uri: str = "",
    contact: str = "",
):
    """Insert or update an agent (ARS Agent model parity)."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                    INSERT INTO ars_agents (name, description, uri, contact)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        description = EXCLUDED.description,
                        uri = EXCLUDED.uri,
                        contact = EXCLUDED.contact
                    """,
                    (name, description, uri, contact),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "upsert_agent", e)
                break
            logger.error(f"Connection error upserting agent after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to upsert agent {name}: {e}")
            break


async def list_agents(logger: logging.Logger) -> List[Dict[str, Any]]:
    """List all registered agents."""
    agents: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    "SELECT name, description, uri, contact FROM ars_agents "
                    "ORDER BY name"
                )
                agents = [
                    {"name": r[0], "description": r[1], "uri": r[2], "contact": r[3]}
                    for r in await cursor.fetchall()
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "list_agents", e)
                break
            logger.error(f"Connection error listing agents after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to list agents: {e}")
            raise
    return agents


async def get_agent(name: str, logger: logging.Logger) -> Union[Dict[str, Any], None]:
    """Return a single agent by name, or None."""
    agent = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    "SELECT name, description, uri, contact FROM ars_agents "
                    "WHERE name = %s",
                    (name,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    agent = {
                        "name": row[0],
                        "description": row[1],
                        "uri": row[2],
                        "contact": row[3],
                    }
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_agent", e)
                break
            logger.error(f"Connection error getting agent after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get agent {name}: {e}")
            break
    return agent


async def upsert_channel(
    name: str,
    logger: logging.Logger,
    description: str = "",
):
    """Insert or update a channel (ARS Channel model parity)."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                    INSERT INTO ars_channels (name, description) VALUES (%s, %s)
                    ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
                    """,
                    (name, description),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "upsert_channel", e)
                break
            logger.error(f"Connection error upserting channel after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to upsert channel {name}: {e}")
            break


async def list_channels(logger: logging.Logger) -> List[Dict[str, Any]]:
    """List all registered channels."""
    channels: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    "SELECT name, description FROM ars_channels ORDER BY name"
                )
                channels = [
                    {"name": r[0], "description": r[1]}
                    for r in await cursor.fetchall()
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "list_channels", e)
                break
            logger.error(f"Connection error listing channels after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to list channels: {e}")
            raise
    return channels


async def get_client(
    client_id: str, logger: logging.Logger
) -> Union[Dict[str, Any], None]:
    """Return a subscriber client row (ARS Client) by id, or None."""
    client = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    "SELECT client_id, client_secret, callback_url, subscriptions, "
                    "active FROM ars_clients WHERE client_id = %s",
                    (client_id,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    client = {
                        "client_id": row[0],
                        "client_secret": row[1],
                        "callback_url": row[2],
                        "subscriptions": row[3] or [],
                        "active": row[4],
                    }
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_client", e)
                break
            logger.error(f"Connection error getting client after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get client {client_id}: {e}")
            break
    return client


async def upsert_client(
    client_id: str,
    client_secret: str,
    callback_url: str,
    logger: logging.Logger,
):
    """Insert or update a subscriber client (encrypted secret + callback)."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                    INSERT INTO ars_clients (client_id, client_secret, callback_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (client_id) DO UPDATE SET
                        client_secret = EXCLUDED.client_secret,
                        callback_url = EXCLUDED.callback_url
                    """,
                    (client_id, client_secret, callback_url),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "upsert_client", e)
                break
            logger.error(f"Connection error upserting client after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to upsert client {client_id}: {e}")
            break


async def set_client_subscriptions(
    client_id: str,
    subscriptions: List[str],
    logger: logging.Logger,
):
    """Replace a client's subscription pk list."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    "UPDATE ars_clients SET subscriptions = %s WHERE client_id = %s",
                    (Json(subscriptions), client_id),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "set_client_subscriptions", e)
                break
            logger.error(
                f"Connection error setting subscriptions after {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to set subscriptions for {client_id}: {e}")
            break


async def get_timed_out_ars_parents(
    timeout_sec: float,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Find ARS parents past their budget with children still pending.

    Returns one record per timed-out parent: ``{qid, response_id, pending}``
    where ``pending`` is the list of ARAs that never reported back. The caller
    (the watchdog) marks those children ERROR and forces the post-merge tail so
    partial results still reach the submitter.
    """
    timed_out: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                    SELECT b.qid, b.response_id,
                           ARRAY_AGG(c.ara) AS pending
                    FROM shepherd_brain b
                    JOIN ars_children c ON c.parent_qid = b.qid
                    WHERE b.is_ars_parent = TRUE
                      AND b.state NOT IN ('COMPLETED', 'ABANDONED')
                      AND b.ars_tail_launched = FALSE
                      AND b.start_time < NOW() - make_interval(secs => %s)
                      AND c.status NOT IN ('DONE', 'ERROR')
                    GROUP BY b.qid, b.response_id
                    """,
                    (float(timeout_sec),),
                )
                rows = await cursor.fetchall()
                timed_out = [
                    {"qid": r[0], "response_id": r[1], "pending": list(r[2])}
                    for r in rows
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_timed_out_ars_parents", e)
                break
            logger.error(
                f"Connection error finding timed-out ARS parents after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to find timed-out ARS parents: {e}")
            break
    return timed_out


async def mark_ars_children_errored(
    parent_qid: str,
    aras: List[str],
    logger: logging.Logger,
):
    """Mark the given parent's pending child ARAs as ERROR (watchdog timeout)."""
    if not aras:
        return
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                await conn.execute(
                    """
                    UPDATE ars_children
                    SET status = 'ERROR', stop_time = NOW(),
                        code = COALESCE(code, 504)
                    WHERE parent_qid = %s AND ara = ANY(%s)
                      AND status NOT IN ('DONE', 'ERROR')
                    """,
                    (parent_qid, aras),
                )
                await conn.commit()
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "mark_ars_children_errored", e)
                break
            logger.error(
                f"Connection error erroring ARS children after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to error ARS children for {parent_qid}: {e}")
            break


async def get_latest_pks(n: int, logger: logging.Logger) -> Dict[str, Any]:
    """Relay ``latest_pk`` parity: per-day parent counts over the last ``n`` days,
    the latest ``n`` parent pks, and parents still running in the last 24h."""
    response: Dict[str, Any] = {
        f"pk_count_last_{n}_days": {},
        f"latest_{n}_pks": [],
        "latest_24hr_running_pks": [],
    }
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                    SELECT start_time::date AS d, COUNT(*)
                    FROM shepherd_brain
                    WHERE is_ars_parent = TRUE
                      AND start_time >= NOW() - make_interval(days => %s)
                    GROUP BY d ORDER BY d
                    """,
                    (n,),
                )
                for row in await cursor.fetchall():
                    response[f"pk_count_last_{n}_days"][str(row[0])] = row[1]
                cursor = await conn.execute(
                    """
                    SELECT qid FROM shepherd_brain WHERE is_ars_parent = TRUE
                    ORDER BY start_time DESC LIMIT %s
                    """,
                    (n,),
                )
                response[f"latest_{n}_pks"] = [r[0] for r in await cursor.fetchall()]
                cursor = await conn.execute(
                    """
                    SELECT qid FROM shepherd_brain
                    WHERE is_ars_parent = TRUE AND state = 'QUEUED'
                      AND start_time > NOW() - interval '24 hours'
                    """
                )
                response["latest_24hr_running_pks"] = [
                    r[0] for r in await cursor.fetchall()
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_latest_pks", e)
                break
            logger.error(f"Connection error in get_latest_pks after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get latest pks: {e}")
            break
    return response


async def get_ars_report(inforesid: str, logger: logging.Logger) -> Dict[str, Any]:
    """Relay ``get_report`` parity: per-child stats for an ARA over the last 24h.

    Matches ``inforesid`` against the child ARA name (``inforesid`` ends with the
    ARA, e.g. ``infores:aragorn`` matches ``aragorn``).
    """
    report: Dict[str, Any] = {}
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                    SELECT child_qid, code, result_count, start_time, stop_time
                    FROM ars_children
                    WHERE start_time > NOW() - interval '24 hours'
                      AND %s ILIKE '%%' || ara
                    """,
                    (inforesid,),
                )
                for row in await cursor.fetchall():
                    child_qid, code, result_count, start_time, stop_time = row
                    elapsed = (
                        str(stop_time - start_time)
                        if (start_time and stop_time)
                        else None
                    )
                    report[child_qid] = {
                        "status_code": code,
                        "time_elapsed": elapsed,
                        "result_count": result_count,
                        "created_at": start_time.isoformat() if start_time else None,
                        "updated_at": stop_time.isoformat() if stop_time else None,
                    }
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "get_ars_report", e)
                break
            logger.error(f"Connection error in get_ars_report after {attempt}: {e}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get report for {inforesid}: {e}")
            break
    return report


async def ping_db(logger: logging.Logger) -> bool:
    """Return True if Postgres answers a trivial query."""
    try:
        async with pool.connection(settings.postgres_pool_timeout) as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"DB ping failed: {e}")
        return False


async def ping_redis(logger: logging.Logger) -> bool:
    """Return True if the Redis data store answers PING."""
    try:
        return bool(await data_db_client.ping())
    except Exception as e:
        logger.error(f"Redis ping failed: {e}")
        return False


async def set_query_retained(
    qid: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Retain a parent query and its ARS children from the purge janitor.

    Relay ``retain_all`` parity: if ``qid`` is a child, the parent is retained
    instead; retention is refused while the parent is still running; on success
    the parent and every child query row get ``retain = TRUE``. Returns a Relay-
    shaped ``{"success", ...}`` dict.
    """
    result: Dict[str, Any] = {"success": False}
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                # Resolve to the parent if a child pk was given.
                cursor = await conn.execute(
                    "SELECT parent_qid FROM ars_children WHERE child_qid = %s LIMIT 1",
                    (qid,),
                )
                row = await cursor.fetchone()
                parent = row[0] if row else qid

                cursor = await conn.execute(
                    "SELECT state FROM shepherd_brain WHERE qid = %s", (parent,)
                )
                state_row = await cursor.fetchone()
                if state_row is None:
                    result = {"success": False, "description": "Invalid PK"}
                    break
                # 'QUEUED' is the in-flight state; refuse like Relay's 'R'.
                if state_row[0] == "QUEUED":
                    result = {
                        "success": False,
                        "parent_pk": parent,
                        "description": "PK still running",
                    }
                    break

                await conn.execute(
                    """
                    UPDATE shepherd_brain SET retain = TRUE
                    WHERE qid = %s
                       OR qid IN (
                           SELECT child_qid FROM ars_children WHERE parent_qid = %s
                       )
                    """,
                    (parent, parent),
                )
                await conn.commit()
                result = {"success": True, "parent_pk": parent}
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "set_query_retained", e)
                break
            logger.error(
                f"Connection error retaining query after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to retain query {qid}: {e}")
            break
    return result


async def list_ars_parents(
    logger: logging.Logger,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """List parent ARS queries (most recent first) for the /ars/messages API."""
    parents: List[Dict[str, Any]] = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(settings.postgres_pool_timeout) as conn:
                cursor = await conn.execute(
                    """
                SELECT qid, start_time, stop_time, response_id, state, status
                FROM shepherd_brain WHERE is_ars_parent = TRUE
                ORDER BY start_time DESC LIMIT %s
                """,
                    (limit,),
                )
                rows = await cursor.fetchall()
                parents = [
                    {
                        "qid": r[0],
                        "start_time": r[1].isoformat() if r[1] else None,
                        "stop_time": r[2].isoformat() if r[2] else None,
                        "response_id": r[3],
                        "state": r[4],
                        "status": r[5],
                    }
                    for r in rows
                ]
            break
        except OperationalError as e:
            if is_disk_full_error(e):
                log_pg_disk_full(logger, "list_ars_parents", e)
                break
            logger.error(
                f"Connection error listing ARS parents after attempt {attempt}: {e}"
            )
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to list ARS parents: {e}")
            raise
    return parents
