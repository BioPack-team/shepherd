"""Postgres DB Manager."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Union

import orjson
import redis.asyncio as aioredis
import zstandard
from psycopg import OperationalError
from psycopg_pool import AsyncConnectionPool

from .config import settings

PG_RETRIES = 5

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
    if conn.closed:
        raise OperationalError("Connection is closed.")


pool = AsyncConnectionPool(
    conninfo=CONNINFO,
    timeout=60,
    min_size=5,
    max_size=20,
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
)


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
):
    """
    Add an initial query to the db.

    Args:
        query (Dict): TRAPI query graph

    Returns:
        query_id: str
    """
    start = time.time()
    try:
        client = await aioredis.Redis(
            connection_pool=data_db_pool,
        )
        # print(f"Putting {query_id} on {ara_target} stream")
        await client.set(query_id, zstandard.compress(orjson.dumps(query)))
        await client.set(response_id, zstandard.compress(orjson.dumps(query)))
        await client.aclose()
    except Exception as e:
        # failed to put message in db
        # TODO: do something more severe
        logger.error(f"Failed to save initial query or response: {e}")
        raise Exception("Failed to save initial query or response.")
    try:
        async with pool.connection(60) as conn:
            await conn.execute(
                """
            INSERT INTO shepherd_brain (qid, start_time, response_id, callback_url, state, status) VALUES (
                %s, NOW(), %s, %s, %s, %s
            )
            """,
                (query_id, response_id, callback_url, "QUEUED", "OK"),
            )
            # await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(query_id)))
            await conn.commit()
    except Exception as e:
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
        compressed = zstandard.compress(orjson.dumps(response))
        logger.info(f"Compression took {time.time() - start_comp}")
        client = await aioredis.Redis(
            connection_pool=data_db_pool,
        )
        # print(f"Putting {query_id} on {ara_target} stream")
        await client.set(callback_id, compressed)
        await client.aclose()
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
) -> Dict:
    """Get the message from db."""
    message = {}
    start = time.time()
    try:
        client = await aioredis.Redis(
            connection_pool=data_db_pool,
        )
        # print(f"Putting {query_id} on {ara_target} stream")
        message = await client.get(message_id)
        await client.aclose()
        if message is not None:
            start_decomp = time.time()
            message = orjson.loads(zstandard.decompress(message))
            logger.debug(f"Decompression took {time.time() - start_decomp}")
    except Exception as e:
        # failed to get message from db
        logger.error(f"Failed to get {message_id} from db: {e}")
    logger.debug(f"Getting message took {time.time() - start} seconds")
    return message


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
        client = await aioredis.Redis(
            connection_pool=logs_db_pool,
        )
        existing_logs = await client.get(response_id)
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
        await client.set(response_id, orjson.dumps(existing_logs))
        await client.aclose()
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
        client = await aioredis.Redis(
            connection_pool=logs_db_pool,
        )
        logs = await client.get(response_id)
        await client.aclose()
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
    logger: logging.Logger,
):
    """Add a callback->query mapping."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(60) as conn:
                await conn.execute(
                    """
                INSERT INTO callbacks (query_id, callback_id) VALUES (
                    %s, %s
                )
                """,
                    (
                        query_id,
                        callback_id,
                    ),
                )
                await conn.commit()
        except OperationalError as e:
            logger.error(f"Connection error on attempt {attempt}: {e}")
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to save callback: {e}")


async def remove_callback_id(
    callback_id: str,
    logger: logging.Logger,
):
    """Once a callback has been processed, remove it."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(60) as conn:
                await conn.execute(
                    """
                DELETE FROM callbacks WHERE callback_id = %s
                """,
                    (callback_id,),
                )
                await conn.commit()
        except OperationalError as e:
            logger.error(
                f"Connection error removing callback id after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error("Failed to remove callback after processing.")


async def get_running_callbacks(
    query_id: str,
    logger: logging.Logger,
) -> List[str]:
    """Get all currently running callbacks for a single query."""
    running_lookups = []
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(60) as conn:
                cursor = await conn.execute(
                    """
                SELECT callback_id FROM callbacks WHERE query_id = %s
                """,
                    (query_id,),
                )
                rows = await cursor.fetchall()
                running_lookups = rows
        except OperationalError as e:
            logger.error(
                f"Connection error getting running callbacks after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get running lookups: {e}")
    return running_lookups


async def get_callback_query_id(
    callback_id: str,
    logger: logging.Logger,
) -> Union[str, None]:
    """Given a callback id, get the associated query id."""
    query_id = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(60) as conn:
                cursor = await conn.execute(
                    """
                SELECT query_id FROM callbacks WHERE callback_id = %s
                """,
                    (callback_id,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    query_id = row[0]
        except OperationalError as e:
            logger.error(
                f"Connection error getting query id from callback after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get a query id from callback: {e}")
    return query_id


async def get_query_state(
    query_id: str,
    logger: logging.Logger,
):
    """Get the query state."""
    query_state = None
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(60) as conn:
                cursor = await conn.execute(
                    """
                SELECT * FROM shepherd_brain WHERE qid = %s
                """,
                    (query_id,),
                )
                row = await cursor.fetchone()
                query_state = row
        except OperationalError as e:
            logger.error(
                f"Connection error getting query state after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to get query state: {e}")
    return query_state


async def set_query_completed(
    query_id: str,
    status: str,
    logger: logging.Logger,
):
    """This query is done."""
    for attempt in range(PG_RETRIES):
        try:
            async with pool.connection(60) as conn:
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
        except OperationalError as e:
            logger.error(
                f"Connection error setting query completed after attempt {attempt}: {e}"
            )
            logger.info(f"Pool stats: {pool.get_stats()}")
            await asyncio.sleep(0.1 * (2**attempt))
            continue
        except Exception as e:
            logger.error(f"Failed to successfully complete query in db: {e}")
