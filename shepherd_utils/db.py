"""Postgres DB Manager."""

import logging
from psycopg_pool import AsyncConnectionPool
import redis.asyncio as aioredis
import orjson

# from reasoner_pydantic import (
#     Response as ReasonerResponse,
# )
import time
from typing import Dict, Any, List, Union
import zstandard

from .config import settings

# from shepherd.merge_messages import merge_messages

pool = AsyncConnectionPool(
    conninfo=f"postgresql://postgres:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}",
    timeout=60,
    max_size=20,
    max_idle=300,
    # initialize with the connection closed
    open=False,
)

data_db_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=1,
    password=settings.redis_password,
    max_connections=10,
    timeout=600,
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
        pass
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
    logger.debug(f"Adding query took {time.time() - start} seconds")


async def save_callback_response(
    callback_id: str,
    response: dict[str, Any],
    logger: logging.Logger,
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
    except Exception as e:
        # failed to put message in db
        # TODO: do something more severe
        logger.error(f"Failed to put it on there {e}")
        pass
    logger.debug(f"Saving message took {time.time() - start} seconds")


async def add_callback_id(
    query_id: str,
    callback_id: str,
    logger: logging.Logger,
):
    """Add a callback->query mapping."""
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
    except Exception as e:
        logger.error(f"Failed to save callback: {e}")


async def remove_callback_id(
    callback_id: str,
    logger: logging.Logger,
):
    """Once a callback has been processed, remove it."""
    try:
        async with pool.connection(60) as conn:
            await conn.execute(
                """
            DELETE FROM callbacks WHERE callback_id = %s
            """,
                (callback_id,),
            )
            await conn.commit()
    except Exception as e:
        logger.error("Failed to remove callback after processing.")


async def get_running_callbacks(
    query_id: str,
    logger: logging.Logger,
) -> List[str]:
    """Get all currently running callbacks for a single query."""
    running_lookups = []
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
    except Exception as e:
        logger.error(f"Failed to get running lookups: {e}")
    return running_lookups


async def get_callback_query_id(
    callback_id: str,
    logger: logging.Logger,
) -> Union[str, None]:
    """Given a callback id, get the associated query id."""
    query_id = None
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
    except Exception as e:
        logger.error(f"Failed to get a query id from callback: {e}")
    return query_id


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
        # TODO: do something more severe
        pass
    logger.debug(f"Getting message took {time.time() - start} seconds")
    return message


async def get_query_state(
    query_id: str,
    logger: logging.Logger,
):
    """Get the query state."""
    query_state = None
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
    except Exception as e:
        logger.error(f"Failed to get query state: {e}")
    return query_state


async def set_query_completed(
    query_id: str,
    status: str,
    logger: logging.Logger,
):
    """This query is done."""
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
    except Exception as e:
        logger.error(f"Failed to successfully complete query in db: {e}")
