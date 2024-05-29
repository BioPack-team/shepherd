"""Postgres DB Manager."""
import copy
import gzip
import json
import logging
from psycopg import Connection, sql
from psycopg_pool import AsyncConnectionPool
import os
from reasoner_pydantic import (
    Response as ReasonerResponse,
)
import time
from typing import Dict, Any

from shepherd.config import settings
from shepherd.merge_messages import merge_messages

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "supersecretpassw0rd")
pool = AsyncConnectionPool(
    conninfo=f"postgresql://postgres:{POSTGRES_PASSWORD}@{settings.postgres_host}:{settings.postgres_port}",
    timeout=5,
    max_size=20,
    max_idle=300,
    # initialize with the connection closed
    open=False,
)


async def initialize_db() -> None:
    """Open connection and create db."""
    await pool.open()
    async with pool.connection() as conn:
        await conn.execute("""
CREATE TABLE IF NOT EXISTS shepherd_brain (
    query_id varchar(255) PRIMARY KEY,
    query bytea,
    merged_message bytea,
    num_queries int
);
""")
        await conn.commit()


async def shutdown_db() -> None:
    """Close the connection to the db."""
    await pool.close()


async def add_query(
    query_id: str,
    query: dict[str, Any],
    logger: logging.Logger,
) -> tuple[Connection, AsyncConnectionPool]:
    """
    Add an initial query to the db.

    Args:
        query (Dict): TRAPI query graph
    
    Returns:
        query_id: str
    """
    conn = await pool.getconn(timeout=10)
    await conn.execute("""
INSERT INTO shepherd_brain VALUES (
    %s, %s, %s
)
""", (
    query_id,
    gzip.compress(json.dumps(query).encode()),
    gzip.compress(json.dumps(query).encode()),
))
    await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(query_id)))
    await conn.commit()
    logger.info("Query saved successfully.")
    return conn, pool


async def update_query(
    query_id: str,
    num_queries: int,
) -> None:
    """
    Update num_queries of query.

    Args:
        query_id (str): Unique query id
        num_queries (int): how many expected result callbacks there are
    """
    conn = await pool.getconn(timeout=10)
    await conn.execute("""
UPDATE shepherd_brain SET num_queries = %s WHERE query_id = %s;
""", (num_queries, query_id,))
    await conn.commit()


async def merge_message(
    query_id: str,
    response: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Merge an incoming message with the existing full message."""
    # retrieve query from db
    # put a lock on the row
    # message merge
    # put merged message back into db
    async with pool.connection(timeout=10) as conn:
        # FOR UPDATE puts a lock on the row until the transaction is completed
        cursor = await conn.execute("""
SELECT query, merged_message, num_queries FROM shepherd_brain WHERE query_id = %s FOR UPDATE;
""", (query_id,))
        row = await cursor.fetchone()
        if row is None:
            raise Exception(f"Query with id {query_id} not found in database.")
        merged_message = json.loads(gzip.decompress(row[1]))
        original_qgraph: Dict = json.loads(gzip.decompress(row[0]))["message"]["query_graph"]
        lookup_qgraph = copy.deepcopy(original_qgraph)
        for qedge_id in lookup_qgraph["edges"]:
            del lookup_qgraph["edges"][qedge_id]["knowledge_type"]
        start = time.time()
        logger.debug(f"Merging new response message")
        # do message merging
        merged_message = merge_messages(original_qgraph, lookup_qgraph, [merged_message, response])
        stop = time.time()
        logger.debug(f"Message merging took {stop - start} seconds")
        await conn.execute("""
UPDATE shepherd_brain SET merged_message = %s, num_queries = %s WHERE query_id = %s;
""", (gzip.compress(json.dumps(merged_message).encode()), row[2] - 1, query_id,))
        await conn.execute(sql.SQL("NOTIFY {}, {};").format(sql.Identifier(query_id), 'done' if row[2] - 1 == 0 else 'not done'))
        await conn.commit()


async def get_message(
    query_id: str,
) -> Dict:
    """Get the final merged message from db."""
    async with pool.connection(timeout=10) as conn:
        cursor = await conn.execute("""
SELECT merged_message FROM shepherd_brain WHERE query_id = %s;
""", (query_id,))
        row = await cursor.fetchone()
        if row is None:
            raise Exception(f"Query with id {query_id} was not found in the database.")
        await conn.commit()
        return json.loads(gzip.decompress(row[0]))


# async def clear_db() -> None:
#     """Clear whole db."""
#     async with pool.connection() as conn:
#         await conn.execute()
