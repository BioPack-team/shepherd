"""Postgres DB Manager."""
import asyncio
import gzip
import json
from psycopg import Connection, sql
from psycopg_pool import AsyncConnectionPool
import os
from reasoner_pydantic import (
    Query,
    AsyncQuery,
    Response as ReasonerResponse,
)
from typing import Dict, Any
import uuid

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "supersecretpassw0rd")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
pool = AsyncConnectionPool(
    conninfo=f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432",
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
    query: Query,
    num_queries: int,
) -> tuple[str, Connection, AsyncConnectionPool]:
    """
    Add an initial query to the db.

    Args:
        query (Dict): TRAPI query graph
        num_queries (int): how many expected result callbacks there are
    
    Returns:
        query_id: str
    """
    query_id = str(uuid.uuid4())[:8]
    conn = await pool.getconn(timeout=10)
    await conn.execute("""
INSERT INTO shepherd_brain VALUES (
    %s, %s, %s, %s
)
""", (
    query_id,
    gzip.compress(json.dumps(query).encode()),
    gzip.compress(json.dumps(query).encode()),
    num_queries,
))
    await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(query_id)))
    await conn.commit()
    return query_id, conn, pool


async def merge_message(
    query_id: str,
    response: ReasonerResponse,
) -> None:
    """Merge an incoming message with the existing full message."""
    print(query_id)
    async with pool.connection(timeout=10) as conn:
        # FOR UPDATE puts a lock on the row until the transaction is completed
        cursor = await conn.execute("""
SELECT merged_message, num_queries FROM shepherd_brain WHERE query_id = %s FOR UPDATE;
""", (query_id,))
        print("got the message")
        row = await cursor.fetchone()
        await asyncio.sleep(2)
        merged_message = response
        # do message merging
        await conn.execute("""
UPDATE shepherd_brain SET merged_message = %s, num_queries = %s WHERE query_id = %s;
""", (gzip.compress(json.dumps(merged_message).encode()), row[1] - 1, query_id,))
        await conn.execute(sql.SQL("NOTIFY {}, {};").format(sql.Identifier(query_id), 'done' if row[1] - 1 == 0 else 'not done'))
        await conn.commit()


async def clear_db() -> None:
    """Clear whole db."""
    async with pool.connection() as conn:
        await conn.execute()
