"""Query Retrieval."""
import asyncio
import httpx
import json
import os
from psycopg import Connection
from typing import Dict, Any

from shepherd.db import get_message, update_query
from shepherd.query_expansion.query_expansion import expand_query

default_retriever_url = "http://localhost:3000/v1/asyncquery"
retriever_url = os.environ.get("RETRIEVER_URL", default_retriever_url)


async def send_query(query_id: str, query, semaphore):
    """Send a single query to Retriever."""
    async_timeout = 30
    try:
        async with semaphore:
            # had an open spot in the queue
            callback_host = os.environ.get("CALLBACK_HOST", "http://127.0.0.1:5439")
            # callback_host = "http://localhost:3000"

            # callback_url = f"{callback_host}/query"
            callback_url = f"{callback_host}/callback/{query_id}"
            query["callback"] = callback_url
            async with httpx.AsyncClient(timeout=async_timeout) as client:
                response = await client.post(
                    retriever_url,
                    json=query,
                )
                response.raise_for_status()
    except httpx.ReadTimeout:
        print(f"Retriever took longer than {async_timeout} seconds to response.")
    except httpx.RequestError:
        print("Request error contacting Retriever.")
    except httpx.HTTPStatusError as e:
        print(e.response.text)
    except Exception as e:
        print(e)


async def track_query(db_conn: Connection):
    notifications = db_conn.notifies()
    async for notification in notifications:
        if notification.payload == "done":
            break


def examine_query(message: Dict[str, Any]) -> bool:
    """Decides whether the input is an infer. Returns the grouping node."""
    # Currently, we support:
    # queries that are any shape with all lookup edges
    # OR
    # A 1-hop infer query.
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qedges = message.get("message", {}).get("query_graph", {}).get("edges", {})
    except Exception:
        qedges = {}
    n_infer_edges = 0
    for edge_id in qedges:
        if qedges.get(edge_id, {}).get("knowledge_type", "lookup") == "inferred":
            n_infer_edges += 1
    if n_infer_edges > 1:
        raise Exception("Only a single infer edge is supported", 400)
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported", 400)
    inferred = n_infer_edges == 1
    
    return inferred


async def retrieve(
    query_id: str,
    query: Dict[str, Any],
    operation: Dict[str, Any],
    shepherd_options: Dict[str, Any],
):
    """Send all queries to Retriever."""
    retrieval_options = {}
    inferred = examine_query(query)
    if inferred:
        # expand query to multiple subqueries, options
        queries, retrieval_options = expand_query(query, shepherd_options)
        print(json.dumps(queries))
    else:
        queries = [query]
    
    # update query in db
    await update_query(query_id, len(queries))

    semaphore = asyncio.Semaphore(retrieval_options.get("concurrency", 1))
    queries = [
        asyncio.create_task(send_query(query_id, query, semaphore))
        for query in queries
    ]
    await asyncio.gather(*queries)
    # track task by postgres notification
    # also have timeout to continue if not all queries are done.
    # https://stackoverflow.com/a/65242071
    query_timeout = 300
    try:
        db_conn: Connection = shepherd_options["conn"]
    except KeyError:
        raise KeyError("DB Connection is not available")
    track_task = asyncio.create_task(track_query(db_conn))
    try:
        await asyncio.wait_for(
            track_task,
            query_timeout,
        )
    except asyncio.TimeoutError:
        print("Timing out lookups.")

    if not track_task.done():
        track_task.cancel()

    # message should be done now
    merged_message = await get_message(query_id)
    return merged_message
