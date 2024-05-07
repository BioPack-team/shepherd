"""Shepherd ARA."""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import (
    Query,
    AsyncQuery,
    Response as ReasonerResponse,
)
from typing import Dict

from shepherd.db import initialize_db, shutdown_db, add_query, merge_message
from shepherd.openapi import construct_open_api_schema

from shepherd.query_expansion.query_expansion import expand_query
from shepherd.retrieval import retrieve

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle db connection."""
    await initialize_db()
    yield
    await shutdown_db()


APP = FastAPI(title="BioPack Shepherd", lifespan=lifespan)

APP.openapi_schema = construct_open_api_schema(APP, description="Sheperd: Fully modular ARA.")

APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def track_query(conn):
    notifications = conn.notifies()
    async for notification in notifications:
        print("got notification")
        print(notification)
        if notification.payload == "done":
            break


@APP.post("/query", status_code=200, response_model=ReasonerResponse)
async def query(
    query: Dict,
) -> dict:
    """Handle synchronous TRAPI queries."""
    # expand query to multiple subqueries, options
    queries, options = expand_query(query, {"target": "aragorn"})
    # save query to db
    query_id, conn, pool = await add_query(query, len(queries))
    print(query_id)
    # retrieve answers
    await retrieve(query_id, queries, options)
    # poll the db for doneness?
    # track task by postgres notification
    # also have timeout to continue if not all queries are done.
    # https://stackoverflow.com/a/65242071
    query_timeout = 300
    track_task = asyncio.create_task(track_query(conn))
    timeout_task = asyncio.create_task(asyncio.sleep(query_timeout))
    await asyncio.wait(
        [track_task, timeout_task],
        return_when=asyncio.FIRST_COMPLETED)

    if not track_task.done():
        track_task.cancel()
    # score/rank
    # filter results top n
    # filter kgraph orphans
    # put the connection back into the pool. Don't want a dry pool.
    await pool.putconn(conn)
    return query


@APP.post("/asyncquery", status_code=200, response_model=ReasonerResponse)
async def async_query(
    query: Dict,
) -> dict:
    """Handle asynchronous TRAPI queries."""
    # expand query to multiple subqueries, options
    queries, options = expand_query(query, {"target": "aragorn"})
    print(queries, options)
    # save query to db
    query_id, conn = await add_query(query, len(queries))
    return


@APP.post("/callback/{query_id}", status_code=200, include_in_schema=False)
async def callback(
    query_id: str,
    response: ReasonerResponse,
) -> None:
    """Handle asynchronous callback queries from subservices."""
    # retrieve query from db
    # put a lock on the row
    # message merge
    # put merged message back into db
    await merge_message(query_id, response)
    return 200


@APP.get("/asyncquery_status/{qid}", status_code=200)
async def query_status(
    qid: str,
) -> dict:
    """Handle query status requests."""
    return {
        "status": "Queued",
        "description": "Query is currently waiting to be run.",
        "logs": [],
    }
