"""Shepherd ARA."""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pydantic
from reasoner_pydantic import (
    Query,
    AsyncQuery,
    Response as ReasonerResponse,
)
from typing import Dict

from shepherd.db import initialize_db, shutdown_db, add_query, merge_message, get_message
from shepherd.openapi import construct_open_api_schema

from shepherd.query_expansion.query_expansion import expand_query
from shepherd.retrieval import retrieve
from shepherd.scoring.score import score_query

from shepherd.operations import (
    sort_results_score,
    filter_results_top_n,
    filter_kgraph_orphans,
)

import json

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
        if notification.payload == "done":
            break


@APP.post("/{target}/query", status_code=200, response_model=ReasonerResponse)
async def query(
    target: str,
    query: Query,
) -> ReasonerResponse:
    """Handle synchronous TRAPI queries."""
    query = query.dict()
    # expand query to multiple subqueries, options
    queries, options = expand_query(query, {"target": target})
    print(json.dumps(queries))
    # save query to db
    query_id, conn, pool = await add_query(query, len(queries))
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
    merged_message = await get_message(query_id)
    # score/rank
    scored_message = await score_query(merged_message, {"target": target})
    # sort results
    sorted_message = sort_results_score(scored_message, query_id)
    # filter results top n
    trimmed_message = filter_results_top_n(sorted_message, query_id)
    # filter kgraph orphans
    filtered_message = filter_kgraph_orphans(trimmed_message, query_id)
    # put the connection back into the pool. Don't want a dry pool.
    await pool.putconn(conn)
    print(f"Returning {len(filtered_message['message']['results'])} results.")
    return filtered_message


@APP.post("/{target}/asyncquery", status_code=200, response_model=ReasonerResponse)
async def async_query(
    target: str,
    query: AsyncQuery,
) -> ReasonerResponse:
    """Handle asynchronous TRAPI queries."""
    query = query.dict()
    # expand query to multiple subqueries, options
    queries, options = expand_query(query, {"target": target})
    print(queries, options)
    # save query to db
    query_id, conn = await add_query(query, len(queries))
    return


@APP.post("/callback/{query_id}", status_code=200, include_in_schema=False)
async def callback(
    query_id: str,
    response: dict,
) -> None:
    """Handle asynchronous callback queries from subservices."""
    # print(json.dumps(response))
    try:
        ReasonerResponse.parse_obj(response)
    except pydantic.ValidationError:
        print("Received a non TRAPI-compliant callback response.")
        response = {
            "message": {
                "query_graph": {
                    "nodes": {},
                    "edges": {},
                },
                "knowledge_graph": None,
                "results": None,
                "auxiliary_graphs": None,
            }
        }
    print(f"Got back {len(response['message']['results'])} results.")
    # TODO: make this a background task
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
