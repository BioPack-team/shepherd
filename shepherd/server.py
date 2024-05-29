"""Shepherd ARA."""
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import pydantic
from reasoner_pydantic import (
    Query,
    AsyncQuery,
    Response as ReasonerResponse,
)

from shepherd.db import initialize_db, shutdown_db, add_query, merge_message
from shepherd.openapi import construct_open_api_schema

from shepherd.retrieval import retrieve
from shepherd.scoring.score import score_query

from shepherd.operations import (
    sort_results_score,
    filter_results_top_n,
    filter_kgraph_orphans,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle db connection."""
    await initialize_db()
    yield
    await shutdown_db()


APP = FastAPI(title="BioPack Shepherd", version="0.0.1", lifespan=lifespan)

APP.openapi_schema = construct_open_api_schema(
    APP,
    description="Sheperd: Fully modular ARA.",
    infores="infores:shepherd",
)

APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


supported_operations = {
    "lookup": retrieve,
    "score": score_query,
    "sort_results_score": sort_results_score,
    "filter_results_top_n": filter_results_top_n,
    "filter_kgraph_orphans": filter_kgraph_orphans,
}

default_workflow = [
    {"id": "lookup"},
    {"id": "score"},
    {"id": "sort_results_score"},
    {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
    {"id": "filter_kgraph_orphans"},
]


async def run_query(
    target: str,
    query: dict,
) -> ReasonerResponse:
    """Run a single query."""
    # save query to db
    query_id, conn, pool = await add_query(query)

    shepherd_options = {"target": target, "conn": conn}
    final_message = query
    workflow = query.get("workflow")
    if workflow is None:
        workflow = default_workflow
    for operation in workflow:
        if operation["id"] in supported_operations:
            try:
                final_message = await supported_operations[operation["id"]](query_id, final_message, operation.get("parameters", {}), shepherd_options)
                print(final_message)
            except Exception as e:
                print(f"Operation {operation['id']} failed! {e}")
        else:
            print(f"Operation {id} is not supported by Shepherd.")
    # put the connection back into the pool. Don't want a dry pool.
    await pool.putconn(conn)
    print(f"Returning {len(final_message['message']['results'])} results.")
    return final_message


@APP.post("/{target}/query", status_code=200, response_model=ReasonerResponse)
async def query(
    target: str,
    query: Query,
) -> ReasonerResponse:
    """Handle synchronous TRAPI queries."""
    query_dict = query.dict()
    return_message = await run_query(target, query_dict)
    return return_message


async def async_run_query(
    target: str,
    query_dict: dict,
    callback_url: str,
) -> None:
    """Run a single async query."""
    return_message = await run_query(target, query_dict)
    async with httpx.AsyncClient(timeout=600) as client:
        await client.post(callback_url, json=return_message)


@APP.post("/{target}/asyncquery", status_code=200, response_model=ReasonerResponse)
async def async_query(
    background_tasks: BackgroundTasks,
    target: str,
    query: AsyncQuery,
) -> Response:
    """Handle asynchronous TRAPI queries."""
    query_dict = query.dict()
    callback_url = query_dict.get("callback")
    if callback_url is None:
        return Response("Missing callback url.", 422)
    background_tasks.add_task(async_run_query, target, query_dict, callback_url)
    return Response("Query received.", 200)


@APP.post("/callback/{query_id}", status_code=200, include_in_schema=False)
async def callback(
    background_tasks: BackgroundTasks,
    query_id: str,
    response: dict,
) -> Response:
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
    background_tasks.add_task(merge_message, query_id, response)
    return Response("Callback received.", 200)


@APP.get("/asyncquery_status/{qid}", status_code=200)
async def query_status(
    qid: str,
) -> dict:
    """Handle query status requests."""
    # get query status from db
    return {
        "status": "Queued",
        "description": "Query is currently waiting to be run.",
        "logs": [],
    }
