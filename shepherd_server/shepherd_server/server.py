"""Shepherd ARA."""
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import logging
import pydantic
from typing import Tuple, Optional
from reasoner_pydantic import (
    Query,
    AsyncQuery,
    Response as ReasonerResponse,
)
import uuid

from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import (
  initialize_db,
  shutdown_db,
  add_query,
  save_callback_response,
  get_callback_query_id,
  get_message,
#   merge_message,
)
from shepherd_utils.broker import add_task
from shepherd_server.shepherd_server.openapi import construct_open_api_schema

# from shepherd.retrieval import retrieve
# from shepherd.scoring.score import score_query

# from shepherd.operations import (
#     sort_results_score,
#     filter_results_top_n,
#     filter_kgraph_orphans,
# )

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle db connection."""
    await initialize_db()
    yield
    await shutdown_db()


APP = FastAPI(title="BioPack Shepherd", version="0.0.3", lifespan=lifespan)

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


# supported_operations = {
#     "lookup": retrieve,
#     "score": score_query,
#     "sort_results_score": sort_results_score,
#     "filter_results_top_n": filter_results_top_n,
#     "filter_kgraph_orphans": filter_kgraph_orphans,
# }

# default_workflow = [
#     {"id": "lookup"},
#     {"id": "score"},
#     {"id": "sort_results_score", "parameters": {"ascending_or_descending": "descending"}},
#     {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
#     {"id": "filter_kgraph_orphans"},
# ]


async def run_query(
    target: str,
    query: dict,
    callback_url: Optional[str] = None,
) -> Tuple[str, str, logging.Logger]:
    """Run a single query."""
    query_id = str(uuid.uuid4())[:8]
    response_id = str(uuid.uuid4())[:8]
    # Set up logger
    log_level = query.get("log_level") or "INFO"
    level_number = logging._nameToLevel[log_level]
    log_handler = QueryLogger().log_handler
    logger = logging.getLogger(f"shepherd.{query_id}")
    logger.setLevel(level_number)
    logger.addHandler(log_handler)

    # save query to db
    try:
        await add_query(query_id, response_id, query, callback_url, logger)
        await add_task(target, {"query_id": query_id})
    except Exception as e:
        logger.error(f"Failed to save query: {e}")
        # TODO: set query to failed state

    return query_id, response_id, logger


# @APP.post("/{target}/query", response_model=ReasonerResponse)
@APP.post("/{target}/query")
async def query(
    target: str,
    # query: Query,
    query: dict,
) -> dict:
    """Handle synchronous TRAPI queries."""
    # query_dict = query.dict()
    query_dict = query
    query_id, target_id, logger = await run_query(target, query_dict)
    # TODO: poll for completed status
    # TODO: grab final response
    return {}


@APP.post("/{target}/asyncquery", response_model=ReasonerResponse)
async def async_query(
    target: str,
    query: AsyncQuery,
) -> Response:
    """Handle asynchronous TRAPI queries."""
    query_dict = query.dict()
    callback_url = query_dict.get("callback")
    if callback_url is None:
        return Response("Missing callback url.", 422)
    query_id, _, _ = await run_query(target, query_dict, callback_url)
    return Response(f"Query {query_id} received.", 200)


@APP.post("/callback/{callback_id}", status_code=200, include_in_schema=False)
async def callback(
    callback_id: str,
    response: dict,
) -> Response:
    """Handle asynchronous callback queries from subservices."""
    # Set up logger
    log_level = response.get("log_level") or "INFO"
    level_number = logging._nameToLevel[log_level]
    log_handler = QueryLogger().log_handler
    logger = logging.getLogger(f"shepherd.{callback_id}")
    logger.setLevel(level_number)
    logger.addHandler(log_handler)
    # try:
    #     ReasonerResponse.parse_obj(response)
    # except pydantic.ValidationError as e:
    #     logger.error(f"Received a non TRAPI-compliant callback response: {e}")
    #     response = {
    #         "message": {
    #             "query_graph": {
    #                 "nodes": {},
    #                 "edges": {},
    #             },
    #             "knowledge_graph": {
    #                 "nodes": {},
    #                 "edges": {},
    #             },
    #             "results": [],
    #             "auxiliary_graphs": {},
    #         }
    #     }
    logger.info(f"Got back {len(response['message']['results'])} results.")
    # get associated query id for this callback
    query_id = await get_callback_query_id(callback_id, logger)
    logger.info(f"Got original query id: {query_id}")
    # save callback to redis
    logger.info(f"Saving callback {callback_id} to redis")
    await save_callback_response(callback_id, response, logger)
    logger.info(f"Saved callback {callback_id} to redis")
    # add new task to merge callback response into original message
    await add_task("merge_message", {"query_id": query_id, "callback_id": callback_id})
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


@APP.get("/response/{query_id}", status_code=200)
async def get_query_response(
    query_id: str,
):
    """Get a query response."""
    response = await get_message(query_id)
    if response is None:
        return 404
    return response
