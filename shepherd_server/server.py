"""Shepherd ARA."""

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional, Tuple

from fastapi import Body, FastAPI, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import (
    get_swagger_ui_html,
)
from fastapi.staticfiles import StaticFiles
from starlette.responses import HTMLResponse

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import inject
from opentelemetry import trace

from shepherd_server.openapi import construct_open_api_schema
from shepherd_utils.broker import add_task
from shepherd_utils.config import settings
from shepherd_utils.db import (
    add_query,
    get_callback_query_id,
    get_logs,
    get_message,
    get_query_state,
    initialize_db,
    save_message,
    shutdown_db,
)
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.otel import setup_tracer

setup_logging()
tracer = setup_tracer("shepherd-server")

DESCRIPTION = """
<img src="/static/favicon.png" width="200px">
<br /><br />
Shepherd: Translator Autonomous Relay Agent Platform
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle db connection."""
    await initialize_db()
    yield
    await shutdown_db()


APP = FastAPI(
    title="BioPack Shepherd",
    description=DESCRIPTION,
    version="0.1.2",
    lifespan=lifespan,
    docs_url=None,
)


APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APP.mount("/static", StaticFiles(directory="shepherd_server/static"), name="static")

FastAPIInstrumentor.instrument_app(APP, excluded_urls="docs,openapi.json")

default_input_query: dict = {
    "message": {
        "query_graph": {
            "edges": {
                "e01": {
                    "object": "n0",
                    "subject": "n1",
                    "predicates": [
                        "biolink:regulates"
                    ]
                }
            },
            "nodes": {
                "n0": {
                    "ids": [
                        "NCBIGene:23321"
                    ],
                    "categories": [
                        "biolink:Gene"
                    ]
                },
                "n1": {
                    "categories": [
                        "biolink:Gene"
                    ]
                }
            }
        },
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "results": [],
        "auxiliary_graphs": {}
    }
}


async def run_query(
    target: str,
    query: dict,
    callback_url: Optional[str] = None,
) -> Tuple[str, str, logging.Logger]:
    """Run a single query."""
    query_id = str(uuid.uuid4())[:8]
    response_id = str(uuid.uuid4())[:8]
    # Set up logger
    log_level = query.get("log_level") or settings.log_level
    level_number = logging._nameToLevel[log_level]
    log_handler = QueryLogger().log_handler
    logger = logging.getLogger(f"shepherd.{query_id}")
    logger.setLevel(level_number)
    logger.addHandler(log_handler)

    logger.info(f"Sending {query_id} to {target}")

    with tracer.start_as_current_span("") as span:
        span_carrier = {}
        # adds otel trace to carrier for next worker
        inject(span_carrier)

    # save query to db
    try:
        await add_query(query_id, response_id, query, callback_url, logger)
        await add_task(
            target,
            {
                "query_id": query_id,
                "response_id": response_id,
                "otel": json.dumps(span_carrier),
            },
            logger,
        )
    except Exception as e:
        logger.error(f"Failed to save query: {e}")
        # TODO: set query to failed state

    return query_id, response_id, logger


# @APP.post("/{target}/query", response_model=ReasonerResponse)
@APP.post("/{target}/query")
async def sync_query(
    target: str = "aragorn",
    # query: Query,
    query: dict = Body(..., example=default_input_query),
) -> dict:
    """Handle synchronous TRAPI queries."""
    # query_dict = query.dict()
    query_dict = query
    query_id, response_id, logger = await run_query(target, query_dict)
    start = time.time()
    now = start
    while now <= start + 360:
        now = time.time()
        # poll for completed status
        query_state = await get_query_state(query_id, logger)
        if query_state is not None:
            # logger.info(query_state)
            state = query_state[9]
            if state == "COMPLETED":
                # grab final response
                response_id = query_state[7]
                response = await get_message(response_id, logger)
                if response is None:
                    return {"status": "ERROR", "description": "Unable to get response"}
                logs = await get_logs(response_id, logger)
                response["logs"] = logs
                return response
        else:
            logger.warning(f"Failed to get the query state of query id {query_id}")
        await asyncio.sleep(5)

    logger.error("Query timed out")
    return {
        "status": "TIMEOUT",
        "description": "Query timeout",
    }


@APP.post("/{target}/asyncquery")
async def async_query(
    target: str = "aragorn",
    query: dict = Body(..., example=default_input_query),
) -> Response:
    """Handle asynchronous TRAPI queries."""
    callback_url = query.get("callback")
    if callback_url is None:
        return Response("Missing callback url.", 422)
    query_id, _, _ = await run_query(target, query, callback_url)
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
    # logger.info(response)
    results = response["message"].get("results")
    if results is None:
        response["message"]["results"] = []
    kgraph = response["message"].get("knowledge_graph")
    if kgraph is None:
        response["message"]["knowledge_graph"] = {
            "nodes": {},
            "edges": {},
        }

    logger.info(f"Got back {len(response['message']['results'])} results.")
    # get associated query id for this callback
    query_id = await get_callback_query_id(callback_id, logger)
    logger.info(f"Got original query id: {query_id}")
    if query_id is None:
        return Response("Couldn't find original query.", 500)
    query_state = await get_query_state(query_id, logger)
    if query_state is None:
        return Response("Failed to get query state.", 500)
    response_id = query_state[7]
    # save callback to redis
    logger.info(f"Saving callback {callback_id} to redis")
    await save_message(callback_id, response, logger)
    logger.info(f"Saved callback {callback_id} to redis")
    # add new task to merge callback response into original message
    await add_task(
        "merge_message",
        {
            "query_id": query_id,
            "response_id": response_id,
            "callback_id": callback_id,
        },
        logger,
    )
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
    level_number = logging._nameToLevel["INFO"]
    log_handler = QueryLogger().log_handler
    logger = logging.getLogger("shepherd.get_query")
    logger.setLevel(level_number)
    logger.addHandler(log_handler)
    response = await get_message(query_id, logger)
    if response is None:
        return 404
    logs = await get_logs(query_id, logger)
    response["logs"] = logs
    return response


@APP.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    print(root_path)
    openapi_url = root_path + APP.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=APP.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


APP.openapi_schema = construct_open_api_schema(
    APP,
    description=DESCRIPTION,
    infores="infores:shepherd",
)
