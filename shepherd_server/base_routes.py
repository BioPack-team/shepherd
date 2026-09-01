"""Base API routes that all Shepherd ARAs can use."""

import asyncio
import json
import logging
import time
import uuid
from enum import Enum
from typing import Optional, Tuple

import orjson
import zstandard
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, ORJSONResponse
from opentelemetry.propagate import extract, inject

from shepherd_utils.broker import add_task
from shepherd_utils.config import settings
from shepherd_utils.db import (
    add_query,
    add_ready_callback,
    decompress_zstd,
    get_callback_query_id,
    get_logs,
    get_message,
    get_query_log_level,
    get_query_state,
    remove_callback_id,
    save_logs,
    save_message,
)
from shepherd_utils.logger import (
    attach_query_handler,
    resolve_log_level,
    setup_logging,
)
from shepherd_utils.otel import setup_tracer

setup_logging()

tracer = setup_tracer("shepherd-server")

base_router = APIRouter()


class QueryIntakeError(Exception):
    """Raised when a query can't be accepted because its initial state could
    not be persisted (e.g. the datastore is full or unavailable).

    The message is client-safe -- it's surfaced verbatim in the HTTP response,
    so it must not leak connection details. The specific underlying cause is
    logged server-side (see ``PG_DISK_FULL`` in ``shepherd_utils.db``)."""


class QueryBodyError(Exception):
    """Raised when a posted query body isn't a JSON object.

    Client-safe like ``QueryIntakeError``: the message is returned verbatim as
    the 422 ``detail``."""


class ARATargetEnum(str, Enum):
    ARAGORN = "aragorn"
    ARAX = "arax"
    BTE = "bte"
    EXAMPLE = "example"
    SIPR = "sipr"


default_input_query: dict = {
    "message": {
        "query_graph": {
            "edges": {
                "e01": {
                    "object": "n0",
                    "subject": "n1",
                    "predicates": ["biolink:regulates"],
                }
            },
            "nodes": {
                "n0": {"ids": ["NCBIGene:23321"], "categories": ["biolink:Gene"]},
                "n1": {"categories": ["biolink:Gene"]},
            },
        },
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "results": [],
        "auxiliary_graphs": {},
    }
}


# OpenAPI overrides for /query and /asyncquery.
#
# Those routes take the raw ``Request`` so the body can be parsed with orjson
# (see ``parse_query_body``), which leaves FastAPI with no body parameter to
# infer a schema from -- and so no auto-generated request body or 422. Declaring
# both here, and wiring them in via each route's ``openapi_extra``, keeps
# /openapi.json equivalent to what the old
# ``query: dict = Body(..., examples=[default_input_query])`` signature
# produced, so the TRAPI validators and the Swagger "Try it out" example are
# unaffected by the parser swap.
#
# The 422 body is documented as ``{"detail": str}`` rather than FastAPI's
# ``HTTPValidationError`` (``{"detail": [ValidationError, ...]}``): the rejection
# is ours now, and it carries a single message.
query_openapi_extra: dict = {
    "requestBody": {
        "content": {
            "application/json": {
                "schema": {
                    "additionalProperties": True,
                    "type": "object",
                    "title": "Query",
                    "examples": [default_input_query],
                }
            }
        },
        "required": True,
    },
    "responses": {
        "422": {
            "description": "Validation Error",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "title": "QueryValidationError",
                        "properties": {"detail": {"type": "string"}},
                    }
                }
            },
        }
    },
}


async def parse_query_body(request: Request) -> dict:
    """Read and parse a posted TRAPI query body.

    Deliberately bypasses FastAPI's own body handling, which routes through
    Starlette's ``Request.json()`` -> stdlib ``json.loads``. TRAPI query bodies
    routinely carry a populated knowledge graph, and stdlib parsing of one costs
    ~100ms per 13MB -- all of it blocking the single event loop this server runs
    on, so it delays every other in-flight request too. orjson is ~30% faster on
    the same payload, and skipping FastAPI's ``dict`` body field drops a
    pydantic pass that only ever re-copied the four top-level keys.

    Raises ``QueryBodyError`` for anything that isn't a JSON object. The old
    ``query: dict`` annotation got that check for free from pydantic; without it
    a posted list or string would reach ``query.get(...)`` downstream and 500.
    """
    raw = await request.body()
    try:
        query = orjson.loads(raw)
    except orjson.JSONDecodeError as e:
        raise QueryBodyError("Invalid request body: not valid JSON.") from e
    if not isinstance(query, dict):
        raise QueryBodyError("Invalid request body: expected a JSON object.")
    return query


async def run_query(
    target: str,
    query: dict,
    callback_url: Optional[str] = None,
) -> Tuple[str, str, logging.Logger]:
    """Run a single query."""
    query_id = str(uuid.uuid4())[:8]
    response_id = str(uuid.uuid4())[:8]
    # Set up logger
    # Same resolver the callback handler and the merge use, so an unparseable
    # level from a client falls back to the default instead of failing intake.
    level_number = resolve_log_level(
        query.get("log_level"), resolve_log_level(settings.log_level)
    )
    logger = logging.getLogger(f"shepherd.{query_id}")
    logger.setLevel(level_number)
    attach_query_handler(logger)

    logger.info(f"Sending {query_id} to {target}")

    span_carrier = {}
    # adds otel trace to carrier for next worker
    inject(span_carrier)

    supported_workflow_operations = set(
        [
            "lookup",  # just for O&O ranker comparison work
            "score",  # Ditto
            "aragorn.lookup",
            "aragorn.pathfinder",
            "aragorn.omnicorp",
            "aragorn.score",
            "arax.pathfinder",
            "arax.rank",
            "bte.lookup",
            "sort_results_score",
            "filter_results_top_n",
            "filter_kgraph_orphans",
            "score_paths",
            "filter_analyses_top_n",
        ]
    )
    workflow = None
    if "workflow" in query and query["workflow"] is not None:
        workflow = query["workflow"]
        if not isinstance(workflow, list):
            raise TypeError("Query workflow must be a list.")
        for operation in workflow:
            if operation.get("id") not in supported_workflow_operations:
                raise KeyError(f"Workflow operation {operation} is not supported.")

    # save query to db
    try:

        # ``target`` is either an ARATargetEnum (which subclasses str -- value
        # like "aragorn") or already a plain string for workflow-driven queries.
        target_name = target.value if hasattr(target, "value") else target
        await add_query(
            query_id,
            response_id,
            query,
            callback_url,
            logger,
            target=target_name,
        )
        await add_task(
            target,
            {
                "query_id": query_id,
                "response_id": response_id,
                "workflow": json.dumps(workflow),
                "log_level": level_number,
                "otel": json.dumps(span_carrier),
                "metadata": json.dumps({}),
            },
            logger,
        )
    except Exception as e:
        # Previously this was swallowed and we returned as if the query had been
        # accepted -- so a full/unavailable datastore left the query unsaved and
        # unqueued: the sync path then polled a row that never existed until it
        # timed out (~6 min), and the async path returned a fake 200 Accepted for
        # a job that would never run. Surface it instead so the caller gets a
        # real error describing what happened.
        logger.error(f"Failed to accept query {query_id}: {e}")
        raise QueryIntakeError(
            "Unable to accept query: failed to persist initial query state "
            "(datastore unavailable). Please retry shortly."
        ) from e

    return query_id, response_id, logger


async def run_sync_query(
    target: ARATargetEnum,
    request: Request,
) -> Response:
    """Handle synchronous TRAPI queries."""
    try:
        query_dict = await parse_query_body(request)
    except QueryBodyError as e:
        return ORJSONResponse(content={"detail": str(e)}, status_code=422)
    try:
        query_id, response_id, logger = await run_query(target, query_dict)
    except QueryIntakeError as e:
        return ORJSONResponse(
            content={"status": "ERROR", "description": str(e)},
            status_code=500,
        )
    start = time.time()
    now = start
    timeout = query_dict.get("parameters", {}).get("timeout", 360)
    logger.info(f"Query running with {timeout} second timeout.")
    while now <= start + timeout:
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
                    return ORJSONResponse(
                        content={
                            "status": "ERROR",
                            "description": "Unable to get response",
                        }
                    )
                logs = await get_logs(response_id, logger)
                response["logs"] = logs
                return ORJSONResponse(content=response)
        else:
            # Debug, not warning: this fires every 0.5s while a query is still
            # in flight (the row just isn't COMPLETED yet) and would otherwise
            # flood the logs -- especially if the DB is unreachable.
            logger.debug(f"Failed to get the query state of query id {query_id}")
        await asyncio.sleep(0.5)

    logger.error("Query timed out")
    return ORJSONResponse(content={"status": "TIMEOUT", "description": "Query timeout"})


async def run_async_query(
    target: ARATargetEnum,
    request: Request,
) -> JSONResponse:
    """Handle asynchronous TRAPI queries."""
    try:
        query = await parse_query_body(request)
    except QueryBodyError as e:
        return JSONResponse(content={"detail": str(e)}, status_code=422)
    callback_url = query.get("callback")
    if callback_url is None:
        return JSONResponse(
            content={
                "status": "Failed",
                "description": "callback URL missing",
            },
            status_code=422,
        )
    try:
        query_id, _, _ = await run_query(target, query, callback_url)
    except QueryIntakeError as e:
        return JSONResponse(
            content={"status": "Failed", "description": str(e)},
            status_code=500,
        )
    return JSONResponse(
        content={
            "status": "Accepted",
            "description": f"Query commenced. Will send result to {callback_url}",
            "job_id": query_id,
        },
        status_code=200,
    )


async def _read_body_within_limit(request: Request, max_bytes: int):
    """Read the request body, aborting if it exceeds ``max_bytes``.

    Returns the body bytes, or ``None`` if the limit was exceeded. A
    ``max_bytes`` of 0 disables the limit and reads the whole body.

    The declared ``Content-Length`` is checked first so well-behaved clients are
    rejected without buffering anything; the stream is then read in chunks and
    aborted the moment the running total crosses the limit, so a missing or
    dishonest ``Content-Length`` can't force us to buffer an unbounded body.
    """
    if max_bytes <= 0:
        return await request.body()

    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                return None
        except ValueError:
            pass

    chunks = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


async def _save_callback_error_logs(callback_id: str, logger: logging.Logger) -> None:
    """Persist a callback handler's logs on an error path.

    The rejection paths (oversized body, unparseable body) bail out before the
    body's ``response_id`` is known, so resolve it from the callback->query
    mapping and flush the logger's records under it -- the same key
    ``finish_query`` reads. If the callback can't be mapped to a live query
    there's nothing to key the logs on, so they're dropped.
    """
    original_query = await get_callback_query_id(callback_id, logger)
    if original_query is None:
        return
    query_state = await get_query_state(original_query[0], logger)
    if query_state is None:
        return
    await save_logs(query_state[7], logger)


async def callback(
    target: ARATargetEnum,
    callback_id: str,
    request: Request,
) -> Response:
    """Handle asynchronous callback queries from subservices."""
    # Set up the query logger up front, keyed only on the callback_id we always
    # have, so the rejection / parse-error paths below can persist their logs
    # too. The requested log level lives in the body -- which those paths never
    # parse -- so leave the logger at its inherited default until we have it.
    logger = logging.getLogger(f"shepherd.{callback_id}")
    attach_query_handler(logger)
    max_bytes = settings.callback_max_request_size_bytes
    raw = await _read_body_within_limit(request, max_bytes)
    if raw is None:
        logger.warning(
            f"Rejecting callback {callback_id}: request body exceeds the maximum "
            f"allowed size of {max_bytes} bytes."
        )
        # Persist the rejection log BEFORE removing the callback: resolving the
        # response_id reads the callback->query mapping that remove_callback_id
        # deletes.
        await _save_callback_error_logs(callback_id, logger)
        # Drop this callback from the running set so the lookup worker stops
        # waiting on it. Without this the lookup blocks until its whole-query
        # timeout, since a callback only leaves the set once merge_message has
        # processed it -- which never happens for a payload we refused to read.
        await remove_callback_id(callback_id, logger)
        return JSONResponse(
            content={
                "detail": (
                    f"Request body exceeds the maximum allowed size of {max_bytes} "
                    "bytes."
                )
            },
            status_code=413,
        )
    try:
        if "zstd" in request.headers.get("content-encoding", "").lower():
            raw = decompress_zstd(raw)
        response = orjson.loads(raw)
    except (orjson.JSONDecodeError, zstandard.ZstdError):
        logger.warning(f"Rejecting callback {callback_id}: invalid request body.")
        await _save_callback_error_logs(callback_id, logger)
        return JSONResponse(
            content={"detail": "Invalid request body"},
            status_code=422,
        )
    # get associated query id for this callback. Resolved before anything else
    # is logged: the level to log it at is a property of the query, and this
    # mapping is the only route back to it.
    original_query = await get_callback_query_id(callback_id, logger)
    if original_query is None:
        # No callback->query mapping, so there's no response_id to persist these
        # logs under; surface it in the console/collector at least.
        logger.warning(f"Callback {callback_id}: couldn't find original query.")
        return Response("Couldn't find original query.", 500)
    # Apply the level the client asked for. It lives in the stored query -- a
    # TRAPI response has no log_level field, so the body we were just posted
    # can't tell us (it used to be read from there, which quietly meant INFO for
    # every callback and dropped a DEBUG query's logs from here on).
    level_number = await get_query_log_level(original_query[0], logger)
    logger.setLevel(level_number)
    logger.debug(f"Got original query: {original_query}")
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

    logger.info(
        f"[{callback_id}] Got back {len(response['message']['results'])} results."
    )
    logger.debug(
        f"[{callback_id}] for query graph: {response['message'].get('query_graph')}"
    )
    # if len(response["message"]["results"]) > 0:
    #     with open(
    #         f"shepherd_server/debug/{query_id}_{callback_id}_response.json",
    #         "w",
    #         encoding="utf-8",
    #     ) as f:
    #         json.dump(response, f, indent=2)
    query_state = await get_query_state(original_query[0], logger)
    if query_state is None:
        logger.warning(
            f"Callback {callback_id}: failed to get query state for "
            f"{original_query[0]}."
        )
        return Response("Failed to get query state.", 500)
    response_id = query_state[7]
    # save callback to redis
    logger.debug(f"Saving callback {callback_id} to redis")
    await save_message(callback_id, response, logger)
    logger.debug(f"Saved callback {callback_id} to redis")
    # Record this callback in the per-query ready index *before* enqueuing the
    # wake task, so that whichever merge_message worker picks up the wake signal
    # can drain every arrived callback for this query under one lock. Set
    # membership implies the payload above is already saved.
    await add_ready_callback(response_id, callback_id, logger)
    # adds otel trace to carrier for next worker
    parent_ctx = extract(json.loads(original_query[1]))
    with tracer.start_as_current_span("callback", context=parent_ctx) as span:
        kgraph = response["message"]["knowledge_graph"]
        span.set_attribute("callback.id", callback_id)
        span.set_attribute("callback.results", len(response["message"]["results"]))
        span.set_attribute("callback.kg_nodes", len(kgraph.get("nodes", {})))
        span.set_attribute("callback.kg_edges", len(kgraph.get("edges", {})))
        span.set_attribute("callback.payload_bytes", len(raw))
        span_carrier = {}
        inject(span_carrier)
        # add new task to merge callback response into original message
        await add_task(
            "merge_message",
            {
                "target": target,
                "query_id": original_query[0],
                "response_id": response_id,
                "callback_id": callback_id,
                "log_level": level_number,
                "otel": json.dumps(span_carrier),
                "metadata": json.dumps({}),
            },
            logger,
        )
    # Persist the logs generated while handling this callback so they land in
    # the query's log list (keyed by response_id, the same key finish_query
    # reads). Without this, everything logged above -- the result count, the
    # callback/query lookups -- is dropped on return.
    await save_logs(response_id, logger)
    return Response("Callback received.", 200)


@base_router.get("/asyncquery_status/{qid}", status_code=200)
async def query_status(
    qid: str,
) -> dict:
    """Handle query status requests."""
    # TODO: get query status from db
    return {
        "status": "Queued",
        "description": "Query is currently waiting to be run.",
        "logs": [],
    }


@base_router.get("/response/{query_id}", status_code=200)
async def get_query_response(
    query_id: str,
):
    """Get a query response."""
    level_number = logging.INFO
    logger = logging.getLogger("shepherd.get_query")
    logger.setLevel(level_number)
    attach_query_handler(logger)
    response = await get_message(query_id, logger)
    if response is None:
        return JSONResponse(content={"error": "Not found"}, status_code=404)
    logs = await get_logs(query_id, logger)
    response["logs"] = logs
    return ORJSONResponse(content=response)
