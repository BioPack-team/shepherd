"""Shepherd's ARAX API, mounted at /arax.

The ARAX UI stays external (DEC-2), but Shepherd serves what it calls under one
base path (the "UI contract" in docs/ARAX_PORT_BASELINE.md). Queries run in the
arax worker, which runs the ported ARAX library in-process (DEC-14).
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Request, Response
from fastapi.openapi.docs import (
    get_swagger_ui_html,
)
from fastapi.responses import ORJSONResponse, StreamingResponse
from starlette.responses import HTMLResponse

from shepherd_server.base_routes import (
    QUERY_BODY_ERROR_CODE,
    QUERY_ERROR_CODE,
    QUERY_TIMEOUT_CODE,
    QUERY_UNAVAILABLE_CODE,
    TERMINAL_QUERY_STATES,
    ARATargetEnum,
    QueryBodyError,
    QueryIntakeError,
    apply_query_status,
    base_router,
    parse_query_body,
    query_openapi_extra,
    query_status_code,
    query_timeout,
    run_async_query,
    run_query,
    wait_for_query,
)
from shepherd_server.openapi import set_open_api_schema
from shepherd_utils.arax_progress import is_done, read_progress
from shepherd_utils.db import get_logs, get_message, get_query_state

ARAX = FastAPI(title="Shepherd ARAX")

# How often the streaming endpoint looks for new progress lines (ARAX's own
# stream loop sleeps 0.2 s between checks)
STREAM_POLL_SEC = 0.2
# ARAX's stream heartbeat: after this long with nothing to send it emits a
# "still progressing" DEBUG entry. The worker relays ARAX's own; this covers a
# query that isn't ARAX's (a pathfinder query routed to arax.pathfinder).
STREAM_HEARTBEAT_SEC = 180.0


def _heartbeat_line() -> str:
    return (
        json.dumps(
            {
                "timestamp": str(datetime.now().isoformat()),
                "level": "DEBUG",
                "code": "",
                "message": "Query is still progressing...",
            }
        )
        + "\n"
    )


def _error_line(description: str, status: str = "ERROR") -> str:
    return json.dumps({"status": status, "description": description}) + "\n"


def is_arax_envelope(response: dict) -> bool:
    """Whether ARAX produced this response (ARAX's create_envelope stamps it)."""
    tool_version = response.get("tool_version")
    return isinstance(tool_version, str) and tool_version.startswith("ARAX ")


async def arax_final_response(
    query_state: tuple, logger: logging.Logger
) -> tuple[Optional[dict], int]:
    """The finished query's response and the HTTP status to answer with.

    A response ARAX produced carries ARAX's own log and (from the
    non-streaming path) ``http_status``, and is returned as ARAX's ``/query``
    returns it. Anything else (an error response
    the worker wrote because ARAX could not answer, or a pathfinder query's
    response) is finished the way Shepherd's generic ``/query`` does it.
    """
    response_id = query_state[7]
    status = query_state[10]
    response = await get_message(response_id, logger)
    if response is None:
        return None, QUERY_ERROR_CODE
    if is_arax_envelope(response):
        return response, response.get("http_status", 200)
    response["logs"] = await get_logs(response_id, logger)
    apply_query_status(response, status)
    return response, query_status_code(status)


async def arax_sync_query(query: dict) -> Response:
    """ARAX's non-streaming ``/query`` (API-01)."""
    try:
        query_id, _, logger = await run_query(ARATargetEnum.ARAX, query)
    except QueryIntakeError as e:
        return ORJSONResponse(
            content={"status": "ERROR", "description": str(e)},
            status_code=QUERY_UNAVAILABLE_CODE,
        )
    timeout = query_timeout(query)
    logger.info(f"Query running with {timeout} second timeout.")
    query_state = await wait_for_query(query_id, logger, timeout)
    if query_state is None:
        logger.error("Query timed out")
        return ORJSONResponse(
            content={"status": "TIMEOUT", "description": "Query timeout"},
            status_code=QUERY_TIMEOUT_CODE,
        )
    response, status_code = await arax_final_response(query_state, logger)
    if response is None:
        return ORJSONResponse(
            content={"status": "ERROR", "description": "Unable to get response"},
            status_code=QUERY_ERROR_CODE,
        )
    return ORJSONResponse(content=response, status_code=status_code)


async def stream_query_progress(
    query_id: str, response_id: str, logger: logging.Logger, timeout: float
) -> AsyncIterator[str]:
    """ARAX's ``stream_progress`` NDJSON (API-02), relayed from the worker.

    Yields each line ARAX's stream produced (log entries, the pid token,
    ``query_plan`` updates, heartbeats) as the worker relays it, then the saved
    response as the final line, serialized as ARAX does (``sort_keys``,
    ``allow_nan=False``).
    """
    start = time.time()
    last_sent = start
    index = 0
    done = False
    finished = False
    while not done:
        lines = await read_progress(response_id, index)
        for line in lines:
            index += 1
            if is_done(line):
                done = True
                break
            last_sent = time.time()
            yield line
        if done or finished:
            break
        query_state = await get_query_state(query_id, logger)
        if query_state is not None and query_state[9] in TERMINAL_QUERY_STATES:
            # One more read for lines pushed just before it finished
            finished = True
            continue
        now = time.time()
        if now > start + timeout:
            logger.error("Query timed out")
            yield _error_line("Query timeout", status="TIMEOUT")
            return
        if now - last_sent > STREAM_HEARTBEAT_SEC:
            last_sent = now
            yield _heartbeat_line()
        await asyncio.sleep(STREAM_POLL_SEC)

    # The worker saves the response before marking the stream done, but the
    # query itself is only COMPLETED once its workflow wraps up.
    query_state = await wait_for_query(
        query_id, logger, max(timeout - (time.time() - start), 1)
    )
    if query_state is None:
        yield _error_line("Query timeout", status="TIMEOUT")
        return
    response, _ = await arax_final_response(query_state, logger)
    if response is None:
        yield _error_line("Unable to get response")
        return
    yield json.dumps(response, sort_keys=True, allow_nan=False) + "\n"


async def arax_stream_query(query: dict) -> Response:
    try:
        query_id, response_id, logger = await run_query(ARATargetEnum.ARAX, query)
    except QueryIntakeError as e:
        return ORJSONResponse(
            content={"status": "ERROR", "description": str(e)},
            status_code=QUERY_UNAVAILABLE_CODE,
        )
    # ARAX streams with HTTP 200 whatever happens; the outcome is in the body
    return StreamingResponse(
        stream_query_progress(query_id, response_id, logger, query_timeout(query)),
        media_type="text/event-stream",
    )


@ARAX.post("/query", openapi_extra=query_openapi_extra())
async def sync_query(request: Request) -> Response:
    try:
        query = await parse_query_body(request)
    except QueryBodyError as e:
        return ORJSONResponse(
            content={"status": "ERROR", "description": str(e)},
            status_code=QUERY_BODY_ERROR_CODE,
        )
    if query.get("stream_progress", False):
        return await arax_stream_query(query)
    return await arax_sync_query(query)


@ARAX.post("/asyncquery", openapi_extra=query_openapi_extra())
async def async_query(request: Request) -> Response:
    response = await run_async_query(ARATargetEnum.ARAX, request)
    return response


ARAX.include_router(base_router, prefix="")


@ARAX.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + ARAX.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=ARAX.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


set_open_api_schema(ARAX, infores="infores:shepherd-arax", subpath="/arax")
