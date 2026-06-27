"""Top-level ARS (Autonomous Relay System) sub-app.

A submitter POSTs a TRAPI query here; Shepherd fans it out to every ARA
(``settings.ars_aras``) using their existing internal workflows, merges the
responses, normalizes/annotates nodes, runs the answer appraiser, then notifies
the submitter. This module exposes the public endpoints; the orchestration runs
in the ``ars`` and ``ars_accumulate`` workers.
"""

import logging

from fastapi import Body, FastAPI, Request, Response
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, ORJSONResponse
from starlette.responses import HTMLResponse

from shepherd_server.base_routes import (
    ARATargetEnum,
    ars_callback,
    base_router,
    default_input_query,
    run_async_query,
    run_sync_query,
)
from shepherd_server.openapi import construct_open_api_schema
from shepherd_utils.db import (
    get_ars_children,
    get_logs,
    get_message,
    get_query_state,
    list_ars_parents,
)
from shepherd_utils.logger import QueryLogger

ARS = FastAPI(title="Shepherd ARS")


def _api_logger() -> logging.Logger:
    """A request-scoped logger wired to the query log handler."""
    logger = logging.getLogger("shepherd.ars.api")
    logger.setLevel(logging.INFO)
    logger.addHandler(QueryLogger().log_handler)
    return logger


@ARS.post("/query")
async def sync_query(
    query: dict = Body(..., examples=[default_input_query]),
) -> Response:
    """Submit a query and block until the merged, appraised result is ready."""
    return await run_sync_query(ARATargetEnum.ARS, query)


@ARS.post("/asyncquery")
async def async_query(
    query: dict = Body(..., examples=[default_input_query]),
) -> Response:
    """Submit a query; the merged result is POSTed to the supplied callback."""
    return await run_async_query(ARATargetEnum.ARS, query)


@ARS.post("/callback/{callback_id}", status_code=200, include_in_schema=False)
async def handle_callback(
    callback_id: str,
    request: Request,
) -> Response:
    """Receive a per-ARA response from a child query's finish_query worker."""
    return await ars_callback(callback_id, request)


@ARS.get("/messages", status_code=200)
async def list_messages():
    """List recent top-level ARS parent queries."""
    logger = _api_logger()
    return ORJSONResponse(content={"messages": await list_ars_parents(logger)})


@ARS.get("/messages/{pk}", status_code=200)
async def get_message_response(pk: str):
    """Return the accumulated/merged response for a parent ARS query."""
    logger = _api_logger()
    query_state = await get_query_state(pk, logger)
    if query_state is None:
        return JSONResponse(content={"error": "Not found"}, status_code=404)
    response_id = query_state[7]
    try:
        response = await get_message(response_id, logger)
    except KeyError:
        return JSONResponse(content={"error": "Response not found"}, status_code=404)
    response["logs"] = await get_logs(response_id, logger)
    return ORJSONResponse(content=response)


@ARS.get("/messages/{pk}/trace", status_code=200)
async def trace_message(pk: str):
    """Return the parent query plus its per-ARA child rows (the message tree)."""
    logger = _api_logger()
    query_state = await get_query_state(pk, logger)
    if query_state is None:
        return JSONResponse(content={"error": "Not found"}, status_code=404)
    children = await get_ars_children(pk, logger)
    return ORJSONResponse(
        content={
            "parent": {
                "qid": query_state[0],
                "state": query_state[9],
                "status": query_state[10],
                "response_id": query_state[7],
            },
            "children": children,
        }
    )


@ARS.get("/status/{pk}", status_code=200)
async def status(pk: str):
    """Compute an overall status (Running/Done/Error) for a parent query."""
    logger = _api_logger()
    query_state = await get_query_state(pk, logger)
    if query_state is None:
        return JSONResponse(content={"error": "Not found"}, status_code=404)
    children = await get_ars_children(pk, logger)
    state = query_state[9]
    if state == "COMPLETED":
        overall = "Done"
    elif state == "ABANDONED":
        overall = "Error"
    elif children and all(c["status"] in ("DONE", "ERROR") for c in children):
        overall = "Merging"
    else:
        overall = "Running"
    return ORJSONResponse(
        content={
            "status": overall,
            "state": state,
            "children": {c["ara"]: c["status"] for c in children},
        }
    )


ARS.include_router(base_router, prefix="")


@ARS.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + ARS.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=ARS.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


ARS.openapi_schema = construct_open_api_schema(
    ARS, infores="infores:shepherd", subpath="/ars"
)
