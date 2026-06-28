"""Top-level ARS (Autonomous Relay System) sub-app.

A submitter POSTs a TRAPI query here; Shepherd fans it out to every ARA
(``settings.ars_aras``) using their existing internal workflows, merges the
responses, normalizes/annotates nodes, runs the answer appraiser, then notifies
the submitter. This module exposes the public endpoints; the orchestration runs
in the ``ars`` and ``ars_accumulate`` workers.
"""

import ast
import json
import logging
import uuid

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
    add_subscriber,
    get_agent,
    get_ars_children,
    get_ars_report,
    get_latest_pks,
    get_logs,
    get_message,
    get_query_state,
    list_actors,
    list_agents,
    list_ars_parents,
    list_channels,
    get_client,
    list_subscribers,
    ping_db,
    ping_redis,
    remove_subscriber,
    save_message,
    set_client_subscriptions,
    set_query_retained,
    upsert_actor,
    upsert_agent,
    upsert_channel,
    upsert_client,
)
from shepherd_utils.ars_clients import (
    EVENT_SIGNATURE_HEADER,
    verify_get_signature,
    verify_post_signature,
)
from shepherd_utils.ars_filter import apply_filters
from shepherd_utils.logger import QueryLogger
from shepherd_utils.shared import filter_kgraph_orphans
from shepherd_utils.smartapi import refresh_actors

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


async def _load_parent_message(pk: str, logger: logging.Logger):
    """Return ``(message, error_response)`` for a parent query's stored response."""
    query_state = await get_query_state(pk, logger)
    if query_state is None:
        return None, JSONResponse(content={"error": "Not found"}, status_code=404)
    try:
        message = await get_message(query_state[7], logger)
    except KeyError:
        return None, JSONResponse(
            content={"error": "Response not found"}, status_code=404
        )
    return message, None


async def _save_derived(message: dict, logger: logging.Logger) -> str:
    """Persist a curated copy under a fresh id and return it."""
    derived_id = str(uuid.uuid4())[:8]
    await save_message(derived_id, message, logger)
    return derived_id


def _remove_nodes(message: dict, nodes: set) -> None:
    """Drop nodes (and edges touching them) from the kgraph in place."""
    kg = message.get("message", {}).get("knowledge_graph") or {}
    kg_nodes = kg.get("nodes") or {}
    kg_edges = kg.get("edges") or {}
    for nid in list(kg_nodes.keys()):
        if nid in nodes:
            del kg_nodes[nid]
    for eid in [
        eid
        for eid, e in kg_edges.items()
        if e.get("subject") in nodes or e.get("object") in nodes
    ]:
        del kg_edges[eid]


@ARS.post("/block/{pk}", status_code=200)
async def block(pk: str, body: dict = Body(...)):
    """Remove the given node curies (and dependent edges) from a stored result.

    Body: ``{"nodes": ["CURIE:1", ...]}``. Returns the id of a derived response.
    """
    logger = _api_logger()
    message, err = await _load_parent_message(pk, logger)
    if err is not None:
        return err
    nodes = set(body.get("nodes") or [])
    _remove_nodes(message, nodes)
    filter_kgraph_orphans(message, logger)
    derived_id = await _save_derived(message, logger)
    return ORJSONResponse(content={"response_id": derived_id})


def _parse_filter_value(raw: str):
    """Parse a filter query-param value as a JSON/Python literal (Relay uses
    ``ast.literal_eval``); fall back to the raw string."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        try:
            return ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            return raw


@ARS.get("/filter/{pk}", status_code=200)
async def filter_results(pk: str, request: Request):
    """Filter a stored result (Relay parity), returning a derived response id.

    Query params (any combination, applied in order): ``hop=<int>`` keeps
    results with fewer than N bound qnodes; ``score=[lo,hi]`` keeps results whose
    normalized_score is strictly between; ``node_type=["Category",...]`` drops
    results binding a node of those categories; ``spec_node=["CURIE",...]`` drops
    results binding those curies.
    """
    logger = _api_logger()
    message, err = await _load_parent_message(pk, logger)
    if err is not None:
        return err
    filters = [
        (key, _parse_filter_value(value))
        for key, value in request.query_params.items()
    ]
    count = apply_filters(message, filters, logger)
    filter_kgraph_orphans(message, logger)
    derived_id = await _save_derived(message, logger)
    return ORJSONResponse(content={"response_id": derived_id, "result_count": count})


@ARS.get("/retain/{pk}", status_code=200)
async def retain(pk: str):
    """Retain a query (and its ARS children) from the purge janitor.

    Relay ``retain`` parity (GET): a child pk retains its parent; retention is
    refused while the query is still running.
    """
    logger = _api_logger()
    return ORJSONResponse(content=await set_query_retained(pk, logger))


@ARS.get("/latest_pk/{n}", status_code=200)
async def latest_pk(n: int):
    """Per-day parent counts over the last n days + latest/running pks."""
    logger = _api_logger()
    return ORJSONResponse(content=await get_latest_pks(n, logger))


@ARS.get("/report/{inforesid}", status_code=200)
async def report(inforesid: str):
    """Per-child performance stats for an ARA over the last 24 hours."""
    logger = _api_logger()
    return ORJSONResponse(content=await get_ars_report(inforesid, logger))


@ARS.get("/health", status_code=200)
async def health():
    """Liveness of the datastore + broker (Relay health parity)."""
    logger = _api_logger()
    db_ok = await ping_db(logger)
    broker_ok = await ping_redis(logger)
    ok = db_ok and broker_ok
    return JSONResponse(
        content={"ok": ok, "db": db_ok, "broker": broker_ok},
        status_code=200 if ok else 503,
    )


@ARS.get("/actors", status_code=200)
async def get_actors():
    """List the registered ARS actors (ARAs)."""
    logger = _api_logger()
    return ORJSONResponse(content={"actors": await list_actors(logger)})


@ARS.post("/actors", status_code=200)
async def register_actor(body: dict = Body(...)):
    """Manually register/update an actor (ARA)."""
    logger = _api_logger()
    if not body.get("infores"):
        return JSONResponse(content={"error": "infores required"}, status_code=422)
    await upsert_actor(
        body["infores"],
        body.get("url", ""),
        body.get("channel", "ARA"),
        body.get("agent_name", body["infores"]),
        body.get("maturity", "production"),
        logger,
        active=body.get("active", True),
    )
    return ORJSONResponse(content={"registered": body["infores"]})


@ARS.post("/actors/discover", status_code=200)
async def discover_actors():
    """Refresh the actor registry from the SmartAPI registry."""
    logger = _api_logger()
    count = await refresh_actors(logger)
    return ORJSONResponse(content={"discovered": count})


@ARS.get("/agents", status_code=200)
async def get_agents():
    """List the registered agents."""
    logger = _api_logger()
    return ORJSONResponse(content={"agents": await list_agents(logger)})


@ARS.post("/agents", status_code=200)
async def register_agent(body: dict = Body(...)):
    """Create or update an agent (ARS agents POST parity)."""
    logger = _api_logger()
    if not body.get("name"):
        return JSONResponse(content={"error": "name required"}, status_code=422)
    await upsert_agent(
        body["name"],
        logger,
        description=body.get("description", ""),
        uri=body.get("uri", ""),
        contact=body.get("contact", ""),
    )
    return ORJSONResponse(content={"registered": body["name"]})


@ARS.get("/agents/{name}", status_code=200)
async def get_single_agent(name: str):
    """Return a single agent by name."""
    logger = _api_logger()
    agent = await get_agent(name, logger)
    if agent is None:
        return JSONResponse(content={"error": "Not found"}, status_code=404)
    return ORJSONResponse(content=agent)


@ARS.get("/channels", status_code=200)
async def get_channels():
    """List the registered channels."""
    logger = _api_logger()
    return ORJSONResponse(content={"channels": await list_channels(logger)})


@ARS.post("/channels", status_code=200)
async def register_channel(body: dict = Body(...)):
    """Create or update a channel (ARS channels POST parity)."""
    logger = _api_logger()
    if not body.get("name"):
        return JSONResponse(content={"error": "name required"}, status_code=422)
    await upsert_channel(body["name"], logger, description=body.get("description", ""))
    return ORJSONResponse(content={"registered": body["name"]})


@ARS.post("/subscribe/{pk}", status_code=200)
async def subscribe(pk: str, body: dict = Body(...)):
    """Register a callback url to receive status updates for a parent query."""
    logger = _api_logger()
    callback_url = body.get("callback_url")
    if not callback_url:
        return JSONResponse(content={"error": "callback_url required"}, status_code=422)
    await add_subscriber(pk, callback_url, logger)
    return ORJSONResponse(content={"subscribed": pk})


@ARS.get("/subscribers/{pk}", status_code=200, include_in_schema=False)
async def get_subscribers(pk: str):
    """List subscriber callback urls for a parent query."""
    logger = _api_logger()
    return ORJSONResponse(content={"subscribers": await list_subscribers(pk, logger)})


@ARS.post("/clients", status_code=200, include_in_schema=False)
async def register_client(body: dict = Body(...)):
    """Register/update a subscriber client (id + AES-encrypted secret + callback)."""
    logger = _api_logger()
    if not body.get("client_id"):
        return JSONResponse(content={"error": "client_id required"}, status_code=422)
    await upsert_client(
        body["client_id"],
        body.get("client_secret", ""),
        body.get("callback_url", ""),
        logger,
    )
    return ORJSONResponse(content={"registered": body["client_id"]})


def _terminal(state: str) -> bool:
    return state in ("COMPLETED", "ABANDONED")


@ARS.post("/query_event_subscribe", status_code=200)
async def query_event_subscribe(request: Request):
    """Signature-verified subscribe: a client subscribes to a set of parent pks.

    Body: ``{"client_id": "...", "pks": ["...", ...]}`` with an
    ``x-event-signature`` HMAC of the raw body under the client secret.
    """
    logger = _api_logger()
    raw = await request.body()
    try:
        body = json.loads(raw)
        pks = body["pks"]
        client_id = body["client_id"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return JSONResponse(content={"message": "Invalid request body"}, status_code=400)

    signature = request.headers.get(EVENT_SIGNATURE_HEADER, "")
    if not await verify_post_signature(signature, raw, client_id, logger):
        return JSONResponse(
            content={"message": "Invalid Signature provided"}, status_code=401
        )

    client = await get_client(client_id, logger)
    callback_url = (client or {}).get("callback_url", "")
    subs = list((client or {}).get("subscriptions") or [])
    result: dict = {"success": [], "failure": {}}
    for pk in pks:
        state = await get_query_state(pk, logger)
        if state is None:
            result["failure"][pk] = "UUID not found"
        elif _terminal(state[9]):
            result["failure"][pk] = "Query already complete"
        else:
            await add_subscriber(pk, callback_url, logger, client_id=client_id)
            if pk not in subs:
                subs.append(pk)
            result["success"].append(pk)
    await set_client_subscriptions(client_id, subs, logger)
    return ORJSONResponse(content=result)


@ARS.get("/query_event_subscribe", status_code=200)
async def query_event_subscribe_list(request: Request, client_id: str):
    """Signature-verified listing of a client's current subscriptions."""
    logger = _api_logger()
    signature = request.headers.get(EVENT_SIGNATURE_HEADER, "")
    verified = await verify_get_signature(
        signature, str(request.url), client_id, logger
    )
    if not verified["verified"]:
        return JSONResponse(
            content={"message": "Invalid Signature provided"}, status_code=401
        )
    return ORJSONResponse(content={"pks": verified["pks"]})


@ARS.post("/query_event_unsubscribe", status_code=200)
async def query_event_unsubscribe(request: Request):
    """Signature-verified unsubscribe from a set of parent pks."""
    logger = _api_logger()
    raw = await request.body()
    try:
        body = json.loads(raw)
        pks = body["pks"]
        client_id = body["client_id"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return JSONResponse(content={"message": "Invalid request body"}, status_code=400)

    signature = request.headers.get(EVENT_SIGNATURE_HEADER, "")
    if not await verify_post_signature(signature, raw, client_id, logger):
        return JSONResponse(
            content={"message": "Invalid Signature provided"}, status_code=401
        )

    client = await get_client(client_id, logger)
    subs = list((client or {}).get("subscriptions") or [])
    result: dict = {"success": [], "failure": {}}
    for pk in pks:
        await remove_subscriber(pk, client_id, logger)
        if pk in subs:
            subs.remove(pk)
        result["success"].append(pk)
    await set_client_subscriptions(client_id, subs, logger)
    return ORJSONResponse(content=result)


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
