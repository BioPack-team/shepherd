"""Translator ARS API surface, served by Shepherd.

A port of NCATSTranslator/Relay @ 3e65975 tr_sys/tr_ars/api.py + urls.py onto
FastAPI. Paths, methods, status codes, error bodies, and the Django
serializer envelope are reproduced exactly (parity: tests/unit/ars/
test_ars_api_contract.py and the differential harness).

Upstream routes that only ever failed are not reproduced: POST /messages
answers 405 instead of 500, and /block, /merge, /post_process and
/timeoutTest are not served at all. /filters and /filter/<pk> are dropped
as well -- unused, and the filter path rewrote and re-saved stored messages.
See the divergences section of docs/ARS_PARITY_REGISTER.md.

This ARS is de-federated: it talks only to the ARAs this Shepherd deployment
hosts (shepherd_utils/ars/aras.py), over the broker. So the upstream
registry surface (/agents, /actors, and the POST /messages/<pk> callback an
external ARA delivered its response to) is not served; GET /aras lists the
hosted ARAs and whether their workers are alive instead.

Background work rides Shepherd's Redis Streams instead of Celery: submit
enqueues ``ars.fanout``, which enqueues each ARA's own worker task; when an
ARA pipeline finishes, finish_query hands the response to ``ars.premerge``
(intake + pre-merge processing + TRAPI validation), which enqueues
``ars.merge`` on success.
"""

import asyncio
import hmac
import json

import orjson
import logging
import uuid
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from opentelemetry.propagate import inject

import shepherd_utils.ars.cache as cache
import shepherd_utils.ars.db as ars_db
import shepherd_utils.broker as broker
import shepherd_utils.db as shepherd_db
from shepherd_utils.ars import aras, crypto
from shepherd_utils.ars.envelope import django_datetime, message_envelope
from shepherd_utils.ars.notify import replay_completion
from shepherd_utils.ars.statuses import to_name
from shepherd_utils.config import settings
from shepherd_utils.logger import resolve_log_level
from shepherd_utils.trapi import TRAPIRequestError, query_log_level, validate_query

logger = logging.getLogger("shepherd.ars")

ARS = FastAPI(title="Translator ARS on Shepherd", docs_url=None, redirect_slashes=False)


def text(body: str, status: int = 200) -> Response:
    """Django HttpResponse equivalent (text/html by default)."""
    return Response(
        content=body, status_code=status, media_type="text/html; charset=utf-8"
    )


def dj_json(obj: Any, status: int = 200) -> Response:
    """json.dumps(obj, indent=2) with application/json, like upstream."""
    return Response(
        content=json.dumps(obj, indent=2, default=str),
        status_code=status,
        media_type="application/json",
    )


def route(path: str, methods: List[str]):
    """Register a handler on both slash variants (Django re_path '/?$')."""

    def decorator(fn):
        paths = {path}
        if "{" not in path:
            # Django's re_path('...?/$') optional-slash routes; path()-style
            # parameterized routes match only their exact form upstream.
            paths |= {path.rstrip("/") or path, path.rstrip("/") + "/"}
        for p in paths:
            ARS.add_api_route(p, fn, methods=methods, include_in_schema=False)
        return fn

    return decorator


async def _retry_transient_pg(coro_factory, logger_, what: str, attempts: int = 3):
    """Retry a Postgres operation through transient pool pressure
    (acquisition timeouts, dropped connections). Anything else -- and the
    final failure -- propagates to submit's catch-all, which answers 400
    exactly as upstream's submit does on any internal error."""
    from psycopg import OperationalError
    from psycopg_pool import PoolTimeout

    last_error: Optional[Exception] = None
    for attempt in range(attempts):
        if attempt:
            await asyncio.sleep(0.1 * (2**attempt))
        try:
            return await coro_factory()
        except (OperationalError, PoolTimeout) as e:
            last_error = e
            logger_.warning(f"{what} failed (attempt {attempt}): {e}")
    raise last_error


def _parse_uuid(key: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(str(key))
    except (ValueError, AttributeError, TypeError):
        return None


async def _envelope_bytes(row, task_logger=logger) -> bytes:
    """Render a message envelope with its payload as JSON bytes WITHOUT
    parsing the payload.

    A merged message is tens of MB of JSON; parsing it costs ~4x its size in
    memory and a GIL-holding pass that stalls every other request on this
    process. Instead the stored bytes are decompressed (GIL-free, in a
    worker thread) and spliced into the serialized envelope in place of a
    sentinel. The server stays an I/O pass-through, as the rest of Shepherd
    is designed."""
    raw = await ars_db.load_message_bytes(row["id"], task_logger)
    env = message_envelope(row, data=None)
    env["fields"]["code"] = int(env["fields"]["code"])
    if raw is None:
        return orjson.dumps(env, default=str)
    sentinel = f"__ARS_DATA_{row['id']}__"
    env["fields"]["data"] = sentinel
    head = orjson.dumps(env, default=str)
    # Anchored on the member, not just the value: a bare first-occurrence
    # replace would splice into whichever field happened to serialize first
    # if its text ever matched (``name`` precedes ``data`` in the envelope).
    marker = b'"data":"' + sentinel.encode() + b'"'
    if head.count(marker) != 1:
        # cannot splice safely -- fall back to a parsed payload
        env["fields"]["data"] = await ars_db.load_message_data(row["id"], task_logger)
        return orjson.dumps(env, default=str)
    return head.replace(marker, b'"data":' + raw, 1)


def _host_base(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"


# ---------------------------------------------------------------------------
# index / redirect
# ---------------------------------------------------------------------------

# (suffix or pattern, reversible) in upstream apipatterns order
_API_PATTERNS = [
    ("", True),
    ("submit/", True),
    ("messages/", True),
    ("aras/", True),
    ("messages/<uuid:key>", False),
    ("reports/<inforesid>", False),
    ("retain/<uuid:key>", False),
    ("latest_pk/<int:n>", False),
    ("query_event_subscribe/", True),
    ("query_event_unsubscribe/", True),
    ("health/", True),
    ("get_status/", True),
]


@route("/api", ["GET"])
async def index(request: Request) -> Response:
    base = _host_base(request)
    current = f"{base}/ars/api/"
    data: Dict[str, Any] = {
        "name": "Translator Autonomous Relay System (ARS) API",
        "entries": [],
    }
    for pattern, reversible in _API_PATTERNS:
        if reversible:
            data["entries"].append(f"{base}/ars/api/{pattern}")
        else:
            data["entries"].append(current + pattern)
    return dj_json(data)


@ARS.get("/", include_in_schema=False)
async def api_redirect() -> Response:
    return RedirectResponse(url="/ars/api/", status_code=302)


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


@route("/api/submit", ["GET", "POST", "PUT", "DELETE", "PATCH"])
async def submit(request: Request) -> Response:
    if request.method != "POST":
        return text("Only POST is permitted!", 405)
    try:
        data = json.loads(await request.body())
        if "paths" in data["message"]["query_graph"]:
            params = {"query_type": "pathfinder"}
        else:
            params = {"query_type": "standard"}
        if "validate" in data:
            params["validate"] = data["validate"]
        if "workflow" in data:
            # Upstream routed a workflow query through its workflow actor and,
            # for a ``workflow`` that is not a non-empty list, never assigned
            # ``message`` -> UnboundLocalError -> 400. Every hosted ARA takes
            # workflow queries, so there is no routing left to do; the
            # contract for a malformed workflow field is kept.
            wf = data["workflow"]
            if not isinstance(wf, list) or len(wf) == 0:
                raise UnboundLocalError(
                    "local variable 'message' referenced before assignment"
                )
        # TRAPI 2.0: every hosted ARA speaks 2.0, so a submit must be a 2.0
        # query -- including the 1.x spellings 2.0 retired (top-level
        # log_level / bypass_cache, qualifier_constraints ...), which the
        # schema would otherwise let through and silently ignore. Checked
        # after upstream's own shape checks above so their error contract
        # is unchanged; the 400 body is upstream's submit error shape.
        try:
            validate_query(data)
        except TRAPIRequestError as e:
            logger.info(f"submit rejected: {e}")
            return text("failing due to %s with the message %s" % (None, str(e)), 400)
        # Response cache (Shepherd-native, docs/ARS_RESPONSE_CACHE_PLAN.md):
        # there is one message tree per distinct query. A structurally
        # identical completed query answers right here with the source
        # parent's pk, already Done (SERVED); an identical in-flight one
        # hands back the leader's pk, still Running (WAITING). Either way
        # the envelope's data is this caller's own body, as for a fresh
        # parent, and the client fetches merged_version as usual. Only a
        # miss -- or bypass_cache / overwrite_cache -- creates a parent and
        # fans out.
        served = await cache.lookup(data, logger)
        if served is not None:
            _, row, payload = served
            return dj_json(message_envelope(row, data=payload), 201)
        message = await _retry_transient_pg(
            lambda: ars_db.create_message(
                agent=aras.DEFAULT_AGENT,
                status="Running",
                code=202,
                params=params,
                name=data.get("name", ""),
            ),
            logger,
            "submit message insert",
        )
        # STRICT save + enqueue: our 201 promises a stored query and a
        # queued fanout. A swallowed failure on either would return success
        # for a query that then sits Running forever (parents are
        # watchdog-exempt) -- raise into the catch-all's honest 400 instead.
        await ars_db.save_message_data(
            message["id"], data, logger, raise_on_failure=True
        )
        # Root the query's trace here and remember the carrier: every later
        # stage (fanout now; premerge/merge/notify from the response side)
        # rejoins this trace, giving one end-to-end trace per query even when
        # an ARA doesn't propagate traceparent into its callback.
        carrier: Dict[str, str] = {}
        inject(carrier)
        await ars_db.save_otel_carrier(message["id"], carrier, logger)
        # Claim leadership of the key (or, having lost that race to a
        # concurrent identical submit, get the winner's answer instead --
        # our own row is discarded in that case).
        outcome, message, payload = await cache.claim_or_serve(message, data, logger)
        if outcome == cache.DISPATCH:
            # post_save broadcast -> the ars_fanout worker
            await broker.add_task(
                "ars.fanout",
                {
                    "parent_pk": str(message["id"]),
                    # query_id keys the shared task-context builder + log store
                    "query_id": str(message["id"]),
                    "log_level": resolve_log_level(
                        query_log_level(data), resolve_log_level(settings.log_level)
                    ),
                    "otel": json.dumps(carrier),
                },
                logger,
                raise_on_failure=True,
            )
        return dj_json(message_envelope(message, data=payload), 201)
    except Exception as e:
        logger.error(f"submit failed: {e}", exc_info=True)
        return text(
            "failing due to %s with the message %s" % (e.__cause__, str(e)), 400
        )


# ---------------------------------------------------------------------------
# messages collection
# ---------------------------------------------------------------------------


# How many recent messages GET /api/messages lists, as upstream.
RECENT_MESSAGE_LIMIT = 10


@route("/api/messages", ["GET", "POST"])
async def messages(request: Request) -> Response:
    if request.method == "GET":
        # Identifiers only. Upstream rendered a full envelope per message
        # with its whole stored payload inline, so listing the last ten
        # queries could mean serving hundreds of MB to answer "what has come
        # through recently". Fetch a listed message by pk to get its
        # payload. Timestamps keep the DjangoJSONEncoder spelling the rest
        # of the API uses.
        return dj_json(
            [
                {"pk": str(row["id"]), "timestamp": django_datetime(row["ts"])}
                for row in await ars_db.get_recent_message_pks(RECENT_MESSAGE_LIMIT)
            ]
        )
    # Upstream looked the actor up in the Agent table and then assigned the
    # result to the actor FK, so this has always been a 500. Rather than
    # reproduce that, or invent an unauthenticated message-creation endpoint
    # nothing has ever been able to use, the collection is read-only.
    return text("Only GET is permitted!", 405)


# ---------------------------------------------------------------------------
# message GET (+trace/compress) and the ARA result callback (POST)
# ---------------------------------------------------------------------------


def _trace_actor(agent: Optional[str]) -> Dict[str, Any]:
    """The ``actor`` block of a trace node. Upstream rendered the actor row
    (pk, channels, path); the de-federated ARS has no such row, so this
    names the agent, the infores it stands for, and -- for an ARA's child --
    the Shepherd target it was dispatched to."""
    ara = aras.by_agent(agent)
    return {
        "agent": agent,
        "inforesid": aras.inforesid_for(agent),
        "ara": ara.name if ara is not None else None,
    }


async def _trace_children(parent_pk, task_logger) -> List[Dict[str, Any]]:
    nodes = []
    for child in await ars_db.get_children(parent_pk):
        if child["agent_name"] == aras.MERGE_AGENT:
            continue
        n = {
            "message": str(child["id"]),
            "status": to_name(child["status"]),
            "parent": str(parent_pk),
            "result_count": child.get("result_count"),
            "result_stat": child.get("result_stat"),
            "code": int(child["code"]),
            "actor": _trace_actor(child["agent_name"]),
            "children": await _trace_children(child["id"], task_logger),
        }
        nodes.append(n)
    return nodes


async def trace_message(key: uuid.UUID) -> Response:
    mesg = await ars_db.get_message_row(key)
    if mesg is None:
        return text(f"Unknown message: {key}", 404)
    data = await ars_db.load_message_data(key, logger)
    query_graph = data.get("message", {}).get("query_graph", {}) if data else {}
    n_merged: Dict[str, Any] = {}
    if mesg["code"] == 200:
        merged_pk = mesg.get("merged_version")
        if merged_pk is not None:
            merged_msg = await ars_db.get_message_row(merged_pk)
            if merged_msg is not None:
                n_merged = {
                    "message": str(merged_pk),
                    "status": to_name(merged_msg["status"]),
                    "parent": str(mesg["id"]),
                    "result_count": str(merged_msg.get("result_count")),
                    "result_stat": merged_msg.get("result_stat"),
                    "code": int(merged_msg["code"]),
                    "actor": _trace_actor(merged_msg.get("agent")),
                    "children": [],
                }
    tree = {
        "message": str(mesg["id"]),
        "status": to_name(mesg["status"]),
        "code": mesg["code"],
        "retain": mesg["retain"],
        "timestamp": str(mesg["ts"]),
        "updated_at": str(mesg["updated_at"]),
        "actor": _trace_actor(mesg.get("agent")),
        "result_count": mesg.get("result_count"),
        "merged_version": str(mesg.get("merged_version")),
        "merged_versions_list": str(mesg.get("merged_versions_list")),
        "query_graph": query_graph,
        "children": [],
    }
    tree["ref"] = str(mesg["ref"]) if mesg.get("ref") is not None else None
    if n_merged:
        tree["children"].append(n_merged)
    tree["children"].extend(await _trace_children(mesg["id"], logger))
    return dj_json(tree)


@route("/api/messages/{key}", ["GET", "POST", "PUT", "DELETE", "PATCH"])
async def message(key: str, request: Request) -> Response:
    pk = _parse_uuid(key)
    if pk is None:
        return text(f"Unknown message: {key}", 404)

    if request.method == "GET":
        if request.query_params.get("trace", False):
            return await trace_message(pk)
        mesg = await ars_db.get_message_row(pk)
        if mesg is None:
            return text(f"Unknown message: {key}", 404)
        if request.query_params.get("compress", False):
            blob = await ars_db.load_message_compressed(pk, logger)
            if blob is not None:
                return Response(
                    content=blob,
                    media_type="application/octet-stream",
                    headers={"X-Content-Compression": "zstd"},
                )
            return text(f"Unknown message: {key}", 404)
        # upstream overwrites fields.name with the actor's agent name
        mesg = dict(mesg, name=mesg.get("agent"))
        return Response(
            content=await _envelope_bytes(mesg), media_type="application/json"
        )

    # Upstream's ARAs POSTed their responses here. The de-federated ARS
    # receives them over the broker instead (finish_query -> ars.premerge),
    # so the route is read-only.
    return text("Only GET is permitted!", 405)


# ---------------------------------------------------------------------------
# aras
#
# The upstream registry surface (/agents, /actors, /channels) is not served:
# the ARS talks only to the ARAs this deployment hosts, a static roster, and
# nothing registers at runtime. This lists that roster with each ARA's live
# worker count, so an operator can see what the ARS can currently reach.
# ---------------------------------------------------------------------------


@route("/api/aras", ["GET"])
async def list_aras(request: Request) -> Response:
    enabled = aras.enabled_aras()
    counts = await aras.live_worker_counts(a.name for a in aras.ARAS)
    base = _host_base(request)
    out = []
    for ara in aras.ARAS:
        workers = counts.get(ara.name, 0)
        out.append(
            {
                "name": ara.name,
                "inforesid": ara.inforesid,
                "agent": ara.agent,
                "url": f"{base}/{ara.name}",
                "stream": ara.name,
                "enabled": ara in enabled,
                "live_workers": workers,
                "available": ara in enabled and workers > 0,
            }
        )
    return JSONResponse(content=out)


# ---------------------------------------------------------------------------
# retain
#
# Four upstream routes are deliberately not served here (see the divergences
# section of docs/ARS_PARITY_REGISTER.md):
#
#   GET /api/block/<pk>        ran the blocklist cascade over an arbitrary
#                              stored message and saved the result in place:
#                              a destructive, unauthenticated edit of a
#                              shared tree. Blocklist removal still runs
#                              where it belongs, in ars_merge's
#                              post-process stage.
#   GET /api/merge/<pk>        called a task that does not exist. Before
#                              dying it created a Running merge child under
#                              the parent -- which is never terminal, so the
#                              parent could never complete again.
#   GET /api/post_process/<pk> passed a dict where a Message was expected.
#   /api/timeoutTest           returned None.
#
# The last three never did anything but 500, so nothing can depend on them.
# ---------------------------------------------------------------------------


async def _retain_all(parent_mesg, json_response):
    if parent_mesg["status"] != "R":
        await ars_db.retain_tree(parent_mesg["id"])
        json_response["success"] = True
        json_response["parent_pk"] = str(parent_mesg["id"])
    else:
        json_response["parent_pk"] = str(parent_mesg["id"])
        json_response["description"] = "PK still running"
    return json_response


@route("/api/retain/{key}", ["GET"])
async def retain(key: str) -> Response:
    pk = _parse_uuid(key)
    if pk is None:
        return text(f"Unknown message: {key}", 404)
    mesg = await ars_db.get_message_row(pk)
    if mesg is None:
        return text(f"Unknown message: {key}", 404)
    json_response: Dict[str, Any] = {"success": False}
    if mesg.get("ref") is None:
        # a parent (submitted query): retain its whole tree
        json_response = await _retain_all(mesg, json_response)
    else:
        parent_mesg = await ars_db.get_message_row(mesg["ref"])
        if parent_mesg is None:
            return text(f"Unknown message: {mesg['ref']}", 404)
        json_response = await _retain_all(parent_mesg, json_response)
    return dj_json(json_response)


# ---------------------------------------------------------------------------
# reports / latest_pk / get_status / health
# ---------------------------------------------------------------------------


@route("/api/reports/{inforesid}", ["GET"])
async def get_report(inforesid: str) -> Response:
    report = {}
    for row in await ars_db.get_report_rows(inforesid):
        time_elapsed = row["updated_at"] - row["ts"]
        report[str(row["id"])] = {
            "status_code": row["code"],
            "time_elapsed": str(time_elapsed),
            "result_count": row["result_count"],
            "created_at": str(row["ts"]),
            "updated_at": str(row["updated_at"]),
        }
    return JSONResponse(content=json.loads(json.dumps(report, default=str)))


# latest_pk's path parameter is both a day count and a row limit; it walks a
# dict entry per day, so an unbounded value is a cheap way to make the server
# build an enormous response.
MAX_LATEST_PK_N = 365


@route("/api/latest_pk/{n}", ["GET"])
async def latest_pk(n: int) -> Response:
    import datetime

    if n < 1 or n > MAX_LATEST_PK_N:
        return text(
            f"n must be between 1 and {MAX_LATEST_PK_N}",
            400,
        )
    response: Dict[str, Any] = {}
    response[f"pk_count_last_{n}_days"] = {}
    response[f"latest_{n}_pks"] = []
    response["latest_24hr_running_pks"] = []
    counts = await ars_db.get_parent_message_counts(n)
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=n)
    while start_date <= end_date:
        day = str(start_date.date())
        response[f"pk_count_last_{n}_days"][day] = counts.get(day, 0)
        start_date += datetime.timedelta(days=1)
    response[f"latest_{n}_pks"] = await ars_db.get_latest_parent_pks(n)
    response["latest_24hr_running_pks"] = await ars_db.get_running_parent_pks_24h()
    return JSONResponse(content=response)


@route("/api/get_status", ["GET", "POST"])
async def get_status(request: Request) -> Response:
    if request.method != "POST":
        return text("Only POST is permitted!", 405)
    try:
        body = json.loads(await request.body())
        pks = body["pks"]
        result_map = await ars_db.get_status_rows(pks)
        response = []
        for pk in pks:
            key = str(pk)
            if key in result_map:
                row = result_map[key]
                params = row.get("params") or {}
                response.append(
                    {
                        "pk": key,
                        "status": to_name(row["status"]),
                        "merged_list": row.get("merged_versions_list"),
                        "stats": params["stats"] if "stats" in params else None,
                    }
                )
            else:
                response.append(
                    {"pk": key, "status": None, "merged_list": None, "stats": None}
                )
        return JSONResponse(content=response)
    except Exception as e:
        logger.error(f"get_status failed: {e}")
        import datetime

        return JSONResponse(
            content={
                "message": str(e),
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
            status_code=405,
        )


# ---------------------------------------------------------------------------
# response cache admin (Shepherd-native; not part of the upstream surface)
# ---------------------------------------------------------------------------


def _admin_authorized(request: Request) -> bool:
    """Bearer-token gate for the cache admin routes. With no token
    configured the routes are disabled outright."""
    token = settings.ars_admin_token
    if not token:
        return False
    header = request.headers.get("authorization", "")
    scheme, _, presented = header.partition(" ")
    return scheme.lower() == "bearer" and hmac.compare_digest(presented.strip(), token)


@route("/api/cache", ["GET"])
async def cache_stats(request: Request) -> Response:
    if not _admin_authorized(request):
        return text("Forbidden", 403)
    return JSONResponse(
        content=json.loads(json.dumps(await cache.stats(), default=str))
    )


@route("/api/cache/invalidate", ["POST"])
async def cache_invalidate(request: Request) -> Response:
    """Bump the cache generation: every cached response becomes a miss."""
    if not _admin_authorized(request):
        return text("Forbidden", 403)
    reason = None
    body = await request.body()
    if body:
        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict) and parsed.get("reason") is not None:
                reason = str(parsed["reason"])
        except json.JSONDecodeError:
            return text("Body must be JSON", 400)
    generation = await cache.invalidate_all(reason)
    logger.info(f"ARS response cache invalidated (generation {generation}): {reason}")
    return JSONResponse(content={"generation": generation, "reason": reason})


async def _database_available() -> bool:
    try:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False


async def _broker_available() -> bool:
    try:
        return bool(await broker.broker_client.ping())
    except Exception:
        return False


@route("/api/health", ["GET", "POST"])
async def health(request: Request) -> Response:
    if request.method != "GET":
        return text("Only GET is permitted!", 405)
    health_body: Dict[str, Any] = {"status": "ok"}
    code = 200
    if await _database_available():
        health_body["database"] = "available"
    else:
        health_body["status"] = "error"
        health_body["database"] = "unavailable"
        code = 500
    # the upstream key name is kept so dashboards don't break; it now
    # reports the task broker's liveness
    if await _broker_available():
        health_body["celery"] = "available"
    else:
        health_body["status"] = "error"
        health_body["celery"] = "unavailable"
        code = 500
    return JSONResponse(content=health_body, status_code=code)


# ---------------------------------------------------------------------------
# subscriptions
# ---------------------------------------------------------------------------


def _analyze_response(response: Dict[str, Any]):
    if "message" in response:
        status = 401
    elif not response["success"]:
        del response["success"]
        status = 400
    elif not response["failure"]:
        del response["failure"]
        status = 200
    else:
        status = 207
    return response, status


async def _verify_signature(request: Request, body: bytes):
    """Port of api.verify_signature for POST (body HMAC) and GET (URL HMAC).

    Returns either a Response (error) or a dict with verified/pks/client_id.
    """
    import datetime

    response: Dict[str, Any] = {}
    event_signature = request.headers.get("x-event-signature")
    if event_signature is None:
        return JSONResponse(
            content={
                "message": "Signature not provided",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
            status_code=400,
        )
    if request.method == "POST":
        try:
            parsed = json.loads(body)
            pks = parsed["pks"]
            client_id = parsed["client_id"]
        except json.decoder.JSONDecodeError:
            return JSONResponse(
                content={
                    "message": "Invalid JSON format",
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                },
                status_code=400,
            )
        client = await ars_db.get_client(client_id)
        if client is None:
            return JSONResponse(
                content={
                    "message": "No such client",
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                },
                status_code=400,
            )
        secret = crypto.decrypt_secret(client["client_secret"], crypto.master_key())
        response["verified"] = crypto.verify_body_signature(
            body, secret, event_signature
        )
        response["pks"] = pks
        response["client_id"] = client_id
        response["_client"] = client
        return response
    # GET
    client_id = request.query_params.get("client_id")
    if client_id:
        client = await ars_db.get_client(client_id)
        if client is None:
            return JSONResponse(
                content={
                    "message": "No such client",
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                },
                status_code=400,
            )
        secret = crypto.decrypt_secret(client["client_secret"], crypto.master_key())
        response["verified"] = crypto.verify_url_signature(
            str(request.url), secret, event_signature
        )
        response["pks"] = client.get("subscriptions")
        response["client_id"] = client_id
        response["_client"] = client
    return response


@route("/api/query_event_subscribe", ["GET", "POST"])
async def query_event_subscribe(request: Request) -> Response:
    import datetime

    body = await request.body()
    response = await _verify_signature(request, body)
    if isinstance(response, Response):
        return response
    if request.method == "POST":
        if isinstance(response, dict) and "verified" in response:
            valid = response["verified"]
            pks = response.get("pks")
            client = response.get("_client")
            out: Dict[str, Any] = {}
            if not valid:
                out["message"] = "Invalid Signature provided"
                out["timestamp"] = datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat()
                out, status = _analyze_response(out)
                return Response(
                    content=json.dumps(out),
                    status_code=status,
                    media_type="text/html; charset=utf-8",
                )
            out["success"] = []
            out["failure"] = {}
            for key in pks:
                mesg = await ars_db.get_message_row(key) if _parse_uuid(key) else None
                if mesg is None:
                    out["failure"][key] = "UUID not found"
                    continue
                if mesg["status"] in ("D", "E"):
                    if settings.ars_cache_enabled:
                        # A response-cache hit hands back a pk that is
                        # already Done, so the client's subscription lands
                        # after every completion event fired. Replay them
                        # to this client instead of upstream's refusal
                        # (documented deviation, parity register 13).
                        await replay_completion(mesg, client["id"], logger)
                        out["success"].append(key)
                    else:
                        out["failure"][key] = "Query already complete"
                else:
                    await ars_db.add_subscription(mesg["id"], client["id"])
                    out["success"].append(key)
            out["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            out, status = _analyze_response(out)
            return Response(
                content=json.dumps(out),
                status_code=status,
                media_type="text/html; charset=utf-8",
            )
        return text("Method POST not supported!", 400)
    if request.method == "GET":
        if isinstance(response, dict) and "verified" in response:
            if response["verified"]:
                out = {
                    "pks": response["pks"],
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                }
                return Response(
                    content=json.dumps(out),
                    status_code=200,
                    media_type="text/html; charset=utf-8",
                )
            out = {
                "message": "Invalid Signature provided",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            return Response(
                content=json.dumps(out),
                status_code=401,
                media_type="text/html; charset=utf-8",
            )
    return text(f"Method {request.method} not supported!", 400)


@route("/api/query_event_unsubscribe", ["GET", "POST"])
async def query_event_unsubscribe(request: Request) -> Response:
    import datetime

    if request.method != "POST":
        return text("Only POST is permitted!", 405)
    body = await request.body()
    response = await _verify_signature(request, body)
    if isinstance(response, Response):
        return response
    if isinstance(response, dict) and "verified" in response:
        valid = response["verified"]
        pks = response.get("pks")
        client = response.get("_client")
        out: Dict[str, Any] = {}
        if valid:
            out["success"] = []
            out["failure"] = {}
            for pk in pks:
                mesg = await ars_db.get_message_row(pk) if _parse_uuid(pk) else None
                if mesg is None:
                    out["failure"][pk] = "UUID not found"
                    continue
                subscribed = client["id"] in (mesg.get("clients") or [])
                if subscribed and mesg["status"] not in ("D", "E"):
                    await ars_db.remove_subscription(mesg["id"], client["id"])
                    out["success"].append(pk)
                elif not subscribed:
                    out["success"].append(pk)
                elif mesg["status"] in ("D", "E"):
                    out["failure"][pk] = "Failure in auto-subscription upon completion"
        else:
            out["message"] = "Invalid Signature provided"
        out["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        out, status = _analyze_response(out)
        return Response(
            content=json.dumps(out),
            status_code=status,
            media_type="text/html; charset=utf-8",
        )
    return text("Only POST is permitted!", 405)
