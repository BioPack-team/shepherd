"""Translator ARS API surface, served by Shepherd.

A port of NCATSTranslator/Relay @ 3e65975 tr_sys/tr_ars/api.py + urls.py onto
FastAPI. Paths, methods, status codes, error bodies, and the Django
serializer envelope are reproduced exactly (parity: tests/unit/ars/
test_ars_api_contract.py and the differential harness). Known-broken
upstream endpoints (POST /messages, POST /actors, GET /merge/<pk>,
timeoutTest) reproduce their observable failures, including side effects
that happen before the upstream crash.

Background work rides Shepherd's Redis Streams instead of Celery: submit
enqueues ``ars.fanout``; a validated result callback enqueues ``ars.merge``.
Pre-merge processing + TRAPI validation run inline in the callback request
(as upstream does in its Django view) via a worker thread.
"""

import ast
import asyncio
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
import shepherd_utils.broker as broker
import shepherd_utils.db as shepherd_db
from shepherd_utils.ars import crypto
from shepherd_utils.ars.blocklist import load_blocklist, remove_blocked
from shepherd_utils.ars.envelope import (
    agent_envelope,
    channel_envelope,
    message_envelope,
)
from shepherd_utils.ars.filters import (
    hop_level_filter,
    node_type_filter,
    score_filter,
    specific_node_filter,
)
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.ars.premerge import (
    ScoreStatCalc,
    get_safe,
    pre_merge_process,
    remove_phantom_support_graphs,
)
from shepherd_utils.ars.statuses import to_name
from shepherd_utils.ars.trapi import validate
from shepherd_utils.config import settings
from shepherd_utils.logger import resolve_log_level

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


def _parse_uuid(key: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(str(key))
    except (ValueError, AttributeError, TypeError):
        return None


async def _envelope_with_data(row, task_logger=logger):
    data = await ars_db.load_message_data(row["id"], task_logger)
    return message_envelope(row, data=data)


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
    ("agents/", True),
    ("actors/", True),
    ("channels/", True),
    ("agents/<name>", False),
    ("messages/<uuid:key>", False),
    ("filters/", True),
    ("filter/<uuid:key>", False),
    ("reports/<inforesid>", False),
    ("timeoutTest/", True),
    ("merge/<uuid:key>", False),
    ("retain/<uuid:key>", False),
    ("block/<uuid:key>", False),
    ("latest_pk/<int:n>", False),
    ("query_event_subscribe/", True),
    ("query_event_unsubscribe/", True),
    ("post_process/<uuid:key>", False),
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
        message = None
        if "workflow" in data:
            wf = data["workflow"]
            if isinstance(wf, list):
                if len(wf) > 0:
                    actor = await lifecycle.ensure_workflow_actor()
                    message = await ars_db.create_message(
                        actor_id=actor["id"],
                        status="Running",
                        code=202,
                        params=params,
                        name=data.get("name", ""),
                    )
        else:
            actor = await lifecycle.ensure_default_actor()
            message = await ars_db.create_message(
                actor_id=actor["id"],
                status="Running",
                code=202,
                params=params,
                name=data.get("name", ""),
            )
        if message is None:
            # upstream: `message` was never assigned -> UnboundLocalError
            raise UnboundLocalError(
                "local variable 'message' referenced before assignment"
            )
        await ars_db.save_message_data(message["id"], data, logger)
        # post_save broadcast -> the ars_fanout worker
        await broker.add_task(
            "ars.fanout",
            {
                "parent_pk": str(message["id"]),
                # query_id keys the shared task-context builder + log store
                "query_id": str(message["id"]),
                "log_level": resolve_log_level(settings.log_level),
                "otel": "{}",
            },
            logger,
        )
        return dj_json(message_envelope(message, data=data), 201)
    except Exception as e:
        logger.error(f"submit failed: {e}", exc_info=True)
        return text(
            "failing due to %s with the message %s" % (e.__cause__, str(e)), 400
        )


# ---------------------------------------------------------------------------
# messages collection
# ---------------------------------------------------------------------------


@route("/api/messages", ["GET", "POST"])
async def messages(request: Request) -> Response:
    if request.method == "GET":
        response = []
        for row in await ars_db.get_recent_messages(10):
            response.append(await _envelope_with_data(row))
        return Response(
            content=json.dumps(response, default=str),
            media_type="application/json",
        )
    # POST /messages is broken upstream (looks the actor up in the Agent
    # table, then assigns it to the actor FK) -- observable result: 500.
    return text("Internal server error", 500)


# ---------------------------------------------------------------------------
# message GET (+trace/compress) and the ARA result callback (POST)
# ---------------------------------------------------------------------------


async def _trace_children(parent_pk, task_logger) -> List[Dict[str, Any]]:
    nodes = []
    for child in await ars_db.get_children(parent_pk):
        if child.get("inforesid") == "infores:ars":
            continue
        channel_names = []
        for ch in child.get("actor_channel") or []:
            channel_names.append(ch["fields"]["name"])
        n = {
            "message": str(child["id"]),
            "status": to_name(child["status"]),
            "parent": str(parent_pk),
            "result_count": child.get("result_count"),
            "result_stat": child.get("result_stat"),
            "code": int(child["code"]),
            "actor": {
                "pk": child["actor_id"],
                "inforesid": child.get("inforesid"),
                "channel": channel_names,
                "agent": child.get("agent_name"),
                "path": child.get("actor_path"),
            },
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
    actor = await ars_db.get_actor(mesg["actor"]) or {}
    channel_names = []
    for ch in actor.get("channel") or []:
        channel_names.append(ch["fields"]["name"])
    n_merged: Dict[str, Any] = {}
    if mesg["code"] == 200:
        merged_pk = mesg.get("merged_version")
        if merged_pk is not None:
            merged_msg = await ars_db.get_message_row(merged_pk)
            if merged_msg is not None:
                merged_actor = await ars_db.get_actor(merged_msg["actor"]) or {}
                n_merged = {
                    "message": str(merged_pk),
                    "status": to_name(merged_msg["status"]),
                    "parent": str(mesg["id"]),
                    "result_count": str(merged_msg.get("result_count")),
                    "result_stat": merged_msg.get("result_stat"),
                    "code": int(merged_msg["code"]),
                    "actor": {
                        "pk": merged_msg["actor"],
                        "inforesid": merged_actor.get("inforesid"),
                        "agent": merged_actor.get("agent_name"),
                    },
                    "children": [],
                }
    tree = {
        "message": str(mesg["id"]),
        "status": to_name(mesg["status"]),
        "code": mesg["code"],
        "retain": mesg["retain"],
        "timestamp": str(mesg["ts"]),
        "updated_at": str(mesg["updated_at"]),
        "actor": {
            "pk": mesg["actor"],
            "inforesid": actor.get("inforesid"),
            "channel": channel_names,
            "agent": actor.get("agent_name"),
            "path": actor.get("path"),
        },
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
            blob = await shepherd_db.data_db_client.get(str(pk))
            if blob is not None:
                return Response(
                    content=blob,
                    media_type="application/octet-stream",
                    headers={"X-Content-Compression": "zstd"},
                )
            return text(f"Unknown message: {key}", 404)
        actor = await ars_db.get_actor(mesg["actor"]) or {}
        mesg = dict(mesg, name=actor.get("agent_name"))
        env = await _envelope_with_data(mesg)
        env["fields"]["code"] = int(env["fields"]["code"])
        return JSONResponse(content=json.loads(json.dumps(env, default=str)))

    if request.method == "POST":
        return await _result_callback(pk, request)

    return text(f"Method {request.method} not supported!", 400)


async def _result_callback(key: uuid.UUID, request: Request) -> Response:
    """POST /ars/api/messages/<child_pk>: an ARA delivering its response."""
    body = await request.body()
    mesg = await ars_db.get_message_row(key)
    if mesg is None:
        return text(f"Unknown state reference {key}", 404)
    try:
        data = json.loads(body)
    except json.decoder.JSONDecodeError:
        return text(
            "Can not decode json:<br>\n%s for the pk: %s"
            % (body.decode(errors="replace"), key),
            500,
        )
    try:
        status = "D"
        code = 200
        if "tr_ars.message.status" in request.headers:
            status = request.headers["tr_ars.message.status"]
        res = get_safe(data, "message", "results")
        actor = await ars_db.get_actor(mesg["actor"]) or {}
        inforesid = actor.get("inforesid")
        agent_name = str(actor.get("agent_name"))
        parent = await ars_db.get_message_row(mesg["ref"]) if mesg.get("ref") else None
        if parent is None:
            return text(f"Unknown state reference {key}", 404)
        result_length = len(res) if res is not None else None
        await notify_subscribers(
            parent,
            {
                "event_type": "ara_response_complete",
                "ara_name": inforesid,
                "child_uuid": str(mesg["id"]),
                "ara_response_status": status,
                "ara_n_results": result_length,
            },
            logger,
        )
        logger.info(
            f"received msg from agent: {inforesid} with parent pk: "
            f"{mesg['ref']} and result: {result_length}"
        )
        if mesg["status"] == "D":
            return text(
                "ARS has already received %s results from pk: %s" % (result_length, key)
            )
        if mesg.get("result_count") is not None and mesg["result_count"] > 0:
            return text(
                "ARS already has a response with: %s results for pk %s \nWe are "
                "temporarily disallowing subsequent updates to PKs which already "
                "have results\n" % (result_length, key),
                409,
            )
        if mesg["status"] == "E":
            return text(
                "Response received but Message is already in state "
                + str(mesg["code"])
                + ". Response rejected\n",
                400,
            )
        result_count = None
        result_stat = None
        if res is not None and result_length > 0:
            result_count = result_length
            result_stat = ScoreStatCalc(res)
            message_to_merge = data
            # pre-merge processing + validation run inline (upstream does
            # this in the Django request); offloaded to a thread so the CPU
            # work doesn't stall the event loop.
            params = mesg.get("params") or {}
            await asyncio.to_thread(
                pre_merge_process, message_to_merge, str(key), agent_name, inforesid
            )
            if "validate" in params.keys() and not params["validate"]:
                valid = True
            else:
                await asyncio.to_thread(remove_phantom_support_graphs, message_to_merge)
                valid = await asyncio.to_thread(validate, message_to_merge)
            if valid:
                if agent_name.startswith("ara-"):
                    await ars_db.save_message_data(key, message_to_merge, logger)
                    await broker.add_task(
                        "ars.merge",
                        {
                            "parent_pk": str(mesg["ref"]),
                            "child_pk": str(key),
                            "agent_name": agent_name,
                            "query_id": str(mesg["ref"]),
                            "otel": "{}",
                        },
                        logger,
                    )
            else:
                logger.debug(
                    f"Validation problem found for agent {agent_name} with pk "
                    f"{mesg['ref']}"
                )
                await ars_db.save_message_data(key, data, logger)
                updated = await ars_db.update_message(
                    key,
                    status="E",
                    code=422,
                    result_count=result_count,
                    result_stat=result_stat,
                )
                await ars_db.persist_data_copy(key, logger)
                await notify_subscribers(
                    parent,
                    {
                        "event_type": "ara_failed_validation",
                        "ara_name": inforesid,
                        "child_uuid": str(mesg["id"]),
                        "ara_response_status": "E",
                        "ara_n_results": result_length,
                    },
                    logger,
                )
                await lifecycle.check_parent_completion(mesg["ref"], logger)
                return text("Problem with TRAPI Validation", 422)

        if result_count is None:
            # save the raw payload for children without results
            await ars_db.save_message_data(key, data, logger)
        updates: Dict[str, Any] = {"status": status, "code": code}
        if result_count is not None:
            updates["result_count"] = result_count
            updates["result_stat"] = result_stat
        elif res is None:
            # design choice upstream (06-09-2026): None results means 0
            updates["result_count"] = 0
        updated = await ars_db.update_message(key, **updates)
        await ars_db.persist_data_copy(key, logger)
        if updated and updated["status"] in ("D", "S", "E", "U"):
            await lifecycle.check_parent_completion(mesg["ref"], logger)
        env = message_envelope(
            updated or mesg, data=await ars_db.load_message_data(key, logger)
        )
        return dj_json(env, 201)
    except Exception as e:
        logger.error(f"callback failed for {key}: {e}", exc_info=True)
        try:
            log_entry = {
                "message": "Internal ARS Server Error",
                "timestamp": str(mesg.get("updated_at")),
                "level": "ERROR",
            }
            if "logs" in data.keys():
                data["logs"].append(log_entry)
            else:
                data["logs"] = [log_entry]
            await ars_db.save_message_data(key, data, logger)
            await ars_db.update_message(key, status="E", code=500)
            await ars_db.persist_data_copy(key, logger)
            await lifecycle.check_parent_completion(mesg["ref"], logger)
        except Exception:
            pass
        return text("Internal server error", 500)


# ---------------------------------------------------------------------------
# agents / actors / channels
# ---------------------------------------------------------------------------


@route("/api/agents", ["GET", "POST"])
async def agents(request: Request) -> Response:
    if request.method == "GET":
        return Response(
            content=json.dumps(
                [agent_envelope(a) for a in await ars_db.list_agents()],
                default=str,
            ),
            media_type="application/json",
        )
    try:
        data = json.loads(await request.body())
        if "model" in data and "tr_ars.agent" == data["model"]:
            data = data["fields"]
        if "name" not in data or "uri" not in data:
            return text('JSON does not contain "name" and "uri" fields', 400)
        agent, status = await ars_db.get_or_create_agent(data)
        return dj_json(agent_envelope(agent), status)
    except Exception as e:
        logger.error(f"agents POST failed: {e}")
        return text("Not a valid json format", 400)


@route("/api/agents/{name}", ["GET"])
async def get_agent(name: str) -> Response:
    agent = await ars_db.get_agent_by_name(name)
    if agent is None:
        return text(f"Unknown agent: {name}", 400)
    return dj_json(agent_envelope(agent))


@route("/api/actors", ["GET", "POST"])
async def actors(request: Request) -> Response:
    if request.method == "GET":
        from shepherd_utils.smartapi import url_remote_from_inforesid

        out = []
        for a in await ars_db.list_actors(exclude_empty_path=True):
            actor = {"model": "tr_ars.actor", "pk": a["id"], "fields": {}}
            actor["fields"]["name"] = a["agent_name"] + "-" + a["path"]
            actor["fields"]["channel"] = [
                ch["fields"]["name"] for ch in (a.get("channel") or [])
            ]
            actor["fields"]["agent"] = a["agent_name"]
            actor["fields"]["urlRemote"] = url_remote_from_inforesid(a.get("inforesid"))
            actor["fields"][
                "path"
            ] = f"{_host_base(request)}{a.get('agent_uri', '')}{a['path']}"
            actor["fields"]["active"] = a["active"]
            actor["fields"]["inforesid"] = a["inforesid"]
            out.append(actor)
        return dj_json(out)
    try:
        data = json.loads(await request.body())
        if "model" in data and "tr_ars.agent" == data["model"]:
            data = data["fields"]
        actor, status = await ars_db.get_or_create_actor(data)
        # Upstream then evaluates actor.channel.name (channel is a list) and
        # the AttributeError lands in the generic handler -> 400. The actor
        # creation side effect above is real, as it is upstream.
        raise AttributeError("'list' object has no attribute 'name'")
    except KeyError as e:
        return text(f"Unknown {str(e)}", 404)
    except Exception as e:
        logger.error(f"actors POST failed: {e}")
        return text("Not a valid json format", 400)


@route("/api/channels", ["GET", "POST"])
async def channels(request: Request) -> Response:
    if request.method == "GET":
        return Response(
            content=json.dumps(
                [channel_envelope(c) for c in await ars_db.list_channels()],
                default=str,
            ),
            media_type="application/json",
        )
    try:
        data = json.loads(await request.body())
        if "model" in data and "tr_ars.channel" == data["model"]:
            data = data["fields"]
        if "name" not in data:
            return text('JSON does not contain "name" field', 400)
        channel, created = await ars_db.get_or_create_channel(
            data["name"], data.get("description")
        )
        status = 201
        if not created:
            status = 302
        return dj_json(channel_envelope(channel), status)
    except Exception as e:
        logger.error(f"channels POST failed: {e}")
        return text("Internal server error", 500)


# ---------------------------------------------------------------------------
# filters
# ---------------------------------------------------------------------------


@route("/api/filters", ["GET"])
async def filters_doc() -> Response:
    filters = {
        "hop_level": {
            "default": int(3),
            "description": "Returns a new message pk with results that contain N nodes or less. Takes one Int parameter, the number of nodes desired",
            "example_url": "https://ars-prod.transltr.io/ars/api/filter/{pk}?hop=3",
        },
        "score_level": {
            "default": [20, 80],
            "description": "Returns a new message pk with results that have normalized scores between a desired range. Takes a list of min and max values to filter on",
            "example_url": "https://ars-prod.transltr.io/ars/api/filter/{pk}?score=[20,80]",
        },
        "node_type": {
            "default": ["ChemicalEntity", "BiologicalEntity"],
            "description": "Returns a new message pk with results that dont hold the given node category. Takes a list of node categories to be eliminated",
            "example_url": 'https://ars-prod.transltr.io/ars/api/filter/{pk}?node_type=["ChemicalEntity","BiologicalEntity"]',
        },
        "spec_node": {
            "default": ["NCBIGene:2064", "MONDO:0005147"],
            "description": "Returns a new message pk with results that dont hold the given node Curie. Takes a list of node Curies to be eliminated",
            "example_url": 'https://ars-prod.transltr.io/ars/api/filter/{pk}?spec_node=["NCBIGene:2064","MONDO:0005147"]',
        },
        "multi-filtering": {
            "example_url": 'https://ars-prod.transltr.io/ars/api/filter/{pk}?hop=3&score=[20,80]&node_type=["ChemicalEntity","BiologicalEntity"]&spec_node=["NCBIGene:2064","MONDO:0005147"]'
        },
    }
    return dj_json(filters)


def _filter_message_deepfirst(rdata, filter_name, arg):
    results = rdata["message"]["results"]
    kg_nodes = rdata["message"]["knowledge_graph"]["nodes"]
    if filter_name == "hop":
        filter_response = hop_level_filter(results, arg)
    elif filter_name == "score":
        filter_response = score_filter(results, arg)
    elif filter_name == "node_type":
        filter_response = node_type_filter(kg_nodes, results, arg)
    elif filter_name == "spec_node":
        filter_response = specific_node_filter(results, arg)
    else:
        raise KeyError(filter_name)
    rdata["message"]["results"] = filter_response
    return rdata, len(filter_response)


@route("/api/filter/{key}", ["GET", "POST"])
async def filter_endpoint(key: str, request: Request) -> Response:
    if request.method != "GET":
        return text("Only GET & POST are permitted!", 405)
    pk = _parse_uuid(key)
    if pk is None:
        return text(f"Unknown message: {key}", 404)
    filter_arg_list = []
    for filter_type in request.query_params.keys():
        value = request.query_params.getlist(filter_type)[0]
        filter_value = ast.literal_eval(value)
        filter_arg_list.append([filter_type, filter_value])

    mesg = await ars_db.get_message_row(pk)
    if mesg is None:
        return text(f"Unknown message: {key}", 404)
    actor = await ars_db.get_actor(mesg["actor"]) or {}
    if str(actor.get("agent_name")) == "ars-default-agent":
        default_actor = await lifecycle.ensure_default_actor()
        new_mesg = await ars_db.create_message(
            actor_id=default_actor["id"], status="Done", code=200
        )
        parent_data = await ars_db.load_message_data(pk, logger)
        await ars_db.save_message_data(new_mesg["id"], parent_data, logger)
        for child in await ars_db.get_children(pk):
            if (
                child["status"] == "D"
                and child.get("result_count") != 0
                and child.get("result_count") is not None
            ):
                rdata = await ars_db.load_message_data(child["id"], logger)
                final_result_count = 0
                for fil in filter_arg_list:
                    rdata, final_result_count = _filter_message_deepfirst(
                        rdata, fil[0], fil[1]
                    )
                child_mesg = await ars_db.create_message(
                    actor_id=child["actor_id"],
                    ref=new_mesg["id"],
                    status="Done",
                    code=200,
                )
                await ars_db.update_message(
                    child_mesg["id"], result_count=final_result_count
                )
                await ars_db.save_message_data(child_mesg["id"], rdata, logger)
        return RedirectResponse(
            url="/ars/api/messages/" + str(new_mesg["id"]) + "?trace=y",
            status_code=302,
        )
    else:
        if mesg["status"] == "D" and mesg.get("result_count") != 0:
            rdata = await ars_db.load_message_data(pk, logger)
            final_result_count = 0
            for fil in filter_arg_list:
                rdata, final_result_count = _filter_message_deepfirst(
                    rdata, fil[0], fil[1]
                )
            child_mesg = await ars_db.create_message(
                actor_id=mesg["actor"], status="Done", code=200
            )
            await ars_db.update_message(
                child_mesg["id"], result_count=final_result_count
            )
            await ars_db.save_message_data(child_mesg["id"], rdata, logger)
            new_id = child_mesg["id"]
        else:
            return text('message doesnt have results or marked as "Done"', 400)
        return RedirectResponse(
            url="/ars/api/messages/" + str(new_id) + "?trace=y", status_code=302
        )


# ---------------------------------------------------------------------------
# retain / block / merge / post_process / timeoutTest
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
    actor = await ars_db.get_actor(mesg["actor"]) or {}
    if str(actor.get("agent_name")) == "ars-default-agent":
        json_response = await _retain_all(mesg, json_response)
    elif mesg.get("ref") is not None:
        parent_mesg = await ars_db.get_message_row(mesg["ref"])
        if parent_mesg is None:
            return text(f"Unknown message: {mesg['ref']}", 404)
        json_response = await _retain_all(parent_mesg, json_response)
    else:
        json_response["description"] = "Invalid PK"
    return dj_json(json_response)


@route("/api/block/{key}", ["GET"])
async def block(key: str) -> Response:
    pk = _parse_uuid(key)
    if pk is None:
        return text(f"Unknown message: {key}", 404)
    mesg = await ars_db.get_message_row(pk)
    if mesg is None:
        return text(f"Unknown message: {key}", 404)
    data = await ars_db.load_message_data(pk, logger) or {}
    report = remove_blocked(data, load_blocklist(), str(mesg["id"]))
    await ars_db.save_message_data(pk, data, logger)
    await ars_db.persist_data_copy(pk, logger)
    httpjson = {
        "pk": report[0],
        "blocked_nodes": report[1],
        "removed_results": report[2],
    }
    return dj_json(httpjson)


@route("/api/merge/{key}", ["GET"])
async def merge_debug(key: str) -> Response:
    """Broken upstream (calls a nonexistent utils.merge task): the shell
    merged message is created, then the request dies -> 500."""
    pk = _parse_uuid(key)
    if pk is None:
        return text(f"Unknown message: {key}", 404)
    parent = await ars_db.get_message_row(pk)
    if parent is None:
        return text(f"Unknown message: {key}", 404)
    ars_actor = await lifecycle.ensure_ars_actor()
    merged = await ars_db.create_message(
        actor_id=ars_actor["id"], status="Running", code=202, ref=pk
    )
    data = await ars_db.load_message_data(pk, logger)
    if data is not None:
        await ars_db.save_message_data(merged["id"], data, logger)
    return text("", 500)


@route("/api/post_process/{key}", ["GET"])
async def post_process_debug(key: str) -> Response:
    """Upstream passes a dict where a Message is expected -> 500."""
    return text("", 500)


@route("/api/timeoutTest", ["GET", "POST"])
async def timeout_test() -> Response:
    """Upstream view returns None -> Django 500."""
    return text("", 500)


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


@route("/api/latest_pk/{n}", ["GET"])
async def latest_pk(n: int) -> Response:
    import datetime

    response: Dict[str, Any] = {}
    response[f"pk_count_last_{n}_days"] = {}
    response[f"latest_{n}_pks"] = []
    response["latest_24hr_running_pks"] = []
    default_actor = await lifecycle.ensure_default_actor()
    counts = await ars_db.get_parent_message_counts(default_actor["id"], n)
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=n)
    while start_date <= end_date:
        day = str(start_date.date())
        response[f"pk_count_last_{n}_days"][day] = counts.get(day, 0)
        start_date += datetime.timedelta(days=1)
    response[f"latest_{n}_pks"] = await ars_db.get_latest_parent_pks(
        default_actor["id"], n
    )
    response["latest_24hr_running_pks"] = await ars_db.get_running_parent_pks_24h(
        default_actor["id"]
    )
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
        return text("Only POST is permitted!", 405)
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
