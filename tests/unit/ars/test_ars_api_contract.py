"""Layer-3 API contract parity for the /ars sub-app.

Every assertion cites upstream NCATSTranslator/Relay @ 3e65975
tr_sys/tr_ars/api.py behavior: exact paths, methods, status codes, error
bodies, and response envelope shapes.
"""

import datetime
import json
import uuid
from unittest.mock import AsyncMock

import httpx
import pytest

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle

UTC = datetime.timezone.utc
TS = datetime.datetime(2026, 9, 1, 12, 0, 0, 123456, tzinfo=UTC)


@pytest.fixture
def app():
    from shepherd_server.aras.ars import ARS

    return ARS


@pytest.fixture
def client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


def make_message(
    pk=None,
    status="R",
    code=202,
    agent="ara-shepherd-aragorn",
    ref=None,
    result_count=None,
    params=None,
    name="",
    merged_version=None,
    merged_versions_list=None,
    retain=False,
):
    return {
        "id": pk or uuid.uuid4(),
        "name": name,
        "code": code,
        "status": status,
        "agent": agent,
        "ref": ref,
        "ts": TS,
        "updated_at": TS,
        "url": None,
        "result_count": result_count,
        "result_stat": None,
        "retain": retain,
        "merge_semaphore": False,
        "merged_version": merged_version,
        "merged_versions_list": merged_versions_list,
        "params": params if params is not None else {"query_type": "standard"},
        "clients": [],
    }


def make_cache_entry(
    generation, key, source_pk, state="ready", label_map=None, age_sec=0.0
):
    """A cache index row. Timestamps are relative to now: a ready entry past
    ars_cache_ready_max_age_sec stops answering, so a fixed past date would
    quietly expire every fixture as the clock moved."""
    stamp = datetime.datetime.now(UTC) - datetime.timedelta(seconds=age_sec)
    return {
        "generation": generation,
        "cache_key": key,
        "state": state,
        "source_pk": source_pk,
        "label_map": label_map,
        "created_at": stamp,
        "ready_at": stamp if state == "ready" else None,
        "hit_count": 0,
        "last_hit_at": None,
    }


@pytest.fixture
def db(mocker):
    """Patch every ars_db collaborator the endpoints use."""
    parent_pk = uuid.uuid4()
    child_pk = uuid.uuid4()
    parent = make_message(pk=parent_pk, agent="ars-default-agent")
    child = make_message(pk=child_pk, agent="ara-shepherd-aragorn", ref=parent_pk)

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    rows = {str(parent_pk): parent, str(child_pk): child}

    mocks = {
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "parent": parent,
        "child": child,
        "get_message_row": _patch(
            "get_message_row",
            side_effect=lambda pk: rows.get(str(pk)),
        ),
        "create_message": _patch(
            "create_message",
            side_effect=lambda **kw: make_message(
                agent=kw.get("agent", "ars-default-agent"),
                status=kw.get("status", "R")[:1] if kw.get("status") else "R",
                code=kw.get("code", 202),
                ref=kw.get("ref"),
                params=kw.get("params"),
                name=kw.get("name", ""),
            ),
        ),
        "update_message": _patch(
            "update_message",
            side_effect=lambda pk, **kw: {
                **rows.get(str(pk), make_message(pk=pk)),
                **{k: v for k, v in kw.items() if k != "skip_coercion"},
            },
        ),
        "save_message_data": _patch("save_message_data"),
        "load_message_data": _patch("load_message_data", return_value=None),
        "load_message_bytes": _patch("load_message_bytes", return_value=None),
        "persist_data_copy": _patch("persist_data_copy"),
        "get_children": _patch("get_children", return_value=[]),
        "get_recent_message_pks": _patch("get_recent_message_pks", return_value=[]),
        "get_status_rows": _patch("get_status_rows", return_value={}),
        "retain_tree": _patch("retain_tree"),
        "get_report_rows": _patch("get_report_rows", return_value=[]),
        "get_parent_message_counts": _patch(
            "get_parent_message_counts", return_value={}
        ),
        "get_latest_parent_pks": _patch("get_latest_parent_pks", return_value=[]),
        "get_running_parent_pks_24h": _patch(
            "get_running_parent_pks_24h", return_value=[]
        ),
        "clear_subscriptions": _patch("clear_subscriptions"),
        "get_client": _patch("get_client", return_value=None),
        "add_subscription": _patch("add_subscription"),
        "remove_subscription": _patch("remove_subscription"),
        # response cache index (shepherd_utils.ars.cache): default = every
        # submit claims leadership of a fresh key and fans out as upstream
        "get_cache_generation": _patch("get_cache_generation", return_value=1),
        "claim_or_get_cache_entry": _patch(
            "claim_or_get_cache_entry",
            side_effect=lambda key, pk: (
                1,
                make_cache_entry(1, key, pk, state="pending"),
                True,
            ),
        ),
        "get_current_cache_entry": _patch(
            "get_current_cache_entry", return_value=(1, None)
        ),
        "record_cache_hit": _patch("record_cache_hit"),
        "delete_cache_entry": _patch("delete_cache_entry", return_value=True),
        "delete_message": _patch("delete_message", return_value=True),
        "message_has_data": _patch("message_has_data", return_value=True),
        "bump_cache_generation": _patch("bump_cache_generation", return_value=2),
        "cache_stats": _patch(
            "cache_stats",
            return_value={"generation": 1, "current": {"ready": 0}},
        ),
        "check_parent_completion": mocker.patch.object(
            lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
    }
    return mocks


QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:0005148"]}, "n1": {}},
            "edges": {"e": {"subject": "n1", "object": "n0"}},
        }
    }
}


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


async def test_submit_get_is_405(client, db, redis_mock):
    resp = await client.get("/api/submit")
    assert resp.status_code == 405
    assert resp.text == "Only POST is permitted!"


async def test_submit_returns_201_envelope_and_enqueues_fanout(client, db, redis_mock):
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    body = resp.json()
    assert body["model"] == "tr_ars.message"
    assert body["fields"]["status"] == "Running"
    assert body["fields"]["code"] == 202
    assert body["fields"]["data"] == QUERY
    # the parent is recorded under the ARS's own agent, and query_type is
    # derived from the query graph
    create_kwargs = db["create_message"].await_args.kwargs
    assert create_kwargs["agent"] == "ars-default-agent"
    assert create_kwargs["params"] == {"query_type": "standard"}
    # a fanout wake task was enqueued for the parent
    from shepherd_utils.broker import get_task
    import logging

    task = await get_task("ars.fanout", "consumer", "t", logging.getLogger())
    assert task is not None
    assert task[1]["parent_pk"] == body["pk"]


async def test_submit_pathfinder_query_type(client, db, redis_mock):
    q = {"message": {"query_graph": {"nodes": {}, "edges": {}, "paths": {}}}}
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 201
    assert db["create_message"].await_args.kwargs["params"] == {
        "query_type": "pathfinder"
    }


async def test_submit_validate_flag_stored(client, db, redis_mock):
    q = dict(QUERY, validate=False)
    await client.post("/api/submit", json=q)
    assert db["create_message"].await_args.kwargs["params"]["validate"] is False


async def test_submit_workflow_query_is_a_plain_parent(client, db, redis_mock):
    """Every hosted ARA takes workflow queries, so there is no workflow actor
    to route through any more: the parent is an ordinary submitted query."""
    q = dict(QUERY, workflow=[{"id": "lookup"}])
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 201
    assert db["create_message"].await_args.kwargs["agent"] == "ars-default-agent"
    assert await _fanout_enqueued()


async def test_submit_empty_workflow_is_400(client, db, redis_mock):
    """Upstream UnboundLocalError -> 400 'failing due to ...'."""
    q = dict(QUERY, workflow=[])
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 400
    assert resp.text.startswith("failing due to")
    db["create_message"].assert_not_awaited()


async def test_submit_non_list_workflow_is_400(client, db, redis_mock):
    q = dict(QUERY, workflow="lookup")
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 400
    assert resp.text.startswith("failing due to")
    db["create_message"].assert_not_awaited()


async def test_submit_no_query_graph_is_400(client, db, redis_mock):
    resp = await client.post("/api/submit", json={"nope": 1})
    assert resp.status_code == 400
    assert resp.text.startswith("failing due to")


async def test_submit_name_from_body(client, db, redis_mock):
    q = dict(QUERY, name="my-query")
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 201
    assert db["create_message"].await_args.kwargs.get("name") == "my-query"


# ---------------------------------------------------------------------------
# messages collection
# ---------------------------------------------------------------------------


async def test_messages_get_recent_returns_pks_and_timestamps(client, db, redis_mock):
    """Identifiers only. Upstream inlined every listed message's whole
    stored payload, so listing the last ten queries could mean serving
    hundreds of MB."""
    db["get_recent_message_pks"].return_value = [
        {"id": db["parent_pk"], "ts": TS},
        {"id": db["child_pk"], "ts": TS},
    ]
    resp = await client.get("/api/messages")
    assert resp.status_code == 200
    body = resp.json()
    assert body == [
        {"pk": str(db["parent_pk"]), "timestamp": "2026-09-01T12:00:00.123Z"},
        {"pk": str(db["child_pk"]), "timestamp": "2026-09-01T12:00:00.123Z"},
    ]
    # no payload is read for a listing
    db["load_message_bytes"].assert_not_awaited()
    db["load_message_data"].assert_not_awaited()


async def test_messages_get_recent_is_empty_when_there_are_none(client, db, redis_mock):
    resp = await client.get("/api/messages")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_messages_get_recent_asks_for_ten(client, db, redis_mock):
    await client.get("/api/messages")
    db["get_recent_message_pks"].assert_awaited_once_with(10)


async def test_messages_post_is_405(client, db, redis_mock):
    """The collection is read-only. Upstream's POST looked the actor up in
    the Agent table and assigned it to the actor FK, so it only ever 500'd;
    nothing can depend on it, and creating messages out of band is not an
    endpoint the ARS should expose unauthenticated."""
    resp = await client.post(
        "/api/messages", json={"actor": 1, "name": "x", "status": "D"}
    )
    assert resp.status_code == 405
    assert resp.text == "Only GET is permitted!"
    db["create_message"].assert_not_awaited()


# ---------------------------------------------------------------------------
# message GET / trace
# ---------------------------------------------------------------------------


async def test_message_get_unknown_404(client, db, redis_mock):
    missing = uuid.uuid4()
    resp = await client.get(f"/api/messages/{missing}")
    assert resp.status_code == 404
    assert resp.text == f"Unknown message: {missing}"


async def test_message_get_envelope_uses_agent_name(client, db, redis_mock):
    db["load_message_bytes"].return_value = b'{"message": {}}'
    resp = await client.get(f"/api/messages/{db['parent_pk']}")
    assert resp.status_code == 200
    body = resp.json()
    # upstream overwrites fields.name with the actor's agent name
    assert body["fields"]["name"] == "ars-default-agent"
    assert isinstance(body["fields"]["code"], int)
    assert body["fields"]["data"] == {"message": {}}


async def test_message_trace_tree(client, db, redis_mock):
    child = dict(
        make_message(ref=db["parent_pk"], status="D", code=200, result_count=5),
        inforesid="infores:shepherd-aragorn",
        agent_name="ara-shepherd-aragorn",
    )
    merge_child = dict(
        make_message(agent="ars-ars-agent", ref=db["parent_pk"], status="D", code=200),
        inforesid="infores:ars",
        agent_name="ars-ars-agent",
    )
    db["get_children"].side_effect = lambda pk: (
        [child, merge_child] if str(pk) == str(db["parent_pk"]) else []
    )
    db["load_message_data"].return_value = {
        "message": {"query_graph": {"nodes": {}, "edges": {}}}
    }
    resp = await client.get(f"/api/messages/{db['parent_pk']}?trace=y")
    assert resp.status_code == 200
    tree = resp.json()
    assert tree["message"] == str(db["parent_pk"])
    assert tree["status"] == "Running"
    assert tree["code"] == 202
    assert tree["retain"] is False
    # str(None) stringification quirks
    assert tree["merged_version"] == "None"
    assert tree["merged_versions_list"] == "None"
    assert tree["query_graph"] == {"nodes": {}, "edges": {}}
    assert tree["ref"] is None
    # the actor block names the agent and its infores (no registry row to
    # render any more); the parent is the ARS itself
    assert tree["actor"] == {"agent": "ars-default-agent", "inforesid": "", "ara": None}
    # merge children (ars-ars-agent) are excluded from children
    assert len(tree["children"]) == 1
    node = tree["children"][0]
    assert node["actor"] == {
        "agent": "ara-shepherd-aragorn",
        "inforesid": "infores:shepherd-aragorn",
        "ara": "aragorn",
    }
    assert node["status"] == "Done"
    assert node["result_count"] == 5
    assert node["parent"] == str(db["parent_pk"])


# ---------------------------------------------------------------------------
# POST /api/messages/{pk}: no longer an ARA callback
# ---------------------------------------------------------------------------


RESPONSE = {
    "message": {
        "query_graph": {"nodes": {}, "edges": {}},
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "results": [],
        "auxiliary_graphs": {},
    }
}


async def test_message_post_is_405(client, db, redis_mock):
    """Upstream's ARAs delivered their responses by POSTing here. The
    de-federated ARS receives them over the broker (finish_query ->
    ars.premerge), so a POST changes nothing and enqueues nothing."""
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=RESPONSE)
    assert resp.status_code == 405
    assert resp.text == "Only GET is permitted!"
    db["save_message_data"].assert_not_awaited()
    db["update_message"].assert_not_awaited()
    db["check_parent_completion"].assert_not_awaited()
    from shepherd_utils.broker import get_task
    import logging

    assert await get_task("ars.premerge", "consumer", "t", logging.getLogger()) is None


async def test_message_other_methods_405(client, db, redis_mock):
    for method in ("put", "delete", "patch"):
        resp = await client.request(method, f"/api/messages/{db['child_pk']}")
        assert resp.status_code == 405, method
        assert resp.text == "Only GET is permitted!"


async def test_message_post_unknown_pk_still_405(client, db, redis_mock):
    resp = await client.post(f"/api/messages/{uuid.uuid4()}", json=RESPONSE)
    assert resp.status_code == 405


# ---------------------------------------------------------------------------
# aras (replaces the upstream agents / actors / channels registry surface)
# ---------------------------------------------------------------------------

import time as _time  # noqa: E402

from shepherd_utils.ars import aras as _aras  # noqa: E402
from shepherd_utils.heartbeat import heartbeat_key as _heartbeat_key  # noqa: E402


async def _beat(broker, stream, consumer, age_sec=0.0):
    await broker.set(
        _heartbeat_key(stream, consumer),
        json.dumps({"stream": stream, "last_seen": _time.time() - age_sec}),
    )


async def test_aras_lists_the_hosted_roster(client, db, redis_mock):
    resp = await client.get("/api/aras")
    assert resp.status_code == 200
    body = resp.json()
    assert [a["name"] for a in body] == [a.name for a in _aras.ARAS]
    aragorn = body[0]
    assert aragorn == {
        "name": "aragorn",
        "inforesid": "infores:shepherd-aragorn",
        "agent": "ara-shepherd-aragorn",
        "url": "http://testserver/aragorn",
        "stream": "aragorn",
        "enabled": True,
        # no worker heartbeats in the broker: nothing would pick a query up
        "live_workers": 0,
        "available": False,
    }
    # both slash variants, like every upstream collection route
    assert (await client.get("/api/aras/")).status_code == 200


async def test_aras_reports_live_workers_from_heartbeats(client, db, redis_mock):
    broker = redis_mock["broker"]
    await _beat(broker, "aragorn", "w1")
    await _beat(broker, "aragorn", "w2")
    await _beat(broker, "bte", "stale", age_sec=3600)
    body = {a["name"]: a for a in (await client.get("/api/aras")).json()}
    assert body["aragorn"]["live_workers"] == 2
    assert body["aragorn"]["available"] is True
    assert body["bte"]["live_workers"] == 0
    assert body["bte"]["available"] is False


async def test_aras_reflects_the_enabled_setting(client, db, redis_mock, monkeypatch):
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "ars_enabled_aras", "arax")
    await _beat(redis_mock["broker"], "aragorn", "w1")
    body = {a["name"]: a for a in (await client.get("/api/aras")).json()}
    # every hosted ARA is listed; only the enabled ones are dispatch targets
    assert set(body) == {"aragorn", "arax", "bte"}
    assert body["arax"]["enabled"] is True
    assert body["aragorn"]["enabled"] is False
    # alive but disabled is not available
    assert body["aragorn"]["live_workers"] == 1
    assert body["aragorn"]["available"] is False


async def test_aras_is_read_only(client, db, redis_mock):
    resp = await client.post("/api/aras", json={"name": "new"})
    assert resp.status_code == 405


async def test_registry_endpoints_are_gone(client, db, redis_mock):
    """The ARS talks only to the ARAs this deployment hosts, so there is no
    registry to list or add to: /agents, /actors and /channels are not
    served, in either slash variant or method."""
    for path in ("/api/agents", "/api/agents/", "/api/agents/ara-aragorn"):
        assert (await client.get(path)).status_code == 404, path
    assert (
        await client.post("/api/agents", json={"name": "x", "uri": "/x/"})
    ).status_code == 404
    for path in ("/api/actors", "/api/actors/"):
        assert (await client.get(path)).status_code == 404, path
    assert (
        await client.post(
            "/api/actors", json={"agent": {"name": "a", "uri": "/a/"}, "path": "p"}
        )
    ).status_code == 404
    assert (await client.get("/api/channels")).status_code == 404
    assert (await client.post("/api/channels", json={"name": "x"})).status_code == 404
    db["create_message"].assert_not_awaited()


# ---------------------------------------------------------------------------
# retain / status / health / misc
# ---------------------------------------------------------------------------


async def test_filter_endpoints_are_gone(client, db, redis_mock):
    """/filters and /filter/<pk> are not served: unused, and the filter path
    rewrote and re-saved stored messages."""
    assert (await client.get("/api/filters")).status_code == 404
    assert (await client.get(f"/api/filter/{db['child_pk']}?hop=3")).status_code == 404


async def test_retain_running_parent_refused(client, db, redis_mock):
    resp = await client.get(f"/api/retain/{db['parent_pk']}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["description"] == "PK still running"


async def test_retain_done_parent(client, db, redis_mock):
    db["parent"]["status"] = "D"
    resp = await client.get(f"/api/retain/{db['parent_pk']}")
    body = resp.json()
    assert body["success"] is True
    assert body["parent_pk"] == str(db["parent_pk"])
    db["retain_tree"].assert_awaited_once()


async def test_get_status_post(client, db, redis_mock):
    pk = str(db["parent_pk"])
    db["get_status_rows"].return_value = {
        pk: {
            "status": "R",
            "merged_versions_list": [["m", "a"]],
            "params": {"stats": {"results": 1}},
        }
    }
    resp = await client.post("/api/get_status", json={"pks": [pk, "unknown"]})
    assert resp.status_code == 200
    rows = resp.json()
    assert rows[0] == {
        "pk": pk,
        "status": "Running",
        "merged_list": [["m", "a"]],
        "stats": {"results": 1},
    }
    assert rows[1] == {
        "pk": "unknown",
        "status": None,
        "merged_list": None,
        "stats": None,
    }


async def test_get_status_get_405(client, db, redis_mock):
    resp = await client.get("/api/get_status")
    assert resp.status_code == 405
    assert resp.text == "Only POST is permitted!"


async def test_health(client, db, redis_mock, mocker):
    mocker.patch(
        "shepherd_server.aras.ars._database_available",
        new_callable=AsyncMock,
        return_value=True,
    )
    resp = await client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["database"] == "available"
    assert body["celery"] == "available"


async def test_index_lists_entries(client, db, redis_mock):
    resp = await client.get("/api/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Translator Autonomous Relay System (ARS) API"
    assert any(e.endswith("/ars/api/submit/") for e in body["entries"])


async def test_index_does_not_advertise_dropped_routes(client, db, redis_mock):
    """The index is the ARS's own route directory: it must not point at
    endpoints this port does not serve."""
    entries = (await client.get("/api/")).json()["entries"]
    for dropped in (
        "block/",
        "merge/",
        "post_process/",
        "timeoutTest",
        "filter",
        "channels",
        "agents",
        "actors",
    ):
        assert not any(dropped in e for e in entries), dropped
    assert any(e.endswith("/ars/api/aras/") for e in entries)


async def test_dead_debug_endpoints_are_gone(client, db, redis_mock):
    """merge, post_process and timeoutTest never did anything but 500 -- and
    merge left a Running merge child behind first, which is never terminal,
    so the parent could never complete again."""
    for path in (
        f"/api/merge/{db['parent_pk']}",
        f"/api/post_process/{db['parent_pk']}",
        "/api/timeoutTest",
        f"/api/block/{db['parent_pk']}",
    ):
        resp = await client.get(path)
        assert resp.status_code == 404, path
    db["create_message"].assert_not_awaited()


async def test_latest_pk_shape(client, db, redis_mock):
    db["get_latest_parent_pks"].return_value = ["abc"]
    resp = await client.get("/api/latest_pk/7")
    assert resp.status_code == 200
    body = resp.json()
    assert "pk_count_last_7_days" in body
    assert body["latest_7_pks"] == ["abc"]
    assert "latest_24hr_running_pks" in body
    db["get_parent_message_counts"].assert_awaited_once_with(7)
    db["get_latest_parent_pks"].assert_awaited_once_with(7)
    db["get_running_parent_pks_24h"].assert_awaited_once_with()


async def test_report_shape(client, db, redis_mock):
    mid = uuid.uuid4()
    db["get_report_rows"].return_value = [
        {
            "code": 200,
            "id": mid,
            "ts": TS,
            "updated_at": TS + datetime.timedelta(seconds=90),
            "result_count": 4,
        }
    ]
    resp = await client.get("/api/reports/aragorn")
    assert resp.status_code == 200
    body = resp.json()
    entry = body[str(mid)]
    assert entry["status_code"] == 200
    assert entry["result_count"] == 4
    assert entry["time_elapsed"] == "0:01:30"


async def test_submit_retries_transient_pg_failure(client, db, mocker, redis_mock):
    """A transient Postgres pool timeout on the parent insert is retried;
    the submit still lands 201."""
    from psycopg_pool import PoolTimeout

    db["create_message"].side_effect = [
        PoolTimeout("couldn't get a connection after 5.00 sec"),
        make_message(agent="ars-default-agent"),
    ]
    mocker.patch("asyncio.sleep")
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    assert db["create_message"].await_count == 2


async def test_submit_enqueue_failure_is_honest_400(client, db, mocker, redis_mock):
    """If the fanout task can't be enqueued, submit must NOT return 201 --
    a success response for a query no worker will ever pick up leaves it
    Running forever (parents are watchdog-exempt)."""
    import shepherd_utils.broker as broker_mod

    mocker.patch.object(
        broker_mod.broker_client,
        "xadd",
        side_effect=TimeoutError("Timeout reading from shepherd_broker:6379"),
    )
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 400
    assert "failing due to" in resp.text


async def test_submit_payload_save_failure_is_honest_400(
    client, db, mocker, redis_mock
):
    """If the query payload can't be stored after retries, submit answers
    400 instead of a phantom 201 for a query with no stored payload."""
    db["save_message_data"].side_effect = TimeoutError(
        "Timeout reading from shepherd_broker:6379"
    )
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 400
    assert "failing due to" in resp.text


# ---------------------------------------------------------------------------
# response cache (Shepherd-native; docs/ARS_RESPONSE_CACHE_PLAN.md)
# ---------------------------------------------------------------------------

import logging as _logging  # noqa: E402

from shepherd_utils.broker import get_task as _get_task  # noqa: E402


async def _fanout_enqueued():
    task = await _get_task("ars.fanout", "consumer", "t", _logging.getLogger())
    return task is not None


MERGED_PAYLOAD = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:0005148"]}, "n1": {}},
            "edges": {"e": {"subject": "n1", "object": "n0"}},
        },
        "knowledge_graph": {"nodes": {"MONDO:0005148": {}}, "edges": {}},
        "results": [
            {
                "node_bindings": {"n0": [{"id": "MONDO:0005148"}], "n1": []},
                "analyses": [{"resource_id": "infores:x", "edge_bindings": {"e": []}}],
            }
        ],
    },
    "logs": [{"message": "merged", "level": "INFO"}],
}

# QUERY under other labels: same key, bindings must come back relabeled
RELABELED_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"disease": {"ids": ["MONDO:0005148"]}, "chem": {}},
            "edges": {"treats": {"subject": "chem", "object": "disease"}},
        }
    }
}


def _source_tree(db):
    """A completed source tree: parent Done -> merged message."""
    from shepherd_utils.ars import cache as _cache

    source_pk, merged_pk = uuid.uuid4(), uuid.uuid4()
    source = make_message(
        pk=source_pk,
        agent="ars-default-agent",
        status="D",
        code=200,
        merged_version=merged_pk,
        merged_versions_list=[[str(merged_pk), "ara-aragorn"]],
        params={"query_type": "standard", "stats": {"results": 1}},
        result_count=1,
    )
    merged = make_message(
        pk=merged_pk, agent="ars-ars-agent", ref=source_pk, status="D", code=200
    )
    rows = {
        str(source_pk): source,
        str(merged_pk): merged,
        str(db["parent_pk"]): db["parent"],
    }
    db["get_message_row"].side_effect = lambda pk: rows.get(str(pk))
    db["load_message_data"].side_effect = lambda pk, *a: (
        json.loads(json.dumps(MERGED_PAYLOAD)) if str(pk) == str(merged_pk) else None
    )
    db["load_message_bytes"].side_effect = lambda pk, *a: (
        json.dumps(MERGED_PAYLOAD).encode() if str(pk) == str(merged_pk) else None
    )
    _, label_map = _cache.canonical_graph(MERGED_PAYLOAD["message"]["query_graph"])
    return source, merged, label_map


async def test_submit_cache_hit_returns_source_pk_without_payload(
    client, db, redis_mock
):
    source, merged, label_map = _source_tree(db)
    db["get_current_cache_entry"].side_effect = lambda key: (
        1,
        make_cache_entry(1, key, source["id"], label_map=label_map),
    )
    resp = await client.post("/api/submit", json=RELABELED_QUERY)
    assert resp.status_code == 201
    body = resp.json()
    # the caller is handed the shared, already-Done pk...
    assert body["pk"] == str(source["id"])
    assert body["fields"]["status"] == "Done"
    assert body["fields"]["code"] == 200
    assert body["fields"]["merged_version"] == str(merged["id"])
    # ...with their own submit body as data, like any fresh parent: the
    # merged response is NOT embedded (the client fetches merged_version)
    assert body["fields"]["data"] == RELABELED_QUERY
    assert len(resp.content) < 4096
    # nothing was created, dispatched, or even decompressed
    db["create_message"].assert_not_awaited()
    db["save_message_data"].assert_not_awaited()
    db["load_message_data"].assert_not_awaited()
    assert not await _fanout_enqueued()
    db["record_cache_hit"].assert_awaited_once()


async def test_submit_pending_entry_returns_leader_pk(client, db, redis_mock):
    leader_pk = uuid.uuid4()
    leader = make_message(pk=leader_pk, agent="ars-default-agent", status="R", code=202)
    db["get_message_row"].side_effect = lambda pk: (
        leader if str(pk) == str(leader_pk) else None
    )
    db["get_current_cache_entry"].side_effect = lambda key: (
        1,
        make_cache_entry(1, key, leader_pk, state="pending"),
    )
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    body = resp.json()
    assert body["pk"] == str(leader_pk)
    assert body["fields"]["status"] == "Running"
    assert body["fields"]["code"] == 202
    assert body["fields"]["data"] == QUERY
    db["create_message"].assert_not_awaited()
    assert not await _fanout_enqueued()


async def test_submit_miss_claims_leadership_and_fans_out(client, db, redis_mock):
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    assert resp.json()["fields"]["params"]["cache"]["role"] == "leader"
    assert await _fanout_enqueued()
    key, pk = db["claim_or_get_cache_entry"].await_args.args
    assert len(key) == 64 and str(pk) == resp.json()["pk"]
    db["delete_message"].assert_not_awaited()


async def test_submit_lost_race_discards_own_row_and_serves_winner(
    client, db, redis_mock
):
    """Two identical misses: the lookup saw nothing, but by the time we
    claim, a concurrent submit already owns the key."""
    source, merged, label_map = _source_tree(db)
    db["claim_or_get_cache_entry"].side_effect = lambda key, pk: (
        1,
        make_cache_entry(1, key, source["id"], label_map=label_map),
        False,
    )
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    assert resp.json()["pk"] == str(source["id"])
    assert resp.json()["fields"]["status"] == "Done"
    assert resp.json()["fields"]["data"] == QUERY
    # our own parent was created, then discarded
    db["create_message"].assert_awaited_once()
    db["delete_message"].assert_awaited_once()
    assert not await _fanout_enqueued()


async def test_submit_bypass_cache_skips_lookup(client, db, redis_mock):
    resp = await client.post("/api/submit", json=dict(QUERY, bypass_cache=True))
    assert resp.status_code == 201
    assert resp.json()["fields"]["params"]["cache"]["role"] == "bypass"
    db["get_current_cache_entry"].assert_not_awaited()
    db["claim_or_get_cache_entry"].assert_not_awaited()
    assert await _fanout_enqueued()


async def test_submit_overwrite_cache_runs_and_marks_role(client, db, redis_mock):
    q = dict(QUERY, parameters={"overwrite_cache": True})
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 201
    assert resp.json()["fields"]["params"]["cache"]["role"] == "overwrite"
    db["get_current_cache_entry"].assert_not_awaited()
    db["claim_or_get_cache_entry"].assert_not_awaited()
    assert await _fanout_enqueued()


async def test_submit_cache_disabled_behaves_as_upstream(
    client, db, redis_mock, monkeypatch
):
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    assert "cache" not in (resp.json()["fields"]["params"] or {})
    db["get_current_cache_entry"].assert_not_awaited()
    db["claim_or_get_cache_entry"].assert_not_awaited()
    assert await _fanout_enqueued()


async def test_submit_broken_ready_entry_falls_back_to_leading(client, db, redis_mock):
    """A ready entry whose source tree is gone is dropped; we run the query."""
    db["get_current_cache_entry"].side_effect = lambda key: (
        1,
        make_cache_entry(1, key, uuid.uuid4()),
    )
    # get_message_row knows nothing about that source -> broken
    resp = await client.post("/api/submit", json=QUERY)
    assert resp.status_code == 201
    assert resp.json()["fields"]["params"]["cache"]["role"] == "leader"
    db["delete_cache_entry"].assert_awaited_once()
    assert await _fanout_enqueued()


async def test_message_get_splices_stored_bytes_without_parsing(client, db, redis_mock):
    """The payload is served from its stored bytes: it is never parsed on the
    server, and the envelope around it is intact."""
    source, merged, _ = _source_tree(db)
    resp = await client.get(f"/api/messages/{merged['id']}")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    body = resp.json()
    assert body["pk"] == str(merged["id"])
    assert body["fields"]["name"] == "ars-ars-agent"
    assert body["fields"]["status"] == "Done"
    assert isinstance(body["fields"]["code"], int)
    assert body["fields"]["data"] == MERGED_PAYLOAD
    db["load_message_data"].assert_not_awaited()
    db["load_message_bytes"].assert_awaited_once()
    # a message without any stored payload renders data: null
    resp = await client.get(f"/api/messages/{source['id']}")
    assert resp.status_code == 200
    assert resp.json()["fields"]["data"] is None


async def test_cache_admin_routes_disabled_without_token(
    client, db, redis_mock, monkeypatch
):
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "ars_admin_token", "")
    assert (await client.get("/api/cache")).status_code == 403
    assert (await client.post("/api/cache/invalidate")).status_code == 403
    db["bump_cache_generation"].assert_not_awaited()


async def test_cache_admin_routes_with_token(client, db, redis_mock, monkeypatch):
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "ars_admin_token", "s3cret")
    bad = {"Authorization": "Bearer nope"}
    good = {"Authorization": "Bearer s3cret"}
    assert (await client.get("/api/cache", headers=bad)).status_code == 403
    resp = await client.get("/api/cache", headers=good)
    assert resp.status_code == 200
    assert resp.json()["generation"] == 1
    resp = await client.post(
        "/api/cache/invalidate", headers=good, json={"reason": "new KG"}
    )
    assert resp.status_code == 200
    assert resp.json() == {"generation": 2, "reason": "new KG"}
    db["bump_cache_generation"].assert_awaited_once_with("new KG")
    resp = await client.post("/api/cache/invalidate", headers=good, content=b"{bad")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# subscriptions to an already-finished pk (response-cache hit)
# ---------------------------------------------------------------------------

from shepherd_utils.ars import crypto as _crypto  # noqa: E402


@pytest.fixture
def subscriber(mocker, db):
    client = {
        "id": 7,
        "client_id": "ui",
        "client_secret": "enc",
        "callback_url": "https://ui.example/notify",
        "active": True,
        "subscriptions": [],
    }
    db["get_client"].return_value = client
    mocker.patch.object(_crypto, "master_key", return_value=b"k" * 32)
    mocker.patch.object(_crypto, "decrypt_secret", return_value="secret")
    mocker.patch.object(_crypto, "verify_body_signature", return_value=True)
    mocker.patch.object(
        ars_db, "load_otel_carrier", new_callable=AsyncMock, return_value="{}"
    )
    return client


async def _notify_tasks():
    out = []
    while True:
        task = await _get_task("ars.notify", "consumer", "t", _logging.getLogger())
        if task is None:
            return out
        out.append(task[1])


async def test_subscribe_to_done_pk_replays_completion_to_that_client(
    client, db, redis_mock, subscriber
):
    source, merged, _ = _source_tree(db)
    body = json.dumps({"client_id": "ui", "pks": [str(source["id"])]})
    resp = await client.post(
        "/api/query_event_subscribe",
        content=body,
        headers={"x-event-signature": "sig", "content-type": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.json()["success"] == [str(source["id"])]
    # no standing subscription is created for a finished query...
    db["add_subscription"].assert_not_awaited()
    # ...the completion events are replayed to this client alone
    tasks = await _notify_tasks()
    assert [json.loads(t["fields"])["event_type"] for t in tasks] == [
        "last_merged_completed",
        "admin",
    ]
    assert all(json.loads(t["client_pks"]) == ["7"] for t in tasks)
    assert all(t["message_pk"] == str(source["id"]) for t in tasks)


async def test_subscribe_to_running_pk_still_subscribes(
    client, db, redis_mock, subscriber
):
    body = json.dumps({"client_id": "ui", "pks": [str(db["parent_pk"])]})
    resp = await client.post(
        "/api/query_event_subscribe",
        content=body,
        headers={"x-event-signature": "sig", "content-type": "application/json"},
    )
    assert resp.status_code == 200
    db["add_subscription"].assert_awaited_once()
    assert await _notify_tasks() == []


async def test_subscribe_to_done_pk_refused_when_cache_disabled(
    client, db, redis_mock, subscriber, monkeypatch
):
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    source, merged, _ = _source_tree(db)
    body = json.dumps({"client_id": "ui", "pks": [str(source["id"])]})
    resp = await client.post(
        "/api/query_event_subscribe",
        content=body,
        headers={"x-event-signature": "sig", "content-type": "application/json"},
    )
    # upstream _analyze_response: nothing succeeded -> 400 with the failure map
    assert resp.status_code == 400
    assert resp.json()["failure"] == {str(source["id"]): "Query already complete"}
    assert await _notify_tasks() == []
