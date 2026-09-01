"""Layer-3 API contract parity for the /ars sub-app.

Every assertion cites upstream NCATSTranslator/Relay @ dd1e71b
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


def make_actor(
    actor_id=7,
    agent="ara-aragorn",
    inforesid="infores:aragorn",
    path="runquery",
    channels=("general",),
    active=True,
    uri=None,
):
    serialized = [
        {
            "model": "tr_ars.channel",
            "pk": i + 1,
            "fields": {"name": name, "description": None},
        }
        for i, name in enumerate(channels)
    ]
    return {
        "id": actor_id,
        "agent": 4,
        "channel": serialized,
        "path": path,
        "inforesid": inforesid,
        "active": active,
        "agent_name": agent,
        "agent_uri": uri if uri is not None else f"/{agent}/api/",
    }


def make_message(
    pk=None,
    status="R",
    code=202,
    actor=7,
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
        "actor": actor,
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


@pytest.fixture
def db(mocker):
    """Patch every ars_db collaborator the endpoints use."""
    parent_pk = uuid.uuid4()
    child_pk = uuid.uuid4()
    default_actor = make_actor(1, "ars-default-agent", "", "", ("general",), uri="")
    ara_actor = make_actor()
    parent = make_message(pk=parent_pk, actor=1)
    child = make_message(pk=child_pk, actor=7, ref=parent_pk)

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    rows = {str(parent_pk): parent, str(child_pk): child}

    mocks = {
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "parent": parent,
        "child": child,
        "ara_actor": ara_actor,
        "default_actor": default_actor,
        "get_message_row": _patch(
            "get_message_row",
            side_effect=lambda pk: rows.get(str(pk)),
        ),
        "create_message": _patch(
            "create_message",
            side_effect=lambda **kw: make_message(
                actor=kw.get("actor_id", 1),
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
        "persist_data_copy": _patch("persist_data_copy"),
        "get_children": _patch("get_children", return_value=[]),
        "get_recent_messages": _patch("get_recent_messages", return_value=[]),
        "get_actor": _patch(
            "get_actor",
            side_effect=lambda aid: {1: default_actor, 7: ara_actor}.get(aid),
        ),
        "get_or_create_actor": _patch(
            "get_or_create_actor", return_value=(ara_actor, 302)
        ),
        "get_or_create_agent": _patch(
            "get_or_create_agent",
            return_value=(
                {
                    "id": 4,
                    "name": "ara-aragorn",
                    "description": None,
                    "uri": "/ara-aragorn/api/",
                    "contact": None,
                    "registered": TS,
                    "updated": TS,
                },
                201,
            ),
        ),
        "get_agent_by_name": _patch("get_agent_by_name", return_value=None),
        "list_agents": _patch("list_agents", return_value=[]),
        "list_channels": _patch("list_channels", return_value=[]),
        "list_actors": _patch("list_actors", return_value=[ara_actor]),
        "get_or_create_channel": _patch(
            "get_or_create_channel",
            return_value=({"id": 1, "name": "general", "description": None}, False),
        ),
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
        "check_parent_completion": mocker.patch.object(
            lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
        "ensure_default_actor": mocker.patch.object(
            lifecycle,
            "ensure_default_actor",
            new_callable=AsyncMock,
            return_value=default_actor,
        ),
        "ensure_workflow_actor": mocker.patch.object(
            lifecycle,
            "ensure_workflow_actor",
            new_callable=AsyncMock,
            return_value=make_actor(
                2, "ars-workflow-agent", "", "", ("workflow",), uri=""
            ),
        ),
        "ensure_ars_actor": mocker.patch.object(
            lifecycle,
            "ensure_ars_actor",
            new_callable=AsyncMock,
            return_value=make_actor(3, "ars-ars-agent", "infores:ars", "", (), uri=""),
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
    # query_type derived from the query graph
    create_kwargs = db["create_message"].await_args.kwargs
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


async def test_submit_workflow_selects_workflow_actor(client, db, redis_mock):
    q = dict(QUERY, workflow=[{"id": "lookup"}])
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 201
    db["ensure_workflow_actor"].assert_awaited_once()


async def test_submit_empty_workflow_is_400(client, db, redis_mock):
    """Upstream UnboundLocalError -> 400 'failing due to ...'."""
    q = dict(QUERY, workflow=[])
    resp = await client.post("/api/submit", json=q)
    assert resp.status_code == 400
    assert resp.text.startswith("failing due to")


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


async def test_messages_get_recent(client, db, redis_mock):
    db["get_recent_messages"].return_value = [db["parent"]]
    resp = await client.get("/api/messages")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["model"] == "tr_ars.message"


async def test_messages_post_is_500(client, db, redis_mock):
    """Upstream POST /messages is broken (Agent-as-actor) -> 500."""
    resp = await client.post(
        "/api/messages", json={"actor": 1, "name": "x", "status": "D"}
    )
    assert resp.status_code == 500
    assert resp.text == "Internal server error"


# ---------------------------------------------------------------------------
# message GET / trace
# ---------------------------------------------------------------------------


async def test_message_get_unknown_404(client, db, redis_mock):
    missing = uuid.uuid4()
    resp = await client.get(f"/api/messages/{missing}")
    assert resp.status_code == 404
    assert resp.text == f"Unknown message: {missing}"


async def test_message_get_envelope_uses_agent_name(client, db, redis_mock):
    db["load_message_data"].return_value = {"message": {}}
    resp = await client.get(f"/api/messages/{db['parent_pk']}")
    assert resp.status_code == 200
    body = resp.json()
    # upstream overwrites fields.name with the actor's agent name
    assert body["fields"]["name"] == "ars-default-agent"
    assert isinstance(body["fields"]["code"], int)
    assert body["fields"]["data"] == {"message": {}}


async def test_message_trace_tree(client, db, redis_mock):
    child = dict(
        make_message(
            actor=7, ref=db["parent_pk"], status="D", code=200, result_count=5
        ),
        inforesid="infores:aragorn",
        actor_channel=db["ara_actor"]["channel"],
        actor_path="runquery",
        agent_name="ara-aragorn",
        actor_id=7,
    )
    merge_child = dict(
        make_message(actor=3, ref=db["parent_pk"], status="D", code=200),
        inforesid="infores:ars",
        actor_channel=[],
        actor_path="",
        agent_name="ars-ars-agent",
        actor_id=3,
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
    # merge children (infores:ars) are excluded from children
    assert len(tree["children"]) == 1
    node = tree["children"][0]
    assert node["actor"]["agent"] == "ara-aragorn"
    assert node["actor"]["channel"] == ["general"]
    assert node["status"] == "Done"
    assert node["result_count"] == 5
    assert node["parent"] == str(db["parent_pk"])


# ---------------------------------------------------------------------------
# callback POST /api/messages/{pk}
# ---------------------------------------------------------------------------


RESPONSE = {
    "message": {
        "query_graph": {"nodes": {}, "edges": {}},
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "results": [],
        "auxiliary_graphs": {},
    }
}


async def test_callback_unknown_pk_404(client, db, redis_mock):
    missing = uuid.uuid4()
    resp = await client.post(f"/api/messages/{missing}", json=RESPONSE)
    assert resp.status_code == 404
    assert resp.text == f"Unknown state reference {missing}"


async def test_callback_bad_json_500(client, db, redis_mock):
    resp = await client.post(
        f"/api/messages/{db['child_pk']}",
        content=b"{nope",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 500
    assert "Can not decode json" in resp.text


async def test_callback_already_done_returns_200_text(client, db, redis_mock):
    db["child"]["status"] = "D"
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=RESPONSE)
    assert resp.status_code == 200
    assert "ARS has already received" in resp.text


async def test_callback_duplicate_results_409(client, db, redis_mock):
    db["child"]["result_count"] = 12
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=RESPONSE)
    assert resp.status_code == 409
    assert "ARS already has a response" in resp.text


async def test_callback_errored_child_400(client, db, redis_mock):
    db["child"]["status"] = "E"
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=RESPONSE)
    assert resp.status_code == 400
    assert "Response rejected" in resp.text


async def test_callback_empty_results_completes_child(client, db, redis_mock):
    """results=[]: child -> D/200, result_count stays None (upstream only
    zeroes it when results is literally absent)."""
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=RESPONSE)
    assert resp.status_code == 201
    update = db["update_message"].await_args_list[-1]
    assert update.kwargs.get("status") == "D"
    assert "result_count" not in update.kwargs
    db["check_parent_completion"].assert_awaited()


async def test_callback_missing_results_zeroes_count(client, db, redis_mock):
    resp = await client.post(
        f"/api/messages/{db['child_pk']}",
        json={"message": {"knowledge_graph": {"nodes": {}, "edges": {}}}},
    )
    assert resp.status_code == 201
    update = db["update_message"].await_args_list[-1]
    assert update.kwargs.get("result_count") == 0


async def test_callback_with_results_premerges_and_enqueues_merge(
    client, db, redis_mock, mocker
):
    import pathlib

    valid = json.loads(
        pathlib.Path("tests/fixtures/ars_corpus/response_aragorn.json").read_text()
    )
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=valid)
    assert resp.status_code == 201
    body = resp.json()
    assert body["fields"]["status"] == "Done"
    # premerged blob saved under the child pk
    save = db["save_message_data"].await_args
    assert str(save.args[0]) == str(db["child_pk"])
    saved_payload = save.args[1]
    # normalize_scores ran (pre-merge processing happened inline)
    assert "normalized_score" in saved_payload["message"]["results"][0]
    # merge wake task enqueued for the ara- agent
    from shepherd_utils.broker import get_task
    import logging

    task = await get_task("ars.merge", "consumer", "t", logging.getLogger())
    assert task is not None
    assert task[1]["parent_pk"] == str(db["parent_pk"])
    assert task[1]["child_pk"] == str(db["child_pk"])
    assert task[1]["agent_name"] == "ara-aragorn"
    # result_count / result_stat recorded on the child
    update = db["update_message"].await_args_list[-1]
    assert update.kwargs.get("result_count") == 2


async def test_callback_invalid_trapi_422(client, db, redis_mock):
    """A payload that survives pre-merge processing but fails TRAPI
    validation (result missing node_bindings) -> 422, child E/422."""
    import pathlib

    invalid = json.loads(
        pathlib.Path("tests/fixtures/ars_corpus/response_aragorn.json").read_text()
    )
    del invalid["message"]["results"][0]["node_bindings"]
    resp = await client.post(f"/api/messages/{db['child_pk']}", json=invalid)
    assert resp.status_code == 422
    assert resp.text == "Problem with TRAPI Validation"
    update = db["update_message"].await_args_list[-1]
    assert update.kwargs.get("status") == "E"
    assert update.kwargs.get("code") == 422
    # validation failure is terminal -> completion check runs
    db["check_parent_completion"].assert_awaited()


async def test_callback_header_status_override(client, db, redis_mock):
    resp = await client.post(
        f"/api/messages/{db['child_pk']}",
        json=RESPONSE,
        headers={"tr_ars.message.status": "S"},
    )
    assert resp.status_code == 201
    update = db["update_message"].await_args_list[-1]
    assert update.kwargs.get("status") == "S"


# ---------------------------------------------------------------------------
# agents / actors / channels
# ---------------------------------------------------------------------------


async def test_agents_get(client, db, redis_mock):
    db["list_agents"].return_value = [
        {
            "id": 4,
            "name": "ara-aragorn",
            "description": None,
            "uri": "/ara-aragorn/api/",
            "contact": None,
            "registered": TS,
            "updated": TS,
        }
    ]
    resp = await client.get("/api/agents")
    assert resp.status_code == 200
    assert resp.json()[0]["model"] == "tr_ars.agent"


async def test_agents_post_missing_fields_400(client, db, redis_mock):
    resp = await client.post("/api/agents", json={"name": "x"})
    assert resp.status_code == 400
    assert resp.text == 'JSON does not contain "name" and "uri" fields'


async def test_agents_post_created_201(client, db, redis_mock):
    resp = await client.post("/api/agents", json={"name": "ara-new", "uri": "/x/api/"})
    assert resp.status_code == 201


async def test_get_agent_unknown_400(client, db, redis_mock):
    resp = await client.get("/api/agents/nope")
    assert resp.status_code == 400
    assert resp.text == "Unknown agent: nope"


async def test_actors_get_shape(client, db, redis_mock):
    resp = await client.get("/api/actors")
    assert resp.status_code == 200
    actor = resp.json()[0]
    fields = actor["fields"]
    assert fields["name"] == "ara-aragorn-runquery"
    assert fields["channel"] == ["general"]
    assert fields["agent"] == "ara-aragorn"
    assert "urlRemote" in fields
    assert fields["path"].endswith("/ara-aragorn/api/runquery")
    assert fields["active"] is True
    assert fields["inforesid"] == "infores:aragorn"


async def test_actors_post_is_400(client, db, redis_mock):
    """Upstream crashes on actor.channel.name after creating the actor ->
    400 'Not a valid json format' (the side effect still happens)."""
    resp = await client.post(
        "/api/actors",
        json={
            "channel": ["general"],
            "agent": {"name": "a", "uri": "/a/"},
            "path": "runquery",
            "inforesid": "infores:a",
        },
    )
    assert resp.status_code == 400
    assert resp.text == "Not a valid json format"
    db["get_or_create_actor"].assert_awaited_once()


async def test_channels_get_and_post(client, db, redis_mock):
    resp = await client.get("/api/channels")
    assert resp.status_code == 200
    db["get_or_create_channel"].return_value = (
        {"id": 9, "name": "new", "description": None},
        True,
    )
    resp = await client.post("/api/channels", json={"name": "new"})
    assert resp.status_code == 201
    db["get_or_create_channel"].return_value = (
        {"id": 9, "name": "new", "description": "d"},
        False,
    )
    resp = await client.post("/api/channels", json={"name": "new", "description": "d"})
    assert resp.status_code == 302
    resp = await client.post("/api/channels", json={"nope": 1})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# filters / retain / status / health / misc
# ---------------------------------------------------------------------------


async def test_filters_documentation(client, db, redis_mock):
    resp = await client.get("/api/filters")
    assert resp.status_code == 200
    doc = resp.json()
    assert doc["hop_level"]["default"] == 3
    assert doc["score_level"]["default"] == [20, 80]
    assert set(doc.keys()) == {
        "hop_level",
        "score_level",
        "node_type",
        "spec_node",
        "multi-filtering",
    }


async def test_filter_redirects_302(client, db, redis_mock):
    """Filtering a non-parent Done message creates a filtered copy and
    302-redirects to its trace view."""
    db["child"]["status"] = "D"
    db["child"]["code"] = 200
    db["child"]["result_count"] = 3
    db["load_message_data"].return_value = {
        "message": {
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "A"}], "n1": [{"id": "B"}]},
                    "normalized_score": 50,
                },
            ],
            "knowledge_graph": {"nodes": {}, "edges": {}},
        }
    }
    resp = await client.get(f"/api/filter/{db['child_pk']}?hop=3")
    assert resp.status_code == 302
    assert resp.headers["location"].startswith("/ars/api/messages/")
    assert resp.headers["location"].endswith("?trace=y")


async def test_filter_not_done_400(client, db, redis_mock):
    resp = await client.get(f"/api/filter/{db['child_pk']}?hop=3")
    assert resp.status_code == 400
    assert resp.text == 'message doesnt have results or marked as "Done"'


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


async def test_merge_debug_endpoint_500(client, db, redis_mock):
    """Upstream utils.merge doesn't exist -> the endpoint dies after creating
    the shell merge message."""
    resp = await client.get(f"/api/merge/{db['parent_pk']}")
    assert resp.status_code == 500
    db["create_message"].assert_awaited_once()


async def test_latest_pk_shape(client, db, redis_mock):
    db["get_latest_parent_pks"].return_value = ["abc"]
    resp = await client.get("/api/latest_pk/7")
    assert resp.status_code == 200
    body = resp.json()
    assert "pk_count_last_7_days" in body
    assert body["latest_7_pks"] == ["abc"]
    assert "latest_24hr_running_pks" in body


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
