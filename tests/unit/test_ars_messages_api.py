"""Tests for the ARS read/registry HTTP endpoints (FastAPI TestClient)."""

import pytest
from fastapi.testclient import TestClient

from shepherd_server import ars


@pytest.fixture
def client():
    return TestClient(ars.ARS)


def test_list_messages(client, mocker):
    mocker.patch.object(
        ars,
        "list_ars_parents",
        new_callable=mocker.AsyncMock,
        return_value=[{"qid": "p1", "state": "QUEUED"}],
    )
    resp = client.get("/messages")
    assert resp.status_code == 200
    assert resp.json() == {"messages": [{"qid": "p1", "state": "QUEUED"}]}


def test_trace_message_404_when_missing(client, mocker):
    mocker.patch.object(
        ars, "get_query_state", new_callable=mocker.AsyncMock, return_value=None
    )
    resp = client.get("/messages/ghost/trace")
    assert resp.status_code == 404


def test_trace_message_returns_children(client, mocker):
    # shepherd_brain row: index 0=qid,7=response_id,9=state,10=status
    state = ["p1", None, None, None, None, None, None, "r1", None, "QUEUED", "OK"]
    mocker.patch.object(
        ars, "get_query_state", new_callable=mocker.AsyncMock, return_value=state
    )
    mocker.patch.object(
        ars,
        "get_ars_children",
        new_callable=mocker.AsyncMock,
        return_value=[{"ara": "aragorn", "status": "DONE"}],
    )
    resp = client.get("/messages/p1/trace")
    assert resp.status_code == 200
    body = resp.json()
    assert body["parent"]["qid"] == "p1"
    assert body["children"][0]["ara"] == "aragorn"


def test_status_overall_running(client, mocker):
    state = ["p1", None, None, None, None, None, None, "r1", None, "QUEUED", "OK"]
    mocker.patch.object(
        ars, "get_query_state", new_callable=mocker.AsyncMock, return_value=state
    )
    mocker.patch.object(
        ars,
        "get_ars_children",
        new_callable=mocker.AsyncMock,
        return_value=[
            {"ara": "aragorn", "status": "DONE"},
            {"ara": "bte", "status": "QUEUED"},
        ],
    )
    resp = client.get("/status/p1")
    assert resp.status_code == 200
    assert resp.json()["status"] == "Running"


def test_list_actors(client, mocker):
    mocker.patch.object(
        ars,
        "list_actors",
        new_callable=mocker.AsyncMock,
        return_value=[{"infores": "infores:bte"}],
    )
    resp = client.get("/actors")
    assert resp.status_code == 200
    assert resp.json() == {"actors": [{"infores": "infores:bte"}]}


def test_register_actor_requires_infores(client, mocker):
    upsert = mocker.patch.object(
        ars, "upsert_actor", new_callable=mocker.AsyncMock
    )
    resp = client.post("/actors", json={"url": "u"})
    assert resp.status_code == 422
    upsert.assert_not_called()


def test_register_actor_upserts(client, mocker):
    upsert = mocker.patch.object(
        ars, "upsert_actor", new_callable=mocker.AsyncMock
    )
    resp = client.post("/actors", json={"infores": "infores:x", "url": "u"})
    assert resp.status_code == 200
    assert resp.json() == {"registered": "infores:x"}
    upsert.assert_awaited_once()


def test_discover_actors(client, mocker):
    mocker.patch.object(
        ars, "refresh_actors", new_callable=mocker.AsyncMock, return_value=3
    )
    resp = client.post("/actors/discover")
    assert resp.status_code == 200
    assert resp.json() == {"discovered": 3}


def test_subscribe_requires_callback(client, mocker):
    add = mocker.patch.object(ars, "add_subscriber", new_callable=mocker.AsyncMock)
    resp = client.post("/subscribe/p1", json={})
    assert resp.status_code == 422
    add.assert_not_called()


def test_health_ok(client, mocker):
    mocker.patch.object(ars, "ping_db", new_callable=mocker.AsyncMock, return_value=True)
    mocker.patch.object(
        ars, "ping_redis", new_callable=mocker.AsyncMock, return_value=True
    )
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "db": True, "broker": True}


def test_health_degraded_returns_503(client, mocker):
    mocker.patch.object(ars, "ping_db", new_callable=mocker.AsyncMock, return_value=True)
    mocker.patch.object(
        ars, "ping_redis", new_callable=mocker.AsyncMock, return_value=False
    )
    resp = client.get("/health")
    assert resp.status_code == 503


def test_latest_pk(client, mocker):
    mocker.patch.object(
        ars,
        "get_latest_pks",
        new_callable=mocker.AsyncMock,
        return_value={"latest_5_pks": ["p1"]},
    )
    resp = client.get("/latest_pk/5")
    assert resp.status_code == 200
    assert resp.json() == {"latest_5_pks": ["p1"]}


def test_report(client, mocker):
    mocker.patch.object(
        ars,
        "get_ars_report",
        new_callable=mocker.AsyncMock,
        return_value={"c1": {"status_code": 200}},
    )
    resp = client.get("/report/aragorn")
    assert resp.status_code == 200
    assert resp.json() == {"c1": {"status_code": 200}}


def test_register_agent_and_get(client, mocker):
    upsert = mocker.patch.object(ars, "upsert_agent", new_callable=mocker.AsyncMock)
    resp = client.post("/agents", json={"name": "BTE", "uri": "u"})
    assert resp.status_code == 200
    upsert.assert_awaited_once()
    mocker.patch.object(
        ars,
        "get_agent",
        new_callable=mocker.AsyncMock,
        return_value={"name": "BTE"},
    )
    resp = client.get("/agents/BTE")
    assert resp.status_code == 200
    assert resp.json() == {"name": "BTE"}


def test_register_channel_requires_name(client, mocker):
    upsert = mocker.patch.object(ars, "upsert_channel", new_callable=mocker.AsyncMock)
    resp = client.post("/channels", json={})
    assert resp.status_code == 422
    upsert.assert_not_called()


def test_filter_endpoint(client, mocker):
    state = ["p1", None, None, None, None, None, None, "r1", None, "COMPLETED", "OK"]
    mocker.patch.object(
        ars, "get_query_state", new_callable=mocker.AsyncMock, return_value=state
    )
    mocker.patch.object(
        ars,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value={
            "message": {
                "knowledge_graph": {"nodes": {}, "edges": {}},
                "results": [
                    {"node_bindings": {"n0": [{"id": "A:1"}]}, "normalized_score": 90},
                    {"node_bindings": {"n0": [{"id": "B:2"}]}, "normalized_score": 10},
                ],
            }
        },
    )
    mocker.patch.object(ars, "save_message", new_callable=mocker.AsyncMock)
    resp = client.get("/filter/p1", params={"score": "[50, 100]"})
    assert resp.status_code == 200
    assert resp.json()["result_count"] == 1


def test_subscribe_rejects_bad_signature(client, mocker):
    mocker.patch.object(
        ars, "verify_post_signature", new_callable=mocker.AsyncMock, return_value=False
    )
    resp = client.post(
        "/query_event_subscribe", json={"client_id": "c1", "pks": ["p1"]}
    )
    assert resp.status_code == 401


def test_subscribe_succeeds_with_valid_signature(client, mocker):
    mocker.patch.object(
        ars, "verify_post_signature", new_callable=mocker.AsyncMock, return_value=True
    )
    mocker.patch.object(
        ars,
        "get_client",
        new_callable=mocker.AsyncMock,
        return_value={"callback_url": "http://cb", "subscriptions": []},
    )
    state = ["p1", None, None, None, None, None, None, "r1", None, "QUEUED", "OK"]
    mocker.patch.object(
        ars, "get_query_state", new_callable=mocker.AsyncMock, return_value=state
    )
    add = mocker.patch.object(ars, "add_subscriber", new_callable=mocker.AsyncMock)
    mocker.patch.object(
        ars, "set_client_subscriptions", new_callable=mocker.AsyncMock
    )
    resp = client.post(
        "/query_event_subscribe", json={"client_id": "c1", "pks": ["p1"]}
    )
    assert resp.status_code == 200
    assert resp.json()["success"] == ["p1"]
    add.assert_awaited_once()


def test_block_endpoint_derives_response(client, mocker):
    state = ["p1", None, None, None, None, None, None, "r1", None, "COMPLETED", "OK"]
    mocker.patch.object(
        ars, "get_query_state", new_callable=mocker.AsyncMock, return_value=state
    )
    mocker.patch.object(
        ars,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value={
            "message": {
                "knowledge_graph": {
                    "nodes": {"A:1": {}, "B:2": {}},
                    "edges": {"e": {"subject": "A:1", "object": "B:2"}},
                },
                "results": [],
            }
        },
    )
    mocker.patch.object(ars, "save_message", new_callable=mocker.AsyncMock)
    resp = client.post("/block/p1", json={"nodes": ["B:2"]})
    assert resp.status_code == 200
    assert "response_id" in resp.json()
