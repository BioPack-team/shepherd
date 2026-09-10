"""Parity tests for the ars_fanout worker.

Upstream reference: NCATSTranslator/Relay @ 3e65975
  - signals.py message_post_save (actor matching on channel intersection)
  - pubsub.py send_messages (skip rules)
  - tasks.py send_message (child creation, callback injection, the response
    state machine: async-200 leaves the child Running with nothing saved,
    sync-200 processes inline, 202 records an aresponse poll URL, >=400 is an
    error except 503 which leaves the child Unknown, exceptions are E/500)
"""

import json
import logging
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from workers.ars_fanout import worker as fanout

LOGGER = logging.getLogger(__name__)


_CHANNEL_PKS = {"general": 1, "workflow": 2}


def serialized_channels(*names):
    """Consistent serialized-channel dicts (upstream matches actors by dict
    equality of these entries, which works because they all come from the
    same channel table)."""
    return [
        {
            "model": "tr_ars.channel",
            "pk": _CHANNEL_PKS[n],
            "fields": {"name": n, "description": None},
        }
        for n in names
    ]


def actor_row(
    actor_id,
    agent,
    inforesid,
    channels=("general",),
    path="runquery",
    active=True,
    uri=None,
):
    return {
        "id": actor_id,
        "agent": actor_id,
        "channel": serialized_channels(*channels),
        "path": path,
        "inforesid": inforesid,
        "active": active,
        "agent_name": agent,
        "agent_uri": uri if uri is not None else f"/{agent}/api/",
    }


QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:1"]}, "n1": {}},
            "edges": {"e": {"subject": "n1", "object": "n0"}},
        }
    }
}


@pytest.fixture
def env(mocker):
    parent_pk = uuid.uuid4()
    parent_actor = actor_row(1, "ars-default-agent", "", ("general",), path="", uri="")
    parent = {
        "id": parent_pk,
        "name": "",
        "code": 202,
        "status": "R",
        "actor": 1,
        "ref": None,
        "result_count": None,
        "params": {"query_type": "standard"},
        "merged_version": None,
        "merged_versions_list": None,
    }
    actors = [
        parent_actor,  # self: skipped (empty path/uri anyway)
        actor_row(7, "ara-aragorn", "infores:aragorn", ("general", "workflow")),
        actor_row(8, "ara-improving", "infores:improving-agent", ("general",)),
        actor_row(9, "ara-wfr", "infores:workflow-runner", ("workflow",)),
        actor_row(10, "ara-off", "infores:off", ("general",), active=False),
        actor_row(11, "kp-genetics", "infores:genetics-data-provider", ("general",)),
    ]
    children = {}

    def _create(**kw):
        pk = uuid.uuid4()
        child = {
            "id": pk,
            "name": kw.get("name", ""),
            "code": 202,
            "status": "R",
            "actor": kw["actor_id"],
            "ref": kw.get("ref"),
            "result_count": None,
            "params": kw.get("params"),
        }
        children[kw["actor_id"]] = child
        return child

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    mocks = {
        "parent_pk": parent_pk,
        "children": children,
        "get_message_row": _patch("get_message_row", return_value=parent),
        "get_actor": _patch("get_actor", return_value=parent_actor),
        "list_actors": _patch("list_actors", return_value=actors),
        "load_message_data": _patch("load_message_data", return_value=QUERY),
        "create_message": _patch("create_message", side_effect=_create),
        "update_message": _patch(
            "update_message",
            side_effect=lambda pk, **kw: {"id": pk, **kw},
        ),
        "save_message_data": _patch("save_message_data"),
        "persist_data_copy": _patch("persist_data_copy"),
        "completion": mocker.patch.object(
            fanout.lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
        "url_remote": mocker.patch.object(
            fanout.smartapi,
            "url_remote_from_inforesid",
            side_effect=lambda i: f"https://remote.example/{i}/asyncquery",
        ),
        "endpoint": mocker.patch.object(
            fanout.smartapi, "endpoint", return_value="asyncquery"
        ),
    }
    mocks["parent"] = parent
    mocks["actors"] = actors
    return mocks


def _task(parent_pk):
    return [
        "tid",
        {
            "parent_pk": str(parent_pk),
            "log_level": "20",
            "otel": '{"traceparent": "00-fan"}',
        },
    ]


def _accepted(
    url="https://remote.example/x",
    status_code=200,
    body=None,
    text_body="",
    headers=None,
):
    import httpx

    return httpx.Response(
        status_code=status_code,
        json=body if body is not None else {"status": "Accepted"},
        headers=headers or {},
        request=httpx.Request("POST", url),
    )


async def test_fanout_creates_children_for_matching_actors(env, mocker, redis_mock):
    post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=_accepted(),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    # general-channel parent: aragorn (general+workflow), improving, and the
    # KP match; wfr (workflow-only), inactive, and self do not
    created_for = set(env["children"].keys())
    assert created_for == {7, 8, 11}
    assert post.await_count == 3
    # children carry parent's params and ref
    child = env["children"][7]
    assert child["params"] == {"query_type": "standard"}
    assert str(child["ref"]) == str(env["parent_pk"])


async def test_fanout_async_accept_leaves_child_running(env, mocker, redis_mock):
    mocker.patch(
        "httpx.AsyncClient.post", new_callable=AsyncMock, return_value=_accepted()
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    # async 200-Accepted: upstream saves nothing -- the child stays R/202
    env["update_message"].assert_not_awaited()
    env["completion"].assert_not_awaited()


async def test_fanout_injects_callback_url(env, mocker, redis_mock):
    post = mocker.patch(
        "httpx.AsyncClient.post", new_callable=AsyncMock, return_value=_accepted()
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for call in post.await_args_list:
        body = call.kwargs["json"]
        child_pk = body["callback"].rsplit("/", 1)[-1]
        assert body["callback"].endswith(f"/ars/api/messages/{child_pk}")
        assert "/ars/api/messages/" in body["callback"]
        assert body["message"] == QUERY["message"]


async def test_fanout_sync_endpoint_processes_inline(env, mocker, redis_mock):
    """query-endpoint actors are called synchronously; results are premerged,
    the child goes D/200 with counts, and an ars.merge task is enqueued for
    ara- agents."""
    import pathlib

    valid = json.loads(
        pathlib.Path("tests/fixtures/ars_corpus/response_aragorn.json").read_text()
    )
    env["endpoint"].side_effect = lambda i: "query"
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=_accepted(body=valid),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)

    updates = {str(c.args[0]): c.kwargs for c in env["update_message"].await_args_list}
    ara_child = env["children"][7]
    up = updates[str(ara_child["id"])]
    assert up["status"] == "D"
    assert up["code"] == 200
    assert up["result_count"] == 2
    env["completion"].assert_awaited()

    from shepherd_utils.broker import get_task

    merge_tasks = []
    while True:
        t = await get_task("ars.merge", "consumer", "t", LOGGER)
        if t is None:
            break
        merge_tasks.append(t)
    # only the two ara- agents enqueue merges; the KP result does not
    agents = {t[1]["agent_name"] for t in merge_tasks}
    assert agents == {"ara-aragorn", "ara-improving"}
    # the fanout task's otel context is forwarded so the merge stays in the
    # query's trace
    assert all(t[1]["otel"] == '{"traceparent": "00-fan"}' for t in merge_tasks)


async def test_fanout_sync_empty_results_done_without_merge(env, mocker, redis_mock):
    env["endpoint"].side_effect = lambda i: "query"
    empty = {
        "message": {
            "query_graph": {"nodes": {}, "edges": {}},
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
        }
    }
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=_accepted(body=empty),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for call in env["update_message"].await_args_list:
        assert call.kwargs["status"] == "D"
        assert "result_count" not in call.kwargs
    from shepherd_utils.broker import get_task

    assert await get_task("ars.merge", "consumer", "t", LOGGER) is None
    env["completion"].assert_awaited()


async def test_fanout_http_error_marks_child_errored(env, mocker, redis_mock):
    import httpx

    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=httpx.Response(
            status_code=500,
            text="boom",
            request=httpx.Request("POST", "https://remote.example/x"),
        ),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for call in env["update_message"].await_args_list:
        assert call.kwargs["status"] == "E"
        assert call.kwargs["code"] == 500
    env["completion"].assert_awaited()
    # error text lands in the saved payload's logs
    saved = env["save_message_data"].await_args_list[0].args[1]
    assert any("boom" in str(entry) for entry in saved.get("logs", []))


async def test_fanout_503_leaves_child_unknown(env, mocker, redis_mock):
    import httpx

    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=httpx.Response(
            status_code=503,
            text="unavailable",
            request=httpx.Request("POST", "https://remote.example/x"),
        ),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for call in env["update_message"].await_args_list:
        assert call.kwargs["status"] == "U"
        assert call.kwargs["code"] == 503
    # 'U' is terminal -> the completion check still runs
    env["completion"].assert_awaited()


async def test_fanout_202_records_aresponse_url(env, mocker, redis_mock):
    import httpx

    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=httpx.Response(
            status_code=202,
            text="job-123",
            request=httpx.Request(
                "POST", "https://remote.example/infores:aragorn/asyncquery"
            ),
        ),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for call in env["update_message"].await_args_list:
        assert call.kwargs["status"] == "R"
        assert call.kwargs["url"].endswith("/aresponse/job-123")
    env["completion"].assert_not_awaited()


async def test_fanout_connect_error_is_e500(env, mocker, redis_mock):
    import httpx

    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("nope"),
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for call in env["update_message"].await_args_list:
        assert call.kwargs["status"] == "E"
        assert call.kwargs["code"] == 500
    env["completion"].assert_awaited()


# ---------------------------------------------------------------------------
# internal dispatch (Shepherd-hosted ARAs; documented deviation)
# ---------------------------------------------------------------------------


def _internal_only(env, mocker):
    """Point the registry at one Shepherd-hosted ARA and arm the intake mocks."""
    env["list_actors"].return_value = [
        env["actors"][0],  # parent/self
        actor_row(12, "ara-shepherd-arax", "infores:shepherd-arax", ("general",)),
    ]
    return {
        "add_query": mocker.patch.object(
            fanout.shepherd_db, "add_query", new_callable=AsyncMock
        ),
        "post": mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock),
    }


async def _stream_tasks(stream):
    from shepherd_utils.broker import get_task

    tasks = []
    while True:
        t = await get_task(stream, "consumer", "t", LOGGER)
        if t is None:
            return tasks
        tasks.append(t)


async def test_fanout_internal_actor_enqueues_ara_task(env, mocker, redis_mock):
    """A Shepherd-hosted ARA gets its worker task enqueued directly -- no
    HTTP POST -- with the same query record the /asyncquery endpoint would
    have created, and a sentinel callback that routes the response back
    through ars.premerge. The child stays R/202 exactly like an async
    accept."""
    from shepherd_utils.ars.internal import internal_callback_url

    mocks = _internal_only(env, mocker)
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)

    mocks["post"].assert_not_awaited()
    child = env["children"][12]
    sentinel = internal_callback_url(child["id"])

    mocks["add_query"].assert_awaited_once()
    call = mocks["add_query"].await_args
    query_id, response_id, saved_query, callback_url = call.args[:4]
    assert call.kwargs["target"] == "arax"
    assert callback_url == sentinel
    assert saved_query["callback"] == sentinel
    assert saved_query["message"] == QUERY["message"]

    tasks = await _stream_tasks("arax")
    assert len(tasks) == 1
    fields = tasks[0][1]
    assert fields["query_id"] == query_id
    assert fields["response_id"] == response_id
    assert json.loads(fields["workflow"]) is None
    assert "otel" in fields

    # async dispatch: nothing saved, child stays R/202 from creation
    env["update_message"].assert_not_awaited()
    env["completion"].assert_not_awaited()


async def test_fanout_internal_enqueue_failure_is_e500(env, mocker, redis_mock):
    """A failed internal dispatch is the same shape as a failed POST: the
    child goes E/500 and the completion check runs."""
    mocks = _internal_only(env, mocker)
    mocks["add_query"].side_effect = RuntimeError("datastore down")
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    call = env["update_message"].await_args
    assert call.kwargs["status"] == "E"
    assert call.kwargs["code"] == 500
    env["completion"].assert_awaited()
    assert await _stream_tasks("arax") == []


async def test_fanout_internal_dispatch_disabled_uses_http(env, mocker, redis_mock):
    """With ars_internal_dispatch off, Shepherd-hosted ARAs go over HTTP
    like any external actor."""
    from shepherd_utils.config import settings

    mocks = _internal_only(env, mocker)
    mocker.patch.object(settings, "ars_internal_dispatch", False)
    mocks["post"].return_value = _accepted()
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    mocks["post"].assert_awaited_once()
    mocks["add_query"].assert_not_awaited()


async def test_fanout_workflow_parent_matches_workflow_channel(env, mocker, redis_mock):
    env["get_actor"].return_value = actor_row(
        2, "ars-workflow-agent", "", ("workflow",), path="", uri=""
    )
    mocker.patch(
        "httpx.AsyncClient.post", new_callable=AsyncMock, return_value=_accepted()
    )
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    assert set(env["children"].keys()) == {7, 9}
