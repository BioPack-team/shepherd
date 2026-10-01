"""Tests for the ars_fanout worker.

The de-federated ARS fans a submitted query out to the Shepherd-hosted ARAs
(shepherd_utils/ars/aras.py) over the broker: one child message per enabled
ARA, the query record persisted as /{ara}/asyncquery would have, the ARA's
worker task enqueued with the handoff sentinel as its callback. What is kept
from the upstream port (NCATSTranslator/Relay @ 3e65975 tasks.py
send_message): the child is created R/202 with the parent's name/params and
stays that way on dispatch (an async accept saves nothing), and a dispatch
failure is the child's E/500 followed by the parent completion check.
"""

import json
import logging
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from shepherd_utils.ars import aras
from shepherd_utils.ars.handoff import handoff_callback_url
from shepherd_utils.config import settings
from shepherd_utils.logger import resolve_log_level
from workers.ars_fanout import worker as fanout

LOGGER = logging.getLogger(__name__)

QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:1"]}, "n1": {}},
            "edges": {"e": {"subject": "n1", "object": "n0"}},
        }
    }
}


@pytest.fixture
def env(mocker, redis_mock):
    parent_pk = uuid.uuid4()
    parent = {
        "id": parent_pk,
        "name": "",
        "code": 202,
        "status": "R",
        "agent": aras.DEFAULT_AGENT,
        "ref": None,
        "result_count": None,
        "params": {"query_type": "standard"},
        "merged_version": None,
        "merged_versions_list": None,
    }
    children = {}

    def _create(**kw):
        pk = uuid.uuid4()
        child = {
            "id": pk,
            "name": kw.get("name", ""),
            "code": 202,
            "status": "R",
            "agent": kw["agent"],
            "ref": kw.get("ref"),
            "result_count": None,
            "params": kw.get("params"),
        }
        children[kw["agent"]] = child
        return child

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    return {
        "parent_pk": parent_pk,
        "parent": parent,
        "children": children,
        "get_message_row": _patch("get_message_row", return_value=parent),
        "load_message_data": _patch(
            "load_message_data", return_value=json.loads(json.dumps(QUERY))
        ),
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
        "add_query": mocker.patch.object(
            fanout.shepherd_db, "add_query", new_callable=AsyncMock
        ),
    }


def _task(parent_pk):
    return [
        "tid",
        {
            "parent_pk": str(parent_pk),
            "query_id": str(parent_pk),
            "log_level": "20",
            "otel": '{"traceparent": "00-fan"}',
        },
    ]


async def _stream_tasks(stream):
    from shepherd_utils.broker import get_task

    tasks = []
    while True:
        t = await get_task(stream, "consumer", "t", LOGGER)
        if t is None:
            return tasks
        tasks.append(t)


async def test_fanout_creates_a_child_per_enabled_ara(env):
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)

    # one child per hosted ARA, recorded under the ARA's agent name, carrying
    # the parent's params and ref
    assert set(env["children"]) == {a.agent for a in aras.ARAS}
    for child in env["children"].values():
        assert child["params"] == {"query_type": "standard"}
        assert str(child["ref"]) == str(env["parent_pk"])

    # the query record /{ara}/asyncquery would have created, per ARA, with
    # the handoff sentinel as its callback
    assert env["add_query"].await_count == len(aras.ARAS)
    for call in env["add_query"].await_args_list:
        query_id, response_id, saved_query, callback_url = call.args[:4]
        ara = aras.by_name(call.kwargs["target"])
        assert ara is not None
        child = env["children"][ara.agent]
        assert callback_url == handoff_callback_url(child["id"])
        assert saved_query["callback"] == callback_url
        assert saved_query["message"] == QUERY["message"]

        # and the ARA's worker task on its own stream
        tasks = await _stream_tasks(ara.name)
        assert len(tasks) == 1
        fields = tasks[0][1]
        assert fields["query_id"] == query_id
        assert fields["response_id"] == response_id
        assert json.loads(fields["workflow"]) is None
        assert "otel" in fields

    # async dispatch: nothing saved, the children stay R/202 from creation
    env["update_message"].assert_not_awaited()
    env["completion"].assert_not_awaited()


async def test_fanout_honors_enabled_aras_setting(env, monkeypatch):
    monkeypatch.setattr(settings, "ars_enabled_aras", "arax, bte")
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    assert set(env["children"]) == {"ara-shepherd-arax", "ara-shepherd-bte"}
    assert await _stream_tasks("aragorn") == []
    assert len(await _stream_tasks("arax")) == 1
    assert len(await _stream_tasks("bte")) == 1


async def test_fanout_forwards_the_workflow(env):
    env["load_message_data"].return_value = dict(QUERY, workflow=[{"id": "lookup"}])
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    for ara in aras.ARAS:
        (task,) = await _stream_tasks(ara.name)
        assert json.loads(task[1]["workflow"]) == [{"id": "lookup"}]


@pytest.mark.parametrize(
    "query_extra, expected",
    [
        ({"parameters": {"log_level": "DEBUG"}}, logging.DEBUG),
        ({"parameters": {"log_level": "ERROR"}}, logging.ERROR),
        # 1.x top-level spelling: not read (submit rejects it anyway)
        ({"log_level": "DEBUG"}, None),
    ],
)
async def test_fanout_log_level_comes_from_parameters(env, query_extra, expected):
    """TRAPI 2.0: each ARA task's log level is the query's
    parameters.log_level, falling back to the configured level."""
    env["load_message_data"].return_value = dict(QUERY, **query_extra)
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    default = resolve_log_level(settings.log_level)
    for ara in aras.ARAS:
        (task,) = await _stream_tasks(ara.name)
        assert int(task[1]["log_level"]) == (
            expected if expected is not None else default
        )


async def test_fanout_does_not_mutate_the_parent_payload(env):
    """Each ARA gets its own copy of the query; the callback injected for
    one must not leak into the parent's stored body or another ARA's."""
    parent_data = env["load_message_data"].return_value
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    assert "callback" not in parent_data
    callbacks = {c.args[3] for c in env["add_query"].await_args_list}
    assert len(callbacks) == len(aras.ARAS)


async def test_fanout_dispatch_failure_is_e500(env):
    """A failed dispatch is the same shape as upstream's failed POST: the
    child goes E/500 and the completion check runs -- and only for that
    ARA; the others are still dispatched."""

    async def _add_query(query_id, response_id, data, callback, logger, target=None):
        if target == "arax":
            raise RuntimeError("datastore down")

    env["add_query"].side_effect = _add_query
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)

    failed = env["children"]["ara-shepherd-arax"]
    (update,) = env["update_message"].await_args_list
    assert str(update.args[0]) == str(failed["id"])
    assert update.kwargs == {"status": "E", "code": 500}
    env["persist_data_copy"].assert_awaited_once()
    env["completion"].assert_awaited_once()
    assert await _stream_tasks("arax") == []
    assert len(await _stream_tasks("aragorn")) == 1
    assert len(await _stream_tasks("bte")) == 1


async def test_fanout_with_no_enabled_aras_completes_the_parent(env, monkeypatch):
    """Nothing would ever answer, and parents are watchdog-exempt, so the
    completion check runs now and the query finishes as an empty result."""
    monkeypatch.setattr(settings, "ars_enabled_aras", "not-an-ara")
    await fanout.ars_fanout(_task(env["parent_pk"]), LOGGER)
    env["create_message"].assert_not_awaited()
    env["add_query"].assert_not_awaited()
    env["completion"].assert_awaited_once_with(env["parent"]["id"], LOGGER)


async def test_fanout_unknown_parent_is_a_noop(env):
    env["get_message_row"].return_value = None
    await fanout.ars_fanout(_task(uuid.uuid4()), LOGGER)
    env["create_message"].assert_not_awaited()
    env["add_query"].assert_not_awaited()
    env["completion"].assert_not_awaited()
