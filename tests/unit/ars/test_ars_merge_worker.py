"""Parity tests for the ars_merge worker.

Upstream reference: NCATSTranslator/Relay @ dd1e71b utils.py
merge_and_post_process + merge_received: lock, merge-child creation, fold
into the running merged_version, parent bookkeeping (merged_version,
merged_versions_list append, params.stats), the merged_version_begun
notification, then post-processing.
"""

import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from workers.ars_merge import worker as merge_worker

LOGGER = logging.getLogger(__name__)


def load_corpus(name):
    return json.loads(
        pathlib.Path(f"tests/fixtures/ars_corpus/{name}").read_text()
    )


@pytest.fixture
def env(mocker, redis_mock):
    parent_pk = uuid.uuid4()
    child_pk = uuid.uuid4()
    merge_pk = uuid.uuid4()
    parent = {
        "id": parent_pk, "status": "R", "code": 202, "actor": 1,
        "merged_version": None, "merged_versions_list": None,
        "params": {"query_type": "standard"}, "result_count": None,
    }

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    blobs = {}

    async def _save(pk, payload, logger):
        blobs[str(pk)] = payload

    async def _load(pk, logger):
        return blobs.get(str(pk))

    mocks = {
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "merge_pk": merge_pk,
        "parent": parent,
        "blobs": blobs,
        "get_message_row": _patch("get_message_row", return_value=parent),
        "create_message": _patch(
            "create_message",
            return_value={"id": merge_pk, "status": "R", "code": 202},
        ),
        "update_message": _patch(
            "update_message",
            side_effect=lambda pk, **kw: {"id": pk, **kw},
        ),
        "save_message_data": _patch("save_message_data", side_effect=_save),
        "load_message_data": _patch("load_message_data", side_effect=_load),
        "notify": mocker.patch.object(
            merge_worker, "notify_subscribers", new_callable=AsyncMock
        ),
        "ensure_ars_actor": mocker.patch.object(
            merge_worker.lifecycle, "ensure_ars_actor", new_callable=AsyncMock,
            return_value={"id": 3, "agent_name": "ars-ars-agent"},
        ),
        "try_lock": mocker.patch.object(
            merge_worker, "acquire_lock", new_callable=AsyncMock, return_value=True
        ),
        "remove_lock": mocker.patch.object(
            merge_worker, "remove_lock", new_callable=AsyncMock
        ),
        "run_merge": mocker.patch.object(
            merge_worker, "_run_merge_in_pool", new_callable=AsyncMock,
        ),
    }
    return mocks


def _task(env, agent="ara-aragorn"):
    return [
        "tid",
        {
            "parent_pk": str(env["parent_pk"]),
            "child_pk": str(env["child_pk"]),
            "agent_name": agent,
            "log_level": "20",
            "otel": "{}",
        },
    ]


async def test_first_merge(env, redis_mock):
    env["run_merge"].return_value = {"results": 2, "knowledge_graph_nodes": 4,
                                     "knowledge_graph_edges": 3,
                                     "auxiliary_graphs": 1, "query_graph": 2}
    await merge_worker.ars_merge(_task(env), LOGGER)

    # merge child created under the ars actor, ref = parent
    kw = env["create_message"].await_args.kwargs
    assert kw["actor_id"] == 3
    assert str(kw["ref"]) == str(env["parent_pk"])

    # the pool merge got no current merged pk (first merge)
    args = env["run_merge"].await_args.args
    assert args[0] is None
    assert str(args[1]) == str(env["child_pk"])
    assert str(args[2]) == str(env["merge_pk"])

    # parent bookkeeping in one update
    pupdate = next(
        c.kwargs for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["parent_pk"]) and "merged_version" in c.kwargs
    )
    assert str(pupdate["merged_version"]) == str(env["merge_pk"])
    assert pupdate["merged_versions_list"] == [[str(env["merge_pk"]), "ara-aragorn"]]
    assert pupdate["params"]["stats"]["results"] == 2

    # merged_version_begun notification with the updated list
    fields = env["notify"].await_args.args[1]
    assert fields["event_type"] == "merged_version_begun"
    assert fields["complete"] is False
    assert fields["merged_versions_list"] == [[str(env["merge_pk"]), "ara-aragorn"]]

    # postprocess wake task enqueued
    from shepherd_utils.broker import get_task

    t = await get_task("ars.postprocess", "consumer", "t", LOGGER)
    assert t is not None
    assert t[1]["merged_pk"] == str(env["merge_pk"])
    assert t[1]["agent_name"] == "ara-aragorn"

    env["remove_lock"].assert_awaited()


async def test_second_merge_uses_current(env, redis_mock):
    prev = uuid.uuid4()
    env["parent"]["merged_version"] = prev
    env["parent"]["merged_versions_list"] = [[str(prev), "ara-arax"]]
    env["run_merge"].return_value = {"results": 3}
    await merge_worker.ars_merge(_task(env), LOGGER)
    args = env["run_merge"].await_args.args
    assert str(args[0]) == str(prev)
    pupdate = next(
        c.kwargs for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["parent_pk"]) and "merged_version" in c.kwargs
    )
    assert pupdate["merged_versions_list"] == [
        [str(prev), "ara-arax"],
        [str(env["merge_pk"]), "ara-aragorn"],
    ]


async def test_lock_busy_reenqueues(env, redis_mock):
    env["try_lock"].return_value = False
    await merge_worker.ars_merge(_task(env), LOGGER)
    env["create_message"].assert_not_awaited()
    from shepherd_utils.broker import get_task

    t = await get_task("ars.merge", "consumer", "t", LOGGER)
    assert t is not None
    assert t[1]["child_pk"] == str(env["child_pk"])


async def test_merge_failure_leaves_merge_child_running(env, redis_mock):
    """Upstream merge_received swallows the failure and returns {}; the shell
    merge child stays Running (the 8-minute watchdog eventually 598s it)."""
    env["run_merge"].side_effect = RuntimeError("merge exploded")
    await merge_worker.ars_merge(_task(env), LOGGER)
    # no parent bookkeeping, no postprocess task
    parent_updates = [
        c for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["parent_pk"]) and "merged_version" in c.kwargs
    ]
    assert parent_updates == []
    from shepherd_utils.broker import get_task

    assert await get_task("ars.postprocess", "consumer", "t", LOGGER) is None
    env["remove_lock"].assert_awaited()


def test_merge_in_child_folds_messages(monkeypatch):
    """The pool-side merge against real (fake) redis blobs matches the
    golden-tested mergeMessages path."""
    import fakeredis

    sync_redis = fakeredis.FakeRedis()
    import shepherd_utils.db as shepherd_db

    monkeypatch.setattr(shepherd_db, "_sync_data_db_client", sync_redis)

    aragorn = load_corpus("response_aragorn.json")
    arax = load_corpus("response_arax.json")
    child_pk = "11111111-1111-1111-1111-111111111111"
    current_pk = "22222222-2222-2222-2222-222222222222"
    new_pk = "33333333-3333-3333-3333-333333333333"

    shepherd_db.save_message_sync(child_pk, aragorn)
    stats = merge_worker.merge_in_child(None, child_pk, current_pk)
    first = shepherd_db.get_message_sync(current_pk)
    assert first["message"]["results"] == aragorn["message"]["results"]
    assert stats["results"] == 2

    shepherd_db.save_message_sync(child_pk, arax)
    stats = merge_worker.merge_in_child(current_pk, child_pk, new_pk)
    merged = shepherd_db.get_message_sync(new_pk)
    assert stats["results"] == 3
    assert set(merged["message"]["knowledge_graph"]["edges"].keys()) == {
        "e1", "e2", "e3", "e9"
    }
