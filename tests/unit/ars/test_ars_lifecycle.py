"""Parity tests for parent-completion orchestration + notification building.

Upstream reference: NCATSTranslator/Relay @ 3e65975
  - signals.py message_post_save (completion transitions, empty-merge
    synthesis, unsubscribe timing)
  - models.py Message.notify_subscribers (status-override rules, stats)
  - tasks.py notify_subscribers_task (payload base, last_merged code forcing)
Behavior register rows: P-LC-1/2, P-NT-1..P-NT-5.
"""

import uuid
from unittest.mock import AsyncMock

import pytest

from shepherd_utils.ars.notify import build_notification
from shepherd_utils.ars import lifecycle

# ---------------------------------------------------------------------------
# build_notification: Message.notify_subscribers override rules
# ---------------------------------------------------------------------------


def _parent(status="R", code=202, result_count=None):
    return {
        "id": uuid.uuid4(),
        "status": status,
        "code": code,
        "result_count": result_count,
    }


def test_notification_running_parent_keeps_custom_fields():
    """P-NT-1: while the parent is 'R', custom event fields survive."""
    fields = {"event_type": "merged_version_begun", "complete": False}
    out = build_notification(_parent("R"), fields, data=None)
    assert out == fields


def test_notification_done_parent_overrides_with_admin():
    """P-NT-2: a 'D' parent always notifies admin/complete."""
    out = build_notification(_parent("D", 200), {"event_type": "custom"}, data=None)
    assert out == {"event_type": "admin", "complete": True}


def test_notification_error_parent_overrides_with_ars_error():
    out = build_notification(_parent("E", 500), None, data=None)
    assert out == {
        "event_type": "ars_error",
        "message": "ARS has run into an Error",
        "complete": True,
    }


def test_notification_stats_attached_when_result_count_set():
    """P-NT-3: stats = {results, auxiliary_graphs} when result_count is set."""
    data = {"message": {"auxiliary_graphs": {"a": {}, "b": {}}}}
    out = build_notification(_parent("D", 200, result_count=7), None, data=data)
    assert out["stats"] == {"results": 7, "auxiliary_graphs": 2}


def test_notification_stats_aux_count_defaults_zero():
    out = build_notification(_parent("D", 200, result_count=7), None, data=None)
    assert out["stats"] == {"results": 7, "auxiliary_graphs": 0}


# ---------------------------------------------------------------------------
# check_parent_completion orchestration
# ---------------------------------------------------------------------------


def _child(status, agent, code=200, result_count=None):
    return {
        "id": uuid.uuid4(),
        "status": status,
        "agent_name": agent,
        "code": code,
        "result_count": result_count,
    }


@pytest.fixture
def orchestration(mocker):
    """Patch the lifecycle module's collaborators; return the mock bundle."""
    parent_pk = uuid.uuid4()
    parent = {
        "id": parent_pk,
        "status": "R",
        "code": 202,
        "result_count": None,
        "merged_versions_list": [["m1", "ara-aragorn"]],
        "params": {"query_type": "standard"},
    }
    mocks = {
        "parent_pk": parent_pk,
        "parent": parent,
        "get_message_row": mocker.patch.object(
            lifecycle.ars_db,
            "get_message_row",
            new_callable=AsyncMock,
            return_value=parent,
        ),
        "get_children": mocker.patch.object(
            lifecycle.ars_db,
            "get_children",
            new_callable=AsyncMock,
            return_value=[],
        ),
        "update_message": mocker.patch.object(
            lifecycle.ars_db,
            "update_message",
            new_callable=AsyncMock,
            side_effect=lambda pk, **kw: {**parent, **kw, "id": pk},
        ),
        "claim": mocker.patch.object(
            lifecycle.ars_db,
            "claim_terminal_transition",
            new_callable=AsyncMock,
            side_effect=lambda pk, status, code, **kw: {
                **parent,
                **kw,
                "id": pk,
                "status": status,
                "code": code,
            },
        ),
        "delete_message": mocker.patch.object(
            lifecycle.ars_db,
            "delete_message",
            new_callable=AsyncMock,
        ),
        "create_message": mocker.patch.object(
            lifecycle.ars_db,
            "create_message",
            new_callable=AsyncMock,
            return_value={"id": uuid.uuid4(), "status": "R", "code": 202},
        ),
        "save_message_data": mocker.patch.object(
            lifecycle.ars_db,
            "save_message_data",
            new_callable=AsyncMock,
        ),
        "load_message_data": mocker.patch.object(
            lifecycle.ars_db,
            "load_message_data",
            new_callable=AsyncMock,
            return_value={
                "message": {
                    "query_graph": {"nodes": {}, "edges": {}},
                    "knowledge_graph": {"nodes": {"n": {}}, "edges": {}},
                    "results": [{"x": 1}],
                    "auxiliary_graphs": {"a": {}},
                }
            },
        ),
        "persist_data_copy": mocker.patch.object(
            lifecycle.ars_db,
            "persist_data_copy",
            new_callable=AsyncMock,
        ),
        "clear_subscriptions": mocker.patch.object(
            lifecycle.ars_db,
            "clear_subscriptions",
            new_callable=AsyncMock,
        ),
        "ensure_ars_actor": mocker.patch.object(
            lifecycle,
            "ensure_ars_actor",
            new_callable=AsyncMock,
            return_value={"id": 42, "agent_name": "ars-ars-agent"},
        ),
        "notify": mocker.patch.object(
            lifecycle,
            "notify_subscribers",
            new_callable=AsyncMock,
        ),
    }
    return mocks


import logging

LOGGER = logging.getLogger(__name__)


async def test_completion_noop_while_child_running(orchestration):
    orchestration["get_children"].return_value = [
        _child("R", "ara-aragorn"),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    orchestration["claim"].assert_not_awaited()
    orchestration["update_message"].assert_not_awaited()
    orchestration["notify"].assert_not_awaited()


async def test_completion_noop_when_counts_mismatch(orchestration):
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=5),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    orchestration["claim"].assert_not_awaited()
    orchestration["update_message"].assert_not_awaited()


async def test_completion_already_done_parent_skips(orchestration):
    orchestration["parent"]["status"] = "D"
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    orchestration["get_children"].assert_not_awaited()


async def test_completion_nonempty(orchestration):
    """P-LC-1: last_merged_completed (parent still R), parent -> D/200,
    admin/complete, subscriptions cleared."""
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=5),
        _child("D", "ars-ars-agent", result_count=5),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)

    # parent flipped to Done/200 through the atomic claim
    orchestration["claim"].assert_awaited_once()
    claim_call = orchestration["claim"].await_args
    assert claim_call.args[1:] == ("D", 200)

    # two notifications: last_merged_completed first (parent still 'R' so
    # custom fields survive), then the save-time admin notification
    notify_calls = orchestration["notify"].await_args_list
    assert len(notify_calls) == 2
    first_fields = notify_calls[0].args[1]
    assert first_fields["event_type"] == "last_merged_completed"
    assert first_fields["complete"] is True
    assert first_fields["merged_versions_list"] == [["m1", "ara-aragorn"]]
    # first notification is built against the pre-update (Running) parent
    assert notify_calls[0].args[0]["status"] == "R"
    # second is against the updated Done parent
    assert notify_calls[1].args[0]["status"] == "D"

    orchestration["clear_subscriptions"].assert_awaited_once()


async def test_completion_empty_synthesizes_merged_message(orchestration):
    """P-LC-2: zero counts -> an empty merged message is created from the
    parent's data with results/kg/aux emptied; parent D/200; NO unsubscribe
    (upstream's empty branch doesn't clear subscriptions)."""
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=0),
        _child("E", "ara-arax", code=598),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)

    # empty merged message created under the ars actor
    orchestration["create_message"].assert_awaited_once()
    create_kwargs = orchestration["create_message"].await_args.kwargs
    assert create_kwargs.get("actor_id") == 42

    # its payload is the parent's data with results/aux/kg emptied
    saved_pk, saved_payload = orchestration["save_message_data"].await_args.args[:2]
    assert saved_payload["message"]["results"] == []
    assert saved_payload["message"]["auxiliary_graphs"] == {}
    assert saved_payload["message"]["knowledge_graph"] == {"nodes": {}, "edges": {}}

    # the parent goes Done and gains its merged_version in ONE claim, so no
    # reader can catch it 'D' with nothing merged
    orchestration["claim"].assert_awaited_once()
    claim_call = orchestration["claim"].await_args
    assert claim_call.args[1:] == ("D", 200)
    assert claim_call.kwargs.get("merged_version") is not None
    mvl = claim_call.kwargs.get("merged_versions_list")
    assert mvl is not None and mvl[0][1] == "ars"

    # only the save-time admin notification; no unsubscribe in the empty path
    assert len(orchestration["notify"].await_args_list) == 1
    orchestration["clear_subscriptions"].assert_not_awaited()


async def test_completion_lost_claim_does_not_renotify(orchestration):
    """Two callers can evaluate the same complete decision at once; only the
    one that wins the conditional UPDATE emits the completion events."""
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=5),
        _child("D", "ars-ars-agent", result_count=5),
    ]
    orchestration["claim"].side_effect = None
    orchestration["claim"].return_value = None
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    orchestration["claim"].assert_awaited_once()
    orchestration["notify"].assert_not_awaited()
    orchestration["clear_subscriptions"].assert_not_awaited()


async def test_completion_lost_claim_discards_empty_merge(orchestration):
    """The empty branch builds its merged message before claiming, so a
    caller that loses the claim has to drop what it just built."""
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=0),
        _child("E", "ara-arax", code=598),
    ]
    orchestration["claim"].side_effect = None
    orchestration["claim"].return_value = None
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    empty_pk = orchestration["create_message"].await_args.kwargs.get("ref")
    assert empty_pk is not None
    orchestration["delete_message"].assert_awaited_once()
    orchestration["notify"].assert_not_awaited()


async def test_parent_error_unsubscribes(orchestration):
    """The elif branch: an 'E' parent with non-parity children clears subs."""
    orchestration["parent"]["status"] = "E"
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=5),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    orchestration["clear_subscriptions"].assert_awaited_once()
    orchestration["claim"].assert_not_awaited()
    orchestration["update_message"].assert_not_awaited()


# ---------------------------------------------------------------------------
# replay_completion: late subscribers to a finished (cached) query
# ---------------------------------------------------------------------------

import json as _json  # noqa: E402

from shepherd_utils.ars import notify as notify_mod  # noqa: E402


@pytest.fixture
def replay_env(mocker):
    tasks = []

    async def fake_add_task(stream, payload, logger, **kw):
        tasks.append((stream, payload))

    mocker.patch.object(notify_mod, "add_task", fake_add_task)
    mocker.patch.object(
        notify_mod.ars_db,
        "load_otel_carrier",
        new_callable=AsyncMock,
        return_value="{}",
    )
    mocker.patch.object(
        notify_mod.ars_db,
        "get_subscribed_clients",
        new_callable=AsyncMock,
        return_value=[{"id": 3}, {"id": 4}],
    )
    return tasks


def _done_parent(merged_versions_list, result_count=5):
    return {
        "id": uuid.uuid4(),
        "ref": None,
        "status": "D",
        "code": 200,
        "result_count": result_count,
        "merged_versions_list": merged_versions_list,
    }


async def test_replay_done_parent_emits_last_merged_then_admin(replay_env):
    mvl = [["m1", "ara-aragorn"], ["m2", "ara-arax"]]
    parent = _done_parent(mvl)
    await notify_mod.replay_completion(parent, 7, LOGGER)
    assert [t[0] for t in replay_env] == ["ars.notify", "ars.notify"]
    first, second = (_json.loads(t[1]["fields"]) for t in replay_env)
    assert first["event_type"] == "last_merged_completed"
    assert first["complete"] is True
    assert first["merged_versions_list"] == mvl
    assert first["stats"]["results"] == 5
    assert second == {
        "event_type": "admin",
        "complete": True,
        "stats": {"results": 5, "auxiliary_graphs": 0},
    }
    assert all(_json.loads(t[1]["client_pks"]) == ["7"] for t in replay_env)
    assert all(t[1]["message_pk"] == str(parent["id"]) for t in replay_env)


async def test_replay_empty_completion_emits_only_admin(replay_env):
    """The empty-completion branch records [[pk, "ars"]] and upstream never
    emitted last_merged_completed for it."""
    parent = _done_parent([["m0", "ars"]], result_count=None)
    await notify_mod.replay_completion(parent, 7, LOGGER)
    assert len(replay_env) == 1
    assert _json.loads(replay_env[0][1]["fields"]) == {
        "event_type": "admin",
        "complete": True,
    }


async def test_replay_error_parent_emits_ars_error(replay_env):
    parent = dict(_done_parent([["m1", "ara-aragorn"]]), status="E", code=500)
    await notify_mod.replay_completion(parent, 7, LOGGER)
    assert len(replay_env) == 1
    fields = _json.loads(replay_env[0][1]["fields"])
    assert fields["event_type"] == "ars_error"
    assert replay_env[0][1]["code"] == "500"


async def test_replay_child_message_emits_save_time_event_only(replay_env):
    child = dict(_done_parent([["m1", "ara-aragorn"]]), ref=uuid.uuid4())
    await notify_mod.replay_completion(child, 7, LOGGER)
    assert len(replay_env) == 1
    assert _json.loads(replay_env[0][1]["fields"])["event_type"] == "admin"


async def test_live_notify_resolves_recipients_at_emit_time(replay_env):
    """The completion path clears subscriptions right after emitting, so the
    recipients must be captured now, not when the worker runs."""
    await notify_mod.notify_subscribers(_done_parent([]), None, LOGGER)
    assert _json.loads(replay_env[0][1]["client_pks"]) == ["3", "4"]


async def test_live_notify_without_resolvable_subscribers_still_enqueues(
    replay_env, mocker
):
    mocker.patch.object(
        notify_mod.ars_db,
        "get_subscribed_clients",
        new_callable=AsyncMock,
        side_effect=RuntimeError("pg down"),
    )
    await notify_mod.notify_subscribers(_done_parent([]), None, LOGGER)
    assert len(replay_env) == 1
    assert "client_pks" not in replay_env[0][1]  # worker falls back to the list
