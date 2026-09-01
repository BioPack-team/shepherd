"""Parity tests for parent-completion orchestration + notification building.

Upstream reference: NCATSTranslator/Relay @ dd1e71b
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
    out = build_notification(
        _parent("D", 200), {"event_type": "custom"}, data=None
    )
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
            lifecycle.ars_db, "get_message_row", new_callable=AsyncMock,
            return_value=parent,
        ),
        "get_children": mocker.patch.object(
            lifecycle.ars_db, "get_children", new_callable=AsyncMock,
            return_value=[],
        ),
        "update_message": mocker.patch.object(
            lifecycle.ars_db, "update_message", new_callable=AsyncMock,
            side_effect=lambda pk, **kw: {**parent, **kw, "id": pk},
        ),
        "create_message": mocker.patch.object(
            lifecycle.ars_db, "create_message", new_callable=AsyncMock,
            return_value={"id": uuid.uuid4(), "status": "R", "code": 202},
        ),
        "save_message_data": mocker.patch.object(
            lifecycle.ars_db, "save_message_data", new_callable=AsyncMock,
        ),
        "load_message_data": mocker.patch.object(
            lifecycle.ars_db, "load_message_data", new_callable=AsyncMock,
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
            lifecycle.ars_db, "persist_data_copy", new_callable=AsyncMock,
        ),
        "clear_subscriptions": mocker.patch.object(
            lifecycle.ars_db, "clear_subscriptions", new_callable=AsyncMock,
        ),
        "ensure_ars_actor": mocker.patch.object(
            lifecycle, "ensure_ars_actor", new_callable=AsyncMock,
            return_value={"id": 42, "agent_name": "ars-ars-agent"},
        ),
        "notify": mocker.patch.object(
            lifecycle, "notify_subscribers", new_callable=AsyncMock,
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
    orchestration["update_message"].assert_not_awaited()
    orchestration["notify"].assert_not_awaited()


async def test_completion_noop_when_counts_mismatch(orchestration):
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=5),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
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

    # parent flipped to Done/200
    update_call = orchestration["update_message"].await_args_list[0]
    assert update_call.kwargs.get("status") == "D"

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

    # parent got merged_version + merged_versions_list [(pk, "ars")]
    parent_updates = [
        c.kwargs for c in orchestration["update_message"].await_args_list
        if c.args and c.args[0] == orchestration["parent_pk"]
    ]
    final = parent_updates[-1]
    assert final.get("status") == "D"
    mvl = final.get("merged_versions_list")
    assert mvl is not None and mvl[0][1] == "ars"

    # only the save-time admin notification; no unsubscribe in the empty path
    assert len(orchestration["notify"].await_args_list) == 1
    orchestration["clear_subscriptions"].assert_not_awaited()


async def test_parent_error_unsubscribes(orchestration):
    """The elif branch: an 'E' parent with non-parity children clears subs."""
    orchestration["parent"]["status"] = "E"
    orchestration["get_children"].return_value = [
        _child("D", "ara-aragorn", result_count=5),
    ]
    await lifecycle.check_parent_completion(orchestration["parent_pk"], LOGGER)
    orchestration["clear_subscriptions"].assert_awaited_once()
    orchestration["update_message"].assert_not_awaited()
