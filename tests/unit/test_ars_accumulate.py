"""Tests for the cross-ARA accumulator worker.

Covers the pure ``merge_child_into_parent`` merge plus the ``ars_accumulate``
gating logic: partial completion (no tail), full completion with the latch won
(tail launched) and lost (no duplicate launch), and the lock-failure discard.
"""

import json
import logging

import pytest

from workers.ars_accumulate.worker import (
    TAIL_WORKFLOW,
    ars_accumulate,
    merge_child_into_parent,
)

logger = logging.getLogger(__name__)


def _node(name="A"):
    return {"name": name, "categories": ["biolink:NamedThing"], "attributes": []}


def _msg(nodes=None, edges=None, results=None, aux=None):
    return {
        "message": {
            "query_graph": {"nodes": {}, "edges": {}},
            "knowledge_graph": {"nodes": nodes or {}, "edges": edges or {}},
            "results": results if results is not None else [],
            "auxiliary_graphs": aux or {},
        }
    }


# --- merge_child_into_parent ----------------------------------------------


def test_merge_child_into_parent_keeps_distinct_answers_and_merges_kg():
    parent = _msg(
        nodes={"n1": _node("A")},
        results=[{"node_bindings": {"n0": [{"id": "A:1"}]}, "analyses": []}],
    )
    child = _msg(
        nodes={"n2": _node("B")},
        results=[{"node_bindings": {"n0": [{"id": "B:2"}]}, "analyses": []}],
        aux={"aux1": {"edges": [], "attributes": []}},
    )
    merged, count = merge_child_into_parent(parent, child, "infores:shepherd", logger)
    msg = merged["message"]
    assert count == 1
    assert set(msg["knowledge_graph"]["nodes"]) == {"n1", "n2"}
    # Distinct answers stay separate.
    assert len(msg["results"]) == 2
    assert "aux1" in msg["auxiliary_graphs"]


def test_merge_child_into_parent_handles_missing_sections():
    """A child with null results/kg merges cleanly into an empty parent."""
    parent = _msg()
    child = {"message": {"results": None, "knowledge_graph": None}}
    merged, count = merge_child_into_parent(parent, child, "infores:shepherd", logger)
    assert count == 0
    assert merged["message"]["results"] == []


# --- ars_accumulate gating ------------------------------------------------


def _task(**overrides):
    payload = {
        "query_id": "parent-1",
        "response_id": "presp-1",
        "callback_id": "cb-1",
        "ara": "aragorn",
        "log_level": 20,
        "otel": "{}",
    }
    payload.update(overrides)
    return ("msg-id", payload)


def _patch_common(mocker, *, pending, claim=True, lock=True):
    """Patch the worker's db/broker dependencies. Returns the add_task mock.

    Every patched dependency is awaited inside the worker, so all are AsyncMocks.
    """

    def am(name, **kwargs):
        return mocker.patch(
            f"workers.ars_accumulate.worker.{name}",
            new_callable=mocker.AsyncMock,
            **kwargs,
        )

    am("acquire_lock", return_value=lock)
    am("remove_lock", return_value=None)
    am(
        "get_message",
        side_effect=lambda mid, log: _msg(nodes={"n": _node()}, results=[{"id": mid}]),
    )
    am("save_message", return_value=None)
    am("set_ars_child_status", return_value=None)
    am("get_pending_ars_children", return_value=pending)
    am("claim_ars_tail", return_value=claim)
    return am("add_task", return_value=None)


@pytest.mark.asyncio
async def test_accumulate_partial_does_not_launch_tail(mocker):
    add_task = _patch_common(mocker, pending=["bte", "sipr"])
    await ars_accumulate(_task(), logger)
    add_task.assert_not_called()


@pytest.mark.asyncio
async def test_accumulate_full_winner_launches_tail(mocker):
    add_task = _patch_common(mocker, pending=[], claim=True)
    await ars_accumulate(_task(), logger)
    add_task.assert_called_once()
    stream, payload = add_task.call_args.args[0], add_task.call_args.args[1]
    assert stream == "node_norm"
    assert payload["query_id"] == "parent-1"
    assert payload["response_id"] == "presp-1"
    assert json.loads(payload["workflow"]) == TAIL_WORKFLOW


@pytest.mark.asyncio
async def test_accumulate_full_loser_does_not_launch_tail(mocker):
    """All ARAs done but another caller already claimed the latch."""
    add_task = _patch_common(mocker, pending=[], claim=False)
    await ars_accumulate(_task(), logger)
    add_task.assert_not_called()


@pytest.mark.asyncio
async def test_accumulate_lock_failure_discards(mocker):
    add_task = _patch_common(mocker, pending=[], lock=False)
    save = mocker.patch(
        "workers.ars_accumulate.worker.save_message",
        new_callable=mocker.AsyncMock,
    )
    await ars_accumulate(_task(), logger)
    save.assert_not_called()
    add_task.assert_not_called()
