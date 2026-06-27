"""Tests for the ARS timeout watchdog (in the ars worker)."""

import json
import logging

import pytest

from workers.ars import worker as ars_worker
from shepherd_utils.ars_workflow import ARS_TAIL_WORKFLOW

logger = logging.getLogger(__name__)


def _patch(mocker, *, timed_out, claim=True):
    mocker.patch.object(
        ars_worker,
        "get_timed_out_ars_parents",
        new_callable=mocker.AsyncMock,
        return_value=timed_out,
    )
    errored = mocker.patch.object(
        ars_worker, "mark_ars_children_errored", new_callable=mocker.AsyncMock
    )
    mocker.patch.object(
        ars_worker, "claim_ars_tail", new_callable=mocker.AsyncMock, return_value=claim
    )
    mocker.patch.object(
        ars_worker, "publish_ars_event", new_callable=mocker.AsyncMock
    )
    add_task = mocker.patch.object(
        ars_worker, "add_task", new_callable=mocker.AsyncMock
    )
    return errored, add_task


@pytest.mark.asyncio
async def test_watchdog_forces_tail_for_timed_out_parent(mocker):
    errored, add_task = _patch(
        mocker,
        timed_out=[
            {"qid": "p1", "response_id": "r1", "pending": ["bte", "sipr"]},
        ],
        claim=True,
    )
    await ars_worker.run_watchdog_once(logger)

    errored.assert_awaited_once_with("p1", ["bte", "sipr"], logger)
    add_task.assert_awaited_once()
    stream, payload = add_task.await_args.args[0], add_task.await_args.args[1]
    assert stream == "node_norm"
    assert payload["query_id"] == "p1"
    assert payload["response_id"] == "r1"
    assert json.loads(payload["workflow"]) == ARS_TAIL_WORKFLOW


@pytest.mark.asyncio
async def test_watchdog_skips_tail_when_latch_lost(mocker):
    """A parent already merging (latch lost) still errors children but does not
    re-launch the tail."""
    errored, add_task = _patch(
        mocker,
        timed_out=[{"qid": "p2", "response_id": "r2", "pending": ["arax"]}],
        claim=False,
    )
    await ars_worker.run_watchdog_once(logger)
    errored.assert_awaited_once()
    add_task.assert_not_called()


@pytest.mark.asyncio
async def test_watchdog_noop_when_nothing_timed_out(mocker):
    errored, add_task = _patch(mocker, timed_out=[])
    await ars_worker.run_watchdog_once(logger)
    errored.assert_not_called()
    add_task.assert_not_called()
