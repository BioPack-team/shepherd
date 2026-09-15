"""Parity tests for the ars_watchdog timeout sweep.

Upstream reference: NCATSTranslator/Relay @ 3e65975 tasks.py
catch_timeout_async: exempt parents; merge messages (ars-ars-agent) time out
after 8 minutes; everything else after 5 (the pathfinder log line says 10 but
the code compares against now-5min); timed-out messages get code 598 /
status E.

Upstream additionally scanned only messages created in the last 15 minutes.
That ceiling is off by default here -- it made a message that outlived the
window permanently unreapable -- so the sweep is bounded by a row limit
instead (see get_running_messages and ars_timeout_scan_window_sec).
"""

import datetime
import logging
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from shepherd_utils.config import settings
from workers.ars_watchdog import worker as watchdog

LOGGER = logging.getLogger(__name__)
UTC = datetime.timezone.utc


def running(agent, age_sec, query_type="standard", ref=None):
    return {
        "id": uuid.uuid4(),
        "ts": datetime.datetime.now(UTC) - datetime.timedelta(seconds=age_sec),
        "params": {"query_type": query_type},
        "agent_name": agent,
        "ref": ref or uuid.uuid4(),
    }


@pytest.fixture
def env(mocker):
    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    return {
        "get_running_messages": _patch("get_running_messages", return_value=[]),
        "update_message": _patch("update_message"),
        "completion": mocker.patch.object(
            watchdog.lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
    }


async def test_standard_over_5min_times_out(env):
    row = running("ara-aragorn", 360)
    env["get_running_messages"].return_value = [row]
    await watchdog.sweep(LOGGER)
    call = env["update_message"].await_args
    assert str(call.args[0]) == str(row["id"])
    assert call.kwargs["code"] == 598
    assert call.kwargs["status"] == "E"
    env["completion"].assert_awaited_once()


async def test_standard_under_5min_untouched(env):
    env["get_running_messages"].return_value = [running("ara-aragorn", 200)]
    await watchdog.sweep(LOGGER)
    env["update_message"].assert_not_awaited()


async def test_pathfinder_times_out_at_5min_code_parity(env):
    """The upstream code (not its log message) uses 5 minutes."""
    env["get_running_messages"].return_value = [
        running("ara-aragorn", 360, query_type="pathfinder")
    ]
    await watchdog.sweep(LOGGER)
    assert env["update_message"].await_args.kwargs["code"] == 598


async def test_merge_child_8min_threshold(env):
    young_merge = running("ars-ars-agent", 400)
    old_merge = running("ars-ars-agent", 500)
    env["get_running_messages"].return_value = [young_merge, old_merge]
    await watchdog.sweep(LOGGER)
    assert env["update_message"].await_count == 1
    assert str(env["update_message"].await_args.args[0]) == str(old_merge["id"])


async def test_parents_exempt(env):
    env["get_running_messages"].return_value = [
        running("ars-default-agent", 4000),
        running("ars-workflow-agent", 4000),
    ]
    await watchdog.sweep(LOGGER)
    env["update_message"].assert_not_awaited()


async def test_kp_child_times_out_like_standard(env):
    env["get_running_messages"].return_value = [running("kp-genetics", 360)]
    await watchdog.sweep(LOGGER)
    assert env["update_message"].await_args.kwargs["code"] == 598


# ---------------------------------------------------------------------------
# scan bounds
# ---------------------------------------------------------------------------


async def test_sweep_queries_without_a_creation_ceiling_by_default(env, monkeypatch):
    """The upper bound is off, and the query still skips rows too young to
    have tripped any threshold."""
    monkeypatch.setattr(settings, "ars_timeout_scan_window_sec", 0.0)
    await watchdog.sweep(LOGGER)
    call = env["get_running_messages"].await_args
    assert call.args[0] == 0.0
    assert call.kwargs["min_age_sec"] == min(
        settings.ars_timeout_standard_sec,
        settings.ars_timeout_pathfinder_sec,
        settings.ars_timeout_merge_sec,
    )
    assert call.kwargs["limit"] == settings.ars_timeout_scan_limit


async def test_long_stuck_message_is_still_reaped(env):
    """A message far older than upstream's 15-minute window -- the case that
    used to be stranded Running forever -- still times out."""
    row = running("ara-aragorn", 86_400)
    env["get_running_messages"].return_value = [row]
    await watchdog.sweep(LOGGER)
    call = env["update_message"].await_args
    assert str(call.args[0]) == str(row["id"])
    assert call.kwargs["code"] == 598
    assert call.kwargs["status"] == "E"
    env["completion"].assert_awaited_once()
