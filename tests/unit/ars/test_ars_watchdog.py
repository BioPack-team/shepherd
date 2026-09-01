"""Parity tests for the ars_watchdog timeout sweep.

Upstream reference: NCATSTranslator/Relay @ dd1e71b tasks.py
catch_timeout_async: scan Running messages created within the last 15
minutes; exempt parents; merge messages (ars-ars-agent) time out after 8
minutes; everything else after 5 (the pathfinder log line says 10 but the
code compares against now-5min); timed-out messages get code 598 / status E.
"""

import datetime
import logging
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
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
