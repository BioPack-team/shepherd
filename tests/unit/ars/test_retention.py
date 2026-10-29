"""Keeping the ARS's durable storage bounded (shepherd_utils.ars.retention).

The SQL that picks trees was checked against a real Postgres; these tests pin
the decisions around it: eviction only starts at the high mark, stops once
it has freed enough to reach the low mark, takes whole trees in the order
the candidates query returns, drops the cache entries of cache-backed trees
it evicts, and says how far short it fell when nothing evictable is left.
"""

import json
import logging
import uuid
from unittest.mock import AsyncMock

import pytest

from shepherd_utils.ars import payload_store, retention
from shepherd_utils.config import settings
from workers.ars_watchdog import worker as watchdog

LOGGER = logging.getLogger(__name__)
GB = 10**9


def tree(gb, cached=False):
    return {"root": uuid.uuid4(), "file_bytes": int(gb * GB), "cached": cached}


@pytest.fixture
def volume(tmp_path, monkeypatch, mocker):
    """A 100 GB payload volume whose usage the test sets; candidates are
    served from ``queue`` in order, and dropping a tree removes it."""
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    monkeypatch.setattr(settings, "ars_payload_evict_high_pct", 80.0)
    monkeypatch.setattr(settings, "ars_payload_evict_low_pct", 65.0)
    state = {"used": 0, "queue": []}
    monkeypatch.setattr(payload_store, "usage", lambda: (state["used"], 100 * GB))

    async def candidates(limit):
        return state["queue"][:limit]

    async def drop(roots, logger):
        dropped = [t for t in state["queue"] if t["root"] in roots]
        state["queue"] = [t for t in state["queue"] if t["root"] not in roots]
        return sum(t["file_bytes"] for t in dropped)

    mocker.patch.object(retention, "_eviction_candidates", side_effect=candidates)
    state["drop"] = mocker.patch.object(retention, "_drop_payloads", side_effect=drop)
    state["drop_cache"] = mocker.patch.object(
        retention, "_drop_cache_entries", new_callable=AsyncMock
    )
    return state


async def test_below_the_high_mark_nothing_is_evicted(volume):
    volume["used"] = 79 * GB
    volume["queue"] = [tree(5)]
    result = await retention.evict_for_space(LOGGER)
    assert result["trees"] == 0
    volume["drop"].assert_not_called()


async def test_evicts_oldest_first_only_until_the_low_mark(volume):
    volume["used"] = 85 * GB  # 20 GB over the 65% low mark
    first, second, third, fourth = tree(8), tree(8), tree(8), tree(8)
    volume["queue"] = [first, second, third, fourth]

    result = await retention.evict_for_space(LOGGER)

    assert result["trees"] == 3
    assert result["freed_bytes"] == 24 * GB
    assert volume["queue"] == [fourth]  # the newest survives
    volume["drop_cache"].assert_not_called()


async def test_cache_backed_trees_lose_their_cache_entries(volume):
    volume["used"] = 81 * GB
    cached = tree(20, cached=True)
    volume["queue"] = [cached]

    await retention.evict_for_space(LOGGER)

    volume["drop_cache"].assert_awaited_once_with([cached["root"]])


async def test_says_how_far_short_when_nothing_is_left_to_evict(volume):
    # the rest of the volume is retained or still-running trees
    volume["used"] = 90 * GB
    volume["queue"] = [tree(5)]

    result = await retention.evict_for_space(LOGGER)

    assert result["trees"] == 1
    assert result["short_bytes"] == 20 * GB


async def test_nothing_happens_without_a_payload_directory(monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    assert await retention.evict_for_space(LOGGER) == {
        "trees": 0,
        "freed_bytes": 0,
        "short_bytes": 0,
    }


async def test_usage_is_published_for_the_monitor(volume, redis_mock, mocker):
    mocker.patch.object(retention, "retained_bytes", AsyncMock(return_value=3 * GB))
    volume["used"] = 85 * GB
    volume["queue"] = [tree(30)]
    eviction = await retention.evict_for_space(LOGGER)

    await retention.publish_usage(eviction, LOGGER, broker=redis_mock["broker"])

    state = json.loads(await redis_mock["broker"].get(retention.USAGE_KEY))
    assert state["used_bytes"] == 55 * GB  # after this pass's deletes
    assert state["total_bytes"] == 100 * GB
    assert state["retained_bytes"] == 3 * GB


async def test_age_passes_are_off_at_zero_days():
    assert await retention.purge_expired_payloads(0, LOGGER) == 0
    assert await retention.delete_expired_trees(0, LOGGER) == 0


# --- the watchdog loop ------------------------------------------------------


@pytest.fixture
def passes(mocker):
    return {
        "evict": mocker.patch.object(
            retention, "evict_for_space", AsyncMock(return_value={})
        ),
        "publish": mocker.patch.object(retention, "publish_usage", AsyncMock()),
        "purge": mocker.patch.object(
            retention, "purge_expired_payloads", AsyncMock(return_value=0)
        ),
        "delete": mocker.patch.object(
            retention, "delete_expired_trees", AsyncMock(return_value=0)
        ),
    }


async def test_eviction_runs_every_pass_the_age_passes_hourly(passes):
    await watchdog.retention_pass(LOGGER, hourly=False)
    passes["evict"].assert_awaited_once()
    passes["purge"].assert_not_awaited()
    passes["delete"].assert_not_awaited()

    await watchdog.retention_pass(LOGGER, hourly=True)
    passes["purge"].assert_awaited_once_with(settings.ars_data_retention_days, LOGGER)
    passes["delete"].assert_awaited_once_with(settings.ars_row_retention_days, LOGGER)


async def test_one_failing_pass_does_not_skip_the_others(passes):
    passes["evict"].side_effect = RuntimeError("volume gone")
    passes["purge"].side_effect = RuntimeError("pg down")

    await watchdog.retention_pass(LOGGER, hourly=True)

    passes["publish"].assert_awaited_once()
    passes["delete"].assert_awaited_once()
