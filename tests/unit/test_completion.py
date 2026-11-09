"""Completion is pushed rather than polled for (#221)."""

import asyncio
import logging
import time
from unittest.mock import AsyncMock

import pytest

from shepherd_server import base_routes
from shepherd_utils import db

logger = logging.getLogger(__name__)


@pytest.fixture
def real_intervals(monkeypatch):
    """The production backoff (conftest shortens it for other tests)."""
    from shepherd_utils import completion

    monkeypatch.setattr(completion, "MIN_INTERVAL_SEC", 0.5)
    monkeypatch.setattr(completion, "MAX_INTERVAL_SEC", 5.0)


def _row(state):
    return ("qid", None, None, None, None, None, None, "rid", None, state, "OK")


async def test_the_sync_wait_reads_postgres_less_while_it_waits(
    real_intervals, monkeypatch
):
    """Each sync /query read Postgres every 0.5s for its whole wait."""
    reads = []

    async def _state(query_id, _logger):
        reads.append(time.monotonic())
        return _row("RUNNING")

    monkeypatch.setattr(base_routes, "get_query_state", _state)

    assert await base_routes.wait_for_query("qid", logger, 3.0) is None

    # 0.5s polling read 7 times in 3s; backing off, it reads at 0, 0.5, 1.5
    # (and on the deadline at most once more).
    assert len(reads) <= 4


async def test_the_sync_wait_wakes_on_the_completion_signal(
    real_intervals, redis_mock, monkeypatch
):
    """finish_query's set_query_completed publishes, so the wait doesn't sit
    out its backoff interval once the query is done."""
    from shepherd_utils import completion

    state = {"value": _row("RUNNING")}

    async def _state(query_id, _logger):
        return state["value"]

    monkeypatch.setattr(base_routes, "get_query_state", _state)

    async def _finish_later():
        await asyncio.sleep(2.2)  # inside the 2s-capped backoff (1.5 -> 3.5)
        state["value"] = _row("COMPLETED")
        await completion.signal(completion.DONE, "qid")

    finisher = asyncio.ensure_future(_finish_later())
    started = time.monotonic()
    result = await base_routes.wait_for_query("qid", logger, 10.0)
    elapsed = time.monotonic() - started
    await finisher

    assert result[9] == "COMPLETED"
    assert elapsed < 3.0  # woken by the signal, not the next check at ~3.5s


async def test_set_query_completed_publishes(redis_mock, postgres_mock, monkeypatch):
    from shepherd_utils import completion

    _, pool = postgres_mock(lambda *a, **k: None)
    monkeypatch.setattr(db, "pool", pool)
    published = []
    monkeypatch.setattr(
        completion,
        "signal",
        AsyncMock(side_effect=lambda kind, qid: published.append((kind, qid))),
    )

    await db.set_query_completed("qid", "OK", logger)

    assert published == [(completion.DONE, "qid")]


async def test_a_lookup_wakes_when_its_last_callback_is_removed(
    redis_mock, monkeypatch, real_intervals
):
    """Lookup workers checked their callback rows every second; removing a
    callback row now wakes the wait for that query."""
    from shepherd_utils import completion

    running = {"ids": [("cb-1",)]}

    async def _get_running(query_id, _logger):
        return list(running["ids"])

    async def _remove_later():
        await asyncio.sleep(0.6)  # just after the first re-check at 0.5s
        running["ids"] = []
        await completion.signal(completion.CALLBACKS, "qid")

    remover = asyncio.ensure_future(_remove_later())
    started = time.monotonic()
    left = await db.wait_for_lookups(
        "qid", "rid", 10.0, logger, _get_running, AsyncMock()
    )
    elapsed = time.monotonic() - started
    await remover

    assert left == []
    assert elapsed < 1.2  # the next timed re-check would have been at 1.5s


async def test_removing_a_callback_signals_its_query(postgres_mock, monkeypatch):
    from shepherd_utils import completion

    conn, pool = postgres_mock(None)
    cursor = AsyncMock()
    cursor.fetchone.return_value = ("qid",)
    conn.execute.side_effect = None
    conn.execute.return_value = cursor
    monkeypatch.setattr(db, "pool", pool)
    published = []
    monkeypatch.setattr(
        completion,
        "signal",
        AsyncMock(side_effect=lambda kind, qid: published.append((kind, qid))),
    )

    await db.remove_callback_id("cb-1", logger)

    assert published == [(completion.CALLBACKS, "qid")]


# --- C6-8 ------------------------------------------------------------------------


def test_the_rules_are_read_once(monkeypatch):
    from workers.aragorn_lookup import worker as lookup

    lookup._amie_expansions.cache_clear()
    opened = []
    real_open = open

    def _counting_open(path, *args, **kwargs):
        if str(path).endswith("rules_with_types_cleaned_finalized.json"):
            opened.append(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", _counting_open)
    for _ in range(3):
        lookup._amie_expansions()

    assert len(opened) == 1


async def test_lookup_submissions_are_bounded_per_process(monkeypatch):
    """An inferred query fans out to ~34 lookups, each inserting a callback
    row; they all went out at once, from every task in the pod."""
    from workers.aragorn_lookup import worker as lookup

    monkeypatch.setattr(lookup, "MAX_LOOKUPS_IN_FLIGHT", 3)
    monkeypatch.setattr(lookup, "_lookup_slots", None)
    running = 0
    peak = 0

    async def _lookup(client, message, query_id, _logger):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1

    monkeypatch.setattr(lookup, "run_async_lookup", _lookup)

    await asyncio.gather(
        *(lookup._bounded_lookup(None, {}, f"q{i % 2}", logger) for i in range(20))
    )

    assert peak == 3


async def test_an_arax_stream_reads_query_state_every_2s_not_every_poll(mocker):
    """The stream read Postgres on every 0.2s progress poll."""
    from shepherd_server.aras import arax as api

    clock = {"now": 1000.0}
    fake_time = mocker.MagicMock()
    fake_time.time.side_effect = lambda: clock["now"]
    mocker.patch.object(api, "time", fake_time)

    async def _sleep(seconds):
        clock["now"] += seconds

    mocker.patch.object(api.asyncio, "sleep", side_effect=_sleep)
    mocker.patch.object(api, "read_progress", AsyncMock(return_value=[]))
    mocker.patch.object(api.arax_status, "is_terminated", AsyncMock(return_value=False))
    reads = []

    async def _state(query_id, _logger):
        reads.append(clock["now"])
        return _row("COMPLETED" if clock["now"] >= 1010 else "RUNNING")

    mocker.patch.object(api, "get_query_state", side_effect=_state)
    mocker.patch.object(
        api, "wait_for_query", AsyncMock(return_value=_row("COMPLETED"))
    )
    mocker.patch.object(
        api, "arax_final_response", AsyncMock(return_value=({"message": {}}, 200))
    )

    lines = [
        line async for line in api.stream_query_progress("qid", "rid", logger, 60, {})
    ]

    assert lines  # ended with the response
    # ~10s of streaming: 50 reads at 0.2s; one every 2s now
    assert len(reads) <= 7
