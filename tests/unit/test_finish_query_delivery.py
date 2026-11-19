"""finish_query delivers each query once, says when it failed, and doesn't
drop a callback on a DNS blip (#210).

- The pipeline delivers tasks at least once and several paths can each queue
  a finish_query for the same query, so a caller could get its callback twice
  (C5-1).
- The sync /query path stamps status: Error onto a failed query's response;
  the async callback carried no status at all (C5-2).
- The callback URL is re-resolved just before delivery, and a transient DNS
  failure skipped the POST with only a log line (N-1).
"""

import json
import logging
from unittest.mock import AsyncMock

import orjson
import pytest

from shepherd_utils.callback_url import CallbackURLError
from workers.finish_query import worker

logger = logging.getLogger(__name__)

CALLBACK = "http://callback.example/1"


def _state(callback=CALLBACK, state="QUEUED"):
    row = [None] * 12
    row[7] = "r1"
    row[8] = callback
    row[9] = state
    return row


@pytest.fixture
def env(monkeypatch, redis_mock):
    stored = {"r1": {"message": {"results": [{"analyses": [{"score": 1}]}]}}}

    async def _get(message_id, _logger, *args, raw=False, **kwargs):
        if message_id == "q1":
            return {"message": {}}
        message = stored[message_id]
        return orjson.dumps(message) if raw else message

    monkeypatch.setattr(worker, "get_message", _get)
    monkeypatch.setattr(worker, "get_query_state", AsyncMock(return_value=_state()))
    monkeypatch.setattr(worker, "set_query_completed", AsyncMock())
    monkeypatch.setattr(worker, "cleanup_callbacks", AsyncMock())
    monkeypatch.setattr(worker, "get_logs", AsyncMock(return_value=[]))
    sent = AsyncMock(return_value=True)
    monkeypatch.setattr(worker, "send_callback", sent)
    monkeypatch.setattr(worker.asyncio, "sleep", AsyncMock())
    return {"stored": stored, "sent": sent}


def _task(**fields):
    return ("1-0", {"query_id": "q1", "response_id": "r1", "workflow": "[]", **fields})


# --- C5-1: one delivery per query ------------------------------------------------


async def test_a_second_finish_for_the_same_query_does_not_deliver_again(
    env, monkeypatch
):
    claims = iter([True, False])
    monkeypatch.setattr(
        worker,
        "claim_query_finish",
        AsyncMock(side_effect=lambda *a, **k: next(claims)),
        raising=False,
    )

    await worker.finish_query(_task(), logger)
    await worker.finish_query(_task(), logger)

    assert env["sent"].await_count == 1


async def test_a_finish_that_loses_the_claim_changes_nothing(env, monkeypatch):
    monkeypatch.setattr(
        worker, "claim_query_finish", AsyncMock(return_value=False), raising=False
    )
    await worker.finish_query(_task(status="ERROR"), logger)
    env["sent"].assert_not_awaited()
    worker.set_query_completed.assert_not_awaited()


# --- C5-2: failed queries say so ---------------------------------------------------


def _delivered(env):
    body = env["sent"].await_args.args[1]
    return orjson.loads(body)


@pytest.fixture
def claimed(monkeypatch):
    monkeypatch.setattr(
        worker, "claim_query_finish", AsyncMock(return_value=True), raising=False
    )


async def test_a_failed_query_is_delivered_with_an_error_status(env, claimed):
    await worker.finish_query(
        _task(status="ERROR 501", description="BTE does not do that."), logger
    )
    body = _delivered(env)
    assert body["status"] == "Error"
    assert body["description"] == "BTE does not do that."
    assert body["message"]["results"] == [{"analyses": [{"score": 1}]}]


async def test_a_timed_out_query_says_so(env, claimed):
    await worker.finish_query(_task(status="TIMEOUT"), logger)
    body = _delivered(env)
    assert body["status"] == "Error"
    assert "TIMEOUT" in body["description"]


async def test_an_ara_reported_error_is_left_alone(env, claimed):
    env["stored"]["r1"] = {"message": {}, "status": "ERROR", "description": "ARA said"}
    await worker.finish_query(_task(status="ERROR"), logger)
    body = _delivered(env)
    assert body["status"] == "ERROR"
    assert body["description"] == "ARA said"


async def test_an_ok_query_is_delivered_untouched(env, claimed):
    await worker.finish_query(_task(), logger)
    body = _delivered(env)
    assert "status" not in body


# --- N-1: DNS blips at delivery ----------------------------------------------------


async def test_a_transient_resolution_failure_is_retried(env, claimed, monkeypatch):
    from shepherd_utils import callback_url

    attempts = []

    async def _validate(url, *args, **kwargs):
        attempts.append(url)
        if len(attempts) < 3:
            raise callback_url.CallbackResolutionError(
                "Invalid callback URL: its host could not be resolved."
            )
        return url

    monkeypatch.setattr(worker, "validate_callback_url", _validate)
    await worker.finish_query(_task(), logger)
    env["sent"].assert_awaited_once()


async def test_a_policy_violation_is_not_retried(env, claimed, monkeypatch):
    attempts = []

    async def _validate(url, *args, **kwargs):
        attempts.append(url)
        raise CallbackURLError(
            "Invalid callback URL: it targets a cloud metadata address."
        )

    monkeypatch.setattr(worker, "validate_callback_url", _validate)
    await worker.finish_query(_task(), logger)
    env["sent"].assert_not_awaited()
    assert len(attempts) == 1


# --- the claim itself, against a real Postgres ------------------------------

PG_DSN = __import__("os").environ.get("SHEPHERD_TEST_PG_DSN")


@pytest.fixture
async def brain(monkeypatch):
    """A throwaway database with the shepherd_brain table. Set
    SHEPHERD_TEST_PG_DSN (a superuser DSN) to run it."""
    if not PG_DSN:
        pytest.skip("SHEPHERD_TEST_PG_DSN not set")
    import uuid

    import psycopg
    from psycopg_pool import AsyncConnectionPool

    from shepherd_utils import db

    name = f"finish_{uuid.uuid4().hex[:8]}"
    async with await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True) as a:
        await a.execute(f"CREATE DATABASE {name}")
    pool = AsyncConnectionPool(
        PG_DSN.rsplit("/", 1)[0] + f"/{name}", min_size=1, max_size=2, open=False
    )
    await pool.open()
    async with pool.connection() as conn:
        await conn.execute(
            "CREATE TABLE shepherd_brain (qid varchar(255) PRIMARY KEY, "
            "start_time TIMESTAMP, stop_time TIMESTAMP, response_id TEXT, "
            "state TEXT, status TEXT)"
        )
        await conn.commit()
    monkeypatch.setattr(db, "pool", pool)
    try:
        yield pool
    finally:
        await pool.close()
        async with await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True) as a:
            await a.execute(f"DROP DATABASE {name}")


async def _insert(pool, qid, state, stop_age_sec=None):
    stop = (
        "NULL"
        if stop_age_sec is None
        else f"NOW() - make_interval(secs => {stop_age_sec})"
    )
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO shepherd_brain (qid, start_time, stop_time, state) "
            f"VALUES (%s, NOW(), {stop}, %s)",
            (qid, state),
        )
        await conn.commit()


async def test_exactly_one_claim_wins(brain):
    from shepherd_utils.db import claim_query_finish

    await _insert(brain, "q", "QUEUED")
    assert await claim_query_finish("q", logger) is True
    assert await claim_query_finish("q", logger) is False


@pytest.mark.parametrize(
    "state, stop_age_sec, claimable",
    [
        ("COMPLETED", 10, False),
        ("FINISHING", 10, False),  # a live finisher
        ("FINISHING", 3600, True),  # its finisher died: take it over
        ("ABANDONED", 10, True),  # the reaper never told the caller
    ],
)
async def test_claim_by_state(brain, state, stop_age_sec, claimable):
    from shepherd_utils.db import claim_query_finish

    await _insert(brain, "q", state, stop_age_sec)
    assert await claim_query_finish("q", logger) is claimable
