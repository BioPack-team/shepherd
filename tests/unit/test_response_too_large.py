"""Tests for discarding a response that has grown too large to process.

The failure mode: a TRAPI response big enough that loading it OOM-kills the
worker (seen as merge_message growing to tens of GB on one query, then
crash-looping once capped). ``shepherd_utils.response_limit`` is the one place
that decides what happens when a response is judged too large; the merge
worker, the shared task lifecycle, ``finish_query`` and the ``/callback``
endpoint all route through it. The policy under test: the whole response is
discarded (never trimmed), every later stage agrees via a marker, the query
finishes ``RESPONSE_TOO_LARGE`` with an empty message saying why, and the event
is logged at CRITICAL.
"""

import asyncio
import json
import logging
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import AsyncMock

import orjson
import pytest
import zstandard

from shepherd_utils import db as db_module
from shepherd_utils import response_limit, shared
from shepherd_utils.config import settings
from shepherd_utils.logger import attach_query_handler
from shepherd_utils.db import (
    DecompressedTooLargeError,
    ResponseTooLargeError,
    add_ready_callback,
    bump_merge_crashes,
    clear_merge_crashes,
    decompress_zstd,
    get_merge_crashes,
    get_message,
    get_ready_callbacks,
    save_message,
)
from shepherd_utils.response_limit import (
    TOO_LARGE_STATUS,
    build_too_large_response,
    fail_response_too_large,
    get_too_large_reason,
    is_response_too_large,
    mark_response_too_large,
)
from workers.merge_message import worker as merge_worker

logger = logging.getLogger(__name__)

QUERY = {
    "message": {
        "query_graph": {
            "nodes": {
                "n0": {"ids": ["MONDO:1"]},
                "n1": {"categories": ["biolink:Drug"]},
            },
            "edges": {"e0": {"subject": "n1", "object": "n0"}},
        }
    }
}


def _big_message(n: int = 2000) -> dict:
    """A message whose uncompressed JSON is comfortably over a few KB."""
    return {
        "message": {
            "query_graph": QUERY["message"]["query_graph"],
            "knowledge_graph": {
                "nodes": {f"N:{i}": {"name": f"node {i}" * 3} for i in range(n)},
                "edges": {},
            },
            "results": [],
        }
    }


@pytest.fixture
def no_postgres(mocker):
    """The failure path clears the query's callback rows; keep it off Postgres."""
    return mocker.patch("shepherd_utils.db.cleanup_callbacks", new=mocker.AsyncMock())


# --- shepherd_utils.response_limit -------------------------------------------


def test_build_too_large_response_is_empty_and_says_why():
    qg = QUERY["message"]["query_graph"]
    response = build_too_large_response(qg, "it was huge")
    assert response["message"]["query_graph"] is qg
    assert response["message"]["results"] == []
    assert response["message"]["knowledge_graph"] == {"nodes": {}, "edges": {}}
    # TRAPI 2.0 forbids an empty auxiliary_graphs: absent instead.
    assert "auxiliary_graphs" not in response["message"]
    assert response["status"] == "Error"
    assert response["description"] == "Response too large: it was huge"
    # finish_query splices the query's logs in itself.
    assert "logs" not in response


def test_build_too_large_response_without_a_query_graph():
    """An empty query graph is invalid TRAPI 2.0, so there is none at all."""
    assert "query_graph" not in build_too_large_response(None, "x")["message"]
    assert "query_graph" not in build_too_large_response({}, "x")["message"]


def test_build_too_large_response_is_a_valid_trapi_2_response():
    from translator_tom import Response

    from shepherd_utils.trapi import finalize_response

    response = build_too_large_response(QUERY["message"]["query_graph"], "big")
    Response.from_dict(finalize_response(response, QUERY))


async def test_fail_response_too_large_settles_everything(
    redis_mock, no_postgres, caplog
):
    await save_message("q1", QUERY, logger)
    await save_message("r1", _big_message(), logger)
    await add_ready_callback("r1", "cb1", logger)
    await add_ready_callback("r1", "cb2", logger)
    assert not await is_response_too_large("r1")

    with caplog.at_level(logging.CRITICAL):
        await fail_response_too_large("q1", "r1", "too big", logger)

    # Marker, for every later stage to agree on.
    assert await get_too_large_reason("r1") == "too big"
    assert await is_response_too_large("r1")
    # The stored response is the empty message, carrying the query graph.
    stored = await get_message("r1", logger)
    assert stored["message"]["results"] == []
    assert stored["message"]["query_graph"] == QUERY["message"]["query_graph"]
    assert stored["status"] == "Error"
    assert stored["description"] == "Response too large: too big"
    # Nothing left for the merge worker to fold in...
    assert await get_ready_callbacks("r1", logger) == []
    # ...and the callback rows are cleared so the lookup stops waiting.
    no_postgres.assert_awaited_once_with("q1", logger)
    # Logged at CRITICAL with the greppable marker.
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical) == 1
    assert "RESPONSE_TOO_LARGE" in critical[0].getMessage()
    assert "q1" in critical[0].getMessage() and "r1" in critical[0].getMessage()
    assert "too big" in critical[0].getMessage()


async def test_fail_response_too_large_without_the_query(redis_mock, no_postgres):
    """A query blob that has already expired doesn't stop the failure from
    being recorded; the response just carries no query graph."""
    await fail_response_too_large("missing-q", "r2", "reason", logger)
    stored = await get_message("r2", logger)
    assert "query_graph" not in stored["message"]
    assert await get_too_large_reason("r2") == "reason"


async def test_marker_survives_a_racing_save(redis_mock, no_postgres):
    """A merge pass in flight when the query was failed can save the big
    accumulator over the empty message; the marker is what finish_query
    trusts, and it rewrites the response from it."""
    await fail_response_too_large("q1", "r3", "reason", logger)
    await save_message("r3", _big_message(), logger)  # the race
    assert await get_too_large_reason("r3") == "reason"


# --- shepherd_utils.db additions ----------------------------------------------


def test_decompress_zstd_bounded_accepts_under_cap():
    raw = b"y" * 5000
    assert decompress_zstd(zstandard.compress(raw), 10_000) == raw


def test_decompress_zstd_bounded_rejects_declared_size_over_cap():
    frame = zstandard.compress(b"y" * 5000)
    assert zstandard.frame_content_size(frame) == 5000
    with pytest.raises(DecompressedTooLargeError):
        decompress_zstd(frame, 1000)


def test_decompress_zstd_bounded_rejects_streaming_frame_over_cap():
    """A streaming frame carries no content size, so the cap has to trip on
    the output as it is produced."""
    raw = b"y" * 5_000_000
    cctx = zstandard.ZstdCompressor()
    import io

    buf = io.BytesIO()
    with cctx.stream_writer(buf, closefd=False) as w:
        w.write(raw)
    frame = buf.getvalue()
    assert zstandard.frame_content_size(frame) in (-1, None)
    with pytest.raises(DecompressedTooLargeError):
        decompress_zstd(frame, 1_000_000)
    assert decompress_zstd(frame, 0) == raw  # 0 disables the cap


async def test_merge_crash_counter_roundtrip(redis_mock):
    assert await get_merge_crashes("r") == 0
    assert await bump_merge_crashes("r") == 1
    assert await bump_merge_crashes("r") == 2
    assert await get_merge_crashes("r") == 2
    await clear_merge_crashes("r")
    assert await get_merge_crashes("r") == 0


# --- shared.run_task_lifecycle guard -----------------------------------------


def _lifecycle_task():
    return [
        "msg-id",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": json.dumps([{"id": "some_stream"}]),
            "log_level": "20",
            "otel": "{}",
            "metadata": "{}",
        },
    ]


class _Limiter:
    def __init__(self):
        self.released = False

    def release(self):
        self.released = True


async def test_lifecycle_guard_is_a_no_op_when_the_cap_is_off(monkeypatch):
    """With max_response_size at 0 (the default) the guard never touches the
    datastore -- this test deliberately has no redis_mock."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    await shared._guard_response_size("some_stream", _lifecycle_task(), logger)


async def test_lifecycle_fails_an_oversized_response_before_the_worker_runs(
    redis_mock, no_postgres, mocker, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "100")
    await save_message("q1", QUERY, logger)
    await save_message("r1", _big_message(), logger)
    worker_fn = mocker.AsyncMock()
    wrap = mocker.patch.object(shared, "wrap_up_task", new_callable=mocker.AsyncMock)
    fail = mocker.patch.object(
        shared, "handle_task_failure", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()

    await shared.run_task_lifecycle(
        "some_stream", "consumer", _lifecycle_task(), None, logger, limiter, worker_fn
    )

    worker_fn.assert_not_awaited()
    assert not wrap.called
    # Routed to finish_query, which records RESPONSE_TOO_LARGE from the marker.
    assert fail.called
    assert limiter.released
    assert "some_stream" in (await get_too_large_reason("r1"))
    assert (await get_message("r1", logger))["message"]["results"] == []


async def test_lifecycle_runs_a_response_under_the_cap(redis_mock, mocker, monkeypatch):
    monkeypatch.setattr(settings, "max_response_size", "100M")
    await save_message("r1", _big_message(), logger)
    worker_fn = mocker.AsyncMock()
    wrap = mocker.patch.object(shared, "wrap_up_task", new_callable=mocker.AsyncMock)
    mocker.patch.object(shared, "handle_task_failure", new_callable=mocker.AsyncMock)

    await shared.run_task_lifecycle(
        "some_stream",
        "consumer",
        _lifecycle_task(),
        None,
        logger,
        _Limiter(),
        worker_fn,
    )

    worker_fn.assert_awaited_once()
    assert wrap.called
    assert not await is_response_too_large("r1")


# --- merge_message: batch sizing helpers ------------------------------------


async def test_fit_batch_passes_everything_through_when_the_cap_is_off(
    redis_mock, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "0")
    batch, reason = await merge_worker._fit_batch_to_budget("r", ["a", "b"], logger)
    assert (batch, reason) == (["a", "b"], None)


async def test_fit_batch_fails_when_the_accumulator_is_over_the_cap(
    redis_mock, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "100")
    await save_message("r", _big_message(), logger)
    await save_message("a", QUERY, logger)
    batch, reason = await merge_worker._fit_batch_to_budget("r", ["a"], logger)
    assert batch == []
    assert "accumulated response" in reason and "max_response_size" in reason


async def test_fit_batch_fails_when_a_single_callback_is_over_the_cap(
    redis_mock, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "10K")
    await save_message("r", QUERY, logger)
    await save_message("small", QUERY, logger)
    await save_message("huge", _big_message(), logger)
    batch, reason = await merge_worker._fit_batch_to_budget(
        "r", ["small", "huge"], logger
    )
    assert batch == []
    assert "callback huge" in reason


async def test_fit_batch_takes_the_prefix_that_fits_but_at_least_one(
    redis_mock, monkeypatch
):
    await save_message("r", QUERY, logger)
    for cb in ("a", "b", "c"):
        await save_message(cb, _big_message(50), logger)
    each = await db_module.get_response_size("a")
    acc = await db_module.get_response_size("r")
    # Room for the accumulator plus two callbacks, not three.
    monkeypatch.setattr(settings, "max_response_size", str(acc + 2 * each + 1))
    batch, reason = await merge_worker._fit_batch_to_budget(
        "r", ["a", "b", "c"], logger
    )
    assert (batch, reason) == (["a", "b"], None)
    # Even when the first callback alone overflows the budget, one is folded so
    # the merge always makes progress; the post-pass check then judges the
    # real merged size.
    monkeypatch.setattr(settings, "max_response_size", str(each + 1))
    batch, reason = await merge_worker._fit_batch_to_budget(
        "r", ["a", "b", "c"], logger
    )
    assert (batch, reason) == (["a"], None)


async def test_check_response_budget(redis_mock, monkeypatch):
    await save_message("r", _big_message(), logger)
    monkeypatch.setattr(settings, "max_response_size", "0")
    assert await merge_worker._check_response_budget("r") is None
    monkeypatch.setattr(settings, "max_response_size", "100M")
    assert await merge_worker._check_response_budget("r") is None
    monkeypatch.setattr(settings, "max_response_size", "100")
    assert "after merging" in await merge_worker._check_response_budget("r")


def test_too_many_crashes_honours_the_breaker_setting(monkeypatch):
    monkeypatch.setattr(settings, "max_task_deliveries", 3)
    assert not merge_worker._too_many_crashes(2)
    assert merge_worker._too_many_crashes(3)
    assert merge_worker._too_many_crashes(7)
    monkeypatch.setattr(settings, "max_task_deliveries", 0)
    assert not merge_worker._too_many_crashes(100)


# --- merge_message: the drain loop, end to end -------------------------------


class _FakePool:
    """Stands in for ProcessPoolManager: scripted results, no child processes."""

    def __init__(self, outcomes):
        # Each entry is either an exception instance to raise, or a coroutine
        # function called as fn(response_id, ready) -> (merged, logs).
        self.outcomes = list(outcomes)
        self.calls = []

    async def run(self, loop, fn, target, query_id, response_id, ready, log_level):
        self.calls.append(list(ready))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return await outcome(response_id, ready)

    def shutdown(self):
        pass


async def _fold_all(response_id, ready):
    """A merge that just reports every callback folded."""
    return list(ready), []


async def _drive_merge(mocker, pool, task_fields):
    """Run merge_message.poll_for_tasks over exactly one wake task.

    ``get_tasks`` is replaced by a generator that yields the task, waits for
    ``process_query`` to release the limiter (i.e. finish), then cancels the
    loop the way a shutdown would. Returns the list of wake tasks re-enqueued.
    """
    done = asyncio.Event()

    class _Limiter:
        def release(self):
            done.set()

    async def _one_task(*args, **kwargs):
        yield ("1-0", dict(task_fields)), None, logger, _Limiter()
        await asyncio.wait_for(done.wait(), timeout=5)
        raise asyncio.CancelledError

    mocker.patch.object(merge_worker, "get_tasks", _one_task)
    mocker.patch.object(merge_worker, "ProcessPoolManager", lambda *a, **k: pool)
    mocker.patch.object(merge_worker, "resolve_pool_workers", lambda *a, **k: 1)
    mocker.patch.object(merge_worker, "remove_callback_id", new=mocker.AsyncMock())
    # Every ready callback is still wanted unless a test patched this first.
    if not isinstance(merge_worker.get_existing_callback_ids, AsyncMock):
        mocker.patch.object(
            merge_worker,
            "get_existing_callback_ids",
            new=mocker.AsyncMock(side_effect=lambda ids, logger: set(ids)),
        )
    mocker.patch.object(merge_worker, "mark_task_as_complete", new=mocker.AsyncMock())
    # fakeredis has no EVALSHA, which the real unlock script needs; record the
    # release instead so the tests can assert the lock is always let go.
    unlocked = mocker.patch.object(merge_worker, "remove_lock", new=mocker.AsyncMock())
    reenqueued = mocker.patch.object(merge_worker, "add_task", new=mocker.AsyncMock())
    await merge_worker.poll_for_tasks()
    assert unlocked.await_count == 1, "the query lock must be released exactly once"
    return reenqueued


def _wake_task(query_id="q1", response_id="r1", callback_id="cb1"):
    return {
        "query_id": query_id,
        "response_id": response_id,
        "callback_id": callback_id,
        "target": "aragorn",
        "log_level": "20",
        "otel": "{}",
        "metadata": "{}",
    }


@pytest.fixture
async def merge_query(redis_mock, no_postgres):
    """A query with its initial response stored and two callbacks ready."""
    await save_message("q1", QUERY, logger)
    await save_message("r1", QUERY, logger)
    for cb in ("cb1", "cb2"):
        await save_message(cb, _big_message(50), logger)
        await add_ready_callback("r1", cb, logger)
    return redis_mock


async def test_merge_fails_the_query_when_the_accumulator_is_over_the_cap(
    merge_query, mocker, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "10K")
    await save_message("r1", _big_message(), logger)  # already too big
    pool = _FakePool([])

    await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == [], "nothing is loaded once the budget is blown"
    assert "accumulated response" in await get_too_large_reason("r1")
    assert (await get_message("r1", logger))["message"]["results"] == []
    assert await get_ready_callbacks("r1", logger) == []
    # The lock is released.


async def test_merge_fails_the_query_when_a_callback_is_over_the_cap(
    merge_query, mocker, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "10K")
    await save_message("cb2", _big_message(), logger)
    pool = _FakePool([])

    await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == []
    assert "callback cb2" in await get_too_large_reason("r1")
    assert await get_ready_callbacks("r1", logger) == []


async def test_merge_fails_the_query_when_a_pass_grows_it_over_the_cap(
    merge_query, mocker, monkeypatch
):
    """The pre-pass sizing is an estimate; the merged size is what counts."""
    monkeypatch.setattr(settings, "max_response_size", "20K")

    async def _grow(response_id, ready):
        await save_message(response_id, _big_message(), logger)
        return list(ready), []

    pool = _FakePool([_grow])

    await _drive_merge(mocker, pool, _wake_task())

    assert len(pool.calls) == 1
    assert "after merging" in await get_too_large_reason("r1")
    assert (await get_message("r1", logger))["message"]["results"] == []
    # The pass came back, so it isn't counted as a crash.
    assert await get_merge_crashes("r1") == 0


async def test_merge_under_the_cap_folds_everything_and_clears_the_counter(
    merge_query, mocker, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "100M")
    pool = _FakePool([_fold_all])

    reenqueued = await _drive_merge(mocker, pool, _wake_task())

    assert sorted(pool.calls[0]) == ["cb1", "cb2"]
    assert not await is_response_too_large("r1")
    assert await get_ready_callbacks("r1", logger) == []
    assert await get_merge_crashes("r1") == 0
    reenqueued.assert_not_awaited()


async def test_merge_drops_callbacks_for_a_query_already_discarded(
    merge_query, mocker, monkeypatch
):
    """A callback that lands after the query was failed must not grow the
    discarded response again."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    await mark_response_too_large("r1", "earlier")
    pool = _FakePool([])
    fail = mocker.patch.object(
        merge_worker, "fail_response_too_large", new=mocker.AsyncMock()
    )

    await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == []
    assert await get_ready_callbacks("r1", logger) == []
    fail.assert_not_awaited()  # already failed; not re-failed


async def test_merge_counts_a_crash_and_retries_below_the_limit(
    merge_query, mocker, monkeypatch
):
    """A child death (BrokenProcessPool) leaves the crash count in place so
    the next attempt -- on this pod or, after a container kill, the next --
    can see it; below the limit the batch is retried as before."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    monkeypatch.setattr(settings, "max_task_deliveries", 3)
    mocker.patch.object(merge_worker.asyncio, "sleep", new=mocker.AsyncMock())
    pool = _FakePool([BrokenProcessPool("child died")])

    reenqueued = await _drive_merge(mocker, pool, _wake_task())

    assert await get_merge_crashes("r1") == 1
    assert not await is_response_too_large("r1")
    # The callbacks are still ready for the retry the re-enqueued wake drives.
    assert sorted(await get_ready_callbacks("r1", logger)) == ["cb1", "cb2"]
    reenqueued.assert_awaited_once()


async def test_merge_fails_the_query_once_it_has_crashed_enough(
    merge_query, mocker, monkeypatch
):
    """Two earlier passes never came back (as after two container OOM kills);
    the third death trips the breaker in-process."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    monkeypatch.setattr(settings, "max_task_deliveries", 3)
    await bump_merge_crashes("r1")
    await bump_merge_crashes("r1")
    pool = _FakePool([BrokenProcessPool("child died")])

    reenqueued = await _drive_merge(mocker, pool, _wake_task())

    reason = await get_too_large_reason("r1")
    assert "crashed the merge worker 3 time(s)" in reason
    assert (await get_message("r1", logger))["message"]["results"] == []
    assert await get_ready_callbacks("r1", logger) == []
    assert await get_merge_crashes("r1") == 0
    reenqueued.assert_not_awaited()


async def test_merge_fails_the_query_on_arrival_after_enough_crashes(
    merge_query, mocker, monkeypatch
):
    """The whole container was killed three times mid-merge: the count is all
    that survived, and the next pod reads it before loading anything."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    monkeypatch.setattr(settings, "max_task_deliveries", 3)
    for _ in range(3):
        await bump_merge_crashes("r1")
    pool = _FakePool([])

    await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == [], "the poison merge is never started again"
    assert "crashed the merge worker 3 time(s)" in await get_too_large_reason("r1")


async def test_merge_python_error_is_not_a_crash(merge_query, mocker, monkeypatch):
    """A merge that raises came back: the task-field retry counter owns that
    case, and the crash counter must not accumulate from it."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    mocker.patch.object(merge_worker.asyncio, "sleep", new=mocker.AsyncMock())
    pool = _FakePool([KeyError("name")])

    await _drive_merge(mocker, pool, _wake_task())

    assert await get_merge_crashes("r1") == 0
    assert not await is_response_too_large("r1")


# --- finish_query -------------------------------------------------------------


def _finish_task(query_id="q1", response_id="r1", status=None):
    fields = {
        "query_id": query_id,
        "response_id": response_id,
        "workflow": "[]",
        "log_level": "20",
        "otel": "{}",
        "metadata": "{}",
    }
    if status is not None:
        fields["status"] = status
    return ["msg", fields]


@pytest.fixture
def finish(mocker):
    from workers.finish_query import worker as fq

    mocker.patch.object(fq, "cleanup_callbacks", new=mocker.AsyncMock())
    completed = mocker.patch.object(fq, "set_query_completed", new=mocker.AsyncMock())
    state = mocker.patch.object(fq, "get_query_state", new=mocker.AsyncMock())
    sent = mocker.patch.object(
        fq, "send_callback", new=mocker.AsyncMock(return_value=True)
    )
    return fq, completed, state, sent


async def test_finish_query_records_too_large_and_delivers_the_empty_message(
    redis_mock, no_postgres, finish
):
    fq, completed, state, sent = finish
    state.return_value = [None] * 7 + ["r1", "http://callback"]
    await save_message("q1", QUERY, logger)
    # A query logger, as the workers have, so the CRITICAL line is persisted
    # to the query's log list the way it is in production.
    query_logger = logging.getLogger("shepherd.test.r1")
    attach_query_handler(query_logger)
    await fail_response_too_large("q1", "r1", "way too big", query_logger)
    # A racing merge pass saved the big accumulator over the empty message.
    await save_message("r1", _big_message(), logger)

    await fq.finish_query(_finish_task(status="OK"), logger)

    completed.assert_awaited_once_with("q1", TOO_LARGE_STATUS, logger)
    # What was delivered is the empty message, logs spliced in...
    url, payload, _ = sent.await_args.args
    body = orjson.loads(payload)
    assert body["message"]["results"] == []
    assert body["status"] == "Error"
    assert body["description"] == "Response too large: way too big"
    assert isinstance(body["logs"], list)
    # ...and the CRITICAL line is in those logs.
    assert any(
        "RESPONSE_TOO_LARGE" in entry.get("message", "") for entry in body["logs"]
    )
    # The stored response was rewritten too, for /response and the sync path.
    assert (await get_message("r1", logger))["message"]["results"] == []


async def test_finish_query_checks_the_cap_itself(
    redis_mock, no_postgres, finish, monkeypatch, mocker
):
    """Nothing before finish_query checked this response (the cap was set
    while it was in flight): it is judged here, before the raw load."""
    fq, completed, state, sent = finish
    state.return_value = [None] * 7 + ["r1", "http://callback"]
    monkeypatch.setattr(settings, "max_response_size", "100")
    await save_message("q1", QUERY, logger)
    await save_message("r1", _big_message(), logger)
    real_get_message = fq.get_message

    async def _load(message_id, logger, *args, **kwargs):
        # Loading the query (to echo its parameters) is fine; the response
        # itself must never be loaded.
        assert message_id != "r1", "the oversized response was loaded"
        return await real_get_message(message_id, logger, *args, **kwargs)

    load = mocker.patch.object(fq, "get_message", side_effect=_load)

    await fq.finish_query(_finish_task(), logger)

    assert all(call.args[0] != "r1" for call in load.call_args_list)
    completed.assert_awaited_once_with("q1", TOO_LARGE_STATUS, logger)
    assert "finish_query" in await get_too_large_reason("r1")
    body = orjson.loads(sent.await_args.args[1])
    assert body["message"]["results"] == []


async def test_finish_query_is_unchanged_for_a_normal_response(
    redis_mock, finish, monkeypatch
):
    fq, completed, state, sent = finish
    state.return_value = [None] * 7 + ["r1", "http://callback"]
    monkeypatch.setattr(settings, "max_response_size", "100M")
    await save_message("r1", _big_message(), logger)

    await fq.finish_query(_finish_task(), logger)

    completed.assert_awaited_once_with("q1", "OK", logger)
    body = orjson.loads(sent.await_args.args[1])
    assert len(body["message"]["knowledge_graph"]["nodes"]) == 2000
    # The query blob ("q1") was never stored here -- as when it has expired
    # under a long query -- and the response is still delivered, as 2.0.
    assert body["schema_version"] == "2.0.0"


# --- merge_message: callbacks the query is no longer waiting for --------------


async def test_merge_drops_callbacks_the_query_no_longer_waits_for(
    merge_query, mocker, monkeypatch
):
    """The lookup timed out (or the query finished) and cleared the callback
    rows: a ready callback with no row is dropped, not merged into a response
    that has already been delivered."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    mocker.patch.object(
        merge_worker,
        "get_existing_callback_ids",
        new=mocker.AsyncMock(side_effect=lambda ids, logger: {"cb1"}),
    )
    pool = _FakePool([_fold_all])

    await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == [["cb1"]]
    assert await get_ready_callbacks("r1", logger) == []


async def test_merge_does_nothing_when_no_callback_is_wanted(
    merge_query, mocker, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "0")
    mocker.patch.object(
        merge_worker,
        "get_existing_callback_ids",
        new=mocker.AsyncMock(side_effect=lambda ids, logger: set()),
    )
    pool = _FakePool([])

    await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == [], "nothing is loaded for a query nobody is waiting on"
    assert await get_ready_callbacks("r1", logger) == []
    assert (
        await get_message("r1", logger)
    ) == QUERY, "the stored response is untouched"


async def test_merge_keeps_everything_when_the_row_check_fails(
    merge_query, mocker, monkeypatch
):
    """A datastore blip must not drop callbacks: an unreadable table fails open."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    mocker.patch.object(
        merge_worker,
        "get_existing_callback_ids",
        new=mocker.AsyncMock(side_effect=lambda ids, logger: None),
    )
    pool = _FakePool([_fold_all])

    await _drive_merge(mocker, pool, _wake_task())

    assert sorted(pool.calls[0]) == ["cb1", "cb2"]
