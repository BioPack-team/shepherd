"""A finished query's response is sealed, and nothing can replace it (#257).

Once ``finish_query`` has claimed a query, its response is about to be
delivered (callback, ARS hand-off, sync caller) and archived. Delivery is at
least once, so a merge pass in flight, a late callback, a redelivered or
reclaimed task or a slow operation could still save over it. finish_query
seals the response after its own last write; every later write is refused,
atomically with the check (WATCH/MULTI/EXEC), and the late task stops.
"""

import json
import logging
import pickle
import uuid

import fakeredis as sync_fakeredis
import orjson
import pytest

from shepherd_utils import db, shared
from shepherd_utils.config import settings
from shepherd_utils.db import ResponseSealedError
from tests.unit.test_response_too_large import (
    QUERY,
    _big_message,
    _drive_merge,
    _FakePool,
    _wake_task,
)
from workers.finish_query import worker as finish_worker
from workers.merge_message import worker as merge_worker

logger = logging.getLogger(__name__)

ORIGINAL = {"message": {"results": [{"analyses": [{"score": 1.0}]}]}}
LATE = {"message": {"results": []}, "description": "written too late"}


async def _stored(response_id):
    return await db.data_db_client.get(response_id)


# --- the seal itself, async writers ------------------------------------------


async def test_a_save_after_the_seal_leaves_the_stored_response_unchanged(
    redis_mock,
):
    await db.save_response("r1", ORIGINAL, logger)
    before = await _stored("r1")
    assert await db.seal_response("r1", logger)

    with pytest.raises(ResponseSealedError) as refused:
        await db.save_response("r1", LATE, logger)

    assert refused.value.response_id == "r1"
    assert "r1" in str(refused.value)
    assert await _stored("r1") == before


async def test_a_refusal_is_reported_even_to_a_best_effort_save(redis_mock):
    """``raise_on_failure=False`` is about storage failures; a refusal is not
    one and is never swallowed, or the caller would carry on as if saved."""
    await db.save_response("r1", ORIGINAL, logger)
    before = await _stored("r1")
    await db.seal_response("r1", logger)

    with pytest.raises(ResponseSealedError):
        await db.save_response("r1", LATE, logger, raise_on_failure=False)
    with pytest.raises(ResponseSealedError):
        await db.save_message("r1", LATE, logger)

    assert await _stored("r1") == before


async def test_a_refused_save_is_not_retried(redis_mock, mocker):
    sleep = mocker.patch.object(db.asyncio, "sleep", new=mocker.AsyncMock())
    await db.seal_response("r1", logger)

    with pytest.raises(ResponseSealedError):
        await db.save_response("r1", LATE, logger)

    sleep.assert_not_awaited()


async def test_the_seal_only_covers_its_own_response(redis_mock):
    await db.seal_response("r1", logger)

    await db.save_response("r2", ORIGINAL, logger)
    await db.save_message("cb-1", ORIGINAL, logger)

    assert await db.get_message("r2", logger) == ORIGINAL
    assert await db.get_message("cb-1", logger) == ORIGINAL


async def test_the_seal_lives_as_long_as_a_response(redis_mock, monkeypatch):
    monkeypatch.setattr(settings, "redis_ttl", 1000)
    await db.save_response("r1", ORIGINAL, logger)
    await db.seal_response("r1", logger)

    seal_ttl = await db.data_db_client.ttl(db.sealed_key("r1"))
    assert 990 < seal_ttl <= 1000
    assert seal_ttl >= await db.data_db_client.ttl("r1")


def test_the_refusal_crosses_a_process_boundary():
    """Process-pool children raise it to their parent, pickled."""
    error = pickle.loads(pickle.dumps(ResponseSealedError("r1")))
    assert isinstance(error, ResponseSealedError)
    assert error.response_id == "r1"


# --- a writer racing the seal ------------------------------------------------


def _seal_after_first_check(pipeline_factory, on_check):
    """A ``pipeline`` that runs ``on_check`` right after the writer's first
    look at the seal -- between its check and its EXEC."""
    calls = {"checks": 0}

    def _pipeline(*args, **kwargs):
        pipe = pipeline_factory(*args, **kwargs)
        real_exists = pipe.exists

        async def _exists(*keys):
            result = await real_exists(*keys)
            calls["checks"] += 1
            if calls["checks"] == 1:
                await on_check()
            return result

        pipe.exists = _exists
        return pipe

    return _pipeline, calls


async def test_a_writer_that_checked_just_before_the_seal_is_refused(
    redis_mock, monkeypatch
):
    """fakeredis supports WATCH: the seal landing after the check aborts the
    EXEC, and the retry sees the seal."""
    await db.save_response("r1", ORIGINAL, logger)
    before = await _stored("r1")
    client = redis_mock["data"]

    async def _finish_query_seals():
        await db.seal_response("r1", logger)

    pipeline, calls = _seal_after_first_check(client.pipeline, _finish_query_seals)
    monkeypatch.setattr(client, "pipeline", pipeline)

    with pytest.raises(ResponseSealedError):
        await db.save_response("r1", LATE, logger)

    assert calls["checks"] == 2, "the aborted EXEC is retried"
    assert await _stored("r1") == before


async def test_a_conflict_that_leaves_no_seal_is_retried_and_the_write_lands(
    redis_mock, monkeypatch
):
    client = redis_mock["data"]

    async def _touch_the_seal_key():
        # Changes the watched key without leaving a seal behind.
        await client.set(db.sealed_key("r1"), b"x")
        await client.delete(db.sealed_key("r1"))

    pipeline, calls = _seal_after_first_check(client.pipeline, _touch_the_seal_key)
    monkeypatch.setattr(client, "pipeline", pipeline)

    await db.save_response("r1", ORIGINAL, logger)

    assert calls["checks"] == 2
    assert await db.get_message("r1", logger) == ORIGINAL


# --- the sync writers (process-pool children) --------------------------------


@pytest.fixture
def sync_store(mocker):
    client = sync_fakeredis.FakeRedis()
    mocker.patch.object(db, "_get_sync_data_db", return_value=client)
    return client


def test_a_sync_save_after_the_seal_leaves_the_stored_response_unchanged(
    sync_store,
):
    db.save_response_sync("r1", ORIGINAL)
    before = sync_store.get("r1")
    sync_store.set(db.sealed_key("r1"), b"1")

    with pytest.raises(ResponseSealedError):
        db.save_response_sync("r1", LATE)
    with pytest.raises(ResponseSealedError):
        db.save_message_sync("r1", LATE)

    assert sync_store.get("r1") == before


def test_a_sync_writer_that_checked_just_before_the_seal_is_refused(sync_store):
    db.save_response_sync("r1", ORIGINAL)
    before = sync_store.get("r1")
    real_pipeline = sync_store.pipeline
    calls = {"checks": 0}

    def _pipeline(*args, **kwargs):
        pipe = real_pipeline(*args, **kwargs)
        real_exists = pipe.exists

        def _exists(*keys):
            result = real_exists(*keys)
            calls["checks"] += 1
            if calls["checks"] == 1:
                sync_store.set(db.sealed_key("r1"), b"1")
            return result

        pipe.exists = _exists
        return pipe

    sync_store.pipeline = _pipeline

    with pytest.raises(ResponseSealedError):
        db.save_response_sync("r1", LATE)

    assert calls["checks"] == 2
    assert sync_store.get("r1") == before


# --- the task lifecycle: a late task stops -----------------------------------


def _task():
    return [
        "msg-1",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": json.dumps([{"id": "some_stream"}, {"id": "next_op"}]),
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


@pytest.fixture
def lifecycle(mocker):
    return {
        "wrap": mocker.patch.object(
            shared, "wrap_up_task", new_callable=mocker.AsyncMock
        ),
        "fail": mocker.patch.object(
            shared, "handle_task_failure", new_callable=mocker.AsyncMock
        ),
        "ack": mocker.patch.object(
            shared, "mark_task_as_complete", new_callable=mocker.AsyncMock
        ),
        "save_logs": mocker.patch.object(
            shared, "save_logs", new_callable=mocker.AsyncMock
        ),
    }


async def test_a_task_whose_save_is_refused_stops_without_handing_the_query_on(
    redis_mock, lifecycle, caplog
):
    """The query finished while the operation ran: its write is refused, the
    task is ACKed, and neither the next operation nor finish_query is queued
    (the query is not marked failed, nor delivered again)."""
    await db.save_response("r1", ORIGINAL, logger)
    before = await _stored("r1")

    async def _operation(task, task_logger):
        await db.seal_response("r1", task_logger)  # finish_query, meanwhile
        await db.save_response("r1", LATE, task_logger)

    limiter = _Limiter()
    with caplog.at_level(logging.WARNING):
        await shared.run_task_lifecycle(
            "some_stream", "consumer", _task(), None, logger, limiter, _operation
        )

    assert await _stored("r1") == before
    lifecycle["wrap"].assert_not_awaited()
    lifecycle["fail"].assert_not_awaited()
    lifecycle["ack"].assert_awaited_once_with(
        "some_stream", "consumer", "msg-1", logger
    )
    # Not flushed into the finished query's (delivered, archived) log list.
    lifecycle["save_logs"].assert_not_awaited()
    assert limiter.released
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "some_stream" in warnings[0].getMessage()
    assert "q1" in warnings[0].getMessage()
    assert "r1" in warnings[0].getMessage()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


async def test_a_task_for_an_already_finished_query_is_not_run(
    redis_mock, lifecycle, mocker
):
    await db.seal_response("r1", logger)
    operation = mocker.AsyncMock()

    await shared.run_task_lifecycle(
        "some_stream", "consumer", _task(), None, logger, _Limiter(), operation
    )

    operation.assert_not_awaited()
    lifecycle["wrap"].assert_not_awaited()
    lifecycle["fail"].assert_not_awaited()
    lifecycle["ack"].assert_awaited_once()


async def test_a_failed_seal_check_still_runs_the_task(
    redis_mock, lifecycle, mocker, monkeypatch
):
    """The check before the operation is an optimization; a Redis blip on it
    must not fail the query. The check on the save is what guarantees."""
    monkeypatch.setattr(
        shared,
        "is_response_sealed",
        mocker.AsyncMock(side_effect=ConnectionError("blip")),
    )
    operation = mocker.AsyncMock()

    await shared.run_task_lifecycle(
        "some_stream", "consumer", _task(), None, logger, _Limiter(), operation
    )

    operation.assert_awaited_once()
    lifecycle["wrap"].assert_awaited_once()
    lifecycle["fail"].assert_not_awaited()


# --- merge_message ------------------------------------------------------------


def test_a_merge_pass_that_finishes_after_the_query_does_not_change_the_response(
    sync_store, mocker
):
    """The pass loaded the response before finish_query claimed the query
    and only tries to save once it has been sealed."""
    from tests.helpers.generate_messages import generate_response, response_2

    query_graph = response_2["message"]["query_graph"]
    db.save_message_sync("qid", {"message": {"query_graph": query_graph}})
    db.save_message_sync("rid", generate_response())
    db.save_message_sync("cb1", response_2)
    before = sync_store.get("rid")
    real_pass = merge_worker.merge_pass

    def _pass_outlived_by_its_query(*args, **kwargs):
        merged = real_pass(*args, **kwargs)
        sync_store.set(db.sealed_key("rid"), b"1")  # finish_query claims
        return merged

    mocker.patch.object(merge_worker, "merge_pass", _pass_outlived_by_its_query)

    with pytest.raises(ResponseSealedError):
        merge_worker.merge_messages_by_ids("aragorn", "qid", "rid", ["cb1"])

    assert sync_store.get("rid") == before


@pytest.fixture
async def merge_query(redis_mock, mocker):
    mocker.patch("shepherd_utils.db.cleanup_callbacks", new=mocker.AsyncMock())
    await db.save_message("q1", QUERY, logger)
    await db.save_response("r1", QUERY, logger)
    for cb in ("cb1", "cb2"):
        await db.save_message(cb, _big_message(50), logger)
        await db.add_ready_callback("r1", cb, logger)
    return redis_mock


async def test_merge_stands_down_when_its_save_is_refused(
    merge_query, mocker, monkeypatch, caplog
):
    """Refused, the pass is neither retried nor counted: the ready callbacks
    are dropped, payloads and all, and nothing is re-enqueued."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    before = await _stored("r1")
    pool = _FakePool([ResponseSealedError("r1")])
    save_logs = mocker.patch.object(merge_worker, "save_logs", new=mocker.AsyncMock())

    with caplog.at_level(logging.WARNING):
        reenqueued = await _drive_merge(mocker, pool, _wake_task())

    assert len(pool.calls) == 1
    reenqueued.assert_not_awaited()
    assert await db.get_ready_callbacks("r1", logger) == []
    assert not await db.message_exists("cb1")
    assert await db.get_merge_crashes("r1") == 0
    assert await _stored("r1") == before
    save_logs.assert_not_awaited()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "r1" in warnings[0].getMessage()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


async def test_merge_does_not_start_a_pass_for_a_finished_query(
    merge_query, mocker, monkeypatch
):
    monkeypatch.setattr(settings, "max_response_size", "0")
    await db.seal_response("r1", logger)
    pool = _FakePool([])

    reenqueued = await _drive_merge(mocker, pool, _wake_task())

    assert pool.calls == []
    reenqueued.assert_not_awaited()
    assert await db.get_ready_callbacks("r1", logger) == []


# --- finish_query -------------------------------------------------------------


def _finish_task(query_id, response_id, **fields):
    return ["msg", {"query_id": query_id, "response_id": response_id, **fields}]


@pytest.fixture
def archive_store(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    return tmp_path


async def test_finish_query_delivers_and_archives_exactly_the_sealed_bytes(
    redis_mock, mocker, archive_store
):
    """A late writer during the delivery is refused, so the callback, the
    archive and the stored copy all agree."""
    from shepherd_utils import response_archive

    query_id, response_id = str(uuid.uuid4()), str(uuid.uuid4())
    await db.save_message(query_id, {"message": {}}, logger)
    await db.save_response(response_id, ORIGINAL, logger)
    mocker.patch.object(
        finish_worker,
        "get_query_state",
        return_value=[""] * 7 + [response_id, "http://callback/1"],
    )
    mocker.patch.object(finish_worker, "set_query_completed")
    refusals = []

    async def _deliver(url, payload, task_logger, has_logs=True):
        try:
            await db.save_response(response_id, LATE, task_logger)
        except ResponseSealedError as e:
            refusals.append(e)
        return True

    sent = mocker.patch.object(finish_worker, "send_callback", side_effect=_deliver)

    await finish_worker.finish_query(_finish_task(query_id, response_id), logger)

    assert len(refusals) == 1
    stored = await _stored(response_id)
    stored_json = db.decompress_zstd(stored)
    delivered = orjson.loads(sent.await_args.args[1])
    assert delivered["message"] == orjson.loads(stored_json)["message"]
    archive = await response_archive.load_archive(response_id, logger)
    assert archive.response == stored_json
    assert await db.is_response_sealed(response_id)
    # The seal outlives the response, whose TTL archiving shortened.
    assert await db.data_db_client.ttl(
        db.sealed_key(response_id)
    ) >= await db.data_db_client.ttl(response_id)


async def test_finish_query_hands_the_ars_exactly_the_sealed_bytes(
    redis_mock, mocker, archive_store
):
    from shepherd_utils import response_archive
    from shepherd_utils.ars.handoff import handoff_callback_url

    query_id, response_id = str(uuid.uuid4()), str(uuid.uuid4())
    await db.save_response(response_id, ORIGINAL, logger)
    mocker.patch.object(
        finish_worker,
        "get_query_state",
        return_value=[""] * 7 + [response_id, handoff_callback_url(uuid.uuid4())],
    )
    mocker.patch.object(finish_worker, "set_query_completed")
    handed = {}

    async def _hand_off(stream, payload, task_logger, **kwargs):
        # The ARS intake reads the response by id; whatever it reads must be
        # what stays stored, however late another task tries to write.
        handed["sealed"] = await db.is_response_sealed(payload["response_id"])
        handed["bytes"] = await _stored(payload["response_id"])
        with pytest.raises(ResponseSealedError):
            await db.save_response(payload["response_id"], LATE, task_logger)

    mocker.patch.object(finish_worker, "add_task", side_effect=_hand_off)

    await finish_worker.finish_query(_finish_task(query_id, response_id), logger)

    assert handed["sealed"]
    assert handed["bytes"] == await _stored(response_id)
    archive = await response_archive.load_archive(response_id, logger)
    assert archive.response == db.decompress_zstd(handed["bytes"])


async def test_finish_querys_own_too_large_rewrite_still_lands(redis_mock, mocker):
    """The rewrite is finish_query's one legitimate write, made before the
    seal; a merge pass that saved the big accumulator before the claim is
    overwritten, and nothing after the seal is."""
    from shepherd_utils.response_limit import fail_response_too_large

    mocker.patch("shepherd_utils.db.cleanup_callbacks", new=mocker.AsyncMock())
    await db.save_message("q1", QUERY, logger)
    await fail_response_too_large("q1", "r1", "way too big", logger)
    await db.save_message("r1", _big_message(), logger)  # in-flight merge pass
    mocker.patch.object(
        finish_worker,
        "get_query_state",
        return_value=[""] * 7 + ["r1", "http://callback/1"],
    )
    mocker.patch.object(finish_worker, "set_query_completed")
    mocker.patch.object(
        finish_worker, "send_callback", new=mocker.AsyncMock(return_value=True)
    )

    await finish_worker.finish_query(_finish_task("q1", "r1"), logger)

    stored = await db.get_message("r1", logger)
    assert stored["message"]["results"] == []
    assert stored["description"] == "Response too large: way too big"
    with pytest.raises(ResponseSealedError):
        await db.save_message("r1", _big_message(), logger)
    assert (await db.get_message("r1", logger)) == stored


async def test_a_finish_that_takes_over_a_sealed_query_still_finishes_it(
    redis_mock, mocker
):
    """A finisher that crashed after sealing leaves its claim to expire and be
    taken over. The takeover's too-large rewrite is refused -- the sealed
    copy is the same message -- and the query is still delivered and
    completed."""
    from shepherd_utils.response_limit import mark_response_too_large

    await db.save_message("q1", QUERY, logger)
    await mark_response_too_large("r1", "way too big")
    await db.save_response("r1", ORIGINAL, logger)
    await db.seal_response("r1", logger)
    before = await _stored("r1")
    mocker.patch.object(
        finish_worker,
        "get_query_state",
        return_value=[""] * 7 + ["r1", "http://callback/1"],
    )
    completed = mocker.patch.object(finish_worker, "set_query_completed")
    sent = mocker.patch.object(
        finish_worker, "send_callback", new=mocker.AsyncMock(return_value=True)
    )

    await finish_worker.finish_query(_finish_task("q1", "r1"), logger)

    assert await _stored("r1") == before
    sent.assert_awaited_once()
    completed.assert_awaited_once()


async def test_a_finish_that_loses_the_claim_does_not_seal(redis_mock, mocker):
    """Only the finisher that won the claim seals; a loser must not touch a
    query another finisher is still working on."""
    mocker.patch.object(
        finish_worker,
        "get_query_state",
        return_value=[""] * 7 + ["r1", None],
    )
    mocker.patch.object(finish_worker, "_wait_for_claim", return_value=False)

    await finish_worker.finish_query(_finish_task("q1", "r1"), logger)

    assert not await db.is_response_sealed("r1")
