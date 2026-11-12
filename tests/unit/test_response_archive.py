"""The archive of finished ARA queries (shepherd_utils.response_archive): one
object per query in the cold store -- the response, the query and the logs
as three zstd frames -- built without decoding the response, and taken apart
again by the read paths once the Redis copy has expired.
"""

import asyncio
import logging
import threading
import uuid
from unittest.mock import AsyncMock

import orjson
import pytest
import zstandard

import shepherd_utils.db as shepherd_db
from shepherd_utils import response_archive
from shepherd_utils.ars import payload_store
from shepherd_utils.config import settings

LOGGER = logging.getLogger(__name__)
RESPONSE = b'{"message":{"results":[]}}'
QUERY = {"message": {"query_graph": {}}, "parameters": {"log_level": "DEBUG"}}
LOGS = [
    {"timestamp": "2026-01-01T00:00:02Z", "level": "INFO", "message": "second"},
    {"timestamp": "2026-01-01T00:00:01Z", "level": "INFO", "message": "first"},
]


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    monkeypatch.setattr(settings, "payload_s3_bucket", "")
    return tmp_path


@pytest.fixture
def ids():
    return {"query": str(uuid.uuid4()), "response": str(uuid.uuid4())}


@pytest.fixture
def redis(monkeypatch, ids):
    """What Redis holds for the query: its blobs by id, and its log list."""
    state = {
        "blobs": {
            ids["response"]: zstandard.compress(RESPONSE),
            ids["query"]: zstandard.compress(orjson.dumps(QUERY)),
        },
        "logs": LOGS,
    }
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "get",
        AsyncMock(side_effect=lambda key: state["blobs"].get(key)),
    )
    monkeypatch.setattr(
        shepherd_db.logs_db_client,
        "lrange",
        AsyncMock(side_effect=lambda *a: [orjson.dumps(e) for e in state["logs"]]),
    )
    monkeypatch.setattr(response_archive.asyncio, "sleep", AsyncMock())
    return state


async def _archive(ids, status="OK", description=None):
    blob = await response_archive.capture_response(ids["response"], LOGGER)
    return await response_archive.archive_query(
        ids["query"], ids["response"], blob, status, description, LOGGER
    )


async def test_a_finished_query_is_one_object_of_three_frames(store, redis, ids):
    assert await _archive(ids)

    blob = payload_store.read(ids["response"], payload_store.ARA)
    # the response and query frames are the bytes Redis held, untouched
    assert blob.startswith(
        redis["blobs"][ids["response"]] + redis["blobs"][ids["query"]]
    )
    response, query, tail = response_archive.split_frames(blob)
    assert response == RESPONSE
    assert orjson.loads(query) == QUERY
    assert orjson.loads(tail) == {"status": "OK", "description": None, "logs": LOGS}
    # in its own directory, not among the ARS messages
    assert payload_store.read(ids["response"]) is None


async def test_it_is_read_back_taken_apart(store, redis, ids):
    await _archive(ids, status="ERROR", description="BTE refused it")

    archive = await response_archive.load_archive(ids["response"], LOGGER)

    assert archive.response == RESPONSE
    assert archive.query == QUERY
    assert (archive.status, archive.description) == ("ERROR", "BTE refused it")
    assert [entry["message"] for entry in archive.logs] == ["first", "second"]
    assert response_archive.load_archive_sync(ids["response"]) == archive


async def test_the_response_and_query_are_never_decoded(store, redis, ids, monkeypatch):
    """finish_query's memory is what limits it: archiving must only ever
    hold compressed bytes."""

    def _refuse(*args, **kwargs):
        raise AssertionError("archiving decoded a payload")

    monkeypatch.setattr(shepherd_db, "decompress_zstd", _refuse)
    monkeypatch.setattr(shepherd_db, "decompress_frame", _refuse)
    monkeypatch.setattr(shepherd_db, "decode_message", _refuse)
    monkeypatch.setattr(zstandard, "decompress", _refuse)
    monkeypatch.setattr(zstandard, "ZstdDecompressor", _refuse)

    assert await _archive(ids)


async def test_a_query_that_already_expired_is_archived_as_null(store, redis, ids):
    del redis["blobs"][ids["query"]]

    assert await _archive(ids)

    archive = await response_archive.load_archive(ids["response"], LOGGER)
    assert archive.query is None
    assert archive.response == RESPONSE


async def test_a_query_with_no_logs_archives_an_empty_list(store, redis, ids):
    redis["logs"] = []
    assert await _archive(ids)
    archive = await response_archive.load_archive(ids["response"], LOGGER)
    assert archive.logs == []


async def test_redis_trouble_reading_the_query_or_logs_still_archives(
    store, redis, ids, monkeypatch
):
    blob = await response_archive.capture_response(ids["response"], LOGGER)
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "get",
        AsyncMock(side_effect=ConnectionError("redis is down")),
    )
    monkeypatch.setattr(
        shepherd_db.logs_db_client,
        "lrange",
        AsyncMock(side_effect=ConnectionError("redis is down")),
    )

    assert await response_archive.archive_query(
        ids["query"], ids["response"], blob, "OK", None, LOGGER
    )

    archive = await response_archive.load_archive(ids["response"], LOGGER)
    assert archive.response == RESPONSE
    assert archive.query is None and archive.logs == []


async def test_no_response_in_redis_archives_nothing(store, redis, ids):
    del redis["blobs"][ids["response"]]

    assert await response_archive.capture_response(ids["response"], LOGGER) is None
    assert not await _archive(ids)
    assert await response_archive.load_archive(ids["response"], LOGGER) is None


async def test_a_redis_error_capturing_the_response_never_escapes(
    store, redis, ids, monkeypatch
):
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "get",
        AsyncMock(side_effect=ConnectionError("redis is down")),
    )
    assert await response_archive.capture_response(ids["response"], LOGGER) is None


async def test_a_response_that_is_not_a_zstd_frame_is_not_archived(store, redis, ids):
    assert not await response_archive.archive_query(
        ids["query"], ids["response"], b'{"plain": "json"}', "OK", None, LOGGER
    )
    assert not payload_store.exists(ids["response"], payload_store.ARA)


async def test_without_a_store_nothing_is_archived_or_read(monkeypatch, redis, ids):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    monkeypatch.setattr(settings, "payload_s3_bucket", "")

    assert await response_archive.capture_response(ids["response"], LOGGER) is None
    shepherd_db.data_db_client.get.assert_not_called()
    assert not await response_archive.archive_query(
        ids["query"], ids["response"], b"blob", "OK", None, LOGGER
    )
    assert await response_archive.load_archive(ids["response"], LOGGER) is None
    assert response_archive.load_archive_sync(ids["response"]) is None


async def test_a_failing_store_is_retried_then_given_up_on_without_raising(
    store, redis, ids, monkeypatch
):
    calls = []

    def failing_write(*args):
        calls.append(args)
        raise OSError("No space left on device")

    monkeypatch.setattr(payload_store, "write", failing_write)

    assert not await _archive(ids)
    assert len(calls) == response_archive.ARCHIVE_ATTEMPTS


async def test_a_transient_failure_is_retried_to_success(
    store, redis, ids, monkeypatch
):
    real_write = payload_store.write
    attempts = []

    def flaky_write(*args):
        attempts.append(args)
        if len(attempts) == 1:
            raise OSError("temporarily unavailable")
        return real_write(*args)

    monkeypatch.setattr(payload_store, "write", flaky_write)

    assert await _archive(ids)
    assert (
        await response_archive.load_archive(ids["response"], LOGGER)
    ).response == RESPONSE


async def test_a_store_that_hangs_costs_the_copy_not_the_worker(
    store, redis, ids, monkeypatch
):
    """The write is abandoned at the deadline instead of holding the
    finish_query task (and its slot) for as long as the store hangs."""
    release = threading.Event()
    monkeypatch.setattr(payload_store, "write", lambda *a: release.wait(5))
    monkeypatch.setattr(response_archive, "ARCHIVE_TIMEOUT_SEC", 0.05)
    try:
        archived = await asyncio.wait_for(_archive(ids), timeout=2)
    finally:
        release.set()
    assert not archived


@pytest.mark.parametrize("bad_id", ["../../etc/passwd", "abcd1234", "", None])
async def test_an_id_that_is_not_a_uuid_has_no_archive(store, redis, ids, bad_id):
    assert await response_archive.capture_response(bad_id, LOGGER) is None
    assert not await response_archive.archive_query(
        ids["query"], bad_id, zstandard.compress(RESPONSE), "OK", None, LOGGER
    )
    assert await response_archive.load_archive(bad_id, LOGGER) is None
    assert response_archive.load_archive_sync(bad_id) is None
    shepherd_db.data_db_client.get.assert_not_called()


@pytest.mark.parametrize(
    "stored",
    [
        b"not a zstd frame",
        zstandard.compress(RESPONSE),  # one frame: not an archive
        zstandard.compress(RESPONSE)[:-3],  # truncated
        zstandard.compress(RESPONSE) * 2 + zstandard.compress(b"[]"),  # bad tail
    ],
)
async def test_an_unreadable_archived_object_reads_as_absent(store, ids, stored):
    payload_store.write(ids["response"], stored, payload_store.ARA)

    assert await response_archive.load_archive(ids["response"], LOGGER) is None
    with pytest.raises(Exception):
        response_archive.load_archive_sync(ids["response"])


# --- an ARS tree owns its children's archives (#252 stage 3) -----------------


@pytest.fixture
def pg(monkeypatch):
    """Mock pool recording the SQL the archive runs."""
    from unittest.mock import MagicMock

    state = {"sql": []}

    async def _execute(sql, params=None):
        state["sql"].append((sql, params))
        return MagicMock()

    conn = AsyncMock()
    conn.execute.side_effect = _execute
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)
    state["conn"] = conn
    return state


async def test_the_archives_size_is_recorded_on_the_ars_child_it_belongs_to(
    store, redis, ids, pg
):
    """An ARS child's pk is its ARA's response id. Recording the archive's
    size there is what puts it under the tree's retention; for a directly
    submitted query the UPDATE matches no row."""
    assert await _archive(ids)

    blob = payload_store.read(ids["response"], payload_store.ARA)
    ((sql, params),) = pg["sql"]
    assert "UPDATE ars_message SET data_size = %s" in sql
    assert "updated_at" not in sql  # bookkeeping, not a change to the message
    assert params == (len(blob), uuid.UUID(ids["response"]))
    pg["conn"].commit.assert_awaited_once()


async def test_no_size_is_recorded_for_an_archive_that_was_not_written(
    store, redis, ids, pg, monkeypatch
):
    monkeypatch.setattr(
        payload_store, "write", lambda *a: (_ for _ in ()).throw(OSError("disk full"))
    )
    assert not await _archive(ids)
    assert pg["sql"] == []


async def test_failing_to_record_the_size_does_not_undo_the_archive(
    store, redis, ids, monkeypatch
):
    from unittest.mock import MagicMock

    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("postgres is down")
    monkeypatch.setattr(shepherd_db, "pool", pool)

    assert await _archive(ids)
    assert payload_store.exists(ids["response"], payload_store.ARA)
