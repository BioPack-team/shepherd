"""The archive of finished ARA responses (shepherd_utils.response_archive):
finish_query's copy of a response and its logs in the cold store, and the
read paths that fall back to it once the Redis copy has expired.
"""

import logging
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
def redis(monkeypatch):
    """What Redis holds for the query: its response blob and its log list."""
    state = {"blob": zstandard.compress(RESPONSE), "logs": LOGS}
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "get",
        AsyncMock(side_effect=lambda *a: state["blob"]),
    )
    monkeypatch.setattr(
        shepherd_db.logs_db_client,
        "lrange",
        AsyncMock(side_effect=lambda *a: [orjson.dumps(e) for e in state["logs"]]),
    )
    monkeypatch.setattr(response_archive.asyncio, "sleep", AsyncMock())
    return state


async def test_a_finished_response_is_archived_as_the_frame_redis_held(store, redis):
    response_id = str(uuid.uuid4())

    assert await response_archive.archive_response(response_id, LOGGER)

    assert payload_store.read(response_id, payload_store.ARA) == redis["blob"]
    # in its own directory, not among the ARS messages
    assert payload_store.read(response_id) is None
    assert await response_archive.load_response_bytes(response_id, LOGGER) == RESPONSE
    assert response_archive.load_response_sync(response_id) == orjson.loads(RESPONSE)


async def test_logs_are_archived_and_read_back_in_order(store, redis):
    response_id = str(uuid.uuid4())

    assert await response_archive.archive_logs(response_id, LOGGER)

    logs = await response_archive.load_logs(response_id, LOGGER)
    assert [entry["message"] for entry in logs] == ["first", "second"]


async def test_nothing_in_redis_archives_nothing(store, redis):
    redis["blob"] = None
    redis["logs"] = []
    response_id = str(uuid.uuid4())

    assert not await response_archive.archive_response(response_id, LOGGER)
    assert not await response_archive.archive_logs(response_id, LOGGER)

    assert await response_archive.load_response_bytes(response_id, LOGGER) is None
    assert await response_archive.load_logs(response_id, LOGGER) == []


async def test_without_a_store_nothing_is_archived_or_read(monkeypatch, redis):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    monkeypatch.setattr(settings, "payload_s3_bucket", "")
    response_id = str(uuid.uuid4())

    assert not await response_archive.archive_response(response_id, LOGGER)
    assert not await response_archive.archive_logs(response_id, LOGGER)
    shepherd_db.data_db_client.get.assert_not_called()
    assert await response_archive.load_response_bytes(response_id, LOGGER) is None
    assert response_archive.load_response_sync(response_id) is None


async def test_a_failing_store_is_retried_then_given_up_on_without_raising(
    store, redis, monkeypatch
):
    calls = []

    def failing_write(*args):
        calls.append(args)
        raise OSError("No space left on device")

    monkeypatch.setattr(payload_store, "write", failing_write)

    assert not await response_archive.archive_response(str(uuid.uuid4()), LOGGER)
    assert len(calls) == response_archive.ARCHIVE_ATTEMPTS


async def test_a_transient_failure_is_retried_to_success(store, redis, monkeypatch):
    real_write = payload_store.write
    attempts = []

    def flaky_write(*args):
        attempts.append(args)
        if len(attempts) == 1:
            raise OSError("temporarily unavailable")
        return real_write(*args)

    monkeypatch.setattr(payload_store, "write", flaky_write)
    response_id = str(uuid.uuid4())

    assert await response_archive.archive_response(response_id, LOGGER)
    assert payload_store.read(response_id, payload_store.ARA) == redis["blob"]


async def test_a_redis_error_never_escapes(store, redis, monkeypatch):
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "get",
        AsyncMock(side_effect=ConnectionError("redis is down")),
    )
    assert not await response_archive.archive_response(str(uuid.uuid4()), LOGGER)


@pytest.mark.parametrize("bad_id", ["../../etc/passwd", "abcd1234", "", None])
async def test_an_id_that_is_not_a_uuid_has_no_archive(store, redis, bad_id):
    assert not await response_archive.archive_response(bad_id, LOGGER)
    assert await response_archive.load_response_bytes(bad_id, LOGGER) is None
    assert await response_archive.load_logs(bad_id, LOGGER) == []
    assert response_archive.load_response_sync(bad_id) is None
    shepherd_db.data_db_client.get.assert_not_called()


async def test_an_unreadable_archived_copy_reads_as_absent(store, redis):
    response_id = str(uuid.uuid4())
    payload_store.write(response_id, b"not a zstd frame", payload_store.ARA)
    payload_store.write(response_id, b"not a zstd frame", payload_store.ARA_LOGS)

    assert await response_archive.load_response_bytes(response_id, LOGGER) is None
    assert response_archive.load_response_sync(response_id) is None
    assert await response_archive.load_logs(response_id, LOGGER) == []


async def test_a_store_that_hangs_costs_the_copy_not_the_query(
    store, redis, monkeypatch
):
    """The write is abandoned at the deadline instead of holding up the
    delivery finish_query makes next."""
    import asyncio
    import threading

    release = threading.Event()
    monkeypatch.setattr(payload_store, "write", lambda *a: release.wait(5))
    monkeypatch.setattr(response_archive, "ARCHIVE_TIMEOUT_SEC", 0.05)
    try:
        archived = await asyncio.wait_for(
            response_archive.archive_response(str(uuid.uuid4()), LOGGER), timeout=2
        )
    finally:
        release.set()
    assert not archived
