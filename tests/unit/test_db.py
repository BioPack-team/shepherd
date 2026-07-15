"""Tests for ``shepherd_utils.db`` helpers.

Covers the pure codecs (``encode_message``/``decode_message``) and the
Redis-backed read/write functions that don't touch postgres. Postgres-backed
helpers (``add_query``, ``add_callback_id`` etc.) are exercised via
``postgres_mock`` from the conftest.
"""

import io
import logging

import orjson
import pytest
import zstandard

from shepherd_utils.config import settings
from shepherd_utils.db import (
    ResponseTooLargeError,
    decode_message,
    decompress_zstd,
    encode_message,
    enforce_response_size_limit,
    get_blob_size,
    get_logs,
    get_message,
    get_message_sync,
    get_response_size,
    save_logs,
    save_message,
    save_message_sync,
)
from shepherd_utils.logger import QueryLogger

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_blob_size_reports_stored_bytes(redis_mock):
    """get_blob_size returns the STRLEN of the stored (compressed) blob, and 0
    for a missing key -- without transferring the payload."""
    await save_message("sizekey", {"message": {"results": [{"score": 1}]}}, logger)
    size = await get_blob_size("sizekey")
    assert size > 0
    # Matches the raw stored length exactly.
    assert size == await redis_mock["data"].strlen("sizekey")
    # Missing key reads as 0, not an error.
    assert await get_blob_size("nope") == 0


@pytest.mark.asyncio
async def test_get_response_size_reports_uncompressed_bytes(redis_mock):
    """get_response_size returns the UNCOMPRESSED size (from the zstd frame
    header), which equals the JSON length -- read without loading the blob."""
    payload = {
        "message": {"results": [{"analyses": [{"score": i}]} for i in range(50)]}
    }
    await save_message("ukey", payload, logger)
    uncompressed = len(orjson.dumps(payload))
    assert await get_response_size("ukey") == uncompressed
    # The uncompressed size exceeds the compressed one for a real payload.
    assert await get_response_size("ukey") > await get_blob_size("ukey")
    # Missing key reads as 0.
    assert await get_response_size("nope") == 0


@pytest.mark.asyncio
async def test_enforce_response_size_limit_disabled_by_default(redis_mock, monkeypatch):
    """With the guard off (0), even a present blob passes without a size read."""
    monkeypatch.setattr(settings, "max_response_size", "0")
    await save_message("k", {"message": {"results": []}}, logger)
    # Should simply return, never raise.
    await enforce_response_size_limit("k", logger)


@pytest.mark.asyncio
async def test_enforce_response_size_limit_raises_when_over(redis_mock, monkeypatch):
    """A blob larger than the configured cap raises before it is ever loaded."""
    monkeypatch.setattr(settings, "max_response_size", "1")  # 1 byte cap
    await save_message("big", {"message": {"results": [{"score": 1}]}}, logger)
    with pytest.raises(ResponseTooLargeError):
        await enforce_response_size_limit("big", logger)


@pytest.mark.asyncio
async def test_enforce_response_size_limit_allows_under(redis_mock, monkeypatch):
    """A blob under the cap passes."""
    monkeypatch.setattr(settings, "max_response_size", "100Mi")
    await save_message("small", {"message": {"results": [{"score": 1}]}}, logger)
    await enforce_response_size_limit("small", logger)


def test_encode_decode_roundtrip_preserves_payload():
    payload = {
        "message": {
            "results": [{"score": 0.5}],
            "knowledge_graph": {"nodes": {"A": {}}, "edges": {}},
        }
    }
    encoded = encode_message(payload)
    assert isinstance(encoded, (bytes, bytearray))
    assert decode_message(encoded) == payload


def test_encode_message_compresses_repeating_input():
    """zstd should achieve good compression on a redundant payload."""
    big_payload = {"message": {"results": [{"x": "y" * 1000}] * 50}}
    encoded = encode_message(big_payload)
    assert len(encoded) < len(orjson.dumps(big_payload))


def test_decompress_zstd_embedded_size_frame():
    """A frame compressed with the one-shot API embeds the content size."""
    raw = orjson.dumps({"message": {"results": [{"score": 0.5}]}})
    frame = zstandard.compress(raw)
    assert decompress_zstd(frame) == raw


def test_decompress_zstd_streaming_frame_without_content_size():
    """Streaming frames omit the content size; the one-shot API can't read them.

    This is the case ``decompress_zstd`` exists to handle: external services
    that compress in streaming mode produce frames the one-shot
    ``zstandard.decompress`` rejects.
    """
    raw = orjson.dumps({"message": {"results": [{"score": 0.5}]}})
    buf = io.BytesIO()
    with zstandard.ZstdCompressor().stream_writer(buf, closefd=False) as writer:
        writer.write(raw)
    frame = buf.getvalue()

    assert decompress_zstd(frame) == raw
    with pytest.raises(zstandard.ZstdError):
        zstandard.decompress(frame)


def test_decompress_zstd_rejects_garbage():
    """Non-zstd bytes raise ZstdError so the route can map it to a 422."""
    with pytest.raises(zstandard.ZstdError):
        decompress_zstd(b"not a zstd frame")


@pytest.mark.asyncio
async def test_save_and_get_message_roundtrip(redis_mock):
    payload = {"message": {"results": [{"score": 0.42}]}}
    await save_message("rid-1", payload, logger)
    fetched = await get_message("rid-1", logger)
    assert fetched == payload


@pytest.mark.asyncio
async def test_get_message_raises_keyerror_for_missing(redis_mock):
    with pytest.raises(KeyError, match="missing-id"):
        await get_message("missing-id", logger)


def test_save_and_get_message_sync_roundtrip(redis_mock, mocker):
    """The sync variants are used inside ProcessPoolExecutor workers; route
    them through fakeredis by patching the lazy client accessor."""
    sync_client = mocker.Mock()
    storage = {}

    def _set(key, blob, ex=None):
        storage[key] = blob

    def _get(key):
        return storage.get(key)

    sync_client.set.side_effect = _set
    sync_client.get.side_effect = _get
    mocker.patch("shepherd_utils.db._get_sync_data_db", return_value=sync_client)

    payload = {"message": {"foo": "bar"}}
    save_message_sync("sid-1", payload)
    assert get_message_sync("sid-1") == payload


def test_get_message_sync_raises_keyerror_for_missing(mocker):
    sync_client = mocker.Mock()
    sync_client.get.return_value = None
    mocker.patch("shepherd_utils.db._get_sync_data_db", return_value=sync_client)

    with pytest.raises(KeyError, match="missing-sid"):
        get_message_sync("missing-sid")


@pytest.mark.asyncio
async def test_save_logs_appends_query_log_handler_records(redis_mock):
    """save_logs reads logs from a QueryLogHandler attached to the logger and
    persists them (newest-first reversed) into the logs db."""
    handler = QueryLogger().log_handler
    sub_logger = logging.getLogger("test.save_logs.appends")
    sub_logger.handlers.clear()
    sub_logger.addHandler(handler)
    sub_logger.setLevel(logging.DEBUG)
    sub_logger.info("first message")
    sub_logger.info("second message")
    try:
        await save_logs("resp-1", sub_logger)
    finally:
        sub_logger.removeHandler(handler)

    raw = await redis_mock["logs"].get("resp-1")
    assert raw is not None
    logs = orjson.loads(raw)
    messages = [entry["message"] for entry in logs]
    # Insertion order: handler emits to a deque (appendleft), reversed in
    # save_logs, so logs end up oldest-first.
    assert messages == ["first message", "second message"]


@pytest.mark.asyncio
async def test_save_logs_extends_existing_logs(redis_mock):
    """A pre-existing log array in redis is preserved and extended."""
    existing = [
        {"message": "from-earlier", "timestamp": "2024-01-01T00:00:00", "level": "INFO"}
    ]
    await redis_mock["logs"].set("resp-2", orjson.dumps(existing))

    handler = QueryLogger().log_handler
    sub_logger = logging.getLogger("test.save_logs.extends")
    sub_logger.handlers.clear()
    sub_logger.addHandler(handler)
    sub_logger.setLevel(logging.DEBUG)
    sub_logger.info("new entry")
    try:
        await save_logs("resp-2", sub_logger)
    finally:
        sub_logger.removeHandler(handler)

    raw = await redis_mock["logs"].get("resp-2")
    logs = orjson.loads(raw)
    assert [entry["message"] for entry in logs] == ["from-earlier", "new entry"]


@pytest.mark.asyncio
async def test_get_logs_returns_empty_list_when_missing(redis_mock):
    """Reading logs for an unknown response id should return an empty list."""
    out = await get_logs("does-not-exist", logger)
    assert out == []


@pytest.mark.asyncio
async def test_get_logs_returns_stored_logs(redis_mock):
    stored = [{"message": "hello", "timestamp": "ts", "level": "INFO"}]
    await redis_mock["logs"].set("resp-3", orjson.dumps(stored))
    out = await get_logs("resp-3", logger)
    assert out == stored


@pytest.mark.asyncio
async def test_get_logs_sorts_by_timestamp(redis_mock):
    """Logs are flushed in worker/callback order, not event order, so get_logs
    must return them sorted chronologically by their ISO8601 timestamp."""
    # Stored out of order: a merge log flushed before the lookup that preceded it.
    stored = [
        {"message": "merge", "timestamp": "2026-07-13T00:00:02+00:00", "level": "INFO"},
        {
            "message": "lookup",
            "timestamp": "2026-07-13T00:00:01+00:00",
            "level": "INFO",
        },
        {
            "message": "finish",
            "timestamp": "2026-07-13T00:00:03+00:00",
            "level": "INFO",
        },
    ]
    await redis_mock["logs"].set("resp-sort", orjson.dumps(stored))
    out = await get_logs("resp-sort", logger)
    assert [entry["message"] for entry in out] == ["lookup", "merge", "finish"]


@pytest.mark.asyncio
async def test_get_logs_sort_is_stable_for_missing_timestamps(redis_mock):
    """Entries lacking a timestamp must not blow up the sort and should keep
    their relative order (stable sort with an empty-string key)."""
    stored = [
        {"message": "a"},
        {"message": "b", "timestamp": "2026-07-13T00:00:01+00:00"},
        {"message": "c"},
    ]
    await redis_mock["logs"].set("resp-missing", orjson.dumps(stored))
    out = await get_logs("resp-missing", logger)
    # "a" and "c" (empty key) sort before "b" and keep their input order.
    assert [entry["message"] for entry in out] == ["a", "c", "b"]


@pytest.mark.asyncio
async def test_save_message_retries_on_failure(redis_mock, mocker):
    """The first call to data_db_client.set raises; save_message should sleep
    and retry rather than dropping the message."""
    real_set = redis_mock["data"].set
    set_mock = mocker.AsyncMock()
    call_state = {"calls": 0}

    async def flaky_set(*args, **kwargs):
        call_state["calls"] += 1
        if call_state["calls"] == 1:
            raise RuntimeError("simulated transient failure")
        return await real_set(*args, **kwargs)

    set_mock.side_effect = flaky_set
    mocker.patch("shepherd_utils.db.data_db_client.set", set_mock)

    # Patch sleep so the test doesn't block.
    mocker.patch("asyncio.sleep", new=mocker.AsyncMock())

    await save_message("retry-1", {"a": 1}, logger)
    # First call failed, second call succeeded via real_set; expect at least 2.
    assert call_state["calls"] >= 2
    assert await get_message("retry-1", logger) == {"a": 1}
