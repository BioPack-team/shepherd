"""The /callback route stores what it was posted without parsing it.

Parsing a large TRAPI message holds the GIL for seconds, which is long enough
for uvicorn's supervisor to take the server worker for hung and kill it. So the
route only stores the body (``encode_callback_body``) and merge_message parses
it (``read_callback_response``), finding there any body that isn't a TRAPI
response.
"""

import copy
import io
import logging

import orjson
import pytest
import zstandard
from starlette.requests import Request

from shepherd_server import base_routes
from shepherd_utils import db
from shepherd_server.base_routes import ARATargetEnum, callback
from shepherd_utils.db import (
    DecompressedTooLargeError,
    NotAJSONObjectError,
    decode_message,
    encode_callback_body,
    get_message,
    get_message_sync,
    save_message,
    save_message_sync,
)
from tests.helpers.generate_messages import generate_response, response_1, response_2
from workers.merge_message.worker import merge_messages_by_ids, read_callback_response

logger = logging.getLogger(__name__)

BODY = {"message": {"results": [{"score": 0.5}], "knowledge_graph": None}}


def streaming_frame(data: bytes) -> bytes:
    """A zstd frame that omits its content size, as a streaming sender writes."""
    buf = io.BytesIO()
    with zstandard.ZstdCompressor().stream_writer(buf, closefd=False) as writer:
        writer.write(data)
    return buf.getvalue()


# --- encode_callback_body ----------------------------------------------------


def test_a_json_body_is_compressed_with_its_size():
    raw = orjson.dumps(BODY)

    blob, size = encode_callback_body(raw, zstd_encoded=False)

    assert size == len(raw)
    assert zstandard.frame_content_size(blob) == len(raw)
    assert decode_message(blob) == BODY


def test_a_zstd_body_with_its_size_is_stored_as_it_came():
    raw = orjson.dumps(BODY)
    frame = zstandard.compress(raw)

    blob, size = encode_callback_body(frame, zstd_encoded=True, max_bytes=10_000)

    assert blob is frame
    assert size == len(raw)


def test_a_streaming_zstd_body_is_recompressed_with_its_size():
    raw = orjson.dumps(BODY)

    blob, size = encode_callback_body(streaming_frame(raw), zstd_encoded=True)

    assert size == len(raw)
    assert zstandard.frame_content_size(blob) == len(raw)
    assert decode_message(blob) == BODY


def test_a_stored_streaming_frame_still_decodes():
    raw = orjson.dumps(BODY)

    assert decode_message(streaming_frame(raw)) == BODY


@pytest.mark.parametrize("streaming", [False, True])
def test_the_cap_is_on_the_decompressed_size(streaming):
    raw = orjson.dumps({"message": {"pad": "x" * 5000}})
    frame = streaming_frame(raw) if streaming else zstandard.compress(raw)

    with pytest.raises(DecompressedTooLargeError):
        encode_callback_body(frame, zstd_encoded=True, max_bytes=1000)
    # no cap
    assert encode_callback_body(frame, zstd_encoded=True)[1] == len(raw)


@pytest.mark.parametrize(
    "body, zstd_encoded",
    [
        (b"this is not json", False),
        (b"  [1, 2]", False),
        (b"", False),
        (zstandard.compress(b"not json"), True),
        (streaming_frame(b"[]"), True),
    ],
)
def test_a_body_that_is_not_a_json_object_is_refused(body, zstd_encoded):
    with pytest.raises(NotAJSONObjectError):
        encode_callback_body(body, zstd_encoded)


def test_a_bad_zstd_frame_is_refused():
    with pytest.raises(zstandard.ZstdError):
        encode_callback_body(b"not a zstd frame", zstd_encoded=True)


# --- the route ---------------------------------------------------------------


def make_request(body: bytes, headers=None) -> Request:
    headers = {"content-length": len(body), **(headers or {})}
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/aragorn/callback/cb-1",
        "headers": [(k.encode(), str(v).encode()) for k, v in headers.items()],
    }
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


@pytest.fixture
def route(redis_mock, monkeypatch, mocker):
    """The route with its Postgres lookups stubbed; returns the enqueued tasks
    and the callbacks recorded as ready."""
    tasks, ready = [], []

    async def _get_callback_query_id(callback_id, logger):
        return ("q-1", "{}")

    async def _get_query_state(query_id, logger):
        return [None] * 7 + ["resp-1"]

    async def _add_ready_callback(response_id, callback_id, logger):
        ready.append((response_id, callback_id))

    async def _add_task(stream, fields, logger):
        tasks.append((stream, fields))

    async def _save_logs(response_id, logger):
        return None

    monkeypatch.setattr(base_routes, "get_callback_query_id", _get_callback_query_id)
    monkeypatch.setattr(base_routes, "get_query_state", _get_query_state)
    monkeypatch.setattr(base_routes, "add_ready_callback", _add_ready_callback)
    monkeypatch.setattr(base_routes, "add_task", _add_task)
    monkeypatch.setattr(base_routes, "save_logs", _save_logs)
    # what the route parses: the stored query, but never the posted body
    loads = mocker.patch("orjson.loads", wraps=orjson.loads)
    return tasks, ready, loads


async def test_the_route_stores_the_body_without_parsing_it(route):
    tasks, ready, loads = route
    await save_message("q-1", {"message": {}}, logger)
    raw = orjson.dumps(BODY)

    response = await callback(ARATargetEnum.ARAGORN, "cb-1", make_request(raw))

    assert response.status_code == 200
    assert raw not in [call.args[0] for call in loads.call_args_list]
    assert await get_message("cb-1", logger) == BODY
    assert ready == [("resp-1", "cb-1")]
    assert tasks[0][0] == "merge_message"


async def test_the_route_stores_a_zstd_body_as_it_came(route):
    await save_message("q-1", {"message": {}}, logger)
    frame = zstandard.compress(orjson.dumps(BODY))

    response = await callback(
        ARATargetEnum.ARAGORN,
        "cb-1",
        make_request(frame, {"content-encoding": "zstd"}),
    )

    assert response.status_code == 200
    assert await db.data_db_client.get("cb-1") == frame


async def test_the_route_refuses_a_body_that_is_not_json(route):
    tasks, ready, _ = route

    response = await callback(
        ARATargetEnum.ARAGORN, "cb-1", make_request(b"<html>oops</html>")
    )

    assert response.status_code == 422
    assert ready == [] and tasks == []


# --- merge_message reads it --------------------------------------------------


@pytest.fixture
def sync_store(mocker):
    storage = {}
    client = mocker.Mock()
    client.set.side_effect = lambda key, blob, ex=None: storage.__setitem__(key, blob)
    client.get.side_effect = lambda key: storage.get(key)
    mocker.patch("shepherd_utils.db._get_sync_data_db", return_value=client)
    return storage


def test_read_callback_response_fills_in_what_is_missing(sync_store, caplog):
    sync_store["cb"] = encode_callback_body(
        orjson.dumps({"message": {"query_graph": {"nodes": {}}}}), False
    )[0]

    with caplog.at_level(logging.INFO):
        response = read_callback_response("cb", logger)

    assert response["message"]["results"] == []
    assert response["message"]["knowledge_graph"] == {"nodes": {}, "edges": {}}
    assert "[cb] Got back 0 results, 0 nodes and 0 edges." in caplog.text


@pytest.mark.parametrize("message", [None, [], "nope"])
def test_read_callback_response_without_a_message(sync_store, message):
    save_message_sync("cb", {"message": message})

    response = read_callback_response("cb", logger)

    assert response["message"] == {
        "results": [],
        "knowledge_graph": {"nodes": {}, "edges": {}},
    }


def test_read_callback_response_logs_a_long_id_list_as_its_count(sync_store, caplog):
    ids = [f"X:{i}" for i in range(500)]
    save_message_sync(
        "cb", {"message": {"query_graph": {"nodes": {"n0": {"ids": ids}}}}}
    )

    with caplog.at_level(logging.DEBUG):
        read_callback_response("cb", logger)

    assert "500 curies" in caplog.text
    assert "X:499" not in caplog.text


@pytest.mark.parametrize(
    "blob",
    [
        zstandard.compress(b'{"message": '),
        zstandard.compress(b"[1, 2]"),
        b"not zstd",
    ],
)
def test_read_callback_response_skips_what_is_not_a_response(sync_store, blob, caplog):
    sync_store["cb"] = blob

    assert read_callback_response("cb", logger) is None
    assert "cb" in caplog.text


def test_read_callback_response_skips_a_missing_callback(sync_store):
    assert read_callback_response("gone", logger) is None


def test_a_bad_callback_does_not_stop_the_rest_merging(sync_store):
    query_graph = response_1["message"]["query_graph"]
    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    sync_store["bad"] = zstandard.compress(b'{"message": {"results": [')
    save_message_sync("good", copy.deepcopy(response_2))

    merged, entries = merge_messages_by_ids("test_ara", "qid", "rid", ["bad", "good"])

    assert merged == ["good"]
    assert get_message_sync("rid")["message"]["results"]
    assert any("bad is not valid JSON" in str(entry) for entry in entries)
