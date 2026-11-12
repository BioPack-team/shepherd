import json
import logging
import pytest

import orjson

from workers.finish_query.worker import finish_query


def _patch_messages(mocker, messages):
    """Patch finish_query's get_message to serve ``messages`` by id: the JSON
    bytes for a ``raw=True`` load (how the response is read), else the dict
    (how the query is read)."""

    async def _get(message_id, logger, *args, raw=False, **kwargs):
        if message_id not in messages:
            raise KeyError(f"Failed to get {message_id} from db")
        message = messages[message_id]
        return orjson.dumps(message) if raw else message

    return mocker.patch("workers.finish_query.worker.get_message", side_effect=_get)


@pytest.mark.asyncio
async def test_finish_sync_query(redis_mock, mocker):
    """Test that a synchronous query is finished correctly."""
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    response_id = "test_response"
    mock_query_state.return_value = ["", "", "", "", "", "", "", response_id, None]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    mock_callback_response = mocker.patch("workers.finish_query.worker.get_message")
    mock_callback_response.return_value = orjson.dumps(
        {
            "message": {
                "results": [
                    {
                        "analyses": [
                            {
                                "score": 0.1,
                            },
                        ],
                    },
                    {
                        "analyses": [
                            {
                                "score": 0.9,
                            },
                        ],
                    },
                ],
            },
        }
    )

    logger = logging.getLogger(__name__)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": response_id,
                "workflow": json.dumps([]),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    mock_set_query_completed.assert_called_once_with("test", "OK", logger)


@pytest.mark.asyncio
async def test_finish_internal_ars_query_enqueues_premerge(redis_mock, mocker):
    """A query whose callback is the ARS handoff sentinel is not POSTed
    anywhere: finish_query enqueues an intake task on ars.premerge (which
    loads the response from the blob store itself) and completes the query.
    """
    import uuid

    from shepherd_utils.ars.handoff import handoff_callback_url
    from shepherd_utils.broker import get_task

    child_pk = str(uuid.uuid4())
    response_id = "test_response"
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    mock_query_state.return_value = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        response_id,
        handoff_callback_url(child_pk),
    ]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    mock_get_message = _patch_messages(
        mocker,
        {
            response_id: {"message": {"results": []}},
            "test": {"message": {}, "parameters": {"log_level": "DEBUG"}},
        },
    )
    mock_post = mocker.patch("httpx.AsyncClient.post")

    logger = logging.getLogger(__name__)
    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}],
        logger,
    )

    mock_post.assert_not_called()
    # The payload is not even loaded here -- the intake worker pulls it from
    # the blob store by response_id -- and nothing is rewritten.
    mock_get_message.assert_not_called()
    task = await get_task("ars.premerge", "consumer", "t", logger)
    assert task is not None
    assert task[1]["intake_child_pk"] == child_pk
    assert task[1]["response_id"] == response_id
    assert task[1]["query_id"] == "test"
    assert "otel" in task[1]
    mock_set_query_completed.assert_called_once_with("test", "OK", logger)


@pytest.mark.asyncio
async def test_finish_query_carries_a_worker_failure_to_postgres_and_the_ars(
    redis_mock, mocker
):
    """A worker that failed with a QueryError (BTE refusing Pathfinder) hands
    finish_query a coded status and a reason. Both are stored -- /query and
    /asyncquery_status read them -- and both ride along to the ARS intake, so
    the ARS child errors with the same code."""
    import uuid

    from shepherd_utils.ars.handoff import handoff_callback_url
    from shepherd_utils.broker import get_task

    child_pk = str(uuid.uuid4())
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "resp",
            handoff_callback_url(child_pk),
        ],
    )
    completed = mocker.patch("workers.finish_query.worker.set_query_completed")
    _patch_messages(mocker, {"resp": {"message": {"results": []}}})
    reason = "BTE does not support Pathfinder type queries."

    logger = logging.getLogger(__name__)
    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "resp",
                "status": "ERROR 501",
                "description": reason,
            },
        ],
        logger,
    )

    completed.assert_called_once_with("test", "ERROR 501", logger, description=reason)
    task = await get_task("ars.premerge", "consumer", "t", logger)
    assert task[1]["ara_status"] == "ERROR 501"
    assert task[1]["ara_description"] == reason


@pytest.mark.asyncio
async def test_finish_async_query(redis_mock, mocker):
    """Test that a synchronous query is finished correctly."""
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    response_id = "test_response"
    mock_query_state.return_value = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        response_id,
        "http://test",
    ]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    final_response = {
        "message": {
            "result": "this is the final response",
        },
    }
    _patch_messages(
        mocker,
        {
            response_id: final_response,
            "test": {"message": {}, "parameters": {"timeout": 30}},
        },
    )

    mock_post = mocker.patch("httpx.AsyncClient.post")

    logger = logging.getLogger(__name__)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": response_id,
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args.kwargs
    assert call_kwargs["headers"]["Content-Type"] == "application/json"
    posted_payload = orjson.loads(call_kwargs["content"])
    assert posted_payload["message"] == final_response["message"]
    # TRAPI 2.0 envelope: version stamps and the query's parameters echoed.
    assert posted_payload["schema_version"] == "2.0.0"
    assert posted_payload["parameters"] == {"timeout": 30}
    mock_set_query_completed.assert_called_once_with("test", "OK", logger)


@pytest.mark.asyncio
async def test_finish_async_query_never_decodes_the_response(redis_mock, mocker):
    """The stored response is only ever loaded as raw bytes -- the payload is
    built around them (delivery_payload) -- while the query is decoded for
    its parameters."""
    response_id = "test_response"
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[""] * 7 + [response_id, "http://test"],
    )
    mocker.patch("workers.finish_query.worker.set_query_completed")
    load = _patch_messages(
        mocker,
        {
            response_id: {"message": {"results": []}},
            "test": {"message": {}, "parameters": {"timeout": 30}},
        },
    )
    mock_post = mocker.patch("httpx.AsyncClient.post")
    logger = logging.getLogger(__name__)

    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}], logger
    )

    response_loads = [c for c in load.call_args_list if c.args[0] == response_id]
    assert response_loads
    assert all(c.kwargs.get("raw") is True for c in response_loads)
    payload = orjson.loads(mock_post.call_args.kwargs["content"])
    assert payload["parameters"] == {"timeout": 30}
    assert payload["message"] == {"results": []}


@pytest.mark.asyncio
async def test_finish_async_query_with_a_lost_response(redis_mock, mocker):
    """A response gone from the data store (e.g. Redis restarted and reloaded
    an older snapshot) is reported to the caller as an error, and the query is
    finished, instead of finish_query crashing on the load and leaving both
    the query and the caller hanging."""
    from translator_tom import Response

    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    mock_query_state.return_value = ["", "", "", "", "", "", "", "lost", "http://test"]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    query_graph = {
        "nodes": {"n0": {"ids": ["X:1"]}, "n1": {}},
        "edges": {"e0": {"subject": "n0", "object": "n1"}},
    }
    _patch_messages(
        mocker,
        {"test": {"message": {"query_graph": query_graph}, "parameters": {}}},
    )
    mock_post = mocker.patch("httpx.AsyncClient.post")
    logger = logging.getLogger(__name__)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "lost",
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )

    mock_post.assert_called_once()
    posted = orjson.loads(mock_post.call_args.kwargs["content"])
    assert posted["status"] == "Error"
    assert posted["description"].startswith("Response lost")
    assert posted["message"]["results"] == []
    assert posted["message"]["query_graph"] == query_graph
    Response.from_dict(posted)
    from workers.finish_query.worker import LOST_DESCRIPTION

    mock_set_query_completed.assert_called_once_with(
        "test", "ERROR", logger, description=LOST_DESCRIPTION
    )


@pytest.mark.asyncio
async def test_finish_query_waits_out_a_postgres_outage(redis_mock, mocker):
    """An unreachable Postgres used to read as "query not found": finish_query
    then skipped delivery and acked the task, and the caller never heard back.
    It waits the outage out instead, then delivers as usual."""
    from shepherd_utils.db import DatabaseUnavailableError
    from workers.finish_query import worker

    sleep = mocker.patch.object(worker.asyncio, "sleep", new=mocker.AsyncMock())
    state = mocker.patch(
        "workers.finish_query.worker.get_query_state",
        side_effect=[
            DatabaseUnavailableError("connection refused"),
            DatabaseUnavailableError("connection refused"),
            ["", "", "", "", "", "", "", "rid", "http://test"],
        ],
    )
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    _patch_messages(mocker, {"rid": {"message": {}}, "test": {"message": {}}})
    mock_post = mocker.patch("httpx.AsyncClient.post")
    logger = logging.getLogger(__name__)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "rid",
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )

    assert state.call_count == 3
    assert all(c.kwargs == {"raise_on_unavailable": True} for c in state.call_args_list)
    assert [c.args[0] for c in sleep.await_args_list[:2]] == [worker.DB_RETRY_SEC] * 2
    mock_post.assert_called_once()
    mock_set_query_completed.assert_called_once_with("test", "OK", logger)


@pytest.mark.asyncio
async def test_finish_query_gives_up_on_a_long_postgres_outage(redis_mock, mocker):
    from shepherd_utils.db import DatabaseUnavailableError
    from workers.finish_query import worker

    mocker.patch.object(worker.asyncio, "sleep", new=mocker.AsyncMock())
    clock = iter(range(0, 10_000, 100))
    mocker.patch.object(worker.time, "time", side_effect=lambda: next(clock))
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        side_effect=DatabaseUnavailableError("connection refused"),
    )
    mock_post = mocker.patch("httpx.AsyncClient.post")

    with pytest.raises(DatabaseUnavailableError):
        await finish_query(
            ["test", {"query_id": "test", "response_id": "rid", "workflow": "[]"}],
            logging.getLogger(__name__),
        )
    mock_post.assert_not_called()


# --- the archive of finished responses (#252) --------------------------------


def _archive_spies(mocker, events):
    """Record the order finish_query archives, hands off and saves logs in."""

    async def _archive_response(response_id, logger):
        events.append(("archive_response", response_id))
        return True

    async def _archive_logs(response_id, logger):
        events.append(("archive_logs", response_id))
        return True

    async def _save_logs(response_id, logger):
        events.append(("save_logs", response_id))

    async def _add_task(stream, *args, **kwargs):
        events.append(("add_task", stream))

    mocker.patch(
        "workers.finish_query.worker.archive_response", side_effect=_archive_response
    )
    mocker.patch("workers.finish_query.worker.archive_logs", side_effect=_archive_logs)
    mocker.patch("workers.finish_query.worker.save_logs", side_effect=_save_logs)
    return _add_task


@pytest.mark.asyncio
async def test_the_response_is_archived_before_the_ars_is_handed_it(redis_mock, mocker):
    """The ARS's premerge rewrites the response it is handed, so the raw copy
    has to be in the cold store first; the logs follow once this worker has
    flushed its own."""
    import uuid

    from shepherd_utils.ars.handoff import handoff_callback_url

    response_id = str(uuid.uuid4())
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[""] * 7 + [response_id, handoff_callback_url(uuid.uuid4())],
    )
    mocker.patch("workers.finish_query.worker.set_query_completed")
    events = []
    add_task = _archive_spies(mocker, events)
    mocker.patch("workers.finish_query.worker.add_task", side_effect=add_task)

    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}],
        logging.getLogger(__name__),
    )

    assert events == [
        ("archive_response", response_id),
        ("add_task", "ars.premerge"),
        ("save_logs", response_id),
        ("archive_logs", response_id),
    ]


@pytest.mark.asyncio
async def test_a_direct_query_is_archived_the_same_way(redis_mock, mocker):
    response_id = "test_response"
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[""] * 7 + [response_id, None],
    )
    completed = mocker.patch("workers.finish_query.worker.set_query_completed")
    events = []
    _archive_spies(mocker, events)

    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}],
        logging.getLogger(__name__),
    )

    assert [name for name, _ in events] == [
        "archive_response",
        "save_logs",
        "archive_logs",
    ]
    completed.assert_called_once()


@pytest.mark.asyncio
async def test_a_failed_archive_write_does_not_stop_the_delivery(
    redis_mock, mocker, tmp_path, monkeypatch
):
    """The store is down for the whole query: it is still handed to the ARS
    and completed."""
    import uuid

    from shepherd_utils.ars import payload_store
    from shepherd_utils.ars.handoff import handoff_callback_url
    from shepherd_utils.broker import get_task
    from shepherd_utils.config import settings
    from shepherd_utils import db, response_archive

    logger = logging.getLogger(__name__)
    response_id = str(uuid.uuid4())
    await db.save_response(response_id, {"message": {"results": []}}, logger)
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    monkeypatch.setattr(response_archive.asyncio, "sleep", mocker.AsyncMock())
    write = mocker.patch.object(
        payload_store, "write", side_effect=OSError("No space left on device")
    )
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[""] * 7 + [response_id, handoff_callback_url(uuid.uuid4())],
    )
    completed = mocker.patch("workers.finish_query.worker.set_query_completed")

    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}], logger
    )

    assert write.call_count >= response_archive.ARCHIVE_ATTEMPTS
    assert await get_task("ars.premerge", "consumer", "t", logger) is not None
    completed.assert_called_once_with("test", "OK", logger)


@pytest.mark.asyncio
async def test_a_finished_response_and_its_logs_land_in_the_store(
    redis_mock, mocker, tmp_path, monkeypatch
):
    import uuid

    from shepherd_utils.config import settings
    from shepherd_utils import db, response_archive

    logger = logging.getLogger(__name__)
    response_id = str(uuid.uuid4())
    await db.save_response(response_id, {"message": {"results": []}}, logger)
    await db._append_logs(
        response_id, [{"timestamp": "2026-01-01T00:00:00Z", "message": "looked up"}]
    )
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[""] * 7 + [response_id, None],
    )
    mocker.patch("workers.finish_query.worker.set_query_completed")

    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}], logger
    )

    stored = await response_archive.load_response_bytes(response_id, logger)
    assert orjson.loads(stored) == {"message": {"results": []}}
    logs = await response_archive.load_logs(response_id, logger)
    assert [entry["message"] for entry in logs] == ["looked up"]


@pytest.mark.asyncio
async def test_a_query_someone_else_finished_is_not_archived_again(redis_mock, mocker):
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        return_value=[""] * 7 + ["test_response", None],
    )
    mocker.patch("workers.finish_query.worker._wait_for_claim", return_value=False)
    events = []
    _archive_spies(mocker, events)

    await finish_query(
        ["test", {"query_id": "test", "response_id": "test_response"}],
        logging.getLogger(__name__),
    )

    assert [name for name, _ in events] == ["save_logs"]
