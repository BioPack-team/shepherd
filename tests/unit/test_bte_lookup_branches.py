"""Branch-coverage tests for ``workers.bte_lookup.worker`` paths not
exercised by ``test_bte_lookup.py``.

Covers:

- ``run_async_lookup`` happy path and HTTPX-raises path.
- ``bte_lookup`` inferred fanout: expand_bte_query is called, requests
  fire via ``run_async_lookup``, failed responses get their callback ids
  removed.
- ``bte_lookup`` polling-loop branches: in-progress callbacks delay,
  timeout cleanup branch.
- ``process_task`` happy and failure paths.
"""

import asyncio
import json
import logging

import httpx
import pytest

from shepherd_utils import shared
from workers.bte_lookup import worker as btel
from workers.bte_lookup.worker import (
    AsyncResponse,
    bte_lookup,
    process_task,
    run_async_lookup,
)

logger = logging.getLogger(__name__)


def _make_task():
    return [
        "test",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([{"id": "bte.lookup"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


# --- run_async_lookup ----------------------------------------------------


@pytest.mark.asyncio
async def test_run_async_lookup_returns_success_on_200(redis_mock, mocker):
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)

    fake_response = mocker.Mock()
    fake_response.status_code = 200
    client = mocker.Mock()
    client.post = mocker.AsyncMock(return_value=fake_response)

    out = await run_async_lookup(client, {"message": {}}, "qid", logger)
    assert isinstance(out, AsyncResponse)
    assert out.success is True
    assert out.status_code == 200
    assert out.error is None


@pytest.mark.asyncio
async def test_run_async_lookup_returns_failure_on_non_200(redis_mock, mocker):
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)

    fake_response = mocker.Mock()
    fake_response.status_code = 500
    client = mocker.Mock()
    client.post = mocker.AsyncMock(return_value=fake_response)

    out = await run_async_lookup(client, {"message": {}}, "qid", logger)
    assert out.success is False
    assert out.status_code == 500


@pytest.mark.asyncio
async def test_run_async_lookup_returns_500_when_post_raises(redis_mock, mocker):
    """A network exception is caught and surfaced as a 500 ``AsyncResponse``
    with the error string populated."""
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)

    client = mocker.Mock()
    client.post = mocker.AsyncMock(side_effect=httpx.ConnectError("boom"))

    out = await run_async_lookup(client, {"message": {}}, "qid", logger)
    assert out.success is False
    assert out.status_code == 500
    assert "boom" in out.error


@pytest.mark.asyncio
async def test_run_async_lookup_writes_callback_url_into_message(redis_mock, mocker):
    """The function mutates the supplied message to include a callback URL
    routed back to the BTE callback endpoint."""
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)

    fake_response = mocker.Mock()
    fake_response.status_code = 200
    client = mocker.Mock()
    client.post = mocker.AsyncMock(return_value=fake_response)

    msg = {"message": {}}
    await run_async_lookup(client, msg, "qid", logger)
    assert "callback" in msg
    assert "/bte/callback/" in msg["callback"]


# --- bte_lookup inferred fanout -----------------------------------------


@pytest.mark.asyncio
async def test_bte_lookup_inferred_fans_out_via_expand_bte_query(redis_mock, mocker):
    """Inferred query: expand_bte_query is called, multiple
    run_async_lookup calls fire (one per expanded message)."""
    inferred_msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "a": {"ids": ["X:1"], "categories": ["biolink:Drug"]},
                    "b": {"categories": ["biolink:Disease"]},
                },
                "edges": {
                    "e0": {
                        "subject": "a",
                        "object": "b",
                        "knowledge_type": "inferred",
                        "predicates": ["biolink:treats"],
                    }
                },
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value=inferred_msg,
    )
    expanded = [
        {"message": {"query_graph": {}}, "parameters": {}, "submitter": "test"},
        {"message": {"query_graph": {}}, "parameters": {}, "submitter": "test"},
    ]
    mocker.patch.object(btel, "expand_bte_query", return_value=expanded)
    mock_run = mocker.patch.object(
        btel,
        "run_async_lookup",
        new_callable=mocker.AsyncMock,
        return_value=AsyncResponse(status_code=200, success=True, callback_id="cb-1"),
    )
    mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    await bte_lookup(_make_task(), logger)
    # One run_async_lookup per expanded message.
    assert mock_run.call_count == 2


@pytest.mark.asyncio
async def test_bte_lookup_inferred_removes_failed_callback_ids(redis_mock, mocker):
    """A run_async_lookup that returns an unsuccessful AsyncResponse should
    trigger remove_callback_id."""
    inferred_msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "a": {"ids": ["X:1"], "categories": ["biolink:Drug"]},
                    "b": {"categories": ["biolink:Disease"]},
                },
                "edges": {
                    "e0": {
                        "subject": "a",
                        "object": "b",
                        "knowledge_type": "inferred",
                        "predicates": ["biolink:treats"],
                    }
                },
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value=inferred_msg,
    )
    mocker.patch.object(
        btel,
        "expand_bte_query",
        return_value=[
            {"message": {"query_graph": {}}, "parameters": {}, "submitter": "t"}
        ],
    )
    mocker.patch.object(
        btel,
        "run_async_lookup",
        new_callable=mocker.AsyncMock,
        return_value=AsyncResponse(
            status_code=500, success=False, callback_id="failed-cb", error="x"
        ),
    )
    mock_remove = mocker.patch.object(
        btel, "remove_callback_id", new_callable=mocker.AsyncMock
    )
    mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    await bte_lookup(_make_task(), logger)
    mock_remove.assert_awaited_once_with("failed-cb", logger)


@pytest.mark.asyncio
async def test_bte_lookup_inferred_logs_exception_responses(redis_mock, mocker):
    """An exception in ``asyncio.gather`` (return_exceptions=True) is logged
    but no remove_callback_id is called for it."""
    inferred_msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "a": {"ids": ["X:1"], "categories": ["biolink:Drug"]},
                    "b": {"categories": ["biolink:Disease"]},
                },
                "edges": {
                    "e0": {
                        "subject": "a",
                        "object": "b",
                        "knowledge_type": "inferred",
                        "predicates": ["biolink:treats"],
                    }
                },
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value=inferred_msg,
    )
    mocker.patch.object(
        btel,
        "expand_bte_query",
        return_value=[
            {"message": {"query_graph": {}}, "parameters": {}, "submitter": "t"}
        ],
    )
    # run_async_lookup raises -> gather captures and returns exception.
    mocker.patch.object(
        btel,
        "run_async_lookup",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("boom"),
    )
    mock_remove = mocker.patch.object(
        btel, "remove_callback_id", new_callable=mocker.AsyncMock
    )
    mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    await bte_lookup(_make_task(), logger)
    assert not mock_remove.called


# --- bte_lookup timeout / polling branches ------------------------------


@pytest.mark.asyncio
async def test_bte_lookup_polling_loop_iterates_until_callbacks_drain(
    redis_mock, mocker
):
    """The polling loop sees in-progress callbacks for one iteration, then
    they drain on the next iteration and the loop breaks."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel, "get_message", new_callable=mocker.AsyncMock, return_value=msg
    )
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    # First call: in-progress; second call: drained.
    running = mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        side_effect=[[("running",)], []],
    )
    # Don't actually sleep.
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    await bte_lookup(_make_task(), logger)
    assert running.call_count == 2


@pytest.mark.asyncio
async def test_bte_lookup_polling_loop_retries_after_db_error(redis_mock, mocker):
    """An exception on get_running_callbacks doesn't abort the loop; it sleeps
    and retries on the next iteration."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel, "get_message", new_callable=mocker.AsyncMock, return_value=msg
    )
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    running = mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        side_effect=[RuntimeError("pg dead"), []],
    )
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    await bte_lookup(_make_task(), logger)
    assert running.call_count == 2


@pytest.mark.asyncio
async def test_bte_lookup_timeout_triggers_cleanup_callbacks(redis_mock, mocker):
    """When the polling loop never sees a drained queue and time runs out,
    cleanup_callbacks is called."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        # Use a tiny but non-zero timeout. We patch time.time to control flow.
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel, "get_message", new_callable=mocker.AsyncMock, return_value=msg
    )
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    # Always say there's a callback in progress.
    mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[("still-running",)],
    )

    # Patch time.time so the loop goes through one iteration and then exceeds
    # the 5s timeout. We can't predict exactly how many ``time.time`` calls
    # the worker makes, so use an iterator that returns "late" forever after
    # the first two calls.
    def _fake_time():
        yield 0
        yield 0.1
        while True:
            yield 100

    gen = _fake_time()
    mocker.patch.object(btel.time, "time", side_effect=lambda: next(gen))
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    mock_cleanup = mocker.patch.object(
        btel, "cleanup_callbacks", new_callable=mocker.AsyncMock
    )
    await bte_lookup(_make_task(), logger)
    assert mock_cleanup.called


# --- bte_lookup non-infer branch hits the http POST --------------------


@pytest.mark.asyncio
async def test_bte_lookup_non_infer_branch_writes_callback_url(redis_mock, mocker):
    """Pure-lookup queries set message['callback'] before posting to the
    retrieval URL."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel, "get_message", new_callable=mocker.AsyncMock, return_value=msg
    )
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)
    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    await bte_lookup(_make_task(), logger)
    posted_msg = mock_post.call_args.kwargs["json"]
    assert "callback" in posted_msg
    assert "/bte/callback/" in posted_msg["callback"]


# --- bte_lookup adds default submitter ----------------------------------


@pytest.mark.asyncio
async def test_bte_lookup_adds_default_submitter_when_missing(redis_mock, mocker):
    """If the input message has no submitter, bte_lookup populates one with
    the shepherd-bte infores string."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch.object(
        btel, "get_message", new_callable=mocker.AsyncMock, return_value=msg
    )
    mocker.patch.object(btel, "add_callback_id", new_callable=mocker.AsyncMock)
    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    mocker.patch.object(
        btel,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    await bte_lookup(_make_task(), logger)
    sent = mock_post.call_args.kwargs["json"]
    assert sent["submitter"].startswith("infores:shepherd-bte")


# --- process_task -------------------------------------------------------


class _Limiter:
    def __init__(self):
        self.released = False

    def release(self):
        self.released = True


@pytest.mark.asyncio
async def test_bte_lookup_process_task_happy_path(redis_mock, mocker):
    mocker.patch.object(btel, "bte_lookup", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(
        shared, "wrap_up_task", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()
    await process_task(_make_task(), None, logger, limiter)
    assert mock_wrap.called
    assert limiter.released


@pytest.mark.asyncio
async def test_bte_lookup_process_task_routes_failure_to_handle_task_failure(
    redis_mock, mocker
):
    mocker.patch.object(
        btel,
        "bte_lookup",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("kaboom"),
    )
    mock_failure = mocker.patch.object(
        shared, "handle_task_failure", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()
    await process_task(_make_task(), None, logger, limiter)
    assert mock_failure.called
    assert limiter.released


@pytest.mark.asyncio
async def test_bte_lookup_process_task_swallows_cancellation(redis_mock, mocker):
    mocker.patch.object(
        btel,
        "bte_lookup",
        new_callable=mocker.AsyncMock,
        side_effect=asyncio.CancelledError,
    )
    mock_failure = mocker.patch.object(
        shared, "handle_task_failure", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()
    await process_task(_make_task(), None, logger, limiter)
    assert not mock_failure.called
    assert limiter.released


@pytest.mark.asyncio
async def test_bte_lookup_process_task_swallows_wrap_up_failure(redis_mock, mocker):
    """A wrap_up_task failure should be logged but not escape."""
    mocker.patch.object(btel, "bte_lookup", new_callable=mocker.AsyncMock)
    mocker.patch.object(
        shared,
        "wrap_up_task",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("redis dropped"),
    )
    limiter = _Limiter()
    # Should not raise.
    await process_task(_make_task(), None, logger, limiter)
    assert limiter.released
