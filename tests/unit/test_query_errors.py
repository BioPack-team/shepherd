"""A worker's failure carries the HTTP code it should end with.

BTE refusing a Pathfinder query is a capability gap (TRAPI's 501), not a
crash (500), and only the ARA's own worker knows which it is. These pin the
first two hops of that code's trip out: the stored-status encoding, and the
lifecycle handing it to finish_query.
"""

import logging

import pytest

from shepherd_utils import shared
from shepherd_utils.query_errors import (
    QueryError,
    UnsupportedQueryError,
    error_status,
    is_error_status,
    status_error_code,
)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "code,status",
    [
        (None, "ERROR"),
        # a plain 500 keeps the bare status every existing reader expects
        (500, "ERROR"),
        (501, "ERROR 501"),
        (422, "ERROR 422"),
        # never a code that would make a failed query read as anything else
        (200, "ERROR"),
        (302, "ERROR"),
        ("nope", "ERROR"),
    ],
)
def test_error_status(code, status):
    assert error_status(code) == status


@pytest.mark.parametrize(
    "status,code",
    [
        ("ERROR 501", 501),
        ("ERROR", None),
        ("OK", None),
        ("TIMEOUT", None),
        ("ERROR 200", None),
        (None, None),
    ],
)
def test_status_error_code(status, code):
    assert status_error_code(status) == code


def test_is_error_status():
    assert is_error_status("ERROR")
    assert is_error_status("ERROR 501")
    assert not is_error_status("TIMEOUT")
    assert not is_error_status("OK")
    assert not is_error_status(None)


def test_unsupported_is_501():
    e = UnsupportedQueryError("BTE does not support Pathfinder type queries.")
    assert e.status_code == 501
    assert e.description == "BTE does not support Pathfinder type queries."
    assert QueryError("x", status_code=429).status_code == 429
    # an unusable code fails the query as a plain 500
    assert QueryError("x", status_code=204).status_code == 500


def _task():
    return (
        "1-0",
        {
            "query_id": "q1",
            "response_id": "r1",
            "otel": "{}",
            "metadata": "{}",
        },
    )


@pytest.fixture
def added(monkeypatch):
    calls = []

    async def _add_task(queue, payload, _logger, **_kwargs):
        calls.append((queue, payload))

    async def _noop(*_a, **_kw):
        return None

    monkeypatch.setattr(shared, "add_task", _add_task)
    monkeypatch.setattr(shared, "mark_task_as_complete", _noop)
    monkeypatch.setattr(shared, "save_logs", _noop)
    monkeypatch.setattr(shared, "_record_task_duration", _noop)
    return calls


@pytest.mark.asyncio
async def test_handle_task_failure_carries_a_query_errors_code(added):
    error = UnsupportedQueryError("BTE does not support Pathfinder type queries.")
    await shared.handle_task_failure("bte", "consumer", _task(), logger, error=error)
    queue, payload = added[0]
    assert queue == "finish_query"
    assert payload["status"] == "ERROR 501"
    assert payload["description"] == "BTE does not support Pathfinder type queries."


@pytest.mark.asyncio
async def test_handle_task_failure_plain_exception_is_still_error(added):
    await shared.handle_task_failure(
        "bte", "consumer", _task(), logger, error=RuntimeError("boom")
    )
    payload = added[0][1]
    assert payload["status"] == "ERROR"
    # an arbitrary exception's text is not client-safe, so it isn't forwarded
    assert "description" not in payload


class _Limiter:
    def release(self):
        pass


@pytest.mark.parametrize(
    "error,status",
    [
        (UnsupportedQueryError("BTE does not support Pathfinder type queries."), 501),
        (RuntimeError("boom"), None),
    ],
)
@pytest.mark.asyncio
async def test_lifecycle_hands_the_workers_error_to_failure_handling(
    mocker, monkeypatch, error, status
):
    async def worker(_task, _logger):
        raise error

    async def _no_guard(*_a, **_kw):
        return None

    monkeypatch.setattr(shared, "_guard_response_size", _no_guard)
    fail = mocker.patch.object(
        shared, "handle_task_failure", new_callable=mocker.AsyncMock
    )
    await shared.run_task_lifecycle(
        "bte", "consumer", _task(), None, logger, _Limiter(), worker
    )
    assert fail.await_args.kwargs["error"] is error
