"""Tests for ``workers.aragorn_lookup.worker``'s wait for lookup callbacks.

The lookup worker waits for its callbacks' rows to clear, which happens once
merge_message has merged them into the query's response. If the query or
response blob is lost from Redis mid-wait (e.g. Redis restarted and reloaded
an older snapshot) those callbacks can never be merged, so the worker must
fail the query right away instead of waiting out the whole timeout.
"""

import json
import logging

import pytest

from shepherd_utils.db import save_message
from workers.aragorn_lookup import worker as lookup_worker

logger = logging.getLogger(__name__)

QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
            "edges": {"e0": {"subject": "a", "object": "b"}},
        }
    },
    "parameters": {"timeout": 30},
}


def _task():
    return [
        "1-0",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([{"id": "aragorn.lookup"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


@pytest.fixture
def lookup(redis_mock, mocker):
    """A pure lookup with the query and response stored and one callback out."""
    mocker.patch.object(
        lookup_worker,
        "get_message",
        new_callable=mocker.AsyncMock,
        side_effect=lambda *a, **k: json.loads(json.dumps(QUERY)),
    )
    mocker.patch.object(lookup_worker, "add_callback_id", new_callable=mocker.AsyncMock)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    mocker.patch.object(lookup_worker.asyncio, "sleep", new_callable=mocker.AsyncMock)
    return {
        "running": mocker.patch.object(
            lookup_worker,
            "get_running_callbacks",
            new_callable=mocker.AsyncMock,
            side_effect=[["cb1"], []],
        ),
        "cleanup": mocker.patch.object(
            lookup_worker, "cleanup_callbacks", new_callable=mocker.AsyncMock
        ),
    }


async def _store(*message_ids):
    for message_id in message_ids:
        await save_message(message_id, QUERY, logger)


async def test_lookup_waits_for_callbacks_while_the_query_data_is_there(lookup):
    await _store("qid", "rid")

    await lookup_worker.aragorn_lookup(_task(), logger)

    assert lookup["running"].await_count == 2
    lookup["cleanup"].assert_not_awaited()


@pytest.mark.parametrize("lost", ["qid", "rid"])
async def test_lookup_fails_fast_when_the_query_data_is_gone(lookup, lost):
    await _store(*({"qid", "rid"} - {lost}))

    with pytest.raises(KeyError, match=lost):
        await lookup_worker.aragorn_lookup(_task(), logger)

    # gave up on the first check instead of waiting out the timeout, and
    # cleared the outstanding callback rows on the way out
    assert lookup["running"].await_count == 1
    lookup["cleanup"].assert_awaited_once_with("qid", logger)


async def test_lookup_retries_when_the_data_check_errors(lookup, mocker):
    """Redis down or still loading is not data loss: keep waiting."""
    await _store("qid", "rid")
    exists = mocker.patch.object(
        lookup_worker,
        "message_exists",
        new_callable=mocker.AsyncMock,
        side_effect=[ConnectionError("loading"), True, True],
    )
    lookup["running"].side_effect = [["cb1"], ["cb1"], []]

    await lookup_worker.aragorn_lookup(_task(), logger)

    assert exists.await_count == 3
    lookup["cleanup"].assert_not_awaited()
