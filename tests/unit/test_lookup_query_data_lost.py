"""The lookup workers' callback wait gives up once the query data is lost.

Each lookup worker waits for its callbacks' rows to clear, which happens once
merge_message has merged them into the query's response. If the query or
response blob is lost from Redis mid-wait (e.g. Redis restarted and reloaded
an older snapshot) those callbacks can never be merged, so the worker must
fail the query right away (``abandon_lookup_if_data_lost``) instead of
waiting out the whole timeout.
"""

import json
import logging

import pytest

from shepherd_utils import db
from shepherd_utils.db import (
    QueryDataLostError,
    abandon_lookup_if_data_lost,
    save_message,
)
from workers.aragorn_lookup import worker as aragorn_lookup_worker
from workers.aragorn_pathfinder import worker as aragorn_pathfinder_worker
from workers.bte_lookup import worker as bte_lookup_worker
from workers.example_lookup import worker as example_lookup_worker

logger = logging.getLogger(__name__)

LOOKUP_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
            "edges": {"e0": {"subject": "a", "object": "b"}},
        }
    },
    "parameters": {"timeout": 30},
}

PATHFINDER_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {
                "n0": {"ids": ["MONDO:0001"]},
                "n1": {"ids": ["MONDO:0002"]},
            },
            "paths": {"p0": {"subject": "n0", "object": "n1"}},
        }
    },
    "parameters": {"timeout": 30},
}

# (worker module, entry point name, the query it is handed)
WORKERS = {
    "aragorn.lookup": (aragorn_lookup_worker, "aragorn_lookup", LOOKUP_QUERY),
    "aragorn.pathfinder": (aragorn_pathfinder_worker, "shadowfax", PATHFINDER_QUERY),
    "bte.lookup": (bte_lookup_worker, "bte_lookup", LOOKUP_QUERY),
    "example.lookup": (example_lookup_worker, "example_lookup", LOOKUP_QUERY),
}


@pytest.fixture
def cleanup(mocker):
    return mocker.patch.object(db, "cleanup_callbacks", new_callable=mocker.AsyncMock)


async def _store(*message_ids):
    for message_id in message_ids:
        await save_message(message_id, LOOKUP_QUERY, logger)


# --- the shared check ---------------------------------------------------------


async def test_check_passes_while_the_query_data_is_there(redis_mock, cleanup):
    await _store("qid", "rid")
    await abandon_lookup_if_data_lost("qid", "rid", ["cb1"], logger)
    cleanup.assert_not_awaited()


@pytest.mark.parametrize("lost", ["qid", "rid"])
async def test_check_abandons_the_lookup_when_the_query_data_is_gone(
    redis_mock, cleanup, lost
):
    await _store(*({"qid", "rid"} - {lost}))

    with pytest.raises(QueryDataLostError, match=lost):
        await abandon_lookup_if_data_lost("qid", "rid", ["cb1"], logger)

    cleanup.assert_awaited_once_with("qid", logger)


async def test_check_is_skipped_with_nothing_outstanding(redis_mock, cleanup, mocker):
    """No callbacks left means the wait is over; the next stage reads the
    response itself."""
    exists = mocker.patch.object(db, "message_exists", new_callable=mocker.AsyncMock)
    await abandon_lookup_if_data_lost("qid", "rid", [], logger)
    exists.assert_not_awaited()
    cleanup.assert_not_awaited()


async def test_check_error_is_not_data_loss(redis_mock, cleanup, mocker):
    """Redis down or still loading its snapshot: keep waiting."""
    mocker.patch.object(
        db,
        "message_exists",
        new_callable=mocker.AsyncMock,
        side_effect=ConnectionError("LOADING Redis is loading the dataset"),
    )
    await abandon_lookup_if_data_lost("qid", "rid", ["cb1"], logger)
    cleanup.assert_not_awaited()


# --- every lookup worker's wait loop uses it ----------------------------------


def _task():
    return [
        "1-0",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([]),
            "log_level": "20",
            "otel": json.dumps({}),
            "metadata": json.dumps({}),
        },
    ]


@pytest.fixture(params=sorted(WORKERS))
def lookup(request, redis_mock, cleanup, mocker):
    """Run one lookup worker with its outbound calls mocked."""
    module, entry, query = WORKERS[request.param]
    mocker.patch.object(
        module,
        "get_message",
        new_callable=mocker.AsyncMock,
        side_effect=lambda *a, **k: json.loads(json.dumps(query)),
    )
    mocker.patch.object(module, "add_callback_id", new_callable=mocker.AsyncMock)
    if hasattr(module, "save_message"):
        mocker.patch.object(module, "save_message", new_callable=mocker.AsyncMock)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    mocker.patch.object(module.asyncio, "sleep", new_callable=mocker.AsyncMock)
    running = mocker.patch.object(
        module,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        side_effect=[["cb1"], []],
    )
    return {"run": getattr(module, entry), "running": running}


async def test_worker_waits_for_callbacks_while_the_query_data_is_there(
    lookup, cleanup
):
    await _store("qid", "rid")

    await lookup["run"](_task(), logger)

    assert lookup["running"].await_count == 2
    cleanup.assert_not_awaited()


async def test_worker_fails_fast_when_the_query_data_is_gone(lookup, cleanup):
    await _store("qid")  # the response was lost

    with pytest.raises(QueryDataLostError, match="rid"):
        await lookup["run"](_task(), logger)

    # gave up on the first poll instead of waiting out the timeout, and
    # cleared the outstanding callback rows on the way out
    assert lookup["running"].await_count == 1
    cleanup.assert_awaited_once_with("qid", logger)
