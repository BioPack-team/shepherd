"""Tests for ``workers.filter_analyses_top_n.worker``.

The worker truncates each result's ``analyses`` array to ``max_analyses``
items (defaulting to 1000).
"""

import json
import logging

import pytest

from shepherd_utils.db import get_message
from workers.filter_analyses_top_n.worker import filter_analyses_top_n


logger = logging.getLogger(__name__)


def _make_task(workflow):
    return [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "workflow": json.dumps(workflow),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


@pytest.mark.asyncio
async def test_filter_analyses_top_n_truncates_to_max(redis_mock, mocker):
    """Each result's analyses list should be capped at max_analyses."""
    mocker.patch(
        "workers.filter_analyses_top_n.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={
            "message": {
                "results": [
                    {"analyses": [{"score": i} for i in range(5)]},
                    {"analyses": [{"score": i} for i in range(2)]},
                ]
            }
        },
    )

    await filter_analyses_top_n(
        _make_task([{"id": "filter_analyses_top_n", "max_analyses": 2}]),
        logger,
    )
    saved = await get_message("test_response", logger)
    assert len(saved["message"]["results"][0]["analyses"]) == 2
    assert len(saved["message"]["results"][1]["analyses"]) == 2


@pytest.mark.asyncio
async def test_filter_analyses_top_n_default_cap_when_unset(redis_mock, mocker):
    """No max_analyses on the workflow op falls back to 1000."""
    mocker.patch(
        "workers.filter_analyses_top_n.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={
            "message": {
                "results": [
                    {"analyses": [{"score": i} for i in range(5)]},
                ]
            }
        },
    )

    await filter_analyses_top_n(_make_task([{"id": "filter_analyses_top_n"}]), logger)
    saved = await get_message("test_response", logger)
    # No truncation when results are smaller than the default cap.
    assert len(saved["message"]["results"][0]["analyses"]) == 5


@pytest.mark.asyncio
async def test_filter_analyses_top_n_handles_empty_results(redis_mock, mocker):
    """No results: the worker should round-trip the empty list with no error."""
    mocker.patch(
        "workers.filter_analyses_top_n.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": {"results": []}},
    )

    await filter_analyses_top_n(
        _make_task([{"id": "filter_analyses_top_n", "max_analyses": 5}]), logger
    )
    saved = await get_message("test_response", logger)
    assert saved["message"]["results"] == []
