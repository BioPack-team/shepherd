import json
import logging
import pytest

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from workers.sort_results_score.worker import sort_results_score

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_default_sort(redis_mock, mocker):
    """Test sort results score."""
    mock_callback_response = mocker.patch(
        "workers.sort_results_score.worker.get_message"
    )
    mock_callback_response.return_value = {
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

    logger = logging.getLogger(__name__)

    await sort_results_score(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps([{"id": "sort_results_score"}]),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    message = await get_message("test_response", logger)

    assert len(message["message"]["results"]) == 2
    assert message["message"]["results"][0]["analyses"][0]["score"] == 0.9
    assert message["message"]["results"][1]["analyses"][0]["score"] == 0.1


@pytest.mark.asyncio
async def test_ascending_sort(redis_mock, mocker):
    """Test sort ascending is applied."""
    mock_callback_response = mocker.patch(
        "workers.sort_results_score.worker.get_message"
    )
    mock_callback_response.return_value = {
        "message": {
            "results": [
                {
                    "analyses": [
                        {
                            "score": 0.9,
                        },
                    ],
                },
                {
                    "analyses": [
                        {
                            "score": 0.1,
                        },
                    ],
                },
            ],
        },
    }

    logger = logging.getLogger(__name__)

    await sort_results_score(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps(
                    [
                        {
                            "id": "sort_results_score",
                            "ascending_or_descending": "ascending",
                        },
                    ],
                ),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    message = await get_message("test_response", logger)

    assert len(message["message"]["results"]) == 2
    assert message["message"]["results"][0]["analyses"][0]["score"] == 0.1
    assert message["message"]["results"][1]["analyses"][0]["score"] == 0.9


@pytest.mark.asyncio
async def test_results_without_analyses_sort_as_zero(redis_mock, mocker):
    """TRAPI 2.0: Result.analyses is optional. A result without it is not an
    error; it sorts as if its score were 0 and keeps no ``analyses`` key."""
    mock_callback_response = mocker.patch(
        "workers.sort_results_score.worker.get_message"
    )
    mock_callback_response.return_value = {
        "message": {
            "results": [
                {"node_bindings": {"n0": {"ids": ["X:1"]}}},
                {"analyses": [{"score": 0.2}, {"score": 0.7}]},
            ],
        },
    }

    await sort_results_score(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps([{"id": "sort_results_score"}]),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    message = await get_message("test_response", logger)
    results = message["message"]["results"]
    assert [a["score"] for a in results[0]["analyses"]] == [0.7, 0.2]
    assert "analyses" not in results[1]


@pytest.mark.asyncio
async def test_missing_results_key_yields_empty_results(redis_mock, mocker):
    """A response with no ``results`` field is handled without error and comes
    out with an (empty) results list attached."""
    mock_get = mocker.patch("workers.sort_results_score.worker.get_message")
    mock_get.return_value = {"message": {"knowledge_graph": {"nodes": {}, "edges": {}}}}

    await sort_results_score(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps([{"id": "sort_results_score"}]),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    message = await get_message("test_response", logger)
    assert message["message"]["results"] == []


@pytest.mark.asyncio
async def test_oversized_response_is_failed_before_load(
    redis_mock, mocker, monkeypatch
):
    """An over-limit response must be refused *before* the memory-expanding
    get_message load, so the worker fails it cleanly instead of being
    OOM-killed and the task crash-looping. The guard lives in
    run_task_lifecycle, so it is exercised through process_task.
    """
    from shepherd_utils import shared
    from workers.sort_results_score.worker import process_task

    monkeypatch.setattr(settings, "max_response_size", "1")  # 1-byte cap
    await save_message("big_resp", {"message": {"results": [{"score": 1}]}}, logger)
    # The failure path clears the query's callback rows in Postgres.
    mocker.patch("shepherd_utils.db.cleanup_callbacks", new=mocker.AsyncMock())
    failure = mocker.patch.object(
        shared, "handle_task_failure", new_callable=mocker.AsyncMock
    )
    wrap = mocker.patch.object(shared, "wrap_up_task", new_callable=mocker.AsyncMock)
    # If the guard works, get_message is never reached.
    load = mocker.patch("workers.sort_results_score.worker.get_message")

    class _Limiter:
        def release(self):
            pass

    await process_task(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "big_resp",
                "workflow": json.dumps([{"id": "sort_results_score"}]),
                "log_level": "20",
                "otel": json.dumps({}),
                "metadata": "{}",
            },
        ],
        None,
        logger,
        _Limiter(),
    )

    load.assert_not_called()
    assert failure.called
    assert not wrap.called
    # The stored response was replaced with the empty too-large message.
    message = await get_message("big_resp", logger)
    assert message["message"]["results"] == []
    assert message["status"] == "Error"
    assert message["description"].startswith("Response too large")
