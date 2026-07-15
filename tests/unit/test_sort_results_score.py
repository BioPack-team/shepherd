import json
import logging
import pytest

from shepherd_utils.config import settings
from shepherd_utils.db import ResponseTooLargeError, get_message, save_message
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
async def test_invalid_json(redis_mock, mocker):
    """Test sort ascending is applied."""
    mock_callback_response = mocker.patch(
        "workers.sort_results_score.worker.get_message"
    )
    mock_callback_response.return_value = {
        "message": {
            "results": [
                {
                    "analysis": {},
                },
            ],
        },
    }

    logger = logging.getLogger(__name__)

    with pytest.raises(KeyError) as e:
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
                            },
                        ],
                    ),
                    "log_level": "20",
                    "otel": json.dumps({}),
                },
            ],
            logger,
        )

    assert "analyses" in str(e.value)


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
async def test_oversized_response_raises_before_load(redis_mock, mocker, monkeypatch):
    """An over-limit response must raise ResponseTooLargeError *before* the
    memory-expanding get_message load, so run_task_lifecycle can fail it cleanly
    instead of the process being OOM-killed and the task crash-looping.
    """
    monkeypatch.setattr(settings, "max_response_size", "1")  # 1-byte cap
    await save_message("big_resp", {"message": {"results": [{"score": 1}]}}, logger)

    # If the guard works, get_message is never reached.
    load = mocker.patch("workers.sort_results_score.worker.get_message")

    with pytest.raises(ResponseTooLargeError):
        await sort_results_score(
            [
                "test",
                {
                    "query_id": "test",
                    "response_id": "big_resp",
                    "workflow": json.dumps([{"id": "sort_results_score"}]),
                    "log_level": "20",
                    "otel": json.dumps({}),
                },
            ],
            logger,
        )

    load.assert_not_called()
