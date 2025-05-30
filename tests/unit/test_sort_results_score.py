import json
import logging
import pytest

from shepherd_utils.db import get_message
from workers.sort_results_score.worker import sort_results_score


@pytest.mark.asyncio
async def test_default_sort(redis_mock, mocker):
    """Test sort results score."""
    mock_callback_id = mocker.patch("workers.sort_results_score.worker.get_query_state")
    response_id = "test"
    mock_callback_id.return_value = ["", "", "", "", "", "", "", response_id]
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
                "workflow": json.dumps([{"id": "sort_results_score"}]),
            },
        ],
        logger,
    )

    message = await get_message(response_id, logger)

    assert len(message["message"]["results"]) == 2
    assert message["message"]["results"][0]["analyses"][0]["score"] == 0.9
    assert message["message"]["results"][1]["analyses"][0]["score"] == 0.1


@pytest.mark.asyncio
async def test_ascending_sort(redis_mock, mocker):
    """Test sort ascending is applied."""
    mock_callback_id = mocker.patch("workers.sort_results_score.worker.get_query_state")
    response_id = "test"
    mock_callback_id.return_value = ["", "", "", "", "", "", "", response_id]
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
                "workflow": json.dumps(
                    [
                        {
                            "id": "sort_results_score",
                            "ascending_or_descending": "ascending",
                        },
                    ],
                ),
            },
        ],
        logger,
    )

    message = await get_message(response_id, logger)

    assert len(message["message"]["results"]) == 2
    assert message["message"]["results"][0]["analyses"][0]["score"] == 0.1
    assert message["message"]["results"][1]["analyses"][0]["score"] == 0.9


@pytest.mark.asyncio
async def test_invalid_json(redis_mock, mocker):
    """Test sort ascending is applied."""
    mock_callback_id = mocker.patch("workers.sort_results_score.worker.get_query_state")
    response_id = "test"
    mock_callback_id.return_value = ["", "", "", "", "", "", "", response_id]
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
                    "workflow": json.dumps(
                        [
                            {
                                "id": "sort_results_score",
                            },
                        ],
                    ),
                },
            ],
            logger,
        )

    assert "Error sorting results" in str(e)
