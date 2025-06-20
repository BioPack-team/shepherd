import copy
import json
import logging

import pytest

from shepherd_utils.db import get_message
from tests.helpers.generate_messages import response_1
from workers.aragorn_score.worker import aragorn_score


@pytest.mark.asyncio
async def test_aragorn_ranker(redis_mock, mocker):
    """Test that Aragorn Ranker returns the correct score."""
    mock_callback_id = mocker.patch("workers.aragorn_score.worker.get_query_state")
    response_id = "test_response"
    mock_callback_id.return_value = ["", "", "", "", "", "", "", response_id]
    mock_callback_response = mocker.patch("workers.aragorn_score.worker.get_message")
    mock_callback_response.return_value = copy.deepcopy(response_1)
    logger = logging.getLogger(__name__)

    await aragorn_score(
        [
            "test",
            {
                "query_id": "test",
                "workflow": json.dumps(
                    [
                        {"id": "aragorn.score"},
                    ]
                ),
            },
        ],
        logger,
    )

    message = await get_message(response_id, logger)

    assert len(message["message"]["results"]) == 2
    assert "score" in message["message"]["results"][0]["analyses"][0]
    assert len(message["message"]["results"][0]["analyses"]) == 1
    assert isinstance(message["message"]["results"][0]["analyses"][0]["score"], float)
    assert (
        message["message"]["results"][0]["analyses"][0]["score"] > 0.063
    )
    assert (
        message["message"]["results"][0]["analyses"][0]["score"] < 0.064
    )
