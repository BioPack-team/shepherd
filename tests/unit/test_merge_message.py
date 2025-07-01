import copy
import json
import logging
import pytest

from shepherd_utils.db import get_message, save_message
from workers.merge_message.worker import merge_message

from tests.helpers.generate_messages import (
    generate_response,
    generate_query,
    response_2,
)


@pytest.mark.asyncio
async def test_message_merge(redis_mock, mocker):
    """Test sort results score."""
    callback_id = "test_callback"
    query_id = "test"
    mock_callback_id = mocker.patch("workers.merge_message.worker.get_query_state")
    response_id = "test_response"
    mock_callback_id.return_value = ["", "", "", "", "", "", "", response_id]
    mock_remove_callback_id = mocker.patch(
        "workers.merge_message.worker.remove_callback_id"
    )

    logger = logging.getLogger(__name__)
    await save_message(query_id, generate_query(), logger)
    callback_response = copy.deepcopy(response_2)
    await save_message(
        f"{callback_id}_query_graph", response_2["message"]["query_graph"], logger
    )
    await save_message(callback_id, callback_response, logger)
    await save_message(response_id, generate_response(), logger)

    await merge_message(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "callback_id": "test_callback",
            },
        ],
        logger,
    )

    message = await get_message(response_id, logger)

    assert len(message["message"]["results"]) == 3

    mock_remove_callback_id.assert_called_once()
