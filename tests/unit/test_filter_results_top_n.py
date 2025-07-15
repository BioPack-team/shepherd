import json
import logging
import pytest

from shepherd_utils.db import get_message
from workers.filter_results_top_n.worker import filter_results_top_n


@pytest.mark.asyncio
async def test_filter_results_top_n(redis_mock, mocker):
    """Test that results are filtered."""
    mock_callback_response = mocker.patch(
        "workers.filter_results_top_n.worker.get_message"
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

    await filter_results_top_n(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps(
                    [{"id": "filter_results_top_n", "max_results": 1}]
                ),
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    message = await get_message("test_response", logger)

    assert len(message["message"]["results"]) == 1
