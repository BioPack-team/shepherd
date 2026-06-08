import json
import logging
import pytest

import orjson

from workers.finish_query.worker import finish_query


@pytest.mark.asyncio
async def test_finish_sync_query(redis_mock, mocker):
    """Test that a synchronous query is finished correctly."""
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    response_id = "test_response"
    mock_query_state.return_value = ["", "", "", "", "", "", "", response_id, None]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    mock_callback_response = mocker.patch("workers.finish_query.worker.get_message_raw")
    mock_callback_response.return_value = orjson.dumps({
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
    })

    logger = logging.getLogger(__name__)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": response_id,
                "workflow": json.dumps([]),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    mock_set_query_completed.assert_called_once_with("test", "OK", logger)


@pytest.mark.asyncio
async def test_finish_async_query(redis_mock, mocker):
    """Test that a synchronous query is finished correctly."""
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    response_id = "test_response"
    mock_query_state.return_value = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        response_id,
        "http://test",
    ]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    final_response = {
        "message": {
            "result": "this is the final response",
        },
    }
    mock_callback_response = mocker.patch("workers.finish_query.worker.get_message_raw")
    mock_callback_response.return_value = orjson.dumps(final_response)

    mock_post = mocker.patch("httpx.AsyncClient.post")

    logger = logging.getLogger(__name__)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": response_id,
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args.kwargs
    assert call_kwargs["headers"]["Content-Type"] == "application/json"
    posted_payload = orjson.loads(call_kwargs["content"])
    assert posted_payload["message"] == final_response["message"]
    mock_set_query_completed.assert_called_once_with("test", "OK", logger)
