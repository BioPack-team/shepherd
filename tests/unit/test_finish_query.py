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
    mock_callback_response = mocker.patch("workers.finish_query.worker.get_message")
    mock_callback_response.return_value = orjson.dumps(
        {
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
    )

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
async def test_finish_internal_ars_query_enqueues_premerge(redis_mock, mocker):
    """A query whose callback is the internal ARS sentinel is not POSTed
    anywhere: finish_query enqueues an intake task on ars.premerge (which
    loads the response from the blob store itself) and completes the query.
    """
    import uuid

    from shepherd_utils.ars.internal import internal_callback_url
    from shepherd_utils.broker import get_task

    child_pk = str(uuid.uuid4())
    response_id = "test_response"
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    mock_query_state.return_value = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        response_id,
        internal_callback_url(child_pk),
    ]
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed"
    )
    mock_get_message = mocker.patch("workers.finish_query.worker.get_message")
    mock_post = mocker.patch("httpx.AsyncClient.post")

    logger = logging.getLogger(__name__)
    await finish_query(
        ["test", {"query_id": "test", "response_id": response_id}],
        logger,
    )

    mock_post.assert_not_called()
    # the payload is not even loaded here -- the intake worker pulls it from
    # the blob store by response_id
    mock_get_message.assert_not_called()
    task = await get_task("ars.premerge", "consumer", "t", logger)
    assert task is not None
    assert task[1]["intake_child_pk"] == child_pk
    assert task[1]["response_id"] == response_id
    assert task[1]["query_id"] == "test"
    assert "otel" in task[1]
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
    mock_callback_response = mocker.patch("workers.finish_query.worker.get_message")
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
