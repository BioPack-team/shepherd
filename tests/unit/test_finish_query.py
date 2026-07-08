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


def _patch_finish_ars(mocker, *, timed_out):
    """Wire finish_query for an ARS parent with no submitter callback url."""
    mock_query_state = mocker.patch("workers.finish_query.worker.get_query_state")
    mock_query_state.return_value = ["", "", "", "", "", "", "", "resp", None]
    mocker.patch(
        "workers.finish_query.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={
            "message": {
                "results": [{"analyses": []}, {"analyses": []}],
                "auxiliary_graphs": {"aux1": {}},
            }
        },
    )
    mocker.patch(
        "workers.finish_query.worker.get_ars_parent_completion_meta",
        new_callable=mocker.AsyncMock,
        return_value={"is_ars_parent": True, "timed_out": timed_out},
    )
    mocker.patch(
        "workers.finish_query.worker.cleanup_callbacks", new_callable=mocker.AsyncMock
    )
    publish = mocker.patch(
        "workers.finish_query.worker.publish_ars_event", new_callable=mocker.AsyncMock
    )
    unsub = mocker.patch(
        "workers.finish_query.worker.remove_all_subscribers",
        new_callable=mocker.AsyncMock,
    )
    completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed",
        new_callable=mocker.AsyncMock,
    )
    return publish, unsub, completed


def _task():
    return [
        "t",
        {
            "query_id": "test",
            "response_id": "resp",
            "workflow": json.dumps([]),
            "log_level": "20",
        },
    ]


@pytest.mark.asyncio
async def test_finish_ars_parent_emits_completion_and_unsubscribes(redis_mock, mocker):
    publish, unsub, completed = _patch_finish_ars(mocker, timed_out=False)
    await finish_query(_task(), logging.getLogger(__name__))

    unsub.assert_awaited_once()
    assert unsub.await_args.args[0] == "test"
    event = publish.await_args.args[0]
    assert event["parent_qid"] == "test"
    assert event["event_type"] == "last_merged_completed"
    assert event["complete"] is True
    assert event["code"] == 200
    assert event["stats"] == {"results": 2, "auxiliary_graphs": 1}
    assert "timed_out" not in event
    # Clean completion keeps the OK status.
    assert completed.await_args.args[1] == "OK"


@pytest.mark.asyncio
async def test_finish_ars_parent_timeout_marks_598(redis_mock, mocker):
    publish, unsub, completed = _patch_finish_ars(mocker, timed_out=True)
    await finish_query(_task(), logging.getLogger(__name__))

    event = publish.await_args.args[0]
    assert event["code"] == 598
    assert event["timed_out"] is True
    # A timed-out parent finishes with the distinct TIMEOUT terminal state.
    assert completed.await_args.args[1] == "TIMEOUT"
