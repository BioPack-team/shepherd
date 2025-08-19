import json
import logging
import pytest
import redis.asyncio
from shepherd_utils.broker import get_task
from shepherd_utils.db import get_message

from workers.example_ara.worker import example_ara
from workers.example_lookup.worker import example_lookup
from workers.example_score.worker import example_score


@pytest.mark.asyncio
async def test_example(redis_mock):
    # _, redis_constructor = await redis_mock()
    # monkeypatch.setattr(redis.asyncio, "Redis", redis_constructor)
    logger = logging.getLogger(__name__)

    await example_ara(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    # Get the task that the ara should have put on the queue
    task = await get_task("example.lookup", "consumer", "test", logger)
    assert task is not None
    workflow = json.loads(task[1]["workflow"])
    # make sure the workflow was correctly passed
    assert len(workflow) == 5
    assert [
        "example.lookup",
        "example.score",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ] == [op["id"] for op in workflow]


@pytest.mark.asyncio
async def test_example_lookup(mocker, redis_mock):
    mock_callback_id = mocker.patch("workers.example_lookup.worker.add_callback_id")
    mock_callback_id.return_value = "test"
    mock_callback_response = mocker.patch("workers.example_lookup.worker.save_message")
    mock_callback_response.return_value = {}
    mock_running_callbacks = mocker.patch(
        "workers.example_lookup.worker.get_running_callbacks"
    )
    mock_running_callbacks.return_value = []
    mock_response = mocker.Mock()
    mocker.patch("httpx.AsyncClient.post", return_value=mock_response)
    logger = logging.getLogger(__name__)

    await example_lookup(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps(
                    [{"id": "example.lookup"}, {"id": "example.score"}]
                ),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    # Get the task that the ara should have put on the queue
    task = await get_task("example.score", "consumer", "test", logger)
    assert task is not None
    workflow = json.loads(task[1]["workflow"])
    # make sure the workflow was correctly passed
    assert len(workflow) == 1
    assert [
        "example.score",
    ] == [op["id"] for op in workflow]


@pytest.mark.asyncio
async def test_example_score(mocker, redis_mock):
    """Test example scoring."""
    response_id = "test_response"
    mock_callback_response = mocker.patch("workers.example_score.worker.get_message")
    mock_callback_response.return_value = {
        "message": {
            "results": [
                {
                    "analyses": [
                        {},
                    ],
                },
            ],
        },
    }
    logger = logging.getLogger(__name__)

    await example_score(
        [
            "test",
            {
                "query_id": "test",
                "response_id": response_id,
                "workflow": json.dumps([{"id": "example.score"}]),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    message = await get_message(response_id, logger)

    assert len(message["message"]["results"]) == 1
    assert "score" in message["message"]["results"][0]["analyses"][0]
    assert len(message["message"]["results"][0]["analyses"]) == 1
    assert isinstance(message["message"]["results"][0]["analyses"][0]["score"], float)
