import json
import logging
import pytest
import redis.asyncio
from shepherd_utils.broker import get_task

from workers.example_ara.worker import example_ara
from workers.example_lookup.worker import example_lookup


@pytest.mark.asyncio
async def test_example(redis_mock):
    # _, redis_constructor = await redis_mock()
    # monkeypatch.setattr(redis.asyncio, "Redis", redis_constructor)
    logger = logging.getLogger(__name__)

    await example_ara(["test", {"query_id": "test"}], logger)

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
    mock_callback_response = mocker.patch(
        "workers.example_lookup.worker.save_callback_response"
    )
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
                "workflow": json.dumps(
                    [{"id": "example.lookup"}, {"id": "example.score"}]
                ),
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
