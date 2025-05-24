import json
import logging
import pytest
import redis.asyncio
from shepherd_utils.broker import get_task
from shepherd_utils.db import add_query
from workers.example_ara.worker import example_ara
from tests.helpers.mock_redis import redis_mock


@pytest.mark.asyncio
async def test_example(monkeypatch, mocker):
    _, redis_constructor = await redis_mock()
    monkeypatch.setattr(redis.asyncio, "Redis", redis_constructor)
    logger = logging.getLogger(__name__)

    await example_ara(["test", {"query_id": "test"}], logger)

    # Get the task that the ara should have put on the queue
    task = await get_task("example.lookup", "consumer", "test", logger)
    assert task is not None
    workflow = json.loads(task[1]["workflow"])
    # make sure the workflow was correctly passed
    assert len(workflow) == 5
    assert ["example.lookup", "example.score", "sort_results_score", "filter_results_top_n", "filter_kgraph_orphans"] == [op["id"] for op in workflow]
