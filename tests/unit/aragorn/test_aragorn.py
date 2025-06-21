import copy
import json
import logging

import pytest

from shepherd_utils.broker import get_task
from tests.helpers.generate_messages import creative_query
from workers.aragorn.worker import aragorn


@pytest.mark.asyncio
async def test_aragorn_entrypoint(redis_mock, mocker):
    """Test that Aragorn Lookup sends and gets back all the queries."""
    mock_callback_response = mocker.patch("workers.aragorn.worker.get_message")
    mock_callback_response.return_value = copy.deepcopy(creative_query)
    logger = logging.getLogger(__name__)

    await aragorn(
        [
            "test",
            {
                "query_id": "test",
            },
        ],
        logger,
    )

    # Get the task that the ara should have put on the queue
    task = await get_task("aragorn.lookup", "consumer", "test", logger)
    assert task is not None
    workflow = json.loads(task[1]["workflow"])
    # make sure the workflow was correctly passed
    assert len(workflow) == 5
    assert [
        "aragorn.lookup",
        "aragorn.score",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ] == [op["id"] for op in workflow]
