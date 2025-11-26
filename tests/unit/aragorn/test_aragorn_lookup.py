import copy
import json
import logging

import pytest

from shepherd_utils.broker import get_task
from tests.helpers.generate_messages import creative_query
from workers.aragorn_lookup.worker import aragorn_lookup


@pytest.mark.asyncio
async def test_aragorn_creative_lookup(redis_mock, mocker):
    """Test that Aragorn Lookup sends and gets back all the queries."""
    mock_callback_response = mocker.patch("workers.aragorn_lookup.worker.get_message")
    mock_callback_response.return_value = copy.deepcopy(creative_query)
    mocker.patch("workers.aragorn_lookup.worker.save_message")
    mocker.patch("workers.aragorn_lookup.worker.add_callback_id")
    mock_running_callbacks = mocker.patch(
        "workers.aragorn_lookup.worker.get_running_callbacks"
    )
    mock_running_callbacks.return_value = []
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_httpx = mocker.patch("httpx.AsyncClient.post", return_value=mock_response)
    logger = logging.getLogger(__name__)

    await aragorn_lookup(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "test_response",
                "workflow": json.dumps(
                    [{"id": "aragorn.lookup"}, {"id": "aragorn.score"}]
                ),
                "log_level": "20",
                "otel": json.dumps({}),
            },
        ],
        logger,
    )

    assert mock_httpx.called

    # Get the task that the ara should have put on the queue
    task = await get_task("aragorn.score", "consumer", "test", logger)
    assert task is not None
    workflow = json.loads(task[1]["workflow"])
    # make sure the workflow was correctly passed
    assert len(workflow) == 1
    assert [
        "aragorn.score",
    ] == [op["id"] for op in workflow]
