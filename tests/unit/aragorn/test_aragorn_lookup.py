import copy
import json
import logging

import pytest

from shepherd_utils.broker import get_task
from shepherd_utils.shared import wrap_up_task
from tests.helpers.generate_messages import creative_query
from workers.aragorn_lookup.worker import GROUP, STREAM, aragorn_lookup


@pytest.mark.asyncio
async def test_aragorn_creative_lookup(redis_mock, mocker):
    """Aragorn lookup runs an inferred-edge query and hands off to aragorn.score."""
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
    mock_httpx = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mock_response,
    )
    logger = logging.getLogger(__name__)

    task = [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "workflow": json.dumps([{"id": "aragorn.lookup"}, {"id": "aragorn.score"}]),
            "log_level": "20",
            "otel": json.dumps({}),
            "metadata": json.dumps({}),
        },
    ]

    await aragorn_lookup(task, logger)
    # In production this is called inside process_task; emulate it here so the
    # downstream task lands on the broker for our assertion.
    await wrap_up_task(STREAM, GROUP, task, logger)

    assert mock_httpx.called

    next_task = await get_task("aragorn.score", "consumer", "test", logger)
    assert next_task is not None
    workflow = json.loads(next_task[1]["workflow"])
    assert [op["id"] for op in workflow] == ["aragorn.score"]
