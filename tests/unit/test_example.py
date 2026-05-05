import json
import logging

import pytest

from shepherd_utils.broker import get_task
from shepherd_utils.db import get_message
from shepherd_utils.shared import wrap_up_task
from workers.example_ara.worker import GROUP as EXAMPLE_GROUP
from workers.example_ara.worker import STREAM as EXAMPLE_STREAM
from workers.example_ara.worker import example_ara
from workers.example_lookup.worker import GROUP as LOOKUP_GROUP
from workers.example_lookup.worker import STREAM as LOOKUP_STREAM
from workers.example_lookup.worker import example_lookup
from workers.example_score.worker import example_score


@pytest.mark.asyncio
async def test_example(redis_mock, mocker):
    """example_ara installs the default workflow and hands off to example.lookup."""
    mocker.patch("workers.example_ara.worker.get_message", return_value={})
    logger = logging.getLogger(__name__)

    task = [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]

    await example_ara(task, logger)
    # In production, process_task calls wrap_up_task. Emulate it here.
    await wrap_up_task(EXAMPLE_STREAM, EXAMPLE_GROUP, task, logger)

    next_task = await get_task("example.lookup", "consumer", "test", logger)
    assert next_task is not None
    workflow = json.loads(next_task[1]["workflow"])
    assert [op["id"] for op in workflow] == [
        "example.lookup",
        "example.score",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ]


@pytest.mark.asyncio
async def test_example_lookup(mocker, redis_mock):
    """example_lookup fans out callback queries and hands off to example.score."""
    mocker.patch(
        "workers.example_lookup.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={"parameters": {"timeout": 5}},
    )
    mocker.patch(
        "workers.example_lookup.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.example_lookup.worker.save_message",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.example_lookup.worker.cleanup_callbacks",
        new_callable=mocker.AsyncMock,
    )
    mock_running_callbacks = mocker.patch(
        "workers.example_lookup.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    logger = logging.getLogger(__name__)

    task = [
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
    ]

    await example_lookup(task, logger)
    await wrap_up_task(LOOKUP_STREAM, LOOKUP_GROUP, task, logger)

    assert mock_post.called
    assert mock_running_callbacks.called

    next_task = await get_task("example.score", "consumer", "test", logger)
    assert next_task is not None
    workflow = json.loads(next_task[1]["workflow"])
    assert [op["id"] for op in workflow] == ["example.score"]


@pytest.mark.asyncio
async def test_example_score(mocker, redis_mock):
    """example_score writes a random score for every analysis on the message."""
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
    assert isinstance(
        message["message"]["results"][0]["analyses"][0]["score"], float
    )
