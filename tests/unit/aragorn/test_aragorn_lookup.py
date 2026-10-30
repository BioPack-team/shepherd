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


QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["MONDO:0001"]}, "b": {}},
            "edges": {"e": {"subject": "a", "object": "b"}},
        }
    },
    "workflow": [{"id": "aragorn.lookup"}, {"id": "aragorn.omnicorp"}],
}


@pytest.mark.asyncio
async def test_ensure_response_content_fills_untouched_response(redis_mock):
    """A lookup that got nothing back passes on an empty kgraph and results."""
    from shepherd_utils.db import get_message, save_message, save_response
    from workers.aragorn_lookup.worker import ensure_response_content

    logger = logging.getLogger(__name__)
    await save_message("q", copy.deepcopy(QUERY), logger)
    await save_response("r", copy.deepcopy(QUERY), logger)

    await ensure_response_content("q", "r", logger)

    response = await get_message("r", logger)
    assert response["message"]["knowledge_graph"] == {"nodes": {}, "edges": {}}
    assert response["message"]["results"] == []
    assert response["message"]["query_graph"] == QUERY["message"]["query_graph"]
    assert response["workflow"] == QUERY["workflow"]


@pytest.mark.asyncio
async def test_ensure_response_content_leaves_merged_response_alone(
    redis_mock, mocker
):
    """A response with merged content is bigger than the query and never loaded."""
    from shepherd_utils.db import get_message, save_message, save_response
    from workers.aragorn_lookup import worker

    logger = logging.getLogger(__name__)
    merged = copy.deepcopy(QUERY)
    merged["message"]["knowledge_graph"] = {
        "nodes": {"MONDO:0001": {"categories": ["biolink:Disease"]}},
        "edges": {},
    }
    merged["message"]["results"] = []
    await save_message("q", copy.deepcopy(QUERY), logger)
    await save_response("r", copy.deepcopy(merged), logger)
    spy = mocker.spy(worker, "get_message")

    await worker.ensure_response_content("q", "r", logger)

    spy.assert_not_called()
    response = await get_message("r", logger)
    assert response["message"]["knowledge_graph"] == merged["message"]["knowledge_graph"]


@pytest.mark.asyncio
async def test_ensure_response_content_skips_when_locked(redis_mock, mocker):
    """A merge still holding the response lock is not written over."""
    from shepherd_utils.db import get_message, save_message, save_response
    from workers.aragorn_lookup import worker

    logger = logging.getLogger(__name__)
    await save_message("q", copy.deepcopy(QUERY), logger)
    await save_response("r", copy.deepcopy(QUERY), logger)
    mocker.patch.object(worker, "acquire_lock", return_value=False)

    await worker.ensure_response_content("q", "r", logger)

    response = await get_message("r", logger)
    assert "knowledge_graph" not in response["message"]
