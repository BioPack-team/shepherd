import copy
import json
import logging

import pytest

from tests.helpers.generate_messages import creative_query
from workers.aragorn.worker import aragorn


@pytest.mark.asyncio
async def test_aragorn_entrypoint(redis_mock, mocker):
    """Aragorn entrypoint sets the auto-generated workflow on the task."""
    mock_callback_response = mocker.patch("workers.aragorn.worker.get_message")
    mock_callback_response.return_value = copy.deepcopy(creative_query)
    logger = logging.getLogger(__name__)

    task = [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            # null workflow triggers the default auto-generated workflow
            "workflow": json.dumps(None),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]

    await aragorn(task, logger)

    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow] == [
        "aragorn.lookup",
        "aragorn.omnicorp",
        "aragorn.score",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ]
