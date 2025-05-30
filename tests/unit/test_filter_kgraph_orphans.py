import copy
import json
import logging
import pytest

from shepherd_utils.db import get_message
from workers.filter_kgraph_orphans.worker import filter_kgraph_orphans


@pytest.mark.asyncio
async def test_filter_kgraph_orphans(redis_mock, mocker):
    """Test that kgraph orphans are removed."""
    mock_query_state = mocker.patch(
        "workers.filter_kgraph_orphans.worker.get_query_state"
    )
    response_id = "test"
    mock_query_state.return_value = ["", "", "", "", "", "", "", response_id, None]
    mock_callback_response = mocker.patch(
        "workers.filter_kgraph_orphans.worker.get_message"
    )
    initial_response = {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0001": {},
                    "MONDO:0002": {},
                    "MONDO:0003": {},
                },
                "edges": {
                    "1234": {},
                    "2345": {},
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "sn": [
                            {"id": "MONDO:0001"},
                        ],
                        "on": [
                            {"id": "MONDO:0002"},
                        ],
                    },
                    "analyses": [
                        {
                            "edge_bindings": {
                                "e0": [
                                    {"id": "1234"},
                                ],
                            },
                            "score": 0.1,
                        },
                    ],
                },
            ],
        },
    }

    # filter kgraph orphans modifies the original message
    mock_callback_response.return_value = copy.deepcopy(initial_response)

    logger = logging.getLogger(__name__)

    await filter_kgraph_orphans(
        [
            "test",
            {
                "query_id": "test",
                "workflow": json.dumps(
                    [{"id": "filter_results_top_n", "max_results": 1}]
                ),
            },
        ],
        logger,
    )

    message = await get_message(response_id, logger)

    assert len(initial_response["message"]["knowledge_graph"]["nodes"]) == 3
    assert len(initial_response["message"]["knowledge_graph"]["edges"]) == 2

    assert len(message["message"]["knowledge_graph"]["nodes"]) == 2
    assert len(message["message"]["knowledge_graph"]["edges"]) == 1
