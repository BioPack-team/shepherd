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


@pytest.mark.asyncio
async def test_aragorn_routes_set_input_queries_to_enrichment(redis_mock, mocker):
    """A set-input (MCQ) query is answered by aragorn.graphenrich, which scores
    its own results, rather than lookup + omnicorp + score."""
    from shepherd_server.base_routes import check_workflow

    mcq = {
        "message": {
            "query_graph": {
                "nodes": {
                    "input": {
                        "ids": ["uuid:1"],
                        "member_ids": ["NCBIGene:5111", "NCBIGene:8856"],
                        "set_interpretation": "MANY",
                        "categories": ["biolink:Gene"],
                    },
                    "output": {"categories": ["biolink:ChemicalEntity"]},
                },
                "edges": {"e0": {"subject": "input", "object": "output"}},
            }
        }
    }
    mocker.patch("workers.aragorn.worker.get_message", return_value=mcq)
    task = [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "workflow": json.dumps(None),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]

    await aragorn(task, logging.getLogger(__name__))

    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow] == [
        "aragorn.graphenrich",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ]
    # a client may send the same workflow itself
    check_workflow(workflow)
