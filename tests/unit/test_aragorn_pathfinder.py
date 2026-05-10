"""Tests for ``workers.aragorn_pathfinder.worker.shadowfax``.

The pathfinder entry point validates the input pathfinder query, builds a
3-hop expanded query, and delegates to the gandalf stream. We exercise the
validation paths and the happy path (mocking out postgres + redis).
"""

import json
import logging

import pytest

from workers.aragorn_pathfinder.worker import shadowfax


logger = logging.getLogger(__name__)


def _make_task(workflow=None):
    return [
        "test",
        {
            "query_id": "qid-1",
            "response_id": "rid-1",
            "workflow": json.dumps(workflow if workflow is not None else []),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


def _pathfinder_message(constraints=None):
    msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["MONDO:0001"]},
                    "n1": {"ids": ["MONDO:0002"]},
                },
                "paths": {
                    "p0": {"subject": "n0", "object": "n1"},
                },
            }
        },
        "parameters": {"timeout": 5},
    }
    if constraints is not None:
        msg["message"]["query_graph"]["paths"]["p0"]["constraints"] = constraints
    return msg


@pytest.mark.asyncio
async def test_shadowfax_happy_path_dispatches_to_gandalf(redis_mock, mocker):
    """A valid pathfinder query: callback registered, threehop saved, gandalf
    task enqueued, polling loop exits when no callbacks remain."""
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=_pathfinder_message(),
    )
    mock_add_cb = mocker.patch(
        "workers.aragorn_pathfinder.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mock_save = mocker.patch(
        "workers.aragorn_pathfinder.worker.save_message",
        new_callable=mocker.AsyncMock,
    )
    mock_add_task = mocker.patch(
        "workers.aragorn_pathfinder.worker.add_task",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )

    await shadowfax(_make_task(), logger)

    assert mock_add_cb.called
    assert mock_save.called
    assert mock_add_task.called
    target_stream, payload = mock_add_task.call_args.args[:2]
    assert target_stream == "gandalf"
    assert payload["target"] == "aragorn"

    # The threehop saved to redis should be a 3-edge query whose endpoints
    # match the original pinned pathfinder nodes.
    saved_id, saved_threehop = mock_save.call_args.args[:2]
    qg = saved_threehop["message"]["query_graph"]
    assert "n0" in qg["nodes"] and "n1" in qg["nodes"]
    assert "intermediate_0" in qg["nodes"] and "intermediate_1" in qg["nodes"]
    assert set(qg["edges"].keys()) == {"e0", "e1", "e2"}
    # Endpoints stitch through the intermediates.
    assert qg["edges"]["e0"]["subject"] == "n0"
    assert qg["edges"]["e0"]["object"] == "intermediate_0"
    assert qg["edges"]["e2"]["object"] == "n1"


@pytest.mark.asyncio
async def test_shadowfax_requires_two_distinct_pinned_nodes(redis_mock, mocker):
    """Only one distinct pinned id is invalid for a pathfinder query."""
    msg = _pathfinder_message()
    msg["message"]["query_graph"]["nodes"]["n1"]["ids"] = ["MONDO:0001"]  # duplicate
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    with pytest.raises(Exception, match="two pinned nodes"):
        await shadowfax(_make_task(), logger)


@pytest.mark.asyncio
async def test_shadowfax_rejects_multiple_constraints(redis_mock, mocker):
    """Multiple constraints on the path is unsupported."""
    msg = _pathfinder_message(
        constraints=[
            {"intermediate_categories": ["biolink:Gene"]},
            {"intermediate_categories": ["biolink:Disease"]},
        ]
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    with pytest.raises(Exception, match="multiple constraints"):
        await shadowfax(_make_task(), logger)


@pytest.mark.asyncio
async def test_shadowfax_rejects_multiple_intermediate_categories(redis_mock, mocker):
    """A single constraint may not list multiple intermediate categories."""
    msg = _pathfinder_message(
        constraints=[{"intermediate_categories": ["biolink:Gene", "biolink:Disease"]}]
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    with pytest.raises(Exception, match="multiple intermediate categories"):
        await shadowfax(_make_task(), logger)


@pytest.mark.asyncio
async def test_shadowfax_uses_intermediate_category_from_constraint(redis_mock, mocker):
    """When a constraint provides an intermediate category, the threehop's
    intermediates carry that category instead of biolink:NamedThing."""
    msg = _pathfinder_message(
        constraints=[{"intermediate_categories": ["biolink:Gene"]}]
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mock_save = mocker.patch(
        "workers.aragorn_pathfinder.worker.save_message",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.add_task",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )

    await shadowfax(_make_task(), logger)
    threehop = mock_save.call_args.args[1]
    nodes = threehop["message"]["query_graph"]["nodes"]
    assert nodes["intermediate_0"]["categories"] == ["biolink:Gene"]
    assert nodes["intermediate_1"]["categories"] == ["biolink:Gene"]


@pytest.mark.asyncio
async def test_shadowfax_propagates_gandalf_parameters(redis_mock, mocker):
    """Custom gandalf_parameters in the input should ride along into the
    saved threehop's parameters."""
    msg = _pathfinder_message()
    msg["parameters"]["gandalf_parameters"] = {
        "min_information_content": 1,
        "max_node_degree": 10,
        "dehydrated": False,
    }
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mock_save = mocker.patch(
        "workers.aragorn_pathfinder.worker.save_message",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.add_task",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )

    await shadowfax(_make_task(), logger)
    threehop = mock_save.call_args.args[1]
    gp = threehop["parameters"]["gandalf_parameters"]
    assert gp == {
        "min_information_content": 1,
        "max_node_degree": 10,
        "dehydrated": False,
    }
