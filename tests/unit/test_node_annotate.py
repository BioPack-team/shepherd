"""Tests for the node annotation worker."""

import logging

import pytest

from workers.node_annotate.worker import (
    ANNOTATION_ATTRIBUTE_TYPE,
    annotate_message,
    get_annotations,
    node_annotate,
)

logger = logging.getLogger(__name__)


def _message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "NCBIGene:1": {"name": "g", "attributes": []},
                    "MONDO:2": {"name": "d"},  # no attributes key
                },
                "edges": {},
            }
        }
    }


def test_annotate_message_attaches_annotations():
    annotations = {
        "NCBIGene:1": {"symbol": "ABC"},
        "MONDO:2": {"label": "disease"},
    }
    message = _message()
    count = annotate_message(message, annotations)
    assert count == 2
    nodes = message["message"]["knowledge_graph"]["nodes"]
    attr = nodes["NCBIGene:1"]["attributes"][-1]
    assert attr["attribute_type_id"] == ANNOTATION_ATTRIBUTE_TYPE
    assert attr["value"] == {"symbol": "ABC"}
    # A node lacking an attributes list still gets one.
    assert nodes["MONDO:2"]["attributes"][-1]["value"] == {"label": "disease"}


def test_annotate_message_skips_unannotated_nodes():
    message = _message()
    count = annotate_message(message, {"NCBIGene:1": {"symbol": "ABC"}})
    assert count == 1
    assert "attributes" not in message["message"]["knowledge_graph"]["nodes"]["MONDO:2"]


@pytest.mark.asyncio
async def test_get_annotations_per_node_get(mocker):
    """Annotation is one GET per curie against the proxy-safe /curie/<curie>."""
    get = mocker.patch(
        "httpx.AsyncClient.get",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(
            json=mocker.Mock(return_value={"NCBIGene:1": {"symbol": "ABC"}}),
            raise_for_status=mocker.Mock(),
        ),
    )
    out = await get_annotations(["NCBIGene:1"], logger)
    assert out == {"NCBIGene:1": {"symbol": "ABC"}}
    # Single-curie GET path, no trailing slash (keeps the CURIE colon literal).
    assert get.call_args.args[0].endswith("/curie/NCBIGene:1")


@pytest.mark.asyncio
async def test_get_annotations_accepts_bare_annotation_shape(mocker):
    """When the endpoint returns the annotation object directly (not keyed by
    curie), it is still attributed to the requested curie."""
    mocker.patch(
        "httpx.AsyncClient.get",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(
            json=mocker.Mock(return_value={"symbol": "ABC"}),
            raise_for_status=mocker.Mock(),
        ),
    )
    out = await get_annotations(["NCBIGene:1"], logger)
    assert out == {"NCBIGene:1": {"symbol": "ABC"}}


def test_annotate_message_skips_notfound_and_empty():
    from workers.node_annotate.worker import _is_empty_annotation

    assert _is_empty_annotation({}) is True
    assert _is_empty_annotation([{"notfound": True}]) is True
    assert _is_empty_annotation({"symbol": "ABC"}) is False

    message = _message()
    count = annotate_message(
        message,
        {"NCBIGene:1": [{"notfound": True}], "MONDO:2": {"label": "disease"}},
    )
    assert count == 1  # only MONDO:2 attached; the notfound marker is skipped
    # NCBIGene:1 keeps its (empty) attributes list -- no annotation appended.
    assert message["message"]["knowledge_graph"]["nodes"]["NCBIGene:1"][
        "attributes"
    ] == []


@pytest.mark.asyncio
async def test_get_annotations_empty_on_failure(mocker):
    """A total outage (every GET fails) yields an empty map -> pass-through."""
    mocker.patch(
        "httpx.AsyncClient.get",
        new_callable=mocker.AsyncMock,
        side_effect=Exception("annotator down"),
    )
    assert await get_annotations(["X:1", "Y:2"], logger) == {}


@pytest.mark.asyncio
async def test_node_annotate_passthrough_when_empty(mocker):
    mocker.patch(
        "workers.node_annotate.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=_message(),
    )
    mocker.patch(
        "workers.node_annotate.worker.get_annotations",
        new_callable=mocker.AsyncMock,
        return_value={},
    )
    save = mocker.patch(
        "workers.node_annotate.worker.save_message", new_callable=mocker.AsyncMock
    )
    await node_annotate(("m", {"response_id": "r1"}), logger)
    save.assert_not_called()
