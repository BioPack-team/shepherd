"""Tests for the node normalization worker."""

import logging

import pytest

from workers.node_norm.worker import (
    canonize_message,
    get_normalized_nodes,
    node_norm,
)

logger = logging.getLogger(__name__)


def _message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "MESH:D001": {"name": "old", "categories": [], "attributes": []},
                    "NCBIGene:1": {
                        "name": "g",
                        "categories": ["biolink:Gene"],
                        "attributes": [],
                    },
                },
                "edges": {
                    "e1": {"subject": "MESH:D001", "object": "NCBIGene:1"},
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": [{"id": "MESH:D001"}],
                        "n1": [{"id": "NCBIGene:1"}],
                    },
                    "analyses": [],
                }
            ],
        }
    }


def test_canonize_message_rewrites_ids_everywhere():
    cmap = {
        "MESH:D001": {
            "id": "PUBCHEM.COMPOUND:1",
            "label": "Canonical",
            "categories": ["biolink:SmallMolecule"],
        }
    }
    message = canonize_message(_message(), cmap, logger)
    nodes = message["message"]["knowledge_graph"]["nodes"]
    assert "PUBCHEM.COMPOUND:1" in nodes
    assert "MESH:D001" not in nodes
    assert nodes["PUBCHEM.COMPOUND:1"]["name"] == "Canonical"
    # Edge subject + result binding rewritten too.
    assert message["message"]["knowledge_graph"]["edges"]["e1"]["subject"] == (
        "PUBCHEM.COMPOUND:1"
    )
    assert (
        message["message"]["results"][0]["node_bindings"]["n0"][0]["id"]
        == "PUBCHEM.COMPOUND:1"
    )


def test_canonize_message_merges_nodes_that_collapse():
    """Two source ids resolving to the same canonical id become one node."""
    msg = {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "A:1": {"name": "a", "categories": ["biolink:Gene"], "attributes": [{"x": 1}]},
                    "B:2": {"name": "", "categories": ["biolink:Protein"], "attributes": [{"y": 2}]},
                },
                "edges": {},
            },
            "results": [],
        }
    }
    cmap = {
        "A:1": {"id": "C:9", "label": "a", "categories": ["biolink:Gene"]},
        "B:2": {"id": "C:9", "label": None, "categories": ["biolink:Protein"]},
    }
    out = canonize_message(msg, cmap, logger)
    nodes = out["message"]["knowledge_graph"]["nodes"]
    assert set(nodes) == {"C:9"}
    assert set(nodes["C:9"]["categories"]) == {"biolink:Gene", "biolink:Protein"}
    assert {"x": 1} in nodes["C:9"]["attributes"]
    assert {"y": 2} in nodes["C:9"]["attributes"]


@pytest.mark.asyncio
async def test_get_normalized_nodes_builds_map(mocker):
    payload = {
        "MESH:D001": {
            "id": {"identifier": "PUBCHEM.COMPOUND:1", "label": "Canonical"},
            "type": ["biolink:SmallMolecule"],
        },
        "UNKNOWN:9": None,
    }
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(
            status_code=200, json=mocker.Mock(return_value=payload), raise_for_status=mocker.Mock()
        ),
    )
    cmap = await get_normalized_nodes(["MESH:D001", "UNKNOWN:9"], logger)
    assert cmap == {
        "MESH:D001": {
            "id": "PUBCHEM.COMPOUND:1",
            "label": "Canonical",
            "categories": ["biolink:SmallMolecule"],
        }
    }


@pytest.mark.asyncio
async def test_get_normalized_nodes_returns_empty_on_failure(mocker):
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        side_effect=Exception("normalizer down"),
    )
    assert await get_normalized_nodes(["X:1"], logger) == {}


@pytest.mark.asyncio
async def test_node_norm_passthrough_when_normalizer_empty(mocker):
    """When the normalizer returns nothing, the message is not re-saved."""
    mocker.patch(
        "workers.node_norm.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=_message(),
    )
    mocker.patch(
        "workers.node_norm.worker.get_normalized_nodes",
        new_callable=mocker.AsyncMock,
        return_value={},
    )
    save = mocker.patch(
        "workers.node_norm.worker.save_message", new_callable=mocker.AsyncMock
    )
    await node_norm(("m", {"response_id": "r1"}), logger)
    save.assert_not_called()
