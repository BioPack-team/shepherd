"""Tests for shared node canonicalization (``shepherd_utils.ars_norm``).

Covers id re-keying everywhere, node-collapse merging, the ``biolink:xref`` /
``biolink:same_as`` attributes emitted on re-keying (B6), the normalizer map
build (with equivalent_identifiers), and the ``normalize_message`` orchestration.
"""

import logging

import pytest

from shepherd_utils.ars_norm import (
    canonize_message,
    get_normalized_nodes,
    normalize_message,
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
            "equivalent_identifiers": [],
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


def test_canonize_message_emits_xref_and_same_as():
    """A re-keyed node records its original id (xref) and synonyms (same_as)."""
    cmap = {
        "MESH:D001": {
            "id": "PUBCHEM.COMPOUND:1",
            "label": "Canonical",
            "categories": ["biolink:SmallMolecule"],
            "equivalent_identifiers": ["PUBCHEM.COMPOUND:1", "CHEBI:1"],
        }
    }
    message = canonize_message(_message(), cmap, logger)
    attrs = message["message"]["knowledge_graph"]["nodes"]["PUBCHEM.COMPOUND:1"][
        "attributes"
    ]
    xref = next(a for a in attrs if a["attribute_type_id"] == "biolink:xref")
    same_as = next(a for a in attrs if a["attribute_type_id"] == "biolink:same_as")
    assert xref["value"] == ["MESH:D001"]
    assert xref["value_type_id"] == "metatype:NodeIdentifier"
    assert same_as["value"] == ["PUBCHEM.COMPOUND:1", "CHEBI:1"]
    assert same_as["original_attribute_name"] == "equivalent_identifiers"


def test_canonize_message_no_attrs_when_id_unchanged():
    """A node already at its canonical id gets no xref/same_as noise."""
    cmap = {
        "NCBIGene:1": {
            "id": "NCBIGene:1",
            "label": "g",
            "categories": ["biolink:Gene"],
            "equivalent_identifiers": ["NCBIGene:1"],
        }
    }
    message = canonize_message(_message(), cmap, logger)
    node = message["message"]["knowledge_graph"]["nodes"]["NCBIGene:1"]
    assert node["attributes"] == []


def test_canonize_message_omits_same_as_when_no_equivalents():
    cmap = {
        "MESH:D001": {
            "id": "PUBCHEM.COMPOUND:1",
            "label": "Canonical",
            "categories": [],
            "equivalent_identifiers": [],
        }
    }
    message = canonize_message(_message(), cmap, logger)
    attrs = message["message"]["knowledge_graph"]["nodes"]["PUBCHEM.COMPOUND:1"][
        "attributes"
    ]
    kinds = {a["attribute_type_id"] for a in attrs}
    assert "biolink:xref" in kinds
    assert "biolink:same_as" not in kinds


def test_canonize_message_merges_nodes_that_collapse():
    """Two source ids resolving to the same canonical id become one node."""
    msg = {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "A:1": {
                        "name": "a",
                        "categories": ["biolink:Gene"],
                        "attributes": [{"x": 1}],
                    },
                    "B:2": {
                        "name": "",
                        "categories": ["biolink:Protein"],
                        "attributes": [{"y": 2}],
                    },
                },
                "edges": {},
            },
            "results": [],
        }
    }
    cmap = {
        "A:1": {
            "id": "C:9",
            "label": "a",
            "categories": ["biolink:Gene"],
            "equivalent_identifiers": [],
        },
        "B:2": {
            "id": "C:9",
            "label": None,
            "categories": ["biolink:Protein"],
            "equivalent_identifiers": [],
        },
    }
    out = canonize_message(msg, cmap, logger)
    nodes = out["message"]["knowledge_graph"]["nodes"]
    assert set(nodes) == {"C:9"}
    assert set(nodes["C:9"]["categories"]) == {"biolink:Gene", "biolink:Protein"}
    assert {"x": 1} in nodes["C:9"]["attributes"]
    assert {"y": 2} in nodes["C:9"]["attributes"]
    # Both original ids are recorded as xref (they collapsed onto C:9).
    xrefs = [
        a["value"][0]
        for a in nodes["C:9"]["attributes"]
        if isinstance(a, dict) and a.get("attribute_type_id") == "biolink:xref"
    ]
    assert set(xrefs) == {"A:1", "B:2"}


@pytest.mark.asyncio
async def test_get_normalized_nodes_builds_map(mocker):
    payload = {
        "MESH:D001": {
            "id": {"identifier": "PUBCHEM.COMPOUND:1", "label": "Canonical"},
            "equivalent_identifiers": [
                {"identifier": "PUBCHEM.COMPOUND:1"},
                {"identifier": "CHEBI:1"},
            ],
            "type": ["biolink:SmallMolecule"],
        },
        "UNKNOWN:9": None,
    }
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(
            status_code=200,
            json=mocker.Mock(return_value=payload),
            raise_for_status=mocker.Mock(),
        ),
    )
    cmap = await get_normalized_nodes(["MESH:D001", "UNKNOWN:9"], logger)
    assert cmap == {
        "MESH:D001": {
            "id": "PUBCHEM.COMPOUND:1",
            "label": "Canonical",
            "categories": ["biolink:SmallMolecule"],
            "equivalent_identifiers": ["PUBCHEM.COMPOUND:1", "CHEBI:1"],
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
async def test_normalize_message_returns_false_when_no_nodes(mocker):
    called = mocker.patch(
        "shepherd_utils.ars_norm.get_normalized_nodes", new_callable=mocker.AsyncMock
    )
    assert await normalize_message({"message": {}}, logger) is False
    called.assert_not_called()


@pytest.mark.asyncio
async def test_normalize_message_passthrough_when_normalizer_empty(mocker):
    mocker.patch(
        "shepherd_utils.ars_norm.get_normalized_nodes",
        new_callable=mocker.AsyncMock,
        return_value={},
    )
    msg = _message()
    assert await normalize_message(msg, logger) is False
    # Untouched: original id still present.
    assert "MESH:D001" in msg["message"]["knowledge_graph"]["nodes"]


@pytest.mark.asyncio
async def test_normalize_message_canonizes_and_returns_true(mocker):
    mocker.patch(
        "shepherd_utils.ars_norm.get_normalized_nodes",
        new_callable=mocker.AsyncMock,
        return_value={
            "MESH:D001": {
                "id": "PUBCHEM.COMPOUND:1",
                "label": "Canonical",
                "categories": [],
                "equivalent_identifiers": [],
            }
        },
    )
    msg = _message()
    assert await normalize_message(msg, logger) is True
    assert "PUBCHEM.COMPOUND:1" in msg["message"]["knowledge_graph"]["nodes"]
