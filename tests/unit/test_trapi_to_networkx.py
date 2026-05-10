"""Tests for ``shepherd_utils.TRAPI_to_NetworkX``.

These tests exercise the public ``trapi_kg_to_nx`` helper across its mode
matrix (multigraph vs. collapsed, directed vs. undirected, payload modes)
and its weight derivation/transform logic.
"""

import json

import networkx as nx
import pytest

from shepherd_utils.TRAPI_to_NetworkX import trapi_kg_to_nx
from tests.helpers.generate_messages import generate_response


SIMPLE_KG = {
    "knowledge_graph": {
        "nodes": {
            "A": {
                "categories": ["biolink:NamedThing"],
                "name": "Node A",
                "attributes": [
                    {
                        "original_attribute_name": "synonym",
                        "value": "alpha",
                    },
                    {
                        "original_attribute_name": "synonym",
                        "value": "alfa",
                    },
                ],
            },
            "B": {"categories": ["biolink:NamedThing"], "name": "Node B"},
        },
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "predicate": "biolink:related_to",
                "attributes": [
                    {
                        "original_attribute_name": "ngd",
                        "value": 0.5,
                    },
                ],
            },
            "e2": {
                "subject": "A",
                "object": "B",
                "predicate": "biolink:treats",
                "attributes": [
                    {
                        "original_attribute_name": "ngd",
                        "value": 0.25,
                    },
                ],
            },
        },
    }
}


def test_default_returns_multidigraph_with_full_metadata():
    g = trapi_kg_to_nx(SIMPLE_KG)
    assert isinstance(g, nx.MultiDiGraph)
    assert set(g.nodes) == {"A", "B"}
    assert g.number_of_edges() == 2
    # Metadata preserved
    edge_data = list(g.get_edge_data("A", "B").values())
    predicates = {e["predicate"] for e in edge_data}
    assert predicates == {"biolink:related_to", "biolink:treats"}
    # attributes_flat keys on the multigraph edge
    assert all("attributes_flat" in e for e in edge_data)


def test_undirected_multigraph_returns_multigraph_class():
    g = trapi_kg_to_nx(SIMPLE_KG, directed=False)
    assert isinstance(g, nx.MultiGraph)
    # Edges are undirected
    assert g.has_edge("A", "B") and g.has_edge("B", "A")


def test_collapsed_directed_returns_digraph():
    g = trapi_kg_to_nx(SIMPLE_KG, multigraph=False)
    assert isinstance(g, nx.DiGraph)
    assert g.number_of_edges() == 1
    # last-seen metadata wins for the (A,B) pair in collapsed/full mode
    assert g["A"]["B"]["predicate"] in {"biolink:related_to", "biolink:treats"}


def test_collapsed_undirected_returns_graph():
    g = trapi_kg_to_nx(SIMPLE_KG, multigraph=False, directed=False)
    assert isinstance(g, nx.Graph)
    assert not isinstance(g, nx.DiGraph)


def test_weights_disabled_no_weight_on_edges():
    g = trapi_kg_to_nx(SIMPLE_KG)
    for _, _, data in g.edges(data=True):
        assert "weight" not in data


def test_weights_enabled_via_attribute():
    g = trapi_kg_to_nx(SIMPLE_KG, edge_weight_attr="ngd")
    weights = sorted(d["weight"] for _, _, d in g.edges(data=True))
    assert weights == [0.25, 0.5]


def test_weights_default_when_attr_missing():
    g = trapi_kg_to_nx(
        {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}},
                "edges": {
                    "e1": {"subject": "A", "object": "B", "attributes": []},
                },
            }
        },
        edge_weight_attr="ngd",
        default_weight=0.7,
    )
    edge_data = next(iter(g.get_edge_data("A", "B").values()))
    assert edge_data["weight"] == 0.7


def test_weight_default_used_when_attr_unparseable():
    """If the attribute exists but isn't numeric, fall back to default_weight."""
    g = trapi_kg_to_nx(
        {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}},
                "edges": {
                    "e1": {
                        "subject": "A",
                        "object": "B",
                        "attributes": [
                            {"original_attribute_name": "ngd", "value": "not-a-number"},
                        ],
                    },
                },
            }
        },
        edge_weight_attr="ngd",
        default_weight=2.5,
    )
    weights = [d["weight"] for _, _, d in g.edges(data=True)]
    assert weights == [2.5]


def test_weight_transform_applied():
    g = trapi_kg_to_nx(
        SIMPLE_KG,
        edge_weight_attr="ngd",
        edge_weight_transform=lambda x: x * 10,
    )
    weights = sorted(d["weight"] for _, _, d in g.edges(data=True))
    assert weights == [2.5, 5.0]


def test_weight_transform_failure_falls_back_to_raw_value():
    """When the transform raises, the raw value (not default) is returned."""

    def bad_transform(x):
        raise RuntimeError("boom")

    g = trapi_kg_to_nx(
        SIMPLE_KG,
        edge_weight_attr="ngd",
        edge_weight_transform=bad_transform,
        default_weight=99.0,
    )
    weights = sorted(d["weight"] for _, _, d in g.edges(data=True))
    assert weights == [0.25, 0.5]


@pytest.mark.parametrize(
    "agg, expected",
    [
        ("sum", 0.75),
        ("max", 0.5),
        ("min", 0.25),
        ("first", 0.5),
    ],
)
def test_collapsed_weight_only_aggregations(agg, expected):
    g = trapi_kg_to_nx(
        SIMPLE_KG,
        multigraph=False,
        edge_weight_attr="ngd",
        edge_payload="weight_only",
        weight_agg=agg,
    )
    assert g["A"]["B"]["weight"] == pytest.approx(expected)


def test_collapsed_weight_only_unknown_agg_falls_back_to_sum():
    """An unrecognized string aggregator name should fall back to sum."""
    g = trapi_kg_to_nx(
        SIMPLE_KG,
        multigraph=False,
        edge_weight_attr="ngd",
        edge_payload="weight_only",
        weight_agg="not-a-real-name",
    )
    assert g["A"]["B"]["weight"] == pytest.approx(0.75)


def test_collapsed_weight_only_with_callable_aggregator():
    """A user callable aggregator: takes (existing, new), returns combined."""
    g = trapi_kg_to_nx(
        SIMPLE_KG,
        multigraph=False,
        edge_weight_attr="ngd",
        edge_payload="weight_only",
        weight_agg=lambda a, b: a * b,
    )
    assert g["A"]["B"]["weight"] == pytest.approx(0.5 * 0.25)


def test_collapsed_weight_only_with_no_weights_strips_attributes():
    g = trapi_kg_to_nx(
        SIMPLE_KG,
        multigraph=False,
        edge_payload="weight_only",
    )
    # No edge_weight_attr, so the bare edge has no attributes.
    assert g["A"]["B"] == {}


def test_invalid_edge_payload_raises():
    with pytest.raises(ValueError, match="edge_payload"):
        trapi_kg_to_nx(SIMPLE_KG, edge_payload="something_else")


def test_full_payload_with_weight_agg_raises():
    with pytest.raises(ValueError, match="weight_agg"):
        trapi_kg_to_nx(SIMPLE_KG, edge_payload="full", weight_agg="max")


def test_accepts_message_root_dict():
    """Real TRAPI responses wrap the KG in ``message.knowledge_graph`` -- the
    function should walk into that automatically."""
    response = generate_response()
    g = trapi_kg_to_nx(response, multigraph=False, directed=True)
    # The fixture has 3 kg nodes
    assert len(g.nodes) == 3
    # And 2 edges, but they go between different node pairs so collapsed
    # graph should still have 2 directed edges.
    assert g.number_of_edges() == 2


def test_accepts_json_string_input():
    g = trapi_kg_to_nx(json.dumps(SIMPLE_KG))
    assert set(g.nodes) == {"A", "B"}


def test_accepts_bytes_input():
    g = trapi_kg_to_nx(json.dumps(SIMPLE_KG).encode("utf-8"))
    assert g.number_of_edges() == 2


def test_missing_kg_raises_keyerror():
    with pytest.raises(KeyError, match="knowledge_graph"):
        trapi_kg_to_nx({"message": {"results": []}})


def test_edge_endpoint_not_in_node_map_is_added_as_bare_node():
    g = trapi_kg_to_nx(
        {
            "knowledge_graph": {
                "nodes": {"A": {"categories": ["biolink:NamedThing"]}},
                "edges": {
                    "e1": {
                        "subject": "A",
                        "object": "DANGLING",
                        "attributes": [],
                    }
                },
            }
        }
    )
    assert "DANGLING" in g.nodes


def test_edge_with_missing_endpoint_is_skipped():
    g = trapi_kg_to_nx(
        {
            "knowledge_graph": {
                "nodes": {"A": {}},
                "edges": {
                    "bad": {"subject": "A", "attributes": []},  # no object
                },
            }
        }
    )
    assert g.number_of_edges() == 0


def test_node_attributes_flat_dedupes_into_list_for_repeated_keys():
    g = trapi_kg_to_nx(SIMPLE_KG)
    a_flat = g.nodes["A"]["attributes_flat"]
    assert a_flat["synonym"] == ["alpha", "alfa"]


def test_multigraph_edge_id_uses_source_id():
    """In multigraph mode the edge ``id`` attribute comes from the TRAPI key."""
    g = trapi_kg_to_nx(SIMPLE_KG)
    edge_ids = {data["id"] for _, _, data in g.edges(data=True)}
    assert edge_ids == {"e1", "e2"}
