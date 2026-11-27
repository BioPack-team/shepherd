"""Upstream trapi2.0's resultify, result-transformer, ranker and filter changes
in the ARAX port (docs/ARAX_TRAPI2_UPSTREAM_SYNC.md, items 24-29). The
end-to-end checks are in test_query_parity.py (golden rules H20 and H21),
upstream_suite/test_ARAX_filter_kg.py and test_arax_rank.py."""

from shepherd_utils.arax import ARAX_resultify
from shepherd_utils.arax.ARAX_filter_kg import ARAXFilterKG
from shepherd_utils.arax.ARAX_ranker import _make_row_data_item
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.ARAX_resultify import (
    analyze_message_get_referenced_IDs,
    set_qnode_is_set,
)
from shepherd_utils.arax.openapi_server.models.analysis import Analysis
from shepherd_utils.arax.openapi_server.models.attribute import Attribute
from shepherd_utils.arax.openapi_server.models.auxiliary_graph import AuxiliaryGraph
from shepherd_utils.arax.openapi_server.models.edge import Edge
from shepherd_utils.arax.openapi_server.models.edge_binding import EdgeBinding
from shepherd_utils.arax.openapi_server.models.knowledge_graph import KnowledgeGraph
from shepherd_utils.arax.openapi_server.models.message import Message
from shepherd_utils.arax.openapi_server.models.node import Node
from shepherd_utils.arax.openapi_server.models.node_binding import NodeBinding
from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
from shepherd_utils.arax.openapi_server.models.q_node import QNode
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.openapi_server.models.result import Result
from shepherd_utils.arax.openapi_server.models.retrieval_source import (
    RetrievalSource,
)
from shepherd_utils.arax.result_transformer import _prune_orphan_node_bindings


def _edge(subject, object_, attributes=None, source="infores:arax"):
    return Edge(
        subject=subject,
        object=object_,
        predicate="biolink:related_to",
        sources=[
            RetrievalSource(
                resource_id=source, resource_role="primary_knowledge_source"
            )
        ],
        attributes=attributes,
        knowledge_level="not_provided",
        agent_type="not_provided",
    )


def test_binding_ids_are_sorted():
    """Item 24: a COLLATE qnode's binding lists its ids sorted, whatever the
    hash order of the set they were collected in."""
    keys = [f"HP:{i}" for i in (9, 10, 2, 33, 1)]
    nodes = {"MONDO:1": Node()}
    nodes["MONDO:1"].qnode_keys = ["n0"]
    edges = {}
    for key in keys:
        nodes[key] = Node()
        nodes[key].qnode_keys = ["n1"]
        edges[f"e-{key}"] = _edge("MONDO:1", key)
        edges[f"e-{key}"].qedge_keys = ["e0"]
    qnodes = {"n0": QNode(), "n1": QNode()}
    set_qnode_is_set(qnodes["n1"], True)
    qg = QueryGraph(nodes=qnodes, edges={"e0": QEdge(subject="n0", object="n1")})
    results = ARAX_resultify._get_results_for_kg_by_qg(
        KnowledgeGraph(nodes=nodes, edges=edges), qg
    )
    assert len(results) == 1
    assert results[0].node_bindings["n1"].ids == sorted(keys)
    edge_ids = results[0].analyses[0].edge_bindings["e0"].ids
    assert edge_ids == sorted(f"e-{key}" for key in keys)


def _message():
    """A result binds A-e1-B and names support graph sg1 (edge e3, B-C);
    e2 (A-B) and e4 (B-C) are bound by nothing."""
    return Message(
        knowledge_graph=KnowledgeGraph(
            nodes={"A": Node(), "B": Node(), "C": Node()},
            edges={
                "e1": _edge("A", "B"),
                "e2": _edge("A", "B"),
                "e3": _edge("B", "C"),
                "e4": _edge("B", "C"),
            },
        ),
        auxiliary_graphs={"sg1": AuxiliaryGraph(edges=["e3"])},
        results=[
            Result(
                node_bindings={
                    "n0": NodeBinding(ids=["A"]),
                    "n1": NodeBinding(ids=["B"]),
                },
                analyses=[
                    Analysis(
                        resource_id="infores:arax",
                        edge_bindings={"e0": EdgeBinding(ids=["e1"])},
                        support_graphs=["sg1"],
                    )
                ],
            )
        ],
    )


def test_both_endpoints_rule_is_the_default():
    nodes, edges, aux_graphs, results = analyze_message_get_referenced_IDs(
        _message(), ARAXResponse()
    )
    assert nodes == {"A", "B", "C"}
    assert edges == {"e1", "e2", "e3", "e4"}
    assert aux_graphs == {"sg1"}
    assert results == {0}


def test_without_the_both_endpoints_rule_only_bound_and_support_edges_count():
    """Item 25: TRAPI 2.0's final cleanup keeps no KG edge that no result or
    support graph references, even if both its nodes are referenced."""
    nodes, edges, aux_graphs, _ = analyze_message_get_referenced_IDs(
        _message(), ARAXResponse(), apply_both_endpoints_rule=False
    )
    assert nodes == {"A", "B", "C"}
    assert edges == {"e1", "e3"}
    assert aux_graphs == {"sg1"}


def test_without_the_both_endpoints_rule_nested_support_graphs_are_followed():
    message = _message()
    message.knowledge_graph.edges["e1"].attributes = [
        Attribute(attribute_type_id="biolink:support_graphs", value=["sg2"])
    ]
    message.auxiliary_graphs["sg2"] = AuxiliaryGraph(edges=["e4"])
    _, edges, aux_graphs, _ = analyze_message_get_referenced_IDs(
        message, ARAXResponse(), apply_both_endpoints_rule=False
    )
    assert edges == {"e1", "e3", "e4"}
    assert aux_graphs == {"sg1", "sg2"}


def test_prune_orphan_node_bindings():
    """Item 26: a qnode with no binding (an unmatched optional group) is
    skipped, not a KeyError; a binding pruned to no ids is deleted."""
    kg_edges = {"e1": _edge("A", "B")}
    node_bindings = {
        "n0": NodeBinding(ids=["A", "A2"]),
        "n1": NodeBinding(ids=["B"]),
        "n3": NodeBinding(ids=["Z"]),
    }
    _prune_orphan_node_bindings(
        node_bindings,
        {"e0": EdgeBinding(ids=["e1"])},
        {"n0", "n1", "n2", "n3"},
        kg_edges,
    )
    assert {k: b.ids for k, b in node_bindings.items()} == {
        "n0": ["A"],
        "n1": ["B"],
    }


def test_row_data_item_is_never_none():
    """Item 27 (shepherd_utils' ARAX_ranker; the arax_rank worker's port is
    checked in test_arax_rank.py)."""
    assert _make_row_data_item(None) == ""
    assert _make_row_data_item(0.5) == 0.5
    assert _make_row_data_item("drug") == "drug"


def test_discrete_attribute_filter_accepts_edge_sources():
    """Item 29: knowledge_source / infores values validate from edge.sources
    (TRAPI 1.4+), so the filter removes the edges they name."""
    message = Message(
        query_graph=QueryGraph(
            nodes={"n0": QNode(), "n1": QNode()},
            edges={"e0": QEdge(subject="n0", object="n1")},
        ),
        knowledge_graph=KnowledgeGraph(
            nodes={"A": Node(), "B": Node()},
            edges={
                "semmed": _edge("A", "B", attributes=[], source="infores:semmeddb"),
                "ctd": _edge("A", "B", attributes=[], source="infores:ctd"),
            },
        ),
    )
    for node_key, node in message.knowledge_graph.nodes.items():
        node.qnode_keys = ["n0" if node_key == "A" else "n1"]
    for edge in message.knowledge_graph.edges.values():
        edge.qedge_keys = ["e0"]
    response = ARAXResponse()
    response.envelope = type("Envelope", (), {"message": message})()
    parameters = {
        "action": "remove_edges_by_discrete_attribute",
        "edge_attribute": "knowledge_source",
        "value": "infores:semmeddb",
        "remove_connected_nodes": "false",
    }
    response = ARAXFilterKG().apply(response, parameters)
    assert response.status == "OK", response.show()
    assert set(message.knowledge_graph.edges) == {"ctd"}
