"""Upstream trapi2.0's Expand changes in the ARAX port
(docs/ARAX_TRAPI2_UPSTREAM_SYNC.md, items 9-16). The end-to-end checks are in
test_expand_parity.py and test_query_parity.py."""

import pytest

from shepherd_utils.arax.ARAX_expander import ARAXExpander
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.Expand import expand_utilities as eu
from shepherd_utils.arax.Expand.expand_utilities import QGOrganizedKnowledgeGraph
from shepherd_utils.arax.Expand.trapi_querier import TRAPIQuerier
from shepherd_utils.arax.openapi_server.models.attribute import Attribute
from shepherd_utils.arax.openapi_server.models.attribute_constraint import (
    AttributeConstraint,
)
from shepherd_utils.arax.openapi_server.models.edge import Edge
from shepherd_utils.arax.openapi_server.models.node import Node
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.openapi_server.models.retrieval_source import (
    RetrievalSource,
)

EVIDENCE_COUNT = "biolink:evidence_count"


def _edge(evidence_counts=(), subject="A", object_="B"):
    return Edge(
        subject=subject,
        object=object_,
        predicate="biolink:related_to",
        sources=[
            RetrievalSource(
                resource_id="infores:ctd", resource_role="primary_knowledge_source"
            )
        ],
        attributes=[
            Attribute(attribute_type_id=EVIDENCE_COUNT, value=v)
            for v in evidence_counts
        ],
        knowledge_level="not_provided",
        agent_type="not_provided",
    )


def _constraint(operator, value, negated=False):
    return AttributeConstraint(
        id=EVIDENCE_COUNT,
        name="evidence count",
        operator=operator,
        value=value,
        _not=negated,
    )


@pytest.mark.parametrize(
    "counts,operator,value,negated,want",
    [
        ([7], ">", 5, False, True),
        ([5], ">", 5, False, False),
        ([5], ">", 5, True, True),  # not > is <=
        ([3], "<", 5, False, True),
        (["3"], "<", "5", False, True),  # numeric strings compare as numbers
        ([5], "==", [4, 5], False, True),  # a list is an IN
        ([5], "==", 5, True, False),
        ([], ">", 0, False, False),  # no attribute: not satisfied
        ([], ">", 0, True, False),  # ... even negated
        (["many"], ">", 0, False, False),  # not a number
        ([True], ">", 0, False, False),  # a boolean is not a number
    ],
)
def test_evidence_count_constraint(counts, operator, value, negated, want):
    """Item 11 (upstream 2f78aa95): _edge_satisfies_numeric_constraint."""
    satisfied = ARAXExpander._edge_satisfies_numeric_constraint(
        _edge(counts), _constraint(operator, value, negated)
    )
    assert satisfied is want


def test_evidence_count_post_filter_removes_failing_edges():
    """Item 11: only edges fulfilling the constrained qedge are filtered."""
    qg = QueryGraph.from_dict(
        {
            "nodes": {"n0": {"ids": ["A"]}, "n1": {"categories": ["biolink:Gene"]}},
            "edges": {
                "e0": {
                    "subject": "n0",
                    "object": "n1",
                    "constraints": {"attributes": [_constraint(">", 5).to_dict()]},
                },
                "e1": {"subject": "n1", "object": "n0"},
            },
        }
    )
    kg = QGOrganizedKnowledgeGraph(
        edges={
            "e0": {"high": _edge([9]), "low": _edge([2]), "none": _edge()},
            "e1": {"none": _edge()},
        }
    )
    ARAXExpander()._apply_qedge_attribute_post_filters("e0", qg, kg, ARAXResponse())
    assert set(kg.edges_by_qg_id["e0"]) == {"high"}
    assert set(kg.edges_by_qg_id["e1"]) == {"none"}


def test_evidence_count_constraint_is_supported():
    """Item 11: >, < and == are supported constraints (any value)."""
    supported = ARAXExpander().supported_qedge_attribute_constraints
    for operator in (">", "<", "=="):
        assert ARAXExpander.is_supported_constraint(_constraint(operator, 3), supported)
    assert not ARAXExpander.is_supported_constraint(
        _constraint("matches", "3"), supported
    )


def test_knowledge_source_constraints_remove_every_offending_edge():
    """Item 10 (upstream 2f78aa95, de6863c3): no stray break after the first."""
    qg = QueryGraph.from_dict(
        {
            "nodes": {"n0": {"ids": ["A"]}, "n1": {}},
            "edges": {
                "e0": {
                    "subject": "n0",
                    "object": "n1",
                    "constraints": {
                        "sources": {"behavior": "ALLOW", "values": ["infores:chembl"]}
                    },
                }
            },
        }
    )
    edges = {f"x{i}": _edge(subject=f"S{i}") for i in range(3)}
    kg = QGOrganizedKnowledgeGraph(edges={"e0": dict(edges)})
    ARAXExpander()._apply_knowledge_source_constraints("e0", qg, kg, ARAXResponse())
    assert kg.edges_by_qg_id["e0"] == {}


def test_request_body_leaves_out_arax_post_filters():
    """Item 12 (upstream 2add4986, 2f78aa95): the FDA approval and
    evidence_count constraints are ARAX's; the containers they empty go too,
    and other constraints stay."""
    fda = {
        "id": "biolink:highest_FDA_approval_status",
        "name": "FDA approval",
        "operator": "==",
        "value": "regular approval",
    }
    other = {"id": "knowledge_source", "name": "ks", "operator": "==", "value": ["x"]}
    qg = QueryGraph.from_dict(
        {
            "nodes": {
                "n0": {"categories": ["biolink:Drug"], "constraints": [fda]},
                "n1": {"ids": ["MONDO:1"]},
            },
            "edges": {
                "e0": {
                    "subject": "n0",
                    "object": "n1",
                    "constraints": {"attributes": [_constraint(">", 5).to_dict()]},
                },
                "e1": {
                    "subject": "n0",
                    "object": "n1",
                    "constraints": {
                        "attributes": [_constraint("<", 5).to_dict(), other]
                    },
                },
            },
        }
    )
    querier = TRAPIQuerier(
        response_object=ARAXResponse(),
        kp_name="infores:retriever",
        user_specified_kp=False,
        kp_timeout=None,
    )
    sent = querier._get_prepped_request_body(qg)["message"]["query_graph"]
    assert "constraints" not in sent["nodes"]["n0"]
    assert "constraints" not in sent["edges"]["e0"]
    (kept,) = sent["edges"]["e1"]["constraints"]["attributes"]
    assert set(sent["edges"]["e1"]["constraints"]) == {"attributes"}
    assert kept["id"] == "knowledge_source"
    # the caller's query graph is unchanged
    assert qg.nodes["n0"].constraints[0].id == fda["id"]


def test_aux_graphs_follow_rekeyed_bound_edges():
    """Item 13 (upstream 2add4986): a support graph that cites a bound edge
    cites it by the ARAX edge key the edge is re-keyed to."""
    source = {"resource_id": "infores:ctd", "resource_role": "primary_knowledge_source"}
    edge = {
        "predicate": "biolink:related_to",
        "sources": [source],
        "knowledge_level": "not_provided",
        "agent_type": "not_provided",
    }
    node = {"categories": ["biolink:NamedThing"]}
    response = {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["A:1"]}, "n1": {}},
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {"A:1": node, "B:1": node, "C:1": node},
                "edges": {
                    "bound": dict(edge, subject="A:1", object="B:1"),
                    "support": dict(edge, subject="A:1", object="C:1"),
                },
            },
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["A:1"]}, "n1": {"ids": ["B:1"]}},
                    "analyses": [
                        {
                            "resource_id": "infores:retriever",
                            "edge_bindings": {"e0": {"ids": ["bound"]}},
                        }
                    ],
                }
            ],
            "auxiliary_graphs": {"aux": {"edges": ["bound", "support"]}},
        }
    }
    qg = QueryGraph.from_dict(response["message"]["query_graph"])
    querier = TRAPIQuerier(
        response_object=ARAXResponse(),
        kp_name="infores:retriever",
        user_specified_kp=False,
        kp_timeout=None,
    )
    answer_kg, aux_graphs = querier._load_kp_json_response(response, qg)
    (bound_key,) = answer_kg.edges_by_qg_id["e0"]
    assert bound_key != "bound"
    assert aux_graphs["aux"].edges == [bound_key, "support"]
    assert set(answer_kg.unbound_edges) == {"support"}


def test_unbound_elements_do_not_overwrite_bound_ones():
    """Item 15 (upstream 2add4986): a node / edge that is both bound and in
    the unbound set keeps its bound object and qnode_keys / qedge_keys; an
    unbound-only one gets empty ones."""
    bound_node, unbound_twin, unbound_node = Node(name="bound"), Node(), Node()
    bound_edge, unbound_edge_twin, unbound_edge = _edge(), _edge(), _edge()
    kg = QGOrganizedKnowledgeGraph(
        nodes={"n0": {"A": bound_node}},
        edges={"e0": {"x": bound_edge}},
    )
    kg.unbound_nodes = {"A": unbound_twin, "B": unbound_node}
    kg.unbound_edges = {"x": unbound_edge_twin, "y": unbound_edge}
    standard = eu.convert_qg_organized_kg_to_standard_kg(kg)
    assert standard.nodes["A"] is bound_node and bound_node.qnode_keys == ["n0"]
    assert standard.edges["x"] is bound_edge and bound_edge.qedge_keys == ["e0"]
    assert standard.nodes["B"].qnode_keys == []
    assert standard.edges["y"].qedge_keys == []


def test_get_support_graph_ids():
    """Item 14 (upstream 2add4986): eu.get_support_graph_ids."""
    edge = _edge()
    edge.attributes = [
        Attribute(attribute_type_id="biolink:support_graphs", value=["a", "b"]),
        Attribute(attribute_type_id="biolink:support_graphs", value="c"),
        Attribute(attribute_type_id="biolink:publications", value=["PMID:1"]),
    ]
    assert eu.get_support_graph_ids(edge) == ["a", "b", "c"]
    assert eu.get_support_graph_ids(_edge()) == []
