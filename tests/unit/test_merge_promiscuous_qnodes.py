"""Regression tests for promiscuous-qnode filtering on branching query graphs.

``filter_promiscuous_results`` exists to drop results that hang off a
promiscuous *intermediate* hub -- the C in ``A<-treats-B-part_of->C<-part_of-D``,
where C can contain thousands of members and produces a tie-scored pile of junk.

The shape test that finds C ("a qnode with two edges sharing predicates and
qualifiers") also matches a qnode that a rule simply *branches from*, which
includes the pinned node and the answer node. Filtering on either is
destructive: the pinned node is bound to the same knode in every result, so the
whole result set is deleted.
"""

import logging

import pytest

from workers.aragorn_lookup.query_templates import TEMPLATES_BY_NAME
from workers.merge_message.worker import (
    filter_promiscuous_results,
    get_promiscuous_qnodes,
)

logger = logging.getLogger(__name__)


def branching_qgraph():
    """The two_witness_inhibition shape: branches from both pinned and answer."""
    return TEMPLATES_BY_NAME["two_witness_inhibition"].render(
        "MONDO:0004979", "ON", "SN"
    )


def two_witness_results(n_a=4, n_b=3):
    return [
        {
            "node_bindings": {
                "ON": [{"id": "MONDO:0004979"}],
                "n_protein_a": [{"id": f"UniProtKB:A{a}"}],
                "n_protein_b": [{"id": f"UniProtKB:B{b}"}],
                "SN": [{"id": f"CHEBI:{100 + a}"}],
            },
            "analyses": [],
        }
        for a in range(n_a)
        for b in range(n_b)
    ]


def response_with(qgraph, results):
    return {
        "message": {
            "query_graph": qgraph,
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "auxiliary_graphs": {},
            "results": results,
        }
    }


def test_pinned_qnode_is_never_promiscuous():
    """It is bound to the same knode in every result by construction."""
    centers = get_promiscuous_qnodes(
        response_with(branching_qgraph(), []), answer_qnode="SN"
    )

    assert "ON" not in centers


def test_answer_qnode_is_never_promiscuous():
    """Its most common binding is the best-supported answer."""
    centers = get_promiscuous_qnodes(
        response_with(branching_qgraph(), []), answer_qnode="SN"
    )

    assert "SN" not in centers


def test_branching_template_results_survive_the_filter():
    """Before the fix this deleted all twelve results, so the whole template was
    silently invisible downstream -- an ablation arm that included it looked
    identical to one that did not."""
    response = response_with(branching_qgraph(), two_witness_results())
    assert len(response["message"]["results"]) == 12

    filter_promiscuous_results(response, logger, answer_qnode="SN")

    assert len(response["message"]["results"]) == 12


def test_a_genuine_intermediate_hub_is_still_filtered():
    """The behaviour the filter exists for must be untouched:
    A<-treats-B-part_of->C<-part_of-D, where C is a promiscuous intermediate."""
    qgraph = {
        "nodes": {
            "ON": {"ids": ["MONDO:0004979"]},
            "B": {"categories": ["biolink:ChemicalEntity"]},
            "C": {"categories": ["biolink:ChemicalEntity"]},
            "SN": {"categories": ["biolink:ChemicalEntity"]},
        },
        "edges": {
            "e0": {"subject": "B", "object": "ON", "predicates": ["biolink:treats"]},
            "e1": {"subject": "B", "object": "C", "predicates": ["biolink:part_of"]},
            "e2": {"subject": "SN", "object": "C", "predicates": ["biolink:part_of"]},
        },
    }
    results = [
        {
            "node_bindings": {
                "ON": [{"id": "MONDO:0004979"}],
                "B": [{"id": f"CHEBI:{i}"}],
                "C": [{"id": "CHEBI:HUB"}],
                "SN": [{"id": f"CHEBI:{900 + i}"}],
            },
            "analyses": [],
        }
        for i in range(12)
    ]
    response = response_with(qgraph, results)

    assert get_promiscuous_qnodes(response, answer_qnode="SN") == ["C"]
    filter_promiscuous_results(response, logger, answer_qnode="SN")
    assert response["message"]["results"] == []


@pytest.mark.parametrize("answer_qnode", [None, "SN"])
def test_pinned_node_protected_even_without_an_answer_qnode(answer_qnode):
    """The pinned node is identifiable from the query graph alone, so callers
    that cannot supply the answer qnode still get the important half."""
    centers = get_promiscuous_qnodes(
        response_with(branching_qgraph(), []), answer_qnode=answer_qnode
    )

    assert "ON" not in centers
