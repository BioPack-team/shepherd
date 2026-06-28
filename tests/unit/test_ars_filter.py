"""Tests for the ARS result filters (Relay parity)."""

import logging

from shepherd_utils.ars_filter import (
    apply_filters,
    hop_level_filter,
    node_type_filter,
    score_filter,
    specific_node_filter,
)

logger = logging.getLogger(__name__)


def _result(ids, normalized_score=None):
    """A result binding one curie per qnode (n0, n1, ...)."""
    nb = {f"n{i}": [{"id": curie}] for i, curie in enumerate(ids)}
    r = {"node_bindings": nb, "analyses": []}
    if normalized_score is not None:
        r["normalized_score"] = normalized_score
    return r


def test_hop_level_filter_keeps_fewer_than_limit():
    results = [_result(["A:1"]), _result(["A:1", "B:2"]), _result(["A:1", "B:2", "C:3"])]
    kept = hop_level_filter(results, 3)
    assert len(kept) == 2  # 1 and 2 bindings kept, 3 dropped


def test_score_filter_strict_range():
    results = [
        _result(["A:1"], normalized_score=10),
        _result(["A:1"], normalized_score=50),
        _result(["A:1"], normalized_score=90),
        _result(["A:1"]),  # no score -> dropped
    ]
    kept = score_filter(results, [20, 80])
    assert [r["normalized_score"] for r in kept] == [50]


def test_node_type_filter_drops_forbidden_category():
    kg_nodes = {
        "A:1": {"categories": ["biolink:Gene"]},
        "B:2": {"categories": ["biolink:Disease"]},
    }
    results = [_result(["A:1"]), _result(["B:2"])]
    kept = node_type_filter(kg_nodes, results, ["Disease"])
    bound = [list(r["node_bindings"].values())[0][0]["id"] for r in kept]
    assert bound == ["A:1"]


def test_specific_node_filter_drops_curie():
    results = [_result(["A:1"]), _result(["B:2"])]
    kept = specific_node_filter(results, ["B:2"])
    assert len(kept) == 1
    assert list(kept[0]["node_bindings"].values())[0][0]["id"] == "A:1"


def test_apply_filters_chains_in_order():
    message = {
        "message": {
            "knowledge_graph": {"nodes": {"A:1": {"categories": ["biolink:Gene"]}}},
            "results": [
                _result(["A:1"], normalized_score=50),
                _result(["A:1", "B:2"], normalized_score=50),
            ],
        }
    }
    count = apply_filters(message, [("hop", 2), ("score", [10, 90])], logger)
    assert count == 1
    assert len(message["message"]["results"]) == 1
