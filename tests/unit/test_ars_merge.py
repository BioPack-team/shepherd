"""Tests for cross-ARA result deduplication (Relay mergeDicts parity)."""

import logging

from shepherd_utils.ars_merge import (
    average_result_scores,
    merge_aux_graphs,
    merge_result_maps,
    result_key,
)

logger = logging.getLogger(__name__)


def _result(curie, score=None, analysis_id="a", normalized_score=None):
    r = {
        "node_bindings": {"n0": [{"id": curie}], "n1": [{"id": "DISEASE:1"}]},
        "analyses": [{"resource_id": analysis_id}],
    }
    if score is not None:
        r["score"] = score
    if normalized_score is not None:
        r["normalized_score"] = normalized_score
    return r


def test_result_key_uses_first_binding_ids():
    r = {"node_bindings": {"n0": [{"id": "A:1"}, {"id": "A:2"}], "n1": [{"id": "B:9"}]}}
    assert result_key(r) == frozenset({"A:1", "B:9"})


def test_identical_answers_collapse_and_concat_analyses():
    parent = [_result("DRUG:1", analysis_id="aragorn")]
    child = [_result("DRUG:1", analysis_id="bte")]
    merged = merge_result_maps(parent, child, logger)
    assert len(merged) == 1
    analyses = merged[0]["analyses"]
    assert {a["resource_id"] for a in analyses} == {"aragorn", "bte"}


def test_distinct_answers_stay_separate():
    parent = [_result("DRUG:1")]
    child = [_result("DRUG:2")]
    merged = merge_result_maps(parent, child, logger)
    assert len(merged) == 2


def test_normalized_score_accumulates_then_averages():
    parent = [_result("DRUG:1", normalized_score=80)]
    child = [_result("DRUG:1", normalized_score=40)]
    merged = merge_result_maps(parent, child, logger)
    # During merge the differing scalars accumulate into a list...
    assert merged[0]["normalized_score"] == [80, 40]
    # ...then a third identical answer keeps accumulating (associative).
    third = [_result("DRUG:1", normalized_score=60)]
    merged = merge_result_maps(merged, third, logger)
    assert merged[0]["normalized_score"] == [80, 40, 60]
    # Finalize averages.
    average_result_scores(merged)
    assert merged[0]["normalized_score"] == 60.0


def test_score_field_renamed_to_scores_on_collision():
    parent = [_result("DRUG:1", score=0.9)]
    child = [_result("DRUG:1", score=0.5)]
    merged = merge_result_maps(parent, child, logger)
    assert merged[0]["scores"] == [0.9, 0.5]
    assert "score" not in merged[0]


def test_average_result_scores_ignores_scalar():
    results = [{"normalized_score": 42}]
    average_result_scores(results)
    assert results[0]["normalized_score"] == 42


def test_merge_aux_graphs_unions_edges_by_id():
    parent_aux = {"aux1": {"edges": ["e1", "e2"], "attributes": []}}
    child_aux = {
        "aux1": {"edges": ["e2", "e3"], "attributes": []},
        "aux2": {"edges": ["e9"], "attributes": []},
    }
    merged = merge_aux_graphs(parent_aux, child_aux)
    assert merged["aux1"]["edges"] == ["e1", "e2", "e3"]
    assert merged["aux2"]["edges"] == ["e9"]
