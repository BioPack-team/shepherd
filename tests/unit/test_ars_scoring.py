"""Tests for the Sugeno final-ranking port (``shepherd_utils.ars_scoring``).

Expected sugeno/weighted_mean/rank values were cross-checked against the ARS
reference ``scoring.py`` (NCATS Translator Relay) for the same inputs.
"""

import logging

from shepherd_utils.ars_scoring import (
    compute_from_results,
    compute_sugeno,
    compute_weighted_mean,
)

logger = logging.getLogger(__name__)


def _oc(confidence=0.0, novelty=0.0, clinical_evidence=0.0):
    return {
        "ordering_components": {
            "confidence": confidence,
            "novelty": novelty,
            "clinical_evidence": clinical_evidence,
        }
    }


def test_weighted_mean_is_confidence_clinical_average():
    # Default weights: confidence=1, novelty=0, clinical=1, blank=0.
    assert compute_weighted_mean(0.9, 0.1, 0.5, 0) == (0.9 + 0.5) / 2


def test_sugeno_is_max_of_full_weight_factors():
    # novelty carries zero weight, so it never drives the Sugeno score.
    _, _, sugeno = compute_sugeno(0.5, 0.99, 0.2, 0)
    assert sugeno == 0.5  # max(confidence=0.5, clinical=0.2)


def test_compute_from_results_ranks_and_sorts_by_sugeno():
    results = [
        _oc(confidence=0.9, novelty=0.1, clinical_evidence=0.5),  # sugeno 0.9
        _oc(confidence=0.2, novelty=0.8, clinical_evidence=0.3),  # sugeno 0.3
        _oc(confidence=0.5, novelty=0.5, clinical_evidence=0.5),  # sugeno 0.5
        _oc(confidence=0.0, novelty=0.0, clinical_evidence=0.0),  # sugeno 0.0
    ]
    out = compute_from_results(results)
    # Sorted best-first by rank.
    assert [r["rank"] for r in out] == [1, 2, 3, 4]
    assert [round(float(r["sugeno"]), 4) for r in out] == [0.9, 0.5, 0.3, 0.0]
    assert [round(float(r["weighted_mean"]), 4) for r in out] == [0.7, 0.5, 0.25, 0.0]


def test_compute_from_results_missing_components_default_zero():
    results = [{"analyses": []}, _oc(confidence=0.4, clinical_evidence=0.4)]
    out = compute_from_results(results)
    # The result with components ranks first; the bare one gets sugeno 0.
    assert out[0]["rank"] == 1
    assert out[0]["sugeno"] == 0.4
    bare = next(r for r in out if "ordering_components" not in r)
    assert bare["sugeno"] == 0
    assert bare["rank"] == 2


def test_compute_from_results_ties_keep_input_order():
    results = [
        _oc(confidence=0.7, clinical_evidence=0.7),
        _oc(confidence=0.7, clinical_evidence=0.7),
    ]
    out = compute_from_results(results)
    assert [r["rank"] for r in out] == [1, 2]


def test_compute_from_results_empty():
    assert compute_from_results([]) == []
