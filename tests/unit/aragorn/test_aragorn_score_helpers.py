"""Tests for the pure helpers in ``workers.aragorn_score.worker``.

Covers:

- ``get_base_weight`` / ``get_source_weight`` / ``get_source_sigmoid`` —
  per-source weight lookups and the 0-centered sigmoid transform.
- ``get_profile`` — selects between the blended/clinical/correlated/curated
  weight profiles.
- ``kirchhoff`` — Kirchhoff index given a graph laplacian.
- ``get_edge_support_kg`` — recursive support-graph kg extraction.
"""

import math

import numpy as np
import pytest

from workers.aragorn_score.worker import (
    BLENDED_PROFILE,
    CLINICAL_PROFILE,
    CORRELATED_PROFILE,
    CURATED_PROFILE,
    DEFAULT_WEIGHT,
    get_base_weight,
    get_edge_support_kg,
    get_profile,
    get_source_sigmoid,
    get_source_weight,
    kirchhoff,
)


# --- get_base_weight ------------------------------------------------------


def test_get_base_weight_known_source():
    assert get_base_weight("infores:omnicorp") == 0


def test_get_base_weight_unknown_source_falls_back_to_default():
    assert get_base_weight("infores:not-real") == DEFAULT_WEIGHT


def test_get_base_weight_custom_weights_table():
    """When a caller provides a custom base_weights table (no default_weight
    key), unknown sources still resolve via DEFAULT_WEIGHT."""
    out = get_base_weight("infores:not-real", base_weights={"infores:foo": 0.7})
    assert out == DEFAULT_WEIGHT


# --- get_source_weight ----------------------------------------------------


def test_get_source_weight_known_source_known_property():
    assert get_source_weight("infores:omnicorp", "literature_co-occurrence") == 1


def test_get_source_weight_known_source_unknown_property():
    """Unknown property of a known source -> 0 (unknown_property)."""
    out = get_source_weight("infores:omnicorp", "not-real")
    assert out == 0


def test_get_source_weight_unknown_source_uses_unknown_source_weight():
    """Unknown source falls back to the unknown_source_weight table."""
    out = get_source_weight("infores:nope", "publications")
    assert out == 1


# --- get_source_sigmoid ---------------------------------------------------


def test_sigmoid_at_midpoint_is_centered():
    """At the midpoint, the sigmoid evaluates to (lower + upper) / 2."""
    parameters = BLENDED_PROFILE["unknown_source_transformation"]["publications"]
    val = get_source_sigmoid(parameters["midpoint"])
    assert val == pytest.approx((parameters["lower"] + parameters["upper"]) / 2)


def test_sigmoid_saturates_below_for_negative_rate():
    """A negative rate (e.g. p_value) means high values approach the lower
    bound; low values approach the upper bound."""
    p_val_low = get_source_sigmoid(
        0.0,
        source="infores:genetics-data-provider",
        property="p_value",
    )
    p_val_high = get_source_sigmoid(
        1.0,
        source="infores:genetics-data-provider",
        property="p_value",
    )
    # Negative rate: high p_value -> low sigmoid; low p_value -> high sigmoid.
    assert p_val_low > p_val_high


def test_sigmoid_unknown_property_uses_unknown_property_default():
    """Property not in the source's transformation table falls back to the
    unknown_source_transformation['unknown_property'] entry, which has lower
    and upper both at 0 -> sigmoid is always 0."""
    val = get_source_sigmoid(
        100.0,
        source="infores:omnicorp",
        property="not-real",
    )
    assert val == 0


# --- get_profile ----------------------------------------------------------


@pytest.mark.parametrize(
    "name, expected_table",
    [
        ("blended", BLENDED_PROFILE),
        ("clinical", CLINICAL_PROFILE),
        ("correlated", CORRELATED_PROFILE),
        ("curated", CURATED_PROFILE),
    ],
)
def test_get_profile_returns_matching_tables(name, expected_table):
    sw, usw, st, ust, bw = get_profile(name)
    assert sw == expected_table["source_weights"]
    assert usw == expected_table["unknown_source_weight"]
    assert st == expected_table["source_transformation"]
    assert ust == expected_table["unknown_source_transformation"]
    assert bw == expected_table["base_weights"]


def test_get_profile_unknown_name_falls_back_to_blended():
    sw, *_ = get_profile("not-a-real-profile")
    assert sw == BLENDED_PROFILE["source_weights"]


# --- kirchhoff ------------------------------------------------------------


def test_kirchhoff_returns_neg_inf_for_invalid_probe_index():
    """When the probe references an index out of range of the laplacian,
    the function catches the IndexError and returns -inf."""
    L = np.array([[1.0, -1.0], [-1.0, 1.0]])
    out = kirchhoff(L, [(0, 5)])  # 5 is out of range
    assert out == -np.inf


def test_kirchhoff_two_node_unit_resistor_returns_unit_distance():
    """The Kirchhoff index between connected unit-weight nodes in a 2-node
    graph is 1."""
    # Laplacian for a 2-node graph with a single unit edge.
    L = np.array([[1.0, -1.0], [-1.0, 1.0]])
    val = kirchhoff(L, [(0, 1)])
    assert val == pytest.approx(1.0)


def test_kirchhoff_returns_real_finite_value_for_three_node_chain():
    """A 3-node line graph 0--1--2 with unit resistors has kirchhoff index 4
    summed over all pairs (1+1+4)/3 ... no wait, summed: (1, 1, 4) = 6 if all
    pairs included. We test a single probe pair which is well-defined."""
    # Laplacian for a 3-node line graph.
    L = np.array(
        [
            [1.0, -1.0, 0.0],
            [-1.0, 2.0, -1.0],
            [0.0, -1.0, 1.0],
        ]
    )
    out = kirchhoff(L, [(0, 1)])
    # A single unit edge: effective resistance is 1.
    assert out == pytest.approx(1.0, abs=1e-6)


# --- get_edge_support_kg --------------------------------------------------


def test_get_edge_support_kg_returns_empty_when_edge_missing():
    """Calling on an edge id that's not in the kg returns the empty default."""
    out = get_edge_support_kg("missing", {"edges": {}}, {})
    assert out == {"node_ids": set(), "edge_ids": set()}


def test_get_edge_support_kg_returns_empty_when_edge_has_no_attributes():
    """An edge with no attributes contributes nothing (the function bails)."""
    kg = {
        "edges": {
            "e1": {"subject": "A", "object": "B"},
        }
    }
    out = get_edge_support_kg("e1", kg, {})
    assert out == {"node_ids": set(), "edge_ids": set()}


def test_get_edge_support_kg_collects_edge_endpoints_when_attributes_present():
    """An edge with any attributes records the edge id and both endpoints."""
    kg = {
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [
                    {"attribute_type_id": "biolink:has_evidence", "value": 5},
                ],
            }
        }
    }
    out = get_edge_support_kg("e1", kg, {})
    assert out == {"node_ids": {"A", "B"}, "edge_ids": {"e1"}}


def test_get_edge_support_kg_recurses_into_support_graphs():
    """Support graphs reference more edges that should be collected too."""
    kg = {
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [
                    {
                        "attribute_type_id": "biolink:support_graphs",
                        "value": ["aux1"],
                    },
                ],
            },
            "e_support": {
                "subject": "B",
                "object": "C",
                "attributes": [
                    {"attribute_type_id": "biolink:has_evidence", "value": 1},
                ],
            },
        }
    }
    aux_graphs = {"aux1": {"edges": ["e_support"], "nodes": ["EXTRA"]}}
    out = get_edge_support_kg("e1", kg, aux_graphs)
    assert out["edge_ids"] == {"e1", "e_support"}
    assert {"A", "B", "C", "EXTRA"}.issubset(out["node_ids"])


def test_get_edge_support_kg_skips_missing_aux_graph():
    """Support graphs that aren't in aux_graphs are silently skipped."""
    kg = {
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [
                    {
                        "attribute_type_id": "biolink:support_graphs",
                        "value": ["doesnt_exist"],
                    },
                ],
            }
        }
    }
    out = get_edge_support_kg("e1", kg, {})
    assert out["edge_ids"] == {"e1"}


def test_get_edge_support_kg_skips_missing_support_edges():
    """Support graph edges that aren't in the kg are silently skipped (defends
    against malformed TRAPI)."""
    kg = {
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [
                    {
                        "attribute_type_id": "biolink:support_graphs",
                        "value": ["aux1"],
                    },
                ],
            }
        }
    }
    aux_graphs = {"aux1": {"edges": ["nonexistent"], "nodes": []}}
    out = get_edge_support_kg("e1", kg, aux_graphs)
    assert out["edge_ids"] == {"e1"}


def test_get_edge_support_kg_threads_through_existing_accumulator():
    """If the caller passes in an existing edge_kg, results are merged into it."""
    kg = {
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [
                    {"attribute_type_id": "biolink:has_evidence", "value": 1},
                ],
            }
        }
    }
    pre_existing = {"node_ids": {"PRE"}, "edge_ids": {"PRE_EDGE"}}
    out = get_edge_support_kg("e1", kg, {}, edge_kg=pre_existing)
    assert "PRE" in out["node_ids"]
    assert "PRE_EDGE" in out["edge_ids"]
    assert "A" in out["node_ids"] and "e1" in out["edge_ids"]
