"""Tests for statistical significance qualifier scoring in arax_rank (shepherd#134)."""

import logging

from workers.arax_rank.ranker import ARAXRanker

logger = logging.getLogger(__name__)


def _edge(**kw):
    return {"subject": "A", "object": "B", "predicate": "biolink:related_to", **kw}


def test_significance_additive_boost():
    """A qualifier-bearing edge scores >= the same edge without the qualifier."""
    ranker = ARAXRanker(logger)
    base = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            attributes=[
                {
                    "attribute_type_id": "biolink:pValue",
                    "original_attribute_name": "pValue",
                    "value": "0.001",
                }
            ]
        ),
    )
    boosted = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            attributes=[
                {
                    "attribute_type_id": "biolink:pValue",
                    "original_attribute_name": "pValue",
                    "value": "0.001",
                }
            ],
            qualifiers=[
                {
                    "qualifier_type_id": "biolink:statistical_significance_qualifier",
                    "qualifier_value": "very_strongly_significant",
                }
            ],
        ),
    )
    assert boosted >= base  # additive qualifier can only help or be neutral


def test_significance_score_mapping():
    """Each band maps to band_score * 0.5 trust, appended to the score list."""
    ranker = ARAXRanker(logger)
    # Edge with no attributes and no qualifier -> base only
    no_qual = ranker._calculate_edge_confidence("infores:test--A--B", _edge())
    # Edge with qualifier only (no attributes) -> base + qualifier boost
    with_qual = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            qualifiers=[
                {
                    "qualifier_type_id": "biolink:statistical_significance_qualifier",
                    "qualifier_value": "significant",
                }
            ]
        ),
    )
    # significant = 0.40 * 0.5 = 0.20 additive boost
    assert with_qual > no_qual


def test_not_significant_adds_nothing():
    """not_significant (score 0.0) adds no boost."""
    ranker = ARAXRanker(logger)
    no_qual = ranker._calculate_edge_confidence("infores:test--A--B", _edge())
    not_sig = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            qualifiers=[
                {
                    "qualifier_type_id": "biolink:statistical_significance_qualifier",
                    "qualifier_value": "not_significant",
                }
            ]
        ),
    )
    assert not_sig == no_qual


def test_biolink_prefix_stripped():
    """biolink:-prefixed qualifier values are handled."""
    ranker = ARAXRanker(logger)
    bare = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            qualifiers=[
                {
                    "qualifier_type_id": "biolink:statistical_significance_qualifier",
                    "qualifier_value": "strongly_significant",
                }
            ]
        ),
    )
    prefixed = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            qualifiers=[
                {
                    "qualifier_type_id": "biolink:statistical_significance_qualifier",
                    "qualifier_value": "biolink:strongly_significant",
                }
            ]
        ),
    )
    assert bare == prefixed


def test_qualifier_works_without_attributes():
    """Qualifier scoring works even for edges with no attributes at all."""
    ranker = ARAXRanker(logger)
    # No attributes, no qualifier -> base only (0.5 for infores)
    base_only = ranker._calculate_edge_confidence("infores:test--A--B", _edge())
    # No attributes, but has qualifier -> base + boost
    with_qual = ranker._calculate_edge_confidence(
        "infores:test--A--B",
        _edge(
            qualifiers=[
                {
                    "qualifier_type_id": "biolink:statistical_significance_qualifier",
                    "qualifier_value": "very_strongly_significant",
                }
            ]
        ),
    )
    assert with_qual > base_only
