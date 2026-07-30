"""Tests for shepherd_utils.statistical_significance_qualifier (shepherd#134)."""

from shepherd_utils.statistical_significance_qualifier import (
    SIGNIFICANCE_BAND_SCORES,
    SIGNIFICANCE_ORDINAL,
    get_statistical_significance,
)


def test_band_scores_descending():
    bands = [
        "very_strongly_significant",
        "strongly_significant",
        "significant",
        "suggestive",
        "not_significant",
    ]
    scores = [SIGNIFICANCE_BAND_SCORES[b] for b in bands]
    assert scores == sorted(scores, reverse=True) and scores[-1] == 0.0


def test_ordinal_matches_band_order():
    assert SIGNIFICANCE_ORDINAL["very_strongly_significant"] == 4
    assert SIGNIFICANCE_ORDINAL["not_significant"] == 0


def test_lookup_in_qualifiers():
    edge = {
        "qualifiers": [
            {
                "qualifier_type_id": "biolink:statistical_significance_qualifier",
                "qualifier_value": "significant",
            }
        ]
    }
    assert get_statistical_significance(edge) == "significant"


def test_attributes_are_ignored():
    # Qualifiers-only by design: an attributes-only qualifier is NOT read
    # (matches ARAX, qualifier-only as of RTX#2859).
    edge = {
        "attributes": [
            {
                "attribute_type_id": "biolink:statistical_significance_qualifier",
                "value": "suggestive",
            }
        ]
    }
    assert get_statistical_significance(edge) is None


def test_lookup_strips_biolink_prefix():
    edge = {
        "qualifiers": [
            {
                "qualifier_type_id": "biolink:statistical_significance_qualifier",
                "qualifier_value": "biolink:significant",
            }
        ]
    }
    assert get_statistical_significance(edge) == "significant"


def test_only_qualifiers_read():
    # The band comes from edge['qualifiers']; attributes are not consulted.
    edge = {
        "qualifiers": [
            {
                "qualifier_type_id": "biolink:statistical_significance_qualifier",
                "qualifier_value": "significant",
            }
        ],
        "attributes": [
            {
                "attribute_type_id": "biolink:statistical_significance_qualifier",
                "value": "not_significant",
            }
        ],
    }
    assert get_statistical_significance(edge) == "significant"


def test_lookup_none_when_absent():
    assert get_statistical_significance({"attributes": []}) is None
    assert get_statistical_significance({}) is None
