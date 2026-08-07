"""Shared helpers for biolink:statistical_significance_qualifier (shepherd#134).

The qualifier (enum StatisticalSignificanceQualifierEnum) is_a statement_qualifier,
a descendant of `qualifier` in biolink-model, so BMT/Retriever route it into TRAPI
edge.qualifiers[]. get_statistical_significance() therefore reads edge['qualifiers']
ONLY. (ARAX is likewise qualifier-only as of RTXteam/RTX#2859 — its defensive
edge.attributes lookup was removed; add a fallback here only if a KP is found to
send the qualifier as an attribute.) Shared by aragorn_score + arax_rank (ranking).
"""

from typing import Any, Dict, Optional

SIGNIFICANCE_QUALIFIER_TYPE_ID = "biolink:statistical_significance_qualifier"

# Conservative ranking scores per band. TODO: revisit once all KGs populate the
# qualifier (rollout asymmetry: qualifier-bearing edges scored against edges that
# lack it entirely). Mirrors the RTX ARAX_ranker change (RTXteam/RTX#2858).
SIGNIFICANCE_BAND_SCORES: Dict[str, float] = {
    "very_strongly_significant": 0.70,
    "strongly_significant": 0.55,
    "significant": 0.40,
    "suggestive": 0.15,
    "not_significant": 0.0,
}

# Ordinal scale of the significance bands (higher = more significant).
SIGNIFICANCE_ORDINAL: Dict[str, int] = {
    "very_strongly_significant": 4,
    "strongly_significant": 3,
    "significant": 2,
    "suggestive": 1,
    "not_significant": 0,
}

# Source-agnostic trust weight applied to the band score in ranking (conservative;
# matches RTX trust=0.5). NOT routed through aragorn's per-source get_source_weight.
SIGNIFICANCE_SOURCE_WEIGHT: float = 0.5


def _strip_biolink(value: Any) -> Optional[str]:
    if isinstance(value, str) and value.startswith("biolink:"):
        return value[len("biolink:"):]
    return value


def get_statistical_significance(edge: Dict[str, Any]) -> Optional[str]:
    """Return the bare significance band for a dict-based TRAPI edge, or None.

    Reads edge['qualifiers'] only (it is a biolink qualifier; BMT/Retriever route it
    there). Strips any biolink: prefix from the value.
    """
    for q in edge.get("qualifiers") or []:
        if q.get("qualifier_type_id") == SIGNIFICANCE_QUALIFIER_TYPE_ID:
            return _strip_biolink(q.get("qualifier_value"))
    return None
