"""Weighted Reciprocal Rank Fusion of the hosted ARAs' rankings.

Ported from NCATSTranslator/Relay @ 84a66c1 tr_sys/tr_ars/ranker_fusion.py
(Relay PR #897). The ARS post-process reorders a merged message's results by
how highly each ranker (an ARA whose answer has a weight here) placed the
same answer: each match contributes ``weight / (c + rank)``. Results gain
``rrf_score``, ``rrf_rank``, ``rrf_ranker_ranks`` and
``rrf_ranker_contributions`` (TRAPI allows extra members on a Result).

Configuration is read from the environment on every call, with upstream's
variable names, defaults and fall-backs (an invalid value is logged and the
default used):

- ``ARS_RRF_ENABLED`` (default ``true``)
- ``ARS_RRF_ARAGORN_WEIGHT`` (default 0.9; ARAX gets ``1 - weight``)
- ``ARS_RRF_C`` (default 40)

TRAPI 2.0 (Shepherd speaks 2.0; upstream is 1.5): a result's key is the set
of ids in its node bindings, which in 2.0 are one ``{"ids": [...]}`` object
per query node instead of a list of ``{"id": ...}`` objects. Both shapes
are read, so the key is the same set upstream builds. See
docs/ARS_PARITY_REGISTER.md, deviation 18.
"""

import logging
import math
import os

logger = logging.getLogger(__name__)


def _get_enabled():
    return os.getenv("ARS_RRF_ENABLED", "true").strip().lower() == "true"


def _parse_ranker_weights():
    default_weights = {
        "infores:shepherd-aragorn": 0.9,
        "infores:shepherd-arax": 0.1,
    }

    try:
        aragorn_weight = float(
            os.getenv(
                "ARS_RRF_ARAGORN_WEIGHT", default_weights["infores:shepherd-aragorn"]
            )
        )
        if (
            not math.isfinite(aragorn_weight)
            or aragorn_weight < 0
            or aragorn_weight > 1
        ):
            raise ValueError("ARAGORN weight must be between 0 and 1")

        return {
            "infores:shepherd-aragorn": aragorn_weight,
            "infores:shepherd-arax": round(1.0 - aragorn_weight, 8),
        }

    except ValueError as e:
        logger.warning(
            "Invalid environment variable ARS_RRF_ARAGORN_WEIGHT set; using "
            f"default weights 0.9 and 0.1, respectively: {e}"
        )
        return default_weights


def _get_c_value():
    default_c = 40
    rrf_c = str(os.getenv("ARS_RRF_C", default_c))
    try:
        c_value = int(rrf_c)
        if c_value < 0:
            raise ValueError("C must be non-negative")

        return c_value

    except Exception as e:
        logger.warning(
            f"Invalid environment variable ARS_RRF_C set; using default {default_c}: {e}"
        )
        return default_c


def get_config():
    return {
        "enabled": _get_enabled(),
        "weights": _parse_ranker_weights(),
        "c_value": _get_c_value(),
    }


def configured_source(inforesid, agent_name, weights):
    """The weighted source an ARA stands for, from its infores or its agent
    name (``ara-shepherd-aragorn`` -> ``infores:shepherd-aragorn``), or
    None when it has no weight."""
    if inforesid:
        source = str(inforesid).strip().lower()
        if source in weights:
            return source

    if agent_name:
        source = str(agent_name).strip().lower()
        if source in weights:
            return source
        if source.startswith("ara-"):
            source = "infores:" + source[4:]
            if source in weights:
                return source

    return None


def _binding_ids(bindings):
    """The node ids one query node's binding names: TRAPI 2.0's
    ``{"ids": [...]}``, or upstream's 1.5 list of ``{"id": ...}``."""
    if isinstance(bindings, dict):
        ids = bindings.get("ids")
        if isinstance(ids, list):
            return [i for i in ids if i is not None]
        return []
    if isinstance(bindings, list):
        return [
            binding["id"]
            for binding in bindings
            if isinstance(binding, dict) and binding.get("id") is not None
        ]
    return []


def _result_key(result):
    """A frozenset of the node ids a result binds, or None if it binds none."""
    node_bindings = result.get("node_bindings") or {}
    ids = set()
    for bindings in node_bindings.values():
        ids.update(_binding_ids(bindings))
    if not ids:
        return None
    return frozenset(ids)


def _rank_map_from_results(results):
    """Map each result key in a ranker's ordered results to its 1-based rank.

    If two results produce the same key, only the first (best) rank is kept.
    """
    rank_map = {}
    for index, result in enumerate(results or [], start=1):
        key = _result_key(result)
        if key is not None and key not in rank_map:
            rank_map[key] = index
    return rank_map


def apply_weighted_rrf(data, ranker_results_by_source, weights, c_value):
    """Sort ``data["message"]["results"]`` in place with weighted RRF.

    ``ranker_results_by_source`` maps each ranker's source (an infores in
    ``weights``) to its own results, in its own order. Each result matched
    in a ranker's list scores ``weights[source] / (c_value + rank)``; the
    results are reordered by total score (ties keep their order) and tied
    scores share a rank. Returns a summary dict for logging.
    """
    message = data.get("message") if isinstance(data, dict) else None
    if not isinstance(message, dict):
        return {"applied": False, "reason": "missing_message"}

    results = message.get("results")
    if not results:
        return {"applied": False, "reason": "missing_results"}

    rank_maps = {}
    for source, source_results in ranker_results_by_source.items():
        source_key = str(source).strip().lower()
        if source_key in weights:
            rank_map = _rank_map_from_results(source_results)
            if rank_map:
                rank_maps[source_key] = rank_map

    if not rank_maps:
        return {"applied": False, "reason": "missing_ranker_results"}

    scored = []
    matched = 0
    for original_index, result in enumerate(results):
        key = _result_key(result)
        score = 0.0
        source_ranks = {}
        source_contributions = {}
        if key is not None:
            for source, rank_map in rank_maps.items():
                rank = rank_map.get(key)
                if rank is None:
                    continue
                contribution = weights[source] / (c_value + rank)
                score += contribution
                source_ranks[source] = rank
                source_contributions[source] = contribution

        if source_ranks:
            matched += 1
        scored.append(
            (score, original_index, result, source_ranks, source_contributions)
        )

    if matched == 0:
        return {"applied": False, "reason": "no_matching_results"}

    scored.sort(key=lambda item: (-item[0], item[1]))

    previous_score = None
    previous_rank = None
    for rank, (score, _, result, source_ranks, source_contributions) in enumerate(
        scored, start=1
    ):
        result["rrf_score"] = score
        result["rrf_ranker_ranks"] = source_ranks
        result["rrf_ranker_contributions"] = source_contributions

        if score != previous_score:
            previous_score = score
            previous_rank = rank
            result["rrf_rank"] = rank
        else:
            # tied score, hence tied rank
            result["rrf_rank"] = previous_rank

    message["results"] = [result for _, _, result, _, _ in scored]
    return {
        "applied": True,
        "ranker_sources": sorted(rank_maps.keys()),
        "matched_results": matched,
        "result_count": len(results),
        "c_value": c_value,
        "weights": {source: weights[source] for source in sorted(rank_maps.keys())},
    }
