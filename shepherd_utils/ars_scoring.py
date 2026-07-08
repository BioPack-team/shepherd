"""Final result ranking (Sugeno integral + weighted mean).

Faithful port of the ARS reference ``scoring.py`` (NCATS Translator Relay). Each
result's appraiser ``ordering_components`` (confidence / novelty /
clinical_evidence, plus a zero blank factor) are aggregated by a Sugeno fuzzy
integral and a weighted mean; results are then ranked by Sugeno score (rank 1 =
best) and returned sorted by rank. This is the ARS's authoritative final ordering
-- run as the last step of ``answer_appraise`` after the appraiser has attached
``ordering_components``.

Two deliberate, output-preserving departures from the reference for robustness:
  * the lambda root of the fuzzy-measure polynomial depends only on the (fixed)
    weights, so it is solved once and cached rather than re-solved per result;
  * ``weight_sets`` accumulates into an explicit dict instead of the reference's
    ``locals()`` mutation (which is unreliable under PEP 667 / Python 3.13). The
    arithmetic and rounding are identical, so the numbers match the reference.
"""

import itertools
from functools import lru_cache
from operator import itemgetter

import sympy
from sympy import expand, simplify, solve, symbols

# The reference's fuzzy-measure weights: confidence and clinical_evidence carry
# full weight, novelty and the blank factor carry none.
DEFAULT_WEIGHTS = (1.0, 0.0, 1.0, 0.0)


@lru_cache(maxsize=None)
def _solve_lambda(
    weight_confidence: float,
    weight_novelty: float,
    weight_clinical: float,
    weight_blank_factor: float,
):
    """Solve the fuzzy-measure lambda for a weight set (Sugeno normalization).

    lambda is the root of ``prod(1 + w_i*x) - (1 + x)`` with ``x >= -1`` and
    ``x != 0``. It depends only on the weights, so it is cached across results.
    """
    x = symbols("lambda")
    polynomial = expand(
        (
            (1 + weight_confidence * x)
            * (1 + weight_novelty * x)
            * (1 + weight_clinical * x)
            * (1 + weight_blank_factor * x)
        )
        - (1 + x)
    )
    simplified_polynomial = simplify(polynomial)
    solutions = solve(simplified_polynomial, x)
    lambda_val = None
    for i in solutions:
        if type(i) is sympy.core.add.Add:
            i = i.as_real_imag()[0]
        if i >= -1 and i != 0:
            lambda_val = i
    return lambda_val


def weight_sets(
    lambda_val,
    weight_confidence,
    weight_novelty,
    weight_clinical,
    weight_blank_factor,
    n=2,
):
    """Build the cumulative fuzzy-measure weights used by the Sugeno integral.

    For every ordered subset (permutation) of the four factors of length 2..n,
    the measure of the subset is ``g(A) + g(B) + lambda*g(A)*g(B)`` where A is the
    prefix and B the final element -- accumulated over increasing lengths.
    """
    list_x = ["confidence", "novelty", "clinical", "blank_factor"]
    # Unrounded running measures keyed by "weight_<a>_<b>..." (the len-1 keys are
    # the raw base weights); dict_t holds the rounded values the caller reads.
    w = {
        "weight_confidence": weight_confidence,
        "weight_novelty": weight_novelty,
        "weight_clinical": weight_clinical,
        "weight_blank_factor": weight_blank_factor,
    }
    dict_t = {}
    for k in range(2, n + 1):
        for perm in itertools.permutations(list_x, r=k):
            t = "weight"
            t_l = "weight"
            t_f = "weight"
            for idj, j in enumerate(perm):
                t = t + "_" + j
                if idj < k - 1:
                    t_l = t_l + "_" + j
                if idj == k - 1:
                    t_f = t_f + "_" + j
            val = w[t_l] + w[t_f] + (lambda_val * (w[t_l] * w[t_f]))
            w[t] = val
            dict_t[t] = round(float(val), 2)
    dict_t["weight_confidence"] = weight_confidence
    dict_t["weight_novelty"] = weight_novelty
    dict_t["weight_clinical"] = weight_clinical
    dict_t["weight_blank_factor"] = weight_blank_factor
    return dict_t


def compute_sugeno(
    score_confidence,
    score_novelty,
    score_clinical,
    score_blank_factor,
    weight_confidence=1.0,
    weight_novelty=0.0,
    weight_clinical=1.0,
    weight_blank_factor=0.0,
):
    """Sugeno fuzzy integral of the four factor scores under the weight set.

    Returns ``(score_sorted, w_sorted, sugeno)`` to mirror the reference.
    """
    lambda_val = _solve_lambda(
        weight_confidence, weight_novelty, weight_clinical, weight_blank_factor
    )
    w_sets = weight_sets(
        lambda_val,
        weight_confidence,
        weight_novelty,
        weight_clinical,
        weight_blank_factor,
        n=4,
    )

    score_all = [
        ["confidence", score_confidence],
        ["novelty", score_novelty],
        ["clinical", score_clinical],
        ["blank_factor", score_blank_factor],
    ]
    score_sorted = sorted(score_all, key=itemgetter(1), reverse=True)
    w_sorted = {}
    a = "weight"
    for i in score_sorted:
        a = a + "_" + i[0]
        w_sorted[a] = w_sets[a]
    keys = list(w_sorted.keys())
    sugeno = max(
        min(score_sorted[0][1], w_sorted[keys[0]]),
        min(score_sorted[1][1], w_sorted[keys[1]]),
        min(score_sorted[2][1], w_sorted[keys[2]]),
        min(score_sorted[3][1], w_sorted[keys[3]]),
    )
    return score_sorted, w_sorted, sugeno


def compute_weighted_mean(
    score_confidence,
    score_novelty,
    score_clinical,
    score_blank_factor,
    weight_confidence=1.0,
    weight_novelty=0.0,
    weight_clinical=1.0,
    weight_blank_factor=0.0,
):
    """Weighted mean of the four factor scores under the weight set."""
    return (
        score_confidence * weight_confidence
        + score_novelty * weight_novelty
        + score_clinical * weight_clinical
        + score_blank_factor * weight_blank_factor
    ) / (
        weight_confidence + weight_novelty + weight_clinical + weight_blank_factor
    )


def compute_sugeno_rank(sugeno_scores):
    """Rank scores descending (rank 1 = highest Sugeno score), ties share input order."""
    sugeno_sorted = sorted(
        enumerate(sugeno_scores), key=lambda x: x[1], reverse=True
    )
    ranks = {index: rank + 1 for rank, (index, value) in enumerate(sugeno_sorted)}
    return [ranks[index] for index in range(len(sugeno_scores))]


def compute_from_results(results):
    """Score, rank, and sort ``results`` by the Sugeno integral (ARS final order).

    Writes ``sugeno``, ``weighted_mean`` and ``rank`` onto each result and returns
    the results sorted by ``rank`` (best first). Missing ``ordering_components``
    default every factor to 0 (matching the reference).
    """
    sugeno_scores = []
    for result in results:
        components = result.get("ordering_components") or {}
        novelty = components.get("novelty", 0)
        confidence = components.get("confidence", 0)
        clinical_evidence = components.get("clinical_evidence", 0)
        score_blank_factor = 0
        sugeno_score = compute_sugeno(
            confidence, novelty, clinical_evidence, score_blank_factor
        )[2]
        weighted_mean = compute_weighted_mean(
            confidence, novelty, clinical_evidence, score_blank_factor
        )
        sugeno_scores.append(sugeno_score)
        result["sugeno"] = sugeno_score
        result["weighted_mean"] = weighted_mean

    final_ranks = compute_sugeno_rank(sugeno_scores)
    for i, rank in enumerate(final_ranks):
        # int() because ranks can arrive as numpy int types.
        results[i]["rank"] = int(rank)
    return sorted(results, key=lambda d: d["rank"])
