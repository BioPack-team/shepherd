"""Sugeno-integral result scoring.

No longer part of the live post-process pipeline: upstream removed the
`compute_from_results` call from `post_process` along with the external
Appraiser (Relay PR #884) in favor of `premerge.appraise_confidence`. The
module (like upstream's scoring.py) remains, golden-tested, in case it is
called again.

Ported from NCATSTranslator/Relay @ 3e65975 tr_sys/tr_ars/scoring.py with two
output-preserving changes, both verified by the golden parity suite:
  - weight_sets uses an explicit dict instead of writing through locals()
    (the upstream trick stops working under PEP 667 / Python 3.13);
  - the sympy lambda solve is memoized per weight tuple (upstream re-solves
    the identical polynomial for every result).
"""

import itertools
from functools import lru_cache
from operator import itemgetter

import sympy
from sympy import expand, simplify, solve, symbols


def compute_from_results(results):
    sugeno_scores = []
    weighted_means = []
    for result in results:
        novelty = 0
        confidence = 0
        clinical_evidence = 0
        score_blank_factor = 0
        if "ordering_components" in result.keys():
            if "novelty" in result["ordering_components"].keys():
                novelty = result["ordering_components"]["novelty"]
            if "confidence" in result["ordering_components"].keys():
                confidence = result["ordering_components"]["confidence"]
            if "clinical_evidence" in result["ordering_components"].keys():
                clinical_evidence = result["ordering_components"]["clinical_evidence"]
        sugeno_score = compute_sugeno(
            confidence, novelty, clinical_evidence, score_blank_factor
        )[2]
        weighted_mean = compute_weighted_mean(
            confidence, novelty, clinical_evidence, score_blank_factor
        )

        sugeno_scores.append(sugeno_score)
        weighted_means.append(weighted_mean)
        result["sugeno"] = sugeno_score
        result["weighted_mean"] = weighted_mean

    final_ranks = compute_sugeno_rank(sugeno_scores)
    for i, rank in enumerate(final_ranks):
        results[i]["rank"] = int(rank)
    results = sorted(results, key=lambda d: d["rank"])
    return results


def weight_sets(
    lambda_val,
    weight_confidence,
    weight_novelty,
    weight_clinical,
    weight_blank_factor,
    n=2,
):
    """Weight sets for the Sugeno integral.

    Same recurrence as upstream (w_ab = w_a + w_b + lambda*w_a*w_b, built up
    over permutations), with an explicit accumulator dict instead of
    locals()-poking. Outputs match upstream bit-for-bit (golden-tested).
    """
    list_x = ["confidence", "novelty", "clinical", "blank_factor"]
    values = {
        "weight_confidence": weight_confidence,
        "weight_novelty": weight_novelty,
        "weight_clinical": weight_clinical,
        "weight_blank_factor": weight_blank_factor,
    }
    dict_t = {}
    for k in range(2, n + 1):
        list_perm = list(itertools.permutations(list_x, r=k))
        for i in list_perm:
            t = "weight"
            t_l = "weight"
            t_f = "weight"
            for idj, j in enumerate(i):
                t = t + "_" + j
                if idj < k - 1:
                    t_l = t_l + "_" + j
                if idj == k - 1:
                    t_f = t_f + "_" + j
            values[t] = (
                values[t_l] + values[t_f] + (lambda_val * (values[t_l] * values[t_f]))
            )
            dict_t[t] = round(float(values[t]), 2)
    dict_t["weight_confidence"] = weight_confidence
    dict_t["weight_novelty"] = weight_novelty
    dict_t["weight_clinical"] = weight_clinical
    dict_t["weight_blank_factor"] = weight_blank_factor
    return dict_t


@lru_cache(maxsize=64)
def _solve_lambda(
    weight_confidence, weight_novelty, weight_clinical, weight_blank_factor
):
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
        if type(i) == sympy.core.add.Add:  # noqa: E721 -- upstream check
            i = i.as_real_imag()[0]
        if i >= -1 and i != 0:
            lambda_val = i
    return lambda_val


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
    A = "weight"
    for idi, i in enumerate(score_sorted):
        A = A + "_" + i[0]
        w_sorted[A] = w_sets[A]
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
    weighted_mean = (
        score_confidence * weight_confidence
        + score_novelty * weight_novelty
        + score_clinical * weight_clinical
        + score_blank_factor * weight_blank_factor
    ) / (weight_confidence + weight_novelty + weight_clinical + weight_blank_factor)
    return weighted_mean


def compute_sugeno_rank(sugeno_scores):
    sugeno_sorted = sorted(enumerate(sugeno_scores), key=lambda x: x[1], reverse=True)
    ranks = {index: rank + 1 for rank, (index, value) in enumerate(sugeno_sorted)}
    indexed_ranks = [ranks[index] for index in range(len(sugeno_scores))]
    return indexed_ranks
