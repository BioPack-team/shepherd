"""Evaluate why ARAX scores differ between Shepherd Dev (internal) and ARAX CI.

Run from the repo root (needs networkx, numpy, scipy):

    python scripts/arax_ranker_eval.py

It reads the four responses in scripts/responses/arax-{dev,ci}/ and:

1. Finds the tie block in each published result list: a run of scores that
   drop by exactly 0.001 per rank, which is what ARAX's
   ``_break_ties_and_preserve_order`` produces for results whose raw ranker
   scores are equal.
2. Checks that set-iteration order under PYTHONHASHSEED=0 (the seed Shepherd's
   ARAX image pins) reproduces Dev's order inside the tie block. Resultify
   creates one result per SN node by iterating a ``set`` of CURIEs, and the
   sort before tie-breaking is stable, so that order decides the scores.
3. Rebuilds an approximate ranker input from each response (the published
   bindings were rewritten by the ResultTransformer after ranking) and
   re-ranks it with Shepherd's ranker port in different result orders, with
   the current tie handling and with an order-independent alternative.

The rebuilt input is not exact: the 17 results the NGD-inf filter removed
after ranking are gone, and the ranking-time bindings are inferred, so
absolute scores won't match the published ones. The experiment is about
whether scores depend on input order, which doesn't need exact values.
"""

import collections
import copy
import json
import logging
import os
import random
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "workers" / "arax_rank"))
import ranker as R  # noqa: E402

logging.disable(logging.CRITICAL)
RESP = ROOT / "scripts" / "responses"
DEV = RESP / "arax-dev" / "MONDO_0007186_response_1.json"
CI = RESP / "arax-ci" / "MONDO_0007186_response_run_1.json"


def load(path):
    with open(path) as f:
        return json.load(f)


def sn(result):
    b = result["node_bindings"]["SN"]
    return b["ids"][0] if isinstance(b, dict) else b[0]["id"]


def ids(binding):
    return binding["ids"] if isinstance(binding, dict) else [x["id"] for x in binding]


def tie_block(results):
    """(start, end) of the longest run of exact 0.001 score steps."""
    s = [r["analyses"][0]["score"] for r in results]
    best, start = (0, 0), None
    for i in range(1, len(s) + 1):
        if i < len(s) and round(s[i - 1] - s[i], 3) == 0.001:
            start = i - 1 if start is None else start
        else:
            if start is not None and i - start > best[1] - best[0]:
                best = (start, i)
            start = None
    return best


def to_ranker_input(envelope):
    """Undo the ResultTransformer well enough to re-run the ranker.

    - Put the N1 (NGD) virtual qedge back in the QG and bind each result's NGD
      edge(s) from its analysis support graphs.
    - Bind the Retriever edges behind each ARAX-prediction treats edge on
      t_edge as well; they carried qedge key t_edge when the ranker ran.
    - CI (TRAPI 1.6) has agent_type as an attribute; lift it to the top-level
      field the port reads.
    """
    env = copy.deepcopy(envelope)
    m = env["message"]
    m["query_graph"]["edges"]["N1"] = {"subject": "ON", "object": "SN"}
    kg = m["knowledge_graph"]["edges"]
    for e in kg.values():
        for a in e.get("attributes") or []:
            if a["attribute_type_id"] == "biolink:agent_type":
                e["agent_type"] = a["value"]
    for r in m["results"]:
        an = r["analyses"][0]
        t = ids(an["edge_bindings"]["t_edge"])
        for e in list(t):
            if e.startswith("ARAX-prediction-edge-"):
                for a in kg[e]["attributes"]:
                    if a["attribute_type_id"] == "biolink:support_graphs":
                        for sg in a["value"]:
                            t += m["auxiliary_graphs"][sg]["edges"]
        eb = {"t_edge": {"ids": t}}
        n1 = [e for sg in an.get("support_graphs") or [] for e in m["auxiliary_graphs"][sg]["edges"]]
        if n1:
            eb["N1"] = {"ids": n1}
        an["edge_bindings"] = eb
    return env


def tie_preserving_scores(scores):
    """Replacement for _break_ties_and_preserve_order: equal raw scores get
    equal output scores; everything else is rounded and clamped as before."""
    n = min(len(scores), 1000)
    out = [round(max(min(s, 1), 0), 3) if i < n else 0 for i, s in enumerate(scores)]
    return out


def rank(envelope, order=None, fixed=False):
    env = copy.deepcopy(envelope)
    results = env["message"]["results"]
    if order is not None:
        by_sn = {sn(r): r for r in results}
        env["message"]["results"] = [by_sn[k] for k in order]
    original = R._break_ties_and_preserve_order
    if fixed:
        R._break_ties_and_preserve_order = tie_preserving_scores
    try:
        R.arax_rank(env, logging.getLogger())
        if fixed:
            # deterministic display order: score, then CURIE
            env["message"]["results"].sort(key=lambda r: (-r["analyses"][0]["score"], sn(r)))
    finally:
        R._break_ties_and_preserve_order = original
    return {sn(r): r["analyses"][0]["score"] for r in env["message"]["results"]}


def raw_scores(envelope):
    """Each result's mean quantile rank before rounding and tie-breaking."""
    m = envelope["message"]
    rk = R.ARAXRanker()
    rk.aggregate_scores_dmk(copy.deepcopy(envelope), logging.getLogger())
    qg = R._get_query_graph_networkx_from_query_graph(m["query_graph"])
    ranks = [
        R._quantile_rank_list(R._score_result_graphs_by_networkx_graph_scorer(rk.edge_confidences, qg, m["results"], f))
        for f in (R._score_networkx_graphs_by_max_flow,
                  R._score_networkx_graphs_by_longest_path,
                  R._score_networkx_graphs_by_frobenius_norm)
    ]
    return {sn(r): float(s) for r, s in zip(m["results"], sum(ranks) / len(ranks))}


def set_order(sn_ids_in_kg_order, seed):
    """Iteration order of a set built from these CURIEs under a hash seed."""
    code = (
        "import json,sys\n"
        "s=set()\n"
        "for x in json.load(sys.stdin): s.add(x)\n"
        "print(json.dumps(list(s)))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        input=json.dumps(sn_ids_in_kg_order),
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONHASHSEED=str(seed)),
        check=True,
    )
    return json.loads(out.stdout)


def main():
    dev, ci = load(DEV), load(CI)
    dres, cres = dev["message"]["results"], ci["message"]["results"]
    dscore = {sn(r): r["analyses"][0]["score"] for r in dres}
    cscore = {sn(r): r["analyses"][0]["score"] for r in cres}

    print("== 1. Published scores")
    changed = [k for k in dscore if dscore[k] != cscore[k]]
    db, cb = tie_block(dres), tie_block(cres)
    dblock = [sn(r) for r in dres[db[0]:db[1]]]
    cblock = [sn(r) for r in cres[cb[0]:cb[1]]]
    print(f"results with different scores: {len(changed)}")
    print(f"Dev tie block: ranks {db[0] + 1}-{db[1]} ({len(dblock)} results), "
          f"{dres[db[0]]['analyses'][0]['score']} down to {dres[db[1] - 1]['analyses'][0]['score']} in 0.001 steps")
    print(f"CI  tie block: ranks {cb[0] + 1}-{cb[1]} ({len(cblock)} results)")
    print(f"same members: {set(dblock) == set(cblock)}; same order: {dblock == cblock}")
    print(f"changed results inside the tie block: {len(set(changed) & set(dblock))}; outside: "
          f"{sorted((dres.index(next(r for r in dres if sn(r) == k)) + 1, k) for k in changed if k not in dblock)}")

    print("\n== 2. Does set order under the pinned hash seed reproduce Dev's order?")
    sns = {sn(r) for r in dres}
    kg_order = [n for n in dev["message"]["knowledge_graph"]["nodes"] if n in sns]
    in_block = set(dblock)
    for seed in (0, 1, 2, 3):
        o = [x for x in set_order(kg_order, seed) if x in in_block]
        print(f"PYTHONHASHSEED={seed}: matches Dev block order: {o == dblock}; matches CI: {o == cblock}")

    print("\n== 3. Re-rank a rebuilt ranker input in different orders")
    dinp, cinp = to_ranker_input(dev), to_ranker_input(ci)
    dev_order = [sn(r) for r in dinp["message"]["results"]]
    raw = raw_scores(dinp)
    raw_counts = collections.Counter(raw.values())
    print(f"raw-score tie groups in the rebuilt input: {sorted((n for n in raw_counts.values() if n > 1), reverse=True)}")
    rng = random.Random(0)
    for fixed in (False, True):
        label = "tie-preserving fix" if fixed else "current ranker"
        a = rank(dinp, fixed=fixed)
        # CI's data, presented in Dev's order and in CI's own order
        b = rank(cinp, order=dev_order, fixed=fixed)
        c = rank(cinp, fixed=fixed)
        shuf = []
        for _ in range(5):
            o = dev_order[:]
            rng.shuffle(o)
            s = rank(dinp, order=o, fixed=fixed)
            shuf.append(sum(s[k] != a[k] for k in a))
        print(f"[{label}]")
        print(f"  Dev data vs CI data, same result order:   {sum(a[k] != b[k] for k in a)} scores differ")
        print(f"  Dev data vs CI data, each in its own order: {sum(a[k] != c[k] for k in a)} scores differ")
        print(f"  Dev data, 5 random result orders:         {shuf} scores differ")
        changed = {k for i in range(3)
                   for s in [rank(dinp, order=random.Random(i).sample(dev_order, len(dev_order)), fixed=fixed)]
                   for k in a if s[k] != a[k]}
        print(f"  results that changed in some order but have a unique raw score: "
              f"{sum(raw_counts[raw[k]] == 1 for k in changed)} (of {len(changed)} changed)")

if __name__ == "__main__":
    main()
