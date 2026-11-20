"""Compare two aragorn.score benchmark results (python_scorer.py and the Rust
runner): timings side by side, then how far apart their scores and rankings
are.

    python compare.py out/python.json out/rust.json [--tol 1e-9] [--top 10]

Exits 1 if any analysis score differs by more than ``--tol`` (relative, with
the same value as an absolute floor near zero), if the two failed different
answers, or if the messages had a different number of results; 0 otherwise.
Ranking differences alone don't fail it: answers whose scores tie to within
rounding can legitimately swap places.
"""

import argparse
import math
import sys
from pathlib import Path

import orjson

PHASES = ["read", "decompress", "parse", "score", "serialize", "compress", "total"]


def load(path):
    return orjson.loads(Path(path).read_bytes())


def fmt_ms(v):
    return f"{v:10.1f}" if v >= 10 else f"{v:10.3f}"


def timing_table(a, b):
    na, nb = a["impl"], b["impl"]
    print(f"Timings (ms; min / median of {a['repeat']} vs {b['repeat']} runs)")
    print(
        f"  {'phase':<11}{na + ' min':>11}{na + ' med':>11}{nb + ' min':>11}{nb + ' med':>11}{'speedup':>10}"
    )
    for phase in PHASES:
        ta, tb = a["timings_ms"].get(phase), b["timings_ms"].get(phase)
        if not ta or not tb:
            continue
        speed = ta["min"] / tb["min"] if tb["min"] > 0 else math.inf
        print(
            f"  {phase:<11}{fmt_ms(ta['min'])} {fmt_ms(ta['median'])} "
            f"{fmt_ms(tb['min'])} {fmt_ms(tb['median'])} {speed:8.1f}x"
        )
    print(
        f"  message: {a['message_bytes'] / 1e6:.1f} MB JSON in, "
        f"{a['n_results']} results, {a['n_analyses']} analyses"
    )
    print()


def as_float(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def score_agreement(a, b, tol, top):
    na, nb = a["impl"], b["impl"]
    ok = True
    if a["n_results"] != b["n_results"]:
        print(f"!! different number of results: {a['n_results']} vs {b['n_results']}")
        return False

    total = exact = within = mismatched_shape = 0
    worst = []  # (rel diff, abs diff, result, analysis, va, vb)
    max_abs = max_rel = 0.0
    special = {na: {"-1": 0, "0": 0}, nb: {"-1": 0, "0": 0}}
    for ri, (sa, sb) in enumerate(zip(a["scores"], b["scores"])):
        if len(sa) != len(sb):
            mismatched_shape += 1
            continue
        for ai, (va, vb) in enumerate(zip(sa, sb)):
            total += 1
            for name, v in ((na, va), (nb, vb)):
                if v == -1:
                    special[name]["-1"] += 1
                elif v == 0:
                    special[name]["0"] += 1
            fa, fb = as_float(va), as_float(vb)
            if fa is None or fb is None:
                if va == vb:
                    exact += 1
                    within += 1
                else:
                    worst.append((math.inf, math.inf, ri, ai, va, vb))
                continue
            if fa == fb:
                exact += 1
                within += 1
                continue
            diff = abs(fa - fb)
            rel = diff / max(abs(fa), abs(fb), tol)
            max_abs, max_rel = max(max_abs, diff), max(max_rel, rel)
            if rel <= tol:
                within += 1
            worst.append((rel, diff, ri, ai, va, vb))

    print("Scores")
    print(f"  analyses compared:     {total}")
    print(f"  bit-identical:         {exact} ({100 * exact / max(total, 1):.2f}%)")
    print(f"  within tol={tol:g}:     {within} ({100 * within / max(total, 1):.2f}%)")
    print(f"  max abs / rel diff:    {max_abs:.3e} / {max_rel:.3e}")
    print(
        f"  scored -1 (NaN guard): {na} {special[na]['-1']}, {nb} {special[nb]['-1']}"
    )
    print(f"  scored 0:              {na} {special[na]['0']}, {nb} {special[nb]['0']}")
    if mismatched_shape:
        ok = False
        print(f"!! {mismatched_shape} result(s) have a different number of analyses")
    bad = [w for w in worst if w[0] > tol]
    if bad:
        ok = False
        print(
            f"!! {len(bad)} analysis score(s) differ by more than tol; worst {min(top, len(bad))}:"
        )
        for rel, diff, ri, ai, va, vb in sorted(bad, key=lambda w: -w[0])[:top]:
            print(
                f"     result {ri} analysis {ai}: {na}={va!r} {nb}={vb!r} (rel {rel:.3e})"
            )
    print()

    fa = {i: e for i, e in a.get("failures", [])}
    fb = {i: e for i, e in b.get("failures", [])}
    print("Answers that failed to score (scored 0)")
    print(f"  {na}: {a.get('failed_answers', 0)}   {nb}: {b.get('failed_answers', 0)}")
    if set(fa) != set(fb) or a.get("failed_answers") != b.get("failed_answers"):
        ok = False
        print("!! the two failed different answers")
    for i in sorted(set(fa) | set(fb), key=lambda x: (x is None, x))[:top]:
        print(f"    result {i}:")
        print(f"      {na}: {fa.get(i, '-')}")
        print(f"      {nb}: {fb.get(i, '-')}")
    print()
    return ok


def best_scores(result):
    """Each result's best analysis score, as the worker sorts by it."""
    out = []
    for s in result["scores"]:
        vals = [as_float(v) for v in s]
        vals = [v for v in vals if v is not None]
        out.append(max(vals) if vals else 0.0)
    return out


def ranking_agreement(a, b, tol, top):
    na, nb = a["impl"], b["impl"]
    oa, ob = a["order"], b["order"]
    print("Ranking (the worker sorts ascending: the best answers are last)")
    if not oa or len(oa) != len(ob):
        print(f"  nothing to compare ({len(oa)} vs {len(ob)} ranked results)\n")
        return
    n = len(oa)
    pa = {r: i for i, r in enumerate(oa)}
    pb = {r: i for i, r in enumerate(ob)}
    moved = [r for r in pa if pa[r] != pb[r]]
    print(f"  identical order:       {oa == ob}")
    print(f"  results in a different position: {len(moved)} of {n}")
    if moved:
        print(
            f"  largest move:          {max(abs(pa[r] - pb[r]) for r in moved)} places"
        )
    for k in (10, 50, 100, 500):
        if k <= n:
            overlap = len(set(oa[-k:]) & set(ob[-k:]))
            print(f"  top-{k:<4} overlap:      {overlap}/{k}")

    # Kendall tau over every pair, when scipy is there to do it in n log n.
    try:
        from scipy.stats import kendalltau

        tau = kendalltau([pa[r] for r in range(n)], [pb[r] for r in range(n)]).statistic
        print(f"  Kendall tau:           {tau:.6f}")
    except Exception:
        pass

    # Do the swaps come from near-ties? Adjacent pairs in A that B flips.
    best = best_scores(a)
    flips = tie_flips = 0
    for i in range(n - 1):
        x, y = oa[i], oa[i + 1]
        if pb[x] > pb[y]:
            flips += 1
            bx, by = best[x], best[y]
            if abs(bx - by) <= tol * max(abs(bx), abs(by), tol):
                tie_flips += 1
    print(
        f"  adjacent pairs flipped: {flips} ({tie_flips} of them near-ties within tol)"
    )
    print()


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("a", help="first result (e.g. out/python.json)")
    p.add_argument("b", help="second result (e.g. out/rust.json)")
    p.add_argument(
        "--tol", type=float, default=1e-9, help="relative tolerance on scores"
    )
    p.add_argument("--top", type=int, default=10, help="how many examples to print")
    args = p.parse_args(argv)

    a, b = load(args.a), load(args.b)
    print(
        f"Comparing {a['impl']} ({args.a}) with {b['impl']} ({args.b}); input {a['input']}\n"
    )
    timing_table(a, b)
    ok = score_agreement(a, b, args.tol, args.top)
    ranking_agreement(a, b, args.tol, args.top)
    print(
        "RESULT: "
        + (
            "scores agree within tolerance"
            if ok
            else "DIFFERENCES FOUND (see !! above)"
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
