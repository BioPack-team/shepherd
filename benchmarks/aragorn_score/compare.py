"""Compare aragorn.score benchmark results: a baseline (normally
python_scorer.py's) against one or more others (the Rust runner's variants):
timings side by side, then, for each other result, how far its scores,
rankings and (when both runs saved it) scored message are from the
baseline's.

    python compare.py out/python.json out/rust-typed-cholesky.json [more.json ...]
                      [--tol 1e-6] [--top 10]

Exits 1 if any analysis score differs by more than ``--tol`` (relative, with
the same value as an absolute floor near zero), if two runs failed different
answers, if the result counts differ, or if a saved message differs beyond
its scores; 0 otherwise. Ranking differences alone don't fail it: answers
whose scores tie to within rounding can legitimately swap places.
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


def timing_table(runs):
    base = runs[0]
    width = max(14, *(len(r["impl"]) + 2 for r in runs))
    print(f"Timings (ms, min of {base['repeat']} runs; speedup vs {base['impl']})")
    print(f"  {'phase':<11}" + "".join(f"{r['impl']:>{width}}" for r in runs))
    for phase in PHASES:
        cells = []
        for r in runs:
            t = r["timings_ms"].get(phase)
            if not t:
                cells.append(f"{'-':>{width}}")
                continue
            cell = fmt_ms(t["min"]).strip()
            tb = base["timings_ms"].get(phase)
            if r is not base and tb and t["min"] > 0:
                cell += f" ({tb['min'] / t['min']:.1f}x)"
            cells.append(f"{cell:>{width}}")
        print(f"  {phase:<11}" + "".join(cells))
    print(
        f"  message: {base['message_bytes'] / 1e6:.1f} MB JSON in, "
        f"{base['n_results']} results, {base['n_analyses']} analyses"
    )
    for r in runs:
        notes = []
        if r.get("solves"):
            sv = r["solves"]
            notes.append(
                f"solves: {sv['cholesky']} cholesky, {sv['svd']} svd "
                f"({sv['cholesky_fallbacks']} cholesky fallbacks)"
            )
        if r.get("note"):
            notes.append(f"NOTE: {r['note']}")
        if notes:
            print(f"  {r['impl']}: " + "; ".join(notes))
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


def load_message(path):
    data = Path(path).read_bytes()
    if data[:4] == b"\x28\xb5\x2f\xfd":
        import zstandard

        data = zstandard.ZstdDecompressor().decompressobj().decompress(data)
    doc = orjson.loads(data)
    return doc["message"] if isinstance(doc.get("message"), dict) else doc


def _strip_scores(result):
    r = dict(result)
    r["analyses"] = [
        {k: v for k, v in a.items() if k != "score"} if isinstance(a, dict) else a
        for a in (result.get("analyses") or [])
    ]
    return orjson.dumps(r, option=orjson.OPT_SORT_KEYS)


def _diff(a, b, path, tol, out, limit):
    if len(out) >= limit:
        return
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                out.append(f"{path}.{k}: only in {'first' if k in a else 'second'}")
            else:
                _diff(a[k], b[k], f"{path}.{k}", tol, out, limit)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append(f"{path}: list length {len(a)} vs {len(b)}")
            return
        for i, (x, y) in enumerate(zip(a, b)):
            _diff(x, y, f"{path}[{i}]", tol, out, limit)
    elif as_float(a) is not None and as_float(b) is not None:
        fa, fb = float(a), float(b)
        if fa != fb and abs(fa - fb) > tol * max(abs(fa), abs(fb), tol):
            out.append(f"{path}: {a!r} vs {b!r}")
    elif a != b:
        out.append(f"{path}: {str(a)[:60]!r} vs {str(b)[:60]!r}")


def message_agreement(a, b, tol, top):
    """Do the two saved scored messages say the same thing? Results are
    matched by their content without scores, since near-ties may be ordered
    differently."""
    pa, pb = a.get("saved_message"), b.get("saved_message")
    if not pa or not pb:
        return True
    print(f"Saved messages ({pa} vs {pb})")
    ma, mb = load_message(pa), load_message(pb)
    diffs = []
    ra, rb = ma.pop("results", None), mb.pop("results", None)
    _diff(ma, mb, "message", tol, diffs, top)
    if isinstance(ra, list) and isinstance(rb, list):
        if len(ra) != len(rb):
            diffs.append(f"message.results: {len(ra)} vs {len(rb)} results")
        else:
            by_key = {}
            for r in rb:
                by_key.setdefault(_strip_scores(r), []).append(r)
            for i, r in enumerate(ra):
                match = by_key.get(_strip_scores(r))
                if not match:
                    diffs.append(
                        f"message.results[{i}]: no result with the same content"
                    )
                    if len(diffs) >= top:
                        break
                    continue
                _diff(r, match.pop(), f"message.results[{i}]", tol, diffs, top)
    elif ra != rb:
        diffs.append("message.results: differ")
    if diffs:
        print(
            f"!! {len(diffs)}{'+' if len(diffs) >= top else ''} difference(s), first {min(top, len(diffs))}:"
        )
        for d in diffs[:top]:
            print(f"     {d}")
    else:
        print("  same content (scores within tol; results matched regardless of order)")
    print()
    return not diffs


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("base", help="baseline result (e.g. out/python.json)")
    p.add_argument("others", nargs="+", help="results to compare with it")
    p.add_argument(
        "--tol", type=float, default=1e-6, help="relative tolerance on scores"
    )
    p.add_argument("--top", type=int, default=10, help="how many examples to print")
    args = p.parse_args(argv)

    runs = [load(args.base)] + [load(o) for o in args.others]
    print(f"Input: {runs[0]['input']}\n")
    timing_table(runs)
    verdicts = []
    for other in runs[1:]:
        print(f"===== {runs[0]['impl']} vs {other['impl']} =====\n")
        ok = score_agreement(runs[0], other, args.tol, args.top)
        ranking_agreement(runs[0], other, args.tol, args.top)
        ok = message_agreement(runs[0], other, args.tol, args.top) and ok
        verdicts.append((other["impl"], ok))
    for name, ok in verdicts:
        print(
            f"RESULT {name}: "
            + ("agrees within tolerance" if ok else "DIFFERENCES FOUND (see !! above)")
        )
    return 0 if all(ok for _, ok in verdicts) else 1


if __name__ == "__main__":
    sys.exit(main())
