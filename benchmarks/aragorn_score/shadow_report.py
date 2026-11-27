"""Summarise the aragorn.score worker's shadow-mode comparisons.

With ARAGORN_SCORER=shadow the worker scores each query with the Python
scorer (whose result is saved) and then the Rust scorer, and logs one line per
query: ``aragorn.score shadow {json}``. This reads those lines from log files
or stdin and reports how often the two agreed and how their times compare.

    docker logs aragorn_score 2>&1 | python shadow_report.py
    python shadow_report.py logs/*.log
    kubectl logs deploy/aragorn-score | python shadow_report.py --list-disagreements

Exits 1 if any query disagreed.
"""

import argparse
import fileinput
import json
import statistics
import sys

MARKER = "aragorn.score shadow "


def percentile(values, q):
    values = sorted(values)
    if not values:
        return float("nan")
    i = min(len(values) - 1, max(0, round(q * (len(values) - 1))))
    return values[i]


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("files", nargs="*", help="log files (default: stdin)")
    p.add_argument(
        "--list-disagreements",
        action="store_true",
        help="print every query that disagreed, not just the first 10",
    )
    args = p.parse_args(argv)

    rows, failures = [], []
    for line in fileinput.input(args.files or ["-"], errors="replace"):
        if MARKER not in line:
            continue
        payload = line.split(MARKER, 1)[1].strip()
        if payload.startswith("{"):
            try:
                rows.append(json.loads(payload))
            except json.JSONDecodeError:
                pass
        elif "the Rust scorer failed" in payload:
            failures.append(payload)

    if not rows and not failures:
        print("No shadow lines found (is ARAGORN_SCORER=shadow set on the worker?)")
        return 0

    agree = [r for r in rows if r.get("agree")]
    disagree = [r for r in rows if not r.get("agree")]
    print(f"Queries compared:      {len(rows)}")
    print(f"  agreed:              {len(agree)}")
    print(f"  disagreed:           {len(disagree)}")
    print(f"  Rust scorer errors:  {len(failures)}")
    if rows:
        print(f"Analyses compared:     {sum(r.get('analyses', 0) for r in rows)}")
        print(
            f"Largest rel. diff:     {max(r.get('max_rel_diff', 0) for r in rows):.2e}"
        )
        moved = sum(1 for r in rows if r.get("results_moved"))
        print(f"Queries where the ranked order differed (near-ties): {moved}")
        top = [r for r in rows if r.get("top10_of")]
        if top:
            full = sum(1 for r in top if r["top10_overlap"] == r["top10_of"])
            print(f"Queries with the same top 10: {full} of {len(top)}")

        print("\nTimes (ms)               median      p90      max")
        for key, label in (
            ("python_rank_ms", "Python Ranker.rank"),
            ("rust_score_ms", "Rust score"),
            ("python_total_ms", "Python, whole task*"),
            ("rust_total_ms", "Rust, whole pipeline"),
        ):
            v = [r[key] for r in rows if key in r]
            if v:
                print(
                    f"  {label:<22}{statistics.median(v):>8.1f} "
                    f"{percentile(v, 0.9):>8.1f} {max(v):>8.1f}"
                )
        s = [r["rank_speedup"] for r in rows if "rank_speedup" in r]
        if s:
            print(
                f"\nScoring speedup (Python rank / Rust score): median "
                f"{statistics.median(s):.1f}x, min {min(s):.1f}x, max {max(s):.1f}x"
            )
        print(
            "  * Python's whole task includes its Redis read and write; Rust's doesn't."
        )

    if disagree:
        print("\nDisagreements:")
        for r in disagree if args.list_disagreements else disagree[:10]:
            print(
                f"  {r.get('response_id')}: {r.get('beyond_tol')} score(s) beyond "
                f"tolerance, worst {r.get('worst')}, shape mismatches "
                f"{r.get('shape_mismatch')}"
            )
    if failures:
        print("\nRust scorer errors:")
        for f in failures[:10]:
            print(f"  {f}")
    return 1 if disagree or failures else 0


if __name__ == "__main__":
    sys.exit(main())
