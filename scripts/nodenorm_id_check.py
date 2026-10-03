"""Check whether the node ids in ARAX responses are already NodeNorm-canonical.

ARAX calls NodeNorm at query time to canonicalize curies it already has: the
NGD overlay canonicalizes every KG node before looking it up in curie_to_pmids
(Overlay/compute_ngd.py), and upstream Expand canonicalized the pinned query
ids (dropped under DEC-9). Those calls change nothing if the ids are already
what NodeNorm would return, which is the claim this script tests: it collects
the knowledge-graph node ids (and the pinned query-graph ids) from TRAPI
responses, asks NodeNorm for each one's preferred_curie, and reports every id
that differs or that NodeNorm doesn't recognize.

By default NodeNorm is called the way ARAX's NodeSynonymizer calls it: POST
/get_normalized_nodes with no conflate flags, so the server's defaults apply.
--conflate / --drug-chemical-conflate try other settings, to see which ones
Retriever's ids agree with.

Sources are TRAPI response JSON files, directories of them, or URLs to GET.
scripts/test_arax_queries.py saves one per case under responses/arax-validation/,
so a typical run is:

    python scripts/test_arax_queries.py --only lookup_ mvp1 --base http://localhost:5439/arax
    python scripts/nodenorm_id_check.py responses/arax-validation/

    python scripts/nodenorm_id_check.py resp.json --nodenorm https://nodenorm.transltr.io/1.4
    python scripts/nodenorm_id_check.py responses/ --drug-chemical-conflate true
    python scripts/nodenorm_id_check.py responses/ --curie-to-pmids arax_dbs/curie_to_pmids.sqlite
    python scripts/nodenorm_id_check.py responses/ --out report.json --fail-on-mismatch

--curie-to-pmids shows what a mismatch would cost NGD if the NGD
canonicalization were dropped too: for each differing id, whether the raw id
and the canonical id have a row in that database.

Exit code: 0, or 1 with --fail-on-mismatch when any id differs from its
preferred_curie.
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import requests

# NodeSynonymizer.NODE_NORMALIZER_URL_BY_MATURITY["development"]
DEFAULT_NODENORM = "https://nodenorm-es.ci.transltr.io"
BATCH_SIZE = 2500  # NodeSynonymizer's batch size
TIMEOUT = 120


def iter_sources(sources):
    """Yield (label, parsed JSON) for each file, *.json in a directory, or URL."""
    for source in sources:
        if source.startswith(("http://", "https://")):
            resp = requests.get(source, timeout=TIMEOUT)
            resp.raise_for_status()
            yield source, resp.json()
            continue
        path = Path(source)
        files = sorted(path.glob("*.json")) if path.is_dir() else [path]
        for f in files:
            try:
                yield str(f), json.loads(f.read_text())
            except (json.JSONDecodeError, UnicodeDecodeError):
                # e.g. a saved NDJSON stream, or a non-JSON error body
                print(f"skipping {f}: not a JSON document", file=sys.stderr)


def find_message(doc):
    """The TRAPI message in a response (or in a {"response": ...} wrapper)."""
    if not isinstance(doc, dict):
        return None
    if isinstance(doc.get("message"), dict):
        return doc["message"]
    for key in ("response", "fields", "data"):
        if isinstance(doc.get(key), dict):
            found = find_message(doc[key])
            if found is not None:
                return found
    return None


def collect_ids(sources):
    """{curie: {"kg": set(labels), "qg": set(labels)}} over every source."""
    seen = defaultdict(lambda: {"kg": set(), "qg": set()})
    n_responses = 0
    for label, doc in iter_sources(sources):
        message = find_message(doc)
        if message is None:
            print(f"skipping {label}: no TRAPI message", file=sys.stderr)
            continue
        n_responses += 1
        kg_nodes = (message.get("knowledge_graph") or {}).get("nodes") or {}
        for curie in kg_nodes:
            seen[curie]["kg"].add(label)
        qg_nodes = (message.get("query_graph") or {}).get("nodes") or {}
        for qnode in qg_nodes.values():
            for curie in (qnode or {}).get("ids") or []:
                seen[curie]["qg"].add(label)
    return seen, n_responses


def normalize(curies, base_url, flags):
    """NodeNorm's answer for each curie (None when it doesn't recognize it)."""
    url = base_url.rstrip("/") + "/get_normalized_nodes"
    session = requests.Session()
    results = {}
    for i in range(0, len(curies), BATCH_SIZE):
        batch = curies[i : i + BATCH_SIZE]
        resp = session.post(url, json={"curies": batch, **flags}, timeout=TIMEOUT)
        resp.raise_for_status()
        results.update(resp.json())
        print(
            f"  NodeNorm: {min(i + BATCH_SIZE, len(curies))}/{len(curies)}",
            file=sys.stderr,
        )
    return results


def pmids_lookup(db_path, curies):
    """The subset of curies that have a row in curie_to_pmids."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    found = set()
    curies = list(curies)
    for i in range(0, len(curies), 500):
        chunk = curies[i : i + 500]
        rows = conn.execute(
            f"SELECT curie FROM curie_to_pmids WHERE curie IN ({','.join('?' * len(chunk))})",
            chunk,
        )
        found.update(row[0] for row in rows)
    conn.close()
    return found


def tri_state(value):
    return {"default": None, "true": True, "false": False}[value]


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "sources", nargs="+", help="response JSON files, directories, or URLs"
    )
    parser.add_argument(
        "--nodenorm",
        default=DEFAULT_NODENORM,
        help=f"NodeNorm base URL (default {DEFAULT_NODENORM})",
    )
    parser.add_argument(
        "--conflate",
        choices=["default", "true", "false"],
        default="default",
        help="GeneProtein conflation flag to send (default: none, as ARAX)",
    )
    parser.add_argument(
        "--drug-chemical-conflate",
        choices=["default", "true", "false"],
        default="default",
        help="DrugChemical conflation flag to send (default: none, as ARAX)",
    )
    parser.add_argument(
        "--curie-to-pmids",
        help="ARAX's curie_to_pmids sqlite, to show the NGD impact of each mismatch",
    )
    parser.add_argument("--out", help="write the full report here as JSON")
    parser.add_argument(
        "--show",
        type=int,
        default=50,
        help="mismatches to print (default 50; 0 for all)",
    )
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="exit 1 if any id differs from its preferred_curie",
    )
    args = parser.parse_args()

    flags = {}
    if tri_state(args.conflate) is not None:
        flags["conflate"] = tri_state(args.conflate)
    if tri_state(args.drug_chemical_conflate) is not None:
        flags["drug_chemical_conflate"] = tri_state(args.drug_chemical_conflate)

    seen, n_responses = collect_ids(args.sources)
    if not seen:
        sys.exit("no node ids found in the given sources")
    curies = sorted(seen)
    print(
        f"{len(curies)} unique ids from {n_responses} responses; asking {args.nodenorm} {flags or '(server defaults)'}",
        file=sys.stderr,
    )
    normalized = normalize(curies, args.nodenorm, flags)

    canonical, mismatched, unrecognized = [], [], []
    for curie in curies:
        info = normalized.get(curie)
        preferred = ((info or {}).get("id") or {}).get("identifier")
        row = {
            "curie": curie,
            "preferred_curie": preferred,
            "in_kg": bool(seen[curie]["kg"]),
            "pinned_in_qg": bool(seen[curie]["qg"]),
            "responses": sorted(seen[curie]["kg"] | seen[curie]["qg"]),
        }
        if preferred is None:
            unrecognized.append(row)
        elif preferred == curie:
            canonical.append(row)
        else:
            mismatched.append(row)

    if args.curie_to_pmids:
        in_db = pmids_lookup(
            args.curie_to_pmids,
            {r["curie"] for r in mismatched}
            | {r["preferred_curie"] for r in mismatched},
        )
        for row in mismatched:
            row["raw_in_curie_to_pmids"] = row["curie"] in in_db
            row["preferred_in_curie_to_pmids"] = row["preferred_curie"] in in_db

    def counts(rows):
        return (
            f"{len(rows)} "
            f"({sum(r['in_kg'] for r in rows)} KG, {sum(r['pinned_in_qg'] for r in rows)} pinned)"
        )

    print()
    print(f"ids checked:                {len(curies)}")
    print(f"  already canonical:        {counts(canonical)}")
    print(f"  differ from NodeNorm:     {counts(mismatched)}")
    print(f"  not recognized:           {counts(unrecognized)}")

    if mismatched:
        by_prefix = defaultdict(int)
        for row in mismatched:
            by_prefix[
                (row["curie"].split(":")[0], row["preferred_curie"].split(":")[0])
            ] += 1
        print("\nmismatches by prefix (id -> preferred_curie):")
        for (src, dst), n in sorted(by_prefix.items(), key=lambda kv: -kv[1]):
            print(f"  {src:>16} -> {dst:<16} {n}")

        shown = mismatched if args.show == 0 else mismatched[: args.show]
        print(
            f"\nmismatches{'' if len(shown) == len(mismatched) else f' (first {len(shown)}; --show 0 for all)'}:"
        )
        for row in shown:
            where = "pinned" if row["pinned_in_qg"] else "kg"
            line = f"  [{where:>6}] {row['curie']} -> {row['preferred_curie']}"
            if args.curie_to_pmids:
                line += (
                    f"   curie_to_pmids: raw={'yes' if row['raw_in_curie_to_pmids'] else 'no'}"
                    f" preferred={'yes' if row['preferred_in_curie_to_pmids'] else 'no'}"
                )
            print(line)
        if args.curie_to_pmids:
            lost = sum(
                1
                for r in mismatched
                if r["preferred_in_curie_to_pmids"] and not r["raw_in_curie_to_pmids"]
            )
            print(
                f"\nNGD: {lost} mismatched id(s) have PMIDs only under the preferred curie "
                "(their NGD would be lost without canonicalization)"
            )

    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {
                    "nodenorm": args.nodenorm,
                    "flags": flags,
                    "n_responses": n_responses,
                    "n_ids": len(curies),
                    "mismatched": mismatched,
                    "unrecognized": unrecognized,
                    "n_canonical": len(canonical),
                },
                indent=2,
            )
        )
        print(f"\nreport written to {args.out}")

    if args.fail_on_mismatch and mismatched:
        sys.exit(1)


if __name__ == "__main__":
    main()
