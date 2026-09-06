"""Side-by-side differential parity run: Shepherd's ARS vs. a deployed ARS.

Submits the SAME query to two ARS instances (near-simultaneously), polls
both to completion, then compares three layers:

  1. state tree     -- per-ARA terminal status/code/result_count multiset,
                       parent status, merged-version presence
                       (tests/parity_e2e/normalize.summarize_tree)
  2. child payloads -- each ARA's stored (pre-merge-processed) response,
                       matched across stacks by canonical agent name. These
                       are the MERGE INPUTS: if they differ, the two ARSes
                       were merging different data and downstream diffs are
                       attributable to the ARAs, not the port.
  3. merged answer  -- the merged_version TRAPI message, deep-diffed after
                       masking volatile identifiers (pks, timestamps,
                       hostnames) with order-insensitive list fallback.

Determinism notes: given identical inputs the merge/premerge pipeline is
deterministic, but a live run has three legitimate noise sources the report
calls out instead of hiding:
  - the ARAs answer each stack's query independently (input drift);
  - node annotations differ unless both stacks use the same annotator
    source (production ARS annotates in-process via biothings_annotator;
    --ignore-annotations strips biothings_annotations attributes to see
    past this);
  - log entries carry wall-clock text (stripped by default; --include-logs
    keeps them, timestamp-masked).

Usage:
  python scripts/ars_compare.py                        # ars-local vs ars-ci
  python scripts/ars_compare.py --left ars-local --right ars-ci \
      --curies MONDO:0005148,MONDO:0004979 --ignore-annotations
"""

import argparse
import asyncio
import importlib.util
import json
import re
import sys
import time
from pathlib import Path

import httpx

SCRIPTS_DIR = Path(__file__).resolve().parent
REPO = SCRIPTS_DIR.parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# Reuse the submit/poll target map + query builder + curie sweep from the
# single-ARS driver, and the masking/diff rules from the layer-4 harness.
test_ars = _load("test_ars", SCRIPTS_DIR / "test_ars.py")
normalize_mod = _load("ars_normalize", REPO / "tests/parity_e2e/normalize.py")

OUT_DIR = "ars_compare"
REPORT_FILE = "ars_compare_report.json"
MAX_DIFFS_SHOWN = 10


def canonical_agent(name: str) -> str:
    """Match actors across stacks: 'ara-shepherd-aragorn', 'ara-aragorn',
    'infores:shepherd-aragorn' and 'infores:aragorn' all become 'aragorn'."""
    if not name:
        return str(name)
    out = name.lower()
    out = re.sub(r"^infores:", "", out)
    out = re.sub(r"^(ara|kp)-", "", out)
    out = re.sub(r"^shepherd-", "", out)
    return out


def strip_for_comparison(payload, ignore_annotations: bool, include_logs: bool):
    """Remove content that legitimately differs between live stacks."""
    if not isinstance(payload, dict):
        return payload
    out = json.loads(json.dumps(payload))
    if not include_logs:
        out.pop("logs", None)
    if ignore_annotations:
        nodes = (out.get("message") or {}).get("knowledge_graph") or {}
        for node in (nodes.get("nodes") or {}).values():
            attrs = node.get("attributes")
            if isinstance(attrs, list):
                node["attributes"] = [
                    a
                    for a in attrs
                    if not (
                        isinstance(a, dict)
                        and a.get("attribute_type_id") == "biothings_annotations"
                    )
                ]
    return out


async def run_side(client: httpx.AsyncClient, target: str, query: dict) -> dict:
    """Submit + poll + fetch on one ARS. Returns trace, merged payload, and
    each ARA child's stored payload keyed by canonical agent."""
    base = test_ars.target_urls[target]
    side = {
        "target": target,
        "parent_pk": None,
        "trace": None,
        "merged": None,
        "children": {},
        "error": None,
    }
    start = time.perf_counter()
    try:
        r = await client.post(f"{base}/api/submit", json=query)
        r.raise_for_status()
        parent_pk = r.json().get("pk")
        side["parent_pk"] = parent_pk
        print(f"  [{target}] submitted, parent {parent_pk}")

        deadline = start + test_ars.COMPLETION_TIMEOUT_SECONDS
        while True:
            await asyncio.sleep(test_ars.POLL_INTERVAL_SECONDS)
            tr = await client.get(f"{base}/api/messages/{parent_pk}?trace=y")
            tr.raise_for_status()
            trace = tr.json()
            if trace.get("status") in test_ars.TERMINAL_STATUSES:
                break
            if time.perf_counter() > deadline:
                raise TimeoutError(f"parent {parent_pk} never left Running")
        side["trace"] = trace
        print(
            f"  [{target}] {trace.get('status')} after "
            f"{time.perf_counter() - start:.0f}s"
        )

        # each ARA child's stored payload (the merge inputs)
        for child in trace.get("children") or []:
            actor = child.get("actor") or {}
            agent = canonical_agent(actor.get("agent") or actor.get("inforesid"))
            if agent in ("ars-ars-agent", "ars-agent", "ars"):
                continue
            cr = await client.get(f"{base}/api/messages/{child.get('message')}")
            if cr.status_code == 200:
                side["children"][agent] = (cr.json().get("fields") or {}).get("data")

        merged_pk = (side["trace"] or {}).get("merged_version")
        if merged_pk not in (None, "", "None"):
            mr = await client.get(f"{base}/api/messages/{merged_pk}")
            mr.raise_for_status()
            side["merged"] = (mr.json().get("fields") or {}).get("data")
    except Exception as e:
        side["error"] = f"{type(e).__name__}: {e}"
        print(f"  [{target}] ERROR: {side['error']}")
    return side


def compare_sides(left: dict, right: dict, args) -> dict:
    """The three-layer comparison; every layer reports diff paths."""
    report = {
        "left": left["target"],
        "right": right["target"],
        "errors": {left["target"]: left["error"], right["target"]: right["error"]},
        "state_tree": {},
        "children": {},
        "merged": {},
        "verdict": None,
    }

    # 1. state tree (agent names canonicalized before comparison)
    def canon_tree(trace):
        summary = normalize_mod.summarize_tree(trace or {})
        children = []
        for fact in summary["children"]:
            agent, *rest = json.loads(fact)
            children.append(json.dumps([canonical_agent(agent), *rest]))
        summary["children"] = sorted(children)
        return summary

    tree_l, tree_r = canon_tree(left["trace"]), canon_tree(right["trace"])
    tree_diffs = normalize_mod.diff(tree_l, tree_r)
    report["state_tree"] = {"equal": not tree_diffs, "diffs": tree_diffs}

    # 2. per-ARA merge inputs
    agents = sorted(set(left["children"]) | set(right["children"]))
    inputs_drifted = False
    for agent in agents:
        lc, rc = left["children"].get(agent), right["children"].get(agent)
        if lc is None or rc is None:
            report["children"][agent] = {
                "equal": False,
                "diffs": [f"$: only present on {'right' if lc is None else 'left'}"],
            }
            inputs_drifted = True
            continue
        lc = strip_for_comparison(lc, args.ignore_annotations, args.include_logs)
        rc = strip_for_comparison(rc, args.ignore_annotations, args.include_logs)
        diffs = normalize_mod.diff(lc, rc)
        report["children"][agent] = {"equal": not diffs, "diffs": diffs}
        inputs_drifted = inputs_drifted or bool(diffs)

    # 3. merged answer
    lm = strip_for_comparison(
        left["merged"], args.ignore_annotations, args.include_logs
    )
    rm = strip_for_comparison(
        right["merged"], args.ignore_annotations, args.include_logs
    )
    if left["merged"] is None or right["merged"] is None:
        merged_diffs = [
            f"$: merged payload missing on "
            f"{'both' if lm is None and rm is None else ('left' if lm is None else 'right')}"
        ]
    else:
        merged_diffs = normalize_mod.diff(lm, rm)
    report["merged"] = {"equal": not merged_diffs, "diffs": merged_diffs}

    if left["error"] or right["error"]:
        report["verdict"] = "ERROR"
    elif report["merged"]["equal"] and report["state_tree"]["equal"]:
        report["verdict"] = "MATCH"
    elif inputs_drifted:
        # the two stacks merged different ARA data: not an ARS parity signal
        report["verdict"] = "INPUT_DRIFT"
    else:
        report["verdict"] = "ARS_DIVERGENCE"
    return report


def print_report(curie: str, report: dict) -> None:
    print(f"\n{'=' * 70}\n{curie}: {report['verdict']}")
    print(f"  state tree : {'equal' if report['state_tree']['equal'] else 'DIFFERS'}")
    for d in report["state_tree"]["diffs"][:MAX_DIFFS_SHOWN]:
        print(f"      {d}")
    for agent, entry in report["children"].items():
        status = "equal" if entry["equal"] else f"DIFFERS ({len(entry['diffs'])})"
        print(f"  input {agent:<12}: {status}")
        for d in entry["diffs"][:3]:
            print(f"      {d}")
    merged = report["merged"]
    status = "equal" if merged["equal"] else f"DIFFERS ({len(merged['diffs'])})"
    print(f"  merged     : {status}")
    for d in merged["diffs"][:MAX_DIFFS_SHOWN]:
        print(f"      {d}")
    if not merged["equal"]:
        annot_only = all("biothings_annotations" in d for d in merged["diffs"])
        if annot_only:
            print(
                "      (all merged diffs are node annotations -- rerun with "
                "--ignore-annotations to compare past annotator-source skew)"
            )


async def compare_curie(curie: str, args) -> dict:
    query = test_ars.generate_query(curie)
    print(f"\n>>> {curie}: {args.left} vs {args.right}")
    timeout = httpx.Timeout(test_ars.REQUEST_TIMEOUT_SECONDS)
    async with httpx.AsyncClient(timeout=timeout) as client:
        left, right = await asyncio.gather(
            run_side(client, args.left, query),
            run_side(client, args.right, query),
        )

    out_dir = Path(OUT_DIR) / curie.replace(":", "_")
    out_dir.mkdir(parents=True, exist_ok=True)
    for side in (left, right):
        prefix = side["target"]
        (out_dir / f"{prefix}_trace.json").write_text(
            json.dumps(side["trace"], indent=2, default=str)
        )
        (out_dir / f"{prefix}_merged.json").write_text(
            json.dumps(side["merged"], indent=2, default=str)
        )
        for agent, payload in side["children"].items():
            (out_dir / f"{prefix}_input_{agent}.json").write_text(
                json.dumps(payload, indent=2, default=str)
            )

    report = compare_sides(left, right, args)
    (out_dir / "diff.json").write_text(json.dumps(report, indent=2, default=str))
    print_report(curie, report)
    return report


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--left", default="ars-local", choices=test_ars.target_urls)
    parser.add_argument("--right", default="ars-ci", choices=test_ars.target_urls)
    parser.add_argument(
        "--curies",
        default=None,
        help="comma-separated curie subset (default: the full test_ars sweep)",
    )
    parser.add_argument(
        "--ignore-annotations",
        action="store_true",
        help="strip biothings_annotations node attributes before diffing "
        "(the two stacks usually annotate from different sources)",
    )
    parser.add_argument(
        "--include-logs",
        action="store_true",
        help="compare the payloads' logs arrays too (timestamp-masked); "
        "off by default since ARA log text is rarely deterministic",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    curies = args.curies.split(",") if args.curies else test_ars.curie_list

    verdicts = {}
    start = time.time()
    for curie in curies:
        report = await compare_curie(curie.strip(), args)
        verdicts[curie.strip()] = report["verdict"]

    Path(REPORT_FILE).write_text(json.dumps(verdicts, indent=2, sort_keys=True))
    print(f"\n{'=' * 70}\nSweep finished in {time.time() - start:.0f}s")
    for verdict in ("MATCH", "INPUT_DRIFT", "ARS_DIVERGENCE", "ERROR"):
        hits = [c for c, v in verdicts.items() if v == verdict]
        if hits:
            print(f"  {verdict:<15} {len(hits):>3}: {', '.join(hits)}")
    print(f"Verdicts saved to {REPORT_FILE}; per-curie artifacts under {OUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
