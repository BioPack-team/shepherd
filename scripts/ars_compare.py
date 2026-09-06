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
  4. replay         -- input drift does NOT blind the parity test: for each
                       stack independently, the ported (golden-tested)
                       pipeline re-folds that stack's own captured inputs in
                       its recorded merge order (merged_versions_list) --
                       blocklist, scrub, confidence, the lot, minus the
                       external annotator -- and the result is diffed
                       against what that ARS actually stored. If the port
                       cannot reproduce the DEPLOYED ARS's answer from the
                       deployed ARS's own inputs, that is ARS divergence no
                       matter how much the ARAs drifted between stacks.

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
import ast
import asyncio
import copy
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

# The ported pipeline itself, for the replay layer. Needs the repo's deps
# (scipy et al.); replay is skipped with a warning if they're missing.
sys.path.insert(0, str(REPO))
try:
    from shepherd_utils.ars.blocklist import load_blocklist, remove_blocked
    from shepherd_utils.ars.merge import TranslatorMessage, mergeMessages
    from shepherd_utils.ars.premerge import appraise_confidence, scrub_null_attributes

    REPLAY_AVAILABLE = True
except ImportError as _replay_err:  # pragma: no cover
    REPLAY_AVAILABLE = False
    _REPLAY_IMPORT_ERROR = _replay_err

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


def merge_order(trace: dict) -> list | None:
    """Canonical agent names in the order the parent actually merged them,
    from the trace's merged_versions_list ([pk, agent] pairs)."""
    mvl = (trace or {}).get("merged_versions_list")
    if isinstance(mvl, str):
        try:
            mvl = ast.literal_eval(mvl)
        except (ValueError, SyntaxError):
            return None
    if not isinstance(mvl, list):
        return None
    order = []
    for entry in mvl:
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            order.append(canonical_agent(entry[1]))
        else:
            return None
    return order


def replay_side(side: dict) -> tuple:
    """Re-run the ported pipeline over this stack's own captured inputs, in
    its own recorded merge order: fold -> blocklist -> scrub -> confidence
    per step, exactly like merge_received + post_process minus the external
    annotator. Returns (replayed_payload, error)."""
    if not REPLAY_AVAILABLE:
        return None, f"pipeline import failed: {_REPLAY_IMPORT_ERROR}"
    order = merge_order(side.get("trace"))
    if not order:
        return None, "no merged_versions_list on the trace"
    current = None
    try:
        for agent in order:
            child = side["children"].get(agent)
            if not isinstance(child, dict) or "message" not in child:
                return None, f"no captured input payload for {agent}"
            newcomer = TranslatorMessage(copy.deepcopy(child["message"]))
            if current is None:
                # first merge: the newcomer IS the merged message
                merged_dict = newcomer.to_dict()
            else:
                t_current = TranslatorMessage(copy.deepcopy(current["message"]))
                merged_dict = mergeMessages([t_current, newcomer], "replay").to_dict()
            remove_blocked(merged_dict, load_blocklist(), "replay")
            scrub_null_attributes(merged_dict)
            results = (merged_dict.get("message") or {}).get("results")
            if results is not None and len(results) > 0:
                appraise_confidence(results)
            current = merged_dict
    except Exception as e:
        return None, f"replay raised {type(e).__name__}: {e}"
    return current, None


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

    # 4. replay: each stack's merged output vs the ported pipeline re-run on
    # that stack's OWN inputs -- the drift-immune parity check. Annotations
    # and logs are always stripped here (replay never calls the annotator).
    report["replay"] = {}
    replays = {}
    if not args.no_replay:
        for name, side in (("left", left), ("right", right)):
            entry = {"equal": None, "diffs": [], "error": None}
            if side["merged"] is None:
                entry["error"] = "no merged payload to compare against"
            else:
                replayed, err = replay_side(side)
                replays[name] = replayed
                if err:
                    entry["error"] = err
                else:
                    actual = strip_for_comparison(side["merged"], True, False)
                    expected = strip_for_comparison(replayed, True, False)
                    diffs = normalize_mod.diff(actual, expected)
                    entry["equal"] = not diffs
                    entry["diffs"] = diffs
            report["replay"][name] = entry

    replay_l = report["replay"].get("left", {})
    replay_r = report["replay"].get("right", {})
    if left["error"] or right["error"]:
        report["verdict"] = "ERROR"
    elif report["merged"]["equal"] and report["state_tree"]["equal"]:
        report["verdict"] = "MATCH"
    elif replay_r.get("equal") is False:
        # the ported pipeline cannot reproduce the deployed ARS's answer
        # from the deployed ARS's own inputs: divergence, drift or not
        report["verdict"] = "ARS_DIVERGENCE"
    elif replay_l.get("equal") is False:
        # our own stack's answer isn't reproduced by our own pipeline code:
        # capture problem or nondeterminism in the port -- investigate
        report["verdict"] = "LOCAL_REPLAY_MISMATCH"
    elif replay_l.get("equal") and replay_r.get("equal"):
        # both stacks' answers are exactly what the ported pipeline produces
        # from their own inputs; the direct diff is input/arrival-order
        # drift, not ARS behavior
        report["verdict"] = "PIPELINE_PARITY"
    elif inputs_drifted:
        # replay unavailable and the stacks merged different ARA data
        report["verdict"] = "INPUT_DRIFT"
    else:
        report["verdict"] = "ARS_DIVERGENCE"
    report["_replays"] = replays  # stripped before saving; used for artifacts
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
    for name, entry in (report.get("replay") or {}).items():
        if entry.get("error"):
            status = f"skipped ({entry['error']})"
        elif entry.get("equal"):
            status = "reproduced"
        else:
            status = f"NOT REPRODUCED ({len(entry['diffs'])})"
        print(f"  replay {name:<5}: {status}")
        for d in entry.get("diffs", [])[:MAX_DIFFS_SHOWN]:
            print(f"      {d}")


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
    replays = report.pop("_replays", {}) or {}
    for name, side in (("left", left), ("right", right)):
        replayed = replays.get(name)
        if replayed is not None:
            (out_dir / f"{side['target']}_replayed.json").write_text(
                json.dumps(replayed, indent=2, default=str)
            )
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
    parser.add_argument(
        "--no-replay",
        action="store_true",
        help="skip the replay layer (re-running the ported pipeline over "
        "each stack's own inputs, the drift-immune parity check)",
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
    for verdict in (
        "MATCH",
        "PIPELINE_PARITY",
        "INPUT_DRIFT",
        "LOCAL_REPLAY_MISMATCH",
        "ARS_DIVERGENCE",
        "ERROR",
    ):
        hits = [c for c, v in verdicts.items() if v == verdict]
        if hits:
            print(f"  {verdict:<15} {len(hits):>3}: {', '.join(hits)}")
    print(f"Verdicts saved to {REPORT_FILE}; per-curie artifacts under {OUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
