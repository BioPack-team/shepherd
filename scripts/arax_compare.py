"""Side-by-side parity run: Shepherd's ARAX port vs. a deployed upstream ARAX.

Sends the SAME TRAPI 2.0 query to both instances (near-simultaneously, one
case at a time per worker) and compares the answers layer by layer:

  1. outcome   -- HTTP status, ARAX's response status and error code
  2. results   -- the set of results, each keyed by its node bindings
                  (qnode -> sorted ids); for the results both sides found,
                  the rank order and the scores
  3. kg        -- KG node ids, and KG edges as (subject, predicate, object,
                  primary knowledge source)
  4. support   -- the number of auxiliary graphs, and their sizes
  5. logs      -- the WARNING / ERROR log codes (texts carry wall-clock and
                  host noise, so only codes are compared)

The cases are the TRAPI 2.0 query cases of scripts/test_arax_queries.py
(lookups, creative mode, ARAXi, workflows, validation errors). Cases that only
test Shepherd (its 400 for TRAPI 1.x spellings, the UI's X prefix), endpoint
cases and cases that reuse an earlier response are left out.

Both sides answer from live services, so a difference is not automatically a
port defect. The report calls out the legitimate noise sources rather than
hiding them:
  - Retriever: each instance queries its own Retriever. Point Shepherd's
    SYNC_KG_RETRIEVAL_URL at the Retriever upstream's dev instance uses, or
    differences in the KG show up as result differences.
  - Data files: the ARAX data build (ARAX_TIER_VERSION) and the pathfinder
    files should match upstream's (see docs/ARAX_PORT_BASELINE.md DEC-30).
  - KP cache: --bypass-cache (the default) sets parameters.bypass_cache on
    both sides, so neither answers from a cache the other doesn't have.
  - Time: KG content changes; run both sides together (this script does).
  - Recorded decisions: where the port deliberately differs (DEC-* in
    docs/ARAX_PORT_BASELINE.md, e.g. DEC-29 or a qnode `name`), a difference
    is expected. --expected lists them per case.

Usage:
  python scripts/arax_compare.py \\
      --port http://localhost:5439/arax \\
      --upstream https://arax.ncats.io/devED/api/arax/v2.0
  python scripts/arax_compare.py --only lookup_ mvp1 --workers 2
  python scripts/arax_compare.py --list

Every pair of responses is saved under --out (default
responses/arax-compare/<case>.{port,upstream}.json), with a per-case diff in
<case>.diff.json and a summary in summary.json and summary.md. The exit code is
1 if any case is DIFF (not SAME, SIMILAR or EXPECTED). A case whose
difference comes from a recorded decision or from how upstream's deployment
behaves is EXPECTED (listed in EXPECTED, with the reason).
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_arax_queries as Q  # noqa: E402

DEFAULT_UPSTREAM = "https://arax.ncats.io/devED/api/arax/v2.0"
DEFAULT_PORT = "http://localhost:5439/arax"
EXAMPLES = 5  # how many differing items to show per layer
ERROR_CHARS = 400  # how much of an error message to keep

# Differences that come from recorded decisions or from how upstream's
# deployment behaves, not from the port. A case listed here whose answers
# differ is EXPECTED (its diff is still saved in full).
EXPECTED = {
    "any_is_set": (
        "upstream's pydantic QNode silently drops the 1.x is_set (extra='ignore'), so "
        "upstream's own check never sees it; the port rejects it as upstream's code "
        "intends (DEC-24)"
    ),
    "val_unknown_qnode_property": (
        "upstream's pydantic models drop unknown qnode properties before its "
        "validation runs; the port rejects them (DEC-24/26)"
    ),
    "val_singular_predicate": (
        "upstream's pydantic QEdge drops the 1.x 'predicate' before its validation "
        "runs (so it answers with no predicate); the port rejects it (DEC-26)"
    ),
    "val_bad_kp_timeout": (
        "upstream's pydantic Query drops the legacy query_options, so its kp_timeout "
        "is never read; the port also reads query_options (sync item 8)"
    ),
    "query_by_name_trapi": "the port resolves a qnode's name to ids (D-25)",
    "val_no_message": (
        "Shepherd answers an empty query with ARAX's own error (400), upstream's "
        "FastAPI with its schema error (422)"
    ),
}

# Cases that are not comparable side by side: they reuse an earlier response
# of the same instance, or only exercise Shepherd
NOT_COMPARABLE = {"message_uris_reuse_a_response"}


def comparable_cases(only=None, groups=None) -> list:
    cases = []
    for c in Q.CASES:
        if c.method or c.after or c.name in NOT_COMPARABLE:
            continue  # endpoint cases, or cases that need an earlier response
        if (c.arax or {}).get("skip"):
            continue  # only tests Shepherd
        if groups and c.group not in groups:
            continue
        if only and not any(o in c.name for o in only):
            continue
        cases.append(c)
    return cases


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def _message(body) -> dict:
    return (body or {}).get("message") or {}


def result_key(result: dict) -> tuple:
    bindings = result.get("node_bindings") or {}
    return tuple(
        sorted(
            (qnode, tuple(sorted((b or {}).get("ids") or [])))
            for qnode, b in bindings.items()
        )
    )


def result_score(result: dict):
    for analysis in result.get("analyses") or []:
        if analysis.get("score") is not None:
            return analysis["score"]
    return None


def primary_source(edge: dict):
    for source in edge.get("sources") or []:
        if source.get("resource_role") == "primary_knowledge_source":
            return source.get("resource_id")
    return None


def summarize(http_status: int, body) -> dict:
    """The comparable layers of one response."""
    if not isinstance(body, dict):
        body = {"_not_json": str(body)[:500]}
    message = _message(body)
    kg = message.get("knowledge_graph") or {}
    results = message.get("results") or []
    aux = message.get("auxiliary_graphs") or {}
    logs = body.get("logs") or []
    return {
        "outcome": {
            "http": http_status,
            "status": body.get("status"),
            "error_code": next(
                (
                    entry.get("code")
                    for entry in logs
                    if entry.get("level") == "ERROR" and entry.get("code")
                ),
                None,
            ),
        },
        "results": [(result_key(r), result_score(r)) for r in results],
        "kg_nodes": sorted((kg.get("nodes") or {}).keys()),
        "kg_edges": sorted(
            {
                (
                    e.get("subject"),
                    e.get("predicate"),
                    e.get("object"),
                    primary_source(e),
                )
                for e in (kg.get("edges") or {}).values()
            },
            key=str,
        ),
        "edge_details": {
            (
                e.get("subject"),
                e.get("predicate"),
                e.get("object"),
                primary_source(e),
            ): (
                tuple(
                    sorted(
                        {
                            a.get("attribute_type_id")
                            for a in e.get("attributes") or []
                            if isinstance(a, dict)
                        },
                        key=str,
                    )
                ),
                e.get("knowledge_level"),
                e.get("agent_type"),
            )
            for e in (kg.get("edges") or {}).values()
        },
        "errors": [
            str(entry.get("message"))[:ERROR_CHARS]
            for entry in logs
            if entry.get("level") == "ERROR"
        ][:2]
        or (
            [str(body.get("description"))[:ERROR_CHARS]]
            if body.get("description")
            and body.get("status") not in (None, "Success", "OK")
            else []
        )
        or (
            [
                str(body.get("detail") or body.get("_error") or body.get("_not_json"))[
                    :ERROR_CHARS
                ]
            ]
            if (body.get("detail") or body.get("_error") or body.get("_not_json"))
            else []
        ),
        "aux_sizes": sorted(len((g or {}).get("edges") or []) for g in aux.values()),
        "log_codes": sorted(
            {
                (entry.get("level"), entry.get("code"))
                for entry in logs
                if entry.get("level") in ("WARNING", "ERROR") and entry.get("code")
            },
            key=str,
        ),
    }


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _set_diff(port: list, upstream: list) -> dict:
    p, u = set(map(_freeze, port)), set(map(_freeze, upstream))
    union = p | u
    return {
        "port": len(p),
        "upstream": len(u),
        "common": len(p & u),
        "jaccard": round(len(p & u) / len(union), 4) if union else 1.0,
        "only_port": sorted(map(str, p - u))[:EXAMPLES],
        "only_upstream": sorted(map(str, u - p))[:EXAMPLES],
    }


def _freeze(item):
    if isinstance(item, list):
        return tuple(_freeze(x) for x in item)
    return item


def compare_results(port: list, upstream: list) -> dict:
    out = _set_diff([k for k, _ in port], [k for k, _ in upstream])
    port_rank = {k: i for i, (k, _) in enumerate(port)}
    up_rank = {k: i for i, (k, _) in enumerate(upstream)}
    port_score, up_score = dict(port), dict(upstream)
    common = set(port_rank) & set(up_rank)
    top = 10
    out["top10_overlap"] = len(
        {k for k, _ in port[:top]} & {k for k, _ in upstream[:top]}
    )
    out["max_rank_shift"] = max(
        (abs(port_rank[k] - up_rank[k]) for k in common), default=0
    )
    diffs = [
        abs(port_score[k] - up_score[k])
        for k in common
        if isinstance(port_score.get(k), (int, float))
        and isinstance(up_score.get(k), (int, float))
    ]
    out["max_score_diff"] = round(max(diffs), 6) if diffs else 0.0
    out["largest_score_diffs"] = [
        {
            "result": str(k),
            "port": port_score[k],
            "upstream": up_score[k],
            "rank_port": port_rank[k],
            "rank_upstream": up_rank[k],
        }
        for k in sorted(
            (
                k
                for k in common
                if isinstance(port_score.get(k), (int, float))
                and isinstance(up_score.get(k), (int, float))
            ),
            key=lambda k: -abs(port_score[k] - up_score[k]),
        )[:3]
    ]
    return out


def compare_edge_details(port: dict, upstream: dict) -> dict:
    """For the KG edges both sides have: which differ in their attribute types
    or knowledge level / agent type (what the ranker scores on)."""
    common = set(port) & set(upstream)
    differing = [k for k in common if port[k] != upstream[k]]
    examples = []
    for k in sorted(differing, key=str)[:EXAMPLES]:
        p_attrs, p_kl, p_at = port[k]
        u_attrs, u_kl, u_at = upstream[k]
        examples.append(
            {
                "edge": str(k),
                "attributes_only_port": sorted(set(p_attrs) - set(u_attrs), key=str),
                "attributes_only_upstream": sorted(
                    set(u_attrs) - set(p_attrs), key=str
                ),
                "kl_at": {"port": [p_kl, p_at], "upstream": [u_kl, u_at]},
            }
        )
    return {"common": len(common), "differing": len(differing), "examples": examples}


def compare(port: dict, upstream: dict, similar_at: float) -> dict:
    diff = {
        "outcome": {
            k: {"port": port["outcome"][k], "upstream": upstream["outcome"][k]}
            for k in port["outcome"]
            if port["outcome"][k] != upstream["outcome"][k]
        },
        "results": compare_results(port["results"], upstream["results"]),
        "kg_nodes": _set_diff(port["kg_nodes"], upstream["kg_nodes"]),
        "kg_edges": _set_diff(port["kg_edges"], upstream["kg_edges"]),
        "edge_details": compare_edge_details(
            port["edge_details"], upstream["edge_details"]
        ),
        "aux_sizes": {"port": port["aux_sizes"], "upstream": upstream["aux_sizes"]},
        "log_codes": _set_diff(port["log_codes"], upstream["log_codes"]),
        "errors": {"port": port["errors"], "upstream": upstream["errors"]},
    }
    r = diff["results"]
    identical = (
        not diff["outcome"]
        and r["jaccard"] == 1.0
        and r["max_rank_shift"] == 0
        and r["max_score_diff"] == 0
        and diff["kg_nodes"]["jaccard"] == 1.0
        and diff["kg_edges"]["jaccard"] == 1.0
        and port["aux_sizes"] == upstream["aux_sizes"]
        and diff["log_codes"]["jaccard"] == 1.0
        and diff["edge_details"]["differing"] == 0
    )
    if identical:
        verdict = "SAME"
    elif (
        not diff["outcome"]
        and r["jaccard"] >= similar_at
        and diff["kg_edges"]["jaccard"] >= similar_at
    ):
        verdict = "SIMILAR"
    else:
        verdict = "DIFF"
    diff["verdict"] = verdict
    return diff


# ---------------------------------------------------------------------------
# Running
# ---------------------------------------------------------------------------


def _post(base: str, body: dict, timeout: float):
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=httpx.Timeout(timeout)) as client:
            resp = client.post(f"{base}/query", json=body)
        try:
            payload = resp.json()
        except ValueError:
            payload = {"_not_json": resp.text[:2000]}
        return resp.status_code, payload, time.perf_counter() - start
    except Exception as e:  # report it as that side's outcome
        return 0, {"_error": f"{type(e).__name__}: {e}"}, time.perf_counter() - start


def bodies(c, args) -> tuple:
    """(port body, upstream body): the case's TRAPI 2.0 query, identical on both
    sides except Shepherd's own arax_internal switch."""
    body = json.loads(json.dumps(c.body))
    parameters = dict(body.get("parameters") or {})
    parameters.setdefault("timeout", c.timeout)
    if args.bypass_cache:
        parameters["bypass_cache"] = True
    upstream = dict(body, parameters=parameters)
    port_parameters = dict(parameters)
    if args.arax_internal:
        port_parameters["arax_internal"] = True
    return dict(body, parameters=port_parameters), upstream


def run_case(c, args, out_dir: Path) -> dict:
    port_body, upstream_body = bodies(c, args)
    timeout = c.timeout + 60
    with ThreadPoolExecutor(max_workers=2) as pool:
        port_f = pool.submit(_post, args.port, port_body, timeout)
        up_f = pool.submit(_post, args.upstream, upstream_body, timeout)
        port_http, port_resp, port_s = port_f.result()
        up_http, up_resp, up_s = up_f.result()
    (out_dir / f"{c.name}.port.json").write_text(
        json.dumps(port_resp, indent=2), encoding="utf-8"
    )
    (out_dir / f"{c.name}.upstream.json").write_text(
        json.dumps(up_resp, indent=2), encoding="utf-8"
    )
    diff = compare(
        summarize(port_http, port_resp), summarize(up_http, up_resp), args.similar_at
    )
    if diff["verdict"] != "SAME" and c.name in EXPECTED:
        diff["verdict"] = "EXPECTED"
        diff["expected_because"] = EXPECTED[c.name]
    diff.update(
        case=c.name,
        group=c.group,
        seconds={"port": round(port_s, 1), "upstream": round(up_s, 1)},
    )
    (out_dir / f"{c.name}.diff.json").write_text(
        json.dumps(diff, indent=2), encoding="utf-8"
    )
    return diff


VERDICTS = ("DIFF", "EXPECTED", "SIMILAR", "SAME")


def _first_error(d: dict, side: str) -> str:
    errors = (d.get("errors") or {}).get(side) or []
    return errors[0].replace("\n", " ")[:160] if errors else ""


def report_line(d: dict) -> str:
    r = d["results"]
    outcome = (
        " outcome "
        + ", ".join(
            f"{k}: {v['port']}≠{v['upstream']}" for k, v in d["outcome"].items()
        )
        if d["outcome"]
        else ""
    )
    line = (
        f"{d['verdict']:8} {d['case']:45} results {r['port']}/{r['upstream']} "
        f"(J={r['jaccard']}, top10={r['top10_overlap']}, Δscore={r['max_score_diff']}) "
        f"edges J={d['kg_edges']['jaccard']}, "
        f"attrs≠{d['edge_details']['differing']}/{d['edge_details']['common']}{outcome}"
    )
    for side in ("port", "upstream"):
        if d["outcome"] and _first_error(d, side):
            line += f"\n         {side}: {_first_error(d, side)}"
    return line


def _counts(diffs: list) -> dict:
    return {v: sum(d["verdict"] == v for d in diffs) for v in VERDICTS}


def _counts_text(counts: dict) -> str:
    return ", ".join(f"{counts[v]} {v}" for v in reversed(VERDICTS))


def _cell(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def write_summary(diffs: list, out_dir: Path, args) -> None:
    (out_dir / "summary.json").write_text(json.dumps(diffs, indent=2), encoding="utf-8")
    lines = [
        "# ARAX parity: Shepherd port vs. upstream",
        "",
        f"- port: `{args.port}`",
        f"- upstream: `{args.upstream}`",
        f"- bypass_cache: {args.bypass_cache}; SIMILAR at Jaccard ≥ {args.similar_at}",
        f"- {_counts_text(_counts(diffs))}",
        "",
        "| verdict | case | results port/upstream | results J | top-10 overlap "
        "| max Δscore | KG edges J | edges with other attributes / KL / AT "
        "| outcome differences | port error | upstream error |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for d in sorted(diffs, key=lambda d: VERDICTS.index(d["verdict"])):
        r = d["results"]
        outcome = "; ".join(
            f"{k}: {v['port']} vs {v['upstream']}" for k, v in d["outcome"].items()
        )
        if d.get("expected_because"):
            outcome = f"{outcome} (expected: {d['expected_because']})"
        lines.append(
            f"| {d['verdict']} | {d['case']} | {r['port']}/{r['upstream']} "
            f"| {r['jaccard']} | {r['top10_overlap']} | {r['max_score_diff']} "
            f"| {d['kg_edges']['jaccard']} "
            f"| {d['edge_details']['differing']}/{d['edge_details']['common']} "
            f"| {_cell(outcome)} | {_cell(_first_error(d, 'port'))} "
            f"| {_cell(_first_error(d, 'upstream'))} |"
        )
    (out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--port",
        default=DEFAULT_PORT,
        help=f"Shepherd's /arax base (default {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--upstream",
        default=DEFAULT_UPSTREAM,
        help=f"upstream ARAX base (default {DEFAULT_UPSTREAM})",
    )
    parser.add_argument(
        "--only", nargs="+", help="cases whose name contains any of these"
    )
    parser.add_argument("--group", nargs="+", help="run only these groups")
    parser.add_argument(
        "--list", action="store_true", help="list the comparable cases and exit"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="cases to run at once (each sends 2 queries)",
    )
    parser.add_argument(
        "--out",
        default="responses/arax-compare",
        help="where responses and diffs are saved",
    )
    parser.add_argument(
        "--no-bypass-cache",
        dest="bypass_cache",
        action="store_false",
        help="let both sides answer from their KP caches",
    )
    parser.add_argument(
        "--no-arax-internal",
        dest="arax_internal",
        action="store_false",
        help="don't ask Shepherd for its in-process ARAX (parameters.arax_internal)",
    )
    parser.add_argument(
        "--similar-at",
        type=float,
        default=0.9,
        help="results / KG-edge Jaccard at or above which a case is SIMILAR (default 0.9)",
    )
    args = parser.parse_args()

    cases = comparable_cases(args.only, args.group)
    if args.list:
        for c in cases:
            print(f"{c.group:12} {c.name}")
        return 0
    if not cases:
        print("no cases match", file=sys.stderr)
        return 2
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"comparing {len(cases)} cases: port {args.port} vs upstream {args.upstream}")
    diffs = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for d in pool.map(lambda c: run_case(c, args, out_dir), cases):
            print(report_line(d), flush=True)
            diffs.append(d)
    write_summary(diffs, out_dir, args)
    counts = _counts(diffs)
    print(f"\n{_counts_text(counts)}; " f"details in {out_dir}/summary.md")
    return 1 if counts["DIFF"] else 0


if __name__ == "__main__":
    sys.exit(main())
