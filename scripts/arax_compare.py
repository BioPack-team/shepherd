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
(lookups, creative mode, ARAXi, workflows, validation errors), except the
pathfinder cases (Shepherd answers path queries with its own arax.pathfinder
worker, DEC-7; upstream's pathfinder is no longer used). Cases that only
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
1 if any case is DIFF. A case whose difference comes from a recorded
decision or from how upstream's deployment behaves is EXPECTED (listed in
EXPECTED, with the reason); one where only upstream failed (an error status, a
gateway error, no answer) is UPSTREAM_ERROR, with upstream's message.

Ranking ties: ARAX's final scores are positional (_break_ties_and_preserve_order
gives results with equal raw scores 0.001 apart in the order they arrive, and 0
past the 1000th), and that order comes from hash and answer order, which differs
between processes. So results with tied raw scores can swap scores, and a
result cap (e.g. 500) can keep different tied results, on two runs of the SAME
instance. Run a control (--port set to the upstream URL, --no-arax-internal) to
see how much of a case's difference is this noise.
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

DEFAULT_UPSTREAM = "https://arax.ncats.io/beta/api/arax/v2.0"
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
# Pathfinder is not compared: Shepherd answers path queries with its own
# arax.pathfinder worker (DEC-7), and upstream ARAX's pathfinder is no longer used
NOT_COMPARED_PREFIXES = ("pathfinder_",)


def comparable_cases(only=None, groups=None) -> list:
    cases = []
    for c in Q.CASES:
        if (
            c.method
            or c.after
            or c.name in NOT_COMPARABLE
            or c.name.startswith(NOT_COMPARED_PREFIXES)
        ):
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


def edge_identity(e: dict) -> tuple:
    """What makes two KG edges the same assertion: two KP edges can share
    subject, predicate, object and primary source (e.g. an xDTD and a heuristic
    treats edge, or one per knowledge level), so all of these count."""
    qualifiers = tuple(
        sorted(
            (q.get("qualifier_type_id"), str(q.get("qualifier_value")))
            for q in e.get("qualifiers") or []
            if isinstance(q, dict)
        )
    )
    return (
        e.get("subject"),
        e.get("predicate"),
        e.get("object"),
        primary_source(e),
        e.get("knowledge_level"),
        e.get("agent_type"),
        qualifiers,
    )


def edge_details(edges: dict) -> dict:
    """{edge identity: sorted list of each such edge's attribute types}"""
    out = {}
    for e in edges.values():
        types = tuple(
            sorted(
                {
                    a.get("attribute_type_id")
                    for a in e.get("attributes") or []
                    if isinstance(a, dict)
                },
                key=str,
            )
        )
        out.setdefault(edge_identity(e), []).append(types)
    return {k: sorted(v) for k, v in out.items()}


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
        "edge_details": edge_details(kg.get("edges") or {}),
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
    """For the KG edges both sides have (by full identity, see edge_identity):
    which carry other attribute types (what the ranker scores on)."""
    common = set(port) & set(upstream)
    differing = [k for k in common if port[k] != upstream[k]]
    examples = []
    for k in sorted(differing, key=str)[:EXAMPLES]:
        p_attrs = {t for types in port[k] for t in types}
        u_attrs = {t for types in upstream[k] for t in types}
        examples.append(
            {
                "edge": str(k),
                "attributes_only_port": sorted(p_attrs - u_attrs, key=str),
                "attributes_only_upstream": sorted(u_attrs - p_attrs, key=str),
                "count": {"port": len(port[k]), "upstream": len(upstream[k])},
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
        json.dumps(port_resp), encoding="utf-8"
    )
    (out_dir / f"{c.name}.upstream.json").write_text(
        json.dumps(up_resp), encoding="utf-8"
    )
    meta = {
        "http": {"port": port_http, "upstream": up_http},
        "seconds": {"port": round(port_s, 1), "upstream": round(up_s, 1)},
    }
    (out_dir / f"{c.name}.meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return diff_case(c, port_resp, up_resp, meta, args, out_dir)


def diff_case(c, port_resp, up_resp, meta: dict, args, out_dir: Path) -> dict:
    http = meta.get("http") or {}
    diff = compare(
        summarize(http.get("port"), port_resp),
        summarize(http.get("upstream"), up_resp),
        args.similar_at,
    )
    if diff["verdict"] != "SAME" and c.name in EXPECTED:
        diff["verdict"] = "EXPECTED"
        diff["expected_because"] = EXPECTED[c.name]
    elif diff["verdict"] == "DIFF" and upstream_failed_alone(
        http.get("port"), port_resp, http.get("upstream"), up_resp
    ):
        diff["verdict"] = "UPSTREAM_ERROR"
    diff.update(case=c.name, group=c.group, seconds=meta.get("seconds"))
    (out_dir / f"{c.name}.diff.json").write_text(
        json.dumps(diff, indent=2), encoding="utf-8"
    )
    return diff


OK_STATUSES = (None, "Success", "OK")


def _failed(http, body) -> bool:
    if http is not None and (http == 0 or http >= 400):
        return True
    if not isinstance(body, dict) or "_not_json" in body or "_error" in body:
        return True
    return body.get("status") not in OK_STATUSES


def upstream_failed_alone(port_http, port_resp, up_http, up_resp) -> bool:
    """Upstream answered with an error (or not at all) where the port answered."""
    return _failed(up_http, up_resp) and not _failed(port_http, port_resp)


def _read_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def saved_meta(c, out_dir: Path) -> dict:
    """The HTTP statuses of a saved pair: from <case>.meta.json, or (runs before
    meta files) from the outcome of the earlier diff, which recorded them only
    when they differed (else they compare equal)."""
    meta_path = out_dir / f"{c.name}.meta.json"
    if meta_path.exists():
        return _read_json(meta_path)
    diff_path = out_dir / f"{c.name}.diff.json"
    old = _read_json(diff_path) if diff_path.exists() else {}
    http = (old.get("outcome") or {}).get("http") or {"port": None, "upstream": None}
    return {"http": http, "seconds": old.get("seconds")}


def rediff_saved(c, args, out_dir: Path):
    """The diff of a case from its saved responses, without querying again."""
    port_path = out_dir / f"{c.name}.port.json"
    up_path = out_dir / f"{c.name}.upstream.json"
    if not (port_path.exists() and up_path.exists()):
        return None
    return diff_case(
        c,
        _read_json(port_path),
        _read_json(up_path),
        saved_meta(c, out_dir),
        args,
        out_dir,
    )


# ---------------------------------------------------------------------------
# Pack: a small, shareable bundle of a run
# ---------------------------------------------------------------------------

DIGEST_RESULTS = 25  # top results kept in a digest


def digest(body, http=None) -> dict:
    """What it takes to explain a difference, without the bulk: the outcome,
    the ERROR / WARNING logs, the query plan, the top results with their
    analyses, and only the KG nodes, edges and support graphs those results
    use (with their attributes)."""
    if not isinstance(body, dict):
        return {"http": http, "body": str(body)[:2000]}
    message = _message(body)
    kg = message.get("knowledge_graph") or {}
    edges, nodes = kg.get("edges") or {}, kg.get("nodes") or {}
    aux = message.get("auxiliary_graphs") or {}
    results = (message.get("results") or [])[:DIGEST_RESULTS]
    edge_ids, node_ids, aux_ids = set(), set(), set()
    for result in results:
        for binding in (result.get("node_bindings") or {}).values():
            node_ids.update((binding or {}).get("ids") or [])
        for analysis in result.get("analyses") or []:
            aux_ids.update(analysis.get("support_graphs") or [])
            for binding in (analysis.get("edge_bindings") or {}).values():
                edge_ids.update((binding or {}).get("ids") or [])
    pending = list(aux_ids)
    while pending:  # support graphs, transitively through their edges' support graphs
        for edge_id in (aux.get(pending.pop()) or {}).get("edges") or []:
            if edge_id in edge_ids:
                continue
            edge_ids.add(edge_id)
            for attribute in (edges.get(edge_id) or {}).get("attributes") or []:
                if attribute.get("attribute_type_id") == "biolink:support_graphs":
                    for aux_id in attribute.get("value") or []:
                        if aux_id not in aux_ids:
                            aux_ids.add(aux_id)
                            pending.append(aux_id)
    for edge_id in edge_ids:
        edge = edges.get(edge_id) or {}
        node_ids.update(filter(None, (edge.get("subject"), edge.get("object"))))
    logs = body.get("logs") or []
    query_options = body.get("query_options") or {}
    return {
        "http": http,
        "status": body.get("status"),
        "description": body.get("description"),
        "schema_version": body.get("schema_version"),
        "biolink_version": body.get("biolink_version"),
        "tool_version": body.get("tool_version"),
        "n_results": len(message.get("results") or []),
        "n_kg_nodes": len(nodes),
        "n_kg_edges": len(edges),
        "n_aux_graphs": len(aux),
        "n_logs": len(logs),
        "logs": [e for e in logs if e.get("level") in ("WARNING", "ERROR")],
        "query_plan": body.get("query_plan") or query_options.get("query_plan"),
        "query_graph": message.get("query_graph"),
        "results": results,
        "knowledge_graph": {
            "nodes": {k: nodes[k] for k in sorted(node_ids) if k in nodes},
            "edges": {k: edges[k] for k in sorted(edge_ids) if k in edges},
        },
        "auxiliary_graphs": {k: aux[k] for k in sorted(aux_ids) if k in aux},
    }


def pack(cases: list, out_dir: Path) -> Path:
    """<out>/arax-compare-pack.zip: the summary, every diff, and a digest of
    every saved response."""
    import zipfile

    path = out_dir / "arax-compare-pack.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in ("summary.md", "summary.json"):
            if (out_dir / name).exists():
                zf.write(out_dir / name, name)
        for c in cases:
            meta = saved_meta(c, out_dir)
            if (out_dir / f"{c.name}.diff.json").exists():
                zf.write(out_dir / f"{c.name}.diff.json", f"{c.name}.diff.json")
            for side in ("port", "upstream"):
                saved = out_dir / f"{c.name}.{side}.json"
                if saved.exists():
                    zf.writestr(
                        f"{c.name}.{side}.digest.json",
                        json.dumps(
                            digest(
                                _read_json(saved), (meta.get("http") or {}).get(side)
                            ),
                            indent=1,
                        ),
                    )
    return path


VERDICTS = ("DIFF", "UPSTREAM_ERROR", "EXPECTED", "SIMILAR", "SAME")


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
    parser.add_argument(
        "--from-saved",
        action="store_true",
        help="re-compare the responses saved under --out, without querying again",
    )
    parser.add_argument(
        "--pack",
        action="store_true",
        help="also write <out>/arax-compare-pack.zip: the summary, the diffs and a "
        "small digest of each response (to share a run)",
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
    diffs = []
    if args.from_saved:
        print(f"re-comparing the saved responses in {out_dir}")
        for c in cases:
            d = rediff_saved(c, args, out_dir)
            if d is not None:
                print(report_line(d), flush=True)
                diffs.append(d)
    else:
        print(
            f"comparing {len(cases)} cases: port {args.port} vs upstream {args.upstream}"
        )
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for d in pool.map(lambda c: run_case(c, args, out_dir), cases):
                print(report_line(d), flush=True)
                diffs.append(d)
    write_summary(diffs, out_dir, args)
    if args.pack:
        path = pack(cases, out_dir)
        print(f"packed {path} ({path.stat().st_size / 1e6:.1f} MB)")
    counts = _counts(diffs)
    print(f"\n{_counts_text(counts)}; " f"details in {out_dir}/summary.md")
    return 1 if counts["DIFF"] else 0


if __name__ == "__main__":
    sys.exit(main())
