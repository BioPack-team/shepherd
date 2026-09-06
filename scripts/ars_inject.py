"""Capture a live ARS run, push the SAME inputs through the local ARS,
compare the final merged messages.

The straightest possible parity test, through the real running stack:

  CAPTURE  submit the query to the live ARS (default ars-ci), poll it to
           completion, download every ARA child's stored payload, the merge
           order (merged_versions_list), and the final merged message.
  INJECT   submit the same query to the local ARS while every local ARA URL
           points at this script's sink server (which answers 200 and never
           calls back, so all children stay Running). Then deliver the
           captured payloads into the local children via the result
           callback (POST /ars/api/messages/<child_pk>) one at a time, in
           the captured merge order, waiting for each merge to land before
           releasing the next. Captured zero-result agents get their empty
           payloads; captured errored agents (and any local-only agents)
           are closed out with the tr_ars.message.status: E header so the
           parent completes without waiting on the watchdog.
  COMPARE  the local merged message vs the captured one, ANNOTATIONS
           INCLUDED, deep-diffed with the harness normalizer. Only the
           logs arrays are excluded (their text is wall-clock/instance
           noise by construction).

Annotations are NOT mocked: reproducing production's annotations is part
of the parity being tested. Both stacks annotate the same way -- the
in-process biothings_annotator package against the live BioThings APIs
(the port pins the package commit; the live ARS installs it unpinned from
master at its image build). Residual annotation diffs therefore mean
package version skew between the two builds (stable across reruns) or
BioThings backend data movement between the capture and the injection
(unstable) -- rerun to classify before treating one as a pipeline bug.

Local stack prerequisites:
  - the ARA URL overrides point every actor at this script's sink,
    e.g. http://host.docker.internal:8210/<agent> (any path; it 200s all)
  - the local registry uses the STANDARD actor inforesids (the default
    ars_config seed), so re-running decorate_edges_with_infores over the
    captured (already-decorated) payloads is a no-op. The script warns and
    compensates if a local inforesid differs, but matching them is cleaner.

Usage:
  python scripts/ars_inject.py --curies MONDO:0005148
  python scripts/ars_inject.py --source ars-ci --local ars-local --sink-port 8210
"""

import argparse
import asyncio
import importlib.util
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


test_ars = _load("test_ars", SCRIPTS_DIR / "test_ars.py")
ars_compare = _load("ars_compare", SCRIPTS_DIR / "ars_compare.py")
normalize_mod = _load("ars_normalize", REPO / "tests/parity_e2e/normalize.py")

OUT_DIR = "ars_inject"
MERGE_WAIT_SECONDS = 180.0
CHILDREN_WAIT_SECONDS = 60.0


# ---------------------------------------------------------------------------
# sink: the "ARA" every local actor is pointed at; 200s everything, never
# calls back, so fanout leaves every child Running for us to inject into.
# (Deliberately NOT an annotator mock -- both stacks annotate in-process via
# the biothings_annotator package, so annotation parity is tested for real.)
# ---------------------------------------------------------------------------


class _SinkHandler(BaseHTTPRequestHandler):
    def _ok(self):
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        print(f"  [sink] swallowed POST {self.path}")
        self._ok()

    def do_GET(self):
        self._ok()

    def log_message(self, *args):
        pass


def start_sink(port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", port), _SinkHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"sink ARA listening on 0.0.0.0:{port} (answers 200, never calls back)")
    return server


# ---------------------------------------------------------------------------
# capture / inject
# ---------------------------------------------------------------------------


def classify_captured_children(trace: dict) -> dict:
    """canonical agent -> {status, code, result_count, inforesid}."""
    out = {}
    for child in trace.get("children") or []:
        actor = child.get("actor") or {}
        agent = ars_compare.canonical_agent(
            actor.get("agent") or actor.get("inforesid")
        )
        out[agent] = {
            "status": child.get("status"),
            "code": child.get("code"),
            "result_count": child.get("result_count"),
            "inforesid": actor.get("inforesid"),
        }
    return out


async def local_children(client, base, parent_pk) -> dict:
    """canonical agent -> {pk, inforesid} for the local run, waiting for the
    fanout to create them."""
    deadline = time.perf_counter() + CHILDREN_WAIT_SECONDS
    while True:
        tr = await client.get(f"{base}/api/messages/{parent_pk}?trace=y")
        tr.raise_for_status()
        trace = tr.json()
        kids = {}
        for child in trace.get("children") or []:
            actor = child.get("actor") or {}
            agent = ars_compare.canonical_agent(
                actor.get("agent") or actor.get("inforesid")
            )
            kids[agent] = {
                "pk": child.get("message"),
                "inforesid": actor.get("inforesid"),
            }
        if kids:
            return kids
        if time.perf_counter() > deadline:
            raise TimeoutError("local fanout never created any children")
        await asyncio.sleep(2)


async def wait_for_merge_count(client, base, parent_pk, count) -> None:
    deadline = time.perf_counter() + MERGE_WAIT_SECONDS
    while True:
        tr = await client.get(f"{base}/api/messages/{parent_pk}?trace=y")
        tr.raise_for_status()
        merged = ars_compare.merge_order(tr.json()) or []
        if len(merged) >= count:
            return
        if time.perf_counter() > deadline:
            raise TimeoutError(
                f"merge #{count} never landed ({len(merged)} in "
                f"{MERGE_WAIT_SECONDS:.0f}s) -- check ars_merge/ars_postprocess logs"
            )
        await asyncio.sleep(2)


def strip_alien_self_sources(merged: dict, alien_inforesids: set) -> dict:
    """When a local actor's inforesid differs from the captured one,
    re-running decorate_edges_with_infores appends the LOCAL inforesid as an
    extra source on that agent's edges. Remove exactly those entries so the
    comparison measures the pipeline, not the registry naming."""
    if not alien_inforesids or not isinstance(merged, dict):
        return merged
    out = json.loads(json.dumps(merged))
    edges = ((out.get("message") or {}).get("knowledge_graph") or {}).get("edges") or {}
    for edge in edges.values():
        sources = edge.get("sources")
        if isinstance(sources, list):
            edge["sources"] = [
                s
                for s in sources
                if not (
                    isinstance(s, dict) and s.get("resource_id") in alien_inforesids
                )
            ]
    return out


async def run_injection(curie: str, args) -> str:
    query = test_ars.generate_query(curie)
    out_dir = Path(OUT_DIR) / curie.replace(":", "_")
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- capture from the live ARS
    print(f"\n>>> {curie}: capturing from {args.source}")
    timeout = httpx.Timeout(test_ars.REQUEST_TIMEOUT_SECONDS)
    async with httpx.AsyncClient(timeout=timeout) as client:
        captured = await ars_compare.run_side(client, args.source, query)
    if captured["error"]:
        print(f"  capture failed: {captured['error']}")
        return "CAPTURE_ERROR"
    (out_dir / "captured_trace.json").write_text(
        json.dumps(captured["trace"], indent=2, default=str)
    )
    (out_dir / "captured_merged.json").write_text(
        json.dumps(captured["merged"], indent=2, default=str)
    )
    for agent, payload in captured["children"].items():
        (out_dir / f"captured_input_{agent}.json").write_text(
            json.dumps(payload, indent=2, default=str)
        )

    kinds = classify_captured_children(captured["trace"])
    order = ars_compare.merge_order(captured["trace"]) or []
    mergers = [a for a in order if a in captured["children"]]
    skipped_mergers = [a for a in order if a not in captured["children"]]
    if skipped_mergers:
        print(f"  WARNING: no captured payload for merged agents {skipped_mergers}")
    empties = [
        a
        for a, k in kinds.items()
        if a not in order
        and k["status"] == "Done"
        and a in captured["children"]
        and captured["children"][a] is not None
    ]
    errored = [a for a in kinds if a not in order and a not in empties]
    print(
        f"  captured: {len(mergers)} merging agents {mergers}, "
        f"{len(empties)} empty, {len(errored)} errored/other"
    )

    # ---- inject into the local ARS
    base = test_ars.target_urls[args.local]
    print(f"  injecting into {args.local}")
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(f"{base}/api/submit", json=query)
        r.raise_for_status()
        parent_pk = r.json().get("pk")
        print(f"  local parent {parent_pk}")

        kids = await local_children(client, base, parent_pk)
        missing = [a for a in mergers + empties if a not in kids]
        if missing:
            print(
                f"  ERROR: local registry has no active actor for captured "
                f"agents {missing} -- activate them and rerun"
            )
            return "MISSING_ACTORS"

        # registry naming mismatches make the callback's decorate pass add
        # the local inforesid as an extra source; note them for stripping
        alien = set()
        for agent, kid in kids.items():
            cap = kinds.get(agent)
            if cap and kid["inforesid"] and cap["inforesid"]:
                if kid["inforesid"] != cap["inforesid"]:
                    alien.add(kid["inforesid"])
                    print(
                        f"  WARNING: inforesid mismatch for {agent}: local "
                        f"{kid['inforesid']} vs captured {cap['inforesid']} -- "
                        "the extra local self-source will be stripped before "
                        "comparison (prefer the standard registry seed)"
                    )

        async def deliver(agent, payload=None, error=False):
            headers = {"Content-Type": "application/json"}
            if error:
                headers["tr_ars.message.status"] = "E"
                payload = {"message": {}}
            resp = await client.post(
                f"{base}/api/messages/{kids[agent]['pk']}",
                json=payload,
                headers=headers,
            )
            if resp.status_code not in (200, 201):
                print(
                    f"  ERROR: callback for {agent} -> {resp.status_code}: "
                    f"{resp.text[:300]}"
                )
            return resp.status_code

        # result-bearing agents, one at a time, in the captured merge order
        for i, agent in enumerate(mergers, start=1):
            print(f"  [{i}/{len(mergers)}] delivering {agent} and awaiting merge")
            code = await deliver(agent, payload=captured["children"][agent])
            if code not in (200, 201):
                return "INJECTION_ERROR"
            await wait_for_merge_count(client, base, parent_pk, i)

        for agent in empties:
            await deliver(agent, payload=captured["children"][agent])
        for agent in errored:
            if agent in kids:
                await deliver(agent, error=True)
        for agent in kids:
            if agent not in kinds:
                print(f"  closing local-only agent {agent} as errored")
                await deliver(agent, error=True)

        # ---- wait for the parent, fetch the local merged answer
        deadline = time.perf_counter() + test_ars.COMPLETION_TIMEOUT_SECONDS
        while True:
            tr = await client.get(f"{base}/api/messages/{parent_pk}?trace=y")
            tr.raise_for_status()
            trace = tr.json()
            if trace.get("status") in test_ars.TERMINAL_STATUSES:
                break
            if time.perf_counter() > deadline:
                raise TimeoutError(f"local parent {parent_pk} never completed")
            await asyncio.sleep(test_ars.POLL_INTERVAL_SECONDS)
        (out_dir / "local_trace.json").write_text(
            json.dumps(trace, indent=2, default=str)
        )
        merged_pk = trace.get("merged_version")
        if merged_pk in (None, "", "None"):
            print(f"  local parent finished {trace.get('status')} with no merge")
            return "NO_LOCAL_MERGE"
        mr = await client.get(f"{base}/api/messages/{merged_pk}")
        mr.raise_for_status()
        local_merged = (mr.json().get("fields") or {}).get("data")
    (out_dir / "local_merged.json").write_text(
        json.dumps(local_merged, indent=2, default=str)
    )

    # ---- compare, annotations included (the sink replayed production's
    # annotations, so they must match too); only logs are excluded
    captured_cmp = ars_compare.strip_for_comparison(
        captured["merged"], args.ignore_annotations, False
    )
    local_cmp = ars_compare.strip_for_comparison(
        local_merged, args.ignore_annotations, False
    )
    local_cmp = strip_alien_self_sources(local_cmp, alien)
    diffs = normalize_mod.diff(local_cmp, captured_cmp)
    report = {
        "curie": curie,
        "source": args.source,
        "local": args.local,
        "merge_order": mergers,
        "verdict": "MATCH" if not diffs else "DIVERGENCE",
        "diff_count": len(diffs),
        "diffs": diffs,
    }
    (out_dir / "diff.json").write_text(json.dumps(report, indent=2, default=str))

    print(f"\n{curie}: {report['verdict']}")
    for d in diffs[:15]:
        print(f"    {d}")
    if len(diffs) > 15:
        print(f"    ... {len(diffs) - 15} more (see {out_dir / 'diff.json'})")
    if diffs and all("biothings_annotations" in d for d in diffs):
        print(
            "    (every diff is a node annotation -- rerun to classify: a "
            "stable diff means biothings_annotator version skew between the "
            "live ARS's unpinned build and the port's pinned one, an "
            "unstable diff means BioThings backend data moved between "
            "capture and injection)"
        )
    return report["verdict"]


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default="ars-ci", choices=test_ars.target_urls)
    parser.add_argument("--local", default="ars-local", choices=test_ars.target_urls)
    parser.add_argument(
        "--curies",
        default=None,
        help="comma-separated curie subset (default: the full test_ars sweep)",
    )
    parser.add_argument(
        "--sink-port",
        type=int,
        default=8210,
        help="port for the sink ARA server (0 to not start one, if you run "
        "your own sink)",
    )
    parser.add_argument(
        "--ignore-annotations",
        action="store_true",
        help="strip biothings_annotations before diffing (an explicit "
        "opt-out; annotation parity is part of the test by default)",
    )
    args = parser.parse_args()

    if args.sink_port:
        start_sink(args.sink_port)
        print(
            "point every local ARA URL override at this sink "
            f"(e.g. http://host.docker.internal:{args.sink_port}/<agent>) "
            "before submitting.\n"
        )

    curies = args.curies.split(",") if args.curies else test_ars.curie_list
    verdicts = {}
    for curie in curies:
        try:
            verdicts[curie.strip()] = await run_injection(curie.strip(), args)
        except Exception as e:
            print(f"{curie}: ERROR {type(e).__name__}: {e}")
            verdicts[curie.strip()] = f"ERROR: {e}"

    print(f"\n{'=' * 70}")
    for curie, verdict in verdicts.items():
        print(f"  {curie:<16} {verdict}")
    Path(OUT_DIR).mkdir(exist_ok=True)
    (Path(OUT_DIR) / "verdicts.json").write_text(
        json.dumps(verdicts, indent=2, sort_keys=True)
    )


if __name__ == "__main__":
    asyncio.run(main())
