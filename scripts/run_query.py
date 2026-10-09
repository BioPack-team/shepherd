"""Send a test query to Shepherd (an ARA's /query or /asyncquery) or to an ARS.

One entrypoint for every manual query: pick the query type, the curies, and
where to send it.

    python scripts/run_query.py aragorn-local                       # MVP1 sweep
    python scripts/run_query.py arax-ci -c MONDO:0005148            # one curie
    python scripts/run_query.py aragorn-local arax-local -c MONDO:0004979 --runs 3
    python scripts/run_query.py ars-local -t mvp2-decreased -c NCBIGene:1017
    python scripts/run_query.py bte-dev -t mvp2-chem-increased -c CHEBI:85078
    python scripts/run_query.py aragorn-local -t pathfinder -c "CHEBI:45783~MONDO:0004979"
    python scripts/run_query.py arax-local --async --callback-host 172.17.0.1
    python scripts/run_query.py http://localhost:5439/arax --query-file my_query.json
    python scripts/run_query.py --list                              # targets, types, curies
    python scripts/run_query.py aragorn-local -t mvp2-increased --print-query

Targets are <service>-<env> names (see --list) or a URL. How the query is
sent depends on the target:

  ARA (aragorn, arax, bte, ...): POST {base}/query, one TRAPI response back.
      A response streamed with --stream (NDJSON progress lines, the response
      last) is parsed too. With --async: POST {base}/asyncquery with a
      callback served by this script, poll {base}/asyncquery_status/{job_id},
      and check the callback body is a TRAPI 2.0 Response to the query.
      Shepherd runs in Docker, so the callback has to reach this machine from
      a container: host.docker.internal works on Docker Desktop; on Linux pass
      --callback-host (e.g. the docker0 bridge IP, often 172.17.0.1).

  ARS (ars-*, shepherd-ars-*, or a URL ending in /ars): POST
      {base}/api/submit, poll {base}/api/messages/{pk}?trace=y until the
      parent leaves Running, then fetch the merged_version's TRAPI response.

Query types (-t):
  mvp1                 what chemicals treat <disease>        (curie: disease)
  mvp2-increased       chemicals that increase <gene>        (curie: gene)
  mvp2-decreased       chemicals that decrease <gene>        (curie: gene)
  mvp2-chem-increased  genes <chemical> increases            (curie: chemical)
  mvp2-chem-decreased  genes <chemical> decreases            (curie: chemical)
  pathfinder           paths between two nodes               (curie: SUBJECT~OBJECT)
Without -c, each type runs its default sweep (--list shows them; --limit N
runs the first N). --query-file sends a TRAPI JSON file as-is instead.

Every response is saved under --out (default responses/) as
<target>[-async]/<type>/<curie>_response.json (ARS runs also save the trace
tree), and each run's timings and counts are appended to --metrics (default
benchmark_metrics.json), keyed type -> curie -> target. The exit code is 1 if
any run errored.

scripts/ars_compare.py and scripts/ars_inject.py reuse this module's ARS
targets, query builders and default curies.
"""

import argparse
import asyncio
import json
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

SHEPHERD_HOSTS = {
    "local": "http://localhost:5439",
    "dev": "https://shepherd.renci.org",
    "ci": "https://shepherd.ci.transltr.io",
    "test": "https://shepherd.test.transltr.io",
}
SHEPHERD_ARAS = ("aragorn", "arax", "bte")

# name -> base URL of an ARA (POST {base}/query)
ARA_TARGETS = {
    f"{ara}-{env}": f"{host}/{ara}"
    for ara in SHEPHERD_ARAS
    for env, host in SHEPHERD_HOSTS.items()
}
ARA_TARGETS["aragorn-prod"] = "https://aragorn.transltr.io/aragorn"

# name -> base URL of an ARS (POST {base}/api/submit). ars-<env> is the
# deployed ARS; ars-local and shepherd-ars-<env> are Shepherd's port of it.
ARS_TARGETS = {
    "ars-prod": "https://ars-prod.transltr.io/ars",
    "ars-test": "https://ars.test.transltr.io/ars",
    "ars-ci": "https://ars.ci.transltr.io/ars",
    "ars-dev": "https://ars-dev.transltr.io/ars",
    "ars-local": f"{SHEPHERD_HOSTS['local']}/ars",
    **{
        f"shepherd-ars-{env}": f"{host}/ars"
        for env, host in SHEPHERD_HOSTS.items()
        if env != "local"
    },
}

REQUEST_TIMEOUT_SECONDS = 600.0
POLL_INTERVAL_SECONDS = 10.0
# ARS children time out at 5 min and merge children at 8 min (code 598), so a
# parent should always terminate well inside this budget.
COMPLETION_TIMEOUT_SECONDS = 15 * 60.0
# Long-form statuses the ARS trace endpoint renders; anything but
# Running/Waiting means the parent is finished.
TERMINAL_STATUSES = {"Done", "Stopped", "Error", "Unknown"}
ASYNC_STATUS_POLL_SECONDS = 2.0


def resolve_target(target: str) -> tuple[str, str]:
    """(kind, base URL) for a target name or URL; kind is "ara" or "ars"."""
    if target in ARA_TARGETS:
        return "ara", ARA_TARGETS[target]
    if target in ARS_TARGETS:
        return "ars", ARS_TARGETS[target]
    if target.startswith(("http://", "https://")):
        base = target.rstrip("/")
        for suffix in ("/query", "/asyncquery", "/api/submit"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
        return ("ars" if base.endswith("/ars") else "ara"), base
    raise SystemExit(f"unknown target {target!r} (see --list, or pass a URL)")


def target_label(target: str) -> str:
    """A directory/metrics-safe name for a target."""
    if target in ARA_TARGETS or target in ARS_TARGETS:
        return target
    return target.split("://", 1)[-1].strip("/").replace("/", "_").replace(":", "_")


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def mvp1_query(disease: str) -> dict:
    """MVP1: what chemicals treat <disease> (inferred)."""
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "ON": {"categories": ["biolink:Disease"], "ids": [disease]},
                    "SN": {"categories": ["biolink:ChemicalEntity"]},
                },
                "edges": {
                    "t_edge": {
                        "subject": "SN",
                        "object": "ON",
                        "predicates": ["biolink:treats"],
                        "knowledge_type": "inferred",
                    }
                },
            },
        },
    }


def mvp2_query(curie: str, direction: str, pinned: str = "gene") -> dict:
    """MVP2: chemicals that <increased|decreased> activity or abundance of a
    gene. pinned="gene" pins the gene (chemicals are the answers);
    pinned="chemical" pins the chemical (genes are the answers). The edge is
    the same either way: chemical affects gene, qualifiers on the gene."""
    gene_node = {"categories": ["biolink:Gene"]}
    chemical_node = {"categories": ["biolink:ChemicalEntity"]}
    (gene_node if pinned == "gene" else chemical_node)["ids"] = [curie]
    return {
        "message": {
            "query_graph": {
                "nodes": {"ON": gene_node, "SN": chemical_node},
                "edges": {
                    "t_edge": {
                        "subject": "SN",
                        "object": "ON",
                        "predicates": ["biolink:affects"],
                        "knowledge_type": "inferred",
                        # TRAPI 2.0: one qualifier set is a
                        # {qualifier_type_id: qualifier_value} mapping
                        "constraints": {
                            "qualifiers": [
                                {
                                    "biolink:object_aspect_qualifier": "activity_or_abundance",
                                    "biolink:object_direction_qualifier": direction,
                                }
                            ]
                        },
                    }
                },
            },
        },
    }


def pathfinder_query(subject: str, obj: str) -> dict:
    """Pathfinder: paths (not edges) between two pinned nodes. The ARS
    classifies any query graph carrying "paths" as a pathfinder query."""
    return {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": [subject]}, "n1": {"ids": [obj]}},
                "paths": {
                    "p0": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:related_to"],
                    }
                },
            },
        },
    }


QUERY_TYPES = (
    "mvp1",
    "mvp2-increased",
    "mvp2-decreased",
    "mvp2-chem-increased",
    "mvp2-chem-decreased",
    "pathfinder",
)
QUERY_TYPE_ALIASES = {"treats": "mvp1"}


def build_query(query_type: str, spec: str) -> dict:
    """The TRAPI query for a curie spec: one curie, or SUBJECT~OBJECT for
    pathfinder. Bare (no parameters/submitter): run_query adds those."""
    query_type = QUERY_TYPE_ALIASES.get(query_type, query_type)
    if query_type == "mvp1":
        return mvp1_query(spec)
    if query_type.startswith("mvp2-"):
        pinned = "chemical" if query_type.startswith("mvp2-chem-") else "gene"
        return mvp2_query(spec, query_type.rsplit("-", 1)[1], pinned=pinned)
    if query_type == "pathfinder":
        parts = [p.strip() for p in spec.split("~")]
        if len(parts) != 2 or not all(parts):
            raise ValueError(f"pathfinder spec must be 'SUBJECT~OBJECT', got {spec!r}")
        return pathfinder_query(*parts)
    raise ValueError(f"unknown query type {query_type!r}")


MVP1_DISEASES = [
    "MONDO:0005301",  # multiple sclerosis
    "MONDO:0011399",  # alpha thalassemia spectrum
    "MONDO:0016006",  # Cockayne Syndrome
    "MONDO:0016063",  # Cowden Disease
    "MONDO:0007186",  # Heartburn / Used for Hong's ranker analysis
    "MONDO:0005148",  # type 2 diabetes mellitus
    "MONDO:0020066",  # Ehlers-Danlos Syndrome
    "MONDO:0011705",  # lymphangioleiomyomatosis
    "MONDO:0004979",  # Asthma
    "MONDO:0001106",  # Kidney Failure
    "MONDO:0015564",  # Castleman Disease
    "MONDO:0100345",  # Lactose Intolerance
    "MONDO:0005799",  # Hookworm infectious disease
    "MONDO:0009265",  # Gaucher disease type I
    "MONDO:0018982",  # Niemann-Pick disease type C
    "MONDO:0018328",  # homozygous familial hypercholesterolemia
    "MONDO:0001119",  # premature menopause
    "MONDO:0016098",  # Immune-mediated Necrotizing Myopathy
    "MONDO:0005267",  # Heart Disorder
    "MONDO:0009831",  # malignant pancreatic neoplasm
    "MONDO:0001982",  # Niemann-Pick disease
    "MONDO:0850283",  # Acute Asthma
    "MONDO:0004975",  # Alzheimers
    "MONDO:0005100",  # systemic sclerosis
    "MONDO:0019293",  # skin vascular disease
    "MONDO:0005015",  # Diabetes Mellitus
    "MONDO:0005180",  # Parkinsons
    "MONDO:0007739",  # Huntington disease
    "MONDO:0002103",
]

MVP2_GENES = [
    "NCBIGene:3845",  # KRAS
    "NCBIGene:1017",  # CDK2
    "NCBIGene:1636",  # ACE
]

MVP2_CHEMICALS = [
    "CHEBI:85078",
    "CHEBI:167574",
    "CHEBI:41879",
]

PATHFINDER_PAIRS = [
    "MONDO:0021095~MONDO:0005105",
    "CHEBI:9139~MONDO:0004975",
    "CHEBI:5118~MONDO:0100233",
    "MONDO:0005180~MONDO:0005105",
    "MONDO:0019632~MONDO:0005340",
    "CHEBI:27881~NCBIGene:2739",
    "CHEBI:45783~MONDO:0004979",  # Imatinib -> Asthma
    "GO:0006914~MONDO:0005265",
    "NCBIGene:3458~CHEBI:16828",
    "MONDO:0005532~MONDO:0005180",
    "CHEBI:15647~UNII:31YO63LBSN",
    "CHEBI:28364~MONDO:0005311",
    "NCBIGene:3458~MONDO:0100096",
    "NCBIGene:27240~MONDO:0100096",
    "CHEBI:3750~MONDO:0013209",
    "CHEBI:83766~MONDO:0008170",
    "CHEBI:45783~MONDO:0004784",
    "UNII:7SE5582Q2P~MONDO:0007037",
    "MONDO:0005011~MONDO:0005180",
    "CHEBI:15365~MONDO:0005575",
    "CHEBI:50924~MONDO:0007256",
    "CHEBI:45713~NCBIGene:2739",
    "NCBIGene:54716~MONDO:0100096",
    "CHEBI:7465~MONDO:0008218",
    # "CHEBI:10033~MONDO:0004992",  # Warfarin -> Cancer, DON'T RUN
]

DEFAULT_SPECS = {
    "mvp1": MVP1_DISEASES,
    "mvp2-increased": MVP2_GENES,
    "mvp2-decreased": MVP2_GENES,
    "mvp2-chem-increased": MVP2_CHEMICALS,
    "mvp2-chem-decreased": MVP2_CHEMICALS,
    "pathfinder": PATHFINDER_PAIRS,
}


def finalize_query(query: dict, kind: str, args) -> dict:
    """Add the run's parameters to a bare query. ARA queries default to
    log_level DEBUG and carry a submitter; ARS queries are sent bare unless a
    parameter is asked for."""
    query = json.loads(json.dumps(query))
    parameters = dict(query.get("parameters") or {})
    log_level = args.log_level or ("DEBUG" if kind == "ara" else None)
    if log_level and log_level.lower() != "none":
        parameters["log_level"] = log_level
    if args.bypass_cache:
        parameters["bypass_cache"] = True
    if args.timeout_param is not None:
        parameters["timeout"] = args.timeout_param
    if parameters:
        query["parameters"] = parameters
    if kind == "ara":
        if args.submitter:
            query.setdefault("submitter", args.submitter)
        if args.stream and not args.async_:
            query["stream_progress"] = True
    return query


# ---------------------------------------------------------------------------
# Responses and metrics
# ---------------------------------------------------------------------------


def parse_response(content: bytes) -> tuple[dict, int]:
    """(TRAPI response, number of progress lines) from a response body. A
    query sent with "stream_progress": true comes back as NDJSON: progress
    lines while it runs, then the TRAPI response as the last line."""
    try:
        return json.loads(content), 0
    except json.JSONDecodeError as e:
        if not e.msg.startswith("Extra data"):
            raise
    lines = [line for line in content.splitlines() if line.strip()]
    return json.loads(lines[-1]), len(lines) - 1


def extract_response_stats(response_json: dict) -> dict:
    """Counts of interest from a TRAPI response."""
    message = response_json.get("message") or {}
    kg = message.get("knowledge_graph") or {}
    results = message.get("results") or []
    return {
        "num_results": len(results),
        "num_analyses": sum(len(r.get("analyses") or []) for r in results),
        "num_kg_nodes": len(kg.get("nodes") or {}),
        "num_kg_edges": len(kg.get("edges") or {}),
        "num_auxiliary_graphs": len(message.get("auxiliary_graphs") or {}),
    }


def empty_stats() -> dict:
    return {key: 0 for key in extract_response_stats({})}


def trapi_2_problem(response: dict, query: dict) -> "str | None":
    """Why ``response`` is not a TRAPI 2.0 Response to ``query``, if it isn't."""
    if response.get("schema_version") != "2.0.0":
        return f"schema_version {response.get('schema_version')!r}, not 2.0.0"
    if (response.get("parameters") or {}) != (query.get("parameters") or {}):
        return "the response does not repeat the query's parameters"
    if "logs" in response and not response["logs"]:
        return "empty logs (TRAPI 2.0: omit them)"
    return None


class Recorder:
    """Saves responses under the output dir and appends metrics to one JSON
    file, keyed query type -> curie spec -> target."""

    def __init__(self, out_dir: str, metrics_file: str):
        self.out_dir = Path(out_dir)
        self.metrics_path = Path(metrics_file)
        self._lock = asyncio.Lock()

    def save(self, label: str, query_type: str, spec: str, suffix: str, payload):
        out_dir = self.out_dir / label / query_type
        out_dir.mkdir(parents=True, exist_ok=True)
        name = spec.replace(":", "_").replace("~", "__")
        with (out_dir / f"{name}_{suffix}.json").open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    async def metrics(self, query_type: str, spec: str, label: str, run: dict):
        async with self._lock:
            data = {}
            if self.metrics_path.exists():
                try:
                    data = json.loads(self.metrics_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    data = {}  # corrupt: start fresh rather than lose this run
            data.setdefault(query_type, {}).setdefault(spec, {}).setdefault(
                label, []
            ).append(run)
            # temp file + atomic rename, so a crash can't half-write it
            tmp = self.metrics_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self.metrics_path)


def secs(value) -> str:
    return "-" if value is None else f"{value}s"


# ---------------------------------------------------------------------------
# Senders: each returns (metrics, response_json, extra files to save)
# ---------------------------------------------------------------------------


async def send_sync(base: str, query: dict, args) -> tuple[dict, dict, dict]:
    """POST {base}/query. Timing is split into server time (request sent ->
    first response byte) and transfer time (first byte -> last byte)."""
    metrics = {
        "url": f"{base}/query",
        "status_code": None,
        "server_processing_time_seconds": None,
        "network_transfer_time_seconds": None,
        "transfer_rate_mb_per_second": None,
        "num_stream_progress_lines": 0,
    }
    content = b""
    response_json: dict = {}
    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(args.timeout)) as client:
            async with client.stream("POST", f"{base}/query", json=query) as response:
                ttfb = time.perf_counter()  # headers are in: the server has answered
                metrics["status_code"] = response.status_code
                content = await response.aread()
                done = time.perf_counter()
                transfer = done - ttfb
                size_mb = len(content) / (1024 * 1024)
                metrics["server_processing_time_seconds"] = round(ttfb - start, 4)
                metrics["network_transfer_time_seconds"] = round(transfer, 4)
                metrics["response_size_bytes"] = len(content)
                metrics["response_size_mb"] = round(size_mb, 4)
                if transfer > 0:
                    metrics["transfer_rate_mb_per_second"] = round(
                        size_mb / transfer, 4
                    )
                response.raise_for_status()
        response_json, metrics["num_stream_progress_lines"] = parse_response(content)
        metrics.update(extract_response_stats(response_json))
    except httpx.HTTPStatusError as e:
        metrics["error"] = f"HTTP {e.response.status_code}"
        try:
            response_json = json.loads(content)
        except ValueError:
            response_json = {
                "error": metrics["error"],
                "body": content.decode(errors="replace")[:2000],
            }
    except Exception as e:
        metrics["error"] = f"{type(e).__name__}: {e}"
        response_json = {"error": metrics["error"]}
    return metrics, response_json, {}


class CallbackReceiver:
    """A small HTTP server that stores each POST body by its URL path."""

    def __init__(self, port: int):
        self._bodies: dict[str, tuple[float, bytes]] = {}
        self._cond = threading.Condition()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                with receiver._cond:
                    receiver._bodies[self.path] = (time.perf_counter(), body)
                    receiver._cond.notify_all()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Callback received.")

            def log_message(self, format, *args):
                pass  # keep the output to this script's own lines

        self._server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()

    def received(self, path: str):
        """(arrival time, body) for a callback path, or None."""
        with self._cond:
            return self._bodies.get(path)


async def send_async(
    base: str, query: dict, args, receiver: CallbackReceiver, tag: str
) -> tuple[dict, dict, dict]:
    """POST {base}/asyncquery, follow its status, wait for the callback."""
    path = f"/callback/{uuid.uuid4().hex}"
    query = {**query, "callback": f"{args.callback_base}{path}"}
    metrics = {
        "url": f"{base}/asyncquery",
        "job_id": None,
        "accept_status_code": None,
        "accept_time_seconds": None,
        "statuses": [],
        "final_status": None,
        "status_completed_seconds": None,
        "callback_seconds": None,
    }
    response_json: dict = {}
    start = time.perf_counter()
    last_status = None

    async def poll_status(client, job_id):
        nonlocal last_status
        status = await client.get(f"{base}/asyncquery_status/{job_id}")
        state = (
            status.json().get("status")
            if status.status_code == 200
            else f"HTTP {status.status_code}"
        )
        if state != last_status:
            elapsed = round(time.perf_counter() - start, 2)
            print(f"  {tag}: {elapsed}s status {state}")
            metrics["statuses"].append({"seconds": elapsed, "status": state})
            last_status = state
            if state == "Completed":
                metrics["status_completed_seconds"] = elapsed
            elif state == "Failed":
                # finish_query still sends the error response to the callback
                print(f"  {tag}: failed: {status.json().get('description')}")

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            accepted = await client.post(f"{base}/asyncquery", json=query)
            metrics["accept_time_seconds"] = round(time.perf_counter() - start, 4)
            metrics["accept_status_code"] = accepted.status_code
            accepted.raise_for_status()
            job_id = metrics["job_id"] = accepted.json()["job_id"]
            print(f"  {tag}: accepted as {job_id} (callback {query['callback']})")

            while time.perf_counter() - start < args.timeout:
                if receiver.received(path) is not None:
                    break
                await poll_status(client, job_id)
                await asyncio.sleep(ASYNC_STATUS_POLL_SECONDS)
            # The callback can land just before the status turns terminal
            # (finish_query sends it, then marks the query finished).
            settle_until = time.perf_counter() + 15
            while (
                receiver.received(path) is not None
                and last_status not in ("Completed", "Failed")
                and time.perf_counter() < settle_until
            ):
                await poll_status(client, job_id)
                if last_status not in ("Completed", "Failed"):
                    await asyncio.sleep(0.5)
            metrics["final_status"] = last_status

        arrived = receiver.received(path)
        if arrived is None:
            raise TimeoutError(f"no callback within {args.timeout}s")
        arrived_at, content = arrived
        metrics["callback_seconds"] = round(arrived_at - start, 4)
        metrics["response_size_bytes"] = len(content)
        metrics["response_size_mb"] = round(len(content) / (1024 * 1024), 4)
        response_json = json.loads(content)
        metrics.update(extract_response_stats(response_json))
        if response_json.get("status") not in (None, "Success", "OK", "Completed"):
            metrics["error"] = (
                f"response status {response_json.get('status')}: "
                f"{response_json.get('description')}"
            )
        elif problem := trapi_2_problem(response_json, query):
            metrics["error"] = problem
    except Exception as e:
        metrics["error"] = f"{type(e).__name__}: {e}"
        response_json = response_json or {"error": metrics["error"]}
    return metrics, response_json, {}


def summarize_children(trace: dict) -> list:
    """The per-ARA child rows of an ARS ?trace=y tree."""
    children = []
    for child in trace.get("children") or []:
        actor = child.get("actor") or {}
        children.append(
            {
                "pk": child.get("message"),
                "agent": actor.get("agent"),
                "inforesid": actor.get("inforesid"),
                "status": child.get("status"),
                "code": child.get("code"),
                "result_count": child.get("result_count"),
            }
        )
    return children


async def send_ars(base: str, query: dict, args, tag: str) -> tuple[dict, dict, dict]:
    """Submit to an ARS, poll the trace to completion, fetch the merged answer."""
    metrics = {
        "url": f"{base}/api/submit",
        "parent_pk": None,
        "merged_pk": None,
        "submit_status_code": None,
        "submit_time_seconds": None,
        "completion_time_seconds": None,
        "download_time_seconds": None,
        "poll_count": 0,
        "final_status": None,
        "children": [],
    }
    response_json = None
    extra = {}
    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(args.timeout)) as client:
            r = await client.post(f"{base}/api/submit", json=query)
            metrics["submit_status_code"] = r.status_code
            metrics["submit_time_seconds"] = round(time.perf_counter() - start, 4)
            r.raise_for_status()
            envelope = r.json()
            parent_pk = envelope.get("pk") or envelope.get("fields", {}).get("pk")
            if parent_pk is None:
                raise ValueError(f"submit response carried no pk: {envelope}")
            metrics["parent_pk"] = parent_pk
            print(f"  {tag}: submitted, parent pk {parent_pk}")

            # Poll first, then sleep: a response-cache hit comes back from
            # /submit already Done, so the first trace read settles it.
            deadline = start + COMPLETION_TIMEOUT_SECONDS
            while True:
                tr = await client.get(f"{base}/api/messages/{parent_pk}?trace=y")
                metrics["poll_count"] += 1
                tr.raise_for_status()
                trace = tr.json()
                metrics["final_status"] = trace.get("status")
                if metrics["final_status"] in TERMINAL_STATUSES:
                    break
                if time.perf_counter() > deadline:
                    raise TimeoutError(
                        f"parent {parent_pk} still {metrics['final_status']} "
                        f"after {COMPLETION_TIMEOUT_SECONDS:.0f}s"
                    )
                await asyncio.sleep(args.poll_interval)
            metrics["completion_time_seconds"] = round(time.perf_counter() - start, 4)
            metrics["children"] = summarize_children(trace)
            extra["trace"] = trace

            merged_pk = trace.get("merged_version")
            if merged_pk in (None, "", "None"):
                raise ValueError(
                    f"parent {parent_pk} finished {trace.get('status')} "
                    "with no merged_version"
                )
            metrics["merged_pk"] = merged_pk
            dl_start = time.perf_counter()
            mr = await client.get(f"{base}/api/messages/{merged_pk}")
            mr.raise_for_status()
            metrics["download_time_seconds"] = round(time.perf_counter() - dl_start, 4)
            response_json = (mr.json().get("fields") or {}).get("data")
            if response_json is None:
                raise ValueError(f"merged message {merged_pk} carried no data")
            metrics["response_size_bytes"] = len(mr.content)
            metrics["response_size_mb"] = round(len(mr.content) / (1024 * 1024), 4)
            metrics.update(extract_response_stats(response_json))
    except Exception as e:
        metrics["error"] = f"{type(e).__name__}: {e}"
        if response_json is None:
            response_json = {"error": metrics["error"]}
    return metrics, response_json, extra


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


async def run_one(
    query_type: str,
    spec: str,
    bare_query: dict,
    target: str,
    args,
    recorder: Recorder,
    receiver: "CallbackReceiver | None",
) -> dict:
    kind, base = resolve_target(target)
    use_async = kind == "ara" and args.async_
    label = target_label(target) + ("-async" if use_async else "")
    tag = f"{spec} @ {label}"
    query = finalize_query(bare_query, kind, args)
    print(f"Running {query_type} {spec} against {label}")

    run = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "query_type": query_type,
        "total_time_seconds": None,
        "response_size_bytes": None,
        "response_size_mb": None,
        **empty_stats(),
        "error": None,
    }
    start = time.perf_counter()
    if kind == "ars":
        metrics, response_json, extra = await send_ars(base, query, args, tag)
    elif use_async:
        metrics, response_json, extra = await send_async(
            base, query, args, receiver, tag
        )
    else:
        metrics, response_json, extra = await send_sync(base, query, args)
    run.update(metrics)
    run["total_time_seconds"] = round(time.perf_counter() - start, 4)

    recorder.save(label, query_type, spec, "response", response_json)
    for suffix, payload in extra.items():
        recorder.save(label, query_type, spec, suffix, payload)

    summary = (
        f"{tag}: {run['num_results']} results "
        f"({run['num_analyses']} analyses), {run['response_size_mb']} MB, "
    )
    if kind == "ars":
        summary += (
            f"{run['final_status']}, completed={secs(run['completion_time_seconds'])}, "
        )
        children = ", ".join(
            f"{c.get('agent') or c.get('inforesid')}={c.get('status')}"
            f"({c.get('result_count')})"
            for c in run["children"]
        )
    elif use_async:
        summary += (
            f"accepted={secs(run['accept_time_seconds'])}, "
            f"callback={secs(run['callback_seconds'])}, "
            f"final status {run['final_status']}, "
        )
        children = ""
    else:
        summary += (
            f"server={secs(run['server_processing_time_seconds'])}, "
            f"transfer={secs(run['network_transfer_time_seconds'])}, "
        )
        children = ""
    summary += f"total={secs(run['total_time_seconds'])}"
    if run.get("num_stream_progress_lines"):
        summary += f" (streamed {run['num_stream_progress_lines']} progress lines)"
    if children:
        summary += f" [{children}]"
    if run["error"]:
        summary += f" [ERROR: {run['error']}]"
    print(summary)

    await recorder.metrics(query_type, spec, label, run)
    return run


def print_list():
    print("ARA targets (POST {base}/query, or /asyncquery with --async):")
    for name, url in ARA_TARGETS.items():
        print(f"  {name:<20} {url}")
    print("\nARS targets (submit, poll, fetch the merged answer):")
    for name, url in ARS_TARGETS.items():
        print(f"  {name:<20} {url}")
    print("\nQuery types and their default curies:")
    for query_type in QUERY_TYPES:
        print(f"  {query_type}: {', '.join(DEFAULT_SPECS[query_type])}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        epilog="Run with --list to see the targets, query types and default curies.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "targets",
        nargs="*",
        metavar="TARGET",
        help="where to send it: a target name (e.g. aragorn-local, arax-ci, "
        "ars-local, ars-ci) or a base URL; several run side by side",
    )
    parser.add_argument(
        "--list", action="store_true", help="list targets, types, curies"
    )
    query = parser.add_argument_group("what to send")
    query.add_argument(
        "-t",
        "--type",
        dest="query_type",
        default="mvp1",
        choices=QUERY_TYPES + tuple(QUERY_TYPE_ALIASES),
        help="query template (default mvp1)",
    )
    query.add_argument(
        "-c",
        "--curie",
        nargs="+",
        dest="curies",
        help="curies to query (SUBJECT~OBJECT pairs for pathfinder); "
        "default: the type's sweep",
    )
    query.add_argument("--limit", type=int, help="run only the first N curies")
    query.add_argument(
        "--query-file",
        help="send this TRAPI JSON file instead of a template (-t/-c ignored)",
    )
    query.add_argument(
        "--log-level",
        help="parameters.log_level (default DEBUG for ARAs, unset for the "
        "ARS; 'none' to omit)",
    )
    query.add_argument(
        "--bypass-cache", action="store_true", help="set parameters.bypass_cache"
    )
    query.add_argument(
        "--timeout-param",
        type=int,
        metavar="SECONDS",
        help="set parameters.timeout",
    )
    query.add_argument(
        "--submitter", default="Max", help="ARA queries' submitter (default Max)"
    )
    query.add_argument(
        "--stream",
        action="store_true",
        help="ARA sync: ask for stream_progress (NDJSON progress lines)",
    )
    query.add_argument(
        "--print-query",
        action="store_true",
        help="print the query each target would get, and exit",
    )

    how = parser.add_argument_group("how to send it")
    how.add_argument("--runs", type=int, default=1, help="runs per target (default 1)")
    how.add_argument(
        "--async",
        dest="async_",
        action="store_true",
        help="ARA targets: use /asyncquery with a callback to this script",
    )
    how.add_argument(
        "--callback-host",
        default="host.docker.internal",
        help="host Shepherd's containers reach this machine at (--async)",
    )
    how.add_argument(
        "--callback-port", type=int, default=8765, help="callback port (--async)"
    )
    how.add_argument(
        "--timeout",
        type=float,
        default=REQUEST_TIMEOUT_SECONDS,
        help=f"HTTP/callback timeout in seconds (default {REQUEST_TIMEOUT_SECONDS:.0f})",
    )
    how.add_argument(
        "--poll-interval",
        type=float,
        default=POLL_INTERVAL_SECONDS,
        help=f"ARS trace poll interval (default {POLL_INTERVAL_SECONDS:.0f}s)",
    )

    out = parser.add_argument_group("output")
    out.add_argument(
        "--out", default="responses", help="response dir (default responses)"
    )
    out.add_argument(
        "--metrics",
        default="benchmark_metrics.json",
        help="metrics file (default benchmark_metrics.json)",
    )

    args = parser.parse_args(argv)
    args.query_type = QUERY_TYPE_ALIASES.get(args.query_type, args.query_type)
    if not args.list and not args.targets:
        parser.error("give at least one TARGET (see --list)")
    return args


async def main(argv=None) -> int:
    args = parse_args(argv)
    if args.list:
        print_list()
        return 0
    for target in args.targets:
        resolve_target(target)  # fail fast on a typo

    if args.query_file:
        query_type = "file"
        jobs = [
            (Path(args.query_file).stem, json.loads(Path(args.query_file).read_text()))
        ]
    else:
        query_type = args.query_type
        specs = args.curies or DEFAULT_SPECS[query_type]
        jobs = [(spec, build_query(query_type, spec)) for spec in specs]
    if args.limit:
        jobs = jobs[: args.limit]

    if args.print_query:
        for spec, bare in jobs:
            for target in args.targets:
                kind, _ = resolve_target(target)
                print(f"# {spec} -> {target}")
                print(json.dumps(finalize_query(bare, kind, args), indent=2))
        return 0

    args.callback_base = f"http://{args.callback_host}:{args.callback_port}"
    needs_callback = args.async_ and any(
        resolve_target(t)[0] == "ara" for t in args.targets
    )
    recorder = Recorder(args.out, args.metrics)
    receiver = CallbackReceiver(args.callback_port) if needs_callback else None

    errors = 0
    start = time.time()
    if receiver:
        receiver.__enter__()
    try:
        for spec, bare in jobs:
            runs = await asyncio.gather(
                *(
                    run_one(query_type, spec, bare, target, args, recorder, receiver)
                    for target in args.targets
                    for _ in range(args.runs)
                )
            )
            errors += sum(1 for run in runs if run["error"])
    finally:
        if receiver:
            receiver.__exit__()
    print(f"\nAll queries took {time.time() - start:.2f} seconds")
    print(f"Responses under {args.out}/, metrics in {args.metrics}")
    if errors:
        print(f"{errors} run(s) errored")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
