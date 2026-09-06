"""Benchmark/smoke-test driver for the ARS surface (hosted Relay port).

The ARA endpoints (scripts/test_shepherd.py) are synchronous: one POST, one
TRAPI response. The ARS is not -- a submit fans the query out to every
active ARA and the answer is assembled asynchronously:

  1. POST {base}/api/submit           -> 201 Django-style envelope, "pk" is
                                         the parent query message
  2. GET  {base}/api/messages/{pk}?trace=y   poll until the parent status
                                         leaves "Running"; the tree carries
                                         per-ARA child statuses and, once
                                         merging finishes, "merged_version"
  3. GET  {base}/api/messages/{merged_version}  -> envelope whose
                                         fields.data is the merged TRAPI
                                         response

Metrics land in ars_benchmark_metrics.json keyed curie -> target; the final
merged TRAPI payload (and the final trace tree) are written under
ars_responses/<target>/ for offline inspection.
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

target_urls = {
    "ars-prod": "https://ars-prod.transltr.io/ars",
    "ars-test": "https://ars.test.transltr.io/ars",
    "ars-ci": "https://ars.ci.transltr.io/ars",
    "ars-dev": "https://ars-dev.transltr.io/ars",
    "shepherd-ars-ci": "https://shepherd.ci.transltr.io/ars",
    "shepherd-ars-dev": "https://shepherd.renci.org/ars",
    "ars-local": "http://localhost:5439/ars",
}

METRICS_FILE = "ars_benchmark_metrics.json"
RESPONSES_DIR = "ars_responses"
REQUEST_TIMEOUT_SECONDS = 600.0
POLL_INTERVAL_SECONDS = 10.0
# Children time out at 5 min and merge children at 8 min (code 598), so a
# parent should always terminate well inside this budget.
COMPLETION_TIMEOUT_SECONDS = 15 * 60.0

# Long-form statuses the trace endpoint renders; anything but Running/Waiting
# means the parent is finished (Done / Stopped / Error / Unknown).
TERMINAL_STATUSES = {"Done", "Stopped", "Error", "Unknown"}

# Serialize writes to the metrics file across concurrent tasks.
_metrics_lock = asyncio.Lock()


def generate_query(curie: str) -> dict:
    """Build a TRAPI 'what chemicals treat <disease>' inferred query for a curie."""
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "ON": {"categories": ["biolink:Disease"], "ids": [curie]},
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


def extract_response_stats(response_json: dict) -> dict:
    """Pull counts of interest out of a TRAPI response payload."""
    message = response_json.get("message") or {}
    kg = message.get("knowledge_graph") or {}
    results = message.get("results") or []
    aux_graphs = message.get("auxiliary_graphs") or {}
    return {
        "num_results": len(results),
        "num_kg_nodes": len(kg.get("nodes") or {}),
        "num_kg_edges": len(kg.get("edges") or {}),
        "num_auxiliary_graphs": len(aux_graphs),
    }


def summarize_children(trace: dict) -> list:
    """Flatten the per-ARA child rows out of a ?trace=y tree."""
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


async def write_metrics(curie: str, target: str, run_metrics: dict) -> None:
    """Append this run's metrics to the persistent JSON file, keyed curie -> target."""
    async with _metrics_lock:
        path = Path(METRICS_FILE)
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                # Corrupt or unreadable file -- start fresh rather than lose this run.
                data = {}
        else:
            data = {}

        data.setdefault(curie, {}).setdefault(target, []).append(run_metrics)

        # Write to a temp file first, then atomic rename, so a crash mid-write
        # doesn't leave a half-written metrics file.
        tmp_path = path.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        tmp_path.replace(path)


def _save_response(target: str, curie: str, suffix: str, payload) -> None:
    out_dir = Path(RESPONSES_DIR) / target
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{curie.replace(':', '_')}_{suffix}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


async def single_lookup(curie: str, target: str) -> dict:
    """Submit one query to one ARS, poll it to completion, fetch the merged answer."""
    query = generate_query(curie)
    base = target_urls[target]
    started_at = datetime.now(timezone.utc)
    print(f"Running {curie} against {target}")

    metrics: dict = {
        "timestamp": started_at.isoformat(),
        "url": f"{base}/api/submit",
        "parent_pk": None,
        "merged_pk": None,
        "submit_status_code": None,
        "submit_time_seconds": None,
        "completion_time_seconds": None,
        "download_time_seconds": None,
        "total_time_seconds": None,
        "poll_count": 0,
        "final_status": None,
        "children": [],
        "response_size_bytes": None,
        "response_size_mb": None,
        "num_results": 0,
        "num_kg_nodes": 0,
        "num_kg_edges": 0,
        "num_auxiliary_graphs": 0,
        "error": None,
    }

    response_json: dict | None = None
    start = time.perf_counter()

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
        ) as client:
            # 1. submit
            r = await client.post(f"{base}/api/submit", json=query)
            metrics["submit_status_code"] = r.status_code
            metrics["submit_time_seconds"] = round(time.perf_counter() - start, 4)
            r.raise_for_status()
            envelope = r.json()
            parent_pk = envelope.get("pk") or envelope.get("fields", {}).get("pk")
            if parent_pk is None:
                raise ValueError(f"submit response carried no pk: {envelope}")
            metrics["parent_pk"] = parent_pk
            print(f"{curie} @ {target}: submitted, parent pk {parent_pk}")

            # 2. poll the trace until the parent leaves Running
            trace: dict = {}
            deadline = start + COMPLETION_TIMEOUT_SECONDS
            while True:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                tr = await client.get(f"{base}/api/messages/{parent_pk}?trace=y")
                metrics["poll_count"] += 1
                tr.raise_for_status()
                trace = tr.json()
                status = trace.get("status")
                metrics["final_status"] = status
                if status in TERMINAL_STATUSES:
                    break
                if time.perf_counter() > deadline:
                    raise TimeoutError(
                        f"parent {parent_pk} still {status} after "
                        f"{COMPLETION_TIMEOUT_SECONDS:.0f}s"
                    )
            metrics["completion_time_seconds"] = round(time.perf_counter() - start, 4)
            metrics["children"] = summarize_children(trace)
            _save_response(target, curie, "trace", trace)

            # 3. fetch the merged answer
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
            merged_envelope = mr.json()
            response_json = (merged_envelope.get("fields") or {}).get("data")
            if response_json is None:
                raise ValueError(f"merged message {merged_pk} carried no data")

            size_bytes = len(mr.content)
            metrics["response_size_bytes"] = size_bytes
            metrics["response_size_mb"] = round(size_bytes / (1024 * 1024), 4)
            metrics.update(extract_response_stats(response_json))

    except Exception as e:
        metrics["error"] = f"{type(e).__name__}: {e}"
        if response_json is None:
            response_json = {"error": metrics["error"]}

    metrics["total_time_seconds"] = round(time.perf_counter() - start, 4)

    # Save the merged TRAPI payload alongside the metrics for offline inspection.
    _save_response(target, curie, "response", response_json)

    child_bits = ", ".join(
        f"{c.get('agent') or c.get('inforesid')}={c.get('status')}"
        f"({c.get('result_count')})"
        for c in metrics["children"]
    )
    summary = (
        f"{curie} @ {target}: {metrics['final_status']}, "
        f"{metrics['num_results']} results, "
        f"{metrics['response_size_mb']} MB, "
        f"completed={metrics['completion_time_seconds']}s, "
        f"total={metrics['total_time_seconds']}s"
    )
    if child_bits:
        summary += f" [{child_bits}]"
    if metrics["error"]:
        summary += f" [ERROR: {metrics['error']}]"
    print(summary)

    await write_metrics(curie, target, metrics)
    return metrics


curie_list = [
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
    "CHEBI:85078",  # MVP2
]


async def main():
    """Run the configured benchmark sweep and time it overall."""
    # Add more keys from target_urls here to compare endpoints in one run.
    targets = ["ars-local"]
    runs_per_target = 1

    start = time.time()
    for curie in curie_list:
        queries = [
            single_lookup(curie, target)
            for target in targets
            for _ in range(runs_per_target)
        ]
        await asyncio.gather(*queries)
    print(f"\nAll queries took {time.time() - start:.2f} seconds")
    print(f"Metrics saved to {METRICS_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
