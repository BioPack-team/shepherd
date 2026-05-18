import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

target_urls = {
    "aragorn-prod": "https://aragorn.transltr.io/aragorn",
    "aragorn-test": "https://aragorn.test.transltr.io/aragorn",
    "aragorn-ci": "https://shepherd.ci.transltr.io/aragorn",
    "arax-ci": "https://shepherd.ci.transltr.io/arax",
    "bte-ci": "https://shepherd.ci.transltr.io/bte",
    "aragorn-dev": "https://shepherd.renci.org/aragorn",
    "arax-dev": "https://shepherd.renci.org/arax",
    "bte-dev": "https://shepherd.renci.org/bte",
    "aragorn-local": "http://localhost:5439/aragorn",
    "arax-local": "http://localhost:5439/arax",
    "bte-local": "http://localhost:5439/bte",
}

METRICS_FILE = "benchmark_metrics.json"
RESPONSES_DIR = "responses"
REQUEST_TIMEOUT_SECONDS = 600.0

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
        "parameters": {},
        "log_level": "DEBUG",
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


async def single_lookup(curie: str, target: str) -> dict:
    """Run one query against one target, capturing timing, size, and result-shape metrics.

    Timing is split into:
      - server_processing_time_seconds: request sent -> first response byte received (TTFB).
        This is roughly how long the server spent computing.
      - network_transfer_time_seconds: TTFB -> last byte received. This is roughly how long
        the (potentially large) payload took to come over the wire.
      - total_time_seconds: full round trip.
    """
    query = generate_query(curie)
    url = f"{target_urls[target]}/query"
    started_at = datetime.now(timezone.utc)
    print(f"Running {curie} against {target}")

    metrics: dict = {
        "timestamp": started_at.isoformat(),
        "url": url,
        "status_code": None,
        "total_time_seconds": None,
        "server_processing_time_seconds": None,
        "network_transfer_time_seconds": None,
        "response_size_bytes": None,
        "response_size_mb": None,
        "transfer_rate_mb_per_second": None,
        "num_results": 0,
        "num_kg_nodes": 0,
        "num_kg_edges": 0,
        "num_auxiliary_graphs": 0,
        "error": None,
    }

    content = b""
    response_json: dict | None = None
    request_start = time.perf_counter()

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS)) as client:
            async with client.stream("POST", url, json=query) as response:
                # Headers are received here -- the server has started sending. Treat as TTFB.
                ttfb = time.perf_counter()
                metrics["status_code"] = response.status_code

                content = await response.aread()
                done = time.perf_counter()

                server_time = ttfb - request_start
                transfer_time = done - ttfb
                total_time = done - request_start
                size_bytes = len(content)
                size_mb = size_bytes / (1024 * 1024)

                metrics["server_processing_time_seconds"] = round(server_time, 4)
                metrics["network_transfer_time_seconds"] = round(transfer_time, 4)
                metrics["total_time_seconds"] = round(total_time, 4)
                metrics["response_size_bytes"] = size_bytes
                metrics["response_size_mb"] = round(size_mb, 4)
                if transfer_time > 0:
                    metrics["transfer_rate_mb_per_second"] = round(size_mb / transfer_time, 4)

                response.raise_for_status()

        response_json = json.loads(content)
        metrics.update(extract_response_stats(response_json))

    except httpx.HTTPStatusError as e:
        metrics["error"] = f"HTTP {e.response.status_code}"
        try:
            response_json = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            response_json = {
                "error": metrics["error"],
                "body": content.decode(errors="replace")[:2000],
            }
    except Exception as e:
        if metrics["total_time_seconds"] is None:
            metrics["total_time_seconds"] = round(time.perf_counter() - request_start, 4)
        metrics["error"] = f"{type(e).__name__}: {e}"
        response_json = {"error": metrics["error"]}

    # Save the raw response payload alongside the metrics for offline inspection.
    out_dir = Path(RESPONSES_DIR) / target
    out_dir.mkdir(parents=True, exist_ok=True)
    response_path = out_dir / f"{curie.replace(':', '_')}_response.json"
    with response_path.open("w", encoding="utf-8") as f:
        json.dump(response_json, f, indent=2)

    summary = (
        f"{curie} @ {target}: "
        f"{metrics['num_results']} results, "
        f"{metrics['response_size_mb']} MB, "
        f"server={metrics['server_processing_time_seconds']}s, "
        f"transfer={metrics['network_transfer_time_seconds']}s, "
        f"total={metrics['total_time_seconds']}s"
    )
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
    "MONDO:0015564",
    "MONDO:0100345",  # Lactose Intolerance
    "MONDO:0005799",  # Hookworm infectious disease
    "MONDO:0009265",
    "MONDO:0018982",
    "MONDO:0018328",
    "MONDO:0001119",
    "MONDO:0016098",  # Immune-mediated Necrotizing Myopathy
    "MONDO:0005267",  # Heart Disorder
    "MONDO:0009831",
    "MONDO:0001982",  # Niemann-Pick disease
    "MONDO:0850283",  # Acute Asthma
]


async def main():
    """Run the configured benchmark sweep and time it overall."""
    # Add more keys from target_urls here to compare endpoints in one run.
    targets = ["aragorn-dev"]
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
