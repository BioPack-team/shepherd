"""Exercise Shepherd's async endpoints end to end: /asyncquery, the status
endpoint, and the callback.

For each curie this:
  1. POSTs the query (from test_shepherd.generate_query) to
     {base}/asyncquery with a callback URL served by this script,
  2. polls {base}/asyncquery_status/{job_id}, printing each status change,
  3. waits for Shepherd's finish_query to POST the response to the callback,
and records timings and result counts in benchmark_metrics.json (under
"<target>-async") and the callback body in responses/<target>-async/.

Shepherd runs in Docker, so the callback URL has to reach this machine from
inside a container: the default host, host.docker.internal, does on Docker
Desktop (Mac/Windows). On Linux, pass --callback-host with an address the
containers can reach (e.g. the docker0 bridge IP, often 172.17.0.1).

Usage:
    python test_async.py [--target arax-local] [--curie MONDO:0005148 ...]
        [--port 8765] [--callback-host host.docker.internal] [--timeout 600]
"""

import argparse
import asyncio
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

from test_shepherd import (
    RESPONSES_DIR,
    extract_response_stats,
    generate_query,
    target_urls,
    write_metrics,
)

STATUS_POLL_SECONDS = 2.0


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


def async_base_url(target: str) -> str:
    """{base} for a test_shepherd target: its /query URL without /query."""
    url = target_urls[target]
    if not url.endswith("/query"):
        raise SystemExit(f"{target} ({url}) is not a /query endpoint")
    return url[: -len("/query")]


async def single_async_query(
    curie: str,
    target: str,
    receiver: CallbackReceiver,
    callback_base: str,
    timeout: float,
) -> dict:
    base = async_base_url(target)
    path = f"/callback/{uuid.uuid4().hex}"
    query = generate_query(curie)
    query.pop("stream_progress", None)  # not an async option
    query["callback"] = f"{callback_base}{path}"
    print(f"Running {curie} against {base}/asyncquery (callback {query['callback']})")

    metrics: dict = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "url": f"{base}/asyncquery",
        "job_id": None,
        "accept_status_code": None,
        "accept_time_seconds": None,
        "statuses": [],
        "final_status": None,
        "status_completed_seconds": None,
        "callback_seconds": None,
        "response_size_mb": None,
        "num_results": 0,
        "num_kg_nodes": 0,
        "num_kg_edges": 0,
        "num_auxiliary_graphs": 0,
        "error": None,
    }
    response_json: dict = {}
    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            accepted = await client.post(f"{base}/asyncquery", json=query)
            metrics["accept_time_seconds"] = round(time.perf_counter() - start, 4)
            metrics["accept_status_code"] = accepted.status_code
            body = accepted.json()
            print(f"  accepted in {metrics['accept_time_seconds']}s: {body}")
            accepted.raise_for_status()
            job_id = metrics["job_id"] = body["job_id"]

            last_status = None
            while time.perf_counter() - start < timeout:
                arrived = receiver.received(path)
                if arrived is not None:
                    break
                status = await client.get(f"{base}/asyncquery_status/{job_id}")
                state = (
                    status.json().get("status")
                    if status.status_code == 200
                    else f"HTTP {status.status_code}"
                )
                if state != last_status:
                    elapsed = round(time.perf_counter() - start, 2)
                    print(f"  {elapsed}s: status {state}")
                    metrics["statuses"].append({"seconds": elapsed, "status": state})
                    last_status = state
                    if state == "Completed":
                        metrics["status_completed_seconds"] = elapsed
                    elif state == "Failed":
                        # finish_query still sends the error response to the callback
                        print(f"  failed: {status.json().get('description')}")
                await asyncio.sleep(STATUS_POLL_SECONDS)
            # The callback can land just before the status turns terminal
            # (finish_query sends it, then the query is marked finished)
            settle_until = time.perf_counter() + 15
            while (
                receiver.received(path) is not None
                and last_status not in ("Completed", "Failed")
                and time.perf_counter() < settle_until
            ):
                status = await client.get(f"{base}/asyncquery_status/{job_id}")
                if status.status_code == 200:
                    state = status.json().get("status")
                    if state != last_status:
                        elapsed = round(time.perf_counter() - start, 2)
                        print(f"  {elapsed}s: status {state}")
                        metrics["statuses"].append(
                            {"seconds": elapsed, "status": state}
                        )
                        last_status = state
                        if state == "Completed":
                            metrics["status_completed_seconds"] = elapsed
                if last_status not in ("Completed", "Failed"):
                    await asyncio.sleep(0.5)
            metrics["final_status"] = last_status

        arrived = receiver.received(path)
        if arrived is None:
            raise TimeoutError(f"no callback within {timeout}s")
        arrived_at, content = arrived
        metrics["callback_seconds"] = round(arrived_at - start, 4)
        metrics["response_size_mb"] = round(len(content) / (1024 * 1024), 4)
        response_json = json.loads(content)
        metrics.update(extract_response_stats(response_json))
        if response_json.get("status") not in (None, "Success", "OK", "Completed"):
            metrics["error"] = (
                f"response status {response_json.get('status')}: "
                f"{response_json.get('description')}"
            )
    except Exception as e:
        metrics["error"] = f"{type(e).__name__}: {e}"
        response_json = response_json or {"error": metrics["error"]}

    out_dir = Path(RESPONSES_DIR) / f"{target}-async"
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"{curie.replace(':', '_')}_response.json").open(
        "w", encoding="utf-8"
    ) as f:
        json.dump(response_json, f, indent=2)

    def secs(key):
        return "-" if metrics[key] is None else f"{metrics[key]}s"

    summary = (
        f"{curie} @ {target} (async): {metrics['num_results']} results, "
        f"{metrics['response_size_mb']} MB, accepted={secs('accept_time_seconds')}, "
        f"callback={secs('callback_seconds')}, final status {metrics['final_status']}"
    )
    if metrics["error"]:
        summary += f" [ERROR: {metrics['error']}]"
    print(summary)
    await write_metrics(curie, f"{target}-async", metrics)
    return metrics


async def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--target", default="arax-local", choices=sorted(target_urls))
    parser.add_argument("--curie", nargs="+", default=["MONDO:0005148"])
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--callback-host", default="host.docker.internal")
    parser.add_argument("--timeout", type=float, default=600.0)
    args = parser.parse_args()

    callback_base = f"http://{args.callback_host}:{args.port}"
    start = time.time()
    with CallbackReceiver(args.port) as receiver:
        for curie in args.curie:
            await single_async_query(
                curie, args.target, receiver, callback_base, args.timeout
            )
    print(f"\nAll queries took {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
