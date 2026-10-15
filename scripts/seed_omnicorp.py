#!/usr/bin/env python3
"""Seed a captured TRAPI response into Redis and inject an aragorn.omnicorp task.

A reliable, backend-free way to exercise the omnicorp worker's robustness paths.
It bypasses the whole lookup pipeline: it writes a message of *known size*
straight into the data store (same zstd+orjson wire format the worker reads) and
XADDs a task onto the worker's stream. Point it at a large captured response
(e.g. one of scripts/test_shepherd.py's responses/*.json, or your 300 MB one) to
hit the memory / OOM / timeout / recovery paths deterministically.

Prereqs:
  * the local stack is running (`docker compose up`) so the worker's consumer
    group exists and it's polling the stream;
  * run from an env with the project deps (orjson, zstandard, redis), e.g. your
    project virtualenv.

Examples:
  # One big task -- OOM-recover / crash-recover / (with a short POOL_TASK_TIMEOUT_SEC) timeout
  python scripts/seed_omnicorp.py responses/aragorn-dev/MONDO_0004979_response.json

  # 200 fresh tasks -- watch child recycling / RSS sawtooth in `docker stats` or the monitor
  python scripts/seed_omnicorp.py <file> --repeat 200

  # Idempotency: same message twice, spaced so the first overlay finishes first.
  # Best paired with POOL_MAX_WORKERS=1 so the two can't run concurrently.
  python scripts/seed_omnicorp.py <file> --repeat 2 --reuse-id --delay 15

Watch:  docker compose logs -f aragorn_omnicorp   |   monitor http://localhost:5440
"""

import argparse
import json
import time
import uuid

import orjson
import redis
import zstandard

STREAM = "aragorn.omnicorp"
DATA_DB = 1  # shepherd_utils.db._get_sync_data_db uses db=1
BROKER_DB = 0  # shepherd_utils.broker uses db=0


def encode_message(obj) -> bytes:
    """Match shepherd_utils.db.encode_message exactly (zstd over orjson bytes)."""
    return zstandard.compress(orjson.dumps(obj))


def load_envelope(path: str) -> dict:
    """Read a captured response and wrap it in the shepherd worker envelope.

    Accepts either a full response ({"message": {...}, ...}) or a bare TRAPI
    message; either way the worker wants {"message": <trapi>, "workflow", "logs"}.
    """
    with open(path, "rb") as f:
        data = orjson.loads(f.read())
    message = data["message"] if isinstance(data, dict) and "message" in data else data
    return {"message": message, "workflow": [{"id": STREAM}], "logs": []}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("response_file", help="Captured TRAPI response JSON to replay.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--password", default="supersecretpassword")
    ap.add_argument("--repeat", type=int, default=1, help="Number of tasks to inject.")
    ap.add_argument(
        "--reuse-id",
        action="store_true",
        help="Seed once and inject the SAME response_id every time. The worker "
        "overlays the first, then skips the rest (idempotency demo). Without "
        "this, each task gets a fresh un-overlaid copy so every one does real "
        "work (OOM / timeout / recycling).",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Seconds between injections. Raise it for --reuse-id so the first "
        "overlay finishes (and is saved) before the next task reads it.",
    )
    ap.add_argument("--ttl", type=int, default=3600, help="Data-store TTL (seconds).")
    args = ap.parse_args()

    data_db = redis.Redis(host=args.host, port=args.port, db=DATA_DB, password=args.password)
    broker = redis.Redis(host=args.host, port=args.port, db=BROKER_DB, password=args.password)
    # Fail fast with a clear message if the stack isn't up / password is wrong.
    broker.ping()

    envelope = load_envelope(args.response_file)
    raw = orjson.dumps(envelope)
    blob = encode_message(envelope)
    print(
        f"Loaded {args.response_file}: "
        f"{len(raw) / 1e6:.1f} MB decompressed, {len(blob) / 1e6:.1f} MB stored"
    )

    shared_id = f"seed-{uuid.uuid4().hex[:8]}"
    if args.reuse_id:
        data_db.set(shared_id, blob, ex=args.ttl)

    for i in range(args.repeat):
        response_id = shared_id if args.reuse_id else f"seed-{uuid.uuid4().hex[:8]}"
        if not args.reuse_id:
            data_db.set(response_id, blob, ex=args.ttl)
        broker.xadd(
            STREAM,
            {
                "query_id": f"seed-q-{uuid.uuid4().hex[:8]}",
                "response_id": response_id,
                "workflow": json.dumps([{"id": STREAM}]),
                "otel": "{}",
                "metadata": "{}",
                "log_level": "10",
            },
        )
        print(f"[{i + 1}/{args.repeat}] injected task response_id={response_id}")
        if i + 1 < args.repeat:
            time.sleep(args.delay)

    print(
        "\nDone. Watch:\n"
        "  docker compose logs -f aragorn_omnicorp\n"
        "  monitor dashboard: http://localhost:5440"
    )


if __name__ == "__main__":
    main()
