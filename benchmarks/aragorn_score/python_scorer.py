"""Benchmark runner for the Python aragorn.score ranker (the real worker code).

    python python_scorer.py MESSAGE [--repeat N] [--out result.json]
                            [--save-message scored.json.zst]

MESSAGE is a stored TRAPI Response (``{"message": ...}``) or a bare Message,
as JSON or zstd-compressed JSON (how Shepherd keeps it in Redis). Each repeat
runs the worker's pipeline on the raw bytes, timed per phase:

    decompress -> parse (orjson) -> score (Ranker.rank) -> serialize (orjson)
    -> compress (zstd)

The scoring is ``workers.aragorn_score.worker.Ranker``, unchanged; a subclass
only records which answers failed to score and why. The result file has the
same shape as the Rust runner's, for ``compare.py``.
"""

import argparse
import logging
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import orjson  # noqa: E402
import zstandard  # noqa: E402

from shepherd_utils.db import decompress_frame  # noqa: E402
from workers.aragorn_score.worker import Ranker  # noqa: E402

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


class TracingRanker(Ranker):
    """The worker's Ranker, noting which answers raise (``rank`` catches the
    exception and scores the answer 0)."""

    def __init__(self, message, logger):
        super().__init__(message, logger)
        self.failures = []
        self._index = {}

    def rank(self, answers, jaccard_like=False):
        self._index = {id(a): i for i, a in enumerate(answers)}
        return super().rank(answers, jaccard_like=jaccard_like)

    def score(self, answer, *args, **kwargs):
        try:
            return super().score(answer, *args, **kwargs)
        except Exception as e:
            self.failures.append(
                [self._index.get(id(answer)), f"{type(e).__name__}: {e}"]
            )
            raise


def run_once(raw: bytes, logger):
    times = {}

    t = time.perf_counter()
    data = decompress_frame(raw) if raw[:4] == ZSTD_MAGIC else raw
    times["decompress"] = time.perf_counter() - t

    t = time.perf_counter()
    doc = orjson.loads(data)
    times["parse"] = time.perf_counter() - t

    t = time.perf_counter()
    message = doc["message"] if isinstance(doc.get("message"), dict) else doc
    results = message.get("results")
    failures, order = [], []
    originals = list(results) if isinstance(results, list) else []
    if isinstance(results, list):
        ranker = TracingRanker(message, logger)
        ranked = ranker.rank(results)
        message["results"] = ranked
        failures = ranker.failures
        position = {id(a): i for i, a in enumerate(originals)}
        order = [position[id(a)] for a in ranked]
    times["score"] = time.perf_counter() - t

    t = time.perf_counter()
    out = orjson.dumps(doc)
    times["serialize"] = time.perf_counter() - t

    t = time.perf_counter()
    compressed = zstandard.compress(out)
    times["compress"] = time.perf_counter() - t

    scores = [[a.get("score") for a in (r.get("analyses") or [])] for r in originals]
    return times, scores, order, failures, compressed, len(out), len(data)


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("message", type=Path)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--out", type=Path)
    p.add_argument("--save-message", type=Path)
    a = p.parse_args(argv)

    logger = logging.getLogger("aragorn_score_bench")
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    logger.setLevel(logging.WARNING)

    t = time.perf_counter()
    raw = a.message.read_bytes()
    read_ms = (time.perf_counter() - t) * 1e3

    runs = []
    for _ in range(max(1, a.repeat)):
        times, scores, order, failures, compressed, out_len, in_len = run_once(
            raw, logger
        )
        runs.append({k: v * 1e3 for k, v in times.items()})

    def stat(values):
        values = sorted(values)
        return {
            "min": values[0],
            "median": statistics.median_low(values),
            "runs": values,
        }

    timings = {"read": stat([read_ms])}
    for phase in ("decompress", "parse", "score", "serialize", "compress"):
        timings[phase] = stat([r[phase] for r in runs])
    timings["total"] = stat([sum(r.values()) for r in runs])

    if a.save_message:
        a.save_message.write_bytes(
            compressed
            if a.save_message.suffix == ".zst"
            else orjson.dumps(
                orjson.loads(zstandard.decompress(compressed)),
                option=orjson.OPT_INDENT_2,
            )
        )

    result = {
        "impl": "python",
        "input": str(a.message),
        "repeat": len(runs),
        "message_bytes": in_len,
        "output_bytes": out_len,
        "n_results": len(scores),
        "n_analyses": sum(len(s) for s in scores),
        "failed_answers": len(failures),
        "failures": failures[:50],
        "timings_ms": timings,
        "scores": scores,
        "order": order,
    }
    print(
        f"python: {len(scores)} results, {result['n_analyses']} analyses, {len(failures)} failed"
        f" | score {timings['score']['min']:.1f} ms (min of {len(runs)})"
        f" | total {timings['total']['min']:.1f} ms",
        file=sys.stderr,
    )
    if a.out:
        a.out.write_bytes(orjson.dumps(result))
    else:
        print(orjson.dumps(timings).decode())


if __name__ == "__main__":
    main()
