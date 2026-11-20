"""Report the support-graph cycles in a message, and how big they make the
answers' support closures.

    python support_cycles.py MESSAGE    # .json or .json.zst (e.g. a shadow dump)

A support-graph cycle is a set of edges whose support graphs reach each other
(A's support holds B, B's holds A). The scorer takes everything reachable
through support graphs into each analysis's graph, so every edge in a cycle
brings in the whole cycle's support. A large cycle makes large analysis
graphs, and scoring time grows with roughly the cube of their size.
"""

import logging
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import orjson  # noqa: E402

from shepherd_utils.db import decompress_frame  # noqa: E402
from shepherd_utils.trapi import binding_ids  # noqa: E402
from workers.aragorn_score.worker import Ranker  # noqa: E402


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    raw = Path(sys.argv[1]).read_bytes()
    doc = orjson.loads(decompress_frame(raw) if raw[:4] == b"\x28\xb5\x2f\xfd" else raw)
    message = doc["message"] if isinstance(doc.get("message"), dict) else doc
    ranker = Ranker(message, logging.getLogger("support_cycles"))
    kg = message.get("knowledge_graph", {}).get("edges", {})

    # Closure of every analysis's bound edges (what it brings into its graph).
    sizes = []
    for result in message.get("results") or []:
        for analysis in result.get("analyses") or []:
            edges = set()
            for eb in (analysis.get("edge_bindings") or {}).values():
                for e in binding_ids(eb):
                    edges.update(ranker._support_closure(e)[1])
            sizes.append(len(edges))
    for e in kg:  # every cycle, not only those bound
        ranker._support_closure(e)

    cycles = sorted(ranker.support_cycles, reverse=True)
    print(f"{len(kg)} KG edges, {len(sizes)} analyses")
    print(f"support-graph cycles: {len(cycles)}")
    if cycles:
        print(f"  edges in cycles: {sum(cycles)}; largest cycles: {cycles[:10]}")
    if sizes:
        print(
            "support-closure edges per analysis: median "
            f"{statistics.median(sizes):.0f}, max {max(sizes)}"
        )


if __name__ == "__main__":
    main()
