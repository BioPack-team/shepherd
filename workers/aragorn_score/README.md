# aragorn.score

Scores and ranks ARAGORN's answers: each analysis gets the effective
resistance between its query nodes, computed over the knowledge-graph edges
(and their support graphs) behind it, mapped to a score in [0, 1]. Results are
saved in ascending order of their best analysis's score.

There are two implementations of the scorer:

- **Python**: the `Ranker` in `worker.py`. This is the reference and the
  default.
- **Rust**: `rust/`, built into the worker image as the `aragorn_score_rs`
  Python module. It's about 10x faster at scoring and agrees with the Python
  scorer to about 1e-13. It never decodes the message in Python: the stored
  blob goes in, and the scored blob comes out.

## Choosing the scorer

`ARAGORN_SCORER` on the worker picks one:

| value | what runs | what's saved |
|---|---|---|
| `python` (default) | the Python `Ranker` | Python's scores |
| `rust` | the Rust scorer; Python for any message it can't score | Rust's scores |
| `shadow` | Python, then Rust on the same input | Python's scores |

In `rust` mode, a message the Rust scorer can't score at all is scored by
Python instead, and the worker logs why. An answer it can't score gets 0, as
in Python. The query's logs say which scorer ran ("Score complete (Rust
scorer, 35 ms)").

`shadow` is for building confidence before switching. Responses stay
Python's. For every query the worker also runs the Rust scorer and logs one
line comparing the two: whether they agree, the largest score difference,
whether the ranked order or the top 10 changed, and each one's time:

    aragorn.score shadow {"response_id": "...", "agree": true, "max_rel_diff": 3.8e-14, "results_moved": 0, "top10_overlap": 10, "python_rank_ms": 228.3, "rust_score_ms": 21.6, ...}

The line goes to the worker's own log (warning level when they disagree), not
to the query's logs. `benchmarks/aragorn_score/shadow_report.py` summarises a
run of them:

    docker logs aragorn_score 2>&1 | python benchmarks/aragorn_score/shadow_report.py

Shadow mode adds the Rust run, roughly a tenth of the Python time, to each
task.

To keep the evidence of a disagreement, set `ARAGORN_SHADOW_DUMP_DIR` (in
compose, `/app/logs/...` is `./logs/...` on the host). The worker then saves
the stored input of each query the two disagree on there, as
`<response_id>.json.zst`, at most `ARAGORN_SHADOW_DUMP_MAX` files (default
20) per worker process. The shadow line names the file (`saved_input`).
Reproduce it with:

    benchmarks/aragorn_score/run.sh logs/aragorn_shadow/<response_id>.json.zst

## Support-graph cycles

The scorer brings everything reachable through an analysis's support graphs
into that analysis's graph. Support graphs can form cycles: edge A's support
holds B, and B's holds A. Every edge in a cycle then brings in the whole
cycle's support. Both scorers find cycles with Tarjan's
strongly-connected-components algorithm, so the result doesn't depend on the
order the edges are walked in.

An earlier version of this change cached closures inside a cycle before they
were complete. That made scores depend on walk order, so Python and Rust
disagreed on real messages. Before that version, Python raised
`RecursionError` on a cycle and scored the answer 0.

Large cycles make large analysis graphs, and scoring time grows with roughly
the cube of their size. The shadow line reports `support_cycles` and
`largest_support_cycle` for each query. `benchmarks/aragorn_score/support_cycles.py
MESSAGE` reports a message's cycles and how many support edges each analysis
gets.

An unknown value, or `rust`/`shadow` without the module installed (a plain
local checkout, for example), logs a warning and runs Python. The worker's
startup log says which scorer it is using.

`ARAGORN_SCORER_SOLVER` picks the Rust scorer's linear solve. `cholesky` is
the default and the fast one. `svd` computes it the way numpy's `lstsq`
does. See `benchmarks/aragorn_score/README.md`.

### With docker compose

Set it in `.env` and recreate the worker:

    echo ARAGORN_SCORER=shadow >> .env
    echo ARAGORN_SHADOW_DUMP_DIR=/app/logs/aragorn_shadow >> .env   # optional
    docker compose up -d --build aragorn_score

### On Kubernetes

Set `ARAGORN_SCORER` in the aragorn-score Deployment's environment.

## Building the Rust scorer

The worker's Dockerfile builds it in a first stage
(`ghcr.io/pyo3/maturin`, manylinux2014) and installs the wheel. The Rust
version is pinned in `rust/rust-toolchain.toml`. To build it outside Docker:

    pip install maturin
    maturin build --release -m workers/aragorn_score/rust/Cargo.toml --out dist
    pip install dist/*.whl

Tests that need the module (`tests/unit/aragorn/test_aragorn_scorer_modes.py`)
skip without it. The "Rust scorer parity" CI job builds the module and runs
them.

The benchmark runner and the Python-vs-Rust comparison tools are in
`benchmarks/aragorn_score/`.
