# aragorn.score: Python vs Rust benchmark

A debugging aid for deciding whether to move the ARAGORN scorer
(`workers/aragorn_score/worker.py`) to Rust. Nothing here is used by the
workers.

- **`python_scorer.py`** runs the real worker `Ranker`, unchanged.
- **`rust/`** holds Rust ports of that ranker and a runner, in two variants
  (see "The Rust variants" below).
- **`compare.py`** compares a baseline run with one or more others: timings,
  scores, rankings, and the scored messages themselves.
- **`gen_message.py`** generates synthetic creative-mode messages.
- **`dump_message.py`** pulls a real message out of Shepherd's Redis.
- **`run.sh`** builds the Rust runner, runs Python and each Rust variant on
  the same message, and compares them all.

## Running it

```sh
cd benchmarks/aragorn_score
./run.sh                                     # synthetic: 2000 results
./run.sh path/to/message.json.zst            # your own message
REPEAT=10 GEN_ARGS="--results 6000 --genes 1000" ./run.sh
GEN_ARGS="--results 300 --quirks --nested" ./run.sh   # parity on odd inputs
VARIANTS="typed:cholesky" ./run.sh           # only the fastest Rust variant
CHECK_MESSAGES=0 ./run.sh                    # skip the scored-message check
```

It needs Python with the repository's dependencies, which the worker
imports, and a Rust toolchain (`cargo`). Outputs go to `out/`:
`python.json`, `rust-<parse>-<solver>.json`, the scored messages, and the
generated message.

To benchmark a real query, grab its response from Redis where the worker's
settings are in the environment, then point `run.sh` at it:

```sh
python dump_message.py <response_id> out/real.json.zst
./run.sh out/real.json.zst
```

Each runner can also run on its own:

```sh
python python_scorer.py MESSAGE --repeat 5 --out out/python.json
rust/target/release/aragorn_score_rs MESSAGE --repeat 5 \
    --parse typed --solver cholesky --out out/rust.json
python compare.py out/python.json out/rust.json [more.json ...] --tol 1e-9
```

`--save-message FILE` on either one writes the scored, ranked message (zstd
if the name ends in `.zst`).

## The Rust variants

Two independent switches choose a variant; `run.sh` runs `value:svd`,
`typed:svd` and `typed:cholesky` by default.

**`--parse value`** (`src/value_ranker.rs`) is the straight port. It parses
the whole message into `serde_json::Value`, scores by looking things up in
it, and serialises the whole message again.

**`--parse typed`** (the default; `src/typed_input.rs`, `src/typed.rs`)
changes how the message is read and written:

- Hand-written serde visitors read only the fields the ranker uses, into
  borrowed structs. Everything else is skipped without being allocated.
- A publications list is reduced to its length and first element, so
  thousands of PMIDs are counted, not built. Other attribute values stay as
  raw JSON until scoring looks at them.
- The parsed message becomes a compact model: node, edge and aux-graph ids
  interned to integers, each edge's admittances computed once. Scoring then
  works on integers.
- The scored message is written by splicing: the input bytes are copied,
  with the results array rewritten in ranked order and `"score"` inserted
  into each analysis. Nothing else is serialised again.
- Python raises on some odd shapes (a null where a list belongs, a node that
  isn't an object) only where the code reads them, and only for that answer.
  The model stores those cases and raises them at the same point. A shape the
  visitors can't represent at all falls back to the value path for the whole
  message, and the result file says so (`note`).

**`--solver svd`** (`src/solve.rs`) solves the probes' effective resistance
the way numpy's `lstsq(L, X, rcond=None)` does: faer's SVD, with the same
cutoff. It also checks that the SVD reconstructs L. nalgebra's SVD and
eigensolver, tried first, sometimes returned unconverged results without
any error.

**`--solver cholesky`** (the default) uses a cheaper exact method. When every
probe sits in one connected component of the graph, ground one node of that
component and Cholesky-factor the rest. The effective resistance this gives
equals `lstsq`'s answer in exact arithmetic. Anything else falls back to the
SVD: probes in different components, a self-loop (non-zero Laplacian row
sums), or a non-positive pivot. The result file counts the solves of each
kind.

The self-loop check is necessary, not just cautious. With it disabled, the
`--quirks` answer that binds one node to both query nodes scores 0.178
instead of Python's 0.119.

## What is timed

Each repeat runs the worker's whole pipeline from the stored bytes, phase by
phase:

| phase | Python | Rust, value | Rust, typed |
|---|---|---|---|
| decompress | zstandard | zstd crate | zstd crate |
| parse | orjson | serde_json `Value` | typed visitors |
| score | `Ranker.rank` | ranker on `Value` | build the model + rank |
| serialize | orjson | serde_json | splice into the input |
| compress | zstandard, level 3 | zstd crate, level 3 | zstd crate, level 3 |

`compare.py` reports the minimum of the runs for each phase; the result files
also hold the median and every run. All runners are single-threaded, the same
as one process-pool worker. The Redis round trip isn't included; it's the
same for every runner.

## Reading compare.py's output

The first file is the baseline; every other file is compared with it.

- **Timings**: one column per run, with speedups against the baseline, and
  how many solves took each path.
- **Scores**: how many analysis scores are bit-identical, how many fall within
  `--tol` (relative), and the largest difference. It lists the worst offenders
  and every answer that failed to score, with each side's reason.
- **Ranking**: whether the final order is the same, the top-K overlap, Kendall's
  tau, and how many swapped neighbours are near-ties.
- **Saved messages**: when both runs saved one, the two scored messages are
  compared field by field. Results are matched by content, scores within
  tolerance. This checks the typed path's splicing writes the same message
  Python does.
- **Exit status**: 1 if any score is outside tolerance, two runs failed
  different answers, result counts differ, or saved messages differ beyond
  their scores. Ranking swaps alone don't fail it.

Scores are not expected to be bit-identical, because the two sides take
different routes to the same numbers:

- The Laplacian's rows come out in a different order. Python builds them in
  `set` (hash) order; Rust uses insertion order.
- The maths runs on different libraries: `ln`, `exp` and row sums come from
  Rust's std, and the solve is faer's SVD or the Cholesky above.

Expect differences around 1e-13 and the odd swap between two answers whose
scores tie to within that. Anything bigger is a bug.

## How faithful the ports are

Both ports follow the Python `Ranker` with the BLENDED profile, which is the
only one the worker uses. They cover:

- the attribute parsing in `get_edge_values`, including the odd cases:
  pipe- or comma-joined publication strings, p-values given as strings or
  lists, `evidence_count`, literature co-occurrence, affinity, unknown
  sources;
- the support-graph closure, including cycles;
- the r-graph, the Laplacian with zero-column pruning, the probes, and the
  `lstsq` cutoff;
- the NaN guard (a score of -1);
- how a failing answer is handled: the analysis that raised and every later
  one keep any score they already had, else get 0;
- the ascending, stable sort.

Values Python would choke on in exotic ways are reported as an error for that
answer. Two examples: a list where an attribute name should be, or a
publications value that is a dict of length 1. In those rare cases Python
might raise a different exception, or none at all.

`gen_message.py --quirks` exercises the realistic odd inputs, the failure
paths and the self-loops, and every variant currently agrees with Python on
all of them. I also checked by hand that already-scored analyses behave as in
Python: the score is replaced when the answer scores and kept when it fails.

`ARAGORN_SCORE_DEBUG_RESULT=<index>` makes either Rust variant print one
result's r-graph edges, admittances and Laplacian. This is how the nalgebra
problems were tracked down.

## Results so far (synthetic messages, this container)

Minimum of 5 runs, in ms. Speedups are against Python.

| message | Python total | value + SVD | typed + SVD | typed + Cholesky |
|---|---|---|---|---|
| 2000 results with omnicorp, 24 MB | 1215 | 422 (2.9x) | 199 (6.1x) | 171 (7.1x) |
| 2000 results, no omnicorp, 7 MB | 502 | 161 (3.1x) | 86 (5.9x) | 62 (8.1x) |
| 2000 results, nested support, 26 MB | 1736 | 843 (2.1x) | 586 (3.0x) | 240 (7.2x) |
| 500 results, up to 40 paths, 39 MB | 1969 | 701 (2.8x) | 338 (5.8x) | 254 (7.8x) |
| 6000 results, 1000 genes, 71 MB | 4895 | 1436 (3.4x) | 643 (7.6x) | 571 (8.6x) |
| quirks + nested, 312 results | 234 | 123 (1.9x) | 79 (3.0x) | 33 (7.2x) |

The same runs, phase by phase for the default 2000-result message:

| phase | Python | value + SVD | typed + SVD | typed + Cholesky |
|---|---|---|---|---|
| decompress | 7 | 10 | 18 | 20 |
| parse | 130 | 115 | 55 (2.3x) | 56 (2.3x) |
| score | 1033 | 220 (4.7x) | 95 (10.8x) | 61 (16.8x) |
| serialize | 24 | 52 | 4 (5.7x) | 3 (6.8x) |
| compress | 21 | 21 | 22 | 21 |

Every variant agrees with Python within 1e-9 on all of these; the largest
relative difference was about 1e-13. The saved messages match too.

What the tables show:

- **Typed parsing roughly halves parse time** and cuts scoring time about 2x
  on top of the value port, because scoring works on integers instead of
  JSON lookups. Splicing makes writing the message 6–8x cheaper than
  re-serialising it.
- **The Cholesky solve matters most for large support graphs.** In the nested
  case (median 29 r-nodes) it cuts scoring from 399 to 113 ms. It never fell
  back on these messages; only the self-loop quirks need the SVD.
- **What's left** is mostly parsing (about a third of the typed + Cholesky
  total) and zstd. In the typed variants, decompression runs about 2x slower
  than in the value variant. The code is identical, so I suspect allocator or
  page-fault effects; I haven't looked into it.

## Headroom and caveats

- **Faster parsing.** A SIMD JSON parser (simd-json, sonic-rs) could cut the
  parse phase further; it's now the biggest piece.
- **The Cholesky trick works in Python too.** It's an algorithm change, not a
  language one. But numpy's `lstsq` was only about 10% of the Python worker's
  time (most of it is the Python-level dict and loop work), so Python would
  gain much less from it than Rust does.
- **No reconstruction check.** Dropping it saves about 6% on SVD solves, but
  it's what keeps a silently failed decomposition from becoming a wrong
  score.
- **Synthetic data.** These are all synthetic messages. Real ARAGORN
  responses may have other shapes: more analyses per result, bigger
  attributes, other sources. Run `dump_message.py` and `run.sh` on a few
  large real responses before relying on these numbers.
