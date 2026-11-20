# aragorn.score: Python vs Rust benchmark

Tools for comparing the ARAGORN scorer's Python and Rust implementations on
the same message. The worker itself can run either one, or both side by side
on real queries (`ARAGORN_SCORER`, see `workers/aragorn_score/README.md`).

- **`python_scorer.py`** runs the real worker `Ranker`, unchanged.
- **`workers/aragorn_score/rust/`** holds the Rust scorer: the library the
  worker runs (as the `aragorn_score_rs` Python module) and this benchmark's
  runner, in two variants (see "The Rust variants" below).
- **`compare.py`** compares a baseline run with one or more others: timings,
  scores, rankings, and the scored messages themselves.
- **`gen_message.py`** generates synthetic creative-mode messages.
- **`dump_message.py`** pulls a real message out of Shepherd's Redis.
- **`run.sh`** builds the Rust runner, runs Python and each Rust variant on
  the same message, and compares them all.
- **`shadow_report.py`** summarises the comparisons the worker logs in shadow
  mode, on real queries (see `workers/aragorn_score/README.md`).
- **`support_cycles.py`** reports a message's support-graph cycles, and how
  many support edges they give each analysis.

## Running it

```sh
cd benchmarks/aragorn_score
./run.sh                                     # synthetic: 2000 results
./run.sh path/to/message.json.zst            # your own message
REPEAT=10 GEN_ARGS="--results 6000 --genes 1000" ./run.sh
GEN_ARGS="--results 300 --quirks --nested" ./run.sh   # parity on odd inputs
GEN_ARGS="--results 300 --cycles" ./run.sh            # support-graph cycles
VARIANTS="typed:cholesky" ./run.sh           # only the fastest Rust variant
VARIANTS="typed:cholesky:sonic" ./run.sh     # ... with sonic-rs as the parser
CHECK_MESSAGES=0 ./run.sh                    # skip the scored-message check
```

It needs Python with the repository's dependencies, which the worker
imports, and a Rust toolchain (`cargo`). Outputs go to `out/`:
`python.json`, `rust-<parse>-<solver>-<json>.json`, the scored messages, and
the generated message.

To benchmark a real query, grab its response from Redis where the worker's
settings are in the environment, then point `run.sh` at it:

```sh
python dump_message.py <response_id> out/real.json.zst
./run.sh out/real.json.zst
```

Each runner can also run on its own:

```sh
python python_scorer.py MESSAGE --repeat 5 --out out/python.json
R=../../workers/aragorn_score/rust/target/release/aragorn_score_rs
$R MESSAGE --repeat 5 \
    --parse typed --solver cholesky --json serde_json --out out/rust.json
$R MESSAGE --bench-parse --repeat 15  # parsers only
python compare.py out/python.json out/rust.json [more.json ...] --tol 1e-6
```

`--save-message FILE` on either one writes the scored, ranked message (zstd
if the name ends in `.zst`).

## The Rust variants

Three switches choose a variant. `run.sh` runs `value:svd`, `typed:svd`,
`typed:cholesky` and `typed:cholesky:sonic` by default
(`VARIANTS=PARSE:SOLVER[:JSON] ...`).

Source paths below are under `workers/aragorn_score/rust/`.

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

**`--json serde_json|sonic`** picks the JSON parser that drives the typed
visitors (serde_json by default). See "JSON parsers" below for why they come
out even.

The build uses mimalloc as its allocator (`--no-default-features` for the
system one); `run.sh` also enables `bench-parsers` (simd-json, for
`--bench-parse`). On the default message it made the whole pipeline about 20%
faster, scoring included.

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

Expect differences around 1e-13, and up to about 1e-9 when an analysis mixes
edges at the weight cap (admittance ~1e7) with ordinary ones (~0.1): the
matrices are then ill-conditioned, so the low digits depend on summation
order. The default tolerance (`--tol`, `TOL`) is 1e-6. Two answers whose
scores tie to within that can swap places. Anything bigger is a bug.

Before the weight cap applied below 1 (see `MAX_EDGE_WEIGHT` in
`worker.py`), edges with 40–64 publications from some sources had
admittances up to ~1e15. Those scores were numerical noise, and the two
scorers disagreed on real messages by up to 12%. `gen_message.py`'s
messages don't contain such edges; the scorer tests add some.

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

Minimum of 5 runs, in ms. Speedups are against Python, measured in the same
run (Python's own times move by about 10% between runs here).

| message | Python | value + SVD | typed + SVD | typed + Cholesky | same, sonic-rs |
|---|---|---|---|---|---|
| 2000 results with omnicorp, 24 MB | 1252 | 417 (3.0x) | 172 (7.3x) | 129 (9.7x) | 127 (9.9x) |
| 2000 results, no omnicorp, 7 MB | 527 | 147 (3.6x) | 72 (7.3x) | 45 (11.7x) | 44 (12.0x) |
| 2000 results, nested support, 26 MB | 2053 | 808 (2.5x) | 514 (4.0x) | 191 (10.8x) | 179 (11.5x) |
| 500 results, up to 40 paths, 39 MB | 2219 | 618 (3.6x) | 272 (8.2x) | 210 (10.6x) | 203 (11.0x) |
| 6000 results, 1000 genes, 71 MB | 5232 | 1304 (4.0x) | 518 (10.1x) | 414 (12.6x) | 381 (13.7x) |
| quirks + nested, 314 results | 273 | 114 (2.4x) | 74 (3.7x) | 28 (9.7x) | 27 (10.0x) |

The same runs, phase by phase for the default 2000-result message:

| phase | Python | value + SVD | typed + SVD | typed + Cholesky |
|---|---|---|---|---|
| decompress | 7.7 | 8.3 | 8.1 | 7.8 |
| parse | 141 | 126 | 38 | 36 (3.9x) |
| score | 1040 | 210 | 96 | 57 (18.2x) |
| serialize | 27 | 43 | 4.9 | 3.8 (7.0x) |
| compress | 23 | 24 | 24 | 23 |
| total | 1252 | 417 | 172 | 129 (9.7x) |

Every variant agrees with Python within 1e-9 on all of these; the largest
relative difference was about 1e-13. The saved messages match too.

What the tables show:

- **Typed parsing** is about 3.5x faster than parsing into a `Value`. It also
  makes scoring about 2x faster, because scoring works on integers instead of
  JSON lookups. Splicing makes writing the message 7–10x cheaper than
  re-serialising it.
- **The Cholesky solve matters most for large support graphs.** In the nested
  case (median 29 r-nodes) it cuts scoring from 420 to 107 ms. It never fell
  back on these messages; only the self-loop quirks need the SVD.
- **What's left**: in the best variant, scoring is about 45% of the total,
  parsing about 28%, and zstd (decompress plus compress, the same library as
  Python's) about 24%.

## JSON parsers

`--bench-parse` times the parsers alone on the same decompressed bytes. Each
value is dropped after the clock stops: freeing a big tree can cost as much
as building it, and the pipeline frees outside its timed phases too.

Default message, 23.8 MB, min of 15:

| parser | time | throughput |
|---|---|---|
| serde_json `Value` (whole document) | 112 ms | 213 MB/s |
| simd-json `BorrowedValue` (whole document) | 58 ms | 409 MB/s |
| sonic-rs `Value` (whole document) | 17 ms | 1.4 GB/s |
| typed visitors, serde_json | 32 ms | 747 MB/s |
| typed visitors, sonic-rs | 30 ms | 789 MB/s |

The 71 MB message gives the same ranking (serde_json `Value` 382 ms,
simd-json 214, sonic-rs `Value` 69, typed visitors 104–106).

**sonic-rs parses a whole document far faster than anything else, but that
doesn't carry over to scoring.** Through serde, where the typed visitors
need it, sonic-rs ties with serde_json: the time goes into the visitor calls
and the structs they build, not into tokenising JSON.

I also tried using sonic-rs's own document parser for the knowledge graph,
converting the result into the typed structs. It took 63–66 ms, twice as
slow. Skipping over the 22 MB knowledge graph with sonic-rs's lazy iterator
costs about 20 ms (as much as parsing it), and the conversion another 16 ms.
Even parsing the whole file at once and converting (about 17 + 16 ms) can't
beat the visitors' single pass, so I removed that prototype.

**simd-json** has no borrowed raw-value type, and its serde mode rewrites
the input buffer in place, so it can't support the splicing. As a whole-
document parser it's 3x slower than sonic-rs here anyway.

What did make the typed parse faster (55 → about 32 ms):

- **One pass.** The top level and the message are parsed together. Before,
  the code scanned the whole document once just to find `"message"`.
- **Parse from `&str`.** UTF-8 is validated once up front (simdutf8), then
  parsing runs from the string. serde_json's `from_slice` re-validates every
  string it reads.
- **Borrowed keys.** Object keys are borrowed from the input. serde's
  `Cow<str>` allocates a `String` for every key.
- **mimalloc.** About 10% off the parse, and more off scoring.

Decompression also matches Python now (14 → 7 ms on the default message). It
allocates the size the zstd frame header declares, as Shepherd's
`decompress_frame` does, instead of growing a buffer.

## Headroom and caveats

- **Parsing is near its limit for this design.** The visitors spend their time
  building the structs the ranker needs. Going faster would mean building
  the model directly from a sonic-rs document, skipping the intermediate
  structs. The prototype above suggests the gain would be small, if any.
- **zstd is now about a quarter of the total.** Compression at level 3 takes
  about 23 ms for 24 MB in both languages. Level 1 or a faster codec would
  matter more than any further parser work, but that's a change to how
  Shepherd stores messages, not to the scorer.
- **The Cholesky trick works in Python too.** It's an algorithm change, not a
  language one. But numpy's `lstsq` was only about 10% of the Python worker's
  time (most of it is Python-level dict and loop work), so Python would gain
  much less from it than Rust does.
- **No reconstruction check.** Dropping it saves about 6% on SVD solves, but
  it's what keeps a silently failed decomposition from becoming a wrong
  score.
- **Synthetic data.** These are all synthetic messages. Real ARAGORN
  responses may have other shapes: more analyses per result, bigger
  attributes, other sources. Run `dump_message.py` and `run.sh` on a few
  large real responses before relying on these numbers.
