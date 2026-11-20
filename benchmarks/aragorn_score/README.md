# aragorn.score: Python vs Rust benchmark

A debugging aid for deciding whether to move the ARAGORN scorer
(`workers/aragorn_score/worker.py`) to Rust. Nothing here is used by the
workers.

- **`python_scorer.py`** runs the real worker `Ranker`, unchanged.
- **`rust/`** is a port of that ranker to Rust (`src/ranker.rs`) plus a
  runner (`src/main.rs`).
- **`compare.py`** compares the two runs' timings, scores and rankings.
- **`gen_message.py`** generates synthetic creative-mode messages.
- **`dump_message.py`** pulls a real message out of Shepherd's Redis.
- **`run.sh`** builds the Rust runner, runs both scorers on the same message and
  compares the results.

## Running it

```sh
cd benchmarks/aragorn_score
./run.sh                                     # synthetic: 2000 results
./run.sh path/to/message.json.zst            # your own message
REPEAT=10 GEN_ARGS="--results 6000 --genes 1000" ./run.sh
GEN_ARGS="--results 300 --quirks --nested" ./run.sh   # parity on odd inputs
```

It needs Python with the repository's dependencies, which the worker
imports, and a Rust toolchain (`cargo`). Outputs go to `out/`:
`python.json`, `rust.json`, and the generated message.

To benchmark a real query, grab its response from Redis where the worker's
settings are in the environment, then point `run.sh` at it:

```sh
python dump_message.py <response_id> out/real.json.zst
./run.sh out/real.json.zst
```

Either runner can also run on its own:

```sh
python python_scorer.py MESSAGE --repeat 5 --out out/python.json
rust/target/release/aragorn_score_rs MESSAGE --repeat 5 --out out/rust.json
python compare.py out/python.json out/rust.json --tol 1e-9
```

`--save-message FILE` on either one writes the scored, ranked message (zstd
if the name ends in `.zst`).

## What is timed

Each repeat runs the worker's whole pipeline from the stored bytes, phase by
phase:

| phase | Python | Rust |
|---|---|---|
| decompress | zstandard (`decompress_frame`) | zstd crate |
| parse | orjson | serde_json (`Value`, key order kept) |
| score | `Ranker.rank` | `ranker::Ranker::rank` |
| serialize | orjson | serde_json |
| compress | zstandard, level 3 | zstd crate, level 3 |

`compare.py` reports the minimum and the median for each phase. Both runners
are single-threaded, the same as one process-pool worker. The Redis round
trip isn't included; it's the same for both.

## Reading compare.py's output

- **Scores**: how many analysis scores are bit-identical, how many fall within
  `--tol` (relative), and the largest difference. It lists the worst offenders
  and every answer that failed to score, with each side's reason.
- **Ranking**: whether the final order is the same, the top-K overlap, Kendall's
  tau, and how many swapped neighbours are near-ties.
- **Exit status**: it exits 1 if any score is outside tolerance, if the two
  sides failed different answers, or if the result counts differ. Ranking swaps
  alone don't fail it.

Scores are not expected to be bit-identical, because the two sides take
different routes to the same numbers:

- The Laplacian's rows come out in a different order. Python builds them in
  `set` (hash) order; Rust uses insertion order.
- The maths runs on different libraries: `ln`, `exp` and row sums come from
  Rust's std, and the SVD from faer instead of numpy/LAPACK.

Expect differences around 1e-13 and the odd swap between two answers whose
scores tie to within that. Anything bigger is a bug in the port (see below).

## How faithful the port is

`rust/src/ranker.rs` follows the Python `Ranker` with the BLENDED profile,
which is the only one the worker uses. It covers:

- the attribute parsing in `get_edge_values`, including the odd cases:
  pipe- or comma-joined publication strings, p-values given as strings or
  lists, `evidence_count`, literature co-occurrence, affinity, unknown
  sources;
- the support-graph closure, including cycles;
- the r-graph, the Laplacian with zero-column pruning, the probes, and the
  `lstsq` cutoff (`rcond=None`);
- the NaN guard (a score of -1);
- how a failing answer is handled: the analysis that raised and every later
  one keep any score they had, else get 0;
- the ascending, stable sort.

Values Python would choke on in exotic ways are reported as an error for that
answer. Two examples: a list where an attribute name should be, or a
publications value that is a dict of length 1. In those rare cases Python
might raise a different exception, or none at all. `gen_message.py --quirks`
exercises the realistic odd inputs and failure paths, and both sides currently
agree on all of them.

### Lessons from building it

The comparison caught two real bugs in the first version of the port. Both
came from nalgebra: its SVD and its symmetric eigendecomposition can silently
return an unconverged decomposition on some of these Laplacians, which put
scores off by up to 30%.

The port now uses faer's SVD instead. It also checks that the decomposition
reconstructs L, so a bad one shows up as a failed answer rather than a wrong
score. Anyone building a production Rust scorer should keep a check like this.

`ARAGORN_SCORE_DEBUG_RESULT=<index>` makes the Rust runner print one
result's r-graph edges, admittances and Laplacian. This is how those bugs
were tracked down.

## Results so far (synthetic messages, this container)

Minimum of 5 runs, in ms (Python / Rust):

| message | parse | score | serialize | total |
|---|---|---|---|---|
| 2000 results with omnicorp, 24 MB | 138 / 115 | 1070 / 245 (4.4x) | 24 / 52 | 1282 / 452 (2.8x) |
| 2000 results, no omnicorp | 38 / 33 | 450 / 93 (4.8x) | 10 / 15 | 517 / 158 (3.3x) |
| 2000 results, nested support | 155 / 192 | 1456 / 542 (2.7x) | 27 / 56 | 1684 / 835 (2.0x) |
| 500 results, up to 40 paths | 224 / 192 | 1693 / 393 (4.3x) | 46 / 80 | 2020 / 720 (2.8x) |
| 6000 results, 1000 genes | 486 / 370 | 4289 / 801 (5.4x) | 90 / 160 | 4982 / 1448 (3.4x) |
| quirks + nested, 312 results | 20 / 20 | 216 / 86 (2.5x) | 5 / 9 | 251 / 126 (2.0x) |

All of these agree within 1e-9; the largest relative difference was about
1e-13.

What the table shows:

- **Overall**: scoring is 2.5–5.4x faster in Rust and the whole step 2–3.4x.
  The gap is smaller than people often expect from Rust.
- **Large support graphs**: the nested case has r-graphs with a median of 29
  nodes. There the time goes into the SVD, and numpy already runs that in
  compiled LAPACK, so Rust has little to gain.
- **JSON**: Rust's serde_json `Value` serialises about 2x slower than orjson
  (itself written in Rust) and parses only slightly faster.

## Headroom in the Rust port

This is a direct, untuned port. Known ways to make it faster:

- **Typed message structs.** Parsing into `#[derive(Deserialize)]` structs, or
  using simd-json or sonic-rs, would cut parse and serialise time. It would
  also remove the per-attribute map lookups and clones in scoring.
- **Grounded Cholesky solve.** For a connected graph, grounding one node and
  using a Cholesky solve gives the same effective resistance far more cheaply
  than an SVD, with a fallback for disconnected graphs. This is an algorithm
  change, so it would apply equally to the Python worker. It would change
  scores in the last bits.
- **No reconstruction check.** Dropping it saves about 6%, but see above for
  why it's there.
