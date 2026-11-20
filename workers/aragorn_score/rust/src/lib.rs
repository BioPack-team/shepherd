//! The ARAGORN scorer (`aragorn.score`) in Rust: a port of the `Ranker` in
//! `workers/aragorn_score/worker.py`.
//!
//! * `pipeline` -- a stored message in, the scored and ranked message out
//!   (`score_blob`); what the worker calls.
//! * `typed_input`, `typed` -- the fast path: typed borrowed parsing, an
//!   interned model, scores spliced into the input bytes.
//! * `value_ranker` -- the straight port on `serde_json::Value`; the fallback
//!   for messages the typed parse can't represent.
//! * `solve` -- the effective-resistance solve (Cholesky, or SVD as numpy's
//!   `lstsq` does it).
//! * `python` (feature `python`) -- the `aragorn_score_rs` Python module.
//!
//! `src/main.rs` is the benchmark runner (benchmarks/aragorn_score).

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod pipeline;
pub mod solve;
pub mod typed;
pub mod typed_input;
pub mod value_ranker;

#[cfg(feature = "python")]
mod python;
