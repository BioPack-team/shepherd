//! The `aragorn_score_rs` Python module: `score_blob`, for the worker.

use std::panic::{catch_unwind, AssertUnwindSafe};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use serde_json::Value;

use crate::pipeline::{self, Scored};
use crate::solve::Solver;
use crate::typed_input::Backend;

fn py_score<'py>(py: Python<'py>, v: &Value) -> PyResult<PyObject> {
    Ok(match v {
        Value::Number(n) if n.is_i64() || n.is_u64() => n.as_i64().unwrap_or(0).into_pyobject(py)?.into_any().unbind(),
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN).into_pyobject(py)?.into_any().unbind(),
        _ => py.None(),
    })
}

/// score_blob(blob, solver="cholesky", json="serde_json") -> (bytes, dict)
///
/// Score a stored message (zstd-compressed JSON, as Shepherd keeps it in
/// Redis) and return it scored and ranked, compressed the same way, with a
/// summary: n_results, n_analyses, failures ([result index, reason] for each
/// answer that could not be scored and got 0), scores (each result's analysis
/// scores, in the input's result order), order (input indices, ranked),
/// solves, note (set when the fast parser fell back), timings_ms.
///
/// The GIL is released while scoring. Raises RuntimeError if the message
/// can't be scored at all (it isn't JSON, say) or the scorer panics.
#[pyfunction]
#[pyo3(signature = (blob, solver = "cholesky", json = "serde_json"))]
fn score_blob<'py>(
    py: Python<'py>,
    blob: &[u8],
    solver: &str,
    json: &str,
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyDict>)> {
    let solver = Solver::parse(solver)
        .ok_or_else(|| PyValueError::new_err(format!("unknown solver {solver:?}")))?;
    let json = Backend::parse(json)
        .ok_or_else(|| PyValueError::new_err(format!("unknown json backend {json:?}")))?;

    let result = py.allow_threads(|| {
        catch_unwind(AssertUnwindSafe(|| pipeline::score_blob(blob, solver, json)))
    });
    let Scored {
        blob: out,
        run,
        decompress_ms,
        compress_ms,
    } = match result {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(PyRuntimeError::new_err(e)),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown panic".into());
            return Err(PyRuntimeError::new_err(format!("the Rust scorer panicked: {msg}")));
        }
    };

    let info = PyDict::new(py);
    info.set_item("n_results", run.scores.len())?;
    info.set_item("n_analyses", run.scores.iter().map(Vec::len).sum::<usize>())?;
    let failures = PyList::empty(py);
    for (i, e) in &run.failures {
        failures.append((i, e))?;
    }
    info.set_item("failures", failures)?;
    let scores = PyList::empty(py);
    for result in &run.scores {
        let row = PyList::empty(py);
        for v in result {
            row.append(py_score(py, v)?)?;
        }
        scores.append(row)?;
    }
    info.set_item("scores", scores)?;
    info.set_item("order", run.order.clone())?;
    let solves = PyDict::new(py);
    solves.set_item("cholesky", run.stats.cholesky)?;
    solves.set_item("svd", run.stats.svd)?;
    solves.set_item("cholesky_fallbacks", run.stats.fallbacks)?;
    info.set_item("solves", solves)?;
    info.set_item("note", run.note.clone())?;
    let timings = PyDict::new(py);
    timings.set_item("decompress", decompress_ms)?;
    timings.set_item("parse", run.parse_ms)?;
    timings.set_item("score", run.score_ms)?;
    timings.set_item("serialize", run.serialize_ms)?;
    timings.set_item("compress", compress_ms)?;
    timings.set_item(
        "total",
        decompress_ms + run.parse_ms + run.score_ms + run.serialize_ms + compress_ms,
    )?;
    info.set_item("timings_ms", timings)?;
    Ok((PyBytes::new(py, &out), info))
}

#[pymodule]
fn aragorn_score_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(score_blob, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
