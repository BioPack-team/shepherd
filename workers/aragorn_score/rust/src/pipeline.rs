//! The scoring pipeline on a stored message: zstd decompress -> parse ->
//! score -> write the scored message -> zstd compress. Used by the worker
//! (through the Python module, `python.rs`) and by the benchmark runner.

use std::time::Instant;

use serde_json::Value;

use crate::solve::{SolveStats, Solver};
use crate::typed_input::{self, Backend};
use crate::{typed, value_ranker};

pub type R<T> = Result<T, String>;

pub const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Decompress a zstd frame into one allocation of the size its header
/// declares (as Shepherd's `decompress_frame` does); stream it when the
/// header doesn't say.
pub fn decompress(raw: &[u8]) -> R<Vec<u8>> {
    match zstd::zstd_safe::get_frame_content_size(raw) {
        Ok(Some(size)) => zstd::bulk::decompress(raw, size as usize),
        _ => zstd::decode_all(raw),
    }
    .map_err(|e| format!("zstd decompress: {e}"))
}

pub fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

/// One pipeline run's products (decompress and compress are timed outside).
pub struct Run {
    pub parse_ms: f64,
    pub score_ms: f64,
    pub serialize_ms: f64,
    pub out_bytes: Vec<u8>,
    pub scores: Vec<Vec<Value>>,
    pub order: Vec<usize>,
    pub failures: Vec<(usize, String)>,
    pub stats: SolveStats,
    pub note: Option<String>,
}

pub fn run_value(bytes: &[u8], solver: Solver) -> R<Run> {
    let t = Instant::now();
    let mut doc: Value = serde_json::from_slice(bytes).map_err(|e| format!("parse JSON: {e}"))?;
    let parse_ms = ms(t);

    let t = Instant::now();
    // Accept a stored TRAPI Response ({"message": ...}) or a bare Message.
    let message = if doc.get("message").map_or(false, Value::is_object) {
        doc.get_mut("message").unwrap()
    } else {
        &mut doc
    };
    let message = message.as_object_mut().ok_or("the message is not an object")?;
    let has_results = matches!(message.get("results"), Some(Value::Array(_)));
    let mut results = match message.get_mut("results") {
        Some(Value::Array(r)) => std::mem::take(r),
        _ => vec![],
    };
    let (order, failures, stats) = if has_results {
        let mut ranker = value_ranker::Ranker::new(message, solver);
        let (order, failures) = ranker.rank(&mut results);
        (order, failures, ranker.stats)
    } else {
        (vec![], vec![], SolveStats::default())
    };
    let scores: Vec<Vec<Value>> = results
        .iter()
        .map(|r| {
            r.get("analyses")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(|x| x.get("score").cloned().unwrap_or(Value::Null)).collect())
                .unwrap_or_default()
        })
        .collect();
    // Put the results back in ranked order, as the worker saves them.
    let mut slots: Vec<Option<Value>> = results.into_iter().map(Some).collect();
    let ranked: Vec<Value> = order.iter().map(|&i| slots[i].take().unwrap()).collect();
    if has_results {
        message.insert("results".into(), Value::Array(ranked));
    }
    let score_ms = ms(t);

    let t = Instant::now();
    let out_bytes = serde_json::to_vec(&doc).map_err(|e| format!("serialise: {e}"))?;
    let serialize_ms = ms(t);
    Ok(Run {
        parse_ms,
        score_ms,
        serialize_ms,
        out_bytes,
        scores,
        order,
        failures,
        stats,
        note: None,
    })
}

/// Err: the typed parse couldn't represent this message.
pub fn run_typed(bytes: &[u8], solver: Solver, json: Backend) -> R<Run> {
    let t = Instant::now();
    let parsed = typed_input::parse(bytes, json)?;
    let parse_ms = ms(t);

    let t = Instant::now();
    let model = typed::Model::build(&parsed);
    let n = model.n_answers();
    let ranked = if parsed.results.is_some() {
        typed::Scorer::new(&model, solver).rank()
    } else {
        typed::Ranked {
            finals: vec![],
            order: vec![],
            failures: vec![],
            stats: SolveStats::default(),
        }
    };
    let score_ms = ms(t);
    debug_assert_eq!(n, ranked.finals.len());

    let t = Instant::now();
    let out_bytes = typed::splice(bytes, &parsed, &ranked);
    let serialize_ms = ms(t);

    let scores = ranked
        .finals
        .iter()
        .map(|f| f.iter().map(|x| x.to_value()).collect())
        .collect();
    Ok(Run {
        parse_ms,
        score_ms,
        serialize_ms,
        out_bytes,
        scores,
        order: ranked.order,
        failures: ranked.failures,
        stats: ranked.stats,
        note: None,
    })
}


/// The typed path, falling back to the value path for a message the typed
/// parse can't represent (`note` says so).
pub fn run(bytes: &[u8], solver: Solver, json: Backend) -> R<Run> {
    match run_typed(bytes, solver, json) {
        Ok(r) => Ok(r),
        Err(e) => {
            let mut r = run_value(bytes, solver)?;
            r.note = Some(format!("typed parse failed ({e}); used the value path"));
            Ok(r)
        }
    }
}

/// A scored stored message, and what happened.
pub struct Scored {
    /// The scored message, zstd-compressed (level 3, as Shepherd stores them).
    pub blob: Vec<u8>,
    pub run: Run,
    pub decompress_ms: f64,
    pub compress_ms: f64,
}

/// Score a stored message blob (zstd-compressed JSON, as Shepherd keeps it in
/// Redis; plain JSON is accepted too) and return it scored and ranked, in
/// the same form.
pub fn score_blob(blob: &[u8], solver: Solver, json: Backend) -> R<Scored> {
    let t = Instant::now();
    let decompressed;
    let bytes: &[u8] = if blob.starts_with(&ZSTD_MAGIC) {
        decompressed = decompress(blob)?;
        &decompressed
    } else {
        blob
    };
    let decompress_ms = ms(t);

    let run = run(bytes, solver, json)?;

    let t = Instant::now();
    let blob = zstd::bulk::compress(&run.out_bytes, 3).map_err(|e| format!("zstd compress: {e}"))?;
    let compress_ms = ms(t);
    Ok(Scored {
        blob,
        run,
        decompress_ms,
        compress_ms,
    })
}
