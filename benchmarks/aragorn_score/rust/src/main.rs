//! Benchmark runner for the Rust ports of the aragorn.score ranker.
//!
//!     aragorn_score_rs <message.json|message.json.zst> [--repeat N]
//!         [--parse typed|value] [--solver cholesky|svd] [--json serde_json|sonic]
//!         [--out result.json] [--save-message scored.json.zst] [--bench-parse]
//!
//! Each repeat runs the full worker pipeline on the raw bytes, timed per
//! phase: zstd decompress -> parse -> score -> write the scored message ->
//! zstd compress. `--parse value` is the straight port on serde_json::Value
//! (value_ranker.rs); `--parse typed` (the default) parses into borrowed
//! structs and splices scores into the input (typed.rs). `--solver` picks
//! the effective-resistance solve (solve.rs), `--json` the parser driving
//! the typed visitors. The result file has the same shape as
//! `python_scorer.py`'s, for `compare.py`. `--bench-parse` times the JSON
//! parsers alone (see README).

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod solve;
mod typed;
mod typed_input;
mod value_ranker;

use std::time::Instant;

use serde_json::{json, Value};
use solve::{SolveStats, Solver};
use typed_input::Backend;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Decompress a zstd frame into one allocation of the size its header
/// declares (as Shepherd's `decompress_frame` does); stream it when the
/// header doesn't say.
fn decompress(raw: &[u8]) -> Vec<u8> {
    match zstd::zstd_safe::get_frame_content_size(raw) {
        Ok(Some(size)) => zstd::bulk::decompress(raw, size as usize).expect("zstd decompress"),
        _ => zstd::decode_all(raw).expect("zstd decompress"),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Parse {
    Value,
    Typed,
}

struct Args {
    input: String,
    repeat: usize,
    parse: Parse,
    solver: Solver,
    json: Backend,
    out: Option<String>,
    save_message: Option<String>,
    bench_parse: bool,
}

const USAGE: &str = "usage: aragorn_score_rs MESSAGE [--repeat N] [--parse typed|value] \
                     [--solver cholesky|svd] [--json sonic|serde_json] [--out RESULT.json] \
                     [--save-message OUT] [--bench-parse]";

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut args = Args {
        input: String::new(),
        repeat: 1,
        parse: Parse::Typed,
        solver: Solver::Cholesky,
        json: Backend::SerdeJson,
        out: None,
        save_message: None,
        bench_parse: false,
    };
    while let Some(a) = it.next() {
        match a.as_str() {
            "--repeat" => args.repeat = it.next().and_then(|v| v.parse().ok()).expect(USAGE),
            "--parse" => {
                args.parse = match it.next().as_deref() {
                    Some("typed") => Parse::Typed,
                    Some("value") => Parse::Value,
                    _ => panic!("{USAGE}"),
                }
            }
            "--solver" => args.solver = it.next().as_deref().and_then(Solver::parse).expect(USAGE),
            "--json" => args.json = it.next().as_deref().and_then(Backend::parse).expect(USAGE),
            "--out" => args.out = it.next(),
            "--save-message" => args.save_message = it.next(),
            "--bench-parse" => args.bench_parse = true,
            "-h" | "--help" => {
                eprintln!("{USAGE}");
                std::process::exit(0);
            }
            _ if args.input.is_empty() => args.input = a,
            _ => panic!("unexpected argument {a}\n{USAGE}"),
        }
    }
    assert!(!args.input.is_empty(), "{USAGE}");
    args.repeat = args.repeat.max(1);
    args
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

/// One pipeline run's products (decompress and compress are timed outside).
struct Run {
    parse_ms: f64,
    score_ms: f64,
    serialize_ms: f64,
    out_bytes: Vec<u8>,
    scores: Vec<Vec<Value>>,
    order: Vec<usize>,
    failures: Vec<(usize, String)>,
    stats: SolveStats,
    note: Option<String>,
}

fn run_value(bytes: &[u8], solver: Solver) -> Run {
    let t = Instant::now();
    let mut doc: Value = serde_json::from_slice(bytes).expect("parse JSON");
    let parse_ms = ms(t);

    let t = Instant::now();
    // Accept a stored TRAPI Response ({"message": ...}) or a bare Message.
    let message = if doc.get("message").map_or(false, Value::is_object) {
        doc.get_mut("message").unwrap()
    } else {
        &mut doc
    };
    let message = message.as_object_mut().expect("message is an object");
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
    let out_bytes = serde_json::to_vec(&doc).expect("serialise");
    let serialize_ms = ms(t);
    Run {
        parse_ms,
        score_ms,
        serialize_ms,
        out_bytes,
        scores,
        order,
        failures,
        stats,
        note: None,
    }
}

/// Err: the typed parse couldn't represent this message.
fn run_typed(bytes: &[u8], solver: Solver, json: Backend) -> Result<Run, String> {
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

/// `--bench-parse`: time each parser on the same decompressed bytes,
/// `repeat` times each (the parse result is dropped outside the timing).
fn bench_parse(raw: &[u8], repeat: usize) {
    let bytes = if raw.starts_with(&ZSTD_MAGIC) {
        decompress(raw)
    } else {
        raw.to_vec()
    };
    let report = |name: &str, mut v: Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mbps = bytes.len() as f64 / 1e6 / (v[0] / 1e3);
        println!("{name:<28} min {:7.1} ms  median {:7.1} ms  ({mbps:.0} MB/s)", v[0], v[v.len() / 2]);
    };
    // Times the parse only: each result is dropped after the clock stops
    // (freeing a big tree can cost as much as building it, and the pipeline
    // drops outside its timed phases too).
    fn time<T>(repeat: usize, mut f: impl FnMut(usize) -> T) -> Vec<f64> {
        (0..repeat)
            .map(|i| {
                let t = Instant::now();
                let v = f(i);
                let elapsed = ms(t);
                drop(v);
                elapsed
            })
            .collect()
    }
    println!("{:.1} MB of JSON, {repeat} runs each", bytes.len() as f64 / 1e6);
    println!("-- full document (reference):");
    report("serde_json Value", time(repeat, |_| serde_json::from_slice::<Value>(&bytes).unwrap()));
    report("sonic-rs Value", time(repeat, |_| sonic_rs::from_slice::<sonic_rs::Value>(&bytes).unwrap()));
    // simd-json parses in place, so each run needs its own copy (not timed).
    let mut times = vec![];
    for _ in 0..repeat {
        let mut copy = bytes.clone(); // not timed
        let t = Instant::now();
        let v = simd_json::to_borrowed_value(&mut copy).unwrap();
        times.push(ms(t));
        drop(v);
    }
    report("simd-json BorrowedValue", times);
    println!("-- typed visitors (what --parse typed runs):");
    for backend in [Backend::SerdeJson, Backend::Sonic] {
        report(
            &format!("typed, {}", backend.name()),
            time(repeat, |_| typed_input::parse(&bytes, backend).unwrap()),
        );
    }
}

fn main() {
    let args = parse_args();
    // One thread, like each Python pool worker: the matrices are small, and
    // faer's default thread pool costs more than it saves on them.
    faer::set_global_parallelism(faer::Par::Seq);

    let t = Instant::now();
    let raw = std::fs::read(&args.input).expect("read input");
    let read_ms = ms(t);

    if args.bench_parse {
        bench_parse(&raw, args.repeat);
        return;
    }

    let phases = ["decompress", "parse", "score", "serialize", "compress"];
    let mut runs: Vec<Vec<f64>> = vec![];
    let mut last = None;
    for _ in 0..args.repeat {
        let t = Instant::now();
        let bytes = if raw.starts_with(&ZSTD_MAGIC) {
            decompress(&raw)
        } else {
            raw.clone()
        };
        let decompress_ms = ms(t);

        let run = match args.parse {
            Parse::Value => run_value(&bytes, args.solver),
            Parse::Typed => run_typed(&bytes, args.solver, args.json).unwrap_or_else(|e| {
                let mut r = run_value(&bytes, args.solver);
                r.note = Some(format!("typed parse failed ({e}); used the value path"));
                r
            }),
        };

        let t = Instant::now();
        let compressed = zstd::bulk::compress(&run.out_bytes, 3).expect("zstd compress");
        let compress_ms = ms(t);

        runs.push(vec![decompress_ms, run.parse_ms, run.score_ms, run.serialize_ms, compress_ms]);
        last = Some((run, compressed, bytes.len()));
    }

    let (run, compressed, in_len) = last.unwrap();
    if let Some(path) = &args.save_message {
        let data = if path.ends_with(".zst") {
            compressed.clone()
        } else {
            run.out_bytes.clone()
        };
        std::fs::write(path, data).expect("write message");
    }

    let stat = |v: Vec<f64>| {
        let mut v = v;
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        json!({"min": v[0], "median": v[v.len() / 2], "runs": v})
    };
    let mut timings = serde_json::Map::new();
    timings.insert("read".into(), stat(vec![read_ms]));
    for (i, p) in phases.iter().enumerate() {
        timings.insert((*p).into(), stat(runs.iter().map(|r| r[i]).collect()));
    }
    timings.insert("total".into(), stat(runs.iter().map(|r| r.iter().sum()).collect()));

    let label = match args.parse {
        Parse::Typed => format!("rust[typed:{},{}]", args.json.name(), args.solver.name()),
        Parse::Value => format!("rust[value,{}]", args.solver.name()),
    };
    let n_analyses: usize = run.scores.iter().map(Vec::len).sum();
    let result = json!({
        "impl": label,
        "input": args.input,
        "repeat": args.repeat,
        "message_bytes": in_len,
        "output_bytes": run.out_bytes.len(),
        "n_results": run.scores.len(),
        "n_analyses": n_analyses,
        "failed_answers": run.failures.len(),
        "failures": run.failures.iter().take(50).map(|(i, e)| json!([i, e])).collect::<Vec<_>>(),
        "solves": {"cholesky": run.stats.cholesky, "svd": run.stats.svd, "cholesky_fallbacks": run.stats.fallbacks},
        "note": run.note,
        "saved_message": args.save_message,
        "timings_ms": timings,
        "scores": run.scores,
        "order": run.order,
    });

    eprintln!(
        "{label}: {} results, {} analyses, {} failed | score {:.1} ms | total {:.1} ms (min of {}) | solves: {} cholesky, {} svd ({} fallbacks){}",
        run.scores.len(),
        n_analyses,
        run.failures.len(),
        result["timings_ms"]["score"]["min"].as_f64().unwrap(),
        result["timings_ms"]["total"]["min"].as_f64().unwrap(),
        args.repeat,
        run.stats.cholesky,
        run.stats.svd,
        run.stats.fallbacks,
        run.note.as_deref().map(|n| format!(" | NOTE: {n}")).unwrap_or_default(),
    );
    if let Some(path) = &args.out {
        std::fs::write(path, serde_json::to_vec(&result).unwrap()).expect("write result");
    } else {
        println!("{}", serde_json::to_string(&result["timings_ms"]).unwrap());
    }
}
