//! Benchmark runner for the Rust aragorn.score scorer (the library in lib.rs).
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

use std::time::Instant;

use aragorn_score_rs::pipeline::{decompress, ms, run, run_value, ZSTD_MAGIC};
use aragorn_score_rs::solve::Solver;
use aragorn_score_rs::typed_input::{self, Backend};
use serde_json::{json, Value};

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

/// `--bench-parse`: time each parser on the same decompressed bytes,
/// `repeat` times each (the parse result is dropped outside the timing).
fn bench_parse(raw: &[u8], repeat: usize) {
    let bytes = if raw.starts_with(&ZSTD_MAGIC) {
        decompress(raw).expect("zstd decompress")
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
    #[cfg(feature = "bench-parsers")]
    {
        let mut times = vec![];
        for _ in 0..repeat {
            let mut copy = bytes.clone(); // not timed
            let t = Instant::now();
            let v = simd_json::to_borrowed_value(&mut copy).unwrap();
            times.push(ms(t));
            drop(v);
        }
        report("simd-json BorrowedValue", times);
    }
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
            decompress(&raw).expect("zstd decompress")
        } else {
            raw.clone()
        };
        let decompress_ms = ms(t);

        let run = match args.parse {
            Parse::Value => run_value(&bytes, args.solver),
            Parse::Typed => run(&bytes, args.solver, args.json),
        }
        .expect("scoring failed");

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
