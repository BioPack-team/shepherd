//! Benchmark runner for the Rust port of the aragorn.score ranker.
//!
//!     aragorn_score_rs <message.json|message.json.zst> [--repeat N]
//!                      [--out result.json] [--save-message scored.json.zst]
//!
//! Each repeat runs the full worker pipeline on the raw bytes, timed per
//! phase: zstd decompress -> JSON parse -> score -> JSON serialise -> zstd
//! compress. The result file has the same shape as `python_scorer.py`'s, for
//! `compare.py`.

mod ranker;

use std::time::Instant;

use serde_json::{json, Value};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

struct Args {
    input: String,
    repeat: usize,
    out: Option<String>,
    save_message: Option<String>,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut args = Args {
        input: String::new(),
        repeat: 1,
        out: None,
        save_message: None,
    };
    while let Some(a) = it.next() {
        match a.as_str() {
            "--repeat" => args.repeat = it.next().and_then(|v| v.parse().ok()).expect("--repeat N"),
            "--out" => args.out = it.next(),
            "--save-message" => args.save_message = it.next(),
            "-h" | "--help" => {
                eprintln!("usage: aragorn_score_rs MESSAGE [--repeat N] [--out RESULT.json] [--save-message OUT]");
                std::process::exit(0);
            }
            _ if args.input.is_empty() => args.input = a,
            _ => panic!("unexpected argument {a}"),
        }
    }
    assert!(!args.input.is_empty(), "usage: aragorn_score_rs MESSAGE [--repeat N] [--out RESULT.json]");
    args.repeat = args.repeat.max(1);
    args
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

fn main() {
    let args = parse_args();
    // One thread, like each Python pool worker: the matrices are small, and
    // faer's default thread pool costs more than it saves on them.
    faer::set_global_parallelism(faer::Par::Seq);

    let t = Instant::now();
    let raw = std::fs::read(&args.input).expect("read input");
    let read_ms = ms(t);

    let phases = ["decompress", "parse", "score", "serialize", "compress"];
    let mut runs: Vec<Vec<f64>> = vec![];
    let mut last = None;
    for _ in 0..args.repeat {
        let mut times = vec![];

        let t = Instant::now();
        let compressed = raw.starts_with(&ZSTD_MAGIC);
        let bytes = if compressed {
            zstd::decode_all(raw.as_slice()).expect("zstd decompress")
        } else {
            raw.clone()
        };
        times.push(ms(t));

        let t = Instant::now();
        let mut doc: Value = serde_json::from_slice(&bytes).expect("parse JSON");
        times.push(ms(t));

        let t = Instant::now();
        // Accept a stored TRAPI Response ({"message": ...}) or a bare Message.
        let message = if doc.get("message").map_or(false, Value::is_object) {
            doc.get_mut("message").unwrap()
        } else {
            &mut doc
        };
        let message = message.as_object_mut().expect("message is an object");
        let mut results = match message.get_mut("results") {
            Some(Value::Array(r)) => std::mem::take(r),
            _ => vec![],
        };
        let has_results = matches!(message.get("results"), Some(Value::Array(_)));
        let (order, failures) = if has_results {
            let mut ranker = ranker::Ranker::new(message);
            ranker.rank(&mut results)
        } else {
            (vec![], vec![])
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
        times.push(ms(t));

        let t = Instant::now();
        let out_bytes = serde_json::to_vec(&doc).expect("serialise");
        times.push(ms(t));

        let t = Instant::now();
        let out_compressed = zstd::bulk::compress(&out_bytes, 3).expect("zstd compress");
        times.push(ms(t));

        runs.push(times);
        last = Some((scores, order, failures, out_compressed, out_bytes.len(), bytes.len()));
    }

    let (scores, order, failures, out_compressed, out_len, in_len) = last.unwrap();
    if let Some(path) = &args.save_message {
        let data = if path.ends_with(".zst") {
            out_compressed.clone()
        } else {
            serde_json::to_vec_pretty(&serde_json::from_slice::<Value>(
                &zstd::decode_all(out_compressed.as_slice()).unwrap(),
            ).unwrap())
            .unwrap()
        };
        std::fs::write(path, data).expect("write message");
    }

    let stat = |i: usize| {
        let mut v: Vec<f64> = runs.iter().map(|r| r[i]).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        json!({"min": v[0], "median": v[v.len() / 2], "runs": v})
    };
    let mut timings = serde_json::Map::new();
    timings.insert("read".into(), json!({"min": read_ms, "median": read_ms, "runs": [read_ms]}));
    for (i, p) in phases.iter().enumerate() {
        timings.insert((*p).into(), stat(i));
    }
    let totals: Vec<f64> = runs.iter().map(|r| r.iter().sum()).collect();
    let mut sorted = totals.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    timings.insert(
        "total".into(),
        json!({"min": sorted[0], "median": sorted[sorted.len() / 2], "runs": totals}),
    );

    let n_analyses: usize = scores.iter().map(Vec::len).sum();
    let result = json!({
        "impl": "rust",
        "input": args.input,
        "repeat": args.repeat,
        "message_bytes": in_len,
        "output_bytes": out_len,
        "n_results": scores.len(),
        "n_analyses": n_analyses,
        "failed_answers": failures.len(),
        "failures": failures.iter().take(50).map(|(i, e)| json!([i, e])).collect::<Vec<_>>(),
        "timings_ms": timings,
        "scores": scores,
        "order": order,
    });

    eprintln!(
        "rust:   {} results, {} analyses, {} failed | score {:.1} ms (min of {}) | total {:.1} ms",
        scores.len(), n_analyses, failures.len(),
        result["timings_ms"]["score"]["min"].as_f64().unwrap(), args.repeat,
        result["timings_ms"]["total"]["min"].as_f64().unwrap(),
    );
    if let Some(path) = &args.out {
        std::fs::write(path, serde_json::to_vec(&result).unwrap()).expect("write result");
    } else {
        println!("{}", serde_json::to_string(&result["timings_ms"]).unwrap());
    }
}
