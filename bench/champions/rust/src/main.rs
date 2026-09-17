//! Champion benchmark: governor's GCRA (Rust) under the champion protocol —
//! bit-identical key files and the same metrics as the Python drivers, so the
//! rows are directly comparable. Same rate configuration as dripline:
//! 1000/minute with burst 100.
//!
//! Protocol:
//!   governor_bench --keys-dir DIR      -> prints {"uniform": {...}, "zipf": {...}}
//!   governor_bench --rss-child K       -> prints RSS delta JSON for K clients

use std::fs;
use std::time::Instant;

use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

struct Params {
    warmup: usize,
    n: usize,
    samples: usize,
    bulk_cap_s: f64,
    percall_cap_s: f64,
}

fn load_keys(path: &std::path::Path) -> Vec<String> {
    let bytes = fs::read(path).expect("keys file");
    let idx: Vec<u32> = bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    idx.into_iter().map(|i| format!("c{}", i)).collect()
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(p * (sorted.len() - 1) as f64) as usize]
}

fn run_dist(keys: &[String], p: &Params) -> serde_json::Value {
    let quota = Quota::per_minute(NonZeroU32::new(1000).unwrap())
        .allow_burst(NonZeroU32::new(100).unwrap());
    let lim: RateLimiter<String, _, _> = RateLimiter::dashmap(quota);

    // warmup
    for key in &keys[..p.warmup] {
        let _ = lim.check_key(key);
    }

    // bulk pass (mean/ops) — only the `n` decisions after warmup, like Python
    let mut done: usize = 0;
    let mut total_ns: u128 = 0;
    let run_start = Instant::now();
    while done < p.n {
        let end = (done + 100_000).min(p.n);
        let t0 = Instant::now();
        for key in &keys[p.warmup + done..p.warmup + end] {
            if lim.check_key(key).is_ok() {}
        }
        total_ns += t0.elapsed().as_nanos();
        done = end;
        if run_start.elapsed().as_secs_f64() > p.bulk_cap_s {
            break;
        }
    }

    // per-call pass (percentiles)
    let perc_end = (p.warmup + p.n + p.samples).min(keys.len());
    let mut samples: Vec<u64> = Vec::with_capacity(p.samples);
    let perc_start = Instant::now();
    for key in &keys[p.warmup + p.n..perc_end] {
        let t0 = Instant::now();
        let _ = lim.check_key(key);
        samples.push(t0.elapsed().as_nanos() as u64);
        if samples.len() >= p.samples {
            break;
        }
        if samples.len() % 1000 == 0 && perc_start.elapsed().as_secs_f64() > p.percall_cap_s {
            break;
        }
    }
    samples.sort_unstable();

    let mean_ns = total_ns as f64 / done as f64;
    let ops_s = done as f64 / (total_ns as f64 / 1e9);
    serde_json::json!({
        "mean_ns": (mean_ns * 10.0).round() / 10.0,
        "ops_s": ops_s.round(),
        "bulk_n": done,
        "sample_n": samples.len(),
        "p50": percentile(&samples, 0.50),
        "p95": percentile(&samples, 0.95),
        "p99": percentile(&samples, 0.99),
    })
}

fn read_rss_kb() -> Option<i64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

fn rss_child(k: i64) {
    let rss0 = read_rss_kb();
    let quota = Quota::per_minute(NonZeroU32::new(1000).unwrap())
        .allow_burst(NonZeroU32::new(100).unwrap());
    let lim: RateLimiter<String, _, _> =
        RateLimiter::dashmap(quota);
    for i in 0..k {
        let _ = lim.check_key(&format!("c{}", i));
    }
    let rss1 = read_rss_kb();
    let delta = match (rss0, rss1) {
        (Some(a), Some(b)) => Some(b - a),
        _ => None,
    };
    println!(
        "{}",
        serde_json::json!({"clients": k, "rss_before_kb": rss0, "rss_after_kb": rss1, "delta_kb": delta})
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--rss-child") {
        let pos = args.iter().position(|a| a == "--rss-child").unwrap();
        let k: i64 = args[pos + 1].parse().expect("K");
        rss_child(k);
        return;
    }
    let keys_dir = args
        .iter()
        .position(|a| a == "--keys-dir")
        .map(|p| std::path::PathBuf::from(&args[p + 1]))
        .expect("--keys-dir DIR");

    let params_raw = fs::read_to_string(keys_dir.join("params.json")).expect("params.json");
    let v: serde_json::Value = serde_json::from_str(&params_raw).expect("params json");
    let p = Params {
        warmup: v["warmup"].as_u64().unwrap() as usize,
        n: v["n"].as_u64().unwrap() as usize,
        samples: v["samples"].as_u64().unwrap() as usize,
        bulk_cap_s: v["bulk_cap_s"].as_f64().unwrap(),
        percall_cap_s: v["percall_cap_s"].as_f64().unwrap(),
    };

    let uniform = load_keys(&keys_dir.join("uniform.u32"));
    let zipf = load_keys(&keys_dir.join("zipf.u32"));
    let out = serde_json::json!({
        "runtime": "Rust (governor crate)",
        "engine": "governor GCRA — dashmap-backed, compiled",
        "uniform": run_dist(&uniform, &p),
        "zipf": run_dist(&zipf, &p),
    });
    println!("{}", out);
}
