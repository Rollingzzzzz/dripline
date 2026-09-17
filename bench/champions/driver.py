#!/usr/bin/env python3
"""Champion driver (runtime-agnostic CPython/PyPy): times dripline's GCRA core
under the champion protocol — bit-identical key files, same metrics, so a
PyPy or CPython run of THIS script is directly comparable to the Rust champion.

Protocol:
    driver.py --keys-dir DIR                    # prints {"uniform": {...}, "zipf": {...}}
    driver.py --rss-child K                      # prints RSS delta JSON for K clients

Keys files are little-endian u32 client indices (key = f"c{idx}"), produced by
bench/champions.py from the same seed as the main suite.
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
from array import array
from pathlib import Path
from time import perf_counter_ns

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

BULK_CHUNK = 100_000


def load_keys(path: Path) -> list[str]:
    idx = array("I")
    with path.open("rb") as fh:
        idx.frombytes(fh.read())
    return [sys.intern(f"c{i}") for i in idx]


def read_rss_kb() -> int | None:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        pass
    return None


def percentiles(samples: array) -> dict:
    s = sorted(samples)

    def pct(p: float) -> int:
        return s[min(len(s) - 1, int(p * (len(s) - 1)))]

    return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}


def run_dist(keys: list[str], p: dict) -> dict:
    from dripline import GcraLimiter
    warm, bulk, perc = (keys[:p["warmup"]],
                        keys[p["warmup"]:p["warmup"] + p["n"]],
                        keys[p["warmup"] + p["n"]:])
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)

    for key in warm:
        lim.try_acquire(key)
    gc.collect()
    gc.disable()
    try:
        done, total, start = 0, 0, perf_counter_ns()
        while done < len(bulk):
            chunk = bulk[done:done + BULK_CHUNK]
            t0 = perf_counter_ns()
            for key in chunk:
                lim.try_acquire(key)
            total += perf_counter_ns() - t0
            done += len(chunk)
            if perf_counter_ns() - start > p["bulk_cap_s"] * 1e9:
                break
        samples = array("q")
        start = perf_counter_ns()
        for key in perc:
            t0 = perf_counter_ns()
            lim.try_acquire(key)
            t1 = perf_counter_ns()
            samples.append(t1 - t0)
            if len(samples) >= p["samples"]:
                break
            if len(samples) % 1000 == 0 and perf_counter_ns() - start > p["percall_cap_s"] * 1e9:
                break
    finally:
        gc.enable()
    mean = total / done if done else float("nan")
    return {"mean_ns": round(mean, 1), "ops_s": round(done / (total / 1e9)) if total else 0,
            "bulk_n": done, "sample_n": len(samples), **percentiles(samples)}


def rss_child(k: int) -> None:
    from dripline import GcraLimiter
    rss0 = read_rss_kb()
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    for i in range(k):
        lim.try_acquire(f"c{i}")
    gc.collect()
    rss1 = read_rss_kb()
    delta = rss1 - rss0 if None not in (rss0, rss1) else None
    print(json.dumps({"clients": k, "rss_before_kb": rss0, "rss_after_kb": rss1,
                      "delta_kb": delta}))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys-dir", required=False)
    ap.add_argument("--rss-child", type=int, default=None)
    args = ap.parse_args()

    if args.rss_child is not None:
        rss_child(args.rss_child)
        return 0

    keys_dir = Path(args.keys_dir)
    params = json.loads((keys_dir / "params.json").read_text())
    out = {"runtime": f"{sys.implementation.name} {sys.version.split()[0]}",
           "engine": "dripline GCRA (dict) — core.py unmodified"}
    for dist in ("uniform", "zipf"):
        keys = load_keys(keys_dir / f"{dist}.u32")
        out[dist] = run_dist(keys, params)
        print(f"{dist}: {out[dist]['mean_ns']:.0f} ns/dec", file=sys.stderr)
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
