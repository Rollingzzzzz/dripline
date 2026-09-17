#!/usr/bin/env python3
"""Engine-level benchmark harness for dripline.

Measures the decision loop (one admission check per offered request) across
dripline and the incumbent engines, plus RSS footprint vs distinct clients.
The methodology contract lives in bench/README.md (Docker, pinned core,
7 runs per cell, median reported, outliers kept in the raw log).

Output: bench/results/<version>.md and <version>.raw.json — generated files,
never edited by hand.

Examples:
    python bench/run.py                     # full run, writes results files
    python bench/run.py --quick             # smoke run, stdout only
    python bench/run.py --full              # adds the 10M-client RSS point
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import platform
import random
import subprocess
import sys
from array import array
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import metadata, util
from pathlib import Path
from statistics import median
from time import perf_counter_ns

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

SEED = 20260917
BULK_CHUNK = 100_000
DISTS = ("uniform", "zipf")

RELEASE = dict(
    n=1_000_000,          # offered decisions in the timed bulk pass
    k=100_000,            # distinct clients in the key population
    warmup=100_000,       # untimed calls before the bulk pass
    samples=200_000,      # per-call-timed calls for the percentile pass
    runs=7, min_runs=7,   # fixed repetition count (no early stop)
    bulk_cap_s=4.0,       # per-run wall-clock caps so slow variants can't
    percall_cap_s=2.0,    # blow up the suite; actual n is recorded
    rss_ks=(1_000, 10_000, 100_000, 1_000_000),
)
# Fast protocol for iteration: 3-run minimum with sequential stopping (extend
# toward 7 only if the 3 run-means disagree by >5%). Justified by measured
# data in this repo: worst 3-run-vs-7-run median deviation across 20 cells
# was 2.6% (see bench/README.md, "Fast vs release protocol").
FAST = dict(
    n=1_000_000, k=100_000, warmup=30_000, samples=100_000,
    runs=7, min_runs=3,
    bulk_cap_s=1.5, percall_cap_s=1.0,
    rss_ks=(10_000, 100_000, 1_000_000),
)
QUICK = dict(
    n=50_000, k=10_000, warmup=10_000, samples=20_000, runs=2, min_runs=2,
    bulk_cap_s=2.0, percall_cap_s=1.0, rss_ks=(1_000, 10_000),
)
FULL = RELEASE  # historical name kept for importers (bench/champions.py)
FULL_RSS_EXTRA_K = 10_000_000  # only with --full (needs the 8g container)


# --------------------------------------------------------------------------- #
# Variants — each make() returns a fresh decider: (key) -> allowed bool.
# All variants face the identical offered key sequence, so wrapper overhead
# (one closure call + one method call) is charged to everyone equally.
# --------------------------------------------------------------------------- #

@dataclass
class Variant:
    name: str
    label: str
    algorithm: str
    config: str
    kind: str                       # "sync" | "async"
    make: Callable
    requires: str | None = None     # import name of the comparison package
    guard_capacity: int | None = None   # instant admits expected; None = skip
    max_rss_k: int | None = None     # RSS sweep cap for this variant


def _make_noop() -> Callable[[str], bool]:
    def decide(key: str) -> bool:
        return True
    return decide


def _make_dripline(rate_per_second: float, burst: int) -> Callable[[], Callable[[str], bool]]:
    def factory() -> Callable[[str], bool]:
        from dripline import GcraLimiter
        lim = GcraLimiter(rate_per_second=rate_per_second, burst=burst)

        def decide(key: str) -> bool:
            return not lim.try_acquire(key)
        return decide
    return factory


def _make_dripline_arena(rate_per_second: float, burst: int,
                         slots: int) -> Callable[[], Callable[[str], bool]]:
    def factory() -> Callable[[str], bool]:
        from dripline import ArenaGcraLimiter
        lim = ArenaGcraLimiter(rate_per_second=rate_per_second, burst=burst,
                               slots=slots)

        def decide(key: str) -> bool:
            return not lim.try_acquire(key)
        return decide
    return factory


def _make_limits(strategy: str, rate_str: str) -> Callable[[], Callable[[str], bool]]:
    def factory() -> Callable[[str], bool]:
        from limits import parse
        from limits.storage import MemoryStorage
        from limits.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter
        storage = MemoryStorage()
        engine = {"fixed": FixedWindowRateLimiter,
                  "moving": MovingWindowRateLimiter}[strategy](storage)
        rate = parse(rate_str)

        def decide(key: str) -> bool:
            return engine.hit(rate, key)
        return decide
    return factory


def _make_aiolimiter_perkey(max_rate: int, period: float) -> Callable[[], Callable[[str], object]]:
    def factory():
        from aiolimiter import AsyncLimiter
        local: dict = {}

        # AsyncLimiter binds to the running loop on first use, so instances
        # must be created and consumed inside one asyncio.run() session.
        async def decide(key: str) -> bool:
            lim = local.get(key)
            if lim is None:
                lim = local[key] = AsyncLimiter(max_rate, period)
            if not lim.has_capacity(1):
                return False
            await lim.acquire(1)
            return True
        return decide
    return factory


def build_variants(profile: str) -> list[Variant]:
    """Registry per profile.

    default: each library's idiomatic '1000 per minute' (burst semantics differ,
    documented in the report) — admit-dominated under the standard workload.
    tight: instant capacity 1 for every engine, so the bulk pass is
    reject-dominated — the red path is what gets measured.
    """
    if profile not in ("default", "tight"):
        raise ValueError(f"unknown profile: {profile}")
    if profile == "default":
        return [
            Variant("no-op", "no-op (floor)", "plain function call", "—",
                     "sync", _make_noop),
            Variant("dripline-gcra", "dripline GCRA", "GCRA, one int per client",
                    "1000/min, burst 100", "sync", _make_dripline(1000 / 60, 100),
                    guard_capacity=100),
            Variant("dripline-arena", "dripline arena (mmap)",
                    "GCRA, fixed 32-B slot arena",
                    "1000/min, burst 100, 2M slots", "sync",
                    _make_dripline_arena(1000 / 60, 100, 2_000_000),
                    guard_capacity=100, max_rss_k=1_000_000),
            Variant("limits-fixed-window", "slowapi engine (limits) — fixed window",
                    "fixed window", "1000/minute", "sync", _make_limits("fixed", "1000/minute"),
                    requires="limits", guard_capacity=1000, max_rss_k=1_000_000),
            Variant("limits-moving-window", "slowapi engine (limits) — moving window",
                    "moving window", "1000/minute", "sync", _make_limits("moving", "1000/minute"),
                    requires="limits", guard_capacity=1000, max_rss_k=1_000_000),
            Variant("aiolimiter-perkey", "aiolimiter (per-key instances)",
                    "leaky bucket, one limiter per key",
                    "AsyncLimiter(1000, 60 s) per key", "async",
                    _make_aiolimiter_perkey(1000, 60),
                    requires="aiolimiter", guard_capacity=1000, max_rss_k=1_000_000),
        ]
    return [
        Variant("no-op", "no-op (floor)", "plain function call", "—",
                 "sync", _make_noop),
        Variant("dripline-gcra", "dripline GCRA", "GCRA, one int per client",
                "10/min, burst 1 (capacity 1)", "sync", _make_dripline(10 / 60, 1),
                guard_capacity=1),
        Variant("dripline-arena", "dripline arena (mmap)",
                "GCRA, fixed 32-B slot arena",
                "10/min, burst 1, 2M slots", "sync",
                _make_dripline_arena(10 / 60, 1, 2_000_000),
                guard_capacity=1, max_rss_k=1_000_000),
        Variant("limits-fixed-window", "slowapi engine (limits) — fixed window",
                "fixed window", "1/minute", "sync", _make_limits("fixed", "1/minute"),
                requires="limits", guard_capacity=1, max_rss_k=1_000_000),
        Variant("limits-moving-window", "slowapi engine (limits) — moving window",
                "moving window", "1/minute", "sync", _make_limits("moving", "1/minute"),
                requires="limits", guard_capacity=1, max_rss_k=1_000_000),
        Variant("aiolimiter-perkey", "aiolimiter (per-key instances)",
                "leaky bucket, one limiter per key",
                "AsyncLimiter(1, 60 s) per key", "async",
                _make_aiolimiter_perkey(1, 60),
                requires="aiolimiter", guard_capacity=1, max_rss_k=1_000_000),
    ]


# --------------------------------------------------------------------------- #
# Environment / dependency bootstrap
# --------------------------------------------------------------------------- #

def _read_proc_line(path: str, prefix: str) -> str | None:
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith(prefix):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return None


def _cgroup_limit(v2_name: str, v1_name: str) -> str:
    for name in (v2_name, v1_name):
        try:
            val = (Path("/sys/fs/cgroup") / name).read_text().strip()
            return f"{name}={val}"
        except OSError:
            continue
    return "unavailable"


def _cpuset() -> str:
    try:
        return Path("/sys/fs/cgroup/cpuset.cpus.effective").read_text().strip()
    except OSError:
        return "unavailable"


def _git_commit() -> str:
    try:
        out = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def env_report(available: list[Variant]) -> dict:
    cpu = _read_proc_line("/proc/cpuinfo", "model name") or platform.processor() or "unknown"
    import dripline
    versions = {"dripline": dripline.__version__}
    for v in available:
        if v.requires:
            try:
                versions[v.requires] = metadata.version(v.requires)
            except metadata.PackageNotFoundError:
                versions[v.requires] = "?"
    return {
        "generated_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "git_commit": _git_commit(),
        "python": f"{platform.python_implementation()} {platform.python_version()}",
        "platform": platform.platform(),
        "cpu": cpu,
        "cgroup_cpu": _cgroup_limit("cpu.max", "cpuset/cpu.cfs_quota_us"),
        "cgroup_memory": _cgroup_limit("memory.max", "memory/memory.limit_in_bytes"),
        "cpuset": _cpuset(),
        "versions": versions,
    }


def resolve_variants(variants: list[Variant], no_install: bool) -> tuple[list[Variant], list[dict]]:
    """Optionally pip-install pinned comparison deps; return (available, skipped)."""
    def missing_mods() -> set[str]:
        return {v.requires for v in variants
                if v.requires and util.find_spec(v.requires) is None}

    missing = missing_mods()
    install_error = None
    if missing and not no_install:
        req = ROOT / "bench" / "requirements.txt"
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "--quiet",
                            "-r", str(req)], check=True, timeout=600)
        except Exception as exc:
            install_error = str(exc)

    still_missing = missing_mods()
    skipped, available = [], []
    for v in variants:
        if v.requires in still_missing:
            reason = (f"pip install failed: {install_error}" if install_error
                      else f"module {v.requires!r} not importable (--no-install)")
            skipped.append({"variant": v.name, "reason": reason})
        else:
            available.append(v)
    return available, skipped


# --------------------------------------------------------------------------- #
# Workload
# --------------------------------------------------------------------------- #

def gen_keys(total: int, k: int, dist: str, seed: int) -> list[str]:
    rng = random.Random(seed)
    if dist == "uniform":
        idx = [rng.randrange(k) for _ in range(total)]
    elif dist == "zipf":
        # Truncated Zipf(1.2) over k ranks — a few hot clients, long tail.
        weights = [1.0 / (i + 1) ** 1.2 for i in range(k)]
        idx = rng.choices(range(k), weights=weights, k=total)
    else:
        raise ValueError(f"unknown distribution: {dist}")
    return [sys.intern(f"c{i}") for i in idx]


# --------------------------------------------------------------------------- #
# Timed passes
# --------------------------------------------------------------------------- #

def _percentiles(samples: array) -> dict:
    if not samples:
        return {"p50": None, "p95": None, "p99": None}
    s = sorted(samples)

    def pct(p: float) -> int:
        return s[min(len(s) - 1, int(p * (len(s) - 1)))]

    return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}


def timer_pair_overhead(reps: int = 20_000) -> int:
    """Median cost of one back-to-back perf_counter_ns pair (report note)."""
    deltas = array("q")
    for _ in range(reps):
        t0 = perf_counter_ns()
        t1 = perf_counter_ns()
        deltas.append(t1 - t0)
    return _percentiles(deltas)["p50"]


def bench_sync(v: Variant, warm: list[str], bulk: list[str], perc: list[str], p: dict):
    decider = v.make()
    for key in warm:
        decider(key)
    gc.collect()
    gc.disable()
    try:
        done, admitted, total, start = 0, 0, 0, perf_counter_ns()
        while done < len(bulk):
            chunk = bulk[done:done + BULK_CHUNK]
            t0 = perf_counter_ns()
            for key in chunk:
                if decider(key):
                    admitted += 1
            total += perf_counter_ns() - t0
            done += len(chunk)
            if perf_counter_ns() - start > p["bulk_cap_s"] * 1e9:
                break
        samples = array("q")
        start = perf_counter_ns()
        for key in perc:
            t0 = perf_counter_ns()
            decider(key)
            t1 = perf_counter_ns()
            samples.append(t1 - t0)
            if len(samples) >= p["samples"]:
                break
            if len(samples) % 1000 == 0 and perf_counter_ns() - start > p["percall_cap_s"] * 1e9:
                break
    finally:
        gc.enable()
    return done, admitted, total, samples


def bench_async(v: Variant, warm: list[str], bulk: list[str], perc: list[str], p: dict):
    async def _run():
        decider = v.make()
        for key in warm:
            await decider(key)
        gc.collect()
        gc.disable()
        try:
            done, admitted, total, start = 0, 0, 0, perf_counter_ns()
            while done < len(bulk):
                chunk = bulk[done:done + BULK_CHUNK]
                t0 = perf_counter_ns()
                for key in chunk:
                    if await decider(key):
                        admitted += 1
                total += perf_counter_ns() - t0
                done += len(chunk)
                if perf_counter_ns() - start > p["bulk_cap_s"] * 1e9:
                    break
            samples = array("q")
            start = perf_counter_ns()
            for key in perc:
                t0 = perf_counter_ns()
                await decider(key)
                t1 = perf_counter_ns()
                samples.append(t1 - t0)
                if len(samples) >= p["samples"]:
                    break
                if (len(samples) % 1000 == 0
                        and perf_counter_ns() - start > p["percall_cap_s"] * 1e9):
                    break
        finally:
            gc.enable()
        return done, admitted, total, samples
    return asyncio.run(_run())


def run_one(v: Variant, dist: str, keys: tuple[list[str], list[str], list[str]], p: dict) -> dict:
    warm, bulk, perc = keys
    fn = bench_sync if v.kind == "sync" else bench_async
    done, admitted, total, samples = fn(v, warm, bulk, perc, p)
    mean_ns = total / done if done else float("nan")
    ops_s = done / (total / 1e9) if total else 0.0
    return {"variant": v.name, "dist": dist, "bulk_n": done, "bulk_ns": total,
            "admit_ratio": round(admitted / done, 4) if done else None,
            "mean_ns": round(mean_ns, 1), "ops_s": round(ops_s),
            "sample_n": len(samples), **_percentiles(samples)}


# --------------------------------------------------------------------------- #
# Correctness guard (untimed): prove each adapter actually enforces its limit
# --------------------------------------------------------------------------- #

def run_guard(v: Variant) -> dict:
    if v.guard_capacity is None:
        return {"variant": v.name, "fired": None, "allowed": None,
                "expected": None, "status": "skipped (baseline holds no state)"}
    fired = v.guard_capacity * 3
    if v.kind == "sync":
        decider = v.make()
        allowed = sum(1 for _ in range(fired) if decider(f"guard-{v.name}"))
    else:
        async def _g():
            decider = v.make()
            key = f"guard-{v.name}"
            allowed = 0
            for _ in range(fired):
                if await decider(key):
                    allowed += 1
            return allowed
        allowed = asyncio.run(_g())
    ok = v.guard_capacity - 2 <= allowed <= v.guard_capacity
    return {"variant": v.name, "fired": fired, "allowed": allowed,
            "expected": v.guard_capacity, "status": "pass" if ok else "FAIL"}


# --------------------------------------------------------------------------- #
# RSS sweep — one subprocess per (variant, K) so states never share a heap
# --------------------------------------------------------------------------- #

def read_rss_kb() -> int | None:
    val = _read_proc_line("/proc/self/status", "VmRSS:")
    return int(val.split()[0]) if val else None


def rss_child_main(name: str, k: int, profile: str) -> None:
    v = {x.name: x for x in build_variants(profile)}[name]
    rss_before = read_rss_kb()
    # Keys are generated on the fly: the child holds only the limiter and its
    # own key storage, which is exactly what a real deployment would hold.
    if v.kind == "sync":
        decider = v.make()
        for i in range(k):
            decider(f"c{i}")
    else:
        async def _consume():
            decider = v.make()
            for i in range(k):
                await decider(f"c{i}")
            return decider  # caller must hold this: the limiter state IS the measurement
        decider = asyncio.run(_consume())
    gc.collect()
    rss_after = read_rss_kb()
    delta = rss_after - rss_before if None not in (rss_before, rss_after) else None
    print(json.dumps({"variant": name, "clients": k,
                      "rss_before_kb": rss_before, "rss_after_kb": rss_after,
                      "delta_kb": delta}))


def rss_sweep(available: list[Variant], ks: tuple[int, ...], profile: str) -> list[dict]:
    rows = []
    script = str(Path(__file__).resolve())
    for k in ks:
        for v in available:
            if v.name == "no-op":
                continue
            if v.max_rss_k is not None and k > v.max_rss_k:
                rows.append({"variant": v.name, "clients": k, "delta_kb": None,
                             "note": "skipped (variant capped)"})
                continue
            proc = subprocess.run(
                [sys.executable, script, "--rss-child", v.name, str(k),
                 "--profile", profile],
                capture_output=True, text=True, timeout=3600)
            try:
                row = json.loads(proc.stdout.strip().splitlines()[-1])
            except (IndexError, json.JSONDecodeError):
                row = {"variant": v.name, "clients": k, "delta_kb": None,
                       "note": f"child failed: {proc.stderr.strip()[:200]}"}
            rows.append(row)
            print(f"  rss {v.name} k={k}: {row.get('delta_kb')} kB", file=sys.stderr)
    return rows


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #

def aggregate(runs: list[dict]) -> dict:
    cells: dict[tuple[str, str], list[dict]] = {}
    for rec in runs:
        cells.setdefault((rec["variant"], rec["dist"]), []).append(rec)
    agg = {}
    for (name, dist), rs in cells.items():
        agg[(name, dist)] = {
            "mean_ns": median(r["mean_ns"] for r in rs),
            "ops_s": median(r["ops_s"] for r in rs),
            "admit_ratio": median(r["admit_ratio"] for r in rs if r["admit_ratio"] is not None),
            "p50": median(r["p50"] for r in rs if r["p50"] is not None),
            "p95": median(r["p95"] for r in rs if r["p95"] is not None),
            "p99": median(r["p99"] for r in rs if r["p99"] is not None),
            "runs": len(rs),
            "min_bulk_n": min(r["bulk_n"] for r in rs),
        }
    return agg


def render_report(version: str, quick: bool, profile: str, env: dict, p: dict,
                  available: list[Variant], agg: dict, guards: list[dict],
                  rss_rows: list[dict] | None, skipped: list[dict],
                  overhead_ns: int) -> str:
    title = f"dripline engine-level benchmark — {version}"
    if profile == "tight":
        title += " (TIGHT profile: instant capacity 1 — reject-dominated)"
    if p.get("min_runs", p["runs"]) < p["runs"]:
        title += " (FAST mode)"
    if quick:
        title += " (SMOKE --quick, not for publishing)"
    runs_desc = (f"{p['runs']} runs per cell" if p["min_runs"] >= p["runs"]
                 else f"{p['min_runs']}-{p['runs']} runs per cell (sequential stop "
                      f"when the spread of run means is within 5%)")
    lines = [
        f"<!-- GENERATED by bench/run.py at {env['generated_utc']} — do not edit by hand. -->",
        f"# {title}",
        "",
        f"Methodology: `bench/README.md` · {runs_desc}, median reported, "
        f"all raw runs in the raw json · seed {SEED}.",
        "",
        "## Environment",
        "",
        f"- {env['python']} on {env['platform']}",
        f"- CPU: {env['cpu']}",
        f"- cgroup: `{env['cgroup_cpu']}`, `{env['cgroup_memory']}` · "
        f"pinned cpuset: `{env['cpuset']}`",
        f"- versions: {', '.join(f'{k} {v}' for k, v in env['versions'].items())} "
        f"(git {env['git_commit']})",
        "",
        "## Variants and rate configuration",
        "",
        "| variant | engine / algorithm | rate configuration |",
        "|---|---|---|",
    ]
    for v in available:
        lines.append(f"| {v.label} | {v.algorithm} | {v.config} |")
    for s in skipped:
        lines.append(f"| ~~{s['variant']}~~ | SKIPPED | {s['reason']} |")

    for dist in DISTS:
        lines += [
            "",
            f"## Decision cost — {dist} keys "
            f"(K={p['k']:,}, offered {p['n']:,}, warmup {p['warmup']:,})",
            "",
            "| variant | mean ns/dec | p50 ns | p95 ns | p99 ns | ops/s | admits | bulk n |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for v in available:
            a = agg.get((v.name, dist))
            if not a:
                continue
            n_cell = (f"{a['min_bulk_n']:,} (capped)" if a["min_bulk_n"] < p["n"]
                      else f"{p['n']:,}")
            lines.append(
                f"| {v.label} | {a['mean_ns']:,.0f} | {a['p50']:,} | {a['p95']:,} "
                f"| {a['p99']:,} | {a['ops_s']:,.0f} | {a['admit_ratio'] * 100:.1f}% "
                f"| {n_cell} |")

    lines += [
        "",
        "## Correctness guard (untimed adapter sanity)",
        "",
        "| variant | fired | allowed | expected | status |",
        "|---|---:|---:|---:|---|",
    ]
    for g in guards:
        fired = "—" if g["fired"] is None else f"{g['fired']:,}"
        allowed = "—" if g["allowed"] is None else f"{g['allowed']:,}"
        expected = "—" if g["expected"] is None else str(g["expected"])
        lines.append(f"| {g['variant']} | {fired} | {allowed} | {expected} | {g['status']} |")

    if rss_rows is not None:
        sweep_variants = [v for v in available if v.name != "no-op"]
        ks = sorted({r["clients"] for r in rss_rows})
        lines += [
            "",
            "## RSS vs distinct clients (Δ from empty limiter, one decision per client)",
            "",
            "| clients | " + " | ".join(v.label for v in sweep_variants) + " |",
            "|---:|" + "---:|" * len(sweep_variants),
        ]
        for k in ks:
            cells = []
            for v in sweep_variants:
                row = next((r for r in rss_rows
                            if r["variant"] == v.name and r["clients"] == k), None)
                if row is None or row.get("delta_kb") is None:
                    cells.append("—")
                else:
                    cells.append(f"{row['delta_kb'] / 1024:.1f}")
            lines.append(f"| {k:,} | " + " | ".join(cells) + " |")
        notes = [f"{r['variant']} @ {r['clients']:,} clients: {r['note']}"
                 for r in rss_rows if r.get("note")]
        if notes:
            lines += ["", "Footnotes:"] + [f"- {n}" for n in notes]

    lines += [
        "",
        "## Notes — read before comparing",
        "",
        "- Engine-level measurement (decision loop), not request-level: the HTTP/middleware",
        "  comparison lands with the v0.1 middleware as a separate table.",
        "- All variants face the identical offered key sequence (same seed); each decision",
        "  goes through one closure call + one library call, charged to everyone equally.",
        "- Mean ns/decision and ops/s come from the untimed-per-call bulk pass.",
        f"- Percentile pass wraps each call in a perf_counter_ns pair (~{overhead_ns} ns",
        "  measured, identical across variants) and reuses the bulk limiter, so sampled",
        "  calls are steady-state, not first-touch. Subtract the no-op row for net cost.",
        "- GC is disabled during timed regions; limiter state is fresh per run.",
    ]
    if profile == "default":
        lines += [
        "- Configurations are each library's idiomatic '1000 per minute' — burst semantics",
        "  differ (see table); the comparison is cost at equivalent offered load, not",
        "  admission-pattern equality.",
        ]
    else:
        lines += [
        "- TIGHT profile: every engine configured for instant capacity 1 (dripline",
        "  10/min burst 1; limits 1/minute; aiolimiter 1 per 60 s), so the bulk pass",
        "  measures the REJECT path — the admits column proves how dominated it is.",
        ]
    lines += [
        "- aiolimiter has no sync path and no per-key mode: its rows measure one",
        "  AsyncLimiter per client key, including await/event-loop overhead, which is",
        "  what using it for per-subscriber limiting actually costs.",
        "- slowapi is a request-context wrapper around the `limits` engine; the engine",
        "  is what runs per request, so that is what is measured here.",
    ]
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="dripline engine-level benchmark harness",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--version", default="v0.1",
                    help="results filename tag")
    ap.add_argument("--runs", type=int, default=None,
                    help="override run count (default 7; --quick uses 2)")
    ap.add_argument("--quick", action="store_true",
                    help="small N/K smoke run; never writes results files")
    ap.add_argument("--full", action="store_true",
                    help="add the 10M-client RSS point (needs the pinned container)")
    ap.add_argument("--skip-rss", action="store_true", help="skip the RSS sweep")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the report, write nothing")
    ap.add_argument("--no-install", action="store_true",
                    help="don't pip-install missing comparison deps")
    ap.add_argument("--profile", choices=("default", "tight"), default="default",
                    help="default: idiomatic 1000/min; tight: capacity-1, reject-dominated")
    ap.add_argument("--release", action="store_true",
                    help="full release protocol (7 runs, larger warmup/samples/caps, "
                         "canonical vX.Y.md output); default is the fast protocol "
                         "writing vX.Y.fast.md")
    ap.add_argument("--rss-child", nargs=2, metavar=("VARIANT", "CLIENTS"),
                    help=argparse.SUPPRESS)
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.rss_child:
        rss_child_main(args.rss_child[0], int(args.rss_child[1]), args.profile)
        return 0

    p = dict(QUICK if args.quick else (RELEASE if args.release else FAST))
    if args.runs:
        p["runs"] = args.runs

    variants = build_variants(args.profile)
    available, skipped = resolve_variants(variants, args.no_install)
    env = env_report(available)

    print("guards…", file=sys.stderr)
    guards = [run_guard(v) for v in available]
    for g in guards:
        print(f"  {g['variant']}: {g['status']}", file=sys.stderr)

    total = p["warmup"] + p["n"] + p["samples"]
    runs = []
    for dist in DISTS:
        print(f"workload {dist} (K={p['k']:,}, {total:,} keys)…", file=sys.stderr)
        all_keys = gen_keys(total, p["k"], dist, SEED)
        keys = (all_keys[:p["warmup"]],
                all_keys[p["warmup"]:p["warmup"] + p["n"]],
                all_keys[p["warmup"] + p["n"]:])
        for v in available:
            cell = []
            while len(cell) < p["runs"]:
                rec = run_one(v, dist, keys, p)
                rec["run"] = len(cell) + 1
                cell.append(rec)
                runs.append(rec)
                means = [r["mean_ns"] for r in cell]
                stable = (max(means) - min(means)) <= 0.05 * median(means)
                if len(cell) >= p["min_runs"] and stable:
                    break
            print(f"  {v.name}/{dist}: {len(cell)} run(s), "
                  f"median {median(r['mean_ns'] for r in cell):,.0f} ns/dec, "
                  f"p99 {cell[-1]['p99']}", file=sys.stderr)
        del all_keys, keys

    rss_rows = None
    if not args.skip_rss:
        ks = p["rss_ks"] + ((FULL_RSS_EXTRA_K,) if args.full and not args.quick else ())
        print(f"rss sweep {ks}…", file=sys.stderr)
        rss_rows = rss_sweep(available, ks, args.profile)

    overhead_ns = timer_pair_overhead()
    agg = aggregate(runs)
    report = render_report(args.version, args.quick, args.profile, env, p, available,
                           agg, guards, rss_rows, skipped, overhead_ns)
    print(report)

    if args.dry_run or args.quick:
        if not args.dry_run:
            print("(--quick: report printed only, no files written)", file=sys.stderr)
    else:
        suffix = "" if args.release else ".fast"
        out_dir = ROOT / "bench" / "results"
        out_dir.mkdir(exist_ok=True)
        (out_dir / f"{args.version}{suffix}.md").write_text(report, encoding="utf-8")
        (out_dir / f"{args.version}{suffix}.raw.json").write_text(
            json.dumps({"env": env, "params": p, "seed": SEED, "runs": runs,
                        "guard": guards, "rss": rss_rows, "skipped": skipped,
                        "timer_pair_overhead_ns": overhead_ns},
                       indent=1), encoding="utf-8")
        print(f"wrote bench/results/{args.version}{suffix}.md and "
              f"{args.version}{suffix}.raw.json", file=sys.stderr)

    return 1 if any(g["status"] == "FAIL" for g in guards) else 0


if __name__ == "__main__":
    sys.exit(main())
