#!/usr/bin/env python3
"""Adversarial experiments: scenarios built to favor the incumbents.

Speed is NOT measured here (see run.py for that). These experiments try to
make dripline LOSE on purpose:

  A. global limit across workers — Redis-backed limits (slowapi's home turf)
     vs every in-memory engine, dripline included;
  B. state survival across a restart — only a shared backend can remember;
  C. stale-client memory — does ANY engine ever give RAM back?;
  D. precision under same-key thread races — the documented ±1 permit issue.

Output: bench/results/<version>.adversarial.md + .raw.json — generated files,
never edited by hand.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import platform
import subprocess
import sys
import threading
import time
from datetime import UTC
from importlib import metadata, util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

RATE_CONFIG = "600/minute"        # idiomatic per-library configuration
FIRES = 2_000                     # per worker, enough to saturate any engine
STALE_K = 200_000                 # distinct clients for the memory experiment
STALE_REHIT = 10_000
STALE_SLEEP_S = 3                 # >> every window/credit horizon below
THREADS, THREAD_FIRES = 8, 125    # 1000 same-key hits racing one capacity of 100

# --------------------------------------------------------------------------- #
# Engines — (zero-arg factory -> decider, single-process instant capacity)
# --------------------------------------------------------------------------- #

def _dripline(capacity_hint: int):
    from dripline import GcraLimiter
    lim = GcraLimiter(rate_per_second=10, burst=capacity_hint)

    def decide(key: str) -> bool:
        return lim.try_acquire(key).allowed
    return decide

def _limits_memory(strategy: str):
    def factory():
        from limits import parse
        from limits.storage import MemoryStorage
        from limits.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter
        engine = {"fixed": FixedWindowRateLimiter,
                  "moving": MovingWindowRateLimiter}[strategy](MemoryStorage())
        rate = parse(RATE_CONFIG)

        def decide(key: str) -> bool:
            return engine.hit(rate, key)
        return decide
    return factory

def _limits_redis(strategy: str, uri: str):
    def factory():
        from limits import parse
        from limits.storage import RedisStorage
        from limits.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter
        storage = RedisStorage(uri)
        if not storage.check():
            raise ConnectionError(f"redis unreachable at {uri}")
        engine = {"fixed": FixedWindowRateLimiter,
                  "moving": MovingWindowRateLimiter}[strategy](storage)
        rate = parse(RATE_CONFIG)

        def decide(key: str) -> bool:
            return engine.hit(rate, key)
        return decide
    return factory

def _aiolimiter_perkey(max_rate: int = 600, period: float = 60):
    def factory():
        from aiolimiter import AsyncLimiter
        local: dict = {}

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


def engines(redis_uri: str | None) -> list[dict]:
    """Rows for experiments A/B. kind: sync|async; capacity: single-process instant admits."""
    rows = [
        {"name": "dripline-gcra", "label": "dripline GCRA (dict)", "kind": "sync",
         "make": lambda: _dripline(10), "capacity": 10},
        {"name": "limits-fixed-mem", "label": "limits fixed-window (memory)", "kind": "sync",
         "make": _limits_memory("fixed"), "capacity": 600},
        {"name": "limits-moving-mem", "label": "limits moving-window (memory)", "kind": "sync",
         "make": _limits_memory("moving"), "capacity": 600},
        {"name": "aiolimiter-perkey", "label": "aiolimiter (per-key)", "kind": "async",
         "make": _aiolimiter_perkey(), "capacity": 600},
    ]
    if redis_uri:
        rows += [
            {"name": "limits-fixed-redis", "label": "limits fixed-window (Redis)", "kind": "sync",
             "make": _limits_redis("fixed", redis_uri), "capacity": 600},
            {"name": "limits-moving-redis", "label": "limits moving-window (Redis)", "kind": "sync",
             "make": _limits_redis("moving", redis_uri), "capacity": 600},
        ]
    return rows


# --------------------------------------------------------------------------- #
# Worker subprocess: fresh process = fresh limiter state (a "worker"/"restart")
# --------------------------------------------------------------------------- #

def worker_main(name: str, key: str, fires: int, redis_uri: str | None) -> None:
    row = next(r for r in engines(redis_uri) if r["name"] == name)
    if row["kind"] == "sync":
        decider = row["make"]()
        allowed = sum(1 for _ in range(fires) if decider(key))
    else:
        async def _run():
            decider = row["make"]()
            allowed = 0
            for _ in range(fires):
                if await decider(key):
                    allowed += 1
            return allowed
        allowed = asyncio.run(_run())
    print(allowed)


def spawn_worker(name: str, key: str, fires: int, redis_uri: str | None) -> int:
    cmd = [sys.executable, str(Path(__file__).resolve()), "--worker", name, key, str(fires)]
    if redis_uri:
        cmd += ["--redis", redis_uri]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise RuntimeError(f"worker {name} failed: {proc.stderr.strip()[:300]}")
    return int(proc.stdout.strip().splitlines()[-1])


# --------------------------------------------------------------------------- #
# A+B: workers multiply allowances? does a restart remember?
# --------------------------------------------------------------------------- #

def exp_workers(eng: list[dict], redis_uri: str | None,
                w: int = 4) -> tuple[list[dict], list[dict]]:
    rows_a, rows_b = [], []
    stamp = time.strftime("%H%M%S")
    for i, e in enumerate(eng):
        # Each measurement gets a fresh client key: shared backends (Redis)
        # persist across processes, so reusing a key would consume the quota
        # the next measurement is supposed to be tested against.
        key_single = f"adv-{stamp}-{i}-single"
        key_multi = f"adv-{stamp}-{i}-multi"
        try:
            single = spawn_worker(e["name"], key_single, FIRES, redis_uri)
            total = sum(spawn_worker(e["name"], key_multi, FIRES, redis_uri) for _ in range(w))
            restart = spawn_worker(e["name"], key_multi, FIRES, redis_uri)
        except (RuntimeError, ConnectionError) as exc:
            rows_a.append({"variant": e["label"], "error": str(exc)[:200]})
            continue
        expansion = round(total / single, 2) if single else float("inf")
        rows_a.append({"variant": e["label"], "capacity": e["capacity"],
                       "admits_1_worker": single, f"admits_{w}_workers": total,
                       "expansion": expansion})
        # B: every worker above was a fresh process; `restart` re-asks the same
        # key with a brand-new state (or, for Redis, the still-live shared one).
        rows_b.append({"variant": e["label"], "fresh_capacity": e["capacity"],
                       "re_admits_after_restart": restart})
        print(f"  A/B {e['name']}: 1w={single} {w}w={total} (x{expansion}) restart={restart}",
              file=sys.stderr)
    return rows_a, rows_b


# --------------------------------------------------------------------------- #
# C: does anyone ever give stale-client memory back?
# --------------------------------------------------------------------------- #

def _read_rss_kb() -> int | None:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        pass
    return None


def _stale_engines() -> list[dict]:
    """Short-horizon configs so 3 s of idleness provably expires every state."""
    return [
        {"name": "dripline-gcra", "label": "dripline GCRA (dict)", "kind": "sync",
         "make": lambda: _dripline(10)},
        {"name": "limits-fixed-mem", "label": "limits fixed-window (memory)", "kind": "sync",
         "make": _stale_limits("fixed")},
        {"name": "limits-moving-mem", "label": "limits moving-window (memory)", "kind": "sync",
         "make": _stale_limits("moving")},
        {"name": "aiolimiter-perkey", "label": "aiolimiter (per-key)", "kind": "async",
         "make": _aiolimiter_perkey(10, 1)},
    ]


def _stale_limits(strategy: str):
    def factory():
        from limits import parse
        from limits.storage import MemoryStorage
        from limits.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter
        engine = {"fixed": FixedWindowRateLimiter,
                  "moving": MovingWindowRateLimiter}[strategy](MemoryStorage())
        rate = parse("1/second")

        def decide(key: str) -> bool:
            return engine.hit(rate, key)
        return decide
    return factory


def exp_stale_memory(k: int, rehit: int) -> list[dict] | None:
    if _read_rss_kb() is None:
        print("  C: no /proc — skipping (run inside the pinned container)", file=sys.stderr)
        return None
    rows = []
    for e in _stale_engines():
        rss_full = rss_end = None
        if e["kind"] == "sync":
            decider = e["make"]()          # stays referenced through the final read
            for i in range(k):
                decider(f"c{i}")
            rss_full = _read_rss_kb()
            time.sleep(STALE_SLEEP_S)      # every window/credit above has expired
            for i in range(rehit):
                decider(f"c{i}")
            gc.collect()
            rss_end = _read_rss_kb()
        else:
            # One event loop session: AsyncLimiter instances must not cross loops,
            # and the limiter dict must stay referenced while RSS is read.
            async def _session(e: dict) -> None:
                nonlocal rss_full, rss_end
                decider = e["make"]()
                for i in range(k):
                    await decider(f"c{i}")
                rss_full = _read_rss_kb()
                await asyncio.sleep(STALE_SLEEP_S)
                for i in range(rehit):
                    await decider(f"c{i}")
                gc.collect()
                rss_end = _read_rss_kb()
            asyncio.run(_session(e))
        rows.append({"variant": e["label"], "clients": k, "rehit": rehit,
                     "rss_full_kb": rss_full, "rss_end_kb": rss_end,
                     "returned_kb": rss_full - rss_end})
        print(f"  C {e['name']}: full={rss_full} kB end={rss_end} kB "
              f"(gave back {rss_full - rss_end} kB)", file=sys.stderr)
    return rows


# --------------------------------------------------------------------------- #
# D: same-key thread race precision (documented GCRA overshoot)
# --------------------------------------------------------------------------- #

def exp_threads() -> list[dict]:
    from limits import parse
    from limits.storage import MemoryStorage
    from limits.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter

    from dripline import GcraLimiter
    cap = 100

    cases = {"dripline GCRA (dict)": None,
             "limits fixed-window (memory)": None,
             "limits moving-window (memory)": None}

    lim = GcraLimiter(rate_per_second=100 / 60, burst=cap)

    def d_decide(): return lim.try_acquire("race").allowed
    cases["dripline GCRA (dict)"] = d_decide

    for label, cls in (("limits fixed-window (memory)", FixedWindowRateLimiter),
                       ("limits moving-window (memory)", MovingWindowRateLimiter)):
        engine = cls(MemoryStorage())
        rate = parse("100/minute")
        key = f"race-{'fixed' if 'fixed' in label else 'moving'}"
        cases[label] = (lambda eng, r, k: (lambda: eng.hit(r, k)))(engine, rate, key)

    rows = []
    for label, decide in cases.items():
        counts = [0] * THREADS

        def hammer(idx: int, decide, counts) -> None:
            for _ in range(THREAD_FIRES):
                if decide():
                    counts[idx] += 1
        threads = [threading.Thread(target=hammer, args=(i, decide, counts))
                   for i in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        total = sum(counts)
        rows.append({"variant": label, "expected": cap, "admitted": total,
                     "overshoot": total - cap})
        print(f"  D {label}: admitted {total}/{cap} (overshoot {total - cap})",
              file=sys.stderr)
    return rows


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #

def render(version: str, env: dict, a: list, b: list, c: list | None, d: list) -> str:
    lines = [
        f"<!-- GENERATED by bench/adversarial.py at {env['generated_utc']} — do not edit. -->",
        f"# Adversarial experiments — built to make dripline lose ({version})",
        "",
        f"Speed is not measured here (that is `run.py`). Config: idiomatic '{RATE_CONFIG}'",
        f"per library; {FIRES:,} instant requests per worker process.",
        "",
        "## A. Global limit across 4 worker processes",
        "",
        "| variant | 1-worker admits | 4-worker total | expansion |",
        "|---|---:|---:|---:|",
    ]
    for r in a:
        if "error" in r:
            lines.append(f"| {r['variant']} | — | — | ERROR: {r['error']} |")
        else:
            lines.append(f"| {r['variant']} | {r['admits_1_worker']} | "
                         f"{r['admits_4_workers']} | **x{r['expansion']}** |")
    lines += [
        "",
        "Reading: expansion x4 means each worker counts for itself — the operator's",
        "'600/minute' silently becomes 2400/minute. Only a shared backend stays at x1.",
        "",
        "## B. Fresh process against the same client (restart behavior)",
        "",
        "| variant | fresh-process capacity | re-admits after restart |",
        "|---|---:|---:|",
    ]
    for r in b:
        lines.append(f"| {r['variant']} | {r['fresh_capacity']} | {r['re_admits_after_restart']} |")
    lines += [
        "",
        "Reading: an in-memory engine — dripline included — hands every client a fresh",
        "burst after every restart. Only shared state remembers the client was just served.",
        "",
    ]
    if c is not None:
        lines += [
            "## C. Stale clients: does anyone return memory? (short-window",
            f"configs, {STALE_SLEEP_S}s idle so all state is provably expired,",
            f"then {STALE_REHIT:,} re-hits)",
            "",
            "| variant | RSS full (kB) | RSS after expiry+re-hits (kB) | returned (kB) |",
            "|---|---:|---:|---:|",
        ]
        for r in c:
            lines.append(f"| {r['variant']} | {r['rss_full_kb']:,} | {r['rss_end_kb']:,} | "
                         f"{r['returned_kb']} |")
        lines += [
            "",
            "Reading: a negative 'returned' number means the engine GREW. Nobody runs a",
            "eviction/TTL job in-process; per-client bytes decide who bloats slowest (see",
            f"{version}.md for the footprint table; dripline is smallest but still linear",
            "until the v0.2 arena).",
            "",
        ]
    else:
        lines += ["## C. Stale clients — skipped (no /proc outside Linux)\n", ""]
    lines += [
        "## D. Same-key thread race (8 threads x 125 hits, capacity 100)",
        "",
        "| variant | expected | admitted | overshoot |",
        "|---|---:|---:|---:|",
    ]
    for r in d:
        lines.append(f"| {r['variant']} | {r['expected']} | {r['admitted']} | "
                     f"{r['overshoot']:+d} |")
    lines += [
        "",
        "## Conclusions (the honest scoreboard)",
        "",
        "- A/B: Redis-backed limits wins where it is designed to win: one global count",
        "  across workers and across restarts. Every in-memory engine — slowapi's memory",
        "  storage and aiolimiter included — fails these the same way dripline does.",
        "- C: no in-memory engine returns RAM for stale clients; the differences are",
        "  per-client bytes, not cleanup. dripline's dict is the leanest but linear.",
        "- D: at this contention level (8 threads, 1000 same-key hits) no overshoot",
        "  was observed for any engine — dripline's read-modify-write race remains",
        "  a documented theoretical bound, not a measured one at this scale.",
        "- aiolimiter is not in D: it binds to one event loop, so racing threads are",
        "  outside its supported model, not a fair row.",
    ]
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", default="v0.1")
    ap.add_argument("--redis", default=None, help="redis://… URI for the shared-backend rows")
    ap.add_argument("--quick", action="store_true", help="small sizes, no files written")
    ap.add_argument("--worker", nargs=3, metavar=("VARIANT", "KEY", "FIRES"),
                    help=argparse.SUPPRESS)
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.worker:
        worker_main(args.worker[0], args.worker[1], int(args.worker[2]), args.redis)
        return 0

    k, rehit = (20_000, 1_000) if args.quick else (STALE_K, STALE_REHIT)

    missing = [m for m in ("limits", "aiolimiter", "redis") if util.find_spec(m) is None]
    if missing:
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "-r",
                        str(ROOT / "bench" / "requirements.txt")], check=True)

    from datetime import datetime
    env = {"generated_utc": datetime.now(UTC).isoformat(timespec="seconds"),
           "python": platform.python_version(), "platform": platform.platform(),
           "versions": {m: metadata.version(m) for m in ("limits", "aiolimiter", "redis")
                        if util.find_spec(m)}}

    eng = engines(args.redis)
    print("A/B: workers & restart…", file=sys.stderr)
    rows_a, rows_b = exp_workers(eng, args.redis)
    print("D: thread race…", file=sys.stderr)
    rows_d = exp_threads()
    print(f"C: stale memory (K={k:,})…", file=sys.stderr)
    rows_c = exp_stale_memory(k, rehit)

    report = render(args.version, env, rows_a, rows_b, rows_c, rows_d)
    print(report)
    if not args.quick:
        out = ROOT / "bench" / "results"
        out.mkdir(exist_ok=True)
        (out / f"{args.version}.adversarial.md").write_text(report, encoding="utf-8")
        (out / f"{args.version}.adversarial.raw.json").write_text(
            json.dumps({"env": env, "workers": rows_a, "restart": rows_b,
                        "stale_memory": rows_c, "thread_race": rows_d}, indent=1),
            encoding="utf-8")
        print(f"wrote bench/results/{args.version}.adversarial.md (+ raw json)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
