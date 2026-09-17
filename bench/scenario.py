#!/usr/bin/env python3
"""Scenario benchmark: one machine, multiple cores, sustained load, no Redis.

The standing format for every future version: Python rivals first, then the
Rust champion (governor). Two scenarios, each a 60-second max-speed hammer:

  loose — budget 1000 admits / 10 s per client (admit-friendly)
  tight — budget 10 admits / 10 s per client (reject-dominated)

Topology (the Redis-less deployment model): W worker processes, each with its
OWN limiter instance, running simultaneously on pinned cores. We report total
sustained decisions/s across workers plus admit counts — admits prove the
configured budget actually held (ceiling per client ≈ burst + rate x duration).

    python bench/scenario.py                       # both scenarios, 60 s each
    python bench/scenario.py --scenario tight      # one scenario
    python bench/scenario.py --quick               # 3 s smoke, no files
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
from importlib import util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

WORKERS = 4
CLIENTS = 100
DURATION_S = 60.0        # release protocol
FAST_DURATION_S = 20.0   # fast default: 2 window rolls, millions of decisions
BLOCK = 1000          # decisions between deadline checks (clock cost amortized)

SCENARIOS = {
    "loose": dict(
        label="1000 admits per 10 s per client",
        rate_per_period=1000, period_s=10, burst=1000,
        dripline=(1000 / 10, 1000), limits_str="1000/10seconds",
        aiolimiter=(1000, 10), ceiling=1000 + (1000 / 10) * DURATION_S),
    "tight": dict(
        label="10 admits per 10 s per client",
        rate_per_period=10, period_s=10, burst=10,
        dripline=(10 / 10, 10), limits_str="10/10seconds",
        aiolimiter=(10, 10), ceiling=10 + (10 / 10) * DURATION_S),
}

ENGINE_ORDER = ["no-op-floor", "dripline-gcra", "limits-fixed-window",
                "limits-moving-window", "aiolimiter-perkey", "governor-rust"]


def decider_for(engine: str, sc: dict):
    if engine == "no-op-floor":
        def decide(key: str) -> bool:
            return True
        return decide
    if engine == "dripline-gcra":
        from dripline import GcraLimiter
        rate, burst = sc["dripline"]
        lim = GcraLimiter(rate_per_second=rate, burst=burst)
        return lambda key: lim.try_acquire(key).allowed
    if engine in ("limits-fixed-window", "limits-moving-window"):
        from limits import parse
        from limits.storage import MemoryStorage
        from limits.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter
        cls = {"limits-fixed-window": FixedWindowRateLimiter,
               "limits-moving-window": MovingWindowRateLimiter}[engine]
        strat = cls(MemoryStorage())
        rate = parse(sc["limits_str"])
        return lambda key: strat.hit(rate, key)
    if engine == "aiolimiter-perkey":
        from aiolimiter import AsyncLimiter
        max_rate, period = sc["aiolimiter"]
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
    raise ValueError(engine)


def worker(engine: str, scenario_name: str, duration: float, clients: int) -> None:
    sc = SCENARIOS[scenario_name]
    keys = [f"c{i}" for i in range(clients)]
    decider = decider_for(engine, sc)
    decisions = admits = 0
    idx = 0
    if engine == "aiolimiter-perkey":
        async def _run():
            nonlocal decisions, admits, idx
            deadline = time.perf_counter() + duration
            while time.perf_counter() < deadline:
                for _ in range(BLOCK):
                    if await decider(keys[idx]):
                        admits += 1
                    decisions += 1
                    idx = (idx + 1) % clients
        t0 = time.perf_counter()
        asyncio.run(_run())
        elapsed = time.perf_counter() - t0
    else:
        t0 = time.perf_counter()
        deadline = t0 + duration
        while time.perf_counter() < deadline:
            for _ in range(BLOCK):
                if decider(keys[idx]):
                    admits += 1
                decisions += 1
                idx = (idx + 1) % clients
        elapsed = time.perf_counter() - t0
    print(json.dumps({"engine": engine, "scenario": scenario_name,
                      "decisions": decisions, "admits": admits,
                      "admit_ratio": round(admits / decisions, 5),
                      "dec_per_s": round(decisions / elapsed),
                      "elapsed_s": round(elapsed, 3)}))


def rust_bin() -> Path | None:
    if os.name != "posix":
        return None
    guess = ROOT / "bench" / "champions" / "rust" / "target" / "release" / "governor_bench"
    return guess if guess.exists() else None


def run_engine(engine: str, scenario_name: str, duration: float,
               workers: int, bin_path: Path | None) -> dict:
    sc = SCENARIOS[scenario_name]
    if engine == "governor-rust":
        cmd = [str(bin_path), "--scenario-worker", str(sc["rate_per_period"]),
               str(sc["burst"]), str(sc["period_s"]), str(duration), str(CLIENTS)]
    else:
        cmd = [sys.executable, str(Path(__file__).resolve()), "--worker",
               engine, scenario_name, str(duration)]
    procs = [subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True) for _ in range(workers)]
    rows = []
    for p in procs:
        out, err = p.communicate(timeout=duration + 120)
        if p.returncode != 0:
            raise RuntimeError(f"{engine} worker failed: {err.strip()[:200]}")
        rows.append(json.loads(out.strip().splitlines()[-1]))
    total_dec = sum(r["decisions"] for r in rows)
    total_adm = sum(r["admits"] for r in rows)
    worst_admits_per_client = max(r["admits"] for r in rows) / CLIENTS
    return {"engine": engine, "scenario": scenario_name, "workers": workers,
            "total_dec_per_s": round(total_dec / duration),
            "per_worker_dec_per_s": [r["dec_per_s"] for r in rows],
            "admits": total_adm,
            "admit_ratio": round(total_adm / total_dec, 5) if total_dec else None,
            "worst_admits_per_client": round(worst_admits_per_client, 1),
            "worker_rows": rows}


def render(version: str, results: list[dict], env: dict, params: dict) -> str:
    from datetime import datetime, timezone
    lines = [
        f"<!-- GENERATED by bench/scenario.py at "
        f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} — do not edit. -->",
        f"# Scenario benchmark — one machine, multiple cores ({version})",
        "",
        f"Standing format: dripline vs Python rivals vs the Rust champion (governor).",
        f"{params['workers']} worker processes, each with its OWN limiter (the Redis-less",
        f"deployment model), {params['workers']} pinned cores, {params['clients']} clients",
        f"round-robin per worker, max-speed load for {params['duration']:.0f} s per",
        "scenario. No shared backend anywhere, by design.",
        "",
        f"Environment: {env['python']} · {env['cpu']} · {env['platform']}",
        "",
    ]
    summary = {}
    for name in ("loose", "tight"):
        sc = SCENARIOS[name]
        rows = [r for r in results if r["scenario"] == name]
        lines += [
            f"## Scenario {name.upper()} — {sc['label']}",
            "",
            f"Theoretical admit ceiling per client over {params['duration']:.0f} s: "
            f"~{sc['ceiling']:.0f} (burst + sustained). Admits far below it mean the",
            "engine rejected everything beyond budget; admits above it would be a bug.",
            "",
            "| engine | total dec/s (all workers) | per-worker dec/s | admits | admit % | admits/client (worst) |",
            "|---|---:|---|---:|---:|---:|",
        ]
        for r in sorted(rows, key=lambda x: ENGINE_ORDER.index(x["engine"])):
            pw = " / ".join(f"{x:,.0f}" for x in r["per_worker_dec_per_s"])
            adm = "—" if r["engine"] == "no-op-floor" else f"{r['admits']:,}"
            ratio = "—" if r["engine"] == "no-op-floor" else f"{r['admit_ratio'] * 100:.2f}%"
            apc = "—" if r["engine"] == "no-op-floor" else f"{r['worst_admits_per_client']:.0f}"
            lines.append(f"| {r['engine']} | {r['total_dec_per_s']:,} | {pw} | {adm} | "
                         f"{ratio} | {apc} |")
            summary.setdefault(r["engine"], {})[name] = r
        lines.append("")

    lines += ["## Summary — the standing scoreboard", "",
              "| engine | loose dec/s | loose admit % | tight dec/s | tight admit % |",
              "|---|---:|---:|---:|---:|"]
    for engine in ENGINE_ORDER:
        s = summary.get(engine)
        if not s:
            continue
        cells = []
        for name in ("loose", "tight"):
            r = s.get(name)
            if r is None:
                cells += ["—", "—"]
            elif engine == "no-op-floor":
                cells += [f"{r['total_dec_per_s']:,}", "—"]
            else:
                cells += [f"{r['total_dec_per_s']:,}", f"{r['admit_ratio'] * 100:.2f}%"]
        lines.append(f"| {engine} | " + " | ".join(cells) + " |")

    ours = summary.get("dripline-gcra", {}).get("loose")
    best_py = None
    for e in ("aiolimiter-perkey", "limits-fixed-window", "limits-moving-window"):
        r = summary.get(e, {}).get("loose")
        if r and (best_py is None or r["total_dec_per_s"] > best_py["total_dec_per_s"]):
            best_py = r
    rust = summary.get("governor-rust", {}).get("loose")
    lines += ["", "Positioning (loose-scenario throughput):", ""]
    if ours and best_py:
        lines.append(f"- vs best Python rival ({best_py['engine']}): "
                     f"**{ours['total_dec_per_s'] / best_py['total_dec_per_s']:.2f}x**")
    if ours and rust:
        lines.append(f"- vs Rust governor: "
                     f"**{ours['total_dec_per_s'] / rust['total_dec_per_s']:.2f}x** "
                     f"(governor is {rust['total_dec_per_s'] / ours['total_dec_per_s']:.1f}x faster)")
    lines += [
        "",
        "## Notes",
        "",
        "- Worker count = core count; every engine faces the identical client cycle",
        "  and duration. Driver clock checks are amortized (1 per 1000 decisions).",
        (f"- This ran the FAST protocol ({params['duration']:.0f} s per scenario); version"
         if params["duration"] < DURATION_S else
         f"- Release protocol ({params['duration']:.0f} s per scenario); fast runs"
         " use 20 s"),
        "- governor runs the same scenario via its --scenario-worker mode: identical",
        "  quota semantics (unit period = window/amount, capacity = burst).",
        "- This is throughput + budget compliance under sustained load, not per-call",
        "  latency (that lives in run.py / champions tables).",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scenario", choices=("loose", "tight", "both"), default="both")
    ap.add_argument("--duration", type=float, default=None,
                    help="override duration (default: fast 20 s, release 60 s)")
    ap.add_argument("--release", action="store_true",
                    help="release protocol: 60 s scenarios, canonical "
                         "vX.Y.scenario.md output (default: fast 20 s, "
                         "vX.Y.scenario.fast.md)")
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument("--version", default="v0.1")
    ap.add_argument("--quick", action="store_true", help="3 s smoke, no files")
    ap.add_argument("--worker", nargs=3, metavar=("ENGINE", "SCENARIO", "DURATION"),
                    help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.worker:
        worker(args.worker[0], args.worker[1], float(args.worker[2]), CLIENTS)
        return 0

    if args.quick:
        args.duration, args.workers = 3.0, 2
    if args.duration is None:
        args.duration = DURATION_S if args.release else FAST_DURATION_S
    SCENARIOS["loose"]["ceiling"] = 1000 + 100 * args.duration
    SCENARIOS["tight"]["ceiling"] = 10 + 1 * args.duration

    if [m for m in ("limits", "aiolimiter") if util.find_spec(m) is None]:
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "-r",
                        str(ROOT / "bench" / "requirements.txt")], check=True)

    cpu = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        cpu = platform.processor() or "unknown"
    env = {"python": platform.python_version(), "cpu": cpu,
           "platform": platform.platform()}
    bin_path = rust_bin()
    engines = [e for e in ENGINE_ORDER if e != "governor-rust" or bin_path]
    if "governor-rust" not in engines:
        print("governor binary unavailable — Rust row skipped (build it first)",
              file=sys.stderr)

    names = ("loose", "tight") if args.scenario == "both" else (args.scenario,)
    results = []
    for name in names:
        for engine in engines:
            print(f"{name}/{engine}…", file=sys.stderr)
            results.append(run_engine(engine, name, args.duration, args.workers, bin_path))
            r = results[-1]
            print(f"  -> {r['total_dec_per_s']:,} dec/s, admits {r['admits']:,}",
                  file=sys.stderr)

    report = render(args.version, results, env,
                    {"workers": args.workers, "clients": CLIENTS,
                     "duration": args.duration})
    print(report)
    if not args.quick:
        suffix = "" if args.release else ".fast"
        out = ROOT / "bench" / "results"
        out.mkdir(exist_ok=True)
        raw_path = out / f"{args.version}.scenario{suffix}.raw.json"
        if raw_path.exists():
            # Merge with previously run scenarios so loose/tight can run as
            # separate invocations yet land in one report.
            try:
                prev = json.loads(raw_path.read_text()).get("results", [])
                names_set = set(names)
                results = [r for r in prev if r.get("scenario") not in names_set] + results
                report = render(args.version, results, env,
                                {"workers": args.workers, "clients": CLIENTS,
                                 "duration": args.duration})
            except (OSError, json.JSONDecodeError):
                pass
        (out / f"{args.version}.scenario{suffix}.md").write_text(report, encoding="utf-8")
        (raw_path).write_text(
            json.dumps({"env": env, "params": {"workers": args.workers,
                                               "clients": CLIENTS,
                                               "duration": args.duration},
                        "results": results}, indent=1), encoding="utf-8")
        print(f"wrote bench/results/{args.version}.scenario{suffix}.md (+ raw json)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
