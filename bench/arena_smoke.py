#!/usr/bin/env python3
"""v0.2 exit measurements: the fixed arena under client pressure.

Three checks, one report (`bench/results/v0.2.arena.md`, generated — never
hand-edited):

1. FOOTPRINT — distinct clients through a 12M-slot arena (K = 1M / 5M / 10M),
   RSS sampled in ONE process as the table fills: residency tracks touched
   slots and stops at the pre-allocated cap (12M x 32 B = 375 MiB). A dict
   core reference is measured at 1M for scale (10M would need ~1.2 GiB).
2. CHURN — 10M distinct clients cycled through a 1M-slot arena (10x
   oversubscription): RSS must stay pinned at the 32 MiB cap while a still
   ACTIVE client's budget keeps holding exactly (active slots are never
   recycled — only drained ones are, and drained == fresh for GCRA).
3. GUARD — the standard correctness check: 1000/min burst 100 admits
   exactly 100 of 300 instant requests.

Usage (inside the pinned container):
    python /bench/bench/arena_smoke.py            # writes results files
    python /bench/bench/arena_smoke.py --quick    # small Ks, stdout only
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

FULL_KS = (1_000_000, 5_000_000, 10_000_000)
QUICK_KS = (10_000, 50_000, 100_000)
CACHE_SLOTS = 131_072        # mirrors the default fingerprint cache
HAMMERS = 5000               # interleaved requests for the active client


def read_rss_kb() -> int | None:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        pass
    return None


def footprint_child(ks: tuple[int, ...]) -> None:
    """One process, one 12M-slot arena: RSS as the table fills past 10M clients."""
    from dripline import ArenaGcraLimiter
    rss0 = read_rss_kb()
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100,
                           slots=12_000_000)
    rows = []
    kmax = ks[-1]
    t0 = time.perf_counter()
    done = 0
    while done < kmax:
        target = next(k for k in ks if k > done)
        for i in range(done, target):
            lim.try_acquire(f"c{i}")
        done = target
        rows.append({"clients": done, "rss_kb": read_rss_kb(),
                     "elapsed_s": round(time.perf_counter() - t0, 1)})
    lim.close()
    rss1 = read_rss_kb()
    print(json.dumps({"kind": "footprint", "rss_before_kb": rss0,
                      "rows": rows, "rss_after_close_kb": rss1}))


def dict_reference_child(k: int) -> None:
    """Scalar dict core at the same Ks for scale (memory that grows)."""
    from dripline import GcraLimiter
    rss0 = read_rss_kb()
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    for i in range(k):
        lim.try_acquire(f"c{i}")
    gc.collect()
    print(json.dumps({"kind": "dict", "clients": k,
                      "rss_delta_kb": (read_rss_kb() or 0) - (rss0 or 0)}))


def churn_child(k: int, slots: int) -> None:
    """10x oversubscription: RSS pinned at the cap, active budgets still exact.

    One client is hammered every k/500 churn keys, so its TAT stays ahead of
    the clock for the whole run. GCRA admits at most burst + rate x elapsed;
    if churn ever stole an ACTIVE slot, the client would re-bank and blow the
    bound by ~burst periods — the assert-equivalent lives in the report.
    """
    from dripline import ArenaGcraLimiter
    rss0 = read_rss_kb()
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=slots)
    active = "active-client"
    admitted = 0
    stride = max(1, k // HAMMERS)
    t0 = time.perf_counter()
    mono0 = time.monotonic_ns()
    for i in range(k):
        lim.try_acquire(f"churn-{i}")
        if i % stride == 0 and lim.try_acquire(active).allowed:
            admitted += 1
    mono1 = time.monotonic_ns()
    gc.collect()
    rss = read_rss_kb()
    # GCRA admits at most burst + rate x elapsed on ONE continuous slot;
    # a stolen slot re-banks ~burst admits and blows the bound. Hammering
    # is faster than the drip, so the count rides the bound.
    bound = 1 + (mono1 - mono0 + lim.burst_ns) // lim.period_ns + 3  # jitter margin
    lim.close()
    print(json.dumps({"kind": "churn", "clients": k, "slots": slots,
                      "rss_delta_kb": (rss or 0) - (rss0 or 0),
                      "active_admits": admitted, "active_bound": bound,
                      "elapsed_s": round(time.perf_counter() - t0, 1)}))


def run_child(mode: str, arg: str) -> dict:
    script = str(Path(__file__).resolve())
    out = subprocess.run([sys.executable, script, "--child", mode, arg],
                         capture_output=True, text=True, timeout=3600)
    if out.returncode != 0:
        return {"error": out.stderr.strip()[-400:]}
    return json.loads(out.stdout.strip().splitlines()[-1])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true", help="small Ks, stdout only")
    ap.add_argument("--child", nargs=2, metavar=("MODE", "ARG"),
                    help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.child:
        mode, arg = args.child
        if mode == "footprint":
            footprint_child(tuple(int(x) for x in arg.split(",")))
        elif mode == "dict":
            dict_reference_child(int(arg))
        elif mode == "churn":
            k, slots = (int(x) for x in arg.split(","))
            churn_child(k, slots)
        return 0

    ks = QUICK_KS if args.quick else FULL_KS
    print(f"footprint (12M-slot arena, Ks {ks})…", file=sys.stderr)
    fp = run_child("footprint", ",".join(str(k) for k in ks))
    print("dict reference…", file=sys.stderr)
    dict_rows = [run_child("dict", str(k)) for k in (ks[0],) if not args.quick]
    print("churn (10M clients through 1M slots)…", file=sys.stderr)
    churn_k, churn_slots = (100_000, 10_000) if args.quick else (10_000_000, 1_000_000)
    churn = run_child("churn", f"{churn_k},{churn_slots}")

    # Budget guard (cheap, in-process).
    from dripline import ArenaGcraLimiter
    guard_lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=1024)
    guard_allowed = sum(guard_lim.try_acquire("g").allowed for _ in range(300))
    guard_lim.close()
    guard_ok = guard_allowed == 100

    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        f"<!-- GENERATED by bench/arena_smoke.py at {stamp} — do not edit. -->",
        "# v0.2 arena — exit measurements (footprint, churn, correctness)",
        "",
        f"Environment: {platform.python_implementation()} "
        f"{platform.python_version()} · {platform.platform()}",
        "",
        "## 1. Footprint — one 12M-slot arena (375 MiB pre-allocated), RSS as it fills",
        "",
        "| distinct clients | RSS (MiB) | fill time (s) |",
        "|---:|---:|---:|",
    ]
    for r in fp.get("rows", []):
        rss = "—" if r["rss_kb"] is None else f"{r['rss_kb'] / 1024:,.1f}"
        lines.append(f"| {r['clients']:,} | {rss} | {r['elapsed_s']} |")
    lines += [
        "",
        "Reference, the v0.1 dict core at the smallest K (grows without bound; the",
        "10M point is deliberately not run for it):",
        "",
    ]
    for d in dict_rows:
        if "error" in d:
            lines.append(f"- dict @ {ks[0]:,}: child failed: {d['error']}")
        else:
            lines.append(
                f"- dict @ {d['clients']:,} clients: **+{d['rss_delta_kb'] / 1024:,.1f} "
                f"MiB delta** (~{d['rss_delta_kb'] * 1024 / d['clients']:.0f} B/client, "
                "grows without bound). The arena column above is TOTAL RSS: random "
                "placement touches whole 4 KiB pages, so the pre-allocated region "
                "becomes resident early and then NEVER moves — size it to peak once.")
    ok_churn = False
    lines += [
        "",
        f"## 2. Churn — {churn_k:,} distinct clients through a "
        f"{churn_slots:,}-slot arena (10x oversubscribed)",
        "",
    ]
    if "error" in churn:
        lines.append(f"child failed: {churn['error']}")
    else:
        cap_mib = churn["slots"] * 32 / 1024 / 1024
        expected_kb = (churn["slots"] * 32 + CACHE_SLOTS * 24) / 1024 + 2048
        rss_ok = churn["rss_delta_kb"] <= expected_kb
        budget_ok = churn["active_admits"] <= churn["active_bound"]
        ok_churn = rss_ok and budget_ok
        lines += [
            f"- RSS delta: **+{churn['rss_delta_kb'] / 1024:,.1f} MiB** "
            f"(cap {expected_kb / 1024:,.1f} MiB: {cap_mib:,.1f} arena + "
            f"{CACHE_SLOTS * 24 / 1024 / 1024:,.1f} fp cache + overhead — "
            f"{'pinned' if rss_ok else 'EXCEEDED'})",
            f"- client hammered throughout the churn: **{churn['active_admits']} "
            f"admits ≤ bound {churn['active_bound']}** "
            f"({'slot never recycled' if budget_ok else 'MISMATCH — active slot stolen'})",
            f"- churn pass: {churn['elapsed_s']} s for {churn['clients']:,} clients",
        ]
    lines += [
        "",
        "## 3. Correctness guard",
        "",
        f"- 300 instant requests, 1000/min burst 100 → **{guard_allowed} admits "
        f"(expected 100, {'pass' if guard_ok else 'FAIL'})**",
        "",
        "## Notes",
        "",
        "- RSS = VmRSS from /proc/self/status inside the pinned container;",
        "  untouched arena pages are not resident, so the footprint column is",
        "  'memory actually used', bounded by the pre-allocated 375 MiB.",
        "- Recycling is GCRA self-ageing: a slot whose TAT fell behind the clock",
        "  is indistinguishable from a fresh client, so claiming it is exact,",
        "  not approximate. Active slots (TAT ahead) are never claimable.",
        "- Lost-update races between two workers hitting one slot simultaneously",
        "  can overshoot by one permit per race (documented limitation), but the",
        "  STRUCTURAL xW over-admission of per-worker limiters is gone — see",
        "  the scenario scoreboard for the multi-worker admit counts.",
    ]
    report = "\n".join(lines) + "\n"
    print(report)
    if not args.quick:
        out = ROOT / "bench" / "results"
        out.mkdir(exist_ok=True)
        (out / "v0.2.arena.md").write_text(report, encoding="utf-8")
        (out / "v0.2.arena.raw.json").write_text(
            json.dumps({"env": {"python": platform.python_version(),
                                "platform": platform.platform()},
                        "footprint": fp, "dict": dict_rows, "churn": churn,
                        "guard_allowed": guard_allowed}, indent=1),
            encoding="utf-8")
        print("wrote bench/results/v0.2.arena.md (+ raw json)", file=sys.stderr)
    return 0 if (guard_ok and ok_churn) else 1


if __name__ == "__main__":
    sys.exit(main())
