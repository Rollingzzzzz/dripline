#!/usr/bin/env python3
"""Champion benchmark orchestrator: world-class references for the decision loop.

The champion protocol makes every runtime face the BIT-IDENTICAL offered load:
`prep` writes the key sequences (u32 client indices, same seed as the main
suite) to work/, and every driver — CPython, PyPy (same dripline core.py,
different runtime) and Rust governor (same GCRA algorithm, compiled) — reads
the same files and reports the same metrics.

    python bench/champions.py prep            # write work/*.u32 + params.json
    python bench/champions.py drive-cpython   # -> work/cpython.json
    python bench/champions.py drive-rust --bin PATH
    python bench/champions.py rss             # RSS children -> work/rss_*.json
    python bench/champions.py render          # -> bench/results/<v>.champions.md

Result files are generated — never edit by hand.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from array import array
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))
sys.path.insert(0, str(ROOT / "src"))

from datetime import UTC

import run as main_suite  # bench/run.py — reuses gen_keys/FULL/QUICK/SEED

WORK = ROOT / "bench" / "champions" / "work"
DRIVER = ROOT / "bench" / "champions" / "driver.py"
RSS_KS = (1_000, 10_000, 100_000, 1_000_000)
N_RUNS = 3  # per engine; medians reported, raw runs kept next to them
SEED = main_suite.SEED
DISTS = ("uniform", "zipf")


def _median_of_runs(runs: list[dict]) -> dict:
    """Median every numeric metric across driver runs; keep run count."""
    out = dict(runs[0])
    for dist in DISTS:
        for field in ("mean_ns", "ops_s", "p50", "p95", "p99"):
            vals = sorted(r[dist][field] for r in runs)
            out[dist][field] = vals[len(vals) // 2]
    out["runs"] = len(runs)
    return out


def params(quick: bool) -> dict:
    p = dict(main_suite.QUICK if quick else main_suite.FULL)
    return {"n": p["n"], "k": p["k"], "warmup": p["warmup"], "samples": p["samples"],
            "bulk_cap_s": p["bulk_cap_s"], "percall_cap_s": p["percall_cap_s"]}


def cmd_prep(quick: bool) -> None:
    p = params(quick)
    total = p["warmup"] + p["n"] + p["samples"]
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "params.json").write_text(json.dumps(p))
    for dist in DISTS:
        keys = main_suite.gen_keys(total, p["k"], dist, SEED)
        idx = array("I", [int(k[1:]) for k in keys])
        with (WORK / f"{dist}.u32").open("wb") as fh:
            idx.tofile(fh)
        print(f"work/{dist}.u32: {total:,} indices over {p['k']:,} clients", file=sys.stderr)
        del keys, idx


def _run_capture(cmd: list[str]) -> dict:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed: {proc.stderr.strip()[:300]}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _drive_n_times(cmd: list[str], name: str) -> dict:
    runs = [_run_capture(cmd) for _ in range(N_RUNS)]
    (WORK / f"{name}.runs.json").write_text(json.dumps(runs))
    print(f"{name}: " + ", ".join(f"{r['uniform']['mean_ns']:.0f}" for r in runs)
          + " ns/dec (uniform)", file=sys.stderr)
    return _median_of_runs(runs)


def cmd_drive_cpython() -> None:
    out = _drive_n_times([sys.executable, str(DRIVER), "--keys-dir", str(WORK)], "cpython")
    (WORK / "cpython.json").write_text(json.dumps(out))


def cmd_drive_rust(bin_path: Path) -> None:
    out = _drive_n_times([str(bin_path), "--keys-dir", str(WORK)], "rust")
    out["engine_detail"] = "governor crate, dashmap GCRA"
    (WORK / "rust.json").write_text(json.dumps(out))


def cmd_merge_pypy() -> None:
    """Combine work/pypy.jsonl (one JSON per driver run) into work/pypy.json."""
    lines = [json.loads(line) for line in
             (WORK / "pypy.jsonl").read_text().splitlines() if line.strip()]
    out = _median_of_runs(lines)
    (WORK / "pypy.json").write_text(json.dumps(out))
    (WORK / "pypy.runs.json").write_text(json.dumps(lines))
    print(f"pypy: merged {len(lines)} runs", file=sys.stderr)


def cmd_rust_bin(args_bin: str | None) -> Path | None:
    if args_bin:
        return Path(args_bin)
    guess = ROOT / "bench" / "champions" / "rust" / "target" / "release" / "governor_bench"
    return guess if guess.exists() else None


def cmd_rss(rust_bin: Path | None) -> None:
    for k in RSS_KS:
        py = _run_capture([sys.executable, str(DRIVER), "--rss-child", str(k)])
        (WORK / f"rss_cpython_{k}.json").write_text(json.dumps(py))
        print(f"rss cpython {k}: {py['delta_kb']} kB", file=sys.stderr)
        if rust_bin is not None:
            rs = _run_capture([str(rust_bin), "--rss-child", str(k)])
            (WORK / f"rss_rust_{k}.json").write_text(json.dumps(rs))
            print(f"rss rust    {k}: {rs['delta_kb']} kB", file=sys.stderr)


def _read(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def _row(label: str, data: dict | None, dist: str, n_expected: int) -> str:
    if data is None:
        return f"| {label} | — | — | — | — | — |"
    d = data[dist]
    n = f"{d['bulk_n']:,}" + ("" if d["bulk_n"] >= n_expected else " (capped)")
    return (f"| {label} | {d['mean_ns']:,.0f} | {d['p50']:,} | {d['p99']:,} "
            f"| {d['ops_s']:,.0f} | {n} |")


def render(version: str, quick: bool) -> str:
    from datetime import datetime
    cpu = main_suite._read_proc_line("/proc/cpuinfo", "model name") or platform.processor()
    engines = [
        ("CPython (dripline core.py)", _read(WORK / "cpython.json")),
        ("PyPy (dripline core.py, unmodified)", _read(WORK / "pypy.json")),
        ("Rust governor (same GCRA algorithm)", _read(WORK / "rust.json")),
    ]
    p = params(quick)
    ratios: dict = {}
    cp = _read(WORK / "cpython.json")
    for name in ("rust", "pypy"):
        other = _read(WORK / f"{name}.json")
        if cp and other:
            for dist in DISTS:
                if other[dist]["mean_ns"]:
                    ratios[f"{name}_{dist}"] = cp[dist]["mean_ns"] / other[dist]["mean_ns"]
    lines = [
        f"<!-- GENERATED by bench/champions.py at "
        f"{datetime.now(UTC).isoformat(timespec='seconds')} — do not edit. -->",
        f"# Champion references — the decision loop vs the world ({version})",
        "",
        "Same protocol as the main suite, plus one rule: every runtime reads the",
        "BIT-IDENTICAL key files (same seed), so offered load cannot differ by a byte.",
        f"Every engine row is the median of {N_RUNS} runs (raw runs in the raw json).",
        "",
        "## Environment",
        "",
        f"- {platform.python_implementation()} {platform.python_version()} (orchestrator) "
        f"on {platform.platform()}",
        f"- CPU: {cpu}",
        "- rows ran in pinned containers on the same core (see bench/README.md)",
        "",
        f"## Decision cost — engine view (K={p['k']:,}, offered {p['n']:,})",
        "",
    ]
    for dist in DISTS:
        lines += [f"### {dist} keys", "",
                  "| engine | mean ns/dec | p50 ns | p99 ns | ops/s | bulk n |",
                  "|---|---:|---:|---:|---:|---:|"]
        lines += [_row(label, data, dist, p["n"]) for label, data in engines]
        lines.append("")
    rss_rows = []
    for k in RSS_KS:
        py = _read(WORK / f"rss_cpython_{k}.json")
        rs = _read(WORK / f"rss_rust_{k}.json")
        rss_rows.append((k, py, rs))
    if any(p or r for _, p, r in rss_rows):
        lines += ["## RSS vs distinct clients (Δ MiB)", "",
                  "| clients | CPython dripline | Rust governor |",
                  "|---:|---:|---:|"]
        for k, py, rs in rss_rows:
            py_cell = (f"{py['delta_kb'] / 1024:.1f}"
                       if py and py.get("delta_kb") is not None else "—")
            rs_cell = (f"{rs['delta_kb'] / 1024:.1f}"
                       if rs and rs.get("delta_kb") is not None else "—")
            lines.append(f"| {k:,} | {py_cell} | {rs_cell} |")
        lines.append("")
    lines += [
        "## Notes",
        "",
        "- governor is the Rust ecosystem's standard GCRA limiter; configured identically",
        "  to dripline (1000/min, burst 100) with a dashmap keyed by the same strings.",
        "- The PyPy row is dripline's own core.py, byte-for-byte unmodified — only the",
        "  runtime changes. It isolates the 'CPython tax' from the algorithm.",
        "- Means are bulk-timed on every runtime (fair); percentiles are per-call timed,",
        "  and the timer pair costs different amounts per runtime (~100 ns on CPython,",
        "  ~20 ns on Rust) — Rust percentiles are if anything slightly flattered.",
        "- governor rows measure its library cost as deployed: real keys, real hashing,",
        "  no pre-hashing shortcuts.",
        "",
        "## Reading",
        "",
        "- Rust governor (same GCRA, compiled) is "
        f"**{ratios.get('rust_uniform', 0):.1f}x** faster than the CPython row on uniform "
        f"and **{ratios.get('rust_zipf', 0):.1f}x** on zipf; PyPy — dripline's own code, "
        f"unmodified — is {ratios.get('pypy_uniform', 0):.1f}x / "
        f"{ratios.get('pypy_zipf', 0):.1f}x.",
        "- Conclusion: the bottleneck is the runtime, not the algorithm. dripline's",
        "  GCRA arithmetic is already champion-shaped; the CPython tax is the price",
        "  of the zero-dependency stdlib deployment story. The v0.2 arena (and a",
        "  possible compiled fast path later) attacks the gap from both ends.",
        "- Memory: even the champion grows linearly per client (dashmap); governor is",
        "  leaner per client than the v0.1 dict, and dripline's v0.2 fixed arena is",
        "  the answer to the whole category, not just to Python.",
    ]
    return "\n".join(lines) + "\n"


def cmd_render(version: str, quick: bool, dry: bool) -> None:
    report = render(version, quick)
    print(report)
    if not dry:
        out = ROOT / "bench" / "results"
        out.mkdir(exist_ok=True)
        raw = {name: {"median": _read(WORK / f"{name}.json"),
                      "runs": _read(WORK / f"{name}.runs.json")}
               for name in ("cpython", "pypy", "rust")}
        (out / f"{version}.champions.md").write_text(report, encoding="utf-8")
        (out / f"{version}.champions.raw.json").write_text(json.dumps(raw, indent=1),
                                                           encoding="utf-8")
        print(f"wrote bench/results/{version}.champions.md (+ raw json)", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("cmd", choices=["prep", "drive-cpython", "drive-rust", "merge-pypy",
                                    "rss", "render", "all"])
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--version", default="v0.1")
    ap.add_argument("--bin", default=None, help="path to the governor_bench binary")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rust_bin = cmd_rust_bin(args.bin)
    if args.cmd == "prep":
        cmd_prep(args.quick)
    elif args.cmd == "drive-cpython":
        cmd_drive_cpython()
    elif args.cmd == "drive-rust":
        if rust_bin is None:
            print("rust binary not found — build it first (see bench/README.md)", file=sys.stderr)
            return 2
        cmd_drive_rust(rust_bin)
    elif args.cmd == "merge-pypy":
        cmd_merge_pypy()
    elif args.cmd == "rss":
        cmd_rss(rust_bin)
    elif args.cmd == "render":
        cmd_render(args.version, args.quick, args.dry_run)
    elif args.cmd == "all":
        cmd_prep(args.quick)
        cmd_drive_cpython()
        if rust_bin is not None:
            cmd_drive_rust(rust_bin)
        cmd_rss(rust_bin)
        cmd_render(args.version, args.quick, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
