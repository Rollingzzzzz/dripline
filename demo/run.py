"""Demo orchestrator (host side): 4 runs, one report.

    python demo/run.py              # full matrix, ~5-6 minutes
    python demo/run.py --quick      # 10 keys / 5 s, smoke the plumbing

Runs the server (per ENGINE/WORKERS) and the client (100 API keys) via
docker compose, collects one JSON per run into demo/out/, then renders
demo/out/report.html. Raw JSONs stay next to the report — the transparency
rule: numbers are generated, never hand-edited.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "demo" / "docker-compose.yml"
OUT_DIR = ROOT / "demo" / "out"

RUNS = [("dripline", "4"), ("aiolimiter", "4"),
        ("dripline", "1"), ("aiolimiter", "1")]


def compose(*args: str, env_extra: dict | None = None) -> None:
    env = {**os.environ, **(env_extra or {})}
    subprocess.run(["docker", "compose", "-f", str(COMPOSE), *args],
                   check=True, env=env)


SPEED_PROBE = {
    # Saturation probe: huge rate spec (nothing rejects), unpaced streams —
    # achieved req/s = pure HTTP+limiter throughput per engine, 4 workers.
    "RATE": "100000/10s", "KEY_COUNT": "20", "RATE_PER_KEY": "100000",
    "STREAMS_PER_KEY": "8", "DURATION": "6", "WORKERS": "4",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="10 keys, 5 s per run — plumbing smoke only")
    args = ap.parse_args()
    common = {"KEY_COUNT": "10", "DURATION": "5", "RATE_PER_KEY": "40"} \
        if args.quick else {}

    OUT_DIR.mkdir(exist_ok=True)
    jsons = []
    try:
        for engine, workers in RUNS:
            tag = f"{engine}-w{workers}"
            out = f"/out/run-{tag}.json"
            # Single-worker runs drop the offered rate: at 50 req/s/key one
            # uvicorn worker saturates and p50 becomes queue noise, not the
            # per-request cost the 1-worker section is meant to show.
            rate = {"RATE_PER_KEY": "10" if workers == "1" else "50"}
            env = {"ENGINE": engine, "WORKERS": workers, "OUT": out,
                   **rate, **common}
            print(f"=== {tag}: server up (engine={engine}, workers={workers})…",
                  file=sys.stderr)
            compose("up", "-d", "server", env_extra=env)
            print(f"=== {tag}: client load…", file=sys.stderr)
            compose("run", "--rm", "client", env_extra=env)
            jsons.append(OUT_DIR / f"run-{tag}.json")
        for engine in ("dripline", "aiolimiter"):
            if args.quick:
                break
            tag = f"{engine}-speed"
            env = {"ENGINE": engine, "OUT": f"/out/run-{tag}.json",
                   **SPEED_PROBE}
            print(f"=== {tag}: saturation probe (4 workers, no rejects)…",
                  file=sys.stderr)
            compose("up", "-d", "server", env_extra=env)
            compose("run", "--rm", "client", env_extra=env)
            jsons.append(OUT_DIR / f"run-{tag}.json")
    finally:
        compose("down", "--volumes")

    render = [sys.executable, str(ROOT / "demo" / "report.py"),
              *[str(p) for p in jsons],
              "--out", str(OUT_DIR / "report.html")]
    subprocess.run(render, check=True)
    print(f"\nopen {OUT_DIR / 'report.html'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
