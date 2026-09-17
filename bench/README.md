# Benchmarks — methodology

Every tagged version re-runs the same suite and commits results to
`bench/results/vX.Y.md`. The environment is pinned so numbers are comparable
across versions (and across contributor machines).

## Environment pinning

Benchmarks run inside Docker with fixed CPU allocation:

```bash
docker run --rm \
  --cpuset-cpus=2 \        # pin to one specific core (no migration noise)
  --memory=8g \            # fixed memory ceiling
  -v "${PWD}:/bench" \
  python:3.12-slim \
  python /bench/bench/run.py
```

- One core, one container, one run-loop per variant: the ceiling is fixed, so
  "faster" means more decisions per second on the *same* budget.
- Each variant runs 7 times; report the median (outlier runs are kept in the
  raw log, never silently dropped).
- Key distributions: uniform, and Zipf (skewed — a few hot clients, long tail),
  because real traffic is skewed.
- Profiles: `default` (idiomatic 1000/min, admit-dominated) and
  `--profile tight` (instant capacity 1 for every engine, reject-dominated;
  the report's `admits` column proves the mix). Results land in
  `vX.Y.md` / `vX.Y.tight.md` respectively.

## What we measure

| Metric | Meaning |
|---|---|
| ns/decision (p50, p99) | hot-path cost of one admission check |
| sustained ops/s | decisions per second, single pinned core |
| RSS vs client count | memory must stay flat after the arena lands (v0.2) |
| guarantee band | clock-free mode: observed admits per window ∈ [limit, limit + burst] |

## Comparisons

`no-op` baseline, `dripline` (GCRA mode; clock-free mode from v0.3), plus the
incumbents on the same pinned container: [slowapi](https://github.com/laurents/slowapi)
and [aiolimiter](https://github.com/mjpieters/aiolimiter).

Harness script lands with v0.1 (`bench/run.py`); this file fixes the rules
before the first number exists.

## Champion references (`bench/champions.py`)

To keep claims honest, the suite also runs world-class references under a
**champion protocol**: every runtime (CPython, PyPy, Rust `governor`) reads the
*bit-identical* key files produced from the same seed, so offered load cannot
differ by a byte. PyPy runs dripline's own `core.py` unmodified — it isolates
the CPython tax from the algorithm; governor is the Rust ecosystem's standard
GCRA limiter, configured identically (1000/min, burst 100).

```bash
# 1) build the champion binary (once, or after changing it)
docker run --rm -v "${PWD}:/bench" rust:1-slim \
  cargo build --release --manifest-path /bench/bench/champions/rust/Cargo.toml

# 2) run everything (pinned container) — prep, CPython+Rust rows, RSS, report
docker run --rm --cpuset-cpus=2 --memory=8g -v "${PWD}:/bench" python:3.12-slim \
  python /bench/bench/champions.py all

# 3) PyPy row (separate image, same pinning; 3 runs -> jsonl), then re-render
docker run --rm --cpuset-cpus=2 --memory=8g -v "${PWD}:/bench" pypy:3.11-slim-bookworm \
  sh -c 'for i in 1 2 3; do pypy3 /bench/bench/champions/driver.py \
    --keys-dir /bench/bench/champions/work; done' \
  > bench/champions/work/pypy.jsonl
docker run --rm -v "${PWD}:/bench" python:3.12-slim \
  sh -c 'python /bench/bench/champions.py merge-pypy && python /bench/bench/champions.py render'
```

Results land in `bench/results/vX.Y.champions.md`; champion work files are
build artifacts and stay untracked.

## Scenario benchmark (`bench/scenario.py`)

The standing scoreboard format for every version: one machine, multiple cores
(worker processes = pinned cores, each with its own limiter — the Redis-less
deployment model), max-speed load for 60 s per scenario against 100 clients
round-robin. Two fixed scenarios: **loose** (1000 admits / 10 s per client)
and **tight** (10 admits / 10 s per client, reject-dominated), three repeats
each, median reported. Rows: dripline, Python rivals (limits fixed-window,
aiolimiter), Rust governor. Reports total sustained decisions/s per repeat
plus admit counts (budget compliance evidence).

```bash
docker run --rm --cpuset-cpus=0-3 --memory=8g -v "${PWD}:/bench" python:3.12-slim \
  python /bench/bench/scenario.py --scenario loose
docker run --rm --cpuset-cpus=0-3 --memory=8g -v "${PWD}:/bench" python:3.12-slim \
  python /bench/bench/scenario.py --scenario tight   # merges into the same report
```

## Standing verification — every core change runs the scoreboard

The scenario benchmark is the project's regression gate, not a one-off. Every
change to `src/dripline` (hot path, storage, algorithm) is verified with a
FAST-protocol run of the standing format before it is committed:

```bash
docker run --rm --cpuset-cpus=0-3 --memory=8g -v "${PWD}:/bench" python:3.12-slim \
  python /bench/bench/scenario.py --version vX.Y
```

The headline tracked across versions is **% of governor**. A change does not
ship as-is if it lowers dripline's share of governor's sustained throughput,
or breaks budget compliance (admits per client above the ceiling). Release
protocol (`--release`) is re-run for version tags on top of this.

## Arena exit measurements (`bench/arena_smoke.py`)

The v0.2 roadmap exit criteria in one run: RSS vs client count through a
fixed 12M-slot arena (flat at the pre-allocated cap), a 10M-client churn
through a 1M-slot arena (10× oversubscribed — RSS pinned, an actively
hammered client still inside its exact GCRA bound, proving active slots are
never recycled), and the 100-admit correctness guard.

```bash
docker run --rm --cpuset-cpus=2 --memory=8g -v "${PWD}:/bench" python:3.12-slim \
  python /bench/bench/arena_smoke.py
```

Results land in `bench/results/v0.2.arena.md`; the dict-core reference at 1M
clients gives the memory scale (the 10M dict point is deliberately not run).

## Fast vs release protocol

Iteration must be cheap; published numbers must be steady. Two modes, same
harnesses:

| | fast (default) | release (`--release`) |
|---|---|---|
| runs/cell | ≥3, sequential stop when run-means agree within 5% | 7, fixed |
| warmup / samples | 30k / 100k | 100k / 200k |
| bulk / percall caps | 1.5 s / 1.0 s | 4 s / 2 s |
| RSS sweep Ks | 10k, 100k, 1M | 1k, 10k, 100k, 1M (+10M with `--full`) |
| scenario duration | 20 s (2 window rolls) | 60 s |
| output files | `vX.Y.*.fast.md` | canonical `vX.Y.*.md` |

Justification (measured on this repo's own raw data, 2026-09-17):
- Within a session, a 3-run median deviated from the 7-run median by at most
  **2.6%** across all 20 decision-cost cells (most ≤2%).
- Cross-session validation (fast run vs release run, separate containers):
  **no ranking changed**, per-cell values stayed within ±15% worst-case
  (mostly ≤±10%); scenario totals within ±5.6%. The claims this suite makes
  are about 2×–17× differences, an order of magnitude above fast-mode drift.
Release protocol stays mandatory for version tags (`CONVENTIONS` rule 4).
