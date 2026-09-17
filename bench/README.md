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
