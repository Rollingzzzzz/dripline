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
