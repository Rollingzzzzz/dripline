# dripline

**Per-subscriber rate limiting that drips, not floods.**
A fixed-footprint, zero-dependency rate limiter for FastAPI / Starlette / any ASGI app — Redis optional, not required.

> **Status: pre-alpha, design-first.** This README is the committed design; code lands version by
> version, and every version ships with published measurements (see [Roadmap](#roadmap)).

## Who it is for (and who it is not)

The **mid segment**: one machine (or a few), 2–8 ASGI workers, 10⁴–10⁷ distinct clients,
and no desire to run Redis just for counting.

- Too small for this? An unbounded in-memory dict works fine — use [slowapi](https://github.com/laurents/slowapi).
- Bigger? Multi-region, exact global counts, tens of millions of clients → keep Redis (or Dragonfly) at the center.
- In between? Your options today are "unbounded dict" or "mandatory Redis". **dripline is the third option.**

## The four properties

1. **No mandatory Redis.** Worker processes share one memory-mapped arena file; counters are
   shared across workers, not per-worker.
2. **Constant memory, by construction.** The arena is pre-allocated (32-byte slots). There is no
   cleanup thread, no TTL scanner, no eviction job — *writing is deleting*.
3. **Scalar hot path, vectorized maintenance.** A request touches one slot with plain Python:
   one hash, one read, one write. The whole-table jobs — token refill, window rollover,
   metrics, peer merge — are single NumPy calls over the arena (`pip install dripline[numpy]`).
4. **A clock-free mode.** Optional tick-driven token buckets with zero clock reads on the
   request path. They guarantee the band you configure (e.g. "100–120 per minute"), and under
   scheduler pressure they only get stricter, never looser.

## How it works

Each client occupies one arena slot holding a **single integer**: the earliest time
(monotonic nanoseconds) at which its next request may be admitted — the GCRA
"theoretical arrival time".

```
request arrives at t; client's slot holds S:

  S' = max(t, S) + period            # advance one drip interval
  S' - t > burst_budget              # would run more than `burst` intervals ahead
      -> REJECT  (retry-after = S' - burst_budget - t;  S untouched)
      otherwise
      -> ALLOW    (slot := S')
```

Consequences — pure arithmetic, no extra code:

- Idle credit is capped at `burst`; a hammering client settles at exactly the configured rate.
- Nothing is ever reset or scheduled: while a client is idle, the clock walks toward `S` and
  the credit melts on its own.
- A rejected request costs nothing and tells the caller exactly when to come back.

```
1000/min with burst 100, client fires instantly:

  #1..#100   ALLOW   (slot walks 60 ms -> 6000 ms)
  #101       REJECT  "retry in 60 ms"   (slot untouched)
  -- 60 ms of silence --
  #103       ALLOW   ... then exactly one per 60 ms, forever
```

## Roadmap — every version ships measurements

Benchmarks run in Docker with pinned CPU cores (`--cpuset-cpus`, `--cpus`) so numbers stay
comparable across versions and machines. Results are committed under `bench/results/vX.Y.md`;
the methodology lives in [bench/README.md](bench/README.md).

| Version | Theme | Delivers | Exit measurement |
|---|---|---|---|
| v0.1 | Scalar core | GCRA limiter (dict-backed), decorator + ASGI middleware | ns/decision p50–p99, sustained ops/s vs slowapi & aiolimiter on one pinned core |
| v0.2 | Fixed arena | mmap slot arena shared across workers; NumPy maintenance (tick refill, metrics) | RSS vs client count (flat), multi-worker correctness, 10M-client smoke test |
| v0.3 | Clock-free mode | Tick-driven token buckets, zero hot-path clock reads | measured 100–120 guarantee band; hot-path cost vs GCRA mode |
| v0.4 | Neighborhood | Optional peer merge (max-merge over arenas), Prometheus metrics | overshoot ≤ formula under partition/rejoin |

Possible later: Redis/Dragonfly backend as an optional extra; a Count-Min sketch mode for 10⁸+
client keys.

## Honest limitations

- v0.1 keeps state in a plain dict — it grows with distinct clients until the v0.2 arena lands.
- Concurrent same-client races can overshoot by ±1 permit (bounded, documented).
- Arena slot collisions make two clients share fate; at default load factors this is negligible,
  but the counting is approximate, not exact.
- Scope is one box (plus experimental peer merge). Planet-scale exactness is explicitly not the goal.

## For AI agents and search

`llms.txt` at the repo root is a compact machine digest of the API; `AGENTS.md` documents the
build/test/bench commands for coding agents.

## License

MIT — see [LICENSE](LICENSE).
