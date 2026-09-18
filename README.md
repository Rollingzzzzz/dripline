# dripline

**Per-subscriber rate limiting that drips, not floods.**
Fixed-footprint, Redis-optional rate limiting for FastAPI / Starlette / any ASGI app —
pure Python at the core, every claim measured.

> 0.4.0 · stdlib-only core (`dripline[numpy]`, `dripline[fastapi]` optional) ·
> benchmarks + methodology in [`bench/`](bench/README.md), raw numbers in [`bench/results/`](bench/results/)

## The 30-second pitch

Run 4 uvicorn workers and every per-worker limiter — slowapi's engine, aiolimiter,
even Rust governor — quietly enforces **four copies of your limit**: you configured
1000/min, you admit 4000/min. We measured it ([scenario benchmark](bench/results/v0.4.scenario.md);
the same finding reproduces in [the live demo](demo/out/report.html)).

dripline's workers share one memory-mapped arena file, so budgets are **global by
construction** — 1× what you configured, no Redis, no coordination service, no
cleanup thread. Memory is a pre-allocated constant you pick at startup and it never
grows. And every rejection carries the exact wait, down to the nanosecond.

## Use it with FastAPI

```bash
pip install 'dripline[fastapi]'        # until PyPI: pip install git+https://github.com/Rollingzzzzz/dripline@v0.4.0
```

```python
from fastapi import FastAPI, Request
from dripline.ext.fastapi import Limiter, api_key_header

app = FastAPI()
limiter = Limiter(key_func=api_key_header)
# multi-worker deployments add one line — every worker then enforces ONE budget:
# limiter = Limiter(key_func=api_key_header, path="/run/dripline.arena")

@app.get("/data")
@limiter.limit("1000/10s")             # burst bank = one window by default
async def get_data(request: Request):
    return {"ok": True}
```

Over budget → `429` with two headers: `Retry-After` (RFC integer seconds) and
`X-RateLimit-Retry-After-Ns` — the exact nanosecond wait, a hint no rival returns
at any precision. Works with sync and async endpoints; plain Starlette or any ASGI
app can use `LimiterMiddleware` instead of the decorator. Source:
[`src/dripline/ext/fastapi.py`](src/dripline/ext/fastapi.py), rate-spec parser in
[`src/dripline/rates.py`](src/dripline/rates.py).

## The engines — read the code, it's short

| engine | file | what it is | measured |
|---|---|---|---|
| **`ApexLimiter`** (the default flagship) | [`src/dripline/apex.py`](src/dripline/apex.py) | three layers in one shared mmap: a generation-stamped presence map (rejects without touching the clock or the table), the stable-fingerprint slot arena, and a coarse tick with exact admits | 9.4M decisions/s across 4 workers = **14% of Rust governor**, with **true global budgets** (admits 301k vs 300k ceiling when every rival runs 4× over) |
| `ArenaGcraLimiter` | [`src/dripline/arena.py`](src/dripline/arena.py) | the mmap slot arena alone: shared budgets, flat RSS, NumPy `stats()` | 67 MiB capped at 1M clients (aiolimiter: 612 MiB and growing); flat 1M→10M clients |
| `TickGcraLimiter` | [`src/dripline/tick.py`](src/dripline/tick.py) | per-worker, exact admits + clock-amortized rejects | 10.9M dec/s = 16% of governor — the fastest per-process engine |
| `GcraLimiter` | [`src/dripline/core.py`](src/dripline/core.py) | the scalar reference: one integer per client, no more | 306 ns/decision (zipf); the semantics everything else implements |

All share one contract: `try_acquire(key) -> int` — `0` = admitted, `>0` = the exact
wait in nanoseconds. Same key → same slot in every process (stable fingerprints);
drained slots are mathematically identical to fresh ones, so newcomers recycle them
— writing is deleting, no TTL jobs anywhere.

## How Apex is fast *and* honest

Three layers answer the three costs of a decision, and every approximation errs
**one way** — toward over-rejecting, never over-admitting ("under pressure the
limiter only gets stricter"):

1. **L1 — presence map:** one shared byte answers "rejected a moment ago"; no
   clock, no table, no arithmetic. Rotation is O(1): bump one generation word,
   yesterday's stamps stop matching — no memset, ever.
2. **L2 — slot arena:** stable fingerprint, one short probe, globally shared slot.
3. **L3/L4 — tick + exact clock:** rejects judge against a cached tick; admits
   always read a fresh clock, so admitted time is exact.

The full engineering story — including the ideas we measured and *rejected*
(a classic Bloom filter loses in CPython: [`docs/adr/0001`](docs/adr/0001-red-bloom-in-cpython.md)) —
lives in the module docstrings and the results files.

## Measured — highlights

Standing scoreboard (4 workers, sustained max-speed load, 10 s windows; release
protocol, raw files in `bench/results/`):

Release-protocol scoreboard (60 s runs, 4 workers; 100 clients, budget
700,000 admits total — raw: [`bench/results/v0.4.scenario.md`](bench/results/v0.4.scenario.md)):

| engine | loose dec/s | % of governor | admits (budget = 700k) |
|---|---:|---:|---:|
| governor (Rust) | 69.2M | 100% | 2,799,700 — 4× over |
| dripline Tick | 11.0M | 16% | 2,800,000 — 4× over |
| **dripline Apex** | **9.9M** | **14%** | **703,792 — 1×, global budget held** |
| aiolimiter | 4.6M | 7% | 2,799,871 — 4× over |
| slowapi engine (limits) | 1.1M | 2% | ~4× over |

Throughput + budget compliance, not cherry-picked: dripline is 3.4× the best Python
rival at the decision layer (306 vs 1052 ns/decision, zipf) while being the *only* engine on
the board holding the configured budget across workers. At HTTP scope both engines
saturate the stack equally (~12.5k req/s each — [demo report](demo/out/report.html));
we say so, because that's what the measurement says.

Memory (RSS at 1M distinct clients): arena-capped **67 MiB** vs dict core 123 MiB,
limits 318–472 MiB, aiolimiter 612 MiB — and no engine but dripline ever gives
stale-client memory back ([adversarial suite](bench/results/v0.1.adversarial.md)).

Reproduce everything: [`bench/README.md`](bench/README.md) (pinned Docker,
protocols, seeds). The 100-API-key live comparison with its HTML report:
[`demo/`](demo/) — `python demo/run.py` or the one-click `python demo/gui.py`.

## Honest limitations

- Same-slot write races across workers can lose one update (±1 permit class,
  measured +0.6%); an over-subscribed arena shares fate at the home bucket —
  counting is approximate, never exact.
- The heat map's index is Python's process-local string hash (deliberate: stable
  alternatives cost more than the layer saves); each worker warms its own overlay,
  so size `map_bytes` for workers × rejected clients.
- Scope is one box (multi-region is explicitly not the goal; peer merge is future
  work). GCRA admits exactly the configured rate — the only burst you get is the
  one you configure.

## Roadmap

| version | theme | status |
|---|---|---|
| 0.1 | scalar GCRA core + engine benchmark harness | ✅ shipped, measured |
| 0.2 | fixed mmap slot arena, flat RSS, 10M smoke | ✅ shipped, measured |
| 0.2.1 | allocation-free hot path (int returns) — 2.6× faster | ✅ shipped, measured |
| 0.3 | clock-pressure modes: coarse-tick engine + Apex (tick + presence map) | ✅ shipped, measured; standalone bloom/presence rejected (ADR 0001) |
| 0.4 | FastAPI/Starlette layer, exact Retry-After, live demo | ✅ shipped (this release) |
| next | neighborhood peer merge (max-merge over arenas), Prometheus metrics | planned |

Possible later: Redis/Dragonfly backend as an optional extra; a Count-Min sketch
mode for 10⁸+ client keys.

## For AI agents and search

[`llms.txt`](llms.txt) is a compact machine digest of the API;
[`AGENTS.md`](AGENTS.md) documents build/test/bench commands for coding agents.

## License

MIT — see [LICENSE](LICENSE).
