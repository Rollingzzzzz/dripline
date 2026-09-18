"""Demo server: one endpoint, two rate-limiting engines, zero differences.

    ENGINE=dripline   -> dripline.ext.fastapi.Limiter over ApexLimiter,
                         every uvicorn worker maps the SAME shared arena file
    ENGINE=aiolimiter -> the library's documented per-key pattern:
                         one AsyncLimiter per API key in a dict

Same rate semantics on both sides: RATE="100/10s" maps to GCRA
(10/s sustained, burst bank 100) and to AsyncLimiter(100, 10)
(capacity 100 over a 10 s period) — identical capacity, identical intent.

Run inside the demo container (see demo/docker-compose.yml); configuration
comes from the environment so the SAME file serves both engines.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, "/dripline/src")  # the repo is mounted, not installed

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

ENGINE = os.environ.get("ENGINE", "dripline")
RATE = os.environ.get("RATE", "100/10s")
WORKERS_DIR_HINT = os.environ.get("ARENA", "/dev/shm/dripline-demo.arena")

app = FastAPI()


def _parse(spec: str) -> tuple[float, int]:
    """'100/10s' -> (10.0 per second sustained, 100 capacity)."""
    amount_s, _, period_s = spec.partition("/")
    amount, period = int(amount_s), float(period_s.rstrip("s") or 1)
    return amount / period, amount


if ENGINE == "dripline":
    from dripline.ext.fastapi import Limiter, api_key_header

    limiter = Limiter(key_func=api_key_header,
                      slots=2048, map_bytes=1 << 16,
                      path=WORKERS_DIR_HINT)  # shared across uvicorn workers

    @app.get("/data")
    @limiter.limit(RATE)
    async def data(request: Request):
        return {"ok": True, "engine": ENGINE}

elif ENGINE == "aiolimiter":
    from aiolimiter import AsyncLimiter

    rate_ps, capacity = _parse(RATE)  # AsyncLimiter(max_rate, time_period)
    limiters: dict[str, AsyncLimiter] = {}

    @app.get("/data")
    async def data(request: Request):
        key = request.headers.get("x-api-key") or "anonymous"
        lim = limiters.get(key)
        if lim is None:
            lim = limiters[key] = AsyncLimiter(capacity, capacity / rate_ps)
        if not lim.has_capacity(1):
            return JSONResponse({"detail": "Rate limit exceeded"},
                                status_code=429)
        await lim.acquire(1)
        return {"ok": True, "engine": ENGINE}

else:
    raise SystemExit(f"unknown ENGINE={ENGINE!r} (dripline | aiolimiter)")
