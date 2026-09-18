"""Demo client: 100 API keys hammering the demo server, everything measured.

Transport is deliberately NOT httpx/aiohttp: a load generator needs
thousands of cheap in-flight requests, and generic client libraries spend
more CPU per request than the trivial fixed endpoint warrants. So this is
a minimal stdlib asyncio HTTP/1.1 speaker — one keep-alive socket per
stream, write request, await response. Zero third-party dependencies; the
report footer says so.

Each key fires from several parallel streams (several sockets), modelling
the real multi-client pattern: many machines or reconnects sharing one API
key. With a single connection per key, keep-alive pins the key to one
uvicorn worker and even per-worker limiters look correct — the deployment
risk only appears when traffic spreads, which is what we measure.

Per key: offered load at a fixed rate for a fixed duration. Per request:
status, latency (socket write -> response parsed) and the Retry-After hint
when the server sends one. Output: one JSON per run; report.py merges.

    URL=http://server:8000 KEY_COUNT=100 RATE_PER_KEY=50 STREAMS_PER_KEY=4
    DURATION=15 OUT=/out/run.json
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform
import time

URL = os.environ.get("URL", "http://server:8000")
KEY_COUNT = int(os.environ.get("KEY_COUNT", "100"))
RATE_PER_KEY = float(os.environ.get("RATE_PER_KEY", "50"))  # offered req/s/key
DURATION = float(os.environ.get("DURATION", "15"))
STREAMS_PER_KEY = int(os.environ.get("STREAMS_PER_KEY", "4"))
INTERVAL = STREAMS_PER_KEY / RATE_PER_KEY  # per-stream pacing
OUT = os.environ.get("OUT", "/out/run.json")
RATE_SPEC = os.environ.get("RATE", "100/10s")

HOSTPORT = URL.removeprefix("http://")
HOST, _, PORT = HOSTPORT.partition(":")
PORT_I = int(PORT or 80)
REQ = (f"GET /data HTTP/1.1\r\nHost: {HOSTPORT}\r\n"
       "Connection: keep-alive\r\nX-Api-Key: {{key}}\r\n\r\n").encode()


class Conn:
    """One keep-alive HTTP/1.1 socket; enough parsing to count statuses."""

    __slots__ = ("reader", "writer")

    async def open(self) -> Conn:
        self.reader, self.writer = await asyncio.open_connection(HOST, PORT_I)
        return self

    async def get(self, key: str) -> tuple[int, dict[str, str]]:
        self.writer.write(REQ.replace(b"{{key}}", key.encode()))
        await self.writer.drain()
        headers: dict[str, str] = {}
        status = 0
        body_len = 0
        while True:
            line = await self.reader.readline()
            if not line:
                raise ConnectionError("closed")
            if line in (b"\r\n", b"\n"):
                break
            if status == 0:
                status = int(line.split()[1])
            else:
                name, _, value = line.decode("latin1").partition(":")
                headers[name.strip().lower()] = value.strip()
                if name.strip().lower() == "content-length":
                    body_len = int(value.strip())
        while body_len and len(await self.reader.read(body_len)) < body_len:
            pass  # drain body (single small JSON)
        return status, headers

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.writer.close()


def _percentiles(xs: list[int]) -> dict:
    if not xs:
        return {}
    xs = sorted(xs)

    def pct(p):
        return xs[min(len(xs) - 1, int(p * (len(xs) - 1)))]
    return {"p50_ms": round(pct(0.50) / 1e6, 2),
            "p95_ms": round(pct(0.95) / 1e6, 2),
            "p99_ms": round(pct(0.99) / 1e6, 2)}


async def stream(key: str, offset: float, stats: dict) -> None:
    per_key = stats["keys"].setdefault(key, {
        "sent": 0, "admitted": 0, "rejected": 0, "errors": 0,
        "retry_after": [], "first_reject_s": None})
    conn: Conn | None = None
    deadline = time.monotonic() + DURATION
    next_at = time.monotonic() + offset
    try:
        while time.monotonic() < deadline:
            now = time.monotonic()
            if now < next_at:
                await asyncio.sleep(next_at - now)
            next_at += INTERVAL
            t0 = time.perf_counter_ns()
            try:
                if conn is None:
                    conn = await Conn().open()
                status, headers = await conn.get(key)
            except (ConnectionError, OSError, asyncio.IncompleteReadError):
                per_key["errors"] += 1
                if conn:
                    conn.close()
                conn = None
                continue
            stats["latencies"].append(time.perf_counter_ns() - t0)
            per_key["sent"] += 1
            if status == 200:
                per_key["admitted"] += 1
            elif status == 429:
                per_key["rejected"] += 1
                if per_key["first_reject_s"] is None:
                    per_key["first_reject_s"] = round(DURATION - (deadline - now), 2)
                hint = headers.get("x-ratelimit-retry-after-ns") \
                    or headers.get("retry-after")
                if hint:
                    per_key["retry_after"].append(hint)
    finally:
        if conn:
            conn.close()


async def wait_for_server(timeout_s: float = 120) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        probe = None
        try:
            probe = await Conn().open()
            await probe.get("warmup")
            return
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            await asyncio.sleep(1)
        finally:
            if probe:
                probe.close()
    raise SystemExit(f"server not reachable at {URL} within {timeout_s}s")


async def main() -> None:
    stats: dict = {"keys": {}, "latencies": []}
    await wait_for_server()
    await asyncio.gather(*(
        stream(f"key-{i:03d}", s / RATE_PER_KEY, stats)
        for i in range(KEY_COUNT) for s in range(STREAMS_PER_KEY)))

    amount = int(RATE_SPEC.partition("/")[0])
    window = float(RATE_SPEC.partition("/")[2].rstrip("s") or 1)
    total_sent = sum(k["sent"] for k in stats["keys"].values())
    total_adm = sum(k["admitted"] for k in stats["keys"].values())
    total_rej = sum(k["rejected"] for k in stats["keys"].values())
    with_retry = [k for k in stats["keys"].values() if k["retry_after"]]
    report = {
        "url": URL,
        "engine": os.environ.get("ENGINE", "?"),
        "workers": int(os.environ.get("WORKERS", "1")),
        "rate_spec": RATE_SPEC,
        "per_key_offered_rps": RATE_PER_KEY,
        "streams_per_key": STREAMS_PER_KEY,
        "duration_s": DURATION,
        "key_count": KEY_COUNT,
        "totals": {"sent": total_sent, "admitted": total_adm,
                   "rejected": total_rej},
        "configured_ceiling": round(
            KEY_COUNT * (amount + amount / window * DURATION)),
        "latency": _percentiles(stats["latencies"]),
        "keys_with_retry_after": len(with_retry),
        "sample_retry_after": with_retry[0]["retry_after"][:3] if with_retry else [],
        "per_key": stats["keys"],
        "client_python": platform.python_version(),
        "client_transport": "stdlib asyncio HTTP/1.1 (no third-party client)",
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(report, f)
    print(json.dumps({k: v for k, v in report.items() if k != "per_key"},
                     indent=1))


if __name__ == "__main__":
    asyncio.run(main())
