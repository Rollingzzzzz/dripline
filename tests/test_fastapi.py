"""Tests for the FastAPI/Starlette integration layer.

Skips silently when fastapi/httpx are not installed (they are optional
extras; the pinned Docker bench installs them for the full run).
"""

import tempfile
from pathlib import Path

try:
    import httpx  # noqa: F401
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route
    from starlette.testclient import TestClient as StarletteClient
except ImportError:
    FastAPI = Request = TestClient = None

if FastAPI is None:
    if __name__ == "__main__":
        print("fastapi/httpx not installed — fastapi tests skipped")
else:
    from dripline.ext.fastapi import Limiter, LimiterMiddleware

    def _client(**limiter_kwargs):
        app = FastAPI()
        limiter = Limiter(**limiter_kwargs)

        @app.get("/burst")
        @limiter.limit("3/minute")
        async def burst(request: Request):
            return {"ok": True}

        @app.get("/sync")
        @limiter.limit("3/minute")
        def sync_endpoint(request: Request):
            return {"ok": True}

        return TestClient(app), limiter

    def test_burst_then_429_with_exact_retry_after():
        client, _ = _client()
        for _ in range(3):
            assert client.get("/burst").status_code == 200
        r = client.get("/burst")
        assert r.status_code == 429
        assert r.headers["Retry-After"] == "20"        # full bank spent: one period
        ns = int(r.headers["X-RateLimit-Retry-After-Ns"])
        assert 19_000_000_000 <= ns <= 20_000_000_000  # exact, not rounded

    def test_keys_are_isolated():
        client, _ = _client()
        for _ in range(3):
            assert client.get("/burst",
                              headers={"X-Api-Key": "alice"}).status_code == 200
        blocked = client.get("/burst", headers={"X-Api-Key": "alice"})
        fresh = client.get("/burst", headers={"X-Api-Key": "bob"})
        assert blocked.status_code == 429 and fresh.status_code == 200

    def test_sync_endpoints_and_key_shorthand():
        app = FastAPI()
        limiter = Limiter(key_func="x-api-key")   # header-name shorthand

        @app.get("/s")
        @limiter.limit("2/minute")
        def endpoint(request: Request):
            return {"ok": True}

        client = TestClient(app)
        assert client.get("/s", headers={"X-Api-Key": "k"}).status_code == 200
        assert client.get("/s", headers={"X-Api-Key": "k"}).status_code == 200
        assert client.get("/s", headers={"X-Api-Key": "k"}).status_code == 429

    def test_shared_arena_is_one_budget_for_two_workers():
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "arena.bin"
            app_a = FastAPI()
            app_b = FastAPI()
            a = Limiter(slots=256, map_bytes=4096, path=path)
            b = Limiter(slots=256, map_bytes=4096, path=path)
            try:
                for app, lim in ((app_a, a), (app_b, b)):
                    @app.get("/x")
                    @lim.limit("3/minute")
                    async def x(request: Request):
                        return {"ok": True}

                ca, cb = TestClient(app_a), TestClient(app_b)
                for _ in range(3):
                    assert ca.get("/x", headers={"X-Api-Key": "shared"}).status_code == 200
                # Worker b feels the budget worker a already spent.
                assert cb.get("/x", headers={"X-Api-Key": "shared"}).status_code == 429
            finally:  # Windows keeps mapped files locked until closed
                for lim in (a, b):
                    for engine in lim.engines.values():
                        engine.close()

    def test_middleware_on_plain_starlette():
        async def home(request):
            return PlainTextResponse("ok")

        app = Starlette(routes=[Route("/", home)])
        app.add_middleware(LimiterMiddleware, rate="2/minute")
        client = StarletteClient(app)
        assert client.get("/").status_code == 200
        assert client.get("/").status_code == 200
        r = client.get("/")
        assert r.status_code == 429 and "Retry-After" in r.headers

    if __name__ == "__main__":
        test_burst_then_429_with_exact_retry_after()
        test_keys_are_isolated()
        test_sync_endpoints_and_key_shorthand()
        test_shared_arena_is_one_budget_for_two_workers()
        test_middleware_on_plain_starlette()
        print("all fastapi tests passed")
