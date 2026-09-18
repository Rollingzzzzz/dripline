"""FastAPI / Starlette integration: one import, one decorator, exact waits.

Works on any Starlette-based framework (FastAPI included) and needs only
Starlette itself — no FastAPI-specific imports::

    from fastapi import FastAPI, Request
    from dripline.ext.fastapi import Limiter, api_key_header

    app = FastAPI()
    limiter = Limiter(key_func=api_key_header)
    # multi-worker deployments: Limiter(key_func=..., path="/run/dripline.arena")

    @app.get("/data")
    @limiter.limit("1000/10s")           # burst bank = one window by default
    async def get_data(request: Request):
        return {"ok": True}

Over-budget requests raise ``429 Too Many Requests`` with two headers:

- ``Retry-After`` — RFC 7231 integer seconds, derived from the limiter's
  exact nanosecond wait (ceil; sub-second waits report 1);
- ``X-RateLimit-Retry-After-Ns`` — the raw nanosecond precision, so callers
  that can honour finer waits are not rounded down.

There is nothing to install on the app — no exception handler, no state
hook — the decorator raises Starlette's own ``HTTPException``.

One engine per rate spec: a ``Limiter`` caches one engine per distinct
``(rate, burst)`` pair, built from ``engine_class`` (default
:class:`dripline.apex.ApexLimiter`) plus the constructor kwargs given to
``Limiter`` (``slots=``, ``path=``, ``map_bytes=``, …). Pass ``path=`` and
every uvicorn worker that opens the same file enforces one shared budget.
For a prebuilt engine applied to every request, use
:class:`LimiterMiddleware` instead of the decorator.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from functools import wraps
from typing import Any

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from dripline.apex import ApexLimiter
from dripline.rates import parse_rate

__all__ = ["Limiter", "LimiterMiddleware", "api_key_header", "remote_addr"]

KeyFunc = Callable[[Request], str]


def api_key_header(request: Request) -> str:
    """Per-subscriber key from the ``X-Api-Key`` header."""
    return request.headers.get("x-api-key") or "anonymous"


def remote_addr(request: Request) -> str:
    """Per-client key from the peer address."""
    return request.client.host if request.client else "anonymous"


def _header_key(name: str) -> KeyFunc:
    def key(request: Request) -> str:
        return request.headers.get(name) or "anonymous"
    return key


def _retry_headers(retry_ns: int) -> dict[str, str]:
    return {"Retry-After": str(max(1, math.ceil(retry_ns / 1_000_000_000))),
            "X-RateLimit-Retry-After-Ns": str(retry_ns)}


def _find_request(args: tuple, kwargs: dict) -> Request | None:
    for value in kwargs.values():
        if isinstance(value, Request):
            return value
    for value in args:
        if isinstance(value, Request):
            return value
    return None


class Limiter:
    """Decorator-style rate limiting for Starlette routes.

    Args:
        key_func: callable taking the request and returning the client key
            (built-ins: :func:`api_key_header`, :func:`remote_addr`).
        engine_class: limiter class instantiated per rate spec — defaults to
            the flagship :class:`dripline.apex.ApexLimiter`.
        engine_kwargs: forwarded to the engine constructor (``slots=``,
            ``path=``, ``map_bytes=`` …). ``path=`` shares budgets across
            worker processes.
    """

    def __init__(self, key_func: KeyFunc | str = api_key_header,
                 *, engine_class: type = ApexLimiter,
                 **engine_kwargs: Any) -> None:
        self._key_func = _header_key(key_func) if isinstance(key_func, str) \
            else key_func
        self._engine_class = engine_class
        self._engine_kwargs = engine_kwargs
        self._engines: dict[tuple[float, int], Any] = {}

    @property
    def engines(self) -> dict[tuple[float, int], Any]:
        """The per-rate engines built so far (introspection/tests)."""
        return dict(self._engines)

    def _engine_for(self, rate_ps: float, burst: int) -> Any:
        spec = (rate_ps, burst)
        engine = self._engines.get(spec)
        if engine is None:
            engine = self._engines[spec] = self._engine_class(
                rate_per_second=rate_ps, burst=burst, **self._engine_kwargs)
        return engine

    def limit(self, rate: str, *, key: KeyFunc | str | None = None,
              burst: int | None = None) -> Callable:
        """Decorate an endpoint with a rate limit.

        ``rate``: spec string (see :mod:`dripline.rates`). ``key``: a key
        callable, or a header name as shorthand (``key="x-api-key"``);
        defaults to the Limiter's ``key_func``. ``burst``: override the
        default one-window bank. The endpoint must declare a
        ``request: Request`` parameter (the slowapi convention) so the key
        can be extracted.
        """
        rate_ps, default_burst = parse_rate(rate)
        if burst is None:
            burst = default_burst
        key_func = _header_key(key) if isinstance(key, str) \
            else (key or self._key_func)

        def decorator(endpoint: Callable) -> Callable:
            @wraps(endpoint)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                request = _find_request(args, kwargs)
                if request is None:
                    raise RuntimeError(
                        "dripline limit() needs a `request: Request` parameter "
                        "in the endpoint signature to extract the client key")
                retry = self._engine_for(rate_ps, burst).try_acquire(
                    key_func(request))
                if retry:
                    raise HTTPException(status_code=429,
                                        detail="Rate limit exceeded",
                                        headers=_retry_headers(retry))
                result = endpoint(*args, **kwargs)
                if hasattr(result, "__await__"):
                    result = await result
                return result
            return wrapper
        return decorator


class LimiterMiddleware:
    """Pure-ASGI rate limiting — framework-agnostic, one engine, all requests.

    Wrap any ASGI app; every HTTP request (optionally restricted to
    ``paths`` prefixes) is checked against one limiter. Give it a prebuilt
    engine, or a ``rate`` spec to have one constructed.

        app.add_middleware(LimiterMiddleware, rate="1000/10s",
                           key_func=api_key_header, path="/run/dripline.arena")
    """

    def __init__(self, app: Any, *, rate: str | None = None,
                 engine: Any | None = None, burst: int | None = None,
                 key_func: KeyFunc = api_key_header,
                 paths: tuple[str, ...] | None = None,
                 **engine_kwargs: Any) -> None:
        if engine is None:
            if rate is None:
                raise ValueError("LimiterMiddleware needs a rate spec or an engine")
            rate_ps, default_burst = parse_rate(rate)
            engine = ApexLimiter(rate_per_second=rate_ps,
                                 burst=burst or default_burst, **engine_kwargs)
        self.app = app
        self._engine = engine
        self._key_func = key_func
        self._paths = paths

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] == "http" and (
                self._paths is None
                or any(scope["path"].startswith(p) for p in self._paths)):
            retry = self._engine.try_acquire(
                self._key_func(Request(scope, receive)))
            if retry:
                response = PlainTextResponse(
                    "Rate limit exceeded", status_code=429,
                    headers=_retry_headers(retry))
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
