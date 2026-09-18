"""Rate specification parsing — "1000/10s" in, (rate, burst) out.

The string form is what FastAPI users write on decorators, so it mirrors the
incumbents' vocabulary (slowapi/limits style) while staying honest about
GCRA semantics:

    "5/minute"    ->  5 per 60 s,   burst bank of 5
    "1000/10s"    ->  100 per 1 s,  burst bank of 1000 (one window's worth)
    "100/hour"    ->  100 per hour, burst bank of 100

The default burst equals the amount — the full window can be banked, the
same capacity semantics as ``aiolimiter.AsyncLimiter(amount, period)``.
Pass an explicit ``burst`` where that is not wanted.
"""

from __future__ import annotations

__all__ = ["parse_rate"]

_UNITS = {
    "s": 1.0, "sec": 1.0, "second": 1.0, "seconds": 1.0,
    "m": 60.0, "min": 60.0, "minute": 60.0, "minutes": 60.0,
    "h": 3600.0, "hour": 3600.0, "hours": 3600.0,
    "d": 86400.0, "day": 86400.0, "days": 86400.0,
}


def parse_rate(spec: str) -> tuple[float, int]:
    """Parse a rate spec into ``(rate_per_second, burst)``.

    Accepted forms: ``"<amount>/<unit>"`` where unit is ``s/m/h/d`` (or the
    spelled-out word), or ``"<amount>/<number>s"`` for arbitrary windows
    (``"1000/10s"``, ``"50/0.2s"``). Case-insensitive.
    """
    amount_s, sep, period_s = spec.partition("/")
    if not sep:
        raise ValueError(f"rate spec needs '<amount>/<window>': {spec!r}")
    try:
        amount = int(amount_s.strip())
    except ValueError as exc:
        raise ValueError(f"rate amount must be an integer: {spec!r}") from exc
    if amount < 1:
        raise ValueError(f"rate amount must be >= 1: {spec!r}")
    period = _parse_period(period_s.strip().lower(), spec)
    return amount / period, amount


def _parse_period(period_s: str, spec: str) -> float:
    if period_s in _UNITS:
        return _UNITS[period_s]
    if period_s.endswith("s"):
        try:
            window = float(period_s[:-1])
        except ValueError as exc:
            raise ValueError(f"unknown rate window: {spec!r}") from exc
        if window > 0:
            return window
    raise ValueError(f"unknown rate window: {spec!r}")
