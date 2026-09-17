"""Scalar GCRA core: one number per client.

Each client's entire state is a single integer S — the earliest time
(monotonic nanoseconds) at which its next request may be admitted.
Every admitted request pushes S one drip-interval forward; S may lead
the real clock by at most ``burst`` intervals. Nothing is ever deleted,
reset, or scheduled: while a client is idle the clock walks toward S and
the credit (S - now) melts on its own.

Admission rule (the whole algorithm)::

    S' = max(now, S) + period_ns
    S' - now > burst_ns   ->  reject, retry_after_ns = S' - burst_ns - now
    otherwise             ->  admit, slot := S'

v0.1 keeps slots in a plain dict. v0.2 replaces the dict with a
fixed-footprint mmap arena shared across worker processes; the
arithmetic in :meth:`GcraLimiter.try_acquire` stays identical.
"""

from __future__ import annotations

import time

__all__ = ["Decision", "GcraLimiter"]

from typing import NamedTuple


class Decision(NamedTuple):
    """Result of one admission check."""

    allowed: bool
    retry_after_ns: int  # 0 when allowed; exact wait otherwise


class GcraLimiter:
    """In-process GCRA limiter.

    Args:
        rate_per_second: sustained allowance (e.g. ``1000 / 60`` for
            1000 requests per minute, i.e. one drip every 60 ms).
        burst: how many drip-intervals a client may bank while firing
            fast (the maximum lead of S over the clock).
    """

    def __init__(self, rate_per_second: float, burst: int = 1) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self._period_ns = round(1_000_000_000 / rate_per_second)
        if self._period_ns < 1:
            raise ValueError("rate too high for nanosecond resolution")
        self._burst_ns = self._period_ns * burst
        self._slots: dict[str, int] = {}

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def burst_ns(self) -> int:
        return self._burst_ns

    def try_acquire(self, key: str, now_ns: int | None = None) -> Decision:
        """Check one request. ``now_ns`` is injectable for deterministic tests."""
        now = time.monotonic_ns() if now_ns is None else now_ns
        slot = self._slots.get(key, 0)
        new_slot = max(now, slot) + self._period_ns
        if new_slot - now > self._burst_ns:
            return Decision(False, new_slot - self._burst_ns - now)
        self._slots[key] = new_slot
        return Decision(True, 0)
