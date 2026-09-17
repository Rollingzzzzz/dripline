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

Implemented branch-shaped for the reject-dominated hot path: for a
returning client, reject iff ``S - now > burst_ns - period_ns`` — one
precomputed threshold (``_bp``), no ``max()`` call, no allocation.
Fresh or fully drained clients (S <= now) admit unconditionally, as
``period_ns <= burst_ns`` always holds.

``try_acquire`` returns an int, not a tuple: ``0`` = admitted, ``>0`` =
exact retry-after in ns (so ``if lim.try_acquire(k)`` means "rejected").
This keeps the hot path allocation-free — the single largest measured
cost of a tuple-returning API in CPython. Callers wanting the explicit
pair use :meth:`GcraLimiter.try_acquire_decision`.

v0.1 kept slots in a plain dict; v0.2 moved them into the mmap arena
(:mod:`dripline.arena`). The arithmetic in this module stays the
reference semantics both storages implement.
"""

from __future__ import annotations

import time
from typing import NamedTuple

__all__ = ["Decision", "GcraLimiter"]


class Decision(NamedTuple):
    """Result of one admission check (explicit form of the int return)."""

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

    __slots__ = ("_bp", "_burst_ns", "_now", "_period_ns", "_slots")

    def __init__(self, rate_per_second: float, burst: int = 1) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self._period_ns = round(1_000_000_000 / rate_per_second)
        if self._period_ns < 1:
            raise ValueError("rate too high for nanosecond resolution")
        self._burst_ns = self._period_ns * burst
        self._bp = self._burst_ns - self._period_ns  # reject threshold, precomputed
        self._slots: dict[str, int] = {}
        self._now = time.monotonic_ns  # bound once; no module lookups per call

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def burst_ns(self) -> int:
        return self._burst_ns

    def try_acquire(self, key: str, now_ns: int | None = None) -> int:
        """Check one request: ``0`` = admitted, ``>0`` = exact wait in ns.

        ``now_ns`` is injectable for deterministic tests.
        """
        try:
            slot = self._slots[key]
        except KeyError:
            slot = 0
        now = self._now() if now_ns is None else now_ns
        if slot > now:
            retry = slot - now - self._bp
            if retry > 0:
                return retry
            self._slots[key] = slot + self._period_ns
            return 0
        self._slots[key] = now + self._period_ns
        return 0

    def try_acquire_decision(self, key: str, now_ns: int | None = None) -> Decision:
        """Explicit pair form of :meth:`try_acquire` for ergonomic call sites."""
        retry = self.try_acquire(key, now_ns)
        return Decision(False, retry) if retry else Decision(True, 0)
