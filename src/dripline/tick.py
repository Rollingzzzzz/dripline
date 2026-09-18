"""Coarse-tick GCRA: exact admits, clock-amortized rejects.

Every *admit* pays a real clock read, so admitted time is always exact.
Rejects compare the slot against a shared coarse ``tick`` — an integer
attribute load instead of a ``clock_gettime`` — refreshed every
``pump_every`` decisions (and on every admit, where the fresh reading is
already in hand).

Direction of error: a stale tick is always in the past, so
``slot - tick`` overestimates the lead of a hot slot. Rejections therefore
become *stricter* as the tick ages, never looser — the same fail-closed
direction as :mod:`dripline.bloom`, bounded here by the pump interval:
staleness in wall time is at most ``pump_every`` decisions, so a client
whose credit just restored may be held back for at most that many
inter-decisions. Budgets are never exceeded; under-admission is bounded.

Why this exists: it measures whether an attribute load + counter beats a
vDSO clock read in CPython. If the scoreboard says no, this module becomes
an ADR ("revisit with driver-level pumping or a C accelerator") rather than
a shipped engine.
"""

from __future__ import annotations

import time

from dripline.core import GcraLimiter

__all__ = ["TickGcraLimiter"]


class TickGcraLimiter(GcraLimiter):
    """GCRA limiter with a coarse shared tick on the reject path.

    Args:
        rate_per_second: sustained allowance (same semantics as
            :class:`dripline.core.GcraLimiter`).
        burst: how many drip-intervals a client may bank.
        pump_every: decisions between tick refreshes on the reject path.
            Smaller = fresher tick, more counter traffic; larger = wider
            strictness band under pressure.
    """

    __slots__ = ("_n", "_pump", "_tick")

    def __init__(self, rate_per_second: float, burst: int = 1,
                 pump_every: int = 512) -> None:
        super().__init__(rate_per_second, burst)
        if pump_every < 1:
            raise ValueError("pump_every must be >= 1")
        self._pump = pump_every
        self._n = pump_every
        self._tick = time.monotonic_ns()

    def try_acquire(self, key: str, now_ns: int | None = None) -> int:
        """``0`` = admitted; ``>0`` = wait in ns (exact on admits,
        conservative when the tick is stale)."""
        tick = self._tick
        try:
            slot = self._slots[key]
        except KeyError:
            slot = 0
        if slot > tick:
            retry = slot - tick - self._bp
            if retry > 0:
                n = self._n - 1
                if n:
                    self._n = n
                    return retry
                # Pump: refresh the tick, then judge this call exactly —
                # the amortized clock read also caps stale-tick over-rejects.
                now = self._now() if now_ns is None else now_ns
                self._n = self._pump
                self._tick = now
                return GcraLimiter.try_acquire(self, key, now)
            # Admitting a returning client: exact clock, never the tick.
            now = self._now() if now_ns is None else now_ns
            retry = slot - now - self._bp
            if retry > 0:
                return retry
            self._tick = now
            self._slots[key] = slot + self._period_ns
            return 0
        # Fresh or fully drained client: exact clock.
        now = self._now() if now_ns is None else now_ns
        self._tick = now
        self._slots[key] = now + self._period_ns
        return 0
