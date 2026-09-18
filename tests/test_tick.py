"""Smoke tests for the coarse-tick reject path."""

from dripline import TickGcraLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_admits_are_exact():
    """Every admit reads a fresh clock: budget arithmetic is unchanged."""
    lim = TickGcraLimiter(rate_per_second=1000 / 60, burst=100, pump_every=64)
    allowed = sum(1 for _ in range(100)
                  if lim.try_acquire("c", now_ns=T) == 0)
    assert allowed == 100
    retry = lim.try_acquire("c", now_ns=T)
    assert 50_000_000 <= retry <= 70_000_000


def test_stale_tick_only_stricter():
    """A never-refreshed tick over-rejects a drained client, never over-admits.

    pump_every is huge, so the tick stays frozen at the last admit; the
    fully-banked client keeps getting rejected on the coarse path. With the
    pump active, the same call admits exactly.
    """
    frozen = TickGcraLimiter(rate_per_second=1000 / 60, burst=100,
                             pump_every=10**9)
    now = frozen._now()
    for _ in range(100):  # full bank; each admit refreshes the tick
        frozen.try_acquire("c")
    # Fast-forward the internal clock past the client's full drain window.
    later = now + frozen.burst_ns + frozen.period_ns
    saved, frozen._now = frozen._now, (lambda: later)
    try:
        assert frozen.try_acquire("c") > 0          # stale tick: stricter
    finally:
        frozen._now = saved

    pumped = TickGcraLimiter(rate_per_second=1000 / 60, burst=100, pump_every=1)
    now = pumped._now()
    for _ in range(100):
        pumped.try_acquire("c")
    later = now + pumped.burst_ns + pumped.period_ns
    saved, pumped._now = pumped._now, (lambda: later)
    try:
        assert pumped.try_acquire("c") == 0         # fresh tick: exact admit
    finally:
        pumped._now = saved


def test_sustained_rate_within_exact_bound():
    lim = TickGcraLimiter(rate_per_second=1000 / 60, burst=100, pump_every=128)
    now = lim._now()
    clock = {"t": now}

    def fake():
        clock["t"] += 10_000_000  # 10 ms apart, one simulated minute
        return clock["t"]

    lim._now = fake
    allowed = sum(1 for _ in range(6000) if lim.try_acquire("c") == 0)
    exact = 100 + 60_000_000_000 // lim.period_ns  # burst + sustained drips
    assert allowed <= exact + 1                    # never more than exact
    assert allowed >= 100                          # burst always admitted


if __name__ == "__main__":
    test_admits_are_exact()
    test_stale_tick_only_stricter()
    test_sustained_rate_within_exact_bound()
    print("all tick tests passed")
