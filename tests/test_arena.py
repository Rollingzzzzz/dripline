"""Smoke tests for the v0.2 mmap slot arena (int-return hot path).

Two groups: parity with the scalar core (same admission arithmetic,
injected clock) and arena-specific contracts — shared budgets across
instances on one file, self-ageing recycling, and bounded share-fate
saturation.
"""

import tempfile
from pathlib import Path

from dripline import ArenaGcraLimiter, GcraLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_parity_with_scalar_core():
    """Same client, same clock sequence → identical decisions, slot for slot."""
    rate, burst = 1000 / 60, 100
    scalar = GcraLimiter(rate_per_second=rate, burst=burst)
    arena = ArenaGcraLimiter(rate_per_second=rate, burst=burst, slots=64)
    nows = [T + 1_000_000 * i for i in range(500)]  # hammering: admit → reject mix
    for now in nows:
        d1 = scalar.try_acquire("client-421", now_ns=now)
        d2 = arena.try_acquire("client-421", now_ns=now)
        assert d1 == d2, f"divergence at now={now}: {d1} vs {d2}"


def test_burst_then_reject_then_drip():
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=64)
    allowed = sum(1 for _ in range(100)
                  if lim.try_acquire("c", now_ns=T) == 0)
    assert allowed == 100
    retry = lim.try_acquire("c", now_ns=T)  # 101st instant request
    assert retry > 0
    assert 50_000_000 <= retry <= 70_000_000
    assert lim.try_acquire("c", now_ns=T + 60_000_000) == 0  # rejects touch nothing


def test_clients_are_independent():
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=1024)
    for _ in range(100):
        assert lim.try_acquire("a", now_ns=T) == 0
    assert lim.try_acquire("a", now_ns=T) > 0
    assert lim.try_acquire("b", now_ns=T) == 0


def test_shared_file_shares_budgets():
    """Two instances on one arena file = two workers, one global budget."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "arena.bin"
        a = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=10, slots=256, path=path)
        try:
            b = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=10, slots=256,
                                 path=path)
            try:
                for _ in range(10):
                    assert a.try_acquire("shared-client", now_ns=T) == 0
                # Worker b sees the budget worker a already spent.
                assert b.try_acquire("shared-client", now_ns=T) > 0
                assert b.try_acquire("other-client", now_ns=T) == 0
            finally:
                b.close()
        finally:
            a.close()


def test_drained_slots_are_recycled():
    """A saturated-but-idle arena hands slots to newcomers (writing is deleting)."""
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=4)
    for i in range(4):  # every slot taken, TAT ~6 s ahead of T
        assert lim.try_acquire(f"first-{i}", now_ns=T) == 0
    late = T + 10**12  # far beyond every TAT: all slots fully drained
    for i in range(8):  # newcomers must be admitted — recycled slots, no growth
        assert lim.try_acquire(f"newcomer-{i}", now_ns=late) == 0


def test_saturation_shares_fate_bounded():
    """All slots hold fully-banked clients → newcomer inherits their lead.

    The newcomer's first request is rejected, but the inherited wait is
    bounded by the incumbent's lead (≤ burst), after which it admits —
    it never loses more than one bank's worth of time.
    """
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=2)
    for _ in range(100):  # both incumbents fully bank their burst
        assert lim.try_acquire("a", now_ns=T) == 0
        assert lim.try_acquire("b", now_ns=T) == 0
    retry = lim.try_acquire("c", now_ns=T)  # no drained slot anywhere → share fate
    assert 0 < retry <= lim.period_ns
    assert lim.try_acquire("c", now_ns=T + lim.burst_ns) == 0


def test_idle_client_starts_fresh():
    lim = ArenaGcraLimiter(rate_per_second=1.0, burst=10, slots=64)
    assert lim.try_acquire("a", now_ns=T) == 0
    assert lim.try_acquire("a", now_ns=T + 10**12) == 0  # no stale stacking


def test_stats_needs_numpy_and_counts():
    lim = ArenaGcraLimiter(rate_per_second=1000 / 60, burst=100, slots=256)
    for i in range(10):
        lim.try_acquire(f"c{i}", now_ns=T + i)  # TAT just ahead of T
    try:
        import numpy  # noqa: F401
    except ImportError:
        return  # optional extra absent: stats() contract tested only where installed
    stats = lim.stats(now_ns=T)
    assert stats["slots"] == 256
    assert stats["occupied"] == 10
    assert stats["active"] == 10
    assert stats["load_factor"] == round(10 / 256, 4)


if __name__ == "__main__":
    test_parity_with_scalar_core()
    test_burst_then_reject_then_drip()
    test_clients_are_independent()
    test_shared_file_shares_budgets()
    test_drained_slots_are_recycled()
    test_saturation_shares_fate_bounded()
    test_idle_client_starts_fresh()
    test_stats_needs_numpy_and_counts()
    print("all arena tests passed")
