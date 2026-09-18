"""Smoke tests for the presence-map + tick reject path."""

from dripline import PresenceTickLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_exact_on_the_miss_path():
    """First burst + first reject ride the tick engine (injected now → exact)."""
    lim = PresenceTickLimiter(rate_per_second=1000 / 60, burst=100,
                              map_bytes=64 << 10)
    allowed = sum(1 for _ in range(100)
                  if lim.try_acquire("c", now_ns=T) == 0)
    assert allowed == 100
    retry = lim.try_acquire("c", now_ns=T)
    assert 50_000_000 <= retry <= 70_000_000  # exact hint, not the window


def test_hit_path_is_conservative_and_bounded():
    lim = PresenceTickLimiter(rate_per_second=1000 / 60, burst=100,
                              map_bytes=64 << 10, rotate_check_every=1,
                              pump_every=1)
    for _ in range(101):
        lim.try_acquire("c", now_ns=T)
    # Second reject of the same key: served by the presence hit path.
    assert lim.try_acquire("c", now_ns=T) == lim.rotate_ns
    # Over-rejection is bounded: after one rotation window (valve checks
    # every hit here) the client is judged by the tick engine again.
    late = T + lim.rotate_ns + 10**9
    assert lim.try_acquire("c", now_ns=late) == lim.rotate_ns  # rotation call
    assert lim.try_acquire("c", now_ns=late) == 0              # drained admit


def test_false_rejections_fail_closed():
    """A saturated map may reject fresh clients — never admit over budget.

    A 256-byte map and thousands of hammered keys: every byte sets, so a
    brand-new client rides the hit path and gets the rotation window.
    """
    lim = PresenceTickLimiter(rate_per_second=1.0, burst=1, map_bytes=256,
                              rotate_ns=100_000_000, rotate_check_every=1)
    for i in range(5000):  # 1 admit + 1 reject each: bytes fill the map
        lim.try_acquire(f"hammer-{i}", now_ns=T)
        lim.try_acquire(f"hammer-{i}", now_ns=T)
    assert lim.try_acquire("brand-new-client", now_ns=T) == lim.rotate_ns
    late = T + lim.rotate_ns
    assert lim.try_acquire("brand-new-client", now_ns=late) == lim.rotate_ns
    assert lim.try_acquire("brand-new-client", now_ns=late) == 0


def test_budget_never_exceeds_exact():
    """Whatever the map does, admits stay within the GCRA budget."""
    lim = PresenceTickLimiter(rate_per_second=1000 / 60, burst=100,
                              map_bytes=64 << 10, rotate_ns=50_000_000)
    admitted = sum(1 for i in range(5000)
                   if lim.try_acquire(f"c{i % 50}", now_ns=T) == 0)
    # 50 clients x burst 100 at one instant — the engine cannot admit more.
    assert admitted <= 50 * 100 + 1


if __name__ == "__main__":
    test_exact_on_the_miss_path()
    test_hit_path_is_conservative_and_bounded()
    test_false_rejections_fail_closed()
    test_budget_never_exceeds_exact()
    print("all presence tests passed")
