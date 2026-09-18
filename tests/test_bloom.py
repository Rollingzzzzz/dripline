"""Smoke tests for the red-bloom reject fast-path."""

from dripline import BloomGcraLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_exact_on_the_miss_path():
    """First burst + first reject are the exact GCRA semantics."""
    lim = BloomGcraLimiter(rate_per_second=1000 / 60, burst=100,
                           bloom_words=64, rotate_ns=250_000_000)
    allowed = sum(1 for _ in range(100)
                  if lim.try_acquire("c", now_ns=T) == 0)
    assert allowed == 100
    retry = lim.try_acquire("c", now_ns=T)
    assert 50_000_000 <= retry <= 70_000_000  # exact hint, not the window


def test_hit_path_is_conservative_and_bounded():
    lim = BloomGcraLimiter(rate_per_second=1000 / 60, burst=100,
                           bloom_words=64, rotate_ns=250_000_000,
                           rotate_check_every=1)
    for _ in range(101):
        lim.try_acquire("c", now_ns=T)
    # Second reject of the same key: served by the bloom hit path.
    assert lim.try_acquire("c", now_ns=T) == lim.rotate_ns
    # …but the over-rejection is bounded: after one rotation window the
    # filter has rotated (valve checks every hit here) and the client is
    # judged exactly again.
    late = T + lim.rotate_ns + 10**9
    assert lim.try_acquire("c", now_ns=late) == lim.rotate_ns  # rotation call
    assert lim.try_acquire("c", now_ns=late) == 0              # exact admit


def test_false_positives_fail_closed():
    """A saturated filter may reject fresh clients — never admit over budget.

    One word of filter and thousands of hammered keys: the word saturates,
    so a brand-new client rides the hit path and gets the rotation window.
    That is the documented fail-closed direction, bounded by rotation.
    """
    lim = BloomGcraLimiter(rate_per_second=1.0, burst=1, bloom_words=1,
                           rotate_ns=100_000_000, rotate_check_every=1)
    for i in range(2000):  # 1 admit + 1 reject each: bits fill the single word
        lim.try_acquire(f"hammer-{i}", now_ns=T)
        lim.try_acquire(f"hammer-{i}", now_ns=T)
    assert lim.try_acquire("brand-new-client", now_ns=T) == lim.rotate_ns
    # Rotation (observed via a clock-advancing hit) clears the block.
    late = T + lim.rotate_ns
    assert lim.try_acquire("brand-new-client", now_ns=late) == lim.rotate_ns
    assert lim.try_acquire("brand-new-client", now_ns=late) == 0


def test_budget_never_exceeds_exact():
    """Whatever the filter does, admits stay within the GCRA budget."""
    lim = BloomGcraLimiter(rate_per_second=1000 / 60, burst=100,
                           bloom_words=256, rotate_ns=50_000_000)
    admitted = sum(1 for i in range(5000)
                   if lim.try_acquire(f"c{i % 50}", now_ns=T) == 0)
    # 50 clients x burst 100 at one instant — the exact core cannot admit more.
    assert admitted <= 50 * 100 + 1


if __name__ == "__main__":
    test_exact_on_the_miss_path()
    test_hit_path_is_conservative_and_bounded()
    test_false_positives_fail_closed()
    test_budget_never_exceeds_exact()
    print("all bloom tests passed")
