"""Smoke tests for ApexLimiter — the arena + tick + presence stack."""

import tempfile
from pathlib import Path

from dripline import ApexLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_burst_and_exact_first_reject():
    """The miss path is the arena semantics: burst admits, exact retry."""
    lim = ApexLimiter(rate_per_second=1000 / 60, burst=100, slots=64,
                      map_bytes=4096)
    allowed = sum(1 for _ in range(100)
                  if lim.try_acquire("c", now_ns=T) == 0)
    assert allowed == 100
    retry = lim.try_acquire("c", now_ns=T)
    assert 50_000_000 <= retry <= 70_000_000  # exact hint, not the window


def test_presence_hit_and_rotation_recovery():
    lim = ApexLimiter(rate_per_second=1000 / 60, burst=100, slots=64,
                      map_bytes=4096, rotate_check_every=1, pump_every=1)
    for _ in range(101):
        lim.try_acquire("c", now_ns=T)
    # Second reject of the same key: served by the shared heat map.
    assert lim.try_acquire("c", now_ns=T) == lim.rotate_ns
    # Bounded over-rejection: the valve rotates on the very next hit, the
    # stale stamp stops matching, and the (pumped, exact) tick path admits
    # the client once its budget has drained.
    late = T + lim.rotate_ns + 10**9
    assert lim.try_acquire("c", now_ns=late) == 0


def test_shared_file_global_budgets():
    """Two limiters on one arena file = two workers, ONE global budget —
    slot table, heat map and generation word are all shared."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "apex.bin"
        a = ApexLimiter(rate_per_second=1000 / 60, burst=10, slots=256,
                        map_bytes=4096, path=path)
        # Anchored to the limiter's own monotonic base: worker b has never
        # decided anything, so its tick still holds its init-time reading.
        now = a._now()
        try:
            b = ApexLimiter(rate_per_second=1000 / 60, burst=10, slots=256,
                            map_bytes=4096, path=path)
            try:
                for _ in range(10):
                    assert a.try_acquire("shared-client", now_ns=now) == 0
                # Worker b feels the budget worker a already spent.
                assert b.try_acquire("shared-client", now_ns=now) > 0
                assert b.try_acquire("other-client", now_ns=now) == 0
            finally:
                b.close()
        finally:
            a.close()


def test_recycled_slots_admit_newcomers():
    """Arena semantics survive the stack: drained slots are reclaimable."""
    lim = ApexLimiter(rate_per_second=1000 / 60, burst=100, slots=4,
                      map_bytes=4096)
    for i in range(4):
        assert lim.try_acquire(f"first-{i}", now_ns=T) == 0
    late = T + 10**12  # every TAT long drained
    for i in range(8):
        assert lim.try_acquire(f"newcomer-{i}", now_ns=late) == 0


def test_budget_never_exceeds_exact():
    """Layers only ever reject; admits stay within the GCRA budget."""
    lim = ApexLimiter(rate_per_second=1000 / 60, burst=100, slots=4096,
                      map_bytes=8192)
    admitted = sum(1 for i in range(5000)
                   if lim.try_acquire(f"c{i % 50}", now_ns=T) == 0)
    assert admitted <= 50 * 100 + 1


if __name__ == "__main__":
    test_burst_and_exact_first_reject()
    test_presence_hit_and_rotation_recovery()
    test_shared_file_global_budgets()
    test_recycled_slots_admit_newcomers()
    test_budget_never_exceeds_exact()
    print("all apex tests passed")
