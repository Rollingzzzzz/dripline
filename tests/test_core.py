"""Smoke tests for the scalar GCRA core (int-return hot path).

The main test is literally the walkthrough from the README: 1000/min with
burst 100, one client firing instantly.
"""

from dripline import Decision, GcraLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_burst_then_reject_then_drip():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)  # 60 ms drip
    allowed = sum(1 for _ in range(100) if lim.try_acquire("client-421", now_ns=T) == 0)
    assert allowed == 100

    # 101st instant request: rejected with an exact retry hint (~60 ms).
    retry = lim.try_acquire("client-421", now_ns=T)
    assert retry > 0
    assert 50_000_000 <= retry <= 70_000_000

    # Rejections are free: slot untouched, so waiting the hint admits again.
    assert lim.try_acquire("client-421", now_ns=T + 60_000_000) == 0


def test_decision_wrapper_matches_int():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    assert lim.try_acquire_decision("c", now_ns=T) == Decision(True, 0)
    for _ in range(100):
        lim.try_acquire("c", now_ns=T)
    d = lim.try_acquire_decision("c", now_ns=T)
    assert d.allowed is False
    assert d.retry_after_ns == lim.try_acquire("c", now_ns=T)


def test_sustained_rate_settles():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    # Hammer at 10 ms spacing for one simulated minute.
    allowed = sum(1 for i in range(6000)
                  if lim.try_acquire("c", now_ns=T + 10_000_000 * i) == 0)
    assert 1000 <= allowed <= 1200  # drip rate + bounded burst bank


def test_idle_client_starts_fresh():
    lim = GcraLimiter(rate_per_second=1.0, burst=10)
    assert lim.try_acquire("a", now_ns=T) == 0
    # Long idle: the stale slot must not stack; max(now, S) restarts from now.
    assert lim.try_acquire("a", now_ns=T + 10**12) == 0


def test_clients_are_independent():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    for _ in range(100):
        assert lim.try_acquire("a", now_ns=T) == 0
    assert lim.try_acquire("a", now_ns=T) > 0
    assert lim.try_acquire("b", now_ns=T) == 0  # untouched by a's burst


if __name__ == "__main__":
    test_burst_then_reject_then_drip()
    test_decision_wrapper_matches_int()
    test_sustained_rate_settles()
    test_idle_client_starts_fresh()
    test_clients_are_independent()
    print("all core tests passed")
