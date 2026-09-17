"""Smoke tests for the scalar GCRA core.

The main test is literally the walkthrough from the README: 1000/min with
burst 100, one client firing instantly.
"""

from dripline import GcraLimiter

T = 1_000_000_000_000  # fixed base time (ns) for deterministic runs


def test_burst_then_reject_then_drip():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)  # 60 ms drip
    allowed = sum(lim.try_acquire("client-421", now_ns=T).allowed for _ in range(100))
    assert allowed == 100

    # 101st instant request: rejected with an exact retry hint (~60 ms).
    decision = lim.try_acquire("client-421", now_ns=T)
    assert not decision.allowed
    assert 50_000_000 <= decision.retry_after_ns <= 70_000_000

    # Rejections are free: slot untouched, so waiting the hint admits again.
    assert lim.try_acquire("client-421", now_ns=T + 60_000_000).allowed


def test_sustained_rate_settles():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    # Hammer at 10 ms spacing for one simulated minute.
    allowed = sum(lim.try_acquire("c", now_ns=T + 10_000_000 * i).allowed for i in range(6000))
    assert 1000 <= allowed <= 1200  # drip rate + bounded burst bank


def test_idle_client_starts_fresh():
    lim = GcraLimiter(rate_per_second=1.0, burst=10)
    assert lim.try_acquire("a", now_ns=T).allowed
    # Long idle: the stale slot must not stack; max(now, S) restarts from now.
    assert lim.try_acquire("a", now_ns=T + 10**12).allowed


def test_clients_are_independent():
    lim = GcraLimiter(rate_per_second=1000 / 60, burst=100)
    for _ in range(100):
        assert lim.try_acquire("a", now_ns=T).allowed
    assert not lim.try_acquire("a", now_ns=T).allowed
    assert lim.try_acquire("b", now_ns=T).allowed  # untouched by a's burst


if __name__ == "__main__":
    test_burst_then_reject_then_drip()
    test_sustained_rate_settles()
    test_idle_client_starts_fresh()
    test_clients_are_independent()
    print("all core tests passed")
