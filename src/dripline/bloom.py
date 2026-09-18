"""Red-bloom: a fail-closed reject fast-path over the exact GCRA core.

The sustained-load scoreboard is reject-dominated (a max-speed driver sees
~99% rejects after the first burst). This module front-loads a rotating
Bloom filter of *recently rejected* keys: membership means "this client was
over budget a moment ago" and is answered WITHOUT a clock read, WITHOUT the
slot table — one array read and a mask. Misses fall through to the exact
GCRA path, which stays the source of truth.

Error direction, by construction:

- Bloom filters have false positives only. A false positive here rejects a
  client that actually had credit — over-rejection, never over-admission.
  That is the same direction as the project's clock-free promise: under
  pressure the limiter only gets *stricter*. The extra wait is bounded by
  one rotation window (``rotate_ns``).
- False negatives (a key whose window rotated) simply take the exact path
  and are re-inserted on their next reject — one exact-path call per window
  per hot key, no correctness impact. Single-buffered rotation keeps the
  over-rejection bound at exactly one window instead of two.

A counter valve on the hit path guarantees rotation checks happen even when
every request hits the filter (otherwise a fully-saturated filter could go
stale forever and over-reject with no bound).

Returned waits are two-tier: the exact GCRA retry on the miss path, and the
rotation window as a conservative upper bound on the hit path.
"""

from __future__ import annotations

from array import array

from dripline.core import GcraLimiter

__all__ = ["BloomGcraLimiter"]


class BloomGcraLimiter(GcraLimiter):
    """GCRA limiter with a rotating red-bloom reject fast-path.

    Args:
        rate_per_second: sustained allowance (same semantics as
            :class:`dripline.core.GcraLimiter`).
        burst: how many drip-intervals a client may bank.
        bloom_words: filter size in 64-bit words (power of two). Rule of
            thumb: one word per hot client keeps false positives near 1.6%.
        rotate_ns: rotation window in ns — the maximum extra wait a false
            positive (or a key that has since drained) can impose.
        rotate_check_every: hit-path calls between wall-clock rotation
            checks (the valve; smaller = tighter staleness, more clock
            reads on the hit path).
    """

    __slots__ = ("_cur", "_n", "_rce", "_rot_at", "_rotate_ns", "_wmask", "_zeros")

    def __init__(self, rate_per_second: float, burst: int = 1,
                 bloom_words: int = 1 << 17,
                 rotate_ns: int = 250_000_000,
                 rotate_check_every: int = 4096) -> None:
        super().__init__(rate_per_second, burst)
        if bloom_words < 1 or bloom_words & (bloom_words - 1):
            raise ValueError("bloom_words must be a power of two")
        if rotate_ns <= 0:
            raise ValueError("rotate_ns must be positive")
        if rotate_check_every < 1:
            raise ValueError("rotate_check_every must be >= 1")
        self._wmask = bloom_words - 1
        self._rotate_ns = rotate_ns
        self._rce = rotate_check_every
        self._n = rotate_check_every
        self._rot_at = 0  # monotonic base; set on the first clocked call
        self._cur = array("Q", bytes(8 * bloom_words))
        self._zeros = array("Q", bytes(8 * bloom_words))

    @property
    def rotate_ns(self) -> int:
        return self._rotate_ns

    def _rotate(self, now: int) -> None:
        self._cur[:] = self._zeros
        self._rot_at = now

    def try_acquire(self, key: str, now_ns: int | None = None) -> int:
        """``0`` = admitted; ``>0`` = wait in ns (exact on the miss path,
        the rotation window as a conservative bound on the hit path)."""
        h = hash(key)
        i = h & self._wmask
        bit = 1 << ((h >> 58) & 63)
        cur = self._cur
        if cur[i] & bit:
            n = self._n - 1              # valve: even a saturated filter must
            if n:                        # still observe the clock sometimes,
                self._n = n              # or rotation would never fire
            else:
                self._n = self._rce
                now = self._now() if now_ns is None else now_ns
                if now - self._rot_at >= self._rotate_ns:
                    self._rotate(now)
            return self._rotate_ns
        retry = GcraLimiter.try_acquire(self, key, now_ns)
        if retry:
            now = self._now() if now_ns is None else now_ns
            if now - self._rot_at >= self._rotate_ns:
                self._rotate(now)
            cur[i] |= bit
        return retry
