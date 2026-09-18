"""Presence map + coarse tick: the cheapest measured reject path.

Layering lesson from docs/adr/0001: a pre-filter only pays when its hit
path is cheaper than the path it replaces, and every layer charges its own
bytecode tax. The bloom attempt lost because bit packing costs a
``1 << k`` int allocation per check — as much as the whole tick reject path
it was trying to bypass.

This module drops the packing: the presence map is a flat ``bytearray``
addressed by the cached string hash — one index computation, one byte
load, one truth test, zero allocations. A set byte means "this client was
rejected a moment ago" and is answered without the clock, without the slot
table, without arithmetic: the hit path is ~150 ns against the tick path's
~267 ns. Misses fall through to the full coarse-tick engine
(:class:`dripline.tick.TickGcraLimiter`), which stays the source of truth
and gives exact admits.

Error direction — identical contract to the bloom and tick modes: the map
can only produce false *rejections* (a fresh key hashing onto a set byte),
never false admits, and the extra wait is bounded by one rotation window
(``rotate_ns``). Under pressure the limiter only gets stricter. A counter
valve on the hit path guarantees rotation checks happen even when every
request hits the map.
"""

from __future__ import annotations

from dripline.tick import TickGcraLimiter

__all__ = ["PresenceTickLimiter"]


class PresenceTickLimiter(TickGcraLimiter):
    """Coarse-tick GCRA with a byte-addressed presence map in front.

    Args:
        rate_per_second: sustained allowance (same semantics as
            :class:`dripline.core.GcraLimiter`).
        burst: how many drip-intervals a client may bank.
        map_bytes: presence map size in bytes (power of two). One set byte
            per recently rejected key; false-rejection rate ≈ active
            rejected keys / map_bytes, bounded by one rotation window.
        rotate_ns: rotation window in ns — the maximum extra wait a false
            rejection can impose. ``None`` (default) picks
            ``min(250 ms, 3 x period)``: wide enough that a hammering
            client re-registers rarely, tight enough that a client whose
            credit restored is not held back across more than a few drips.
        rotate_check_every: hit-path calls between wall-clock rotation
            checks (the valve).
        pump_every: tick pump interval, inherited by the underlying
            :class:`dripline.tick.TickGcraLimiter`.
    """

    __slots__ = ("_flags", "_fmask", "_n2", "_rce", "_rot_at", "_rotate_ns",
                 "_zeros")

    def __init__(self, rate_per_second: float, burst: int = 1,
                 map_bytes: int = 1 << 16,
                 rotate_ns: int | None = None,
                 rotate_check_every: int = 4096,
                 pump_every: int = 512) -> None:
        super().__init__(rate_per_second, burst, pump_every=pump_every)
        if map_bytes < 1 or map_bytes & (map_bytes - 1):
            raise ValueError("map_bytes must be a power of two")
        if rotate_ns is None:
            rotate_ns = min(250_000_000, 3 * self._period_ns)
        if rotate_ns <= 0:
            raise ValueError("rotate_ns must be positive")
        if rotate_check_every < 1:
            raise ValueError("rotate_check_every must be >= 1")
        self._fmask = map_bytes - 1
        self._rotate_ns = rotate_ns
        self._rce = rotate_check_every
        self._n2 = rotate_check_every
        self._rot_at = 0  # monotonic base; set on the first clocked call
        self._flags = bytearray(map_bytes)
        self._zeros = bytes(map_bytes)

    @property
    def rotate_ns(self) -> int:
        return self._rotate_ns

    def _rotate(self, now: int) -> None:
        self._flags[:] = self._zeros   # memset-speed via slice assignment
        self._rot_at = now

    def try_acquire(self, key: str, now_ns: int | None = None) -> int:
        """``0`` = admitted; ``>0`` = wait in ns (exact on admits, the
        rotation window as a conservative bound on presence hits)."""
        i = hash(key) & self._fmask
        if self._flags[i]:
            n = self._n2 - 1              # valve: even a saturated map must
            if n:                         # still observe the clock sometimes,
                self._n2 = n              # or rotation would never fire
            else:
                self._n2 = self._rce
                now = self._now() if now_ns is None else now_ns
                if now - self._rot_at >= self._rotate_ns:
                    self._rotate(now)
            return self._rotate_ns
        retry = TickGcraLimiter.try_acquire(self, key, now_ns)
        if retry:
            now = self._now() if now_ns is None else now_ns
            if now - self._rot_at >= self._rotate_ns:
                self._rotate(now)
            self._flags[i] = 1
        return retry
