"""Apex: the whole stack in one shared mmap — arena + tick + presence.

Three ideas, one region, one file::

    request ──►  L1  shared presence map     hit? ──► reject (window bound)
                    │  miss
                    ▼
                 L2  arena slot lookup       (stable fingerprint, one probe)
                    ▼
                 L3  coarse tick on the slot over budget? ──► reject, no clock
                    │  has credit
                    ▼
                 L4  admit: fresh clock, exact arithmetic, shared slot write

What each layer buys, and what it costs:

**L1 — the shared heat map.** A flat byte per key hash, stamped with the
current *generation*. A hit means "some worker rejected this client a
moment ago" and is answered without a clock read, without the slot table,
without arithmetic. Rotation is O(1): instead of zeroing the map, every
worker occasionally bumps the generation word in the same file — bytes
stamped with older generations simply stop matching. Writing is deleting,
again. The map is shared across workers, so the whole deployment warms one
filter; false positives can only over-reject, bounded by one rotation
window (fail-closed, the standing direction of every dripline mode).

**L2 — the slot, not a dict.** No fingerprint cache here, by design: the
heat map already caches rejects — the ~99% case — so a cache would only
accelerate admits, which pay a clock read anyway. First sight of a key
pays one blake2b and a short probe; placement is identical in every
process (same key, same slot), so budgets are global with no coordination.

**L3/L4 — tick admits nothing false.** Rejects judge the slot against a
per-process coarse tick (an attribute load, not a ``clock_gettime``);
admits always read a fresh clock, so admitted time is exact and a stale
tick can only make the limiter stricter, never looser. Every ``pump_every``
decisions the pump refreshes the tick and re-judges that call exactly,
which also bounds how long a stale tick can hold a drained client back.

Races, all bounded and fail-closed: two workers writing one slot can lose
one update (±1 permit, the documented GCRA race class); concurrent
generation bumps can lose an increment or briefly move the generation back
one step (extends staleness by at most one window, never admits).
"""

from __future__ import annotations

import os

from dripline.arena import SLOT_BYTES, ArenaGcraLimiter, _fingerprint

__all__ = ["ApexLimiter"]

_GEN_MIN = 2      # generation stamps cycle 2..255; 0/1 are never written so
_GEN_MAX = 255    # a wrapped generation cannot resurrect truly stale bytes


class ApexLimiter(ArenaGcraLimiter):
    """Multi-process GCRA limiter: shared arena + tick + presence map.

    One mmap region, optionally file-backed (``path=``) so every worker
    process enforces the same global budgets — no Redis, no coordination,
    no cleanup thread. Open the same path in each worker and the heat map
    and slot table are warmed collectively.

    Args:
        rate_per_second: sustained allowance (same semantics as
            :class:`dripline.core.GcraLimiter`).
        burst: how many drip-intervals a client may bank.
        slots: arena capacity in 32-byte slots; size for ≤ 0.7 load factor
            at peak active clients. Idle slots drain and are recycled by
            newcomers (a drained slot is indistinguishable from a fresh
            client), so RSS is capped at the region size forever.
        path: arena file. ``None`` → private anonymous region (same code
            path, per-process budgets).
        map_bytes: presence map size in bytes (power of two), shared across
            workers. False-rejection rate ≈ recently rejected clients /
            map_bytes, bounded by one rotation window.
        rotate_ns: rotation window; ``None`` picks ``min(250 ms,
            3 x period)`` — wide enough that hammering clients re-register
            rarely, tight enough that restored credit is never held back
            across more than a few drips.
        rotate_check_every: presence-hit calls between rotation checks.
        pump_every: decisions between tick refreshes on the reject path.
    """

    __slots__ = ("_fmask", "_gen", "_genw", "_map", "_map_bytes", "_n", "_n2",
                 "_pump", "_rce", "_rot_at", "_rotate_ns", "_tick")

    def __init__(self, rate_per_second: float, burst: int = 1,
                 slots: int = 1_000_000,
                 path: str | os.PathLike | None = None,
                 map_bytes: int = 1 << 16,
                 rotate_ns: int | None = None,
                 rotate_check_every: int = 4096,
                 pump_every: int = 512) -> None:
        if map_bytes < 1 or map_bytes & (map_bytes - 1):
            raise ValueError("map_bytes must be a power of two")
        self._map_bytes = map_bytes
        # cache_slots=0: the presence map is the reject cache (see L2 above)
        super().__init__(rate_per_second, burst, slots=slots, path=path,
                         cache_slots=0)
        if rotate_ns is None:
            rotate_ns = min(250_000_000, 3 * self._period_ns)
        if rotate_ns <= 0:
            raise ValueError("rotate_ns must be positive")
        if rotate_check_every < 1 or pump_every < 1:
            raise ValueError("rotate_check_every and pump_every must be >= 1")
        self._fmask = map_bytes - 1
        self._rotate_ns = rotate_ns
        self._rce = rotate_check_every
        self._n2 = rotate_check_every
        self._rot_at = 0
        self._pump = pump_every
        self._n = pump_every
        self._tick = self._now()
        region = memoryview(self._mm)
        base = slots * SLOT_BYTES
        self._map = region[base:base + map_bytes]
        self._genw = region[base + map_bytes:].cast("q")  # the 8-byte tail
        if self._genw[0] < _GEN_MIN:                       # first opener boots
            self._genw[0] = _GEN_MIN                       # the generation
        self._gen = self._genw[0]

    def _region_size(self, slots: int) -> int:
        return slots * SLOT_BYTES + self._map_bytes + 8  # slots + map + gen

    @property
    def rotate_ns(self) -> int:
        return self._rotate_ns

    def close(self) -> None:
        self._map.release()      # our extra views must let go of the mmap
        self._genw.release()     # before the parent closes it
        ArenaGcraLimiter.close(self)

    def _rotate(self, now: int) -> None:
        # O(1) rotation: bump the shared generation; older stamps stop
        # matching. Two workers bumping at once can lose an increment —
        # harmless, the window just runs one step longer (fail-closed).
        g = self._genw[0]
        g = _GEN_MIN if g < _GEN_MIN or g >= _GEN_MAX else g + 1
        self._genw[0] = g
        self._gen = g
        self._rot_at = now

    def try_acquire(self, key: str, now_ns: int | None = None) -> int:
        """``0`` = admitted; ``>0`` = wait in ns (exact on admits, the
        rotation window as a conservative bound on presence hits)."""
        # L1 — shared heat map: one byte answers "seen over budget lately".
        i = hash(key) & self._fmask
        mp = self._map
        if mp[i] == self._gen:
            n = self._n2 - 1               # valve: a saturated map must still
            if n:                          # observe the clock sometimes, or
                self._n2 = n               # rotation would never fire
            else:
                self._n2 = self._rce
                now = self._now() if now_ns is None else now_ns
                if now - self._rot_at >= self._rotate_ns:
                    self._rotate(now)
            return self._rotate_ns

        # L2 — locate the slot in the shared arena: stable fingerprint,
        # linear probing, drained slots claimable by newcomers.
        w = self._words
        fp = _fingerprint(key)
        cap = self._cap
        tick = self._tick
        idx = home = fp % cap
        free = -1
        for _ in range(self._probes):
            base = idx * 4
            if w[base + 1] == fp:
                break
            if free < 0 and w[base] <= tick:   # empty or drained → claimable
                free = base
            idx += 1
            if idx == cap:
                idx = 0
        else:
            base = free if free >= 0 else home * 4  # claim, or share fate
            w[base + 1] = fp                       # (keeping the incumbent's TAT)
        tat = w[base]

        # L3 — coarse tick: reject without reading the clock.
        if tat > tick:
            retry = tat - tick - self._bp
            if retry > 0:
                n = self._n - 1
                if n:
                    self._n = n
                    mp[i] = self._gen        # mark hot in the shared map
                    return retry
                now = self._now() if now_ns is None else now_ns   # pump:
                self._n = self._pump                                # fresh
                self._tick = tick = now                              # tick,
                retry = tat - now - self._bp                         # exact
                if retry > 0:                                        # re-judge
                    mp[i] = self._gen
                    return retry
                w[base] = tat + self._period_ns
                return 0
            now = self._now() if now_ns is None else now_ns   # returning
            retry = tat - now - self._bp                      # client with
            if retry > 0:                                     # credit: judge
                mp[i] = self._gen                             # exactly
                return retry
            self._tick = now
            w[base] = tat + self._period_ns
            return 0

        # L4 — fresh or drained client: exact clock, shared slot write.
        now = self._now() if now_ns is None else now_ns
        self._tick = now
        w[base] = now + self._period_ns
        return 0
