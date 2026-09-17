"""Fixed-footprint mmap slot arena — the v0.2 storage for the GCRA core.

The admission arithmetic is byte-for-byte the one in :mod:`dripline.core`;
only the storage changes. Instead of a dict that grows forever, the arena is
one pre-allocated table of 32-byte slots (4 x int64 words) inside an
``mmap`` region:

    word 0  TAT          earliest admit time (monotonic ns) — the GCRA state
    word 1  FINGERPRINT  stable 64-bit client identity (blake2b-64)
    word 2  reserved     lock/epoch word for later versions (clock-free mode)
    word 3  reserved

Three properties fall out of that layout:

**Shared across workers (no Redis).** With ``path=`` the arena is a
file-backed shared mapping: every worker process opens the same file and the
counters are global, not per-worker — the xW over-admission of per-worker
limiters disappears. Concurrent read-modify-write on one slot from two
workers can lose an update; the overshoot is bounded (one permit per race,
the same ±1 class as in-process races, listed under honest limitations).

**Constant memory by construction.** The region is allocated once; there is
no cleanup thread and no eviction job. A slot whose TAT has fallen behind
the clock is fully drained — for GCRA that is *identical* to a fresh client
(max(now, S) restarts from now) — so any newcomer may claim it. Writing is
deleting: active clients keep their slots hot, idle ones are reclaimed by
the next newcomer, and RSS never exceeds ``slots x 32 bytes`` no matter how
many distinct clients pass through.

**Stable placement.`` Built-in ``hash(str)`` is randomized per process and
would scatter one client across different slots in different workers, so the
fingerprint is blake2b-64 of the key: same key, same slot, every process.

Lookup is open addressing with linear probing from ``fingerprint % slots``.
Within a bounded probe window (16): a fingerprint match is a hit; the first
drained slot is remembered as a claim candidate. No hit → claim the drained
slot, or — if the window is full of active clients — share fate at the home
bucket (overwrite the fingerprint, keep the TAT: the newcomer waits out the
incumbent's lead, bounded by ``burst`` intervals). Sizing rule: keep the
steady-state active-client count at ≤ 0.7 x slots and collisions become
noise; the counting is approximate, never exact.
"""

from __future__ import annotations

import mmap
import os
import time
from array import array
from hashlib import blake2b

__all__ = ["ArenaGcraLimiter"]

SLOT_BYTES = 32          # 4 x int64: TAT, FINGERPRINT, reserved, reserved
MAX_PROBES = 16          # bounded probe window; beyond it, share fate


def _fingerprint(key: str | bytes) -> int:
    """Stable 64-bit fingerprint (0 is never returned: fresh slots hold 0).

    Read signed so every value fits the arena's int64 words; Python's modulo
    keeps negative values mapping into [0, slots).
    """
    digest = blake2b(key if isinstance(key, bytes) else key.encode(),
                     digest_size=8).digest()
    fp = int.from_bytes(digest, "little", signed=True)
    return fp or 1


class ArenaGcraLimiter:
    """GCRA limiter over a fixed mmap slot arena.

    Args:
        rate_per_second: sustained allowance (identical semantics to
            :class:`dripline.core.GcraLimiter`).
        burst: how many drip-intervals a client may bank.
        slots: arena capacity in 32-byte slots. Size for ≤ 0.7 load factor
            at peak active clients (e.g. 10M clients → 16M slots ≈ 512 MiB).
        path: arena file. ``None`` → private anonymous arena (fixed
            footprint, but per-process). A path → file-backed shared
            mapping: open the same path in every worker and the budgets
            are enforced globally.
        cache_slots: size of the fixed direct-mapped fingerprint cache
            (power of two; 24 bytes per entry — three int64 arrays).
            The hot path becomes hash → cached slot → verify fingerprint;
            blake2b runs only on first sight of a key. ``0`` disables it.
    """

    __slots__ = ("_bp", "_burst_ns", "_cache_fp", "_cache_idx", "_cache_keys",
                 "_cap", "_cmask", "_mm", "_now", "_period_ns", "_probes", "_words")

    def __init__(self, rate_per_second: float, burst: int = 1,
                 slots: int = 1_000_000, path: str | os.PathLike | None = None,
                 cache_slots: int = 131_072) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        if slots < 1:
            raise ValueError("slots must be >= 1")
        if cache_slots < 0 or cache_slots & (cache_slots - 1):
            raise ValueError("cache_slots must be a power of two (or 0)")
        self._period_ns = round(1_000_000_000 / rate_per_second)
        if self._period_ns < 1:
            raise ValueError("rate too high for nanosecond resolution")
        self._burst_ns = self._period_ns * burst
        self._bp = self._burst_ns - self._period_ns  # reject threshold, precomputed
        self._now = time.monotonic_ns  # bound once; no module lookups per call
        self._cap = slots
        self._probes = min(MAX_PROBES, slots)
        if cache_slots:
            self._cmask = cache_slots - 1
            self._cache_keys = array("q", bytes(8 * cache_slots))
            self._cache_idx = array("q", bytes(8 * cache_slots))
            self._cache_fp = array("q", bytes(8 * cache_slots))
        else:
            self._cmask = -1
            self._cache_keys = None
        size = slots * SLOT_BYTES
        if path is None:
            self._mm = mmap.mmap(-1, size)
        else:
            # The handle is scoped to setup: the mapping stays valid after the
            # handle closes on both POSIX and Windows (the recommended pattern
            # for shared maps — other processes can open the file meanwhile).
            with open(path, "a+b") as f:     # create-or-open, never truncates down
                if os.fstat(f.fileno()).st_size < size:
                    f.truncate(size)
                self._mm = mmap.mmap(f.fileno(), size)  # shared map
        self._words = memoryview(self._mm).cast("q")

    # -- hot path ---------------------------------------------------------- #

    def try_acquire(self, key: str, now_ns: int | None = None) -> int:
        """Check one request: ``0`` = admitted, ``>0`` = exact wait in ns.

        ``now_ns`` injectable for deterministic tests.
        """
        now = self._now() if now_ns is None else now_ns
        period = self._period_ns
        bp = self._bp
        w = self._words
        ck = self._cache_keys
        if ck is not None:
            h = hash(key)
            ci = h & self._cmask
            if ck[ci] == h:                     # cached placement…
                base = self._cache_idx[ci] * 4
                if w[base + 1] == self._cache_fp[ci]:   # …still verified by fp
                    tat = w[base]
                    if tat > now:
                        retry = tat - now - bp
                        if retry > 0:
                            return retry
                        w[base] = tat + period
                        return 0
                    w[base] = now + period
                    return 0
        fp = _fingerprint(key)
        cap = self._cap
        idx = home = fp % cap
        hit = free = -1
        for _ in range(self._probes):
            base = idx * 4
            if w[base + 1] == fp:
                hit = base
                break
            if free < 0 and w[base] <= now:  # empty or fully drained → claimable
                free = base
            idx += 1
            if idx == cap:
                idx = 0
        if hit >= 0:
            base = hit
            tat = w[base]
        else:
            base = free if free >= 0 else home * 4
            w[base + 1] = fp               # claim (or share fate, keeping TAT)
            tat = w[base]
        if ck is not None:                 # prime the cache; entries are
            ck[ci] = h                     # self-healing — a stolen slot
            self._cache_idx[ci] = base // 4  # fails the fp verify next time
            self._cache_fp[ci] = fp
        if tat > now:
            retry = tat - now - bp
            if retry > 0:
                return retry
            w[base] = tat + period
            return 0
        w[base] = now + period
        return 0

    def try_acquire_decision(self, key: str, now_ns: int | None = None):
        """Explicit pair form of :meth:`try_acquire` (see :mod:`dripline.core`)."""
        from dripline.core import Decision
        retry = self.try_acquire(key, now_ns)
        return Decision(False, retry) if retry else Decision(True, 0)

    # -- maintenance (whole-table, vectorized — needs the numpy extra) ----- #

    def stats(self, now_ns: int | None = None) -> dict:
        """One vectorized pass over the arena: occupancy and activity.

        Requires NumPy (``pip install 'dripline[numpy]'``); the hot path
        never touches it.
        """
        try:
            import numpy as np
        except ImportError as exc:
            raise ImportError("ArenaGcraLimiter.stats() needs NumPy: "
                              "pip install 'dripline[numpy]'") from exc
        now = time.monotonic_ns() if now_ns is None else now_ns
        table = np.frombuffer(self._mm, dtype=np.int64).reshape(-1, 4)
        occupied = table[:, 1] != 0
        active = occupied & (table[:, 0] > now)
        occupied_n = int(occupied.sum())
        return {"slots": self._cap,
                "occupied": occupied_n,
                "active": int(active.sum()),
                "drained": occupied_n - int(active.sum()),
                "load_factor": round(occupied_n / self._cap, 4)}

    # -- lifecycle ---------------------------------------------------------- #

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def burst_ns(self) -> int:
        return self._burst_ns

    @property
    def slots(self) -> int:
        return self._cap

    def close(self) -> None:
        self._words.release()
        self._mm.close()

    def __enter__(self) -> ArenaGcraLimiter:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
