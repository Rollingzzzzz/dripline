# ADR 0001: red-bloom reject fast-path — rejected for CPython

Date: 2026-09-18 · Status: rejected as a shipped engine (class kept, experimental)

## Context

The standing scoreboard is reject-dominated (~99% rejects under max-speed
load). Idea: front the exact GCRA path with a rotating Bloom filter of
recently rejected keys — a hit answers "reject" with one array read and a
mask, no clock, no slot table; false positives only ever over-reject
(fail-closed), bounded by the rotation window. Implemented as
`BloomGcraLimiter` (`src/dripline/bloom.py`), measured in the pinned
container (v0.3 results).

## Measured (FAST protocol, Docker, Ryzen 3600X, CPython 3.12)

| cell | gcra (exact) | bloom | verdict |
|---|---:|---:|---|
| scenario loose dec/s | 8.87M | 7.23M | bloom 18% slower |
| scenario tight dec/s | 8.51M | 7.49M | bloom 12% slower |
| microbench zipf ns/dec | 306 | 444 | bloom 45% slower |
| microbench uniform ns/dec | 538 | 920 | admit tax +382 ns |

## Why it loses

After v0.2.1's allocation-free diet, the exact reject path costs ~300 ns:
cached `hash`, dict subscript, clock read, two int ops, int return. The
bloom hit path costs ~250-300 ns on its own (`hash` + word index + `1 << k`
allocation + array read + mask + valve counter) and it must run *in front
of* the exact path, so misses and admits pay it as pure overhead. The
pre-filter stopped being cheaper than the thing it filters. In C this
inverts (governor's own cost is ~15 ns); in CPython the interpreter charges
per bytecode, not per cleverness.

## Where the idea still wins

- Fronting the **mmap arena** (`ArenaGcraLimiter`): its miss path pays a
  blake2b fingerprint (~200+ ns) and probing; a ~250 ns bloom hit that skips
  fingerprinting could genuinely pay there. Not measured yet.
- Any future world with a cheaper filter check (C accelerator, or a
  cheaper-to-compute bit index without the `1 << k` allocation).

## Decision

`BloomGcraLimiter` (and its byte-map descendant `PresenceTickLimiter`,
which measured 15% vs tick's 16% in v0.3.1) were removed from the package
at the 0.4.0 release to keep the public surface sharp — git history keeps
both. The idea won where it was always going to win: the byte-addressed,
generation-stamped presence map is the L1 layer of `ApexLimiter`
(src/dripline/apex.py). Revisit a standalone filter only if a future
engine's miss path becomes expensive enough to front.
