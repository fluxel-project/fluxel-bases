# fluxel-assets baseline and decision

> Review note: the formal baseline below remains unchanged. The requested
> slot/lock cross-test and its reproducible rejected-candidate bundles are
> recorded in the final section.

## Decision

Retain the measured implementation. The representative prepared-asset workload
is allocation-free and its three timing rounds are stable enough to establish a
baseline. No observed cost currently justifies a more complex synchronization
or storage design, so this milestone records evidence and regression workloads
without speculative optimization.

This is an **Accepted baseline**, not an assertion that the implementation is
globally optimal. The benchmark models the CPU asset contract only; rendering
residency must be measured independently before it can motivate a candidate.

## Exact source and environment

- Benchmark source revision: `ed992a7af4a08415f2044a46c9ff6d5edad7485b`
- The retained source differs from that revision only by these recorded result
  files and accompanying documentation; benchmark and library sources are
  unchanged.
- Toolchain: `rustc 1.98.0 (88d9e12ae 2026-08-18)`, Cargo 1.98.0,
  `x86_64-pc-windows-msvc`.
- OS: Microsoft Windows 11 Home Chinese, version 10.0.26200, build 26200.
- CPU: AMD Ryzen 7 8845H with Radeon 780M Graphics, 16 logical processors.
- Visible RAM: 64,163,266,560 bytes.
- Power: Windows High performance scheme
  `8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c`, AC power, battery 99%.
- Background conditions: normal interactive development session; no deliberate
  competing build or benchmark was run. Thermal state was not instrumented.
- Profile: Cargo `bench` optimized profile. Criterion 0.5.1 without plot/report
  features; System allocator for the allocation probe.

## Protocol

Timing command, repeated as three adjacent independent rounds with distinct
saved Criterion baselines:

```console
cargo +stable bench -p fluxel-assets --bench asset_store --locked -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 30 \
  --save-baseline asset-store-roundN
```

Allocation command, three adjacent rounds:

```console
cargo +stable bench -p fluxel-assets --bench allocations --locked -- \
  --iterations=100
```

Benchmark smoke was run separately with Criterion `--test` and one allocation
iteration. Smoke numbers were not included in the decision. Criterion estimates
are nanoseconds per full workload invocation and report 95% confidence
intervals. Durable summaries are in
[`timings.csv`](0.13.3-timings.csv) and
[`allocations.csv`](0.13.3-allocations.csv).

## Representative results

The primary workload prepares 4,096 ready identities outside the timed window,
then performs 1,024 deterministic frame-facing uses. Each use calls both
`acquire` and `observe`, consumes the immutable snapshot, and releases it.

| Round | Mean | 95% confidence interval | Allocations |
| --- | ---: | ---: | ---: |
| 1 | 48.344 us | 48.179–48.518 us | 0 |
| 2 | 48.563 us | 48.433–48.715 us | 0 |
| 3 | 48.322 us | 48.190–48.468 us | 0 |

The round means span about 0.5%. One workload invocation contains 2,048 store
queries, so the mean is about 23.6 ns per query on this machine; that quotient
is descriptive only and is not a separately sampled latency distribution.

The secondary cold batch creates, produces, commits, and observes 256 assets:
52.248 us, 53.013 us, and 52.208 us across the three rounds. It consistently
made 1,032 allocations requesting 112,384 gross bytes per batch, ending with no
allocation-counter live-byte delta after the batch store was dropped.

## Guards and diagnostics

- One replacement: 135.8–140.9 ns mean across rounds, three allocations and
  232 requested bytes.
- Collecting 256 ready values: 19.5–20.7 us round means. Its wider within-round
  confidence intervals make small collection changes inconclusive without a
  longer dedicated run.
- Two-thread single-flight: 125.8–130.0 us round means. This row includes OS
  thread creation and joining, so it is a semantic/concurrency guard rather
  than a steady-state scheduler benchmark.
- Stale identity rejection: 182.9–189.3 ns round means. This diagnostic includes
  store creation, removal, slot reuse, and the structured rejection.
- Isolated 64-byte commit: six allocations, 712 requested and peak-live bytes.

The collection allocation row has a negative end-live delta by construction:
its 256-value fixture is created before the measurement window, then destroyed
inside the window. It is useful for allocation-call/peak diagnostics but is not
a process-memory total.

All 24 correctness tests, including real two-thread producer election,
exact-attempt waiter completion, stale recycling, replacement failure/cancel,
deterministic collection, and reentrant destructor lock boundaries, remained
green. Stable/MSRV, Clippy, docs, examples, and benchmark smoke are delivery
gates and are recorded separately from timing evidence.

## Experiment record

ID and status: `asset-store-sync — Deferred`

- Hypothesis: sharding or read-optimized synchronization might reduce hot
  lookup contention.
- Evidence: the representative single-thread frame is already allocation-free;
  this campaign did not demonstrate a product-level contention cost. The
  two-thread row is dominated by thread lifecycle and cannot attribute lock
  cost.
- Decision: do not implement or A/B a candidate. Additional locks, shards, or
  unsafe machinery would add semantic and maintenance risk without an observed
  representative benefit.
- Retest condition: residency profiling shows sustained multi-threaded
  frame-preparation contention, or a stable consumer benchmark demonstrates a
  material regression against this baseline.

No candidate code was retained or rejected in this campaign. Profiling was not
used to invent an attribution because the representative measurement did not
identify a cost requiring an optimization hypothesis.

### Requested lock and slot cross-test

The review pass added a bounded cross-test without rewriting the formal
baseline. It used the same primary workload in an interleaved rapid screen: 0.5
second warm-up, 1 second measurement, 20 Criterion samples, and the sequence
baseline/Mutex/RwLock/RwLock/Mutex/baseline/Mutex/baseline/RwLock. Durable
estimates are in [`lock-cross-test.csv`](0.13.4-lock-cross-test.csv).

The rapid screen used the same machine, OS, power scheme, toolchain, allocator,
and primary workload recorded above. Every timed sample used:

```console
cargo +stable bench -p fluxel-assets --bench asset_store --locked -- \
  --warm-up-time 0.5 --measurement-time 1 --sample-size 20
```

The rejected sources and their commit objects are preserved as Git bundles
under [`experiments/`](experiments/). Clone `parking-locks.bundle` and select
`experiment/parking-mutex` for `297ffe3` or `experiment/parking-rwlock` for
`debf7ca`; clone `slotmap-reuse-audit.bundle` and select
`exp/slotmap-reuse-audit` for `e0085cb`. Each history is rooted in the recorded
`502bfb6` baseline. Run
`cargo test -p fluxel-assets --all-targets --locked` and the benchmark command
above after each lock candidate. The CSV `sequence` column records the actual
interleaved measurement order. These bundles are evidence only and are not
production dependencies.

ID and status: `slot-store-replacement — Rejected before timing`

- Candidate: exact `slotmap = 1.0.7` replacing the hand-rolled slot store.
- Static and correctness evidence: `SlotMap` reuses the head of its internal
  free list, so removing slot 1 and then slot 2 makes the next insertion reuse
  slot 2. The accepted asset contract deterministically chooses the lowest free
  slot, slot 1. Its public API cannot insert into a chosen vacant slot.
- `KeyData::as_ffi` guarantees round trip, not an identity-layout contract, and
  slotmap's wrapping generation cannot preserve checked exhaustion. Retaining a
  second index/generation map would keep the current mechanism while adding a
  second source of truth.
- Decision: reject before timing. A diagnostic speed cannot override identity,
  stale-handle, and deterministic-recycle correctness.

ID and status: `parking-mutex — Rejected`

- Candidate: exact `parking_lot = 0.12.5`, drop-in `parking_lot::Mutex`, local
  experiment revision `297ffe3e04fed5c65932722d92b84d1a134df48d`.
- Correctness: 24/24 pre-overflow contract tests and Clippy passed in the
  isolated experiment.
- Rapid A/B: the three candidate means were 47.725, 48.798, and 47.787 us;
  adjacent standard-library means were 48.295, 48.243, and 49.427 us. One pair
  favored the candidate by about 1.2%, one made it about 1.1% slower, and the
  last comparison coincided with an elevated baseline. The direction is not
  stable enough to clear environmental variation.
- Trade-off: parking_lot removes poisoning, making the public
  `SynchronizationPoisoned` path unreachable, and adds five locked transitive
  packages. An inconclusive small timing difference does not justify that
  semantic and dependency cost.
- Decision: reject; no candidate code retained and no formal validation run.

ID and status: `parking-rwlock — Rejected`

- Candidate: `parking_lot::RwLock` plus atomic last-touch tickets and true read
  fast paths for ready/failed/producing `acquire` and `observe`, local experiment
  revision `debf7ca8f79d78fd43929bebf00fbc18b9cf190a`.
- Correctness: the same 24/24 tests and Clippy passed in isolation; no public API
  or `unsafe` code was added.
- Rapid A/B: candidate means were 59.072, 59.191, and 59.039 us, consistently
  around 22% slower than the adjacent 48–49 us standard-library baselines.
  Atomic touch traffic and RwLock machinery are present in the measurement, but
  this screen does not separately attribute their shares.
- Trade-off: it also loses poisoning semantics and adds dependency/atomic
  complexity.
- Decision: reject at short A/B; no guard expansion or formal validation was
  warranted.

The production choice remains the accepted hand-rolled deterministic slot
store with `std::sync::Mutex`.

## Limits

These results describe one Windows machine, toolchain, power configuration,
allocator, and synthetic renderer-facing access shape. They do not cover I/O,
decoding, arbitrary consumer key distributions, GPU residency, process-wide
memory, mobile hardware, or browser scheduling. Future comparisons must keep
the source workload and environment compatible or record a separate baseline.
