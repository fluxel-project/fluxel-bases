# 0.13.3 performance decision plan

This document defines the measurement and release decision for the asset
resource-manager implementation.  It is a plan, not a performance claim: no
result is implied until measurements are recorded against the candidate source
revision.

The work is organized as a small evidence-led performance stage, following the
separation of design, contract, implementation, and benchmark evidence used by
the `slot-graph` history, without copying its workloads or conclusions.

## Decision to make

The primary product goal is representative end-to-end asset lookup and
generation validation latency while a frame-facing client resolves a mixed set
of live CPU assets.  The chosen implementation must keep this path predictable
as the manager grows; a microbenchmark result alone cannot select a release.

Secondary metrics are:

- allocations and allocated bytes per representative operation;
- retained manager memory at each input scale;
- throughput for sustained resolve/release traffic; and
- latency and allocation behaviour for guard workloads, including stale-handle
  rejection and cache churn.

Correctness is a gate, not a metric.  Typed identity, generation invalidation,
reuse, and error semantics must continue to pass their contract tests before a
candidate can be considered.

## Workload roles and inputs

Each benchmark case declares one role; rows do not receive equal decision
weight.

| Role | Workload | Input scales | Use in decision |
| --- | --- | --- | --- |
| Correctness gate | create, acquire, release, recycle, exact-attempt waiting, and stale rejection | 24 deterministic public-contract cases, including two-thread single-flight | failure rejects a candidate |
| Representative | a prepared table of 4,096 ready assets resolving and observing 1,024 frame-facing uses | fixed 4,096/1,024 table/frame shape | primary release metric |
| Secondary representative | cold identity creation, unique production, commit, and observation | batches of 256 assets with 64-byte values | prevents the hot path from hiding production cost |
| Guard | replacement, explicit collection, and two-thread single-flight | one replacement, 256 collected values, two acquirers | catches material alternate-path regressions |
| Diagnostic | stale rejection, isolated commit, and allocation accounting | one recycled identity or one 64-byte commit | explains measured shape; never selects alone |

The primary representative case models CPU-side identity and generation
resolution in a stable renderer-facing live set; the fixed stride walks the
prepared table deterministically. It intentionally does not model file I/O, decoding, GPU upload,
scheduler contention, or application-specific asset eviction policy; those are
outside the 0.13 asset-manager contract.

## Measurement protocol

The first task is to measure the shipped implementation as the baseline.  No
optimization is assumed or required.  A change is considered only when the
baseline evidence identifies a concrete cost and an expected user-relevant gain
can be tested.

Smoke and formal measurement are distinct:

- **Smoke:** build the benchmark suite and execute every selected case once,
  checking setup and assertions.  It detects broken measurement plumbing and
  produces no performance conclusion.
- **Formal baseline or candidate measurement:** use fixed inputs and settings,
  include warm-up, and collect three independent rounds.  Keep samples adjacent
  or randomized/interleaved between baseline and candidate rather than
  comparing runs separated by unrelated machine activity.

For every formal round, record the exact Git revision, Rust toolchain and
Cargo profile, benchmark command and parameters, operating system and version,
CPU model and logical-core count, RAM, storage-relevant conditions if any,
power plan and power source, background load, and allocator/profiler settings.
Also preserve the raw benchmark summary and any allocation or profiling data
needed to explain a subsequent decision.

The three rounds must begin from equivalent machine conditions.  If a run is
interrupted, thermally unstable, or affected by an identified competing load,
record and repeat it rather than silently replacing it.  Results from different
machines, toolchains, or input definitions are separate baselines.

## Candidate funnel

Only evidence-backed candidates enter the funnel, with at most one to three
independent candidates in this release stage.

1. State the measured cost, the hypothesis, the narrowly scoped change, API
   impact, semantic risk, and expected representative benefit.
2. Perform static review and a cheap probe: targeted correctness tests,
   allocation/counter checks, and a short representative A/B run.
3. Promote only promising candidates to an interleaved short A/B run covering
   the representative workload, relevant guards, and diagnostics.
4. Formally measure only the provisional winner: three independent rounds,
   complete correctness gate, and release checks.  Combine candidates only when
   their mechanisms are demonstrably independent, and measure the combination
   directly.

Candidates that fail correctness, introduce unjustified public API or
maintenance cost, or show no credible representative benefit stop at their
current stage.  Rejected implementation code is removed; its evidence remains
in the release record.

## Noise-aware release rule

Compare like with like and inspect the distribution across all three rounds,
not an attractive single percentage.  A change must exceed observed normal
variation on the representative workload, have plausible user value, and avoid
a material unexplained guard regression.  Diagnostics may explain the result,
but cannot override the representative outcome.

When the difference overlaps normal noise, improve measurement quality or mark
the result inconclusive; do not invent an attribution.  This stage optimizes
only if the evidence clears that bar.  Otherwise the baseline remains the
release implementation and its measurements become the durable baseline.

## Experiment record template

Use one completed record per meaningful experiment.  Do not pre-fill result
fields before measurement.

```text
ID and status: Accepted | Rejected | Deferred | Inconclusive
Baseline and candidate revision:
Environment and benchmark command:
Hypothesis and isolated change:
Target metric and workload role:
Inputs, warm-up, and three-round measurement record:
Representative result:
Guard and diagnostic results:
Allocation/profile evidence:
Correctness and compatibility status:
Noise/confidence limits and trade-offs:
Decision rationale:
Future retest condition:
```

An **Accepted** record identifies the selected implementation and its formal
evidence.  A **Rejected** record identifies why it is not retained.  A
**Deferred** record names the missing prerequisite or time-box reason.  An
**Inconclusive** record states what overlap, instability, or missing evidence
prevented a decision and the condition required to retest.

## Stop condition

0.13.3 is complete when the current implementation has a reproducible formal
baseline and a recorded decision: retain that baseline, or retain a sufficiently
validated winner.  It is expressly valid—and preferred over speculative
complexity—to finish with no optimization when no candidate clears the
noise-aware representative-workload threshold.
