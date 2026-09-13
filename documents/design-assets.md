# Fluxel logical asset core

Status: accepted architecture for `fluxel-assets` 0.13

## Purpose

`fluxel-assets` owns platform-neutral logical identity, immutable content
generations, producer coordination, typed ownership, and deterministic CPU
cache collection. It lets independent consumers share one logical asset and
one in-progress production attempt without teaching the core how bytes are
located, decoded, scheduled, uploaded, or rendered.

The first demonstrated consumer is rendering residency in Fluxel Rendering
0.14. That consumer needs a stable pair of logical identity and immutable
content generation before it can safely key a device-specific realization.
The dependency direction is therefore:

```text
caller-owned source/loader policy
              |
              v
        fluxel-assets
              |
              v
rendering frame preparation -> RHI / RenderGraph
```

`fluxel-assets` remains a leaf crate. It does not depend on Rendering, Host,
JSBridge, an executor, a decoder, or a platform I/O API.

## Three identity domains

The design deliberately separates identities that are often conflated:

1. **Logical asset identity** identifies the user-visible asset across content
   replacements. It is a typed slot plus a slot generation. Recycling a slot
   creates a different identity; stale identities fail closed.
2. **Content generation** identifies one atomically committed immutable value
   for a logical identity. It advances only after a successful commit or
   replacement. Existing snapshots keep the generation they observed.
3. **Source or loader identity** belongs to the caller. A path, URL, embedded
   key, decoder request, or generated-data recipe is neither accepted nor
   interpreted by this crate.

GPU resource identity and device generation form a fourth, separate domain
owned by Rendering. They never appear in this crate's key or state.

## Typed identity and ownership

An `AssetKind` marker prevents a mesh handle from being used as an image
handle. An opaque `AssetId<K>` is copyable identity only and confers no
ownership. A `Handle<K>` is a strong logical user; a `WeakHandle<K>` does not
keep content live and can only be upgraded through the owning store.

The implementation uses a per-identity token. Strong handles own that token,
while the store retains its internal reference. Handles do not contain a raw
or typed pointer back to the store. This gives RAII ownership without a manager
back-pointer. A slot can be recycled only when it is not producing and the
store is the token's sole strong owner. Dropping the store token invalidates all
weak handles; a later occupant has a different slot generation.

An immutable `AssetSnapshot<K, V>` owns both a strong handle and the exact
content value, so replacement or cache collection cannot mutate or invalidate
already observed content. Handles and snapshots never implement implicit load
or state-changing dereference behavior.

## State machine

Each live identity is in exactly one public state:

```text
Missing --begin--> Producing(attempt)
   ^                   |       |
   |                 fail    commit
 retry                 |       |
   |                   v       v
 Failed(attempt) <---------  Ready(content generation)
   ^                              |
   |                              |
   +-------- replacement ---------+
```

`Missing` has no committed value. `Producing` names an attempt generation and
has exactly one valid producer permit. `Ready` exposes one immutable content
generation, reported resident bytes, and a snapshot. `Failed` exposes a typed,
shared failure value and the attempt that failed. Starting a retry or
replacement is explicit; observing a state never starts work.

Content replacement preserves the current `Ready` snapshot while a new attempt
is producing. The store atomically swaps in the new value only on successful
commit. A failed replacement leaves the previous committed generation
observable and reports the failed attempt separately. No observer can see a
half-written value.

## Producer coordination

Acquisition is identity-level single-flight. For a missing or explicitly
retrying identity, exactly one caller receives `ProducerPermit<K>`. Other
callers receive a waiter for that same attempt or observe the existing ready
snapshot/failure. The permit carries identity, slot generation, and attempt
generation; success, failure, or cancellation is accepted only from the
currently active permit. Late, foreign, stale, and already-completed permits
return structured errors and have no side effects.

Dropping an unfinished permit marks that attempt cancelled and wakes observers;
it does not choose a retry policy. Cancellation without previous content returns
the public identity to `Missing`; cancellation of a replacement restores the
previous `Ready` generation. Exact-attempt waiters observe `Cancelled` in both
cases. Waiters are standard-library `Future` capabilities tied to that exact
attempt, not to an executor. The crate registers wakers
but owns no thread, queue, timer, executor, I/O operation, or scheduling policy.

## CPU cache and budget

Ready content with no external strong logical users becomes a cache candidate;
it is not destroyed immediately. The producer reports resident bytes on every
successful commit. The store accounts only bytes retained by its current
committed cache values. Bytes retained solely by old external snapshots are
owned and accounted by those consumers, not silently charged to the store.

Collection is explicit. Given a fixed `CacheBudget`, it:

1. scans ready entries and excludes identities with external strong users or an
   active production attempt;
2. orders eligible entries by a store-local monotonic last-touch sequence, then
   by typed identity as a stable tie-break;
3. evicts until retained ready bytes are within the budget; and
4. returns a `CollectReport` containing before/after bytes and exact identities
   and generations evicted.

Retaining a strong handle or snapshot is the public pin mechanism. Producing
content and pinned/live content are never collected. Collection does not
recycle logical identity; explicit removal may recycle an empty identity only
after the strong-user and producer checks pass.

The budget is a deterministic policy boundary, not a claim that all process
memory is measured. Allocator overhead, caller-owned snapshots, loader buffers,
decoded intermediates, and GPU allocations lie outside it.

## Concurrency and atomicity

The store is safe to share across threads. A short store-owned lock serializes
identity allocation, state transitions, waker registration, byte accounting,
and collection decisions. Producers perform source work outside that lock.
Committed values and failures are shared immutably. User destructors are never
run while the store lock is held: replaced or evicted values are moved to a
retirement list and dropped after unlock.

The contract favors simple, auditable synchronization over speculative
lock-free machinery. Poisoning is converted to a structured closed/poisoned
error; it does not expose partially committed state. The crate contains no
`unsafe` code.

## Structured failures

Public operations distinguish at least:

- stale or foreign identity/handle;
- identity still has live strong users;
- identity is producing;
- no active production attempt;
- expired, foreign, or already-completed producer permit;
- production failed or was cancelled;
- resident-byte arithmetic overflow; and
- store closure or synchronization poisoning.

Caller production failures are typed values, not flattened strings. Query and
mutation errors do not trigger loading, retry, collection, or removal as a side
effect.

## Rendering 0.14 handoff

Frame preparation may obtain an immutable snapshot and construct the
rendering-owned key:

```text
(AssetId<K>, ContentGeneration, RhiDeviceGeneration)
```

Rendering resolves that key to a persistent GPU realization before graph
execution and imports the prepared resource into RenderGraph. A render pass
does not look up an asset store. Submission completion, last GPU use, upload
commit, retirement, and device-loss recreation remain Rendering/RHI concerns.
Device loss discards only the GPU generation; the logical snapshot remains
available to rebuild residency.

## Non-goals

0.13 does not define:

- filesystem, network, URL, package, or embedded-resource lookup;
- decoding, transcoding, hot reload, retry timing, priorities, or cancellation
  policy;
- an executor, worker pool, async runtime, application lifecycle, or blocking
  I/O abstraction;
- GPU objects, upload commands, device generation, residency, retirement, or
  RenderGraph resources;
- scene, mesh, image, material, shader, or pipeline formats;
- physical memory totals, a global singleton, or raw manager back-pointers.

New API is admitted only when the examples, public contract tests, and the 0.14
consumer require it.

## Release construction

The 0.13 series deliberately mirrors the evidence order demonstrated by
`slot-graph`: 0.13.0 freezes this architecture and the crate boundary; 0.13.1
adds numbered examples, public-only contract tests, and declarations; 0.13.2
implements those frozen contracts; 0.13.3 adds representative benchmarks and a
reproducible baseline/decision. A 0.13.4 exists only if independent review
finds a material contract or evidence defect.
