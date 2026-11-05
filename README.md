# Fluxel Bases

`fluxel-bases` is the monorepo for Fluxel's shared, platform-neutral mechanisms
and contracts.

## Scope

This repository owns common mechanisms that can be shared without importing a
renderer, a native host, or a language SDK. Its crate boundaries are introduced
only when a demonstrated consumer needs them; this repository does not promise
that every named capability already exists.

Expected ownership includes:

- asset identity, typed handles, generations, lifecycle state, and reuse
  contracts;
- structured diagnostics: records, schemas, filtering, and routing;
- portable loader state machines;
- time and input value models;
- portable image data and codec-facing contracts; and
- small shared byte, encoding, identity, and value utilities.

`fluxel-assets` is the single owner of logical asset identity. Domain crates may
define asset payloads such as meshes, images, or materials, but reference them
through `AssetId<K>` and the associated content generation instead of creating
parallel `MeshId`, `ImageId`, or `MaterialAssetId` systems. Those payloads are
not GPU objects; a renderer may derive a device-specific realization from the
logical identity and generation.

## Non-scope

`fluxel-bases` does not own GPU residency, texture upload, render passes,
windows, surfaces, application loops, filesystem or network implementations,
or platform log sinks. It does not write to a browser console, file, logcat, or
`os_log`; hosts and language adapters provide those endpoints.

Asset identity and lifecycle belong here, while GPU-specific upload, residency,
and retirement remain rendering concerns. Logical identity is intentionally
separate from source/loader identity and from GPU resource identity, so a path,
URL, or device handle cannot become a second asset key. Diagnostics schemas and
routers belong here, while final output sinks remain host or language-adapter
concerns.

## Dependencies

This repository is a leaf ownership boundary: it must not depend on
`fluxel-rendering`, `fluxel-host`, or `fluxel-jsbridge`. Those repositories may
depend on its demonstrated contracts.

## Status and plan

The first demonstrated shared crate is:

- `fluxel-assets`: typed logical identity, immutable content generations,
  producer coordination, and deterministic CPU-cache policy.

The implemented asset contract includes public-only contract tests, numbered
examples, reproducible benchmark evidence, and the independently reviewed
failed-completion lifecycle correction. See the accepted
[architecture](documents/design-assets.md), [performance plan](documents/performance-plan.md),
and [recorded baseline](documents/performance/asset-store-baseline.md).

The `07_file_loader` example is a caller-owned file consumer of the asset
contract. It demonstrates source keys, producer/waiter sharing, bounded loading,
and explicit blocking on a thread where the caller can guarantee progress:

```console
cargo run -p fluxel-assets --example 07_file_loader -- <input> [output]
```

The example's filesystem and scheduling policy is not a public loader API or a
process-wide memory limit. In particular it does not resolve symlink aliases,
watch for changes, guarantee same-length concurrent file writes are consistent,
or identify every context where blocking a thread would deadlock.

GPU residency remains owned by `fluxel-rendering`. It consumes a logical asset
identity plus immutable content generation to select a device-specific
realization, but it owns upload, GPU lifetime, completion-safe retirement, and
device-loss recovery. A content generation is neither a GPU-residency version
nor a device generation.

## Rendering consumer evidence

Rendering's historical bounded S7/S8 consumer result consumed
Bases revision `ee640d5` for logical asset identity and content generation.
The [S7 historical closure](https://github.com/fluxel-project/fluxel-rendering/blob/main/documents/s7-deferred-closure.md)
and [S8 measured closure](https://github.com/fluxel-project/fluxel-rendering/blob/main/documents/s8-measured-closure.md)
exercise replacement generations while Rendering retains GPU upload, residency,
completion, device-loss, and retirement ownership. This is consumer evidence
only: it neither adds GPU objects to Bases nor turns the asset cache into a
GPU-memory budget.

The subsequent [Rendering maintenance review](https://github.com/fluxel-project/fluxel-rendering/blob/main/documents/s0-s8-review-follow-up.md)
selects Bases `124e825` for the Stable/MSRV identity-allocation fix described
below. Updated consumer manifests and lockfiles have separate CPU validation;
the historical native archives keep their original `ee640d5` identity.

New crates still require a reviewed, demonstrated need rather than a reserved
name alone. Ecosystem stages, ownership, and extraction gates are maintained in
the [Fluxel roadmap](https://github.com/fluxel-project/.github/blob/main/ROADMAP.md)
and [ecosystem architecture](https://github.com/fluxel-project/.github/blob/main/ECOSYSTEM_ARCHITECTURE.md).

## Development

The workspace MSRV is Rust 1.87. The standard local gates are:

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo doc --workspace --all-features --no-deps --locked
```

### Toolchain compatibility maintenance

Rust 1.99 deprecated `AtomicU64::fetch_update` in favor of `try_update`, but
the latter is unavailable on the Rust 1.87 MSRV. The store-identity allocator
therefore uses a relaxed compare-exchange loop: it returns the successfully
claimed value, retries after contention, and fails closed on `u64` exhaustion.
This keeps the existing unique-store identity and foreign-handle rejection
contract available to both toolchains.

The same compatibility pass replaced the test-only no-op `Wake` implementation
with `Waker::noop()` and retained the drop-driven cancellation waker with a
documented, narrow lint allowance because its destructor is the behavior under
test.
