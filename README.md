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

GPU residency remains owned by `fluxel-rendering`. It consumes a logical asset
identity plus immutable content generation to select a device-specific
realization, but it owns upload, GPU lifetime, completion-safe retirement, and
device-loss recovery. A content generation is neither a GPU-residency version
nor a device generation.

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
