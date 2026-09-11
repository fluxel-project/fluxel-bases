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

## Non-scope

`fluxel-bases` does not own GPU residency, texture upload, render passes,
windows, surfaces, application loops, filesystem or network implementations,
or platform log sinks. It does not write to a browser console, file, logcat, or
`os_log`; hosts and language adapters provide those endpoints.

Asset identity and lifecycle belong here, while GPU-specific upload, residency,
and retirement remain rendering concerns. Diagnostics schemas and routers belong
here, while final output sinks remain host or language-adapter concerns.

## Dependencies

This repository is a leaf ownership boundary: it must not depend on
`fluxel-rendering`, `fluxel-host`, or `fluxel-jsbridge`. Those repositories may
depend on its demonstrated contracts.

## Status and roadmap

The repository establishes ownership boundaries before all of their internal
crates are extracted. New crates require a reviewed, demonstrated need rather
than a reserved name alone. Ecosystem stages, ownership, and extraction gates
are maintained in the [Fluxel roadmap](https://github.com/fluxel-project/.github/blob/main/ROADMAP.md)
and [ecosystem architecture](https://github.com/fluxel-project/.github/blob/main/ECOSYSTEM_ARCHITECTURE.md).
