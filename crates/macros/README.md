# dagr-macros

The optional, **build-time-only** proc-macro authoring layer for
[dagr](https://github.com/athvin/dagr). It exports two attributes and a derive:

- **`#[task]`**, applied to an inherent `impl` block, expands to the exact
  `impl Task for Foo { … }` a task author writes by hand today — so an author
  writes only the `run` fn and the four declarations (input type, output type,
  execution class, work) are generated.
- **`#[dag]`**, applied to a `fn(&mut FlowBuilder)`, keeps the fn and emits a DAG
  factory plus its link-time registration, so one binary can host many
  auto-discovered DAGs.
- **`#[derive(StableName)]`**, on a task or payload struct, emits the one-line
  `impl StableName` the graph-emittable registrars require.

Every one of them is **purely additive and opt-in**. Hand-written `impl Task`
stays the first-class, zero-dependency escape hatch, and nothing here is required
to use dagr.

## You do not depend on this crate directly

```text
cli ──────► core, artifact, render, macros  (re-exports `#[dag]`)
core ─────► macros  (build-time, opt-in)    (re-exports `#[task]`)   ◄── here
```

`dagr-core` re-exports `#[task]` behind its default-on `macros` feature, and
`dagr-cli` re-exports `#[dag]` behind its default-on `dag` feature. Write
`use dagr_core::task;` or `use dagr_cli::dag;`, not a direct dependency on this
crate. It is published because a published `dagr-core` with its default features
on needs it on the registry — not because it is independently useful.

## It is never linked into your binary

This is a `proc-macro = true` crate: its only dependencies are the build-time
`syn` / `quote` / `proc-macro2`, and a proc macro runs **inside the compiler**.
The expansion references only existing `dagr-core` / `dagr-cli` items, so a
produced program's **runtime** dependency graph is byte-for-byte unchanged —
`dagr-core`'s zero-runtime-dependency guarantee is preserved, and
`cargo build --no-default-features` drops the edge entirely.

## Diagnostics are pinned

The accept/reject boundary of each macro is held by a `trybuild` corpus with
byte-exact `.stderr` snapshots under the workspace-pinned toolchain: a
bad-signature `#[dag]`, a mis-shaped `#[task]` impl, and a wrong-arity input list
each produce a diagnostic that is checked in and reviewed like code, so a macro
cannot silently start explaining itself worse.

## Documentation

The rationale for the macro layer is recorded in ADR 082 and ADR 092; the
component specification is
[`docs/arch.md`](https://github.com/athvin/dagr/blob/main/docs/arch.md) — C1
(task).

Licensed MIT.
