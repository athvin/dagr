# dagr

A Rust framework for pipelines that compile: you write units of work, declare
how they connect, and build one binary that *is* the pipeline — no server, no
scheduler, no database, no config file describing the graph, no parsing step.

> **Status:** early scaffolding. This repository contains repository hygiene,
> specifications, and an empty-but-compiling Cargo workspace skeleton (the four
> member crates below have placeholder targets only). Nothing below claims a
> feature the code already has.

## What it is not

Permanently, dagr is **not** a scheduler, a distributed execution system, a
metadata store, a web interface, a domain-specific language, or a backfill
orchestrator. **The graph's shape never changes at runtime** — a task that
discovers N files does not become N nodes; it iterates internally with bounded,
declared concurrency. Every one of those is a reasonable thing to want, and none
of them belong here. See [`docs/arch.md`](docs/arch.md) for the full component
specification.

## MSRV

**MSRV: Rust 1.95.0.** The supported minimum is pinned in
[`rust-toolchain.toml`](rust-toolchain.toml) and in the workspace manifest
([`Cargo.toml`](Cargo.toml), `[workspace.package].rust-version`), and must match
this line with no drift. Raising the MSRV is a minor version bump, called out in
release notes.

## Workspace layout

dagr is a multi-crate Cargo workspace. Member crates live under `crates/`; the
topology and rationale are recorded in the ADR embedded in
[ticket T1](docs/implementation/003-T1-crate-layout-and-workspace-skeleton.md).

| Crate | Role | Depends on |
|---|---|---|
| `core` (`dagr-core`) | Authoring surface and execution core — the code that *is* a running pipeline. Kept to a minimal, review-gated dependency set. | *(nothing)* |
| `artifact` (`dagr-artifact`) | The serializable records a run leaves behind (graph artifact, run artifact, event records) — the boundary a renderer consumes. | *(nothing)* |
| `render` (`dagr-render`) | Reads an artifact and emits diagram source (DOT / Mermaid). Library plus a standalone renderer binary. | `artifact` **only** |
| `cli` (`dagr-cli`) | The pipeline binary and its command-line contract. | `core`, `artifact`, `render` |

The only allowed dependency edges are `cli → {core, artifact, render}` and
`render → artifact`. **`render` has no dependency edge onto `core`**, so a
renderer is structurally incapable of reaching the live pipeline — it consumes
artifacts only and needs no access to the binary that produced them
([`docs/arch.md`](docs/arch.md) C24 · Renderers). The standalone `dagr-render`
binary builds without `core` or `cli`, which is that guarantee made concrete.

## Platform support

- **Tier 1 — Linux containers.** Everything works; the full test suite runs in
  CI here.
- **Dev-supported — macOS.** Compiles and runs; documented divergences only
  (no cgroups; different fsync semantics). A CI job runs the core suite.
- **Windows — unsupported in v1.** The signal and process models differ enough
  that pretending otherwise would mean untested promises. Revisit on demand.

## Quickstart

<!-- QUICKSTART PLACEHOLDER -->
*Not written yet.* A later documentation ticket will add a verbatim,
CI-verified walkthrough that goes from an empty directory to a compiled, run,
and artifact-inspected two-node pipeline. Until then there is nothing runnable
here to quote.

## When not to use this

A three-node script that runs one thing after another does not need a framework.
Reach for dagr when work must overlap under a memory ceiling, when retries
interact with ordering, when a run needs explaining after the fact, or when a
long pipeline died partway and had to start over. Below that, plain tokio is the
honest recommendation.

## Contributing

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a change. It is the
process contract every ticket ships under: one branch and one pull request per
implementation ticket (branch name copied verbatim from the ticket header),
tests written first as a hard rule, and a fixed merge gate of CI checks
(`cargo fmt --check`, `cargo clippy` with warnings denied, the test suite, the
rustdoc lint, and `cargo audit` / `cargo deny` where configured). Open PRs with
the [pull request template](.github/pull_request_template.md); review ownership
is assigned in [`.github/CODEOWNERS`](.github/CODEOWNERS). Tickets live under
[`docs/implementation/`](docs/implementation/README.md).

## License

Licensed under the [MIT License](LICENSE) (`SPDX-License-Identifier: MIT`).
