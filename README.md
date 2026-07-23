# dagr

A Rust framework for pipelines that compile: you write units of work, declare
how they connect, and build one binary that *is* the pipeline — no server, no
scheduler, no database, no config file describing the graph, no parsing step.

> **Status:** early scaffolding. This repository currently contains repository
> hygiene and specifications only; no crate exists yet (the workspace is a later
> milestone). Nothing below claims a feature the code already has.

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
[`rust-toolchain.toml`](rust-toolchain.toml) and must match this line with no
drift. Raising the MSRV is a minor version bump, called out in release notes.

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
