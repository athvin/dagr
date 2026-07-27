# dagr

A Rust framework for pipelines that compile: you write units of work, declare
how they connect, and build one binary that *is* the pipeline — no server, no
scheduler, no database, no config file describing the graph, no parsing step.

## What it is not

Permanently, dagr is **not** a scheduler, a distributed execution system, a
metadata store, a web interface, a domain-specific language, or a backfill
orchestrator. **The graph's shape never changes at runtime** — a task that
discovers N files does not become N nodes; it iterates internally with bounded,
declared concurrency. Every one of those is a reasonable thing to want, and none
of them belong here. See [`docs/arch.md`](docs/arch.md) for the full component
specification.

## Quickstart

From an empty directory to a compiled, run **two-node pipeline** — for a
developer comfortable with Rust and cargo, **no async experience required**. You
write two tasks and a DAG; the framework runs them. There is no server, no
database, and no scheduler: the binary and a run-store directory are all you
need.

> This walkthrough is CI-verified. The Rust block below is byte-identical to the
> compiled, run example at [`crates/cli/examples/quickstart.rs`](crates/cli/examples/quickstart.rs),
> enforced by `crates/cli/tests/readme_quickstart.rs` — so what you copy here is
> exactly what the build compiles and runs, never a paraphrase that rots.

**1. Create a project and add dagr as a dependency.** In an empty directory:

```console
$ cargo new quickstart && cd quickstart
```

Then declare the dependencies in `Cargo.toml`. `dagr-cli` carries the `#[dag]`
attribute and the one-line `run` entrypoint; `dagr-core` carries `#[task]` /
`#[derive(StableName)]`; the `#[dag]` expansion registers your DAG through
`inventory`:

```toml
[dependencies]
dagr-cli = { git = "https://github.com/athvin/dagr" }
dagr-core = { git = "https://github.com/athvin/dagr" }
inventory = "0.3"
```

**2. Write the pipeline.** Put this in `src/main.rs`. It authors two tasks
(`Count`, which consumes nothing and produces a number; `Double`, which consumes
that number and produces its double), groups them into a `#[dag]`, and delegates
`main` to `dagr_cli::run`. You write business logic only — never scheduling,
retry, sink, clock, or permit code.

```rust
use dagr_cli::prelude::*;

// The one payload the two tasks pass along. `#[derive(StableName)]` gives it the
// author-declared name the graph artifact records — one line, no trait body.
#[derive(Clone, StableName)]
struct Number(u64);

// --- Author two tasks. A task is a value holding its configuration plus one
// `async fn run` body — that is the whole authoring surface. `#[task]` reads the
// `run` signature and generates the four things you declare (input type, output type,
// execution class, and the work), so you write business logic only — no scheduling,
// retry, or permit code. `#[derive(StableName)]` names the task struct so the DAG can
// graph it.

/// The source: consumes nothing (an `()` input) and produces a starting number.
#[derive(StableName)]
struct Count {
    up_to: u64,
}

#[task]
impl Count {
    async fn run(&mut self, _input: ()) -> Result<Number, TaskError> {
        Ok(Number(self.up_to))
    }
}

/// The sink: consumes the source's number and produces its double.
#[derive(StableName)]
struct Double;

#[task]
impl Double {
    async fn run(&mut self, input: Number) -> Result<Number, TaskError> {
        Ok(Number(input.0 * 2))
    }
}

// --- Declare the DAG. `#[dag]` groups the tasks into a named flow. `f.source` is a
// root (no upstream); `f.task(..).depends_on(..)` makes a node downstream of another —
// this reads "double depends on count", and the handle threads the edge. A wrong-typed
// binding is a *compile* error, not a runtime surprise. This is the whole graph — two
// nodes, one edge — and the flow's name defaults to the fn name (`quickstart`).
#[dag]
fn quickstart(f: &mut FlowBuilder) {
    let count = f.source("count", Count { up_to: 21 });
    let _double = f.task("double", Double).depends_on(count);
}

// --- The whole binary is one line. `dagr_cli::run` discovers every `#[dag]`, builds
// the run store, event sink, clock, and run id for you, and dispatches the verbs every
// pipeline binary shares: `run` drives the flow, `graph` emits the DAG, `list` names
// the flows. No server, no database, no scheduler — the binary and a run-store
// directory are all you need.
fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
```

**3. Run it.** `dagr_cli::run` gives your binary the standard verbs. `list` names
the DAGs, `graph` emits one as an artifact, and `run` drives it — writing a real
event stream under the run-store directory:

```console
$ cargo run -- list
quickstart

$ cargo run -- graph quickstart      # the DAG's graph artifact, as JSON on stdout

$ cargo run -- run --store ./quickstart-runs
```

`run` drives both nodes to `succeeded`, `double` produces `42` (twice `Count`'s
`21`), and writes the run's event stream to
`./quickstart-runs/quickstart/<run-id>/events.jsonl`. That stream is the
crash-proof record everything else derives from — you can fold it into a run
artifact and render a diagram from it without ever re-running the pipeline.

> **You are running the compiled example here.** This repository ships the same
> code as `cargo run --example quickstart -- run --store ./quickstart-runs`, which
> is what CI executes to prove the quickstart works verbatim.

Prefer to build and drive a flow **programmatically** (embedding it, inspecting a
node's value in-process) instead of through the CLI verbs? Register tasks on a
`RunnableFlow` and call `flow.run_to_store("name", base)` — one call, no
hand-written sink or clock. See the [cookbook](docs/cookbook.md).

For the patterns the design forces on authors — fan-out inside one node, fan-in,
branch-in-task, incremental cursors, durable stage boundaries, the non-`Send`
capture error, and same-typed resources — see the
[cookbook](docs/cookbook.md).

## Workspace layout

dagr is a multi-crate Cargo workspace. Member crates live under `crates/`; the
topology and rationale are recorded in the ADR embedded in
[ticket T1](docs/implementation/003-T1-crate-layout-and-workspace-skeleton.md).

| Crate | Role | Depends on |
|---|---|---|
| `core` (`dagr-core`) | Authoring surface and execution core — the code that *is* a running pipeline. Kept to a minimal, review-gated dependency set. | `macros` (build-time, opt-in) |
| `macros` (`dagr-macros`) | Build-time proc-macro crate: `#[task]`, `#[dag]`, and `#[derive(StableName)]`. Runs inside the compiler and is never linked into the shipped binary, so it adds no runtime dependency. | *(nothing)* |
| `artifact` (`dagr-artifact`) | The serializable records a run leaves behind (graph artifact, run artifact, event records) — the boundary a renderer consumes. | *(nothing)* |
| `render` (`dagr-render`) | Reads an artifact and emits diagram source (DOT / Mermaid). Library plus a standalone renderer binary. | `artifact` **only** |
| `cli` (`dagr-cli`) | The pipeline binary and its command-line contract (`run` / `graph` / `validate` / `list` / …). | `core`, `artifact`, `render` |

The allowed dependency edges are `cli → {core, artifact, render}`,
`render → artifact`, and `core → macros` (build-time only). **`render` has no
dependency edge onto `core`**, so a renderer is structurally incapable of
reaching the live pipeline — it consumes artifacts only and needs no access to
the binary that produced them ([`docs/arch.md`](docs/arch.md) C24 · Renderers).
The standalone `dagr-render` binary builds without `core` or `cli`, which is that
guarantee made concrete.

## MSRV

**MSRV: Rust 1.95.0.** The supported minimum is pinned in
[`rust-toolchain.toml`](rust-toolchain.toml) and in the workspace manifest
([`Cargo.toml`](Cargo.toml), `[workspace.package].rust-version`), and must match
this line with no drift. Raising the MSRV is a minor version bump, called out in
release notes.

## Platform support

- **Tier 1 — Linux containers.** Everything works; the full test suite runs in
  CI here.
- **Dev-supported — macOS.** Compiles and runs; documented divergences only
  (no cgroups; different fsync semantics). A CI job runs the core suite.
- **Windows — unsupported in v1.** The signal and process models differ enough
  that pretending otherwise would mean untested promises. Revisit on demand.

## Many DAGs in one binary

The quickstart hosts one DAG. To host **many** named DAGs in one binary and
select one per invocation (`dagr run etl` versus `dagr run analytics`,
`dagr graph etl`, `dagr list`), write another `#[dag]` fn — `dagr_cli::run`
auto-discovers every DAG in the binary, so adding one needs no registry edit:

```rust,ignore
#[dag] // name defaults to the fn name; #[dag(name = "nightly")] overrides
fn analytics(f: &mut FlowBuilder) {
    let rows = f.source("extract", Extract { rows: 3 });
    let _report = f.task("load", Load).depends_on(rows); // wrong wiring => COMPILE error
}
```

A caller who prefers the short `dagr::run` spelling writes `use dagr_cli as dagr;`
(there is no facade crate — ADR 092). See the compiled example
[`crates/cli/examples/many_dags.rs`](crates/cli/examples/many_dags.rs) and the
cookbook's [declarative-DAGs section](docs/cookbook.md#declaring-dags-with-dag-and-running-them-with-one-line).
Prefer to wire the flows by hand? The explicit `FlowRegistry` stays a first-class
fallback — see the [flow-registry guide](docs/flow-registry.md) and
[`crates/cli/examples/multi_flow.rs`](crates/cli/examples/multi_flow.rs).

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
