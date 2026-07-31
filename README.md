# dagr

A Rust framework for pipelines that compile: you write units of work, declare
how they connect, and build one binary that *is* your pipeline — or every
pipeline you declared, one selected per invocation. Nothing it needs is a server,
a scheduler, or a database; there is no config file describing the graph and no
parsing step.

## What it is not

Permanently, dagr is **not** a scheduler, a *distributed* execution system, a
*coordinating* metadata store, a web interface, a domain-specific language, or a
backfill orchestrator. **The graph's shape never changes at runtime** — a task that
discovers N files does not become N nodes; it iterates internally with bounded,
declared concurrency. Every one of those is a reasonable thing to want, and none
of them belong here.

Two of those words are read narrowly, by recorded decision, and each carve-out
moves nothing else on the list:

- **"Metadata store"** means a store the engine *depends on to coordinate*. A
  **local, embedded, opt-in, non-coordinating run index** derived from the event
  stream is permitted — that is the [run index](#the-run-index-metastore) below
  (ADR 097).
- **"Distributed execution system"** means an engine that distributes *the graph
  and its control* — cooperating orchestrators, work-stealing, cross-run queues, a
  control plane that outlives a run. A **single orchestrator process placing
  individual node attempts on remote compute it owns for one run** is permitted
  (ADR 115): one process still owns the graph and the event stream and still exits
  when the run ends. Remote execution changes *where* a node runs, never *how many*
  nodes there are and never *when* a pipeline runs.

See [`docs/arch.md`](docs/arch.md) for the full component specification.

## Quickstart

From an empty directory to a compiled, run **two-node pipeline** — for a
developer comfortable with Rust and cargo, **no async experience required**. You
write two tasks and a DAG; the framework runs them. No server, database, or
scheduler has to be running: the binary and a run-store directory are all you
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
// the flows. Nothing here needs a server, a database, or a scheduler running — the
// binary and a run-store directory are all you need.
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
| `metastore` (`dagr-metastore`) | The local, embedded, **opt-in** run index (M7): a queryable libSQL/SQLite projection of the event stream. Reached from `cli` only behind a default-off `metastore` feature. | `artifact` + `libsql` + `tokio` (no `core`) |
| `blob` (`dagr-blob`) | The **opt-in** blob port and its local filesystem backend (M10): content-addressed, atomically-written opaque bytes. Reached from `cli` only behind a default-off `blob` feature. | *(nothing at all — no `core`, no third-party crate)* |
| `cli` (`dagr-cli`) | The pipeline binary and its command-line contract (`run` / `graph` / `validate` / `list` / …). | `core`, `artifact`, `render` (+ `metastore` and `blob`, opt-in) |

The allowed dependency edges are `cli → {core, artifact, render}`,
`render → artifact`, `metastore → artifact`, and `core → macros` (build-time
only); `cli → metastore` and `cli → blob` exist only behind their default-off
features. **None of `render`, `metastore`, or `blob` has a dependency edge onto
`core`**, so a renderer (and the run index, and the blob store) is structurally
incapable of reaching the live pipeline — it consumes artifacts, or opaque bytes,
and needs no access to the binary that produced them
([`docs/arch.md`](docs/arch.md) C24 · Renderers). The standalone `dagr-render`
binary builds without `core` or `cli`, which is that guarantee made concrete; so
is `dagr-blob` compiling with an empty dependency table.

## MSRV

**MSRV: Rust 1.97.1.** The supported minimum is pinned in
[`rust-toolchain.toml`](rust-toolchain.toml) and in the workspace manifest
([`Cargo.toml`](Cargo.toml), `[workspace.package].rust-version`), and must match
this line with no drift. Raising the MSRV is a minor version bump, called out in
release notes.

dagr is an **edition 2024** workspace, with the MSRV-aware dependency resolver
(`resolver = "3"`) so a transitive upgrade cannot silently raise that minimum.
The pinned channel *is* the MSRV, deliberately: the pin is what makes dagr's
compile-fail diagnostics byte-reproducible across machines, so the declared
minimum and the pinned toolchain cannot be different numbers. Raising the pin
therefore raises the minimum — a minor version bump, in the release notes, with
all six sites that name it ([`Cargo.toml`](Cargo.toml),
[`rust-toolchain.toml`](rust-toolchain.toml), [`rustfmt.toml`](rustfmt.toml),
this line, [`scripts/check-stability-and-criteria.sh`](scripts/check-stability-and-criteria.sh),
and `crates/core/tests/ui.rs`) moving together.
[`scripts/check-edition-and-msrv-pins.sh`](scripts/check-edition-and-msrv-pins.sh)
fails the build if any of them disagrees.

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

## The run index (metastore)

Hosting many DAGs in one binary gives you **one place to query their state across
runs**. That place is the **run index** (the "metastore", ADR 097): an opt-in,
embedded, non-coordinating projection of the event streams runs already write, into a
queryable `libSQL` file. It is **off by default** (a default-off `metastore` cargo
feature — a build that never asks for it never pulls `libsql`), and it coordinates
nothing: the event stream stays the source of truth, and the index is a *guaranteed
projection* of it.

**Native access only, same host.** The index file is **byte-compatible with stock
`SQLite`**, so you query it with plain `sqlite3 metastore.db "SELECT …"` (or `turso db
shell` / the `libsql` CLI) — **zero new tools**. There is **no** Postgres wire
protocol, **no** server, and **no** remote access: the file is read embedded, on the
same host's local filesystem as the runs it indexes.

Two write paths put rows into the index; both are the same projection of the same
stream, so they produce the same rows for a given run:

- **Guaranteed live.** Turn the tee on and every `run` *also* writes its rows as it
  executes. Use the reserved `--dagr.metastore` flag, or set `DAGR_METASTORE=1` in the
  environment once (`flag > env > default`); point it with `--dagr.metastore-store
  <path>` (default `<store>/metastore.db`). A metastore write is as durable as an
  event-stream write — a failed index write surfaces as the sink-failure exit code,
  never silently swallowed (it is **guaranteed**, not best-effort).

  ```sh
  # Build with the feature, then run with the live tee on.
  cargo run --features metastore --example many_dags -- \
      run alpha --store ./runs --dagr.metastore
  ```

- **Reconcile (backfill).** For runs that finished *before* the tee was on (or ran on
  another binary), fold their finished streams into the index after the fact:

  ```sh
  dagr metastore init [--store <path>]                       # create/open + migrate (idempotent)
  dagr metastore sync [--store <path>] [--follow] <run-store-base>
  ```

  `sync` walks the run store (`<base>/<pipeline>/<run-id>/events.jsonl`), folds each
  finished stream, and UPSERTs it idempotently; a run with no readable stream is
  reported and skipped, never aborting the batch. `--follow` re-runs the pass on an
  interval, consolidating newly-finalized runs incrementally until interrupted.

Query it with the cookbook's [worked examples](docs/cookbook.md#querying-run-state-across-dags)
(runs per DAG by state, slowest nodes, latest terminal state per node) — plain
`sqlite3` against the file. Cross-run **data lineage** is projected too: the
`output_produced` / `input_consumed` / `asset` tables answer "which runs produced or
consumed dataset X", referencing a dataset **by its `uri` value** with no hard foreign
key (a lineage row survives GC of the referent). This is a local, non-coordinating
provenance index — dagr is **not** an asset scheduler (no data-triggered runs, no
asset queues/watchers/partitions).

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
