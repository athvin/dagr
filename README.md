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

From an empty directory to a compiled, run, artifact-inspected **two-node
pipeline**, for a developer comfortable with Rust and cargo — **no async
experience required**. You write two plain tasks and a flow; the framework runs
them. There is no server, no database, and no scheduler: the binary and a
run-store directory are all you need.

> This walkthrough is CI-verified. The Rust block below is byte-identical to the
> compiled, run example at [`crates/cli/examples/quickstart.rs`](crates/cli/examples/quickstart.rs),
> enforced by `crates/cli/tests/readme_quickstart.rs` — so what you copy here is
> exactly what the build compiles and runs, never a paraphrase that rots.

**1. Create a project and add dagr as a dependency.** In an empty directory:

```console
$ cargo new quickstart && cd quickstart
```

Then declare the dependency in `Cargo.toml` (dagr's `cli` crate carries the
one-call run seam and the event-stream types the quickstart reads):

```toml
[dependencies]
dagr-cli = { git = "https://github.com/athvin/dagr" }
dagr-core = { git = "https://github.com/athvin/dagr" }
dagr-artifact = { git = "https://github.com/athvin/dagr" }
```

**2. Write the pipeline.** Put this in `src/main.rs`. It authors two tasks
(`Count`, which consumes nothing and produces a number; `Double`, which consumes
that number and produces its double), wires them into a flow, runs the whole
graph in one call, and inspects the result. You write business logic only — never
scheduling, retry, or permit code.

```rust
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::task;

// --- 1. Author two tasks. A task is a value holding its configuration, plus one
// `async fn run` body — that is the whole authoring surface. `#[task]` reads the
// `run` signature and generates the four things you declare (the input type, the
// output type, the execution class, the work), so you write business logic only —
// no scheduling, retry, permit, or trait-impl scaffolding. (Prefer to write the
// `impl Task` by hand? It stays a first-class fallback — see the cookbook.)

/// The source: consumes nothing (an `()` input) and produces a starting number.
struct Count {
    up_to: u64,
}

#[task]
impl Count {
    async fn run(&mut self, _input: ()) -> Result<u64, TaskError> {
        Ok(self.up_to)
    }
}

/// The sink: consumes the source's `u64` and produces its double.
struct Double;

#[task]
impl Double {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

fn main() -> ExitCode {
    // The one thing this binary needs from you: a run-store directory. Everything
    // the run leaves behind — its event stream — lives under it. No network, no
    // database, no scheduler.
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./quickstart-runs".to_string());

    // --- 2. Wire the flow. Register the two tasks; `register` binds the sink to
    // the source's typed handle. A wrong-typed binding would be a *compile* error,
    // not a runtime surprise. This is the whole graph — two nodes, one edge.
    let mut flow = RunnableFlow::new();
    let counted = flow.register_source("count", Count { up_to: 21 });
    let doubled = flow.register::<Double, _>("double", Double, counted);

    // --- 3. Run it in one call. `run` assembles the pipeline, builds every node's
    // runner for you, and drives the whole graph to completion — writing a real
    // event stream to `<base>/quickstart/<run-id>/events.jsonl`.
    let sink = match FileSink::create(&base, "quickstart", "quickstart-run") {
        Ok(sink) => sink,
        Err(err) => {
            eprintln!("could not open the run store under {base}: {err}");
            return ExitCode::from(2);
        }
    };
    let config = RunConfig::new(base.clone()).run_id("quickstart-run");
    let report = match flow.run("quickstart", &config, sink, TickClock::default()) {
        Ok(report) => report,
        Err(err) => {
            eprintln!("the flow did not assemble: {err}");
            return ExitCode::from(2);
        }
    };

    // --- 4. Inspect the result. Read a node's produced value back by its handle,
    // and fold the event stream to confirm the two-node shape and terminal states.
    let value = report.output(doubled);
    println!("run {} finished: {:?}", report.run_id(), report.outcome());
    println!("count -> {:?}", report.terminal_state("count"));
    println!(
        "double -> {:?} (value {value:?})",
        report.terminal_state("double")
    );

    // The event stream is the crash-proof record everything else derives from.
    let stream_path = Path::new(&base)
        .join("quickstart")
        .join("quickstart-run")
        .join("events.jsonl");
    if let Ok(bytes) = std::fs::read(&stream_path) {
        let stream = read_records(&bytes).expect("the event stream parses");
        let terminals = stream
            .records
            .iter()
            .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("node-terminal"))
            .count();
        println!("the stream records {terminals} node-terminal events (one per node)");
    }

    if report.outcome() == RunOutcome::Succeeded {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// A minimal append-only event sink writing each line to `events.jsonl` under the
/// run store. dagr injects the sink so the run store is *your* one job; a real
/// deployment points the base at durable storage.
struct FileSink {
    file: File,
}

impl FileSink {
    fn create(base: &str, pipeline: &str, run_id: &str) -> io::Result<Self> {
        let dir = Path::new(base).join(pipeline).join(run_id);
        create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("events.jsonl"))?;
        Ok(Self { file })
    }
}

impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.file.write_all(line)?;
        self.file.flush()
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A monotonic clock advanced one tick per read — deterministic, wall-clock-free.
/// Durations in the artifact are computed from these monotonic offsets.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}
```

**3. Run it.** Point the binary at a run-store directory. Everything the run
leaves behind — its event stream — lives under it:

```console
$ cargo run -- ./quickstart-runs
run quickstart-run finished: Succeeded
count -> Some(Succeeded)
double -> Some(Succeeded) (value Some(42))
the stream records 2 node-terminal events (one per node)
```

Both nodes reached `succeeded`, `double` produced `42` (twice `Count`'s `21`),
and the event stream at `./quickstart-runs/quickstart/quickstart-run/events.jsonl`
records the two-node shape. That stream is the crash-proof record everything else
derives from — you can fold it into a run artifact and render a diagram from it
without ever re-running the pipeline.

> **You are running the compiled example here.** This repository ships the same
> code as `cargo run --example quickstart -- ./quickstart-runs`, which is what CI
> executes to prove the quickstart works verbatim.

For the patterns the design forces on authors — fan-out inside one node, fan-in,
branch-in-task, incremental cursors, durable stage boundaries, the non-`Send`
capture error, and same-typed resources — see the
[cookbook](docs/cookbook.md).

To host **many named flows** in one binary and select one per invocation
(`dagr run etl` versus `dagr run analytics`, `dagr graph etl`, `dagr list`), see
the [flow-registry guide](docs/flow-registry.md) and the compiled example
[`crates/cli/examples/multi_flow.rs`](crates/cli/examples/multi_flow.rs).

To declare those DAGs with less boilerplate, use the **`#[dag]` attribute** — the
declarative-DAG sugar (ADR 092): put `#[dag]` on a `fn(f: &mut FlowBuilder)`, and
`dagr_cli::run` **auto-discovers** every DAG so the whole `main` is one line:

```rust
use dagr_cli::prelude::*; // or `use dagr_cli as dagr;` for the `dagr::run` spelling

#[dag] // name defaults to the fn name; #[dag(name = "nightly")] overrides
fn alpha(f: &mut FlowBuilder) {
    let rows = f.source("extract", Extract { rows: 3 });
    let _report = f.node("load", Load, rows); // wrong wiring => COMPILE error
}

fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
```

The app crate depends on `inventory = "0.3"`, and the `#[dag]`s live in the binary
crate (`inventory` collects reliably only in the leaf binary; cross-crate DAG
libraries are out of scope). See the [cookbook](docs/cookbook.md#declaring-dags-with-dag-and-running-them-with-one-line)
and the compiled example
[`crates/cli/examples/many_dags.rs`](crates/cli/examples/many_dags.rs). The
hand-wired `FlowRegistry` above stays the explicit fallback.

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
