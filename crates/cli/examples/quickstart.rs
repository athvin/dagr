//! The **README quickstart**, as a compiled-and-run reference (arch.md
//! "Documentation"; system-level acceptance criterion 1; ticket T64).
//!
//! This file is the *single source of truth* for the quickstart's Rust code. The
//! `## Quickstart` section of the repository `README.md` quotes the region
//! between the `ANCHOR: quickstart` / `ANCHOR_END: quickstart` markers below
//! **verbatim**, and the test `crates/cli/tests/readme_quickstart.rs` fails the
//! build if the two ever diverge — so the walkthrough a reader copies is exactly
//! the code CI compiles and runs, never a hand-maintained paraphrase that rots.
//!
//! It takes a reader from an empty directory to a compiled, run,
//! artifact-inspected **two-node pipeline** using the one-call
//! [`RunnableFlow`](dagr_cli::run_flow::RunnableFlow) seam (ADR 081): the author
//! writes two plain [`Task`](dagr_core::task::Task)s and a flow, and *never*
//! writes a scheduler, a `NodeRunner`, or any slot wiring (arch.md C1). No async
//! experience is required — the two task bodies are ordinary code returning a
//! value; the framework runs them.
//!
//! No server, database, or scheduler is involved: the binary and its one
//! argument (a run-store directory) are sufficient (system criterion 7). Run it
//! with `cargo run --example quickstart -- ./quickstart-runs`.

// ANCHOR: quickstart
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::task::{RunContext, Task};
use dagr_core::TaskError;

// --- 1. Author two tasks. A task is a value holding its configuration, with a
// declared input type, a declared output type, and an `async fn run` body that
// returns a value or a classified error. You write business logic only — no
// scheduling, retry, or permit code (arch.md C1).

/// The source: consumes nothing (`Input = ()`) and produces a starting number.
struct Count {
    up_to: u64,
}

impl Task for Count {
    type Input = ();
    type Output = u64;

    async fn run(&mut self, _ctx: &RunContext, _input: ()) -> Result<u64, TaskError> {
        Ok(self.up_to)
    }
}

/// The sink: consumes the source's `u64` and produces its double.
struct Double;

impl Task for Double {
    type Input = u64;
    type Output = u64;

    async fn run(&mut self, _ctx: &RunContext, input: u64) -> Result<u64, TaskError> {
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
/// run store. dagr injects the sink so the run store is *your* one job (arch.md
/// "The shape of a run"); a real deployment points the base at durable storage.
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
// ANCHOR_END: quickstart
