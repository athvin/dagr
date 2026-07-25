//! **Layer B — the execution core** (arch.md "Layer B"; ticket T64).
//!
//! The parts that *run* the pipeline — readiness, admission, the attempt runner,
//! retries — which a pipeline developer should be able to ignore entirely. This
//! example proves that: it registers three real [`Task`]s (the middle one flaky
//! with one retry) and drives the whole graph in **one call** through
//! [`RunnableFlow`](dagr_cli::run_flow::RunnableFlow), writing a real event
//! stream. The author writes tasks + a flow and **never** a scheduler, a
//! `NodeRunner`, or any retry/permit code (arch.md C1). Run it with
//! `cargo run --example layer_b_execution -- ./layer-b-runs`.

use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A source producing a seed value.
struct Seed {
    value: u64,
}
impl Task for Seed {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.value)
    }
}

/// A deterministically-flaky transform: fails retryably on attempt one (the
/// framework's retry machinery, which the author never writes, recovers it),
/// doubles on attempt two.
struct FlakyDouble;
impl Task for FlakyDouble {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, ctx: &RunContext, input: u64) -> Result<u64, TaskError> {
        if ctx.attempt() == 1 {
            Err(TaskError::retryable(
                "transient hiccup on the first attempt",
            ))
        } else {
            Ok(input * 2)
        }
    }
}

/// A sink adding one, so the final value is distinct from the transform's.
struct AddOne;
impl Task for AddOne {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input + 1)
    }
}

fn main() -> std::process::ExitCode {
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./layer-b-runs".to_string());

    // Register three real tasks. The flaky middle node gets one retry via its
    // policy; because a retrying node's owned input cannot be re-formed, the edge
    // opts into clone-on-read (each attempt gets a fresh input) — assembly would
    // reject an owned edge into a retrying node otherwise (arch.md C1).
    let mut flow = RunnableFlow::new();
    let seed = flow.register_source("seed", Seed { value: 21 });
    let doubled = flow.register_with::<FlakyDouble, _>(
        "double",
        FlakyDouble,
        seed.clone_on_read(),
        NodePolicy::new().retries(1),
    );
    let result = flow.register::<AddOne, _>("add-one", AddOne, doubled);

    let sink = MemorySink::default();
    let config = RunConfig::new(base).run_id("layer-b-run");
    let report = flow
        .run("layer-b", &config, sink, TickClock::default())
        .expect("the flow assembles and runs");

    // The author reads a node's produced value back by its handle — no lookup.
    println!("run {} -> {:?}", report.run_id(), report.outcome());
    for node in ["seed", "double", "add-one"] {
        println!("  {node} -> {:?}", report.terminal_state(node));
    }
    println!("final value: {:?}", report.output(result)); // 21*2 + 1 = 43

    if report.outcome() == RunOutcome::Succeeded {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}

/// An in-memory sink (this example keeps the stream in memory; the quickstart
/// shows the on-disk file sink). The run store is the operator's one job.
#[derive(Default)]
struct MemorySink {
    lines: Vec<u8>,
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A deterministic monotonic clock advanced one tick per read.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}
