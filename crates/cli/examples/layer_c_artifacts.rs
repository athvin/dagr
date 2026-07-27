//! **Layer C — artifacts and observability.**
//!
//! The parts that make a finished run *explicable*. The event stream is the
//! crash-proof record everything else derives from; this example runs a small
//! flow through [`RunnableFlow`](dagr_cli::run_flow::RunnableFlow), captures the
//! stream in memory, then reads it back with the public
//! [`read_records`](dagr_artifact::event_stream::read_records) reader — with **no
//! access to the running pipeline** — and answers "what happened?" from the
//! records alone. Run it with `cargo run --example layer_c_artifacts`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

struct Seed;
impl Task for Seed {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(10)
    }
}
struct FlakyTriple;
impl Task for FlakyTriple {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, ctx: &RunContext, input: u64) -> Result<u64, TaskError> {
        if ctx.attempt() == 1 {
            Err(TaskError::retryable("first attempt hiccup"))
        } else {
            Ok(input * 3)
        }
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let seed = flow.register_source("seed", Seed);
    let _tripled = flow.register_with::<FlakyTriple, _>(
        "triple",
        FlakyTriple,
        seed.clone_on_read(),
        NodePolicy::new().retries(1),
    );

    let sink = MemorySink::default();
    let report = flow
        .run(
            "layer-c",
            &RunConfig::new("./layer-c-runs").run_id("layer-c-run"),
            sink.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");
    assert_eq!(report.outcome(), RunOutcome::Succeeded);

    // --- Observability from the artifact alone. Parse the captured stream and
    // answer questions about the run without touching the (finished) pipeline.
    let bytes = sink.bytes();
    let stream = read_records(&bytes).expect("the event stream parses");

    // Every state transition is an event. Count attempts per node — the retry on
    // `triple` shows up as two `attempt-started` records, which is exactly the
    // capacity-planning signal a per-attempt record preserves.
    let attempts_of = |node: &str| -> usize {
        stream
            .records
            .iter()
            .filter(|r| {
                r.get("kind").and_then(|k| k.as_str()) == Some("attempt-started")
                    && r.get("node").and_then(|n| n.as_str()) == Some(node)
            })
            .count()
    };
    println!("records in stream: {}", stream.records.len());
    println!("seed attempts:   {}", attempts_of("seed"));
    println!(
        "triple attempts: {} (one retry, so two)",
        attempts_of("triple")
    );

    // The stream is complete: it opens with `run-started` and closes with
    // `run-finished`, so even a truncated stream identifies its run.
    let first = stream
        .records
        .first()
        .and_then(|r| r.get("kind"))
        .and_then(|k| k.as_str());
    let last = stream
        .records
        .last()
        .and_then(|r| r.get("kind"))
        .and_then(|k| k.as_str());
    println!("bookends: {first:?} .. {last:?}");
    assert_eq!(first, Some("run-started"));
    assert_eq!(last, Some("run-finished"));
    assert_eq!(
        attempts_of("triple"),
        2,
        "the retry is visible in the stream"
    );
}

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().clone()
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}
