//! `dagr-run-a-flow-demo` — the reference driver that **runs a real `Flow` of
//! real `Task`s through the one-call `RunnableFlow` seam**, writing a real on-disk
//! `events.jsonl`.
//!
//! # Why this binary exists
//!
//! A task author writes **tasks + a flow** and never writes the
//! scheduling/permit plumbing. Every prior milestone demo hand-wrote ~150 lines of
//! type-erased `NodeRunner` impls per pipeline to drive a run. This binary is the
//! demonstration that the run-a-Flow ergonomics remove that: it registers three
//! real tasks on a `RunnableFlow` and drives the whole run with a **single**
//! `RunnableFlow::run` call — **no** hand-written `NodeRunner`, no manual slot
//! wiring, no `RunPlan` assembly. The convenience path builds the type-erased
//! runners generically (the registration-time adapter) and drives them through the
//! same real `drive` loop the hand-wired demos use, so
//! the on-disk stream is a genuine run stream.
//!
//! The chain mirrors the earlier `source → transform → sink` demo (the middle node
//! deterministically flaky with one retry), so this binary is the executable proof
//! that the seam runs a real chain — with a real retry — end to end.
//!
//! # Determinism
//!
//! A monotonic tick clock stamps every event; the flakiness is a deterministic
//! attempt counter (never timing/randomness) and the retry backoff resolves
//! immediately, so two runs leave byte-identical streams outside run identity. No
//! wall-clock sleeps.
//!
//! # Usage
//!
//! ```text
//! dagr-run-a-flow-demo <run-store-base> [<run-id>]
//! ```
//! It writes `<base>/run-a-flow-demo/<run-id>/events.jsonl` and prints the overall
//! outcome and the sink's produced value. Exit code `0` on a succeeded run, `1`
//! otherwise (the run-failure surface, reported by the driver).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome, EVENTS_FILE_NAME};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// The pipeline identity this demo runs under (feeds the run-store path).
const PIPELINE: &str = "run-a-flow-demo";
const SOURCE: &str = "source";
const TRANSFORM: &str = "transform";
const SINK: &str = "sink";
const SEED: u64 = 21;

/// A minimal append-only local-file event-stream sink: appends each complete line
/// to the run's `events.jsonl` and flushes to the OS.
struct FileSink {
    file: File,
}
impl FileSink {
    fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
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

/// A monotonic clock advanced per read — deterministic, independent of wall time.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

// === The example tasks — pure `Task`s, no plumbing (the demo chain) ==========

/// The source: a no-input task producing a seed value; succeeds on its first try.
struct Source {
    value: u64,
}
impl Task for Source {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.value)
    }
}

/// The deterministically-flaky middle: fails retryably on attempt 1, doubles on 2.
struct FlakyTransform;
impl Task for FlakyTransform {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, c: &RunContext, input: u64) -> Result<u64, TaskError> {
        if c.attempt() == 1 {
            Err(TaskError::retryable(
                "transient hiccup on the first attempt",
            ))
        } else {
            Ok(input * 2)
        }
    }
}

/// The sink: consumes `transform`'s output and adds one so the value is distinct.
struct Sink;
impl Task for Sink {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input + 1)
    }
}

fn stream_path(base: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base)
        .join(PIPELINE)
        .join(run_id)
        .join(EVENTS_FILE_NAME)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let Some(base) = args.get(1).cloned() else {
        eprintln!("usage: dagr-run-a-flow-demo <run-store-base> [<run-id>]");
        return ExitCode::from(2);
    };
    // A stable run id (operator-overridable) so the store path is predictable.
    let run_id = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "demo-run".to_string());

    // --- Build the chain through the one-call seam. No hand-written
    // `NodeRunner`, no manual slot wiring, no `RunPlan` — just real tasks + a flow.
    let mut flow = RunnableFlow::new();
    let source = flow.register_source(SOURCE, Source { value: SEED });
    let transform = flow.register_with::<FlakyTransform, _>(
        TRANSFORM,
        FlakyTransform,
        source.clone_on_read(),
        NodePolicy::new().retries(1),
    );
    let sink_handle = flow.register::<Sink, _>(SINK, Sink, transform);

    // --- Open the real on-disk sink and drive the whole flow in ONE call.
    let path = stream_path(&base, &run_id);
    let sink = match FileSink::create(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot open event stream at {}: {e}", path.display());
            return ExitCode::from(2);
        }
    };
    let cfg = RunConfig::new(base).run_id(&run_id);
    let report = match flow.run(PIPELINE, &cfg, sink, TickClock::default()) {
        Ok(report) => report,
        Err(err) => {
            eprintln!("the flow did not assemble: {err}");
            return ExitCode::from(2);
        }
    };

    let produced = report.output(sink_handle);
    println!(
        "run {} → outcome {:?}; sink produced {:?}; stream at {}",
        report.run_id(),
        report.outcome(),
        produced,
        path.display()
    );

    if report.outcome() == RunOutcome::Succeeded {
        ExitCode::from(0)
    } else {
        ExitCode::from(1)
    }
}
