//! `RunnableFlow::run_to_store` — the one-call golden-path run seam.
//!
//! The fully-explicit `run(pipeline, &RunConfig, sink, clock)` requires the author to
//! hand-write a `FileSink` and a clock. `run_to_store` needs neither: it builds the
//! default local-file sink, the wall-clock `SystemClock`, a fresh run id, and a
//! default `RunConfig`, drives the flow, and writes a real event stream under the run
//! store. This suite pins that path — a real stream lands, and the two distinct
//! failure modes (`Store` for an unopenable store, `Assembly` for a flow that does
//! not assemble) surface as distinct `RunToStoreError` variants so a caller can map
//! each to its own exit code.
//!
//! Determinism/portability: each test uses a PRIVATE temp base under the OS temp dir
//! (disjoint per process + nanos), and no wall-clock sleeps. macOS is in CI.

use dagr_artifact::event_stream::{MonotonicClock, RunOutcome, read_records};
use dagr_cli::run_flow::{RunToStoreError, RunnableFlow};
use dagr_cli::run_store::{SystemClock, TickClock};
use dagr_core::TaskError;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;

/// A source producing a fixed number.
struct Count {
    up_to: u64,
}
impl Task for Count {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.up_to)
    }
}

/// A sink doubling its input.
struct Double;
impl Task for Double {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

/// The count of records of a given `kind` in a parsed stream.
fn count_kind(bytes: &[u8], kind: &str) -> usize {
    let stream = read_records(bytes).expect("the event stream parses");
    stream
        .records
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some(kind))
        .count()
}

#[test]
fn run_to_store_writes_a_real_event_stream_with_no_hand_written_plumbing() {
    let base = TempBase::new("stream");
    let base_str = base.as_str().to_owned();

    let mut flow = RunnableFlow::new();
    let counted = flow.register_source("count", Count { up_to: 21 });
    let doubled = flow.register::<Double, _>("double", Double, counted);

    // The whole run: no sink, no clock, no RunConfig — one call.
    let report = flow
        .run_to_store("quickstart", &base_str)
        .expect("the flow assembles and the store opens");

    assert_eq!(
        report.outcome(),
        RunOutcome::Succeeded,
        "both nodes succeed"
    );
    assert_eq!(report.output(doubled), Some(42), "double(21) == 42");

    // A real, parseable stream landed at the conventional store path, keyed by the
    // framework-minted run id (never a caller-fixed segment).
    let stream = base
        .path()
        .join("quickstart")
        .join(report.run_id())
        .join("events.jsonl");
    let bytes = std::fs::read(&stream)
        .unwrap_or_else(|e| panic!("the run store wrote a stream at {}: {e}", stream.display()));

    assert_eq!(
        count_kind(&bytes, "run-started"),
        1,
        "one run-started bookend"
    );
    assert_eq!(
        count_kind(&bytes, "run-finished"),
        1,
        "one run-finished bookend"
    );
    assert_eq!(
        count_kind(&bytes, "node-terminal"),
        2,
        "one node-terminal per node"
    );
}

#[test]
fn run_to_store_surfaces_a_store_open_failure_as_the_store_variant() {
    // Point the store base at an existing FILE, so creating
    // `<base>/<pipeline>/<run-id>/` under it fails (a file is not a directory). The
    // failure must surface BEFORE assembly. The file lives inside a private temp
    // base, so it is reclaimed with everything else when the guard drops.
    let holder = TempBase::new("blocked");
    let base_file = holder.path().join("not-a-directory");
    std::fs::write(&base_file, b"not a directory").expect("plant the blocking file");
    let base_str = base_file
        .to_str()
        .expect("temp path is valid UTF-8")
        .to_owned();

    let mut flow = RunnableFlow::new();
    let _ = flow.register_source("count", Count { up_to: 1 });

    let result = flow.run_to_store("quickstart", &base_str);
    assert!(
        matches!(result, Err(RunToStoreError::Store(_))),
        "an unopenable store is a Store error, not Assembly"
    );
}

#[test]
fn run_to_store_surfaces_an_assembly_failure_as_the_assembly_variant() {
    let base = TempBase::new("badflow");
    let base_str = base.as_str().to_owned();

    // Two nodes registered under the SAME name — a duplicate-name assembly failure.
    let mut flow = RunnableFlow::new();
    let _a = flow.register_source("dup", Count { up_to: 1 });
    let _b = flow.register_source("dup", Count { up_to: 2 });

    let result = flow.run_to_store("quickstart", &base_str);
    assert!(
        matches!(result, Err(RunToStoreError::Assembly(_))),
        "a flow that does not assemble is an Assembly error"
    );
}

#[test]
fn default_store_targets_the_documented_base() {
    // `run_to_default_store` writes under `./dagr-runs/<pipeline>/…` (the default
    // base). Assert the report's run id maps to a directory there, then clean it up.
    let mut flow = RunnableFlow::new();
    let _ = flow.register_source("count", Count { up_to: 3 });

    let report = flow
        .run_to_default_store("default-base-probe")
        .expect("assembles and runs to the default store");

    let dir = std::path::Path::new(dagr_cli::DEFAULT_STORE_BASE)
        .join("default-base-probe")
        .join(report.run_id());
    assert!(
        dir.join("events.jsonl").exists(),
        "the default store base holds the run's stream at {}",
        dir.display()
    );

    let _ = std::fs::remove_dir_all(
        std::path::Path::new(dagr_cli::DEFAULT_STORE_BASE).join("default-base-probe"),
    );
}

#[test]
fn system_clock_is_monotonic_and_tick_clock_increments() {
    // `SystemClock` never goes backward (it is `Instant`-derived).
    let sys = SystemClock::new();
    let a = sys.elapsed_ns();
    let b = sys.elapsed_ns();
    assert!(
        b >= a,
        "SystemClock is monotonic non-decreasing: {b} >= {a}"
    );

    // `TickClock` advances exactly one per read (deterministic).
    let tick = TickClock::default();
    let first = tick.elapsed_ns();
    let second = tick.elapsed_ns();
    assert_eq!(second, first + 1, "TickClock increments by one per read");
}
