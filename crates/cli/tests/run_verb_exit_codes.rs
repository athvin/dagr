//! C26 · **`run` verb → exit code** end-to-end tests — ticket T55 (068). TDD.
//!
//! These drive the **real** run-loop driver (`dagr_cli::driver::drive`) through
//! the library `run` verb's outcome→exit-code selection and assert the **numeric**
//! C26 exit code, so the exit-code table is load-bearing end-to-end (not only over
//! synthetic reports in `exit_code_table.rs`):
//!
//! - a run whose single node ends `failed` (no signal) → the run-failure code;
//! - stop-on-first-failure where a failure triggers self-inflicted cancellation of
//!   a pending node → still the run-failure code (the consequent cancellation does
//!   **not** mask the failure — the precedence assertion);
//! - a skip-only run → the success code.
//!
//! The exhaustive cross-cause table (assembly / bootstrap / cancellation / sink /
//! usage precedence) is asserted table-driven in `exit_code_table.rs`; the driver
//! already produces those `RunReport`s in its own suites.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock};
use dagr_cli::contract::{exit_code_for_run, ExitCode};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{FailureMode, Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{NodePolicy, TaskError};

// --- injection seams --------------------------------------------------------

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
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

// --- tasks ------------------------------------------------------------------

struct Succeeds;
impl Task for Succeeds {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

struct Fails;
impl Task for Fails {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::permanent("nope"))
    }
}

struct Skips;
impl Task for Skips {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::skip("nothing to do"))
    }
}

// --- a generic source runner over the real caught attempt path --------------

struct SourceRunner<T: Task<Input = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<T::Output>>,
}
impl<T: Task<Input = ()>> SourceRunner<T> {
    fn boxed(name: &str, task: T, slot: Arc<Slot<T::Output>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot,
        })
    }
}
impl<T: Task<Input = ()>> NodeRunner for SourceRunner<T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("source runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

fn slot_for<T: Send + Sync + 'static>(name: &str) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

/// The library `run` verb's exit-code selection: drive the plan, then map the
/// report through the C26 table. This is exactly what `run_verb` does around the
/// driver.
fn run_and_exit(config: &RunConfig, pipeline: Pipeline, runners: BTreeMap<String, Box<dyn NodeRunner>>) -> ExitCode {
    let report = drive(
        config,
        "demo",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    );
    exit_code_for_run(&report)
}

// ===========================================================================
// Tests
// ===========================================================================

/// A run whose single non-teardown node ends `failed` and no external signal
/// arrives exits with the run-failure code.
#[test]
fn a_failed_node_exits_run_failure() {
    let mut flow = Flow::new();
    let _h = flow.register_source("boom", &Fails);
    let pipeline = flow.finish();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("boom".into(), SourceRunner::boxed("boom", Fails, slot_for::<u64>("boom")));

    let exit = run_and_exit(&RunConfig::new("/tmp/dagr-t55-run"), pipeline, runners);
    assert_eq!(exit, ExitCode::RunFailure, "a failed node exits run-failure");
}

/// Under stop-on-first-failure, a node fails and the failure triggers
/// self-inflicted cancellation of pending nodes; the run still exits with the
/// run-failure code — the consequent cancellation does not mask it. (The
/// precedence assertion.)
#[test]
fn stop_on_first_failure_still_exits_run_failure() {
    // Two independent sources: one fails, one succeeds. Under stop-on-first-failure
    // the failure routes through the cancellation core with a FailureUnderStop
    // origin; the C26 table must still choose run-failure.
    let mut flow = Flow::new();
    let _f = flow.register_source("boom", &Fails);
    let _s = flow.register_source("other", &Succeeds);
    let pipeline = flow.finish();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("boom".into(), SourceRunner::boxed("boom", Fails, slot_for::<u64>("boom")));
    runners.insert("other".into(), SourceRunner::boxed("other", Succeeds, slot_for::<u64>("other")));

    let config = RunConfig::new("/tmp/dagr-t55-run").failure_mode(FailureMode::StopOnFirstFailure);
    let exit = run_and_exit(&config, pipeline, runners);
    assert_eq!(
        exit,
        ExitCode::RunFailure,
        "run failure beats the consequent stop-on-first-failure cancellation"
    );
}

/// A run in which every node ends in a skip-family state and none failed/timed-out
/// exits with the success code.
#[test]
fn a_skip_only_run_exits_success() {
    let mut flow = Flow::new();
    let _h = flow.register_source("skip", &Skips);
    let pipeline = flow.finish();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("skip".into(), SourceRunner::boxed("skip", Skips, slot_for::<u64>("skip")));

    let exit = run_and_exit(&RunConfig::new("/tmp/dagr-t55-run"), pipeline, runners);
    assert_eq!(exit, ExitCode::Success, "a skip-only run is a successful run");
}
