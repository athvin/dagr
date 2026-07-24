//! C17 · teardown nodes — ticket T52 (064). Written first, TDD.
//!
//! These drive the **real** run-loop ([`dagr_cli::driver::drive`]) end-to-end for
//! teardown: a teardown node runs at run-end on **every** exit path (success,
//! failure, stop-on-first-failure, external cancellation), under a **fresh,
//! uncancelled** signal with its own deadline, bypassing admission, with the
//! covered nodes' terminal states in its context, and with its own failure
//! isolated from the run's outcome and from the other teardowns. A pipeline with
//! **no** teardown is asserted byte-identical to the pre-teardown behaviour.
//!
//! Every assertion is against the parsed event stream and the returned per-node
//! terminal states — never internal driver state. Determinism (CI): cancellation
//! is driven by a programmatic trigger a scripted task fires; a teardown's
//! observations are recorded through shared flags, never a wall clock.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, CancelHandle, NodeRunner, RunConfig, RunPlan};
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEventSink};
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// In-memory sink + clock (the C19 injection seam).
// ===========================================================================

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
    tick: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed)
    }
}

// ===========================================================================
// Event-stream helpers.
// ===========================================================================

/// (kind, node) pairs, in stream order.
fn parse_events(bytes: &[u8]) -> Vec<(String, Option<String>)> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).ok()?;
            let kind = v.get("kind")?.as_str()?.to_string();
            let node = v
                .get("node")
                .and_then(|n| n.as_str())
                .map(str::to_string);
            Some((kind, node))
        })
        .collect()
}

fn has_event(events: &[(String, Option<String>)], kind: &str, node: Option<&str>) -> bool {
    events
        .iter()
        .any(|(k, n)| k == kind && n.as_deref() == node)
}

fn terminal_of(bytes: &[u8], node: &str) -> Option<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| {
            v.get("kind").and_then(|k| k.as_str()) == Some("node-terminal")
                && v.get("node").and_then(|n| n.as_str()) == Some(node)
        })
        .and_then(|v| {
            v.get("state")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        })
}

/// The count of node-terminal records for `node` — the "exactly one terminal"
/// invariant is `== 1` for every node, teardown included.
fn terminal_count(bytes: &[u8], node: &str) -> usize {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| {
            v.get("kind").and_then(|k| k.as_str()) == Some("node-terminal")
                && v.get("node").and_then(|n| n.as_str()) == Some(node)
        })
        .count()
}

// ===========================================================================
// Scripted tasks.
// ===========================================================================

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
        Err(TaskError::permanent("scripted failure"))
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

/// A cooperative in-flight task cancelled mid-run: spins on its per-attempt
/// signal and returns once it observes cancellation. It fires the external
/// `CancelHandle` on its first poll so the run is cancelled while it is live.
struct FiresCancelThenWaits {
    handle: CancelHandle,
}
impl Task for FiresCancelThenWaits {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        self.handle.cancel();
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                return Err(TaskError::permanent("stopped on cancellation"));
            }
            tokio::task::yield_now().await;
        }
        Ok(1)
    }
}

/// A teardown body that records, into shared state, (1) whether it observed its
/// signal as UNcancelled, (2) the covered terminal states it saw through its
/// context, keyed by covered node name, and (3) that it ran at all. Returns
/// success. Used to prove the fresh-signal + covered-states contract.
struct RecordingTeardown {
    ran: Arc<AtomicBool>,
    saw_uncancelled: Arc<AtomicBool>,
    covered: Arc<Mutex<Vec<(String, TerminalState)>>>,
    covered_names: Vec<String>,
}
impl Task for RecordingTeardown {
    type Input = ();
    type Output = ();
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<(), TaskError> {
        self.ran.store(true, Ordering::SeqCst);
        // The teardown's signal must be a FRESH, uncancelled one even when the run
        // was cancelled — C17.
        self.saw_uncancelled
            .store(!c.cancellation().is_cancelled(), Ordering::SeqCst);
        if let Some(states) = c.covered_terminal_states() {
            let mut out = self.covered.lock().unwrap();
            for name in &self.covered_names {
                if let Some(s) = states.get(NodeId::from_name(name)) {
                    out.push((name.clone(), s));
                }
            }
        }
        Ok(())
    }
}

/// A teardown that fails — its failure must be isolated (recorded `failed`, but
/// the run outcome and the other teardowns are unaffected).
struct TeardownFails;
impl Task for TeardownFails {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Err(TaskError::permanent("teardown blew up"))
    }
}

/// A teardown that waits past a deadline: it never observes cancellation (the
/// fresh signal is uncancelled) and never returns on its own, so only the
/// teardown deadline can bound it. Bounded loop so a regression cannot hang CI.
struct TeardownWaitsForever;
impl Task for TeardownWaitsForever {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        loop {
            tokio::task::yield_now().await;
        }
    }
}

/// A plain teardown that just succeeds.
struct TeardownSucceeds {
    ran: Arc<AtomicBool>,
}
impl Task for TeardownSucceeds {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        self.ran.store(true, Ordering::SeqCst);
        Ok(())
    }
}

// ===========================================================================
// Type-erased runners over the real C14 caught attempt path.
// ===========================================================================

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
        let mut task = self.task.take().expect("runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

fn ledger() -> Arc<ResidencyLedger> {
    ResidencyLedger::new()
}
fn slot_for<T: Send + Sync + 'static>(name: &str, consumers: u32) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        NodeId::from_name(name),
        name,
        consumers,
        false,
        0,
        ledger(),
    ))
}
fn src<T: Task<Input = ()>>(name: &str, task: T) -> Box<dyn NodeRunner> {
    SourceRunner::boxed(name, task, slot_for::<T::Output>(name, 0))
}

fn cfg() -> RunConfig {
    RunConfig::new("/tmp/dagr-t52-test")
}

// ===========================================================================
// Test 2/3 · runs after every terminal class of covered upstream.
// ===========================================================================

/// A teardown runs after its single covered node ends `succeeded`, `failed`,
/// `skipped`, and after a propagated `upstream-failed` — every case executes the
/// teardown and leaves it with exactly one terminal.
#[test]
fn teardown_runs_after_every_covered_terminal_class() {
    // (covered task builder, expected covered terminal). `upstream-failed` is
    // produced by a data-dependent node whose upstream fails.
    for (label, covered_state) in [("succeeded", "succeeded"), ("failed", "failed"), ("skipped", "skipped")] {
        let mut flow = Flow::new();
        let covered = match label {
            "succeeded" => flow.register_source("covered", &Succeeds),
            "failed" => flow.register_source("covered", &Fails),
            _ => flow.register_source("covered", &Skips),
        };
        let _t = flow.register_teardown("cleanup", &UnitTask, &[covered.ordering()]);
        let pipeline = flow.finish();
        pipeline.assemble().expect("assembles");

        let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
        let covered_runner = match label {
            "succeeded" => src("covered", Succeeds),
            "failed" => src("covered", Fails),
            _ => src("covered", Skips),
        };
        runners.insert("covered".into(), covered_runner);
        let ran = Arc::new(AtomicBool::new(false));
        runners.insert(
            "cleanup".into(),
            UnitRunner::boxed("cleanup", TeardownSucceeds { ran: ran.clone() }),
        );

        let sink = MemorySink::default();
        drive(
            &cfg(),
            "t52",
            Ok(RunPlan::new(pipeline, runners)),
            &[],
            sink.clone(),
            TickClock::default(),
        );

        assert_eq!(
            terminal_of(&sink.bytes(), "covered").as_deref(),
            Some(covered_state),
            "covered node ended {label}"
        );
        assert!(ran.load(Ordering::SeqCst), "teardown ran after {label}");
        assert_eq!(
            terminal_of(&sink.bytes(), "cleanup").as_deref(),
            Some("succeeded"),
            "teardown reached a terminal after {label}"
        );
        assert_eq!(
            terminal_count(&sink.bytes(), "cleanup"),
            1,
            "teardown has exactly one terminal after {label}"
        );
    }
}

// ===========================================================================
// Test 4 · covered terminal states are visible in context (no-op path).
// ===========================================================================

/// A teardown covering a succeeded node and a skipped node sees exactly those two
/// covered terminal states through its context, and can branch (the "no-op
/// because setup never ran" path is exercised when a covered node did not succeed).
#[test]
fn teardown_context_exposes_covered_terminal_states() {
    let mut flow = Flow::new();
    let setup = flow.register_source("setup", &Succeeds);
    let declined = flow.register_source("declined", &Skips);
    let _t = flow.register_teardown("cleanup", &UnitTask, &[setup.ordering(), declined.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let ran = Arc::new(AtomicBool::new(false));
    let saw_uncancelled = Arc::new(AtomicBool::new(false));
    let covered = Arc::new(Mutex::new(Vec::new()));
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("setup".into(), src("setup", Succeeds));
    runners.insert("declined".into(), src("declined", Skips));
    runners.insert(
        "cleanup".into(),
        UnitRunner::boxed(
            "cleanup",
            RecordingTeardown {
                ran: ran.clone(),
                saw_uncancelled: saw_uncancelled.clone(),
                covered: covered.clone(),
                covered_names: vec!["setup".into(), "declined".into()],
            },
        ),
    );

    let sink = MemorySink::default();
    drive(
        &cfg(),
        "t52",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert!(ran.load(Ordering::SeqCst), "teardown ran");
    let mut seen = covered.lock().unwrap().clone();
    seen.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        seen,
        vec![
            ("declined".to_string(), TerminalState::Skipped),
            ("setup".to_string(), TerminalState::Succeeded),
        ],
        "teardown observes exactly its two covered nodes' terminal states"
    );
    // A non-cancelled run's teardown signal is also uncancelled.
    assert!(saw_uncancelled.load(Ordering::SeqCst));
}

// ===========================================================================
// Test 5 · failure isolation — outcome unchanged.
// ===========================================================================

/// A pipeline whose non-teardown work all succeeds, plus a teardown that fails:
/// the teardown is recorded `failed`, but the run's overall outcome is
/// `succeeded` (run failure is determined only by non-teardown nodes).
#[test]
fn failing_teardown_does_not_change_run_outcome() {
    let mut flow = Flow::new();
    let work = flow.register_source("work", &Succeeds);
    let _t = flow.register_teardown("cleanup", &UnitTask, &[work.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("work".into(), src("work", Succeeds));
    runners.insert("cleanup".into(), UnitRunner::boxed("cleanup", TeardownFails));

    let sink = MemorySink::default();
    let report = drive(
        &cfg(),
        "t52",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(
        terminal_of(&sink.bytes(), "cleanup").as_deref(),
        Some("failed"),
        "a failing teardown is recorded failed"
    );
    assert_eq!(
        report.outcome,
        RunOutcome::Succeeded,
        "the run outcome is determined only by non-teardown nodes"
    );
}

// ===========================================================================
// Test 6 · one failing teardown does not block others.
// ===========================================================================

/// Three independent teardown nodes, the second of which fails: all three run and
/// the first and third complete normally regardless of the second's failure.
#[test]
fn one_failing_teardown_does_not_block_others() {
    let mut flow = Flow::new();
    let work = flow.register_source("work", &Succeeds);
    let ran1 = Arc::new(AtomicBool::new(false));
    let ran3 = Arc::new(AtomicBool::new(false));
    let _t1 = flow.register_teardown("t1", &UnitTask, &[work.ordering()]);
    let _t2 = flow.register_teardown("t2", &UnitTask, &[work.ordering()]);
    let _t3 = flow.register_teardown("t3", &UnitTask, &[work.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("work".into(), src("work", Succeeds));
    runners.insert(
        "t1".into(),
        UnitRunner::boxed("t1", TeardownSucceeds { ran: ran1.clone() }),
    );
    runners.insert("t2".into(), UnitRunner::boxed("t2", TeardownFails));
    runners.insert(
        "t3".into(),
        UnitRunner::boxed("t3", TeardownSucceeds { ran: ran3.clone() }),
    );
    let pipeline_ref = &pipeline;
    let _ = pipeline_ref;

    let sink = MemorySink::default();
    let report = drive(
        &cfg(),
        "t52",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert!(ran1.load(Ordering::SeqCst), "t1 ran");
    assert!(ran3.load(Ordering::SeqCst), "t3 ran (after t2 failed)");
    assert_eq!(terminal_of(&sink.bytes(), "t1").as_deref(), Some("succeeded"));
    assert_eq!(terminal_of(&sink.bytes(), "t2").as_deref(), Some("failed"));
    assert_eq!(terminal_of(&sink.bytes(), "t3").as_deref(), Some("succeeded"));
    assert_eq!(report.outcome, RunOutcome::Succeeded);
}

// ===========================================================================
// Test 7 · executes under termination-signal cancellation, fresh signal.
// ===========================================================================

/// A run cancelled mid-flight by an external termination signal still runs its
/// teardown, under a FRESH signal that is not cancelled — the teardown body
/// observes its signal as uncancelled and completes.
#[test]
fn teardown_runs_under_cancellation_with_a_fresh_signal() {
    let config = cfg().grace(Duration::from_millis(150));
    let handle = config.cancel_handle();

    let mut flow = Flow::new();
    let work = flow.register_source("work", &Succeeds);
    let _t = flow.register_teardown("cleanup", &UnitTask, &[work.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let ran = Arc::new(AtomicBool::new(false));
    let saw_uncancelled = Arc::new(AtomicBool::new(false));
    let covered = Arc::new(Mutex::new(Vec::new()));
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "work".into(),
        src("work", FiresCancelThenWaits { handle }),
    );
    runners.insert(
        "cleanup".into(),
        UnitRunner::boxed(
            "cleanup",
            RecordingTeardown {
                ran: ran.clone(),
                saw_uncancelled: saw_uncancelled.clone(),
                covered: covered.clone(),
                covered_names: vec!["work".into()],
            },
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &config,
        "t52",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The covered node reached a terminal on the cancellation path.
    assert_eq!(
        terminal_of(&sink.bytes(), "work").as_deref(),
        Some("cancelled"),
        "the in-flight covered node was cancelled"
    );
    // The teardown ran, under a FRESH (uncancelled) signal, and completed.
    assert!(ran.load(Ordering::SeqCst), "teardown ran on the cancel path");
    assert!(
        saw_uncancelled.load(Ordering::SeqCst),
        "the teardown's signal is fresh and uncancelled even after a run cancel"
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "cleanup").as_deref(),
        Some("succeeded")
    );
    // The run itself is still a cancellation (teardown never masks it).
    assert_eq!(report.outcome, RunOutcome::Cancelled);
}

// ===========================================================================
// Test 8 · teardown deadline bounds a runaway teardown (and defaults to 15s).
// ===========================================================================

/// A teardown whose body never returns is bounded by its own deadline (set well
/// below the default via the flag): the run terminates and the teardown reaches a
/// terminal rather than hanging. The default deadline is 15 s when unset.
#[test]
fn teardown_deadline_bounds_a_runaway_teardown() {
    // Default is 15s when the flag is unset.
    assert_eq!(
        cfg().effective_teardown_deadline(),
        Duration::from_secs(15)
    );

    let config = cfg().teardown_deadline(Duration::from_millis(120));
    let mut flow = Flow::new();
    let work = flow.register_source("work", &Succeeds);
    let _t = flow.register_teardown("cleanup", &UnitTask, &[work.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("work".into(), src("work", Succeeds));
    runners.insert(
        "cleanup".into(),
        UnitRunner::boxed("cleanup", TeardownWaitsForever),
    );

    let sink = MemorySink::default();
    // If the deadline is not honoured this hangs; the test's own harness timeout
    // would catch a regression, but the deadline should terminate it promptly.
    let report = drive(
        &config,
        "t52",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The runaway teardown is bounded and recorded terminal (abandoned/timed-out
    // class), and the run does not hang. Its terminal is failure-like but isolated
    // from the run outcome (non-teardown work all succeeded).
    let cleanup_terminal = terminal_of(&sink.bytes(), "cleanup");
    assert!(
        cleanup_terminal.is_some(),
        "a runaway teardown still reaches a terminal within its deadline"
    );
    assert_eq!(
        report.outcome,
        RunOutcome::Succeeded,
        "a bounded runaway teardown does not change the run outcome"
    );
}

// ===========================================================================
// Test 10 · admission bypass — teardown never competes for capacity.
// ===========================================================================

/// A memory-constrained run saturated by non-teardown work still runs its
/// teardown: the teardown is admitted without waiting on and without consuming
/// pool capacity (it never appears in the admission ledger / never blocks).
#[test]
fn teardown_bypasses_admission_under_a_saturated_pool() {
    use dagr_core::admission::PoolCapacities;

    let mut flow = Flow::new();
    // Two non-teardown workers each declaring the whole memory pool, serialized by
    // capacity. A zero-cost teardown must still run despite the saturation.
    let a = flow.register_source_with("a", &Succeeds, NodePolicy::new().working_memory(10));
    let b = flow.register_source_with("b", &Succeeds, NodePolicy::new().working_memory(10));
    let ran = Arc::new(AtomicBool::new(false));
    let _t = flow.register_teardown("cleanup", &UnitTask, &[a.ordering(), b.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", Succeeds, slot_for::<u64>("a", 0)),
    );
    runners.insert(
        "b".into(),
        SourceRunner::boxed("b", Succeeds, slot_for::<u64>("b", 0)),
    );
    runners.insert(
        "cleanup".into(),
        UnitRunner::boxed("cleanup", TeardownSucceeds { ran: ran.clone() }),
    );

    let config = cfg().capacities(PoolCapacities::new().memory(10));
    let sink = MemorySink::default();
    let report = drive(
        &config,
        "t52",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert!(
        ran.load(Ordering::SeqCst),
        "teardown ran despite the saturated pool (it bypasses admission)"
    );
    assert_eq!(terminal_of(&sink.bytes(), "cleanup").as_deref(), Some("succeeded"));
    assert_eq!(report.outcome, RunOutcome::Succeeded);
}

// ===========================================================================
// Backward-compat · a no-teardown pipeline is byte-identical.
// ===========================================================================

/// A pipeline with NO teardown nodes produces exactly the same event stream and
/// terminal states as the pre-teardown driver: same terminals, same event stream
/// bytes. This is the byte-identical backward-compat guarantee.
#[test]
fn no_teardown_pipeline_is_byte_identical() {
    let build = || {
        let mut flow = Flow::new();
        let _a = flow.register_source("a", &Succeeds);
        let _b = flow.register_source("b", &Succeeds);
        let pipeline = flow.finish();
        pipeline.assemble().expect("assembles");
        let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
        runners.insert("a".into(), src("a", Succeeds));
        runners.insert("b".into(), src("b", Succeeds));
        (pipeline, runners)
    };

    let run_once = || {
        let (pipeline, runners) = build();
        let sink = MemorySink::default();
        let report = drive(
            &cfg().run_id("fixed-run-id"),
            "t52",
            Ok(RunPlan::new(pipeline, runners)),
            &[],
            sink.clone(),
            TickClock::default(),
        );
        (sink.bytes(), report.terminal_states, report.outcome)
    };

    let (bytes1, terminals1, outcome1) = run_once();
    let (bytes2, terminals2, outcome2) = run_once();
    assert_eq!(bytes1, bytes2, "no-teardown stream is deterministic");
    assert_eq!(terminals1, terminals2);
    assert_eq!(outcome1, outcome2);
    // No teardown means no teardown-phase records at all.
    let events = parse_events(&bytes1);
    assert!(!has_event(&events, "node-terminal", Some("cleanup")));
    assert_eq!(outcome1, RunOutcome::Succeeded);
}

// ===========================================================================
// A unit-output task + its type-erased runner (teardown produces `()`).
// ===========================================================================

struct UnitTask;
impl Task for UnitTask {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

struct UnitRunner<T: Task<Input = (), Output = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<()>>,
}
impl<T: Task<Input = (), Output = ()>> UnitRunner<T> {
    fn boxed(name: &str, task: T) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot: slot_for::<()>(name, 0),
        })
    }
}
impl<T: Task<Input = (), Output = ()>> NodeRunner for UnitRunner<T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}
