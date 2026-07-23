//! C16 · cancellation core and graceful drain — ticket T35 (045). Written first, TDD.
//!
//! These exercise the **real** T24/T34 run-loop driver ([`dagr_cli::driver::drive`])
//! end-to-end for the C16 cancellation core: a run-scoped token with per-attempt
//! children, a **programmatic** cancellation trigger (no OS signals — that is T36),
//! the cooperative grace period, drain-before-exit, and the `cancelled`-vs-
//! `abandoned` terminal classification. Every scenario asserts against the parsed
//! event stream and the returned per-node terminal states — never internal state.
//!
//! Determinism (CI): cancellation is driven by a **programmatic trigger** a
//! scripted task fires, and a task's cooperation is controlled by a shared flag and
//! observable barriers — never a wall clock, never the network, never a real OS
//! signal. A task that ignores cancellation past a **short** configured grace is
//! abandoned after the grace elapses, so even the grace-bound tests terminate
//! quickly and deterministically (the assertion is on the terminal state and that
//! the run terminates, not on a precise duration).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{
    drive, shutdown_budget, CancelHandle, NodeRunner, RunConfig, RunPlan,
    DEFAULT_FINAL_FLUSH, DEFAULT_GRACE, DEFAULT_TEARDOWN_DEADLINE,
};
use dagr_core::assembly::NodePolicy;
use dagr_core::binding::TriggerRule;
use dagr_core::context::{CancellationOrigin, RunContext, TerminalState};
use dagr_core::execution::AttemptEventSink;
use dagr_core::flow::{FailureMode, Flow};
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
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

// ===========================================================================
// Parsed-stream helpers.
// ===========================================================================

fn parse_events(bytes: &[u8]) -> Vec<(String, Option<String>)> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .map(|rec| {
            let kind = rec
                .get("event")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let node = rec
                .get("body")
                .and_then(|b| b.get("node"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            (kind, node)
        })
        .collect()
}

fn has_event(events: &[(String, Option<String>)], kind: &str, node: Option<&str>) -> bool {
    events
        .iter()
        .any(|(k, n)| k == kind && n.as_deref() == node)
}

fn terminal_of(bytes: &[u8], node: &str) -> Option<String> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream.records.iter().find_map(|rec| {
        let is_terminal = rec.get("event").and_then(|v| v.as_str()) == Some("node-terminal");
        let this_node = rec
            .get("body")
            .and_then(|b| b.get("node"))
            .and_then(|v| v.as_str());
        if is_terminal && this_node == Some(node) {
            rec.get("body")
                .and_then(|b| b.get("state"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        } else {
            None
        }
    })
}

/// Count `node-terminal` records for `node` (the exactly-once check).
fn terminal_count(bytes: &[u8], node: &str) -> usize {
    parse_events(bytes)
        .iter()
        .filter(|(k, n)| k == "node-terminal" && n.as_deref() == Some(node))
        .count()
}

/// The whole stream parses and is gapless (sequence numbers 0..N contiguous).
fn stream_is_complete_and_parseable(bytes: &[u8]) {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    for (i, rec) in stream.records.iter().enumerate() {
        let seq = rec.get("seq").and_then(serde_json::Value::as_u64);
        assert_eq!(
            seq,
            Some(i as u64),
            "gapless sequence: record {i} carries seq {seq:?}"
        );
    }
    // A complete run stream ends with a run-finished record.
    let kinds: Vec<&str> = stream
        .records
        .iter()
        .filter_map(|r| r.get("event").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(
        kinds.last().copied(),
        Some("run-finished"),
        "the stream ends with run-finished (complete)"
    );
}

// ===========================================================================
// Scripted, cancellation-aware tasks.
// ===========================================================================

/// A task that fires the programmatic cancel trigger the instant it runs, then
/// returns success — the deterministic in-run cancellation trigger (a scripted
/// task holding the external `CancelHandle`, standing in for T36's signal source).
struct FiresCancel {
    handle: CancelHandle,
}
impl Task for FiresCancel {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        self.handle.cancel();
        Ok(1)
    }
}

/// A **cooperative** await-bound task: it spins on the cancellation signal and
/// returns promptly once it observes cancellation. It yields between checks so it
/// never starves the async runtime. Because it is in flight when the run is
/// cancelled and returns within grace, the driver records it `cancelled` (the slot
/// is left unfilled). If cancellation never came it would return the fallback
/// value — but every test using it triggers cancellation, so it observes and
/// returns. Returning `permanent` here is deliberate: it proves the driver
/// classifies an in-flight-at-cancel return as `cancelled`, never as the raw
/// failure the aborted work happened to produce.
struct CooperativeWaiter;
impl Task for CooperativeWaiter {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                // Observed the signal — return promptly, filling nothing.
                return Err(TaskError::permanent("stopped on cancellation"));
            }
            tokio::task::yield_now().await;
        }
        Ok(7)
    }
}

/// A task that **ignores** cancellation and keeps running well past a short grace.
/// It parks on a barrier the test releases only after it has asserted the node was
/// abandoned, so the driver must not wait for it — the drain abandons it after
/// grace and proceeds. Deterministic: no wall-clock dependence for the *assertion*;
/// the grace is configured short so the run terminates quickly.
struct IgnoresCancel {
    // Released by the test once it has observed the abandonment, so the closure can
    // finally return and the process can exit cleanly (no leaked busy thread).
    release: Arc<Mutex<bool>>,
}
impl Task for IgnoresCancel {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // Ignore cancellation entirely: loop until the test releases the barrier.
        loop {
            if *self.release.lock().unwrap() {
                return Ok(9);
            }
            tokio::task::yield_now().await;
        }
    }
}

/// A source task that permanently fails.
struct Fails;
impl Task for Fails {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::permanent("scripted failure"))
    }
}

/// A source task that succeeds.
struct Succeeds;
impl Task for Succeeds {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

// ===========================================================================
// A type-erased source runner over the real C14 caught attempt path.
// ===========================================================================

use dagr_core::execution::run_attempt_caught;
use dagr_core::slot::{ResidencyLedger, Slot};

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
        dagr_core::handle::NodeId::from_name(name),
        name,
        consumers,
        false,
        0,
        ledger(),
    ))
}

fn order(pairs: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    pairs
        .iter()
        .map(|(node, ups)| {
            (
                (*node).to_string(),
                ups.iter().map(|s| (*s).to_string()).collect(),
            )
        })
        .collect()
}

/// A short grace so the abandonment tests terminate quickly and deterministically.
const SHORT_GRACE: Duration = Duration::from_millis(150);

// ===========================================================================
// Core token semantics (arch.md C16 — run token / per-attempt children).
// ===========================================================================

/// **Run token cancels all live children exactly once and is idempotent.**
#[test]
fn run_token_cancels_all_children_once_and_is_idempotent() {
    use dagr_core::context::CancellationSource;
    let run = CancellationSource::new();
    let a = run.child();
    let b = run.child();
    let sig_a = a.signal();
    let sig_b = b.signal();
    assert!(!sig_a.is_cancelled() && !sig_b.is_cancelled());

    run.cancel();
    assert!(sig_a.is_cancelled(), "child A observes the run cancel");
    assert!(sig_b.is_cancelled(), "child B observes the run cancel");

    // Idempotent: a second cancel changes nothing observable.
    run.cancel();
    assert!(sig_a.is_cancelled() && sig_b.is_cancelled());
}

/// **A child cancel touches neither the sibling nor the parent run token.**
#[test]
fn child_cancel_does_not_touch_siblings_or_parent() {
    use dagr_core::context::CancellationSource;
    let run = CancellationSource::new();
    let a = run.child();
    let b = run.child();

    a.cancel();
    assert!(a.signal().is_cancelled(), "the cancelled child is cancelled");
    assert!(
        !b.signal().is_cancelled(),
        "the sibling child stays uncancelled"
    );
    assert!(
        !run.is_cancelled(),
        "the parent run token stays uncancelled — a child cancel is not a run cancel"
    );
}

// ===========================================================================
// Cooperative cancellation and drain through the real driver.
// ===========================================================================

/// **A prompt cooperative observer is recorded `cancelled`.** One source fires the
/// programmatic cancel; a concurrent await-bound waiter observes the run token and
/// returns promptly, recorded exactly `cancelled` (distinct from failed/timed-out),
/// filling no slot. The run drains and terminates with a complete stream.
#[test]
fn prompt_cooperative_observer_is_cancelled() {
    let mut flow = Flow::new();
    let _t = flow.register_source("trigger", &Succeeds);
    let _w = flow.register_source("waiter", &Succeeds);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let cfg = RunConfig::new("/tmp/dagr-t35").grace(SHORT_GRACE);
    let handle = cfg.cancel_handle();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancel {
                handle: handle.clone(),
            },
            slot_for::<u64>("trigger", 0),
        ),
    );
    runners.insert(
        "waiter".into(),
        SourceRunner::boxed("waiter", CooperativeWaiter, slot_for::<u64>("waiter", 0)),
    );

    let sink = MemorySink::default();
    let report = drive(
        &cfg,
        "coop",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(
        terminal_of(&sink.bytes(), "waiter").as_deref(),
        Some("cancelled"),
        "a prompt cooperative observer is recorded cancelled"
    );
    assert_ne!(terminal_of(&sink.bytes(), "waiter").as_deref(), Some("failed"));
    assert_eq!(terminal_count(&sink.bytes(), "waiter"), 1);
    stream_is_complete_and_parseable(&sink.bytes());
    assert_eq!(report.outcome, RunOutcome::Cancelled);
    assert_eq!(
        report.cancellation_origin,
        Some(CancellationOrigin::ExternalInterrupt),
        "the programmatic trigger records an external-interrupt origin"
    );
}

/// **Non-returning work is recorded `abandoned` after grace; the run still
/// terminates.** A waiter ignores cancellation and keeps running past the short
/// grace; the driver abandons it and proceeds — no indefinite wait — and the
/// abandoned attempt can never fill its slot. The barrier is released only after
/// the abandonment is observed, proving the driver did not wait for the closure.
#[test]
fn non_returning_work_is_abandoned_after_grace_and_run_terminates() {
    let release = Arc::new(Mutex::new(false));

    let mut flow = Flow::new();
    let _t = flow.register_source("trigger", &Succeeds);
    let _w = flow.register_source("ignorer", &Succeeds);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let cfg = RunConfig::new("/tmp/dagr-t35").grace(SHORT_GRACE);
    let handle = cfg.cancel_handle();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancel {
                handle: handle.clone(),
            },
            slot_for::<u64>("trigger", 0),
        ),
    );
    runners.insert(
        "ignorer".into(),
        SourceRunner::boxed(
            "ignorer",
            IgnoresCancel {
                release: Arc::clone(&release),
            },
            slot_for::<u64>("ignorer", 0),
        ),
    );

    let sink = MemorySink::default();
    let start = Instant::now();
    let report = drive(
        &cfg,
        "abandon",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let elapsed = start.elapsed();
    // Release the still-running closure now that the drive returned, so its thread
    // can exit (no leaked busy loop after the test).
    *release.lock().unwrap() = true;

    assert_eq!(
        terminal_of(&sink.bytes(), "ignorer").as_deref(),
        Some("abandoned"),
        "work that ignores cancellation past grace is recorded abandoned"
    );
    assert_eq!(
        terminal_count(&sink.bytes(), "ignorer"),
        1,
        "abandoned is the node's single terminal state (never a second)"
    );
    stream_is_complete_and_parseable(&sink.bytes());
    assert_eq!(report.outcome, RunOutcome::Failed, "abandoned is failure-like");
    // The drive returned without waiting indefinitely: it terminated in roughly the
    // grace window, far under any hang. A generous ceiling keeps CI non-flaky.
    assert!(
        elapsed < Duration::from_secs(5),
        "the run terminated promptly after grace ({elapsed:?}), it did not hang"
    );
}

/// **`cancelled`, `abandoned`, and `failed` are distinct with no
/// cross-contamination.** Three concurrent nodes: one cooperates (cancelled), one
/// ignores (abandoned), one fails before cancellation (failed).
#[test]
fn cancelled_abandoned_and_failed_are_distinct() {
    let release = Arc::new(Mutex::new(false));

    let mut flow = Flow::new();
    let _f = flow.register_source("bad", &Fails);
    let _c = flow.register_source("coop", &Succeeds);
    let _i = flow.register_source("ignorer", &Succeeds);
    let _tr = flow.register_source("trigger", &Succeeds);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let cfg = RunConfig::new("/tmp/dagr-t35").grace(SHORT_GRACE);
    let handle = cfg.cancel_handle();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "bad".into(),
        SourceRunner::boxed("bad", Fails, slot_for::<u64>("bad", 0)),
    );
    runners.insert(
        "coop".into(),
        SourceRunner::boxed("coop", CooperativeWaiter, slot_for::<u64>("coop", 0)),
    );
    runners.insert(
        "ignorer".into(),
        SourceRunner::boxed(
            "ignorer",
            IgnoresCancel {
                release: Arc::clone(&release),
            },
            slot_for::<u64>("ignorer", 0),
        ),
    );
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancel {
                handle: handle.clone(),
            },
            slot_for::<u64>("trigger", 0),
        ),
    );

    let sink = MemorySink::default();
    let _report = drive(
        &cfg,
        "distinct",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    *release.lock().unwrap() = true;

    assert_eq!(terminal_of(&sink.bytes(), "bad").as_deref(), Some("failed"));
    assert_eq!(
        terminal_of(&sink.bytes(), "coop").as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "ignorer").as_deref(),
        Some("abandoned")
    );
    for node in ["bad", "coop", "ignorer"] {
        assert_eq!(terminal_count(&sink.bytes(), node), 1, "{node} decided once");
    }
    stream_is_complete_and_parseable(&sink.bytes());
}

/// **No new admission after cancellation.** A ready-but-unstarted node is settled
/// to `cancelled` rather than executed once cancellation has fired. Admission is
/// serialized by pinning the memory pool so the unstarted node is still pending
/// when the trigger fires.
#[test]
fn no_new_admission_after_cancellation() {
    use dagr_core::admission::PoolCapacities;

    let costed = || NodePolicy::new().working_memory(10);
    let mut flow = Flow::new();
    // `trigger` fires cancellation the moment it runs; `later` is a costed
    // default-rule node that has not yet been admitted (pool serialized to one).
    let _tr = flow.register_source_with("trigger", &Succeeds, costed());
    let _later = flow.register_source_with("later", &Succeeds, costed());
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let cfg = RunConfig::new("/tmp/dagr-t35")
        .grace(SHORT_GRACE)
        .capacities(PoolCapacities::new().memory(10));
    let handle = cfg.cancel_handle();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancel {
                handle: handle.clone(),
            },
            slot_for::<u64>("trigger", 0),
        ),
    );
    runners.insert(
        "later".into(),
        SourceRunner::boxed("later", Succeeds, slot_for::<u64>("later", 0)),
    );

    let sink = MemorySink::default();
    let _ = drive(
        &cfg,
        "no-admit",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(
        terminal_of(&sink.bytes(), "later").as_deref(),
        Some("cancelled"),
        "a ready-but-unstarted node is settled cancelled, not executed, after cancellation"
    );
    assert!(
        !has_event(&parse_events(&sink.bytes()), "attempt-started", Some("later")),
        "no new attempt is spawned after cancellation"
    );
    stream_is_complete_and_parseable(&sink.bytes());
}

// ===========================================================================
// Stop-on-first-failure routes through the cancellation core.
// ===========================================================================

/// **Stop-on-first-failure triggers cancellation via the core with a failure
/// origin.** A failing node under stop mode leaves a pending unrelated default-rule
/// node `cancelled` (T34's resolved rule) and the recorded origin is
/// failure-under-stop, so later exit-code logic can prefer run failure.
#[test]
fn stop_on_first_failure_routes_through_cancellation_core_with_failure_origin() {
    use dagr_core::admission::PoolCapacities;

    let costed = || NodePolicy::new().working_memory(10);
    let mut flow = Flow::new();
    let _bad = flow.register_source_with("bad", &Fails, costed());
    let _later = flow.register_source_with("later", &Succeeds, costed());
    let _n = flow.register_source_with_trigger(
        "contingency",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AnyFailed,
    );
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let cfg = RunConfig::new("/tmp/dagr-t35")
        .failure_mode(FailureMode::StopOnFirstFailure)
        .capacities(PoolCapacities::new().memory(10));

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "bad".into(),
        SourceRunner::boxed("bad", Fails, slot_for::<u64>("bad", 0)),
    );
    runners.insert(
        "later".into(),
        SourceRunner::boxed("later", Succeeds, slot_for::<u64>("later", 0)),
    );
    runners.insert(
        "contingency".into(),
        SourceRunner::boxed("contingency", Succeeds, slot_for::<u64>("contingency", 0)),
    );

    let sink = MemorySink::default();
    let report = drive(
        &cfg,
        "stop",
        Ok(RunPlan::with_ordering(
            pipeline,
            runners,
            order(&[("contingency", &["bad"])]),
        )),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(terminal_of(&sink.bytes(), "bad").as_deref(), Some("failed"));
    assert_eq!(
        terminal_of(&sink.bytes(), "later").as_deref(),
        Some("cancelled"),
        "a pending unrelated default-rule node ends cancelled under stop mode"
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "contingency").as_deref(),
        Some("succeeded"),
        "a firing contingency still runs"
    );
    assert_eq!(report.outcome, RunOutcome::Failed);
    assert_eq!(
        report.cancellation_origin,
        Some(CancellationOrigin::FailureUnderStop),
        "stop-on-first-failure records a failure origin so exit-code logic can prefer the failure"
    );
    stream_is_complete_and_parseable(&sink.bytes());
}

// ===========================================================================
// Grace default + flag, and the shutdown budget.
// ===========================================================================

/// **Grace default is 10 s; an override is honoured.** The run config's effective
/// grace is the default when unset, and the override otherwise. (The override's
/// effect on the drain is exercised by the abandonment tests, which run under a
/// short grace and terminate quickly.)
#[test]
fn grace_default_is_ten_seconds_and_override_is_honoured() {
    assert_eq!(DEFAULT_GRACE, Duration::from_secs(10));
    let default_cfg = RunConfig::new("/tmp/dagr-t35");
    assert_eq!(default_cfg.effective_grace(), Duration::from_secs(10));
    let overridden = RunConfig::new("/tmp/dagr-t35").grace(Duration::from_secs(3));
    assert_eq!(overridden.effective_grace(), Duration::from_secs(3));
}

/// **The worst-case shutdown budget is grace + teardown deadline + final flush,
/// with the arithmetic total; it reflects the effective flag values.** Defaults
/// total 27 s (10 + 15 + 2); overriding grace changes the total accordingly, and
/// the printed line shows the arithmetic.
#[test]
fn shutdown_budget_is_grace_plus_teardown_plus_flush_and_reflects_flags() {
    assert_eq!(DEFAULT_GRACE, Duration::from_secs(10));
    assert_eq!(DEFAULT_TEARDOWN_DEADLINE, Duration::from_secs(15));
    assert_eq!(DEFAULT_FINAL_FLUSH, Duration::from_secs(2));

    let budget = shutdown_budget(DEFAULT_GRACE, DEFAULT_TEARDOWN_DEADLINE);
    assert_eq!(budget.grace(), Duration::from_secs(10));
    assert_eq!(budget.teardown_deadline(), Duration::from_secs(15));
    assert_eq!(budget.final_flush(), Duration::from_secs(2));
    assert_eq!(
        budget.total(),
        Duration::from_secs(27),
        "defaults total 27 s (10 + 15 + 2)"
    );

    // The printed line shows the arithmetic and the total.
    let line = budget.to_string();
    assert!(line.contains("27"), "the printed budget shows the total: {line}");
    assert!(line.contains("10"), "the printed budget shows the grace: {line}");
    assert!(line.contains("15"), "the printed budget shows the teardown: {line}");
    assert!(line.contains('2'), "the printed budget shows the flush: {line}");

    // Overriding grace changes the total accordingly.
    let overridden = shutdown_budget(Duration::from_secs(5), DEFAULT_TEARDOWN_DEADLINE);
    assert_eq!(overridden.total(), Duration::from_secs(22));
}

// ===========================================================================
// Natural run end still bounds the zombie wait (T24, not double-counted).
// ===========================================================================

/// **Natural run end (no cancellation) is unchanged.** A run that completes
/// normally with no cancellation ends success/failed exactly as before — the
/// cancellation core does not alter a non-cancelled run.
#[test]
fn non_cancelled_run_is_unchanged() {
    let mut flow = Flow::new();
    let _a = flow.register_source("a", &Succeeds);
    let _b = flow.register_source("b", &Succeeds);
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

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t35"),
        "normal",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(report.cancellation_origin, None, "no cancellation, no origin");
    assert_eq!(terminal_of(&sink.bytes(), "a").as_deref(), Some("succeeded"));
    assert_eq!(terminal_of(&sink.bytes(), "b").as_deref(), Some("succeeded"));
    stream_is_complete_and_parseable(&sink.bytes());
}
