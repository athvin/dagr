//! C15 · failure policy, propagation, and trigger-rule runtime — ticket T34 (044).
//! Written first, TDD.
//!
//! These exercise the **real** T24/T34 run-loop driver ([`dagr_cli::driver::drive`])
//! end-to-end: a graph is assembled, driven through the real two-runtime loop, and
//! asserted against the parsed event stream and the returned per-node terminal
//! states — never internal state. They cover the C15 runtime that T18 (readiness
//! evaluation) and T24 (the M1 loop that fired only `all-succeeded`) left to T34:
//!
//! - the runtime **firing of the non-default rules** `all-terminal` and
//!   `any-failed` (a cleanup fires after an upstream failure; a contingency fires
//!   on a failure and is `skipped` when none arose) — over the run-level ordering
//!   seam that stands in for T50's graph ordering edges;
//! - **failure propagation** by state class through the real driver (a failed
//!   upstream deadens an `all-succeeded` downstream to `upstream-failed`; a skip to
//!   `upstream-skipped`; a cancellation to `cancelled`);
//! - the two **failure modes** — continue-independent (unrelated branches
//!   complete) and stop-on-first-failure (no further default work admitted after
//!   the first failure; unrelated pending default nodes end `cancelled`; a firing
//!   contingency still runs);
//! - the **exactly-one-terminal-state** invariant across a mixed graph.
//!
//! Determinism: outcomes are scripted (a task either succeeds, fails, or skips);
//! ordering is by dependency structure, never sleeps — so CI is deterministic.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::assembly::NodePolicy;
use dagr_core::binding::TriggerRule;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt, run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{FailureMode, Flow};
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
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
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let node = rec.get("node").and_then(|v| v.as_str()).map(str::to_string);
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
        let is_terminal = rec.get("kind").and_then(|v| v.as_str()) == Some("node-terminal");
        let this_node = rec.get("node").and_then(|v| v.as_str());
        if is_terminal && this_node == Some(node) {
            rec.get("state")
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

/// A memory-hog task that **holds the whole working-memory pool until the run is
/// cancelled**, then returns. It cooperatively spins on its per-attempt
/// cancellation signal and returns the moment it observes the flip. Its purpose is
/// purely structural: by occupying the entire pinned memory pool for the whole
/// pre-stop window it keeps a serialized, ready-but-unstarted sibling **provably
/// pending** (never admitted) until the stop-under-failure settles that sibling
/// `cancelled`. This closes the initial-frontier permit-release race in
/// `stop_mode_cancels_pending_unrelated_default_and_runs_contingency`, where the
/// failing node cannot itself hold its permit until the stop it triggers (its
/// *return* is the trigger). No wall clock, no sleep — only the cancellation flag it
/// observes. Its own terminal state is not asserted. (Mirror of the identical helper
/// in `cancellation_core_and_drain.rs`.)
struct HoldsMemoryUntilCancelled;
impl Task for HoldsMemoryUntilCancelled {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // Spin cooperatively until the loop has entered cancellation (observed
        // through this attempt's child signal). Bounded so a regression that never
        // propagates the stop cannot hang the test — the fallback return then leaves
        // the gate non-vacuous.
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                return Ok(0);
            }
            tokio::task::yield_now().await;
        }
        Ok(0)
    }
}

/// A one-input pass-through consumer (data-dependent, always `all-succeeded`).
struct PassThrough;
impl Task for PassThrough {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
        Ok(i)
    }
}

// ===========================================================================
// Type-erased runners over the real C14 attempt path.
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

struct MapRunner<U: Send + Sync + 'static, T: Task<Input = U>> {
    name: String,
    task: Option<T>,
    upstream: SlotRef<U>,
    slot: Arc<Slot<T::Output>>,
}
impl<U: Send + Sync + Clone + 'static, T: Task<Input = U>> MapRunner<U, T> {
    fn boxed(
        name: &str,
        task: T,
        upstream: SlotRef<U>,
        slot: Arc<Slot<T::Output>>,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            upstream,
            slot,
        })
    }
}
impl<U: Send + Sync + Clone + 'static, T: Task<Input = U>> NodeRunner for MapRunner<U, T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("runs once");
        let slot = Arc::clone(&self.slot);
        let input = (*self.upstream.read()).clone();
        let mut bound = Bound {
            inner: task,
            input: Some(input),
        };
        Box::pin(async move {
            run_attempt(&mut bound, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

struct Bound<U, T> {
    inner: T,
    input: Option<U>,
}
impl<U: Send + 'static, T: Task<Input = U>> Task for Bound<U, T> {
    type Input = ();
    type Output = T::Output;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, TaskError> {
        let input = self.input.take().expect("consumed once");
        self.inner.run(ctx, input).await
    }
}

// ===========================================================================
// Builders.
// ===========================================================================

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

/// A run-level ordering map "node -> upstream names".
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

fn config(mode: FailureMode) -> RunConfig {
    RunConfig::new("/tmp/dagr-t34-test").failure_mode(mode)
}

// ===========================================================================
// Runtime firing of the NON-DEFAULT rules (C15 · T0.4 §5b/§5c — runtime).
// ===========================================================================

/// `all-terminal` cleanup fires after an upstream failure — verified under BOTH
/// modes. The cleanup node is ordered after a failing source; its `all-terminal`
/// rule fires regardless of class, so it EXECUTES and is never `upstream-failed`.
/// This is the entire reason non-default rules exist. (C15 def-of-done: all-terminal.)
#[test]
fn all_terminal_cleanup_fires_after_a_failure_in_both_modes() {
    for mode in [
        FailureMode::ContinueIndependent,
        FailureMode::StopOnFirstFailure,
    ] {
        let mut flow = Flow::new();
        let _f = flow.register_source("work", &Fails);
        let _c = flow.register_source_with_trigger(
            "cleanup",
            &Succeeds,
            NodePolicy::new(),
            TriggerRule::AllTerminal,
        );
        let pipeline = flow.finish();
        pipeline.assemble().expect("assembles");

        let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
        runners.insert(
            "work".into(),
            SourceRunner::boxed("work", Fails, slot_for::<u64>("work", 0)),
        );
        runners.insert(
            "cleanup".into(),
            SourceRunner::boxed("cleanup", Succeeds, slot_for::<u64>("cleanup", 0)),
        );

        let sink = MemorySink::default();
        let report = drive(
            &config(mode),
            "cleanup-run",
            Ok(RunPlan::with_ordering(
                pipeline,
                runners,
                order(&[("cleanup", &["work"])]),
            )),
            &[],
            sink.clone(),
            TickClock::default(),
        );

        assert_eq!(
            terminal_of(&sink.bytes(), "work").as_deref(),
            Some("failed")
        );
        assert_eq!(
            terminal_of(&sink.bytes(), "cleanup").as_deref(),
            Some("succeeded"),
            "all-terminal cleanup executes to a real outcome even after an upstream failure ({mode:?})"
        );
        // It actually executed (attempt-started present) — not a propagated state.
        let events = parse_events(&sink.bytes());
        assert!(
            has_event(&events, "attempt-started", Some("cleanup")),
            "the cleanup node truly executed ({mode:?})"
        );
        // The run still reports failed (the self-inflicted stop never masks the failure).
        assert_eq!(report.outcome, RunOutcome::Failed);
    }
}

/// `any-failed` contingency fires on a failure-like upstream: a consume-nothing
/// notify node ordered after a failing source executes. (C15 def-of-done: any-failed.)
#[test]
fn any_failed_contingency_fires_on_a_failure() {
    let mut flow = Flow::new();
    let _f = flow.register_source("work", &Fails);
    let _n = flow.register_source_with_trigger(
        "notify",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AnyFailed,
    );
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "work".into(),
        SourceRunner::boxed("work", Fails, slot_for::<u64>("work", 0)),
    );
    runners.insert(
        "notify".into(),
        SourceRunner::boxed("notify", Succeeds, slot_for::<u64>("notify", 0)),
    );

    let sink = MemorySink::default();
    let _ = drive(
        &config(FailureMode::ContinueIndependent),
        "notify-run",
        Ok(RunPlan::with_ordering(
            pipeline,
            runners,
            order(&[("notify", &["work"])]),
        )),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(
        terminal_of(&sink.bytes(), "notify").as_deref(),
        Some("succeeded"),
        "the any-failed contingency executes on the failure"
    );
    assert!(
        has_event(
            &parse_events(&sink.bytes()),
            "attempt-started",
            Some("notify")
        ),
        "the contingency truly executed"
    );
}

/// `any-failed` contingency that never arose → `skipped`: all ordering upstreams
/// succeed, so the guarded contingency did not arise; the node ends `skipped`
/// without executing, and the run is a success. (C15 def-of-done: any-failed skipped.)
#[test]
fn any_failed_contingency_never_arose_is_skipped_and_run_succeeds() {
    let mut flow = Flow::new();
    let _s = flow.register_source("work", &Succeeds);
    let _n = flow.register_source_with_trigger(
        "notify",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AnyFailed,
    );
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "work".into(),
        SourceRunner::boxed("work", Succeeds, slot_for::<u64>("work", 0)),
    );
    runners.insert(
        "notify".into(),
        SourceRunner::boxed("notify", Succeeds, slot_for::<u64>("notify", 0)),
    );

    let sink = MemorySink::default();
    let report = drive(
        &config(FailureMode::ContinueIndependent),
        "notify-run",
        Ok(RunPlan::with_ordering(
            pipeline,
            runners,
            order(&[("notify", &["work"])]),
        )),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(
        terminal_of(&sink.bytes(), "notify").as_deref(),
        Some("skipped"),
        "an any-failed contingency that never arose is skipped"
    );
    assert!(
        !has_event(
            &parse_events(&sink.bytes()),
            "attempt-started",
            Some("notify")
        ),
        "a never-arose contingency does not execute"
    );
    assert_eq!(
        report.outcome,
        RunOutcome::Succeeded,
        "a run whose only non-success outcome is a skip is a success"
    );
}

// ===========================================================================
// Failure propagation by state class through the real driver (§5a).
// ===========================================================================

/// A failing data upstream deadens an `all-succeeded` downstream to `upstream-failed`
/// and the deadened node never executes. (C15 def-of-done: no node runs on a
/// non-succeeded data dependency; propagated-state selection.)
#[test]
fn failed_data_upstream_propagates_upstream_failed() {
    let mut flow = Flow::new();
    let up = flow.register_source("up", &Fails);
    let _down = flow.register::<PassThrough, _>("down", &PassThrough, up);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let up_slot = slot_for::<u64>("up", 1);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "up".into(),
        SourceRunner::boxed("up", Fails, Arc::clone(&up_slot)),
    );
    runners.insert(
        "down".into(),
        MapRunner::boxed(
            "down",
            PassThrough,
            up_slot.shared_ref(),
            slot_for::<u64>("down", 0),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &config(FailureMode::ContinueIndependent),
        "prop",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::Failed);
    assert_eq!(
        terminal_of(&sink.bytes(), "down").as_deref(),
        Some("upstream-failed")
    );
    assert!(
        !has_event(
            &parse_events(&sink.bytes()),
            "attempt-started",
            Some("down")
        ),
        "the deadened node never executes"
    );
}

/// A skipping data upstream propagates `upstream-skipped` to its `all-succeeded`
/// downstream, and the run reports overall success (only skips among non-successes).
/// (C15 def-of-done: upstream-skipped; skip-only success.)
#[test]
fn skipped_data_upstream_propagates_upstream_skipped_and_run_succeeds() {
    let mut flow = Flow::new();
    let up = flow.register_source("up", &Skips);
    let _down = flow.register::<PassThrough, _>("down", &PassThrough, up);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let up_slot = slot_for::<u64>("up", 1);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "up".into(),
        SourceRunner::boxed("up", Skips, Arc::clone(&up_slot)),
    );
    runners.insert(
        "down".into(),
        MapRunner::boxed(
            "down",
            PassThrough,
            up_slot.shared_ref(),
            slot_for::<u64>("down", 0),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &config(FailureMode::ContinueIndependent),
        "prop",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "down").as_deref(),
        Some("upstream-skipped")
    );
    assert_eq!(
        report.outcome,
        RunOutcome::Succeeded,
        "a run whose only non-success outcomes are skips is a success"
    );
}

// ===========================================================================
// Failure modes.
// ===========================================================================

/// Continue-independent: an unrelated branch with no ancestral relationship to the
/// failure runs to completion. `bad` fails; the independent chain `a`→`b` both
/// succeed. (C15 def-of-done: continue-independent.)
#[test]
fn continue_independent_runs_unrelated_branch() {
    let mut flow = Flow::new();
    let _bad = flow.register_source("bad", &Fails);
    let a = flow.register_source("a", &Succeeds);
    let _b = flow.register::<PassThrough, _>("b", &PassThrough, a);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let a_slot = slot_for::<u64>("a", 1);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "bad".into(),
        SourceRunner::boxed("bad", Fails, slot_for::<u64>("bad", 0)),
    );
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", Succeeds, Arc::clone(&a_slot)),
    );
    runners.insert(
        "b".into(),
        MapRunner::boxed(
            "b",
            PassThrough,
            a_slot.shared_ref(),
            slot_for::<u64>("b", 0),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &config(FailureMode::ContinueIndependent),
        "continue",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "a").as_deref(),
        Some("succeeded")
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "b").as_deref(),
        Some("succeeded"),
        "the unrelated branch completes under continue-independent"
    );
    assert_eq!(report.outcome, RunOutcome::Failed);
}

/// Stop-on-first-failure cancels a pending unrelated default-rule node. A firing
/// `any-failed` contingency (zero cost, ordered after `bad`) still runs.
/// (C15 def-of-done: stop admits no further default work; pending unrelated →
/// cancelled; contingency still runs.)
///
/// Determinism (same observable-signal gating the C16 sibling
/// `stop_on_first_failure_routes_through_cancellation_core_with_failure_origin`
/// uses): `later`'s `cancelled` terminal depends on the stop landing **before**
/// `later` is admitted. `bad` fails and triggers the stop, but it cannot itself
/// hold a pool permit until the stop takes effect — its *return* is what triggers
/// the stop. So rather than assume the stop-cancel beats the scheduler re-offering
/// `pending` and admitting `later` (the observed CI flake: `later` came back
/// `succeeded`, not `cancelled`), admission is **serialized by pinning the memory
/// pool**: `bad` is made cost-free and a separate `keeper` node occupies the
/// **entire** pinned memory pool until the run is cancelled. That keeps the
/// unrelated default-rule `later` provably pending (never admitted) across the whole
/// pre-stop window — the stop settles it `cancelled` before any permit it could grab
/// is freed. `later` consumes nothing from `bad`/`keeper` (no data edge), so it stays
/// an unrelated default-rule node. No wall clock, no sleep.
#[test]
fn stop_mode_cancels_pending_unrelated_default_and_runs_contingency() {
    use dagr_core::admission::PoolCapacities;

    // `keeper` and `later` each declare 10 bytes of working memory; the memory pool
    // is pinned to 10, so exactly one fits — admission is serialized. `keeper` grabs
    // the sole permit (name order: `bad` < `keeper` < `later`; `bad` is cost-free)
    // and holds it until the run is cancelled, so `later` is still pending (waiting
    // for capacity) when the stop lands and ends `cancelled`, never admitted after
    // the failure. The zero-cost `contingency` (any-failed, ordered after `bad`)
    // still fires. `keeper`'s own terminal is not asserted.
    let costed = || NodePolicy::new().working_memory(10);
    let mut flow = Flow::new();
    let _bad = flow.register_source_with("bad", &Fails, NodePolicy::new());
    let _keeper = flow.register_source_with("keeper", &Succeeds, costed());
    let _later = flow.register_source_with("later", &Succeeds, costed());
    let _n = flow.register_source_with_trigger(
        "contingency",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AnyFailed,
    );
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "bad".into(),
        SourceRunner::boxed("bad", Fails, slot_for::<u64>("bad", 0)),
    );
    runners.insert(
        "keeper".into(),
        SourceRunner::boxed(
            "keeper",
            HoldsMemoryUntilCancelled,
            slot_for::<u64>("keeper", 0),
        ),
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
        &config(FailureMode::StopOnFirstFailure).capacities(PoolCapacities::new().memory(10)),
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
    assert!(
        !has_event(
            &parse_events(&sink.bytes()),
            "attempt-started",
            Some("later")
        ),
        "no default-rule non-teardown node is admitted after the first failure"
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "contingency").as_deref(),
        Some("succeeded"),
        "a firing contingency still runs under stop mode"
    );
    assert!(
        has_event(
            &parse_events(&sink.bytes()),
            "attempt-started",
            Some("contingency")
        ),
        "the contingency truly executed under stop mode"
    );
    assert_eq!(report.outcome, RunOutcome::Failed);
}

// ===========================================================================
// The exactly-one-terminal-state invariant across a mixed graph.
// ===========================================================================

/// A mixed graph exercising success, failure, propagated failure, propagated skip,
/// and a firing all-terminal cleanup: every node — including those that never ran —
/// appears with exactly one terminal state, and none appears twice. (C15 def-of-done:
/// exactly one terminal state.)
#[test]
fn every_node_has_exactly_one_terminal_state() {
    let mut flow = Flow::new();
    let f = flow.register_source("f", &Fails); // fails
    let _fd = flow.register::<PassThrough, _>("fd", &PassThrough, f); // upstream-failed
    let s = flow.register_source("s", &Skips); // skips
    let _sd = flow.register::<PassThrough, _>("sd", &PassThrough, s); // upstream-skipped
    let _ok = flow.register_source("ok", &Succeeds); // succeeds
    let _cleanup = flow.register_source_with_trigger(
        "cleanup",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AllTerminal,
    ); // fires after f
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let f_slot = slot_for::<u64>("f", 1);
    let s_slot = slot_for::<u64>("s", 1);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "f".into(),
        SourceRunner::boxed("f", Fails, Arc::clone(&f_slot)),
    );
    runners.insert(
        "fd".into(),
        MapRunner::boxed(
            "fd",
            PassThrough,
            f_slot.shared_ref(),
            slot_for::<u64>("fd", 0),
        ),
    );
    runners.insert(
        "s".into(),
        SourceRunner::boxed("s", Skips, Arc::clone(&s_slot)),
    );
    runners.insert(
        "sd".into(),
        MapRunner::boxed(
            "sd",
            PassThrough,
            s_slot.shared_ref(),
            slot_for::<u64>("sd", 0),
        ),
    );
    runners.insert(
        "ok".into(),
        SourceRunner::boxed("ok", Succeeds, slot_for::<u64>("ok", 0)),
    );
    runners.insert(
        "cleanup".into(),
        SourceRunner::boxed("cleanup", Succeeds, slot_for::<u64>("cleanup", 0)),
    );

    let sink = MemorySink::default();
    let report = drive(
        &config(FailureMode::ContinueIndependent),
        "mixed",
        Ok(RunPlan::with_ordering(
            pipeline,
            runners,
            order(&[("cleanup", &["f"])]),
        )),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    let expected = [
        ("f", "failed"),
        ("fd", "upstream-failed"),
        ("s", "skipped"),
        ("sd", "upstream-skipped"),
        ("ok", "succeeded"),
        ("cleanup", "succeeded"),
    ];
    for (node, state) in expected {
        assert_eq!(
            terminal_of(&sink.bytes(), node).as_deref(),
            Some(state),
            "{node} ends {state}"
        );
        assert_eq!(
            terminal_count(&sink.bytes(), node),
            1,
            "{node} has exactly one terminal state"
        );
        assert!(
            report.terminal_states.contains_key(node),
            "{node} appears in the report"
        );
    }
    assert_eq!(report.outcome, RunOutcome::Failed);
}
