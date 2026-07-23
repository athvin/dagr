//! C12 admission-controller **driver integration** test — ticket T31 (041).
//! Written first, TDD.
//!
//! This exercises the **real** T24 run-loop driver ([`dagr_cli::driver::drive`])
//! with a **pinned** C12 memory pool, proving the admission point is wired into
//! the loop: a node whose declared cost does not fit the pool's remaining capacity
//! **waits** until a running node releases its permit, then is admitted — the
//! ledger returns to full at run end (no leak). Admission is controlled by
//! **counts** (a pinned pool + declared costs), never sleeps, so it is
//! deterministic in CI.
//!
//! The termination + event semantics T24/T25 own are unchanged: the run still ends
//! precisely when nothing is pending and nothing is in flight, and every node
//! still reaches its terminal state. The only new behaviour is *when* a node is
//! admitted — gated on capacity.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// In-memory sink + clock (the C19 injection seam)
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

// ===========================================================================
// A source task that succeeds.
// ===========================================================================

struct SucceedsWith(u64);
impl Task for SucceedsWith {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0)
    }
}

struct SourceRunner {
    name: String,
    task: Option<SucceedsWith>,
    slot: Arc<Slot<u64>>,
}

impl SourceRunner {
    fn boxed(name: &str, value: u64, slot: Arc<Slot<u64>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(SucceedsWith(value)),
            slot,
        })
    }
}

impl NodeRunner for SourceRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("source runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            let outcome = run_attempt_caught(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

fn slot_for(name: &str) -> Arc<Slot<u64>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

/// Two independent (zero-dependency) source nodes, each declaring `mem` bytes of
/// working memory, both succeeding.
fn two_source_plan(mem: u64) -> (Pipeline, RunPlan) {
    let mut flow = Flow::new();
    let policy = NodePolicy::new().working_memory(mem);
    let _a = flow.register_source_with("alpha", &SucceedsWith(1), policy);
    let _b = flow.register_source_with("beta", &SucceedsWith(2), policy);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "alpha".into(),
        SourceRunner::boxed("alpha", 1, slot_for("alpha")),
    );
    runners.insert(
        "beta".into(),
        SourceRunner::boxed("beta", 2, slot_for("beta")),
    );
    let plan = RunPlan::new(pipeline.clone(), runners);
    (pipeline, plan)
}

/// One zero-dependency source node declaring `over` bytes of working memory, plus
/// one declaring `ok` bytes — for the can-never-fit guard: with a pool pinned
/// below `over` but at/above `ok`, the first node can NEVER be admitted (its demand
/// exceeds the pool's total capacity) while the second admits normally.
fn over_demand_plan(over: u64, ok: u64) -> (Pipeline, RunPlan) {
    let mut flow = Flow::new();
    let _big = flow.register_source_with(
        "toobig",
        &SucceedsWith(1),
        NodePolicy::new().working_memory(over),
    );
    let _small = flow.register_source_with(
        "fits",
        &SucceedsWith(2),
        NodePolicy::new().working_memory(ok),
    );
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "toobig".into(),
        SourceRunner::boxed("toobig", 1, slot_for("toobig")),
    );
    runners.insert(
        "fits".into(),
        SourceRunner::boxed("fits", 2, slot_for("fits")),
    );
    let plan = RunPlan::new(pipeline.clone(), runners);
    (pipeline, plan)
}

// ===========================================================================
// The tests
// ===========================================================================

/// With a memory pool pinned to admit only **one** of the two same-cost source
/// nodes at a time, the driver still runs both to success and terminates — the
/// second node **waits** for the first's permit to release, then is admitted.
/// The run's termination and event semantics are unchanged; only *when* the second
/// node is admitted is gated on capacity.
#[test]
fn a_pinned_pool_admits_one_node_at_a_time_and_the_run_still_completes() {
    // Each node declares 600 bytes; the pool holds 1000 → only one fits at a time.
    let (_pipeline, plan) = two_source_plan(600);
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-admission").capacities(PoolCapacities::new().memory(1_000)),
        "admission-demo",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The run completes successfully — the second node was admitted after the
    // first released its permit, never wedged at admission (no deadlock, no leak).
    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        report.terminal_states.get("alpha").copied(),
        Some(TerminalState::Succeeded)
    );
    assert_eq!(
        report.terminal_states.get("beta").copied(),
        Some(TerminalState::Succeeded)
    );

    // Both nodes appear in the stream, each admitted and reaching terminal.
    let events = parse_events(&sink.bytes());
    for node in ["alpha", "beta"] {
        assert!(
            events
                .iter()
                .any(|(k, n)| k == "node-admitted" && n.as_deref() == Some(node)),
            "node {node} was admitted; got {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|(k, n)| k == "node-terminal" && n.as_deref() == Some(node)),
            "node {node} reached terminal; got {events:?}"
        );
    }
}

/// With an **unconstrained** pool (the default), admission gates nothing: both
/// zero-dependency nodes are admitted at once, exactly as the M1 driver behaved
/// before T31 — the integration is behaviour-preserving unless a pool is pinned.
#[test]
fn an_unconstrained_pool_admits_every_ready_node_at_once() {
    let (_pipeline, plan) = two_source_plan(1_000_000);
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-admission"), // default: unconstrained pools
        "admission-demo",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        report.terminal_states.get("alpha").copied(),
        Some(TerminalState::Succeeded)
    );
    assert_eq!(
        report.terminal_states.get("beta").copied(),
        Some(TerminalState::Succeeded)
    );
}

/// A node whose declared cost EXCEEDS a pool's **total** capacity can never be
/// admitted — no release could ever free enough capacity. Left in the pending
/// queue it would strand forever: when nothing else is in flight the run loop
/// exits, the node never reaches a terminal state, and the run is (wrongly)
/// reported as complete — a silent violation of "every reachable node reaches a
/// terminal state". The run must NOT exit silently with a stranded node.
///
/// **Superseded by T32 (042).** When T31 shipped, the *full* bootstrap-time
/// rejection of too-big nodes was deferred, so the driver caught this can-never-fit
/// node with a defensive admission-time guard, folding it to a `Failed` terminal
/// *inside* the loop. T32 makes the rejection authoritative and moves it **before**
/// the loop: a too-big node now fails the run at **bootstrap**, before any node
/// executes, with the distinct `bootstrap-failed` outcome (arch.md C12: "fails at
/// bootstrap, not at admission time"). This test therefore asserts the T32
/// behaviour — the run is rejected at bootstrap and nothing runs. The T31 driver
/// guard (`can_ever_fit` / `reject_over_demand`) is retained unchanged as a
/// defensive backstop but is not reached on the default drive path, because the
/// bootstrap check intercepts first. The T32 owner-tests for this path live in
/// `crates/cli/tests/container_limits_driver.rs`.
#[test]
fn an_over_demand_node_is_rejected_at_bootstrap_not_silently_stranded() {
    // Pool holds 1000 bytes total. "toobig" demands 5000 (> total → can never fit);
    // "fits" demands 400. T32's bootstrap check rejects the run before the loop.
    let (_pipeline, plan) = over_demand_plan(5_000, 400);
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-admission").capacities(PoolCapacities::new().memory(1_000)),
        "admission-overdemand",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The run is rejected at bootstrap — distinct from a mid-run Failed outcome and
    // from a silent success. Nothing executed.
    assert_eq!(
        report.outcome,
        RunOutcome::BootstrapFailed,
        "an over-demand node must fail the run at bootstrap (T32), not strand or succeed silently"
    );
    // No node reached a terminal state, because the bootstrap check ran before the
    // loop — neither the too-big node nor the otherwise-fitting one executed.
    assert!(
        report.terminal_states.is_empty(),
        "no node executes when bootstrap rejects the run; got {:?}",
        report.terminal_states
    );

    // The rejection lands in the durable record: run-started then a bootstrap-failed
    // run-finished, with no attempt records — the failure is not silently missing.
    let events = parse_events(&sink.bytes());
    assert!(
        !events.iter().any(|(k, _)| k == "attempt-started"),
        "bootstrap rejection runs before any attempt; got {events:?}"
    );
    assert_eq!(
        events.last().map(|(k, _)| k.as_str()),
        Some("run-finished"),
        "the run terminates with a run-finished record; got {events:?}"
    );
}
