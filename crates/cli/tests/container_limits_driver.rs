//! C12 · **Container-limit bootstrap rejection** driver integration test —
//! ticket T32 (042). Written first, TDD.
//!
//! This exercises the **real** T24 run-loop driver ([`dagr_cli::driver::drive`])
//! on the too-big-node path: a node whose declared cost exceeds a pool's total
//! capacity is rejected **at bootstrap, before any node executes**, and the run
//! records a `bootstrap-failed` outcome — distinct from `assembly-failed`, and
//! distinct from T31's admission-time can-never-fit guard (which is a per-node
//! `Failed` terminal *inside* the loop). Capacity is pinned via the T32 flag so
//! the scenario is deterministic in CI (never reads the real host).
//!
//! The key assertions the ticket names: bootstrap fails **fast** (no
//! attempt-started record exists — nothing ran), the outcome is `bootstrap-failed`
//! on the wire, and the run never hangs.

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
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let node = rec.get("node").and_then(|v| v.as_str()).map(str::to_string);
            (kind, node)
        })
        .collect()
}

/// The `run-finished` record's `outcome` field on the wire, or `None`.
fn finished_outcome(bytes: &[u8]) -> Option<String> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream.records.iter().find_map(|rec| {
        if rec.get("kind").and_then(|v| v.as_str()) == Some("run-finished") {
            rec.get("outcome")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        } else {
            None
        }
    })
}

// ===========================================================================
// A source task and its runner.
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

/// A pipeline with one node whose declared cost exceeds `pin` bytes (so it can
/// never fit) plus one normally-fitting node.
fn too_big_plan(over: u64, ok: u64) -> (Pipeline, RunPlan) {
    let mut flow = Flow::new();
    let _big = flow.register_source_with(
        "hog",
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
    runners.insert("hog".into(), SourceRunner::boxed("hog", 1, slot_for("hog")));
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

/// **too-big node rejected at bootstrap, not admission.** With the memory pool
/// pinned to a small value (via the T32 flag), a node demanding more than the
/// pool's total capacity fails the run at **bootstrap** — before any attempt runs.
/// The run records a `bootstrap-failed` outcome (distinct from `assembly-failed`),
/// no `attempt-started` record exists (nothing executed), and the run terminates
/// (never hangs).
#[test]
fn a_too_big_node_fails_the_run_at_bootstrap_before_any_node_executes() {
    // Pool pinned to 1000 bytes; "hog" demands 5000 (> total → too big).
    let (_pipeline, plan) = too_big_plan(5_000, 400);
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t32").capacities(PoolCapacities::new().memory(1_000)),
        "t32-too-big",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The overall outcome is bootstrap-failed — distinct from assembly-failed and
    // from a mere Failed run.
    assert_eq!(report.outcome, RunOutcome::BootstrapFailed);

    let events = parse_events(&sink.bytes());
    // No node executed: there is no attempt-started record for any node.
    assert!(
        !events.iter().any(|(k, _)| k == "attempt-started"),
        "bootstrap fails before any attempt runs; got {events:?}"
    );
    // The stream still opened and closed cleanly: run-started then run-finished.
    assert!(
        events.iter().any(|(k, _)| k == "run-started"),
        "run-started recorded even though bootstrap failed: {events:?}"
    );
    assert_eq!(
        events.last().map(|(k, _)| k.as_str()),
        Some("run-finished"),
        "the run terminated with a run-finished record"
    );
    // The wire spelling is bootstrap-failed, distinct from assembly-failed.
    assert_eq!(
        finished_outcome(&sink.bytes()).as_deref(),
        Some("bootstrap-failed")
    );
}

/// **a run that fits its pinned capacity is not bootstrap-rejected.** With the
/// pool pinned large enough for every node, bootstrap passes and the run proceeds
/// normally to success — the rejection is strictly for too-big nodes.
#[test]
fn a_run_within_pinned_capacity_passes_bootstrap_and_succeeds() {
    let (_pipeline, plan) = too_big_plan(400, 300); // both fit a 1000-byte pool
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t32").capacities(PoolCapacities::new().memory(1_000)),
        "t32-fits",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        report.terminal_states.get("hog").copied(),
        Some(TerminalState::Succeeded)
    );
    assert_eq!(
        report.terminal_states.get("fits").copied(),
        Some(TerminalState::Succeeded)
    );
}
