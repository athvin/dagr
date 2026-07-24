//! The **scale benchmark** — the CI-runnable measurement of pure dagr framework
//! overhead across a thousand-node no-op graph (arch.md `## Performance
//! envelope`; ticket T69).
//!
//! # What the budget covers
//!
//! arch.md's Performance envelope fixes a hard budget: *framework overhead per
//! node — scheduling, admission, event writing; everything but the task's own
//! work — is budgeted at **under one millisecond**, held by a CI benchmark that
//! runs a thousand-node no-op graph and fails on regression.* This module is that
//! benchmark's reusable core: it constructs a graph of exactly
//! [`SCALE_NODE_COUNT`] no-op nodes, drives it through the **real** T24 run-loop
//! driver ([`crate::driver::drive`]) — readiness (C11), admission (C12), attempt
//! running (C14), and event-stream writing (C19), no stubbed scheduler — and
//! reports the per-node framework overhead so the caller can threshold it.
//!
//! Because every node's task body does **no** real work (it returns immediately),
//! the wall-clock time of one full run, divided by the node count, is the
//! framework overhead per node — the number the spec budgets. The task's own
//! (near-zero) work is what the budget explicitly *excludes*; the deterministic
//! phase-breakdown check (in the benchmark's tests) confirms each no-op attempt's
//! window is a small bounded constant — no per-node task work inflates it — and
//! that the framework's own per-node readiness/admission/event-writing is recorded,
//! so the measured number is framework overhead and not task work.
//!
//! # The threshold, the margin, and the hard ceiling
//!
//! Two numbers guard the budget:
//!
//! - [`SPEC_CEILING_NS_PER_NODE`] — the spec's hard limit, **one millisecond per
//!   node**. The measured overhead must never exceed it; this is the number the
//!   Performance envelope names.
//! - [`CI_BUDGET_NS_PER_NODE`] — the checked-in CI threshold the build fails
//!   against. It is a **generous** bound set well above the ceiling
//!   (`16 × SPEC_CEILING_NS_PER_NODE`, i.e. 16 ms/node), because a wall-clock
//!   measurement on a **shared CI runner is inherently noisy** — a tight budget
//!   set at the spec ceiling would flap the build on ordinary runner variance and
//!   erode trust in the gate. The generous bound still catches a *regression*: a
//!   genuine overhead blow-up (an O(n²) admission scan, a per-node syscall storm,
//!   an accidental sleep) moves the number by orders of magnitude, far past this
//!   headroom, while normal jitter stays comfortably under it. The deterministic
//!   invariants the benchmark also asserts — exactly [`SCALE_NODE_COUNT`] nodes,
//!   every node in one success terminal state, each no-op attempt window a small
//!   bounded constant, admission capacity pinned not host-discovered — carry the
//!   correctness weight that a wall-clock number cannot on a noisy host.
//!
//! [`over_budget`] is the pure threshold check the benchmark and its failure-path
//! test both drive: it returns `Some(diagnostic)` when a measured per-node
//! overhead exceeds a threshold, with a message naming the measured value, the
//! threshold, and the node count — so a CI failure is diagnosable from the log
//! without re-running locally.
//!
//! # Pinned, deterministic capacity (not host-discovered)
//!
//! The benchmark pins the C12 admission-pool capacities to
//! [`bench_capacities`] via the T32 pinning flag ([`crate::driver::RunConfig::capacities`]),
//! so the measurement is a property of dagr's overhead and **not** of the CI
//! host's discovered cgroup/host limits. Two runs on the same host use identical
//! pinned capacity, so the number is reproducible run to run and machine to
//! machine. The run touches the filesystem only under a **private per-run temp
//! base** ([`run_scale_benchmark`]), never shared `/tmp`, so concurrent test
//! binaries never collide.
//!
//! # Re-baselining
//!
//! When a *legitimate* change moves the measured per-node overhead (a new
//! per-node record field, a deliberate admission-path change), re-baseline by
//! running the benchmark, reading the printed `per-node overhead` line, and — if
//! the new number is genuinely under the [`SPEC_CEILING_NS_PER_NODE`] budget but
//! approaching [`CI_BUDGET_NS_PER_NODE`] — raising the CI budget here with a
//! comment recording why, keeping the hard ceiling assertion at the spec limit.
//! Never raise the ceiling; it is the spec's number.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::TaskError;

use crate::driver::{drive, NodeRunner, RunConfig, RunPlan};

/// The exact node count the benchmark exercises — the thousand-node ceiling the
/// spec's Performance envelope names (arch.md `## Performance envelope`: *"a
/// thousand-node no-op graph"*). Fixed: the benchmark targets the top of the
/// 10–1,000-node envelope, no more and no less.
pub const SCALE_NODE_COUNT: usize = 1_000;

/// The spec's **hard ceiling**: framework overhead per node must stay under **one
/// millisecond** (arch.md `## Performance envelope`). Expressed in nanoseconds so
/// it composes with the [`Instant`]-measured overhead. The measured per-node
/// overhead must never exceed this; it is the number the Performance envelope
/// budgets and is **not** to be raised — it is the spec's limit.
pub const SPEC_CEILING_NS_PER_NODE: u64 = 1_000_000; // 1 ms

/// The checked-in **CI budget** the build fails against — a deliberately
/// **generous** bound (`16 ×` the [`SPEC_CEILING_NS_PER_NODE`], i.e. 16 ms/node).
///
/// A wall-clock benchmark on a shared CI runner is noisy; a threshold pinned at
/// the 1 ms spec ceiling would flap the build on ordinary variance. This headroom
/// margin keeps the gate reliable while still catching a *regression*: a genuine
/// overhead blow-up moves the number by orders of magnitude, far past this bound,
/// while normal jitter stays well under it. The correctness of the run (node
/// count, all-succeeded terminals, negligible task-body time, pinned capacity) is
/// carried by the benchmark's **deterministic** assertions, not by this number.
pub const CI_BUDGET_NS_PER_NODE: u64 = 16 * SPEC_CEILING_NS_PER_NODE; // 16 ms, generous

/// The pinned C12 admission-pool capacities the benchmark drives under (arch.md
/// C12 / T32 pinning flag). Every pool is pinned to a fixed, finite, benchmark-
/// owned value so admission uses this fixed configuration rather than the CI
/// host's discovered cgroup/host limits — the measurement is a property of
/// dagr's overhead, deterministic run to run and machine to machine.
///
/// The no-op nodes declare zero working-memory cost, so a memory pinned large
/// enough to admit them all keeps every ready node admissible immediately (the
/// benchmark measures overhead, not contention); the point of pinning is host-
/// independence, not to create a binding ceiling (that is the M2 overcommit
/// demo's concern, T38).
#[must_use]
pub fn bench_capacities() -> PoolCapacities {
    PoolCapacities::new()
        // A fixed, host-independent memory ceiling comfortably above the graph's
        // total declared demand (every no-op node declares zero cost), so pinning
        // fixes the configuration without gating admission.
        .memory(1 << 40)
        // Fixed thread-pool sizes so the surfaces are not sized from the host's
        // core count — the pinned, deterministic configuration.
        .blocking_threads(64)
        .compute_threads(64)
}

/// Build the benchmark's graph of exactly [`SCALE_NODE_COUNT`] **no-op** source
/// nodes.
///
/// Every node is an independent (zero-dependency) source whose task body does no
/// real work, so the whole graph is admissible immediately and the measured run
/// time is framework overhead — readiness, admission, attempt dispatch, and
/// event-stream writing — not task work. Returns the assembled [`Pipeline`] and
/// the node names in build order.
///
/// # Panics
///
/// Panics if the assembled pipeline does not carry exactly [`SCALE_NODE_COUNT`]
/// nodes — a framework defect, surfaced loudly.
#[must_use]
pub fn build_scale_graph() -> (Pipeline, Vec<String>) {
    let mut flow = Flow::new();
    // Zero declared cost: a no-op body has no honest working-memory demand, and
    // the benchmark measures overhead, not admission contention.
    let policy = NodePolicy::new().working_memory(0);
    let mut names = Vec::with_capacity(SCALE_NODE_COUNT);
    for i in 0..SCALE_NODE_COUNT {
        let name = node_name(i);
        let _ = flow.register_source_with(name.clone(), &NoOp, policy);
        names.push(name);
    }
    let pipeline = flow.finish();
    pipeline
        .assemble()
        .expect("the scale graph assembles (independent no-op sources)");
    assert_eq!(
        pipeline.nodes().count(),
        SCALE_NODE_COUNT,
        "the scale graph must carry exactly {SCALE_NODE_COUNT} nodes"
    );
    (pipeline, names)
}

/// The stable name of the `i`-th benchmark node (zero-padded so sort order is
/// build order — purely cosmetic, node identity is by name).
#[must_use]
fn node_name(i: usize) -> String {
    format!("noop-{i:04}")
}

/// The observed result of one scale-benchmark run: the measured per-node
/// framework overhead, the run report, and the recorded event-stream bytes for
/// the deterministic-invariant checks.
pub struct ScaleBenchResult {
    /// The **total** framework overhead measured by a real [`Instant`] wrapping the
    /// whole [`drive`] call (nanoseconds) — everything but the no-op task bodies.
    pub total_overhead_ns: u128,
    /// The number of nodes the run drove ([`SCALE_NODE_COUNT`]).
    pub node_count: usize,
    /// The overall run outcome the driver surfaced.
    pub outcome: RunOutcome,
    /// Each node's terminal state, keyed by node name.
    pub terminal_states: BTreeMap<String, TerminalState>,
    /// The raw recorded C19 event stream (JSON Lines) the real driver wrote — the
    /// authoritative record the benchmark's tests fold and walk.
    pub stream: Vec<u8>,
    /// The node names in build order.
    pub node_names: Vec<String>,
}

impl ScaleBenchResult {
    /// The **per-node framework overhead** in nanoseconds: total measured overhead
    /// divided by the node count. This is the single machine-readable number the CI
    /// job thresholds against ([`over_budget`]).
    #[must_use]
    pub fn per_node_overhead_ns(&self) -> u64 {
        // node_count is SCALE_NODE_COUNT (>0), so the division is well-defined.
        u64::try_from(self.total_overhead_ns / self.node_count as u128).unwrap_or(u64::MAX)
    }
}

/// Run the scale benchmark once: build the thousand-node no-op graph, drive it
/// through the **real** T24 driver with capacity **pinned** to
/// [`bench_capacities`] under a **private per-run temp base**, measuring the total
/// framework overhead with a real [`Instant`], and return the
/// [`ScaleBenchResult`].
///
/// The monotonic clock injected into the driver is a deterministic tick clock (so
/// the folded per-attempt phase breakdown is reproducible); the **budget**
/// measurement is a separate real wall-clock [`Instant`] wrapping the whole drive,
/// which is the honest framework-overhead number the spec budgets. Determinism:
/// no task reads a wall clock, the graph is fixed, and capacity is pinned, so the
/// terminal-state picture and the folded phase breakdown are identical run to run;
/// only the wall-clock overhead number varies with host load (as any timing must),
/// which is exactly why the CI budget is generous.
#[must_use]
pub fn run_scale_benchmark() -> ScaleBenchResult {
    let (pipeline, node_names) = build_scale_graph();

    // A type-erased no-op runner per node, over the real C14 caught-attempt path.
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    for name in &node_names {
        runners.insert(name.clone(), NoOpRunner::boxed(name));
    }

    let temp = PrivateTempBase::new();
    let config = RunConfig::new(temp.base()).capacities(bench_capacities());
    let sink = MemorySink::default();

    // The honest framework-overhead measurement: a real Instant around the whole
    // drive. The task bodies are no-ops, so this wall-clock time is framework
    // overhead (scheduling, admission, event writing) and not task work.
    let started = Instant::now();
    let report = drive(
        &config,
        "scale-benchmark",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let total_overhead_ns = started.elapsed().as_nanos();

    ScaleBenchResult {
        total_overhead_ns,
        node_count: SCALE_NODE_COUNT,
        outcome: report.outcome,
        terminal_states: report.terminal_states,
        stream: sink.bytes(),
        node_names,
    }
}

/// The pure **budget check** the CI benchmark fails on regression (arch.md
/// `## Performance envelope`; T69).
///
/// Returns [`None`] when `measured_ns_per_node` is at or under `threshold_ns`, and
/// `Some(diagnostic)` when it exceeds it — a message naming the **measured value,
/// the threshold, and the node count**, so a CI failure is diagnosable from the
/// build log without re-running locally. Both the passing benchmark (against
/// [`CI_BUDGET_NS_PER_NODE`] and [`SPEC_CEILING_NS_PER_NODE`]) and its failure-path
/// test (feeding a deliberately-over value) drive this same function, so the
/// "fails on regression" clause is proven real and not vacuous.
#[must_use]
pub fn over_budget(
    measured_ns_per_node: u64,
    threshold_ns: u64,
    node_count: usize,
) -> Option<String> {
    if measured_ns_per_node <= threshold_ns {
        return None;
    }
    Some(format!(
        "scale benchmark REGRESSION: per-node framework overhead {measured_ns_per_node} ns/node \
         exceeds the {threshold_ns} ns/node budget across {node_count} nodes \
         (spec ceiling {SPEC_CEILING_NS_PER_NODE} ns/node)"
    ))
}

// ===========================================================================
// The no-op task + type-erased runner (real C14 caught-attempt path)
// ===========================================================================

/// A **no-op** source task: it does no real work and returns immediately, so the
/// measured run time is framework overhead and not task work. It reads no wall
/// clock and holds no state, so the benchmark is deterministic.
struct NoOp;
impl Task for NoOp {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // No real work — the point of the benchmark is to measure the framework's
        // overhead around this near-instant body, not the body itself.
        Ok(0)
    }
}

/// The type-erased [`NodeRunner`] the benchmark hands the driver for each no-op
/// node, driving the [`NoOp`] task through the **real** C14 caught-attempt path so
/// the emitted C14/C19 records — and therefore the overhead they cost — are
/// genuine, not stubbed.
struct NoOpRunner {
    name: String,
    task: Option<NoOp>,
    slot: Arc<Slot<u64>>,
}
impl NoOpRunner {
    fn boxed(name: &str) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(NoOp),
            slot: Arc::new(Slot::new(
                NodeId::from_name(name),
                name,
                0,
                false,
                0,
                ResidencyLedger::new(),
            )),
        })
    }
}
impl NodeRunner for NoOpRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("a node runs exactly once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

// ===========================================================================
// Deterministic injection seam: in-memory sink + tick clock + private temp base
// ===========================================================================

/// An in-memory [`EventSink`] capturing every appended line, so the benchmark
/// folds and walks the **real** event stream the driver wrote (matching the
/// production run path's injected run-store sink).
#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().expect("sink mutex not poisoned").clone()
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .expect("sink mutex not poisoned")
            .extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock ticking one nanosecond per read — strictly increasing offsets
/// with **no wall clock**, so the folded per-attempt phase durations are
/// deterministic. (The budget's wall-clock overhead is measured separately by a
/// real [`Instant`]; this clock only drives the deterministic phase breakdown.)
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// A **private per-run temp base** for a benchmark run (arch.md C16; the
/// shared-`/tmp` flake class the CI reliability notes call out). Created fresh
/// under the process temp dir with a per-run unique suffix, and removed when the
/// run's captures are collected — so two benchmark runs, even concurrent ones,
/// never collide on the run store.
struct PrivateTempBase {
    path: std::path::PathBuf,
}
impl PrivateTempBase {
    fn new() -> Self {
        // Per-run unique: pid + a process-monotonic counter (no wall clock).
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("dagr-scale-benchmark-{}-{n}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        Self { path: dir }
    }
    fn base(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}
impl Drop for PrivateTempBase {
    fn drop(&mut self) {
        // Best-effort cleanup — a racing detached temp-reclaim thread may hold a
        // handle; the process exits promptly rather than blocking on it.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The threshold check's failure path — the "fails on regression" clause,
    // unit-tested at the library level so the pure logic is proven independent of a
    // full run. (The integration benchmark exercises the passing path end to end.)

    #[test]
    fn over_budget_is_none_under_and_at_the_threshold() {
        assert!(over_budget(500_000, CI_BUDGET_NS_PER_NODE, SCALE_NODE_COUNT).is_none());
        // Exactly at the threshold is under budget (<=), not a regression.
        assert!(over_budget(
            CI_BUDGET_NS_PER_NODE,
            CI_BUDGET_NS_PER_NODE,
            SCALE_NODE_COUNT
        )
        .is_none());
    }

    #[test]
    fn over_budget_names_value_threshold_and_node_count_when_over() {
        // A deliberately-over per-node value (double the CI budget) must be reported
        // a regression, with a diagnostic naming the measured value, the threshold,
        // and the node count.
        let measured = CI_BUDGET_NS_PER_NODE * 2;
        let diag = over_budget(measured, CI_BUDGET_NS_PER_NODE, SCALE_NODE_COUNT)
            .expect("an over-budget value must be reported a regression");
        assert!(
            diag.contains(&measured.to_string()),
            "names the measured value: {diag}"
        );
        assert!(
            diag.contains(&CI_BUDGET_NS_PER_NODE.to_string()),
            "names the threshold: {diag}"
        );
        assert!(
            diag.contains(&SCALE_NODE_COUNT.to_string()),
            "names the node count: {diag}"
        );
    }

    #[test]
    fn ci_budget_is_generous_relative_to_the_spec_ceiling() {
        // The CI budget must be strictly above the spec ceiling (the headroom margin
        // that keeps a noisy wall-clock gate from flapping), while the ceiling stays
        // the spec's 1 ms/node. Compile-time so a bad edit to either constant fails
        // the build, not a run.
        const {
            assert!(
                CI_BUDGET_NS_PER_NODE > SPEC_CEILING_NS_PER_NODE,
                "the CI budget carries headroom above the spec ceiling"
            );
            assert!(
                SPEC_CEILING_NS_PER_NODE == 1_000_000,
                "the spec ceiling is 1 ms/node"
            );
        }
    }

    #[test]
    fn bench_capacities_are_pinned_finite_not_host_discovered() {
        use dagr_core::admission::Pool;
        let caps = bench_capacities();
        // Every pool is pinned to a finite, benchmark-owned value — not the
        // unconstrained u64::MAX/u32::MAX host-independent default, and not derived
        // from the host.
        assert!(
            caps.total(Pool::Memory) < u64::MAX,
            "memory is pinned finite"
        );
        assert_eq!(caps.total(Pool::BlockingThreads), 64);
        assert_eq!(caps.total(Pool::ComputeThreads), 64);
    }

    #[test]
    fn the_bench_duration_helper_divides_total_by_node_count() {
        let r = ScaleBenchResult {
            total_overhead_ns: 2_000_000, // 2 ms total
            node_count: 1_000,
            outcome: RunOutcome::Succeeded,
            terminal_states: BTreeMap::new(),
            stream: Vec::new(),
            node_names: Vec::new(),
        };
        assert_eq!(r.per_node_overhead_ns(), 2_000); // 2 µs/node
    }
}
