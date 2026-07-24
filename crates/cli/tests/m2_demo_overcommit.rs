//! **M2 demo · part 1 — overcommit completes without exceeding capacity** —
//! ticket T38 (049). Written first, TDD. **This is half of the M2 gate: the
//! spec's "It survives" done-when for the overcommit path, executed in CI.**
//!
//! arch.md's **Build order** states M2 is *done when a pipeline whose combined
//! declared demand exceeds the configured memory capacity completes without
//! exceeding it* (the clean-stop half lives in `m2_demo_clean_stop.rs`). This file
//! is the overcommit proof: a pipeline of parallel-ready nodes whose **combined**
//! declared working-memory cost strictly exceeds a **pinned** memory-pool capacity
//! `M`, while every **single** node's declared cost fits under `M`, driven through
//! the **real** T24 run-loop driver ([`dagr_cli::driver::drive`]) with the memory
//! pool pinned via the T32 [`PoolCapacities`] flag so the ceiling is deterministic
//! on any runner.
//!
//! # What the demo exercises (composes merged components — adds no capability)
//!
//! This is a **feature (demo)** ticket: it composes already-merged components and
//! adds **zero** engine capability. Everything it drives is real:
//!
//! - the **admission controller** (C12 / T31) that turns the memory ceiling into a
//!   throughput limit — the driver admits a ready node only when its declared cost
//!   fits every pool's remaining capacity, so the combined admitted cost never
//!   exceeds `M`;
//! - the **T32 pinning flag** ([`RunConfig::capacities`] → [`PoolCapacities::memory`])
//!   that overrides cgroup/host detection so the ceiling is a fixed demo value,
//!   portable across CI runners (this ticket only *exercises* the flag — the cgroup
//!   v2/v1/host probing itself is T32's and is out of scope here);
//! - the **single-oversized-node bootstrap rejection** (C12 / T32): a node whose
//!   declared cost exceeds `M` fails fast at **bootstrap**, before any node is
//!   admitted, with the `bootstrap-failed` outcome — never wedged at admission.
//!
//! # How "never exceeds `M`" is observed without reaching into the driver
//!
//! The driver **owns** its [`AdmissionController`] internally and does not hand it
//! out, so the demo cannot poll the ledger directly. Instead it observes the
//! *effect* of admission with a **task-side concurrency probe**, the same
//! observable-signal discipline the merged T34/T35 tests use (no wall clock, no
//! sleep): every overcommit task, keyed off a shared [`Concurrency`] meter,
//! increments an admitted-count on entry and decrements it on return, and records
//! the **peak** count concurrently admitted. Because every overcommit node declares
//! the **same** honest per-node cost `PER`, the combined declared cost of the nodes
//! admitted at any instant is exactly `peak_concurrency · PER`, and asserting
//! `peak_concurrency · PER <= M` is exactly the C12 capacity invariant *"the
//! combined declared cost of executing nodes never exceeds pool capacity"* observed
//! end-to-end. A regression that admitted all nodes at once (a broken ceiling)
//! would push `peak_concurrency` to `N` and the product past `M`, which the
//! assertion bites on.
//!
//! # How the ceiling is proven *binding* (not incidentally sufficient)
//!
//! The overcommit is real only if admission actually gated it. With `N` nodes each
//! costing `PER` and the pool pinned to `M`, at most `floor(M / PER)` fit at once;
//! the demo pins `M` so that `floor(M / PER) < N`, so **at least one** node could
//! not be co-admitted with the others and **observably waited** for a permit. The
//! observable proxy for "its recorded permit-wait time is nonzero" is
//! `peak_concurrency < N`: if the peak concurrent admission is strictly below the
//! node count, at least one admission was serialized behind a release — the ceiling
//! is binding, not incidentally sufficient. This is the deterministic, count-based
//! restatement of the ticket's nonzero-permit-wait requirement.
//!
//! # Determinism (pinned ceiling on any runner)
//!
//! Capacity is pinned to fixed demo values via the T32 flag, so the terminal-state
//! picture and the pass/fail verdict are identical regardless of the runner's real
//! cgroup/host memory — the pinning overrides detection. Admission is decided by
//! **counts** (a pinned pool + declared costs), never by sleeps or a wall clock, so
//! the concurrency probe's peak is bounded deterministically. The tasks cooperate
//! through a shared gate so the peak is *reliably* driven up to the pool's real
//! headroom under `--test-threads` variation, without ever exceeding it.
//!
//! # Scope (T38 — integration demo only)
//!
//! Adds **no** framework surface. It does not re-prove the per-outcome permit-release
//! matrix (T37) or the two-concurrent-runs guarantee (T67) — it consumes their
//! invariants. It asserts terminal states, ledger balance (all pools full at end),
//! and the capacity invariant; it renders nothing (artifacts/diagrams are M3, C20–C25).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::admission::{Pool, PoolCapacities, PoolCost};
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{detect_capacities, TaskError};

// ===========================================================================
// Fixed demo knobs (pinned → deterministic on any runner)
// ===========================================================================

/// The declared **working-memory** cost of one overcommit node, in bytes. Every
/// node declares the same honest cost, so the combined admitted demand is exactly
/// `peak_concurrency · PER` and the capacity assertion is a clean product.
const PER: u64 = 300;

/// The number of parallel-ready overcommit nodes. Their **combined** declared cost
/// (`N · PER`) strictly exceeds the pinned pool capacity `M`, while each single
/// node's cost `PER` fits under `M`.
const N: u64 = 5;

/// The **pinned** memory-pool capacity, in bytes (the T32 flag value). Chosen so
/// that:
/// - `N · PER` (= 1500) strictly exceeds `M` — the run is genuinely overcommitted;
/// - `PER` (= 300) fits under `M` — every single node can be admitted;
/// - `floor(M / PER)` (= 3) is strictly below `N` (= 5) — the ceiling is *binding*:
///   at least one node must wait for a permit, so admission actually serializes.
const M: u64 = 900;

/// The most nodes that can be co-admitted under `M`: `floor(M / PER)`. The peak
/// concurrent admission must never exceed this, and (because it is `< N`) the run
/// must serialize at least one admission.
const MAX_COFIT: u64 = M / PER; // = 3

// ===========================================================================
// A capturing in-memory sink + monotonic clock (the C19 injection seam)
// ===========================================================================

/// An in-memory [`EventSink`] — the driver writes its stream here so the demo can
/// walk the **real** event stream it wrote.
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

/// A monotonic clock ticking one nanosecond per read — deterministic offsets with
/// no real clock (no wall clock is consulted anywhere in this demo).
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
// The task-side concurrency probe — the observable oracle for admission
// ===========================================================================

/// A shared **admitted-concurrency meter**: each overcommit task increments `live`
/// when it starts executing (it has just been admitted) and decrements it when it
/// returns (its permit is about to release), recording the **peak** `live` seen.
///
/// The whole overcommit proof rests on this: because every node declares the same
/// cost `PER`, the combined declared cost of the nodes admitted at any instant is
/// `live · PER`, so `peak · PER` is the high-water combined admitted demand — which
/// C12 promises never exceeds `M`. It is a plain integer meter (no wall clock), so
/// the reading is deterministic.
#[derive(Default)]
struct Concurrency {
    live: AtomicU64,
    peak: AtomicU64,
    /// A cooperative gate: tasks spin here until enough have arrived that the pool's
    /// real headroom is co-occupied, so the peak is *reliably* driven up to
    /// `MAX_COFIT` (never beyond) regardless of `--test-threads`. Purely to make the
    /// binding-ceiling observation robust; it never lets `live` exceed the pool.
    arrived: AtomicU64,
}
impl Concurrency {
    fn enter(&self) -> u64 {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        now
    }
    fn leave(&self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
    fn peak(&self) -> u64 {
        self.peak.load(Ordering::SeqCst)
    }
}

/// An overcommit node: declares `PER` bytes of working memory, and while executing
/// it holds a slot on the shared [`Concurrency`] meter so the demo can observe the
/// peak concurrent admission. It cooperatively waits (bounded, no sleep) until the
/// pool's headroom is co-occupied, so the meter's peak reliably reaches the pool's
/// real capacity `MAX_COFIT` — then returns, freeing its permit for the next waiter.
struct OvercommitTask {
    meter: Arc<Concurrency>,
}
impl Task for OvercommitTask {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // We were just admitted — record ourselves live. Because admission gated on
        // `PER` vs `M`, `live` can never exceed `MAX_COFIT` here; the meter's peak
        // is the high-water admitted concurrency.
        let mine = self.meter.enter();
        // Mark arrival and cooperatively wait until the pool's headroom is full
        // (up to `MAX_COFIT` co-admitted) so the peak reliably reaches capacity —
        // but only if we are one of the first `MAX_COFIT` to be admitted. A later
        // waiter (one that had to wait for a release) never blocks here: it returns
        // immediately so it cannot deadlock the drain. Bounded spin, never a sleep.
        self.meter.arrived.fetch_add(1, Ordering::SeqCst);
        if mine <= MAX_COFIT {
            for _ in 0..100_000 {
                if self.meter.live.load(Ordering::SeqCst) >= MAX_COFIT {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        self.meter.leave();
        Ok(1)
    }
}

// ===========================================================================
// A type-erased source runner over the real C14 caught attempt path
// ===========================================================================

struct SourceRunner {
    name: String,
    task: Option<OvercommitTask>,
    slot: Arc<Slot<u64>>,
}
impl SourceRunner {
    fn boxed(name: &str, meter: Arc<Concurrency>, slot: Arc<Slot<u64>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(OvercommitTask { meter }),
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
        let mut task = self.task.take().expect("runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

/// A trivial always-succeeds source with a **too-big** declared cost — used only by
/// the single-oversized-node bootstrap-rejection scenario, where it never executes.
struct Trivial;
impl Task for Trivial {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}
struct TrivialRunner {
    name: String,
    task: Option<Trivial>,
    slot: Arc<Slot<u64>>,
}
impl TrivialRunner {
    fn boxed(name: &str, slot: Arc<Slot<u64>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(Trivial),
            slot,
        })
    }
}
impl NodeRunner for TrivialRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
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

/// A **collision-proof, per-invocation** run-store base under the OS temp dir.
///
/// Determinism (CI fs race): this binary runs concurrently with its sibling
/// `m2_demo_clean_stop` under `--test-threads>1`, and each `drive_*` here spawns the
/// driver's detached next-invocation reclamation sweep over its pipeline directory.
/// This file makes **no** temp-cleanup-timing assertion (it never reads back the temp
/// dir), so a shared fixed `/tmp` base is not itself a flake source here — but giving
/// every drive a private, disjoint base removes all cross-drive contention on a shared
/// pipeline subtree outright, mirroring the `temp_base()` fix in
/// `os_signals_flush_and_cleanup.rs` and `m2_demo_clean_stop.rs`. No production change.
fn temp_base() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "dagr-t38-overcommit-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        unique
    ))
}

// ===========================================================================
// Stream helpers
// ===========================================================================

fn parse_events(bytes: &[u8]) -> Vec<(String, Option<String>)> {
    let stream = read_records(bytes).expect("stream parses");
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

fn terminal_of(bytes: &[u8], node: &str) -> Option<String> {
    let stream = read_records(bytes).expect("stream parses");
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

fn finished_outcome(bytes: &[u8]) -> Option<String> {
    let stream = read_records(bytes).expect("stream parses");
    stream.records.iter().find_map(|rec| {
        if rec.get("event").and_then(|v| v.as_str()) == Some("run-finished") {
            rec.get("body")
                .and_then(|b| b.get("outcome"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        } else {
            None
        }
    })
}

// ===========================================================================
// The overcommit fixture: N same-cost parallel-ready nodes, pool pinned to M
// ===========================================================================

/// The observed outcome of one overcommit drive: the run report, the raw stream
/// bytes, and the peak concurrent admission the task probe recorded.
struct Overcommit {
    report: dagr_cli::driver::RunReport,
    bytes: Vec<u8>,
    peak_concurrency: u64,
}

/// Build `N` independent (zero-dependency) parallel-ready source nodes, each
/// declaring `PER` bytes of working memory, and drive them through the real driver
/// with the memory pool **pinned to `M`** via the T32 flag. Every node shares one
/// [`Concurrency`] meter so the peak concurrent admission is observable.
fn drive_overcommit() -> Overcommit {
    let meter = Arc::new(Concurrency::default());

    let mut flow = Flow::new();
    let policy = NodePolicy::new().working_memory(PER);
    for i in 0..N {
        let _ = flow.register_source_with(format!("job-{i}"), &Trivial, policy);
    }
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("overcommit pipeline assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    for i in 0..N {
        let name = format!("job-{i}");
        runners.insert(
            name.clone(),
            SourceRunner::boxed(&name, Arc::clone(&meter), slot_for(&name)),
        );
    }

    let base = temp_base();
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new(base.to_str().expect("temp base is valid UTF-8"))
            .capacities(PoolCapacities::new().memory(M)),
        "m2-overcommit",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    // Leave no debris under the OS temp dir (the driver's best-effort cleanup already
    // reclaims the run's own `tmp/` subtree; this removes the whole private base).
    let _ = std::fs::remove_dir_all(&base);

    Overcommit {
        report,
        bytes: sink.bytes(),
        peak_concurrency: meter.peak(),
    }
}

// ===========================================================================
// Scenario 1 — overcommit completes; combined admitted cost never exceeds M
// ===========================================================================

/// The overcommit pipeline's **combined** declared cost strictly exceeds the pinned
/// capacity while each single node fits, and it completes with every node
/// `succeeded` — the ceiling became a throughput limit, not a crash (arch.md C12;
/// M2 done-when). And at the observed peak, the combined admitted declared cost
/// never exceeded `M`.
#[test]
fn overcommit_completes_and_combined_admitted_cost_never_exceeds_capacity() {
    // The pipeline is genuinely overcommitted, each single node fits, and the
    // ceiling is binding — the pinned demo constants encode all three (checked at
    // compile time so a bad edit to the constants fails the build, not a run).
    const {
        assert!(
            N * PER > M,
            "combined declared cost must exceed the pinned pool"
        );
        assert!(
            PER <= M,
            "each single node's declared cost must fit under the pool"
        );
        assert!(
            MAX_COFIT < N,
            "the ceiling must be binding (not all nodes co-fit)"
        );
    }

    let run = drive_overcommit();

    // Every node succeeded and the overall outcome is success — the run neither
    // crashed nor OOM'd; admission serialized it instead.
    assert_eq!(
        run.report.outcome,
        RunOutcome::Succeeded,
        "the overcommit run succeeds"
    );
    for i in 0..N {
        let node = format!("job-{i}");
        assert_eq!(
            run.report.terminal_states.get(&node).copied(),
            Some(TerminalState::Succeeded),
            "{node} ends succeeded"
        );
        assert_eq!(
            terminal_of(&run.bytes, &node).as_deref(),
            Some("succeeded"),
            "{node} records a succeeded terminal in the stream"
        );
    }

    // The capacity invariant, observed end-to-end: because every node costs the
    // same `PER`, the combined admitted declared cost at the high-water mark is
    // exactly `peak_concurrency · PER`, and it never exceeded `M`.
    assert!(
        run.peak_concurrency * PER <= M,
        "combined admitted declared cost ({} · {} = {}) must never exceed the pinned capacity {}",
        run.peak_concurrency,
        PER,
        run.peak_concurrency * PER,
        M,
    );
    // And it never exceeded the co-fit ceiling in node count either.
    assert!(
        run.peak_concurrency <= MAX_COFIT,
        "at most {} nodes may be co-admitted under the pinned pool; peak was {}",
        MAX_COFIT,
        run.peak_concurrency,
    );
}

// ===========================================================================
// Scenario 2 — the ceiling is genuinely binding (admission serialized)
// ===========================================================================

/// Capacity is genuinely binding, not incidentally sufficient: at least one node
/// could not be co-admitted with the others and observably waited for a permit
/// (arch.md C12). The deterministic, count-based proxy for "recorded permit-wait is
/// nonzero" is `peak_concurrency < N` — the peak admitted concurrency is strictly
/// below the node count, so at least one admission was serialized behind a release.
#[test]
fn capacity_is_binding_at_least_one_admission_serialized() {
    let run = drive_overcommit();
    assert_eq!(run.report.outcome, RunOutcome::Succeeded);

    // The overcommit was real: fewer than all `N` nodes were ever admitted at once,
    // so at least one waited for a permit. A run where everything happened to fit
    // would show `peak_concurrency == N` — the very thing a binding ceiling forbids.
    assert!(
        run.peak_concurrency < N,
        "at least one node must have waited for a permit (peak concurrency {} must be below the \
         node count {}) — proving the overcommit was real and admission gated it",
        run.peak_concurrency,
        N,
    );
    // The cooperative gate drives the peak up to the pool's real headroom, so the
    // binding observation is non-vacuous (the pool genuinely fits `MAX_COFIT` at
    // once, not fewer by accident of scheduling).
    assert_eq!(
        run.peak_concurrency, MAX_COFIT,
        "the peak concurrent admission reaches the pool's real headroom {MAX_COFIT} \
         (binding, non-vacuous)",
    );
}

// ===========================================================================
// Scenario 3 — a single oversized node fails fast at bootstrap, not admission
// ===========================================================================

/// A node whose declared cost exceeds the pinned pool's total capacity `M` fails at
/// **bootstrap**, before any node is admitted, with the `bootstrap-failed` outcome
/// and no attempt records — the fail-fast path, not a wedged admission queue
/// (arch.md C12 / T32). The demo confirms the bootstrap-failure artifact is
/// produced (run-started then a bootstrap-failed run-finished).
#[test]
fn a_single_oversized_node_fails_fast_at_bootstrap() {
    // One node demands `M + 1` (strictly over the pinned pool total → can never
    // fit); a sibling fits normally. The bootstrap check rejects the run before the
    // loop, so neither node executes.
    let mut flow = Flow::new();
    let _big = flow.register_source_with(
        "oversized",
        &Trivial,
        NodePolicy::new().working_memory(M + 1),
    );
    let _ok = flow.register_source_with("fits", &Trivial, NodePolicy::new().working_memory(PER));
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    // The pinned pool the driver bootstraps against.
    let capacities = PoolCapacities::new().memory(M);

    // Reconstruct EXACTLY the `(node, cost)` input the driver feeds its bootstrap
    // capacity check — `PoolCost::from_cost_vector(node.policy().cost())` over every
    // pipeline node — so we can, after the drive confirms `BootstrapFailed`, invoke
    // the SAME production `detect_capacities` the driver calls and assert on the REAL
    // bootstrap-failure artifact it produces (see the DoD #4 assertions below). This
    // is not an invented message: it is the identical function, capacities, and node
    // costs the driver's own bootstrap check ran (crate `dagr-cli` `driver::drive`).
    let node_costs: Vec<(String, PoolCost)> = pipeline
        .nodes()
        .map(|n| {
            (
                n.name().to_string(),
                PoolCost::from_cost_vector(n.policy().cost()),
            )
        })
        .collect();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "oversized".into(),
        TrivialRunner::boxed("oversized", slot_for("oversized")),
    );
    runners.insert(
        "fits".into(),
        TrivialRunner::boxed("fits", slot_for("fits")),
    );

    let base = temp_base();
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new(base.to_str().expect("temp base is valid UTF-8")).capacities(capacities),
        "m2-oversized",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    // Leave no debris under the OS temp dir.
    let _ = std::fs::remove_dir_all(&base);

    // The run failed at bootstrap — distinct from a mid-run failure and from
    // success. Nothing executed.
    assert_eq!(
        report.outcome,
        RunOutcome::BootstrapFailed,
        "a node bigger than the pinned pool must fail the run at bootstrap, not at admission"
    );
    assert!(
        report.terminal_states.is_empty(),
        "no node executes when bootstrap rejects the run; got {:?}",
        report.terminal_states
    );

    let events = parse_events(&sink.bytes());
    // Bootstrap failed before any attempt ran — the fail-fast path.
    assert!(
        !events.iter().any(|(k, _)| k == "attempt-started"),
        "bootstrap rejection runs before any attempt; got {events:?}"
    );
    // The bootstrap-failure artifact is produced: run-started opened the record and
    // a bootstrap-failed run-finished closed it (never a hang).
    assert!(
        events.iter().any(|(k, _)| k == "run-started"),
        "run-started recorded even though bootstrap failed: {events:?}"
    );
    assert_eq!(
        events.last().map(|(k, _)| k.as_str()),
        Some("run-finished"),
        "the run terminates with a run-finished record (no wedge): {events:?}"
    );
    assert_eq!(
        finished_outcome(&sink.bytes()).as_deref(),
        Some("bootstrap-failed"),
        "the wire outcome names bootstrap-failed, distinct from assembly-failed"
    );

    // --- DoD #4 (literal clause): the bootstrap failure NAMES the offending node
    // AND the pool. The node/pool identity does not travel on the C19 stream (the
    // `run-finished` body carries only `{ "outcome": "bootstrap-failed" }`) nor on
    // the `RunReport`; the driver surfaces it by rendering the very
    // `CapacityBootstrapFailure` that `detect_capacities` returns (driver::drive:
    // `eprintln!("{failure}")`). So we assert on the REAL artifact by calling that
    // same production function over the identical capacities + node costs the driver
    // bootstrapped with — the offending run genuinely produced this failure.
    let failure = detect_capacities(&capacities, &node_costs)
        .expect_err("the oversized node must make the production bootstrap check fail");

    // The failure names the offending node AND the pool it overran — structured,
    // non-string proof over the artifact's own accessors (`CapacityError::node` /
    // `::pool`). Non-vacuous: this bites the moment the artifact stops carrying the
    // offending node id or the pool it overran.
    assert!(
        failure
            .errors()
            .iter()
            .any(|e| e.node() == "oversized" && e.pool() == Pool::Memory),
        "the bootstrap-failure artifact names the offending node `oversized` and the Memory pool \
         it overran; got {:?}",
        failure.errors(),
    );
    // The `fits` node is under capacity, so the failure must NOT name it — proving
    // the naming is the OFFENDING node specifically, not every node blindly.
    assert!(
        !failure.errors().iter().any(|e| e.node() == "fits"),
        "the under-capacity `fits` node must not appear in the bootstrap failure; got {:?}",
        failure.errors(),
    );

    // The human-facing message the driver renders to stderr (`Display`) literally
    // carries both the offending node's identifier AND the pool's name — the exact
    // "message naming the offending node and pool" DoD #4 requires. Asserting on the
    // rendered text keeps the human-readable surface honest too, and is non-vacuous:
    // drop the node id or the pool from the `Display` impl and this fails.
    let message = failure.to_string();
    assert!(
        message.contains("oversized"),
        "the bootstrap-failure message names the offending node `oversized`: {message:?}"
    );
    assert!(
        message.contains("Memory"),
        "the bootstrap-failure message names the Memory pool: {message:?}"
    );
}

// ===========================================================================
// Scenario 4 — deterministic under the pinned ceiling (repeatable verdict)
// ===========================================================================

/// The overcommit demo produces the same terminal-state picture and the same
/// pass/fail verdict across repetitions, because the T32 pinning flag overrides
/// detection so the ceiling is a fixed value independent of the runner's real
/// cgroup/host memory (arch.md C12; M2 test plan: deterministic on any runner).
#[test]
fn overcommit_is_deterministic_under_the_pinned_ceiling() {
    for _ in 0..3 {
        let run = drive_overcommit();
        assert_eq!(
            run.report.outcome,
            RunOutcome::Succeeded,
            "the verdict is stable across repetitions (pinned ceiling)"
        );
        for i in 0..N {
            assert_eq!(
                run.report.terminal_states.get(&format!("job-{i}")).copied(),
                Some(TerminalState::Succeeded),
                "the terminal-state picture is stable across repetitions"
            );
        }
        assert!(
            run.peak_concurrency * PER <= M,
            "capacity invariant holds every run"
        );
        assert!(run.peak_concurrency < N, "the ceiling is binding every run");
    }
}
