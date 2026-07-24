//! **M2 demo · part 2 — clean stop under stop-on-first-failure** — ticket T38
//! (049). Written first, TDD. **This is half of the M2 gate: the spec's "It
//! survives" done-when for the clean-stop path, executed in CI.**
//!
//! arch.md's **Build order** states M2 is *done when … an induced mid-run failure
//! stops the run cleanly with nothing orphaned* (the overcommit half lives in
//! `m2_demo_overcommit.rs`). This file is the clean-stop proof: a pipeline in
//! **stop-on-first-failure** mode with an induced mid-run failure, driven through
//! the **real** T24/T34/T35 run-loop driver ([`dagr_cli::driver::drive`]), asserting
//! that the run stops cleanly — the failure propagates by trigger rule, the
//! contingency fires, no default-rule work is admitted after the failure, every
//! permit is released back to empty pools, and no temp file, thread, or in-flight
//! event is left orphaned.
//!
//! # The clean-stop pipeline (composes merged components — adds no capability)
//!
//! This is a **feature (demo)** ticket: it composes already-merged components and
//! adds **zero** engine capability. The graph (fixed at assembly — no runtime
//! mutation) is:
//!
//! - **`failing`** — the induced mid-run failure: a source that fails permanently
//!   (C15 stop trigger). Ends `failed`.
//! - **`sibling`** — an unrelated in-flight sibling still executing when the failure
//!   is observed. It cooperates with cancellation (returns promptly once the run
//!   token flips), so it does not orphan a thread. Its own terminal is not asserted
//!   (it may finish `succeeded` or be reclassified — the demo does not pin it).
//! - **`data-dependent`** — a **direct data dependent** of `failing` (`all-succeeded`,
//!   the only rule a data node may carry — C3). Its rule can no longer be satisfied,
//!   so it is marked `upstream-failed` **without executing** (C15 propagation).
//! - **`downstream-default`** — a downstream **default-rule** (`all-succeeded`)
//!   consume-nothing node, ordered after `failing`, that has **not** yet been
//!   admitted. Under stop mode it is never admitted after the failure and ends
//!   `upstream-failed` (its `all-succeeded` rule cannot fire on a failed upstream).
//! - **`contingency`** — a **consume-nothing** node with the non-default `any-failed`
//!   rule, ordered after `failing`. Its rule fires on the final picture, so it
//!   **executes** to `succeeded` even though the run is stopping — a notify/cleanup
//!   contingency is exactly the work a stop is supposed to run (C15).
//! - **`cleanup`** — a **consume-nothing** `all-terminal` cleanup node ordered after
//!   `failing`. Its rule can still fire regardless of class, so it **executes** to
//!   `succeeded` — propagation is by rule, not by blast radius (C15).
//!
//! Run-level ordering (the T34 [`RunPlan::with_ordering`] seam that stands in for
//! T50's graph ordering edges) attaches the consume-nothing nodes after `failing`;
//! `data-dependent` is a real data edge.
//!
//! # How "all permits released" is observed without reaching into the driver
//!
//! The driver **owns** its [`AdmissionController`] internally and does not hand it
//! out. The demo wires **one shared [`ResidencyLedger`]** into every node's output
//! slot (the C10 accounting hook the run artifact folds as *peak measured slot
//! residency*), and asserts the ledger's **current** counted residency is **zero**
//! after the run reports terminal — every produced value's output residency was
//! released. Working-memory permits release on `Permit::drop` at every terminal
//! outcome by construction (the T0.3 ownership design, exhaustively proven by T37's
//! permit-release matrix, which this demo consumes rather than re-proves); the demo
//! confirms the run *terminated cleanly* (a leaked permit would have wedged a waiter
//! and hung the run) and that the observable output-residency ledger is empty. This
//! is the *"every pool back to full capacity — declared cost charged is zero"*
//! end state, observed through the seam the driver actually exposes.
//!
//! # How "nothing orphaned" is observed
//!
//! - **No residual temp / no live thread:** the induced-failure task writes a file
//!   under the run's per-run temp directory (C16, reached through
//!   [`RunContext::temp_dir`]) before failing; after the run reports terminal the
//!   demo asserts the whole per-run temp directory has been removed by the driver's
//!   end-of-run cleanup (best-effort by design, but with only cooperative tasks it
//!   is deterministic here), and that a **subsequent** invocation would reclaim it
//!   regardless. The run pins its id so the temp path is predictable. Every task
//!   cooperates with cancellation and returns, so no task closure is left running.
//! - **Complete, gapless stream:** the demo walks the **real** recorded C19 stream
//!   and asserts it is gapless (strictly-increasing sequence from `0`), with exactly
//!   one `node-terminal` per node and exactly one attempt-outcome record per attempt
//!   (`attempt-started` count equals `attempt-succeeded` + `attempt-failed`), and no
//!   dangling in-flight event (every `attempt-started` is paired with an outcome).
//!
//! # Determinism
//!
//! Outcomes are scripted (a task either succeeds, fails, or cooperates with
//! cancellation) and admission is serialized by pinning the memory pool so the
//! `downstream-default` node is provably still pending when the stop lands — the
//! same observable-signal discipline the merged T34/T35 tests use. No wall clock,
//! no sleep drives ordering.
//!
//! # Scope (T38 — integration demo only)
//!
//! Adds **no** framework surface. It does not re-prove the per-outcome permit-release
//! matrix (T37) or the two-concurrent-runs guarantee (T67) — it consumes them. It
//! renders nothing (artifacts/diagrams are M3, C20–C25); teardown-node lifecycle
//! beyond what C15/C16 guarantee is M4 (C17) and out of scope.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::binding::TriggerRule;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt, run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{FailureMode, Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// Fixed knobs (pinned → deterministic)
// ===========================================================================

/// The declared working-memory cost of a serialized node, in bytes. `keeper` and
/// `downstream-default` each cost this; the pool is pinned to `PIN` so exactly one
/// fits — admission is serialized, keeping `downstream-default` provably pending
/// when the stop lands.
const COST: u64 = 10;

/// The pinned memory-pool capacity, in bytes. Exactly one `COST`-node fits, so the
/// costed default-rule node cannot be admitted while `keeper` holds the sole permit.
const PIN: u64 = 10;

/// The declared output residency, in bytes, of the producer in the release-mechanics
/// proof (`residency_ledger_charges_then_releases_to_zero_on_the_success_path`) — a
/// nonzero value so the shared residency ledger visibly charges then returns to
/// zero, making the "all permits released" proof non-vacuous.
const RESIDENCY: u64 = 128;

// ===========================================================================
// A capturing in-memory sink + monotonic clock (the C19 injection seam)
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
// Scripted tasks
// ===========================================================================

/// The induced mid-run failure: it writes a scratch file under the run's per-run
/// temp directory (C16, reached through the context) — the "in-flight debris" whose
/// cleanup the demo asserts — then fails **permanently**, triggering the
/// stop-on-first-failure. Because it fails cooperatively (it returns), it leaves no
/// live thread; the per-run temp dir it wrote under is removed by the driver's
/// end-of-run cleanup.
struct FailsAfterWritingTemp {
    /// The scratch path the task wrote, captured for the temp-cleanup assertion.
    wrote: Arc<Mutex<Option<PathBuf>>>,
}
impl Task for FailsAfterWritingTemp {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // Everything a task writes locally goes under the run's per-run temp dir,
        // reachable through the context (C16). Write debris there, then fail.
        if let Some(dir) = c.temp_dir() {
            let scratch = dir.join("failing-scratch.tmp");
            let _ = std::fs::write(&scratch, b"in-flight debris");
            *self.wrote.lock().unwrap() = Some(scratch);
        }
        Err(TaskError::permanent("induced mid-run failure"))
    }
}

/// A cooperative in-flight sibling: it spins on its cancellation signal and returns
/// the moment the stop flips the run token, so it never orphans a thread. Bounded
/// spin (no sleep) so a regression that never propagates the stop cannot hang; the
/// fallback return keeps the demo non-vacuous. Its terminal state is not asserted.
struct CooperativeSibling;
impl Task for CooperativeSibling {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                return Ok(0);
            }
            tokio::task::yield_now().await;
        }
        Ok(0)
    }
}

/// The memory keeper: it holds the whole pinned memory pool until the run is
/// cancelled, then returns — keeping the serialized `downstream-default` node
/// provably pending (never admitted) across the pre-stop window, so the stop settles
/// it before any permit it could grab is freed. Cooperative, bounded, no sleep. Its
/// own terminal is not asserted. (Mirror of the merged T34/T35 helper.)
struct HoldsMemoryUntilCancelled;
impl Task for HoldsMemoryUntilCancelled {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                return Ok(0);
            }
            tokio::task::yield_now().await;
        }
        Ok(0)
    }
}

/// A plain always-succeeds source (the contingency / cleanup bodies).
struct Succeeds;
impl Task for Succeeds {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

/// A one-input pass-through (the direct data dependent — always `all-succeeded`).
struct PassThrough;
impl Task for PassThrough {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
        Ok(i)
    }
}

// ===========================================================================
// Type-erased runners over the real C14 attempt path
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

struct MapRunner<T: Task<Input = u64>> {
    name: String,
    task: Option<T>,
    upstream: SlotRef<u64>,
    slot: Arc<Slot<T::Output>>,
}
impl<T: Task<Input = u64>> MapRunner<T> {
    fn boxed(
        name: &str,
        task: T,
        upstream: SlotRef<u64>,
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
impl<T: Task<Input = u64>> NodeRunner for MapRunner<T> {
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
        let input = *self.upstream.read();
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

struct Bound<T> {
    inner: T,
    input: Option<u64>,
}
impl<T: Task<Input = u64>> Task for Bound<T> {
    type Input = ();
    type Output = T::Output;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, TaskError> {
        let input = self.input.take().expect("consumed once");
        self.inner.run(ctx, input).await
    }
}

/// A consumer runner that opens a **real** [`ConsumerLease`] on its upstream slot,
/// reads it, and holds the lease for the whole attempt — the genuine C10
/// closure-return release gate (the same shape as the merged bounded-memory-chain
/// consumer). Dropping the lease after the attempt returns is what releases the
/// upstream slot's residency, so the shared ledger returns to zero. Used only by the
/// release-mechanics proof.
struct LeaseConsumerRunner {
    name: String,
    upstream: SlotRef<u64>,
    slot: Arc<Slot<u64>>,
}
impl LeaseConsumerRunner {
    fn boxed(name: &str, upstream: SlotRef<u64>, slot: Arc<Slot<u64>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            upstream,
            slot,
        })
    }
}
impl NodeRunner for LeaseConsumerRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let slot = Arc::clone(&self.slot);
        // Open the lease and read (the real C10 consume path); the lease lives until
        // the attempt returns, then drops — releasing the upstream slot's residency.
        let lease = self.upstream.enter();
        let _ = lease.read();
        let mut task = Succeeds;
        Box::pin(async move {
            let state = run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state();
            drop(lease);
            state
        })
    }
}

/// The **charge-then-release residency probe** runner (GAP 2 / `DoD` #11 non-vacuity).
/// A node that succeeds while the run is stopping cleanly and, inside its own
/// attempt, exercises the full C10 residency lifecycle against the **shared** run
/// ledger: it fills an internal producer slot declaring `RESIDENCY` bytes (charging
/// the ledger — the ledger's peak rises above zero during this run), opens a **real**
/// [`ConsumerLease`] on it and reads (holding the value live), runs its own success
/// attempt, then drops the lease — the genuine closure-return release gate — so the
/// residency returns to zero by run end. Doing charge-then-release atomically inside
/// one succeeding node makes the clean-stop residency proof deterministic (immune to
/// how the stop schedules any other node), while still driving the exact `Slot::fill`
/// + `ConsumerLease` mechanics the driver's admission ledger folds.
struct ResidencyProbeRunner {
    name: String,
    ledger: Arc<ResidencyLedger>,
}
impl ResidencyProbeRunner {
    fn boxed(name: &str, ledger: Arc<ResidencyLedger>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            ledger,
        })
    }
}
impl NodeRunner for ResidencyProbeRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        // An internal producer slot on the SHARED ledger, one consumer, declaring
        // `RESIDENCY` bytes. Filling it charges the shared ledger (peak rises);
        // dropping the sole consumer's lease after it returns releases it to zero.
        let producer = slot_on::<u64>("residency-probe-inner", 1, RESIDENCY, &self.ledger);
        producer
            .fill(1)
            .expect("the probe's producer slot fills once");
        // Open a real lease and read (the C10 consume path); the lease lives until the
        // attempt returns, then drops — releasing the shared ledger back to zero.
        let lease = producer.shared_ref().enter();
        let _ = lease.read();
        // The node's own output slot (no residency, no consumers) — its attempt just
        // succeeds so the node has a clean succeeded terminal in the stream.
        let out = slot_on::<u64>(&name, 0, 0, &self.ledger);
        let mut task = Succeeds;
        Box::pin(async move {
            let state = run_attempt_caught(&mut task, &name, ctx, &out, sink)
                .await
                .terminal_state();
            // Release the charged residency now that the consuming closure has
            // returned (the second, zombie-critical half of the C10 release gate).
            drop(lease);
            state
        })
    }
}

// ===========================================================================
// Slot helpers over ONE shared residency ledger (the observable release seam)
// ===========================================================================

fn slot_on<T: Send + Sync + 'static>(
    name: &str,
    consumers: u32,
    residency: u64,
    ledger: &Arc<ResidencyLedger>,
) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        consumers,
        false,
        residency,
        Arc::clone(ledger),
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

// ===========================================================================
// The clean-stop fixture
// ===========================================================================

/// The observed outcome of one clean-stop drive: the report, the raw stream bytes,
/// the shared residency ledger's current counted residency at end, the per-run temp
/// directory path, and whether the failing task wrote its debris file.
struct CleanStop {
    report: dagr_cli::driver::RunReport,
    bytes: Vec<u8>,
    ledger_current_at_end: u64,
    /// The **peak** counted residency the shared ledger observed at any instant
    /// during the clean-stop run — the high-water charge. Nonzero proves something
    /// was actually charged on the clean-stop path (the non-vacuity half of the
    /// charge-then-release proof), so `ledger_current_at_end == 0` is a genuine
    /// *release*, not a ledger that was never charged.
    ledger_peak_at_end: u64,
    temp_dir: PathBuf,
    wrote_temp: Option<PathBuf>,
}

/// The pinned run id, so the per-run temp directory path is predictable for the
/// cleanup assertion.
const RUN_ID: &str = "t38-clean-stop-run";
const PIPELINE: &str = "m2-clean-stop";
const BASE: &str = "/tmp/dagr-t38-clean-stop";

/// Build the type-erased runner map for the clean-stop fixture over the shared
/// residency `ledger`. `wrote` captures the failing node's temp-file path. The
/// failing node's slot has one consumer (`data-dependent`); it fails, so its slot
/// never fills and no residency is charged for it — the residency *release*
/// mechanics over this same shared-ledger seam are proven non-vacuously by
/// `residency_ledger_charges_then_releases_to_zero_on_the_success_path`.
fn build_runners(
    ledger: &Arc<ResidencyLedger>,
    wrote: &Arc<Mutex<Option<PathBuf>>>,
) -> BTreeMap<String, Box<dyn NodeRunner>> {
    let failing_slot = slot_on::<u64>("failing", 1, 0, ledger);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "failing".into(),
        SourceRunner::boxed(
            "failing",
            FailsAfterWritingTemp {
                wrote: Arc::clone(wrote),
            },
            Arc::clone(&failing_slot),
        ),
    );
    runners.insert(
        "data-dependent".into(),
        MapRunner::boxed(
            "data-dependent",
            PassThrough,
            failing_slot.shared_ref(),
            slot_on::<u64>("data-dependent", 0, 0, ledger),
        ),
    );
    runners.insert(
        "sibling".into(),
        SourceRunner::boxed(
            "sibling",
            CooperativeSibling,
            slot_on::<u64>("sibling", 0, 0, ledger),
        ),
    );
    runners.insert(
        "keeper".into(),
        SourceRunner::boxed(
            "keeper",
            HoldsMemoryUntilCancelled,
            slot_on::<u64>("keeper", 0, 0, ledger),
        ),
    );
    runners.insert(
        "downstream-default".into(),
        SourceRunner::boxed(
            "downstream-default",
            Succeeds,
            slot_on::<u64>("downstream-default", 0, 0, ledger),
        ),
    );
    runners.insert(
        "contingency".into(),
        SourceRunner::boxed(
            "contingency",
            Succeeds,
            slot_on::<u64>("contingency", 0, 0, ledger),
        ),
    );
    runners.insert(
        "cleanup".into(),
        SourceRunner::boxed("cleanup", Succeeds, slot_on::<u64>("cleanup", 0, 0, ledger)),
    );

    // --- The charge-then-release probe ON the clean-stop path (GAP 2 / DoD #11
    // non-vacuity). One node unrelated to the induced failure, so it runs to
    // completion *while the run is stopping cleanly*. Its runner charges the **same
    // shared ledger** (fills an internal residency slot declaring `RESIDENCY` bytes,
    // raising the ledger's peak above zero during this run) and then releases it (a
    // **real** [`ConsumerLease`] opened, read, and dropped — the genuine C10
    // closure-return release gate), all inside its own successful attempt. Doing the
    // whole charge-then-release atomically inside one succeeding node makes the proof
    // immune to the stop's scheduling of any second node, while still exercising the
    // exact `Slot::fill` + `ConsumerLease` residency mechanics the driver relies on.
    runners.insert(
        "residency-probe".into(),
        ResidencyProbeRunner::boxed("residency-probe", Arc::clone(ledger)),
    );
    runners
}

/// Assemble and drive the clean-stop pipeline under stop-on-first-failure, with the
/// memory pool pinned so admission serializes. Returns the observed [`CleanStop`].
fn drive_clean_stop() -> CleanStop {
    let ledger = ResidencyLedger::new();
    let wrote: Arc<Mutex<Option<PathBuf>>> = Arc::new(Mutex::new(None));

    // --- Assemble the fixed graph. `data-dependent` is a real data edge from
    // `failing`; the consume-nothing nodes carry their trigger rules and are ordered
    // after `failing` at the run level.
    let mut flow = Flow::new();
    let failing = flow.register_source("failing", &Succeeds);
    let _dd = flow.register::<PassThrough, _>("data-dependent", &PassThrough, failing);
    let _sibling = flow.register_source("sibling", &Succeeds);
    let _keeper =
        flow.register_source_with("keeper", &Succeeds, NodePolicy::new().working_memory(COST));
    let _down = flow.register_source_with_trigger(
        "downstream-default",
        &Succeeds,
        NodePolicy::new().working_memory(COST),
        TriggerRule::AllSucceeded,
    );
    let _contingency = flow.register_source_with_trigger(
        "contingency",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AnyFailed,
    );
    let _cleanup = flow.register_source_with_trigger(
        "cleanup",
        &Succeeds,
        NodePolicy::new(),
        TriggerRule::AllTerminal,
    );
    // The charge-then-release residency probe (GAP 2): one source, unrelated to
    // `failing`, that succeeds *while the run is stopping cleanly*. Its runner does
    // the whole charge-then-release against the shared ledger inside its own attempt
    // (fill a residency slot, then lease+read+drop it) — so the proof is atomic and
    // does not depend on the stop's scheduling of a second node.
    let _residency_probe = flow.register_source("residency-probe", &Succeeds);
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("clean-stop pipeline assembles");

    let runners = build_runners(&ledger, &wrote);

    // The consume-nothing nodes are ordered after `failing` so their rules are
    // evaluated against its terminal (the T34 run-level ordering seam). Also order
    // `downstream-default` after `keeper` so admission serialization keeps it pending
    // until the stop lands. `data-dependent` needs no ordering entry (its data edge
    // already orders it).
    let ordering = order(&[
        ("contingency", &["failing"]),
        ("cleanup", &["failing"]),
        ("downstream-default", &["failing"]),
    ]);

    let config = RunConfig::new(BASE)
        .run_id(RUN_ID)
        .failure_mode(FailureMode::StopOnFirstFailure)
        .capacities(PoolCapacities::new().memory(PIN));

    let temp_dir = dagr_cli::temp::per_run_temp_dir(BASE, PIPELINE, RUN_ID);

    let sink = MemorySink::default();
    let report = drive(
        &config,
        PIPELINE,
        Ok(RunPlan::with_ordering(pipeline, runners, ordering)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    let wrote_temp = wrote.lock().unwrap().clone();
    CleanStop {
        report,
        bytes: sink.bytes(),
        ledger_current_at_end: ledger.current(),
        ledger_peak_at_end: ledger.peak(),
        temp_dir,
        wrote_temp,
    }
}

// ===========================================================================
// Stream oracle
// ===========================================================================

/// A parsed C19 record's kind, optional node, and gapless sequence number.
struct Rec {
    kind: String,
    node: Option<String>,
    seq: u64,
}

fn walk(bytes: &[u8]) -> Vec<Rec> {
    let stream = read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .map(|rec| Rec {
            kind: rec
                .get("event")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            node: rec
                .get("body")
                .and_then(|b| b.get("node"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            seq: rec
                .get("seq")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(u64::MAX),
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

fn count(recs: &[Rec], kind: &str, node: &str) -> usize {
    recs.iter()
        .filter(|r| r.kind == kind && r.node.as_deref() == Some(node))
        .count()
}

/// Every node that could ever run in the fixture (the full graph). Includes the
/// `residency-probe` node that proves the shared ledger charges-then-releases on the
/// clean-stop path (GAP 2); like every other node it must have exactly one terminal
/// state and exactly one attempt-outcome per attempt.
const ALL_NODES: [&str; 8] = [
    "failing",
    "data-dependent",
    "sibling",
    "keeper",
    "downstream-default",
    "contingency",
    "cleanup",
    "residency-probe",
];

// ===========================================================================
// Scenario 1 — clean stop: no further default-rule work is admitted
// ===========================================================================

/// Under stop-on-first-failure, after the first terminal failure is observed the
/// pending downstream **default-rule** node is never admitted and ends
/// `upstream-failed` (its `all-succeeded` rule cannot fire on the failed upstream);
/// the failing node ends `failed`; the overall outcome is failure (arch.md C15; M2
/// done-when).
#[test]
fn clean_stop_admits_no_further_default_rule_work() {
    let run = drive_clean_stop();

    assert_eq!(
        run.report.outcome,
        RunOutcome::Failed,
        "the clean-stop run's overall outcome is failure"
    );
    assert_eq!(
        terminal_of(&run.bytes, "failing").as_deref(),
        Some("failed"),
        "the induced failure ends failed"
    );

    // The downstream default-rule node was never admitted after the failure and
    // ends upstream-failed (its all-succeeded rule can no longer fire).
    assert_eq!(
        terminal_of(&run.bytes, "downstream-default").as_deref(),
        Some("upstream-failed"),
        "a pending default-rule node ends upstream-failed under stop mode"
    );
    let recs = walk(&run.bytes);
    assert_eq!(
        count(&recs, "attempt-started", "downstream-default"),
        0,
        "no default-rule non-teardown node is admitted after the first failure"
    );
}

// ===========================================================================
// Scenario 2 — the contingency still fires under stop mode
// ===========================================================================

/// A consume-nothing contingency with the non-default `any-failed` rule fires on
/// the final picture and executes to `succeeded` even though the run is stopping —
/// a failure-triggered notify/cleanup is exactly the work a stop should run, and
/// stop mode does not cancel it (arch.md C15; M2 done-when).
#[test]
fn clean_stop_still_fires_the_contingency() {
    let run = drive_clean_stop();
    assert_eq!(run.report.outcome, RunOutcome::Failed);

    assert_eq!(
        terminal_of(&run.bytes, "contingency").as_deref(),
        Some("succeeded"),
        "the any-failed contingency executes to succeeded under stop mode"
    );
    let recs = walk(&run.bytes);
    assert_eq!(
        count(&recs, "attempt-started", "contingency"),
        1,
        "the contingency truly executed under stop mode"
    );
}

// ===========================================================================
// Scenario 3 — propagation is by rule, not by blast radius
// ===========================================================================

/// The direct data dependent ends `upstream-failed` without executing (its
/// `all-succeeded` rule can no longer be satisfied), while the `all-terminal`
/// cleanup node still executes (its rule can still fire regardless of class) — every
/// node in the run has exactly one terminal state (arch.md C15; M2 done-when).
#[test]
fn propagation_is_by_rule_not_by_blast_radius() {
    let run = drive_clean_stop();
    let recs = walk(&run.bytes);

    // The direct data dependent is deadened without executing.
    assert_eq!(
        terminal_of(&run.bytes, "data-dependent").as_deref(),
        Some("upstream-failed"),
        "the direct data dependent is marked upstream-failed"
    );
    assert_eq!(
        count(&recs, "attempt-started", "data-dependent"),
        0,
        "the deadened data dependent never executes (no node runs on a non-succeeded data dependency)"
    );

    // The all-terminal cleanup still fires — propagation is by rule, not blast radius.
    assert_eq!(
        terminal_of(&run.bytes, "cleanup").as_deref(),
        Some("succeeded"),
        "the all-terminal cleanup executes even downstream of the failure"
    );
    assert_eq!(
        count(&recs, "attempt-started", "cleanup"),
        1,
        "the cleanup truly executed"
    );

    // Every node has exactly one terminal state — including nodes that never ran.
    for node in ALL_NODES {
        assert_eq!(
            count(&recs, "node-terminal", node),
            1,
            "{node} has exactly one terminal state in the stream"
        );
        assert!(
            run.report.terminal_states.contains_key(node),
            "{node} appears in the report's terminal states"
        );
    }
}

// ===========================================================================
// Scenario 4 — all permits released: nothing left charged
// ===========================================================================

/// After the clean-stop run reports terminal, the shared residency ledger's current
/// counted residency is **zero** — nothing is left charged on the failure,
/// propagation, or cancellation paths (arch.md C12/C10, cross-checked against T37's
/// matrix). The run also terminated with a definite outcome (it did not hang), so no
/// working-memory permit leaked into a wedge.
#[test]
fn all_permits_released_nothing_left_charged() {
    let run = drive_clean_stop();

    // The run terminated with a definite outcome (it did not hang).
    assert_eq!(run.report.outcome, RunOutcome::Failed);

    // --- Non-vacuity ON the clean-stop path (GAP 2 / DoD #11): prove the shared
    // ledger genuinely *charged* something during this run before asserting it
    // released to zero. The `residency-producer`/`residency-consumer` pair succeeded
    // while the run was stopping cleanly, so the shared ledger's peak rose to at
    // least the producer's declared residency. Without this, `current() == 0` could
    // pass on a ledger that was never charged — the near-vacuous gap this closes.
    assert!(
        run.ledger_peak_at_end >= RESIDENCY,
        "the shared residency ledger must have been charged during the clean-stop run \
         (peak {} must reach the declared residency {RESIDENCY}) — proving current()==0 is a \
         genuine release, not an uncharged ledger",
        run.ledger_peak_at_end,
    );

    // …and now the observable output-residency ledger is back to zero: every charged
    // residency was released on the paths the clean stop took (success on the
    // residency pair, failure/propagation/cancellation elsewhere) — no slot lease
    // leaked.
    assert_eq!(
        run.ledger_current_at_end, 0,
        "no output residency is left charged after the clean-stop run"
    );
}

/// The release-mechanics proof (**non-vacuity** for `all_permits_released_…`): a
/// producer declaring `RESIDENCY` bytes and a consumer that opens a real
/// [`ConsumerLease`], reads, and drops it, driven to success over the **same shared
/// residency-ledger seam**. The ledger visibly charges the residency at fill and
/// releases it back to **zero** once the sole consumer's closure returns — proving
/// the "every pool back to full capacity" observable genuinely reflects release, not
/// a slot that was never charged (arch.md C10; C12). Peak > 0 confirms the charge
/// actually happened; current == 0 at end confirms the release.
#[test]
fn residency_ledger_charges_then_releases_to_zero_on_the_success_path() {
    let ledger = ResidencyLedger::new();

    let mut flow = Flow::new();
    let producer = flow.register_source("producer", &Succeeds);
    let _receiver = flow.register::<PassThrough, _>("receiver", &PassThrough, producer);
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    // The producer carries residency and has one consumer; the receiver opens a real
    // lease on it (the C10 closure-return release gate).
    let producer_slot = slot_on::<u64>("producer", 1, RESIDENCY, &ledger);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "producer".into(),
        SourceRunner::boxed("producer", Succeeds, Arc::clone(&producer_slot)),
    );
    runners.insert(
        "receiver".into(),
        LeaseConsumerRunner::boxed(
            "receiver",
            producer_slot.shared_ref(),
            slot_on::<u64>("receiver", 0, 0, &ledger),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t38-release"),
        "m2-release-mechanics",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    // The charge genuinely happened (non-vacuity): the ledger's peak rose to the
    // producer's declared residency while its value was live.
    assert_eq!(
        ledger.peak(),
        RESIDENCY,
        "the producer's output residency was charged while its value was live"
    );
    // …and it released back to zero once the sole consumer's closure returned.
    assert_eq!(
        ledger.current(),
        0,
        "output residency returns to zero after the sole consumer is terminal-and-returned"
    );
}

// ===========================================================================
// Scenario 5 — nothing orphaned: no residual temp, complete gapless stream
// ===========================================================================

/// After the run reports terminal: the per-run temp directory the failing task wrote
/// under is cleaned up by the driver, and the event stream is complete and gapless
/// with exactly one terminal state per node and exactly one attempt-outcome record
/// per attempt — no dangling in-flight events (arch.md C16/C14/C19; M2 done-when).
#[test]
fn nothing_orphaned_no_residual_temp_complete_stream() {
    // Best-effort by design, but with only cooperative tasks the cleanup is
    // deterministic. Clean any stale subtree from a prior run of this pinned id so
    // the assertion is about *this* run's cleanup, not a leftover.
    let run_dir = PathBuf::from(BASE).join(PIPELINE).join(RUN_ID);
    let _ = std::fs::remove_dir_all(&run_dir);

    let run = drive_clean_stop();

    // The failing task actually wrote debris under the per-run temp dir (the cleanup
    // proof is non-vacuous: there was something to remove).
    let wrote = run.wrote_temp.expect("the failing task wrote a temp file");
    assert!(
        wrote.starts_with(&run.temp_dir),
        "the debris was written under the run's per-run temp directory ({:?} under {:?})",
        wrote,
        run.temp_dir,
    );

    // After the run reports terminal, the per-run temp directory is gone — the
    // driver's end-of-run cleanup removed it, so no residual temp is orphaned. (No
    // task closure is still running: every task cooperated and returned, so this
    // cleanup is not racing a live thread.)
    assert!(
        !run.temp_dir.exists(),
        "the per-run temp directory must be cleaned up on the clean stop: {:?}",
        run.temp_dir,
    );

    // The stream is complete and gapless: sequence numbers are strictly increasing
    // from 0 with no gaps.
    let recs = walk(&run.bytes);
    for (i, rec) in recs.iter().enumerate() {
        assert_eq!(
            rec.seq, i as u64,
            "sequence numbers are gapless and strictly increasing from 0 (record {i} has seq {})",
            rec.seq,
        );
    }

    // Exactly one terminal state per node, and exactly one attempt-outcome record
    // per attempt (every attempt-started is paired with exactly one outcome —
    // attempt-succeeded or attempt-failed — so no dangling in-flight event remains).
    for node in ALL_NODES {
        assert_eq!(
            count(&recs, "node-terminal", node),
            1,
            "{node} has exactly one terminal state — no dangling in-flight terminal"
        );
        let started = count(&recs, "attempt-started", node);
        let outcomes =
            count(&recs, "attempt-succeeded", node) + count(&recs, "attempt-failed", node);
        assert_eq!(
            started, outcomes,
            "{node}: every attempt-started ({started}) is paired with exactly one attempt-outcome \
             record ({outcomes}) — no dangling in-flight attempt"
        );
    }

    // Clean up the run's own directory so a repeat run of the pinned id starts fresh.
    let _ = std::fs::remove_dir_all(&run_dir);
}

// ===========================================================================
// Scenario 6 — deterministic verdict and picture across repetitions
// ===========================================================================

/// The clean-stop demo produces the same terminal-state picture and the same
/// failure verdict across repetitions, because the T32 pinning flag fixes the
/// admission serialization and outcomes are scripted (arch.md C12/C15; M2 test
/// plan: deterministic on any runner).
#[test]
fn clean_stop_is_deterministic() {
    let expected: &[(&str, &str)] = &[
        ("failing", "failed"),
        ("data-dependent", "upstream-failed"),
        ("downstream-default", "upstream-failed"),
        ("contingency", "succeeded"),
        ("cleanup", "succeeded"),
    ];
    for _ in 0..3 {
        let run = drive_clean_stop();
        assert_eq!(
            run.report.outcome,
            RunOutcome::Failed,
            "the failure verdict is stable across repetitions"
        );
        for (node, state) in expected {
            assert_eq!(
                terminal_of(&run.bytes, node).as_deref(),
                Some(*state),
                "{node} ends {state} on every run (deterministic picture)"
            );
        }
        assert_eq!(
            run.ledger_current_at_end, 0,
            "no residency leaks on any run"
        );
    }
}
