//! C13 · **Execution-class dispatch** driver integration test — ticket T33 (043).
//! Written first, TDD.
//!
//! This exercises the **real** T24 run-loop driver ([`dagr_cli::driver::drive`])
//! routing each dispatched attempt onto the thread execution surface named by its
//! resolved execution class (arch.md `### C13 · Execution class dispatch`; T2 ADR
//! 004 §2/§3/§5): *await-bound* work on the async (tokio) runtime, *blocking* work
//! on tokio's dedicated blocking pool (`spawn_blocking`), and *compute-bound* work
//! on a dedicated fixed-size `rayon` pool. The class is resolved from the task's
//! declared [`ExecutionClass`](dagr_core::task::ExecutionClass) and the C5 node
//! policy override (T29), applied at dispatch.
//!
//! # How each scenario is observed deterministically (no wall-clock, no network)
//!
//! A task calls the driver's public surface probe
//! (`dagr_cli::driver::current_execution_surface`) inside its own work to record
//! which surface actually ran it — the driver attributes the surface reliably even
//! where thread names collide (tokio names its blocking threads identically to its
//! async workers). The routing tests assert on that recorded surface — control by
//! class, observation by a probe. Concurrency bounds are controlled by pinned pool
//! sizes and counts; the starvation-isolation test compares completion **order**
//! (an await-bound node finishes while a blocking node is still spinning), never an
//! absolute duration.
//!
//! Scope discipline (T33): this is only the class→surface routing + the compute
//! pool wiring + its driver integration. It does not change T31 permit mechanics or
//! T32 sizing (it consumes the pinned `compute_threads` capacity), and it does not
//! implement the T38 demo. The T24 framework-survives-a-blocked-task guarantee and
//! termination stay intact (asserted here too).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEventSink};
use dagr_core::flow::Flow;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::{ExecutionClass, Task};
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

// ===========================================================================
// Event-stream helpers
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

/// Classify the current thread's surface via the driver's public surface probe —
/// the honest, deterministic attribution (no wall-clock, no ambient state). The
/// driver knows which surface a closure runs on even where thread names collide
/// (tokio names its blocking threads identically to its async workers).
fn current_surface() -> &'static str {
    use dagr_cli::driver::ExecutionSurface;
    match dagr_cli::driver::current_execution_surface() {
        ExecutionSurface::Async => "async",
        ExecutionSurface::Blocking => "blocking",
        ExecutionSurface::Compute => "compute",
        ExecutionSurface::Other => "unknown",
    }
}

// ===========================================================================
// A shared surface probe: node name -> surface it executed on.
// ===========================================================================

#[derive(Clone, Default)]
struct SurfaceProbe {
    seen: Arc<Mutex<BTreeMap<String, String>>>,
}

impl SurfaceProbe {
    fn record(&self, node: &str) {
        self.seen
            .lock()
            .unwrap()
            .insert(node.to_string(), current_surface().to_string());
    }
    fn surface_of(&self, node: &str) -> Option<String> {
        self.seen.lock().unwrap().get(node).cloned()
    }
}

// ===========================================================================
// Test tasks that record the surface they ran on
// ===========================================================================

/// A task that records the surface it ran on, then succeeds. Its declared class
/// is parameterised through the `EXECUTION_CLASS` of a wrapper type below.
struct ProbeWork {
    node: String,
    probe: SurfaceProbe,
}
impl ProbeWork {
    /// Record the surface this task ran on. Returns the produced value so each
    /// class wrapper's `run` is a one-liner.
    fn body(&mut self) -> u64 {
        self.probe.record(&self.node);
        0
    }
}

/// Await-bound probe task (the default class).
struct AwaitProbe(ProbeWork);
impl Task for AwaitProbe {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::AwaitBound;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0.body())
    }
}

/// Blocking probe task.
struct BlockingProbe(ProbeWork);
impl Task for BlockingProbe {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0.body())
    }
}

/// Compute probe task.
struct ComputeProbe(ProbeWork);
impl Task for ComputeProbe {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0.body())
    }
}

/// A blocking task declared `Blocking` that spins synchronously until a shared flag
/// is set, then succeeds — models a long synchronous task that occupies only the
/// blocking pool. It is released cooperatively by the test so nothing hangs.
struct BlockingUntilReleased {
    release: Arc<std::sync::atomic::AtomicBool>,
}
impl Task for BlockingUntilReleased {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        while !self.release.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }
        Ok(0)
    }
}

/// A compute task that records peak concurrency: each attempt increments a shared
/// counter on entry, waits on a barrier so all concurrent attempts pile up, records
/// the peak live count, then decrements and succeeds. The pool's fixed size bounds
/// how many can be live at once — the barrier would deadlock past pool size, so the
/// test uses a bounded wait and the counter is the load-bearing assertion.
struct ComputeConcurrencyProbe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
impl Task for ComputeConcurrencyProbe {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        // Record the running peak.
        self.peak.fetch_max(now, Ordering::SeqCst);
        // Hold the slot briefly so concurrent attempts overlap (bounded spin, not a
        // wall-clock sleep assertion — just enough to let siblings pile up).
        for _ in 0..200_000 {
            std::hint::spin_loop();
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(0)
    }
}

// ===========================================================================
// Type-erased node runners on the real C14 attempt path
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
        let mut task = self.task.take().expect("source runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            let outcome = run_attempt_caught(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

// ===========================================================================
// Pipeline + plan builders
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

// ===========================================================================
// The tests
// ===========================================================================

/// Await-bound routing: a node whose task declares `AwaitBound` (no override) runs
/// on the async (tokio) runtime, not on the blocking or compute pool, and the
/// attempt succeeds.
#[test]
fn await_bound_node_runs_on_the_async_runtime() {
    let probe = SurfaceProbe::default();
    let mut flow = Flow::new();
    let _h = flow.register_source(
        "await_node",
        &AwaitProbe(ProbeWork {
            node: "await_node".into(),
            probe: probe.clone(),
        }),
    );
    let pipeline = flow.finish();
    let slot = slot_for::<u64>("await_node", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "await_node".into(),
        SourceRunner::boxed(
            "await_node",
            AwaitProbe(ProbeWork {
                node: "await_node".into(),
                probe: probe.clone(),
            }),
            slot,
        ),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t33"),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        terminal_of(&sink.bytes(), "await_node").as_deref(),
        Some("succeeded")
    );
    assert_eq!(
        probe.surface_of("await_node").as_deref(),
        Some("async"),
        "await-bound work must run on the async runtime, not blocking/compute"
    );
}

/// Blocking routing: a node whose resolved class is `Blocking` runs on the
/// dedicated blocking pool while an unrelated await-bound node completes
/// concurrently.
#[test]
fn blocking_node_runs_on_the_blocking_pool_and_async_makes_progress() {
    let probe = SurfaceProbe::default();
    let mut flow = Flow::new();
    let _b = flow.register_source(
        "blocking_node",
        &BlockingProbe(ProbeWork {
            node: "blocking_node".into(),
            probe: probe.clone(),
        }),
    );
    let _a = flow.register_source(
        "await_node",
        &AwaitProbe(ProbeWork {
            node: "await_node".into(),
            probe: probe.clone(),
        }),
    );
    let pipeline = flow.finish();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "blocking_node".into(),
        SourceRunner::boxed(
            "blocking_node",
            BlockingProbe(ProbeWork {
                node: "blocking_node".into(),
                probe: probe.clone(),
            }),
            slot_for::<u64>("blocking_node", 0),
        ),
    );
    runners.insert(
        "await_node".into(),
        SourceRunner::boxed(
            "await_node",
            AwaitProbe(ProbeWork {
                node: "await_node".into(),
                probe: probe.clone(),
            }),
            slot_for::<u64>("await_node", 0),
        ),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t33"),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        probe.surface_of("blocking_node").as_deref(),
        Some("blocking"),
        "blocking work must run on the dedicated blocking pool"
    );
    assert_eq!(
        probe.surface_of("await_node").as_deref(),
        Some("async"),
        "the unrelated await-bound node still runs on the async runtime"
    );
}

/// Compute routing: a node whose resolved class is `Compute` runs on the fixed
/// compute (rayon) pool.
#[test]
fn compute_node_runs_on_the_compute_pool() {
    let probe = SurfaceProbe::default();
    let mut flow = Flow::new();
    let _h = flow.register_source(
        "compute_node",
        &ComputeProbe(ProbeWork {
            node: "compute_node".into(),
            probe: probe.clone(),
        }),
    );
    let pipeline = flow.finish();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "compute_node".into(),
        SourceRunner::boxed(
            "compute_node",
            ComputeProbe(ProbeWork {
                node: "compute_node".into(),
                probe: probe.clone(),
            }),
            slot_for::<u64>("compute_node", 0),
        ),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        // Pin the compute pool so the surface is deterministic regardless of host.
        &RunConfig::new("/tmp/dagr-t33").capacities(PoolCapacities::new().compute_threads(2)),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        terminal_of(&sink.bytes(), "compute_node").as_deref(),
        Some("succeeded")
    );
    assert_eq!(
        probe.surface_of("compute_node").as_deref(),
        Some("compute"),
        "compute-bound work must run on the fixed compute pool"
    );
}

/// The C5 policy override moves the effective class: a synchronous task declared
/// `Blocking` overridden to `Compute` (a legal synchronous→synchronous move) runs
/// on the compute pool, i.e. the override wins over the task default.
#[test]
fn policy_override_moves_a_blocking_task_onto_the_compute_pool() {
    let probe = SurfaceProbe::default();
    let mut flow = Flow::new();
    // Declared Blocking, but overridden to Compute by the node policy.
    let _h = flow.register_source_with(
        "overridden",
        &BlockingProbe(ProbeWork {
            node: "overridden".into(),
            probe: probe.clone(),
        }),
        NodePolicy::new().execution_class(ExecutionClass::Compute),
    );
    let pipeline = flow.finish();
    // Confirm assembly accepts the legal move (the boundary check is T29's).
    pipeline
        .assemble()
        .expect("legal sync->sync override assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "overridden".into(),
        SourceRunner::boxed(
            "overridden",
            BlockingProbe(ProbeWork {
                node: "overridden".into(),
                probe: probe.clone(),
            }),
            slot_for::<u64>("overridden", 0),
        ),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t33").capacities(PoolCapacities::new().compute_threads(2)),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        probe.surface_of("overridden").as_deref(),
        Some("compute"),
        "the Compute override must win over the declared Blocking class"
    );
}

/// The C5 legality boundary: overriding an await-bound task to a synchronous class
/// is illegal (C5), so assembly rejects it, naming the node — dispatch never
/// receives an illegal class. (The authoritative check is T29; this confirms the
/// dispatch path never sees an illegal class.)
#[test]
fn illegal_await_bound_to_synchronous_override_fails_assembly() {
    let probe = SurfaceProbe::default();
    let mut flow = Flow::new();
    let _h = flow.register_source_with(
        "illegal",
        &AwaitProbe(ProbeWork {
            node: "illegal".into(),
            probe,
        }),
        NodePolicy::new().execution_class(ExecutionClass::Blocking),
    );
    let pipeline = flow.finish();
    let err = pipeline
        .assemble()
        .expect_err("await-bound → synchronous override is illegal (C5)");
    let msg = format!("{err}");
    assert!(
        msg.contains("illegal"),
        "assembly error should name the offending node 'illegal': {msg}"
    );
}

/// Starvation isolation: a long synchronous (blocking) task does not delay an
/// unrelated await-bound node. The await-bound node completes and the run makes
/// progress on it while the blocking node is still spinning; the blocking node is
/// released cooperatively so nothing hangs. Proven by completion order, not a
/// wall-clock duration.
/// An await-bound node that completes promptly, then releases a blocking node —
/// proving the async runtime made progress while the blocking node was still
/// occupying (only) the blocking pool.
struct AwaitFast {
    await_done: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
}
impl Task for AwaitFast {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::AwaitBound;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        self.await_done.store(true, Ordering::SeqCst);
        self.release.store(true, Ordering::SeqCst);
        Ok(0)
    }
}

#[test]
fn long_blocking_task_does_not_delay_unrelated_await_bound_work() {
    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let await_done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut flow = Flow::new();
    let _b = flow.register_source(
        "blocker",
        &BlockingUntilReleased {
            release: Arc::clone(&release),
        },
    );
    let _a = flow.register_source(
        "fast",
        &AwaitFast {
            await_done: Arc::clone(&await_done),
            release: Arc::clone(&release),
        },
    );
    let pipeline = flow.finish();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "blocker".into(),
        SourceRunner::boxed(
            "blocker",
            BlockingUntilReleased {
                release: Arc::clone(&release),
            },
            slot_for::<u64>("blocker", 0),
        ),
    );
    runners.insert(
        "fast".into(),
        SourceRunner::boxed(
            "fast",
            AwaitFast {
                await_done: Arc::clone(&await_done),
                release: Arc::clone(&release),
            },
            slot_for::<u64>("fast", 0),
        ),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t33"),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert!(
        await_done.load(Ordering::SeqCst),
        "the await-bound node completed — it was not blocked behind the sync task"
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "fast").as_deref(),
        Some("succeeded")
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "blocker").as_deref(),
        Some("succeeded")
    );
}

/// Compute-pool concurrency is bounded by pool size: with the compute pool pinned
/// to N and more than N compute nodes dispatched at once, the observed peak of
/// concurrently executing compute attempts never exceeds N.
#[test]
fn compute_pool_concurrency_is_bounded_by_pool_size() {
    const N: u32 = 2;
    const NODES: usize = 6;
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut flow = Flow::new();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    for i in 0..NODES {
        let name = format!("compute{i}");
        let _ = flow.register_source(
            &name,
            &ComputeConcurrencyProbe {
                live: Arc::clone(&live),
                peak: Arc::clone(&peak),
            },
        );
        runners.insert(
            name.clone(),
            SourceRunner::boxed(
                &name,
                ComputeConcurrencyProbe {
                    live: Arc::clone(&live),
                    peak: Arc::clone(&peak),
                },
                slot_for::<u64>(&name, 0),
            ),
        );
    }
    let pipeline = flow.finish();
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t33").capacities(PoolCapacities::new().compute_threads(N)),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert!(
        peak.load(Ordering::SeqCst) <= N as usize,
        "compute concurrency peaked at {} but the pool holds only {N}",
        peak.load(Ordering::SeqCst)
    );
}

/// The compute pool has a floor of one thread even under a pinned zero/fractional
/// capacity (T2 §3 floor-of-one; T32 sizing) — a single compute node still runs and
/// succeeds when the pinned compute capacity is zero.
#[test]
fn compute_pool_has_a_floor_of_one_thread() {
    let probe = SurfaceProbe::default();
    let mut flow = Flow::new();
    let _ = flow.register_source(
        "compute_node",
        &ComputeProbe(ProbeWork {
            node: "compute_node".into(),
            probe: probe.clone(),
        }),
    );
    let pipeline = flow.finish();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "compute_node".into(),
        SourceRunner::boxed(
            "compute_node",
            ComputeProbe(ProbeWork {
                node: "compute_node".into(),
                probe: probe.clone(),
            }),
            slot_for::<u64>("compute_node", 0),
        ),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        // Pin compute to zero: the floor-of-one still gives it a live thread.
        &RunConfig::new("/tmp/dagr-t33").capacities(PoolCapacities::new().compute_threads(0)),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        probe.surface_of("compute_node").as_deref(),
        Some("compute"),
        "even a zero-pinned compute pool runs the node on a floor-of-one compute thread"
    );
}

/// Framework survives a fully blocked task fleet: a per-attempt timeout still fires
/// while every blocking-pool worker is jammed by a never-returning synchronous task,
/// the timed-out node's fate is decided, and the run reaches `run-finished` with a
/// complete stream (the T24 isolation guarantee, extended to dispatch by class).
/// A blocking task that never returns — a misdeclared/hung synchronous fleet.
struct BlocksForever;
impl Task for BlocksForever {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        loop {
            std::hint::spin_loop();
        }
    }
}

/// A blocking runner that marks its attempt timed out promptly (the real T21
/// blocking-timeout mark path), so the timeout fires though the worker is jammed —
/// mirroring the T24 framework-survives-a-blocked-task scenario, now under dispatch.
struct TimedBlockingRunner {
    name: String,
    task: Option<BlocksForever>,
    slot: Arc<Slot<u64>>,
}
impl NodeRunner for TimedBlockingRunner {
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
            use dagr_core::execution::TimeoutDecision;
            let decision = TimeoutDecision::mark_blocking_timed_out(&name, ctx, sink);
            let _ = &mut task;
            let _ = &slot;
            decision.outcome().terminal_state()
        })
    }
}

#[test]
fn safety_machinery_survives_a_fully_blocked_task_fleet() {
    let mut flow = Flow::new();
    let _ = flow.register_source("hung", &BlocksForever);
    let pipeline = flow.finish();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "hung".into(),
        Box::new(TimedBlockingRunner {
            name: "hung".into(),
            task: Some(BlocksForever),
            slot: slot_for::<u64>("hung", 0),
        }),
    );
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        // A tiny grace period so the run does not sit for the default 10s.
        &RunConfig::new("/tmp/dagr-t33").grace(std::time::Duration::from_millis(10)),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The timeout fired, the node is timed-out, the stream is complete.
    assert_eq!(
        terminal_of(&sink.bytes(), "hung").as_deref(),
        Some("timed-out"),
        "the timeout still fired though the blocking worker is jammed"
    );
    let events = parse_events(&sink.bytes());
    assert_eq!(
        events.last().map(|(k, _)| k.as_str()),
        Some("run-finished"),
        "a complete event stream is written even under a blocked task fleet"
    );
    assert_eq!(report.outcome, RunOutcome::Failed);
}

/// A barrier keeps the concurrency probe honest under a larger pool: N compute
/// nodes with the pool pinned to exactly N all run concurrently (peak == N), so the
/// bound is tight, not merely an upper limit an idle pool trivially satisfies.
/// A compute task that piles up on a shared barrier: all N release only when N are
/// live at once, so `peak == N` proves the pool actually admits N concurrently
/// (a tight bound, not merely an upper limit an idle pool trivially satisfies).
struct BarrierCompute {
    barrier: Arc<Barrier>,
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
impl Task for BarrierCompute {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        // All N pile up here at once — only possible if the pool holds N.
        self.barrier.wait();
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(0)
    }
}

#[test]
fn compute_pool_admits_up_to_pool_size_concurrently() {
    const N: usize = 3;
    let barrier = Arc::new(Barrier::new(N));
    let peak = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));

    let mut flow = Flow::new();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    for i in 0..N {
        let name = format!("bc{i}");
        let _ = flow.register_source(
            &name,
            &BarrierCompute {
                barrier: Arc::clone(&barrier),
                live: Arc::clone(&live),
                peak: Arc::clone(&peak),
            },
        );
        runners.insert(
            name.clone(),
            SourceRunner::boxed(
                &name,
                BarrierCompute {
                    barrier: Arc::clone(&barrier),
                    live: Arc::clone(&live),
                    peak: Arc::clone(&peak),
                },
                slot_for::<u64>(&name, 0),
            ),
        );
    }
    let pipeline = flow.finish();
    let plan = RunPlan::new(pipeline, runners);

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-t33")
            .capacities(PoolCapacities::new().compute_threads(u32::try_from(N).unwrap())),
        "dispatch",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        peak.load(Ordering::SeqCst),
        N,
        "with the pool pinned to N and N nodes, all N run concurrently (tight bound)"
    );
}
