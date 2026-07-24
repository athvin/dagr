//! The M1 **run-loop driver** — the component that orchestrates one complete run
//! from an assembled pipeline to a truthful end (arch.md `### The shape of a
//! run`, `### C11`, `### C14`, `### C19`; ticket T24).
//!
//! # What the driver does
//!
//! The driver is the seam where the M1 pieces become an actual run. It:
//!
//! 1. mints [run identity](RunId) as a `UUIDv7` at bootstrap (operator-overridable
//!    via [`RunConfig::run_id`]) and opens the run store and event stream
//!    **before** assembly executes, so an assembly failure still records itself
//!    (arch.md C19: even an assembly failure has a place to record itself);
//! 2. captures the allowlisted environment values declared at pipeline
//!    construction (empty by default) and emits the `run-started` header carrying
//!    every field known at start (identity, pipeline identity, both fingerprints,
//!    parameters/data interval, captured environment);
//! 3. drives the execution loop: it admits the ready nodes the C11
//!    [`ReadinessTracker`] reports,
//!    **dispatches** each admitted node's attempt through the C14 attempt runner
//!    onto the execution surface named by its **effective execution class** (C13 /
//!    T33 — await-bound on the async task runtime, blocking on the dedicated
//!    blocking pool, compute on the fixed rayon pool), and feeds every terminal
//!    outcome back into the tracker so dependents decrement and either become ready
//!    or receive their propagated terminal state — **never batching a whole level
//!    into a wave**;
//! 4. runs its own machinery (the loop, timers, cancellation fan-out, the
//!    event-stream writer) on the **isolated framework runtime** per the T2 ADR,
//!    kept off every task-execution surface so a misbehaving task cannot disable the
//!    loop, the timeout, or the event stream;
//! 5. terminates **exactly** when nothing is pending and nothing is in flight —
//!    where an abandoned-but-running closure counts as *decided*, not in-flight:
//!    at natural run end it waits a bounded grace period for any zombie closures
//!    to return, emits a `zombie-at-exit` event for each that does not, then emits
//!    `run-finished` and returns;
//! 6. surfaces the run's overall [outcome](RunOutcome) so the caller (the run
//!    verb) can select the exit code — the driver reports the outcome, it does
//!    **not** own the C26 code table.
//!
//! # Execution-class dispatch + the isolated framework runtime (T2 · C13 / T33)
//!
//! Per the T2 async-runtime ADR the framework machinery runs on an **isolated**
//! runtime, separate from every surface task attempts execute on. The driver builds
//! an execution-class `Dispatcher` owning the **three task surfaces** the ADR
//! fixed — the async tokio task runtime (await-bound work), tokio's dedicated
//! blocking pool via `spawn_blocking` (blocking work), and a dedicated fixed-size
//! `rayon` compute pool (compute work) — plus a separate one-worker `framework`
//! runtime that drives the loop, the per-attempt timers, and the event writer.
//! Each admitted node's attempt is **dispatched by its effective execution class**
//! (the C5 policy override if set — validated legal at assembly by T29 — else the
//! task's declared execution class `Task::EXECUTION_CLASS`), so blocking work never
//! starves the async workers and compute concurrency is bounded structurally by the
//! rayon pool's fixed size (C13 acceptance). A task that jams every
//! task/blocking/compute worker (a synchronous busy-loop) still cannot stall the
//! framework runtime — the
//! per-attempt timeout still fires and the event stream is still written (the
//! all-workers-blocked scenario, C13's third acceptance criterion).
//!
//! # The termination condition
//!
//! The run ends **precisely** when the tracker reports nothing pending and the
//! driver holds nothing in flight. A node whose attempt was abandoned-but-running
//! at a blocking timeout is *decided* (its terminal state is fixed) the moment the
//! timeout marks it, so it does not hold the run open; its leftover thread is
//! given at most the [grace period](RunConfig::grace) to return and, if it does
//! not, a `zombie-at-exit` event is emitted for it before `run-finished`. This is
//! the *"nothing pending and nothing in flight"* half of C11's run-end condition
//! (the tracker owns the *"nothing pending"* half).
//!
//! # Scope (M1 only)
//!
//! This is the minimal readiness-driven loop, nothing more. It is **not** a
//! scheduler; it admits the nodes the tracker/runner hand it against the C12
//! admission surface (T31) and dispatches each by its execution class (C13 / T33).
//! Deadlock property tests (T25), the hundred-node scale authority (T26), fault
//! injection (T27), runtime firing of non-default trigger rules and cancellation
//! triggering (T34/T35), the run artifact fold (T42), and resume (C27) all belong
//! to later tickets. This loop only consumes the C16 grace period as the bounded
//! zombie wait at *natural* run end; it triggers no cancellation and handles no
//! signals.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, Event, EventSink, EventStreamWriter, MonotonicClock, RunOutcome,
    RunStartedHeader, TerminalState as WireTerminalState, FINGERPRINT_ALGORITHM_VERSION,
};
pub use dagr_artifact::event_stream::{RunId, RunOutcome as OverallOutcome};
use dagr_core::admission::{AdmissionController, Permit, PoolCapacities, PoolCost};
use dagr_core::assembly::AssemblyError;
use dagr_core::context::{
    CancellationOrigin, CancellationSource, PipelineId, RunContext, RunId as CoreRunId,
    TerminalState,
};
use dagr_core::execution::{AttemptEvent, AttemptEventSink};
use dagr_core::flow::{FailureMode, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::limits::detect_capacities;
use dagr_core::readiness::{Decision, ReadinessTracker};
use dagr_core::task::ExecutionClass;
use tracing::Instrument;

use crate::dispatch::{Dispatcher, Surface};

/// The thread execution **surface** a unit of work ran on — the observable half of
/// C13's class→surface routing (arch.md `### C13`; T33). Await-bound work runs on
/// [`Async`](ExecutionSurface::Async) (the tokio runtime), blocking work on
/// [`Blocking`](ExecutionSurface::Blocking) (the dedicated blocking pool), and
/// compute-bound work on [`Compute`](ExecutionSurface::Compute) (the fixed rayon
/// pool). [`current_execution_surface`] reports the surface the calling code runs
/// on, which is how a task can honestly attribute itself to its class's surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionSurface {
    /// The async (tokio) task runtime — where [`ExecutionClass::AwaitBound`] work
    /// runs.
    Async,
    /// The dedicated blocking pool — where [`ExecutionClass::Blocking`] work runs.
    Blocking,
    /// The fixed compute (rayon) pool — where [`ExecutionClass::Compute`] work runs.
    Compute,
    /// Not one of the three task surfaces (the isolated framework runtime, or a
    /// plain thread).
    Other,
}

/// The execution [surface](ExecutionSurface) the **calling** code is running on
/// (arch.md `### C13`; T33). A task's work can call this to observe which surface
/// its class routed it onto — the honest, deterministic way to prove dispatch
/// placed the work correctly, with no wall-clock or ambient state.
#[must_use]
pub fn current_execution_surface() -> ExecutionSurface {
    match crate::dispatch::current_surface() {
        Surface::Async => ExecutionSurface::Async,
        Surface::Blocking => ExecutionSurface::Blocking,
        Surface::Compute => ExecutionSurface::Compute,
        Surface::Unknown => ExecutionSurface::Other,
    }
}

/// The default bounded grace period the driver waits for a zombie closure to
/// return at natural run end **and** for in-flight cooperative work to return on
/// the cancellation drain (arch.md C16; T35 makes it a flag). A blocking timeout's
/// leftover thread — or an await-bound attempt asked to stop — is given at most
/// this long before it is left behind (`zombie-at-exit` at natural end, or
/// `abandoned` on the cancellation path) and the run proceeds.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(10);

/// The default teardown deadline (arch.md C16 / C17): the wall-clock budget a
/// teardown phase is allowed under its own fresh, uncancelled signal. T35 does not
/// run teardown (that is T52); it only **consumes** this value for the worst-case
/// [shutdown-budget](ShutdownBudget) arithmetic printed at startup.
pub const DEFAULT_TEARDOWN_DEADLINE: Duration = Duration::from_secs(15);

/// The bounded final event-stream flush allowance (arch.md C16): the last, bounded
/// window the process spends flushing the event stream before exit. T35 folds it
/// into the printed [shutdown budget](ShutdownBudget); the fsync/flush mechanics
/// and the unwritable-sink exit code are T36's.
pub const DEFAULT_FINAL_FLUSH: Duration = Duration::from_secs(2);

/// The worst-case **shutdown budget** (arch.md `### C16`): grace + teardown
/// deadline + bounded final flush. The binary prints this at startup so a
/// misconfiguration (a budget that does not fit the orchestrator's kill window —
/// the defaults assume Kubernetes' 30-second `terminationGracePeriodSeconds`) is
/// visible *before it matters*. This is arithmetic, not hope; the [total](Self::total)
/// is the sum of the three components, and [`Display`](std::fmt::Display) renders
/// the arithmetic and the total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownBudget {
    grace: Duration,
    teardown_deadline: Duration,
    final_flush: Duration,
}

impl ShutdownBudget {
    /// The cooperative grace period component (this ticket's flag).
    #[must_use]
    pub fn grace(&self) -> Duration {
        self.grace
    }

    /// The teardown-deadline component (C17; consumed here for the arithmetic).
    #[must_use]
    pub fn teardown_deadline(&self) -> Duration {
        self.teardown_deadline
    }

    /// The bounded final-flush component (a fixed 2 s allowance).
    #[must_use]
    pub fn final_flush(&self) -> Duration {
        self.final_flush
    }

    /// The worst-case total: grace + teardown deadline + final flush.
    #[must_use]
    pub fn total(&self) -> Duration {
        self.grace + self.teardown_deadline + self.final_flush
    }
}

impl std::fmt::Display for ShutdownBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shutdown budget: grace {}s + teardown-deadline {}s + final-flush {}s = {}s worst-case",
            self.grace.as_secs(),
            self.teardown_deadline.as_secs(),
            self.final_flush.as_secs(),
            self.total().as_secs(),
        )
    }
}

/// Compute the worst-case [shutdown budget](ShutdownBudget) from the effective
/// `grace` and `teardown_deadline` flag values (arch.md C16): grace + teardown
/// deadline + a fixed [bounded final flush](DEFAULT_FINAL_FLUSH). The binary prints
/// this at startup so a misconfiguration is visible before it matters.
#[must_use]
pub fn shutdown_budget(grace: Duration, teardown_deadline: Duration) -> ShutdownBudget {
    ShutdownBudget {
        grace,
        teardown_deadline,
        final_flush: DEFAULT_FINAL_FLUSH,
    }
}

/// The **shutdown exit selection** a completed drive surfaces for the C26
/// exit-code contract (arch.md `### C16` / `### C26`; ticket T36).
///
/// The driver **reports** which of these applies; it does **not** own the numeric
/// C26 code table (that is T55). The selection follows C26 precedence:
///
/// 1. [`RunFailure`](ShutdownExit::RunFailure) — a non-teardown node ended
///    `failed`/`timed-out` (a genuine run failure). **Highest precedence:** a run
///    failure wins over cancellation *and* over a sink failure at shutdown.
/// 2. [`SinkFailure`](ShutdownExit::SinkFailure) — the event sink was unwritable at
///    the final flush. Distinct from a run failure: the failure to *record* is a
///    sink fault (C19 "event stream unwritable"), not a node ending failed. Reported
///    only when no node failed; the process waited a **bounded** time for the flush
///    and did not hang (C16 / C26).
/// 3. [`Cancelled`](ShutdownExit::Cancelled) — the run was cancelled by an external
///    interrupt (a termination signal / the `CancelHandle` seam) with no run failure
///    and a writable stream. Reported only for externally-originated termination.
/// 4. [`Success`](ShutdownExit::Success) — the run completed and its stream was
///    flushed cleanly.
///
/// A cancellation driven by *stop-on-first-failure* (a `FailureUnderStop` origin)
/// surfaces as [`RunFailure`](ShutdownExit::RunFailure), because a run failure
/// caused it — the origin the report also records lets the caller keep that
/// precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownExit {
    /// The run completed and its final flush succeeded.
    Success,
    /// A non-teardown node ended `failed`/`timed-out` — a run failure (highest
    /// precedence).
    RunFailure,
    /// The run was cancelled by external termination with no run failure and a
    /// writable stream.
    Cancelled,
    /// The event sink was unwritable at the final flush (bounded wait, distinct
    /// code — never a hang, never a success/plain-cancellation report).
    SinkFailure,
}

/// The programmatic **cancellation trigger** the caller obtains from
/// [`RunConfig::cancel_handle`] (arch.md `### C16`; T35).
///
/// This is the internal cancellation entry point exercised from a test or, in
/// production, the seam the **T36** OS-signal handler will drive — *not* an
/// OS-signal wiring itself. Firing it ([`cancel`](Self::cancel)) requests
/// cancellation of the run with an [external-interrupt](CancellationOrigin::ExternalInterrupt)
/// origin; the driver observes the request, stops admitting new work, drains
/// in-flight work within the grace period, and terminates. It is cheaply cloneable
/// and idempotent — firing twice changes nothing.
#[derive(Debug, Clone)]
pub struct CancelHandle {
    trigger: Arc<CancelTrigger>,
}

impl CancelHandle {
    /// Request cancellation of the run (external-interrupt origin). Idempotent: the
    /// first fire wins the recorded origin; a later fire changes nothing.
    pub fn cancel(&self) {
        self.trigger.request(CancellationOrigin::ExternalInterrupt);
    }
}

/// The shared cancellation-request state behind a [`CancelHandle`] and the run
/// loop. It records *whether* cancellation was requested and its **origin**
/// (first-request-wins), and notifies the loop so it can react promptly without
/// polling. The run-scoped [`CancellationSource`] the attempts observe is separate
/// (owned by the loop) and flipped when the loop acts on a request; this state is
/// only the *request channel* into the loop.
#[derive(Debug, Default)]
struct CancelTrigger {
    // The first-request-wins origin; `None` until requested. A `Mutex<Option<_>>`
    // rather than an atomic because the origin is a two-variant enum and the
    // set-once discipline is clearest expressed as "insert if empty".
    origin: Mutex<Option<CancellationOrigin>>,
    // The run loop's attempt-channel sender, installed by the loop at startup. A
    // request pushes a `CANCEL_WAKE_SENTINEL` `AttemptDone` through the **same**
    // channel the loop already awaits, so the loop wakes on a request without a
    // separate select/poll — no extra tokio feature (no `macros`/`select!`) is
    // needed. `None` before a run starts or after it ends (a late request is then a
    // harmless no-op).
    waker: Mutex<Option<tokio::sync::mpsc::UnboundedSender<AttemptDone>>>,
}

impl CancelTrigger {
    fn new() -> Self {
        Self::default()
    }

    /// Install the loop's wake channel (called once by the loop at startup). A
    /// cancellation [request](Self::request) then wakes the loop through it.
    fn install_waker(&self, tx: tokio::sync::mpsc::UnboundedSender<AttemptDone>) {
        *self.waker.lock().expect("cancel-waker mutex not poisoned") = Some(tx);
    }

    /// Uninstall the wake channel at run end, so a late request cannot touch a
    /// finished loop.
    fn clear_waker(&self) {
        *self.waker.lock().expect("cancel-waker mutex not poisoned") = None;
    }

    /// Record a cancellation request with `origin` (first request wins the origin)
    /// and wake the loop by pushing the sentinel onto its channel. Idempotent on the
    /// origin; a request after the loop ended is a harmless no-op.
    fn request(&self, origin: CancellationOrigin) {
        {
            let mut guard = self
                .origin
                .lock()
                .expect("cancel-origin mutex not poisoned");
            if guard.is_none() {
                *guard = Some(origin);
            }
        }
        if let Some(tx) = self
            .waker
            .lock()
            .expect("cancel-waker mutex not poisoned")
            .as_ref()
        {
            let _ = tx.send(AttemptDone {
                node: CANCEL_WAKE_SENTINEL.to_string(),
                state: TerminalState::Cancelled,
                events: Vec::new(),
            });
        }
    }

    /// The recorded origin, or `None` if no cancellation was requested.
    fn recorded_origin(&self) -> Option<CancellationOrigin> {
        *self
            .origin
            .lock()
            .expect("cancel-origin mutex not poisoned")
    }
}

// ===========================================================================
// Configuration
// ===========================================================================

/// The bootstrap configuration for one run (arch.md "The shape of a run").
///
/// It carries the resolved run-store base location, the optional operator run-id
/// override (absent → a fresh `UUIDv7` is minted), and the bounded zombie
/// [grace period](Self::grace). The environment-capture **allowlist** is not here
/// — it is declared at pipeline construction and read off the assembly artifact —
/// but the *captured values* are read from the process environment at bootstrap
/// against that allowlist.
#[derive(Debug, Clone)]
pub struct RunConfig {
    base: String,
    run_id: Option<String>,
    grace: Duration,
    teardown_deadline: Duration,
    parameters: BTreeMap<String, String>,
    data_interval: Option<[String; 2]>,
    capacities: PoolCapacities,
    failure_mode: FailureMode,
    // The programmatic cancellation trigger (C16 / T35): a shared request channel a
    // caller (a test, or T36's future signal handler) fires and the run loop
    // observes. Cloned into the loop; a `CancelHandle` handed out by
    // `cancel_handle` shares the same `Arc`. Never serialized/compared.
    cancel_trigger: Arc<CancelTrigger>,
}

impl RunConfig {
    /// A run configuration writing under `base`, minting a fresh `UUIDv7` run id,
    /// with the [default grace period](DEFAULT_GRACE) and no parameters/interval.
    #[must_use]
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            run_id: None,
            grace: DEFAULT_GRACE,
            teardown_deadline: DEFAULT_TEARDOWN_DEADLINE,
            parameters: BTreeMap::new(),
            data_interval: None,
            // Admission pools default to **unconstrained** (T31 takes capacities as
            // an input; deriving them from container limits is T32). An
            // unconstrained controller admits every ready node immediately, so the
            // M1 run loop's behaviour is unchanged unless a capacity is pinned.
            capacities: PoolCapacities::new(),
            // The failure mode defaults to continue-independent (C15 / T34): a
            // failure cancels nothing, so an unset mode leaves the M1 loop's
            // behaviour unchanged. Stop-on-first-failure is opt-in.
            failure_mode: FailureMode::default(),
            // A fresh, un-fired cancellation trigger. Unless a caller fires the
            // handle (or stop-on-first-failure routes through the core), the run is
            // never cancelled and its behaviour is unchanged from T24/T34.
            cancel_trigger: Arc::new(CancelTrigger::new()),
        }
    }

    /// Override the minted run identity with an operator-supplied value, used
    /// **verbatim** everywhere the minted id would appear (T0.6 §4).
    #[must_use]
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Set the cooperative **grace period** (default [`DEFAULT_GRACE`], 10 s;
    /// arch.md C16). This is the single operator flag this ticket owns: it bounds
    /// *both* the zombie wait at natural run end (T24) and the cancellation drain
    /// wait for in-flight cooperative work (T35), and it drives the printed
    /// [shutdown budget](ShutdownBudget). On cancellation, in-flight await-bound
    /// attempts are asked to stop and given up to this long to return before being
    /// recorded `abandoned`.
    #[must_use]
    pub fn grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// The **effective** grace period this run will honour (the override if set,
    /// else [`DEFAULT_GRACE`]).
    #[must_use]
    pub fn effective_grace(&self) -> Duration {
        self.grace
    }

    /// Set the **teardown deadline** (default [`DEFAULT_TEARDOWN_DEADLINE`], 15 s;
    /// arch.md C16 / C17). T35 only **consumes** this value for the worst-case
    /// [shutdown-budget](ShutdownBudget) arithmetic printed at startup; teardown
    /// execution under its own fresh, uncancelled signal and this deadline is T52.
    #[must_use]
    pub fn teardown_deadline(mut self, deadline: Duration) -> Self {
        self.teardown_deadline = deadline;
        self
    }

    /// The worst-case [shutdown budget](ShutdownBudget) for this run: the effective
    /// grace + the teardown deadline + the bounded final flush. Printed at startup.
    #[must_use]
    pub fn shutdown_budget(&self) -> ShutdownBudget {
        shutdown_budget(self.grace, self.teardown_deadline)
    }

    /// Obtain the programmatic **cancellation trigger** for this run (arch.md C16;
    /// T35). Firing the returned [`CancelHandle`] requests cancellation with an
    /// external-interrupt origin; the driver stops admitting new work, drains
    /// in-flight work within the grace period, and terminates. This is the internal
    /// entry point a test drives and the seam T36's OS-signal handler will fire —
    /// wiring an actual SIGINT/SIGTERM to it is **not** this ticket's. Multiple
    /// handles may be obtained; they all drive the same run.
    #[must_use]
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle {
            trigger: Arc::clone(&self.cancel_trigger),
        }
    }

    /// Record the run's parameters for the `run-started` header (name→value).
    #[must_use]
    pub fn parameters(mut self, parameters: BTreeMap<String, String>) -> Self {
        self.parameters = parameters;
        self
    }

    /// Record the run's opaque data interval for the `run-started` header.
    #[must_use]
    pub fn data_interval(mut self, interval: [String; 2]) -> Self {
        self.data_interval = Some(interval);
        self
    }

    /// Pin the run's C12 admission-pool capacities (arch.md C12; T31). The default
    /// is **unconstrained** (every ready node admitted at once); pinning a pool
    /// bounds admission against it. Container-limit derivation of these capacities
    /// is the T32 [`ContainerLimitProbe`](dagr_core::limits::ContainerLimitProbe)
    /// (cgroup v2 → v1 → host, with the 20% headroom default); pass its
    /// [`detect`](dagr_core::limits::ContainerLimitProbe::detect) output here to
    /// size the pools from the machine, or a pinned set (the operator flag, which
    /// is also how CI makes capacity deterministic).
    #[must_use]
    pub fn capacities(mut self, capacities: PoolCapacities) -> Self {
        self.capacities = capacities;
        self
    }

    /// Select the run-level [failure mode](FailureMode) (arch.md C15; T34). This
    /// is the driver-side override seam the builder/assembly mode
    /// ([`Flow::failure_mode`](dagr_core::flow::Flow::failure_mode)) feeds and the
    /// operator/CLI override (T55) will feed too, without a signature change. The
    /// default is [`ContinueIndependent`](FailureMode::ContinueIndependent) — a
    /// failure cancels nothing — so leaving it unset preserves the M1 run loop's
    /// behaviour exactly.
    #[must_use]
    pub fn failure_mode(mut self, mode: FailureMode) -> Self {
        self.failure_mode = mode;
        self
    }

    /// The resolved run identity: the operator override verbatim if present, else
    /// a freshly-minted `UUIDv7` (T0.6 §4).
    #[must_use]
    fn resolve_run_id(&self) -> RunId {
        match &self.run_id {
            Some(id) => RunId::from_operator(id.clone()),
            None => RunId::generate(),
        }
    }
}

// ===========================================================================
// Node runners (type-erased attempt path)
// ===========================================================================

/// A single node's **type-erased attempt path** — what the driver spawns for an
/// admitted node.
///
/// A pipeline's nodes have heterogeneous output types, so the run loop cannot be
/// generic over one `T`; instead each node is presented to the driver as a boxed
/// `NodeRunner`. The runner owns its task, its output [slot](dagr_core::slot::Slot),
/// and its input wiring (the upstream slot references it reads), and it exposes a
/// single operation: run the node to its terminal state, emitting the C14 attempt
/// records through the injected sink.
///
/// The driver supplies the per-attempt [`RunContext`] (carrying run/pipeline/node
/// identity) and the sink; the runner drives the C14 attempt path (the caught
/// single-attempt/retry runner) and returns the node's normative
/// [`TerminalState`]. Reading inputs from upstream slots is the runner's concern —
/// by the time the driver admits a node, every upstream has succeeded, so the
/// upstream slots are filled.
pub trait NodeRunner: Send {
    /// The node's author-declared identity name (T13) — keys every emitted record.
    fn name(&self) -> &str;

    /// Run this node to its terminal state, emitting the C14 attempt records
    /// through `sink`. Called once, spawned on the **task-execution runtime**
    /// after the driver has admitted the node. Returns the node's normative
    /// terminal state.
    ///
    /// The returned future is boxed and `Send` so the trait stays object-safe over
    /// a pipeline's heterogeneous node types while the driver spawns it on the task
    /// runtime. A misbehaving body (a blocking busy-loop) may never resolve; the
    /// driver arms the per-attempt timeout on the isolated framework runtime, so
    /// the timeout still fires and this node's fate is decided even if the body
    /// jams its worker — the leftover work is then a zombie the driver waits for a
    /// bounded grace period at run end.
    ///
    /// `sink` is a buffering sink the driver drains into the authoritative writer
    /// on the framework runtime (the writer is single-owner — C19); the runner
    /// never touches the real writer.
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>>;
}

// ===========================================================================
// The run plan
// ===========================================================================

/// Everything one run needs beyond its bootstrap [`RunConfig`]: the assembled
/// pipeline plus the type-erased [runners](NodeRunner) for its nodes, keyed by
/// node name.
///
/// The driver consumes a `RunPlan` (or the assembly error that prevented one).
/// Building the plan — assembling the pipeline and wiring each node's runner with
/// its input slot references — is the caller's; the driver orchestrates.
pub struct RunPlan {
    pipeline: Pipeline,
    runners: BTreeMap<String, Box<dyn NodeRunner>>,
    /// Run-level **ordering upstreams** (C15 / T34): a node name → the names of
    /// nodes it must run *after* even though it consumes no value from them. This
    /// is how a consume-nothing node with a non-default trigger rule
    /// (`all-terminal` / `any-failed`) acquires the upstreams its rule is evaluated
    /// against — the runtime firing of the non-default rules. Empty for a plan
    /// built with [`new`](RunPlan::new). The graph-authoring ordering-edge API and
    /// its fingerprint/render treatment are T50; this seam seeds only the readiness
    /// tracker's dependency structure.
    ordering: BTreeMap<String, Vec<String>>,
}

impl RunPlan {
    /// Build a run plan over an assembled `pipeline` and its node `runners` (keyed
    /// by node name). Every node in the pipeline should have a runner; a node with
    /// no runner is treated as an immediate framework defect at drive time. No
    /// run-level ordering upstreams — every node's upstreams are its data edges.
    #[must_use]
    pub fn new(pipeline: Pipeline, runners: BTreeMap<String, Box<dyn NodeRunner>>) -> Self {
        Self {
            pipeline,
            runners,
            ordering: BTreeMap::new(),
        }
    }

    /// Build a run plan that additionally declares run-level **ordering
    /// upstreams** (C15 / T34): `ordering` maps a node's name to the names of nodes
    /// it must run *after* without consuming their value.
    ///
    /// This is the run-level seam a consume-nothing node with a non-default trigger
    /// rule uses to be ordered after the nodes its rule watches (a notify-on-failure
    /// or cleanup contingency ordered after the work it guards) — the runtime firing
    /// of `all-terminal` / `any-failed` that this ticket lands. It seeds the
    /// readiness tracker via
    /// [`ReadinessTracker::new_with_ordering`](dagr_core::readiness::ReadinessTracker::new_with_ordering)
    /// and touches neither the graph artifact nor the fingerprint (that is T50's
    /// graph ordering-edge API).
    #[must_use]
    pub fn with_ordering(
        pipeline: Pipeline,
        runners: BTreeMap<String, Box<dyn NodeRunner>>,
        ordering: BTreeMap<String, Vec<String>>,
    ) -> Self {
        Self {
            pipeline,
            runners,
            ordering,
        }
    }
}

// ===========================================================================
// The run report
// ===========================================================================

/// The outcome of one drive: the overall [outcome](RunOutcome) the driver
/// surfaces to its caller, plus the per-node terminal states.
///
/// The caller (the run verb) maps the overall outcome to an exit code (C26 / T55);
/// the driver reports it, it does not own the code table.
#[derive(Debug, Clone)]
pub struct RunReport {
    /// The overall run outcome carried by the final `run-finished` record.
    pub outcome: RunOutcome,
    /// Each node's terminal state, keyed by node name.
    pub terminal_states: BTreeMap<String, TerminalState>,
    /// The resolved run identity (as it appears in the store path and every
    /// record).
    pub run_id: String,
    /// The run-store event-stream path this run wrote under —
    /// `<base>/<pipeline>/<run-id>/events.jsonl` (T0.6 §3). Because the path
    /// embeds both the pipeline identity and the run-unique id, two concurrent
    /// runs — even of the same binary and pipeline — write disjoint files.
    pub stream_path: String,
    /// Why the run was cancelled, or [`None`] if it was not cancelled (arch.md
    /// C16). Recorded so the C26 exit-code logic (T55) can prefer *run failure over
    /// cancellation*: a [`FailureUnderStop`](CancellationOrigin::FailureUnderStop)
    /// origin means a failure triggered the cancellation (failure wins), while an
    /// [`ExternalInterrupt`](CancellationOrigin::ExternalInterrupt) with no run
    /// failure is reported as a cancellation. This ticket records the origin; it
    /// does not own the exit-code mapping.
    pub cancellation_origin: Option<CancellationOrigin>,
    /// The C26 **shutdown exit selection** for this run (arch.md C16 / C26; T36):
    /// which of run-failure / sink-failure / cancellation / success applies, by C26
    /// precedence. Derived from the run outcome, the cancellation origin, and whether
    /// the bounded final flush succeeded. The driver reports it; T55 owns the numeric
    /// code table.
    pub shutdown_exit: ShutdownExit,
}

// ===========================================================================
// The event sink port (C19 writer adapter)
// ===========================================================================

/// Map a core [`TerminalState`] onto the C19 wire [`WireTerminalState`]. The two
/// enums are structurally identical (both are the arch.md normative taxonomy);
/// this is the crate-boundary bridge the driver owns.
fn wire_terminal(state: TerminalState) -> WireTerminalState {
    match state {
        TerminalState::Succeeded => WireTerminalState::Succeeded,
        TerminalState::Failed => WireTerminalState::Failed,
        TerminalState::TimedOut => WireTerminalState::TimedOut,
        TerminalState::Skipped => WireTerminalState::Skipped,
        TerminalState::UpstreamSkipped => WireTerminalState::UpstreamSkipped,
        TerminalState::UpstreamFailed => WireTerminalState::UpstreamFailed,
        TerminalState::Cancelled => WireTerminalState::Cancelled,
        TerminalState::Abandoned => WireTerminalState::Abandoned,
        TerminalState::SatisfiedFromPrior => WireTerminalState::SatisfiedFromPrior,
    }
}

/// Translate one abstract [`AttemptEvent`] (the port the C14 runner emits
/// through) into the concrete C19 [`Event`] and write it through the writer.
///
/// This is exactly the adaptation the T20 design left to T24: the runner emits
/// abstract attempt records, and the driver stamps them into the real
/// event-stream envelope (run identity, schema version, gapless sequence, wall
/// stamp, monotonic offset) via the writer. Non-attempt records (`node-ready`,
/// `run-started`, `run-finished`, `zombie-at-exit`) are the driver's own and are
/// written directly.
fn write_attempt_event<S, C>(
    writer: &mut EventStreamWriter<S, C>,
    event: &AttemptEvent,
) -> Result<(), dagr_artifact::event_stream::SinkFault>
where
    S: EventSink,
    C: MonotonicClock,
{
    let wire = match event {
        AttemptEvent::NodeAdmitted { node } => Event::NodeAdmitted { node: node.clone() },
        AttemptEvent::AttemptStarted { node, attempt } => Event::AttemptStarted {
            node: node.clone(),
            attempt: *attempt,
        },
        AttemptEvent::AttemptSucceeded { node, attempt } => Event::AttemptSucceeded {
            node: node.clone(),
            attempt: *attempt,
        },
        // Every non-success attempt-outcome record maps onto the C19
        // `attempt-failed` transition (the closed C19 vocabulary carries one
        // failure-outcome record; the specific terminal state travels on the
        // node-terminal record). The richer per-outcome records (timed-out,
        // panicked, backoff) fold into the artifact at C22 fold time (T42).
        AttemptEvent::AttemptFailed { node, attempt }
        | AttemptEvent::AttemptTimedOut { node, attempt }
        | AttemptEvent::AttemptPanicked { node, attempt, .. }
        | AttemptEvent::BackoffStarted { node, attempt, .. } => Event::AttemptFailed {
            node: node.clone(),
            attempt: *attempt,
        },
        AttemptEvent::NodeTerminal { node, state } => Event::NodeTerminal {
            node: node.clone(),
            state: wire_terminal(*state),
        },
        // `AttemptEvent` is `#[non_exhaustive]`; a future outcome record is still
        // an attempt-outcome record, so it maps onto the C19 `attempt-failed`
        // transition until the closed C19 vocabulary grows a matching variant.
        other => {
            // Preserve the node name for any future `{ node, .. }`-shaped variant
            // by falling through to a best-effort admitted-then-failed pairing is
            // unnecessary — there is no such variant today. Drop unknown records
            // rather than fabricate a mislabelled one.
            let _ = other;
            return Ok(());
        }
    };
    writer.emit_event(&wire)
}

/// A buffering [`AttemptEventSink`] a spawned node attempt emits into off the
/// framework runtime.
///
/// The C14 runner emits synchronously through an [`AttemptEventSink`], but the
/// authoritative event writer lives on the isolated framework runtime and must
/// not be touched from a task worker (write-through, single-writer — C19). So a
/// spawned attempt emits into this in-memory buffer, and the framework loop drains
/// the buffer into the real writer in order once the attempt returns. This keeps
/// the writer single-owner while every attempt record still reaches the stream.
#[derive(Clone, Default)]
struct BufferingSink {
    records: Arc<Mutex<Vec<AttemptEvent>>>,
}

impl BufferingSink {
    fn drain(&self) -> Vec<AttemptEvent> {
        let mut guard = self
            .records
            .lock()
            .expect("event buffer mutex not poisoned");
        std::mem::take(&mut *guard)
    }
}

impl AttemptEventSink for BufferingSink {
    fn emit(&mut self, event: AttemptEvent) {
        self.records
            .lock()
            .expect("event buffer mutex not poisoned")
            .push(event);
    }
}

// ===========================================================================
// The driver entry point
// ===========================================================================

/// Drive one complete run to a truthful end (arch.md "The shape of a run"; T24).
///
/// This is the run-verb path's driver. It mints run identity, opens the store and
/// stream **before** `plan` (or `assembly_error`) is acted on, emits `run-started`,
/// drives the readiness-driven execution loop admitting ready nodes and feeding
/// outcomes back, waits the bounded grace period for any zombie closures at
/// natural run end, emits `zombie-at-exit` for each leftover thread, emits
/// `run-finished`, and returns the overall outcome and per-node terminal states.
///
/// `sink` is the injected C19 [`EventSink`] (the run store's local-file sink in
/// production, or a test sink); `clock` is the authoritative monotonic clock. Both
/// are injected per T0.6 so the driver constructs no store itself.
///
/// # The assembly-failure path
///
/// `assembled` is the result of the pure assembly pass, computed by the caller
/// **after** the store/stream were opened (that ordering is the point — an
/// assembly failure still lands in the record). When it is `Err`, the driver emits
/// a `run-started` header with no fingerprints and a `run-finished` carrying
/// [`RunOutcome::AssemblyFailed`], and returns — no node runs.
///
/// # The bootstrap-failure path (C12/T32)
///
/// After a successful assembly and the `run-started` header, the driver runs the
/// C12 too-big-node bootstrap check ([`detect_capacities`]): if any node's declared
/// cost exceeds a pool's total capacity, it can never be admitted, so the run fails
/// fast — the driver emits a `run-finished` carrying
/// [`RunOutcome::BootstrapFailed`] (distinct from `assembly-failed`) and returns
/// with **no** node executed, rather than wedging at admission time.
///
/// # Panics
///
/// Panics only on a framework defect it cannot record (a poisoned internal mutex
/// or a task runtime that could not be built); a sink fault is absorbed and
/// surfaced through the returned report's outcome, never a panic.
#[must_use]
#[allow(
    clippy::too_many_lines,
    reason = "the driver is one linear bootstrap-then-drive sequence (mint identity, \
              open the stream, record the run-started header, run the assembly/bootstrap \
              fail-fast checks, drive the loop, finalize shutdown); its early-return \
              failure paths each record a full run-started/run-finished pair, so splitting \
              them would scatter the single ordered narrative the record-before-act \
              contract (arch.md C19) depends on"
)]
pub fn drive<S, C>(
    config: &RunConfig,
    pipeline_name: &str,
    assembled: Result<RunPlan, AssemblyError>,
    env_allowlist: &[String],
    sink: S,
    clock: C,
) -> RunReport
where
    S: EventSink + 'static,
    C: MonotonicClock + 'static,
{
    // --- Bootstrap: install the single process-global tracing subscriber (C25 /
    // T45) once, before anything runs, so every framework/attempt line beneath it
    // is formatted and attributable. Idempotent and coexistence-safe: a repeat
    // call or a pre-existing subscriber (e.g. a test harness's) is a no-op, never
    // a panic. The output mode (structured default / human) is read from the
    // DAGR_LOG_FORMAT env var (arch.md C25). This is the developer/operator
    // observability layer, distinct from the C19 event stream opened just below.
    let _ = crate::logging::init_tracing();

    // --- Bootstrap: mint identity, open the stream BEFORE assembly is acted on.
    let run_id = config.resolve_run_id();
    let run_id_str = run_id.as_str().to_string();
    let mut writer = EventStreamWriter::new(sink, clock, run_id, pipeline_name.to_string());
    // The run-store path this run writes under: <base>/<pipeline>/<run-id>/…
    // (T0.6 §3). Two concurrent runs write disjoint files by construction.
    let stream_path = writer.stream_path(&config.base);

    // Capture the allowlisted environment values (empty allowlist → nothing).
    let captured_env = capture_env(env_allowlist);

    // --- Print the worst-case shutdown budget at startup (arch.md C16): grace +
    // teardown deadline + bounded final flush. Printed before anything runs so a
    // misconfiguration (a budget that would not fit the orchestrator's kill window)
    // is visible before it matters. Operator-facing, so it goes to stderr and never
    // into the event stream.
    eprintln!("{}", config.shutdown_budget());

    // --- The per-run temp-directory convention (arch.md C16; T36). Create this
    // run's own temp dir and sweep prior runs' leftovers (see `bootstrap_temp_dir`).
    let temp_dir = bootstrap_temp_dir(&config.base, pipeline_name, &run_id_str);

    // --- The assembly-failure path: the store/stream are already open, so an
    // assembly failure still records itself (arch.md C19). Emit a fingerprint-less
    // header and a run-finished carrying the assembly-failed outcome, then return.
    let plan = match assembled {
        Ok(plan) => plan,
        Err(_error) => {
            let header = RunStartedHeader {
                pipeline: pipeline_name.to_string(),
                fingerprint_structural: None,
                fingerprint_policy: None,
                fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
                parameters: config.parameters.clone(),
                data_interval: config.data_interval.clone(),
                captured_env,
                resumed_from: None,
            };
            let _ = writer.run_started(header);
            let _ = writer.run_finished(RunOutcome::AssemblyFailed);
            // The bounded final flush + temp reclaim run even on this early path (no
            // node executed, but the temp dir was created).
            let flush_ok = finalize_shutdown(&mut writer, &temp_dir);
            return RunReport {
                outcome: RunOutcome::AssemblyFailed,
                terminal_states: BTreeMap::new(),
                run_id: run_id_str,
                stream_path,
                // No node ran, so no cancellation path was entered.
                cancellation_origin: None,
                shutdown_exit: select_shutdown_exit(RunOutcome::AssemblyFailed, None, flush_ok),
            };
        }
    };

    // --- The successful path: assembly produced a valid artifact. Emit the
    // run-started header carrying every field known at start (both fingerprints
    // present because assembly succeeded), then drive the execution loop.
    let RunPlan {
        pipeline,
        runners,
        ordering,
    } = plan;
    let artifact = pipeline
        .assemble()
        .expect("the plan carries an already-assembled pipeline");
    let fp = artifact.fingerprint();
    let header = RunStartedHeader {
        pipeline: pipeline_name.to_string(),
        fingerprint_structural: Some(format!("{:016x}", fp.structural())),
        fingerprint_policy: Some(format!("{:016x}", fp.policy())),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: config.parameters.clone(),
        data_interval: config.data_interval.clone(),
        captured_env,
        resumed_from: None,
    };
    let _ = writer.run_started(header);

    // --- The C12 too-big-node bootstrap check (T32): reject, before any node
    // executes, any node whose declared cost exceeds a pool's total capacity —
    // fail fast rather than wedge at admission time. This runs after the header is
    // recorded (so a bootstrap failure still lands in the stream, like an assembly
    // failure) and before the loop starts (so nothing runs). It is distinct from
    // T31's admission-time can-never-fit guard and produces the `bootstrap-failed`
    // outcome. The capacities are the resolved pool totals (container-limit derived
    // or operator-pinned via the T32 flag); the declared costs come from C5.
    let node_costs: Vec<(String, PoolCost)> = pipeline
        .nodes()
        .map(|n| {
            (
                n.name().to_string(),
                PoolCost::from_cost_vector(n.policy().cost()),
            )
        })
        .collect();
    if let Err(failure) = detect_capacities(&config.capacities, &node_costs) {
        // A too-big node: fail bootstrap. The complete error list names every
        // offending node, its pool, declared cost, and capacity — surface it so an
        // operator can fix the run, then record the bootstrap-failed outcome and
        // return. No node executed (zero attempts), and the run does not hang.
        eprintln!("{failure}");
        let _ = writer.run_finished(RunOutcome::BootstrapFailed);
        let flush_ok = finalize_shutdown(&mut writer, &temp_dir);
        return RunReport {
            outcome: RunOutcome::BootstrapFailed,
            terminal_states: BTreeMap::new(),
            run_id: run_id_str,
            stream_path,
            // No node ran, so no cancellation path was entered.
            cancellation_origin: None,
            shutdown_exit: select_shutdown_exit(RunOutcome::BootstrapFailed, None, flush_ok),
        };
    }

    // Seed the readiness tracker with the run-level ordering upstreams (C15 / T34):
    // this is how a consume-nothing node with a non-default trigger rule acquires
    // the upstreams its rule fires against. An empty ordering map yields exactly the
    // M1 data-edge-only tracker.
    let tracker = ReadinessTracker::new_with_ordering(&pipeline, &artifact, &ordering);
    // The C12 admission controller for this run (T31). Its pools are pinned from
    // the run config (container-limit-derived or operator-pinned — T32). The
    // too-big-node bootstrap check above already rejected any node that could never
    // fit, so the loop's admission never strands a can-never-fit node here.
    let admission = AdmissionController::new(config.capacities);
    let (outcome, terminal_states, cancellation_origin) = run_loop(
        &pipeline,
        &run_id_str,
        pipeline_name,
        runners,
        tracker,
        config.grace,
        config.failure_mode,
        &admission,
        &config.capacities,
        &config.cancel_trigger,
        &temp_dir,
        &mut writer,
    );

    let _ = writer.run_finished(outcome);
    let flush_ok = finalize_shutdown(&mut writer, &temp_dir);

    RunReport {
        outcome,
        terminal_states,
        run_id: run_id_str,
        stream_path,
        cancellation_origin,
        shutdown_exit: select_shutdown_exit(outcome, cancellation_origin, flush_ok),
    }
}

/// The shutdown finalize shared by every exit path (arch.md `### C16`; T36):
/// perform the **bounded final flush** and reclaim the run's **per-run temp
/// directory**, returning whether the flush succeeded.
///
/// The [final flush](final_flush) is the single fsync-at-run-end/cancellation
/// boundary (C19); a `false` return is the unwritable-sink-at-shutdown fault
/// (bounded, not a hang) the caller maps onto the distinct sink-failure exit. The
/// [temp cleanup](crate::temp::cleanup_temp_dir) removes this run's temp directory
/// whether the run ended normally or was cancelled — best-effort by design (a racing
/// zombie thread may hold a file open, and the process exits promptly rather than
/// blocking on it).
fn finalize_shutdown<S, C>(writer: &mut EventStreamWriter<S, C>, temp_dir: &std::path::Path) -> bool
where
    S: EventSink,
    C: MonotonicClock,
{
    let flush_ok = final_flush(writer);
    crate::temp::cleanup_temp_dir(temp_dir);
    flush_ok
}

/// Bootstrap the run's per-run temp directory (arch.md `### C16`; T36) and return
/// its path.
///
/// Creates this run's own `<base>/<pipeline>/<run-id>/tmp/` synchronously — a task
/// needs it the moment it runs; everything a task writes locally goes under it
/// (reached through the [context](RunContext::temp_dir)), and the driver removes it
/// at run end (normal or cancelled). Then reclaims any leftover per-run temp
/// directories from **prior** runs of this pipeline (regardless of how the prior
/// process ended — an abrupt kill leaves debris the next invocation sweeps),
/// confined to this pipeline (dagr reaps no other process's work — a permanent
/// non-goal). The reclamation is best-effort housekeeping over what a *previous*
/// process left behind and is independent of the current run, so it runs on a
/// **detached background thread**, kept off the bootstrap-to-loop hot path so its
/// O(retained-runs) directory scan never adds latency or jitter to the run about to
/// start. It touches only the ephemeral `tmp/` subtree of *other* run directories,
/// never a reserved output and never the current run's own temp dir.
fn bootstrap_temp_dir(base: &str, pipeline: &str, run_id: &str) -> std::path::PathBuf {
    let temp_dir = crate::temp::per_run_temp_dir(base, pipeline, run_id);
    if let Err(err) = crate::temp::create_temp_dir(&temp_dir) {
        // Non-fatal to record-keeping: a task that needs the temp dir will surface
        // its own error. Report best-effort to stderr (never into the stream).
        eprintln!(
            "could not create per-run temp directory {}: {err}",
            temp_dir.display()
        );
    }
    let (base, pipeline, keep) = (base.to_string(), pipeline.to_string(), run_id.to_string());
    // Detached: never joined. A sweep that outlives this process is simply the
    // *following* invocation's to finish — the guarantee is eventual reclamation by
    // a next invocation, not a synchronous one.
    std::thread::spawn(move || {
        crate::temp::reclaim_leftover_temp_dirs(&base, &pipeline, &keep);
    });
    temp_dir
}

/// Capture the values of the allowlisted environment variable names, in name
/// order (empty allowlist → empty map). Nothing outside the allowlist is read
/// into the map — the negative half of the C7/C22 capture contract.
fn capture_env(allowlist: &[String]) -> BTreeMap<String, String> {
    let mut captured = BTreeMap::new();
    for name in allowlist {
        if let Ok(value) = std::env::var(name) {
            captured.insert(name.clone(), value);
        }
    }
    captured
}

/// The message a finished attempt sends back to the framework loop: the node's
/// name, its terminal state, and the buffered attempt records it emitted (drained
/// into the single-owner writer by the loop, in order).
struct AttemptDone {
    node: String,
    state: TerminalState,
    events: Vec<AttemptEvent>,
}

/// The reserved sentinel node name for a **cancellation wake** pushed through the
/// attempt channel (C16 / T35). A real node name is never empty (assembly rejects
/// an empty name), so an [`AttemptDone`] carrying this name is unambiguously the
/// cancellation-request wake, not a finished attempt. Routing the wake through the
/// *same* channel the loop already awaits keeps the loop a plain `recv().await` — no
/// `tokio::select!` (and so no added `macros` feature) is needed for the loop to
/// react promptly to a request.
const CANCEL_WAKE_SENTINEL: &str = "";

/// The shared set of node names currently **in flight** (admitted, not yet
/// terminal) — the drain target when the run is cancelled (C16 / T35). A name is
/// inserted when the node is admitted and removed when its [`AttemptDone`] is
/// received; whatever remains after the grace-bounded drain is recorded
/// `abandoned`. Shared behind an `Arc<Mutex<_>>` because `admit` inserts from the
/// (framework-runtime) loop while the loop removes on completion.
type LiveSet = Arc<Mutex<std::collections::BTreeSet<String>>>;

/// The **immutable shared context** every admission/dispatch helper reads: the
/// assembled pipeline, the run identity, the type-erased runners, the C13 execution
/// dispatcher, the loop's attempt channel, the C12 admission controller, the
/// run-scoped C16 cancellation token, and the in-flight [`LiveSet`]. Bundling these
/// keeps `offer_or_pend`/`admit`/`drain_pending`/`apply_decisions` to a small
/// argument list (the per-call mutable state — `pending`, `in_flight`, the writer,
/// the terminal maps — is still passed explicitly, because it is mutated).
struct AdmitCtx<'a> {
    pipeline: &'a Pipeline,
    run_id: &'a str,
    runners: &'a Arc<Mutex<BTreeMap<String, Box<dyn NodeRunner>>>>,
    dispatcher: &'a Dispatcher,
    tx: &'a tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    admission: &'a AdmissionController,
    run_cancel: &'a CancellationSource,
    live: &'a LiveSet,
    // The run's per-run temp directory (arch.md C16; T36), threaded into each
    // attempt's `RunContext` so a task reaches its confined local scratch through
    // the context. Created at bootstrap and reclaimed at run end by the driver.
    temp_dir: &'a std::path::Path,
}

/// The readiness-driven execution loop (arch.md C11; the driver's half of the
/// run-end condition).
///
/// It runs on the isolated **framework runtime** and admits ready nodes onto the
/// [`Dispatcher`]'s three execution surfaces **by execution class** (C13 / T33) —
/// await-bound onto the tokio task runtime, blocking onto the dedicated blocking
/// pool, compute onto the fixed rayon pool — feeding each terminal outcome back
/// into the tracker so dependents decrement and either become ready (admitted next)
/// or receive their propagated terminal state (recorded without executing) — never
/// batching a level into a wave. It terminates precisely when nothing is pending and
/// nothing is in flight, then waits the bounded grace period for zombie candidates
/// (blocking timeouts) and emits a `zombie-at-exit` event for each. Returns the
/// overall outcome and the per-node terminal states.
#[allow(clippy::too_many_arguments)]
#[allow(
    clippy::too_many_lines,
    reason = "the readiness-driven execution loop is one cohesive state machine (admit → \
              await → feed back → drain-on-cancel → post-drain → bounded zombie wait); \
              splitting its single `block_on` future across functions would fragment the \
              shared mutable loop state (in-flight count, live set, pending queue, \
              cancellation flags) without aiding readability. The self-contained steps \
              (per-attempt recording, abandonment, cancellation entry, message receipt) \
              are already extracted into helpers."
)]
fn run_loop<S, C>(
    pipeline: &Pipeline,
    run_id: &str,
    _pipeline_name: &str,
    runners: BTreeMap<String, Box<dyn NodeRunner>>,
    mut tracker: ReadinessTracker,
    grace: Duration,
    failure_mode: FailureMode,
    admission: &AdmissionController,
    capacities: &PoolCapacities,
    cancel_trigger: &Arc<CancelTrigger>,
    temp_dir: &std::path::Path,
    writer: &mut EventStreamWriter<S, C>,
) -> (
    RunOutcome,
    BTreeMap<String, TerminalState>,
    Option<CancellationOrigin>,
)
where
    S: EventSink,
    C: MonotonicClock,
{
    // The execution-class dispatcher (C13 / T33): it owns the three task surfaces —
    // the async task runtime (await-bound), the dedicated blocking pool (blocking,
    // via `spawn_blocking`), and the fixed rayon compute pool (compute) — built from
    // the run's pinned pool capacities (the compute pool sized to the pinned
    // `compute_threads`, floor of one; T2 §3, consuming T31/T32 sizing). Each is a
    // *task* surface, separate from the framework runtime below, so a task that jams
    // every task/blocking/compute worker cannot stall the loop, its timers, or the
    // writer (T2 · isolated framework runtime).
    let dispatcher = Dispatcher::new(capacities);
    // The framework runtime — drives this loop, the grace timer, and the drain. It
    // is NOT one of the dispatcher's task surfaces, which is the isolation the C13
    // acceptance (all-workers-blocked timeout still fires) depends on.
    let framework = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .expect("framework runtime builds");

    let runners = Arc::new(Mutex::new(runners));
    let mut terminal_states: BTreeMap<String, TerminalState> = BTreeMap::new();
    // Zombie candidates, each paired with the 1-based attempt number whose thread
    // was left behind (the `zombie-at-exit` record keys pinned-time accounting off
    // `(node, attempt)`; C14 / C22 fold).
    let mut zombie_candidates: Vec<(String, u32)> = Vec::new();
    // The run-scoped cancellation token (C16 / T35): the driver owns it, each
    // admitted attempt observes a per-attempt child (threaded into its
    // `RunContext`), and the cancellation core flips it so every in-flight attempt
    // observes cancellation at once. Uncancelled unless the trigger fires or
    // stop-on-first-failure routes through the core — so a non-cancelled run's
    // attempts observe exactly the fresh-uncancelled signal T24/T34 gave them.
    let run_cancel = CancellationSource::new();
    // The recorded cancellation origin (first-cause-wins), surfaced to the report
    // for the C26 exit-code precedence (T55). `None` for a non-cancelled run.
    let mut cancel_origin: Option<CancellationOrigin> = None;

    framework.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AttemptDone>();
        // Install the loop's wake channel on the cancellation trigger so a request
        // (the programmatic handle now; T36's signal handler later) wakes the loop by
        // pushing a sentinel through this same channel — no `select!` needed.
        cancel_trigger.install_waker(tx.clone());
        // Nodes admitted and not yet reported terminal — the "in flight" count.
        let mut in_flight: usize = 0;
        // The **names** of nodes currently in flight (admitted, not yet terminal).
        // On cancellation this is exactly the set of attempts to drain and classify
        // `cancelled`/`abandoned`. Maintained alongside `in_flight`: a name is
        // inserted when the node is admitted and removed when its `AttemptDone`
        // arrives. Under a non-cancelled run it is only bookkeeping.
        let live: Arc<Mutex<std::collections::BTreeSet<String>>> =
            Arc::new(Mutex::new(std::collections::BTreeSet::new()));
        // Ready nodes that could not yet acquire their C12 permit (a pool at
        // capacity), oldest-ready-first (T31). Each is re-offered when a permit is
        // released — a terminal outcome frees capacity, which is what unblocks the
        // next waiter. Under the default unconstrained pools this stays empty and
        // every ready node is admitted at once (the M1 behaviour).
        let mut pending: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        // C15 / T34: whether stop-on-first-failure has been triggered — set the
        // first time a failure-like terminal is observed under stop mode. Once set,
        // no further default-rule non-teardown node is admitted; a firing
        // consume-nothing non-default-rule contingency still runs. In
        // continue-independent mode this stays false and the loop admits exactly as
        // the M1 driver did.
        let mut stopping = false;
        // C16 / T35 · **full-drain cancellation** (an external interrupt): admit
        // nothing new *at all* (not even a firing contingency) and grace-drain the
        // in-flight attempts, reclassifying an in-flight-at-cancel return to
        // `cancelled` and a non-returning attempt to `abandoned` after grace. This
        // is deliberately distinct from `stopping`: a **stop-on-first-failure** also
        // routes through the cancellation core (it flips the run token and records a
        // failure origin) but keeps T34's exact loop behaviour — it admits firing
        // contingencies and lets the in-flight complete naturally, so a non-cancelled
        // stop run is byte-for-byte the T34 run. Only an external interrupt sets this.
        let mut draining = false;
        // The single grace deadline for the whole drain, set once when the full drain
        // begins: `now + grace`. The drain waits for in-flight attempts only until
        // this instant, then abandons whatever remains — the bound that guarantees
        // termination even if a task ignores cancellation. `None` until the drain
        // begins.
        let mut drain_deadline: Option<tokio::time::Instant> = None;

        // The immutable shared context every admission/dispatch helper reads.
        let actx = AdmitCtx {
            pipeline,
            run_id,
            runners: &runners,
            dispatcher: &dispatcher,
            tx: &tx,
            admission,
            run_cancel: &run_cancel,
            live: &live,
            temp_dir,
        };

        // Offer the initial-ready frontier (every zero-dependency source node) to
        // admission. A node that fits its pools is admitted (in flight); one that
        // does not waits in `pending` for a release.
        for id in tracker.initial_ready().to_vec() {
            if let Some(name) = node_name(pipeline, id) {
                offer_or_pend(&actx, &name, writer, &mut pending, &mut in_flight);
            }
        }

        // Drive until nothing is pending, nothing is in flight, and no ready node
        // is waiting for capacity. A node whose attempt reports terminal is fed
        // back into the tracker; each unlocked decision either offers a ready node
        // to admission or records a propagated terminal (which cascades, without
        // executing). A terminal outcome also releases that attempt's permit, so
        // the pending waiters are re-offered against the freed capacity. A
        // cancellation request (fired even synchronously before the first wait,
        // e.g. by a source that already finished) reaches the loop as the
        // `CANCEL_WAKE_SENTINEL` message the trigger pushed onto this same channel.
        while in_flight > 0 {
            // Await the next channel message: a finished attempt, or a cancellation
            // wake (a `CANCEL_WAKE_SENTINEL` `AttemptDone` the trigger pushed). Once a
            // full drain has begun the wait is bounded by the single grace deadline —
            // whatever has not returned by then is abandoned and the run proceeds, the
            // bound that guarantees termination even if a task ignores cancellation.
            let Some(done) = recv_next(&mut rx, draining, drain_deadline).await else {
                break;
            };

            // A cancellation wake (the reserved-name sentinel): enter the cancellation
            // core (full drain — an external interrupt), which arms the drain
            // deadline. It is not a real attempt, so it does not decrement `in_flight`.
            if done.node == CANCEL_WAKE_SENTINEL {
                if !draining {
                    enter_cancellation(
                        &actx,
                        cancel_trigger.recorded_origin(),
                        true,
                        grace,
                        &mut cancel_origin,
                        &mut draining,
                        &mut stopping,
                        &mut drain_deadline,
                        &mut pending,
                        &mut in_flight,
                    );
                }
                continue;
            }

            in_flight -= 1;
            live.lock()
                .expect("live set not poisoned")
                .remove(&done.node);
            // Write the attempt's buffered records, classify its (possibly
            // cancellation-reclassified) terminal, and record it exactly once.
            let recorded_state = record_attempt_outcome(
                &done,
                draining,
                writer,
                &mut terminal_states,
                &mut zombie_candidates,
            );

            // C15 / T34 · stop-on-first-failure. The instant the first failure-like
            // terminal is observed under stop mode, route through the cancellation
            // core with a failure origin: stop admitting default-rule non-teardown
            // work and cancel every pending default-rule node unrelated to the
            // failure. The in-flight drain completes on its own; consume-nothing
            // non-default-rule contingencies whose rule fires on the resulting
            // picture are admitted as they become ready (below). Teardown-node
            // ordering after the contingencies (C17) is left to T52.
            if failure_mode == FailureMode::StopOnFirstFailure
                && !stopping
                && is_failure_like(recorded_state)
            {
                // `full_drain = false`: keep T34's loop behaviour exactly (firing
                // contingencies still admitted; in-flight completes naturally). The
                // core still flips the run token (so any cooperative in-flight work
                // can observe cancellation) and records the failure origin.
                enter_cancellation(
                    &actx,
                    Some(CancellationOrigin::FailureUnderStop),
                    false,
                    grace,
                    &mut cancel_origin,
                    &mut draining,
                    &mut stopping,
                    &mut drain_deadline,
                    &mut pending,
                    &mut in_flight,
                );
            }

            // Feed the executed-terminal outcome back into the tracker and act on
            // every decision it unlocks (ready → offer to admission or, under an
            // active stop, cancel a default-rule node / admit a firing contingency;
            // propagated → record). Under an active cancellation, a newly-ready node
            // is never admitted — it is settled `cancelled` (no new work).
            let id = NodeId::from_name(&done.node);
            let decisions = tracker.notify_terminal(id, recorded_state);
            apply_decisions(
                &actx,
                &decisions,
                writer,
                stopping,
                draining,
                &mut terminal_states,
                &mut zombie_candidates,
                &mut pending,
                &mut in_flight,
            );
            // The finished attempt released its permit (dropped in its closure
            // before it reported done), so freed capacity may now admit a waiter.
            // Re-offer the pending queue oldest-first, admitting whatever now fits.
            // Under an active stop only non-default-rule contingencies remain in
            // `pending` (the default ones were cancelled at the stop transition); a
            // full drain cancels those too and admits nothing, so it is skipped.
            if !draining {
                drain_pending(&actx, writer, &mut pending, &mut in_flight);
            }
        }

        // C16 / T35 · post-drain. If the full drain left attempts in flight past
        // grace, record each as `abandoned` and proceed — the bound that guarantees
        // the run terminates even when a task ignores cancellation.
        if draining {
            abandon_leftover(&live, writer, &mut terminal_states, &mut zombie_candidates);
        }

        // Natural run end: nothing pending, nothing in flight. Give any zombie
        // candidate (a blocking timeout whose leftover work has not confirmed
        // return — the M1 ledger that would confirm it is T31) at most the grace
        // period, then emit a zombie-at-exit event for each. This does not change
        // any node's terminal state (a timed-out node stays timed-out; an abandoned
        // node stays abandoned). On the full-drain path the drain above already
        // spent up to grace waiting for in-flight work, so this is not double-counted
        // for cancelled runs — the leftover candidates were already past grace.
        if !zombie_candidates.is_empty() {
            if !draining {
                tokio::time::sleep(grace).await;
            }
            for (node, attempt) in &zombie_candidates {
                let _ = writer.zombie_at_exit(node, *attempt);
            }
        }
    });

    // Uninstall the cancellation wake channel: the loop has ended, so a late request
    // (a signal racing shutdown, T36) must not touch this finished run's channel.
    cancel_trigger.clear_waker();

    // Shut the dispatcher's task surfaces down **without joining** any
    // abandoned-but-running (zombie) blocking/compute closure: a leftover thread
    // counts as *decided*, not in-flight, so it must not hold the run open.
    // `Runtime::drop` (and rayon's pool `Drop`) would block forever on an unkillable
    // busy closure; `shutdown_background` returns immediately, leaving any zombie to
    // be reaped by process exit (the driver already emitted its `zombie-at-exit`
    // event above). Every well-behaved attempt has already reported terminal before
    // this point.
    dispatcher.shutdown_background();

    let outcome = overall_outcome(&terminal_states);
    (outcome, terminal_states, cancel_origin)
}

/// Await the next loop message (C16 / T35). Returns the next [`AttemptDone`] — a
/// finished attempt or a `CANCEL_WAKE_SENTINEL` cancellation wake — or [`None`] to
/// stop the loop (the channel closed, or, once a full drain is under way, the grace
/// deadline elapsed with work still in flight). During the drain the wait is bounded
/// by `drain_deadline` (`now + grace`, set when the drain began), which is the bound
/// that guarantees termination even if a task ignores cancellation.
async fn recv_next(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<AttemptDone>,
    draining: bool,
    drain_deadline: Option<tokio::time::Instant>,
) -> Option<AttemptDone> {
    if draining {
        let deadline = drain_deadline.expect("deadline set when the drain began");
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        // `Ok(msg)` is the received message (or channel-closed `None`); `Err` is the
        // grace deadline firing — both stop the drain when they yield `None`.
        tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or(None)
    } else {
        rx.recv().await
    }
}

/// Write a finished attempt's buffered records and record its terminal state,
/// classifying a return that raced a full-drain cancellation (C16 / T35).
///
/// An attempt that was still in flight when the run was **externally** cancelled
/// (`draining`) and returns within grace, without having reached a terminal before
/// the drain began, is recorded `cancelled` — the run is being torn down and its
/// output is discarded regardless of the raw outcome the aborted work produced. On
/// such a reclassify the attempt's own `NodeTerminal` buffered record (its raw
/// terminal) is suppressed and the authoritative `cancelled` node-terminal is
/// emitted instead; the opening records still write, so the stream honestly shows
/// the attempt ran and was cut short. Otherwise the attempt keeps its real terminal
/// (a stop-on-first-failure keeps T34's exact outcomes). `record_terminal` is
/// exactly-once, so a late report never overwrites a prior classification. Returns
/// the recorded terminal state.
fn record_attempt_outcome<S, C>(
    done: &AttemptDone,
    draining: bool,
    writer: &mut EventStreamWriter<S, C>,
    terminal_states: &mut BTreeMap<String, TerminalState>,
    zombie_candidates: &mut Vec<(String, u32)>,
) -> TerminalState
where
    S: EventSink,
    C: MonotonicClock,
{
    let reclassified = draining && !terminal_states.contains_key(&done.node);
    let recorded_state = if reclassified {
        TerminalState::Cancelled
    } else {
        done.state
    };
    // Drain the buffered per-transition events, and — alongside each attempt's
    // CLOSING outcome event — emit the single rich `attempt-outcome` record for
    // that attempt (arch.md l.331: "Every attempt produces exactly one
    // attempt-outcome record … alongside its per-transition events"). A retried
    // node buffers several attempts, so this emits one outcome record per attempt,
    // each just before the (shared) node-terminal. The record carries the
    // attempt's status/number/panic-message the driver has; cost/metrics/worker
    // are not yet measured at M1/M2 (the C22 fold defaults each absent field).
    // This records what happened; it changes no execution behavior.
    //
    // On a drain-cancel reclassify the raw buffered node-terminal is suppressed
    // and one authoritative `cancelled` outcome + terminal are emitted instead.
    let mut last_attempt = 1;
    for ev in &done.events {
        if reclassified && matches!(ev, AttemptEvent::NodeTerminal { .. }) {
            continue;
        }
        let _ = write_attempt_event(writer, ev);
        if !reclassified {
            if let Some(record) = closing_outcome_record(&done.node, ev) {
                last_attempt = record.attempt;
                let _ = writer.attempt_outcome(record);
            }
        }
    }
    if reclassified {
        // The whole node is being torn down: one authoritative cancelled outcome.
        let attempt = attempt_number_of(&done.events);
        last_attempt = attempt;
        let _ = writer.attempt_outcome(AttemptOutcomeRecord::new(
            &done.node,
            attempt,
            wire_terminal(TerminalState::Cancelled).as_str(),
        ));
    }
    record_terminal(&done.node, recorded_state, terminal_states);
    if reclassified {
        let _ = writer.node_terminal(&done.node, wire_terminal(recorded_state));
    }
    if is_zombie_candidate(recorded_state) {
        zombie_candidates.push((done.node.clone(), last_attempt));
    }
    recorded_state
}

/// If `ev` is an attempt's **closing** outcome event (succeeded / failed /
/// timed-out / panicked — not the mid-cycle backoff marker or the node-terminal),
/// build the single `attempt-outcome` record for that attempt: its node, status
/// (the normative kebab-case token the fold reads), attempt number, and — for a
/// panic — the captured message. The richer fold fields (metrics, cost, worker,
/// durable reference) are not measured at M1/M2, so they are left absent (the
/// fold defaults each). Returns `None` for a non-closing event.
fn closing_outcome_record(node: &str, ev: &AttemptEvent) -> Option<AttemptOutcomeRecord> {
    let (attempt, status, message) = match ev {
        AttemptEvent::AttemptSucceeded { attempt, .. } => (*attempt, "succeeded", None),
        AttemptEvent::AttemptFailed { attempt, .. } => (*attempt, "failed", None),
        AttemptEvent::AttemptTimedOut { attempt, .. } => (*attempt, "timed-out", None),
        AttemptEvent::AttemptPanicked {
            attempt, message, ..
        } => (*attempt, "failed", Some(message.clone())),
        // The backoff marker is a phase, not an attempt outcome; node-terminal is
        // the node's decided state, not an attempt-outcome record.
        _ => return None,
    };
    let mut record = AttemptOutcomeRecord::new(node, attempt, status);
    record.message = message;
    Some(record)
}

/// The 1-based attempt number the buffered events name (the last-seen
/// attempt-numbered event), defaulting to 1 for a never-numbered outcome.
fn attempt_number_of(events: &[AttemptEvent]) -> u32 {
    events
        .iter()
        .rev()
        .find_map(|e| match e {
            AttemptEvent::AttemptStarted { attempt, .. }
            | AttemptEvent::AttemptSucceeded { attempt, .. }
            | AttemptEvent::AttemptFailed { attempt, .. }
            | AttemptEvent::AttemptTimedOut { attempt, .. }
            | AttemptEvent::AttemptPanicked { attempt, .. }
            | AttemptEvent::BackoffStarted { attempt, .. } => Some(*attempt),
            _ => None,
        })
        .unwrap_or(1)
}

/// Record every attempt still in flight past the cancellation grace as `abandoned`
/// (C16 / T35). Each is a node whose closure ignored cancellation and did not
/// return within grace; the driver does not wait for it. `record_terminal` is
/// exactly-once, so a node that did reach a terminal is left untouched; an
/// abandoned closure is a zombie candidate (its thread may run on and is reaped at
/// process exit — a `zombie-at-exit` event, C19).
fn abandon_leftover<S, C>(
    live: &LiveSet,
    writer: &mut EventStreamWriter<S, C>,
    terminal_states: &mut BTreeMap<String, TerminalState>,
    zombie_candidates: &mut Vec<(String, u32)>,
) where
    S: EventSink,
    C: MonotonicClock,
{
    let leftover: Vec<String> = live
        .lock()
        .expect("live set not poisoned")
        .iter()
        .cloned()
        .collect();
    for node in leftover {
        if !terminal_states.contains_key(&node) {
            record_terminal(&node, TerminalState::Abandoned, terminal_states);
            let _ = writer.node_terminal(&node, wire_terminal(TerminalState::Abandoned));
            // The M1 driver has no permit ledger to name the leftover attempt's
            // number (that is T31); a leftover attempt is attempt 1 in M1's
            // no-retry-past-abandonment model.
            zombie_candidates.push((node, 1));
        }
    }
}

/// Enter the cancellation core (arch.md `### C16`; T35). The single internal entry
/// point every cancellation origin routes through:
///
/// - **records the origin** (first cause wins) so the C26 exit-code precedence
///   (T55) can later prefer run failure over cancellation;
/// - **flips the run token** so every live per-attempt child observes cancellation
///   at once (in-flight cooperative work can return `cancelled`), exactly once and
///   idempotently;
/// - **cancels every pending default-rule node** waiting for capacity (T34's
///   resolved rule — a pending unrelated default node ends `cancelled`), while a
///   non-default-rule contingency in `pending` is kept for a stop-mode run;
/// - sets `stopping` (T34's admit-no-more-default-work rule).
///
/// `full_drain` selects the drain discipline. An **external interrupt** passes
/// `true`: the caller then enters the grace-bounded drain that admits nothing at
/// all and reclassifies in-flight returns `cancelled`/`abandoned`. A
/// **stop-on-first-failure** passes `false`: the loop keeps T34's exact behaviour
/// (firing contingencies still run, in-flight completes naturally), so a
/// non-cancelled stop run is byte-for-byte the T34 run — the core only adds the
/// token flip and the recorded origin.
#[allow(clippy::too_many_arguments)]
fn enter_cancellation(
    ctx: &AdmitCtx,
    origin: Option<CancellationOrigin>,
    full_drain: bool,
    grace: Duration,
    cancel_origin: &mut Option<CancellationOrigin>,
    draining: &mut bool,
    stopping: &mut bool,
    drain_deadline: &mut Option<tokio::time::Instant>,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) {
    let (tx, admission) = (ctx.tx, ctx.admission);
    // Record the origin once (first cause wins — a failure that then triggers a
    // later external interrupt keeps the failure origin for C26 precedence).
    if cancel_origin.is_none() {
        *cancel_origin = origin;
    }
    // Flip the run-scoped token: every live per-attempt child now observes
    // cancellation (idempotent — a second flip changes nothing).
    ctx.run_cancel.cancel();
    if full_drain {
        // Arm the single grace deadline for the whole drain and enter drain mode.
        *draining = true;
        *drain_deadline = Some(tokio::time::Instant::now() + grace);
    }
    // T34's admit-no-more-default-work + cancel-pending-unrelated-default rule. On
    // the first transition only; a repeat is a no-op (pending already partitioned).
    if !*stopping {
        *stopping = true;
        cancel_pending_default_nodes(ctx.pipeline, tx, admission, pending, in_flight);
    }
    // A full drain additionally declines to keep even the non-default-rule
    // contingencies still waiting for capacity — an external interrupt admits
    // nothing at all. (Under stop mode these are kept and re-offered by
    // `drain_pending`.) Cancelling them here settles them terminally so the run
    // does not strand them past drain end.
    if full_drain {
        let leftover: Vec<String> = pending.drain(..).collect();
        for name in leftover {
            cancel_node(&name, admission, tx, in_flight);
        }
    }
}

/// The C12 declared cost of `name`, read from its C5 node policy (T29) — the
/// per-pool demand the admission controller acquires against (arch.md C12). Reads
/// the node's `NodePolicy::cost` without duplicating the definition; an unknown
/// node (a framework defect handled downstream) demands nothing.
fn declared_cost(pipeline: &Pipeline, name: &str) -> PoolCost {
    pipeline
        .node(NodeId::from_name(name))
        .map_or_else(PoolCost::new, |n| {
            PoolCost::from_cost_vector(n.policy().cost())
        })
}

/// Offer `name` to the C12 admission controller (T31). If its declared cost fits
/// every pool it is **admitted** immediately (spawned, one more in flight); if a
/// pool is at capacity it is **held** in `pending` (oldest-ready-first) to be
/// re-offered when a release frees capacity. Under the default unconstrained pools
/// every ready node fits, so this admits at once (the M1 behaviour).
fn offer_or_pend<S, C>(
    ctx: &AdmitCtx,
    name: &str,
    writer: &mut EventStreamWriter<S, C>,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) where
    S: EventSink,
    C: MonotonicClock,
{
    let admission = ctx.admission;
    let cost = declared_cost(ctx.pipeline, name);
    match admission.try_admit(name, &cost) {
        Some(permit) => {
            admit(ctx, name, writer, permit);
            *in_flight += 1;
        }
        // The node did not fit the pool's *current* remaining capacity. It either
        // waits for a release (a fit is possible once capacity frees) or can *never*
        // fit — its declared demand exceeds a pool's TOTAL capacity, so no release
        // could ever admit it. A can-never-fit node pushed onto `pending` would sit
        // there forever: when `in_flight` reached 0 the run loop would exit, leaving
        // the node with no terminal state and reporting the run as complete — a
        // silent violation of "every reachable node reaches a terminal state". So we
        // reject it here with a defined FAILED terminal carrying the honest reason,
        // fed back through the normal terminal path (counted in flight, cascaded to
        // dependents, and folded into the run's Failed outcome) exactly as the
        // no-runner defect below. This is only the defensive driver-level guard; the
        // full bootstrap-time rejection of too-big nodes is deferred to T32.
        None if !admission.can_ever_fit(&cost) => {
            reject_over_demand(name, admission, &cost, ctx.tx);
            *in_flight += 1;
        }
        None => pending.push_back(name.to_string()),
    }
}

/// Fail a **can-never-fit** node terminally instead of stranding it (T31
/// termination guard). Its declared cost exceeds a pool's total capacity, so no
/// release could ever admit it; leaving it in `pending` would strand it past run
/// end with no terminal state. We give it a `Failed` terminal carrying the honest
/// over-demand reason and feed it back through the loop's normal terminal path
/// (via `tx`), so it is recorded, cascaded to dependents, and folds the run to a
/// `Failed` outcome — the same shape the no-runner framework-defect path uses. The
/// caller counts it into `in_flight`. Full bootstrap-time rejection is T32.
fn reject_over_demand(
    name: &str,
    admission: &AdmissionController,
    cost: &PoolCost,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
) {
    let reason = admission
        .over_demand_reason(cost)
        .unwrap_or_else(|| "declared cost exceeds pool capacity".to_string());
    eprintln!("node '{name}' can never be admitted: {reason}; failing it");
    // Carry a `NodeTerminal` record so the failure lands in the event stream (the
    // node never ran, so no attempt records exist otherwise). The loop drains this
    // into the writer, then feeds the Failed state into the tracker to cascade.
    let _ = tx.send(AttemptDone {
        node: name.to_string(),
        state: TerminalState::Failed,
        events: vec![AttemptEvent::NodeTerminal {
            node: name.to_string(),
            state: TerminalState::Failed,
        }],
    });
}

/// Whether a terminal state is **failure-like** (arch.md Vocabulary state classes;
/// T0.4 §3) — the trigger for stop-on-first-failure (C15 / T34). `cancelled`
/// (stop-like) and the skip classes never trigger a stop; only a genuine failure
/// does.
fn is_failure_like(state: TerminalState) -> bool {
    matches!(
        state,
        TerminalState::Failed
            | TerminalState::TimedOut
            | TerminalState::Abandoned
            | TerminalState::UpstreamFailed
    )
}

/// Whether `name` runs under the **default** `all-succeeded` trigger rule (C15 /
/// T34). A default-rule node is ordinary work; a **non-default**-rule node
/// (`all-terminal` / `any-failed`) is a consume-nothing contingency — the work a
/// failure is meant to trigger — which stop mode must still run. An unknown node
/// (a framework defect handled elsewhere) is treated as default-rule.
fn is_default_rule_node(pipeline: &Pipeline, name: &str) -> bool {
    pipeline
        .node(NodeId::from_name(name))
        .is_none_or(|n| n.trigger_rule() == dagr_core::binding::TriggerRule::AllSucceeded)
}

/// Mark `name` **`cancelled`** without executing it (C15 / T34 stop mode): it was
/// a pending default-rule node unrelated to the failure, or a newly-ready
/// default-rule node the stop refuses to admit. It never acquired a C12 permit
/// (never admitted), so there is nothing to release. A `NodeTerminal(cancelled)`
/// record is carried through the normal terminal path (via `tx`) so the state
/// lands in the event stream, is counted in flight, cascades to dependents, and
/// folds into the run's `cancelled`/`failed` outcome exactly like any other
/// terminal. The caller counts it into `in_flight`.
fn cancel_node(
    name: &str,
    _admission: &AdmissionController,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    in_flight: &mut usize,
) {
    let _ = tx.send(AttemptDone {
        node: name.to_string(),
        state: TerminalState::Cancelled,
        events: vec![AttemptEvent::NodeTerminal {
            node: name.to_string(),
            state: TerminalState::Cancelled,
        }],
    });
    *in_flight += 1;
}

/// At the stop-on-first-failure transition, **cancel every default-rule node still
/// waiting for capacity** (C15 / T34): these are pending nodes unrelated to the
/// failure that stop mode declines to admit. A **non-default-rule** contingency in
/// `pending` (waiting only for capacity) is kept — it is the work a failure is
/// meant to trigger and is re-offered by `drain_pending` when capacity frees.
/// Each cancelled node is fed through the normal terminal path (counted in flight,
/// cascaded), so its dependents propagate correctly.
fn cancel_pending_default_nodes(
    pipeline: &Pipeline,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    admission: &AdmissionController,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) {
    let (to_cancel, kept): (Vec<String>, Vec<String>) = pending
        .drain(..)
        .partition(|name| is_default_rule_node(pipeline, name));
    *pending = kept.into();
    for name in to_cancel {
        cancel_node(&name, admission, tx, in_flight);
    }
}

/// Re-offer the pending waiters oldest-first after a release freed capacity (T31).
/// Walks `pending` front to back; each waiter that now fits its pools is admitted
/// and removed, and a waiter that still does not fit stays queued behind its place
/// — the oldest waiter is never bypassed by a younger one that would delay it.
fn drain_pending<S, C>(
    ctx: &AdmitCtx,
    writer: &mut EventStreamWriter<S, C>,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) where
    S: EventSink,
    C: MonotonicClock,
{
    // The oldest waiter is admitted whenever it fits; a younger one bypasses only
    // when the oldest still does not fit (so admitting the younger cannot delay
    // it). This is the bounded-bypass discipline C12 mandates against starvation.
    let mut index = 0;
    while index < pending.len() {
        let name = pending[index].clone();
        let cost = declared_cost(ctx.pipeline, &name);
        if let Some(permit) = ctx.admission.try_admit(&name, &cost) {
            pending.remove(index);
            admit(ctx, &name, writer, permit);
            *in_flight += 1;
            // Restart from the front: admitting one may have consumed the capacity
            // a still-waiting older node needs, so re-check oldest-first.
            index = 0;
        } else if index == 0 {
            // The oldest waiter does not fit: do not bypass it (that could delay
            // it). Stop — nothing is admissible without risking the oldest.
            break;
        } else {
            index += 1;
        }
    }
}

/// Admit `name`: emit its `node-ready` record and **dispatch** its attempt onto the
/// execution surface named by its **effective execution class** (C13 / T33) — the
/// async task runtime for [await-bound](ExecutionClass::AwaitBound), the dedicated
/// blocking pool for [blocking](ExecutionClass::Blocking), the fixed compute pool
/// for [compute](ExecutionClass::Compute) — which reports the terminal state and
/// buffered records back over `tx` when it finishes.
///
/// The effective class is [`PipelineNode::effective_class`], which is the C5
/// policy override if one is set (validated legal at assembly by T29 — an illegal
/// override never assembles, so it never reaches here) else the class the task
/// declared ([`Task::EXECUTION_CLASS`]). Resolving it here, at dispatch time, is the
/// whole of T33's class routing.
///
/// `permit` is the C12 admission permit acquired for this attempt (T31). It is
/// **moved into the dispatched closure** — the T0.3 ownership trick — so it is
/// dropped (and its cost released to every pool) exactly when the attempt returns,
/// *before* the loop is told the attempt is done, on whichever surface ran it. That
/// is what keeps the permit held for the whole attempt and released on its terminal
/// outcome; a blocking/compute timeout zombie that runs on past its mark keeps
/// holding it until its closure actually returns (this driver does not fabricate an
/// early release, and dispatch does not change permit mechanics).
fn admit<S, C>(actx: &AdmitCtx, name: &str, writer: &mut EventStreamWriter<S, C>, permit: Permit)
where
    S: EventSink,
    C: MonotonicClock,
{
    let _ = writer.node_ready(name);
    // Node identity is name-derived (T0.7), so this is the same id assembly and the
    // tracker use — no pipeline lookup needed.
    let node_id = NodeId::from_name(name);

    // Resolve the effective execution class at dispatch (C13 / T33): the C5 policy
    // override if set (assembly already rejected any illegal override — T29), else
    // the task's declared class. An unknown node (a framework defect handled below)
    // defaults to await-bound.
    let class = actx.pipeline.node(node_id).map_or(
        ExecutionClass::AwaitBound,
        dagr_core::flow::PipelineNode::effective_class,
    );

    let Some(mut runner) = actx
        .runners
        .lock()
        .expect("runners mutex not poisoned")
        .remove(name)
    else {
        // A framework defect (no runner for an admitted node): decide it failed
        // rather than hang the run. Report it as a permanent failure terminal. The
        // permit drops here, releasing its cost (the attempt never ran).
        drop(permit);
        let _ = actx.tx.send(AttemptDone {
            node: name.to_string(),
            state: TerminalState::Failed,
            events: Vec::new(),
        });
        return;
    };

    // Register this node as in flight (C16 / T35): on cancellation the drain reads
    // this set to know which attempts to await and, past grace, abandon. Removed by
    // the loop when the attempt's `AttemptDone` arrives.
    actx.live
        .lock()
        .expect("live set not poisoned")
        .insert(name.to_string());

    // The per-attempt **child** cancellation signal (C16 / T35): each attempt
    // observes its own child of the run-scoped token, so a run cancel reaches every
    // live attempt at once while the task-facing side stays observe-only. A
    // non-cancelled run's child is never flipped, so the attempt sees exactly the
    // fresh-uncancelled signal it did before this ticket.
    let attempt_signal = actx.run_cancel.child().signal();

    let run_id = actx.run_id.to_string();
    let name_owned = name.to_string();
    let dispatcher = actx.dispatcher;
    let tx = actx.tx.clone();
    // The run's per-run temp directory (arch.md C16; T36), threaded into the
    // attempt's context so a task reaches its confined local scratch through the
    // context (`RunContext::temp_dir`). Owned into the future so it outlives `actx`.
    let temp_dir = actx.temp_dir.to_path_buf();
    // The attempt future — driven on the surface `class` names. It owns the runner,
    // the buffering sink, and the permit; producing the `(state, events)` the loop
    // records once the attempt returns.
    let attempt = async move {
        // A per-attempt buffering sink: the attempt emits into it off the
        // framework runtime; the loop drains it into the writer in order.
        let mut sink = BufferingSink::default();
        let ctx = RunContext::builder(CoreRunId::new(run_id), PipelineId::new("pipeline"), node_id)
            .cancellation(attempt_signal)
            .temp_dir(temp_dir)
            .build();
        // (C25 / T45) Open the attempt span — run/node/attempt identity — and
        // instrument the attempt future with it, so every line the task or a
        // third-party library it calls emits beneath this future carries that
        // identity across `.await` points and is attributable without timestamp
        // correlation. This attaches to (does not compete with) the C14 attempt
        // lifecycle; its identity is read off the C8 context's dep-free `LogSpan`.
        let span = crate::logging::attempt_span_from(ctx.span(), &name_owned);
        let state = runner.run(&ctx, &mut sink).instrument(span).await;
        // Release the C12 permit at the attempt's terminal state (its working
        // memory + thread cost returns to the pools) BEFORE reporting done, so the
        // loop sees freed capacity when it re-offers the pending waiters. An
        // await-bound cancellation would drop the permit with the future instead;
        // a blocking/compute-timeout zombie keeps it until its closure returns
        // (T0.3 ADR). The permit drops on whichever surface ran the attempt.
        drop(permit);
        (name_owned, state, sink.drain())
    };
    // Route by class (C13 / T33). `on_done` sends the finished attempt back to the
    // framework loop over `tx`; it runs on the surface the attempt ran on, off the
    // framework runtime, so a jammed task surface never touches the writer.
    dispatcher.dispatch(class, attempt, move |(node, state, events)| {
        let _ = tx.send(AttemptDone {
            node,
            state,
            events,
        });
    });
}

/// Act on each decision the tracker unlocked. A [`Decision::Ready`] node is
/// **offered to admission** (admitted if its pools fit, else held in `pending`); a
/// [`Decision::PropagatedTerminal`] node is recorded directly — it never executes
/// — and its cascade is already folded into the tracker, so its own dependents'
/// decisions are handled recursively here. Admitted nodes are counted into
/// `in_flight` by [`offer_or_pend`].
#[allow(clippy::too_many_arguments)]
fn apply_decisions<S, C>(
    ctx: &AdmitCtx,
    decisions: &[Decision],
    writer: &mut EventStreamWriter<S, C>,
    stopping: bool,
    draining: bool,
    terminal_states: &mut BTreeMap<String, TerminalState>,
    zombie_candidates: &mut Vec<(String, u32)>,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) where
    S: EventSink,
    C: MonotonicClock,
{
    let pipeline = ctx.pipeline;
    for decision in decisions {
        match decision {
            Decision::Ready(id) => {
                if let Some(name) = node_name(pipeline, *id) {
                    // C16 / T35: under a **full drain** (an external interrupt) no
                    // new work is admitted at all — every newly-ready node is settled
                    // `cancelled` (including a contingency). C15 / T34: under a
                    // **stop** only, a newly-ready **default-rule** node is cancelled
                    // while a **non-default-rule** contingency whose rule fired is the
                    // work a failure is meant to trigger, so it is still admitted.
                    if draining || (stopping && is_default_rule_node(pipeline, &name)) {
                        cancel_node(&name, ctx.admission, ctx.tx, in_flight);
                    } else {
                        offer_or_pend(ctx, &name, writer, pending, in_flight);
                    }
                }
            }
            Decision::PropagatedTerminal { node, state, .. } => {
                // A propagated-terminal node never executes: record its state and
                // its node-terminal record directly (the tracker already cascaded
                // it, so no further notify_terminal is needed for it here).
                if let Some(name) = node_name(pipeline, *node) {
                    let _ = writer.node_terminal(&name, wire_terminal(*state));
                    record_terminal(&name, *state, terminal_states);
                    if is_zombie_candidate(*state) {
                        // A propagated-terminal node never executed an attempt;
                        // attempt 1 is the conservative attribution.
                        zombie_candidates.push((name, 1));
                    }
                }
            }
        }
    }
}

/// Resolve a node id to its author-declared name, or `None` if it is not in the
/// pipeline.
fn node_name(pipeline: &Pipeline, id: NodeId) -> Option<String> {
    pipeline.node(id).map(|n| n.name().to_string())
}

/// Record a node's terminal state exactly once (a node's terminal state is
/// decided exactly once — Vocabulary; a repeat is a defensive no-op).
fn record_terminal(
    node: &str,
    state: TerminalState,
    terminal_states: &mut BTreeMap<String, TerminalState>,
) {
    terminal_states.entry(node.to_string()).or_insert(state);
}

/// Whether a terminal state marks a **zombie candidate** at run end: a blocking
/// timeout (or a left-behind abandoned closure) whose leftover work may still be
/// running. The M1 driver has no permit ledger to confirm the closure returned
/// (that is T31), so it treats a `timed-out`/`abandoned` node as a candidate and
/// emits a `zombie-at-exit` event for it after the bounded grace wait.
fn is_zombie_candidate(state: TerminalState) -> bool {
    matches!(state, TerminalState::TimedOut | TerminalState::Abandoned)
}

/// The overall run outcome from the per-node terminal states (arch.md Vocabulary /
/// C19): failed if any node ended failure-like, cancelled if any ended stop-like
/// (and none failure-like), else succeeded. A run containing only skips (or
/// successes) is a **successful** run.
fn overall_outcome(terminal_states: &BTreeMap<String, TerminalState>) -> RunOutcome {
    let mut any_failure = false;
    let mut any_stop = false;
    for state in terminal_states.values() {
        match state {
            TerminalState::Failed
            | TerminalState::TimedOut
            | TerminalState::Abandoned
            | TerminalState::UpstreamFailed => any_failure = true,
            TerminalState::Cancelled => any_stop = true,
            TerminalState::Succeeded
            | TerminalState::Skipped
            | TerminalState::UpstreamSkipped
            | TerminalState::SatisfiedFromPrior => {}
        }
    }
    if any_failure {
        RunOutcome::Failed
    } else if any_stop {
        RunOutcome::Cancelled
    } else {
        RunOutcome::Succeeded
    }
}

/// The **bounded final flush** at shutdown (arch.md `### C16`; C19 fsync-at-run-end;
/// T36). Perform the single run-end/cancellation `fsync` through the sink
/// (`writer.finish()`), and report whether it succeeded.
///
/// Returns `true` when the flush completed (the stream is complete and durable),
/// `false` when the sink was **unwritable at shutdown** — the distinct sink-failure
/// path. The `finish` call is itself the bounded operation: the sink's `flush`
/// either returns or errors, so the wait is bounded by the sink and never a hang;
/// the caller maps a `false` here onto [`ShutdownExit::SinkFailure`] within the
/// [final-flush budget](DEFAULT_FINAL_FLUSH). On failure a best-effort report goes
/// to stderr (operator-facing, never into the event stream), per T0.6 §5.
fn final_flush<S, C>(writer: &mut EventStreamWriter<S, C>) -> bool
where
    S: EventSink,
    C: MonotonicClock,
{
    match writer.finish() {
        Ok(()) => true,
        Err(fault) => {
            // Best-effort stderr report; do not hang, do not pretend success.
            eprintln!("final flush failed at shutdown: {fault}");
            false
        }
    }
}

/// Select the C26 [shutdown exit](ShutdownExit) by precedence (arch.md C16 / C26;
/// T36): run failure > sink failure > cancellation > success.
///
/// `outcome` is the overall run outcome, `origin` the recorded cancellation origin
/// (if any), and `flush_ok` whether the [bounded final flush](final_flush)
/// succeeded. A run failure (a non-teardown node ended `failed`/`timed-out`, which
/// also covers a `FailureUnderStop` cancellation) wins over everything; otherwise a
/// failed final flush is the distinct sink-failure code; otherwise an external
/// interrupt is a cancellation; otherwise success. The driver reports this — T55
/// owns the numeric mapping.
fn select_shutdown_exit(
    outcome: RunOutcome,
    origin: Option<CancellationOrigin>,
    flush_ok: bool,
) -> ShutdownExit {
    // 1. Run failure wins (a genuine node failure, incl. a stop-on-first-failure
    //    cancellation whose origin is a failure; an assembly/bootstrap failure is
    //    likewise a run failure for exit-code purposes — the full C26 code table and
    //    its distinct assembly/bootstrap codes are T55's, so they fold under
    //    `RunFailure` here, which this ticket does not claim to enumerate).
    let failed = matches!(
        outcome,
        RunOutcome::Failed | RunOutcome::AssemblyFailed | RunOutcome::BootstrapFailed
    ) || origin == Some(CancellationOrigin::FailureUnderStop);
    if failed {
        return ShutdownExit::RunFailure;
    }
    // 2. Sink failure at shutdown — distinct from a run failure.
    if !flush_ok {
        return ShutdownExit::SinkFailure;
    }
    // 3. Cancellation by external interrupt with a writable stream.
    if origin == Some(CancellationOrigin::ExternalInterrupt)
        || matches!(outcome, RunOutcome::Cancelled)
    {
        return ShutdownExit::Cancelled;
    }
    // 4. A clean success.
    ShutdownExit::Success
}
