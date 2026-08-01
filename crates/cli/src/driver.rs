//! The **run-loop driver** — the component that orchestrates one complete run
//! from an assembled pipeline to a truthful end.
//!
//! # What the driver does
//!
//! The driver is the seam where the run pieces become an actual run. It:
//!
//! 1. mints [run identity](RunId) as a `UUIDv7` at bootstrap (operator-overridable
//!    via [`RunConfig::run_id`]) and opens the run store and event stream
//!    **before** assembly executes, so even an assembly failure still has a place
//!    to record itself;
//! 2. captures the allowlisted environment values declared at pipeline
//!    construction (empty by default) and emits the `run-started` header carrying
//!    every field known at start (identity, pipeline identity, both fingerprints,
//!    parameters/data interval, captured environment);
//! 3. drives the execution loop: it admits the ready nodes the
//!    [`ReadinessTracker`] reports,
//!    **dispatches** each admitted node's attempt through the attempt runner
//!    onto the execution surface named by its **effective execution class**
//!    (await-bound on the async task runtime, blocking on the dedicated
//!    blocking pool, compute on the fixed rayon pool), and feeds every terminal
//!    outcome back into the tracker so dependents decrement and either become ready
//!    or receive their propagated terminal state — **never batching a whole level
//!    into a wave**;
//! 4. runs its own machinery (the loop, timers, cancellation fan-out, the
//!    event-stream writer) on the **isolated framework runtime**, kept off every
//!    task-execution surface so a misbehaving task cannot disable the loop, the
//!    timeout, or the event stream;
//! 5. terminates **exactly** when nothing is pending and nothing is in flight —
//!    where an abandoned-but-running closure counts as *decided*, not in-flight:
//!    at natural run end it waits a bounded grace period for any zombie closures
//!    to return, emits a `zombie-at-exit` event for each that does not, then emits
//!    `run-finished` and returns;
//! 6. surfaces the run's overall [outcome](RunOutcome) so the caller (the run
//!    verb) can select the exit code — the driver reports the outcome, it does
//!    **not** own the exit-code table.
//!
//! # Mutex poisoning
//!
//! **Poison policy: panic**, at every lock in this module — the cancel-waker, the
//! cancel-origin, the buffered event records, the live set, and the runner map.
//! The workspace rule is *recover where user-or-defect code can panic while the
//! lock is held, panic otherwise*
//! ([`dagr_core::slot`] and [`crate::signals`] are the recovering half, and say
//! why at their locks). Every lock here is held across a short bookkeeping
//! mutation with no task body, no user callback, and no defect assertion beneath
//! it — so a poisoned lock cannot mean "a task panicked", only "the framework
//! panicked inside its own critical section and left this state half-written".
//! Continuing on that state would corrupt the run record it exists to produce
//! (the wrong abandoned set, a duplicated runner, an inconsistent event stream),
//! which is strictly worse than failing loudly. Each site restates the reason in
//! one line so a reader never has to come back here to know which policy applies.
//!
//! # Execution-class dispatch + the isolated framework runtime
//!
//! The framework machinery runs on an **isolated** runtime, separate from every
//! surface task attempts execute on. The driver builds an execution-class
//! `Dispatcher` owning the **three task surfaces** — the async tokio task runtime
//! (await-bound work), tokio's dedicated blocking pool via `spawn_blocking`
//! (blocking work), and a dedicated fixed-size `rayon` compute pool (compute work)
//! — plus a separate one-worker `framework` runtime that drives the loop, the
//! per-attempt timers, and the event writer. Each admitted node's attempt is
//! **dispatched by its effective execution class** (the policy override if set —
//! validated legal at assembly — else the task's declared execution class
//! `Task::EXECUTION_CLASS`), so blocking work never starves the async workers and
//! compute concurrency is bounded structurally by the rayon pool's fixed size. A
//! task that jams every task/blocking/compute worker (a synchronous busy-loop)
//! still cannot stall the framework runtime — the per-attempt timeout still fires
//! and the event stream is still written (the all-workers-blocked scenario).
//!
//! # The termination condition
//!
//! The run ends **precisely** when the tracker reports nothing pending and the
//! driver holds nothing in flight. A node whose attempt was abandoned-but-running
//! at a blocking timeout is *decided* (its terminal state is fixed) the moment the
//! timeout marks it, so it does not hold the run open; its leftover thread is
//! given at most the [grace period](RunConfig::grace) to return and, if it does
//! not, a `zombie-at-exit` event is emitted for it before `run-finished`. This is
//! the *"nothing pending and nothing in flight"* half of the run-end condition
//! (the tracker owns the *"nothing pending"* half).
//!
//! # Scope
//!
//! This is the minimal readiness-driven loop, nothing more. It is **not** a
//! scheduler; it admits the nodes the tracker/runner hand it against the admission
//! surface and dispatches each by its execution class.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, ConsumedInput, Event, EventSink, EventStreamWriter,
    FINGERPRINT_ALGORITHM_VERSION, MonotonicClock, OutputProducedRecord, RunOutcome,
    RunStartedHeader, TerminalState as WireTerminalState,
};
pub use dagr_artifact::event_stream::{RunId, RunOutcome as OverallOutcome};
use dagr_core::admission::{
    AdmissionController, Permit, PlacementHandling, PoolCapacities, PoolCost,
};
use dagr_core::assembly::AssemblyError;
use dagr_core::context::{
    CancellationOrigin, CancellationSource, CoveredNodeStates, PipelineId, RunContext,
    RunId as CoreRunId, TerminalState,
};
use dagr_core::execution::{AttemptEvent, AttemptEventSink};
use dagr_core::flow::{FailureMode, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::limits::detect_capacities;
use dagr_core::readiness::{Decision, ReadinessTracker};
use dagr_core::task::ExecutionClass;
use tracing::Instrument;

use crate::dispatch::{Dispatcher, Surface};
use crate::executor::ExecutorKind;

/// The thread execution **surface** a unit of work ran on — the observable half of
/// the class→surface routing. Await-bound work runs on
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

/// The execution [surface](ExecutionSurface) the **calling** code is running on.
/// A task's work can call this to observe which surface
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
/// the cancellation drain. A blocking timeout's leftover thread — or an
/// await-bound attempt asked to stop — is given at most this long before it is
/// left behind (`zombie-at-exit` at natural end, or `abandoned` on the
/// cancellation path) and the run proceeds.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(10);

/// The default teardown deadline: the wall-clock budget a teardown phase is
/// allowed under its own fresh, uncancelled signal. Also **consumed** for the
/// worst-case [shutdown-budget](ShutdownBudget) arithmetic printed at startup.
pub const DEFAULT_TEARDOWN_DEADLINE: Duration = Duration::from_secs(15);

/// The bounded final event-stream flush allowance: the last, bounded window the
/// process spends flushing the event stream before exit. Folded into the printed
/// [shutdown budget](ShutdownBudget).
pub const DEFAULT_FINAL_FLUSH: Duration = Duration::from_secs(2);

/// The worst-case **shutdown budget**: grace + teardown deadline + bounded final
/// flush. The binary prints this at startup so a
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
    /// The cooperative grace period component (the operator `--grace` flag).
    #[must_use]
    pub fn grace(&self) -> Duration {
        self.grace
    }

    /// The teardown-deadline component (consumed here for the arithmetic).
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
/// `grace` and `teardown_deadline` flag values: grace + teardown deadline + a
/// fixed [bounded final flush](DEFAULT_FINAL_FLUSH). The binary prints this at
/// startup so a misconfiguration is visible before it matters.
#[must_use]
pub fn shutdown_budget(grace: Duration, teardown_deadline: Duration) -> ShutdownBudget {
    ShutdownBudget {
        grace,
        teardown_deadline,
        final_flush: DEFAULT_FINAL_FLUSH,
    }
}

/// The **shutdown exit selection** a completed drive surfaces for the exit-code
/// contract.
///
/// The driver **reports** which of these applies; it does **not** own the numeric
/// code table. The selection follows this precedence:
///
/// 1. [`RunFailure`](ShutdownExit::RunFailure) — a non-teardown node ended
///    `failed`/`timed-out` (a genuine run failure). **Highest precedence:** a run
///    failure wins over cancellation *and* over a sink failure at shutdown.
/// 2. [`SinkFailure`](ShutdownExit::SinkFailure) — the event sink was unwritable at
///    the final flush. Distinct from a run failure: the failure to *record* is a
///    sink fault (event stream unwritable), not a node ending failed. Reported
///    only when no node failed; the process waited a **bounded** time for the flush
///    and did not hang.
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
/// [`RunConfig::cancel_handle`].
///
/// This is the internal cancellation entry point exercised from a test or, in
/// production, the seam an OS-signal handler drives — *not* an OS-signal wiring
/// itself. Firing it ([`cancel`](Self::cancel)) requests
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
        // Poison policy: panic — a single `Option` assignment runs under this lock
        // and nothing else, so poisoning can only mean the framework panicked
        // inside its own critical section (see the module rule).
        *self.waker.lock().expect("cancel-waker mutex not poisoned") = Some(tx);
    }

    /// Uninstall the wake channel at run end, so a late request cannot touch a
    /// finished loop.
    fn clear_waker(&self) {
        // Poison policy: panic — same lock, same reason as `install_waker`.
        *self.waker.lock().expect("cancel-waker mutex not poisoned") = None;
    }

    /// Record a cancellation request with `origin` (first request wins the origin)
    /// and wake the loop by pushing the sentinel onto its channel. Idempotent on the
    /// origin; a request after the loop ended is a harmless no-op.
    fn request(&self, origin: CancellationOrigin) {
        {
            // Poison policy: panic — the set-once origin decides the run's exit
            // code; recovering a half-written one would report the wrong cause.
            let mut guard = self
                .origin
                .lock()
                .expect("cancel-origin mutex not poisoned");
            if guard.is_none() {
                *guard = Some(origin);
            }
        }
        // Poison policy: panic — the waker lock, as above.
        if let Some(tx) = self
            .waker
            .lock()
            .expect("cancel-waker mutex not poisoned")
            .as_ref()
        {
            let _ = tx.send(AttemptDone::attempt(
                CANCEL_WAKE_SENTINEL.to_string(),
                TerminalState::Cancelled,
                Vec::new(),
            ));
        }
    }

    /// The recorded origin, or `None` if no cancellation was requested.
    fn recorded_origin(&self) -> Option<CancellationOrigin> {
        // Poison policy: panic — the origin lock, as in `request`.
        *self
            .origin
            .lock()
            .expect("cancel-origin mutex not poisoned")
    }
}

// ===========================================================================
// Queue-depth observability (T97)
// ===========================================================================

/// An observer of the run loop's `AttemptDone` queue, recording the **peak
/// occupancy** the loop ever sees.
///
/// The queue is deliberately unbounded (see the comment at its construction site in
/// [`run_loop`]), and the argument for that is an invariant rather than a capacity.
/// An invariant nothing measures is a comment, so this probe exists to let a test
/// *measure* the depth instead of inferring it from the loop's own bookkeeping.
/// Install one with [`RunConfig::attempt_queue_probe`]; when none is installed the
/// loop does no extra work at all.
///
/// This is an observability seam for dagr's own tests, not part of the supported
/// API — it is hidden from the documented surface and may change without notice.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct AttemptQueueProbe {
    peak: std::sync::atomic::AtomicUsize,
}

impl AttemptQueueProbe {
    /// A fresh probe that has observed nothing (peak zero).
    #[doc(hidden)]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The greatest queue occupancy observed so far, counting the message being
    /// dequeued. Zero means the probe was never consulted — which a test asserting
    /// a bound must treat as a failure, not a pass.
    #[doc(hidden)]
    #[must_use]
    pub fn peak_depth(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Fold one observation into the running maximum.
    fn observe(&self, depth: usize) {
        self.peak
            .fetch_max(depth, std::sync::atomic::Ordering::Relaxed);
    }
}

// ===========================================================================
// Configuration
// ===========================================================================

/// The bootstrap configuration for one run.
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
    executor: ExecutorKind,
    // The programmatic cancellation trigger: a shared request channel a caller (a
    // test, or an OS-signal handler) fires and the run loop observes. Cloned into
    // the loop; a `CancelHandle` handed out by `cancel_handle` shares the same
    // `Arc`. Never serialized/compared.
    cancel_trigger: Arc<CancelTrigger>,
    // An optional observer of the loop's `AttemptDone` queue depth. `None` for
    // every production run — the loop then does no extra work — and installed by a
    // test that needs to measure the queue rather than infer it.
    attempt_queue_probe: Option<Arc<AttemptQueueProbe>>,
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
            // Admission pools default to **unconstrained**. An unconstrained
            // controller admits every ready node immediately, so the run loop's
            // behaviour is unchanged unless a capacity is pinned.
            capacities: PoolCapacities::new(),
            // The failure mode defaults to continue-independent: a failure cancels
            // nothing, so an unset mode leaves the loop's behaviour unchanged.
            // Stop-on-first-failure is opt-in.
            failure_mode: FailureMode::default(),
            // Every node runs in this process unless an operator selects otherwise.
            // A node's placement is then recorded and ignored, which is what lets one
            // binary be both a laptop run and a placed one.
            executor: ExecutorKind::Local,
            // A fresh, un-fired cancellation trigger. Unless a caller fires the
            // handle (or stop-on-first-failure routes through the core), the run is
            // never cancelled and its behaviour is unchanged.
            cancel_trigger: Arc::new(CancelTrigger::new()),
            // No queue observer: the loop's receive path is exactly what it was.
            attempt_queue_probe: None,
        }
    }

    /// Install an [`AttemptQueueProbe`] so the loop reports the peak occupancy of
    /// its `AttemptDone` queue.
    ///
    /// An observability seam for dagr's own tests (the channel is unbounded by
    /// design and the bound that makes that safe deserves to be measured, not
    /// asserted in prose). Not part of the supported API.
    #[doc(hidden)]
    #[must_use]
    pub fn attempt_queue_probe(mut self, probe: Arc<AttemptQueueProbe>) -> Self {
        self.attempt_queue_probe = Some(probe);
        self
    }

    /// Override the minted run identity with an operator-supplied value, used
    /// **verbatim** everywhere the minted id would appear.
    #[must_use]
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Set the cooperative **grace period** (default [`DEFAULT_GRACE`], 10 s). It
    /// bounds *both* the zombie wait at natural run end and the cancellation drain
    /// wait for in-flight cooperative work, and it drives the printed
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

    /// Set the **teardown deadline** (default [`DEFAULT_TEARDOWN_DEADLINE`], 15 s).
    /// Consumed for the worst-case [shutdown-budget](ShutdownBudget) arithmetic
    /// printed at startup, and bounds each teardown attempt run at run end under its
    /// own fresh, uncancelled signal.
    #[must_use]
    pub fn teardown_deadline(mut self, deadline: Duration) -> Self {
        self.teardown_deadline = deadline;
        self
    }

    /// The **effective** teardown deadline this run will honour (the override if
    /// set, else [`DEFAULT_TEARDOWN_DEADLINE`], 15 s). Bounds each teardown attempt
    /// run at run end under its own fresh, uncancelled signal.
    #[must_use]
    pub fn effective_teardown_deadline(&self) -> Duration {
        self.teardown_deadline
    }

    /// The worst-case [shutdown budget](ShutdownBudget) for this run: the effective
    /// grace + the teardown deadline + the bounded final flush. Printed at startup.
    #[must_use]
    pub fn shutdown_budget(&self) -> ShutdownBudget {
        shutdown_budget(self.grace, self.teardown_deadline)
    }

    /// Obtain the programmatic **cancellation trigger** for this run. Firing the
    /// returned [`CancelHandle`] requests cancellation with an external-interrupt
    /// origin; the driver stops admitting new work, drains in-flight work within the
    /// grace period, and terminates. This is the internal entry point a test drives
    /// and the seam an OS-signal handler fires. Multiple handles may be obtained;
    /// they all drive the same run.
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

    /// Pin the run's admission-pool capacities. The default is **unconstrained**
    /// (every ready node admitted at once); pinning a pool bounds admission against
    /// it. Container-limit derivation of these capacities is the
    /// [`ContainerLimitProbe`](dagr_core::limits::ContainerLimitProbe)
    /// (cgroup v2 → v1 → host, with the 20% headroom default); pass its
    /// [`detect`](dagr_core::limits::ContainerLimitProbe::detect) output here to
    /// size the pools from the machine, or a pinned set (the operator flag, which
    /// is also how CI makes capacity deterministic).
    #[must_use]
    pub fn capacities(mut self, capacities: PoolCapacities) -> Self {
        self.capacities = capacities;
        self
    }

    /// Select the run-level [failure mode](FailureMode). This is the driver-side
    /// override seam the builder/assembly mode
    /// ([`Flow::failure_mode`](dagr_core::flow::Flow::failure_mode)) feeds and the
    /// operator/CLI override feeds too, without a signature change. The default is
    /// [`ContinueIndependent`](FailureMode::ContinueIndependent) — a failure cancels
    /// nothing — so leaving it unset preserves the run loop's behaviour exactly.
    #[must_use]
    pub fn failure_mode(mut self, mode: FailureMode) -> Self {
        self.failure_mode = mode;
        self
    }

    /// The **effective** run-level [failure mode](FailureMode) this run will honour
    /// (the override if set, else
    /// [`ContinueIndependent`](FailureMode::ContinueIndependent)).
    #[must_use]
    pub fn effective_failure_mode(&self) -> FailureMode {
        self.failure_mode
    }

    /// Select the [executor](ExecutorKind) that runs this invocation's node
    /// attempts — the driver-side seam the resolved `--dagr.executor` /
    /// `DAGR_EXECUTOR` knob feeds ([`resolve_executor`](crate::config::resolve_executor)).
    ///
    /// The default is [`Local`](ExecutorKind::Local), so leaving it unset preserves
    /// the run loop's behaviour exactly. Selecting an executor this build does not
    /// implement is refused at **bootstrap** with the executor's own diagnostic — a
    /// `bootstrap-failed` run with zero attempts, never a quiet local substitution.
    #[must_use]
    pub fn executor(mut self, executor: ExecutorKind) -> Self {
        self.executor = executor;
        self
    }

    /// The **effective** [executor](ExecutorKind) this run will use (the selection
    /// if set, else [`Local`](ExecutorKind::Local)).
    #[must_use]
    pub fn effective_executor(&self) -> ExecutorKind {
        self.executor
    }

    /// Set the **grace period** from `flag > env > default`: the already-parsed
    /// `--grace` flag `flag` if present, else the `DAGR_GRACE` environment variable,
    /// else the [default](DEFAULT_GRACE) (10 s).
    ///
    /// This is one of the three opt-in, **fallible** env-fallback builders (grace,
    /// teardown-deadline, failure-mode). A binary that wants the standard
    /// `flag > env > default` behaviour calls this instead of [`grace`](Self::grace);
    /// [`RunConfig::new`](Self::new) stays infallible and env-free, and this method
    /// is the *only* place `DAGR_GRACE` is read (in `dagr-cli`, never `dagr-core`).
    ///
    /// # Errors
    ///
    /// Returns an [`EnvParseError`](crate::config::EnvParseError) naming `DAGR_GRACE`
    /// (kind `Parse` → [`InvalidUsage`](crate::contract::ExitCode::InvalidUsage))
    /// when the environment value is not a duration — a bad env value fails loudly
    /// and is never silently ignored.
    pub fn grace_from_env(
        self,
        flag: Option<Duration>,
    ) -> Result<Self, crate::config::EnvParseError> {
        let resolved = crate::config::resolve::<crate::config::EnvDuration>(
            flag.map(crate::config::EnvDuration),
            crate::config::DAGR_GRACE,
            crate::config::EnvDuration(DEFAULT_GRACE),
        )?;
        Ok(self.grace(resolved.into_inner()))
    }

    /// Set the **teardown deadline** from `flag > env > default`: the already-parsed
    /// `--teardown-deadline` flag if present, else `DAGR_TEARDOWN_DEADLINE`, else the
    /// [default](DEFAULT_TEARDOWN_DEADLINE) (15 s).
    ///
    /// # Errors
    ///
    /// Returns an [`EnvParseError`](crate::config::EnvParseError) naming
    /// `DAGR_TEARDOWN_DEADLINE` (kind `Parse` → `InvalidUsage`) when the environment
    /// value is not a duration.
    pub fn teardown_deadline_from_env(
        self,
        flag: Option<Duration>,
    ) -> Result<Self, crate::config::EnvParseError> {
        let resolved = crate::config::resolve::<crate::config::EnvDuration>(
            flag.map(crate::config::EnvDuration),
            crate::config::DAGR_TEARDOWN_DEADLINE,
            crate::config::EnvDuration(DEFAULT_TEARDOWN_DEADLINE),
        )?;
        Ok(self.teardown_deadline(resolved.into_inner()))
    }

    /// Set the run-level **failure mode** from `flag > env > default`: the
    /// already-parsed `--failure-mode` flag if present, else `DAGR_FAILURE_MODE`,
    /// else the default
    /// ([`ContinueIndependent`](FailureMode::ContinueIndependent)).
    ///
    /// # Errors
    ///
    /// Returns an [`EnvParseError`](crate::config::EnvParseError) naming
    /// `DAGR_FAILURE_MODE` (kind `Parse` → `InvalidUsage`) when the environment
    /// value is neither `continue-independent` nor `stop-on-first-failure`.
    pub fn failure_mode_from_env(
        self,
        flag: Option<FailureMode>,
    ) -> Result<Self, crate::config::EnvParseError> {
        let resolved = crate::config::resolve::<crate::config::EnvFailureMode>(
            flag.map(crate::config::EnvFailureMode),
            crate::config::DAGR_FAILURE_MODE,
            crate::config::EnvFailureMode(FailureMode::default()),
        )?;
        Ok(self.failure_mode(resolved.into_inner()))
    }

    /// The resolved run identity: the operator override verbatim if present, else
    /// a freshly-minted `UUIDv7`.
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

/// A run's type-erased [node runners](NodeRunner), keyed by node name — the map
/// the driver splits (main vs teardown) and the loop/teardown phase consume.
type RunnerMap = BTreeMap<String, Box<dyn NodeRunner>>;

/// A single node's **type-erased attempt path** — what the driver spawns for an
/// admitted node.
///
/// A pipeline's nodes have heterogeneous output types, so the run loop cannot be
/// generic over one `T`; instead each node is presented to the driver as a boxed
/// `NodeRunner`. The runner owns its task, its output [slot](dagr_core::slot::Slot),
/// and its input wiring (the upstream slot references it reads), and it exposes a
/// single operation: run the node to its terminal state, emitting the attempt
/// records through the injected sink.
///
/// The driver supplies the per-attempt [`RunContext`] (carrying run/pipeline/node
/// identity) and the sink; the runner drives the attempt path (the caught
/// single-attempt/retry runner) and returns the node's normative
/// [`TerminalState`]. Reading inputs from upstream slots is the runner's concern —
/// by the time the driver admits a node, every upstream has succeeded, so the
/// upstream slots are filled.
pub trait NodeRunner: Send {
    /// The node's author-declared identity name — keys every emitted record.
    fn name(&self) -> &str;

    /// Run this node to its terminal state, emitting the attempt records
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
    /// on the framework runtime (the writer is single-owner); the runner never
    /// touches the real writer.
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>>;

    /// The **durable reference** this node's succeeded attempt recorded, or
    /// [`None`] for a non-durable node (an in-memory value that cannot be
    /// rehydrated). Read by the driver **after** a successful attempt and stamped
    /// into that attempt's `attempt-outcome` record (via
    /// `dagr_artifact::event_stream::record_durable_reference`), so a later resume
    /// finds the reference in the folded prior artifact and can rehydrate the value.
    ///
    /// The default is [`None`] — every existing runner is a non-durable node and its
    /// stream is byte-identical. A durable node's runner overrides this to return the
    /// reference its output type serialized (the durable-output contract). This is
    /// the minimal seam that lets a **durable producer record its reference through
    /// the real `drive()` loop** — the resume gate demo's stage boundary depends on
    /// it.
    fn durable_reference(&self) -> Option<String> {
        None
    }

    /// The **optional durable-reference metadata** this node's succeeded attempt
    /// recorded (T89), or [`None`] when the durable output supplied none (or the
    /// node is non-durable). Read by the driver **after** a successful attempt,
    /// alongside [`durable_reference`](NodeRunner::durable_reference), and stamped
    /// onto that attempt's `attempt-outcome` record via
    /// `dagr_artifact::event_stream::record_durable_reference_meta`, so a later
    /// resume can verify the referent was not overwritten out-of-band.
    ///
    /// The default is [`None`] — every existing runner reports no metadata and its
    /// stream is byte-identical. A durable runner overrides this to supply the
    /// metadata its output type produced (the additive
    /// [`DurableOutput::durable_reference_meta`](dagr_core::assembly::DurableOutput::durable_reference_meta)
    /// contract).
    fn durable_reference_meta(&self) -> Option<dagr_artifact::event_stream::DurableReferenceMeta> {
        None
    }

    /// The **per-attempt timeout fate cell** this runner shares with the driver for
    /// an *unkillable* (blocking / compute) node whose policy declares a timeout, or
    /// [`None`] for every other runner.
    ///
    /// A blocking or compute closure cannot be killed, so the driver arms that
    /// node's deadline on the **isolated framework runtime** (a jammed task worker
    /// cannot delay it) and, when it fires, decides the node's fate through this
    /// cell: it claims the timeout, marks the attempt `timed-out`, and hands the
    /// runner the [`LateResultBarrier`](dagr_core::execution::LateResultBarrier) that
    /// refuses the abandoned closure's late slot fill. The runner claims the cell
    /// from the other side the moment its body returns, so **exactly one** side wins
    /// and the node's terminal state is decided once.
    ///
    /// The default is [`None`]: an await-bound runner arms its own deadline (its
    /// future can truly be dropped), and a runner whose node declares no timeout has
    /// no deadline to arm — in both cases the driver arms no timer and the run is
    /// byte-identical.
    fn timeout_fate(&self) -> Option<Arc<AttemptFate>> {
        None
    }
}

/// The **exactly-once fate** of one unkillable (blocking / compute) attempt that
/// runs under a per-attempt timeout — the hand-off between the driver's isolated
/// deadline timer and the runner whose closure it cannot stop.
///
/// A blocking/compute closure runs on past its timeout as *abandoned-but-running*
/// work (C14). Two parties may therefore reach the attempt's fate at nearly the same
/// instant: the framework timer at the deadline, and the closure when it finally
/// returns. Exactly one must win, because the node's terminal state is decided
/// **once**:
///
/// - the **timer** wins ⇒ the attempt is marked `timed-out` immediately, the permit
///   stays held until the closure actually returns, and the closure's late result is
///   refused through the [`LateResultBarrier`](dagr_core::execution::LateResultBarrier)
///   stored here — it never fills the slot;
/// - the **closure** wins ⇒ it completed inside its budget, fills the slot as usual,
///   and the timer's mark is discarded (nothing is written).
pub struct AttemptFate {
    state: Mutex<FateState>,
}

/// The three states of an [`AttemptFate`]. A node's attempts move `Running` →
/// `Completed` → `Running` … as its retry loop turns; the first party to reach
/// `TimedOut` ends the sequence, and the cell never leaves it.
enum FateState {
    /// An attempt is live: whichever party claims next decides it.
    Running,
    /// The live attempt's closure returned inside its budget — its result stands.
    /// The next attempt (if the retry budget allows one) re-enters `Running`.
    Completed,
    /// The deadline fired while an attempt was live: the node is `timed-out`, and
    /// the barrier refuses whatever the abandoned closure produces afterwards.
    TimedOut(dagr_core::execution::LateResultBarrier),
}

impl AttemptFate {
    /// A fresh fate cell with its first attempt live, shared between the runner and
    /// the driver.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FateState::Running),
        })
    }

    /// Announce that a **further attempt** of the same node is now live (the runner
    /// calls this as each attempt begins). A cell already claimed by the timeout
    /// stays claimed — the node is decided and no attempt of it can undo that.
    ///
    /// # Panics
    ///
    /// Panics only on a framework defect: a poisoned fate cell (a panic left a
    /// node's fate half-decided), which must surface rather than risk recording
    /// two terminal states or none.
    pub fn attempt_begins(&self) {
        // Poison policy: panic — this cell decides a node's terminal state; a
        // half-decided fate could record two terminals or none.
        let mut state = self.state.lock().expect("attempt fate not poisoned");
        if matches!(*state, FateState::Completed) {
            *state = FateState::Running;
        }
    }

    /// The **driver's** claim, made at the deadline with the barrier minted by
    /// [`TimeoutDecision::mark_blocking_timed_out`](dagr_core::execution::TimeoutDecision::mark_blocking_timed_out).
    /// Returns `true` when the timeout won (the mark stands and must be recorded)
    /// and `false` when no attempt was live to claim (the closure had already
    /// returned) — in which case the mark is discarded and nothing is written.
    ///
    /// # Panics
    ///
    /// Panics only on a framework defect: a poisoned fate cell (a panic left a
    /// node's fate half-decided), which must surface rather than risk recording
    /// two terminal states or none.
    #[must_use]
    pub fn claim_timeout(&self, barrier: dagr_core::execution::LateResultBarrier) -> bool {
        // Poison policy: panic — as above.
        let mut state = self.state.lock().expect("attempt fate not poisoned");
        match *state {
            FateState::Running => {
                *state = FateState::TimedOut(barrier);
                true
            }
            FateState::Completed | FateState::TimedOut(_) => false,
        }
    }

    /// The **runner's** claim, made the instant an attempt's body returns and before
    /// its value may reach the output slot. Returns [`None`] when the closure won
    /// (the value stands) and `Some(barrier)` when the timeout already marked the
    /// node — the caller must route the value through that barrier, which refuses it.
    ///
    /// # Panics
    ///
    /// Panics only on a framework defect: a poisoned fate cell (a panic left a
    /// node's fate half-decided), which must surface rather than risk recording
    /// two terminal states or none.
    #[must_use]
    pub fn claim_completion(&self) -> Option<dagr_core::execution::LateResultBarrier> {
        // Poison policy: panic — as above.
        let mut state = self.state.lock().expect("attempt fate not poisoned");
        match *state {
            FateState::Running => {
                *state = FateState::Completed;
                None
            }
            FateState::Completed => None,
            FateState::TimedOut(barrier) => Some(barrier),
        }
    }

    /// Whether the deadline has claimed this node — the runner's signal that the
    /// node is already decided, so no further attempt may start.
    ///
    /// # Panics
    ///
    /// Panics only on a framework defect: a poisoned fate cell (a panic left a
    /// node's fate half-decided), which must surface rather than risk recording
    /// two terminal states or none.
    #[must_use]
    pub fn is_timed_out(&self) -> bool {
        matches!(
            *self.state.lock().expect("attempt fate not poisoned"),
            FateState::TimedOut(_)
        )
    }
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
    /// Run-level **ordering upstreams**: a node name → the names of nodes it must
    /// run *after* even though it consumes no value from them. This is how a
    /// consume-nothing node with a non-default trigger rule
    /// (`all-terminal` / `any-failed`) acquires the upstreams its rule is evaluated
    /// against — the runtime firing of the non-default rules. Empty for a plan
    /// built with [`new`](RunPlan::new). This seam seeds only the readiness
    /// tracker's dependency structure, touching neither the graph artifact nor the
    /// fingerprint.
    ordering: BTreeMap<String, Vec<String>>,
    /// **Resume pre-satisfied nodes**: node name → the durable reference its prior
    /// success recorded (or `None` for an undemanded/non-durable prior success).
    /// Each is a node the resume plan left `satisfied-from-prior` — it does **not**
    /// re-execute (it has no runner in `runners`), and it carries no data-upstream
    /// re-run. Before the loop starts the driver records each terminal
    /// `satisfied-from-prior` (a success-like state), so its dependents in the
    /// must-run set become ready and its recorded durable reference is copied onto its
    /// `attempt-outcome` record. When a demanded consumer reads a satisfied producer's
    /// value, its output slot is pre-filled by rehydration by the caller (the resume
    /// driver). **Empty for a non-resume run** — the loop is then byte-for-byte the
    /// non-resume run.
    pre_satisfied: BTreeMap<String, Option<String>>,
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
            pre_satisfied: BTreeMap::new(),
        }
    }

    /// Build a run plan that additionally declares run-level **ordering
    /// upstreams**: `ordering` maps a node's name to the names of nodes it must run
    /// *after* without consuming their value.
    ///
    /// This is the run-level seam a consume-nothing node with a non-default trigger
    /// rule uses to be ordered after the nodes its rule watches (a notify-on-failure
    /// or cleanup contingency ordered after the work it guards) — the runtime firing
    /// of `all-terminal` / `any-failed`. It seeds the readiness tracker via
    /// [`ReadinessTracker::new_with_ordering`](dagr_core::readiness::ReadinessTracker::new_with_ordering)
    /// and touches neither the graph artifact nor the fingerprint.
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
            pre_satisfied: BTreeMap::new(),
        }
    }

    /// Declare the run's **resume pre-satisfied nodes**: `pre_satisfied` maps each
    /// node the resume plan left `satisfied-from-prior` to the durable reference its
    /// prior success recorded (or `None` for an undemanded/non-durable prior
    /// success).
    ///
    /// This is the **resume driver seam** the resume gate demo composes: it lets a
    /// resumed run drive only the **must-run subset** (the `runners` map) while the
    /// satisfied-from-prior nodes are pre-seeded terminal, so their dependents become
    /// ready without re-executing and a demanded durable producer's slot is filled by
    /// rehydration (the caller pre-fills the slot before drive). A pre-satisfied node
    /// must **not** also have a runner. An empty map yields exactly the
    /// [`with_ordering`](Self::with_ordering) run — a non-resume run is unchanged.
    #[must_use]
    pub fn with_resume(mut self, pre_satisfied: BTreeMap<String, Option<String>>) -> Self {
        self.pre_satisfied = pre_satisfied;
        self
    }
}

// ===========================================================================
// The run report
// ===========================================================================

/// The outcome of one drive: the overall [outcome](RunOutcome) the driver
/// surfaces to its caller, plus the per-node terminal states.
///
/// The caller (the run verb) maps the overall outcome to an exit code; the driver
/// reports it, it does not own the code table.
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
    /// `<base>/<pipeline>/<run-id>/events.jsonl`. Because the path embeds both the
    /// pipeline identity and the run-unique id, two concurrent runs — even of the
    /// same binary and pipeline — write disjoint files.
    pub stream_path: String,
    /// Why the run was cancelled, or [`None`] if it was not cancelled. Recorded so
    /// the exit-code logic can prefer *run failure over cancellation*: a
    /// [`FailureUnderStop`](CancellationOrigin::FailureUnderStop) origin means a
    /// failure triggered the cancellation (failure wins), while an
    /// [`ExternalInterrupt`](CancellationOrigin::ExternalInterrupt) with no run
    /// failure is reported as a cancellation. The report records the origin; it does
    /// not own the exit-code mapping.
    pub cancellation_origin: Option<CancellationOrigin>,
    /// The **shutdown exit selection** for this run: which of run-failure /
    /// sink-failure / cancellation / success applies, by precedence. Derived from
    /// the run outcome, the cancellation origin, and whether the bounded final flush
    /// succeeded. The driver reports it but does not own the numeric code table.
    pub shutdown_exit: ShutdownExit,
}

// ===========================================================================
// The event sink port (writer adapter)
// ===========================================================================

/// Map a core [`TerminalState`] onto the wire [`WireTerminalState`]. The two
/// enums are structurally identical (both are the normative taxonomy); this is
/// the crate-boundary bridge the driver owns.
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

/// Translate one abstract [`AttemptEvent`] (the port the attempt runner emits
/// through) into the concrete [`Event`] and write it through the writer.
///
/// The runner emits abstract attempt records, and the driver stamps them into the
/// real event-stream envelope (run identity, schema version, gapless sequence,
/// wall stamp, monotonic offset) via the writer. Non-attempt records
/// (`node-ready`, `run-started`, `run-finished`, `zombie-at-exit`) are the
/// driver's own and are written directly.
pub(crate) fn write_attempt_event<S, C>(
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
        // Every non-success attempt-outcome record maps onto the
        // `attempt-failed` transition (the closed event vocabulary carries one
        // failure-outcome record; the specific terminal state travels on the
        // node-terminal record). The richer per-outcome records (timed-out,
        // panicked, backoff) fold into the artifact at fold time.
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
        // an attempt-outcome record, so it maps onto the `attempt-failed`
        // transition until the closed event vocabulary grows a matching variant.
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
/// The runner emits synchronously through an [`AttemptEventSink`], but the
/// authoritative event writer lives on the isolated framework runtime and must
/// not be touched from a task worker (write-through, single-writer). So a
/// spawned attempt emits into this in-memory buffer, and the framework loop drains
/// the buffer into the real writer in order once the attempt returns. This keeps
/// the writer single-owner while every attempt record still reaches the stream.
#[derive(Clone, Default)]
struct BufferingSink {
    records: Arc<Mutex<Vec<AttemptEvent>>>,
}

impl BufferingSink {
    fn drain(&self) -> Vec<AttemptEvent> {
        // Poison policy: panic — the buffer is drained into the run's event stream;
        // a poisoned buffer means a panic left the record set half-mutated, and a
        // recovered drain would write an inconsistent stream (see the module rule).
        let mut guard = self
            .records
            .lock()
            .expect("event buffer mutex not poisoned");
        std::mem::take(&mut *guard)
    }
}

impl AttemptEventSink for BufferingSink {
    fn emit(&mut self, event: AttemptEvent) {
        // Poison policy: panic — the event buffer, as in `drain`.
        self.records
            .lock()
            .expect("event buffer mutex not poisoned")
            .push(event);
    }
}

// ===========================================================================
// The driver entry point
// ===========================================================================

/// Drive one complete run to a truthful end.
///
/// This is the run-verb path's driver. It mints run identity, opens the store and
/// stream **before** `plan` (or `assembly_error`) is acted on, emits `run-started`,
/// drives the readiness-driven execution loop admitting ready nodes and feeding
/// outcomes back, waits the bounded grace period for any zombie closures at
/// natural run end, emits `zombie-at-exit` for each leftover thread, emits
/// `run-finished`, and returns the overall outcome and per-node terminal states.
///
/// `sink` is the injected [`EventSink`] (the run store's local-file sink in
/// production, or a test sink); `clock` is the authoritative monotonic clock. Both
/// are injected so the driver constructs no store itself.
///
/// # The assembly-failure path
///
/// `assembled` is the result of the pure assembly pass, computed by the caller
/// **after** the store/stream were opened (that ordering is the point — an
/// assembly failure still lands in the record). When it is `Err`, the driver emits
/// a `run-started` header with no fingerprints and a `run-finished` carrying
/// [`RunOutcome::AssemblyFailed`], and returns — no node runs.
///
/// # The bootstrap-failure path
///
/// After a successful assembly and the `run-started` header, the driver runs the
/// too-big-node bootstrap check ([`detect_capacities`]): if any node's declared
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
#[expect(
    clippy::too_many_lines,
    reason = "the driver is one linear bootstrap-then-drive sequence (mint identity, \
              open the stream, record the run-started header, run the assembly/bootstrap \
              fail-fast checks, drive the loop, finalize shutdown); its early-return \
              failure paths each record a full run-started/run-finished pair, so splitting \
              them would scatter the single ordered narrative the record-before-act \
              contract depends on"
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
    // --- Bootstrap: install the single process-global tracing subscriber once,
    // before anything runs, so every framework/attempt line beneath it is
    // formatted and attributable. Idempotent and coexistence-safe: a repeat call
    // or a pre-existing subscriber (e.g. a test harness's) is a no-op, never a
    // panic. The output mode (structured default / human) is read from the
    // DAGR_LOG_FORMAT env var. This is the developer/operator observability layer,
    // distinct from the event stream opened just below.
    let _ = crate::logging::init_tracing();

    // --- Bootstrap: mint identity, open the stream BEFORE assembly is acted on.
    let run_id = config.resolve_run_id();
    let run_id_str = run_id.as_str().to_string();
    let mut writer = EventStreamWriter::new(sink, clock, run_id, pipeline_name.to_string());
    // The run-store path this run writes under: <base>/<pipeline>/<run-id>/…
    // Two concurrent runs write disjoint files by construction.
    let stream_path = writer.stream_path(&config.base);

    // Capture the allowlisted environment values (empty allowlist → nothing).
    let captured_env = capture_env(env_allowlist);

    // --- Print the worst-case shutdown budget at startup: grace + teardown
    // deadline + bounded final flush. Printed before anything runs so a
    // misconfiguration (a budget that would not fit the orchestrator's kill window)
    // is visible before it matters. Operator-facing, so it goes to stderr and never
    // into the event stream.
    eprintln!("{}", config.shutdown_budget());

    // --- The per-run temp-directory convention. Create this run's own temp dir and
    // sweep prior runs' leftovers (see `bootstrap_temp_dir`).
    let temp_dir = bootstrap_temp_dir(&config.base, pipeline_name, &run_id_str);

    // --- The assembly-failure path: the store/stream are already open, so an
    // assembly failure still records itself. Emit a fingerprint-less header and a
    // run-finished carrying the assembly-failed outcome, then return.
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
        pre_satisfied,
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

    // --- The executor check. An executor this build does not implement is refused
    // HERE, at bootstrap, rather than at the CLI verb alone: a caller driving the
    // engine programmatically must get the same answer, because the alternative is
    // running every node in-process while the operator believes their placement was
    // honoured. Zero attempts, a `bootstrap-failed` outcome, and the executor's own
    // actionable diagnostic naming the ticket that implements it.
    if let Err(refusal) = config.executor.ensure_available() {
        eprintln!("{refusal}");
        let _ = writer.run_finished(RunOutcome::BootstrapFailed);
        let flush_ok = finalize_shutdown(&mut writer, &temp_dir);
        return RunReport {
            outcome: RunOutcome::BootstrapFailed,
            terminal_states: BTreeMap::new(),
            run_id: run_id_str,
            stream_path,
            cancellation_origin: None,
            shutdown_exit: select_shutdown_exit(RunOutcome::BootstrapFailed, None, flush_ok),
        };
    }

    // How this run's executor charges a placed node: an executor that honours
    // placement runs the attempt elsewhere (one remote slot, near-zero local cost),
    // and the local one records the placement and runs the node here anyway, so its
    // declared local cost stands. One decision, read by both the bootstrap capacity
    // check below and the loop's admission.
    let placement_handling = config.executor.placement_handling();

    // --- The too-big-node bootstrap check: reject, before any node executes, any
    // node whose declared cost exceeds a pool's total capacity — fail fast rather
    // than wedge at admission time. This runs after the header is recorded (so a
    // bootstrap failure still lands in the stream, like an assembly failure) and
    // before the loop starts (so nothing runs). It is distinct from the
    // admission-time can-never-fit guard and produces the `bootstrap-failed`
    // outcome. The capacities are the resolved pool totals (container-limit derived
    // or operator-pinned); the declared costs come from each node's policy.
    let node_costs: Vec<(String, PoolCost)> = pipeline
        .nodes()
        .map(|n| {
            (
                n.name().to_string(),
                PoolCost::from_policy(n.policy(), placement_handling),
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

    // Seed the readiness tracker with the run-level ordering upstreams: this is how
    // a consume-nothing node with a non-default trigger rule acquires the upstreams
    // its rule fires against. An empty ordering map yields exactly the
    // data-edge-only tracker.
    let tracker = ReadinessTracker::new_with_ordering(&pipeline, &artifact, &ordering);
    // The admission controller for this run. Its pools are pinned from the run
    // config (container-limit-derived or operator-pinned). The too-big-node
    // bootstrap check above already rejected any node that could never fit, so the
    // loop's admission never strands a can-never-fit node here.
    let admission = AdmissionController::new(config.capacities);
    // Partition the runners: teardown nodes are held back from the main loop and
    // run in a dedicated post-loop teardown phase (below). A pipeline with no
    // teardown node splits into the full set plus an empty teardown map, so the
    // loop and its byte-for-byte behaviour are unchanged.
    let (main_runners, teardown_runners) = partition_teardown_runners(&pipeline, runners);
    let (outcome, mut terminal_states, cancellation_origin) = run_loop(
        &pipeline,
        &run_id_str,
        pipeline_name,
        main_runners,
        tracker,
        config.grace,
        config.failure_mode,
        &admission,
        &config.capacities,
        placement_handling,
        &config.cancel_trigger,
        &temp_dir,
        &config.base,
        &pre_satisfied,
        config.attempt_queue_probe.as_ref(),
        &mut writer,
    );

    // --- The teardown phase. After the main graph reaches terminal on ANY exit
    // path (success, failure, stop-on-first-failure, external
    // cancellation), every teardown node still runs: under a FRESH, uncancelled
    // signal with its own deadline, bypassing admission, with its covered nodes'
    // terminal states in its context. A teardown's own failure is recorded but does
    // not change the run `outcome` (already computed over non-teardown nodes only)
    // and does not prevent the other teardowns from running. On a no-teardown
    // pipeline this is a no-op, so the stream stays byte-identical.
    // The covered nodes' terminal states are exactly the loop's output (teardown
    // nodes were excluded from the loop, so this snapshot carries no teardown
    // terminals); clone it so the phase can fold each teardown's own terminal into
    // `terminal_states` while still reading the covered picture.
    let covered_states = terminal_states.clone();
    run_teardown_phase(
        &pipeline,
        &run_id_str,
        pipeline_name,
        teardown_runners,
        &covered_states,
        config.teardown_deadline,
        &temp_dir,
        &config.base,
        &mut writer,
        &mut terminal_states,
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

/// The shutdown finalize shared by every exit path: perform the **bounded final
/// flush** and reclaim the run's **per-run temp directory**, returning whether the
/// flush succeeded.
///
/// The [final flush](final_flush) is the single fsync-at-run-end/cancellation
/// boundary; a `false` return is the unwritable-sink-at-shutdown fault
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

/// Bootstrap the run's per-run temp directory and return its path.
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
/// into the map — the negative half of the capture contract.
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
    /// The durable reference the node's succeeded attempt recorded, or [`None`] for
    /// a non-durable node. Stamped by the loop onto the succeeded attempt's
    /// `attempt-outcome` record so a later resume can rehydrate it. `None` for every
    /// non-durable node — the stream is then byte-identical.
    durable_reference: Option<String>,
    /// The optional durable-reference metadata the node's succeeded attempt
    /// recorded (T89), or [`None`] when none was supplied. Stamped alongside
    /// `durable_reference`; `None` for every non-durable node, so the stream is
    /// byte-identical.
    durable_reference_meta: Option<dagr_artifact::event_stream::DurableReferenceMeta>,
    /// What this message *is*: a finished (or synthesized) attempt report, or the
    /// isolated framework timer reporting an unkillable attempt's elapsed deadline.
    kind: DoneKind,
}

impl AttemptDone {
    /// A finished-attempt report — the shape every non-timeout message takes.
    fn attempt(node: String, state: TerminalState, events: Vec<AttemptEvent>) -> Self {
        Self {
            node,
            state,
            events,
            durable_reference: None,
            durable_reference_meta: None,
            kind: DoneKind::Attempt,
        }
    }
}

/// What an [`AttemptDone`] message carries.
enum DoneKind {
    /// A node's attempt reported its terminal state (or the loop synthesized one for
    /// a node that never ran: a cancelled waiter, an over-demand rejection, a
    /// missing runner).
    Attempt,
    /// The **per-attempt deadline** of an unkillable (blocking / compute) node
    /// elapsed on the framework runtime. It carries what the loop needs to decide
    /// the node without touching the jammed task thread: the still-held permit (so
    /// the zombie's cost stays counted and is registered as a live zombie) and the
    /// runner's [`AttemptFate`] (so exactly one of timer and closure wins).
    TimeoutFired {
        /// The attempt's admission permit, still held by the running closure. The
        /// loop reads it to register the zombie; it is released only when the
        /// closure itself takes it out of the cell and drops it.
        permit: Arc<Mutex<Option<Permit>>>,
        /// The runner's fate cell, or [`None`] for a runner that exposes none.
        fate: Option<Arc<AttemptFate>>,
    },
}

/// How an admitted attempt holds its permit for the whole attempt.
///
/// The permit is **moved into the dispatched closure** either way, so it is dropped
/// exactly when the attempt returns, on whichever surface ran it. The two shapes
/// differ only in whether the framework loop can *look at* the permit while the
/// closure still holds it:
///
/// - [`Owned`](PermitHold::Owned) — the ordinary shape: the closure owns the permit
///   outright and the run is byte-identical to before the per-attempt timeout was
///   enforced.
/// - [`Shared`](PermitHold::Shared) — used **only** for an unkillable node whose
///   policy declares a timeout: the closure owns the permit through a cell the loop
///   also holds, so at the deadline the loop can register the still-counted cost as
///   a live zombie **without** releasing it. The closure still takes the permit out
///   and drops it when it returns, which is what finally frees the capacity.
enum PermitHold {
    /// The closure owns the permit outright.
    Owned(Permit),
    /// The closure owns the permit through a cell the framework loop can read.
    Shared(Arc<Mutex<Option<Permit>>>),
}

impl PermitHold {
    /// Release the permit — its cost returns to every pool, and any live-zombie
    /// record for the node clears (the closure has returned).
    fn release(self) {
        match self {
            PermitHold::Owned(permit) => drop(permit),
            // Poison policy: panic — the permit cell gates pool capacity; a poisoned
            // cell would leak a permit and stall every waiter behind it.
            PermitHold::Shared(cell) => {
                let permit = cell.lock().expect("permit cell not poisoned").take();
                drop(permit);
            }
        }
    }
}

/// The reserved sentinel node name for a **cancellation wake** pushed through the
/// attempt channel. A real node name is never empty (assembly rejects
/// an empty name), so an [`AttemptDone`] carrying this name is unambiguously the
/// cancellation-request wake, not a finished attempt. Routing the wake through the
/// *same* channel the loop already awaits keeps the loop a plain `recv().await` — no
/// `tokio::select!` (and so no added `macros` feature) is needed for the loop to
/// react promptly to a request.
const CANCEL_WAKE_SENTINEL: &str = "";

/// The shared set of node names currently **in flight** (admitted, not yet
/// terminal) — the drain target when the run is cancelled. A name is
/// inserted when the node is admitted and removed when its [`AttemptDone`] is
/// received; whatever remains after the grace-bounded drain is recorded
/// `abandoned`. Shared behind an `Arc<Mutex<_>>` because `admit` inserts from the
/// (framework-runtime) loop while the loop removes on completion.
type LiveSet = Arc<Mutex<std::collections::BTreeSet<String>>>;

/// The **immutable shared context** every admission/dispatch helper reads: the
/// assembled pipeline, the run identity, the type-erased runners, the execution
/// dispatcher, the loop's attempt channel, the admission controller, the
/// run-scoped cancellation token, and the in-flight [`LiveSet`]. Bundling these
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
    // How this run's executor charges a PLACED node against the pools. The local
    // executor records a placement and ignores it, so a placed node pays its
    // declared local cost; an executor that honours placement charges one remote
    // slot and near-zero local capacity instead.
    placement_handling: PlacementHandling,
    run_cancel: &'a CancellationSource,
    live: &'a LiveSet,
    // The run's per-run temp directory, threaded into each attempt's `RunContext`
    // so a task reaches its confined local scratch through the context. Created at
    // bootstrap and reclaimed at run end by the driver.
    temp_dir: &'a std::path::Path,
    // The run-store base, threaded into each attempt's `RunContext` so a task
    // reaches its **durable scratch store** — its per-node namespace
    // `<base>/<pipeline>/<run-id>/scratch/<node>/`. This is the wiring the resume
    // gate demo depends on: a re-executing node reads its carried-forward
    // checkpoint through the ordinary context API. A task that touches no scratch
    // is unaffected, so a non-scratch run is byte-identical.
    scratch_base: &'a str,
    // The pipeline identity used to resolve the scratch namespace (and, at teardown,
    // the same), so the driver's scratch layout and a later resume's carry-forward
    // agree on `<base>/<pipeline>/…`.
    pipeline_name: &'a str,
    // A handle to the **isolated framework runtime** — where the per-attempt
    // deadline of an unkillable (blocking / compute) node is armed. Arming it here
    // rather than on a task surface is what makes the timeout fire even when every
    // task/blocking/compute worker is jammed by a synchronous busy-loop.
    framework: &'a tokio::runtime::Handle,
}

/// The readiness-driven execution loop (the driver's half of the run-end
/// condition).
///
/// It runs on the isolated **framework runtime** and admits ready nodes onto the
/// [`Dispatcher`]'s three execution surfaces **by execution class** —
/// await-bound onto the tokio task runtime, blocking onto the dedicated blocking
/// pool, compute onto the fixed rayon pool — feeding each terminal outcome back
/// into the tracker so dependents decrement and either become ready (admitted next)
/// or receive their propagated terminal state (recorded without executing) — never
/// batching a level into a wave. It terminates precisely when nothing is pending and
/// nothing is in flight, then waits the bounded grace period for zombie candidates
/// (blocking timeouts) and emits a `zombie-at-exit` event for each. Returns the
/// overall outcome and the per-node terminal states.
#[expect(
    clippy::too_many_arguments,
    reason = "the loop threads the whole run's mutable bookkeeping explicitly \
              (pending queue, live set, in-flight count, terminal states, zombie \
              candidates, cancellation flags) rather than hiding it in shared state a \
              helper could mutate out of order"
)]
#[expect(
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
    pipeline_name: &str,
    runners: BTreeMap<String, Box<dyn NodeRunner>>,
    mut tracker: ReadinessTracker,
    grace: Duration,
    failure_mode: FailureMode,
    admission: &AdmissionController,
    capacities: &PoolCapacities,
    placement_handling: PlacementHandling,
    cancel_trigger: &Arc<CancelTrigger>,
    temp_dir: &std::path::Path,
    scratch_base: &str,
    pre_satisfied: &BTreeMap<String, Option<String>>,
    attempt_queue_probe: Option<&Arc<AttemptQueueProbe>>,
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
    // The execution-class dispatcher: it owns the three task surfaces — the async
    // task runtime (await-bound), the dedicated blocking pool (blocking, via
    // `spawn_blocking`), and the fixed rayon compute pool (compute) — built from
    // the run's pinned pool capacities (the compute pool sized to the pinned
    // `compute_threads`, floor of one). Each is a *task* surface, separate from the
    // framework runtime below, so a task that jams every task/blocking/compute
    // worker cannot stall the loop, its timers, or the writer.
    let dispatcher = Dispatcher::new(capacities);
    // The framework runtime — drives this loop, the grace timer, and the drain. It
    // is NOT one of the dispatcher's task surfaces, which is the isolation the
    // all-workers-blocked-timeout-still-fires guarantee depends on.
    let framework = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .expect("framework runtime builds");

    let runners = Arc::new(Mutex::new(runners));
    let mut terminal_states: BTreeMap<String, TerminalState> = BTreeMap::new();
    // Zombie candidates, each paired with the 1-based attempt number whose thread
    // was left behind (the `zombie-at-exit` record keys pinned-time accounting off
    // `(node, attempt)`).
    let mut zombie_candidates: Vec<(String, u32)> = Vec::new();
    // The run-scoped cancellation token: the driver owns it, each admitted attempt
    // observes a per-attempt child (threaded into its `RunContext`), and the
    // cancellation core flips it so every in-flight attempt observes cancellation
    // at once. Uncancelled unless the trigger fires or stop-on-first-failure routes
    // through the core — so a non-cancelled run's attempts observe exactly the
    // fresh-uncancelled signal.
    let run_cancel = CancellationSource::new();
    // The recorded cancellation origin (first-cause-wins), surfaced to the report
    // for the exit-code precedence. `None` for a non-cancelled run.
    let mut cancel_origin: Option<CancellationOrigin> = None;

    framework.block_on(async {
        // The loop's completion channel. It is **unbounded on purpose**, and the
        // invariant that makes that safe is stronger than any capacity would be:
        //
        //   the queue holds at most one message per node counted into `in_flight`,
        //   plus one sentinel per cancellation request, plus at most one elapsed
        //   deadline per timeout-declaring unkillable node.
        //
        // Enforced by the pairing this loop maintains and asserts on the receive
        // side: a node is counted into `in_flight` exactly once — when it is
        // admitted (`admit`), cancelled without running (`cancel_node`), or rejected
        // as over-demand (`reject_over_demand`) — and each counted node sends
        // exactly one `AttemptDone`. So the depth never exceeds `in_flight`, which
        // never exceeds the node count. The deadline timers add at most one message
        // each and are armed at most once per admitted node (`arm_unkillable_deadline`,
        // only for a blocking/compute node whose policy declares a timeout), so the
        // bound stays a small multiple of the node count and is still not a capacity
        // anyone has to guess.
        //
        // That is the whole bound: the admission limit (C12) and the execution-class
        // pools (C13) do **not** tighten it, however narrow they are pinned. A
        // permit is released when the attempt returns, *before* the loop is told it
        // finished (see `admit`), so a node stays counted in `in_flight` after its
        // permit is gone — and the frontier and `drain_pending` walks can admit its
        // successor, and its successor's successor, without the loop returning to
        // the receive point in between. A one-permit pool therefore still queues one
        // completion per node. Measured, not assumed:
        // `crates/cli/tests/async_and_allocation_review.rs` pins the execution bound
        // (one attempt body at a time) and the queue bound (the node count)
        // separately, because they are separate facts.
        //
        // A **bounded** channel would not merely duplicate that invariant, it would
        // deadlock: `cancel_node` and `reject_over_demand` send from *inside* this
        // loop, so a full queue would block the only task that drains it — and at
        // the stop-on-first-failure transition the loop emits one such message per
        // pending node, in a single synchronous burst that exceeds the permit count
        // by design.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AttemptDone>();
        // Install the loop's wake channel on the cancellation trigger so a request
        // (the programmatic handle or an OS-signal handler) wakes the loop by
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
        // Ready nodes that could not yet acquire their admission permit (a pool at
        // capacity), oldest-ready-first. Each is re-offered when a permit is
        // released — a terminal outcome frees capacity, which is what unblocks the
        // next waiter. Under the default unconstrained pools this stays empty and
        // every ready node is admitted at once.
        let mut pending: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        // T90 produced-output lineage: each durable node's succeeded reference, keyed
        // by node name, recorded when the node produces it and consulted to populate a
        // downstream consumer's `inputs[]` (its data-edge producers' references). A
        // non-durable run leaves this empty, so its stream is byte-identical.
        let mut produced_refs: BTreeMap<String, ConsumedInput> = BTreeMap::new();
        // Whether stop-on-first-failure has been triggered — set the first time a
        // failure-like terminal is observed under stop mode. Once set, no further
        // default-rule non-teardown node is admitted; a firing consume-nothing
        // non-default-rule contingency still runs. In continue-independent mode this
        // stays false and the loop admits every ready node.
        let mut stopping = false;
        // **Full-drain cancellation** (an external interrupt): admit nothing new
        // *at all* (not even a firing contingency) and grace-drain the in-flight
        // attempts, reclassifying an in-flight-at-cancel return to `cancelled` and a
        // non-returning attempt to `abandoned` after grace. This is deliberately
        // distinct from `stopping`: a **stop-on-first-failure** also routes through
        // the cancellation core (it flips the run token and records a failure
        // origin) but keeps its exact loop behaviour — it admits firing contingencies
        // and lets the in-flight complete naturally, so a non-cancelled stop run is
        // byte-for-byte an ordinary stop run. Only an external interrupt sets this.
        let mut draining = false;
        // The single grace deadline for the whole drain, set once when the full drain
        // begins: `now + grace`. The drain waits for in-flight attempts only until
        // this instant, then abandons whatever remains — the bound that guarantees
        // termination even if a task ignores cancellation. `None` until the drain
        // begins.
        let mut drain_deadline: Option<tokio::time::Instant> = None;

        // Nodes whose fate a per-attempt timeout already decided while their
        // unkillable closure runs on. The closure's eventual report must NOT be
        // recorded (the terminal state is decided exactly once) and must not
        // decrement the in-flight count a second time — it was decremented at the
        // mark, which is what lets the run proceed past a zombie.
        let mut timed_out_zombies: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        // The immutable shared context every admission/dispatch helper reads.
        let framework_handle = tokio::runtime::Handle::current();
        let actx = AdmitCtx {
            pipeline,
            run_id,
            runners: &runners,
            dispatcher: &dispatcher,
            tx: &tx,
            admission,
            placement_handling,
            run_cancel: &run_cancel,
            live: &live,
            temp_dir,
            scratch_base,
            pipeline_name,
            framework: &framework_handle,
        };

        // --- Resume pre-satisfied nodes -------------------------------------
        // Before the frontier, record every node the resume plan left
        // `satisfied-from-prior` as terminal (a success-like state): emit its ready /
        // satisfied attempt-outcome (carrying the copied-forward durable reference) /
        // node-terminal records, mark its terminal state, and feed it into the tracker
        // so its dependents in the must-run set become ready. It has no runner (it is
        // NOT re-executed); its demanded value, if any, was pre-filled into its output
        // slot by the caller (rehydration). For a non-resume run `pre_satisfied` is
        // empty and this whole block is skipped, so the loop is byte-for-byte the
        // non-resume run.
        for (node, durable_reference) in pre_satisfied {
            let _ = writer.node_ready(node);
            let mut record = AttemptOutcomeRecord::new(
                node,
                1,
                wire_terminal(TerminalState::SatisfiedFromPrior).as_str(),
            );
            dagr_artifact::event_stream::record_durable_reference(
                &mut record,
                durable_reference.clone(),
            );
            let _ = writer.attempt_outcome(record);
            record_terminal(
                node,
                TerminalState::SatisfiedFromPrior,
                &mut terminal_states,
            );
            let _ = writer.node_terminal(node, wire_terminal(TerminalState::SatisfiedFromPrior));
            // Cascade: a satisfied producer's dependents in the must-run set can now
            // become ready (success-like upstream).
            let decisions =
                tracker.notify_terminal(NodeId::from_name(node), TerminalState::SatisfiedFromPrior);
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
        }

        // Offer the initial-ready frontier (every zero-dependency source node) to
        // admission. A node that fits its pools is admitted (in flight); one that
        // does not waits in `pending` for a release. A node already settled
        // satisfied-from-prior above is skipped here (it is decided, not ready).
        for id in tracker.initial_ready().to_vec() {
            if let Some(name) = node_name(pipeline, id) {
                if terminal_states.contains_key(&name) {
                    continue;
                }
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
        // The bounded wait for an abandoned-but-running closure to free the capacity
        // a waiter needs. `None` unless the loop finds itself with waiters queued but
        // nothing in flight — which can only happen when a *timed-out*
        // blocking/compute attempt still holds its permit (every other release
        // re-offers the queue synchronously, so a waiter never outlives the last
        // in-flight attempt).
        let mut zombie_capacity_deadline: Option<tokio::time::Instant> = None;
        loop {
            if in_flight > 0 {
                // Work is moving again: any capacity wait armed earlier is spent.
                zombie_capacity_deadline = None;
            } else {
                // Nothing in flight. With the queue empty this is the run's end.
                if pending.is_empty() || draining {
                    break;
                }
                let now = tokio::time::Instant::now();
                match zombie_capacity_deadline {
                    // Give the zombie the same bounded grace the zombie-at-exit wait
                    // gives it to return and release its permit.
                    None => zombie_capacity_deadline = Some(now + grace),
                    Some(deadline) if now >= deadline => {
                        // It did not return within grace, so the capacity it pins may
                        // never come back. A waiter left in the queue would be
                        // stranded with no terminal state, so each is failed with the
                        // honest reason — the same strand guard the over-demand
                        // rejection applies to a node that can never be admitted.
                        for name in pending.drain(..) {
                            reject_zombie_pinned(&name, &tx, &mut in_flight);
                        }
                        zombie_capacity_deadline = None;
                    }
                    Some(_) => {}
                }
                if in_flight == 0 && pending.is_empty() {
                    break;
                }
            }
            // Await the next channel message: a finished attempt, an unkillable
            // attempt's elapsed deadline, or a cancellation wake (a
            // `CANCEL_WAKE_SENTINEL` `AttemptDone` the trigger pushed). Once a
            // full drain has begun the wait is bounded by the single grace deadline —
            // whatever has not returned by then is abandoned and the run proceeds, the
            // bound that guarantees termination even if a task ignores cancellation.
            let bounded = if draining {
                drain_deadline
            } else {
                zombie_capacity_deadline
            };
            let Some(done) = recv_next(&mut rx, bounded).await else {
                // A bounded wait that elapsed with nothing received: re-check the
                // capacity guard above. Anything else ends the loop.
                if !draining && bounded.is_some() {
                    continue;
                }
                break;
            };
            // Observe the queue's real occupancy at the moment of dequeue (the
            // backlog still queued, plus the message just taken), so the bound
            // argued at the channel's construction site is measured rather than
            // asserted. `None` in production — this is a test seam.
            if let Some(probe) = attempt_queue_probe {
                probe.observe(rx.len() + 1);
            }

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

            // The isolated framework timer reporting an unkillable attempt's
            // elapsed deadline. Mark the node `timed-out` NOW — its fate is decided
            // while its closure runs on as abandoned-but-running work whose permit
            // stays held — and turn the mark into the ordinary attempt report the
            // recording path below consumes. A mark that lost the race to a closure
            // that had just returned yields `None` and is discarded.
            let done = match &done.kind {
                DoneKind::TimeoutFired { permit, fate } => {
                    // A node the loop already settled is decided: a deadline that
                    // fires afterwards changes nothing (a terminal state is decided
                    // exactly once). The fate cell closes the same race for a runner
                    // that exposes one; this guard covers a runner that does not.
                    if terminal_states.contains_key(&done.node) {
                        continue;
                    }
                    let Some(marked) =
                        mark_unkillable_timeout(&done.node, permit, fate.as_ref(), admission)
                    else {
                        continue;
                    };
                    timed_out_zombies.insert(done.node.clone());
                    marked
                }
                // A zombie whose fate the deadline already decided finally returned.
                // Its report is refused: the node has its one terminal state, its
                // records would duplicate the mark, and it was already counted out
                // of flight. Its permit dropped as it returned, so the waiters it
                // was blocking may now be admitted.
                DoneKind::Attempt if timed_out_zombies.remove(&done.node) => {
                    if !draining {
                        drain_pending(&actx, writer, &mut pending, &mut in_flight);
                    }
                    continue;
                }
                DoneKind::Attempt => done,
            };

            // A real attempt reported terminal, so the count of admitted-not-yet-
            // terminal nodes drops by one. The **paired invariant** — every
            // `AttemptDone` that reaches here was counted in flight when its node
            // was admitted, cancelled, or rejected — is asserted rather than
            // relied on silently: it is the one thing that makes this decrement
            // safe, and five call sites across async control flow do the pairing.
            //
            // The subtraction **saturates**, matching the discipline every other
            // counter in the workspace follows (`crates/core/src/slot.rs`'s
            // near-identical in-flight-lease counter saturates for the same
            // reason). A bare `-= 1` fails in two different ways under the T93
            // profiles: it panics in dev/test (overflow checks on) and wraps to
            // `usize::MAX` in release, and a wrapped counter turns `while
            // in_flight > 0` into a loop that never ends. Saturating means a
            // hypothetical unpaired report ends the run early — visibly, with a
            // node missing its terminal — instead of hanging the process.
            debug_assert!(
                in_flight > 0,
                "in-flight underflow: `{}` reported terminal without having been counted in \
                 flight (framework defect in the admit/cancel/reject pairing)",
                done.node
            );
            in_flight = in_flight.saturating_sub(1);
            // Poison policy: panic — the live set, as in `abandon_leftover`.
            live.lock()
                .expect("live set not poisoned")
                .remove(&done.node);
            // Write the attempt's buffered records, classify its (possibly
            // cancellation-reclassified) terminal, and record it exactly once. Threads
            // the pipeline (to resolve a consumer's data-edge producers) and the
            // produced-reference map (T90 produced/consumed lineage).
            let recorded_state = record_attempt_outcome(
                &done,
                draining,
                writer,
                pipeline,
                &mut produced_refs,
                &mut terminal_states,
                &mut zombie_candidates,
            );

            // Stop-on-first-failure. The instant the first failure-like terminal is
            // observed under stop mode, route through the cancellation core with a
            // failure origin: stop admitting default-rule non-teardown work and
            // cancel every pending default-rule node unrelated to the failure. The
            // in-flight drain completes on its own; consume-nothing non-default-rule
            // contingencies whose rule fires on the resulting picture are admitted
            // as they become ready (below).
            if failure_mode == FailureMode::StopOnFirstFailure
                && !stopping
                && is_failure_like(recorded_state)
            {
                // `full_drain = false`: keep the ordinary loop behaviour exactly
                // (firing contingencies still admitted; in-flight completes
                // naturally). The core still flips the run token (so any cooperative
                // in-flight work can observe cancellation) and records the failure
                // origin.
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

        // Post-drain. If the full drain left attempts in flight past grace, record
        // each as `abandoned` and proceed — the bound that guarantees the run
        // terminates even when a task ignores cancellation.
        if draining {
            abandon_leftover(&live, writer, &mut terminal_states, &mut zombie_candidates);
        }

        // Natural run end: nothing pending, nothing in flight. Give any zombie
        // candidate (a blocking timeout whose leftover work has not confirmed
        // return) at most the grace period, then emit a zombie-at-exit event for
        // each. This does not change
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
    // (a signal racing shutdown) must not touch this finished run's channel.
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

/// Await the next loop message. Returns the next [`AttemptDone`] — a
/// finished attempt or a `CANCEL_WAKE_SENTINEL` cancellation wake — or [`None`] to
/// stop the loop (the channel closed, or, once a full drain is under way, the grace
/// deadline elapsed with work still in flight). During the drain the wait is bounded
/// by `drain_deadline` (`now + grace`, set when the drain began), which is the bound
/// that guarantees termination even if a task ignores cancellation.
async fn recv_next(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<AttemptDone>,
    deadline: Option<tokio::time::Instant>,
) -> Option<AttemptDone> {
    match deadline {
        Some(deadline) => {
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            // `Ok(msg)` is the received message (or channel-closed `None`); `Err` is
            // the deadline firing — both yield `None`, which the caller reads as
            // "the bound elapsed".
            tokio::time::timeout_at(deadline, rx.recv())
                .await
                .unwrap_or(None)
        }
        None => rx.recv().await,
    }
}

/// Write a finished attempt's buffered records and record its terminal state,
/// classifying a return that raced a full-drain cancellation.
///
/// An attempt that was still in flight when the run was **externally** cancelled
/// (`draining`) and returns within grace, without having reached a terminal before
/// the drain began, is recorded `cancelled` — the run is being torn down and its
/// output is discarded regardless of the raw outcome the aborted work produced. On
/// such a reclassify the attempt's own `NodeTerminal` buffered record (its raw
/// terminal) is suppressed and the authoritative `cancelled` node-terminal is
/// emitted instead; the opening records still write, so the stream honestly shows
/// the attempt ran and was cut short. Otherwise the attempt keeps its real terminal
/// (a stop-on-first-failure keeps its exact outcomes). `record_terminal` is
/// exactly-once, so a late report never overwrites a prior classification. Returns
/// the recorded terminal state.
fn record_attempt_outcome<S, C>(
    done: &AttemptDone,
    draining: bool,
    writer: &mut EventStreamWriter<S, C>,
    pipeline: &Pipeline,
    produced_refs: &mut BTreeMap<String, ConsumedInput>,
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
    // T90 consumed lineage: the durable inputs this node read, resolved from its
    // static data-edge producers' recorded references (a fresh run). A node with no
    // durable upstream reads nothing, so its record is byte-identical.
    let consumed = consumed_inputs_for(pipeline, &done.node, produced_refs);
    // Drain the buffered per-transition events, and — alongside each attempt's
    // CLOSING outcome event — emit the single rich `attempt-outcome` record for
    // that attempt (every attempt produces exactly one attempt-outcome record
    // alongside its per-transition events). A retried node buffers several
    // attempts, so this emits one outcome record per attempt, each just before the
    // (shared) node-terminal. The record carries the attempt's
    // status/number/panic-message the driver has; cost/metrics/worker are not yet
    // measured (the fold defaults each absent field). This records what happened;
    // it changes no execution behavior.
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
            if let Some(mut record) = closing_outcome_record(&done.node, ev) {
                last_attempt = record.attempt;
                // Stamp the durable reference onto the SUCCEEDED attempt-outcome
                // record so a later resume finds it in the folded artifact. Only a
                // succeeded outcome carries a reference; a non-durable node reports
                // `None`, leaving the field absent (the fold defaults it), so a
                // non-durable run's record is unchanged.
                if matches!(ev, AttemptEvent::AttemptSucceeded { .. }) {
                    dagr_artifact::event_stream::record_durable_reference(
                        &mut record,
                        done.durable_reference.clone(),
                    );
                    // Alongside the reference, stamp the OPTIONAL metadata (T89) the
                    // durable output supplied — `None` for a non-durable node leaves
                    // the field absent, so a non-durable run's record is unchanged.
                    dagr_artifact::event_stream::record_durable_reference_meta(
                        &mut record,
                        done.durable_reference_meta.clone(),
                    );
                    // T90 consumed lineage: record the durable inputs this succeeded
                    // attempt read (its data-edge producers' references). Empty ⇒ the
                    // field stays absent, so a non-consuming record is unchanged.
                    dagr_artifact::event_stream::record_consumed_inputs(
                        &mut record,
                        consumed.clone(),
                    );
                }
                let _ = writer.attempt_outcome(record);
                // T90 produced lineage: alongside a durable node's SUCCEEDED
                // attempt-outcome, emit the explicit output-produced event and record
                // the reference for downstream consumers. Attributed to THIS run
                // (a fresh produce, not a resume carry-forward). A non-durable success
                // reports no reference, so nothing is emitted and the stream is
                // byte-identical.
                if matches!(ev, AttemptEvent::AttemptSucceeded { .. }) {
                    if let Some(uri) = &done.durable_reference {
                        let meta = done.durable_reference_meta.clone().unwrap_or_default();
                        produced_refs.insert(
                            done.node.clone(),
                            ConsumedInput {
                                uri: uri.clone(),
                                content_hash: meta.content_hash.clone(),
                            },
                        );
                        let _ = writer.output_produced(OutputProducedRecord {
                            node: done.node.clone(),
                            attempt: last_attempt,
                            uri: uri.clone(),
                            content_hash: meta.content_hash.clone(),
                            size_bytes: meta.size_bytes,
                            kind: meta.scheme.clone(),
                            // The produced-at offset the T89 metadata supplied; 0 when
                            // none (the record envelope's own `offset_ns` still stamps
                            // the real produce time, and the fold reads that too).
                            produced_at_offset_ns: meta.produced_at_offset_ns.unwrap_or(0),
                            originating_run: writer.run_id().to_string(),
                        });
                    }
                }
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
/// durable reference) are not measured here, so they are left absent (the fold
/// defaults each). Returns `None` for a non-closing event.
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

/// Arm the per-attempt **deadline** of an *unkillable* (blocking / compute) node,
/// returning how its admission permit is held for the attempt.
///
/// An await-bound attempt arms its own deadline — its future can truly be dropped,
/// and that drop *is* the cancellation. A blocking or compute closure can be neither
/// dropped nor even *polled* while it runs (its own future is what is jammed), so its
/// deadline is armed **here**, on the isolated framework runtime: exactly the
/// isolation C13 promises, so a task that jams every task/blocking/compute worker
/// still cannot delay the timer. The timer sleeps the declared budget and sends one
/// message to the loop's own channel, carrying the permit cell and the runner's fate
/// so the loop can mark the attempt without touching the jammed thread.
///
/// A node whose policy declares no timeout (or an await-bound one) arms nothing: no
/// timer is spawned, the permit is moved into the closure exactly as before, and the
/// run is byte-identical.
fn arm_unkillable_deadline(
    actx: &AdmitCtx,
    name: &str,
    node_id: NodeId,
    class: ExecutionClass,
    runner: &dyn NodeRunner,
    permit: Permit,
) -> PermitHold {
    let budget = actx
        .pipeline
        .node(node_id)
        .and_then(|n| n.policy().timeout_budget())
        .filter(|_| matches!(class, ExecutionClass::Blocking | ExecutionClass::Compute));
    let Some(budget) = budget else {
        return PermitHold::Owned(permit);
    };

    // The permit moves into a cell the closure owns and the loop can read: at the
    // deadline the loop registers the still-counted cost as a live zombie without
    // releasing it (the closure releases it when it finally returns).
    let cell = Arc::new(Mutex::new(Some(permit)));
    let fate = runner.timeout_fate();
    let timer_tx = actx.tx.clone();
    let timer_cell = Arc::clone(&cell);
    let timer_node = name.to_string();
    actx.framework.spawn(async move {
        tokio::time::sleep(budget).await;
        let _ = timer_tx.send(AttemptDone {
            node: timer_node,
            state: TerminalState::TimedOut,
            events: Vec::new(),
            durable_reference: None,
            durable_reference_meta: None,
            kind: DoneKind::TimeoutFired {
                permit: timer_cell,
                fate,
            },
        });
    });
    PermitHold::Shared(cell)
}

/// Decide an **unkillable** (blocking / compute) attempt's fate at its per-attempt
/// deadline, and hand the loop the attempt report to record.
///
/// This is C14's blocking/compute timeout, wired to the real run path: the attempt
/// is *marked* `timed-out` immediately — the framework cannot stop the thread and
/// does not pretend to — through the merged
/// [`TimeoutDecision::mark_blocking_timed_out`](dagr_core::execution::TimeoutDecision::mark_blocking_timed_out)
/// mechanism, which emits the `attempt-timed-out` outcome record and the `timed-out`
/// node-terminal at the mark. Around that mark this function does exactly the three
/// things the mark deliberately does not:
///
/// - **holds the permit.** The permit stays inside the still-running closure; the
///   loop only *registers* it as a live zombie ([`AdmissionController::mark_zombie`]),
///   so its cost stays counted (and visible in the zombie report) until the closure
///   returns and drops it.
/// - **bars the late result.** The [`AttemptFate`] hand-off gives the runner the
///   decision's [`LateResultBarrier`](dagr_core::execution::LateResultBarrier), so
///   whatever the abandoned closure computes after the mark is refused rather than
///   filling the output slot.
/// - **decides exactly once.** The claim is the arbiter: if the closure had already
///   returned inside its budget, the claim fails, [`None`] is returned, and the
///   mark's records are discarded unwritten — the attempt's own report stands.
///
/// The attempt number is `1`: the loop cannot see a runner's internal attempt
/// boundaries, and a marked node is decided (no further attempt of it runs), the
/// same "attempt 1 in the no-retry-past-abandonment model" the cancellation drain's
/// [`abandon_leftover`] records.
fn mark_unkillable_timeout(
    node: &str,
    permit: &Arc<Mutex<Option<Permit>>>,
    fate: Option<&Arc<AttemptFate>>,
    admission: &AdmissionController,
) -> Option<AttemptDone> {
    // Mint the decision into a private buffer first: if the claim below loses, the
    // records are simply dropped and nothing reaches the stream.
    let ctx = RunContext::builder(
        CoreRunId::new(String::new()),
        PipelineId::new(String::new()),
        NodeId::from_name(node),
    )
    .attempt(1)
    .max_attempts(1)
    .build();
    let mut sink = BufferingSink::default();
    let decision =
        dagr_core::execution::TimeoutDecision::mark_blocking_timed_out(node, &ctx, &mut sink);

    // Exactly-once: the timer claims the node only while an attempt is live. A
    // runner that exposes no fate cell cannot race us, so the mark stands.
    let claimed = fate.is_none_or(|f| f.claim_timeout(decision.barrier()));
    if !claimed {
        return None;
    }

    // The closure runs on holding its permit: register the zombie so the cost stays
    // counted and observable, and let the closure's own return release it.
    // Poison policy: panic — the permit cell, as in `PermitHold::release`.
    if let Some(held) = permit.lock().expect("permit cell not poisoned").as_ref() {
        admission.mark_zombie(held);
    }

    Some(AttemptDone::attempt(
        node.to_string(),
        decision.outcome().terminal_state(),
        sink.drain(),
    ))
}

/// Record every attempt still in flight past the cancellation grace as
/// `abandoned`. Each is a node whose closure ignored cancellation and did not
/// return within grace; the driver does not wait for it. `record_terminal` is
/// exactly-once, so a node that did reach a terminal is left untouched; an
/// abandoned closure is a zombie candidate (its thread may run on and is reaped at
/// process exit — a `zombie-at-exit` event).
fn abandon_leftover<S, C>(
    live: &LiveSet,
    writer: &mut EventStreamWriter<S, C>,
    terminal_states: &mut BTreeMap<String, TerminalState>,
    zombie_candidates: &mut Vec<(String, u32)>,
) where
    S: EventSink,
    C: MonotonicClock,
{
    // Poison policy: panic — the live set decides which attempts are abandoned at
    // grace; a half-mutated set would abandon the wrong nodes.
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
            // The driver has no permit ledger to name the leftover attempt's
            // number; a leftover attempt is attempt 1 in the
            // no-retry-past-abandonment model.
            zombie_candidates.push((node, 1));
        }
    }
}

/// Enter the cancellation core. The single internal entry point every cancellation
/// origin routes through:
///
/// - **records the origin** (first cause wins) so the exit-code precedence can
///   later prefer run failure over cancellation;
/// - **flips the run token** so every live per-attempt child observes cancellation
///   at once (in-flight cooperative work can return `cancelled`), exactly once and
///   idempotently;
/// - **cancels every pending default-rule node** waiting for capacity (a pending
///   unrelated default node ends `cancelled`), while a non-default-rule contingency
///   in `pending` is kept for a stop-mode run;
/// - sets `stopping` (the admit-no-more-default-work rule).
///
/// `full_drain` selects the drain discipline. An **external interrupt** passes
/// `true`: the caller then enters the grace-bounded drain that admits nothing at
/// all and reclassifies in-flight returns `cancelled`/`abandoned`. A
/// **stop-on-first-failure** passes `false`: the loop keeps its ordinary behaviour
/// (firing contingencies still run, in-flight completes naturally), so a
/// non-cancelled stop run is byte-for-byte an ordinary stop run — the core only
/// adds the token flip and the recorded origin.
#[expect(
    clippy::too_many_arguments,
    reason = "cancellation entry mutates seven distinct pieces of the loop's own \
              bookkeeping (origin, draining/stopping flags, drain deadline, pending \
              queue, terminal states); they are borrowed individually so the borrow \
              checker still separates them, which a bundling struct would give up"
)]
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
    // later external interrupt keeps the failure origin for exit-code precedence).
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
    // The admit-no-more-default-work + cancel-pending-unrelated-default rule. On
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

/// The declared cost of `name`, read from its node policy — the per-pool demand
/// the admission controller acquires against. Reads the node's `NodePolicy::cost`
/// without duplicating the definition; an unknown node (a framework defect handled
/// downstream) demands nothing.
fn declared_cost(ctx: &AdmitCtx, name: &str) -> PoolCost {
    ctx.pipeline
        .node(NodeId::from_name(name))
        .map_or_else(PoolCost::new, |n| {
            PoolCost::from_policy(n.policy(), ctx.placement_handling)
        })
}

/// Offer `name` to the admission controller. If its declared cost fits every pool
/// it is **admitted** immediately (spawned, one more in flight); if a pool is at
/// capacity it is **held** in `pending` (oldest-ready-first) to be re-offered when
/// a release frees capacity. Under the default unconstrained pools every ready
/// node fits, so this admits at once.
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
    let cost = declared_cost(ctx, name);
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
        // full bootstrap-time rejection of too-big nodes runs before the loop starts.
        None if !admission.can_ever_fit(&cost) => {
            reject_over_demand(name, admission, &cost, ctx.tx);
            *in_flight += 1;
        }
        None => pending.push_back(name.to_string()),
    }
}

/// Fail a **can-never-fit** node terminally instead of stranding it (the
/// termination guard). Its declared cost exceeds a pool's total capacity, so no
/// release could ever admit it; leaving it in `pending` would strand it past run
/// end with no terminal state. We give it a `Failed` terminal carrying the honest
/// over-demand reason and feed it back through the loop's normal terminal path
/// (via `tx`), so it is recorded, cascaded to dependents, and folds the run to a
/// `Failed` outcome — the same shape the no-runner framework-defect path uses. The
/// caller counts it into `in_flight`.
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
    let _ = tx.send(AttemptDone::attempt(
        name.to_string(),
        TerminalState::Failed,
        vec![AttemptEvent::NodeTerminal {
            node: name.to_string(),
            state: TerminalState::Failed,
        }],
    ));
}

/// Fail a waiter whose capacity is **pinned by an abandoned (timed-out) closure**
/// that did not return within the grace period — the zombie-shaped sibling of
/// [`reject_over_demand`]'s strand guard.
///
/// A timed-out blocking/compute attempt holds its permit until its closure actually
/// returns (C14), and that closure may never return. A node queued for the capacity
/// it pins would then sit in `pending` past run end with no terminal state — a
/// silent violation of *"every node ends in exactly one terminal state"*. It is
/// instead failed with the honest reason and fed through the loop's normal terminal
/// path, so it is recorded, cascaded to its dependents, and folded into the run's
/// outcome. The caller counts it into `in_flight`.
fn reject_zombie_pinned(
    name: &str,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    in_flight: &mut usize,
) {
    eprintln!(
        "node '{name}' could not be admitted: the capacity it needs is pinned by a timed-out \
         attempt whose closure has not returned within the grace period; failing it"
    );
    let _ = tx.send(AttemptDone::attempt(
        name.to_string(),
        TerminalState::Failed,
        vec![AttemptEvent::NodeTerminal {
            node: name.to_string(),
            state: TerminalState::Failed,
        }],
    ));
    *in_flight += 1;
}

/// Whether a terminal state is **failure-like** — the trigger for
/// stop-on-first-failure. `cancelled` (stop-like) and the skip classes never
/// trigger a stop; only a genuine failure does.
fn is_failure_like(state: TerminalState) -> bool {
    matches!(
        state,
        TerminalState::Failed
            | TerminalState::TimedOut
            | TerminalState::Abandoned
            | TerminalState::UpstreamFailed
    )
}

/// Whether `name` runs under the **default** `all-succeeded` trigger rule. A
/// default-rule node is ordinary work; a **non-default**-rule node
/// (`all-terminal` / `any-failed`) is a consume-nothing contingency — the work a
/// failure is meant to trigger — which stop mode must still run. An unknown node
/// (a framework defect handled elsewhere) is treated as default-rule.
fn is_default_rule_node(pipeline: &Pipeline, name: &str) -> bool {
    pipeline
        .node(NodeId::from_name(name))
        .is_none_or(|n| n.trigger_rule() == dagr_core::binding::TriggerRule::AllSucceeded)
}

/// Mark `name` **`cancelled`** without executing it (stop mode): it was a pending
/// default-rule node unrelated to the failure, or a newly-ready default-rule node
/// the stop refuses to admit. It never acquired an admission permit (never
/// admitted), so there is nothing to release. A `NodeTerminal(cancelled)`
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
    let _ = tx.send(AttemptDone::attempt(
        name.to_string(),
        TerminalState::Cancelled,
        vec![AttemptEvent::NodeTerminal {
            node: name.to_string(),
            state: TerminalState::Cancelled,
        }],
    ));
    *in_flight += 1;
}

/// At the stop-on-first-failure transition, **cancel every default-rule node still
/// waiting for capacity**: these are pending nodes unrelated to the failure that
/// stop mode declines to admit. A **non-default-rule** contingency in
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

/// Re-offer the pending waiters oldest-first after a release freed capacity.
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
    // it). This is the bounded-bypass discipline that guards against starvation.
    let mut index = 0;
    while index < pending.len() {
        let name = pending[index].clone();
        let cost = declared_cost(ctx, &name);
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
/// execution surface named by its **effective execution class** — the async task
/// runtime for [await-bound](ExecutionClass::AwaitBound), the dedicated blocking
/// pool for [blocking](ExecutionClass::Blocking), the fixed compute pool for
/// [compute](ExecutionClass::Compute) — which reports the terminal state and
/// buffered records back over `tx` when it finishes.
///
/// The effective class is [`PipelineNode::effective_class`], which is the policy
/// override if one is set (validated legal at assembly — an illegal override never
/// assembles, so it never reaches here) else the class the task declared
/// ([`Task::EXECUTION_CLASS`]). Resolving it here, at dispatch time, is the whole
/// of the class routing.
///
/// `permit` is the admission permit acquired for this attempt. It is **moved into
/// the dispatched closure** — the ownership trick — so it is dropped (and its cost
/// released to every pool) exactly when the attempt returns, *before* the loop is
/// told the attempt is done, on whichever surface ran it. That is what keeps the
/// permit held for the whole attempt and released on its terminal outcome; a
/// blocking/compute timeout zombie that runs on past its mark keeps holding it
/// until its closure actually returns (this driver does not fabricate an early
/// release, and dispatch does not change permit mechanics).
fn admit<S, C>(actx: &AdmitCtx, name: &str, writer: &mut EventStreamWriter<S, C>, permit: Permit)
where
    S: EventSink,
    C: MonotonicClock,
{
    let _ = writer.node_ready(name);
    // Node identity is name-derived, so this is the same id assembly and the
    // tracker use — no pipeline lookup needed.
    let node_id = NodeId::from_name(name);

    // Resolve the effective execution class at dispatch: the policy override if set
    // (assembly already rejected any illegal override), else the task's declared
    // class. An unknown node (a framework defect handled below) defaults to
    // await-bound.
    let class = actx.pipeline.node(node_id).map_or(
        ExecutionClass::AwaitBound,
        dagr_core::flow::PipelineNode::effective_class,
    );

    // Poison policy: panic — the runner map is take-once per node; recovering a
    // half-mutated map could hand the same runner out twice.
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
        let _ = actx.tx.send(AttemptDone::attempt(
            name.to_string(),
            TerminalState::Failed,
            Vec::new(),
        ));
        return;
    };

    // Register this node as in flight: on cancellation the drain reads this set to
    // know which attempts to await and, past grace, abandon. Removed by the loop
    // when the attempt's `AttemptDone` arrives.
    // Poison policy: panic — the live set, as in `abandon_leftover`.
    actx.live
        .lock()
        .expect("live set not poisoned")
        .insert(name.to_string());

    // The per-attempt deadline of an unkillable node, armed on the isolated
    // framework runtime; an ordinary node's permit is simply moved into its closure.
    let permit = arm_unkillable_deadline(actx, name, node_id, class, runner.as_ref(), permit);

    // The per-attempt **child** cancellation signal: each attempt observes its own
    // child of the run-scoped token, so a run cancel reaches every live attempt at
    // once while the task-facing side stays observe-only. A non-cancelled run's
    // child is never flipped, so the attempt sees exactly the fresh-uncancelled
    // signal.
    let attempt_signal = actx.run_cancel.child().signal();

    let run_id = actx.run_id.to_string();
    let name_owned = name.to_string();
    let dispatcher = actx.dispatcher;
    let tx = actx.tx.clone();
    // The run's per-run temp directory, threaded into the attempt's context so a
    // task reaches its confined local scratch through the context
    // (`RunContext::temp_dir`). Owned into the future so it outlives `actx`.
    let temp_dir = actx.temp_dir.to_path_buf();
    // The run-store base, threaded into the attempt's context so a task reaches its
    // **durable scratch store** through `RunContext::scratch` — its per-node
    // namespace `<base>/<pipeline>/<run-id>/scratch/<node>/`. A task that touches no
    // scratch is unaffected. Owned into the future so it outlives `actx`.
    let scratch_base = actx.scratch_base.to_string();
    let pipeline_name = actx.pipeline_name.to_string();
    // The attempt future — driven on the surface `class` names. It owns the runner,
    // the buffering sink, and the permit; producing the `(state, events)` the loop
    // records once the attempt returns.
    let attempt = async move {
        // A per-attempt buffering sink: the attempt emits into it off the
        // framework runtime; the loop drains it into the writer in order.
        let mut sink = BufferingSink::default();
        let ctx = RunContext::builder(
            CoreRunId::new(run_id),
            PipelineId::new(pipeline_name),
            node_id,
        )
        .cancellation(attempt_signal)
        .temp_dir(temp_dir)
        // The run-store base, so the node's scratch resolves to its real per-node
        // namespace under the run directory (where a resume carries prior scratch
        // forward). The namespace directory is created LAZILY on the first write, so
        // a task that never touches scratch leaves no subtree and its run is
        // byte-identical (the run store's base is always non-empty in a real run).
        .scratch_root(std::path::PathBuf::from(scratch_base))
        .build();
        // Open the attempt span — run/node/attempt identity — and instrument the
        // attempt future with it, so every line the task or a third-party library it
        // calls emits beneath this future carries that identity across `.await`
        // points and is attributable without timestamp correlation. This attaches to
        // (does not compete with) the attempt lifecycle; its identity is read off
        // the context's dep-free `LogSpan`.
        let span = crate::logging::attempt_span_from(ctx.span(), &name_owned);
        let state = runner.run(&ctx, &mut sink).instrument(span).await;
        // A durable node's runner reports the reference its output serialized once
        // the attempt succeeded; the loop stamps it onto the succeeded
        // `attempt-outcome` record so a later resume can rehydrate the value. `None`
        // for every non-durable node (the default), so the stream is byte-identical
        // for a non-durable run.
        let durable_reference = if state == TerminalState::Succeeded {
            runner.durable_reference()
        } else {
            None
        };
        // Alongside the reference, a durable runner may report OPTIONAL metadata
        // (T89: content hash / size / scheme / produced-at). `None` for every
        // non-durable node (the default), keeping the stream byte-identical.
        let durable_reference_meta = if state == TerminalState::Succeeded {
            runner.durable_reference_meta()
        } else {
            None
        };
        // Release the admission permit at the attempt's terminal state (its working
        // memory + thread cost returns to the pools) BEFORE reporting done, so the
        // loop sees freed capacity when it re-offers the pending waiters. An
        // await-bound cancellation would drop the permit with the future instead;
        // a blocking/compute-timeout zombie keeps it until its closure returns —
        // which is *this* release, reached only when the abandoned closure finally
        // returns. The permit drops on whichever surface ran the attempt.
        permit.release();
        (
            name_owned,
            state,
            sink.drain(),
            durable_reference,
            durable_reference_meta,
        )
    };
    // Route by class. `on_done` sends the finished attempt back to the framework
    // loop over `tx`; it runs on the surface the attempt ran on, off the framework
    // runtime, so a jammed task surface never touches the writer.
    dispatcher.dispatch(
        class,
        attempt,
        move |(node, state, events, durable_reference, durable_reference_meta)| {
            let _ = tx.send(AttemptDone {
                node,
                state,
                events,
                durable_reference,
                durable_reference_meta,
                kind: DoneKind::Attempt,
            });
        },
    );
}

/// Act on each decision the tracker unlocked. A [`Decision::Ready`] node is
/// **offered to admission** (admitted if its pools fit, else held in `pending`); a
/// [`Decision::PropagatedTerminal`] node is recorded directly — it never executes
/// — and its cascade is already folded into the tracker, so its own dependents'
/// decisions are handled recursively here. Admitted nodes are counted into
/// `in_flight` by [`offer_or_pend`].
#[expect(
    clippy::too_many_arguments,
    reason = "the readiness tracker's decisions are applied against six independently \
              borrowed pieces of loop state; passing them separately is what lets one \
              call mutate the terminal-state map and the pending queue at once"
)]
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
                    // A node already settled terminal (a resume pre-satisfied node)
                    // is decided, not ready — never offer it to admission.
                    if terminal_states.contains_key(&name) {
                        continue;
                    }
                    // Under a **full drain** (an external interrupt) no new work is
                    // admitted at all — every newly-ready node is settled `cancelled`
                    // (including a contingency). Under a **stop** only, a newly-ready
                    // **default-rule** node is cancelled while a **non-default-rule**
                    // contingency whose rule fired is the work a failure is meant to
                    // trigger, so it is still admitted.
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

/// The **consumed durable inputs** (T90) a node read on a fresh run: for each of
/// its static **data-edge** producers that recorded a durable reference (present
/// in `produced_refs`), one `{ uri, content_hash }` in the node's declared input
/// order. A node with no data edge — or whose producers are all non-durable —
/// reads no durable reference and yields an empty list, so its record is
/// byte-identical. Data already computed at build time (C3/C11), so this is
/// near-free; ordering edges (C4) carry no value and are excluded.
fn consumed_inputs_for(
    pipeline: &Pipeline,
    node: &str,
    produced_refs: &BTreeMap<String, ConsumedInput>,
) -> Vec<ConsumedInput> {
    let Some(pnode) = pipeline.node(NodeId::from_name(node)) else {
        return Vec::new();
    };
    let mut inputs = Vec::new();
    for edge in pnode.data_edges() {
        if let Some(producer) = node_name(pipeline, edge.upstream()) {
            if let Some(reference) = produced_refs.get(&producer) {
                inputs.push(reference.clone());
            }
        }
    }
    inputs
}

/// Split a run's runners into (**main**, **teardown**) sets by the teardown
/// flag. The main set drives the readiness loop; the teardown set is held
/// back for the post-loop [teardown phase](run_teardown_phase). A pipeline with no
/// teardown node yields the full set plus an empty teardown map — so the loop is
/// byte-identical to the pre-teardown driver.
fn partition_teardown_runners(pipeline: &Pipeline, runners: RunnerMap) -> (RunnerMap, RunnerMap) {
    let is_teardown = |name: &str| {
        pipeline
            .node(NodeId::from_name(name))
            .is_some_and(|n| n.policy().is_teardown())
    };
    let mut main = BTreeMap::new();
    let mut teardown = BTreeMap::new();
    for (name, runner) in runners {
        if is_teardown(&name) {
            teardown.insert(name, runner);
        } else {
            main.insert(name, runner);
        }
    }
    (main, teardown)
}

/// The **teardown phase**: run every teardown node once the main graph is
/// terminal, on **every** exit path, and fold each teardown's own terminal into
/// `terminal_states`.
///
/// Each teardown runs under a **fresh, uncancelled** [`CancellationSource`] (so a
/// cancelled run still cleans up after itself), bounded by the operator's
/// `teardown_deadline` (default 15 s). It bypasses admission — no permit, no
/// pool cost — so it never competes with the run it is cleaning up after. Its
/// context exposes the terminal states of the nodes it covers (from the completed
/// run's `covered_states`), so cleanup can no-op when setup never ran.
///
/// Failure isolation: a teardown that fails is recorded `failed`, but the run's
/// overall `outcome` was already computed over the non-teardown nodes only, and
/// each teardown runs independently — one teardown's failure (or its deadline
/// being hit) never prevents the others from running. On an abrupt process kill
/// mid-teardown, cleanup is best-effort by design: the deadline bounds each
/// attempt, and the driver proceeds rather than hanging.
///
/// Teardowns run in deterministic name order on a small bounded runtime, one at a
/// time (a teardown consumes nothing and holds no capacity, so there is nothing to
/// parallelize and serial order keeps the stream deterministic).
#[expect(
    clippy::too_many_arguments,
    reason = "the teardown phase needs the full run identity, the covered terminal \
              states, both directory roots, the deadline, and the writer — the same \
              orthogonal set the main drive loop holds, handed over rather than \
              rebuilt"
)]
fn run_teardown_phase<S, C>(
    pipeline: &Pipeline,
    run_id: &str,
    pipeline_name: &str,
    mut teardown_runners: RunnerMap,
    covered_states: &BTreeMap<String, TerminalState>,
    teardown_deadline: Duration,
    temp_dir: &std::path::Path,
    scratch_base: &str,
    writer: &mut EventStreamWriter<S, C>,
    terminal_states: &mut BTreeMap<String, TerminalState>,
) where
    S: EventSink,
    C: MonotonicClock,
{
    // The teardown → covered-node-names map (name-ordered by the BTreeMap). Empty
    // for a no-teardown pipeline, so this whole phase is a no-op then.
    let covered = pipeline.teardown_covered_nodes();
    if covered.is_empty() {
        return;
    }

    // A small isolated runtime for the teardown phase — separate from the run
    // loop's framework runtime (already dropped) and from the task surfaces (shut
    // down). Enables timers so the per-teardown deadline can bound a runaway body.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .expect("teardown-phase runtime builds");

    for (name, runner) in &covered {
        let Some(mut runner_box) = teardown_runners.remove(name) else {
            // A framework defect (a teardown node with no runner): decide it failed
            // rather than skip cleanup silently, and keep going with the rest.
            let _ = writer.node_ready(name);
            record_terminal(name, TerminalState::Failed, terminal_states);
            let _ = writer.node_terminal(name, wire_terminal(TerminalState::Failed));
            continue;
        };

        // The teardown's context: its covered nodes' terminal states, and a FRESH,
        // uncancelled signal — never the run's (possibly cancelled) token. A covered
        // node absent from `covered_states` simply is not recorded, which is exactly
        // the "setup never ran" no-op case the teardown branches on.
        let mut covered_view = CoveredNodeStates::new();
        for covered_name in runner {
            if let Some(state) = covered_states.get(covered_name) {
                covered_view = covered_view.with(NodeId::from_name(covered_name), *state);
            }
        }
        let fresh = CancellationSource::new();
        let node_id = NodeId::from_name(name);
        let ctx = RunContext::builder(
            CoreRunId::new(run_id.to_string()),
            PipelineId::new(pipeline_name),
            node_id,
        )
        .cancellation(fresh.signal())
        .covered_terminal_states(covered_view)
        .temp_dir(temp_dir.to_path_buf())
        // A teardown reaches its own per-node scratch namespace too, so a teardown
        // that checkpoints is on the same footing as any node.
        .scratch_root(std::path::PathBuf::from(scratch_base))
        .build();

        // Emit `node-ready` (mirroring the main loop's admit), then run the teardown
        // attempt bounded by the teardown deadline. A teardown that does not return
        // within its deadline is recorded `abandoned` (best-effort cleanup): the
        // deadline is the bound that guarantees the phase terminates even if a
        // teardown body ignores cooperation.
        let _ = writer.node_ready(name);
        let mut sink = BufferingSink::default();
        let state = rt.block_on(async {
            match tokio::time::timeout(teardown_deadline, runner_box.run(&ctx, &mut sink)).await {
                Ok(state) => state,
                // Deadline hit: the attempt's own records may be incomplete. Record
                // the node `abandoned` (a runaway teardown left behind past its
                // budget) and proceed. Its buffered opening records still flush below
                // so the stream honestly shows it started.
                Err(_elapsed) => TerminalState::Abandoned,
            }
        });

        // Drain the teardown attempt's buffered records and record its single
        // terminal, exactly like the main loop's `record_attempt_outcome` for a
        // non-drain outcome. On a deadline hit the buffered node-terminal (if any)
        // is suppressed in favour of the authoritative `abandoned` terminal.
        let deadline_hit = state == TerminalState::Abandoned
            && !sink_reported_terminal(&sink, TerminalState::Abandoned);
        let done = AttemptDone::attempt(name.clone(), state, sink.drain());
        record_teardown_outcome(&done, deadline_hit, writer, terminal_states);
    }
}

/// Whether the teardown's buffered records already carry a node-terminal for
/// `state` — used to decide whether a deadline-imposed `abandoned` is the
/// authoritative terminal (the body did not emit its own) or the body genuinely
/// returned `abandoned` on its own.
fn sink_reported_terminal(sink: &BufferingSink, _state: TerminalState) -> bool {
    // Poison policy: panic — the teardown record buffer, as in `BufferingSink`.
    sink.records
        .lock()
        .expect("event buffer mutex not poisoned")
        .iter()
        .any(|ev| matches!(ev, AttemptEvent::NodeTerminal { .. }))
}

/// Record one teardown attempt's outcome into the stream and `terminal_states` —
/// the teardown-phase analogue of [`record_attempt_outcome`], minus the
/// cancellation-drain reclassification (a teardown runs under a fresh, uncancelled
/// signal, so there is no drain to reclassify against). Drains the buffered
/// per-transition records, emits the single `attempt-outcome` record alongside the
/// closing outcome event, and records the node's terminal exactly once.
///
/// When `deadline_imposed` is set (the teardown blew its deadline and never
/// emitted its own terminal), the buffered node-terminal is suppressed and one
/// authoritative `abandoned` outcome + terminal is emitted instead — mirroring how
/// the drain path substitutes an authoritative `cancelled`.
fn record_teardown_outcome<S, C>(
    done: &AttemptDone,
    deadline_imposed: bool,
    writer: &mut EventStreamWriter<S, C>,
    terminal_states: &mut BTreeMap<String, TerminalState>,
) where
    S: EventSink,
    C: MonotonicClock,
{
    for ev in &done.events {
        if deadline_imposed && matches!(ev, AttemptEvent::NodeTerminal { .. }) {
            continue;
        }
        let _ = write_attempt_event(writer, ev);
        if !deadline_imposed {
            if let Some(record) = closing_outcome_record(&done.node, ev) {
                let _ = writer.attempt_outcome(record);
            }
        }
    }
    if deadline_imposed {
        let attempt = attempt_number_of(&done.events);
        let _ = writer.attempt_outcome(AttemptOutcomeRecord::new(
            &done.node,
            attempt,
            wire_terminal(TerminalState::Abandoned).as_str(),
        ));
        record_terminal(&done.node, TerminalState::Abandoned, terminal_states);
        let _ = writer.node_terminal(&done.node, wire_terminal(TerminalState::Abandoned));
    } else {
        record_terminal(&done.node, done.state, terminal_states);
    }
}

/// Record a node's terminal state exactly once (a node's terminal state is
/// decided exactly once; a repeat is a defensive no-op).
fn record_terminal(
    node: &str,
    state: TerminalState,
    terminal_states: &mut BTreeMap<String, TerminalState>,
) {
    terminal_states.entry(node.to_string()).or_insert(state);
}

/// Whether a terminal state marks a **zombie candidate** at run end: a blocking
/// timeout (or a left-behind abandoned closure) whose leftover work may still be
/// running. The driver has no permit ledger to confirm the closure returned, so it
/// treats a `timed-out`/`abandoned` node as a candidate and emits a
/// `zombie-at-exit` event for it after the bounded grace wait.
fn is_zombie_candidate(state: TerminalState) -> bool {
    matches!(state, TerminalState::TimedOut | TerminalState::Abandoned)
}

/// The overall run outcome from the per-node terminal states: failed if any node
/// ended failure-like, cancelled if any ended stop-like (and none failure-like),
/// else succeeded. A run containing only skips (or successes) is a **successful**
/// run.
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

/// The **bounded final flush** at shutdown (the fsync-at-run-end boundary).
/// Perform the single run-end/cancellation `fsync` through the sink
/// (`writer.finish()`), and report whether it succeeded.
///
/// Returns `true` when the flush completed (the stream is complete and durable),
/// `false` when the sink was **unwritable at shutdown** — the distinct sink-failure
/// path. The `finish` call is itself the bounded operation: the sink's `flush`
/// either returns or errors, so the wait is bounded by the sink and never a hang;
/// the caller maps a `false` here onto [`ShutdownExit::SinkFailure`] within the
/// [final-flush budget](DEFAULT_FINAL_FLUSH). On failure a best-effort report goes
/// to stderr (operator-facing, never into the event stream).
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

/// Select the [shutdown exit](ShutdownExit) by precedence: run failure > sink
/// failure > cancellation > success.
///
/// `outcome` is the overall run outcome, `origin` the recorded cancellation origin
/// (if any), and `flush_ok` whether the [bounded final flush](final_flush)
/// succeeded. A run failure (a non-teardown node ended `failed`/`timed-out`, which
/// also covers a `FailureUnderStop` cancellation) wins over everything; otherwise a
/// failed final flush is the distinct sink-failure code; otherwise an external
/// interrupt is a cancellation; otherwise success. The driver reports this — it
/// does not own the numeric mapping.
fn select_shutdown_exit(
    outcome: RunOutcome,
    origin: Option<CancellationOrigin>,
    flush_ok: bool,
) -> ShutdownExit {
    // 1. Run failure wins (a genuine node failure, incl. a stop-on-first-failure
    //    cancellation whose origin is a failure; an assembly/bootstrap failure is
    //    likewise a run failure for exit-code purposes — the full code table and its
    //    distinct assembly/bootstrap codes live in the run verb, so they fold under
    //    `RunFailure` here, which this selection does not claim to enumerate).
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
