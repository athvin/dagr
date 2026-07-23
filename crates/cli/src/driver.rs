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
//!    [`ReadinessTracker`](dagr_core::readiness::ReadinessTracker) reports, spawns
//!    each admitted node's attempt through the C14 attempt runner on the
//!    **task-execution runtime**, and feeds every terminal outcome back into the
//!    tracker so dependents decrement and either become ready or receive their
//!    propagated terminal state — **never batching a whole level into a wave**;
//! 4. runs its own machinery (the loop, timers, cancellation fan-out, the
//!    event-stream writer) on the **isolated framework runtime** per the T2 ADR,
//!    kept off the task-execution runtime so a misbehaving task cannot disable the
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
//! # The two runtimes (T2 · isolated framework runtime)
//!
//! Per the T2 async-runtime ADR the framework machinery runs on an **isolated**
//! runtime, separate from the runtime task attempts execute on. The driver builds
//! **two** multi-threaded tokio runtimes: a `framework` runtime that drives the
//! loop, the per-attempt timers, and the event writer, and a `tasks` runtime that
//! attempts are spawned onto. A task that jams every `tasks` worker (a blocking
//! busy-loop) therefore cannot stall the framework runtime — the per-attempt
//! timeout still fires and the event stream is still written (the
//! all-workers-blocked scenario, C13-adjacent; the full dispatch story is T33).
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
//! scheduler; it admits the nodes the tracker/runner hand it against whatever
//! admission surface the runner already exposes. Advanced concurrency dispatch
//! (T33), deadlock property tests (T25), the hundred-node scale authority (T26),
//! fault injection (T27), the permit/semaphore matrix (T31), runtime firing of
//! non-default trigger rules and cancellation triggering (T34/T35), the run
//! artifact fold (T42), and resume (C27) all belong to later tickets. This loop
//! only consumes the C16 grace period as the bounded zombie wait at *natural* run
//! end; it triggers no cancellation and handles no signals.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{
    Event, EventSink, EventStreamWriter, MonotonicClock, RunOutcome, RunStartedHeader,
    TerminalState as WireTerminalState,
};
pub use dagr_artifact::event_stream::{RunId, RunOutcome as OverallOutcome};
use dagr_core::assembly::AssemblyError;
use dagr_core::context::{PipelineId, RunContext, RunId as CoreRunId, TerminalState};
use dagr_core::execution::{AttemptEvent, AttemptEventSink};
use dagr_core::flow::Pipeline;
use dagr_core::handle::NodeId;
use dagr_core::readiness::{Decision, ReadinessTracker};

/// The default bounded grace period the driver waits for a zombie closure to
/// return at natural run end (arch.md C16; T35 makes it a flag). A blocking
/// timeout's leftover thread is given at most this long before a `zombie-at-exit`
/// event is emitted and the run proceeds.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(10);

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
    parameters: BTreeMap<String, String>,
    data_interval: Option<[String; 2]>,
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
            parameters: BTreeMap::new(),
            data_interval: None,
        }
    }

    /// Override the minted run identity with an operator-supplied value, used
    /// **verbatim** everywhere the minted id would appear (T0.6 §4).
    #[must_use]
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Set the bounded zombie grace period (default [`DEFAULT_GRACE`]).
    #[must_use]
    pub fn grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
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
}

impl RunPlan {
    /// Build a run plan over an assembled `pipeline` and its node `runners` (keyed
    /// by node name). Every node in the pipeline should have a runner; a node with
    /// no runner is treated as an immediate framework defect at drive time.
    #[must_use]
    pub fn new(pipeline: Pipeline, runners: BTreeMap<String, Box<dyn NodeRunner>>) -> Self {
        Self { pipeline, runners }
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
        let mut guard = self.records.lock().expect("event buffer mutex not poisoned");
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
/// # Panics
///
/// Panics only on a framework defect it cannot record (a poisoned internal mutex);
/// a sink fault is surfaced through the returned report's outcome, never a panic.
#[must_use]
pub fn drive<S, C>(
    _config: &RunConfig,
    _pipeline_name: &str,
    _assembled: Result<RunPlan, AssemblyError>,
    _env_allowlist: &[String],
    _sink: S,
    _clock: C,
) -> RunReport
where
    S: EventSink + 'static,
    C: MonotonicClock + 'static,
{
    // Implemented in the next commit (TDD: tests are written and failing first).
    unimplemented!("run-loop driver body lands after the failing tests")
}

// Silence unused-item warnings on the skeleton the implementation commit
// consumes, so the failing-tests commit still compiles under `-D warnings`. The
// implementation commit deletes this shim.
#[allow(dead_code)]
fn _touch_skeleton(config: &RunConfig, plan: &RunPlan) {
    let _ = config.resolve_run_id();
    let _ = (&config.base, config.grace, &config.parameters, &config.data_interval);
    let _ = plan.pipeline.len();
    let _ = plan.runners.len();
    let _ = std::any::type_name::<AtomicBool>();
    let _ = std::any::type_name::<CoreRunId>();
    let _ = std::any::type_name::<PipelineId>();
    let _ = std::any::type_name::<NodeId>();
    let _ = std::any::type_name::<Decision>();
    let _ = std::any::type_name::<ReadinessTracker>();
    let _ = std::any::type_name::<RunStartedHeader>();
    let _ = std::any::type_name::<OverallOutcome>();
    let _ = Ordering::SeqCst;
    let _: fn(&BufferingSink) -> Vec<AttemptEvent> = BufferingSink::drain;
    let _: fn(
        &mut EventStreamWriter<VoidSink, ZeroClock>,
        &AttemptEvent,
    ) -> Result<(), dagr_artifact::event_stream::SinkFault> = write_attempt_event;
}

// Minimal internal sink/clock stand-ins used only by `_touch_imports` above.
struct VoidSink;
impl EventSink for VoidSink {
    fn append_line(&mut self, _line: &[u8]) -> std::io::Result<()> {
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct ZeroClock;
impl MonotonicClock for ZeroClock {
    fn elapsed_ns(&self) -> u64 {
        0
    }
}
