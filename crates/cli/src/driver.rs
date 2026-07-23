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
//!    spawns each admitted node's attempt through the C14 attempt runner on the
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{
    Event, EventSink, EventStreamWriter, MonotonicClock, RunOutcome, RunStartedHeader,
    TerminalState as WireTerminalState,
};
pub use dagr_artifact::event_stream::{RunId, RunOutcome as OverallOutcome};
use dagr_core::admission::{AdmissionController, Permit, PoolCapacities, PoolCost};
use dagr_core::assembly::AssemblyError;
use dagr_core::context::{PipelineId, RunContext, RunId as CoreRunId, TerminalState};
use dagr_core::execution::{AttemptEvent, AttemptEventSink};
use dagr_core::flow::Pipeline;
use dagr_core::handle::NodeId;
use dagr_core::limits::detect_capacities;
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
    capacities: PoolCapacities,
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
            // Admission pools default to **unconstrained** (T31 takes capacities as
            // an input; deriving them from container limits is T32). An
            // unconstrained controller admits every ready node immediately, so the
            // M1 run loop's behaviour is unchanged unless a capacity is pinned.
            capacities: PoolCapacities::new(),
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
    /// The run-store event-stream path this run wrote under —
    /// `<base>/<pipeline>/<run-id>/events.jsonl` (T0.6 §3). Because the path
    /// embeds both the pipeline identity and the run-unique id, two concurrent
    /// runs — even of the same binary and pipeline — write disjoint files.
    pub stream_path: String,
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
    // --- Bootstrap: mint identity, open the stream BEFORE assembly is acted on.
    let run_id = config.resolve_run_id();
    let run_id_str = run_id.as_str().to_string();
    let mut writer = EventStreamWriter::new(sink, clock, run_id, pipeline_name.to_string());
    // The run-store path this run writes under: <base>/<pipeline>/<run-id>/…
    // (T0.6 §3). Two concurrent runs write disjoint files by construction.
    let stream_path = writer.stream_path(&config.base);

    // Capture the allowlisted environment values (empty allowlist → nothing).
    let captured_env = capture_env(env_allowlist);

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
                parameters: config.parameters.clone(),
                data_interval: config.data_interval.clone(),
                captured_env,
                resumed_from: None,
            };
            let _ = writer.run_started(header);
            let _ = writer.run_finished(RunOutcome::AssemblyFailed);
            let _ = writer.finish();
            return RunReport {
                outcome: RunOutcome::AssemblyFailed,
                terminal_states: BTreeMap::new(),
                run_id: run_id_str,
                stream_path,
            };
        }
    };

    // --- The successful path: assembly produced a valid artifact. Emit the
    // run-started header carrying every field known at start (both fingerprints
    // present because assembly succeeded), then drive the execution loop.
    let RunPlan { pipeline, runners } = plan;
    let artifact = pipeline
        .assemble()
        .expect("the plan carries an already-assembled pipeline");
    let fp = artifact.fingerprint();
    let header = RunStartedHeader {
        pipeline: pipeline_name.to_string(),
        fingerprint_structural: Some(format!("{:016x}", fp.structural())),
        fingerprint_policy: Some(format!("{:016x}", fp.policy())),
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
        let _ = writer.finish();
        return RunReport {
            outcome: RunOutcome::BootstrapFailed,
            terminal_states: BTreeMap::new(),
            run_id: run_id_str,
            stream_path,
        };
    }

    let tracker = ReadinessTracker::new(&pipeline, &artifact);
    // The C12 admission controller for this run (T31). Its pools are pinned from
    // the run config (container-limit-derived or operator-pinned — T32). The
    // too-big-node bootstrap check above already rejected any node that could never
    // fit, so the loop's admission never strands a can-never-fit node here.
    let admission = AdmissionController::new(config.capacities);
    let (outcome, terminal_states) = run_loop(
        &pipeline,
        &run_id_str,
        pipeline_name,
        runners,
        tracker,
        config.grace,
        &admission,
        &mut writer,
    );

    let _ = writer.run_finished(outcome);
    let _ = writer.finish();

    RunReport {
        outcome,
        terminal_states,
        run_id: run_id_str,
        stream_path,
    }
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

/// The readiness-driven execution loop (arch.md C11; the driver's half of the
/// run-end condition).
///
/// It runs on the isolated **framework runtime** and admits ready nodes onto a
/// separate **tasks runtime**, feeding each terminal outcome back into the tracker
/// so dependents decrement and either become ready (admitted next) or receive
/// their propagated terminal state (recorded without executing) — never batching a
/// level into a wave. It terminates precisely when nothing is pending and nothing
/// is in flight, then waits the bounded grace period for zombie candidates
/// (blocking timeouts) and emits a `zombie-at-exit` event for each. Returns the
/// overall outcome and the per-node terminal states.
#[allow(clippy::too_many_arguments)]
fn run_loop<S, C>(
    pipeline: &Pipeline,
    run_id: &str,
    _pipeline_name: &str,
    runners: BTreeMap<String, Box<dyn NodeRunner>>,
    mut tracker: ReadinessTracker,
    grace: Duration,
    admission: &AdmissionController,
    writer: &mut EventStreamWriter<S, C>,
) -> (RunOutcome, BTreeMap<String, TerminalState>)
where
    S: EventSink,
    C: MonotonicClock,
{
    // The tasks runtime — a separate multi-threaded runtime attempts spawn onto,
    // so a task jamming every task worker cannot stall the framework runtime that
    // drives this loop, its timers, and the writer (T2 · isolated framework
    // runtime).
    let tasks = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("tasks runtime builds");
    // The framework runtime — drives this loop, the grace timer, and the drain.
    let framework = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .expect("framework runtime builds");

    let runners = Arc::new(Mutex::new(runners));
    let mut terminal_states: BTreeMap<String, TerminalState> = BTreeMap::new();
    let mut zombie_candidates: Vec<String> = Vec::new();

    framework.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AttemptDone>();
        // Nodes admitted and not yet reported terminal — the "in flight" count.
        let mut in_flight: usize = 0;
        // Ready nodes that could not yet acquire their C12 permit (a pool at
        // capacity), oldest-ready-first (T31). Each is re-offered when a permit is
        // released — a terminal outcome frees capacity, which is what unblocks the
        // next waiter. Under the default unconstrained pools this stays empty and
        // every ready node is admitted at once (the M1 behaviour).
        let mut pending: std::collections::VecDeque<String> = std::collections::VecDeque::new();

        // Offer the initial-ready frontier (every zero-dependency source node) to
        // admission. A node that fits its pools is admitted (in flight); one that
        // does not waits in `pending` for a release.
        for id in tracker.initial_ready().to_vec() {
            if let Some(name) = node_name(pipeline, id) {
                offer_or_pend(
                    pipeline,
                    run_id,
                    &name,
                    &runners,
                    &tasks,
                    &tx,
                    admission,
                    writer,
                    &mut pending,
                    &mut in_flight,
                );
            }
        }

        // Drive until nothing is pending, nothing is in flight, and no ready node
        // is waiting for capacity. A node whose attempt reports terminal is fed
        // back into the tracker; each unlocked decision either offers a ready node
        // to admission or records a propagated terminal (which cascades, without
        // executing). A terminal outcome also releases that attempt's permit, so
        // the pending waiters are re-offered against the freed capacity.
        while in_flight > 0 {
            let Some(done) = rx.recv().await else { break };
            in_flight -= 1;
            // Drain this attempt's buffered records into the single-owner writer.
            for ev in &done.events {
                let _ = write_attempt_event(writer, ev);
            }
            record_terminal(&done.node, done.state, &mut terminal_states);
            if is_zombie_candidate(done.state) {
                zombie_candidates.push(done.node.clone());
            }
            // Feed the executed-terminal outcome back into the tracker and act on
            // every decision it unlocks (ready → offer to admission; propagated →
            // record).
            let id = NodeId::from_name(&done.node);
            let decisions = tracker.notify_terminal(id, done.state);
            apply_decisions(
                pipeline,
                run_id,
                &decisions,
                &runners,
                &tasks,
                &tx,
                admission,
                writer,
                &mut terminal_states,
                &mut zombie_candidates,
                &mut pending,
                &mut in_flight,
            );
            // The finished attempt released its permit (dropped in its closure
            // before it reported done), so freed capacity may now admit a waiter.
            // Re-offer the pending queue oldest-first, admitting whatever now fits.
            drain_pending(
                pipeline,
                run_id,
                &runners,
                &tasks,
                &tx,
                admission,
                writer,
                &mut pending,
                &mut in_flight,
            );
        }

        // Natural run end: nothing pending, nothing in flight. Give any zombie
        // candidate (a blocking timeout whose leftover work has not confirmed
        // return — the M1 ledger that would confirm it is T31) at most the grace
        // period, then emit a zombie-at-exit event for each. This does not change
        // any node's terminal state (a timed-out node stays timed-out).
        if !zombie_candidates.is_empty() {
            tokio::time::sleep(grace).await;
            for node in &zombie_candidates {
                let _ = writer.zombie_at_exit(node);
            }
        }
    });

    // Shut the tasks runtime down **without joining** any abandoned-but-running
    // (zombie) blocking closure: a leftover thread counts as *decided*, not
    // in-flight, so it must not hold the run open. `Runtime::drop` would block
    // forever on an unkillable busy blocking thread; `shutdown_background` returns
    // immediately, leaving any zombie to be reaped by process exit (the driver
    // already emitted its `zombie-at-exit` event above). Every well-behaved
    // attempt has already reported terminal before this point.
    tasks.shutdown_background();

    let outcome = overall_outcome(&terminal_states);
    (outcome, terminal_states)
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
#[allow(clippy::too_many_arguments)]
fn offer_or_pend<S, C>(
    pipeline: &Pipeline,
    run_id: &str,
    name: &str,
    runners: &Arc<Mutex<BTreeMap<String, Box<dyn NodeRunner>>>>,
    tasks: &tokio::runtime::Runtime,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    admission: &AdmissionController,
    writer: &mut EventStreamWriter<S, C>,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) where
    S: EventSink,
    C: MonotonicClock,
{
    let cost = declared_cost(pipeline, name);
    match admission.try_admit(name, &cost) {
        Some(permit) => {
            admit(run_id, name, runners, tasks, tx, writer, permit);
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
            reject_over_demand(name, admission, &cost, tx);
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

/// Re-offer the pending waiters oldest-first after a release freed capacity (T31).
/// Walks `pending` front to back; each waiter that now fits its pools is admitted
/// and removed, and a waiter that still does not fit stays queued behind its place
/// — the oldest waiter is never bypassed by a younger one that would delay it.
#[allow(clippy::too_many_arguments)]
fn drain_pending<S, C>(
    pipeline: &Pipeline,
    run_id: &str,
    runners: &Arc<Mutex<BTreeMap<String, Box<dyn NodeRunner>>>>,
    tasks: &tokio::runtime::Runtime,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    admission: &AdmissionController,
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
        let cost = declared_cost(pipeline, &name);
        if let Some(permit) = admission.try_admit(&name, &cost) {
            pending.remove(index);
            admit(run_id, &name, runners, tasks, tx, writer, permit);
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

/// Admit `name`: emit its `node-ready` record and spawn its attempt onto the
/// `tasks` runtime, which reports the terminal state and buffered records back
/// over `tx` when it finishes.
///
/// `permit` is the C12 admission permit acquired for this attempt (T31). It is
/// **moved into the spawned closure** — the T0.3 ownership trick — so it is
/// dropped (and its cost released to every pool) exactly when the attempt returns,
/// *before* the loop is told the attempt is done. That is what keeps the permit
/// held for the whole attempt and released on its terminal outcome; a
/// blocking-timeout zombie that runs on past its mark keeps holding it until its
/// closure actually returns (this driver does not fabricate an early release).
fn admit<S, C>(
    run_id: &str,
    name: &str,
    runners: &Arc<Mutex<BTreeMap<String, Box<dyn NodeRunner>>>>,
    tasks: &tokio::runtime::Runtime,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    writer: &mut EventStreamWriter<S, C>,
    permit: Permit,
) where
    S: EventSink,
    C: MonotonicClock,
{
    let _ = writer.node_ready(name);
    // Node identity is name-derived (T0.7), so this is the same id assembly and the
    // tracker use — no pipeline lookup needed.
    let node_id = NodeId::from_name(name);

    let Some(mut runner) = runners
        .lock()
        .expect("runners mutex not poisoned")
        .remove(name)
    else {
        // A framework defect (no runner for an admitted node): decide it failed
        // rather than hang the run. Report it as a permanent failure terminal. The
        // permit drops here, releasing its cost (the attempt never ran).
        drop(permit);
        let _ = tx.send(AttemptDone {
            node: name.to_string(),
            state: TerminalState::Failed,
            events: Vec::new(),
        });
        return;
    };

    let run_id = run_id.to_string();
    let name_owned = name.to_string();
    let tx = tx.clone();
    tasks.spawn(async move {
        // A per-attempt buffering sink: the attempt emits into it off the
        // framework runtime; the loop drains it into the writer in order.
        let mut sink = BufferingSink::default();
        let ctx = RunContext::builder(CoreRunId::new(run_id), PipelineId::new("pipeline"), node_id)
            .build();
        let state = runner.run(&ctx, &mut sink).await;
        // Release the C12 permit at the attempt's terminal state (its working
        // memory + thread cost returns to the pools) BEFORE reporting done, so the
        // loop sees freed capacity when it re-offers the pending waiters. An
        // await-bound cancellation would drop the permit with the future instead;
        // a blocking-timeout zombie keeps it until its closure returns (T0.3 ADR).
        drop(permit);
        let _ = tx.send(AttemptDone {
            node: name_owned,
            state,
            events: sink.drain(),
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
    pipeline: &Pipeline,
    run_id: &str,
    decisions: &[Decision],
    runners: &Arc<Mutex<BTreeMap<String, Box<dyn NodeRunner>>>>,
    tasks: &tokio::runtime::Runtime,
    tx: &tokio::sync::mpsc::UnboundedSender<AttemptDone>,
    admission: &AdmissionController,
    writer: &mut EventStreamWriter<S, C>,
    terminal_states: &mut BTreeMap<String, TerminalState>,
    zombie_candidates: &mut Vec<String>,
    pending: &mut std::collections::VecDeque<String>,
    in_flight: &mut usize,
) where
    S: EventSink,
    C: MonotonicClock,
{
    for decision in decisions {
        match decision {
            Decision::Ready(id) => {
                if let Some(name) = node_name(pipeline, *id) {
                    offer_or_pend(
                        pipeline, run_id, &name, runners, tasks, tx, admission, writer, pending,
                        in_flight,
                    );
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
                        zombie_candidates.push(name);
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
