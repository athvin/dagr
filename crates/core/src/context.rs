//! The C8 run context — what every task invocation is told about the run it is
//! part of (arch.md `### C8 · Run context`).
//!
//! [`RunContext`] is a **read-only, hand-constructable handle** passed into every
//! [`Task::run`](crate::task::Task::run). It carries everything a task may know
//! about its run and **nothing it may change**: run / pipeline / node identity,
//! the current attempt number and the configured maximum, the run's parameters,
//! an optional [data interval](DataInterval), a [cancellation signal](CancellationSignal),
//! a [logging span](LogSpan), and accessors for the [resource registry](ResourceRegistry)
//! and the [durable scratch store](ScratchStore). A teardown node's context
//! additionally exposes the [terminal states](CoveredNodeStates) of the nodes it
//! covers (C17), so cleanup can no-op when setup never ran.
//!
//! # It is a capability surface, not an execution engine
//!
//! Every public method is a **read**. There is no API here to modify the graph,
//! reorder work, register or rescind a resource, or influence scheduling — and no
//! route back to the runtime or scheduler. The context holds **no mutable shared
//! state** the task can reach. This is C8's no-authority contract, and it is
//! load-bearing: dagr is not a scheduler and the graph's shape never changes at
//! runtime (arch.md "What this is not, permanently").
//!
//! # The data interval is caller-supplied and tool-opaque
//!
//! The [data interval](DataInterval) is a **caller-supplied, tool-opaque pair of
//! values recorded verbatim**. The tool **never** computes an interval, **never**
//! advances one, and **never** persists one between runs — a backfill is the
//! *caller* looping over invocations with different intervals. **This is the
//! boundary with "backfill orchestrator,"** stated here so nobody rediscovers it
//! in a design meeting: no framework code path in this module (or any other)
//! parses, orders, validates, or normalizes an interval's contents.
//!
//! # Hand-construction for tests
//!
//! A `RunContext` can be built by hand in a plain unit test — **no runtime, no
//! store, no registry, no clock, no network** — via [`RunContext::builder`] (full
//! control of every field) or [`RunContext::for_test`] (a fully-populated
//! zero-argument default). This is the C8 acceptance criterion that feeds the
//! single-task test kit (C28 / T60): a single task can be exercised in isolation
//! with a context constructed entirely in-process.
//!
//! # Seams landing with later tickets
//!
//! Two accessors are **additive seams** whose *substance* arrives with later
//! tickets, marked inline:
//!
//! - [`RunContext::resources`] — the [`ResourceRegistry`] (C9). Landed here as a
//!   stable, honestly-empty seam; type-keyed retrieval, newtype disambiguation,
//!   secret wrapping, and bootstrap validation are **T30**'s.
//! - [`RunContext::scratch`] — the [`ScratchStore`] (C18). Landed here as a stable
//!   seam that is **honestly unimplemented** (reads and writes report
//!   not-yet-available rather than pretending to persist); key-value persistence,
//!   run/node namespacing, and resume copy-forward are **T53**'s.
//!
//! The [`CoveredNodeStates`] shape is defined here; the **runtime-side population**
//! of covered states (teardown ordering, the fresh uncancelled signal, the
//! teardown deadline) is finished under **C17 / T52**.
//!
//! The [`ResourceRequirements`] declaration plumbing is also landed here: a node
//! records the resource types it requires at registration in a form bootstrap
//! (T30) can validate against a registry and a graph artifact (C20) can later
//! render.

use std::any::{type_name, Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::handle::NodeId;

/// A run's identity (arch.md `### C8`; C19 mints a UUIDv7 at bootstrap,
/// operator-overridable). A dagr-owned, opaque newtype so task authors program
/// against a dagr type; the framework does not interpret its contents here.
///
/// Hand-constructable in tests via [`RunId::new`]; the runtime mints the real
/// value at bootstrap (T-later), which is **not** this ticket's concern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(String);

impl RunId {
    /// Wrap an already-minted run identity verbatim. dagr owns the type; the
    /// content is opaque to the framework.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identity as a string slice, exactly as supplied.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A pipeline's identity (arch.md `### C8`). A dagr-owned, opaque newtype;
/// hand-constructable in tests via [`PipelineId::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineId(String);

impl PipelineId {
    /// Wrap a pipeline identity verbatim. dagr owns the type; the content is
    /// opaque to the framework.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identity as a string slice, exactly as supplied.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A **caller-supplied, tool-opaque** pair of values recorded verbatim
/// (arch.md `### C8`, "The data interval").
///
/// The framework **never** parses, orders, validates, normalizes, computes,
/// advances, or persists an interval — a backfill is the *caller* looping over
/// invocations with different intervals. The two endpoints are opaque strings
/// whose meaning is entirely the caller's; naming them `start` and `end` is a
/// convenience for the caller, **not** a claim that the framework treats one as
/// earlier than the other. **This is the boundary with "backfill orchestrator."**
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataInterval {
    start: String,
    end: String,
}

impl DataInterval {
    /// Record an opaque interval verbatim. The endpoints are stored **exactly**
    /// as supplied — reversed order, identical endpoints, empty content, and
    /// bytes nonsensical as a timestamp are all recorded unchanged, because no
    /// framework code path interprets them.
    #[must_use]
    pub fn new(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// The first opaque endpoint, exactly as supplied. "Start" is the caller's
    /// label; the framework attaches no ordering meaning to it.
    #[must_use]
    pub fn start(&self) -> &str {
        &self.start
    }

    /// The second opaque endpoint, exactly as supplied. "End" is the caller's
    /// label; the framework attaches no ordering meaning to it.
    #[must_use]
    pub fn end(&self) -> &str {
        &self.end
    }
}

/// The **read-only** cancellation signal a task observes (arch.md `### C8`,
/// `### C16`).
///
/// This is the **task-facing** half: it offers **only** observation
/// ([`is_cancelled`](Self::is_cancelled)) — there is deliberately **no** method to
/// cancel the run from here, consistent with C8's "no route back to the
/// scheduler." The run-scoped token and its per-attempt children, future-drop
/// cancellation of await-bound work, and cooperative-only marking of
/// blocking/compute work are wired by the runner (C14 / C16, T20 / T21 / T35) via
/// a [`CancellationSource`]; per the T2 async-runtime ADR the eventual backing is
/// `tokio_util::sync::CancellationToken`, but the type task authors see is this
/// dagr-owned wrapper, never a bare tokio type.
///
/// # T20/T35 seam
///
/// The internal representation here is a simple shared flag, enough to satisfy
/// C8's hand-constructability and observation contract with **no runtime**. When
/// the runner lands (T20/T35) the backing becomes the real cancellation token;
/// this task-facing surface — observe-only, no lever — does not change.
#[derive(Debug, Clone)]
pub struct CancellationSignal {
    flag: Arc<AtomicBool>,
}

impl CancellationSignal {
    /// Whether cancellation has been signalled. This is the **only** thing a task
    /// may do with the signal: observe it and return promptly (recorded
    /// `cancelled`) or not (recorded `abandoned` — C16). There is no lever to
    /// cancel the run from the task side.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}

/// The **runtime/test-side** handle that can *raise* a [`CancellationSignal`].
///
/// This is held by the runner (or a test), **never** handed to a task: the split
/// between this source and the observe-only [`CancellationSignal`] is exactly
/// what makes the task-facing side a read channel and not a lever. A test flips
/// cancellation with [`cancel`](Self::cancel) to exercise a task's observation of
/// it; the runner does the same on the cancellation path (C16).
#[derive(Debug, Clone, Default)]
pub struct CancellationSource {
    flag: Arc<AtomicBool>,
}

impl CancellationSource {
    /// A fresh, uncancelled source.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The observe-only [`CancellationSignal`] this source drives — the one a
    /// [`RunContext`] carries. Any number of signals may share one source; they
    /// all observe the same flip.
    #[must_use]
    pub fn signal(&self) -> CancellationSignal {
        CancellationSignal {
            flag: Arc::clone(&self.flag),
        }
    }

    /// Raise cancellation. Every [`CancellationSignal`] derived from this source
    /// now observes [`is_cancelled`](CancellationSignal::is_cancelled) as `true`.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }
}

/// The **dagr-owned** logging span a task's attempt runs inside (arch.md
/// `### C8`, `### C25`).
///
/// Every attempt runs beneath a span carrying run / node / attempt identity, so
/// every line emitted under it is attributable without timestamp correlation
/// (C25). This is a dagr-owned handle (per the T2 ADR: context-exposed types are
/// dagr-owned wherever practical), carrying the identity the span is keyed on.
///
/// # C25 seam
///
/// The **subscriber integration** — structured-vs-human output, third-party line
/// capture, secret scrubbing on framework paths — is C25's, not this ticket's.
/// This type fixes only the span's *identity payload* and its placement on the
/// context; the tracing wiring lands with logging integration.
#[derive(Debug, Clone)]
pub struct LogSpan {
    run: RunId,
    node: NodeId,
    attempt: u32,
}

impl LogSpan {
    /// The run this span is attributed to.
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run
    }

    /// The node this span is attributed to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node
    }

    /// The attempt this span is attributed to.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// The resource-registry accessor **seam** (arch.md `### C9`; concrete registry
/// is **T30**).
///
/// # Honestly-empty seam — T30 lands the substance
///
/// C8 fixes that a task reaches the registry *through the context*; C9 / **T30**
/// builds the registry itself (type-keyed retrieval, newtype disambiguation,
/// ambiguity failure, secret wrapping, bootstrap validation against declared
/// [`ResourceRequirements`]). Landed here as a **stable, honestly-empty** handle:
/// [`get`](Self::get) returns [`None`] for every type rather than fabricating a
/// silently-wrong resource. When T30 lands it fills this in with the real
/// immutable, shared-for-the-run registry; the accessor's signature does not
/// change.
#[derive(Debug, Clone, Default)]
pub struct ResourceRegistry {
    // T30 (C9): the immutable type-keyed store of long-lived clients lands here.
    // Intentionally empty at this milestone — the seam is honest, not silently
    // wrong.
    _seam: (),
}

impl ResourceRegistry {
    /// Retrieve a resource by type. **T30 seam:** always [`None`] here, because
    /// the concrete registry (C9) is not landed — never a silently-wrong
    /// resource. T30 replaces this with real type-keyed retrieval and updates the
    /// covering test to assert it.
    #[must_use]
    #[allow(
        clippy::unused_self,
        reason = "stable T30 seam: the real registry (C9) reads `self`; here it is honestly empty"
    )]
    pub fn get<R: Any>(&self) -> Option<&R> {
        // T30 (C9): type-keyed retrieval against the real registry lands here.
        None
    }

    /// Whether the registry holds no resources. **T30 seam:** always `true` here.
    #[must_use]
    #[allow(
        clippy::unused_self,
        reason = "stable T30 seam: the real registry (C9) reads `self`; here it is honestly empty"
    )]
    pub fn is_empty(&self) -> bool {
        true
    }
}

/// The error a [`ScratchStore`] operation reports (arch.md `### C18`; concrete
/// store is **T53**).
///
/// Until C18 lands, the only variant is [`NotYetAvailable`](ScratchError::NotYetAvailable):
/// the seam is **honest**, surfacing a not-yet-available result rather than
/// pretending a read succeeded or a write persisted. T53 grows this into the real
/// error surface (I/O failure classified retry-eligible — C18).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScratchError {
    /// The durable scratch store (C18) is not landed yet — its substance arrives
    /// with **T53**. The seam does not pretend to persist.
    NotYetAvailable,
}

impl std::fmt::Display for ScratchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotYetAvailable => {
                write!(f, "durable scratch store is not yet available (lands with T53 / C18)")
            }
        }
    }
}

impl std::error::Error for ScratchError {}

/// The durable-scratch accessor **seam** (arch.md `### C18`; concrete store is
/// **T53**).
///
/// # Honestly-unimplemented seam — T53 lands the substance
///
/// C8 fixes that a task reaches its per-node scratch *through the context*; C18 /
/// **T53** builds the store (opaque-byte key-value, run/node namespacing,
/// read-after-write across attempts, resume copy-forward, success-time cleanup).
/// Landed here as a **stable seam that is honestly unimplemented**:
/// [`get`](Self::get) and [`put`](Self::put) return
/// [`ScratchError::NotYetAvailable`] rather than pretending to persist. When T53
/// lands, its covering test asserts read-after-write across attempts; the
/// accessor signatures do not change.
#[derive(Debug, Clone, Default)]
pub struct ScratchStore {
    // T53 (C18): the local run-store-backed key-value scratch lands here.
    _seam: (),
}

impl ScratchStore {
    /// Read a scratch value by opaque key. **T53 seam:** always
    /// [`Err`]`(`[`ScratchError::NotYetAvailable`]`)` — the store does not pretend
    /// to hold a value. T53 replaces this with the real read-after-write-across-
    /// attempts behaviour.
    ///
    /// # Errors
    ///
    /// Always [`ScratchError::NotYetAvailable`] until T53 (C18) lands.
    #[allow(
        clippy::unused_self,
        reason = "stable T53 seam: the real store (C18) reads `self`; here it is honestly unimplemented"
    )]
    pub fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, ScratchError> {
        // T53 (C18): namespaced key-value read against the run store lands here.
        Err(ScratchError::NotYetAvailable)
    }

    /// Write a scratch value under an opaque key. **T53 seam:** always
    /// [`Err`]`(`[`ScratchError::NotYetAvailable`]`)` — the store does not pretend
    /// to persist. T53 replaces this with the real write, readable on the next
    /// attempt.
    ///
    /// # Errors
    ///
    /// Always [`ScratchError::NotYetAvailable`] until T53 (C18) lands.
    #[allow(
        clippy::unused_self,
        reason = "stable T53 seam: the real store (C18) reads `self`; here it is honestly unimplemented"
    )]
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), ScratchError> {
        // T53 (C18): namespaced key-value write to the run store lands here.
        Err(ScratchError::NotYetAvailable)
    }
}

/// A node's **terminal state**, from arch.md's normative taxonomy (Vocabulary —
/// "Terminal states"). Every node ends a run in exactly one of these.
///
/// This ticket needs the taxonomy for the [teardown extension](CoveredNodeStates):
/// a teardown node reads the terminal states of the nodes it covers so cleanup
/// can no-op when setup never ran (C17). The names are the exact canonical ones;
/// the readiness tracker, failure policy, and run artifact (C11 / C15 / C22) that
/// *assign* these states are later tickets — this enum only carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TerminalState {
    /// The task returned a value; the slot was filled. *(success-like)*
    Succeeded,
    /// Permanent failure, retries exhausted, or a caught panic. *(failure-like)*
    Failed,
    /// The final attempt exceeded its per-attempt timeout. *(failure-like)*
    TimedOut,
    /// The task itself returned a deliberate skip (an *originated* skip).
    /// *(skip-like)*
    Skipped,
    /// Never ran because an upstream skip propagated to it. *(skip-like)*
    UpstreamSkipped,
    /// Never ran because its trigger rule can no longer be satisfied due to an
    /// upstream failure. *(failure-like)*
    UpstreamFailed,
    /// Observed the cancellation signal and returned promptly, or was never
    /// admitted after cancellation began. *(stop-like)*
    Cancelled,
    /// Was asked to cancel and never returned within the grace period; its thread
    /// was left behind. *(failure-like)*
    Abandoned,
    /// Not executed in this run; resume (C27) carried its prior success forward.
    /// *(success-like)*
    SatisfiedFromPrior,
}

/// The **teardown-only** view of covered nodes' terminal states (arch.md
/// `### C8`, `### C17`).
///
/// A teardown node's context additionally exposes the terminal states of the
/// nodes it covers, so cleanup can **no-op when setup never ran**. This type
/// defines the *shape* of that extension and is hand-constructable for tests;
/// the **runtime-side population** of covered states — teardown ordering, the
/// fresh uncancelled signal, the teardown deadline — is completed under **C17 /
/// T52**. A **non-teardown** context carries no [`CoveredNodeStates`] at all
/// ([`RunContext::covered_terminal_states`] returns [`None`]), which is how the
/// absence of a covered set is represented.
#[derive(Debug, Clone, Default)]
pub struct CoveredNodeStates {
    // Keyed by NodeId (Eq + Hash, not Ord — a HashMap, not a BTreeMap): this is a
    // keyed lookup a teardown does ("what state is the node I cover in?"), not a
    // rendered, order-sensitive collection.
    states: HashMap<NodeId, TerminalState>,
}

impl CoveredNodeStates {
    /// An empty covered-states set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a covered node's terminal state (builder-style). Used by C17 / T52
    /// to populate the set from the runtime, and by tests to hand-construct one.
    #[must_use]
    pub fn with(mut self, node: NodeId, state: TerminalState) -> Self {
        self.states.insert(node, state);
        self
    }

    /// The terminal state of a covered node, or [`None`] if this teardown does
    /// not cover that node (so cleanup can no-op — e.g. setup never ran).
    #[must_use]
    pub fn get(&self, node: NodeId) -> Option<TerminalState> {
        self.states.get(&node).copied()
    }

    /// The number of covered nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Whether no nodes are covered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
}

/// One declared resource requirement: a node's dependency on a resource *type*
/// (arch.md `### C9`). Carries the type's identity for validation and its
/// author-declared type name for rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceRequirement {
    type_id: TypeId,
    type_name: &'static str,
}

impl ResourceRequirement {
    /// The requirement for resource type `R`.
    #[must_use]
    pub fn of<R: Any>() -> Self {
        Self {
            type_id: TypeId::of::<R>(),
            type_name: type_name::<R>(),
        }
    }

    /// The required type's identity — what bootstrap (T30) keys registry
    /// validation on.
    #[must_use]
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// The required type's name, for rendering into the graph artifact (C20 /
    /// T30). Informational only — identity is [`type_id`](Self::type_id).
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }
}

/// The **resource-requirement declaration plumbing** (arch.md `### C9`): the set
/// of resource types a node declares it requires at registration.
///
/// This is the mechanism a node uses to record its required resource types so
/// **bootstrap (T30)** can validate the registry against the declared
/// requirements — a missing resource is a startup failure, never a mid-run
/// surprise — and so those declarations can later surface in the **graph artifact
/// (C20)**. This ticket lands only the *declaration* and its queryable form; the
/// registry itself, and the bootstrap validation against it, are **T30**.
///
/// A node declaring nothing reports an [empty](Self::is_empty) requirement set;
/// declarations are additive and do not affect a context's other fields.
#[derive(Debug, Clone, Default)]
pub struct ResourceRequirements {
    // Keyed by TypeId so declaring the same type twice is idempotent (a node
    // requiring a type "twice" requires it once). Ordered for stable rendering.
    required: BTreeMap<TypeId, ResourceRequirement>,
}

impl ResourceRequirements {
    /// An empty requirement set — the default for a node that declares nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare that the node requires resource type `R` (builder-style).
    /// Idempotent: declaring the same type twice records it once.
    #[must_use]
    pub fn require<R: Any>(mut self) -> Self {
        let req = ResourceRequirement::of::<R>();
        self.required.insert(req.type_id(), req);
        self
    }

    /// Whether the node declares it requires resource type `R`.
    #[must_use]
    pub fn requires<R: Any>(&self) -> bool {
        self.required.contains_key(&TypeId::of::<R>())
    }

    /// The number of distinct declared requirements.
    #[must_use]
    pub fn len(&self) -> usize {
        self.required.len()
    }

    /// Whether the node declares no requirements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.required.is_empty()
    }

    /// The declared requirements, in a stable order — the form bootstrap (T30)
    /// validates and a graph artifact (C20) renders.
    pub fn iter(&self) -> impl Iterator<Item = &ResourceRequirement> {
        self.required.values()
    }
}

/// The **read-only** handle every task invocation is told about the run it is
/// part of (arch.md `### C8 · Run context`).
///
/// See the [module docs](self) for the full contract: it carries run / pipeline /
/// node identity, the current attempt and the maximum, the run's parameters, an
/// optional [data interval](DataInterval), a [cancellation signal](CancellationSignal),
/// a [logging span](LogSpan), and the [registry](ResourceRegistry) /
/// [scratch](ScratchStore) accessors — and it exposes **only reads**, with no
/// route back to the scheduler. Build one by hand with [`RunContext::builder`] or
/// [`RunContext::for_test`].
#[derive(Debug, Clone)]
pub struct RunContext {
    run: RunId,
    pipeline: PipelineId,
    node: NodeId,
    attempt: u32,
    max_attempts: u32,
    parameters: Option<Arc<dyn Any + Send + Sync>>,
    data_interval: Option<DataInterval>,
    cancellation: CancellationSignal,
    span: LogSpan,
    resources: ResourceRegistry,
    scratch: ScratchStore,
    covered_terminal_states: Option<CoveredNodeStates>,
}

impl RunContext {
    /// Begin hand-constructing a context with the required identity fields. The
    /// remaining fields take sensible, spec-consistent defaults (attempt 1, max 1,
    /// no parameters, no data interval, a fresh uncancelled signal, empty seams,
    /// non-teardown) until set on the returned [`RunContextBuilder`].
    ///
    /// This is the C8 hand-construction path — **no runtime, no store, no
    /// registry, no clock, no network** — that feeds the single-task test kit
    /// (C28 / T60). The runtime constructs and threads the *real* context (T20 /
    /// C14); that is out of scope here.
    #[must_use]
    pub fn builder(run: RunId, pipeline: PipelineId, node: NodeId) -> RunContextBuilder {
        RunContextBuilder::new(run, pipeline, node)
    }

    /// A fully-populated context for exercising a single task in isolation, with
    /// **no arguments** and **no runtime running** (arch.md C8 / C28). Every field
    /// is present: recognizable placeholder identities, attempt 1 of 1, no
    /// parameters, no data interval, a fresh uncancelled signal, the honest
    /// registry/scratch seams, and no covered-states set (non-teardown).
    ///
    /// This is the seam T9's task tests already call and T60 builds on. For a
    /// context with specific field values, use [`RunContext::builder`].
    #[must_use]
    pub fn for_test() -> Self {
        Self::builder(
            RunId::new("test-run"),
            PipelineId::new("test-pipeline"),
            NodeId::from_name("test-node"),
        )
        .build()
    }

    /// The run's identity.
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run
    }

    /// The pipeline's identity.
    #[must_use]
    pub fn pipeline_id(&self) -> &PipelineId {
        &self.pipeline
    }

    /// This node's identity.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node
    }

    /// The current attempt number — carries the retry count in a form logs and
    /// artifacts consume (arch.md C8; it increments across retries, driven by the
    /// runner, C14 / T22). It is **not** fixed or defaulted-away: every
    /// invocation, including the first attempt of the first node, carries it.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The configured maximum number of attempts for this node.
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// The run's parameters, downcast to the caller's parameter type `P`, or
    /// [`None`] if no parameters were supplied or the requested type does not
    /// match what was supplied.
    ///
    /// Parameters are carried **opaquely**: the framework does not interpret them
    /// (they are parsed at bootstrap, after the pure assembly phase — C7 / C26)
    /// and this accessor only hands the task back the value it was given, by type.
    #[must_use]
    pub fn parameters<P: Any>(&self) -> Option<&P> {
        self.parameters.as_ref().and_then(|p| p.downcast_ref::<P>())
    }

    /// The run's optional [data interval](DataInterval), or [`None`] when none was
    /// supplied. **Caller-supplied and tool-opaque** — returned exactly as
    /// supplied; no framework code path interprets its contents (arch.md C8).
    #[must_use]
    pub fn data_interval(&self) -> Option<&DataInterval> {
        self.data_interval.as_ref()
    }

    /// The **observe-only** [cancellation signal](CancellationSignal). A task may
    /// observe it and return promptly; there is no lever here to cancel the run
    /// (arch.md C8: no route back to the scheduler).
    #[must_use]
    pub fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// The [logging span](LogSpan) this attempt runs inside (arch.md C8 / C25).
    #[must_use]
    pub fn span(&self) -> &LogSpan {
        &self.span
    }

    /// The [resource-registry accessor](ResourceRegistry) — a **stable seam**;
    /// the concrete registry (C9) lands with **T30** (see [`ResourceRegistry`]).
    #[must_use]
    pub fn resources(&self) -> &ResourceRegistry {
        &self.resources
    }

    /// The [durable-scratch accessor](ScratchStore) — a **stable seam**; the
    /// concrete store (C18) lands with **T53** (see [`ScratchStore`]).
    #[must_use]
    pub fn scratch(&self) -> &ScratchStore {
        &self.scratch
    }

    /// The terminal states of the nodes a **teardown** node covers, or [`None`]
    /// for a non-teardown context (arch.md C8 / C17). A teardown reads these so
    /// cleanup can no-op when setup never ran; the runtime-side population is
    /// finished under **C17 / T52** (see [`CoveredNodeStates`]).
    #[must_use]
    pub fn covered_terminal_states(&self) -> Option<&CoveredNodeStates> {
        self.covered_terminal_states.as_ref()
    }
}

/// The hand-construction builder for a [`RunContext`] (arch.md C8 / C28).
///
/// Obtained from [`RunContext::builder`]. Fields not set take sensible,
/// spec-consistent defaults; [`build`](Self::build) yields the immutable context.
/// This is the **no-runtime** path — nothing here touches the filesystem, the
/// clock, the network, or a registry — that a plain unit test and the single-task
/// test kit (T60) use to exercise a task in isolation.
#[derive(Debug, Clone)]
pub struct RunContextBuilder {
    run: RunId,
    pipeline: PipelineId,
    node: NodeId,
    attempt: u32,
    max_attempts: u32,
    parameters: Option<Arc<dyn Any + Send + Sync>>,
    data_interval: Option<DataInterval>,
    cancellation: Option<CancellationSignal>,
    resources: ResourceRegistry,
    scratch: ScratchStore,
    covered_terminal_states: Option<CoveredNodeStates>,
}

impl RunContextBuilder {
    fn new(run: RunId, pipeline: PipelineId, node: NodeId) -> Self {
        Self {
            run,
            pipeline,
            node,
            attempt: 1,
            max_attempts: 1,
            parameters: None,
            data_interval: None,
            cancellation: None,
            resources: ResourceRegistry::default(),
            scratch: ScratchStore::default(),
            covered_terminal_states: None,
        }
    }

    /// Set the current attempt number (default 1).
    #[must_use]
    pub fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    /// Set the configured maximum number of attempts (default 1).
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Supply the run's parameters, carried opaquely and read back by type via
    /// [`RunContext::parameters`]. The value must be `Send + Sync + 'static` so
    /// the context can be shared with the worker driving the attempt.
    #[must_use]
    pub fn parameters(mut self, parameters: Arc<dyn Any + Send + Sync>) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Supply the run's opaque [data interval](DataInterval). Omit it for a run
    /// with no interval (the default), which [`RunContext::data_interval`] reports
    /// as [`None`].
    #[must_use]
    pub fn data_interval(mut self, interval: DataInterval) -> Self {
        self.data_interval = Some(interval);
        self
    }

    /// Supply the [cancellation signal](CancellationSignal) a task observes,
    /// obtained from a [`CancellationSource`] the caller (runtime or test) holds.
    /// Omit it for a fresh, never-cancelled signal (the default).
    #[must_use]
    pub fn cancellation(mut self, signal: CancellationSignal) -> Self {
        self.cancellation = Some(signal);
        self
    }

    /// Mark this as a **teardown** context by supplying the terminal states of the
    /// nodes it covers (arch.md C17). Omit it for a non-teardown context, which
    /// [`RunContext::covered_terminal_states`] reports as [`None`].
    #[must_use]
    pub fn covered_terminal_states(mut self, covered: CoveredNodeStates) -> Self {
        self.covered_terminal_states = Some(covered);
        self
    }

    /// Build the immutable [`RunContext`]. Every field is populated: the required
    /// identities, the attempt/max, and — for any field not explicitly set — its
    /// spec-consistent default (no parameters, no data interval, a fresh
    /// uncancelled signal, honest empty seams, non-teardown). The span is derived
    /// from the run/node/attempt identity.
    #[must_use]
    pub fn build(self) -> RunContext {
        let cancellation = self
            .cancellation
            .unwrap_or_else(|| CancellationSource::new().signal());
        let span = LogSpan {
            run: self.run.clone(),
            node: self.node,
            attempt: self.attempt,
        };
        RunContext {
            run: self.run,
            pipeline: self.pipeline,
            node: self.node,
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            parameters: self.parameters,
            data_interval: self.data_interval,
            cancellation,
            span,
            resources: self.resources,
            scratch: self.scratch,
            covered_terminal_states: self.covered_terminal_states,
        }
    }
}
