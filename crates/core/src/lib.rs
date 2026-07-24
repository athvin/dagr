//! `dagr-core` — the dagr execution core (the live-pipeline surface).
//!
//! This crate holds dagr's authoring surface and execution core: the task
//! abstraction, typed handles, dependency binding, flow assembly, and the
//! run-loop machinery — the code that *is* a running pipeline. It is the crate
//! whose dependency set is kept minimal and review-gated (arch.md "Stability"),
//! and it is the "live pipeline" surface that renderers must never reach into
//! (arch.md C24 · Renderers).
//!
//! # What lives here so far
//!
//! The **C1 task abstraction** (ticket T9): the atomic unit of work.
//!
//! - [`task::Task`] — the trait an author implements on a configuration-holding
//!   `struct`, declaring C1's four elements: the consumed input type, the
//!   produced output type, the execution class ([`task::ExecutionClass`],
//!   default await-bound), and the `&mut self` work over a [`task::RunContext`]
//!   reference.
//! - [`TaskError`] — the task-facing classified error, three-valued
//!   (retry-eligible / permanent / deliberate skip) per the T3 ADR.
//!
//! The **C2 typed handle** (ticket T10): a typed claim on a value that does not
//! exist yet.
//!
//! - [`handle::Handle<T>`] — the cheap, freely copyable handle a node
//!   registration returns; it carries the node's [identity](handle::NodeId) plus
//!   the value type the node will produce, and it is the *only* way to refer to
//!   another node's output. It has no public constructor and no lookup by
//!   name/index/string key — a handle is obtained solely by registering a node.
//! - [`handle::NodeId`] — the opaque, name-derived identity token a handle
//!   carries (identity comes from the registration name, never from order).
//!
//! The **C3 typed data-dependency binding** (ticket T11): how a downstream node
//! declares its data dependencies at registration.
//!
//! - [`binding::Deps`] — the sealed positional encoding that binds one or more
//!   already-registered [`Handle`]s to a task, exact value-type and arity
//!   matching enforced at compile time (single input is a bare `Handle<T>`,
//!   multi-input a tuple up to [`binding::MAX_INPUT_ARITY`]).
//! - [`binding::ReceiveMode`] / [`binding::DataEdge`] — the per-edge receive mode
//!   (owned / shared / clone-on-read) recorded verbatim and left un-adjudicated
//!   (mode conflicts are assembly's job, C7 / T14).
//! - [`binding::NodeBinding`] — the trigger-rule typestate that makes any rule
//!   other than `all-succeeded` inexpressible on a data-dependent node.
//!
//! The **C8 run context** (ticket T16): what every task invocation is told about
//! the run it is part of.
//!
//! - [`context::RunContext`] — the read-only, hand-constructable handle passed
//!   into [`task::Task::run`], carrying run/pipeline/node identity, the current
//!   attempt and the maximum, opaque parameters, an optional
//!   [`context::DataInterval`], an observe-only [`context::CancellationSignal`], a
//!   [`context::LogSpan`], and the [`context::ResourceRegistry`] (C9) /
//!   [`context::ScratchStore`] accessor seams (scratch substance is T53). Built
//!   by hand with [`context::RunContext::builder`] /
//!   [`context::RunContext::for_test`] — no runtime, store, registry, clock, or
//!   network — which feeds the single-task test kit (C28 / T60).
//!
//! The **C9 resource registry** (ticket T30): dependency injection for
//! long-lived external clients, built once in the developer's `main`.
//!
//! - [`context::ResourceRegistry`] / [`context::ResourceRegistryBuilder`] — the
//!   immutable, type-keyed store a task reads through
//!   [`context::RunContext::resources`]. Resources are retrieved by concrete type
//!   ([`ResourceRegistry::get`](context::ResourceRegistry::get), no string
//!   lookup); registering a second resource of the identical type fails
//!   construction as ambiguous ([`context::RegistryError`]); two same-typed
//!   resources are distinguished by newtype wrappers; stored resources are
//!   `Send + Sync + 'static` (a non-thread-safe client uses the documented
//!   owning-worker pattern). Any resource is replaceable by a fake in a test with
//!   no task change.
//! - [`context::Secret`] — the secret marker wrapper with **no** `Debug`/`Display`
//!   path, so the framework never renders secret material onto a
//!   framework-controlled output path (the guarantee boundary is C25; end-to-end
//!   redaction is T45, building on this wrapper and its sentinel hook).
//! - [`context::ResourceRequirements`] — the resource-requirement *declaration*
//!   plumbing a node records at registration, validated at bootstrap by
//!   [`ResourceRegistry::validate_requirements`](context::ResourceRegistry::validate_requirements)
//!   (a missing resource yields a [`context::BootstrapFailure`] naming the
//!   resource and its requiring nodes, distinct from an assembly failure, with
//!   zero attempts recorded) and surfaced for the graph artifact (C20) by
//!   [`context::surface_requirements`].
//! - [`context::CoveredNodeStates`] / [`context::TerminalState`] — the
//!   teardown-only view of covered nodes' terminal states (C17), from arch.md's
//!   normative taxonomy; the runtime-side population is C17 / T52.
//!
//! The **C7 flow builder and node identity** (ticket T13): accumulating node
//! registrations into an immutable pipeline.
//!
//! - [`flow::Flow`] — the builder that accepts node registrations (each carrying
//!   an explicit caller-supplied name), hands back the typed [`Handle`] from T10,
//!   and finalizes into an immutable [`flow::Pipeline`]. Node identity is derived
//!   **solely** from the registration name — never from order — so renaming a
//!   node changes its identity while reordering registrations changes nothing.
//! - [`flow::Pipeline`] — the immutable, read-only pipeline finalization yields;
//!   once produced, no registration or mutation is possible (a compile-time
//!   fact). Its node set is keyed by identity name, so lookup and content
//!   comparison are order-insensitive.
//! - [`flow::PipelineNode`] — a node's preserved record: its identity, its
//!   handle linkage, its recorded data edges and trigger rule, and the
//!   group-label slot (C6 / T51) carried alongside identity but **excluded** from
//!   it.
//!
//! The **C7 assembly validation and precomputation** (ticket T14): the total,
//! pure pass that turns the immutable [`Pipeline`] into a validated,
//! runtime-ready [`AssemblyArtifact`].
//!
//! - [`flow::Pipeline::assemble`] — reports **every** problem it finds (never
//!   just the first): duplicate node names (naming both declarations), an empty
//!   pipeline, invalid execution-class overrides, durable-without-contract nodes,
//!   ownership-mode conflicts, and nonzero teardown costs. It precomputes what
//!   the runtime consumes — per-node consumer counts, remaining-dependency
//!   counts, a topological execution order, and the fingerprint slot — and freezes
//!   them into the immutable artifact. Assembly is **pure** (no network,
//!   filesystem, clock, credentials, or parameter values) and performs **no**
//!   capacity/cost-fit check (that is bootstrap's, T0.5).
//! - [`assembly::NodePolicy`] — the full C5 node-policy value (T29): durability,
//!   retention, retries + backoff shape, per-attempt timeout, teardown, declared
//!   cost, and the constrained class override. Its resolved
//!   [`assembly::EffectivePolicy`] — the defaults-written-out view including the
//!   binding trigger rule and the group label — is what reaches the graph
//!   artifact (arch.md C5) and the two hashes (C21).
//! - [`assembly::DurableOutput`] — the durable-output contract marker (C27 /
//!   T0.8) whose presence assembly checks for a durable-marked node.
//!
//! The **C10 output slot** (ticket T17): where a node's produced value lives
//! between its production and its last consumption.
//!
//! - [`slot::Slot<T>`] — the typed, once-writable slot a node owns; empty until
//!   the node succeeds, refusing a second [`fill`](slot::Slot::fill). Consumers
//!   are wired at assembly time by minting a typed [`slot::SlotRef<T>`]
//!   ([`shared_ref`](slot::Slot::shared_ref) / [`owned_ref`](slot::Slot::owned_ref)
//!   / [`clone_on_read_ref`](slot::Slot::clone_on_read_ref)), so a read is a
//!   direct access with no lookup and no runtime type check (the single erasure
//!   boundary downcasts infallibly by construction — see the module rustdoc).
//! - [`slot::ConsumerLease<T>`] — the per-attempt lease that gates release on the
//!   **closure actually returning** (not the terminal decision), so an
//!   abandoned-but-running (zombie) consumer pins the value and its residency
//!   until it returns (T0.2 ADR §7).
//! - [`slot::ResidencyLedger`] — the single-count residency accounting hook the
//!   memory pool (C12) and run artifact (C23) consume, including peak measured
//!   residency.
//! - [`slot::RedemptionHandle<T>`] — the post-run redemption API: a `retained`
//!   node's value is exchanged for its handle once the run has ended; a released
//!   value is not redeemable ([`slot::RedeemError`]).
//!
//! The **C11 readiness tracker** (ticket T18): the pure decision engine that
//! decides what is eligible to run, and when.
//!
//! - [`readiness::ReadinessTracker`] — maintains a per-node remaining-dependency
//!   countdown seeded from T14's precomputed counts; a
//!   [`notify_terminal`](readiness::ReadinessTracker::notify_terminal) call
//!   decrements every dependent and, once a node's countdown reaches zero (every
//!   upstream terminal), evaluates its trigger rule — emitting the node as
//!   [`Ready`](readiness::Decision::Ready) when the rule fires, or assigning a
//!   [`PropagatedTerminal`](readiness::Decision::PropagatedTerminal) state without
//!   executing when it can never fire (which itself cascades). It surfaces the
//!   source frontier ([`initial_ready`](readiness::ReadinessTracker::initial_ready))
//!   and a [`pending_count`](readiness::ReadinessTracker::pending_count) for T24's
//!   "nothing pending" run-end signal. Pure: no spawning, scheduling, timing, or
//!   event writing.
//! - [`readiness::evaluate_rule`] — the pure rule-evaluation seam over all three
//!   T0.4 rules (`all-succeeded`, `all-terminal`, `any-failed`); M1 wires the
//!   `all-succeeded` path onto runtime nodes, leaving the other two reachable for
//!   T34.
//!
//! The **C14 single-attempt execution core** (ticket T20): the load-bearing
//! spine of the attempt runner — running *one* attempt of *one* node.
//!
//! - [`execution::run_attempt`] — the runtime-agnostic `async fn` that opens the
//!   attempt span (already on the [`RunContext`]), records
//!   the admission phase marker, dispatches the already-placed
//!   [`Task::run`](task::Task) work and awaits it, classifies the outcome
//!   into the normative taxonomy, fills the [`Slot`](slot::Slot<T>) (C10) on
//!   success only, and emits the ordered per-transition events plus exactly one
//!   attempt-outcome record. It adds no async-runtime dependency (the caller's
//!   runtime drives it; execution-class placement is C13 / T33).
//! - [`execution::AttemptOutcome`] — the classified single-attempt outcome
//!   (success / permanent failure / retry-eligible failure / deliberate skip /
//!   **timed-out**), a `#[non_exhaustive]` enum; T21 (031) added the `TimedOut`
//!   variant, and the panic (T23) variant is still reserved.
//! - [`execution::AttemptEventSink`] / [`execution::AttemptEvent`] — the
//!   abstract C19 event-emission port the runner writes through, so `dagr-core`
//!   emits events without depending on `dagr-artifact`'s writer (workspace ADR
//!   T1 / the C24 boundary). The run-loop driver (T24) adapts the concrete
//!   `EventStreamWriter` to this port. T21 added the `AttemptTimedOut` outcome
//!   record.
//!
//! The **C14 per-attempt timeout** (ticket T21): a runtime-agnostic per-attempt
//! timeout with per-class abandonment (arch.md C14; the T0.3 ADR, 009).
//!
//! - [`execution::run_attempt_with_timeout`] — the **await-bound** path: races
//!   the attempt future against a caller-provided deadline future (no tokio
//!   dependency added — the isolated framework timer drives the real one, C13);
//!   on timeout the future is dropped (true cancellation) and a permit-shaped
//!   guard moved into it releases immediately.
//! - [`execution::TimeoutDecision`] / [`execution::LateResultBarrier`] /
//!   [`execution::ZombieObserver`] — the **blocking / compute** path: mark
//!   `timed-out` immediately, hold the permit until the (unkillable) closure
//!   returns, defer the retry until then (C1 exclusivity), and bar any late
//!   slot fill or scratch write. The concrete admission ledger the permit guard
//!   stands in for is C12 / T31.
//!
//! The **C14 retry with jittered exponential backoff** (ticket T22): the bounded
//! retry loop that wraps the single-attempt core (arch.md C14; "Backoff is
//! exponential with jitter and a cap").
//!
//! - [`execution::run_with_retries`] — the retry loop: it drives the shared
//!   single-attempt core up to the configured maximum, emitting one
//!   attempt-outcome record **per attempt** and exactly one node-terminal record
//!   at loop end. Only retry-eligible outcomes (retry-eligible failure or
//!   timeout) consume the budget and trigger a backoff; permanent failure,
//!   deliberate skip, and success end the loop at once. Between attempts it
//!   records a distinct [`execution::AttemptEvent::BackoffStarted`] backoff phase
//!   (feeding C23 phase timings) and awaits a caller-provided timer future — it
//!   reads no system clock, so the sleeping is the driver's concern (T24 / T33).
//! - [`execution::RetryConfig`] / [`execution::Backoff`] — the retry knob the
//!   runner reads (max attempts + base/factor/cap) whose conservative default is
//!   **no retries** (a single attempt). Since **T29** retry/backoff configuration
//!   has one authoring home — the C5 [`assembly::NodePolicy`] — and `RetryConfig`
//!   is the derived runner input [`assembly::NodePolicy::retry_config`] produces.
//! - [`execution::Jitter`] / [`execution::SeededJitter`] / [`execution::NoJitter`]
//!   — the **injectable, deterministic** jitter source: a dependency-free seeded
//!   `splitmix64` PRNG for production (a distinct seed per node spreads a fan-out
//!   so simultaneous retries do not resynchronize) and a zero source for exact,
//!   assertable schedules in tests. The loop never reads a thread/global RNG.
//!
//! The **C14 panic containment** (ticket T23): a task that panics fails **only
//! its own node** rather than unwinding the run (arch.md C14 *Panics*; the T3
//! error-taxonomy ADR, 016).
//!
//! - [`execution::run_attempt_caught`] / [`execution::run_with_retries_caught`] —
//!   the panic-catching counterparts of [`execution::run_attempt`] /
//!   [`execution::run_with_retries`]: a panic unwinding out of the task future's
//!   poll is **caught** at the attempt boundary (a dependency-free,
//!   `unsafe`-free `catch_unwind` + `AssertUnwindSafe` adapter over the poll — no
//!   `futures` crate), converted to a **permanent** failure
//!   ([`execution::AttemptOutcome::Panicked`] → `failed`, never retry-eligible so
//!   it stops the retry loop), its message captured on the
//!   [`execution::AttemptEvent::AttemptPanicked`] outcome record, attributed to
//!   its node via task-local state, and the output slot left empty.
//! - [`execution::install_panic_hook`] — installs the framework's panic hook
//!   idempotently (safe under concurrent first-use) and **chains** to any
//!   pre-existing hook, including the test harness's, without replacing it.
//! - [`execution::check_panic_strategy`] / [`execution::detect_panic_strategy`] /
//!   [`execution::PanicStrategy`] / [`execution::BootstrapRefusal`] — the startup
//!   check that refuses to run under `panic = "abort"` (nothing to catch), with a
//!   refusal message naming the required profile setting (`panic = "unwind"`).
//!
//! Execution-class dispatch (T33) and the run-loop driver (T24) build on this
//! core rather than reshape it.
//!
//! The M1+ execution tickets land later; this crate grows one component at a
//! time.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod admission;
pub mod assembly;
pub mod binding;
pub mod context;
pub mod error;
pub mod execution;
pub mod flow;
pub mod handle;
pub mod limits;
pub mod metrics;
pub mod readiness;
pub mod scratch;
pub mod slot;
pub mod stable_name;
pub mod task;

pub use assembly::{
    AssemblyArtifact, AssemblyError, CostVector, DurableOutput, DurableWitness, EffectivePolicy,
    FingerprintSlot, NodePolicy, Problem, ProblemKind, Warning, FINGERPRINT_ALGORITHM_VERSION,
};
pub use binding::{
    BoundInput, CloneOnRead, DataEdge, Deps, EdgeKind, NodeBinding, OrderingEdge, OrderingHandle,
    ReceiveMode, RegisteredNode, Shared, TriggerRule, MAX_INPUT_ARITY,
};
pub use context::{
    surface_requirements, BootstrapFailure, BootstrapOutcome, CancellationOrigin,
    CancellationSignal, CancellationSource, CoveredNodeStates, DataInterval, LogSpan,
    MissingResourceError, PipelineId, RegistryError, ResourceRegistry, ResourceRegistryBuilder,
    ResourceRequirement, ResourceRequirements, RunContext, RunContextBuilder, RunId, ScratchError,
    ScratchStore, Secret, TerminalState,
};
pub use error::{TaskError, TaskErrorClass};
pub use execution::{
    check_panic_strategy, detect_panic_strategy, install_panic_hook, run_attempt,
    run_attempt_caught, run_attempt_with_timeout, run_with_retries, run_with_retries_caught,
    AttemptEvent, AttemptEventSink, AttemptOutcome, Backoff, BootstrapRefusal, Jitter,
    LateResultBarrier, NoJitter, PanicStrategy, RetryConfig, SeededJitter, TimeoutDecision,
    ZombieObserver,
};
pub use flow::{FailureMode, Flow, Pipeline, PipelineNode, StableTypeNames};
pub use handle::{Handle, NodeId};
pub use limits::{
    detect_capacities, CapacityBootstrapFailure, CapacityError, ContainerLimitProbe, PinnedPools,
    HEADROOM_DEFAULT,
};
pub use metrics::{
    AttemptMetrics, AttemptScope, AttributingAllocator, MetricError, MetricValue,
    MAX_ENCODED_BYTES, MAX_ENTRIES, METRIC_PEAK_MEMORY_BYTES, METRIC_TRUNCATED,
    METRIC_TRUNCATED_DROPPED_BYTES, METRIC_TRUNCATED_DROPPED_ENTRIES, PERMIT_PREFIX, PHASE_PREFIX,
    RESERVED_PREFIX, VALUE_ENCODED_BYTES,
};
pub use readiness::{evaluate_rule, Decision, ReadinessTracker, RuleOutcome};
pub use slot::{
    ConsumerLease, DeliveryMode, FillError, RedeemError, RedemptionHandle, ResidencyLedger, Slot,
    SlotRef,
};
pub use stable_name::{is_well_formed, StableInputNames, StableName, UNIT_STABLE_NAME};
pub use task::{ExecutionClass, Task};
