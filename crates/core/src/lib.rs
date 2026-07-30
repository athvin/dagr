//! `dagr-core` — the dagr execution core (the live-pipeline surface).
//!
//! This crate holds dagr's authoring surface and execution core: the task
//! abstraction, typed handles, dependency binding, flow assembly, and the
//! run-loop machinery — the code that *is* a running pipeline. It is the crate
//! whose dependency set is kept minimal and review-gated, and it is the "live
//! pipeline" surface that renderers must never reach into.
//!
//! # What lives here so far
//!
//! The **task abstraction**: the atomic unit of work.
//!
//! - [`task::Task`] — the trait an author implements on a configuration-holding
//!   `struct`, declaring its four elements: the consumed input type, the
//!   produced output type, the execution class ([`task::ExecutionClass`],
//!   default await-bound), and the `&mut self` work over a [`task::RunContext`]
//!   reference.
//! - [`TaskError`] — the task-facing classified error, three-valued
//!   (retry-eligible / permanent / deliberate skip).
//!
//! The **typed handle**: a typed claim on a value that does not
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
//! The **typed data-dependency binding**: how a downstream node
//! declares its data dependencies at registration.
//!
//! - [`binding::Deps`] — the sealed positional encoding that binds one or more
//!   already-registered [`Handle`]s to a task, exact value-type and arity
//!   matching enforced at compile time (single input is a bare `Handle<T>`,
//!   multi-input a tuple up to [`binding::MAX_INPUT_ARITY`]).
//! - [`binding::ReceiveMode`] / [`binding::DataEdge`] — the per-edge receive mode
//!   (owned / shared / clone-on-read) recorded verbatim and left un-adjudicated
//!   (mode conflicts are assembly's job).
//! - [`binding::NodeBinding`] — the trigger-rule typestate that makes any rule
//!   other than `all-succeeded` inexpressible on a data-dependent node.
//!
//! The **run context**: what every task invocation is told about
//! the run it is part of.
//!
//! - [`context::RunContext`] — the read-only, hand-constructable handle passed
//!   into [`task::Task::run`], carrying run/pipeline/node identity, the current
//!   attempt and the maximum, opaque parameters, an optional
//!   [`context::DataInterval`], an observe-only [`context::CancellationSignal`], a
//!   [`context::LogSpan`], and the [`context::ResourceRegistry`] /
//!   [`context::ScratchStore`] accessor seams. Built
//!   by hand with [`context::RunContext::builder`] /
//!   [`context::RunContext::for_test`] — no runtime, store, registry, clock, or
//!   network — which feeds the single-task test kit.
//!
//! The **resource registry**: dependency injection for
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
//!   framework-controlled output path (this wrapper and its sentinel hook are the
//!   foundation the end-to-end redaction path builds on).
//! - [`context::ResourceRequirements`] — the resource-requirement *declaration*
//!   plumbing a node records at registration, validated at bootstrap by
//!   [`ResourceRegistry::validate_requirements`](context::ResourceRegistry::validate_requirements)
//!   (a missing resource yields a [`context::BootstrapFailure`] naming the
//!   resource and its requiring nodes, distinct from an assembly failure, with
//!   zero attempts recorded) and surfaced for the graph artifact by
//!   [`context::surface_requirements`].
//! - [`context::CoveredNodeStates`] / [`context::TerminalState`] — the
//!   teardown-only view of covered nodes' terminal states, from the framework's
//!   normative terminal-state taxonomy.
//!
//! The **flow builder and node identity**: accumulating node
//! registrations into an immutable pipeline.
//!
//! - [`flow::Flow`] — the builder that accepts node registrations (each carrying
//!   an explicit caller-supplied name), hands back the typed [`Handle`],
//!   and finalizes into an immutable [`flow::Pipeline`]. Node identity is derived
//!   **solely** from the registration name — never from order — so renaming a
//!   node changes its identity while reordering registrations changes nothing.
//! - [`flow::Pipeline`] — the immutable, read-only pipeline finalization yields;
//!   once produced, no registration or mutation is possible (a compile-time
//!   fact). Its node set is keyed by identity name, so lookup and content
//!   comparison are order-insensitive.
//! - [`flow::PipelineNode`] — a node's preserved record: its identity, its
//!   handle linkage, its recorded data edges and trigger rule, and the
//!   group-label slot carried alongside identity but **excluded** from
//!   it.
//!
//! The **assembly validation and precomputation**: the total,
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
//!   capacity/cost-fit check (that is bootstrap's).
//! - [`assembly::NodePolicy`] — the full node-policy value: durability,
//!   retention, retries + backoff shape, per-attempt timeout, teardown, declared
//!   cost, and the constrained class override. Its resolved
//!   [`assembly::EffectivePolicy`] — the defaults-written-out view including the
//!   binding trigger rule and the group label — is what reaches the graph
//!   artifact and the two hashes.
//! - [`assembly::DurableOutput`] — the durable-output contract marker whose
//!   presence assembly checks for a durable-marked node.
//!
//! The **output slot**: where a node's produced value lives
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
//!   until it returns.
//! - [`slot::ResidencyLedger`] — the single-count residency accounting hook the
//!   memory pool and run artifact consume, including peak measured
//!   residency.
//! - [`slot::RedemptionHandle<T>`] — the post-run redemption API: a `retained`
//!   node's value is exchanged for its handle once the run has ended; a released
//!   value is not redeemable ([`slot::RedeemError`]).
//!
//! The **readiness tracker**: the pure decision engine that
//! decides what is eligible to run, and when.
//!
//! - [`readiness::ReadinessTracker`] — maintains a per-node remaining-dependency
//!   countdown seeded from assembly's precomputed counts; a
//!   [`notify_terminal`](readiness::ReadinessTracker::notify_terminal) call
//!   decrements every dependent and, once a node's countdown reaches zero (every
//!   upstream terminal), evaluates its trigger rule — emitting the node as
//!   [`Ready`](readiness::Decision::Ready) when the rule fires, or assigning a
//!   [`PropagatedTerminal`](readiness::Decision::PropagatedTerminal) state without
//!   executing when it can never fire (which itself cascades). It surfaces the
//!   source frontier ([`initial_ready`](readiness::ReadinessTracker::initial_ready))
//!   and a [`pending_count`](readiness::ReadinessTracker::pending_count) for the
//!   driver's "nothing pending" run-end signal. Pure: no spawning, scheduling,
//!   timing, or event writing.
//! - [`readiness::evaluate_rule`] — the pure rule-evaluation seam over all three
//!   trigger rules (`all-succeeded`, `all-terminal`, `any-failed`); the
//!   `all-succeeded` path is wired onto runtime nodes, with the other two
//!   reachable through the same seam.
//!
//! The **single-attempt execution core**: the load-bearing
//! spine of the attempt runner — running *one* attempt of *one* node.
//!
//! - [`execution::run_attempt`] — the runtime-agnostic `async fn` that opens the
//!   attempt span (already on the [`RunContext`]), records
//!   the admission phase marker, dispatches the already-placed
//!   [`Task::run`](task::Task) work and awaits it, classifies the outcome
//!   into the normative taxonomy, fills the [`Slot`](slot::Slot<T>) on
//!   success only, and emits the ordered per-transition events plus exactly one
//!   attempt-outcome record. It adds no async-runtime dependency (the caller's
//!   runtime drives it; execution-class placement is handled separately).
//! - [`execution::AttemptOutcome`] — the classified single-attempt outcome
//!   (success / permanent failure / retry-eligible failure / deliberate skip /
//!   **timed-out**), a `#[non_exhaustive]` enum; the panic variant is still
//!   reserved.
//! - [`execution::AttemptEventSink`] / [`execution::AttemptEvent`] — the
//!   abstract event-emission port the runner writes through, so `dagr-core`
//!   emits events without depending on the artifact crate's writer. The run-loop
//!   driver adapts the concrete `EventStreamWriter` to this port.
//!
//! The **per-attempt timeout**: a runtime-agnostic per-attempt
//! timeout with per-class abandonment.
//!
//! - [`execution::run_attempt_with_timeout`] — the **await-bound** path: races
//!   the attempt future against a caller-provided deadline future (no tokio
//!   dependency added — the isolated framework timer drives the real one);
//!   on timeout the future is dropped (true cancellation) and a permit-shaped
//!   guard moved into it releases immediately.
//! - [`execution::TimeoutDecision`] / [`execution::LateResultBarrier`] /
//!   [`execution::ZombieObserver`] — the **blocking / compute** path: mark
//!   `timed-out` immediately, hold the permit until the (unkillable) closure
//!   returns, defer the retry until then (single-attempt exclusivity), and bar
//!   any late slot fill or scratch write. The permit guard stands in for the
//!   concrete admission ledger.
//!
//! The **retry with jittered exponential backoff**: the bounded
//! retry loop that wraps the single-attempt core (backoff is exponential with
//! jitter and a cap).
//!
//! - [`execution::run_with_retries`] — the retry loop: it drives the shared
//!   single-attempt core up to the configured maximum, emitting one
//!   attempt-outcome record **per attempt** and exactly one node-terminal record
//!   at loop end. Only retry-eligible outcomes (retry-eligible failure or
//!   timeout) consume the budget and trigger a backoff; permanent failure,
//!   deliberate skip, and success end the loop at once. Between attempts it
//!   records a distinct [`execution::AttemptEvent::BackoffStarted`] backoff phase
//!   (feeding the phase timings) and awaits a caller-provided timer future — it
//!   reads no system clock, so the sleeping is the driver's concern.
//! - [`execution::RetryConfig`] / [`execution::Backoff`] — the retry knob the
//!   runner reads (max attempts + base/factor/cap) whose conservative default is
//!   **no retries** (a single attempt). Retry/backoff configuration has one
//!   authoring home — the [`assembly::NodePolicy`] — and `RetryConfig`
//!   is the derived runner input [`assembly::NodePolicy::retry_config`] produces.
//! - [`execution::Jitter`] / [`execution::SeededJitter`] / [`execution::NoJitter`]
//!   — the **injectable, deterministic** jitter source: a dependency-free seeded
//!   `splitmix64` PRNG for production (a distinct seed per node spreads a fan-out
//!   so simultaneous retries do not resynchronize) and a zero source for exact,
//!   assertable schedules in tests. The loop never reads a thread/global RNG.
//!
//! The **panic containment**: a task that panics fails **only
//! its own node** rather than unwinding the run.
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
//! The **resume core**: the pure gate + seed/closure/demand plan.
//!
//! - [`resume::plan_resume`] — given this binary's [`Pipeline`] and a prior run's
//!   decoded per-node facts ([`resume::PriorRun`]), computes a demand-driven
//!   re-execution [plan](resume::ResumePlan) or [refuses](resume::ResumeRefusal).
//!   The gate refuses a changed structural fingerprint, an incomparable
//!   fingerprint-algorithm version, or a cross-tool-version prior; a policy-hash
//!   divergence proceeds with a [diff](resume::PolicyDiff). The must-run seed is
//!   every non-`succeeded` node plus every teardown-covered node, closed downward
//!   and resolved upward — a demanded durable producer is rehydrated (its
//!   reference [existence-probed](resume::ReferenceExistence); a definite absence
//!   fails the plan), a demanded in-memory producer re-executes and cascades.
//!   Every prior success left outside the must-run set is `satisfied-from-prior`
//!   carrying its originating run identity. Pure and dependency-free; the CLI
//!   (`dagr_cli::contract`) wires it behind the `resume` verb.
//!
//! The **single-task test kit**, behind the default-on
//! `test-kit` feature: the first of the three testing levels — a *shipped*
//! utility downstream test code calls to exercise **one** task in isolation.
//!
//! - [`test_kit::SingleTaskTest`] — configures a controlled [`RunContext`]
//!   and a fake [`ResourceRegistry`], then drives
//!   the task with [`run_sync`](test_kit::SingleTaskTest::run_sync) (**no async
//!   runtime**) or [`run_await`](test_kit::SingleTaskTest::run_await) (the kit's
//!   **provided** dependency-free test runtime — the caller stands up none). It
//!   runs one task with **no driver, scheduler, or event stream**, and threads
//!   cancellation/attempt/params/interval/scratch/secrets in explicitly so two
//!   runs of the same inputs behave identically.
//! - [`test_kit::TaskOutcome`] — the captured result: the produced output *or*
//!   the classified [`TaskError`] (classification readable), the observed attempt,
//!   the attempt [`metrics`](metrics::AttemptMetrics), the
//!   [`scratch`](context::ScratchStore) store, and a framework-output dump
//!   used to prove a marked [`Secret`] never leaks. The
//!   context-construction and fake-injection surface it exposes is the seam the
//!   full-pipeline harness reuses. Adds **no** dependency and **no**
//!   production runtime behavior.
//!
//! Execution-class dispatch and the run-loop driver build on this
//! core rather than reshape it.
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
pub mod resume;
pub mod scratch;
pub mod slot;
pub mod stable_name;
pub mod task;
#[cfg(feature = "test-kit")]
pub mod test_kit;

pub use assembly::{
    AssemblyArtifact, AssemblyError, CostVector, DurableOutput, DurableReferenceMeta,
    DurableWitness, EffectivePolicy, FINGERPRINT_ALGORITHM_VERSION, FingerprintSlot, NodePolicy,
    Problem, ProblemKind, Warning,
};
pub use binding::{
    BoundInput, CloneOnRead, DataEdge, Deps, EdgeKind, MAX_INPUT_ARITY, NodeBinding, OrderingEdge,
    OrderingHandle, ReceiveMode, RegisteredNode, Shared, TriggerRule,
};
pub use context::{
    BootstrapFailure, BootstrapOutcome, CancellationOrigin, CancellationSignal, CancellationSource,
    CoveredNodeStates, DataInterval, LogSpan, MissingResourceError, PipelineId, RegistryError,
    ResourceRegistry, ResourceRegistryBuilder, ResourceRequirement, ResourceRequirements,
    RunContext, RunContextBuilder, RunId, ScratchError, ScratchStore, Secret, TerminalState,
    surface_requirements,
};
pub use error::{RehydrateClass, RehydrateError, TaskError, TaskErrorClass};
pub use execution::{
    AttemptEvent, AttemptEventSink, AttemptOutcome, Backoff, BootstrapRefusal, Jitter,
    LateResultBarrier, NoJitter, PanicStrategy, RetryConfig, SeededJitter, TimeoutDecision,
    ZombieObserver, check_panic_strategy, detect_panic_strategy, install_panic_hook, run_attempt,
    run_attempt_caught, run_attempt_with_timeout, run_with_retries, run_with_retries_caught,
};
pub use flow::{FailureMode, Flow, Pipeline, PipelineNode, StableTypeNames};
pub use handle::{Handle, NodeId};
pub use limits::{
    CapacityBootstrapFailure, CapacityError, ContainerLimitProbe, HEADROOM_DEFAULT, PinnedPools,
    detect_capacities,
};
pub use metrics::{
    AttemptMetrics, AttemptScope, AttributingAllocator, MAX_ENCODED_BYTES, MAX_ENTRIES,
    METRIC_PEAK_MEMORY_BYTES, METRIC_TRUNCATED, METRIC_TRUNCATED_DROPPED_BYTES,
    METRIC_TRUNCATED_DROPPED_ENTRIES, MetricError, MetricValue, PERMIT_PREFIX, PHASE_PREFIX,
    RESERVED_PREFIX, VALUE_ENCODED_BYTES,
};
pub use readiness::{Decision, ReadinessTracker, RuleOutcome, evaluate_rule};
pub use resume::{
    PolicyDiff, PriorNode, PriorRun, ReferenceExistence, ResumePlan, ResumeRefusal, plan_resume,
};
pub use slot::{
    ConsumerLease, DeliveryMode, FillError, RedeemError, RedemptionHandle, ResidencyLedger, Slot,
    SlotRef,
};
pub use stable_name::{StableInputNames, StableName, UNIT_STABLE_NAME, is_well_formed};
pub use task::{ExecutionClass, Task};

/// The `#[derive(StableName)]` derive macro — the one-line ergonomic that emits the
/// `impl StableName` the graph-emittable registrars require, so `#[task]` tasks and
/// their payloads compose with `#[dag]` / `FlowBuilder::source` / `node` with no
/// hand-written trait bodies. Re-exported behind the default-on `macros` feature
/// exactly as [`macro@task`] is; `--no-default-features` drops it and the runtime
/// dependency graph is unchanged (a proc-macro is never linked into the binary).
///
/// The derive (macro namespace) coexists with the [`StableName`] trait (type
/// namespace) under this one name, the standard trait+derive pairing — `use
/// dagr_core::StableName;` brings both into scope.
#[cfg(feature = "macros")]
pub use dagr_macros::StableName;
/// The `#[task]` attribute macro — the optional ergonomic authoring layer.
/// Re-exported from the build-time-only
/// [`dagr-macros`](dagr_macros) proc-macro crate **behind the default-on
/// `macros` feature**, so `use dagr_core::task;` resolves the attribute and
/// `--no-default-features` (or disabling `macros`) turns it off — preserving
/// core's zero-runtime-dependency guarantee (a proc-macro is a build-time
/// compiler plugin, never linked into the shipped binary).
///
/// Applied to an inherent `impl` block, `#[task]` expands to the exact
/// [`impl Task`](task::Task) an author writes by hand, inferring `Input`,
/// `Output`, and the `AwaitBound` class from the `run` signature. See the
/// [`dagr_macros::task`] docs for the full expansion rules; this
/// covers zero-input and single-input tasks.
///
/// # Zero-input `#[task]`
///
/// A task that consumes nothing writes a `()` input; `#[task]` generates
/// `type Input = ()`, `type Output = u64`, and the await-bound class:
///
/// ```
/// use dagr_core::task;
/// use dagr_core::task::{RunContext, Task};
/// use dagr_core::TaskError;
///
/// struct Answer { base: u64 }
///
/// #[task]
/// impl Answer {
///     async fn run(&mut self, _input: ()) -> Result<u64, TaskError> {
///         Ok(self.base)
///     }
/// }
///
/// // The generated `impl Task` has the hand-written shape and runs the body.
/// fn assert_shape<T: Task<Input = (), Output = u64>>() {}
/// assert_shape::<Answer>();
/// ```
///
/// # Single-input `#[task]`
///
/// One dependency argument `input: T` is delivered **bare** (never `(T,)`);
/// an optional `ctx: &RunContext` is threaded into the body only when declared:
///
/// ```
/// use dagr_core::task;
/// use dagr_core::task::{RunContext, Task};
/// use dagr_core::TaskError;
///
/// struct Stringify;
///
/// #[task]
/// impl Stringify {
///     async fn run(&mut self, ctx: &RunContext, input: u64) -> Result<String, TaskError> {
///         let _ = ctx.attempt(); // the declared ctx is usable in the body
///         Ok(input.to_string())
///     }
/// }
///
/// fn assert_shape<T: Task<Input = u64, Output = String>>() {}
/// assert_shape::<Stringify>();
/// ```
///
/// # `run` must return `Result<T, TaskError>`
///
/// A `run` written with a bare `-> T` (no `Result`) is rejected at compile time
/// with a message naming the required shape (the failure channel is explicit):
///
/// ```compile_fail
/// use dagr_core::task;
/// use dagr_core::TaskError;
///
/// struct BadReturn;
///
/// #[task]
/// impl BadReturn {
///     // Bare `-> u64` is rejected: a task's `run` must return
///     // `Result<T, TaskError>`.
///     async fn run(&mut self, _input: ()) -> u64 {
///         7
///     }
/// }
/// ```
#[cfg(feature = "macros")]
pub use dagr_macros::task;
#[cfg(feature = "test-kit")]
pub use test_kit::{SingleTaskTest, TaskOutcome};
