//! The **run-a-Flow** convenience seam — a public one-call path that runs a
//! [`Flow`] of real [`Task`]s to a completed run **without** the author
//! hand-writing any scheduling or permit plumbing.
//!
//! # Why this module exists
//!
//! A task author writes **tasks + a flow** and *never* writes the scheduling/permit
//! plumbing. Without this seam every milestone demo hand-wrote ~150 lines of
//! type-erased [`NodeRunner`] impls per pipeline — a
//! `Pin<Box<dyn Future<Output = TerminalState> + Send>>` per node that reads input
//! [`SlotRef`]s, calls `run_attempt` / `run_with_retries_caught`, and fills the
//! output [`Slot`]. That is exactly the plumbing the author must not write.
//! [`RunnableFlow`] captures that plumbing **generically, once**, at registration
//! time (where the concrete `Task` + `Input`/`Output` types are known), so the
//! author registers real tasks and calls [`run`](RunnableFlow::run).
//!
//! # How the generic adapter works (the crux)
//!
//! A pipeline's nodes have heterogeneous input/output types, so the driver's
//! [`NodeRunner`] is type-erased. Building one for a
//! node needs the node's concrete `Task` type **and** the concrete types of its
//! upstreams' output slots — knowledge that exists **only at the registration
//! call site**, where `T: Task` and the bound `deps` values are in scope. So at
//! **registration time** `RunnableFlow` captures a boxed `RunnerFactory` closure
//! that, given the run's slot registry (node id → its type-erased
//! output slot), downcasts each upstream slot back to its concrete
//! `Arc<Slot<Upstream>>` (the downcast is infallible by construction — the same
//! type was stored under that id), reads the wired input value through the declared
//! [receive mode](dagr_core::binding::ReceiveMode), and builds a
//! `GenericNodeRunner` that drives the node through the **same** real attempt
//! path the hand-wired demos use. Nothing about the adapter is per-type: it is
//! generic over `T: Task` and over the input arity via the [`InputWiring`] seam
//! (implemented for the bare-input and the tuple dep shapes, arities 1..=8), so a
//! three-node chain and an eight-input fan-in are built by the identical code.
//!
//! # What stays intact
//!
//! This path is **purely additive**. The existing
//! [`NodeRunner`] / [`RunPlan::new`](crate::driver::RunPlan)
//! surface is untouched — the milestone demos and the `full_pipeline` fake harness
//! still hand-write runners and still compile. The generic runners this module
//! builds are ordinary `NodeRunner`s driven by the real
//! [`drive`] loop, so the `scratch_root` wiring and the
//! resume seam apply to them unchanged: a single-attempt node reaches its real
//! per-node durable scratch namespace through the driver's per-attempt context.
//!
//! # The policies this path enforces
//!
//! A node's [`NodePolicy`] is not merely recorded here — its **retry backoff** and
//! its **per-attempt timeout** are enforced on every run this seam drives:
//!
//! - the retry loop's computed backoff is really waited, through the injected
//!   [`AttemptTimer`] (production [`SystemTimer`]), so the `BackoffStarted` delay in
//!   the event stream is a claim about *elapsed* time;
//! - the declared timeout is armed per attempt, by class: **await-bound** work is
//!   raced against its deadline and the losing future is dropped (true cancellation,
//!   permit released at the mark), while **blocking / compute** work — which cannot
//!   be stopped, or even polled, while it runs — is marked `timed-out` by the
//!   driver's isolated timer, its permit held until its closure returns and its late
//!   result refused (see [`AttemptFate`]).
//!
//! A node that declares neither policy is unaffected: nothing is armed, no timer is
//! spawned, and its event stream is what it was.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::binding::{BoundInput, Deps, ReceiveMode};
use dagr_core::context::{ResourceRegistry, RunContext, TerminalState};
use dagr_core::execution::{
    AttemptEventSink, NoJitter, RetryConfig, run_attempt_caught, run_attempt_caught_with_timeout,
    run_with_retries_caught, run_with_retries_caught_timed,
};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::{Handle, NodeId};
use dagr_core::payload::{self, Payload};
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::stable_name::{StableInputNames, StableName};
use dagr_core::task::{ExecutionClass, Task};

use crate::driver::{AttemptFate, NodeRunner, RunConfig, drive};
use crate::run_store::{FileSink, SystemClock, mint_run_id};

/// The run's **slot registry**: node [id](NodeId) → that node's type-erased output
/// slot (`Arc<Slot<Output>>` boxed as `Arc<dyn Any + Send + Sync>`).
///
/// A registration-time `RunnerFactory` downcasts each slot it needs back to the
/// concrete `Arc<Slot<T>>`. The downcast is infallible by construction: the slot
/// was stored under this id as exactly that type (the producing node's output
/// type), so the consumer's captured upstream type — proven equal to the
/// producer's output by the `Deps<Inputs = T::Input>` bound at registration — is
/// the same type. Keying by [`NodeId`] (not name) means an edge's `NodeId` resolves
/// its upstream slot directly, with no impossible id→name reverse lookup.
type SlotRegistry = HashMap<NodeId, Arc<dyn Any + Send + Sync>>;

/// A **registration-time runner factory**: given the run's slot registry, the
/// run's [timer](AttemptTimer), and the [attempt discipline](AttemptDiscipline),
/// build this node's type-erased [`NodeRunner`]. Captured where the node's concrete
/// `Task` + input/output types are known, so it can wire the typed slots the driver
/// only ever sees erased.
type RunnerFactory = Box<
    dyn FnOnce(&SlotRegistry, &Arc<dyn AttemptTimer>, AttemptDiscipline) -> Box<dyn NodeRunner>
        + Send,
>;

/// How many attempts a built runner is allowed, and whether it arms the node's
/// declared timeout.
///
/// The run loop uses [`NodePolicy`](AttemptDiscipline::NodePolicy): the node's stated
/// retry budget and timeout, enforced here. A **pod-side** attempt uses
/// [`ExactlyOnce`](AttemptDiscipline::ExactlyOnce): retry, backoff, and timeout are
/// the *orchestrator's* decisions (ADR 115 §2), and two retry loops running at once
/// would duplicate an attempt. The discipline is chosen when the runner is built
/// rather than inside it, because the retry budget is baked into the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptDiscipline {
    /// The node's declared retry budget and per-attempt timeout, enforced here —
    /// the local run loop.
    NodePolicy,
    /// **Exactly one** attempt, with no backoff and no timeout armed: the caller
    /// owns those decisions.
    ExactlyOnce,
}

impl AttemptDiscipline {
    /// The retry configuration a runner is built with under this discipline.
    fn retry_config(self, declared: RetryConfig) -> RetryConfig {
        match self {
            Self::NodePolicy => declared,
            // One attempt: `run_attempt_caught` is then the path taken, so no
            // `BackoffStarted` can be emitted — the retry loop is never entered.
            Self::ExactlyOnce => RetryConfig::new(1, *declared.backoff()),
        }
    }

    /// The timeout enforcement a runner is built with under this discipline.
    fn enforcement(self, declared: TimeoutEnforcement) -> TimeoutEnforcement {
        match self {
            Self::NodePolicy => declared,
            Self::ExactlyOnce => TimeoutEnforcement::None,
        }
    }
}

/// The **type-erased codec** for one node's output value: decode bytes into its
/// output slot, and encode whatever its slot holds.
///
/// Both halves are plain `fn` pointers monomorphized at registration, where the
/// concrete output type is still known — so a node registered through a
/// `Payload`-bounded registrar carries the ability to cross a process boundary, and
/// one registered through an ordinary registrar carries `None` and simply cannot.
/// That is ADR 115 §8's "remote-eligible **iff** its types implement `Payload`",
/// expressed as a captured capability rather than a runtime check.
#[derive(Clone, Copy)]
struct NodeCodec {
    fill: fn(&SlotRegistry, NodeId, &[u8]) -> Result<(), payload::CodecError>,
    encode: fn(&SlotRegistry, NodeId) -> Option<Vec<u8>>,
    stable_name: &'static str,
}

/// The codec pair for output type `V`, captured where `V` is concrete.
fn codec_of<V>() -> NodeCodec
where
    V: Payload + Send + Sync + 'static,
{
    NodeCodec {
        fill: fill_slot_from_bytes::<V>,
        encode: encode_slot::<V>,
        stable_name: V::STABLE_NAME,
    }
}

/// Decode `bytes` as `V` and fill the slot registered under `id`.
fn fill_slot_from_bytes<V>(
    registry: &SlotRegistry,
    id: NodeId,
    bytes: &[u8],
) -> Result<(), payload::CodecError>
where
    V: Payload + Send + Sync + 'static,
{
    let value = V::decode(bytes)?;
    if let Some(slot) = registry
        .get(&id)
        .and_then(|any| any.downcast_ref::<Arc<Slot<V>>>())
    {
        // A rejected fill means the slot was already filled, which cannot happen for
        // a freshly-minted registry; ignoring it keeps this infallible for the
        // caller, whose only real failure mode is the codec.
        let _ = slot.fill(value);
    }
    Ok(())
}

/// A **one-node filler** over `id`'s slot: hand it the bytes a remote attempt
/// produced and it decodes them into that slot, so the node's local consumers read
/// the value exactly as they would after a local run.
///
/// It captures a one-entry registry rather than the whole one because the whole
/// registry is moved into the [`RunReport`] at the end of the run and the filler has
/// to outlive that move; a single `Arc` clone of the slot is all it needs, and
/// narrowing it means a filler for one node cannot touch another's.
///
/// The error is a `String` because it crosses into [`crate::remote`], which is
/// `k8s`-gated and must not name the codec's own error type — the *classification*
/// is done here, where the type is in scope.
#[cfg(feature = "k8s")]
fn slot_filler(registry: &SlotRegistry, id: NodeId, codec: NodeCodec) -> crate::remote::SlotFill {
    let mut only: SlotRegistry = HashMap::new();
    if let Some(slot) = registry.get(&id) {
        only.insert(id, Arc::clone(slot));
    }
    let fill = codec.fill;
    let stable_name = codec.stable_name;
    Box::new(move |bytes: &[u8]| {
        fill(&only, id, bytes).map_err(|err| {
            format!("the bytes the attempt produced are not a valid `{stable_name}`: {err}")
        })
    })
}

/// Encode whatever the slot registered under `id` holds, or `None` when it holds
/// nothing (the attempt did not succeed, or released its value already).
fn encode_slot<V>(registry: &SlotRegistry, id: NodeId) -> Option<Vec<u8>>
where
    V: Payload + Send + Sync + 'static,
{
    let slot = registry.get(&id)?.downcast_ref::<Arc<Slot<V>>>()?;
    slot.is_filled()
        .then(|| slot.shared_ref().read().encode_to_vec())
}

/// The **injected wait seam** every enforced policy on this path measures its time
/// through: the retry loop's backoff and an await-bound attempt's per-attempt
/// deadline.
///
/// It exists so that `dagr-core` gains **no clock** — the engine computes *what* to
/// wait (the jittered exponential delay, the declared timeout budget) and awaits a
/// future this seam supplies, exactly as [`Jitter`](dagr_core::execution::Jitter)
/// injects randomness. Production passes [`SystemTimer`], whose future really
/// elapses, so an emitted `BackoffStarted` delay is a claim about *elapsed* time and
/// a declared timeout is a real deadline; a test passes a recording timer that
/// resolves at once, so the scheduled sequence is assertable without sleeping.
pub trait AttemptTimer: Send + Sync {
    /// A future that resolves once `delay` has elapsed. It is polled on whichever
    /// surface the node's class routed the attempt onto, so it must not require the
    /// caller to be inside any particular runtime.
    fn sleep(
        &self,
        delay: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
}

/// The production [`AttemptTimer`]: a real, elapsing wait.
///
/// It prefers the ambient async runtime's timer (`tokio::time::sleep`) so an
/// await-bound attempt yields its worker while it waits. On the **compute** surface
/// there is no async runtime at all (the rayon pool drives the attempt future with a
/// park-based executor), so it falls back to parking the thread — honest for a
/// dedicated compute thread that is already the node's own, and it keeps the seam
/// runtime-agnostic rather than forcing a runtime onto a pool that has none.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTimer;

impl SystemTimer {
    /// The production timer.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl AttemptTimer for SystemTimer {
    fn sleep(
        &self,
        delay: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        if tokio::runtime::Handle::try_current().is_ok() {
            Box::pin(tokio::time::sleep(delay))
        } else {
            // No ambient runtime (the compute pool's threads): park this thread for
            // the delay. The attempt owns the thread, so nothing else is delayed.
            Box::pin(async move { std::thread::sleep(delay) })
        }
    }
}

/// One registered node's captured build recipe: its identity, the maker for its
/// typed output slot (so the assembler can mint it with the right `T` and consumer
/// count), and its runner factory.
struct RegisteredRunner {
    /// The node's identity name (the driver keys its runner map + records by name).
    name: String,
    /// The node's identity id (the slot registry keys by id).
    id: NodeId,
    /// Mint this node's output slot as `Arc<dyn Any + Send + Sync>` (a boxed
    /// `Arc<Slot<Output>>`), given the assembly-precomputed consumer count.
    make_slot: Box<dyn FnOnce(u32) -> Arc<dyn Any + Send + Sync> + Send>,
    /// Build the node's runner once every slot is in the registry.
    factory: RunnerFactory,
    /// This node's output codec, present exactly when it was registered through a
    /// `Payload`-bounded registrar — which is what makes it remote-eligible.
    codec: Option<NodeCodec>,
}

/// The result of a one-call [`RunnableFlow::run`]: the driver's overall
/// [outcome](RunOutcome), the per-node terminal states, and typed read-back of any
/// node's produced value by its [`Handle`].
///
/// This wraps the driver's [`driver::RunReport`](crate::driver::RunReport) and
/// additionally retains the run's output slots, so a caller can read a node's value
/// after the run through [`output`](RunReport::output) — the value a hand-wired demo
/// otherwise reads by re-wiring a slot it kept a reference to.
pub struct RunReport {
    inner: crate::driver::RunReport,
    slots: SlotRegistry,
}

impl RunReport {
    /// The overall run [outcome](RunOutcome) the driver surfaced.
    #[must_use]
    pub fn outcome(&self) -> RunOutcome {
        self.inner.outcome
    }

    /// This node's terminal state, or [`None`] if the pipeline had no such node.
    #[must_use]
    pub fn terminal_state(&self, node: &str) -> Option<TerminalState> {
        self.inner.terminal_states.get(node).copied()
    }

    /// The run's resolved identity (as it appears in the store path and every
    /// record).
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.inner.run_id
    }

    /// The driver's full [`driver::RunReport`](crate::driver::RunReport) (exit
    /// selection, cancellation origin, stream path, …).
    #[must_use]
    pub fn driver_report(&self) -> &crate::driver::RunReport {
        &self.inner
    }

    /// Read the value a node produced, by the [`Handle`] its registration returned,
    /// or [`None`] if the node did not fill its slot (it failed, was skipped, or its
    /// output was already released to its consumers).
    ///
    /// The value type is the handle's `T`, so this is a **typed** read with no
    /// downcast in the caller's code. A node whose slot still holds its value reads
    /// it back; a node whose consumers already moved the value out reads [`None`].
    #[must_use]
    pub fn output<T>(&self, handle: Handle<T>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let any = self.slots.get(&handle.id())?;
        let slot = any.downcast_ref::<Arc<Slot<T>>>()?;
        if slot.is_filled() {
            Some(slot.shared_ref().read().as_ref().clone())
        } else {
            None
        }
    }
}

/// The **run-a-Flow** builder — register real [`Task`]s with typed data
/// dependencies, then run the whole flow in **one call** with no hand-written
/// [`NodeRunner`].
///
/// It wraps a [`Flow`] (so node identity, typed handles, dependency binding, and
/// assembly are exactly the framework's) and, alongside each registration, captures
/// a generic `RunnerFactory` that reproduces — for that node's concrete task and
/// input/output types — the plumbing the demos hand-wrote: read the wired inputs,
/// drive the real attempt/retry path, fill the output slot.
///
/// The registration surface mirrors [`Flow`]'s: [`register_source`](Self::register_source)
/// for a consume-nothing node, [`register`](Self::register) for a data-dependent
/// node, and [`register_with`](Self::register_with) for a policy-carrying (e.g.
/// retrying) node. Each returns the node's typed [`Handle`], usable to bind
/// downstream and to read the produced value off the [`RunReport`].
pub struct RunnableFlow {
    flow: Flow,
    runners: Vec<RegisteredRunner>,
    /// The run's wait seam — the backoff and per-attempt-deadline futures every
    /// node's runner awaits. [`SystemTimer`] unless a caller injects another.
    timer: Arc<dyn AttemptTimer>,
    /// The **local codec check** (`--dagr.force-roundtrip`, default off), shared with
    /// every payload-bounded node this flow registers so the operator's answer can
    /// arrive before *or* after registration — which it must, because a hosted flow
    /// is built by a factory that never sees the invocation's argv.
    force_roundtrip: Arc<AtomicBool>,
    /// The live resource handles this flow's tasks obtain through
    /// [`RunContext::resources`] on the **pod-side** attempt path
    /// ([`prepare_attempt`](Self::prepare_attempt)).
    ///
    /// A `ResourceRegistry` is exactly the thing ADR 115 §2 says is *not*
    /// transported: it holds live client handles, so the pod rebuilds it by running
    /// the binary's own flow-building code. Attaching it here is what makes "the
    /// binary's own `main()` path" a real statement rather than a hope.
    resources: Option<ResourceRegistry>,
}

impl Default for RunnableFlow {
    fn default() -> Self {
        Self::new()
    }
}

impl RunnableFlow {
    /// Begin a fresh runnable flow with no nodes registered, waiting through the
    /// production [`SystemTimer`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            flow: Flow::new(),
            runners: Vec::new(),
            timer: Arc::new(SystemTimer::new()),
            force_roundtrip: Arc::new(AtomicBool::new(false)),
            resources: None,
        }
    }

    /// Attach the live [`ResourceRegistry`] this flow's tasks obtain their resources
    /// from on the **pod-side** attempt path ([`prepare_attempt`](Self::prepare_attempt)).
    ///
    /// Build it inside the flow factory: because a pod re-enters the same binary and
    /// calls the same factory, the registry is reconstructed there from the same
    /// code, and a resource holding a connection pool, a lock file, or a local cache
    /// therefore exists **once per pod** rather than once per run. That consequence
    /// is documented, not prevented (ADR 115 §Consequences).
    ///
    /// The in-process `run` path builds its per-attempt contexts inside the driver,
    /// which owns resource injection there; this method does not change it.
    #[must_use]
    pub fn with_resources(mut self, resources: ResourceRegistry) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Turn the **local codec check** on or off for this flow — the
    /// `--dagr.force-roundtrip` operator toggle, default **off**.
    ///
    /// On, every handoff registered through the payload-bounded registrars
    /// ([`register_source_payload`](Self::register_source_payload) /
    /// [`register_payload`](Self::register_payload)) is encoded and decoded before it
    /// reaches its slot, so a codec bug fails a node *here* instead of in a cluster
    /// at hour three. A value that does not survive its own round trip fails the node
    /// permanently, naming the classified codec error.
    ///
    /// Off — the default — is the in-memory fast path, untouched: nothing is encoded,
    /// no codec method is called, and the event stream is byte-for-byte what it was.
    ///
    /// The setting may be applied **before or after** registration (it is shared with
    /// the nodes already registered), which is what lets `dagr run <flow>` honour the
    /// flag on a flow its factory built without ever seeing the invocation.
    #[must_use]
    pub fn force_roundtrip(self, on: bool) -> Self {
        self.force_roundtrip.store(on, Ordering::Relaxed);
        self
    }

    /// Run this flow's waits through an injected [`AttemptTimer`] instead of the
    /// production [`SystemTimer`] — the seam that mirrors the engine's existing
    /// [`Jitter`](dagr_core::execution::Jitter) injection.
    ///
    /// A test passes a recording timer that captures each scheduled delay and
    /// resolves immediately, so the backoff schedule is assertable **deterministically
    /// and without sleeping**. Production code has no reason to call this.
    #[must_use]
    pub fn with_timer(mut self, timer: Arc<dyn AttemptTimer>) -> Self {
        self.timer = timer;
        self
    }

    /// Register a **source** node (one whose task consumes nothing) under `name`,
    /// returning its output [`Handle`]. Runs a single attempt through the real
    /// caught attempt runner on the driver's per-attempt context (so its durable
    /// scratch namespace is reachable).
    ///
    /// This uses the **type-erased** flow registrar, so the node carries **no**
    /// author-declared stable names and is not emittable to the graph artifact.
    /// For a graph-emittable source (so `graph <flow>` / `validate <flow>` work
    /// through [`crate::registry::run_registry`]), use the stable-name-aware
    /// [`register_source_named`](Self::register_source_named) instead.
    #[must_use]
    pub fn register_source<T>(&mut self, name: impl Into<String>, task: T) -> Handle<T::Output>
    where
        T: Task<Input = ()> + Send + 'static,
        T::Output: Send + Sync + 'static,
    {
        let name = name.into();
        let handle = self.flow.register_source::<T>(&name, &task);
        self.push_source_runner(&name, task, handle);
        handle
    }

    /// Register a **stable-name-aware source** node (one whose task consumes
    /// nothing) under `name`, returning its output [`Handle`].
    ///
    /// The graph-emittable counterpart of [`register_source`](Self::register_source):
    /// it registers through the flow's stable-name-aware registrar (so the built
    /// [`Pipeline`] records `T::STABLE_NAME` and `T::Output`'s stable name), which is
    /// what lets `graph <flow>` emit the graph artifact for a registry-hosted flow
    /// ([`crate::registry::run_registry`]). The run behaviour is identical to
    /// [`register_source`](Self::register_source) — a single caught attempt on the
    /// driver's per-attempt context.
    #[must_use]
    pub fn register_source_named<T>(
        &mut self,
        name: impl Into<String>,
        task: T,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()> + StableName + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
    {
        self.register_source_named_with(name, task, NodePolicy::new())
    }

    /// Register a **stable-name-aware source** node under `name` with an explicit
    /// [`NodePolicy`], returning its output [`Handle`].
    ///
    /// The policy-carrying counterpart of
    /// [`register_source_named`](Self::register_source_named): identical in every
    /// other respect, and the registrar to reach for when a graph-emittable node
    /// also needs a stated policy — a [placement](dagr_core::Placement), a retry
    /// budget, a declared cost. Both facets are needed together often enough (a
    /// placed node is exactly a node whose *policy* the graph artifact must show)
    /// that having to choose between stable names and a policy would be a real gap.
    #[must_use]
    pub fn register_source_named_with<T>(
        &mut self,
        name: impl Into<String>,
        task: T,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()> + StableName + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
    {
        let name = name.into();
        // The stable-name-aware flow registrar captures `T`'s author-declared stable
        // names into the pipeline node, so the built pipeline is graph-emittable.
        let handle = self
            .flow
            .register_source_named::<T>(&name, &task, None::<String>, policy);
        self.push_source_runner_with(&name, task, policy, handle, None);
        handle
    }

    /// Register a **payload-bounded source** node under `name`, returning its output
    /// [`Handle`] — the source registrar whose produced value the local codec check
    /// can round-trip.
    ///
    /// It behaves exactly like [`register_source`](Self::register_source) and differs
    /// in one bound: `T::Output` must be a [`Payload`], i.e. it has a codec and an
    /// author-declared stable name. With
    /// [`force_roundtrip`](Self::force_roundtrip) off (the default) the value moves
    /// through the slot in memory with **no** encode call; with it on, the value is
    /// encoded and decoded before it reaches the slot, and a codec defect fails the
    /// node permanently.
    ///
    /// The bound is not a *requirement* on anything: ordinary registrations are
    /// unaffected, and remote eligibility — where the same bound becomes mandatory —
    /// is a later ticket's. Registration itself is the **type-erased** one
    /// [`register_source`](Self::register_source) performs, with the same consequence
    /// for graph emission described there.
    #[must_use]
    pub fn register_source_payload<T>(
        &mut self,
        name: impl Into<String>,
        task: T,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()> + Send + 'static,
        T::Output: Payload + Send + Sync + 'static,
    {
        let name = name.into();
        // The flow registration is the ordinary one: the codec check is a property of
        // how the node RUNS, never of the graph it is part of (nothing about the
        // pipeline, its fingerprints, or its artifact changes).
        let handle = self.flow.register_source::<T>(&name, &task);
        let wrapped = self.wrap_roundtrip(task);
        self.push_source_runner_with(
            &name,
            wrapped,
            NodePolicy::new(),
            handle,
            Some(codec_of::<T::Output>()),
        );
        handle
    }

    /// Register a **payload-bounded, stable-name-aware source** node under `name`
    /// with an explicit [`NodePolicy`] — the source registrar a *remote-eligible*
    /// node uses.
    ///
    /// It is [`register_source_named_with`](Self::register_source_named_with) plus
    /// `T::Output: Payload`: the node is graph-emittable, carries a stated policy
    /// (including a [placement](dagr_core::Placement)), and — because the bound makes
    /// its output type's codec known at registration — can have its value encoded and
    /// stored by the framework when the attempt runs somewhere else.
    #[must_use]
    pub fn register_source_payload_with<T>(
        &mut self,
        name: impl Into<String>,
        task: T,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()> + StableName + Send + 'static,
        T::Output: Payload + StableName + Send + Sync + 'static,
    {
        let name = name.into();
        let handle = self
            .flow
            .register_source_named::<T>(&name, &task, None::<String>, policy);
        let wrapped = self.wrap_roundtrip(task);
        self.push_source_runner_with(
            &name,
            wrapped,
            policy,
            handle,
            Some(codec_of::<T::Output>()),
        );
        handle
    }

    /// Register a **placed source** node: one an operator means to run somewhere
    /// else (M10, T108, ADR 115 §8).
    ///
    /// This is the registrar that makes remote eligibility a **compile-time** fact.
    /// A placed node is one dagr may hand to a pod, and a pod can only be handed
    /// bytes — so `T::Output: Payload` is a bound here rather than a check later.
    /// Registering a placement alongside a payload type with no codec reds the
    /// build, naming the type and the missing bound, instead of assembling cleanly
    /// and failing at submission time with a value nobody can serialize. That is in
    /// keeping with mis-wiring already being a compile error.
    ///
    /// `placement` is merged into `policy`, so the two cannot disagree: a placement
    /// stated here is the one the policy hash sees, the graph artifact shows, and
    /// the executor reads.
    ///
    /// Nothing about the local path changes. Under `--dagr.executor=local` a
    /// placement is recorded and ignored (T105) and the node runs in-process exactly
    /// as an unplaced one does — which is what makes one binary genuinely both.
    #[must_use]
    pub fn register_source_placed<T>(
        &mut self,
        name: impl Into<String>,
        task: T,
        policy: NodePolicy,
        placement: Placement,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()> + StableName + Send + 'static,
        T::Output: Payload + Send + Sync + 'static,
    {
        self.register_source_payload_with(name, task, policy.placement(placement))
    }

    /// Register a **placed data-dependent** node — the data-node twin of
    /// [`register_source_placed`](Self::register_source_placed), with the same
    /// `T::Output: Payload` bound and the same reason for it.
    #[must_use]
    pub fn register_placed<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
        policy: NodePolicy,
        placement: Placement,
    ) -> Handle<T::Output>
    where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Payload + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.register_payload_with(name, task, deps, policy.placement(placement))
    }

    /// Capture the runner factory + output-slot maker for a **source** node — the
    /// run-plumbing shared by [`register_source`](Self::register_source) and
    /// [`register_source_named`](Self::register_source_named) (which differ only in
    /// whether the flow registration captured stable names).
    fn push_source_runner<T>(&mut self, name: &str, task: T, handle: Handle<T::Output>)
    where
        T: Task<Input = ()> + Send + 'static,
        T::Output: Send + Sync + 'static,
    {
        self.push_source_runner_with(name, task, NodePolicy::new(), handle, None);
    }

    /// [`push_source_runner`](Self::push_source_runner) with an explicit policy —
    /// the shared body, so the policy-carrying and default-policy source registrars
    /// build the same runner from one place.
    fn push_source_runner_with<T>(
        &mut self,
        name: &str,
        task: T,
        policy: NodePolicy,
        handle: Handle<T::Output>,
        codec: Option<NodeCodec>,
    ) where
        T: Task<Input = ()> + Send + 'static,
        T::Output: Send + Sync + 'static,
    {
        let declared_retry = policy.retry_config();
        let declared_enforcement = TimeoutEnforcement::for_node::<T>(&policy);
        let node_name = name.to_string();
        let factory: RunnerFactory = Box::new(
            move |registry: &SlotRegistry,
                  timer: &Arc<dyn AttemptTimer>,
                  discipline: AttemptDiscipline| {
                let slot = downcast_slot::<T::Output>(registry, handle.id(), &node_name);
                Box::new(GenericNodeRunner {
                    name: node_name,
                    task: Some(task),
                    // A source consumes nothing: its input is ready without any slot read.
                    input: InputSource::Ready(Some(())),
                    slot,
                    retry_config: discipline.retry_config(declared_retry),
                    enforcement: discipline.enforcement(declared_enforcement),
                    timer: Arc::clone(timer),
                }) as Box<dyn NodeRunner>
            },
        );
        self.runners.push(RegisteredRunner {
            name: name.to_string(),
            id: handle.id(),
            make_slot: make_slot_boxed::<T::Output>(name),
            factory,
            codec,
        });
    }

    /// Register a **data-dependent** node under `name`, binding `deps` (whose value
    /// types must **exactly match** `T::Input` — a compile-time check via the
    /// `D: Deps<Inputs = T::Input>` bound), returning its output [`Handle`]. Runs a
    /// single attempt through the real caught attempt runner.
    #[must_use]
    pub fn register<T, D>(&mut self, name: impl Into<String>, task: T, deps: D) -> Handle<T::Output>
    where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.register_with::<T, D>(name, task, deps, NodePolicy::new())
    }

    /// Register a **data-dependent** node under `name`, binding `deps`, with an
    /// explicit [`NodePolicy`] (e.g. [`retries`](NodePolicy::retries)), returning its
    /// output [`Handle`].
    ///
    /// When the policy grants retries the node is driven through the **real** bounded
    /// retry loop ([`run_with_retries_caught`]); otherwise it is a single caught
    /// attempt. The choice, the two-attempt cycle, and the emitted records are the
    /// framework's — identical to what the hand-wired retrying demo produced.
    #[must_use]
    pub fn register_with<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        let name = name.into();
        // `deps` is `Clone` so we register it on the flow AND capture its wiring for
        // the runner factory. The flow does the real edge binding (and the
        // exact-type / arity / acyclicity compile-time checks); the captured reader
        // knows each upstream's concrete value type + declared receive mode.
        let handle = self
            .flow
            .register_with::<T, D>(&name, &task, deps.clone(), policy);
        self.push_data_runner::<T, D>(&name, task, deps, policy, handle, None);
        handle
    }

    /// Register a **stable-name-aware data-dependent** node under `name`, binding
    /// `deps`, returning its output [`Handle`].
    ///
    /// The graph-emittable counterpart of [`register`](Self::register): it registers
    /// through the flow's stable-name-aware registrar (so the built [`Pipeline`]
    /// records the stable task name, the ordered stable input type names, and the
    /// stable output type name), which is what lets `graph <flow>` emit the graph
    /// artifact for a registry-hosted flow ([`crate::registry::run_registry`]). The
    /// dependency binding (`D: Deps<Inputs = T::Input>`, the exact-type / arity /
    /// acyclicity checks) and the run behaviour are identical to
    /// [`register`](Self::register) — a single caught attempt through the real runner
    /// (this node carries the default [`NodePolicy`]).
    #[must_use]
    pub fn register_named<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
    ) -> Handle<T::Output>
    where
        T: Task + StableName + Send + 'static,
        T::Input: StableInputNames + Clone + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.register_named_with(name, task, deps, NodePolicy::new())
    }

    /// Register a **stable-name-aware data-dependent** node under `name`, binding
    /// `deps`, with an explicit [`NodePolicy`], returning its output [`Handle`].
    ///
    /// The policy-carrying counterpart of [`register_named`](Self::register_named)
    /// — the registrar a graph-emittable node with a stated policy needs. A
    /// [placed](dagr_core::Placement) node is the motivating case: its placement is
    /// policy, and the whole point of declaring it is that the graph artifact and a
    /// structure diff show it, which requires the node to carry stable names too.
    #[must_use]
    pub fn register_named_with<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task + StableName + Send + 'static,
        T::Input: StableInputNames + Clone + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        let name = name.into();
        // The stable-name-aware flow registrar captures `T`/`T::Input`/`T::Output`'s
        // author-declared stable names into the pipeline node (so the built pipeline
        // is graph-emittable) while binding the same edges `register` does.
        let handle =
            self.flow
                .register_named::<T, D>(&name, &task, deps.clone(), None::<String>, policy);
        self.push_data_runner::<T, D>(&name, task, deps, policy, handle, None);
        handle
    }

    /// Register a **payload-bounded data-dependent** node under `name`, binding
    /// `deps`, returning its output [`Handle`] — the data registrar whose produced
    /// value the local codec check can round-trip.
    ///
    /// The payload-bounded sibling of [`register`](Self::register): identical
    /// dependency binding (`D: Deps<Inputs = T::Input>`, the exact-type / arity /
    /// acyclicity checks) and identical run behaviour, with `T::Output: Payload`
    /// added. See [`register_source_payload`](Self::register_source_payload) for what
    /// the bound does and does not mean.
    ///
    /// A consumer reads what its producer put in the slot, so round-tripping each
    /// produced value covers **both** ends of every handoff between two such nodes.
    #[must_use]
    pub fn register_payload<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
    ) -> Handle<T::Output>
    where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Payload + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.register_payload_with::<T, D>(name, task, deps, NodePolicy::new())
    }

    /// Register a **payload-bounded data-dependent** node under `name` with an
    /// explicit [`NodePolicy`] — the data registrar a *remote-eligible* node uses.
    ///
    /// The policy-carrying counterpart of [`register_payload`](Self::register_payload).
    /// The `Payload` bound on `T::Output` is what captures the node's codec at
    /// registration, which is what lets the framework encode and store its value when
    /// the attempt runs in another process; the policy is where a
    /// [placement](dagr_core::Placement) and a retry budget are stated. Both are
    /// needed together on exactly the nodes an operator means to place.
    #[must_use]
    pub fn register_payload_with<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Payload + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        let name = name.into();
        let handle = self
            .flow
            .register_with::<T, D>(&name, &task, deps.clone(), policy);
        let wrapped = self.wrap_roundtrip(task);
        self.push_data_runner::<RoundTripTask<T>, D>(
            &name,
            wrapped,
            deps,
            policy,
            handle,
            Some(codec_of::<T::Output>()),
        );
        handle
    }

    /// Wrap `task` in the codec-check guard, sharing this flow's toggle cell — so the
    /// operator's answer can arrive after registration and still reach every node.
    fn wrap_roundtrip<T>(&self, task: T) -> RoundTripTask<T>
    where
        T: Task,
        T::Output: Payload,
    {
        RoundTripTask {
            inner: task,
            enabled: Arc::clone(&self.force_roundtrip),
        }
    }

    /// Capture the runner factory + output-slot maker for a **data-dependent** node
    /// — the run-plumbing shared by [`register_with`](Self::register_with) and
    /// [`register_named`](Self::register_named) (which differ only in whether the
    /// flow registration captured stable names).
    fn push_data_runner<T, D>(
        &mut self,
        name: &str,
        task: T,
        deps: D,
        policy: NodePolicy,
        handle: Handle<T::Output>,
        codec: Option<NodeCodec>,
    ) where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        let reader = deps.input_reader();
        let declared_retry = policy.retry_config();
        // The node's declared per-attempt timeout, resolved against its effective
        // execution class — the class decides *which* half of C14's honest timeout
        // semantics applies (drop the future, or mark the unkillable closure).
        let declared_enforcement = TimeoutEnforcement::for_node::<T>(&policy);
        let node_name = name.to_string();
        let factory: RunnerFactory = Box::new(
            move |registry: &SlotRegistry,
                  timer: &Arc<dyn AttemptTimer>,
                  discipline: AttemptDiscipline| {
                let slot = downcast_slot::<T::Output>(registry, handle.id(), &node_name);
                // Defer the input read to inside `run()`: by construction the driver only
                // admits this node after every upstream has succeeded, so its upstream
                // slots are filled THEN — reading here (at plan-assembly, before the run)
                // would hit an empty slot. The runner keeps a cheap `Arc`-clone snapshot
                // of the registry so it can resolve its inputs at attempt time.
                Box::new(GenericNodeRunner {
                    name: node_name,
                    task: Some(task),
                    input: InputSource::Deferred {
                        reader: Some(reader),
                        registry: registry.clone(),
                    },
                    slot,
                    retry_config: discipline.retry_config(declared_retry),
                    enforcement: discipline.enforcement(declared_enforcement),
                    timer: Arc::clone(timer),
                }) as Box<dyn NodeRunner>
            },
        );
        self.runners.push(RegisteredRunner {
            name: name.to_string(),
            id: handle.id(),
            make_slot: make_slot_boxed::<T::Output>(name),
            factory,
            codec,
        });
    }

    /// **Assemble and run** the whole flow in one call: finalize the pipeline,
    /// mint each node's typed output slot with its assembly-precomputed consumer
    /// count, build every node's generic runner (wiring producer→consumer slots),
    /// and drive it through the real [`drive`] loop against the
    /// injected `sink` and `clock`.
    ///
    /// `pipeline_name` is the run's pipeline identity; `config` is the bootstrap
    /// [`RunConfig`] (run-store base, grace, capacities, …). Returns a
    /// [`RunReport`] carrying the outcome, per-node terminal states, and typed
    /// read-back of produced values.
    ///
    /// # Errors
    ///
    /// Returns the [`AssemblyError`](dagr_core::assembly::AssemblyError) the pure
    /// assembly pass produces if the flow does not assemble (a duplicate name, an
    /// illegal edge, …). A run whose assembly succeeds always returns `Ok`; a node
    /// that *fails at run time* is reported through the [`RunReport`]'s outcome and
    /// terminal states, not this `Result`.
    pub fn run<S, C>(
        self,
        pipeline_name: &str,
        config: &RunConfig,
        sink: S,
        clock: C,
    ) -> Result<RunReport, dagr_core::assembly::AssemblyError>
    where
        S: EventSink + 'static,
        C: MonotonicClock + 'static,
    {
        let RunnableFlow {
            flow,
            runners,
            timer,
            // The codec-check cell was cloned into every payload-bounded node's guard
            // at registration; the run itself reads nothing from it.
            force_roundtrip: _,
            // Resource injection on this path is the driver's, which builds its own
            // per-attempt contexts; `with_resources` states that plainly.
            resources: _,
        } = self;
        let pipeline = flow.finish();
        // Assemble once here so we can (a) surface an assembly error to the caller
        // eagerly and (b) read the precomputed per-node consumer counts to size each
        // output slot exactly as the hand-wired demos did.
        let artifact = pipeline.assemble()?;

        // Build every node's typed output slot (keyed by id) with its precomputed
        // consumer count, THEN build every runner — two passes, so every slot exists
        // before any consumer's factory reads its upstreams' slots.
        let mut registry: SlotRegistry = HashMap::new();
        let mut factories: Vec<(String, RunnerFactory)> = Vec::with_capacity(runners.len());
        for r in runners {
            let consumers = artifact.consumer_count(r.id).unwrap_or(0);
            registry.insert(r.id, (r.make_slot)(consumers));
            factories.push((r.name, r.factory));
        }
        let mut node_runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
        for (name, factory) in factories {
            node_runners.insert(
                name,
                factory(&registry, &timer, AttemptDiscipline::NodePolicy),
            );
        }

        let plan = crate::driver::RunPlan::new(pipeline, node_runners);
        let report = drive(config, pipeline_name, Ok(plan), &[], sink, clock);
        Ok(RunReport {
            inner: report,
            slots: registry,
        })
    }

    /// **Assemble and run** the whole flow with its **placed** nodes on remote
    /// compute (M10, T112, ADR 115) — the wired counterpart to [`run`](Self::run).
    ///
    /// This is what T108 and T109 both deferred to T112: the flow-level wiring that
    /// turns a placed node into a `K8sNodeRunner`, and the startup discovery pass
    /// that reclaims a killed orchestrator's pods before anything is submitted.
    ///
    /// The shape is deliberately the same as [`run`](Self::run)'s — one plan, one
    /// `drive()` call, no driver change — because that is ADR 115's central claim:
    /// `NodeRunner` was *already* the "where does this node run" seam, so remoteness
    /// is one more implementation of it and the readiness cascade, admission ledger,
    /// teardown phase, resume path and exit-code precedence are untouched.
    ///
    /// Unplaced nodes run **in this process**, exactly as they always did. That is
    /// the point of the pair: iterate locally at full speed, then give the one node
    /// whose work wants 64 GiB the infrastructure it actually needs.
    ///
    /// # Errors
    ///
    /// Every [`RemoteRunError`](crate::remote::RemoteRunError) is a **bootstrap**
    /// condition — nothing has been submitted and no node has run:
    /// the flow does not assemble; the configuration carries no explicit run id
    /// (adoption after a restart is scoped to one); the observer's runtime could not
    /// be built; a placed node consumes upstream values this executor cannot bind;
    /// or the run's pods could not be listed, so what is already in flight is
    /// unknown. A failure *during* the run is reported through the run's outcome and
    /// terminal states like any other.
    #[cfg(feature = "k8s")]
    pub fn run_placed<A, L, S, C>(
        self,
        pipeline_name: &str,
        config: &RunConfig,
        sink: S,
        clock: C,
        cluster: crate::remote::RemoteCluster<A, L>,
    ) -> Result<crate::remote::PlacedRun, crate::remote::RemoteRunError>
    where
        A: dagr_k8s::api::PodApi,
        L: dagr_k8s::api::PodLifecycle + Clone,
        S: EventSink + 'static,
        C: MonotonicClock + 'static,
    {
        use crate::remote::RemoteRunError;

        let run_id = config
            .effective_run_id()
            .ok_or(RemoteRunError::NoRunId)?
            .to_string();

        let RunnableFlow {
            flow,
            runners,
            timer,
            force_roundtrip: _,
            resources: _,
        } = self;
        let pipeline = flow.finish();
        let artifact = pipeline.assemble().map_err(RemoteRunError::Assembly)?;
        let fp = artifact.fingerprint();
        let structural = crate::graph::format_fingerprint_structural(&fp);
        let policy_hash = crate::graph::format_fingerprint_policy(&fp);

        // Which nodes are placed, and can this executor actually bind their inputs?
        // A remote attempt is bound to references it is *given*; nothing in the
        // shipped path turns a local upstream's in-memory value into one, so a placed
        // node with data edges is refused by name rather than run in the wrong place.
        let mut placed: BTreeMap<String, dagr_core::assembly::Placement> = BTreeMap::new();
        for node in pipeline.nodes() {
            if let Some(placement) = node.policy().placement_spec() {
                if !node.data_edges().is_empty() {
                    return Err(RemoteRunError::UnboundInputs {
                        node: node.name().to_string(),
                        arity: node.data_edges().len(),
                    });
                }
                placed.insert(node.name().to_string(), placement);
            }
        }

        // Local slots and local runners first: the placed nodes' slots still exist
        // (their consumers read them through the ordinary registry), and the local
        // factories are what every unplaced node runs through.
        //
        // The **codec** is kept alongside each recipe because a placed node's value
        // has to make the return trip: the pod encodes it into the blob container,
        // and this process has to decode it back into the node's own slot or its
        // local consumers read an unfilled slot. That is the mirror image of
        // `prepare_attempt`'s `fill_input`, and it uses the same captured `fn`
        // pointer, so the two directions cannot disagree about the encoding.
        let mut registry: SlotRegistry = HashMap::new();
        let mut factories: Vec<(String, NodeId, RunnerFactory, Option<NodeCodec>)> =
            Vec::with_capacity(runners.len());
        for r in runners {
            let consumers = artifact.consumer_count(r.id).unwrap_or(0);
            registry.insert(r.id, (r.make_slot)(consumers));
            factories.push((r.name, r.id, r.factory, r.codec));
        }

        // A placed node with no codec provably cannot cross a process boundary, so it
        // is refused here rather than submitted to a pod whose bytes nothing could
        // decode. `register_source_placed` makes this unreachable by bound; a
        // placement attached through a non-`Payload` registrar's `NodePolicy` is the
        // path that reaches it.
        for (name, _, _, codec) in &factories {
            if placed.contains_key(name) && codec.is_none() {
                return Err(RemoteRunError::NotRemoteEligible { node: name.clone() });
            }
        }

        let must_run: std::collections::BTreeSet<String> =
            factories.iter().map(|(name, ..)| name.clone()).collect();
        let wiring = crate::remote::stand_up(cluster, &run_id, &structural, must_run)?;

        // The submission log owns the run's sequence counter: a write-ahead
        // submission record must be durable *before* its pod is created, and the
        // driver's buffering attempt sink drains only after an attempt returns. One
        // process, one mutex, one counter — the orchestrator is still single-writer.
        let log = crate::submission_log::SubmissionLog::over(sink, &run_id, pipeline_name);

        let mut node_runners: BTreeMap<String, Box<dyn crate::driver::NodeRunner>> =
            BTreeMap::new();
        for (name, id, factory, codec) in factories {
            let runner = match placed.get(&name) {
                Some(placement) => {
                    let node = pipeline
                        .nodes()
                        .find(|n| n.name() == name)
                        .expect("the placed name came from this pipeline");
                    let codec = codec.expect("a placed node without a codec was refused above");
                    wiring.runner(
                        node,
                        pipeline_name,
                        &structural,
                        &policy_hash,
                        *placement,
                        log.handle(),
                        Arc::clone(&timer),
                        slot_filler(&registry, id, codec),
                    )
                }
                None => factory(&registry, &timer, AttemptDiscipline::NodePolicy),
            };
            node_runners.insert(name, runner);
        }

        let plan = crate::driver::RunPlan::new(pipeline, node_runners).remote_wired(true);
        let report = drive(config, pipeline_name, Ok(plan), &[], log.sink(), clock);
        let (discovery, diagnostics) = wiring.finish();
        Ok(crate::remote::PlacedRun {
            report: RunReport {
                inner: report,
                slots: registry,
            },
            discovery,
            diagnostics,
        })
    }

    /// **Assemble and run** the whole flow to a local run store in **one call** — no
    /// hand-written sink, clock, or [`RunConfig`].
    ///
    /// This is the golden-path counterpart to the fully-explicit [`run`](Self::run):
    /// it mints a fresh run id, opens the default local-file [`FileSink`] at
    /// `<base>/<pipeline_name>/<run-id>/events.jsonl` (creating the directories),
    /// drives the flow with the wall-clock-derived [`SystemClock`] (so the artifact's
    /// durations are real), and returns the [`RunReport`]. Advanced callers who need a
    /// custom sink, a
    /// deterministic clock, or a tuned [`RunConfig`] keep using [`run`](Self::run); this
    /// wraps it and adds no execution logic of its own.
    ///
    /// # Errors
    /// - [`RunToStoreError::Store`] if the run store cannot be opened at `base` (an
    ///   unwritable or inaccessible directory) — surfaced **before** assembly, since
    ///   there is nowhere to record an artifact.
    /// - [`RunToStoreError::Assembly`] if the flow does not assemble (a duplicate name,
    ///   an illegal edge, …) — the same
    ///   [`AssemblyError`](dagr_core::assembly::AssemblyError) [`run`](Self::run)
    ///   returns. A run whose assembly succeeds always returns `Ok`; a node that fails
    ///   at run time is reported through the [`RunReport`]'s outcome, not this `Result`.
    pub fn run_to_store(
        self,
        pipeline_name: &str,
        base: impl AsRef<str>,
    ) -> Result<RunReport, RunToStoreError> {
        let base = base.as_ref();
        // Mint the id ONCE and thread it into both the sink's directory and the
        // `RunConfig`, so the eagerly-created store directory and the driver's
        // resolved stream path agree (the driver builds the path from `config.base`
        // and the resolved run id).
        let run_id = mint_run_id();
        let sink = FileSink::create_in_store(base, pipeline_name, &run_id)
            .map_err(RunToStoreError::Store)?;
        let config = RunConfig::new(base).run_id(run_id);
        self.run(pipeline_name, &config, sink, SystemClock::new())
            .map_err(RunToStoreError::Assembly)
    }

    /// [`run_to_store`](Self::run_to_store) targeting the default local store base
    /// ([`DEFAULT_STORE_BASE`](crate::run_store::DEFAULT_STORE_BASE), `./dagr-runs`) —
    /// the whole run in one call with no arguments beyond the pipeline name.
    ///
    /// # Errors
    /// The same as [`run_to_store`](Self::run_to_store).
    pub fn run_to_default_store(self, pipeline_name: &str) -> Result<RunReport, RunToStoreError> {
        self.run_to_store(pipeline_name, crate::run_store::DEFAULT_STORE_BASE)
    }

    /// **Finish** the flow into its immutable [`Pipeline`], consuming the flow — the
    /// live `&Pipeline` the *inspection* verbs (`graph`, `validate`) need without
    /// driving a run.
    ///
    /// [`run`](Self::run) **consumes** the flow and [`RunnableFlow`] is not `Clone`,
    /// so one instance answers at most one verb; the registry ([`run_registry`])
    /// therefore calls the flow's factory **once per verb** — `graph <flow>` builds a
    /// fresh flow, calls this to obtain the pipeline, and emits the graph artifact via
    /// [`graph_verb`](crate::graph::graph_verb); `validate <flow>` builds another and
    /// runs [`validate_verb`](crate::contract::validate_verb) over the pipeline. The
    /// runner factories captured at registration are for *execution* only and are
    /// dropped here; the returned pipeline carries the frozen node/edge structure the
    /// inspection verbs read (and, when the nodes were registered through the
    /// stable-name-aware surface — [`register_source_named`](Self::register_source_named)
    /// / [`register_named`](Self::register_named) — the author-declared stable names
    /// the graph artifact records).
    ///
    /// [`run_registry`]: crate::registry::run_registry
    #[must_use]
    pub fn into_pipeline(self) -> Pipeline {
        self.flow.finish()
    }

    /// Prepare **exactly one attempt** of `node`, without running the flow — the
    /// seam the pod-side `exec-node` verb drives.
    ///
    /// The flow is assembled exactly as [`run`](Self::run) assembles it (same
    /// pipeline, same slot sizing, same runner construction), and then only the
    /// requested node's runner is built — under
    /// [`AttemptDiscipline::ExactlyOnce`], because retry, backoff, and timeout are
    /// the orchestrator's decisions and two retry loops would duplicate an attempt.
    ///
    /// The returned [`PreparedAttempt`] exposes the node's declared input arity and
    /// its upstreams' codecs, so a caller can fill each upstream slot from bytes it
    /// fetched, run the attempt, and read the produced value back out encoded. It
    /// deliberately knows nothing about *where* those bytes came from: this module
    /// gains no storage dependency.
    ///
    /// # Errors
    ///
    /// [`PrepareAttemptError::Assembly`] if the flow does not assemble,
    /// [`PrepareAttemptError::UnknownNode`] if this build's graph has no such node,
    /// and [`PrepareAttemptError::NotRemoteEligible`] if the node or one of its
    /// upstreams was not registered through a `Payload`-bounded registrar — such a
    /// node's value has no codec, so it provably cannot cross a process boundary.
    pub fn prepare_attempt(self, node: &str) -> Result<PreparedAttempt, PrepareAttemptError> {
        self.prepare_attempt_with_discipline(node, AttemptDiscipline::ExactlyOnce)
    }

    /// [`prepare_attempt`](Self::prepare_attempt) under a stated
    /// [discipline](AttemptDiscipline).
    ///
    /// The pod-side verb always wants [`ExactlyOnce`](AttemptDiscipline::ExactlyOnce)
    /// and reaches for the shorter name. This form exists so a test can prepare the
    /// *same* node under the run loop's own [`NodePolicy`](AttemptDiscipline::NodePolicy)
    /// discipline and compare the two — which is how "the pod attempts it once" is
    /// shown to mean something for a node that really does retry locally.
    ///
    /// # Errors
    ///
    /// The same as [`prepare_attempt`](Self::prepare_attempt).
    pub fn prepare_attempt_with_discipline(
        self,
        node: &str,
        discipline: AttemptDiscipline,
    ) -> Result<PreparedAttempt, PrepareAttemptError> {
        let RunnableFlow {
            flow,
            runners,
            timer,
            force_roundtrip: _,
            resources,
        } = self;
        let pipeline = flow.finish();
        let artifact = pipeline.assemble().map_err(PrepareAttemptError::Assembly)?;
        let fingerprint = artifact.fingerprint();

        let mut registry: SlotRegistry = HashMap::new();
        let mut recipes: Vec<(String, NodeId, RunnerFactory, Option<NodeCodec>)> =
            Vec::with_capacity(runners.len());
        for r in runners {
            let consumers = artifact.consumer_count(r.id).unwrap_or(0);
            registry.insert(r.id, (r.make_slot)(consumers));
            recipes.push((r.name, r.id, r.factory, r.codec));
        }

        let Some(target) = pipeline.nodes().find(|n| n.name() == node) else {
            return Err(PrepareAttemptError::UnknownNode {
                node: node.to_string(),
                available: pipeline.nodes().map(|n| n.name().to_string()).collect(),
            });
        };
        let target_id = target.id();
        // Positional order is the declared order of the node's data edges — the same
        // order the typed binding assembled its input tuple from.
        let upstream_ids: Vec<NodeId> = target
            .data_edges()
            .iter()
            .map(dagr_core::binding::DataEdge::upstream)
            .collect();

        let mut inputs = Vec::with_capacity(upstream_ids.len());
        for id in &upstream_ids {
            let (name, _, _, codec) =
                recipes
                    .iter()
                    .find(|(_, rid, _, _)| rid == id)
                    .ok_or_else(|| PrepareAttemptError::UnknownNode {
                        node: format!("<upstream of `{node}`>"),
                        available: Vec::new(),
                    })?;
            let codec = codec
                .ok_or_else(|| PrepareAttemptError::NotRemoteEligible { node: name.clone() })?;
            inputs.push(PreparedInput {
                node: name.clone(),
                id: *id,
                codec,
            });
        }

        let mut target_factory = None;
        let mut output_codec = None;
        for (name, id, factory, codec) in recipes {
            if id == target_id {
                target_factory = Some(factory);
                output_codec = codec;
                debug_assert_eq!(name, node);
            }
        }
        let factory = target_factory.ok_or_else(|| PrepareAttemptError::UnknownNode {
            node: node.to_string(),
            available: Vec::new(),
        })?;
        let output_codec = output_codec.ok_or_else(|| PrepareAttemptError::NotRemoteEligible {
            node: node.to_string(),
        })?;

        let runner = factory(&registry, &timer, discipline);
        Ok(PreparedAttempt {
            node: node.to_string(),
            runner,
            slots: registry,
            inputs,
            output_id: target_id,
            output_codec,
            fingerprint,
            resources,
        })
    }
}

/// One node's attempt, assembled and ready to run exactly once — the pod-side
/// counterpart of the run loop's per-node plumbing.
///
/// It owns the run's slot registry, so filling an upstream slot here is the same act
/// the driver performs when it admits a node whose upstream just succeeded: the
/// node's own runner then reads its inputs through the ordinary deferred read, with
/// no special case anywhere in the attempt path.
pub struct PreparedAttempt {
    node: String,
    runner: Box<dyn NodeRunner>,
    slots: SlotRegistry,
    inputs: Vec<PreparedInput>,
    output_id: NodeId,
    output_codec: NodeCodec,
    fingerprint: dagr_core::assembly::FingerprintSlot,
    resources: Option<ResourceRegistry>,
}

/// One positional input of a prepared attempt: the producing node and its codec.
struct PreparedInput {
    node: String,
    id: NodeId,
    codec: NodeCodec,
}

impl PreparedAttempt {
    /// The node this attempt runs.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The node's **declared** input arity — how many references the attempt must be
    /// given. Zero for a consume-nothing source.
    #[must_use]
    pub fn input_arity(&self) -> usize {
        self.inputs.len()
    }

    /// The producing node of positional input `index`.
    #[must_use]
    pub fn input_producer(&self, index: usize) -> Option<&str> {
        self.inputs.get(index).map(|input| input.node.as_str())
    }

    /// The author-declared stable name of positional input `index`'s value type.
    #[must_use]
    pub fn input_type_name(&self, index: usize) -> Option<&'static str> {
        self.inputs.get(index).map(|input| input.codec.stable_name)
    }

    /// The author-declared stable name of the produced value's type.
    #[must_use]
    pub fn output_type_name(&self) -> &'static str {
        self.output_codec.stable_name
    }

    /// This build's fingerprints — what the shard records so a reader can refuse a
    /// shard from a different program.
    #[must_use]
    pub fn fingerprint(&self) -> dagr_core::assembly::FingerprintSlot {
        self.fingerprint
    }

    /// The resource registry this flow attached, rebuilt by the binary's own
    /// flow-building code in this process.
    #[must_use]
    pub fn resources(&self) -> Option<&ResourceRegistry> {
        self.resources.as_ref()
    }

    /// Decode `bytes` as positional input `index`'s value and fill its producer's
    /// slot, so the attempt reads it exactly as it would in a local run.
    ///
    /// # Errors
    ///
    /// The codec's own [`CodecError`](dagr_core::payload::CodecError) when the bytes
    /// are not a valid encoding of that input's declared type.
    pub fn fill_input(&self, index: usize, bytes: &[u8]) -> Result<(), payload::CodecError> {
        let Some(input) = self.inputs.get(index) else {
            return Ok(());
        };
        (input.codec.fill)(&self.slots, input.id, bytes)
    }

    /// Run the node's **single** attempt through the real caught attempt path,
    /// emitting into `sink`, and return its terminal state.
    pub fn run_once<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        self.runner.run(ctx, sink)
    }

    /// Encode the value the attempt produced, or `None` when it produced none.
    #[must_use]
    pub fn encode_output(&self) -> Option<Vec<u8>> {
        (self.output_codec.encode)(&self.slots, self.output_id)
    }
}

/// Why an attempt could not be prepared.
#[derive(Debug)]
#[non_exhaustive]
pub enum PrepareAttemptError {
    /// The flow does not assemble.
    Assembly(dagr_core::assembly::AssemblyError),
    /// This build's graph has no node by that name — so the invoker and this binary
    /// disagree about what the pipeline is.
    UnknownNode {
        /// The requested name.
        node: String,
        /// The names this build does have.
        available: Vec<String>,
    },
    /// The node, or one of its upstreams, was not registered through a
    /// `Payload`-bounded registrar, so its value has no codec and provably cannot
    /// cross a process boundary.
    NotRemoteEligible {
        /// The node whose value cannot be encoded.
        node: String,
    },
}

impl std::fmt::Display for PrepareAttemptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Assembly(err) => write!(f, "the flow did not assemble: {err}"),
            Self::UnknownNode { node, available } if available.is_empty() => {
                write!(f, "this build's graph has no node `{node}`")
            }
            Self::UnknownNode { node, available } => write!(
                f,
                "this build's graph has no node `{node}` (it has: {})",
                available.join(", ")
            ),
            Self::NotRemoteEligible { node } => write!(
                f,
                "node `{node}` was not registered through a `Payload`-bounded registrar, \
                 so its value has no codec and cannot cross a process boundary — register \
                 it with `register_payload` / `register_source_payload`"
            ),
        }
    }
}

impl std::error::Error for PrepareAttemptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Assembly(err) => Some(err),
            Self::UnknownNode { .. } | Self::NotRemoteEligible { .. } => None,
        }
    }
}

/// The failure modes of [`RunnableFlow::run_to_store`]: the run store could not be
/// opened, or the flow did not assemble.
///
/// [`run`](RunnableFlow::run) collapses these into a single result (a run-store open
/// failure surfaces through the sink, an assembly failure through the returned
/// `Result`). The one-call [`run_to_store`](RunnableFlow::run_to_store) opens the
/// store itself, so it keeps the two distinct — letting a caller map each to its own
/// exit code (store-open → a sink failure, assembly → an assembly failure), exactly
/// as the registry's hand-written run path does.
#[derive(Debug)]
pub enum RunToStoreError {
    /// The run store could not be opened at the given base (an unwritable or
    /// inaccessible directory). Carries the underlying [`std::io::Error`].
    Store(std::io::Error),
    /// The flow did not assemble (a duplicate name, an illegal edge, …). Carries the
    /// underlying [`AssemblyError`](dagr_core::assembly::AssemblyError).
    Assembly(dagr_core::assembly::AssemblyError),
}

impl std::fmt::Display for RunToStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(err) => write!(f, "the run store could not be opened: {err}"),
            Self::Assembly(err) => write!(f, "the flow did not assemble: {err}"),
        }
    }
}

impl std::error::Error for RunToStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(err) => Some(err),
            Self::Assembly(err) => Some(err),
        }
    }
}

/// The shared run-wide residency accounting hook (a fresh ledger — the run-a-Flow
/// path charges no declared residency, matching the demos' `slot_for` helper).
fn ledger() -> Arc<ResidencyLedger> {
    ResidencyLedger::new()
}

/// Build a boxed slot-maker for output type `T`: given the consumer count, mint the
/// node's once-writable [`Slot`] and box it type-erased for the registry.
fn make_slot_boxed<T>(name: &str) -> Box<dyn FnOnce(u32) -> Arc<dyn Any + Send + Sync> + Send>
where
    T: Send + Sync + 'static,
{
    let name = name.to_string();
    Box::new(move |consumers: u32| {
        let slot: Arc<Slot<T>> = Arc::new(Slot::new(
            NodeId::from_name(&name),
            &name,
            consumers,
            false,
            0,
            ledger(),
        ));
        Arc::new(slot) as Arc<dyn Any + Send + Sync>
    })
}

/// Downcast the type-erased output slot registered under `id` back to the concrete
/// `Arc<Slot<T>>`. Infallible by construction (the slot was stored as exactly this
/// type); a miss is a framework defect and panics loudly, naming the node.
fn downcast_slot<T>(registry: &SlotRegistry, id: NodeId, name: &str) -> Arc<Slot<T>>
where
    T: Send + Sync + 'static,
{
    registry
        .get(&id)
        .and_then(|any| any.downcast_ref::<Arc<Slot<T>>>())
        .cloned()
        .unwrap_or_else(|| panic!("run-a-flow: no output slot registered for node `{name}`"))
}

// ===========================================================================
// The generic node runner — the plumbing captured once, for every `Task`.
// ===========================================================================

/// The **one** generic [`NodeRunner`] the run-a-Flow path builds for every node —
/// the code the demos hand-wrote per node, written once and generic over `T: Task`.
///
/// It owns the node's task, its already-read input value (`T::Input`), and its
/// output slot, plus the [`RetryConfig`] derived from the node's policy. On
/// [`run`](NodeRunner::run) it drives the node through the **real** attempt path:
/// a single caught attempt on the driver's own per-attempt context (so scratch,
/// temp-dir, and cancellation are threaded) when the node does not retry, or the
/// real bounded-retry loop otherwise. The emitted records are the genuine framework
/// ones — this reproduces, not re-implements, the hand-wired behaviour.
struct GenericNodeRunner<T: Task> {
    name: String,
    task: Option<T>,
    input: InputSource<T::Input>,
    slot: Arc<Slot<T::Output>>,
    retry_config: RetryConfig,
    /// How this node's declared per-attempt timeout is enforced — decided at
    /// registration from the policy budget and the node's effective class.
    enforcement: TimeoutEnforcement,
    /// The run's injected wait seam (backoff, and an await-bound deadline).
    timer: Arc<dyn AttemptTimer>,
}

/// How a node's declared per-attempt [timeout](NodePolicy::timeout) is enforced —
/// C14's *"timeout semantics differ by class, honestly"* resolved once, at
/// registration, from the policy budget and the node's **effective** execution class
/// (the policy override if set, else the class the task declared).
#[derive(Clone)]
enum TimeoutEnforcement {
    /// No timeout declared: nothing is armed and the attempt path is exactly what it
    /// was before per-attempt timeouts were enforced.
    None,
    /// **Await-bound** work — the one shape Rust can cancel. The attempt future is
    /// raced against a deadline drawn from the run's timer and *dropped* when the
    /// deadline wins, which is true cancellation and releases the permit at once.
    DropTheFuture(Duration),
    /// **Blocking / compute** work — an unkillable synchronous closure. The
    /// framework cannot stop the thread, so the driver arms this node's deadline on
    /// its isolated runtime and marks the attempt `timed-out` there; the runner's
    /// side of the hand-off is this shared [`AttemptFate`], which brackets each
    /// attempt and refuses the abandoned closure's late result.
    MarkUnkillable(Arc<AttemptFate>),
}

impl TimeoutEnforcement {
    /// Resolve the enforcement for a node from its policy and its task's declared
    /// execution class.
    fn for_node<T: Task>(policy: &NodePolicy) -> Self {
        let Some(budget) = policy.timeout_budget() else {
            return Self::None;
        };
        match policy.class_override().unwrap_or(T::EXECUTION_CLASS) {
            ExecutionClass::AwaitBound => Self::DropTheFuture(budget),
            ExecutionClass::Blocking | ExecutionClass::Compute => {
                Self::MarkUnkillable(AttemptFate::new())
            }
        }
    }
}

/// Where a node's input value comes from — [ready](InputSource::Ready) (a source
/// consumes nothing) or [deferred](InputSource::Deferred) (a data node reads its
/// wired upstream slots at attempt time, once the driver has admitted it and its
/// upstreams have succeeded — never at plan-assembly, when the slots are still
/// empty).
enum InputSource<I> {
    /// A ready-to-use value with no slot read (a source's `()`). Wrapped in `Option`
    /// so the single `run` takes it by move.
    Ready(Option<I>),
    /// A deferred read: resolve the node's inputs from the run's slot registry the
    /// first time the runner runs.
    Deferred {
        reader: Option<Box<dyn InputReader<I>>>,
        registry: SlotRegistry,
    },
}

impl<I> InputSource<I> {
    /// Resolve the node's input value, reading its wired upstream slots now if the
    /// read was deferred. Called once, inside the runner's `run()`.
    fn resolve(&mut self) -> I {
        match self {
            InputSource::Ready(v) => v.take().expect("a node runs exactly once"),
            InputSource::Deferred { reader, registry } => reader
                .take()
                .expect("a node runs exactly once")
                .resolve(registry),
        }
    }
}

impl<T> NodeRunner for GenericNodeRunner<T>
where
    T: Task + Send + 'static,
    T::Input: Clone + Send,
    T::Output: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn timeout_fate(&self) -> Option<Arc<AttemptFate>> {
        match &self.enforcement {
            TimeoutEnforcement::MarkUnkillable(fate) => Some(Arc::clone(fate)),
            TimeoutEnforcement::None | TimeoutEnforcement::DropTheFuture(_) => None,
        }
    }

    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("a node runs exactly once");
        // Read the wired inputs NOW (inside run) — the driver only calls this after
        // every upstream succeeded, so the upstream slots are filled. Reading at
        // plan-assembly would hit an empty slot (read-before-fill).
        let inputs = self.input.resolve();
        let slot = Arc::clone(&self.slot);
        let retry_config = self.retry_config;
        let enforcement = self.enforcement.clone();
        let timer = Arc::clone(&self.timer);
        // Adapt the arbitrary-input task to the consume-nothing shape the attempt
        // runner drives: bind the already-read input value, so the single attempt
        // consumes it once and each retry attempt sees a fresh clone of it.
        let bound = BoundTask {
            inner: task,
            input: inputs,
        };
        Box::pin(async move {
            // The backoff wait: the loop computes the jittered exponential delay and
            // awaits THIS future, so the delay it emits is a claim about elapsed
            // time. `dagr-core` still reads no clock — the seam supplies the wait.
            let backoff_timer = |delay: Duration| timer.sleep(delay);
            let retries = retry_config.max_attempts() > 1;
            match enforcement {
                // No declared timeout: exactly the attempt path this node had
                // before, save that its backoff is now really waited.
                TimeoutEnforcement::None => {
                    if retries {
                        // The REAL bounded-retry loop. It mints its own per-attempt
                        // context off the driver's run/pipeline identity.
                        run_with_retries_caught(
                            bound,
                            &name,
                            ctx.run_id().clone(),
                            ctx.pipeline_id().clone(),
                            &slot,
                            sink,
                            &retry_config,
                            &mut NoJitter,
                            backoff_timer,
                        )
                        .await
                        .terminal_state()
                    } else {
                        // A single caught attempt through the REAL runner on the
                        // DRIVER's context (scratch_root / temp_dir / cancellation
                        // threaded).
                        let mut bound = bound;
                        run_attempt_caught(&mut bound, &name, ctx, &slot, sink)
                            .await
                            .terminal_state()
                    }
                }
                // Await-bound: race each attempt against its deadline and drop the
                // future when the deadline wins — true cancellation, so the attempt
                // stops and the permit the driver holds around it releases at the
                // mark.
                TimeoutEnforcement::DropTheFuture(budget) => {
                    if retries {
                        run_with_retries_caught_timed(
                            bound,
                            &name,
                            ctx.run_id().clone(),
                            ctx.pipeline_id().clone(),
                            &slot,
                            sink,
                            &retry_config,
                            &mut NoJitter,
                            backoff_timer,
                            Some(budget),
                        )
                        .await
                        .terminal_state()
                    } else {
                        let mut bound = bound;
                        run_attempt_caught_with_timeout(
                            &mut bound,
                            &name,
                            ctx,
                            &slot,
                            sink,
                            timer.sleep(budget),
                            // The admission permit is held by the driver around the
                            // whole attempt (it releases when this runner returns),
                            // so this attempt carries no permit of its own.
                            (),
                        )
                        .await
                        .terminal_state()
                    }
                }
                // Blocking / compute: the closure cannot be stopped, so the driver's
                // isolated timer marks it and this fate cell bars whatever the
                // abandoned closure computes afterwards. The attempt path itself is
                // unchanged — the guard wraps the task, it does not replace the
                // runner.
                TimeoutEnforcement::MarkUnkillable(fate) => {
                    let guarded = FateGuardedTask {
                        inner: bound,
                        fate: Arc::clone(&fate),
                        slot: Arc::clone(&slot),
                    };
                    let outcome = if retries {
                        run_with_retries_caught(
                            guarded,
                            &name,
                            ctx.run_id().clone(),
                            ctx.pipeline_id().clone(),
                            &slot,
                            sink,
                            &retry_config,
                            &mut NoJitter,
                            backoff_timer,
                        )
                        .await
                    } else {
                        let mut guarded = guarded;
                        run_attempt_caught(&mut guarded, &name, ctx, &slot, sink).await
                    };
                    if fate.is_timed_out() {
                        // The deadline decided this node while the closure ran on:
                        // its terminal state is `timed-out` and was recorded by the
                        // driver at the mark. Reporting it again changes nothing —
                        // the driver refuses a zombie's late report — but reporting
                        // it *honestly* keeps this runner's return value true.
                        TerminalState::TimedOut
                    } else {
                        outcome.terminal_state()
                    }
                }
            }
        })
    }
}

/// A **late-result guard** wrapped around an unkillable node's task: it brackets
/// each attempt against the shared [`AttemptFate`] so that a closure which returns
/// *after* its per-attempt timeout was marked can neither fill the output slot nor
/// count as the node's outcome.
///
/// This is the runner's half of C14's blocking/compute timeout. The framework cannot
/// stop the closure, so the value it eventually produces is routed through the
/// decision's [`LateResultBarrier`](dagr_core::execution::LateResultBarrier) —
/// which refuses it — and the attempt reports a permanent failure, which also stops
/// the retry loop: a retry is deferred until the previous closure returns
/// (exclusivity), and by then the node is already decided, so no further attempt of
/// a marked node runs.
struct FateGuardedTask<T: Task> {
    inner: T,
    fate: Arc<AttemptFate>,
    slot: Arc<Slot<T::Output>>,
}

impl<T> Task for FateGuardedTask<T>
where
    T: Task<Input = ()> + Send,
    T::Output: Send + Sync + 'static,
{
    type Input = ();
    type Output = T::Output;
    const EXECUTION_CLASS: dagr_core::task::ExecutionClass = T::EXECUTION_CLASS;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, dagr_core::TaskError> {
        // A further attempt of this node is live: the deadline may claim it.
        self.fate.attempt_begins();
        let produced = self.inner.run(ctx, ()).await;
        match self.fate.claim_completion() {
            // This attempt returned inside its budget: its result stands.
            None => produced,
            // The deadline already marked this attempt: whatever it produced is
            // discarded through the barrier — a timed-out attempt never fills its
            // slot and never writes scratch.
            Some(barrier) => {
                if let Ok(value) = produced {
                    let filled = barrier.fill_slot(&self.slot, value);
                    debug_assert!(!filled, "the late-result barrier always refuses a fill");
                }
                let wrote_scratch = barrier.write_scratch();
                debug_assert!(
                    !wrote_scratch,
                    "the late-result barrier always refuses a scratch write"
                );
                Err(dagr_core::TaskError::permanent(
                    "the attempt was marked timed-out before it returned; its late result was refused",
                ))
            }
        }
    }
}

/// The **local codec check** wrapped around a payload-bounded node's task: with the
/// operator's toggle on, the value the task produced is encoded and decoded before it
/// reaches the slot.
///
/// This is the whole of `--dagr.force-roundtrip`'s execution side, and it is
/// deliberately a *wrapper*: with the toggle off the guard reads one relaxed atomic
/// and hands the value straight back, so the in-memory fast path performs **no**
/// encode, allocates nothing, and emits nothing. Ordinary registrations never carry
/// the wrapper at all.
///
/// A value that does not survive its own round trip is a **codec defect**, not a
/// transient one, so it fails the attempt permanently (retrying a deterministic
/// encoder buys nothing) with the classified [`CodecError`](dagr_core::CodecError)
/// named in the message and preserved as the error's source.
struct RoundTripTask<T: Task> {
    inner: T,
    /// The flow's shared toggle cell — read per attempt, so the operator's answer may
    /// arrive after the node was registered.
    enabled: Arc<AtomicBool>,
}

impl<T> Task for RoundTripTask<T>
where
    T: Task + Send,
    T::Input: Send,
    T::Output: Payload + Send + Sync + 'static,
{
    type Input = T::Input;
    type Output = T::Output;
    const EXECUTION_CLASS: ExecutionClass = T::EXECUTION_CLASS;

    async fn run(
        &mut self,
        ctx: &RunContext,
        input: Self::Input,
    ) -> Result<T::Output, dagr_core::TaskError> {
        let produced = self.inner.run(ctx, input).await?;
        if !self.enabled.load(Ordering::Relaxed) {
            // The fast path: the value moves on untouched, exactly as it always has.
            return Ok(produced);
        }
        payload::round_trip(&produced).map_err(|err| {
            let detail = err.to_string();
            dagr_core::TaskError::permanent_from(
                format!(
                    "--dagr.force-roundtrip: this node's `{}` output did not survive its own \
                     codec round trip, so it could not have crossed a process boundary either: \
                     {detail}",
                    <T::Output as StableName>::STABLE_NAME
                ),
                err,
            )
        })
    }
}

/// A **consume-nothing adapter** over an arbitrary-input task: it owns the
/// already-read input value and hands the inner task a fresh clone on each attempt.
///
/// The attempt runner ([`run_attempt_caught`]) and the retry loop
/// ([`run_with_retries_caught`]) both drive a `Task<Input = ()>`, running the task
/// by `&mut self` once per attempt. This adapter binds the read input so a retrying
/// node's second attempt sees the same input the first did (each attempt gets a
/// fresh clone — the clone-on-read discipline a retrying node's edge declares), and
/// a single-attempt node consumes it once. It holds the task by value so it is
/// `'static + Send`, satisfying the runner's bounds.
struct BoundTask<T: Task> {
    inner: T,
    input: T::Input,
}

impl<T> Task for BoundTask<T>
where
    T: Task + Send,
    T::Input: Clone + Send,
    T::Output: Send + Sync + 'static,
{
    type Input = ();
    type Output = T::Output;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, dagr_core::TaskError> {
        self.inner.run(ctx, self.input.clone()).await
    }
}

// ===========================================================================
// Input wiring — read a node's declared inputs from the slot registry, generic
// over the `Deps` shape (arity + per-edge receive mode).
// ===========================================================================

/// A captured, type-erased **input reader**: produces the node's `Inputs` value by
/// reading the wired upstream slots out of the run's slot registry.
///
/// Built at registration time from a [`Deps`] value (via [`InputWiring`]) so it
/// knows each upstream's concrete value type, id, and declared receive mode — none
/// of which survive on the type-erased edge the pipeline stores. The reader is what
/// makes the generic runner generic over input arity.
#[doc(hidden)]
pub trait InputReader<Inputs>: Send {
    /// Read every wired input out of `registry` (whose slots are all filled by the
    /// time this node is admitted) and assemble the node's `Inputs` value.
    fn resolve(&self, registry: &SlotRegistry) -> Inputs;
}

/// The seam that turns a concrete [`Deps`] value into a boxed [`InputReader`] for
/// its `Inputs`, implemented for every dep shape the registration surface accepts:
/// the three arity-1 bound-input types (a bare [`Handle`], a `Shared`, a
/// `CloneOnRead`) and the tuple shapes for arities **2..=8** (`MAX_INPUT_ARITY`).
///
/// It reads the edge set the framework already computes ([`Deps::into_edges`], the
/// same `(upstream id, receive mode)` pairs the pipeline binds), so it is generic
/// over each upstream's concrete value type and its declared receive mode with no
/// per-type code at the call site. A registration's `deps` argument yields both the
/// pipeline edges (via `Deps` on the flow) and the runtime input reader (via this
/// trait) from one cloned value. The arity-1 impls deliver the bare value; each
/// tuple impl assembles its positions into the declared tuple in edge order. A
/// dep shape of arity 9+ has no `Deps` impl at all (the sealed tuple impls stop at
/// 8), so it is rejected at the registration site before this seam is consulted.
pub trait InputWiring: Deps {
    /// Build the boxed input reader for this dep shape.
    #[doc(hidden)]
    fn input_reader(self) -> Box<dyn InputReader<Self::Inputs>>;
}

/// One upstream edge's captured read recipe: the producer's node [id](NodeId) and
/// this edge's declared receive mode. The value type is carried by the monomorphized
/// reader that owns this, so reading is a typed downcast with no runtime type check.
#[derive(Clone, Copy)]
struct EdgeRead {
    upstream: NodeId,
    mode: ReceiveMode,
}

impl EdgeRead {
    /// Read this edge's value out of the registry as a concrete `V`. Honours the
    /// declared receive mode: clone-on-read takes a fresh clone; shared/owned read
    /// the shared value. Both resolve to a cloned `V` — the generic runner binds
    /// the value by move into the attempt, and the retry loop re-clones per attempt.
    fn read<V>(&self, registry: &SlotRegistry) -> V
    where
        V: Clone + Send + Sync + 'static,
    {
        let slot = downcast_slot::<V>(registry, self.upstream, "<upstream>");
        let r: SlotRef<V> = match self.mode {
            ReceiveMode::CloneOnRead => slot.clone_on_read_ref(),
            ReceiveMode::Shared | ReceiveMode::Owned => slot.shared_ref(),
        };
        // The upstream succeeded before this node was admitted, so its slot is
        // filled. A cloned value is what the generic runner binds per attempt.
        r.read().as_ref().clone()
    }
}

/// The reader for a **single** bound input (arity 1): delivers the bare value `V`.
struct SingleReader<V> {
    edge: EdgeRead,
    _ty: std::marker::PhantomData<fn() -> V>,
}

impl<V> InputReader<V> for SingleReader<V>
where
    V: Clone + Send + Sync + 'static,
{
    fn resolve(&self, registry: &SlotRegistry) -> V {
        self.edge.read::<V>(registry)
    }
}

/// Build the arity-1 [`SingleReader`] from a single bound input's edge — the body
/// shared by the three concrete arity-1 [`InputWiring`] impls below.
fn single_input_reader<D>(dep: D) -> Box<dyn InputReader<D::Inputs>>
where
    D: Deps,
    D::Inputs: Clone + Send + Sync + 'static,
{
    let mut edges = dep.into_edges();
    debug_assert_eq!(
        edges.len(),
        1,
        "a single bound input yields exactly one edge"
    );
    let (upstream, mode) = edges.pop().expect("exactly one edge");
    Box::new(SingleReader::<D::Inputs> {
        edge: EdgeRead { upstream, mode },
        _ty: std::marker::PhantomData,
    })
}

// Arity 1: a single bound input delivers the bare value `V` (never a `(V,)`),
// mirroring `Deps`'s arity-1 rule. `InputWiring` is implemented on the THREE
// concrete arity-1 dep types — a bare `Handle<T>`, a `Shared<T>`, and a
// `CloneOnRead<T>` — rather than as a `BoundInput`-keyed blanket. A blanket over
// the (foreign, sealed) `BoundInput` trait would be seen by the compiler as
// *potentially* overlapping the tuple impls below (it cannot see the seal from
// this crate, so it must assume `dagr_core` might one day impl `BoundInput` for a
// tuple), which is a coherence error. Three concrete impls plus the tuple impls
// are pairwise disjoint by construction, so there is no overlap. The single
// `(id, mode)` pair comes from the framework's own `Deps::into_edges`.
impl<T> InputWiring for Handle<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn input_reader(self) -> Box<dyn InputReader<Self::Inputs>> {
        single_input_reader(self)
    }
}
impl<T> InputWiring for dagr_core::binding::Shared<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn input_reader(self) -> Box<dyn InputReader<Self::Inputs>> {
        single_input_reader(self)
    }
}
impl<T> InputWiring for dagr_core::binding::CloneOnRead<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn input_reader(self) -> Box<dyn InputReader<Self::Inputs>> {
        single_input_reader(self)
    }
}

/// Generate the tuple [`InputReader`] + [`InputWiring`] impls for arity `N`
/// (2..=8), mirroring [`SingleReader`] and the arity-1 blanket impl above.
///
/// Each generated reader owns one [`EdgeRead`] per position (in
/// [`Deps::into_edges`] order, position `0..N`), and on `resolve` downcasts each
/// upstream slot to that position's concrete element type, honouring that edge's
/// declared receive mode, and assembles the positional tuple `(V0, V1, …, V{N-1})`.
/// The read is deferred to attempt time exactly as the single-input reader is: it
/// runs only when the generic runner calls `resolve` inside `run()`, after the
/// driver has admitted the node and its upstreams have succeeded (so every slot is
/// filled). Nothing here is per-type: it is generic over each position's value
/// type and its declared [`ReceiveMode`].
macro_rules! tuple_reader {
    ($reader:ident, $($ty:ident => $idx:tt),+) => {
        /// A tuple reader: one [`EdgeRead`] per bound position, assembled into the
        /// declared tuple in [`Deps::into_edges`] order.
        struct $reader<$($ty),+> {
            edges: [EdgeRead; count_idents!($($ty),+)],
            _ty: std::marker::PhantomData<fn() -> ($($ty,)+)>,
        }

        impl<$($ty),+> InputReader<($($ty,)+)> for $reader<$($ty),+>
        where
            $($ty: Clone + Send + Sync + 'static),+
        {
            fn resolve(&self, registry: &SlotRegistry) -> ($($ty,)+) {
                // Read each position through its own edge (declared receive mode
                // honoured) into its concrete element type, in declared order.
                ($(self.edges[$idx].read::<$ty>(registry),)+)
            }
        }

        // The tuple `Deps` shape for this arity — `($ty0, $ty1, …)` of `BoundInput`s
        // — yields an `InputWiring` whose reader is this arity's `$reader`. Disjoint
        // from the arity-1 `BoundInput` blanket (a tuple is not a `BoundInput`), so
        // no overlap. `Deps::Inputs` for this shape is `($ty::Value, …)`, so the
        // reader's element types are exactly the tuple's value types.
        impl<$($ty: BoundInput),+> InputWiring for ($($ty,)+)
        where
            $(<$ty as BoundInput>::Value: Clone + Send + Sync + 'static),+
        {
            fn input_reader(self) -> Box<dyn InputReader<Self::Inputs>> {
                let edges = Deps::into_edges(self);
                debug_assert_eq!(
                    edges.len(),
                    count_idents!($($ty),+),
                    "a {}-tuple `Deps` yields exactly that many edges",
                    count_idents!($($ty),+),
                );
                // `Deps::into_edges` returns the positional `(id, mode)` pairs in
                // input order; index them into the fixed-size edge array.
                let edges: [EdgeRead; count_idents!($($ty),+)] = [
                    $(EdgeRead { upstream: edges[$idx].0, mode: edges[$idx].1 }),+
                ];
                Box::new($reader::<$(<$ty as BoundInput>::Value),+> {
                    edges,
                    _ty: std::marker::PhantomData,
                })
            }
        }
    };
}

/// Count the identifiers passed to it — the arity of a `tuple_reader!` expansion,
/// used to size the fixed edge array.
macro_rules! count_idents {
    ($($ty:ident),+) => { <[()]>::len(&[$(count_idents!(@one $ty)),+]) };
    (@one $ty:ident) => { () };
}

// Arities 2..=8: tuple inputs, mirroring the arity-1 reader. The 8-arity ceiling
// matches `MAX_INPUT_ARITY` — a 9+-tuple `Deps` has no impl (the sealed `Deps`
// tuple impls stop at 8) and never reaches here, so the `on_unimplemented`
// diagnostic fires at the registration site before this seam is consulted.
tuple_reader!(TupleReader2, V0 => 0, V1 => 1);
tuple_reader!(TupleReader3, V0 => 0, V1 => 1, V2 => 2);
tuple_reader!(TupleReader4, V0 => 0, V1 => 1, V2 => 2, V3 => 3);
tuple_reader!(TupleReader5, V0 => 0, V1 => 1, V2 => 2, V3 => 3, V4 => 4);
tuple_reader!(TupleReader6, V0 => 0, V1 => 1, V2 => 2, V3 => 3, V4 => 4, V5 => 5);
tuple_reader!(TupleReader7, V0 => 0, V1 => 1, V2 => 2, V3 => 3, V4 => 4, V5 => 5, V6 => 6);
tuple_reader!(TupleReader8, V0 => 0, V1 => 1, V2 => 2, V3 => 3, V4 => 4, V5 => 5, V6 => 6, V7 => 7);
