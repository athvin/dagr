//! The **run-a-Flow** convenience seam (operator-approved 2026-07-24; ADR 081) —
//! a public one-call path that runs a [`Flow`] of real [`Task`]s to a completed
//! run **without** the author hand-writing any scheduling or permit plumbing.
//!
//! # Why this module exists (arch.md C1)
//!
//! arch.md C1 says a task author writes **tasks + a flow** and *never* writes the
//! scheduling/permit plumbing. Yet until now every milestone demo hand-wrote
//! ~150 lines of type-erased [`NodeRunner`] impls per
//! pipeline — a `Pin<Box<dyn Future<Output = TerminalState> + Send>>` per node
//! that reads input [`SlotRef`]s, calls `run_attempt` /
//! `run_with_retries_caught`, and
//! fills the output [`Slot`]. That is exactly the plumbing C1 says the author must
//! not write. [`RunnableFlow`] captures that plumbing **generically, once**, at
//! registration time (where the concrete `Task` + `Input`/`Output` types are
//! known), so the author registers real tasks and calls [`run`](RunnableFlow::run).
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
//! (implemented for every [`Deps`] shape), so a three-node chain and a
//! twelve-node fan-in are built by the identical code.
//!
//! # What stays intact
//!
//! This path is **purely additive**. The existing
//! [`NodeRunner`] / [`RunPlan::new`](crate::driver::RunPlan)
//! surface is untouched — the milestone demos and the `full_pipeline` fake harness
//! still hand-write runners and still compile. The generic runners this module
//! builds are ordinary `NodeRunner`s driven by the real
//! [`drive`] loop, so the T63 `scratch_root` wiring and the
//! resume seam apply to them unchanged: a single-attempt node reaches its real
//! per-node durable scratch namespace through the driver's per-attempt context.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_core::assembly::NodePolicy;
use dagr_core::binding::{Deps, ReceiveMode};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{
    run_attempt_caught, run_with_retries_caught, AttemptEventSink, NoJitter, RetryConfig,
};
use dagr_core::flow::Flow;
use dagr_core::handle::{Handle, NodeId};
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;

use crate::driver::{drive, NodeRunner, RunConfig};

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

/// A **registration-time runner factory**: given the run's slot registry,
/// build this node's type-erased [`NodeRunner`]. Captured where the node's
/// concrete `Task` + input/output types are known, so it can wire the typed slots
/// the driver only ever sees erased.
type RunnerFactory = Box<dyn FnOnce(&SlotRegistry) -> Box<dyn NodeRunner> + Send>;

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
}

/// The result of a one-call [`RunnableFlow::run`]: the driver's overall
/// [outcome](RunOutcome), the per-node terminal states, and typed read-back of any
/// node's produced value by its [`Handle`].
///
/// This wraps the driver's [`driver::RunReport`](crate::driver::RunReport) and
/// additionally retains the run's output slots, so a caller can read a node's value
/// after the run through [`output`](RunReport::output) — the value the M1 demo
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
/// [`NodeRunner`] (arch.md C1; ADR 081).
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
#[derive(Default)]
pub struct RunnableFlow {
    flow: Flow,
    runners: Vec<RegisteredRunner>,
}

impl RunnableFlow {
    /// Begin a fresh runnable flow with no nodes registered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            flow: Flow::new(),
            runners: Vec::new(),
        }
    }

    /// Register a **source** node (one whose task consumes nothing) under `name`,
    /// returning its output [`Handle`]. Runs a single attempt through the real
    /// caught attempt runner on the driver's per-attempt context (so its durable
    /// scratch namespace is reachable — T63).
    #[must_use]
    pub fn register_source<T>(&mut self, name: impl Into<String>, task: T) -> Handle<T::Output>
    where
        T: Task<Input = ()> + Send + 'static,
        T::Output: Send + Sync + 'static,
    {
        let name = name.into();
        let handle = self.flow.register_source::<T>(&name, &task);
        let retry_config = NodePolicy::new().retry_config();
        let node_name = name.clone();
        let factory: RunnerFactory = Box::new(move |registry: &SlotRegistry| {
            let slot = downcast_slot::<T::Output>(registry, handle.id(), &node_name);
            Box::new(GenericNodeRunner {
                name: node_name,
                task: Some(task),
                // A source consumes nothing: its input is ready without any slot read.
                input: InputSource::Ready(Some(())),
                slot,
                retry_config,
            }) as Box<dyn NodeRunner>
        });
        self.runners.push(RegisteredRunner {
            name: name.clone(),
            id: handle.id(),
            make_slot: make_slot_boxed::<T::Output>(&name),
            factory,
        });
        handle
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
        let reader = deps.input_reader();
        let retry_config = policy.retry_config();
        let node_name = name.clone();
        let factory: RunnerFactory = Box::new(move |registry: &SlotRegistry| {
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
                retry_config,
            }) as Box<dyn NodeRunner>
        });
        self.runners.push(RegisteredRunner {
            name: name.clone(),
            id: handle.id(),
            make_slot: make_slot_boxed::<T::Output>(&name),
            factory,
        });
        handle
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
        let RunnableFlow { flow, runners } = self;
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
            node_runners.insert(name, factory(&registry));
        }

        let plan = crate::driver::RunPlan::new(pipeline, node_runners);
        let report = drive(config, pipeline_name, Ok(plan), &[], sink, clock);
        Ok(RunReport {
            inner: report,
            slots: registry,
        })
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
/// temp-dir, and cancellation are threaded — T63) when the node does not retry, or
/// the real bounded-retry loop otherwise. The emitted C14/C19 records are the
/// genuine framework ones — this reproduces, not re-implements, the hand-wired
/// behaviour.
struct GenericNodeRunner<T: Task> {
    name: String,
    task: Option<T>,
    input: InputSource<T::Input>,
    slot: Arc<Slot<T::Output>>,
    retry_config: RetryConfig,
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
        // Adapt the arbitrary-input task to the consume-nothing shape the attempt
        // runner drives: bind the already-read input value, so the single attempt
        // consumes it once and each retry attempt sees a fresh clone of it.
        let bound = BoundTask {
            inner: task,
            input: inputs,
        };
        Box::pin(async move {
            if retry_config.max_attempts() > 1 {
                // The REAL bounded-retry loop (T22/T23). It mints its own per-attempt
                // context off the driver's run/pipeline identity; the backoff timer
                // resolves immediately (attempt-counter-driven, no wall-clock sleep).
                run_with_retries_caught(
                    bound,
                    &name,
                    ctx.run_id().clone(),
                    ctx.pipeline_id().clone(),
                    &slot,
                    sink,
                    &retry_config,
                    &mut NoJitter,
                    |_delay: Duration| async {},
                )
                .await
                .terminal_state()
            } else {
                // A single caught attempt through the REAL runner on the DRIVER's
                // context (scratch_root / temp_dir / cancellation threaded — T63).
                let mut bound = bound;
                run_attempt_caught(&mut bound, &name, ctx, &slot, sink)
                    .await
                    .terminal_state()
            }
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
/// its `Inputs` — a blanket impl over every [`Deps`] whose `Inputs` is a single
/// cloneable value (the shape the M1 chain and every one-input map uses).
///
/// It reads the edge set the framework already computes ([`Deps::into_edges`], the
/// same `(upstream id, receive mode)` pairs the pipeline binds), so it is generic
/// over the upstream's concrete value type and its declared receive mode with no
/// per-type code at the call site. A registration's `deps` argument yields both the
/// pipeline edges (via `Deps` on the flow) and the runtime input reader (via this
/// trait) from one cloned value. Multi-input tuple arities extend this the same way
/// (one small `impl` per arity to assemble the tuple); the current registration
/// surface exercises the single-input shape.
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

// Arity 1: a single bound input (`Handle`/`Shared`/`CloneOnRead`) delivers the bare
// value `V` (never a `(V,)`), mirroring `Deps`'s arity-1 rule. The single `(id,
// mode)` pair comes from the framework's own `Deps::into_edges`, so this works for
// every value type with no per-type code.
impl<D> InputWiring for D
where
    D: Deps,
    D::Inputs: Clone + Send + Sync + 'static,
{
    fn input_reader(self) -> Box<dyn InputReader<Self::Inputs>> {
        let mut edges = self.into_edges();
        assert_eq!(
            edges.len(),
            1,
            "run-a-flow: register/register_with currently support single-input nodes; \
             a multi-input node needs the per-arity tuple reader"
        );
        let (upstream, mode) = edges.pop().expect("exactly one edge");
        Box::new(SingleReader::<D::Inputs> {
            edge: EdgeRead { upstream, mode },
            _ty: std::marker::PhantomData,
        })
    }
}
