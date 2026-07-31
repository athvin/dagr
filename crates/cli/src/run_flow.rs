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
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_core::assembly::NodePolicy;
use dagr_core::binding::{BoundInput, Deps, ReceiveMode};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{
    AttemptEventSink, NoJitter, RetryConfig, run_attempt_caught, run_attempt_caught_with_timeout,
    run_with_retries_caught, run_with_retries_caught_timed,
};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::{Handle, NodeId};
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

/// A **registration-time runner factory**: given the run's slot registry and the
/// run's [timer](AttemptTimer), build this node's type-erased [`NodeRunner`].
/// Captured where the node's concrete `Task` + input/output types are known, so it
/// can wire the typed slots the driver only ever sees erased.
type RunnerFactory =
    Box<dyn FnOnce(&SlotRegistry, &Arc<dyn AttemptTimer>) -> Box<dyn NodeRunner> + Send>;

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
        }
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
        let name = name.into();
        // The stable-name-aware flow registrar captures `T`'s author-declared stable
        // names into the pipeline node, so the built pipeline is graph-emittable.
        let handle =
            self.flow
                .register_source_named::<T>(&name, &task, None::<String>, NodePolicy::new());
        self.push_source_runner(&name, task, handle);
        handle
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
        let policy = NodePolicy::new();
        let retry_config = policy.retry_config();
        let enforcement = TimeoutEnforcement::for_node::<T>(&policy);
        let node_name = name.to_string();
        let factory: RunnerFactory = Box::new(
            move |registry: &SlotRegistry, timer: &Arc<dyn AttemptTimer>| {
                let slot = downcast_slot::<T::Output>(registry, handle.id(), &node_name);
                Box::new(GenericNodeRunner {
                    name: node_name,
                    task: Some(task),
                    // A source consumes nothing: its input is ready without any slot read.
                    input: InputSource::Ready(Some(())),
                    slot,
                    retry_config,
                    enforcement,
                    timer: Arc::clone(timer),
                }) as Box<dyn NodeRunner>
            },
        );
        self.runners.push(RegisteredRunner {
            name: name.to_string(),
            id: handle.id(),
            make_slot: make_slot_boxed::<T::Output>(name),
            factory,
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
        self.push_data_runner::<T, D>(&name, task, deps, policy, handle);
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
        let name = name.into();
        let policy = NodePolicy::new();
        // The stable-name-aware flow registrar captures `T`/`T::Input`/`T::Output`'s
        // author-declared stable names into the pipeline node (so the built pipeline
        // is graph-emittable) while binding the same edges `register` does.
        let handle =
            self.flow
                .register_named::<T, D>(&name, &task, deps.clone(), None::<String>, policy);
        self.push_data_runner::<T, D>(&name, task, deps, policy, handle);
        handle
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
    ) where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        let reader = deps.input_reader();
        let retry_config = policy.retry_config();
        // The node's declared per-attempt timeout, resolved against its effective
        // execution class — the class decides *which* half of C14's honest timeout
        // semantics applies (drop the future, or mark the unkillable closure).
        let enforcement = TimeoutEnforcement::for_node::<T>(&policy);
        let node_name = name.to_string();
        let factory: RunnerFactory = Box::new(
            move |registry: &SlotRegistry, timer: &Arc<dyn AttemptTimer>| {
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
                    enforcement,
                    timer: Arc::clone(timer),
                }) as Box<dyn NodeRunner>
            },
        );
        self.runners.push(RegisteredRunner {
            name: name.to_string(),
            id: handle.id(),
            make_slot: make_slot_boxed::<T::Output>(name),
            factory,
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
            node_runners.insert(name, factory(&registry, &timer));
        }

        let plan = crate::driver::RunPlan::new(pipeline, node_runners);
        let report = drive(config, pipeline_name, Ok(plan), &[], sink, clock);
        Ok(RunReport {
            inner: report,
            slots: registry,
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
