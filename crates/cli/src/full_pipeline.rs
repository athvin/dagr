//! The C28 **full-pipeline fakes harness** — the third and last of C28's three
//! testing levels (arch.md `### C28 · Testing surface`; ticket T62).
//!
//! This is a **shipped** testing utility: downstream test code assembles a small
//! flow of **fake** tasks (declarative — no real work), injects fake resources,
//! and drives the whole flow through the **real** T24 run-loop driver
//! ([`crate::driver::drive`]) against a tiny fixture, deterministically, with **no
//! live network, no database, and no hand-rolled scheduler**. The point is that
//! **fakes run through real orchestration**: readiness (C11), admission (C12),
//! execution-class dispatch (C13), failure propagation and trigger-rule evaluation
//! (C15), and cancellation (C16) are the framework's, reproduced verbatim by the
//! harness rather than computed by the test.
//!
//! It ships inside the library — behind the default-on `test-kit` feature — so
//! **no pipeline ever writes its own full-pipeline harness** (arch.md C28
//! acceptance). It is the sibling of the single-task kit
//! ([`dagr_core::test_kit::SingleTaskTest`], T60): that kit exercises **one** task
//! with a hand-built context; this harness runs a **whole flow** through the real
//! driver. It does **not** reimplement the driver — it composes the real
//! [`crate::driver::drive`] entry point, the real
//! [`dagr_core::flow`] authoring API, and the real
//! [`dagr_artifact::fold`] run-artifact fold.
//!
//! # What you build, what you capture
//!
//! Build a test with [`FullPipelineTest::new`], add fake nodes (each with a name,
//! its upstream data dependencies, and a scripted [`Outcome`]), register fake
//! resources, set the run-level knobs a test cares about (failure mode,
//! parameters, data interval), then [`run`](FullPipelineTest::run) it and read the
//! [`HarnessRun`]:
//!
//! - **build** — [`source`](FullPipelineTest::source) (no upstreams),
//!   [`node`](FullPipelineTest::node) (data-dependent, up to a two-input fan-in),
//!   [`contingency`](FullPipelineTest::contingency) (a consume-nothing node
//!   attached by ordering edges with a non-default trigger rule — the notify /
//!   cleanup pattern C15's non-default rules exist for);
//!   [`register_fake`](FullPipelineTest::register_fake) (a fake resource retrieved
//!   by type, C9); [`stop_on_first_failure`](FullPipelineTest::stop_on_first_failure),
//!   [`parameter`](FullPipelineTest::parameter),
//!   [`data_interval`](FullPipelineTest::data_interval),
//!   [`run_id`](FullPipelineTest::run_id).
//! - **capture** — the overall [outcome](HarnessRun::overall_outcome), each node's
//!   [terminal state](HarnessRun::terminal_state), the raw
//!   [event stream](HarnessRun::event_stream) (with per-node/per-run query
//!   helpers), the folded [run artifact](HarnessRun::artifact), whether C9
//!   [bootstrap resource validation passed](HarnessRun::bootstrap_resource_validation_passed),
//!   and the normalized [interpretive artifact](HarnessRun::interpretive_artifact_json)
//!   for the T65 interpretive-determinism replay.
//!
//! # Scripted outcomes — the interpretive-determinism replay surface (T65)
//!
//! Each node's outcome is **scripted** ([`Outcome`]): succeed (optionally
//! producing a value, requiring a resource, or after a real `.await`), fail
//! permanently, fail-then-succeed (retry-eligibly, keyed off the C8 attempt
//! number — never timing or randomness), or deliberately skip. The scripted result
//! is driven through the **real** attempt runner and the framework's propagation
//! logic, so a downstream user tests **their** pipeline's structural behaviour, not
//! the test's. Given the same scripts, parameters, and data interval, repeated runs
//! produce identical terminal states, identical propagation decisions, and
//! byte-identical [interpretive artifact content](HarnessRun::interpretive_artifact_json)
//! (volatile header/timing fields excluded) — the deterministic replay surface T65
//! drives (system criterion 4(b)).
//!
//! # Determinism and isolation
//!
//! The harness reads **no** wall clock: it drives the run with a hand-stepped
//! monotonic clock and an in-memory event sink, and its retry backoff resolves
//! immediately (no sleep). The run store base is a **private per-run temp
//! directory** (the shared-`/tmp` flake class has bitten CI), created fresh and
//! removed at the end of each run, so two harness runs — even concurrent ones —
//! never collide. So a full-pipeline fake run is reproducible run to run and
//! machine to machine, and completes in well under a second (arch.md C28's
//! completes-in-seconds budget).
//!
//! # A note on the interim retry surface
//!
//! A **retrying** fake ([`Outcome::fail_then_succeed`]) is driven through the real
//! bounded-retry loop ([`dagr_core::execution::run_with_retries_caught`]), which —
//! per its own rustdoc — mints a fresh per-attempt context off run/pipeline
//! identity only (the C5-policy/context fold is a later ticket's). A retrying node
//! therefore does not receive injected resources through that loop; a
//! resource-consuming fake is a single-attempt [`Outcome::succeed`]. The two
//! scenarios are distinct in the T62 Test plan, so this composes cleanly; it is a
//! known boundary of the interim retry surface, not a gap in the real driver.
//!
//! # Example
//!
//! ```
//! use dagr_cli::full_pipeline::{FullPipelineTest, Outcome};
//! use dagr_core::context::TerminalState;
//!
//! // a → b, a → c, (b, c) → d : a chain with a fan-in, all fakes, all succeed.
//! let run = FullPipelineTest::new("example")
//!     .source("a", Outcome::succeed())
//!     .node("b", &["a"], Outcome::succeed())
//!     .node("c", &["a"], Outcome::succeed())
//!     .node("d", &["b", "c"], Outcome::succeed())
//!     .run();
//!
//! assert_eq!(run.overall_outcome(), "succeeded");
//! assert_eq!(run.terminal_state("d"), Some(TerminalState::Succeeded));
//! ```

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_artifact::fold::{fold_stream, RunArtifact};
use dagr_core::assembly::NodePolicy;
use dagr_core::binding::TriggerRule;
use dagr_core::context::{ResourceRegistry, ResourceRequirements, RunContext, TerminalState};
use dagr_core::execution::{
    run_attempt_caught, run_with_retries_caught, AttemptEventSink, Backoff, NoJitter, RetryConfig,
};
use dagr_core::flow::{FailureMode, Flow, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;
use dagr_core::TaskError;

use crate::driver::{drive, NodeRunner, RunConfig, RunPlan};

/// A run's type-erased node runners, keyed by node name — the map the driver
/// consumes.
type RunnerMap = BTreeMap<String, Box<dyn NodeRunner>>;

/// The assembled artifacts one harness run drives: the pipeline, its node runners,
/// and the run-level ordering-upstream map (for consume-nothing contingencies).
struct BuiltPlan {
    pipeline: Pipeline,
    runners: RunnerMap,
    ordering: BTreeMap<String, Vec<String>>,
}

// ===========================================================================
// Scripted outcomes
// ===========================================================================

/// The **script** for one fake node's outcome (arch.md C28; T62). A fake produces
/// this deterministically, driven through the real attempt runner and the
/// framework's propagation logic — so a test asserts on the framework's decisions,
/// not the test's.
///
/// Construct with the outcome constructors ([`succeed`](Outcome::succeed),
/// [`fail_permanent`](Outcome::fail_permanent),
/// [`fail_then_succeed`](Outcome::fail_then_succeed), [`skip`](Outcome::skip), and
/// their variants), then optionally chain [`requires`](Outcome::requires) to
/// declare a C9 resource requirement the node's context must satisfy at bootstrap.
#[derive(Clone)]
pub struct Outcome {
    script: Script,
    /// The C9 resource requirements this node declares (validated at bootstrap).
    requirements: ResourceRequirements,
    /// A by-type probe asserting the injected registry holds the required fake at
    /// run time (the "receives the fake by type" scenario) — a closure capturing the
    /// concrete resource type, so the lookup is the real type-keyed
    /// `get::<R>()`. `None` for a node that reads no resource.
    resource_probe: Option<ResourceProbe>,
    /// The C12 declared working-memory cost this node demands of the admission
    /// pool (default 0). Pinning a capacity + costing nodes is how a test serializes
    /// admission deterministically (the pending-cancellation scenario).
    working_memory: u64,
}

/// A type-erased "is the fake of type `R` reachable by type?" probe (the C9 no-
/// string-lookup path), captured at declaration time.
type ResourceProbe = Arc<dyn Fn(&ResourceRegistry) -> bool + Send + Sync>;

/// The kind of scripted behaviour a fake node performs.
#[derive(Clone, Copy)]
enum Script {
    /// Succeed on the first attempt, producing the fake value.
    Succeed,
    /// Succeed after a real `.await` yield (proves the provided runtime drives it).
    SucceedAfterYield,
    /// Fail permanently (not retry-eligible) on every attempt.
    FailPermanent,
    /// Fail retry-eligibly for the first `n` attempts, then succeed.
    FailThenSucceed(u32),
    /// Deliberately skip (a self-originated skip).
    Skip,
    /// Cooperatively hold (occupying its pool cost) until the run is cancelled,
    /// then return `succeeded` — the "be slow / be cancelled" fake. Bounded so a
    /// regression that never cancels cannot hang; it reads only its cancellation
    /// signal, never a wall clock.
    HoldUntilCancelled,
}

impl Outcome {
    fn with_script(script: Script) -> Self {
        Self {
            script,
            requirements: ResourceRequirements::new(),
            resource_probe: None,
            working_memory: 0,
        }
    }

    /// The node **succeeds** on its first attempt, producing the fake value.
    #[must_use]
    pub fn succeed() -> Self {
        Self::with_script(Script::Succeed)
    }

    /// The node **succeeds after a real `.await`** — proof that an await-bound fake
    /// runs on the runtime the harness provides, with no externally started async
    /// runtime.
    #[must_use]
    pub fn succeed_after_yield() -> Self {
        Self::with_script(Script::SucceedAfterYield)
    }

    /// The node **succeeds iff the injected fake resource of type `R` is reachable
    /// by type** at run time — the "the task receives the fake, no task edit"
    /// scenario. Also declares the C9 requirement on `R` (so bootstrap validates
    /// it) and probes for it in the attempt.
    #[must_use]
    pub fn succeed_if_resource<R: Any + Send + Sync>() -> Self {
        let mut o = Self::with_script(Script::Succeed);
        o.resource_probe = Some(Arc::new(|registry: &ResourceRegistry| {
            registry.get::<R>().is_some()
        }));
        o.requirements = o.requirements.require::<R>();
        o
    }

    /// The node **fails permanently** — a non-retry-eligible failure on every
    /// attempt (a genuine run failure the framework propagates).
    #[must_use]
    pub fn fail_permanent() -> Self {
        Self::with_script(Script::FailPermanent)
    }

    /// The node **fails retry-eligibly for its first `retries` attempts, then
    /// succeeds** — keyed off the C8 attempt number, never timing or randomness. Its
    /// retry budget is granted automatically (max attempts = `retries + 1`).
    #[must_use]
    pub fn fail_then_succeed(retries: u32) -> Self {
        Self::with_script(Script::FailThenSucceed(retries))
    }

    /// The node **deliberately skips** — a self-originated skip that propagates as
    /// `upstream-skipped` to default-rule downstreams (C15).
    #[must_use]
    pub fn skip() -> Self {
        Self::with_script(Script::Skip)
    }

    /// The node **cooperatively holds** (occupying its declared pool cost) until the
    /// run is cancelled, then returns `succeeded` — the "be slow / be cancelled"
    /// fake. Combined with a pinned [capacity](FullPipelineTest::capacity_memory) and
    /// a [declared cost](Self::working_memory), it serializes admission so a test can
    /// keep an unrelated node **provably pending** across a stop-on-first-failure
    /// window (the deterministic pending-cancellation scenario). It reads only its
    /// cancellation signal — no wall clock, no sleep — and is bounded so a
    /// non-cancelling regression cannot hang.
    #[must_use]
    pub fn hold_until_cancelled() -> Self {
        Self::with_script(Script::HoldUntilCancelled)
    }

    /// Declare this node's C12 **working-memory cost** (bytes) — the per-pool demand
    /// the admission controller acquires against (arch.md C12). Pair it with
    /// [`FullPipelineTest::capacity_memory`] to serialize admission deterministically.
    #[must_use]
    pub fn working_memory(mut self, bytes: u64) -> Self {
        self.working_memory = bytes;
        self
    }

    /// Declare that this node **requires** resource type `R` (C9). A flow whose
    /// declared requirements are all satisfied by registered fakes passes bootstrap
    /// resource validation; a missing one is a bootstrap failure.
    #[must_use]
    pub fn requires<R: Any>(mut self) -> Self {
        self.requirements = self.requirements.require::<R>();
        self
    }

    /// The retry budget this outcome needs: `retries + 1` total attempts for a
    /// fail-then-succeed script, else a single attempt.
    fn max_attempts(&self) -> u32 {
        match self.script {
            Script::FailThenSucceed(retries) => retries + 1,
            _ => 1,
        }
    }
}

// ===========================================================================
// Fake resources
// ===========================================================================

/// A **fake resource** to inject into the harness's C9 registry (arch.md C9;
/// T62). A node retrieves it by type through `ctx.resources().get::<R>()` with
/// **no change to the task's own code** versus production — the substitution is
/// purely through the registry.
///
/// Wrap any `Send + Sync + 'static` fake and register it with
/// [`FullPipelineTest::register_fake`].
pub struct FakeResource {
    register: Box<dyn FnOnce(&mut ResourceRegistryStager)>,
}

impl FakeResource {
    /// Wrap `resource` as a fake to inject by its concrete type. Two fakes of the
    /// **same** underlying type are distinguished by newtype wrappers (the C9
    /// no-string-lookup pattern), exactly as in production.
    #[must_use]
    pub fn new<R: Any + Send + Sync + 'static>(resource: R) -> Self {
        Self {
            register: Box::new(move |stager: &mut ResourceRegistryStager| {
                stager.register(resource);
            }),
        }
    }
}

/// An accumulator for the fakes a harness run injects, built into the immutable C9
/// [`ResourceRegistry`] once every fake is staged. Internal — a caller reaches it
/// only through [`FakeResource`].
#[doc(hidden)]
pub struct ResourceRegistryStager {
    builder: Option<dagr_core::context::ResourceRegistryBuilder>,
}

impl ResourceRegistryStager {
    fn new() -> Self {
        Self {
            builder: Some(ResourceRegistry::builder()),
        }
    }

    fn register<R: Any + Send + Sync + 'static>(&mut self, resource: R) {
        let builder = self.builder.take().expect("staging builder present");
        // A duplicate same-typed fake is a test-authoring error surfaced here (the
        // C9 ambiguous-registration rule); newtype-wrap to distinguish two of a kind.
        self.builder = Some(
            builder
                .register(resource)
                .expect("fake resource of an unambiguous type; newtype-wrap same-typed fakes"),
        );
    }

    fn build(self) -> ResourceRegistry {
        self.builder.expect("staging builder present").build()
    }
}

// ===========================================================================
// The test builder
// ===========================================================================

/// One declared fake node awaiting assembly.
struct NodeSpec {
    name: String,
    /// Data-edge upstream names (0, 1, or 2 — the fan-in ceiling this harness
    /// registers dynamically).
    data_upstreams: Vec<String>,
    /// Ordering-edge upstream names (for a consume-nothing contingency).
    ordering_upstreams: Vec<String>,
    /// The trigger rule (default `all-succeeded`; a contingency states a
    /// non-default rule).
    trigger_rule: TriggerRule,
    outcome: Outcome,
}

/// A configured full-pipeline fake test (arch.md C28 full-pipeline level; T62).
///
/// Construct with [`new`](Self::new), add fake nodes and resources, set the
/// run-level knobs a test cares about, then [`run`](Self::run) it and assert on the
/// [`HarnessRun`]. See the [module docs](self) for the full contract and a worked
/// example.
#[must_use]
pub struct FullPipelineTest {
    pipeline_name: String,
    run_id: Option<String>,
    nodes: Vec<NodeSpec>,
    fakes: Vec<FakeResource>,
    failure_mode: FailureMode,
    parameters: BTreeMap<String, String>,
    data_interval: Option<[String; 2]>,
    /// A pinned C12 working-memory pool capacity (bytes), or `None` for
    /// unconstrained pools (every ready node admitted at once — the default).
    capacity_memory: Option<u64>,
}

impl FullPipelineTest {
    /// Begin a full-pipeline fake test for a pipeline named `pipeline_name` (the
    /// run-store directory name). The run defaults to continue-independent failure
    /// mode, no parameters, no data interval, and a fresh minted run id (override
    /// with [`run_id`](Self::run_id) for a byte-stable interpretive replay).
    pub fn new(pipeline_name: impl Into<String>) -> Self {
        Self {
            pipeline_name: pipeline_name.into(),
            run_id: None,
            nodes: Vec::new(),
            fakes: Vec::new(),
            failure_mode: FailureMode::ContinueIndependent,
            parameters: BTreeMap::new(),
            data_interval: None,
            capacity_memory: None,
        }
    }

    /// Add a **source** fake node (no upstreams) named `name` with the scripted
    /// `outcome`.
    pub fn source(mut self, name: impl Into<String>, outcome: Outcome) -> Self {
        self.nodes.push(NodeSpec {
            name: name.into(),
            data_upstreams: Vec::new(),
            ordering_upstreams: Vec::new(),
            trigger_rule: TriggerRule::AllSucceeded,
            outcome,
        });
        self
    }

    /// Add a **data-dependent** fake node named `name`, consuming the fake values of
    /// the nodes named in `upstreams` (0, 1, or a two-name fan-in), with the
    /// scripted `outcome`. A data-consuming node always runs on the default
    /// `all-succeeded` rule (C15).
    ///
    /// # Panics
    ///
    /// Panics if more than two data upstreams are named — this harness registers a
    /// two-input fan-in as its ceiling (aggregate more into an intermediate node,
    /// the same nudge the arity-8 `Deps` ceiling gives).
    pub fn node(mut self, name: impl Into<String>, upstreams: &[&str], outcome: Outcome) -> Self {
        assert!(
            upstreams.len() <= 2,
            "the full-pipeline harness registers at most a two-input fan-in; \
             aggregate more upstreams into an intermediate node"
        );
        self.nodes.push(NodeSpec {
            name: name.into(),
            data_upstreams: upstreams.iter().map(|s| (*s).to_string()).collect(),
            ordering_upstreams: Vec::new(),
            trigger_rule: TriggerRule::AllSucceeded,
            outcome,
        });
        self
    }

    /// Add a **consume-nothing contingency** named `name`, attached by **ordering**
    /// edges to the nodes named in `after`, firing on the non-default `trigger_rule`
    /// (`all-terminal` — cleanup — or `any-failed` — notify-on-failure). This is the
    /// notify / cleanup pattern C15's non-default rules exist for; the harness seeds
    /// the readiness tracker's ordering structure so the rule is evaluated against
    /// the ordered-after nodes at run time (C15).
    pub fn contingency(
        mut self,
        name: impl Into<String>,
        after: &[&str],
        trigger_rule: TriggerRule,
        outcome: Outcome,
    ) -> Self {
        self.nodes.push(NodeSpec {
            name: name.into(),
            data_upstreams: Vec::new(),
            ordering_upstreams: after.iter().map(|s| (*s).to_string()).collect(),
            trigger_rule,
            outcome,
        });
        self
    }

    /// Register a **fake resource** (C9) the flow's nodes retrieve by type. The
    /// substitution needs no task edit — a node reads the fake through the same
    /// `ctx.resources().get::<R>()` path it uses in production.
    pub fn register_fake(mut self, fake: FakeResource) -> Self {
        self.fakes.push(fake);
        self
    }

    /// Drive the run under **stop-on-first-failure** (C15): after the first failure
    /// no further default-rule work is admitted; an unrelated pending default node
    /// ends `cancelled`, while a firing non-default contingency still runs. The
    /// default is continue-independent (a failure cancels nothing).
    pub fn stop_on_first_failure(mut self) -> Self {
        self.failure_mode = FailureMode::StopOnFirstFailure;
        self
    }

    /// Pin the C12 working-memory admission-pool capacity (bytes), serializing
    /// admission against declared [node costs](Outcome::working_memory). The default
    /// is unconstrained (every ready node admitted at once). Pinning it (with a
    /// [`hold_until_cancelled`](Outcome::hold_until_cancelled) keeper occupying the
    /// pool) is how a test keeps an unrelated node **provably pending** — the
    /// deterministic pending-cancellation scenario, no wall clock.
    pub fn capacity_memory(mut self, bytes: u64) -> Self {
        self.capacity_memory = Some(bytes);
        self
    }

    /// Override the minted run identity (used verbatim). Fix it for a byte-stable
    /// [interpretive artifact](HarnessRun::interpretive_artifact_json) across
    /// repeated runs (the T65 replay).
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Record a run parameter (name→value) for the `run-started` header.
    pub fn parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    /// Record the run's opaque data interval for the `run-started` header — returned
    /// verbatim in the artifact, exactly as supplied (C8's opaque-interval
    /// invariant).
    pub fn data_interval(mut self, start: impl Into<String>, end: impl Into<String>) -> Self {
        self.data_interval = Some([start.into(), end.into()]);
        self
    }

    /// Assemble the flow, validate its C9 resource requirements against the injected
    /// fakes, drive it through the **real** T24 run loop with an injected
    /// deterministic clock and captured in-memory sink under a **private per-run
    /// temp base**, fold the recorded stream, and return the [`HarnessRun`] to
    /// assert on.
    ///
    /// # Panics
    ///
    /// Panics on a test-authoring error (an unknown upstream name, an ambiguous
    /// same-typed fake, or a flow that fails to assemble) — these are mistakes in
    /// the test, surfaced loudly rather than papered over.
    #[must_use]
    pub fn run(mut self) -> HarnessRun {
        // --- Build the immutable fake registry (C9), shared into every runner.
        let mut stager = ResourceRegistryStager::new();
        for fake in std::mem::take(&mut self.fakes) {
            (fake.register)(&mut stager);
        }
        let registry = stager.build();

        // --- C9 bootstrap resource validation (arch.md C9): every declared
        // requirement across the flow must be satisfied by a registered fake,
        // BEFORE any node executes. The harness records whether it passed so a test
        // can assert the no-infrastructure guarantee.
        let declarations: Vec<(NodeId, ResourceRequirements)> = self
            .nodes
            .iter()
            .map(|n| (NodeId::from_name(&n.name), n.outcome.requirements.clone()))
            .collect();
        let bootstrap_ok = registry.validate_requirements(&declarations).is_ok();

        // --- Assemble the flow through the REAL authoring API and wire each node's
        // runner with its input slot(s). All fake outputs are `u64`, so edges wire
        // uniformly; data edges opt into clone-on-read (safe for retry and
        // non-retry, and `u64: Clone`).
        let BuiltPlan {
            pipeline,
            runners,
            ordering,
        } = self.build_plan(&registry);

        // --- Drive through the REAL driver with an injected deterministic clock +
        // captured in-memory sink, under a PRIVATE per-run temp base.
        let temp = PrivateTempBase::new(&self.pipeline_name);
        let mut config = RunConfig::new(temp.base())
            .failure_mode(self.failure_mode)
            .parameters(self.parameters.clone());
        if let Some(id) = &self.run_id {
            config = config.run_id(id.clone());
        }
        if let Some(interval) = &self.data_interval {
            config = config.data_interval(interval.clone());
        }
        if let Some(bytes) = self.capacity_memory {
            config = config.capacities(dagr_core::admission::PoolCapacities::new().memory(bytes));
        }

        let sink = MemorySink::default();
        let report = drive(
            &config,
            &self.pipeline_name,
            Ok(RunPlan::with_ordering(pipeline.clone(), runners, ordering)),
            &[],
            sink.clone(),
            TickClock::default(),
        );

        // --- Fold the recorded stream into the C22 run artifact (the graph roster
        // gives never-ran nodes their propagated terminal state in the artifact).
        let stream = sink.bytes();
        let node_roster: Vec<String> = pipeline.nodes().map(|n| n.name().to_string()).collect();
        let artifact = fold_stream(&stream, &node_roster)
            .expect("the harness-recorded stream folds into a run artifact");

        // The declared data-edge structure (node → its data-upstream names), so the
        // harness can surface a propagated-skip's originating identity from the
        // observable terminal-state map + the flow it assembled (C15) — the C19 wire
        // `node-terminal` record carries no origin field, and the published schema is
        // not this ticket's to change.
        let data_edges: BTreeMap<String, Vec<String>> = self
            .nodes
            .iter()
            .map(|n| (n.name.clone(), n.data_upstreams.clone()))
            .collect();

        HarnessRun {
            outcome: report.outcome,
            terminal_states: report.terminal_states,
            stream,
            artifact,
            bootstrap_ok,
            data_edges,
        }
    }

    /// Assemble the pipeline and build the runner map + ordering map. Split out so
    /// [`run`](Self::run) reads as one linear drive sequence.
    fn build_plan(&self, registry: &ResourceRegistry) -> BuiltPlan {
        let mut flow = Flow::new();
        // Handles, keyed by name, so a later node can bind an earlier one.
        let mut handles: BTreeMap<String, dagr_core::handle::Handle<u64>> = BTreeMap::new();

        // Register every node in declaration order (a valid topological order —
        // upstreams are declared before downstreams by construction of the test).
        let mut ordering: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for spec in &self.nodes {
            let handle = register_node(&mut flow, spec, &handles, &mut ordering);
            handles.insert(spec.name.clone(), handle);
        }

        let pipeline = flow.finish();
        pipeline
            .assemble()
            .expect("the fake flow assembles (a test-authoring error otherwise)");

        // How many downstream data consumers each node has (its slot's consumer
        // count — the residency bookkeeping the real slot tracks).
        let mut consumer_count: BTreeMap<String, u32> = BTreeMap::new();
        for spec in &self.nodes {
            for up in &spec.data_upstreams {
                *consumer_count.entry(up.clone()).or_insert(0) += 1;
            }
        }

        // Build each node's output slot with its real consumer count.
        let ledger = ResidencyLedger::new();
        let mut slots: BTreeMap<String, Arc<Slot<u64>>> = BTreeMap::new();
        for spec in &self.nodes {
            let consumers = consumer_count.get(&spec.name).copied().unwrap_or(0);
            slots.insert(
                spec.name.clone(),
                Arc::new(Slot::new(
                    NodeId::from_name(&spec.name),
                    &spec.name,
                    consumers,
                    false,
                    0,
                    Arc::clone(&ledger),
                )),
            );
        }

        // Build the type-erased runner for every node, reading its upstream slot(s).
        let mut runners: RunnerMap = BTreeMap::new();
        for spec in &self.nodes {
            let slot = Arc::clone(&slots[&spec.name]);
            let upstream_refs: Vec<SlotRef<u64>> = spec
                .data_upstreams
                .iter()
                .map(|up| slots[up].shared_ref())
                .collect();
            runners.insert(
                spec.name.clone(),
                Box::new(FakeRunner {
                    name: spec.name.clone(),
                    outcome: spec.outcome.clone(),
                    slot,
                    upstream_refs,
                    registry: registry.clone(),
                    ran: false,
                }),
            );
        }

        BuiltPlan {
            pipeline,
            runners,
            ordering,
        }
    }
}

/// Register one fake node on `flow` by its data-upstream arity (0 source, 0 +
/// ordering contingency, 1 map, 2 fan-in join), recording a contingency's ordering
/// upstreams into `ordering`. Returns the node's output handle.
///
/// # Panics
///
/// Panics if the node names an upstream that was not registered earlier — a
/// test-authoring error surfaced loudly.
fn register_node(
    flow: &mut Flow,
    spec: &NodeSpec,
    handles: &BTreeMap<String, dagr_core::handle::Handle<u64>>,
    ordering: &mut BTreeMap<String, Vec<String>>,
) -> dagr_core::handle::Handle<u64> {
    let policy = NodePolicy::new().working_memory(spec.outcome.working_memory);
    let lookup = |up: &str| {
        *handles
            .get(up)
            .unwrap_or_else(|| panic!("node `{}` references unknown upstream `{up}`", spec.name))
    };
    match spec.data_upstreams.len() {
        0 if spec.ordering_upstreams.is_empty() => {
            flow.register_source_with_trigger(&spec.name, &FakeSource, policy, spec.trigger_rule)
        }
        0 => {
            // A consume-nothing contingency attached by ordering edges.
            let ordering_handles: Vec<_> = spec
                .ordering_upstreams
                .iter()
                .map(|up| lookup(up).ordering())
                .collect();
            ordering.insert(spec.name.clone(), spec.ordering_upstreams.clone());
            flow.register_source_ordered_after_with_trigger(
                &spec.name,
                &FakeSource,
                &ordering_handles,
                policy,
                spec.trigger_rule,
            )
        }
        1 => flow.register_with::<FakeMap, _>(
            &spec.name,
            &FakeMap,
            lookup(&spec.data_upstreams[0]).clone_on_read(),
            policy,
        ),
        _ => flow.register_with::<FakeJoin, _>(
            &spec.name,
            &FakeJoin,
            (
                lookup(&spec.data_upstreams[0]).clone_on_read(),
                lookup(&spec.data_upstreams[1]).clone_on_read(),
            ),
            policy,
        ),
    }
}

// ===========================================================================
// The captured run
// ===========================================================================

/// The observable results of one full-pipeline fake run (arch.md C28; T62): the
/// overall outcome, per-node terminal states, the emitted event stream, and the
/// folded run artifact — the surface a test asserts on.
pub struct HarnessRun {
    outcome: RunOutcome,
    terminal_states: BTreeMap<String, TerminalState>,
    stream: Vec<u8>,
    artifact: RunArtifact,
    bootstrap_ok: bool,
    /// The declared data-edge structure (node → data-upstream names), for surfacing
    /// a propagated skip's originating identity from observable data.
    data_edges: BTreeMap<String, Vec<String>>,
}

impl HarnessRun {
    /// The overall run outcome as the normative lowercase token (`succeeded`,
    /// `failed`, `cancelled`, …) the real driver surfaced — a run whose only
    /// non-success outcomes are skips is `succeeded` (C15).
    #[must_use]
    pub fn overall_outcome(&self) -> &'static str {
        match self.outcome {
            RunOutcome::Succeeded => "succeeded",
            RunOutcome::Failed => "failed",
            RunOutcome::Cancelled => "cancelled",
            RunOutcome::AssemblyFailed => "assembly-failed",
            RunOutcome::BootstrapFailed => "bootstrap-failed",
        }
    }

    /// The terminal state the framework assigned `node`, or [`None`] if the node is
    /// unknown. Every reachable node has exactly one (including nodes that never
    /// ran).
    #[must_use]
    pub fn terminal_state(&self, node: &str) -> Option<TerminalState> {
        self.terminal_states.get(node).copied()
    }

    /// Whether the flow's declared C9 resource requirements were all satisfied by
    /// the injected fakes at bootstrap (the no-infrastructure guarantee).
    #[must_use]
    pub fn bootstrap_resource_validation_passed(&self) -> bool {
        self.bootstrap_ok
    }

    /// The raw recorded C19 event stream (JSON Lines) the real driver wrote — the
    /// authoritative record a test can walk directly.
    #[must_use]
    pub fn event_stream(&self) -> &[u8] {
        &self.stream
    }

    /// The folded C22 [run artifact](RunArtifact) (attempts, per-node terminals,
    /// overall outcome, summary) — the artifact surface a test asserts on.
    #[must_use]
    pub fn artifact(&self) -> &RunArtifact {
        &self.artifact
    }

    /// The **normalized interpretive artifact JSON** for the T65 interpretive-
    /// determinism replay: the folded artifact with volatile header/timing fields
    /// (run id, generation time, monotonic offsets, worker, elapsed/critical-path
    /// timings) blanked to canonical placeholders, so two runs of the same script
    /// yield byte-identical content. The interpretive content — per-node terminal
    /// states, propagation decisions, attempt statuses/numbers — is preserved.
    #[must_use]
    pub fn interpretive_artifact_json(&self) -> String {
        let mut value = self.artifact.to_value();
        normalize_volatile(&mut value);
        dagr_artifact::canonical::to_canonical_string(&value)
    }

    // --- Event-stream query helpers (the observable oracle) ----------------

    /// The parsed `(kind, node)` transitions in stream order.
    fn transitions(&self) -> Vec<(String, Option<String>)> {
        let stream = read_records(&self.stream).expect("the recorded stream parses");
        stream
            .records
            .iter()
            .map(|rec| {
                let kind = rec
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let node = rec.get("node").and_then(|v| v.as_str()).map(str::to_string);
                (kind, node)
            })
            .collect()
    }

    /// The event kinds in stream order (`run-started`, `node-ready`, …).
    #[must_use]
    pub fn event_kinds(&self) -> Vec<String> {
        self.transitions().into_iter().map(|(k, _)| k).collect()
    }

    /// Whether a `(kind, node)` transition appears anywhere in the stream.
    #[must_use]
    pub fn has_event(&self, kind: &str, node: Option<&str>) -> bool {
        self.transitions()
            .iter()
            .any(|(k, n)| k == kind && n.as_deref() == node)
    }

    /// The index of the first `(kind, node)` transition, or [`None`].
    #[must_use]
    pub fn event_index(&self, kind: &str, node: Option<&str>) -> Option<usize> {
        self.transitions()
            .iter()
            .position(|(k, n)| k == kind && n.as_deref() == node)
    }

    /// How many `kind` transitions name `node`.
    #[must_use]
    pub fn event_count(&self, kind: &str, node: &str) -> usize {
        self.transitions()
            .iter()
            .filter(|(k, n)| k == kind && n.as_deref() == Some(node))
            .count()
    }

    /// How many `node-terminal` records `node` has — exactly one for every
    /// reachable node (the single-terminal-state invariant).
    #[must_use]
    pub fn terminal_count(&self, node: &str) -> usize {
        self.event_count("node-terminal", node)
    }

    /// The **originating node identity** carried on `node`'s propagated
    /// `upstream-skipped` terminal — the skip-class data-upstream whose skip
    /// deadened it (C15), or [`None`] if `node` is not upstream-skipped.
    ///
    /// The framework decides the propagation (the readiness tracker's
    /// propagated-terminal `origin`, C15); this surfaces that decision from observable data —
    /// the run's per-node terminal-state map and the flow the harness assembled —
    /// because the C19 wire `node-terminal` record carries no origin field and the
    /// published schema is not this ticket's to change. When more than one upstream
    /// is skip-class, the first in declaration order is reported (the tracker's own
    /// tie-break).
    #[must_use]
    pub fn upstream_skip_origin(&self, node: &str) -> Option<String> {
        if self.terminal_state(node) != Some(TerminalState::UpstreamSkipped) {
            return None;
        }
        // The originating upstream is the (declaration-first) data-upstream that
        // itself reached a skip-class terminal (`skipped` or `upstream-skipped`).
        self.data_edges.get(node)?.iter().find_map(|up| {
            let up_terminal = self.terminal_state(up)?;
            matches!(
                up_terminal,
                TerminalState::Skipped | TerminalState::UpstreamSkipped
            )
            .then(|| up.clone())
        })
    }
}

/// Blank the volatile header/timing fields of a folded artifact JSON so two runs
/// of the same script are byte-identical (the T65 interpretive-determinism
/// replay). Everything interpretive — per-node terminal statuses, attempt numbers,
/// propagation origins, overall outcome — is preserved.
fn normalize_volatile(value: &mut serde_json::Value) {
    use serde_json::Value;
    if let Some(header) = value.get_mut("header").and_then(Value::as_object_mut) {
        // Run identity + any generation-time-shaped header fields are volatile.
        for key in ["run_id", "generated_at", "generation_time", "wall"] {
            if header.contains_key(key) {
                header.insert(key.into(), Value::from("<normalized>"));
            }
        }
    }
    // Attempt records: blank the clock-derived timing fields, then sort by
    // `(node, attempt)`. The fold orders attempts by stream `(seq)`, but the real
    // driver runs INDEPENDENT nodes concurrently, so the interleaving of unrelated
    // nodes' records in the stream is a scheduling artifact, not interpretive
    // content. Sorting by identity makes the interpretive content (each node's
    // ordered attempts, statuses, numbers, propagation origins) order-independent
    // and byte-stable — a retried node's own attempts keep their `1, 2, …` order
    // because the sort key breaks ties on the attempt number.
    if let Some(attempts) = value.get_mut("attempts").and_then(Value::as_array_mut) {
        for attempt in attempts.iter_mut() {
            if let Some(obj) = attempt.as_object_mut() {
                obj.insert("phase_durations_ns".into(), serde_json::json!({}));
                obj.insert("worker".into(), Value::from("<normalized>"));
            }
        }
        attempts.sort_by(|a, b| {
            let key = |v: &Value| {
                (
                    v.get("node")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    v.get("attempt").and_then(Value::as_u64).unwrap_or_default(),
                )
            };
            key(a).cmp(&key(b))
        });
    }
    // The summary's timing fields are likewise clock-derived.
    if value.get("summary").is_some_and(Value::is_object) {
        value
            .as_object_mut()
            .unwrap()
            .insert("summary".into(), Value::from("<normalized>"));
    }
    // The fold-reader block carries no interpretive content; blank its volatile
    // interrupted mirror is unnecessary (deterministic), leave it as is.
}

// ===========================================================================
// The fake tasks (public `Task` trait) — one per input arity, one shared script.
// ===========================================================================

/// A no-input fake source / contingency task.
struct FakeSource;
impl Task for FakeSource {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // The runner drives the script; this body is never the scripting authority
        // (the FakeRunner binds a scripted adapter). Reached only if a runner ran a
        // source directly — it produces the fake value.
        Ok(FAKE_VALUE)
    }
}

/// A one-input fake map task (consumes one `u64`, produces the fake value).
struct FakeMap;
impl Task for FakeMap {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: u64) -> Result<u64, TaskError> {
        Ok(FAKE_VALUE)
    }
}

/// A two-input fake join task (a fan-in; consumes two `u64`s, produces the fake
/// value).
struct FakeJoin;
impl Task for FakeJoin {
    type Input = (u64, u64);
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: (u64, u64)) -> Result<u64, TaskError> {
        Ok(FAKE_VALUE)
    }
}

/// The fake value every scripted node produces (arbitrary, but stable so a test can
/// assert value flow if it wants to).
const FAKE_VALUE: u64 = 1;

// ===========================================================================
// The scripted adapter + the type-erased runner
// ===========================================================================

/// A no-input owned adapter that runs the **script** for one node against a context
/// enriched with the fake registry. Holds a shared attempt counter so the
/// retry-loop's re-runs observe the incrementing attempt number without the runner
/// re-binding per attempt (the counter is authoritative, the ctx attempt mirrors
/// it).
struct ScriptedAdapter {
    script: Script,
    resource_probe: Option<ResourceProbe>,
    /// The fake registry to inject into each attempt's context. `None` on the retry
    /// path (the retry loop mints its own resource-less context — see module docs).
    registry: Option<ResourceRegistry>,
    /// The observed attempt count, incremented on each `run`, so the retry loop's
    /// successive calls see 1, 2, 3, … deterministically.
    attempts_seen: Arc<AtomicU64>,
}

impl Task for ScriptedAdapter {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // The attempt number this call runs under: the retry loop's minted ctx
        // carries it; the single-attempt path reads ctx.attempt() (always 1).
        let attempt = self.attempts_seen.fetch_add(1, Ordering::SeqCst) + 1;
        // Enrich the context with the fake registry so a resource probe by type
        // succeeds (the single-attempt injection path). The retry loop supplies no
        // registry, so a retrying node reads none — documented as a known boundary.
        let ctx = self.enriched_ctx(ctx);
        match self.script {
            Script::Succeed => self.probe_then(&ctx, Ok(FAKE_VALUE)),
            Script::SucceedAfterYield => {
                // A REAL await point — proof the provided runtime drives it.
                tokio::task::yield_now().await;
                self.probe_then(&ctx, Ok(FAKE_VALUE))
            }
            Script::FailPermanent => Err(TaskError::permanent("scripted permanent failure")),
            Script::FailThenSucceed(retries) => {
                if attempt <= u64::from(retries) {
                    Err(TaskError::retryable("scripted retry-eligible failure"))
                } else {
                    Ok(FAKE_VALUE)
                }
            }
            Script::Skip => Err(TaskError::skip("scripted deliberate skip")),
            Script::HoldUntilCancelled => {
                // Spin cooperatively until the run enters cancellation (observed
                // through this attempt's child signal), then return. Bounded so a
                // regression that never propagates the cancel cannot hang — the
                // fallback return then leaves the gate non-vacuous. No wall clock.
                for _ in 0..1_000_000 {
                    if ctx.cancellation().is_cancelled() {
                        return Ok(FAKE_VALUE);
                    }
                    tokio::task::yield_now().await;
                }
                Ok(FAKE_VALUE)
            }
        }
    }
}

impl ScriptedAdapter {
    /// Build the context the scripted body reads: the driver's context enriched with
    /// the fake registry (identity, attempt, cancellation, `temp_dir` threaded
    /// through). On the retry path (`registry == None`) the driver's context is used
    /// as-is (the retry loop already minted it).
    fn enriched_ctx(&self, driver_ctx: &RunContext) -> RunContext {
        let Some(registry) = &self.registry else {
            return clone_ctx(driver_ctx, None);
        };
        clone_ctx(driver_ctx, Some(registry.clone()))
    }

    /// Assert the resource probe (if any) is reachable by type in the enriched
    /// context, then return `result`. A probe whose fake is absent turns a scripted
    /// success into a permanent failure, so the "receives the fake by type"
    /// scenario is a real assertion.
    fn probe_then(
        &self,
        ctx: &RunContext,
        result: Result<u64, TaskError>,
    ) -> Result<u64, TaskError> {
        if let Some(probe) = &self.resource_probe {
            if !probe(ctx.resources()) {
                return Err(TaskError::permanent(
                    "scripted resource probe: the required fake was not reachable by type",
                ));
            }
        }
        result
    }
}

/// Rebuild a `RunContext` carrying the driver's identity/attempt/cancellation/
/// `temp_dir`, optionally with a resource registry added. The single-task kit builds
/// its context the same way; this is the full-pipeline analogue for injecting
/// resources into a driver-minted context.
fn clone_ctx(src: &RunContext, registry: Option<ResourceRegistry>) -> RunContext {
    let mut builder = RunContext::builder(
        src.run_id().clone(),
        src.pipeline_id().clone(),
        src.node_id(),
    )
    .attempt(src.attempt())
    .max_attempts(src.max_attempts())
    .cancellation(src.cancellation().clone());
    if let Some(interval) = src.data_interval() {
        builder = builder.data_interval(interval.clone());
    }
    if let Some(temp) = src.temp_dir() {
        builder = builder.temp_dir(temp.to_path_buf());
    }
    if let Some(registry) = registry {
        builder = builder.resources(registry);
    }
    builder.build()
}

/// The type-erased [`NodeRunner`] the harness hands the driver for each fake node.
/// It reads its upstream slot(s) (proving the real data edges are wired), then
/// drives the scripted adapter through the **real** attempt runner — a single
/// caught attempt for most scripts, or the real bounded-retry loop for a
/// fail-then-succeed script — so the emitted C14/C19 records are genuine.
struct FakeRunner {
    name: String,
    outcome: Outcome,
    slot: Arc<Slot<u64>>,
    upstream_refs: Vec<SlotRef<u64>>,
    registry: ResourceRegistry,
    ran: bool,
}

impl NodeRunner for FakeRunner {
    fn name(&self) -> &str {
        &self.name
    }

    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        assert!(!self.ran, "a node runs exactly once");
        self.ran = true;
        // Read every upstream slot — by the time the driver admits this node its
        // upstreams have succeeded, so the slots are filled (clone-on-read gives a
        // fresh clone). Proves the real data edges carried a value; the fake script
        // ignores the value.
        for up in &self.upstream_refs {
            let _ = *up.read();
        }
        let name = self.name.clone();
        let slot = Arc::clone(&self.slot);
        let outcome = self.outcome.clone();
        let registry = self.registry.clone();
        let attempts_seen = Arc::new(AtomicU64::new(0));

        Box::pin(async move {
            let max_attempts = outcome.max_attempts();
            if max_attempts > 1 {
                // The REAL bounded-retry loop (T22/T23). It mints its own per-attempt
                // context (no resource injection — see module docs), so a retrying
                // fake carries no resource probe. The backoff timer resolves
                // immediately (no wall-clock sleep).
                let adapter = ScriptedAdapter {
                    script: outcome.script,
                    resource_probe: None,
                    registry: None,
                    attempts_seen: Arc::clone(&attempts_seen),
                };
                run_with_retries_caught(
                    adapter,
                    &name,
                    ctx.run_id().clone(),
                    ctx.pipeline_id().clone(),
                    &slot,
                    sink,
                    &RetryConfig::new(
                        max_attempts,
                        Backoff::new(Duration::ZERO, 2.0, Duration::ZERO),
                    ),
                    &mut NoJitter,
                    |_delay: Duration| async {},
                )
                .await
                .terminal_state()
            } else {
                // A single caught attempt through the REAL runner, with the fake
                // registry injected into the attempt's context.
                let mut adapter = ScriptedAdapter {
                    script: outcome.script,
                    resource_probe: outcome.resource_probe.clone(),
                    registry: Some(registry),
                    attempts_seen,
                };
                run_attempt_caught(&mut adapter, &name, ctx, &slot, sink)
                    .await
                    .terminal_state()
            }
        })
    }
}

// ===========================================================================
// Deterministic injection seam: in-memory sink + monotonic clock + private temp
// ===========================================================================

/// An in-memory [`EventSink`] capturing every appended line, so the harness folds
/// and walks the **real** event stream the driver wrote — matching the production
/// run path's injected run-store sink.
#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().expect("sink mutex not poisoned").clone()
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .expect("sink mutex not poisoned")
            .extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock ticking one nanosecond per read — strictly increasing offsets
/// with **no wall clock**, so any derived durations are deterministic.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// A **private per-run temp base** for a harness run (arch.md C16; the shared-`/tmp`
/// flake class). Created fresh under the process temp dir with a per-run unique
/// suffix, and removed when the run's captures are collected — so two harness runs,
/// even concurrent ones, never collide on the run store.
struct PrivateTempBase {
    path: std::path::PathBuf,
}
impl PrivateTempBase {
    fn new(pipeline: &str) -> Self {
        // Per-run unique: pid + a process-monotonic counter (no wall clock).
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "dagr-full-pipeline-{}-{}-{n}",
            sanitize(pipeline),
            std::process::id()
        ));
        // Best-effort create; the driver also creates its own per-run subtree.
        let _ = std::fs::create_dir_all(&dir);
        Self { path: dir }
    }
    fn base(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}
impl Drop for PrivateTempBase {
    fn drop(&mut self) {
        // Best-effort cleanup — a racing detached temp-reclaim thread may hold a
        // handle; the process exits promptly rather than blocking on it.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Reduce a pipeline name to a filesystem-safe fragment for the temp dir name.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}
