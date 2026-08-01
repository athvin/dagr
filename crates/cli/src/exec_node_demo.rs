//! The **reference pipeline the `exec-node` suite drives** — one flow with a node
//! for every shape the pod-side verb has to get right.
//!
//! It exists because the honest way to test a verb whose whole premise is
//! *re-entrancy* is to run the real binary: a subprocess invocation is
//! indistinguishable from a pod invocation from the verb's point of view, so the
//! suite launches `dagr-exec-node-demo` and reads back what it wrote. No cluster is
//! involved anywhere.
//!
//! Every node is registered through a **`Payload`-bounded** registrar, which is what
//! makes it remote-eligible (ADR 115 §8: a node is remote-eligible *iff* its input
//! and output types implement `Payload`). The flow also attaches a
//! [`ResourceRegistry`] built **inside the factory**, which is the point of the
//! per-pod lifetime test: because the pod re-enters this same code, the resource is
//! constructed once per invocation rather than once per run.
//!
//! Behaviour that a test needs to vary is read from the environment rather than
//! compiled in, because the subprocess is the unit under test and the environment is
//! how a test reaches it.
//!
//! It ships behind `test-kit` **and** `blob`, so it is in no released binary.

use std::sync::atomic::{AtomicU64, Ordering};

use dagr_core::context::{ResourceRegistry, RunContext, TerminalState};
use dagr_core::error::TaskError;
use dagr_core::execution::{AttemptEvent, AttemptEventSink};
use dagr_core::task::Task;
use dagr_core::payload::Payload as PayloadCodec;
use dagr_core::{Payload, StableName};

use crate::run_flow::RunnableFlow;

/// The value every node in this flow produces — a `Payload`, so the framework can
/// encode it, store it, and hand its reference to the next attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, StableName, Payload)]
pub struct Counted {
    /// The number this node computed.
    pub n: u64,
}

/// The environment variable the `seed` source reads its value from.
pub const SEED_ENV: &str = "DAGR_DEMO_SEED";

/// The environment variable naming the file the demo resource appends a line to on
/// **each construction** — how a test observes the once-per-pod lifetime.
pub const RESOURCE_LOG_ENV: &str = "DAGR_DEMO_RESOURCE_LOG";

/// The environment variable choosing which [`TaskError`] class `boom` returns.
pub const BOOM_ENV: &str = "DAGR_DEMO_BOOM";

/// The environment variable naming the marker file `sleeper` touches once it is
/// running, so a test can send SIGTERM at a known moment instead of sleeping.
pub const SLEEPER_MARKER_ENV: &str = "DAGR_DEMO_SLEEPER_MARKER";

/// The pipeline identity this flow is hosted under.
pub const PIPELINE: &str = "exec-node-demo";

// ===========================================================================
// The resource
// ===========================================================================

/// A resource that **records its own construction**.
///
/// Standing in for the connection pool, lock file, or local cache ADR 115 warns
/// about: it is built by the flow factory, so it exists once per process — which,
/// because a pod re-enters this binary, means once per pod rather than once per run.
#[derive(Debug, Clone, Copy)]
pub struct ConstructionCounter {
    /// A process-unique id, appended to the log file when this value was built.
    pub id: u64,
}

impl ConstructionCounter {
    /// Construct one, appending its id to the log file the environment names.
    fn build() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        if let Ok(path) = std::env::var(RESOURCE_LOG_ENV) {
            use std::io::Write;
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                let _ = writeln!(file, "{id}");
            }
        }
        Self { id }
    }
}

// ===========================================================================
// The tasks
// ===========================================================================

/// A consume-nothing source producing the value the environment names (default 7).
#[derive(Debug, Clone, Copy, StableName)]
pub struct Seed;

impl Task for Seed {
    type Input = ();
    type Output = Counted;

    async fn run(&mut self, _ctx: &RunContext, (): ()) -> Result<Counted, TaskError> {
        let n = std::env::var(SEED_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(7);
        Ok(Counted { n })
    }
}

/// Doubles its input — the golden path, and the node the equivalence comparison runs
/// on both sides.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Double;

impl Task for Double {
    type Input = Counted;
    type Output = Counted;

    async fn run(&mut self, _ctx: &RunContext, input: Counted) -> Result<Counted, TaskError> {
        Ok(Counted { n: input.n * 2 })
    }
}

/// Combines two inputs **order-sensitively** (`first * 10 + second`), so swapping the
/// references a test supplies changes the answer — which is how positional order is
/// proved rather than asserted.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Combine;

impl Task for Combine {
    type Input = (Counted, Counted);
    type Output = Counted;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        (first, second): (Counted, Counted),
    ) -> Result<Counted, TaskError> {
        Ok(Counted {
            n: first.n * 10 + second.n,
        })
    }
}

/// Fails with the [`TaskError`] class the environment names — permanent, retryable,
/// or a deliberate skip.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Boom;

impl Task for Boom {
    type Input = Counted;
    type Output = Counted;

    async fn run(&mut self, _ctx: &RunContext, _input: Counted) -> Result<Counted, TaskError> {
        let class = std::env::var(BOOM_ENV).unwrap_or_else(|_| "permanent".to_string());
        Err(match class.as_str() {
            "retryable" => TaskError::retryable("boom: a retryable failure"),
            "skip" => TaskError::skip("boom: a deliberate skip"),
            _ => TaskError::permanent("boom: a permanent failure"),
        })
    }
}

/// Panics — the containment path, unchanged from a local attempt.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Panicky;

impl Task for Panicky {
    type Input = Counted;
    type Output = Counted;

    async fn run(&mut self, _ctx: &RunContext, _input: Counted) -> Result<Counted, TaskError> {
        panic!("panicky went bang");
    }
}

/// Runs until it observes cancellation, then returns promptly — the cooperative
/// shape arch.md C16's grace period is defined over.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Sleeper;

impl Task for Sleeper {
    type Input = Counted;
    type Output = Counted;

    async fn run(&mut self, ctx: &RunContext, input: Counted) -> Result<Counted, TaskError> {
        if let Ok(marker) = std::env::var(SLEEPER_MARKER_ENV) {
            let _ = std::fs::write(marker, b"running\n");
        }
        while !ctx.cancellation().is_cancelled() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // Observing cancellation and returning promptly is the contract; the verb
        // decides what the *outcome* is called, because the operator originated it.
        let _ = input;
        Err(TaskError::retryable("sleeper observed cancellation"))
    }
}

/// Reads the registered resource and reports its construction id — the proof that
/// the registry was rebuilt in this process and reached the task.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Resourceful;

impl Task for Resourceful {
    type Input = ();
    type Output = Counted;

    async fn run(&mut self, ctx: &RunContext, (): ()) -> Result<Counted, TaskError> {
        let counter = ctx
            .resources()
            .get::<ConstructionCounter>()
            .ok_or_else(|| TaskError::permanent("no `ConstructionCounter` in the registry"))?;
        Ok(Counted { n: counter.id })
    }
}

/// Fails **retryably** and is registered with a retry budget, so a *local* run really
/// does attempt it several times — which is what makes "the pod attempts it once"
/// mean something.
#[derive(Debug, Clone, Copy, StableName)]
pub struct Retrying;

impl Task for Retrying {
    type Input = Counted;
    type Output = Counted;

    async fn run(&mut self, _ctx: &RunContext, _input: Counted) -> Result<Counted, TaskError> {
        Err(TaskError::retryable("retrying always fails"))
    }
}

// ===========================================================================
// The flow
// ===========================================================================

/// Build the demo flow — the factory the binary registers and the pod re-enters.
///
/// Every node goes through a `Payload`-bounded registrar, and the resource registry
/// is constructed **here**, inside the factory, which is precisely why it exists once
/// per invocation.
#[must_use]
pub fn build_exec_node_demo_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    // Every edge off `seed` opts into clone-on-read: the value is multiply consumed
    // (assembly refuses an owned edge then), and `retrying` additionally needs each
    // attempt to get a fresh clone. `Counted` is `Copy`, so the clone is free.
    let seed = flow.register_source_payload("seed", Seed);
    let double = flow.register_payload("double", Double, seed.clone_on_read());
    let _combine =
        flow.register_payload("combine", Combine, (seed.clone_on_read(), double));
    let _boom = flow.register_payload("boom", Boom, seed.clone_on_read());
    let _panicky = flow.register_payload("panicky", Panicky, seed.clone_on_read());
    let _sleeper = flow.register_payload("sleeper", Sleeper, seed.clone_on_read());
    let _resourceful = flow.register_source_payload("resourceful", Resourceful);
    let _retrying = flow.register_payload_with(
        "retrying",
        Retrying,
        seed.clone_on_read(),
        dagr_core::assembly::NodePolicy::new().retries(2),
    );
    flow.with_resources(
        ResourceRegistry::builder()
            .register(ConstructionCounter::build())
            .expect("one resource type, registered once")
            .build(),
    )
}

/// This build's fingerprints, rendered exactly as the shard records them.
///
/// A test compares them against what the subprocess wrote; because both come from
/// the same pure computation over the same flow, an accidental divergence is a real
/// failure rather than a fixture drift.
#[must_use]
pub fn demo_fingerprints() -> (String, String) {
    let fingerprint = build_exec_node_demo_flow().into_pipeline().fingerprint();
    (
        crate::graph::format_fingerprint_structural(&fingerprint),
        crate::graph::format_fingerprint_policy(&fingerprint),
    )
}

// ===========================================================================
// The local side of the equivalence comparison
// ===========================================================================

/// Run `node`'s attempt **locally, in this process**, through the ordinary run loop,
/// and return what it emitted.
///
/// This is the other half of "the shard's records are the local records": the
/// expectation is a real engine run of the same flow, never a hand-written list.
fn local_attempt(node: &str) -> (Vec<AttemptEvent>, TerminalState) {
    let mut prepared = build_exec_node_demo_flow()
        .prepare_attempt_with_discipline(node, crate::run_flow::AttemptDiscipline::NodePolicy)
        .expect("the demo flow prepares");
    // Feed the node the same input a local run would have handed it: the value its
    // upstream source produces, encoded through the same codec.
    for index in 0..prepared.input_arity() {
        prepared
            .fill_input(index, &upstream_value(node, index).encode_to_vec())
            .expect("the demo codec round-trips");
    }
    let ctx = RunContext::builder(
        dagr_core::context::RunId::new("local"),
        dagr_core::context::PipelineId::new(PIPELINE),
        dagr_core::handle::NodeId::from_name(node),
    )
    .build();
    let mut sink = Recorder::default();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("a local attempt runtime");
    let terminal = runtime.block_on(prepared.run_once(&ctx, &mut sink));
    (sink.events, terminal)
}

/// The value the `index`th upstream of `node` would have produced locally.
fn upstream_value(node: &str, index: usize) -> Counted {
    // `seed` is every data node's first upstream; `combine`'s second is `double`.
    let seed = Counted {
        n: std::env::var(SEED_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(7),
    };
    if node == "combine" && index == 1 {
        Counted { n: seed.n * 2 }
    } else {
        seed
    }
}

/// The wire `kind`s a **local** attempt of `node` emits, in order.
#[must_use]
pub fn local_attempt_kinds(node: &str) -> Vec<String> {
    let (events, _) = local_attempt(node);
    crate::shard::records_for("local", &events)
        .iter()
        .filter_map(|r| {
            r.get("kind")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

/// The terminal state a **local** attempt of `node` reaches, in the normative
/// spelling the shard records.
#[must_use]
pub fn local_terminal_state(node: &str) -> String {
    let (_, terminal) = local_attempt(node);
    format!("{}", TerminalLabel(terminal))
}

/// How many attempts a **local** run of `node` makes under its own policy — the
/// number the pod-side "exactly one" claim is measured against.
#[must_use]
pub fn local_attempt_count(node: &str) -> usize {
    let (events, _) = local_attempt(node);
    events
        .iter()
        .filter(|e| matches!(e, AttemptEvent::AttemptStarted { .. }))
        .count()
}

/// Renders a [`TerminalState`] in the normative vocabulary's spelling.
struct TerminalLabel(TerminalState);

impl std::fmt::Display for TerminalLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            TerminalState::Succeeded => "succeeded",
            TerminalState::Failed => "failed",
            TerminalState::TimedOut => "timed-out",
            TerminalState::Skipped => "skipped",
            TerminalState::UpstreamSkipped => "upstream-skipped",
            TerminalState::UpstreamFailed => "upstream-failed",
            TerminalState::Cancelled => "cancelled",
            TerminalState::Abandoned => "abandoned",
            TerminalState::SatisfiedFromPrior => "satisfied-from-prior",
        })
    }
}

/// A sink that keeps every emitted record, in order.
#[derive(Default)]
struct Recorder {
    events: Vec<AttemptEvent>,
}

impl AttemptEventSink for Recorder {
    fn emit(&mut self, event: AttemptEvent) {
        self.events.push(event);
    }
}
