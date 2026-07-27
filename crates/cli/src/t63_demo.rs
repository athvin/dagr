//! The **gate demo reference pipeline**. Shared, shipped-in-test-support
//! scaffolding assembled by the demo run/resume binary
//! (`crates/cli/src/bin/dagr-t63-demo-run.rs`) **and** by the review structure test
//! (`crates/cli/tests/m4_demo_kill_resume_review.rs`), so the *same* reference
//! pipeline is proven end-to-end (kill → resume) and its structure is the one the
//! checked-in review fixture guards.
//!
//! # Adds no engine capability
//!
//! This module composes **already-merged** machinery — the durable scratch store,
//! the durable-output contract, the real `drive()` loop (with its
//! scratch/durable-reference wiring), the `resume_verb`, and the structure snapshot
//! — through the **shipped** driver. It defines the reference pipeline's fake tasks
//! (declarative, no real work) and the type-erased [`NodeRunner`]s the driver
//! spawns, plus the assembly the review fixture snapshots. It ships behind no
//! feature and in no released binary; it is checked-in scaffolding.
//!
//! # The reference pipeline (fixed at assembly; a durable stage boundary + teardown)
//!
//! A small graph with every shape the kill/resume demo needs:
//!
//! - **`expensive`** — a **durable** stage-boundary producer (its output implements
//!   the durable-reference contract; marked durable, so its reference is recorded
//!   and a resume rehydrates it instead of recomputing). Group `stage`.
//! - **`inmem`** — an **in-memory** (non-durable) producer, covered by the teardown.
//!   Group `stage`.
//! - **`consumer`** — a data-dependent consumer that **demands** `expensive`'s value
//!   (so a resume rehydrates it). Ordered after `checkpoint` so it is *pending* at a
//!   mid-run kill. Group `publish`.
//! - **`publish`** — an ordering-only **publish** whose value nothing demands (the
//!   cleanup-after-publish shape's upstream). Group `publish`.
//! - **`cleanup`** — a **cleanup** node ordered after `publish` (and `checkpoint`),
//!   so it is *pending* at the kill and re-runs on resume; its rule sees the
//!   satisfied publish and fires. Group `publish`.
//! - **`checkpoint`** — the **scratch-checkpointing** node: it writes a high-water
//!   mark into its scratch, signals mid-run readiness on disk, then holds until
//!   the process is killed — so at the kill it is in-flight (non-succeeded, scratch
//!   retained), and on resume it re-executes and reads its carried-forward
//!   checkpoint. Ordered after `expensive`. Group `stage`.
//! - **`teardown`** — a **teardown** covering the producer `inmem`, so `inmem`
//!   is always in the resume must-run seed and never satisfied-from-prior.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt, run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::{RehydrateError, TaskError};

use crate::driver::{NodeRunner, RunPlan};

/// The fixed pipeline identity for the demo's run-store layout.
pub const PIPELINE: &str = "m4-demo-pipeline";

/// The scratch key the checkpoint node writes its high-water mark under.
pub const CHECKPOINT_KEY: &[u8] = b"high-water";

/// The distinctive high-water mark the checkpoint node writes — a re-execution that
/// reads this back proves it continued from the checkpoint, not from zero.
pub const CHECKPOINT_MARK: &[u8] = b"finished-item-K";

/// The distinctive value the durable stage-boundary producer serializes — a resume
/// that rehydrates it proves the value came from the prior run, not a recomputation.
pub const STAGE_VALUE: &str = "STAGE-OUTPUT";

// ===========================================================================
// Payload — a durable blob carrying the durable-reference contract + a stable name.
// ===========================================================================

/// The stage boundary's durable payload. Its reference is `blob://<content>`, so a
/// distinctive marker survives serialize → record → rehydrate and can be traced to
/// the prior run. Carries a [`StableName`] so the durable producer is snapshottable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob(pub String);

impl StableName for Blob {
    const STABLE_NAME: &'static str = "Blob";
}

impl DurableOutput for Blob {
    fn serialize_reference(&self) -> String {
        format!("blob://{}", self.0)
    }
    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        reference
            .strip_prefix("blob://")
            .map(|s| Blob(s.to_string()))
            .ok_or_else(|| RehydrateError::corruption("malformed blob reference"))
    }
}

/// A stable-named unit payload for the effect-only nodes' edges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unit;
impl StableName for Unit {
    const STABLE_NAME: &'static str = "Unit";
}

// ===========================================================================
// Fake tasks (declarative — no real work).
// ===========================================================================

/// The durable stage-boundary producer: emits the distinctive [`STAGE_VALUE`] blob.
pub struct Expensive;
impl StableName for Expensive {
    const STABLE_NAME: &'static str = "Expensive";
}
impl Task for Expensive {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        Ok(Blob(STAGE_VALUE.to_string()))
    }
}

/// The in-memory (non-durable) producer, covered by the teardown.
pub struct InMemory;
impl StableName for InMemory {
    const STABLE_NAME: &'static str = "InMemory";
}
impl Task for InMemory {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        Ok(Blob("IN-MEM".to_string()))
    }
}

/// A shared capture for what the consumer **received** — a distinctive value that a
/// non-vacuous test traces back to the rehydrated durable reference (never a constant
/// next to the assertion). Written by the consumer's task body, read by the caller
/// after the resumed drive returns.
pub type ConsumerCapture = Arc<std::sync::Mutex<Option<String>>>;

/// The consumer that demands `expensive`'s value. It records the content of the blob
/// it **received** into a shared capture (so a test can trace the exact rehydrated
/// value end-to-end), then passes the value through.
pub struct Consumer {
    /// Where to record the received blob's content, if capturing.
    pub received: Option<ConsumerCapture>,
}
impl StableName for Consumer {
    const STABLE_NAME: &'static str = "Consumer";
}
impl Task for Consumer {
    type Input = Blob;
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, i: Blob) -> Result<Blob, TaskError> {
        if let Some(cap) = &self.received {
            *cap.lock().unwrap() = Some(i.0.clone());
        }
        Ok(i)
    }
}

/// An effect-only source (unit output) — used for `publish` and `cleanup`.
pub struct Effect;
impl StableName for Effect {
    const STABLE_NAME: &'static str = "Effect";
}
impl Task for Effect {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        Ok(Unit)
    }
}

/// The teardown node covering `inmem` (unit output).
pub struct Teardown;
impl StableName for Teardown {
    const STABLE_NAME: &'static str = "Teardown";
}
impl Task for Teardown {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        Ok(Unit)
    }
}

/// The scratch-checkpointing node. On a **first** run (the kill scenario) it writes
/// its high-water mark, signals mid-run readiness on disk, then holds until killed
/// (`hold` set). On a **resume** it re-executes and returns the checkpoint it read
/// (`hold` clear), so a caller can observe that its carried-forward scratch was
/// visible. It reads no wall clock: the hold spins on the cancellation signal only,
/// and is bounded so a non-killing regression cannot hang.
pub struct Checkpoint {
    /// The on-disk marker to signal mid-run readiness (the parent kills once it
    /// appears). `None` on a resume (no kill, no marker).
    pub ready_marker: Option<PathBuf>,
    /// Whether to hold (spin) until killed after writing the checkpoint — set on the
    /// first (kill) run, clear on resume.
    pub hold: bool,
}
impl StableName for Checkpoint {
    const STABLE_NAME: &'static str = "Checkpoint";
}
impl Task for Checkpoint {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        // Read the checkpoint FIRST (through the ordinary scratch context): on a
        // resume it observes the carried-forward mark; on the first run it is absent.
        let observed = c.scratch().get(CHECKPOINT_KEY)?;
        if observed.is_none() {
            // First attempt of this node in this run: write the high-water mark so a
            // later resume can carry it forward.
            c.scratch().put(CHECKPOINT_KEY, CHECKPOINT_MARK)?;
        }
        if self.hold {
            // Signal mid-run readiness on disk (atomically) so the parent kills once
            // the durable stage succeeded AND this node's scratch is written.
            if let Some(marker) = &self.ready_marker {
                signal_ready(marker);
            }
            // Hold until killed — spin on the cancellation signal only (no wall
            // clock), bounded so a regression that never kills cannot hang forever.
            for _ in 0..2_000_000_000u64 {
                if c.cancellation().is_cancelled() {
                    return Err(TaskError::permanent("stopped on cancellation"));
                }
                std::hint::spin_loop();
            }
        }
        Ok(Unit)
    }
}

/// Write the on-disk `ready` marker atomically (write + rename), so the parent
/// observes a complete file the instant it appears.
fn signal_ready(marker: &std::path::Path) {
    let tmp = marker.with_extension("tmp");
    if std::fs::write(&tmp, b"ready").is_ok() {
        let _ = std::fs::rename(&tmp, marker);
    }
}

// ===========================================================================
// Assembly — the reference pipeline (identical for kill/resume + review).
// ===========================================================================

/// Assemble the reference pipeline (the structure the review fixture snapshots
/// and the graph the kill/resume scenario drives). `group_of` supplies each node's
/// group label so the review scenario can rename a group without touching the
/// fingerprint; pass [`base_groups`] for the canonical layout.
///
/// `consumer_from` chooses which producer `consumer` demands — it is always
/// [`Expensive`](ConsumerFrom::Expensive) for the reference pipeline; the review's
/// rewiring variant redirects it to [`InMemory`](ConsumerFrom::InMemory) (a pure edge
/// rewire over an unchanged node set).
#[must_use]
pub fn assemble(
    group_of: impl Fn(&str) -> Option<&'static str>,
    consumer_from: ConsumerFrom,
) -> Pipeline {
    let mut flow = Flow::new();

    // The durable stage boundary + the in-memory producer (both in `stage`).
    let expensive = flow.register_source_named_durable::<Expensive>(
        "expensive",
        &Expensive,
        group_of("expensive"),
        NodePolicy::new(),
    );
    let inmem = flow.register_source_named::<InMemory>(
        "inmem",
        &InMemory,
        group_of("inmem"),
        NodePolicy::new(),
    );

    // The scratch-checkpointing node, ordered after `expensive`.
    let checkpoint = flow.register_source_named_ordered_after::<Checkpoint>(
        "checkpoint",
        &Checkpoint {
            ready_marker: None,
            hold: false,
        },
        &[expensive.ordering()],
        group_of("checkpoint"),
        NodePolicy::new(),
    );

    // The consumer demands one producer's Blob and is ordered after `checkpoint`
    // (so it is pending at a mid-run kill).
    let demanded = match consumer_from {
        ConsumerFrom::Expensive => expensive,
        ConsumerFrom::InMemory => inmem,
    };
    let _consumer = flow.register_named_ordered_after::<Consumer, _>(
        "consumer",
        &Consumer { received: None },
        demanded,
        &[checkpoint.ordering()],
        group_of("consumer"),
        NodePolicy::new(),
    );

    // The cleanup-after-publish shape: `publish` (ordering-only, undemanded) and
    // `cleanup` ordered after it (and after `checkpoint`, so it is pending at kill).
    let publish = flow.register_source_named::<Effect>(
        "publish",
        &Effect,
        group_of("publish"),
        NodePolicy::new(),
    );
    let _cleanup = flow.register_source_named_ordered_after::<Effect>(
        "cleanup",
        &Effect,
        &[publish.ordering(), checkpoint.ordering()],
        group_of("cleanup"),
        NodePolicy::new(),
    );

    // The teardown covering the `inmem` producer.
    let _teardown = flow.register_teardown_named(
        "teardown",
        &Teardown,
        &[inmem.ordering()],
        NodePolicy::new(),
    );

    flow.finish()
}

/// Which producer the `consumer` node demands — the reference pipeline uses
/// [`Expensive`](ConsumerFrom::Expensive); the review rewiring variant uses
/// [`InMemory`](ConsumerFrom::InMemory) over an otherwise-identical node set.
#[derive(Debug, Clone, Copy)]
pub enum ConsumerFrom {
    /// `consumer` demands the durable `expensive` producer (the reference wiring).
    Expensive,
    /// `consumer` demands the in-memory `inmem` producer (the rewired variant).
    InMemory,
}

/// The canonical group layout for the reference pipeline.
#[must_use]
pub fn base_groups(name: &str) -> Option<&'static str> {
    match name {
        "expensive" | "inmem" | "checkpoint" => Some("stage"),
        "consumer" | "publish" | "cleanup" => Some("publish"),
        _ => None,
    }
}

// ===========================================================================
// Type-erased runners for the real drive() loop.
// ===========================================================================

/// A source runner over the real caught attempt path. Optionally reports a
/// **durable reference** for its succeeded attempt (the driver seam records it),
/// so the durable stage boundary's reference lands in the folded artifact.
struct SourceRunner<T: Task<Input = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<T::Output>>,
    /// The durable reference to report post-success (the value the task's output
    /// serialized), or `None` for a non-durable node.
    durable_reference: Option<String>,
}
impl<T: Task<Input = ()>> NodeRunner for SourceRunner<T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("source runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
    fn durable_reference(&self) -> Option<String> {
        self.durable_reference.clone()
    }
}

/// An owned no-input adapter over a one-input task, so the real single-attempt
/// runner drives a data consumer over a value read from its upstream slot.
struct Bound<U, T> {
    inner: T,
    input: Option<U>,
}
impl<U: Send + Sync + 'static, T: Task<Input = U>> Task for Bound<U, T> {
    type Input = ();
    type Output = T::Output;
    const EXECUTION_CLASS: dagr_core::task::ExecutionClass = T::EXECUTION_CLASS;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<T::Output, TaskError> {
        let input = self.input.take().expect("bound input present");
        self.inner.run(c, input).await
    }
}

/// A one-input data runner: reads its upstream slot, binds the value, and drives the
/// real single-attempt runner over it.
struct MapRunner<U: Send + Sync + 'static, T: Task<Input = U>> {
    name: String,
    task: Option<T>,
    upstream: SlotRef<U>,
    slot: Arc<Slot<T::Output>>,
}
impl<U, T> NodeRunner for MapRunner<U, T>
where
    U: Send + Sync + Clone + 'static,
    T: Task<Input = U>,
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
        let task = self.task.take().expect("map runner runs once");
        let slot = Arc::clone(&self.slot);
        let input = (*self.upstream.read()).clone();
        let mut bound = Bound {
            inner: task,
            input: Some(input),
        };
        Box::pin(async move {
            run_attempt(&mut bound, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

/// The knobs one demo `drive()` run needs beyond the assembled pipeline — the
/// checkpoint hold/marker (first run), the satisfied-from-prior `omit` set + the
/// rehydrated `prefill` slots (resume), and the optional consumer capture.
pub struct DemoRun<'a> {
    /// Arm the checkpoint's mid-run hold (first/kill run only).
    pub hold_checkpoint: bool,
    /// The ready marker the checkpoint signals before holding (first run only).
    pub ready_marker: Option<PathBuf>,
    /// Nodes NOT to include (the satisfied-from-prior set — they never re-run).
    pub omit: &'a [&'a str],
    /// Producers whose slot is pre-filled by rehydration (resume), so a re-running
    /// consumer reads the value without the producer re-executing.
    pub prefill: &'a BTreeMap<String, Blob>,
    /// Where the consumer records the blob it received (for a non-vacuous
    /// rehydration assertion), or `None`.
    pub consumer_capture: Option<ConsumerCapture>,
}

/// The slots the demo's nodes write to, keyed by name — Blob-output producers and
/// Unit-output effect nodes, built with each node's real consumer count.
struct DemoSlots {
    expensive: Arc<Slot<Blob>>,
    inmem: Arc<Slot<Blob>>,
    consumer: Arc<Slot<Blob>>,
    checkpoint: Arc<Slot<Unit>>,
    publish: Arc<Slot<Unit>>,
    cleanup: Arc<Slot<Unit>>,
    teardown: Arc<Slot<Unit>>,
}

impl DemoSlots {
    /// Build every node's slot. `demanded` is the producer the consumer reads (so it
    /// carries one consumer for its residency count) when the consumer is present.
    fn new(demanded: &str, consumer_present: bool) -> Self {
        let ledger = ResidencyLedger::new();
        let blob = |name: &str, consumers: u32| {
            Arc::new(Slot::new(
                NodeId::from_name(name),
                name,
                consumers,
                false,
                0,
                Arc::clone(&ledger),
            ))
        };
        let unit = |name: &str| {
            Arc::new(Slot::new(
                NodeId::from_name(name),
                name,
                0,
                false,
                0,
                Arc::clone(&ledger),
            ))
        };
        let demand = |name: &str| u32::from(name == demanded && consumer_present);
        Self {
            expensive: blob("expensive", demand("expensive")),
            inmem: blob("inmem", demand("inmem")),
            consumer: blob("consumer", 0),
            checkpoint: unit("checkpoint"),
            publish: unit("publish"),
            cleanup: unit("cleanup"),
            teardown: unit("teardown"),
        }
    }
}

/// Build the demo's runner map + ordering map for `drive()` over the assembled
/// pipeline. See [`DemoRun`] for the per-run knobs.
///
/// # Panics
///
/// Panics on a demo-authoring error only: a rehydrated producer slot that is somehow
/// already filled — a mistake in the demo's own wiring, surfaced loudly.
#[must_use]
pub fn build_runner_set(
    pipeline: &Pipeline,
    consumer_from: ConsumerFrom,
    run: &DemoRun<'_>,
) -> (RunPlan, BTreeMap<String, Vec<String>>) {
    let omit: &[&str] = run.omit;
    let include = |name: &str| !omit.contains(&name);
    let demanded = match consumer_from {
        ConsumerFrom::Expensive => "expensive",
        ConsumerFrom::InMemory => "inmem",
    };
    let s = DemoSlots::new(demanded, include("consumer"));

    // Pre-fill any rehydrated producer slot so a re-running consumer reads the
    // rehydrated value WITHOUT the producer re-executing (the resume seam).
    for (node, value) in run.prefill {
        let slot = match node.as_str() {
            "expensive" => &s.expensive,
            "inmem" => &s.inmem,
            _ => continue,
        };
        slot.fill(value.clone())
            .expect("a rehydrated producer slot fills exactly once");
    }

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    let mut add = |name: &str, runner: Box<dyn NodeRunner>| {
        if include(name) {
            runners.insert(name.to_string(), runner);
        }
    };
    add(
        "expensive",
        blob_source("expensive", Expensive, Arc::clone(&s.expensive), true),
    );
    add(
        "inmem",
        blob_source("inmem", InMemory, Arc::clone(&s.inmem), false),
    );
    add(
        "publish",
        unit_source("publish", Effect, Arc::clone(&s.publish)),
    );
    add(
        "cleanup",
        unit_source("cleanup", Effect, Arc::clone(&s.cleanup)),
    );
    add(
        "checkpoint",
        unit_source(
            "checkpoint",
            Checkpoint {
                ready_marker: run.ready_marker.clone(),
                hold: run.hold_checkpoint,
            },
            Arc::clone(&s.checkpoint),
        ),
    );
    let upstream = if demanded == "inmem" {
        s.inmem.shared_ref()
    } else {
        s.expensive.shared_ref()
    };
    add(
        "consumer",
        Box::new(MapRunner {
            name: "consumer".into(),
            task: Some(Consumer {
                received: run.consumer_capture.clone(),
            }),
            upstream,
            slot: Arc::clone(&s.consumer),
        }),
    );
    // The teardown runner is always present (the driver runs it in the teardown
    // phase regardless of the must-run subset), so it is inserted unconditionally.
    runners.insert(
        "teardown".into(),
        unit_source("teardown", Teardown, Arc::clone(&s.teardown)),
    );

    let ordering = build_ordering(&include);
    (
        RunPlan::with_ordering(pipeline.clone(), runners, ordering.clone()),
        ordering,
    )
}

/// A Blob-output source runner; `durable` records the [`Expensive`] reference.
fn blob_source<T>(name: &str, task: T, slot: Arc<Slot<Blob>>, durable: bool) -> Box<dyn NodeRunner>
where
    T: Task<Input = (), Output = Blob> + Send + 'static,
{
    Box::new(SourceRunner {
        name: name.to_string(),
        task: Some(task),
        slot,
        durable_reference: durable.then(|| Blob(STAGE_VALUE.to_string()).serialize_reference()),
    })
}

/// A Unit-output (effect / checkpoint / teardown) source runner — never durable.
fn unit_source<T>(name: &str, task: T, slot: Arc<Slot<Unit>>) -> Box<dyn NodeRunner>
where
    T: Task<Input = (), Output = Unit> + Send + 'static,
{
    Box::new(SourceRunner {
        name: name.to_string(),
        task: Some(task),
        slot,
        durable_reference: None,
    })
}

/// The run-level ordering upstreams the driver seeds into the readiness tracker for
/// the demo's consume-nothing / effect nodes — only for nodes present in this run.
fn build_ordering(include: &impl Fn(&str) -> bool) -> BTreeMap<String, Vec<String>> {
    let mut ordering: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (node, ups) in [
        ("checkpoint", &["expensive"][..]),
        ("consumer", &["checkpoint"][..]),
        ("cleanup", &["publish", "checkpoint"][..]),
    ] {
        if !include(node) {
            continue;
        }
        let present: Vec<String> = ups
            .iter()
            .filter(|u| include(u))
            .map(|u| (*u).to_string())
            .collect();
        if !present.is_empty() {
            ordering.insert(node.to_string(), present);
        }
    }
    ordering
}
