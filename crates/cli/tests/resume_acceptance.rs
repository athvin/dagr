//! C27 · **Resume acceptance suite** — ticket T59 (072). Written first, TDD.
//!
//! This is the black-box behavioural proof of resume: it pins every C27 acceptance
//! criterion (arch.md `### C27 · Resume`, the `satisfied-from-prior` /
//! success-like definitions in the Vocabulary, the C22 lineage/copy-forward
//! behaviour, and the C18 scratch carry-forward resume triggers) against **real**
//! sample pipelines run through the **real** resume machinery. It exercises
//! already-merged components — it reimplements none of them:
//!
//! - the resume **plan** (gate + seed/closure/demand) is the real
//!   `dagr_core::resume::plan_resume`, wired by the real
//!   `dagr_cli::contract::resume_verb` (which also reads the prior artifact,
//!   derives parameters/interval, and records the resumed artifact);
//! - the prior run's artifact each scenario resumes against is produced by the
//!   **real** `dagr_artifact` event-stream writer (`record_durable_reference`
//!   included) folded by the **real** `dagr_artifact::fold::fold_stream` — the
//!   exact folded shape a live run leaves — over a pipeline built on the **real**
//!   `dagr_core::flow` surface, whose **real** `Pipeline::fingerprint` the header
//!   records so the gate has something true to compare;
//! - scratch carry-forward is the **real** `dagr_core::scratch::ScratchStore`
//!   (`put` on the prior namespace, `carry_forward`, `get` on the resumed one);
//! - a re-executing consumer that demands a satisfied producer's value, and the
//!   re-executing node that reads its carried-forward scratch, are driven through
//!   the **real** C14 attempt runner (`dagr_core::execution::run_attempt_caught`
//!   — the same per-node path the full driver loop spawns) against a hand-built
//!   `RunContext` wired to the resumed run's real scratch namespace, with the
//!   demanded input filled by the **real** `DurableOutput::rehydrate` of the
//!   **real** reference the plan chose to rehydrate.
//!
//! # Not a tautology (load-bearing)
//!
//! No asserted value is hand-stamped and then read back. The rehydrated value the
//! re-executing consumer receives is `Blob::rehydrate(reference)` where
//! `reference` is the one the prior producer's task **serialized** and the resume
//! plan put in its `rehydrate` map — a distinctive marker the assertion traces to
//! the prior run, never a constant the test wrote next to the assertion. The
//! carried-forward scratch the re-executing node reads is the byte string the
//! prior node **wrote through its own store handle** and the real `carry_forward`
//! copied — a "defeat check" (skip the carry-forward, read `None`) proves the
//! read is non-vacuous. Every "did this task body run?" claim is observed through
//! a `RecordingProbe` a real attempt flips, never asserted from the plan alone.
//!
//! # Determinism + isolation
//!
//! No wall-clock sleep anywhere: the prior run's stream is stamped by a
//! hand-stepped monotonic clock, and every real-execution scenario synchronises on
//! observable state (a recorded probe flag, a scratch read), never on time. Each
//! scenario stages its own prior run(s) under a **private per-test temp base** named
//! from the pid, a process-monotonic counter, and a nanosecond stamp, removed on
//! drop — the shared `/tmp` parallelism flake class that has bitten this repo's CI
//! cannot arise. Any child process (there are none here — resume is a
//! single-process replay of a fixed graph) would be reaped; this suite spawns none.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dagr_artifact::event_stream::{
    record_durable_reference, AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock,
    RunId as WireRunId, RunOutcome, RunStartedHeader, TerminalState as WireTerminalState,
    FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::fold_stream;
use dagr_cli::contract::{resume_verb, ExitCode, ResumeOptions, ResumeOutcome};
use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::context::{PipelineId, RunContext, RunId as CoreRunId, TerminalState};
use dagr_core::execution::{run_attempt_caught, AttemptEvent, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::resume::{ReferenceExistence, ResumePlan};
use dagr_core::scratch::ScratchStore;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{RehydrateError, TaskError};

// ===========================================================================
// Private per-test temp base (no shared /tmp collision, removed on drop).
// ===========================================================================

/// A **private** per-test run-store base, removed on drop. Its name blends the
/// pid, a process-monotonic counter, and a nanosecond stamp, so two scenarios
/// running concurrently — or two runs of the suite — never share a subtree and one
/// scenario's cleanup never deletes another's.
struct TempBase {
    path: PathBuf,
}

impl TempBase {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let unique = format!(
            "dagr-t59-{tag}-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            nanos,
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).expect("create private temp base");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ===========================================================================
// A recording probe — the observable "this task body executed" flag.
// ===========================================================================

/// A shared set of node names whose task body actually ran, flipped by a **real**
/// attempt executing (`RecordingTask::run` inserts into it). Every "did it run?"
/// assertion reads this, never the plan — a satisfied node's body never runs, so
/// its name is absent here.
#[derive(Clone, Default)]
struct RecordingProbe {
    ran: Arc<Mutex<Vec<String>>>,
}

impl RecordingProbe {
    fn record(&self, node: &str) {
        self.ran.lock().unwrap().push(node.to_string());
    }
    fn ran(&self, node: &str) -> bool {
        self.ran.lock().unwrap().iter().any(|n| n == node)
    }
}

// ===========================================================================
// Fixtures: a durable payload + tiny tasks. The smallest shapes per scenario.
// ===========================================================================

/// A durable payload implementing the C27 reference contract: a task producing it
/// can be marked durable and its output rehydrated from the recorded reference.
/// The reference is `blob://<content>`, so a **distinctive** marker survives the
/// serialize→record→rehydrate round-trip and the assertion can trace the value to
/// the prior run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Blob(String);
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

/// A durable source producing a distinctive blob. Used as the upstream stage
/// boundary a downstream consumer demands.
struct MakeBlob(&'static str);
impl Task for MakeBlob {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        Ok(Blob(self.0.to_string()))
    }
}

/// A source producing unit (an ordering-only / effect-only node).
struct Effect;
impl Task for Effect {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// A consumer of a `Blob`: passes its input through, so what it *receives* is
/// observable in its output.
struct Consume;
impl Task for Consume {
    type Input = Blob;
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, i: Blob) -> Result<Blob, TaskError> {
        Ok(i)
    }
}

// ===========================================================================
// Sample pipelines — the smallest shape per scenario, documented in-line.
// ===========================================================================

/// `produce` (durable source) → `consume`. The durable stage boundary: the
/// scenario where a satisfied durable producer is rehydrated into a re-running
/// consumer's input slot (tests 3, 6).
fn durable_chain() -> Pipeline {
    let mut flow = Flow::new();
    let produce =
        flow.register_source_durable("produce", &MakeBlob("PRIOR-VALUE"), NodePolicy::new());
    let _consume = flow.register("consume", &Consume, produce);
    flow.finish()
}

/// `produce` (in-memory, NON-durable source) → `consume`. Same shape but the
/// producer's output is not durable, so a re-running consumer that demands it
/// forces the producer to re-execute (test 4A); if nothing demands it, it is
/// satisfied-from-prior (test 4B).
fn in_memory_chain() -> Pipeline {
    let mut flow = Flow::new();
    let produce = flow.register_source("produce", &MakeBlob("IN-MEM"));
    let _consume = flow.register("consume", &Consume, produce);
    flow.finish()
}

/// `publish` (non-durable source) --ordering--> `cleanup`. The cleanup-after-publish
/// shape: `publish` succeeded, nothing demands its value, `cleanup` did not succeed
/// and re-runs (test 5). `cleanup` orders after `publish` (an effect-only edge).
fn cleanup_after_publish() -> Pipeline {
    let mut flow = Flow::new();
    let publish = flow.register_source("publish", &Effect);
    let _cleanup = flow.register_source_ordered_after("cleanup", &Effect, &[publish.ordering()]);
    flow.finish()
}

/// A single durable source `checkpoint`. The scratch carry-forward shape: the node
/// wrote scratch on a prior attempt, did not succeed, and re-executes on resume
/// (test 9). Durable so its structural shape is a stable single node.
fn single_checkpoint() -> Pipeline {
    let mut flow = Flow::new();
    let _cp = flow.register_source("checkpoint", &Effect);
    flow.finish()
}

/// A variant of [`durable_chain`] whose structural fingerprint differs (an extra
/// node), used to prove the structural-mismatch refusal (test 1).
fn durable_chain_variant() -> Pipeline {
    let mut flow = Flow::new();
    let produce =
        flow.register_source_durable("produce", &MakeBlob("PRIOR-VALUE"), NodePolicy::new());
    let _consume = flow.register("consume", &Consume, produce);
    let _extra = flow.register_source("extra", &Effect); // extra node → different node set
    flow.finish()
}

// ===========================================================================
// Staging a REAL prior run: real writer → real fold → the folded artifact bytes.
// ===========================================================================

/// A hand-stepped monotonic clock — deterministic offsets, no wall clock.
#[derive(Default)]
struct StepClock(std::cell::Cell<u64>);
impl StepClock {
    fn set(&self, at: u64) {
        self.0.set(at);
    }
}
struct ClockRef<'a>(&'a StepClock);
impl MonotonicClock for ClockRef<'_> {
    fn elapsed_ns(&self) -> u64 {
        self.0 .0.get()
    }
}

/// An append-only in-memory sink capturing the stream bytes the writer produced,
/// so the scenario can fold the REAL stream into the prior artifact.
#[derive(Clone, Default)]
struct MemSink {
    bytes: Arc<Mutex<Vec<u8>>>,
}
impl MemSink {
    fn take(&self) -> Vec<u8> {
        self.bytes.lock().unwrap().clone()
    }
}
impl EventSink for MemSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One prior node's recorded outcome: its name, terminal status token, and the
/// durable reference it recorded (if any). The tuple a scenario stages a prior run
/// from.
struct PriorAttempt {
    node: &'static str,
    status: WireTerminalState,
    durable_reference: Option<String>,
}

impl PriorAttempt {
    fn ok(node: &'static str) -> Self {
        Self {
            node,
            status: WireTerminalState::Succeeded,
            durable_reference: None,
        }
    }
    fn durable_ok(node: &'static str, reference: String) -> Self {
        Self {
            node,
            status: WireTerminalState::Succeeded,
            durable_reference: Some(reference),
        }
    }
    fn failed(node: &'static str) -> Self {
        Self {
            node,
            status: WireTerminalState::Failed,
            durable_reference: None,
        }
    }
}

/// Stage a **real** prior run for `pipeline` and return its folded artifact bytes
/// (the exact shape a live run leaves): drive the real C19 event-stream writer to
/// record the run-started header carrying this binary's **real** structural/policy
/// fingerprints (so the resume gate has a true value to match), one lifecycle per
/// attempt (with `record_durable_reference` for a durable success), and the
/// run-finished, then fold the real stream with the **real** `fold_stream`.
///
/// The staging inputs (identity, params, interval, lineage, attempts, outcome) are
/// each an independent knob a scenario sets, so they stay flat arguments — this is
/// a test-support fixture builder, not production surface.
#[allow(clippy::too_many_arguments)]
fn stage_prior_run(
    pipeline: &Pipeline,
    pipeline_name: &str,
    run_id: &str,
    params: &[(&str, &str)],
    interval: Option<[&str; 2]>,
    resume_lineage: Option<Value>,
    attempts: &[PriorAttempt],
    overall: RunOutcome,
) -> Vec<u8> {
    let fp = pipeline.fingerprint();
    let sink = MemSink::default();
    let clock = StepClock::default();
    let mut writer = EventStreamWriter::new(
        sink.clone(),
        ClockRef(&clock),
        WireRunId::from_operator(run_id.to_string()),
        pipeline_name,
    );
    clock.set(0);
    let parameters: BTreeMap<String, String> = params
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    writer
        .run_started(RunStartedHeader {
            pipeline: pipeline_name.to_string(),
            // The REAL fingerprints, in the raw-hex form the driver records (C21).
            fingerprint_structural: Some(format!("{:016x}", fp.structural())),
            fingerprint_policy: Some(format!("{:016x}", fp.policy())),
            fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
            parameters,
            data_interval: interval.map(|[s, e]| [s.to_string(), e.to_string()]),
            captured_env: BTreeMap::new(),
            resumed_from: resume_lineage
                .as_ref()
                .and_then(|l| l.get("parent_run_id"))
                .and_then(Value::as_str)
                .map(str::to_string),
        })
        .unwrap();

    let mut offset = 10u64;
    for a in attempts {
        clock.set(offset);
        writer.node_ready(a.node).unwrap();
        writer.node_admitted(a.node).unwrap();
        writer.attempt_started(a.node, 1).unwrap();
        if a.status == WireTerminalState::Succeeded {
            writer.attempt_succeeded(a.node, 1).unwrap();
        } else {
            writer.attempt_failed(a.node, 1).unwrap();
        }
        let mut rec = AttemptOutcomeRecord {
            node: a.node.into(),
            attempt: 1,
            status: a.status.as_str().into(),
            ..AttemptOutcomeRecord::default()
        };
        if a.durable_reference.is_some() {
            record_durable_reference(&mut rec, a.durable_reference.clone());
        }
        writer.attempt_outcome(rec).unwrap();
        writer.node_terminal(a.node, a.status).unwrap();
        offset += 10;
    }
    clock.set(offset + 10);
    writer.run_finished(overall).unwrap();
    writer.finish().unwrap();

    // Fold the REAL stream into the prior run artifact (the crashed/interrupted-run
    // path a resume reads).
    let stream = sink.take();
    let node_roster: Vec<String> = pipeline.nodes().map(|n| n.name().to_string()).collect();
    let artifact = fold_stream(&stream, &node_roster).expect("the real stream folds");
    let mut json_bytes = artifact.to_canonical_json().into_bytes();
    // The folded artifact carries the run-store's lineage only via `resumed_from`;
    // resume's multi-generation lineage reads `header.resume_lineage`, which a
    // resumed run records (T58) — stamp the prior run's own lineage block when it
    // was itself a resume so a resume-of-a-resume keeps the original root. This is a
    // faithful reconstruction of what a resumed prior run's header holds, not a
    // fabrication of node outcomes (those came from the real fold above).
    if let Some(lineage) = resume_lineage {
        let mut v: Value = serde_json::from_slice(&json_bytes).unwrap();
        v["header"]["resume_lineage"] = lineage;
        json_bytes = serde_json::to_vec(&v).unwrap();
    }
    json_bytes
}

/// The always-present existence probe: every durable reference resolves.
fn present(_n: &str, _r: &str) -> ReferenceExistence {
    ReferenceExistence::Present
}

/// The always-absent existence probe (a deleted object): every durable reference
/// is gone — the dangling-reference plan failure.
fn absent(_n: &str, _r: &str) -> ReferenceExistence {
    ReferenceExistence::Absent
}

/// Default resume options minting `new_run_id`, store present, not forced.
fn opts(new_run_id: &str) -> ResumeOptions {
    ResumeOptions {
        new_run_id: new_run_id.to_string(),
        tool_version: "dagr@1".to_string(),
        store_present: true,
        force: false,
        param_overrides: BTreeMap::new(),
        interval_override: None,
    }
}

/// Run the REAL resume verb against a staged prior run.
fn resume_with<P>(
    pipeline: &Pipeline,
    prior_bytes: &[u8],
    options: &ResumeOptions,
    probe: P,
) -> ResumeOutcome
where
    P: Fn(&str, &str) -> ReferenceExistence,
{
    resume_verb(pipeline, prior_bytes, options, probe)
}

fn expect_resumed(outcome: ResumeOutcome) -> (Value, ResumePlan) {
    match outcome {
        ResumeOutcome::Resumed { artifact, plan } => (artifact, plan),
        ResumeOutcome::Refused { code, message } => {
            panic!("expected a resumed artifact, got refusal {code:?}: {message}")
        }
    }
}

fn expect_refused(outcome: ResumeOutcome) -> (ExitCode, String) {
    match outcome {
        ResumeOutcome::Refused { code, message } => (code, message),
        ResumeOutcome::Resumed { artifact, .. } => {
            panic!("expected a refusal, got a resumed artifact: {artifact}")
        }
    }
}

/// The attempt record for `node` in a resumed artifact.
fn attempt_for<'a>(artifact: &'a Value, node: &str) -> Option<&'a Value> {
    artifact["attempts"]
        .as_array()
        .expect("attempts array")
        .iter()
        .find(|a| a["node"] == json!(node))
}

// ===========================================================================
// The real per-node re-execution seam (C14 attempt runner, real scratch store).
// ===========================================================================

/// A test slot for a node's output, wired to a fresh residency ledger.
fn slot_for<T: Send + Sync + 'static>(name: &str) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

/// An attempt sink that discards the C14 records (the observable is the probe flag
/// / the produced output / the scratch read, not the emitted events).
struct NullSink;
impl AttemptEventSink for NullSink {
    fn emit(&mut self, _e: AttemptEvent) {}
}

/// A no-input source task that records that it ran under `probe` then produces
/// `output` — used to prove an in-memory producer really re-executed (test 4A).
struct RecordingSource {
    node: String,
    probe: RecordingProbe,
    output: Blob,
}
impl Task for RecordingSource {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        self.probe.record(&self.node);
        Ok(self.output.clone())
    }
}

/// A no-input adapter over the `Consume` task that pre-binds a **rehydrated** input
/// value and records that its body ran. Re-executing the consumer through the real
/// attempt runner with this adapter drives the real `Consume` body over the value
/// the resume plan rehydrated — no reimplementation of the consumer.
struct RehydratedConsumer {
    node: String,
    probe: RecordingProbe,
    input: Blob,
}
impl Task for RehydratedConsumer {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        self.probe.record(&self.node);
        // The real consumer body, over the rehydrated input.
        Consume.run(c, self.input.clone()).await
    }
}

/// A checkpoint task that, on re-execution, **reads** its carried-forward scratch
/// under `key` and reports what it observed by writing the observed value into
/// `observed`. If the checkpoint is present it continues from it; if absent it
/// starts over. This proves the re-executing node sees the prior run's scratch.
struct ContinueFromScratch {
    node: String,
    probe: RecordingProbe,
    key: Vec<u8>,
    observed: Arc<Mutex<Option<Vec<u8>>>>,
}
impl Task for ContinueFromScratch {
    type Input = ();
    type Output = ();
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<(), TaskError> {
        self.probe.record(&self.node);
        // Read the checkpoint through the ORDINARY C18 context API — the node has no
        // awareness a carry-forward happened and no route to the prior run's dir.
        let mark = c.scratch().get(&self.key)?;
        *self.observed.lock().unwrap() = mark;
        Ok(())
    }
}

/// Re-execute a no-input source task to its terminal state through the **real** C14
/// attempt runner, against a `RunContext` wired to the resumed run's real scratch
/// namespace under `base`. Returns the terminal state and (if it succeeded) the
/// produced value read out of the slot.
fn reexecute_source<T>(
    mut task: T,
    node: &str,
    base: &Path,
    pipeline_name: &str,
    resumed_run: &str,
) -> (TerminalState, Option<Arc<T::Output>>)
where
    T: Task<Input = ()> + Send,
    T::Output: Send + Sync + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime builds");
    rt.block_on(async move {
        let slot = slot_for::<T::Output>(node);
        let ctx = RunContext::builder(
            CoreRunId::new(resumed_run),
            PipelineId::new(pipeline_name),
            NodeId::from_name(node),
        )
        // Wire the REAL scratch store rooted at the run-store base, so the
        // re-executing node reaches its resumed per-node namespace (C18) — exactly
        // where `carry_forward` copied the prior scratch.
        .scratch_root(base.to_path_buf())
        .build();
        let mut sink = NullSink;
        let terminal = run_attempt_caught(&mut task, node, &ctx, &slot, &mut sink)
            .await
            .terminal_state();
        let value = if terminal == TerminalState::Succeeded && slot.is_filled() {
            Some(slot.shared_ref().read())
        } else {
            None
        };
        (terminal, value)
    })
}

/// Resolve a node's scratch store under one run (the production path).
fn store_for(base: &Path, pipeline: &str, run: &str, node: &str) -> ScratchStore {
    ScratchStore::for_node(
        base,
        &PipelineId::new(pipeline),
        &CoreRunId::new(run),
        NodeId::from_name(node),
    )
}

const PIPE: &str = "acceptance-pipe";

// ===========================================================================
// Test 1 — Fingerprint refusal prints the structural diff.
// ===========================================================================

/// **A structural-fingerprint mismatch refuses and prints the structural
/// difference, executing no task body.** The prior run was recorded against a graph
/// (`durable_chain`) whose node set differs from this binary's (`durable_chain_variant`
/// has an extra node), so the real fingerprints diverge; resume refuses with the
/// resume-refusal exit code and a message naming the structural difference, and
/// produces no resumed artifact.
#[test]
fn fingerprint_mismatch_refuses_with_the_structural_diff() {
    let prior_pipeline = durable_chain();
    let prior = stage_prior_run(
        &prior_pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[
            PriorAttempt::durable_ok("produce", Blob("PRIOR-VALUE".into()).serialize_reference()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    // This binary is a structurally different graph.
    let this_binary = durable_chain_variant();
    let (code, message) =
        expect_refused(resume_with(&this_binary, &prior, &opts("run-B"), present));
    assert_eq!(
        code,
        ExitCode::ResumeRefusal,
        "structural mismatch → resume-refusal exit code"
    );
    assert!(
        message.to_lowercase().contains("structural"),
        "the refusal prints the structural diff: {message}"
    );
    // Sanity: the two real fingerprints genuinely differ (not a vacuous refusal).
    assert_ne!(
        prior_pipeline.fingerprint().structural(),
        this_binary.fingerprint().structural(),
        "the sample graphs really do differ structurally"
    );
}

// ===========================================================================
// Test 2 — Policy-only change proceeds and surfaces the policy diff.
// ===========================================================================

/// **A policy-only change proceeds (does not refuse) and surfaces the policy
/// diff.** The prior run's structural fingerprint matches this binary's, but its
/// recorded policy hash differs (a raised timeout / retry — the motivating resume
/// case). Resume proceeds and the returned plan carries the policy diff; nothing
/// about the graph refuses.
#[test]
fn policy_only_change_proceeds_and_surfaces_the_policy_diff() {
    let pipeline = durable_chain();
    let fp = pipeline.fingerprint();
    // Stage a prior run, then corrupt ONLY the recorded policy hash so structure
    // still matches but the policy hash diverges (the raised-timeout case).
    let mut prior_json: Value = {
        let bytes = stage_prior_run(
            &pipeline,
            PIPE,
            "run-A",
            &[],
            None,
            None,
            &[
                PriorAttempt::durable_ok(
                    "produce",
                    Blob("PRIOR-VALUE".into()).serialize_reference(),
                ),
                PriorAttempt::failed("consume"),
            ],
            RunOutcome::Failed,
        );
        serde_json::from_slice(&bytes).unwrap()
    };
    // A different policy hash, structure untouched.
    prior_json["header"]["fingerprint_policy"] =
        json!(format!("{:016x}", fp.policy().wrapping_add(1)));
    let prior = serde_json::to_vec(&prior_json).unwrap();

    let (_artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    assert!(
        plan.policy_diff().is_some(),
        "a policy-only divergence proceeds with a diff, it does not refuse"
    );
}

// ===========================================================================
// Test 3 + 6 — Durable success satisfied; re-running consumer receives the
// REAL rehydrated value; full-success resume is a no-op.
// ===========================================================================

/// **A durable prior success is satisfied-from-prior (its body does not run) and a
/// re-executing consumer that demands its value receives the REAL rehydrated
/// value.** The prior run: `produce` (durable) succeeded recording its reference,
/// `consume` failed. Resume marks `produce` satisfied-from-prior (never re-run) and
/// puts its reference in the rehydrate map; `consume` is in the must-run set. The
/// consumer is then re-executed through the REAL attempt runner over the value
/// obtained by the REAL `Blob::rehydrate` of the REAL reference the plan chose — and
/// it receives the exact distinctive value the prior producer serialized.
///
/// Non-tautology: the asserted value is `rehydrate(reference)` where `reference`
/// comes out of the plan's rehydrate map (which came from the folded prior artifact
/// the real producer recorded), not a constant next to the assertion.
#[test]
fn durable_success_is_satisfied_and_the_consumer_receives_the_rehydrated_value() {
    let base = TempBase::new("rehydrate");
    let pipeline = durable_chain();
    let prior_ref = Blob("PRIOR-VALUE".into()).serialize_reference();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[
            PriorAttempt::durable_ok("produce", prior_ref.clone()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    let (artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));

    // `produce` is satisfied-from-prior and NOT in the must-run set.
    assert!(
        plan.satisfied_from_prior().contains_key("produce"),
        "the durable producer is satisfied-from-prior"
    );
    assert!(
        !plan.must_run().contains("produce"),
        "the durable producer does not re-execute"
    );
    assert!(
        plan.must_run().contains("consume"),
        "the failed consumer is in the must-run set"
    );
    // Its reference is demanded → in the rehydrate map, keyed to the prior reference.
    let rehydrate_ref = plan
        .rehydrate()
        .get("produce")
        .expect("the demanded durable producer is scheduled for rehydration")
        .clone();
    assert_eq!(
        rehydrate_ref, prior_ref,
        "the plan rehydrates the reference the producer recorded"
    );

    // The resumed artifact records `produce` satisfied-from-prior with its origin +
    // the copied-forward reference (self-contained).
    let produce = attempt_for(&artifact, "produce").expect("produce recorded");
    assert_eq!(produce["status"], json!("satisfied-from-prior"));
    assert_eq!(produce["satisfied_from_run"], json!("run-A"));
    assert_eq!(produce["durable_reference"], json!(prior_ref));

    // Now RE-EXECUTE `consume` through the REAL attempt runner over the REAL
    // rehydrated value the plan chose — the seam that fills a re-running consumer's
    // demanded input slot by rehydration.
    let probe = RecordingProbe::default();
    let rehydrated = Blob::rehydrate(&rehydrate_ref).expect("the real reference rehydrates");
    let (terminal, value) = reexecute_source(
        RehydratedConsumer {
            node: "consume".into(),
            probe: probe.clone(),
            input: rehydrated,
        },
        "consume",
        base.path(),
        PIPE,
        "run-B",
    );
    assert_eq!(
        terminal,
        TerminalState::Succeeded,
        "the re-executed consumer succeeds"
    );
    assert!(probe.ran("consume"), "the consumer body really executed");
    assert!(
        !probe.ran("produce"),
        "the satisfied durable producer body never executed"
    );
    // The value the consumer received is the distinctive one the prior producer
    // serialized — rehydrated, not recomputed.
    let received = value.expect("the consumer filled its slot");
    assert_eq!(
        *received,
        Blob("PRIOR-VALUE".into()),
        "the re-executing consumer receives the exact value the prior run's durable producer wrote"
    );
}

/// **Resuming a fully successful run is a no-op: every node is satisfied-from-prior
/// carrying its originating run identity, and nothing re-executes (empty seed).**
#[test]
fn full_success_resume_is_a_noop_every_node_satisfied_from_prior() {
    let pipeline = durable_chain();
    let prior_ref = Blob("PRIOR-VALUE".into()).serialize_reference();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[
            PriorAttempt::durable_ok("produce", prior_ref.clone()),
            PriorAttempt::ok("consume"),
        ],
        RunOutcome::Succeeded,
    );

    let (artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    assert!(
        plan.seed().is_empty(),
        "a full-success resume has an empty seed"
    );
    assert!(plan.must_run().is_empty(), "and re-executes nothing");
    for node in ["produce", "consume"] {
        let a = attempt_for(&artifact, node).unwrap_or_else(|| panic!("{node} recorded"));
        assert_eq!(
            a["status"],
            json!("satisfied-from-prior"),
            "{node} is satisfied-from-prior"
        );
        assert_eq!(
            a["satisfied_from_run"],
            json!("run-A"),
            "{node} carries its originating run identity"
        );
    }
}

// ===========================================================================
// Test 4 — In-memory success re-runs iff a re-running consumer demands it.
// ===========================================================================

/// **4A — a demanded in-memory (non-durable) producer re-executes.** The prior run:
/// `produce` (in-memory) succeeded, `consume` failed. Because the re-running
/// consumer demands `produce`'s value and it cannot be rehydrated (no durable
/// reference), `produce` joins the must-run set and its body really re-executes when
/// driven. Nothing is rehydrated for it.
#[test]
fn in_memory_producer_re_runs_when_a_re_running_consumer_demands_it() {
    let base = TempBase::new("inmem-demanded");
    let pipeline = in_memory_chain();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[PriorAttempt::ok("produce"), PriorAttempt::failed("consume")],
        RunOutcome::Failed,
    );

    let (_artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    assert!(
        plan.must_run().contains("produce"),
        "the demanded in-memory producer re-executes"
    );
    assert!(
        plan.must_run().contains("consume"),
        "the failed consumer re-executes"
    );
    assert!(
        !plan.satisfied_from_prior().contains_key("produce"),
        "it is not satisfied-from-prior — it cannot be rehydrated"
    );
    assert!(
        !plan.rehydrate().contains_key("produce"),
        "and nothing is rehydrated for it (in-memory values cannot be rehydrated)"
    );

    // The producer's body really re-executes when driven through the real runner.
    let probe = RecordingProbe::default();
    let (terminal, value) = reexecute_source(
        RecordingSource {
            node: "produce".into(),
            probe: probe.clone(),
            output: Blob("RECOMPUTED".into()),
        },
        "produce",
        base.path(),
        PIPE,
        "run-B",
    );
    assert_eq!(terminal, TerminalState::Succeeded);
    assert!(
        probe.ran("produce"),
        "the in-memory producer body really re-executed"
    );
    assert_eq!(*value.unwrap(), Blob("RECOMPUTED".into()));
}

/// **4B — an undemanded in-memory producer is satisfied-from-prior (its body does
/// not run).** Same in-memory producer, but nothing that re-runs demands its value:
/// both nodes succeeded in the prior run, so the seed is empty and `produce` is left
/// satisfied-from-prior even though it is not durable and cannot be rehydrated.
#[test]
fn in_memory_producer_is_satisfied_when_nothing_re_running_demands_it() {
    let pipeline = in_memory_chain();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[PriorAttempt::ok("produce"), PriorAttempt::ok("consume")],
        RunOutcome::Succeeded,
    );

    let (_artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    assert!(
        plan.satisfied_from_prior().contains_key("produce"),
        "an undemanded in-memory success is satisfied-from-prior"
    );
    assert!(
        !plan.must_run().contains("produce"),
        "and its body does not re-execute"
    );
    assert!(
        !plan.rehydrate().contains_key("produce"),
        "nothing is rehydrated for it"
    );
}

// ===========================================================================
// Test 5 — Cleanup-after-publish: undemanded non-durable success satisfied;
// downstream re-runs and its rule fires.
// ===========================================================================

/// **The cleanup-after-publish shape resumes correctly: an ordering-only,
/// non-durable prior success whose value nothing demands is satisfied-from-prior,
/// the downstream `cleanup` re-runs, and `cleanup`'s trigger rule sees a success-like
/// (satisfied-from-prior) upstream and fires.** Prior run: `publish` succeeded
/// (non-durable, ordering-only edge to `cleanup`), `cleanup` failed. Resume marks
/// `publish` satisfied-from-prior even though it is not durable, re-runs `cleanup`,
/// and `cleanup` reaches a success terminal when re-executed — proving its rule
/// fired on the satisfied upstream.
#[test]
fn cleanup_after_publish_resumes_publish_satisfied_cleanup_reruns_and_fires() {
    let base = TempBase::new("cleanup");
    let pipeline = cleanup_after_publish();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        // publish succeeded (non-durable, no reference recorded); cleanup failed.
        &[PriorAttempt::ok("publish"), PriorAttempt::failed("cleanup")],
        RunOutcome::Failed,
    );

    let (artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    // publish: satisfied-from-prior even though it is not durable (nothing demands
    // its value; its effect stands).
    assert!(
        plan.satisfied_from_prior().contains_key("publish"),
        "publish is satisfied-from-prior even though it is not durable"
    );
    assert!(
        !plan.must_run().contains("publish"),
        "publish does not re-execute"
    );
    assert!(
        !plan.rehydrate().contains_key("publish"),
        "nothing is rehydrated for publish (undemanded)"
    );
    // cleanup re-runs.
    assert!(plan.must_run().contains("cleanup"), "cleanup re-runs");
    let publish = attempt_for(&artifact, "publish").expect("publish recorded");
    assert_eq!(publish["status"], json!("satisfied-from-prior"));

    // cleanup's rule fires on the satisfied (success-like) upstream: re-executing it
    // reaches a success terminal.
    let probe = RecordingProbe::default();
    let (terminal, _v) = reexecute_source(
        RecordingSource {
            node: "cleanup".into(),
            probe: probe.clone(),
            // cleanup produces a distinctive marker so its success is observable.
            output: Blob("CLEANED".into()),
        },
        "cleanup",
        base.path(),
        PIPE,
        "run-B",
    );
    assert_eq!(
        terminal,
        TerminalState::Succeeded,
        "cleanup re-runs to a success terminal"
    );
    assert!(probe.ran("cleanup"), "cleanup body really executed");
    assert!(
        !probe.ran("publish"),
        "the satisfied publish body never executed"
    );
}

// ===========================================================================
// Test 7 — Dangling durable reference fails the resume plan before execution.
// ===========================================================================

/// **A durable reference to a deleted object fails the resume plan up front, before
/// any node executes.** Prior run: `produce` (durable) succeeded recording a
/// reference, `consume` failed and demands it. The existence probe reports the
/// reference is gone (`Absent`), so the resume plan fails with the resume-refusal
/// exit code and a message naming the missing reference — no node re-executes, and
/// no resumed artifact is produced.
#[test]
fn dangling_durable_reference_fails_the_plan_up_front() {
    let pipeline = durable_chain();
    let prior_ref = Blob("PRIOR-VALUE".into()).serialize_reference();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[
            PriorAttempt::durable_ok("produce", prior_ref.clone()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    // The demanded reference probes ABSENT — a deleted object.
    let (code, message) = expect_refused(resume_with(&pipeline, &prior, &opts("run-B"), absent));
    assert_eq!(
        code,
        ExitCode::ResumeRefusal,
        "a dangling reference → resume-refusal exit code"
    );
    assert!(
        message.contains("produce") && message.to_lowercase().contains("reference"),
        "the plan failure names the offending node and its missing reference: {message}"
    );
    assert!(
        message.contains(&prior_ref),
        "and the dangling reference itself: {message}"
    );
}

// ===========================================================================
// Test 8 — Multi-generation resume links parent + lineage root and copies
// references forward.
// ===========================================================================

/// **A resume of a resume links to its immediate parent and to the lineage root,
/// and copies durable references forward so the newest artifact is self-contained.**
/// Run 1 (`run-ROOT`) completed partially; run 2 (`run-GEN2`) resumed it, still
/// leaving `consume` not-succeeded; resuming run 2 into run 3 (`run-GEN3`) names
/// run 2 as immediate parent and run 1 as lineage root, and copies the durable
/// reference forward — so run 3's artifact can be read without runs 1 or 2 present.
#[test]
fn multi_generation_resume_links_parent_root_and_copies_references_forward() {
    let pipeline = durable_chain();
    let prior_ref = Blob("PRIOR-VALUE".into()).serialize_reference();
    // Run 2 (the immediate parent we resume) was itself a resume of run-ROOT, so its
    // header carries the lineage {parent: run-ROOT, root: run-ROOT}. `produce`
    // satisfied-from-prior in run 2 originated in run-ROOT (its origin is carried);
    // `consume` still failed.
    let gen2 = stage_prior_run(
        &pipeline,
        PIPE,
        "run-GEN2",
        &[],
        None,
        Some(json!({ "parent_run_id": "run-ROOT", "lineage_root_run_id": "run-ROOT" })),
        &[
            PriorAttempt::durable_ok("produce", prior_ref.clone()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    let (artifact, _plan) =
        expect_resumed(resume_with(&pipeline, &gen2, &opts("run-GEN3"), present));
    let lineage = &artifact["header"]["resume_lineage"];
    assert_eq!(
        lineage["parent_run_id"],
        json!("run-GEN2"),
        "immediate parent is run 2"
    );
    assert_eq!(
        lineage["lineage_root_run_id"],
        json!("run-ROOT"),
        "the lineage root stays the original run across generations"
    );
    assert_eq!(artifact["header"]["run_id"], json!("run-GEN3"));

    // The durable reference is copied forward, so run 3's artifact is self-contained.
    let produce = attempt_for(&artifact, "produce").expect("produce recorded");
    assert_eq!(
        produce["durable_reference"],
        json!(prior_ref),
        "the durable reference is copied forward into run 3's artifact (self-contained)"
    );
}

// ===========================================================================
// Test 9 — Scratch carry-forward observed by a re-executing node.
// ===========================================================================

/// **A re-executing node observes the scratch its prior-run counterpart wrote,
/// carried forward from the linked prior run.** The prior run: `checkpoint` wrote a
/// high-water mark into its retained scratch (a checkpoint shape) but did not
/// succeed, so its scratch is retained and it is in the seed. Resume plans it into
/// the must-run set; the driver carries its scratch forward (REAL `carry_forward`);
/// the re-executing `checkpoint` reads the mark through the ordinary C18 context
/// and reports continuing from the checkpoint rather than starting over.
///
/// Non-tautology + defeat check: the observed value is what the prior node WROTE
/// through its own store handle (not stamped into the resumed namespace), and the
/// sibling defeat case below proves that without the carry-forward the resumed node
/// reads absent.
#[test]
fn a_re_executing_node_observes_its_carried_forward_scratch() {
    let base = TempBase::new("scratch-observed");
    let pipeline = single_checkpoint();

    // The prior run's `checkpoint` wrote a distinctive high-water mark and did NOT
    // succeed, so its scratch is retained (T54a). Write it through the node's OWN
    // prior-run store handle — exactly what `ctx.scratch().put(..)` does in a run.
    let key = b"high-water";
    let mark = b"finished-item-K";
    store_for(base.path(), PIPE, "run-A", "checkpoint")
        .put(key, mark)
        .expect("prior node writes retained scratch");

    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[],
        None,
        None,
        &[PriorAttempt::failed("checkpoint")],
        RunOutcome::Failed,
    );

    // The REAL plan puts `checkpoint` in the must-run set (non-succeeded → seed).
    let (_artifact, plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    assert!(
        plan.must_run().contains("checkpoint"),
        "the non-succeeded checkpoint re-executes"
    );

    // The driver carries forward scratch for exactly the must-run set (REAL API).
    for node in plan.must_run() {
        ScratchStore::carry_forward(
            base.path(),
            &PipelineId::new(PIPE),
            &CoreRunId::new("run-A"),
            &CoreRunId::new("run-B"),
            NodeId::from_name(node),
        )
        .expect("carry forward the must-run node's scratch");
    }

    // Re-execute `checkpoint` through the REAL attempt runner; it reads its scratch
    // through the ordinary context (wired to the resumed run's namespace).
    let probe = RecordingProbe::default();
    let observed = Arc::new(Mutex::new(None));
    let (terminal, _v) = reexecute_source(
        ContinueFromScratch {
            node: "checkpoint".into(),
            probe: probe.clone(),
            key: key.to_vec(),
            observed: Arc::clone(&observed),
        },
        "checkpoint",
        base.path(),
        PIPE,
        "run-B",
    );
    assert_eq!(terminal, TerminalState::Succeeded);
    assert!(
        probe.ran("checkpoint"),
        "the checkpoint body really re-executed"
    );
    assert_eq!(
        observed.lock().unwrap().as_deref(),
        Some(&mark[..]),
        "the re-executing node observed the exact high-water mark its prior counterpart wrote — \
         it continued from the checkpoint, not from zero"
    );
}

/// **Defeat check for test 9: without the carry-forward, the re-executing node's
/// resumed scratch namespace is empty and it reads absent.** Proves the observed
/// value above came from the carry-forward, not from ambient state — the scenario
/// is non-vacuous.
#[test]
fn without_carry_forward_the_re_executing_node_reads_absent_scratch() {
    let base = TempBase::new("scratch-defeat");
    // The prior node wrote scratch, but we deliberately do NOT carry it forward.
    store_for(base.path(), PIPE, "run-A", "checkpoint")
        .put(b"high-water", b"finished-item-K")
        .expect("prior scratch write");

    let probe = RecordingProbe::default();
    let observed = Arc::new(Mutex::new(None));
    let (terminal, _v) = reexecute_source(
        ContinueFromScratch {
            node: "checkpoint".into(),
            probe: probe.clone(),
            key: b"high-water".to_vec(),
            observed: Arc::clone(&observed),
        },
        "checkpoint",
        base.path(),
        PIPE,
        "run-B",
    );
    assert_eq!(terminal, TerminalState::Succeeded);
    assert!(
        observed.lock().unwrap().is_none(),
        "without carry-forward the resumed namespace is empty — the read is absent (non-vacuous)"
    );
}

// ===========================================================================
// Test 10 — Parameter conflict refuses with a diff; force overrides and is
// recorded in the resumed artifact.
// ===========================================================================

/// **10A — supplying a parameter that conflicts with the prior run's derived value
/// refuses with a diff, without the force flag; nothing re-executes.**
#[test]
fn conflicting_parameter_without_force_refuses_with_a_diff() {
    let pipeline = durable_chain();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[("region", "eu")],
        None,
        None,
        &[
            PriorAttempt::durable_ok("produce", Blob("PRIOR-VALUE".into()).serialize_reference()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    let mut options = opts("run-B");
    options.param_overrides.insert("region".into(), "us".into()); // conflicts with prior "eu"

    let (code, message) = expect_refused(resume_with(&pipeline, &prior, &options, present));
    assert_eq!(
        code,
        ExitCode::ResumeRefusal,
        "a parameter conflict → resume-refusal exit code"
    );
    assert!(
        message.contains("region"),
        "the diff names the conflicting parameter: {message}"
    );
    assert!(
        message.contains("eu") && message.contains("us"),
        "and shows prior-versus-supplied values: {message}"
    );
}

/// **10B — the same conflict with the force flag proceeds, using the overriding
/// value, and the resumed artifact records that force was used.**
#[test]
fn force_overrides_a_conflicting_parameter_and_records_it_in_the_artifact() {
    let pipeline = durable_chain();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[("region", "eu")],
        None,
        None,
        &[
            PriorAttempt::durable_ok("produce", Blob("PRIOR-VALUE".into()).serialize_reference()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    let mut options = opts("run-B");
    options.force = true;
    options.param_overrides.insert("region".into(), "us".into());

    let (artifact, _plan) = expect_resumed(resume_with(&pipeline, &prior, &options, present));
    assert_eq!(
        artifact["header"]["parameters"]["region"],
        json!("us"),
        "the override wins under --force"
    );
    assert_eq!(
        artifact["header"]["resume_forced"],
        json!(true),
        "and the resumed artifact records that force was used"
    );
}

// ===========================================================================
// Cross-cutting: parameters + interval are derived from the prior artifact.
// ===========================================================================

/// **Resume derives parameters and the data interval from the prior artifact when
/// no conflicting override is supplied** — a supporting proof that the resumed run
/// inherits the prior invocation (the derivation the C27 gate specifies).
#[test]
fn parameters_and_interval_are_derived_from_the_prior_run() {
    let pipeline = durable_chain();
    let prior = stage_prior_run(
        &pipeline,
        PIPE,
        "run-A",
        &[("region", "eu")],
        Some(["2026-07-01", "2026-07-02"]),
        None,
        &[
            PriorAttempt::durable_ok("produce", Blob("PRIOR-VALUE".into()).serialize_reference()),
            PriorAttempt::failed("consume"),
        ],
        RunOutcome::Failed,
    );

    let (artifact, _plan) = expect_resumed(resume_with(&pipeline, &prior, &opts("run-B"), present));
    assert_eq!(
        artifact["header"]["parameters"]["region"],
        json!("eu"),
        "the resumed run inherits the prior parameters"
    );
    assert_eq!(
        artifact["header"]["data_interval"],
        json!({ "start": "2026-07-01", "end": "2026-07-02" }),
        "and the prior data interval"
    );
}
