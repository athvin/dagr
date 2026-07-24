//! `dagr-m3-demo-run` — the **test-support reference-pipeline producer** for the
//! M3 gate demo, ticket T49 (061). It drives the **real** M3 artifact producers
//! to leave a graph artifact and a run artifact on disk, then the demo test
//! (`crates/cli/tests/m3_demo_explain_a_run.rs`) explains the run **from those
//! artifacts alone**.
//!
//! # Why a producer harness (mirrors T68's `dagr-crashy-run`)
//!
//! arch.md's Build order fixes the M3 done-when: *"a run produces both artifacts,
//! the rendered diagram is reviewable, and 'which node was slowest, and was it
//! waiting or working?' is answerable from the artifacts alone — without reading a
//! single log line."* The honest way to demonstrate that is to run the **real**
//! producers and then read only what they left behind:
//!
//! - the **T40 graph emitter** ([`emit_graph`]) over a **real assembled
//!   [`Pipeline`]** writes `graph.json` (C20), carrying the **computed C21
//!   structural fingerprint**;
//! - the **real merged C19 [`EventStreamWriter`]** (T19) — the same producer T68
//!   drives — writes a real on-disk `events.jsonl` (C19), whose `run-started`
//!   header carries the **same** structural fingerprint string the graph emitter
//!   computed, so the two artifacts **join** (C22 fingerprint-match), and whose
//!   `attempt-outcome` records carry **real C23 metrics** built through the merged
//!   [`AttemptMetrics`] facility (T44).
//!
//! The M1/M2 run-loop driver (T24/T34) is deliberately *not* used to emit the
//! stream here: at M1/M2 it records only `node`/`attempt`/`status` on each
//! `attempt-outcome` and measures **no** metrics, cost, worker, or per-phase
//! offsets (see `closing_outcome_record` in `dagr_cli::driver`). Driving the C19
//! writer directly is how T68's harness produces a metrics-and-phase-rich stream
//! from the **real** producer, and it is what lets this demo exercise the C23
//! metrics path and the phase/critical-path summary end-to-end. This binary adds
//! **no** engine capability — it composes merged components only (T40/T41 emit +
//! fingerprint, T19/C19 writer, T44/C23 metrics) — and ships in **no** released
//! binary; it is checked-in scaffolding the T49 demo runs.
//!
//! # The reference pipeline (fixed at assembly; legibility, not volume)
//!
//! A small chain plus two contrast nodes and a skip pair, chosen so "slowest node"
//! and "waiting vs working" are unambiguous and stable:
//!
//! - `load` → `transform` → `publish` — the happy data chain (each a real data
//!   edge, `all-succeeded`).
//! - `slow-compute` — the **designed bottleneck**: the largest total elapsed, and
//!   **compute-bound** (its `executing` phase dominates its waiting phases), so the
//!   explainer names it slowest and classifies it **working**.
//! - `queue-limited` — a node whose total is dominated by **permit-wait** (it
//!   waited far longer for an admission permit than it spent executing), so the
//!   explainer classifies it **waiting** — the resource/queue-limited contrast to
//!   the compute-bound bottleneck.
//! - `decide-skip` — a source that returns a **deliberate (originated) skip**
//!   (`skipped`), and `skipped-consumer`, its data dependent, which **never runs**
//!   and carries the **propagated** `upstream-skipped` state (node coverage +
//!   originated-vs-propagated skip distinguishable). A run containing only skips is
//!   still a **successful** run (arch.md Vocabulary), so the overall outcome stays
//!   `succeeded`.
//!
//! # Determinism (no wall clock, no ordering dependence)
//!
//! A hand-stepped monotonic clock stamps every event, so every phase duration, the
//! total elapsed, and the critical path are fixed numbers independent of the host
//! or the scheduler. `run-started` is seq 0; offsets are strictly increasing. Two
//! runs of this harness leave byte-identical artifacts outside the graph's
//! generation-time field.
//!
//! # Usage
//!
//! ```text
//! dagr-m3-demo-run <run-store-base> <run-id>
//! ```
//! It writes `<base>/m3-demo-pipeline/<run-id>/graph.json` and `.../events.jsonl`
//! and exits `0`. A sentinel env var (`DAGR_M3_DEMO_SENTINEL`) that is **not** on
//! the pipeline's declared allowlist is read from the environment if present but
//! must never reach the artifact (C22 allowlist criterion); the allowlisted
//! `DAGR_REGION` is captured.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState, EVENTS_FILE_NAME, FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_cli::graph::{emit_graph, BuildProvenance};
use dagr_core::metrics::AttemptMetrics;
use dagr_core::stable_name::StableName;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};

/// The fixed pipeline identity for the demo run store layout.
pub const PIPELINE: &str = "m3-demo-pipeline";

/// The graph-artifact file name the demo reads (T0.6 §3 reserved name).
pub const GRAPH_FILE_NAME: &str = "graph.json";

/// The allowlisted environment variable the pipeline declares it may capture
/// (C7/C22). Its value reaches the artifact header.
pub const ALLOWLISTED_ENV: &str = "DAGR_REGION";

/// A sentinel env var deliberately **not** on the allowlist. If present in the
/// environment it must appear **nowhere** in the emitted artifacts (C22 allowlist
/// criterion) — the demo plants it and scans for it.
pub const SENTINEL_ENV: &str = "DAGR_M3_DEMO_SENTINEL";

// === A minimal append-only local-file sink (the real C19 sink surface) =======

/// A minimal append-only local-file [`EventSink`]: it appends each complete line
/// to the run's `events.jsonl` and flushes to the OS. The same shape T68's
/// harness uses — the real on-disk stream the fold later reads.
struct FileSink {
    file: File,
}

impl FileSink {
    fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }
}

impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.file.write_all(line)?;
        self.file.flush()
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A monotonic clock advanced by hand, so offsets are deterministic, distinct, and
/// independent of wall-clock timing (the same discipline as T68's `StepClock`).
struct StepClock {
    now: std::cell::Cell<u64>,
}

impl StepClock {
    fn new() -> Self {
        Self {
            now: std::cell::Cell::new(0),
        }
    }
    fn set(&self, at: u64) {
        self.now.set(at);
    }
}

impl MonotonicClock for StepClock {
    fn elapsed_ns(&self) -> u64 {
        self.now.get()
    }
}

/// A by-reference clock adapter so `main` keeps ownership of the [`StepClock`] (to
/// call `set`) while the writer holds a reference to it.
struct ClockRef<'a> {
    clock: &'a StepClock,
}

impl MonotonicClock for ClockRef<'_> {
    fn elapsed_ns(&self) -> u64 {
        self.clock.elapsed_ns()
    }
}

// === The reference pipeline's tasks (real, StableName-carrying) ==============

/// A stable-named unit payload type carried on the happy chain's data edges.
struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// The chain's source (`load`): compute-class, succeeds.
struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "load-task";
}
impl Task for Load {
    type Input = ();
    type Output = Rows;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

/// The chain's middle (`transform`): a data dependent of `load`.
struct Transform;
impl StableName for Transform {
    const STABLE_NAME: &'static str = "transform-task";
}
impl Task for Transform {
    type Input = Rows;
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, i: Rows) -> Result<Rows, TaskError> {
        Ok(i)
    }
}

/// The chain's sink (`publish`): a data dependent of `transform`.
struct Publish;
impl StableName for Publish {
    const STABLE_NAME: &'static str = "publish-task";
}
impl Task for Publish {
    type Input = Rows;
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<(), TaskError> {
        Ok(())
    }
}

/// The designed **compute-bound bottleneck** (`slow-compute`): compute-class,
/// succeeds. Its stream offsets make its `executing` phase dominate.
struct SlowCompute;
impl StableName for SlowCompute {
    const STABLE_NAME: &'static str = "slow-compute-task";
}
impl Task for SlowCompute {
    type Input = ();
    type Output = ();
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// The **queue/permit-limited** contrast node (`queue-limited`): its stream
/// offsets make its `permit-wait` phase dominate.
struct QueueLimited;
impl StableName for QueueLimited {
    const STABLE_NAME: &'static str = "queue-limited-task";
}
impl Task for QueueLimited {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// The **originated-skip** source (`decide-skip`): returns a deliberate skip.
struct DecideSkip;
impl StableName for DecideSkip {
    const STABLE_NAME: &'static str = "decide-skip-task";
}
impl Task for DecideSkip {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Err(TaskError::skip("nothing to do"))
    }
}

/// The **propagated-skip** consumer (`skipped-consumer`): a data dependent of
/// `decide-skip` that never runs and carries `upstream-skipped`.
struct SkippedConsumer;
impl StableName for SkippedConsumer {
    const STABLE_NAME: &'static str = "skipped-consumer-task";
}
impl Task for SkippedConsumer {
    type Input = Rows;
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<(), TaskError> {
        Ok(())
    }
}

/// Assemble the real reference pipeline (the C20 structure the graph artifact
/// serializes). Each data edge is a real edge; `slow-compute` carries a declared
/// working-memory cost so the artifact juxtaposes declared vs measured cost.
fn build_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    // The pipeline declares its env-capture allowlist (C7/C22): only DAGR_REGION.
    flow.allow_env_capture([ALLOWLISTED_ENV.to_string()]);

    let load = flow.register_source_named::<Load>("load", &Load, None::<String>, NodePolicy::new());
    let transform =
        flow.register_named::<Transform, _>("transform", &Transform, load, None::<String>, NodePolicy::new());
    let _publish =
        flow.register_named::<Publish, _>("publish", &Publish, transform, None::<String>, NodePolicy::new());

    let _slow = flow.register_source_named::<SlowCompute>(
        "slow-compute",
        &SlowCompute,
        None::<String>,
        NodePolicy::new().working_memory(4096),
    );
    let _queue = flow.register_source_named::<QueueLimited>(
        "queue-limited",
        &QueueLimited,
        None::<String>,
        NodePolicy::new(),
    );

    let decide = flow.register_source_named::<DecideSkip>(
        "decide-skip",
        &DecideSkip,
        None::<String>,
        NodePolicy::new(),
    );
    let _skipped = flow.register_named::<SkippedConsumer, _>(
        "skipped-consumer",
        &SkippedConsumer,
        decide,
        None::<String>,
        NodePolicy::new(),
    );

    flow.finish()
}

/// Build one node's **real** C23 metric set through the merged [`AttemptMetrics`]
/// facility (T44): a task metric attached under a unit-suffixed name, plus
/// framework-contributed peak memory and phase timings — then rendered to the open
/// numeric JSON map the `attempt-outcome` record carries and the fold copies
/// unmodified.
fn metrics_json(rows_read: u64, peak_bytes: u64, executing_ns: u64) -> serde_json::Value {
    let mut m = AttemptMetrics::new();
    // Task-attached, unit-in-the-name (C23 convention). A reserved-prefix attach
    // would fail loudly at attach time (proven by the demo test).
    m.attach("rows_read", rows_read)
        .expect("rows_read is not under the reserved prefix");
    // Framework-contributed measurements — present even though the task attached
    // one of its own (C23).
    m.set_peak_memory_bytes(peak_bytes);
    m.set_phase_timings(&[("executing", executing_ns)]);
    m.finalize_task_metrics();
    // Render the collected (name-ordered) set into the artifact's open numeric map.
    let mut obj = serde_json::Map::new();
    for (name, value) in m.collected() {
        obj.insert(name, serde_json::json!(value));
    }
    serde_json::Value::Object(obj)
}

/// Emit the full lifecycle for one node that **executed** to a terminal, at the
/// given offsets, carrying real metrics and declared/measured cost. Offsets are
/// set on the clock before each write so the writer stamps them.
#[allow(clippy::too_many_arguments)]
fn emit_ran_node<S: EventSink>(
    writer: &mut EventStreamWriter<S, ClockRef<'_>>,
    clock: &StepClock,
    node: &str,
    ready: u64,
    admitted: u64,
    started: u64,
    finished: u64,
    metrics: serde_json::Value,
) {
    clock.set(ready);
    let _ = writer.node_ready(node);
    clock.set(admitted);
    let _ = writer.node_admitted(node);
    clock.set(started);
    let _ = writer.attempt_started(node, 1);
    clock.set(finished);
    let _ = writer.attempt_succeeded(node, 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: node.into(),
        attempt: 1,
        status: TerminalState::Succeeded.as_str().into(),
        worker: Some(format!("compute#{node}")),
        metrics: Some(metrics),
        cost_declared: Some(serde_json::json!({ "working_memory_bytes": 4096 })),
        cost_measured: Some(serde_json::json!({ "working_memory_bytes": 4096 })),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal(node, TerminalState::Succeeded);
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, base, run_id] = args.as_slice() else {
        eprintln!("usage: dagr-m3-demo-run <run-store-base> <run-id>");
        return ExitCode::from(2);
    };

    let pipeline = build_pipeline();

    // --- (1) Emit the REAL graph artifact (C20 / T40) and capture its computed
    // structural fingerprint (C21 / T41), so the run stream can carry the SAME
    // string and the two artifacts JOIN (C22).
    let provenance = BuildProvenance::embedded();
    let graph_json = match emit_graph(&pipeline, PIPELINE, "2026-07-24T00:00:00Z", &provenance) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("graph emit failed: {e}");
            return ExitCode::from(2);
        }
    };
    let graph: serde_json::Value = match serde_json::from_str(&graph_json) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("graph artifact is not JSON: {e}");
            return ExitCode::from(2);
        }
    };
    let fp_structural = graph["header"]["fingerprint_structural"]
        .as_str()
        .expect("graph carries a structural fingerprint")
        .to_string();
    let fp_policy = graph["header"]["fingerprint_policy"]
        .as_str()
        .expect("graph carries a policy fingerprint")
        .to_string();

    let run_dir = PathBuf::from(base).join(PIPELINE).join(run_id);
    if let Err(e) = std::fs::create_dir_all(&run_dir) {
        eprintln!("cannot create run dir {}: {e}", run_dir.display());
        return ExitCode::from(2);
    }
    let graph_path = run_dir.join(GRAPH_FILE_NAME);
    if let Err(e) = std::fs::write(&graph_path, graph_json.as_bytes()) {
        eprintln!("cannot write graph artifact: {e}");
        return ExitCode::from(2);
    }

    // --- (2) Drive the REAL C19 writer to a real on-disk events.jsonl (T19),
    // stamping the graph's fingerprint string into the run-started header so the
    // run artifact joins the graph artifact (C22 fingerprint-match). Capture only
    // the allowlisted env value; the sentinel (if set) is never read into the
    // header (C22 allowlist criterion).
    let stream_path = run_dir.join(EVENTS_FILE_NAME);
    let sink = match FileSink::create(&stream_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot open event stream: {e}");
            return ExitCode::from(2);
        }
    };
    let clock = StepClock::new();
    let mut writer = EventStreamWriter::new(
        sink,
        ClockRef { clock: &clock },
        RunId::from_operator(run_id.clone()),
        PIPELINE,
    )
    .with_wall_clock(|| "2026-07-24T00:00:00.000Z".to_string());

    let mut captured_env = BTreeMap::new();
    if let Ok(region) = std::env::var(ALLOWLISTED_ENV) {
        captured_env.insert(ALLOWLISTED_ENV.to_string(), region);
    }
    // The sentinel is deliberately NOT captured, even if present — it is not on the
    // declared allowlist. (Reading it here only to prove it is dropped, never
    // recorded.)
    let _ = std::env::var(SENTINEL_ENV);

    let mut parameters = BTreeMap::new();
    parameters.insert("date".to_string(), "2026-07-24".to_string());

    clock.set(0);
    if writer
        .run_started(RunStartedHeader {
            pipeline: PIPELINE.to_string(),
            fingerprint_structural: Some(fp_structural),
            fingerprint_policy: Some(fp_policy),
            fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
            parameters,
            data_interval: Some([
                "2026-07-24T00:00:00Z".to_string(),
                "2026-07-25T00:00:00Z".to_string(),
            ]),
            captured_env,
            resumed_from: None,
        })
        .is_err()
    {
        eprintln!("run-started write failed");
        return ExitCode::from(2);
    }

    // The happy chain: load → transform → publish. Small, quick executes.
    // Offsets: ready/admitted/started/finished. `executing = finished − started`.
    emit_ran_node(&mut writer, &clock, "load", 10, 20, 30, 130, metrics_json(1_000, 2_048, 100));
    emit_ran_node(&mut writer, &clock, "transform", 140, 150, 160, 260, metrics_json(1_000, 2_048, 100));
    emit_ran_node(&mut writer, &clock, "publish", 270, 280, 290, 390, metrics_json(0, 1_024, 100));

    // The designed compute-bound bottleneck: LONGEST total AND executing dominates.
    // total = 900 − 400 = 500; executing = 900 − 500 = 400 (started at 500); the
    // waits (ready→admitted→started) are small, so it reads "working".
    emit_ran_node(
        &mut writer,
        &clock,
        "slow-compute",
        400,
        420,
        500,
        900,
        metrics_json(0, 65_536, 400),
    );

    // The queue/permit-limited contrast: total dominated by **permit-wait**. It
    // became ready, entered its attempt, then blocked waiting for an admission
    // permit for a long span *inside* the attempt window before finally executing a
    // brief body. The C22 fold carves a phase out of the attempt total only for a
    // lifecycle offset that falls WITHIN `[attempt-started, attempt-outcome]`
    // (`dagr_artifact::fold` — "streams that carry in-window sub-phase offsets"), so
    // the permit-wait is recorded by emitting `node-admitted` AFTER
    // `attempt-started`: the fold then reports permit-wait = admitted − started =
    // 200 ≫ executing = 20, and the node reads "waiting". (attempt-started 500 →
    // node-admitted 700 → attempt-outcome 720.)
    clock.set(210);
    let _ = writer.node_ready("queue-limited");
    clock.set(500);
    let _ = writer.attempt_started("queue-limited", 1);
    clock.set(700);
    let _ = writer.node_admitted("queue-limited");
    clock.set(720);
    let _ = writer.attempt_succeeded("queue-limited", 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: "queue-limited".into(),
        attempt: 1,
        status: TerminalState::Succeeded.as_str().into(),
        worker: Some("await-bound#queue-limited".into()),
        metrics: Some(metrics_json(0, 1_024, 20)),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal("queue-limited", TerminalState::Succeeded);

    // The originated skip and its propagated (never-ran) consumer.
    clock.set(30);
    let _ = writer.node_ready("decide-skip");
    clock.set(40);
    let _ = writer.node_admitted("decide-skip");
    clock.set(50);
    let _ = writer.attempt_started("decide-skip", 1);
    clock.set(60);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: "decide-skip".into(),
        attempt: 1,
        status: TerminalState::Skipped.as_str().into(),
        worker: Some("await-bound#decide-skip".into()),
        message: Some("nothing to do".into()),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal("decide-skip", TerminalState::Skipped);
    // The consumer never runs; it carries the PROPAGATED upstream-skipped state,
    // naming the originating node (arch.md Vocabulary). No attempt-started.
    clock.set(70);
    let _ = writer.node_terminal("skipped-consumer", TerminalState::UpstreamSkipped);

    // A skip-only-among-these run is still a SUCCESSFUL run (arch.md Vocabulary).
    clock.set(1_000);
    let _ = writer.run_finished(RunOutcome::Succeeded);
    let _ = writer.finish();

    ExitCode::SUCCESS
}
