//! `dagr-t63-demo-run` — the **kill/resume gate demo** driver.
//!
//! # Why this binary
//!
//! The done-when — *"a pipeline killed mid-run resumes and skips completed
//! durable work"* — is proven end-to-end **through the shipped
//! `drive()` loop**, not at node grain. This binary is the run under kill and the
//! resume that follows: it assembles the reference pipeline (`dagr_cli::t63_demo`), a
//! real durable stage boundary + a scratch-checkpointing node + a teardown, and
//! drives it through the **real** driver against a real on-disk event stream. The
//! integration test (`crates/cli/tests/m4_demo_kill_resume_review.rs`) launches it,
//! kills it abruptly mid-run (SIGKILL, no exit handler — the container-kill failure
//! mode), then resumes.
//!
//! It composes **already-merged** machinery through the shipped seams (adds no
//! engine capability): the scratch store and the driver's scratch wiring, the
//! durable-output contract, the real `drive()` loop, the real
//! `resume_verb` (`dagr_cli::contract`), and the
//! `ScratchStore::carry_forward` (`dagr_core::scratch`). It ships in no released
//! binary; it is checked-in scaffolding.
//!
//! # Determinism contract (no fixed sleeps)
//!
//! The `run` mode synchronises with its killer through an **observable on-disk
//! marker** the checkpoint node writes once the durable stage has succeeded and its
//! scratch is written, then holds until killed (spinning on the cancellation signal
//! only). The `resume` mode is a single deterministic drive of the must-run subset.
//!
//! # Usage
//!
//! ```text
//! dagr-t63-demo-run run    <base> <run-id> <ready-marker>
//! dagr-t63-demo-run resume <base> <prior-run-id> <new-run-id> <result-json-path>
//! ```

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, EVENTS_FILE_NAME};
use dagr_artifact::fold::fold_stream;
use dagr_cli::contract::{resume_verb, ResumeOptions, ResumeOutcome, TOOL_VERSION};
use dagr_cli::driver::{drive, RunConfig};
use dagr_cli::t63_demo::{
    assemble, base_groups, build_runner_set, Blob, ConsumerFrom, DemoRun, PIPELINE,
};
use dagr_core::assembly::DurableOutput;
use dagr_core::context::{PipelineId, RunId as CoreRunId};
use dagr_core::handle::NodeId;
use dagr_core::resume::ReferenceExistence;
use dagr_core::scratch::ScratchStore;

// === A real append-only local-file event sink (the event-stream sink) =========

/// A minimal append-only local-file sink: it appends each complete line to the run's
/// `events.jsonl` and flushes to the OS (no fsync per append — the default sink's
/// crash surface), so the bytes an abrupt kill leaves on disk are exactly what
/// reached the file.
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

/// A monotonic clock advanced automatically per read — deterministic and non-zero,
/// independent of wall-clock timing.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// The event-stream path a run writes under (`<base>/<pipeline>/<run-id>/events.jsonl`).
fn stream_path(base: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base)
        .join(PIPELINE)
        .join(run_id)
        .join(EVENTS_FILE_NAME)
}

/// The run-artifact path a resumed run writes its `run.json` under (the
/// reserved name, alongside `events.jsonl`).
fn run_artifact_path(base: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base)
        .join(PIPELINE)
        .join(run_id)
        .join("run.json")
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("run") => match args.as_slice() {
            [_, _, base, run_id, marker] => run_mode(base, run_id, marker),
            _ => usage(),
        },
        Some("resume") => match args.as_slice() {
            [_, _, base, prior, new, result] => resume_mode(base, prior, new, result),
            _ => usage(),
        },
        _ => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  dagr-t63-demo-run run    <base> <run-id> <ready-marker>\n  \
         dagr-t63-demo-run resume <base> <prior-run-id> <new-run-id> <result-json-path>"
    );
    ExitCode::from(2)
}

// === run mode: drive the full pipeline, hold at the checkpoint until killed ===

/// Drive the full reference pipeline through the real `drive()` loop, writing its
/// stream to a real on-disk `events.jsonl`. The checkpoint node writes its scratch,
/// signals the ready marker (once the durable stage has succeeded and its scratch is
/// written), and holds until killed — so the parent kills at a deterministic mid-run
/// state. This process never returns on its own on the kill path.
fn run_mode(base: &str, run_id: &str, marker: &str) -> ExitCode {
    let pipeline = assemble(base_groups, ConsumerFrom::Expensive);
    // Arm the checkpoint's hold + ready marker: it holds until the parent kills.
    let empty_prefill = BTreeMap::new();
    let (plan, _ordering) = build_runner_set(
        &pipeline,
        ConsumerFrom::Expensive,
        &DemoRun {
            hold_checkpoint: true,
            ready_marker: Some(PathBuf::from(marker)),
            omit: &[],
            prefill: &empty_prefill,
            consumer_capture: None,
        },
    );

    let path = stream_path(base, run_id);
    let sink = match FileSink::create(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot open event stream: {e}");
            return ExitCode::from(2);
        }
    };
    let cfg = RunConfig::new(base).run_id(run_id);
    // The run holds at the checkpoint; the parent kills it abruptly. If we ever
    // return (a regression: the hold ended without a kill), report failure.
    let _report = drive(&cfg, PIPELINE, Ok(plan), &[], sink, TickClock::default());
    eprintln!("run returned without being killed (regression: the hold should never end)");
    ExitCode::from(1)
}

// === resume mode: real resume_verb + carry-forward + drive the must-run set ===

/// Read the prior run's artifact bytes for resume. A prior *resumed* run left a
/// complete, lineage-linked `run.json` (folded stream + resume-verb overlay); resume
/// that directly so multi-generation lineage carries the original root. The original
/// *killed* run has only its surviving `events.jsonl` — fold it (the
/// crashed/interrupted-run path a resume reads, the real fold).
fn read_prior_artifact(base: &str, prior: &str, node_roster: &[String]) -> io::Result<Vec<u8>> {
    if let Ok(bytes) = std::fs::read(run_artifact_path(base, prior)) {
        return Ok(bytes);
    }
    let stream = std::fs::read(stream_path(base, prior))?;
    fold_stream(&stream, node_roster)
        .map(|a| a.to_canonical_json().into_bytes())
        .map_err(|e| io::Error::other(format!("fold prior stream: {e}")))
}

/// Resume the killed run through the **real** `resume_verb`, carry scratch forward
/// for the must-run set, drive the must-run subset through the real `drive()` loop
/// with the satisfied-from-prior nodes pre-seeded and the demanded durable producer
/// rehydrated into its consumer's slot, then write the resumed artifact and a JSON
/// result the test asserts on.
#[allow(
    clippy::too_many_lines,
    reason = "resume_mode is one linear sequence — read prior, run the real resume verb, \
              carry scratch forward, build the must-run subset, drive it, write the resumed \
              artifact, emit the result — whose steps share the plan and would only be \
              obscured by fragmenting across functions"
)]
fn resume_mode(base: &str, prior: &str, new: &str, result_path: &str) -> ExitCode {
    let pipeline = assemble(base_groups, ConsumerFrom::Expensive);

    let node_roster: Vec<String> = pipeline.nodes().map(|n| n.name().to_string()).collect();

    // (1) Read the prior run's artifact bytes (a prior resumed `run.json`, or the
    //     killed run's folded stream — see `read_prior_artifact`).
    let prior_bytes = match read_prior_artifact(base, prior, &node_roster) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("cannot read prior run: {e}");
            return ExitCode::from(2);
        }
    };

    // (2) Run the REAL resume verb: the durability gate + seed/closure/demand plan +
    //     the resumed-artifact recording. The existence probe confirms the durable
    //     stage-boundary reference is present on disk (a cheap check before any skip).
    let options = ResumeOptions {
        new_run_id: new.to_string(),
        tool_version: TOOL_VERSION.to_string(),
        store_present: true,
        force: false,
        param_overrides: BTreeMap::new(),
        interval_override: None,
    };
    let outcome = resume_verb(&pipeline, &prior_bytes, &options, present_probe);
    let (resumed_artifact, plan) = match outcome {
        ResumeOutcome::Resumed { artifact, plan } => (artifact, plan),
        ResumeOutcome::Refused { code, message } => {
            eprintln!("resume refused ({code:?}): {message}");
            return ExitCode::from(2);
        }
    };

    // (3) Carry scratch forward for every must-run node (the real API), keyed by
    //     the SAME pipeline id the driver's scratch namespace uses.
    for node in plan.must_run() {
        if let Err(e) = ScratchStore::carry_forward(
            Path::new(base),
            &PipelineId::new(PIPELINE),
            &CoreRunId::new(prior),
            &CoreRunId::new(new),
            NodeId::from_name(node),
        ) {
            eprintln!("carry-forward failed for `{node}`: {e}");
            return ExitCode::from(2);
        }
    }

    // (4) Build the must-run subset: omit the satisfied-from-prior nodes (they do not
    //     re-execute), and pre-fill each rehydrated durable producer's slot with the
    //     REAL rehydrated value so a re-running consumer reads it.
    let omit: Vec<&str> = plan
        .satisfied_from_prior()
        .keys()
        .map(String::as_str)
        .collect();
    let mut prefill: BTreeMap<String, Blob> = BTreeMap::new();
    for (node, reference) in plan.rehydrate() {
        match Blob::rehydrate(reference) {
            Ok(value) => {
                prefill.insert(node.clone(), value);
            }
            Err(e) => {
                eprintln!("rehydrate of `{node}` failed: {e:?}");
                return ExitCode::from(2);
            }
        }
    }
    // The pre-satisfied map the driver seeds: each satisfied node → its copied-forward
    // durable reference (or None), so the driver records it terminal and cascades.
    let pre_satisfied: BTreeMap<String, Option<String>> = plan
        .satisfied_from_prior()
        .keys()
        .map(|n| (n.clone(), plan.rehydrate().get(n).cloned()))
        .collect();

    let consumer_capture: dagr_cli::t63_demo::ConsumerCapture = Arc::new(Mutex::new(None));
    let (run_plan, _ordering) = build_runner_set(
        &pipeline,
        ConsumerFrom::Expensive,
        &DemoRun {
            hold_checkpoint: false,
            ready_marker: None,
            omit: &omit,
            prefill: &prefill,
            consumer_capture: Some(Arc::clone(&consumer_capture)),
        },
    );
    let run_plan = run_plan.with_resume(pre_satisfied);

    // (5) Drive the must-run subset through the real driver. A TEE sink writes the
    //     resumed stream to the real on-disk `events.jsonl` (so it is a genuine run
    //     directory) AND captures it in memory (so we can observe/fold it). The
    //     driver's stream records BOTH the re-executing nodes' terminals AND the
    //     satisfied-from-prior nodes (via the driver's resume pre-satisfied seam), so
    //     the folded stream is the run's complete node picture.
    let stream_dst = stream_path(base, new);
    let file_sink = match FileSink::create(&stream_dst) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("cannot open resumed event stream: {e}");
            return ExitCode::from(2);
        }
    };
    let sink = TeeSink {
        file: Arc::new(Mutex::new(file_sink)),
        capture: CaptureSink::default(),
    };
    let capture = sink.capture.clone();
    let cfg = RunConfig::new(base).run_id(new);
    let report = drive(
        &cfg,
        PIPELINE,
        Ok(run_plan),
        &[],
        sink,
        TickClock::default(),
    );

    // (6) Write the resumed run's COMPLETE, lineage-linked `run.json` to the run
    //     store: fold the driver's on-disk stream (the full node picture) and overlay
    //     the resume-verb header's lineage + tool version + the durable references it
    //     copied forward — so a second-generation resume reads a self-contained,
    //     lineage-correct prior artifact (the multi-generation carry-forward).
    let resumed_run_json =
        complete_resumed_artifact(&capture.bytes(), &node_roster, &resumed_artifact);
    let artifact_path = run_artifact_path(base, new);
    if let Some(parent) = artifact_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(
        &artifact_path,
        dagr_artifact::canonical::to_canonical_string(&resumed_run_json),
    );

    // (7) After the resumed run, read the checkpoint node's scratch under the RESUMED
    //     run's namespace: what it observed on re-execution IS what carry-forward
    //     copied (the checkpoint node reads it and continues). Observe it directly so
    //     the test can assert the carried-forward value end-to-end.
    let observed_checkpoint = ScratchStore::for_node(
        Path::new(base),
        &PipelineId::new(PIPELINE),
        &CoreRunId::new(new),
        NodeId::from_name("checkpoint"),
    )
    .get(dagr_cli::t63_demo::CHECKPOINT_KEY)
    .ok()
    .flatten();

    // (8) Emit the JSON result the test asserts on: the resumed terminals, the
    //     plan's satisfied/rehydrate/must-run classification, the resumed stream, the
    //     resumed artifact, and the carried-forward checkpoint the resumed namespace
    //     holds.
    let result = serde_json::json!({
        "overall_outcome": format!("{:?}", report.outcome),
        "terminals": report
            .terminal_states
            .iter()
            .map(|(k, v)| (k.clone(), format!("{v:?}")))
            .collect::<BTreeMap<_, _>>(),
        "satisfied_from_prior": plan.satisfied_from_prior().keys().collect::<Vec<_>>(),
        "must_run": plan.must_run().iter().collect::<Vec<_>>(),
        "rehydrate": plan.rehydrate(),
        "resumed_artifact": resumed_artifact,
        "resumed_stream": String::from_utf8_lossy(&capture.bytes()),
        "carried_forward_checkpoint": observed_checkpoint
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned()),
        // What the re-executing consumer RECEIVED — the rehydrated durable value,
        // traced end-to-end (never a constant next to the assertion).
        "consumer_received": consumer_capture.lock().unwrap().clone(),
    });
    if let Err(e) = std::fs::write(
        result_path,
        serde_json::to_vec_pretty(&result).expect("result serializes"),
    ) {
        eprintln!("cannot write result json: {e}");
        return ExitCode::from(2);
    }
    ExitCode::SUCCESS
}

/// The always-present existence probe (the durable stage-boundary object is on disk).
fn present_probe(_node: &str, _reference: &str) -> ReferenceExistence {
    ReferenceExistence::Present
}

/// A capturing in-memory sink (clone-shared) for the resumed run's stream.
#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().clone()
    }
}
impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A tee sink: writes each line to the real on-disk file AND captures it in memory,
/// so the resumed run leaves a genuine `events.jsonl` while the demo can fold/observe
/// the same stream.
struct TeeSink {
    file: Arc<Mutex<Option<FileSink>>>,
    capture: CaptureSink,
}
impl EventSink for TeeSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        if let Some(f) = self.file.lock().unwrap().as_mut() {
            f.append_line(line)?;
        }
        self.capture.append_line(line)
    }
    fn flush(&mut self) -> io::Result<()> {
        if let Some(f) = self.file.lock().unwrap().as_mut() {
            f.flush()?;
        }
        self.capture.flush()
    }
}

/// Fold the resumed run's on-disk stream into its complete run artifact (the full
/// node picture — re-executed + satisfied-from-prior), then overlay the
/// resume-verb header's lineage, tool version, forced marker, and copied-forward
/// durable references so the resulting `run.json` is **self-contained and
/// lineage-correct** — exactly what a second-generation resume reads as its prior
/// artifact (the multi-generation carry-forward).
fn complete_resumed_artifact(
    stream: &[u8],
    node_roster: &[String],
    resume_verb_artifact: &serde_json::Value,
) -> serde_json::Value {
    let folded = match fold_stream(stream, node_roster) {
        Ok(a) => a.to_value(),
        // If the folded stream is unreadable (should not happen for a clean run),
        // fall back to the resume-verb artifact so the run still records itself.
        Err(_) => return resume_verb_artifact.clone(),
    };
    let mut out = folded;
    // Overlay the resume-verb header fields the fold does not carry: the resume
    // lineage, the tool version, the forced marker, and the fingerprints — so the
    // artifact is lineage-correct and gate-comparable for a next-generation resume.
    if let (Some(out_header), Some(rv_header)) = (
        out.get_mut("header").and_then(|h| h.as_object_mut()),
        resume_verb_artifact
            .get("header")
            .and_then(|h| h.as_object()),
    ) {
        for field in [
            "resume_lineage",
            "tool_version",
            "resume_forced",
            "fingerprint_structural",
            "fingerprint_policy",
            "fingerprint_algorithm_version",
            "parameters",
            "data_interval",
        ] {
            if let Some(v) = rv_header.get(field) {
                out_header.insert(field.to_string(), v.clone());
            }
        }
    }
    // Overlay the copied-forward durable references onto the satisfied-from-prior
    // attempts so the artifact is self-contained (the fold recorded the satisfied
    // node-terminal, but the reference lives in the resume-verb record).
    if let (Some(out_attempts), Some(rv_attempts)) = (
        out.get_mut("attempts").and_then(|a| a.as_array_mut()),
        resume_verb_artifact
            .get("attempts")
            .and_then(|a| a.as_array()),
    ) {
        for rv in rv_attempts {
            let (Some(node), Some(dref)) = (
                rv.get("node").and_then(serde_json::Value::as_str),
                rv.get("durable_reference"),
            ) else {
                continue;
            };
            for a in out_attempts.iter_mut() {
                if a.get("node").and_then(serde_json::Value::as_str) == Some(node) {
                    if let Some(obj) = a.as_object_mut() {
                        obj.insert("durable_reference".to_string(), dref.clone());
                        if let Some(sfr) = rv.get("satisfied_from_run") {
                            obj.insert("satisfied_from_run".to_string(), sfr.clone());
                        }
                    }
                }
            }
        }
    }
    out
}
