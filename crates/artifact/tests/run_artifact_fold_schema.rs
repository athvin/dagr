//! C22 · Run-artifact **fold** schema round-trip — ticket T42 (053). Written
//! first, TDD.
//!
//! The load-bearing interlock with T39 (ticket 050): a **real folded** run
//! artifact validates against the published `schemas/run/v1.schema.json` via the
//! T39 validation helper (`dagr_artifact::schema`), across the full-run,
//! interrupted, and pre-execution-failure variants. This proves the fold emits
//! to the *published* contract, and that the fold-reader version declaration the
//! fold adds is additive (unknown fields validate, T0.10). A deliberately
//! corrupted copy is rejected — proving the check has teeth.
//!
//! Gated behind the `schema-validation` feature (default OFF), which pulls the
//! CI-/dev-scoped `jsonschema` validator (T4 ADR 017 §4); CI runs it with the
//! feature ON in a dedicated step, mirroring T39/T40. The shipped binary and the
//! bare `cargo test --workspace` never activate it.

#![cfg(feature = "schema-validation")]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::fold_stream;
use dagr_artifact::schema::{validate_value, ArtifactKind};

fn env(seq: u64, offset_ns: u64, kind: &str) -> Value {
    json!({
        "schema_version": "dagr.event-stream@1",
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "seq": seq,
        "wall": "2026-07-23T00:00:00.000Z",
        "offset_ns": offset_ns,
        "kind": kind,
    })
}

fn with(mut v: Value, fields: &[(&str, Value)]) -> Value {
    let o = v.as_object_mut().unwrap();
    for (k, val) in fields {
        o.insert((*k).to_string(), val.clone());
    }
    v
}

fn start_header() -> Value {
    json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
        "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
        "fingerprint_algorithm_version": 1,
        "parameters": { "date": "2026-07-23" },
        "data_interval": { "start": "2026-07-23T00:00:00Z", "end": "2026-07-24T00:00:00Z" },
        "captured_environment": { "DAGR_REGION": "us-east-1" },
        "resume_lineage": null,
    })
}

fn stream(records: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for r in records {
        out.push_str(&serde_json::to_string(r).unwrap());
        out.push('\n');
    }
    out.into_bytes()
}

fn full_run_stream() -> Vec<u8> {
    stream(&[
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(env(1, 100, "node-ready"), &[("node", json!("load"))]),
        with(env(2, 300, "node-admitted"), &[("node", json!("load"))]),
        with(
            env(3, 500, "attempt-started"),
            &[("node", json!("load")), ("attempt", json!(1))],
        ),
        with(
            env(4, 1000, "attempt-outcome"),
            &[
                ("node", json!("load")),
                ("attempt", json!(1)),
                ("status", json!("failed")),
                ("message", json!("transient failure")),
                ("error", json!({ "kind": "transient", "detail": "timeout" })),
                ("metrics", json!({ "rows_read": 0 })),
                ("cost_declared", json!({ "memory_bytes": 1024 })),
                ("cost_measured", json!({ "memory_bytes": 512 })),
            ],
        ),
        with(
            env(5, 1100, "attempt-started"),
            &[("node", json!("load")), ("attempt", json!(2))],
        ),
        with(
            env(6, 2000, "attempt-outcome"),
            &[
                ("node", json!("load")),
                ("attempt", json!(2)),
                ("status", json!("succeeded")),
                ("metrics", json!({ "rows_read": 1000 })),
                ("cost_declared", json!({ "memory_bytes": 1024 })),
                ("cost_measured", json!({ "memory_bytes": 900 })),
                ("retained", json!(true)),
                ("slot_residency", json!(2)),
                (
                    "durable_reference",
                    json!({ "storage_key": "file:///runs/example/load/output" }),
                ),
            ],
        ),
        with(
            env(7, 2000, "node-terminal"),
            &[("node", json!("load")), ("state", json!("succeeded"))],
        ),
        with(
            env(8, 2000, "node-terminal"),
            &[
                ("node", json!("sink")),
                ("state", json!("satisfied-from-prior")),
                (
                    "satisfied_from_run",
                    json!("018f0000-0000-7000-8000-000000000001"),
                ),
            ],
        ),
        with(
            env(9, 2000, "run-finished"),
            &[("outcome", json!("succeeded"))],
        ),
    ])
}

#[test]
fn folded_full_run_validates_against_published_schema() {
    let art = fold_stream(
        &full_run_stream(),
        &["load".to_string(), "sink".to_string()],
    )
    .expect("fold");
    let value = art.to_value();
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("REAL folded run artifact must validate: {e}"));

    // The fold-reader declaration is additive and validates (unknown fields
    // ignored, T0.10).
    assert!(
        value.get("fold_reader").is_some(),
        "fold declares its reader version"
    );

    // Teeth: a corrupted copy (non-integer phase duration) is rejected.
    let mut bad = value.clone();
    bad["attempts"][0]["phase_durations_ns"]["executing"] = json!("nope");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the schema check has teeth"
    );
}

#[test]
fn folded_interrupted_run_validates() {
    // A crash-truncated stream (no run-finished + a trailing partial) folds to an
    // interrupted artifact that still validates.
    let mut bytes = stream(&[
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(
            env(1, 500, "attempt-started"),
            &[("node", json!("load")), ("attempt", json!(1))],
        ),
        with(
            env(2, 1000, "attempt-outcome"),
            &[
                ("node", json!("load")),
                ("attempt", json!(1)),
                ("status", json!("succeeded")),
            ],
        ),
        with(
            env(3, 1000, "node-terminal"),
            &[("node", json!("load")), ("state", json!("succeeded"))],
        ),
    ]);
    bytes.extend_from_slice(br#"{"schema_version":"dagr.event-stream@1","seq":4,"#);
    let art = fold_stream(&bytes, &["load".to_string()]).expect("fold");
    assert!(art.is_interrupted());
    validate_value(ArtifactKind::Run, 1, &art.to_value())
        .unwrap_or_else(|e| panic!("folded interrupted artifact must validate: {e}"));
}

#[test]
fn folded_assembly_failed_validates() {
    let mut header = start_header();
    let h = header.as_object_mut().unwrap();
    h.remove("fingerprint_structural");
    h.remove("fingerprint_policy");
    h.remove("fingerprint_algorithm_version");
    let bytes = stream(&[
        with(env(0, 0, "run-started"), &[("header", header)]),
        with(
            env(1, 0, "run-finished"),
            &[
                ("outcome", json!("assembly-failed")),
                ("errors", json!(["node `a` duplicates node `a`"])),
            ],
        ),
    ]);
    let art = fold_stream(&bytes, &[]).expect("fold");
    validate_value(ArtifactKind::Run, 1, &art.to_value())
        .unwrap_or_else(|e| panic!("folded assembly-failed artifact must validate: {e}"));
}

// === GAP 1: end-to-end REAL-writer → fold → run-schema round-trip ==========
//
// The prior tests fold *hand-built* stream bytes — they never prove the real C19
// writer (`EventStreamWriter`, reconciled to the published event-stream schema on
// main) and the C22 fold agree. These two tests drive an **actual writer** to
// produce real event-stream bytes, fold THOSE bytes, and assert the folded
// artifact validates against `schemas/run/v1.schema.json`. If the writer's wire
// shape (`kind` names, top-level field spread, `attempt-outcome` payload) and the
// fold's reader ever diverge, the fold sees the wrong records and these
// non-vacuous assertions fail (see the per-assertion notes).

/// A sink that keeps every appended line, so the test can recover the exact bytes
/// the real writer emitted and hand them straight to the fold.
#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CaptureSink {
    /// The full on-disk stream: every appended line concatenated in order.
    fn bytes(&self) -> Vec<u8> {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .copied()
            .collect()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.lines.lock().unwrap().push(line.to_vec());
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock the test advances explicitly, so offsets (and therefore the
/// folded phase durations) are deterministic and non-zero.
#[derive(Clone)]
struct ManualClock {
    now: Rc<Cell<u64>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(0)),
        }
    }
    fn set(&self, ns: u64) {
        self.now.set(ns);
    }
}

impl MonotonicClock for ManualClock {
    fn elapsed_ns(&self) -> u64 {
        self.now.get()
    }
}

/// The full start-header known at run start (assembly succeeded → real
/// fingerprints), driven through the real writer's typed `RunStartedHeader`.
fn writer_header() -> RunStartedHeader {
    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-23".to_string());
    let mut env = BTreeMap::new();
    env.insert("DAGR_REGION".to_string(), "us-east-1".to_string());
    RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: Some(
            "blake3:1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        fingerprint_policy: Some(
            "blake3:2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: params,
        data_interval: Some([
            "2026-07-23T00:00:00Z".to_string(),
            "2026-07-24T00:00:00Z".to_string(),
        ]),
        captured_env: env,
        resumed_from: None,
    }
}

/// Drive a real `EventStreamWriter` through a full multi-node run and return the
/// bytes it actually wrote. The run exercises, over the REAL writer:
///   - `run-started` carrying the full header;
///   - node `load` — a **retry**: node-ready/admitted, attempt 1 fails,
///     attempt 2 succeeds (two `attempt-outcome` records, one node terminal);
///   - node `sink` — a plain single-attempt success;
///   - node `orphan` — a **never-ran** node whose only presence is a
///     `node-terminal` (upstream-failed), with no attempt events;
///   - `run-finished(succeeded)` when `finish_run` is true (a truncated stream
///     drops it).
///
/// Every offset is stamped from the advanced `ManualClock`, so the folded phases
/// are real deltas, not zeros.
fn drive_real_run(finish_run: bool) -> Vec<u8> {
    let sink = CaptureSink::default();
    let clock = ManualClock::new();
    // Hold the wall stamp fixed so the record bytes are deterministic (it is
    // informational only — the fold reads offsets, never the wall).
    let mut w = EventStreamWriter::new(
        sink.clone(),
        clock.clone(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-0123456789ab"),
        "example-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    clock.set(0);
    w.run_started(writer_header()).expect("run-started");

    // --- node `load`: fails attempt 1, retries, succeeds attempt 2 ---------
    clock.set(100);
    w.node_ready("load").expect("node-ready");
    clock.set(300);
    w.node_admitted("load").expect("node-admitted");
    clock.set(500);
    w.attempt_started("load", 1).expect("attempt-started 1");
    clock.set(1000);
    w.attempt_failed("load", 1).expect("attempt-failed 1");
    w.attempt_outcome(AttemptOutcomeRecord {
        node: "load".into(),
        attempt: 1,
        status: TerminalState::Failed.as_str().into(),
        worker: Some("compute#1".into()),
        message: Some("transient failure".into()),
        error: Some(json!({ "kind": "transient", "detail": "timeout" })),
        metrics: Some(json!({ "rows_read": 0 })),
        cost_declared: Some(json!({ "memory_bytes": 1024 })),
        cost_measured: Some(json!({ "memory_bytes": 512 })),
        ..AttemptOutcomeRecord::default()
    })
    .expect("attempt-outcome 1");
    // Retry: a second attempt for the same node.
    clock.set(1100);
    w.attempt_started("load", 2).expect("attempt-started 2");
    clock.set(2000);
    w.attempt_succeeded("load", 2).expect("attempt-succeeded 2");
    w.attempt_outcome(AttemptOutcomeRecord {
        node: "load".into(),
        attempt: 2,
        status: TerminalState::Succeeded.as_str().into(),
        worker: Some("compute#1".into()),
        metrics: Some(json!({ "rows_read": 1000 })),
        cost_declared: Some(json!({ "memory_bytes": 1024 })),
        cost_measured: Some(json!({ "memory_bytes": 900 })),
        durable_reference: Some(json!({ "storage_key": "file:///runs/example/load/output" })),
        ..AttemptOutcomeRecord::default()
    })
    .expect("attempt-outcome 2");
    w.node_terminal("load", TerminalState::Succeeded)
        .expect("node-terminal load");

    // --- node `sink`: a plain single-attempt success -----------------------
    clock.set(2100);
    w.node_ready("sink").expect("node-ready sink");
    clock.set(2200);
    w.node_admitted("sink").expect("node-admitted sink");
    clock.set(2300);
    w.attempt_started("sink", 1).expect("attempt-started sink");
    clock.set(3000);
    w.attempt_succeeded("sink", 1)
        .expect("attempt-succeeded sink");
    w.attempt_outcome(AttemptOutcomeRecord::new(
        "sink",
        1,
        TerminalState::Succeeded.as_str(),
    ))
    .expect("attempt-outcome sink");
    w.node_terminal("sink", TerminalState::Succeeded)
        .expect("node-terminal sink");

    // --- node `orphan`: never ran (only a propagated terminal) -------------
    clock.set(3000);
    w.node_terminal("orphan", TerminalState::UpstreamFailed)
        .expect("node-terminal orphan");

    if finish_run {
        clock.set(3000);
        w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    }
    w.finish().expect("flush");

    sink.bytes()
}

#[test]
fn e2e_real_writer_stream_folds_and_validates_against_run_schema() {
    // Drive a REAL writer, then fold the REAL bytes it emitted.
    let bytes = drive_real_run(true);
    let graph_nodes = ["load".to_string(), "sink".to_string(), "orphan".to_string()];
    let art = fold_stream(&bytes, &graph_nodes).expect("fold of a real-writer stream");
    let value = art.to_value();

    // (1) Load-bearing: the folded REAL artifact validates against the published
    // run schema. Non-vacuous — if the writer emitted a shape the fold could not
    // read, the artifact would be malformed (missing required attempt fields) and
    // fail here. Teeth: a corrupted copy is rejected below.
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("REAL-writer folded run artifact must validate: {e}"));

    // (2) One attempt record per attempt — the retry ⇒ TWO records for `load`,
    // in ascending attempt order, with the correct per-attempt statuses. If the
    // writer's `attempt-outcome` payload and the fold diverged, the fold would
    // drop or misread these and the count/status would be wrong.
    let load: Vec<_> = art
        .attempts()
        .iter()
        .filter(|a| a.node() == "load")
        .collect();
    assert_eq!(load.len(), 2, "retry ⇒ two attempt records for `load`");
    assert_eq!(load[0].attempt_number(), 1);
    assert_eq!(load[1].attempt_number(), 2);
    assert_eq!(load[0].status(), "failed", "attempt 1 failed");
    assert_eq!(load[1].status(), "succeeded", "attempt 2 succeeded");
    // The rich attempt-outcome payload survived the real writer → fold path.
    assert_eq!(load[0].message(), Some("transient failure"));
    assert_eq!(load[0].worker(), "compute#1");
    assert_eq!(
        load[1].durable_reference(),
        Some(&json!({ "storage_key": "file:///runs/example/load/output" })),
    );

    // (3) Phases sum bit-exactly to each attempt's total (from real offsets, not
    // wall clocks — every `wall` stamp is the identical fixed string, so a
    // wall-based duration would be 0). `load` attempt 2 ran 1100→2000 = 900ns.
    for a in &load {
        let sum: u64 = a.phase_durations_ns().values().copied().sum();
        assert_eq!(sum, a.total_elapsed_ns(), "phases sum to the attempt total");
    }
    assert_eq!(
        load[1].total_elapsed_ns(),
        900,
        "attempt 2 total = terminal offset (2000) − attempt-started offset (1100)"
    );

    // (4) The never-ran `orphan` node is covered, carrying its propagated state.
    let orphan = art
        .attempts()
        .iter()
        .find(|a| a.node() == "orphan")
        .expect("never-ran node covered");
    assert_eq!(orphan.status(), "upstream-failed");
    // Every graph node appears at least once.
    for n in ["load", "sink", "orphan"] {
        assert!(
            art.attempts().iter().any(|a| a.node() == n),
            "graph node {n} appears in the artifact"
        );
    }

    // (5) The header is complete, folded from the writer's run-started alone.
    assert_eq!(art.header_run_id(), "018f4a1e-6c2a-7b3d-9e10-0123456789ab");
    assert_eq!(art.header_pipeline(), "example-pipeline");
    assert_eq!(
        art.header_fingerprint_structural(),
        Some("blake3:1111111111111111111111111111111111111111111111111111111111111111"),
    );
    assert!(art.header_parameters().get("date").is_some());
    assert!(art.header_data_interval().is_some());
    assert_eq!(art.overall_outcome(), "succeeded");
    assert!(!art.is_interrupted(), "a complete run is not interrupted");

    // Teeth: a corrupted copy (non-integer phase duration) is rejected, proving
    // the schema check is not vacuously passing everything.
    let mut bad = value.clone();
    let idx = value["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .position(|a| a["node"] == json!("load"))
        .unwrap();
    bad["attempts"][idx]["phase_durations_ns"]["executing"] = json!("nope");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the schema round-trip has teeth",
    );
}

#[test]
fn e2e_real_writer_truncated_stream_folds_interrupted_and_validates() {
    // Drive a real run but DROP the trailing `run-finished`, then byte-truncate
    // the last record (an abrupt kill mid-write) — the fold must tolerate the one
    // trailing partial, mark the artifact interrupted, and still validate.
    let mut bytes = drive_real_run(false);
    // Simulate a process killed mid-append: cut the final (real) record part-way
    // through, so the stream ends on a genuine byte-truncated record — the one
    // trailing partial the fold must tolerate. Find the last two newlines; keep
    // everything up to the last full record's newline, then half of the final
    // record's bytes (an unterminated, unparseable tail).
    let last_nl = bytes.iter().rposition(|&b| b == b'\n').unwrap();
    let prev_nl = bytes[..last_nl]
        .iter()
        .rposition(|&b| b == b'\n')
        .expect("at least two records");
    let final_start = prev_nl + 1;
    let keep = final_start + (last_nl - final_start) / 2;
    bytes.truncate(keep);
    assert_ne!(bytes.last(), Some(&b'\n'), "the stream ends mid-record");

    let graph_nodes = ["load".to_string(), "sink".to_string(), "orphan".to_string()];
    let art = fold_stream(&bytes, &graph_nodes).expect("fold tolerates one trailing partial");

    // Interrupted representation (GAP 2, first-class field): the crash-truncated
    // run reads `overall_outcome = cancelled` INSIDE the closed enum, and the
    // distinction lives in the first-class `interrupted` flag.
    assert!(art.is_interrupted(), "no run-finished ⇒ interrupted");
    assert!(
        art.trailing_partial_discarded(),
        "the single trailing partial was tolerated and discarded"
    );
    assert_eq!(
        art.overall_outcome(),
        "cancelled",
        "outcome stays inside the closed schema enum"
    );

    let value = art.to_value();
    // The interrupted signal is a FIRST-CLASS, top-level artifact field — a
    // consumer detects the crash-truncation without reading `fold_reader`.
    assert_eq!(
        value.get("interrupted"),
        Some(&json!(true)),
        "interrupted is promoted to a top-level artifact field"
    );

    // Still schema-valid against the UNMODIFIED published run schema (the field is
    // additive; the schema is open-world at every level).
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("folded interrupted REAL-writer artifact must validate: {e}"));

    // Everything up to the crash is present — `load` succeeded on retry before the
    // cut. Non-vacuous: had the writer/fold wire shapes diverged, no `load`
    // succeeded attempt would appear.
    assert!(
        art.attempts()
            .iter()
            .any(|a| a.node() == "load" && a.status() == "succeeded"),
        "attempts up to the crash are included"
    );
}

// === T43: a REAL folded artifact WITH the critical path validates ==========
//
// T42 populated `critical_path_ns` with a conservative placeholder; T43 makes it
// the true dependency-respecting longest chain. This test folds a real two-node
// DEPENDENCY CHAIN stream, asserts the summary now carries the dependency-aware
// critical path (a→b executing chain, NOT the single longest attempt), and
// asserts the artifact — with that critical path — validates against the
// UNMODIFIED `schemas/run/v1.schema.json` (`critical_path_ns` is an existing
// summary field; T43 edits no schema).

/// A two-node chain a→b with known offsets: `a` executes 100→1000 (900ns), `b`
/// becomes ready only after `a` terminal (1000) and executes 1000→3000 (2000ns).
/// The dependency-respecting critical path is a(900)+b(2000) = 2900ns.
fn chain_stream() -> Vec<u8> {
    stream(&[
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(env(1, 50, "node-ready"), &[("node", json!("a"))]),
        with(env(2, 80, "node-admitted"), &[("node", json!("a"))]),
        with(
            env(3, 100, "attempt-started"),
            &[("node", json!("a")), ("attempt", json!(1))],
        ),
        with(
            env(4, 1000, "attempt-outcome"),
            &[
                ("node", json!("a")),
                ("attempt", json!(1)),
                ("status", json!("succeeded")),
            ],
        ),
        with(
            env(5, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
        // b becomes ready the instant a is terminal (1000), then executes 2000ns.
        with(env(6, 1000, "node-ready"), &[("node", json!("b"))]),
        with(env(7, 1000, "node-admitted"), &[("node", json!("b"))]),
        with(
            env(8, 1000, "attempt-started"),
            &[("node", json!("b")), ("attempt", json!(1))],
        ),
        with(
            env(9, 3000, "attempt-outcome"),
            &[
                ("node", json!("b")),
                ("attempt", json!(1)),
                ("status", json!("succeeded")),
            ],
        ),
        with(
            env(10, 3000, "node-terminal"),
            &[("node", json!("b")), ("state", json!("succeeded"))],
        ),
        with(
            env(11, 3000, "run-finished"),
            &[("outcome", json!("succeeded"))],
        ),
    ])
}

#[test]
fn folded_run_with_critical_path_validates_against_published_schema() {
    let art = fold_stream(&chain_stream(), &["a".to_string(), "b".to_string()]).expect("fold");
    let value = art.to_value();

    // (1) The summary carries the dependency-aware critical path: the a→b
    // executing chain 900 + 2000 = 2900, NOT the single longest attempt (2000).
    let cp = value["summary"]["critical_path_ns"].as_u64().unwrap();
    assert_eq!(cp, 2900, "critical path is the a→b executing chain");
    assert!(
        cp > value["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["phase_durations_ns"]["executing"].as_u64().unwrap())
            .max()
            .unwrap(),
        "the dependency chain exceeds any single node ⇒ T43, not the T42 placeholder"
    );
    // Total elapsed is the monotonic wall.
    assert_eq!(value["summary"]["total_elapsed_ns"].as_u64().unwrap(), 3000);

    // (2) The REAL artifact WITH the critical path validates against the
    // UNMODIFIED published run schema (`critical_path_ns` is an existing field).
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("folded artifact WITH critical path must validate: {e}"));

    // Teeth: a non-integer critical path is rejected — the check is not vacuous.
    let mut bad = value.clone();
    bad["summary"]["critical_path_ns"] = json!("nope");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the schema check has teeth on the critical-path field"
    );
}
