//! T89 · durable-reference **metadata** recording + fold. Written first (TDD).
//!
//! T57 landed the opaque `durable_reference` recording bridge. This suite covers
//! the OPTIONAL metadata that rides alongside it: `durable_reference_meta {
//! content_hash, size_bytes, scheme, produced_at_offset_ns }`, supplied by the
//! task's `DurableOutput` impl. It is additive end to end:
//!
//!   - a durable success WITH metadata carries `durable_reference_meta` on the
//!     `attempt-outcome` event, and `fold_stream` places it on the `AttemptRecord`;
//!   - a durable success (or any attempt) that supplies NO metadata leaves the
//!     field ABSENT — the stream is byte-identical to a pre-T89 run;
//!   - an OLD stream carrying a `durable_reference` but no metadata still folds,
//!     the reference intact and the meta field absent (open-world, additive).
//!
//! The metadata is opaque per-field data the impl chose; dagr records it verbatim.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, DurableReferenceMeta, EventSink, EventStreamWriter,
    FINGERPRINT_ALGORITHM_VERSION, MonotonicClock, RunId, RunOutcome, RunStartedHeader,
    TerminalState, record_durable_reference, record_durable_reference_meta,
};
use dagr_artifact::fold::{RunArtifact, fold_stream};

// === Test scaffolding: a real writer over a capture sink + manual clock ====

#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl CaptureSink {
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

fn header() -> RunStartedHeader {
    RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: Some(
            "blake3:1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        fingerprint_policy: Some(
            "blake3:2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: BTreeMap::new(),
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    }
}

fn ok(node: &str, attempt: u32) -> AttemptOutcomeRecord {
    AttemptOutcomeRecord::new(node, attempt, TerminalState::Succeeded.as_str())
}

/// Drive a REAL writer through one attempt lifecycle per record and fold the bytes.
fn drive_and_fold(records: &[AttemptOutcomeRecord], graph_nodes: &[&str]) -> RunArtifact {
    let sink = CaptureSink::default();
    let clock = ManualClock::new();
    let mut w = EventStreamWriter::new(
        sink.clone(),
        clock.clone(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-0123456789ab"),
        "example-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    let mut t = 0u64;
    clock.set(t);
    w.run_started(header()).expect("run-started");

    for rec in records {
        let node = rec.node.clone();
        let attempt = rec.attempt;
        t += 100;
        clock.set(t);
        w.attempt_started(&node, attempt).expect("attempt-started");
        t += 100;
        clock.set(t);
        w.attempt_succeeded(&node, attempt).expect("succeeded");
        w.attempt_outcome(rec.clone()).expect("attempt-outcome");
    }
    for n in graph_nodes {
        t += 10;
        clock.set(t);
        w.node_terminal(n, TerminalState::Succeeded)
            .expect("node-terminal");
    }
    t += 10;
    clock.set(t);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    let nodes: Vec<String> = graph_nodes.iter().map(|s| (*s).to_string()).collect();
    fold_stream(&sink.bytes(), &nodes).expect("fold")
}

// ---------------------------------------------------------------------------
// The recording bridge: supplied metadata is carried on the record; None leaves
// the field absent.
// ---------------------------------------------------------------------------

#[test]
fn recording_metadata_carries_it_on_the_record() {
    let mut rec = ok("snap", 1);
    record_durable_reference(&mut rec, Some("snap/output".to_string()));
    let meta = DurableReferenceMeta::new()
        .content_hash("sha256:deadbeef")
        .size_bytes(1024)
        .scheme("s3")
        .produced_at_offset_ns(500);
    record_durable_reference_meta(&mut rec, Some(meta));
    let carried = rec
        .durable_reference_meta
        .as_ref()
        .expect("the metadata is carried on the record");
    assert_eq!(carried["content_hash"], json!("sha256:deadbeef"));
    assert_eq!(carried["size_bytes"], json!(1024));
    assert_eq!(carried["scheme"], json!("s3"));
    assert_eq!(carried["produced_at_offset_ns"], json!(500));
}

#[test]
fn recording_no_metadata_leaves_the_field_absent() {
    let mut rec = ok("plain", 1);
    record_durable_reference_meta(&mut rec, None);
    assert_eq!(
        rec.durable_reference_meta, None,
        "no metadata recorded ⇒ the field stays absent"
    );
}

// ---------------------------------------------------------------------------
// End-to-end through the REAL writer + fold: a durable success WITH metadata
// lands it on the folded AttemptRecord; a success with none lands nothing.
// ---------------------------------------------------------------------------

#[test]
fn folded_durable_success_carries_its_metadata() {
    let mut durable = ok("snap", 1);
    record_durable_reference(&mut durable, Some("snap/output".to_string()));
    record_durable_reference_meta(
        &mut durable,
        Some(
            DurableReferenceMeta::new()
                .content_hash("sha256:abc123")
                .size_bytes(4096)
                .scheme("file"),
        ),
    );

    let art = drive_and_fold(&[durable], &["snap"]);
    let snap = art
        .attempts()
        .iter()
        .find(|a| a.node() == "snap")
        .expect("snap present");

    assert_eq!(
        snap.durable_reference(),
        Some(&json!("snap/output")),
        "the opaque reference is unchanged"
    );
    let meta = snap
        .durable_reference_meta()
        .expect("the folded record carries the metadata");
    assert_eq!(meta["content_hash"], json!("sha256:abc123"));
    assert_eq!(meta["size_bytes"], json!(4096));
    assert_eq!(meta["scheme"], json!("file"));
}

#[test]
fn a_durable_success_without_metadata_folds_with_the_field_absent() {
    let mut durable = ok("snap", 1);
    record_durable_reference(&mut durable, Some("snap/output".to_string()));
    // No metadata recorded.

    let art = drive_and_fold(&[durable], &["snap"]);
    let snap = art
        .attempts()
        .iter()
        .find(|a| a.node() == "snap")
        .expect("snap present");
    assert_eq!(
        snap.durable_reference(),
        Some(&json!("snap/output")),
        "the reference is intact"
    );
    assert_eq!(
        snap.durable_reference_meta(),
        None,
        "no metadata ⇒ the field is absent (behavior unchanged)"
    );
}

#[test]
fn a_run_without_any_metadata_is_byte_identical() {
    // A no-metadata run whose records pass through the meta bridge with `None` is
    // byte-identical to one that never touched the bridge — additive, no field.
    let baseline = drive_and_fold(&[ok("a", 1), ok("b", 1)], &["a", "b"]);
    let via_bridge = drive_and_fold(
        &[
            {
                let mut r = ok("a", 1);
                record_durable_reference_meta(&mut r, None);
                r
            },
            {
                let mut r = ok("b", 1);
                record_durable_reference_meta(&mut r, None);
                r
            },
        ],
        &["a", "b"],
    );
    assert_eq!(
        serde_json::to_string(&baseline.to_value()).unwrap(),
        serde_json::to_string(&via_bridge.to_value()).unwrap(),
        "a no-metadata run is byte-identical; the meta slot is absent throughout"
    );
}

// ---------------------------------------------------------------------------
// Open-world: an OLD stream (a durable_reference, no metadata field) still folds,
// the reference intact and the meta field absent — additive evolution.
// ---------------------------------------------------------------------------

#[test]
fn an_old_stream_without_the_meta_field_still_folds() {
    // Hand-authored pre-T89 wire records: run-started, an attempt-outcome carrying
    // only `durable_reference` (no `durable_reference_meta`), node-terminal,
    // run-finished. The fold must read the reference and default the meta absent.
    let sv = "dagr.event-stream@1";
    let run_id = "018f4a1e-6c2a-7b3d-9e10-0123456789ab";
    let mut bytes = String::new();
    // run-started (header spread under `header`)
    let started = json!({
        "schema_version": sv, "run_id": run_id, "seq": 0, "wall": "2026-07-23T00:00:00.000Z",
        "offset_ns": 0, "kind": "run-started",
        "header": {
            "run_id": run_id, "pipeline": "old", "fingerprint_structural": "blake3:aa",
            "fingerprint_policy": "blake3:bb", "fingerprint_algorithm_version": 1,
            "parameters": {}, "data_interval": Value::Null, "captured_environment": {},
            "resume_lineage": Value::Null
        }
    });
    bytes.push_str(&started.to_string());
    bytes.push('\n');
    // attempt-outcome with only durable_reference (the pre-T89 shape)
    let outcome = json!({
        "schema_version": sv, "run_id": run_id, "seq": 1, "wall": "2026-07-23T00:00:00.010Z",
        "offset_ns": 100, "kind": "attempt-outcome",
        "node": "snap", "attempt": 1, "status": "succeeded",
        "durable_reference": "snap/output"
    });
    bytes.push_str(&outcome.to_string());
    bytes.push('\n');
    let terminal = json!({
        "schema_version": sv, "run_id": run_id, "seq": 2, "wall": "2026-07-23T00:00:00.020Z",
        "offset_ns": 200, "kind": "node-terminal", "node": "snap", "state": "succeeded"
    });
    bytes.push_str(&terminal.to_string());
    bytes.push('\n');
    let finished = json!({
        "schema_version": sv, "run_id": run_id, "seq": 3, "wall": "2026-07-23T00:00:00.030Z",
        "offset_ns": 300, "kind": "run-finished", "outcome": "succeeded"
    });
    bytes.push_str(&finished.to_string());
    bytes.push('\n');

    let art = fold_stream(bytes.as_bytes(), &["snap".to_string()]).expect("old stream folds");
    let snap = art
        .attempts()
        .iter()
        .find(|a| a.node() == "snap")
        .expect("snap present");
    assert_eq!(
        snap.durable_reference(),
        Some(&json!("snap/output")),
        "the old stream's reference folds unchanged"
    );
    assert_eq!(
        snap.durable_reference_meta(),
        None,
        "the absent meta field defaults to None (open-world, additive)"
    );
}
