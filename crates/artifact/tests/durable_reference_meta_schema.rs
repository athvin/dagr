//! T89 · durable-reference **metadata schema round-trip**, written first (TDD).
//! Gated behind `schema-validation` (default OFF), like the other schema tests.
//!
//! A REAL folded run artifact carrying `durable_reference_meta` validates against
//! the published `schemas/run/v1.schema.json`. The schema is minor-bumped
//! **additively** — the new optional `durable_reference_meta` object is described
//! but nothing is required and no object is closed — so BOTH a document carrying
//! the metadata and an OLD document without it validate. This suite proves both
//! and gives the additive property teeth (a malformed metadata value is rejected).

#![cfg(feature = "schema-validation")]

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
use dagr_artifact::fold::fold_stream;
use dagr_artifact::schema::{ArtifactKind, validate_value};

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

#[test]
fn a_real_folded_artifact_with_reference_metadata_validates() {
    let sink = CaptureSink::default();
    let clock = ManualClock {
        now: Rc::new(Cell::new(0)),
    };
    let mut w = EventStreamWriter::new(
        sink.clone(),
        clock.clone(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-0123456789ab"),
        "example-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    clock.now.set(0);
    w.run_started(header()).expect("run-started");
    clock.now.set(100);
    w.attempt_started("snap", 1).expect("attempt-started");
    clock.now.set(500);
    w.attempt_succeeded("snap", 1).expect("succeeded");

    let mut rec = AttemptOutcomeRecord::new("snap", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut rec, Some("file:///runs/r1/snap".to_string()));
    record_durable_reference_meta(
        &mut rec,
        Some(
            DurableReferenceMeta::new()
                .content_hash("sha256:abc123")
                .size_bytes(4096)
                .scheme("file")
                .produced_at_offset_ns(500),
        ),
    );
    w.attempt_outcome(rec).expect("attempt-outcome");

    clock.now.set(500);
    w.node_terminal("snap", TerminalState::Succeeded)
        .expect("node-terminal");
    clock.now.set(500);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    let art = fold_stream(&sink.bytes(), &["snap".to_string()]).expect("fold");
    let value = art.to_value();

    assert_eq!(
        value["attempts"][0]["durable_reference_meta"]["content_hash"],
        json!("sha256:abc123"),
        "the metadata is populated on the attempt record"
    );

    // The REAL artifact validates against the published (minor-bumped, additive) schema.
    validate_value(ArtifactKind::Run, 1, &value).unwrap_or_else(|e| {
        panic!("REAL folded artifact with durable_reference_meta must validate: {e}")
    });

    // Teeth: a malformed metadata value (a non-integer size) is rejected — the
    // additive schema still constrains the field's shape.
    let mut bad = value.clone();
    bad["attempts"][0]["durable_reference_meta"]["size_bytes"] = json!("not-a-number");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the metadata schema round-trip has teeth"
    );
}

#[test]
fn an_old_artifact_without_reference_metadata_still_validates() {
    // A pre-T89 run artifact: a durable reference, NO metadata field. It must still
    // validate against the minor-bumped schema (additive: the field is optional).
    let old = json!({
        "header": {
            "run_id": "r-old", "pipeline": "old",
            "fingerprint_structural": "blake3:aa", "fingerprint_policy": "blake3:bb",
            "fingerprint_algorithm_version": 1,
            "parameters": {}, "data_interval": Value::Null, "captured_environment": {},
            "resume_lineage": Value::Null, "overall_outcome": "succeeded"
        },
        "attempts": [{
            "node": "snap", "attempt": 1, "status": "succeeded",
            "phase_durations_ns": { "executing": 400, "ready-wait": 0, "permit-wait": 0, "backoff": 0 },
            "worker": "unknown", "metrics": {},
            "durable_reference": "snap/output"
        }],
        "summary": Value::Null
    });
    validate_value(ArtifactKind::Run, 1, &old)
        .unwrap_or_else(|e| panic!("an old artifact without metadata must still validate: {e}"));
}
