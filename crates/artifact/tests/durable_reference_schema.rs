//! C27 · Durable reference **schema round-trip** — ticket T57 (067), written
//! first (TDD). Gated behind `schema-validation` (default OFF), the CI-/dev-scoped
//! validator (T4 ADR 017 §4), like the other T39/T42 schema round-trips.
//!
//! A REAL folded run artifact carrying a durable node's recorded reference
//! validates against the UNMODIFIED published `schemas/run/v1.schema.json` (T39,
//! §`durable_reference` — "OPAQUE to the schema"). T57 edits **no** schema: the
//! `durable_reference` slot was published by T39; this proves the recording bridge
//! emits into it validly. Teeth: a corrupted copy is rejected.

#![cfg(feature = "schema-validation")]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dagr_artifact::event_stream::{
    record_durable_reference, AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock,
    RunId, RunOutcome, RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::fold_stream;
use dagr_artifact::schema::{validate_value, ArtifactKind};

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
fn a_real_folded_artifact_with_a_durable_reference_validates() {
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

    // The durable success: the recording bridge stamps the serialized reference.
    let mut rec = AttemptOutcomeRecord::new("snap", 1, TerminalState::Succeeded.as_str());
    let reference = json!({ "url": "file:///runs/r1/snap", "sha256": "abc123" }).to_string();
    record_durable_reference(&mut rec, Some(reference.clone()));
    w.attempt_outcome(rec).expect("attempt-outcome");

    clock.now.set(500);
    w.node_terminal("snap", TerminalState::Succeeded)
        .expect("node-terminal");
    clock.now.set(500);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    let art = fold_stream(&sink.bytes(), &["snap".to_string()]).expect("fold");
    let value = art.to_value();

    // The recorded reference is present on the attempt record …
    assert_eq!(
        value["attempts"][0]["durable_reference"],
        Value::String(reference),
        "the recorded reference is populated on the attempt record"
    );

    // … and the REAL artifact validates against the UNMODIFIED published schema.
    validate_value(ArtifactKind::Run, 1, &value).unwrap_or_else(|e| {
        panic!("REAL folded artifact with a durable reference must validate: {e}")
    });

    // Teeth: a corrupted copy (non-integer phase duration) is rejected — the check
    // is not vacuously passing everything with the reference present.
    let mut bad = value.clone();
    bad["attempts"][0]["phase_durations_ns"]["executing"] = json!("nope");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the schema round-trip has teeth"
    );
}
