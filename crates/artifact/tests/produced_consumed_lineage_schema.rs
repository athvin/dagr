//! T90 · produced/consumed **lineage schema round-trip**, written first (TDD).
//! Gated behind `schema-validation` (default OFF), like the other schema tests.
//!
//! A REAL folded run artifact carrying the append-only `outputs[]` and per-attempt
//! consumed `inputs[]` validates against the published `schemas/run/v1.schema.json`,
//! and a REAL event stream carrying an `output-produced` record validates against
//! `schemas/event-stream/v1.schema.json`. Both schemas are minor-bumped
//! **additively** (`dagr.run@1.2` / `dagr.event-stream@1.2`, same major, same
//! `v1.schema.json` file, nothing required, no object closed) — so BOTH a document
//! carrying the lineage and an OLD document without it validate. Teeth: a malformed
//! lineage value is rejected.

#![cfg(feature = "schema-validation")]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dagr_artifact::event_stream::{
    record_consumed_inputs, record_durable_reference, record_durable_reference_meta,
    AttemptOutcomeRecord, ConsumedInput, DurableReferenceMeta, EventSink, EventStreamWriter,
    MonotonicClock, OutputProducedRecord, RunId, RunOutcome, RunStartedHeader, TerminalState,
    FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::fold_stream;
use dagr_artifact::schema::{validate_bytes, validate_value, ArtifactKind};

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
    fn lines(&self) -> Vec<Vec<u8>> {
        self.lines.lock().unwrap().clone()
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

const RUN_ID: &str = "018f4a1e-6c2a-7b3d-9e10-0123456789ab";

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
fn a_real_folded_artifact_with_lineage_validates_and_the_stream_does_too() {
    let sink = CaptureSink::default();
    let clock = ManualClock {
        now: Rc::new(Cell::new(0)),
    };
    let mut w = EventStreamWriter::new(
        sink.clone(),
        clock.clone(),
        RunId::from_operator(RUN_ID),
        "example-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    clock.now.set(0);
    w.run_started(header()).expect("run-started");

    // Producer p (durable output).
    clock.now.set(100);
    w.attempt_started("p", 1).expect("started");
    clock.now.set(200);
    w.attempt_succeeded("p", 1).expect("succeeded");
    let mut p = AttemptOutcomeRecord::new("p", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut p, Some("file:///runs/r1/p".to_string()));
    record_durable_reference_meta(
        &mut p,
        Some(
            DurableReferenceMeta::new()
                .content_hash("sha256:abc123")
                .size_bytes(4096)
                .scheme("file")
                .produced_at_offset_ns(200),
        ),
    );
    w.attempt_outcome(p).expect("outcome");
    w.output_produced(OutputProducedRecord {
        node: "p".to_string(),
        attempt: 1,
        uri: "file:///runs/r1/p".to_string(),
        content_hash: Some("sha256:abc123".to_string()),
        size_bytes: Some(4096),
        kind: Some("file".to_string()),
        produced_at_offset_ns: 200,
        originating_run: RUN_ID.to_string(),
    })
    .expect("output-produced");
    w.node_terminal("p", TerminalState::Succeeded).expect("t p");

    // Consumer c reads p.
    clock.now.set(300);
    w.attempt_started("c", 1).expect("started");
    clock.now.set(400);
    w.attempt_succeeded("c", 1).expect("succeeded");
    let mut c = AttemptOutcomeRecord::new("c", 1, TerminalState::Succeeded.as_str());
    record_consumed_inputs(
        &mut c,
        vec![ConsumedInput {
            uri: "file:///runs/r1/p".to_string(),
            content_hash: Some("sha256:abc123".to_string()),
        }],
    );
    w.attempt_outcome(c).expect("outcome");
    w.node_terminal("c", TerminalState::Succeeded).expect("t c");

    clock.now.set(400);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    // Every event-stream record — including the output-produced one — validates.
    for line in sink.lines() {
        validate_bytes(ArtifactKind::EventStream, 1, &line).unwrap_or_else(|e| {
            panic!(
                "every stream record must validate: {} :: {e}",
                String::from_utf8_lossy(&line)
            )
        });
    }

    let art = fold_stream(&sink.bytes(), &["p".to_string(), "c".to_string()]).expect("fold");
    let value = art.to_value();

    assert_eq!(value["outputs"][0]["uri"], json!("file:///runs/r1/p"));
    let cc = value["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["node"] == json!("c"))
        .unwrap();
    assert_eq!(cc["inputs"][0]["uri"], json!("file:///runs/r1/p"));

    // The REAL artifact validates against the published (minor-bumped) run schema.
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("REAL folded artifact with lineage must validate: {e}"));

    // Teeth: a malformed produced-output value (a non-integer size) is rejected.
    let mut bad = value.clone();
    bad["outputs"][0]["size_bytes"] = json!("not-a-number");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the lineage schema round-trip has teeth"
    );
}

#[test]
fn an_old_artifact_without_lineage_still_validates() {
    // A pre-T90 run artifact: no outputs[], no inputs[]. It must still validate
    // against the minor-bumped schema (additive: both are optional).
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
        .unwrap_or_else(|e| panic!("an old artifact without lineage must still validate: {e}"));
}
