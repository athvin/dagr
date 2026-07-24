//! C27 · Durable-output **reference recording** — ticket T57 (067). Written
//! first, TDD.
//!
//! T0.8 (ADR 014 §6) decided the per-attempt `durable_reference` field; T39/T42
//! landed the schema slot and the fold that reads it. T57 lands the **recording
//! bridge**: when a durable node produces its output successfully, the serialized
//! reference (the `String` the output type's `DurableOutput::serialize_reference`
//! yields, C27) is captured into that attempt's outcome record, folds into the run
//! artifact through the REAL C19 writer, and round-trips through the run schema.
//!
//! What this suite proves (test plan, ticket 067):
//!   - a durable node's **successful** attempt records exactly one reference;
//!   - a **non-durable** success records **none**;
//!   - a **failed** attempt records none; a fail-then-succeed node records the
//!     reference **only** on the succeeding attempt, one record per attempt;
//!   - the recorded reference is a plain **self-contained serialized value** with
//!     no live handle, and round-trips through the run artifact and back;
//!   - a run with **no** durable declaration is byte-identical (determinism).
//!
//! The reference recorded is opaque to dagr — whatever the task's output type
//! serialized. A `String` from the core contract is carried as a JSON string.

use std::cell::Cell;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dagr_artifact::event_stream::{
    record_durable_reference, AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock,
    RunId, RunOutcome, RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::{fold_stream, RunArtifact};

// === Test scaffolding: a real writer over a capture sink + manual clock ====

#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().iter().flatten().copied().collect()
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
        parameters: Default::default(),
        data_interval: None,
        captured_env: Default::default(),
        resumed_from: None,
    }
}

/// A minimal succeeded attempt-outcome record.
fn ok(node: &str, attempt: u32) -> AttemptOutcomeRecord {
    AttemptOutcomeRecord::new(node, attempt, TerminalState::Succeeded.as_str())
}
fn failed(node: &str, attempt: u32) -> AttemptOutcomeRecord {
    AttemptOutcomeRecord::new(node, attempt, TerminalState::Failed.as_str())
}

/// Drive a REAL writer through one attempt lifecycle per outcome record and fold
/// the bytes it actually wrote into a run artifact. Each record's node reaches a
/// terminal on its final listed attempt.
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
        // The closing per-transition event matching the record's status.
        match rec.status.as_str() {
            s if s == TerminalState::Succeeded.as_str() => {
                w.attempt_succeeded(&node, attempt).expect("succeeded");
            }
            _ => {
                w.attempt_failed(&node, attempt).expect("failed");
            }
        }
        w.attempt_outcome(rec.clone()).expect("attempt-outcome");
    }
    // One node-terminal per node, at its final recorded status.
    for n in graph_nodes {
        let last = records.iter().rev().find(|r| r.node == *n);
        let state = last.map_or(TerminalState::Succeeded, |r| {
            if r.status == TerminalState::Succeeded.as_str() {
                TerminalState::Succeeded
            } else {
                TerminalState::Failed
            }
        });
        t += 10;
        clock.set(t);
        w.node_terminal(n, state).expect("node-terminal");
    }
    t += 10;
    clock.set(t);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    let nodes: Vec<String> = graph_nodes.iter().map(|s| s.to_string()).collect();
    fold_stream(&sink.bytes(), &nodes).expect("fold")
}

// ---------------------------------------------------------------------------
// The recording bridge: a durable success's serialized reference is carried on
// the attempt-outcome record (the `String` from the core contract → the opaque
// artifact reference value).
// ---------------------------------------------------------------------------

#[test]
fn a_durable_success_records_its_serialized_reference() {
    let mut rec = AttemptOutcomeRecord::new("snap", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut rec, Some("snap-node/output".to_string()));
    assert_eq!(
        rec.durable_reference,
        Some(json!("snap-node/output")),
        "the serialized reference is carried as an opaque value on the record"
    );
}

#[test]
fn recording_none_leaves_the_reference_absent() {
    let mut rec = AttemptOutcomeRecord::new("plain", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut rec, None);
    assert_eq!(rec.durable_reference, None, "no reference is recorded");
}

// ---------------------------------------------------------------------------
// End-to-end through the REAL writer + fold: a durable success lands one
// reference in the attempt record; a non-durable success lands none.
// ---------------------------------------------------------------------------

#[test]
fn folded_durable_success_carries_exactly_one_reference() {
    let mut durable = ok("snap", 1);
    record_durable_reference(&mut durable, Some("snap/output".to_string()));
    let plain = ok("plain", 1);

    let art = drive_and_fold(&[durable, plain], &["snap", "plain"]);

    let snap = art
        .attempts()
        .iter()
        .find(|a| a.node() == "snap")
        .expect("snap present");
    assert_eq!(
        snap.durable_reference(),
        Some(&json!("snap/output")),
        "the durable node's succeeded attempt carries its reference"
    );

    let plain = art
        .attempts()
        .iter()
        .find(|a| a.node() == "plain")
        .expect("plain present");
    assert_eq!(
        plain.durable_reference(),
        None,
        "the non-durable success carries no reference; status/phases stand"
    );
    assert_eq!(plain.status(), "succeeded");
    assert!(
        plain.total_elapsed_ns() > 0,
        "phases are recorded as usual for the non-durable node"
    );
}

// ---------------------------------------------------------------------------
// Failed then retried: the failed attempt records no reference; the succeeding
// attempt records exactly one; one record per attempt (retries not collapsed).
// ---------------------------------------------------------------------------

#[test]
fn failed_attempt_records_no_reference_the_succeeding_one_records_it() {
    let fail = failed("snap", 1);
    let mut ok2 = ok("snap", 2);
    record_durable_reference(&mut ok2, Some("snap/final".to_string()));

    let art = drive_and_fold(&[fail, ok2], &["snap"]);

    let attempts: Vec<_> = art
        .attempts()
        .iter()
        .filter(|a| a.node() == "snap")
        .collect();
    assert_eq!(
        attempts.len(),
        2,
        "one record per attempt; retries not collapsed"
    );
    assert_eq!(attempts[0].attempt_number(), 1);
    assert_eq!(attempts[0].status(), "failed");
    assert_eq!(
        attempts[0].durable_reference(),
        None,
        "the failed attempt records no reference"
    );
    assert_eq!(attempts[1].attempt_number(), 2);
    assert_eq!(attempts[1].status(), "succeeded");
    assert_eq!(
        attempts[1].durable_reference(),
        Some(&json!("snap/final")),
        "the succeeding attempt records exactly one reference"
    );
}

// ---------------------------------------------------------------------------
// The recorded reference is self-contained (no live handle) and round-trips
// through the run artifact and back to the SAME value.
// ---------------------------------------------------------------------------

#[test]
fn recorded_reference_is_self_contained_and_round_trips_through_the_artifact() {
    let mut durable = ok("snap", 1);
    // A structured, self-describing reference (a JSON blob a real task serialized).
    let reference = json!({ "url": "file:///runs/r1/snap", "sha256": "abc123" }).to_string();
    record_durable_reference(&mut durable, Some(reference.clone()));

    let art = drive_and_fold(&[durable], &["snap"]);
    let value = art.to_value();

    let idx = value["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .position(|a| a["node"] == json!("snap"))
        .unwrap();
    // The reference is a plain value in the serialized artifact (no handle).
    assert_eq!(
        value["attempts"][idx]["durable_reference"],
        Value::String(reference.clone()),
        "the reference is a self-contained serialized value in the artifact"
    );

    // Re-read the artifact bytes and recover the identical reference string.
    let reserialized = serde_json::to_string(&value).unwrap();
    let reread: Value = serde_json::from_str(&reserialized).unwrap();
    assert_eq!(
        reread["attempts"][idx]["durable_reference"],
        Value::String(reference),
        "the reference deserializes back to the same value"
    );
}

// ---------------------------------------------------------------------------
// Determinism: a run with NO durable declaration is byte-identical whether or
// not each record passes through the recording bridge with `None`.
// ---------------------------------------------------------------------------

#[test]
fn a_run_with_no_durable_declaration_is_byte_identical() {
    let baseline = drive_and_fold(
        &[ok("a", 1), ok("b", 1), ok("c", 1)],
        &["a", "b", "c"],
    );

    let via_bridge = drive_and_fold(
        &[
            {
                let mut r = ok("a", 1);
                record_durable_reference(&mut r, None);
                r
            },
            {
                let mut r = ok("b", 1);
                record_durable_reference(&mut r, None);
                r
            },
            {
                let mut r = ok("c", 1);
                record_durable_reference(&mut r, None);
                r
            },
        ],
        &["a", "b", "c"],
    );

    assert_eq!(
        serde_json::to_string(&baseline.to_value()).unwrap(),
        serde_json::to_string(&via_bridge.to_value()).unwrap(),
        "a no-declaration run is byte-identical; the reference slot is absent throughout"
    );
    assert!(
        via_bridge
            .attempts()
            .iter()
            .all(|a| a.durable_reference().is_none()),
        "no reference is recorded when nothing is durable"
    );
}
