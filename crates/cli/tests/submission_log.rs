//! T108 · **the submission log**: the seam that makes a write-ahead record durable
//! *before* the pod exists without giving the run a second event-stream writer.
//! Written first (TDD).
//!
//! The tension it resolves is real. `NodeRunner::run` emits through a **buffering**
//! `AttemptEventSink` that the driver drains into the authoritative writer only
//! once the attempt returns — which is exactly right for an attempt's own records
//! and exactly wrong for a record whose whole purpose is to be durable *before* the
//! work is created. So the submission log wraps the run's `EventSink` and becomes
//! the run's **sequence authority**: driver lines pass through re-stamped with its
//! own counter, and a submission record it writes directly takes the next number.
//! One process, one mutex, one counter — the orchestrator is still the single
//! writer and `seq` is still gapless.
//!
//! The load-bearing assertion is byte-stability: with no submission records
//! interleaved, a stream through the log is **byte-identical** to the same stream
//! through a plain sink. If re-stamping perturbed canonicalization, every local
//! guarantee in the repo would be quietly wrong.

#![cfg(feature = "k8s")]

use std::io;
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, AttemptSubmittedRecord, EventSink, EventStreamWriter,
    FINGERPRINT_ALGORITHM_VERSION, MonotonicClock, RunId, RunOutcome, RunStartedHeader,
    TerminalState,
};
use dagr_cli::submission_log::SubmissionLog;
use serde_json::Value;

const RUN_ID: &str = "018f4a1e-6c2a-7b3d-9e10-0123456789ab";

#[derive(Clone, Default)]
struct CaptureSink {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().expect("sink mutex").clone()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.bytes
            .lock()
            .expect("sink mutex")
            .extend_from_slice(line);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct FrozenClock;

impl MonotonicClock for FrozenClock {
    fn elapsed_ns(&self) -> u64 {
        7_000
    }
}

fn header() -> RunStartedHeader {
    RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: Some("blake3:1111".to_string()),
        fingerprint_policy: Some("blake3:2222".to_string()),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: std::collections::BTreeMap::new(),
        data_interval: None,
        captured_env: std::collections::BTreeMap::new(),
        resumed_from: None,
    }
}

/// Write the same small run through `sink`.
fn a_run<S: EventSink>(sink: S) {
    let mut w = EventStreamWriter::new(
        sink,
        FrozenClock,
        RunId::new(RUN_ID),
        "example-pipeline".to_string(),
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());
    w.run_started(header()).expect("run-started");
    w.node_ready("extract").expect("node-ready");
    w.node_admitted("extract").expect("node-admitted");
    w.attempt_started("extract", 1).expect("attempt-started");
    w.attempt_succeeded("extract", 1).expect("attempt-succeeded");
    w.attempt_outcome(AttemptOutcomeRecord::new("extract", 1, "succeeded"))
        .expect("attempt-outcome");
    w.node_terminal("extract", TerminalState::Succeeded)
        .expect("node-terminal");
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");
}

fn records(bytes: &[u8]) -> Vec<Value> {
    dagr_artifact::event_stream::read_records(bytes)
        .expect("the stream parses")
        .records
}

#[test]
fn a_stream_through_the_log_with_no_submissions_is_byte_identical_to_a_plain_one() {
    let plain = CaptureSink::default();
    a_run(plain.clone());

    let wrapped = CaptureSink::default();
    let log = SubmissionLog::over(wrapped.clone(), RUN_ID, "example-pipeline");
    a_run(log.sink());

    assert_eq!(
        String::from_utf8_lossy(&wrapped.bytes()),
        String::from_utf8_lossy(&plain.bytes()),
        "re-stamping `seq` must be a no-op when the number is unchanged — otherwise \
         canonicalization has drifted and every byte-identity guarantee is void"
    );
}

#[test]
fn a_submission_record_takes_the_next_sequence_and_the_stream_stays_gapless() {
    let sink = CaptureSink::default();
    let log = SubmissionLog::over(sink.clone(), RUN_ID, "example-pipeline");
    let handle = log.handle();

    let mut w = EventStreamWriter::new(
        log.sink(),
        FrozenClock,
        RunId::new(RUN_ID),
        "example-pipeline".to_string(),
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    w.run_started(header()).expect("run-started");
    w.node_ready("extract").expect("node-ready");
    // …the runner records its intent out of band, before the pod exists…
    handle
        .record(AttemptSubmittedRecord::new("extract", 1).target_name("dagr-extract-1"))
        .expect("the submission record is written and flushed");
    w.node_admitted("extract").expect("node-admitted");
    w.attempt_started("extract", 1).expect("attempt-started");
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");

    let recs = records(&sink.bytes());
    let kinds: Vec<&str> = recs.iter().map(|r| r["kind"].as_str().unwrap()).collect();
    assert_eq!(
        kinds,
        vec![
            "run-started",
            "node-ready",
            "attempt-submitted",
            "node-admitted",
            "attempt-started",
            "run-finished"
        ],
        "the submission record lands in stream order, where it happened"
    );
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(
            r["seq"].as_u64(),
            Some(i as u64),
            "gapless and strictly increasing across both writers"
        );
    }
    assert!(
        recs.iter().all(|r| r["run_id"] == RUN_ID
            && r["schema_version"] == "dagr.event-stream@1"),
        "every record carries the run identity and schema version"
    );
}

#[test]
fn a_recorded_submission_is_flushed_the_moment_it_is_written() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("events.jsonl");
    let log = SubmissionLog::open(&path, RUN_ID, "example-pipeline").expect("the log opens");
    log.handle()
        .record(AttemptSubmittedRecord::new("extract", 1))
        .expect("written");

    // Read it back through a second file handle: nothing is sitting in a buffer.
    let bytes = std::fs::read(&path).expect("the file exists");
    let recs = records(&bytes);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["kind"], "attempt-submitted");
}
