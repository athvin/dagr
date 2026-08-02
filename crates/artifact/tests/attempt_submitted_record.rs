//! **The `attempt-submitted` write-ahead record (`dagr.event-stream@1.3`).**
//! Written first (TDD).
//!
//! ADR 115 §9: everything else in the stream records an attempt's *outcome*, and
//! an outcome is written after the fact. The submission record is the one written
//! **before** the remote work exists, so a crash between deciding and submitting
//! still leaves a durable statement of intent — and so "what was this task launched
//! with?" survives the platform garbage-collecting its own work object.
//!
//! What is pinned here is the record's *shape and its additivity*: the identity
//! triple, the ordered positional inputs (an empty array for a consume-nothing
//! source, never a null), intent and reality recorded separately, and the fact that
//! a stream carrying these records folds to exactly the artifact it folds to
//! without them. The ordering guarantee — flushed before the create call — is a
//! property of the executor and is pinned in `dagr-cli`'s suite, where the create
//! call is.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, AttemptSubmittedRecord, ConsumedInput, EventSink, EventStreamWriter,
    FINGERPRINT_ALGORITHM_VERSION, MonotonicClock, RunId, RunOutcome, RunStartedHeader,
    TerminalState,
};
use dagr_artifact::fold::fold_stream;
use serde_json::Value;

// ---------------------------------------------------------------------------
// A minimal in-memory sink and a frozen clock. The clock is frozen on purpose:
// the additivity comparison below folds two streams that differ ONLY by the
// presence of the new kind, and a stepping clock would let the extra records
// shift every later offset and turn a real comparison into a trivially-failing
// one.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.lines
            .lock()
            .expect("sink mutex")
            .iter()
            .flatten()
            .copied()
            .collect()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.lines.lock().expect("sink mutex").push(line.to_vec());
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct FrozenClock;

impl MonotonicClock for FrozenClock {
    fn elapsed_ns(&self) -> u64 {
        1_000
    }
}

const RUN_ID: &str = "018f4a1e-6c2a-7b3d-9e10-0123456789ab";

fn writer(sink: CaptureSink) -> EventStreamWriter<CaptureSink, FrozenClock> {
    EventStreamWriter::new(
        sink,
        FrozenClock,
        RunId::from_operator(RUN_ID),
        "example-pipeline".to_string(),
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string())
}

fn header() -> RunStartedHeader {
    RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: Some("blake3:1111".to_string()),
        fingerprint_policy: Some("blake3:2222".to_string()),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: BTreeMap::new(),
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    }
}

/// Every emitted line, parsed.
fn records(bytes: &[u8]) -> Vec<Value> {
    dagr_artifact::event_stream::read_records(bytes)
        .expect("the stream parses")
        .records
}

fn submitted(node: &str, attempt: u32) -> AttemptSubmittedRecord {
    AttemptSubmittedRecord::new(node, attempt)
}

// ---------------------------------------------------------------------------
// Identity, fingerprints, provenance
// ---------------------------------------------------------------------------

#[test]
fn a_submission_record_carries_the_identity_triple_and_the_launch_provenance() {
    let sink = CaptureSink::default();
    let mut w = writer(sink.clone());
    w.attempt_submitted(
        submitted("extract", 3)
            .structural_fingerprint("blake3:1111")
            .policy_hash("blake3:2222")
            .tool_version("dagr@1")
            .image_digest("sha256:cafebabe")
            .executor("k8s")
            .target_name("dagr-extract-3"),
    )
    .expect("the record is emitted");

    let recs = records(&sink.bytes());
    assert_eq!(recs.len(), 1, "one record was emitted");
    let r = &recs[0];

    assert_eq!(r["kind"], "attempt-submitted");
    // `run_id` is on the envelope; `node` + `attempt` complete the triple.
    assert_eq!(r["run_id"], RUN_ID);
    assert_eq!(r["node"], "extract");
    assert_eq!(r["attempt"], 3);
    assert_eq!(r["schema_version"], "dagr.event-stream@1");

    assert_eq!(r["structural_fingerprint"], "blake3:1111");
    assert_eq!(r["policy_hash"], "blake3:2222");
    assert_eq!(r["tool_version"], "dagr@1");
    assert_eq!(r["image_digest"], "sha256:cafebabe");
    assert_eq!(r["executor"], "k8s");
    assert_eq!(
        r["target_name"], "dagr-extract-3",
        "the INTENDED name is recorded before creation"
    );
}

#[test]
fn intent_and_reality_are_separate_facts_and_the_observed_identity_is_additive() {
    let sink = CaptureSink::default();
    let mut w = writer(sink.clone());
    // The write-ahead half: intent only, no observed identity yet.
    w.attempt_submitted(submitted("extract", 1).target_name("dagr-extract-1"))
        .expect("intent is emitted");
    // Once creation returns, the platform's own identity is recorded additively.
    w.attempt_submitted(
        submitted("extract", 1)
            .target_name("dagr-extract-1")
            .observed_name("dagr-extract-1")
            .observed_uid("6f0f1b2c-3d4e-5a6b-7c8d-9e0f1a2b3c4d")
            .observed_host("kind-worker2"),
    )
    .expect("reality is emitted");

    let recs = records(&sink.bytes());
    assert_eq!(recs.len(), 2);

    assert!(
        recs[0].get("observed_name").is_none()
            && recs[0].get("observed_uid").is_none()
            && recs[0].get("observed_host").is_none(),
        "the write-ahead record cannot carry an identity that does not exist yet"
    );
    assert_eq!(recs[1]["observed_name"], "dagr-extract-1");
    assert_eq!(
        recs[1]["observed_uid"],
        "6f0f1b2c-3d4e-5a6b-7c8d-9e0f1a2b3c4d"
    );
    assert_eq!(recs[1]["observed_host"], "kind-worker2");
}

// ---------------------------------------------------------------------------
// The ordered positional inputs
// ---------------------------------------------------------------------------

#[test]
fn inputs_are_recorded_in_declared_positional_order_with_their_content_hashes() {
    let sink = CaptureSink::default();
    let mut w = writer(sink.clone());
    w.attempt_submitted(submitted("join", 1).inputs(vec![
        ConsumedInput {
            uri: "dagr-blob+local://c/sha256/aaa".to_string(),
            content_hash: Some("sha256:aaa".to_string()),
        },
        ConsumedInput {
            uri: "dagr-blob+local://c/sha256/bbb".to_string(),
            content_hash: None,
        },
        ConsumedInput {
            uri: "dagr-blob+local://c/sha256/ccc".to_string(),
            content_hash: Some("sha256:ccc".to_string()),
        },
    ]))
    .expect("the record is emitted");

    let recs = records(&sink.bytes());
    let inputs = recs[0]["inputs"].as_array().expect("inputs is an array");
    assert_eq!(inputs.len(), 3, "one entry per declared input");
    assert_eq!(inputs[0]["uri"], "dagr-blob+local://c/sha256/aaa");
    assert_eq!(inputs[0]["content_hash"], "sha256:aaa");
    assert_eq!(
        inputs[1].get("content_hash"),
        None,
        "a producer that supplied no hash records none, rather than a null"
    );
    assert_eq!(inputs[2]["uri"], "dagr-blob+local://c/sha256/ccc");
}

#[test]
fn a_consume_nothing_source_records_an_empty_inputs_array_not_a_null_and_not_absent() {
    let sink = CaptureSink::default();
    let mut w = writer(sink.clone());
    w.attempt_submitted(submitted("source", 1))
        .expect("the record is emitted");

    let recs = records(&sink.bytes());
    let inputs = recs[0]
        .get("inputs")
        .expect("`inputs` is present even for a source");
    assert!(!inputs.is_null(), "`inputs` is never null");
    assert_eq!(
        inputs.as_array().map(Vec::len),
        Some(0),
        "a consume-nothing source encodes as an EMPTY array — the encoding that \
         makes an arity mismatch detectable"
    );
}

// ---------------------------------------------------------------------------
// Additivity: the fold is unperturbed, and a local run emits none of these
// ---------------------------------------------------------------------------

/// Write one complete two-node run, optionally interleaving submission records.
fn a_complete_run(with_submissions: bool) -> Vec<u8> {
    let sink = CaptureSink::default();
    let mut w = writer(sink.clone());
    w.run_started(header()).expect("run-started");
    for node in ["extract", "load"] {
        w.node_ready(node).expect("node-ready");
        w.node_admitted(node).expect("node-admitted");
        if with_submissions {
            w.attempt_submitted(
                submitted(node, 1)
                    .executor("k8s")
                    .target_name(format!("dagr-{node}-1")),
            )
            .expect("attempt-submitted");
        }
        w.attempt_started(node, 1).expect("attempt-started");
        w.attempt_succeeded(node, 1).expect("attempt-succeeded");
        w.attempt_outcome(AttemptOutcomeRecord::new(node, 1, "succeeded"))
            .expect("attempt-outcome");
        w.node_terminal(node, TerminalState::Succeeded)
            .expect("node-terminal");
    }
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    sink.bytes()
}

#[test]
fn a_stream_carrying_submission_records_folds_to_the_same_artifact_as_one_without() {
    let nodes = ["extract".to_string(), "load".to_string()];
    let plain = fold_stream(&a_complete_run(false), &nodes).expect("the plain stream folds");
    let with = fold_stream(&a_complete_run(true), &nodes).expect("the @1.3 stream folds");

    assert_eq!(
        with.to_value(),
        plain.to_value(),
        "an unknown-to-the-fold kind perturbs the folded artifact not at all"
    );
}

#[test]
fn a_stream_carrying_submission_records_still_has_gapless_strictly_increasing_seq() {
    let stream = a_complete_run(true);
    let recs = records(&stream);
    assert!(
        recs.iter().any(|r| r["kind"] == "attempt-submitted"),
        "the fixture really does carry the new kind"
    );
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(
            r["seq"].as_u64(),
            Some(u64::try_from(i).expect("a record index fits a u64")),
            "seq is gapless and strictly increasing across the mixed stream"
        );
    }
}

#[test]
fn a_local_run_emits_no_submission_record_and_stays_byte_identical() {
    assert_eq!(
        a_complete_run(false),
        a_complete_run(false),
        "the fixture is deterministic"
    );
    let local = a_complete_run(false);
    assert!(
        !String::from_utf8_lossy(&local).contains("attempt-submitted"),
        "a run that never submits remotely emits none of the new kind — a local \
         stream is byte-identical to a pre-@1.3 run"
    );
}
