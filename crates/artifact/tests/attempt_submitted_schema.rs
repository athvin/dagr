//! T108 · the `attempt-submitted` **schema round-trip** and its fixture-corpus
//! member, written first (TDD). Gated behind `schema-validation` (default OFF),
//! like the other schema suites.
//!
//! The commitment this exists to hold is `arch.md`'s: *"a checked-in fixture corpus
//! with one artifact per released schema version is parsed in CI forever after."*
//! `@1.3` is a released minor of `dagr.event-stream@1`, so the corpus gains a
//! member for its new kind — and, per the lesson recorded in `DEVIATIONS.md` for
//! the T19/T39 divergence, the member is checked against the **real producer's**
//! bytes rather than hand-authored, so a writer that quietly drifts from the
//! published schema is caught rather than flattered.

#![cfg(feature = "schema-validation")]

use std::io;
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{
    AttemptSubmittedRecord, ConsumedInput, EventSink, EventStreamWriter, MonotonicClock, RunId,
};
use dagr_artifact::schema::{ArtifactKind, validate_bytes};
use serde_json::Value;

#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CaptureSink {
    fn lines(&self) -> Vec<Vec<u8>> {
        self.lines.lock().expect("sink mutex").clone()
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
        200
    }
}

const RUN_ID: &str = "018f4a1e-6c2a-7b3d-9e10-0123456789ab";

/// The canonical corpus record, produced by the **real writer**. The single source
/// of truth for both the validation test and the checked-in fixture.
fn corpus_record_line() -> Vec<u8> {
    let sink = CaptureSink::default();
    let mut w = EventStreamWriter::new(
        sink.clone(),
        FrozenClock,
        RunId::from_operator(RUN_ID),
        "example-pipeline".to_string(),
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    w.attempt_submitted(
        AttemptSubmittedRecord::new("extract", 1)
            .executor("k8s")
            .target_name("dagr-extract-1")
            .observed_name("dagr-extract-1")
            .observed_uid("6f0f1b2c-3d4e-5a6b-7c8d-9e0f1a2b3c4d")
            .observed_host("kind-worker2")
            .structural_fingerprint(
                "blake3:1111111111111111111111111111111111111111111111111111111111111111",
            )
            .policy_hash("blake3:2222222222222222222222222222222222222222222222222222222222222222")
            .tool_version("dagr@1")
            .image_digest("sha256:cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe")
            .inputs(vec![ConsumedInput {
                uri: "dagr-blob+local://blobs/sha256/2c26b46b68ffc68ff99b453c1d304134\
                      13422d706483bfa0f98a5e886266e7ae"
                    .to_string(),
                content_hash: "sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e88\
                      6266e7ae"
                    .to_string()
                    .into(),
            }]),
    )
    .expect("the record is emitted");

    let lines = sink.lines();
    assert_eq!(lines.len(), 1, "exactly one record");
    lines.into_iter().next().expect("one line")
}

/// Pretty-print one canonical line, the corpus's stored form.
fn pretty(line: &[u8]) -> Vec<u8> {
    let v: Value = serde_json::from_slice(line).expect("record parses");
    let mut s = serde_json::to_vec_pretty(&v).expect("pretty prints");
    s.push(b'\n');
    s
}

fn fixture_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/corpus/event-stream/v1/attempt-submitted.json")
}

#[test]
fn the_real_writers_attempt_submitted_output_validates_against_the_published_schema() {
    let line = corpus_record_line();
    validate_bytes(ArtifactKind::EventStream, 1, &line)
        .expect("the producer's own bytes validate against the published @1 schema");
}

#[test]
fn a_consume_nothing_submission_validates_with_an_empty_inputs_array() {
    let sink = CaptureSink::default();
    let mut w = EventStreamWriter::new(
        sink.clone(),
        FrozenClock,
        RunId::from_operator(RUN_ID),
        "example-pipeline".to_string(),
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());
    w.attempt_submitted(AttemptSubmittedRecord::new("source", 1))
        .expect("emitted");

    let line = sink.lines().into_iter().next().expect("one line");
    let value: Value = serde_json::from_slice(&line).expect("parses");
    assert_eq!(
        value["inputs"].as_array().map(Vec::len),
        Some(0),
        "the required `inputs` array is present and empty"
    );
    validate_bytes(ArtifactKind::EventStream, 1, &line)
        .expect("an empty inputs array satisfies the published schema");
}

#[test]
fn the_checked_in_corpus_fixture_matches_the_real_writers_output() {
    let path = fixture_path();
    let on_disk = std::fs::read(&path).unwrap_or_else(|err| {
        panic!(
            "the @1.3 corpus fixture must be checked in at {} ({err}) — regenerate \
             with `cargo test -p dagr-artifact --features schema-validation --test \
             attempt_submitted_schema regenerate_attempt_submitted_fixture -- --ignored`",
            path.display()
        )
    });
    assert_eq!(
        String::from_utf8_lossy(&on_disk),
        String::from_utf8_lossy(&pretty(&corpus_record_line())),
        "the corpus member is the real producer's bytes, not a hand-authored \
         approximation of them"
    );
    validate_bytes(ArtifactKind::EventStream, 1, &on_disk)
        .expect("the checked-in fixture still validates — the forever-after commitment");
}

/// Regenerate the checked-in `@1.3` corpus member from the real writer.
/// `cargo test -p dagr-artifact --features schema-validation --test
/// attempt_submitted_schema regenerate_attempt_submitted_fixture -- --ignored`
#[test]
#[ignore = "regenerates a checked-in fixture; run deliberately and review the diff"]
fn regenerate_attempt_submitted_fixture() {
    let path = fixture_path();
    std::fs::create_dir_all(path.parent().expect("a parent directory")).expect("corpus dir");
    std::fs::write(&path, pretty(&corpus_record_line())).expect("fixture written");
}
