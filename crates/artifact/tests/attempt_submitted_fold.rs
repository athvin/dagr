//! **Folding the `attempt-submitted` write-ahead record onto the run artifact.**
//! Written first (TDD).
//!
//! Every other thing the fold surfaces derives from a *finished* attempt: the fold
//! reduces terminal outcomes. A submission record is the opposite — it exists
//! precisely so that an attempt with **no** outcome still leaves a trace. So the
//! fold has to surface submissions that never produced an attempt-outcome, and
//! "submitted, never completed" has to be a first-class fact rather than an
//! absence. That row is what a crashed orchestrator left behind, and it is the
//! whole point of the audit trail.
//!
//! Two invariants are asserted here alongside the new behaviour, because both
//! would regress silently:
//!
//! * The fold stays a **reader**: submissions are folded from the bytes it is
//!   handed and nothing else, deterministically.
//! * The published artifact JSON is **unchanged**. A stream carrying submission
//!   records still folds to exactly the `to_value()` of one without them — the
//!   additivity guarantee the submission record shipped under. Submissions are
//!   surfaced on the `RunArtifact` *type*, which is what the run-index projection
//!   reads; they are not a new key in the artifact document.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, AttemptSubmittedRecord, ConsumedInput, EventSink, EventStreamWriter,
    FINGERPRINT_ALGORITHM_VERSION, MonotonicClock, RunId, RunOutcome, RunStartedHeader,
    TerminalState,
};
use dagr_artifact::fold::fold_stream;

// ---------------------------------------------------------------------------
// Scaffolding: an in-memory sink and a settable monotonic clock.
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

/// A clock the test can advance, so submission offsets are exact.
#[derive(Clone, Default)]
struct SharedClock {
    n: Arc<std::sync::atomic::AtomicU64>,
}

impl SharedClock {
    fn set(&self, v: u64) {
        self.n.store(v, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MonotonicClock for SharedClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.load(std::sync::atomic::Ordering::SeqCst)
    }
}

const RUN_ID: &str = "018f4a1e-6c2a-7b3d-9e10-0123456789ab";

fn writer(sink: CaptureSink, clock: SharedClock) -> EventStreamWriter<CaptureSink, SharedClock> {
    EventStreamWriter::new(
        sink,
        clock,
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

fn input(uri: &str, hash: Option<&str>) -> ConsumedInput {
    ConsumedInput {
        uri: uri.to_string(),
        content_hash: hash.map(String::from),
    }
}

// ---------------------------------------------------------------------------
// The case the record exists for: submitted, never completed
// ---------------------------------------------------------------------------

/// A submission with **no** matching attempt-outcome is surfaced, is identifiable
/// as submitted-but-never-completed, and is not dressed up as a failure.
#[test]
fn a_submission_with_no_outcome_is_surfaced_as_submitted_but_never_completed() {
    let sink = CaptureSink::default();
    let clock = SharedClock::default();
    let mut w = writer(sink.clone(), clock.clone());
    w.run_started(header()).expect("run-started");
    clock.set(7_000);
    w.attempt_submitted(
        AttemptSubmittedRecord::new("extract", 1)
            .executor("k8s")
            .target_name("dagr-extract-1"),
    )
    .expect("attempt-submitted");
    // No attempt-outcome, no node-terminal, no run-finished: the orchestrator died
    // between recording its intent and anything else happening.
    drop(w);

    let artifact = fold_stream(&sink.bytes(), &[]).expect("the stream folds");
    let submissions = artifact.submissions();
    assert_eq!(submissions.len(), 1, "the submission is surfaced, not dropped");
    let s = &submissions[0];
    assert_eq!(s.node(), "extract");
    assert_eq!(s.attempt_number(), 1);
    assert!(
        !s.completed(),
        "an attempt with no outcome is not completed"
    );
    assert_eq!(
        s.outcome_state(),
        None,
        "there is no terminal state to claim — not `failed`, not anything"
    );
    assert_eq!(
        s.submitted_at_offset_ns(),
        7_000,
        "the write-ahead point is the submission's offset"
    );
    assert!(
        artifact.is_interrupted(),
        "the run itself is still interrupted, as it is today"
    );
}

/// A submission followed by a successful outcome is joinable to that attempt on
/// `(node, attempt)`, and carries the attempt's terminal state.
#[test]
fn a_submission_with_an_outcome_carries_that_outcome_and_joins_on_node_and_attempt() {
    let bytes = a_placed_run();
    let artifact = fold_stream(&bytes, &[]).expect("the stream folds");

    let s = artifact
        .submissions()
        .iter()
        .find(|s| s.node() == "extract" && s.attempt_number() == 1)
        .expect("the extract submission is surfaced");
    assert!(s.completed(), "the attempt produced an outcome");
    assert_eq!(s.outcome_state(), Some("succeeded"));

    // The join key is exactly the attempt record's identity.
    let joined = artifact
        .attempts()
        .iter()
        .find(|a| a.node() == s.node() && a.attempt_number() == s.attempt_number())
        .expect("the submission joins to its attempt record");
    assert_eq!(joined.status(), s.outcome_state().expect("an outcome"));
}

/// A stream truncated mid-record **after** a submission still yields the
/// submission, and the run is still marked `interrupted` as it is today.
#[test]
fn a_stream_truncated_after_a_submission_still_yields_the_submission() {
    let sink = CaptureSink::default();
    let clock = SharedClock::default();
    let mut w = writer(sink.clone(), clock.clone());
    w.run_started(header()).expect("run-started");
    clock.set(100);
    w.attempt_submitted(
        AttemptSubmittedRecord::new("extract", 1)
            .inputs(vec![input("blob://in", Some("sha256:aaaa"))])
            .target_name("dagr-extract-1"),
    )
    .expect("attempt-submitted");
    drop(w);

    // Append a half-written next record (the crash case: the last block was cut).
    let mut bytes = sink.bytes();
    bytes.extend_from_slice(br#"{"kind":"attempt-star"#);

    let artifact = fold_stream(&bytes, &[]).expect("the truncated stream folds");
    assert!(
        artifact.trailing_partial_discarded(),
        "the one tolerated trailing partial was discarded"
    );
    assert!(artifact.is_interrupted(), "an interrupted run, as today");
    assert_eq!(
        artifact.submissions().len(),
        1,
        "the submission survives the truncation — that is what it is for"
    );
    assert!(!artifact.submissions()[0].completed());
}

// ---------------------------------------------------------------------------
// Ordering and shape
// ---------------------------------------------------------------------------

/// A node with N inputs projects them in **declared positional order**, and the
/// reference at position *k* is recoverable.
#[test]
fn positional_input_order_is_preserved_and_position_k_is_recoverable() {
    let sink = CaptureSink::default();
    let clock = SharedClock::default();
    let mut w = writer(sink.clone(), clock.clone());
    w.run_started(header()).expect("run-started");
    w.attempt_submitted(AttemptSubmittedRecord::new("join", 1).inputs(vec![
        input("blob://zeta", Some("sha256:0")),
        input("blob://alpha", Some("sha256:1")),
        input("blob://mid", None),
    ]))
    .expect("attempt-submitted");
    drop(w);

    let artifact = fold_stream(&sink.bytes(), &[]).expect("the stream folds");
    let s = &artifact.submissions()[0];
    assert_eq!(s.input_count(), Some(3));
    let uris: Vec<String> = s.inputs().iter().map(|i| i.uri().to_string()).collect();
    assert_eq!(
        uris,
        vec!["blob://zeta", "blob://alpha", "blob://mid"],
        "positional order is preserved verbatim — NOT sorted"
    );
    assert_eq!(
        s.input_at(1).expect("position 1 exists").uri(),
        "blob://alpha",
        "the reference at position k is recoverable"
    );
    assert_eq!(
        s.input_at(2).expect("position 2 exists").content_hash(),
        None,
        "a reference whose producer supplied no hash keeps a null hash"
    );
    assert!(s.input_at(3).is_none(), "there is no position 3");
}

/// A consume-nothing source records **zero** inputs, and that is distinguishable
/// from a record that never stated its inputs at all.
#[test]
fn zero_inputs_is_distinguishable_from_unknown_inputs() {
    let sink = CaptureSink::default();
    let clock = SharedClock::default();
    let mut w = writer(sink.clone(), clock.clone());
    w.run_started(header()).expect("run-started");
    // A real consume-nothing source: the writer always emits `inputs`, as an
    // empty array.
    w.attempt_submitted(AttemptSubmittedRecord::new("source", 1))
        .expect("attempt-submitted");
    drop(w);

    let mut bytes = sink.bytes();
    // A hand-built record that omits `inputs` entirely — the "unknown" shape a
    // minimal or older producer could leave behind. The fold must not read it as
    // "zero".
    bytes.extend_from_slice(
        format!(
            r#"{{"schema_version":"dagr.event-stream@1","run_id":"{RUN_ID}","seq":99,"wall":"2026-07-23T00:00:00.000Z","offset_ns":0,"kind":"attempt-submitted","node":"mystery","attempt":1}}"#
        )
        .as_bytes(),
    );
    bytes.push(b'\n');

    let artifact = fold_stream(&bytes, &[]).expect("the stream folds");
    let source = artifact
        .submissions()
        .iter()
        .find(|s| s.node() == "source")
        .expect("the source submission");
    assert_eq!(
        source.input_count(),
        Some(0),
        "a consume-nothing source is KNOWN to have zero inputs"
    );

    let mystery = artifact
        .submissions()
        .iter()
        .find(|s| s.node() == "mystery")
        .expect("the input-less record is still surfaced");
    assert_eq!(
        mystery.input_count(),
        None,
        "a record that never stated its inputs is UNKNOWN, not zero"
    );
    assert!(mystery.inputs().is_empty(), "and it iterates as empty");
}

/// Intent and reality are separate facts, and both survive: the observed identity
/// arrives as a second, additive record and merges onto the same submission
/// without displacing the intended name.
#[test]
fn intended_and_observed_target_identity_are_both_kept_and_can_differ() {
    let bytes = a_placed_run();
    let artifact = fold_stream(&bytes, &[]).expect("the stream folds");
    let s = artifact
        .submissions()
        .iter()
        .find(|s| s.node() == "extract")
        .expect("the extract submission");

    assert_eq!(
        s.target_name(),
        Some("dagr-extract-1"),
        "the INTENDED name, recorded before creation"
    );
    assert_eq!(
        s.observed_name(),
        Some("dagr-extract-1-adopted"),
        "the name the platform actually created — different, and not lost"
    );
    assert_eq!(s.observed_uid(), Some("uid-9f"));
    assert_eq!(s.observed_host(), Some("node-7"));
    assert_ne!(
        s.target_name(),
        s.observed_name(),
        "the two diverge, which is exactly why both are kept"
    );

    // The two records merge into ONE submission, not two.
    assert_eq!(
        artifact
            .submissions()
            .iter()
            .filter(|s| s.node() == "extract" && s.attempt_number() == 1)
            .count(),
        1,
        "the additive observed record merges onto the write-ahead one"
    );
    // And the write-ahead offset is the FIRST record's, not the second's.
    assert_eq!(
        s.submitted_at_offset_ns(),
        60,
        "the submission is stamped at the write-ahead point"
    );

    // The launch provenance is all there.
    assert_eq!(s.executor(), Some("k8s"));
    assert_eq!(s.structural_fingerprint(), Some("blake3:1111"));
    assert_eq!(s.policy_hash(), Some("blake3:2222"));
    assert_eq!(s.tool_version(), Some("dagr 0.0.0"));
    assert_eq!(s.image_digest(), Some("sha256:image"));
}

// ---------------------------------------------------------------------------
// The fold stays a deterministic reader, and the artifact document is unchanged
// ---------------------------------------------------------------------------

/// Folding the same bytes twice yields the same submissions, in the same order —
/// the fold reads the bytes it is given and nothing else.
#[test]
fn the_submission_fold_is_deterministic_and_in_stream_order() {
    let bytes = a_placed_run();
    let first = fold_stream(&bytes, &[]).expect("fold once");
    let second = fold_stream(&bytes, &[]).expect("fold twice");
    assert_eq!(
        first.submissions(),
        second.submissions(),
        "the same bytes fold to the same submissions"
    );
    let order: Vec<&str> = first.submissions().iter().map(|s| s.node()).collect();
    assert_eq!(
        order,
        vec!["extract", "load"],
        "submissions are surfaced in stream (seq) order"
    );
    assert_eq!(
        first.to_canonical_json(),
        second.to_canonical_json(),
        "the artifact stays byte-identical across folds"
    );
}

/// The published artifact **document** is unchanged: a stream carrying submission
/// records still folds to exactly the `to_value()` of one without them. The
/// submissions are surfaced on the type, which is what the projection reads.
#[test]
fn the_artifact_document_is_unchanged_by_submission_records() {
    let nodes = ["extract".to_string(), "load".to_string()];
    let plain = fold_stream(&a_complete_run(false), &nodes).expect("the plain stream folds");
    let with = fold_stream(&a_complete_run(true), &nodes).expect("the @1.3 stream folds");

    assert_eq!(
        with.to_value(),
        plain.to_value(),
        "the artifact document gains no key from the submission records"
    );
    assert_eq!(
        with.submissions().len(),
        2,
        "but the submissions ARE surfaced on the artifact type"
    );
    assert!(
        plain.submissions().is_empty(),
        "a stream with no submissions surfaces none"
    );
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A run in which `extract` is placed (submitted, observed, and completed) and
/// `load` is submitted and never completes.
fn a_placed_run() -> Vec<u8> {
    let sink = CaptureSink::default();
    let clock = SharedClock::default();
    let mut w = writer(sink.clone(), clock.clone());
    clock.set(0);
    w.run_started(header()).expect("run-started");

    clock.set(50);
    w.node_ready("extract").expect("node-ready");
    w.node_admitted("extract").expect("node-admitted");
    clock.set(60);
    let base = AttemptSubmittedRecord::new("extract", 1)
        .inputs(vec![
            input("blob://in-0", Some("sha256:0000")),
            input("blob://in-1", Some("sha256:1111")),
        ])
        .executor("k8s")
        .target_name("dagr-extract-1")
        .structural_fingerprint("blake3:1111")
        .policy_hash("blake3:2222")
        .tool_version("dagr 0.0.0")
        .image_digest("sha256:image");
    w.attempt_submitted(base.clone()).expect("write-ahead");
    clock.set(65);
    w.attempt_submitted(
        base.observed_name("dagr-extract-1-adopted")
            .observed_uid("uid-9f")
            .observed_host("node-7"),
    )
    .expect("observed, additively");
    clock.set(70);
    w.attempt_started("extract", 1).expect("attempt-started");
    clock.set(100);
    w.attempt_succeeded("extract", 1).expect("attempt-succeeded");
    w.attempt_outcome(AttemptOutcomeRecord::new(
        "extract",
        1,
        TerminalState::Succeeded.as_str(),
    ))
    .expect("attempt-outcome");
    w.node_terminal("extract", TerminalState::Succeeded)
        .expect("node-terminal");

    // `load` is submitted and never reports — the crashed-orchestrator shape.
    clock.set(150);
    w.node_ready("load").expect("node-ready");
    w.node_admitted("load").expect("node-admitted");
    clock.set(160);
    w.attempt_submitted(
        AttemptSubmittedRecord::new("load", 1)
            .inputs(vec![input("blob://mid", Some("sha256:2222"))])
            .executor("k8s")
            .target_name("dagr-load-1"),
    )
    .expect("write-ahead");
    drop(w);
    sink.bytes()
}

/// One complete two-node run, optionally interleaving submission records — the
/// additivity comparison's pair of streams.
fn a_complete_run(with_submissions: bool) -> Vec<u8> {
    let sink = CaptureSink::default();
    let clock = SharedClock::default();
    let mut w = writer(sink.clone(), clock.clone());
    w.run_started(header()).expect("run-started");
    for node in ["extract", "load"] {
        w.node_ready(node).expect("node-ready");
        w.node_admitted(node).expect("node-admitted");
        if with_submissions {
            w.attempt_submitted(
                AttemptSubmittedRecord::new(node, 1)
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
