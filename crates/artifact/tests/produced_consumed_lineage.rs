//! T90 · produced/consumed **lineage** events + fold. Written first (TDD).
//!
//! T89 landed the optional `durable_reference_meta` (content hash / size / scheme)
//! that rides alongside a durable node's `durable_reference`. T90 promotes "a
//! durable output was produced here" from implicit-in-an-attempt-row to an
//! explicit, append-only lineage record, and adds the consumed side. It is
//! event-stream-first and additive end to end (the metastore projection is T91):
//!
//!   - a durable node's succeeded attempt emits an `output-produced` event
//!     `{ node, attempt, uri, content_hash, size_bytes, kind, produced_at_offset_ns,
//!     originating_run }` (uri/hash/size REUSED from the T89 reference + metadata),
//!     and `fold_stream` folds those into an APPEND-ONLY `outputs[]` on the run
//!     artifact — immutable, with NO foreign key to any asset row;
//!   - on resume, a carried-forward prior durable output appears as an
//!     `output-produced` entry attributed to its `originating_run`
//!     (satisfied-from-prior), NOT re-produced;
//!   - a consuming node's `AttemptRecord` carries `inputs[] { uri, content_hash }`
//!     for the durable references it actually read;
//!   - an OLD stream with no `output-produced` events still folds + the fold
//!     tolerates their absence (`outputs[]` empty), open-world and additive.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dagr_artifact::event_stream::{
    record_consumed_inputs, record_durable_reference, record_durable_reference_meta,
    AttemptOutcomeRecord, ConsumedInput, DurableReferenceMeta, Event, EventSink, EventStreamWriter,
    MonotonicClock, OutputProducedRecord, RunId, RunOutcome, RunStartedHeader, TerminalState,
    FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::{fold_stream, RunArtifact};

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

fn writer(sink: &CaptureSink, clock: &ManualClock) -> EventStreamWriter<CaptureSink, ManualClock> {
    EventStreamWriter::new(
        sink.clone(),
        clock.clone(),
        RunId::from_operator(RUN_ID),
        "example-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string())
}

/// Read every raw wire record of a `kind` from the captured stream bytes.
fn records_of_kind(bytes: &[u8], kind: &str) -> Vec<Value> {
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .filter(|v| v.get("kind").and_then(Value::as_str) == Some(kind))
        .collect()
}

// ---------------------------------------------------------------------------
// PRODUCED: two durable-output nodes ⇒ two `output-produced` events, folded into
// an append-only `outputs[]` (no FK to any asset row).
// ---------------------------------------------------------------------------

#[test]
fn two_durable_nodes_emit_two_output_produced_events_folded_into_outputs() {
    let sink = CaptureSink::default();
    let clock = ManualClock::new();
    let mut w = writer(&sink, &clock);

    clock.set(0);
    w.run_started(header()).expect("run-started");

    // Node `a`: durable output at offset 100.
    clock.set(50);
    w.attempt_started("a", 1).expect("started a");
    clock.set(100);
    w.attempt_succeeded("a", 1).expect("succeeded a");
    let mut a = AttemptOutcomeRecord::new("a", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut a, Some("s3://bucket/a".to_string()));
    record_durable_reference_meta(
        &mut a,
        Some(
            DurableReferenceMeta::new()
                .content_hash("sha256:aaaa")
                .size_bytes(11)
                .scheme("s3")
                .produced_at_offset_ns(100),
        ),
    );
    w.attempt_outcome(a).expect("outcome a");
    // The output-produced event carries the SAME uri/hash/size (reused from T89).
    w.output_produced(OutputProducedRecord {
        node: "a".to_string(),
        attempt: 1,
        uri: "s3://bucket/a".to_string(),
        content_hash: Some("sha256:aaaa".to_string()),
        size_bytes: Some(11),
        kind: Some("s3".to_string()),
        produced_at_offset_ns: 100,
        originating_run: RUN_ID.to_string(),
    })
    .expect("output-produced a");

    // Node `b`: durable output at offset 200.
    clock.set(150);
    w.attempt_started("b", 1).expect("started b");
    clock.set(200);
    w.attempt_succeeded("b", 1).expect("succeeded b");
    let mut b = AttemptOutcomeRecord::new("b", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut b, Some("s3://bucket/b".to_string()));
    record_durable_reference_meta(
        &mut b,
        Some(
            DurableReferenceMeta::new()
                .content_hash("sha256:bbbb")
                .size_bytes(22)
                .scheme("s3")
                .produced_at_offset_ns(200),
        ),
    );
    w.attempt_outcome(b).expect("outcome b");
    w.output_produced(OutputProducedRecord {
        node: "b".to_string(),
        attempt: 1,
        uri: "s3://bucket/b".to_string(),
        content_hash: Some("sha256:bbbb".to_string()),
        size_bytes: Some(22),
        kind: Some("s3".to_string()),
        produced_at_offset_ns: 200,
        originating_run: RUN_ID.to_string(),
    })
    .expect("output-produced b");

    clock.set(210);
    w.node_terminal("a", TerminalState::Succeeded).expect("t a");
    w.node_terminal("b", TerminalState::Succeeded).expect("t b");
    clock.set(220);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    // Exactly two output-produced events on the wire, with the correct fields.
    let produced = records_of_kind(&sink.bytes(), "output-produced");
    assert_eq!(produced.len(), 2, "two durable nodes ⇒ two produced events");
    assert_eq!(produced[0]["node"], json!("a"));
    assert_eq!(produced[0]["uri"], json!("s3://bucket/a"));
    assert_eq!(produced[0]["content_hash"], json!("sha256:aaaa"));
    assert_eq!(produced[0]["size_bytes"], json!(11));
    assert_eq!(produced[0]["produced_at_offset_ns"], json!(100));
    assert_eq!(produced[0]["originating_run"], json!(RUN_ID));

    // The fold yields a matching, append-only outputs[] (stream order).
    let art = fold_stream(&sink.bytes(), &["a".to_string(), "b".to_string()]).expect("fold");
    let outputs = art.outputs();
    assert_eq!(
        outputs.len(),
        2,
        "outputs[] has one entry per produced event"
    );
    assert_eq!(outputs[0].node(), "a");
    assert_eq!(outputs[0].uri(), "s3://bucket/a");
    assert_eq!(outputs[0].content_hash(), Some("sha256:aaaa"));
    assert_eq!(outputs[0].size_bytes(), Some(11));
    assert_eq!(outputs[0].originating_run(), RUN_ID);
    assert_eq!(outputs[1].node(), "b");
    assert_eq!(outputs[1].uri(), "s3://bucket/b");

    // No foreign key to any asset row: the folded output carries a uri BY VALUE
    // and no asset-identity id field.
    let value = art.to_value();
    let arr = value["outputs"].as_array().expect("outputs array");
    assert_eq!(arr.len(), 2);
    for o in arr {
        assert!(
            o.get("asset_id").is_none() && o.get("asset").is_none(),
            "a produced output has NO foreign key to any asset-identity row (uri by value only)"
        );
        assert!(o.get("uri").is_some(), "the uri is carried by value");
    }
}

// ---------------------------------------------------------------------------
// RESUME: a carried-forward prior durable output appears as an output-produced
// entry attributed to its originating_run (satisfied-from-prior), NOT re-produced.
// ---------------------------------------------------------------------------

#[test]
fn resume_copies_a_prior_output_forward_attributed_to_its_originating_run() {
    let sink = CaptureSink::default();
    let clock = ManualClock::new();
    let mut w = writer(&sink, &clock);

    let prior_run = "01111111-1111-7111-8111-111111111111";

    clock.set(0);
    // A resumed run (resumed_from carries the prior run identity).
    let mut h = header();
    h.resumed_from = Some(prior_run.to_string());
    w.run_started(h).expect("run-started");

    // `snap` was satisfied-from-prior: NOT re-executed; its prior durable output is
    // copied forward, attributed to the PRIOR run, at the offset it is copied.
    clock.set(10);
    w.node_ready("snap").expect("ready snap");
    let mut satisfied =
        AttemptOutcomeRecord::new("snap", 1, TerminalState::SatisfiedFromPrior.as_str());
    satisfied.satisfied_from_run = Some(prior_run.to_string());
    record_durable_reference(&mut satisfied, Some("s3://bucket/snap".to_string()));
    w.attempt_outcome(satisfied).expect("outcome snap");
    // The copy-forward produced event: marked satisfied-from-prior via its
    // originating_run = the PRIOR run (not this resumed run).
    w.output_produced(OutputProducedRecord {
        node: "snap".to_string(),
        attempt: 1,
        uri: "s3://bucket/snap".to_string(),
        content_hash: Some("sha256:snap".to_string()),
        size_bytes: Some(7),
        kind: Some("s3".to_string()),
        produced_at_offset_ns: 10,
        originating_run: prior_run.to_string(),
    })
    .expect("output-produced snap");
    w.node_terminal("snap", TerminalState::SatisfiedFromPrior)
        .expect("t snap");

    clock.set(20);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    // Exactly ONE produced event; the durable output is not re-produced.
    let produced = records_of_kind(&sink.bytes(), "output-produced");
    assert_eq!(produced.len(), 1, "the carried-forward output appears once");

    let art = fold_stream(&sink.bytes(), &["snap".to_string()]).expect("fold");
    let outputs = art.outputs();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].node(), "snap");
    assert_eq!(
        outputs[0].originating_run(),
        prior_run,
        "the carried-forward output is attributed to its ORIGINATING (prior) run, \
         not the resumed run"
    );
    assert_ne!(
        outputs[0].originating_run(),
        RUN_ID,
        "it is satisfied-from-prior, not re-produced by this run"
    );
}

// ---------------------------------------------------------------------------
// CONSUMED: a consuming node's AttemptRecord carries inputs[] { uri, content_hash }
// for the durable references it read; the identity matches the producing output.
// ---------------------------------------------------------------------------

#[test]
fn a_consuming_node_records_the_inputs_it_read_matching_the_producing_output() {
    let sink = CaptureSink::default();
    let clock = ManualClock::new();
    let mut w = writer(&sink, &clock);

    clock.set(0);
    w.run_started(header()).expect("run-started");

    // Producer `p` — a durable output.
    clock.set(50);
    w.attempt_started("p", 1).expect("started p");
    clock.set(100);
    w.attempt_succeeded("p", 1).expect("succeeded p");
    let mut p = AttemptOutcomeRecord::new("p", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut p, Some("s3://bucket/p".to_string()));
    record_durable_reference_meta(
        &mut p,
        Some(
            DurableReferenceMeta::new()
                .content_hash("sha256:pppp")
                .size_bytes(9),
        ),
    );
    w.attempt_outcome(p).expect("outcome p");
    w.output_produced(OutputProducedRecord {
        node: "p".to_string(),
        attempt: 1,
        uri: "s3://bucket/p".to_string(),
        content_hash: Some("sha256:pppp".to_string()),
        size_bytes: Some(9),
        kind: None,
        produced_at_offset_ns: 100,
        originating_run: RUN_ID.to_string(),
    })
    .expect("output-produced p");
    w.node_terminal("p", TerminalState::Succeeded).expect("t p");

    // Consumer `c` — reads p's durable reference. Its attempt records the inputs
    // it actually read, matching the producing output's identity.
    clock.set(150);
    w.attempt_started("c", 1).expect("started c");
    clock.set(200);
    w.attempt_succeeded("c", 1).expect("succeeded c");
    let mut c = AttemptOutcomeRecord::new("c", 1, TerminalState::Succeeded.as_str());
    record_consumed_inputs(
        &mut c,
        vec![ConsumedInput {
            uri: "s3://bucket/p".to_string(),
            content_hash: Some("sha256:pppp".to_string()),
        }],
    );
    w.attempt_outcome(c).expect("outcome c");
    w.node_terminal("c", TerminalState::Succeeded).expect("t c");

    clock.set(210);
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    let art = fold_stream(&sink.bytes(), &["p".to_string(), "c".to_string()]).expect("fold");
    let consumer = art
        .attempts()
        .iter()
        .find(|a| a.node() == "c")
        .expect("c present");
    let inputs = consumer.inputs();
    assert_eq!(inputs.len(), 1, "c read exactly one durable input");
    assert_eq!(inputs[0].uri(), "s3://bucket/p");
    assert_eq!(inputs[0].content_hash(), Some("sha256:pppp"));

    // The consumed input's identity matches the producing output's identity.
    let produced = art
        .outputs()
        .iter()
        .find(|o| o.node() == "p")
        .expect("p output");
    assert_eq!(
        inputs[0].uri(),
        produced.uri(),
        "the consumed input's uri matches the producing output"
    );
    assert_eq!(
        inputs[0].content_hash(),
        produced.content_hash(),
        "the consumed input's content hash matches the producing output"
    );

    // A node that read no durable input carries no inputs (absent, not empty-null).
    let source_attempt = art.attempts().iter().find(|a| a.node() == "p").unwrap();
    assert!(
        source_attempt.inputs().is_empty(),
        "a node that consumed no durable reference records no inputs"
    );
}

// ---------------------------------------------------------------------------
// ADDITIVITY: an OLD fixture stream with no `output-produced` events still folds;
// outputs[] is empty; a run with no lineage is byte-identical.
// ---------------------------------------------------------------------------

#[test]
fn an_old_stream_without_output_produced_events_still_folds_with_empty_outputs() {
    // Hand-authored pre-T90 wire records: run-started, one attempt-outcome (no
    // output-produced), node-terminal, run-finished. The fold must tolerate the
    // absence and yield an empty outputs[].
    let sv = "dagr.event-stream@1";
    let mut bytes = String::new();
    let started = json!({
        "schema_version": sv, "run_id": RUN_ID, "seq": 0, "wall": "2026-07-23T00:00:00.000Z",
        "offset_ns": 0, "kind": "run-started",
        "header": {
            "run_id": RUN_ID, "pipeline": "old", "fingerprint_structural": "blake3:aa",
            "fingerprint_policy": "blake3:bb", "fingerprint_algorithm_version": 1,
            "parameters": {}, "data_interval": Value::Null, "captured_environment": {},
            "resume_lineage": Value::Null
        }
    });
    bytes.push_str(&started.to_string());
    bytes.push('\n');
    let outcome = json!({
        "schema_version": sv, "run_id": RUN_ID, "seq": 1, "wall": "2026-07-23T00:00:00.010Z",
        "offset_ns": 100, "kind": "attempt-outcome",
        "node": "snap", "attempt": 1, "status": "succeeded",
        "durable_reference": "snap/output"
    });
    bytes.push_str(&outcome.to_string());
    bytes.push('\n');
    let terminal = json!({
        "schema_version": sv, "run_id": RUN_ID, "seq": 2, "wall": "2026-07-23T00:00:00.020Z",
        "offset_ns": 200, "kind": "node-terminal", "node": "snap", "state": "succeeded"
    });
    bytes.push_str(&terminal.to_string());
    bytes.push('\n');
    let finished = json!({
        "schema_version": sv, "run_id": RUN_ID, "seq": 3, "wall": "2026-07-23T00:00:00.030Z",
        "offset_ns": 300, "kind": "run-finished", "outcome": "succeeded"
    });
    bytes.push_str(&finished.to_string());
    bytes.push('\n');

    let art = fold_stream(bytes.as_bytes(), &["snap".to_string()]).expect("old stream folds");
    assert!(
        art.outputs().is_empty(),
        "an old stream with no output-produced events folds with an empty outputs[]"
    );
    // outputs[] is serialized as an (empty) array — additive, always present.
    assert_eq!(
        art.to_value()["outputs"],
        json!([]),
        "outputs[] serializes as an empty array when the stream carried none"
    );
    // The consumer inputs default absent too.
    let snap = art.attempts().iter().find(|a| a.node() == "snap").unwrap();
    assert!(
        snap.inputs().is_empty(),
        "no consumed inputs on the old stream"
    );
}

#[test]
fn a_run_without_any_lineage_is_byte_identical_via_the_bridges() {
    // A run whose records pass through the consumed-inputs bridge with an empty
    // vec, and emit no output-produced events, is byte-identical to one that never
    // touched the lineage surface — additive, no field.
    fn drive(with_bridge: bool) -> RunArtifact {
        let sink = CaptureSink::default();
        let clock = ManualClock::new();
        let mut w = writer(&sink, &clock);
        clock.set(0);
        w.run_started(header()).expect("run-started");
        for node in ["a", "b"] {
            clock.set(50);
            w.attempt_started(node, 1).expect("started");
            clock.set(100);
            w.attempt_succeeded(node, 1).expect("succeeded");
            let mut rec = AttemptOutcomeRecord::new(node, 1, TerminalState::Succeeded.as_str());
            if with_bridge {
                record_consumed_inputs(&mut rec, Vec::new());
            }
            w.attempt_outcome(rec).expect("outcome");
            w.node_terminal(node, TerminalState::Succeeded).expect("t");
        }
        clock.set(110);
        w.run_finished(RunOutcome::Succeeded).expect("run-finished");
        w.finish().expect("flush");
        fold_stream(&sink.bytes(), &["a".to_string(), "b".to_string()]).expect("fold")
    }
    let baseline = drive(false);
    let via_bridge = drive(true);
    assert_eq!(
        serde_json::to_string(&baseline.to_value()).unwrap(),
        serde_json::to_string(&via_bridge.to_value()).unwrap(),
        "an empty-inputs run is byte-identical; the inputs slot is absent throughout"
    );
    assert!(baseline.outputs().is_empty());
}

// ---------------------------------------------------------------------------
// The event maps to its wire form directly (the Event enum surface).
// ---------------------------------------------------------------------------

#[test]
fn output_produced_event_carries_its_wire_kind_and_fields() {
    let sink = CaptureSink::default();
    let clock = ManualClock::new();
    let mut w = writer(&sink, &clock);
    clock.set(0);
    w.run_started(header()).expect("run-started");
    clock.set(100);
    w.emit_event(&Event::OutputProduced(OutputProducedRecord {
        node: "n".to_string(),
        attempt: 2,
        uri: "file:///out".to_string(),
        content_hash: None,
        size_bytes: None,
        kind: None,
        produced_at_offset_ns: 100,
        originating_run: RUN_ID.to_string(),
    }))
    .expect("emit output-produced");
    w.finish().expect("flush");

    let produced = records_of_kind(&sink.bytes(), "output-produced");
    assert_eq!(produced.len(), 1);
    assert_eq!(produced[0]["node"], json!("n"));
    assert_eq!(produced[0]["attempt"], json!(2));
    assert_eq!(produced[0]["uri"], json!("file:///out"));
    assert_eq!(produced[0]["produced_at_offset_ns"], json!(100));
    assert_eq!(produced[0]["originating_run"], json!(RUN_ID));
    // Absent optionals are omitted (open-world; the fold defaults them). The
    // output kind/scheme travels under `output_kind` (not `kind`, the discriminator).
    assert_eq!(produced[0]["kind"], json!("output-produced"));
    assert!(produced[0].get("content_hash").is_none());
    assert!(produced[0].get("size_bytes").is_none());
    assert!(produced[0].get("output_kind").is_none());
}
