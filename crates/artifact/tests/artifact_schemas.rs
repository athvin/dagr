//! T39 (ticket 050) — published-artifact-schema validation suite.
//!
//! These tests realize the ticket's Test plan against the three published,
//! versioned JSON Schema documents (arch.md C19 event stream, C20 graph
//! artifact, C22 run artifact) and the shared validation helper. They are the
//! covering suite for "the artifact validates against its published schema"
//! that T40/T42/T48 lean on. Every scenario drives a hand-authored fixture (or a
//! seeded corpus fixture) through [`dagr_artifact::schema`] and asserts the
//! published schema accepts the valid shapes and rejects the invalid ones with
//! an actionable, artifact-naming error.
//!
//! The validation helper depends on the `jsonschema` crate, which the T4 ADR
//! (017 §4) scopes to CI/tests only; this suite therefore lives behind the
//! `schema-validation` cargo feature and is run by CI with that feature on.
#![cfg(feature = "schema-validation")]

use serde_json::{json, Value};

use dagr_artifact::schema::{
    check_corpus, published_schema_versions, validate_value, ArtifactKind, SchemaValidationError,
    CORPUS_DIR, SCHEMA_DIR,
};

// === helpers ==============================================================

/// Validate a JSON value against the published schema for `kind`@`version`,
/// asserting acceptance. Panics with the actionable error on rejection.
#[track_caller]
fn assert_valid(kind: ArtifactKind, version: u32, instance: &Value) {
    if let Err(e) = validate_value(kind, version, instance) {
        panic!("expected {kind:?}@{version} fixture to VALIDATE, but it was rejected: {e}");
    }
}

/// Validate a JSON value, asserting rejection, and return the error so the
/// caller can assert on its (actionable) contents.
#[track_caller]
fn assert_invalid(kind: ArtifactKind, version: u32, instance: &Value) -> SchemaValidationError {
    match validate_value(kind, version, instance) {
        Ok(()) => panic!("expected {kind:?}@{version} fixture to be REJECTED, but it validated"),
        Err(e) => e,
    }
}

// === reusable minimal fixtures ============================================

/// A minimal run-artifact success header carrying every field known at start
/// plus the end-only overall outcome — the shape a run-started event copies
/// (minus outcome/summary).
fn success_header() -> Value {
    json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
        "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
        "fingerprint_algorithm_version": 1,
        "parameters": { "date": "2026-07-23" },
        "data_interval": { "start": "2026-07-23T00:00:00Z", "end": "2026-07-24T00:00:00Z" },
        "captured_environment": {},
        "resume_lineage": null,
        "overall_outcome": "succeeded"
    })
}

/// One attempt record for node `n` carrying every required field.
fn attempt_record(node: &str, attempt: u32, status: &str) -> Value {
    json!({
        "node": node,
        "attempt": attempt,
        "status": status,
        "phase_durations_ns": { "queued": 10, "running": 90 },
        "worker": "worker-0",
        "message": "ok",
        "error": null,
        "metrics": { "rows_read": 42 },
        "cost_declared": { "memory_bytes": 1024 },
        "cost_measured": { "memory_bytes": 900 }
    })
}

// === event stream (C19) ===================================================

/// A minimal event record carrying the T0.6/§C19 header every record shares.
fn event_header(kind: &str) -> Value {
    json!({
        "schema_version": "dagr.event-stream@1",
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "seq": 3,
        "wall": "2026-07-23T00:00:00.123Z",
        "offset_ns": 123_456_789,
        "kind": kind
    })
}

#[test]
fn event_record_of_every_kind_validates_and_carries_the_header() {
    // Every event kind (arch.md C19 + Vocabulary) validates and carries the
    // five shared header fields: run identity, schema version, sequence,
    // wall-clock stamp, monotonic offset.
    let simple_kinds = [
        "node-ready",
        "node-admitted",
        "attempt-started",
        "attempt-succeeded",
        "attempt-failed",
        "attempt-outcome",
        "node-terminal",
        "run-finished",
    ];
    for kind in simple_kinds {
        let mut rec = event_header(kind);
        rec.as_object_mut()
            .unwrap()
            .insert("node".into(), json!("n1"));
        assert_valid(ArtifactKind::EventStream, 1, &rec);
        let obj = rec.as_object().unwrap();
        for field in ["run_id", "schema_version", "seq", "wall", "offset_ns"] {
            assert!(obj.contains_key(field), "{kind} record missing {field}");
        }
    }

    // run-started carries the full start-header (checked in its own test).
    let mut run_started = event_header("run-started");
    run_started
        .as_object_mut()
        .unwrap()
        .insert("header".into(), start_header_from(&success_header()));
    assert_valid(ArtifactKind::EventStream, 1, &run_started);

    // zombie-at-exit references an attempt.
    let mut zombie = event_header("zombie-at-exit");
    let z = zombie.as_object_mut().unwrap();
    z.insert("node".into(), json!("n1"));
    z.insert("attempt".into(), json!(1));
    assert_valid(ArtifactKind::EventStream, 1, &zombie);
}

/// Strip the two end-only fields from a full run header to get the start-header
/// the run-started event carries.
fn start_header_from(full: &Value) -> Value {
    let mut h = full.clone();
    let o = h.as_object_mut().unwrap();
    o.remove("overall_outcome");
    o.remove("summary");
    h
}

#[test]
fn run_started_header_completeness() {
    // Accepted with every start-header field; the two end-only fields are
    // absent.
    let mut ev = event_header("run-started");
    ev.as_object_mut()
        .unwrap()
        .insert("header".into(), start_header_from(&success_header()));
    assert_valid(ArtifactKind::EventStream, 1, &ev);

    // Missing a required start-header field (pipeline identity) is rejected.
    let mut bad = ev.clone();
    bad["header"].as_object_mut().unwrap().remove("pipeline");
    assert_invalid(ArtifactKind::EventStream, 1, &bad);

    // Carrying an end-only field (overall_outcome) in the start header is
    // rejected: run-started predates the outcome.
    let mut leaks_end = ev.clone();
    leaks_end["header"]
        .as_object_mut()
        .unwrap()
        .insert("overall_outcome".into(), json!("succeeded"));
    assert_invalid(ArtifactKind::EventStream, 1, &leaks_end);
}

#[test]
fn sequence_and_offset_typing() {
    // Negative sequence -> rejected.
    let mut neg = event_header("node-ready");
    neg.as_object_mut()
        .unwrap()
        .insert("node".into(), json!("n1"));
    neg["seq"] = json!(-1);
    assert_invalid(ArtifactKind::EventStream, 1, &neg);

    // Non-integer sequence -> rejected.
    let mut frac = event_header("node-ready");
    frac.as_object_mut()
        .unwrap()
        .insert("node".into(), json!("n1"));
    frac["seq"] = json!(1.5);
    assert_invalid(ArtifactKind::EventStream, 1, &frac);

    // Absent monotonic offset -> rejected (offset is authoritative).
    let mut no_offset = event_header("node-ready");
    no_offset
        .as_object_mut()
        .unwrap()
        .insert("node".into(), json!("n1"));
    no_offset.as_object_mut().unwrap().remove("offset_ns");
    assert_invalid(ArtifactKind::EventStream, 1, &no_offset);
}

#[test]
fn zombie_at_exit_is_an_event_not_a_terminal_transition() {
    let mut zombie = event_header("zombie-at-exit");
    let z = zombie.as_object_mut().unwrap();
    z.insert("node".into(), json!("n1"));
    z.insert("attempt".into(), json!(2));
    assert_valid(ArtifactKind::EventStream, 1, &zombie);

    // It is an event record (has the event header + kind), confirmed by
    // validating against the EVENT schema; it is NOT a run-artifact attempt
    // record — feeding it as a node-terminal transition would require a
    // terminal status, which zombie-at-exit deliberately lacks.
    assert!(
        zombie.get("status").is_none(),
        "zombie-at-exit is not a terminal state"
    );
}

#[test]
fn event_stream_supports_concatenation_and_partition_by_run_identity() {
    // Two records from two different runs both validate; each carries its own
    // run_id, so a concatenated stream partitions safely by run identity.
    let mut a = event_header("node-ready");
    a.as_object_mut()
        .unwrap()
        .insert("node".into(), json!("n1"));
    let mut b = event_header("node-ready");
    let bo = b.as_object_mut().unwrap();
    bo.insert("node".into(), json!("n1"));
    bo.insert(
        "run_id".into(),
        json!("018f4a1e-6c2a-7b3d-9e10-ffffffffffff"),
    );
    assert_valid(ArtifactKind::EventStream, 1, &a);
    assert_valid(ArtifactKind::EventStream, 1, &b);
    assert_ne!(
        a["run_id"], b["run_id"],
        "records carry distinct run identities"
    );
}

// === graph artifact (C20) =================================================

fn graph_artifact() -> Value {
    json!({
        "header": {
            "schema_version": "dagr.graph@1",
            "tool_version": "0.0.0",
            "generated_at": "2026-07-23T00:00:00Z",
            "pipeline": "example-pipeline",
            "build_provenance": {
                "tool_version": "0.0.0",
                "git_commit": "abcdef0123456789abcdef0123456789abcdef01",
                "lockfile_hash": "blake3:3333333333333333333333333333333333333333333333333333333333333333"
            },
            "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
            "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
            "fingerprint_algorithm_version": 1
        },
        "nodes": [
            {
                "name": "load",
                "group": "ingest",
                "task_name": "LoadTask",
                "input_type_names": [],
                "output_type_name": "Rows",
                "execution_class": "compute",
                "policy": {
                    "retries": 0,
                    "timeout_ms": 60000,
                    "cost": { "memory_bytes": 1024 },
                    "execution_class": "compute",
                    "durable": false,
                    "trigger_rule": "all-succeeded"
                },
                "resources": { "memory_bytes": 1024 },
                "dependencies": []
            },
            {
                "name": "sink",
                "group": "ingest",
                "task_name": "SinkTask",
                "input_type_names": ["Rows"],
                "output_type_name": "Unit",
                "execution_class": "compute",
                "policy": {
                    "retries": 0,
                    "timeout_ms": 60000,
                    "cost": { "memory_bytes": 1024 },
                    "execution_class": "compute",
                    "durable": false,
                    "trigger_rule": "all-succeeded"
                },
                "resources": { "memory_bytes": 1024 },
                "dependencies": ["load"]
            }
        ],
        "edges": [
            { "from": "load", "to": "sink", "kind": "data", "type_name": "Rows" },
            { "from": "load", "to": "sink", "kind": "ordering" }
        ]
    })
}

#[test]
fn graph_artifact_validates_with_one_data_and_one_ordering_edge() {
    assert_valid(ArtifactKind::Graph, 1, &graph_artifact());

    // A data edge that omits its carried stable type name is rejected.
    let mut no_type = graph_artifact();
    no_type["edges"][0]
        .as_object_mut()
        .unwrap()
        .remove("type_name");
    assert_invalid(ArtifactKind::Graph, 1, &no_type);

    // A header that omits build provenance is rejected.
    let mut no_prov = graph_artifact();
    no_prov["header"]
        .as_object_mut()
        .unwrap()
        .remove("build_provenance");
    assert_invalid(ArtifactKind::Graph, 1, &no_prov);

    // A header that omits a fingerprint hash is rejected.
    let mut no_fp = graph_artifact();
    no_fp["header"]
        .as_object_mut()
        .unwrap()
        .remove("fingerprint_structural");
    assert_invalid(ArtifactKind::Graph, 1, &no_fp);
}

#[test]
fn stable_name_only_identity_type_name_is_informational_debug() {
    // A value in the reserved `type_name` debug field on a node is accepted as
    // informational (never identity).
    let mut with_debug = graph_artifact();
    with_debug["nodes"][0]
        .as_object_mut()
        .unwrap()
        .insert("type_name".into(), json!("some::rustc::type_name<T>"));
    assert_valid(ArtifactKind::Graph, 1, &with_debug);

    // Using `type_name` where a declared stable name is required (dropping the
    // stable `output_type_name` and offering only the debug `type_name`) is
    // rejected: the stable name is mandatory, the debug field can never stand in
    // for it.
    let mut misused = graph_artifact();
    let node = misused["nodes"][0].as_object_mut().unwrap();
    node.remove("output_type_name");
    node.insert("type_name".into(), json!("some::rustc::type_name<T>"));
    assert_invalid(ArtifactKind::Graph, 1, &misused);
}

// === run artifact (C22) ===================================================

fn run_artifact_full() -> Value {
    json!({
        "header": success_header(),
        "attempts": [
            attempt_record("n1", 1, "failed"),
            attempt_record("n1", 2, "succeeded")
        ],
        "summary": {
            "total_elapsed_ns": 1_000_000,
            "critical_path_ns": 800_000,
            "peak_slot_residency": 2,
            "retained_values": ["n1"],
            "abandoned_pinned_time_ns": 0,
            "abandoned_pinned_capacity": 0
        }
    })
}

#[test]
fn run_artifact_full_run_validates() {
    assert_valid(ArtifactKind::Run, 1, &run_artifact_full());

    // Malformed phase-duration set (a non-integer value) is rejected.
    let mut bad_phase = run_artifact_full();
    bad_phase["attempts"][0]["phase_durations_ns"]["running"] = json!("nope");
    assert_invalid(ArtifactKind::Run, 1, &bad_phase);

    // Malformed worker field (not a string) is rejected.
    let mut bad_worker = run_artifact_full();
    bad_worker["attempts"][0]["worker"] = json!(7);
    assert_invalid(ArtifactKind::Run, 1, &bad_worker);
}

#[test]
fn attempt_taxonomy_coverage() {
    // Every terminal status from the normative taxonomy validates on an attempt
    // record.
    let taxonomy = [
        "succeeded",
        "failed",
        "timed-out",
        "skipped",
        "upstream-skipped",
        "upstream-failed",
        "cancelled",
        "abandoned",
        "satisfied-from-prior",
    ];
    for status in taxonomy {
        let mut rec = attempt_record("n1", 1, status);
        if status == "satisfied-from-prior" {
            rec.as_object_mut().unwrap().insert(
                "satisfied_from_run".into(),
                json!("018f0000-0000-7000-8000-000000000001"),
            );
        }
        let mut art = run_artifact_full();
        art["attempts"] = json!([rec]);
        assert_valid(ArtifactKind::Run, 1, &art);
    }

    // A satisfied-from-prior record missing its originating run identity is
    // rejected.
    let mut missing_origin = attempt_record("n1", 1, "satisfied-from-prior");
    missing_origin
        .as_object_mut()
        .unwrap()
        .remove("satisfied_from_run");
    let mut art = run_artifact_full();
    art["attempts"] = json!([missing_origin]);
    assert_invalid(ArtifactKind::Run, 1, &art);

    // A status outside the taxonomy (`not-requested` — an artifact marking, not
    // a terminal state) is rejected as an attempt status.
    let mut art2 = run_artifact_full();
    art2["attempts"] = json!([attempt_record("n1", 1, "not-requested")]);
    assert_invalid(ArtifactKind::Run, 1, &art2);
}

#[test]
fn durable_reference_field_present_and_copied_forward() {
    // An attempt carrying a durable-output reference (T0.8 shape: an opaque,
    // serde-serializable reference) validates.
    let mut rec = attempt_record("n1", 1, "succeeded");
    rec.as_object_mut().unwrap().insert(
        "durable_reference".into(),
        json!({ "storage_key": "s3://bucket/n1/output", "content_hash": "blake3:dead" }),
    );
    let mut art = run_artifact_full();
    art["attempts"] = json!([rec]);
    assert_valid(ArtifactKind::Run, 1, &art);

    // A reference copied forward under satisfied-from-prior (resume lineage)
    // also validates.
    let mut carried = attempt_record("n2", 1, "satisfied-from-prior");
    let c = carried.as_object_mut().unwrap();
    c.insert(
        "satisfied_from_run".into(),
        json!("018f0000-0000-7000-8000-000000000001"),
    );
    c.insert(
        "durable_reference".into(),
        json!({ "storage_key": "s3://bucket/n2/output" }),
    );
    let mut art2 = run_artifact_full();
    art2["attempts"] = json!([carried]);
    assert_valid(ArtifactKind::Run, 1, &art2);
}

#[test]
fn assembly_failed_variant() {
    // outcome assembly-failed, no fingerprint, non-empty error list, zero
    // attempts -> accepted.
    let art = json!({
        "header": {
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "pipeline": "example-pipeline",
            "parameters": {},
            "data_interval": null,
            "captured_environment": {},
            "resume_lineage": null,
            "overall_outcome": "assembly-failed",
            "errors": ["node `a` duplicates node `a`", "node `b` lacks the durability contract"]
        },
        "attempts": [],
        "summary": null
    });
    assert_valid(ArtifactKind::Run, 1, &art);

    // The same fixture bearing a fingerprint is rejected: a fingerprint is
    // present only when assembly succeeded.
    let mut with_fp = art.clone();
    with_fp["header"].as_object_mut().unwrap().insert(
        "fingerprint_structural".into(),
        json!("blake3:1111111111111111111111111111111111111111111111111111111111111111"),
    );
    assert_invalid(ArtifactKind::Run, 1, &with_fp);

    // A non-empty error list is required for a pre-execution failure variant.
    let mut empty_errors = art.clone();
    empty_errors["header"]["errors"] = json!([]);
    assert_invalid(ArtifactKind::Run, 1, &empty_errors);
}

#[test]
fn bootstrap_failed_variant_is_distinct_from_assembly_failed() {
    let bootstrap = json!({
        "header": {
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "pipeline": "example-pipeline",
            "parameters": {},
            "data_interval": null,
            "captured_environment": {},
            "resume_lineage": null,
            "overall_outcome": "bootstrap-failed",
            "errors": ["the machine could not open the run store"]
        },
        "attempts": [],
        "summary": null
    });
    let mut assembly = bootstrap.clone();
    assembly["header"]["overall_outcome"] = json!("assembly-failed");

    assert_valid(ArtifactKind::Run, 1, &bootstrap);
    assert_valid(ArtifactKind::Run, 1, &assembly);

    // The two variants are SEPARATELY EXPRESSIBLE: conflating them into one
    // value must be impossible. An invented conflated value fails.
    let mut conflated = bootstrap.clone();
    conflated["header"]["overall_outcome"] = json!("pre-execution-failed");
    assert_invalid(ArtifactKind::Run, 1, &conflated);
    // And the two accepted values are genuinely different tokens.
    assert_ne!(
        bootstrap["header"]["overall_outcome"], assembly["header"]["overall_outcome"],
        "assembly-failed and bootstrap-failed are distinct outcome values"
    );
}

#[test]
fn not_requested_single_node_replay_variant() {
    // A single-node replay artifact: nodes outside the request carry the
    // `not-requested` marking (an artifact marking, distinct from any terminal
    // state), on the node roster rather than as an attempt status.
    let art = json!({
        "header": success_header(),
        "variant": "single-node-replay",
        "requested_node": "n1",
        "node_markings": {
            "n1": "requested",
            "n2": "not-requested",
            "n3": "not-requested"
        },
        "attempts": [ attempt_record("n1", 1, "succeeded") ],
        "summary": null
    });
    assert_valid(ArtifactKind::Run, 1, &art);

    // Using `not-requested` as a terminal status on an in-request node's attempt
    // record is rejected: it is a marking, never a terminal state.
    let mut bad = art.clone();
    bad["attempts"] = json!([attempt_record("n1", 1, "not-requested")]);
    assert_invalid(ArtifactKind::Run, 1, &bad);
}

#[test]
fn allowlisted_environment_capture() {
    // A header whose captured-environment map holds only allowlisted (string)
    // values validates.
    let mut art = run_artifact_full();
    art["header"]["captured_environment"] = json!({ "DAGR_REGION": "us-east-1" });
    assert_valid(ArtifactKind::Run, 1, &art);

    // The default allowlist is empty: an empty captured_environment is the
    // baseline and validates.
    let mut empty = run_artifact_full();
    empty["header"]["captured_environment"] = json!({});
    assert_valid(ArtifactKind::Run, 1, &empty);

    // A non-allowlisted-SHAPED value (a nested object rather than a captured
    // scalar) is rejected: the schema does not silently sanction unbounded
    // environment capture.
    let mut unbounded = run_artifact_full();
    unbounded["header"]["captured_environment"] =
        json!({ "SECRET": { "nested": "unbounded structure" } });
    assert_invalid(ArtifactKind::Run, 1, &unbounded);
}

// === evolution posture (T0.10) ============================================

#[test]
fn additive_only_evolution_ignores_unknown_and_defaults_missing() {
    // A version-1 artifact carrying an EXTRA field unknown to the v1 reader is
    // tolerated (open-world schema): validation passes.
    let mut with_unknown = run_artifact_full();
    with_unknown["header"]
        .as_object_mut()
        .unwrap()
        .insert("a_future_minor_field".into(), json!("hello"));
    with_unknown["summary"]
        .as_object_mut()
        .unwrap()
        .insert("another_future_field".into(), json!(123));
    assert_valid(ArtifactKind::Run, 1, &with_unknown);

    // A fixture MISSING an additively-introduced optional field still passes —
    // the reader defaults it. `message` is optional/defaulted on an attempt.
    let mut missing_optional = run_artifact_full();
    missing_optional["attempts"][0]
        .as_object_mut()
        .unwrap()
        .remove("message");
    assert_valid(ArtifactKind::Run, 1, &missing_optional);
}

// === corpus + helper ergonomics ==========================================

#[test]
fn published_schema_files_exist_for_every_family() {
    // Every artifact family publishes at least one versioned schema, at the
    // T4-fixed path `schemas/<kind>/v<version>.schema.json`.
    for kind in [
        ArtifactKind::EventStream,
        ArtifactKind::Graph,
        ArtifactKind::Run,
    ] {
        let versions = published_schema_versions(kind);
        assert!(
            versions.contains(&1),
            "{kind:?} must publish schema v1; SCHEMA_DIR={SCHEMA_DIR}"
        );
    }
}

#[test]
fn fixture_corpus_round_trip() {
    // Every checked-in corpus fixture validates against its declared version's
    // schema; the helper fails loudly (naming the offending fixture) if any does
    // not. This is the standing CI obligation (T0.10 / Stability), exercised
    // here over the seeded corpus.
    check_corpus()
        .unwrap_or_else(|e| panic!("corpus round-trip failed: {e}\nCORPUS_DIR={CORPUS_DIR}"));
}

// === writer → schema round-trip (C19 ↔ the published schema) ==============
//
// The missing end-to-end guarantee: a REAL `EventStreamWriter` (T19) must emit
// records that validate against the published event-stream schema (T39) it is
// contracted to. Before this reconciliation the writer emitted a divergent wire
// form (`event`/`body`, integer `wall`, `captured_env`, an array `data_interval`,
// and no `attempt-outcome`), so a real writer stream could not be folded (C22 /
// T42). This test drives the writer to produce **every** record kind and
// validates EACH emitted line — it FAILS if the writer ever diverges from the
// schema again.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{
    read_records, AttemptOutcomeRecord, Event, EventSink, EventStreamWriter, MonotonicClock, RunId,
    RunOutcome, RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
};

/// An in-memory sink capturing every appended line in order.
#[derive(Clone, Default)]
struct VecSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl EventSink for VecSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.lines.lock().unwrap().push(line.to_vec());
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock returning distinct, non-decreasing offsets.
#[derive(Default)]
struct TickClock {
    n: std::sync::atomic::AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

/// A fixed RFC3339 wall stamp so the record bytes are deterministic (T4 §6).
fn fixed_wall() -> String {
    "2026-07-23T00:00:00.000Z".to_string()
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one non-vacuous end-to-end guarantee: it drives a real writer through \
              EVERY record kind (a full run-started header, each per-transition event, \
              the rich attempt-outcome, zombie-at-exit, run-finished) plus the \
              assembly-failed variant, and validates each emitted line — splitting it \
              would fragment the single writer-conformance narrative"
)]
fn writer_output_validates_against_published_schema_for_every_kind() {
    let sink = VecSink::default();
    let mut w = EventStreamWriter::new(
        sink.clone(),
        TickClock::default(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-0123456789ab"),
        "example-pipeline",
    )
    .with_wall_clock(fixed_wall);

    // run-started with a full assembly-succeeded header (both fingerprints, a
    // data interval, captured env, resume lineage).
    let mut params = std::collections::BTreeMap::new();
    params.insert("date".to_string(), "2026-07-23".to_string());
    let mut env = std::collections::BTreeMap::new();
    env.insert("REGION".to_string(), "us-east-1".to_string());
    w.run_started(RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: Some("blake3:aaaa".to_string()),
        fingerprint_policy: Some("blake3:bbbb".to_string()),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: params,
        data_interval: Some([
            "2026-07-23T00:00:00Z".to_string(),
            "2026-07-24T00:00:00Z".to_string(),
        ]),
        captured_env: env,
        resumed_from: Some("018f4a1e-0000-7b3d-9e10-0123456789ab".to_string()),
    })
    .unwrap();
    w.node_ready("n").unwrap();
    w.node_admitted("n").unwrap();
    w.attempt_started("n", 1).unwrap();
    w.attempt_succeeded("n", 1).unwrap();
    w.attempt_failed("n", 1).unwrap();
    // The rich attempt-outcome carrying every fold field.
    let mut outcome = AttemptOutcomeRecord::new("n", 1, "succeeded");
    outcome.worker = Some("compute#3".to_string());
    outcome.message = Some("ok".to_string());
    outcome.metrics = Some(json!({ "rows_read": 42 }));
    outcome.cost_declared = Some(json!({ "memory_bytes": 1024 }));
    outcome.cost_measured = Some(json!({ "memory_bytes": 900 }));
    outcome.durable_reference = Some(json!({ "storage_key": "s3://bucket/n/output" }));
    w.attempt_outcome(outcome).unwrap();
    // A minimal attempt-outcome (the M1/M2 driver's shape: node/attempt/status).
    w.attempt_outcome(AttemptOutcomeRecord::new("n", 1, "succeeded"))
        .unwrap();
    w.node_terminal("n", TerminalState::Succeeded).unwrap();
    w.zombie_at_exit("n", 1).unwrap();
    w.run_finished(RunOutcome::Succeeded).unwrap();
    w.finish().unwrap();

    let lines = sink.lines.lock().unwrap().clone();
    // The kinds present must cover the whole closed vocabulary (non-vacuous: a
    // writer that emitted nothing, or omitted a kind, fails here).
    let mut kinds: Vec<String> = Vec::new();
    for line in &lines {
        let value: Value = serde_json::from_slice(line).expect("record parses as JSON");
        // EACH emitted record validates against the published schema.
        assert_valid(ArtifactKind::EventStream, 1, &value);
        kinds.push(
            value
                .get("kind")
                .and_then(Value::as_str)
                .expect("record carries a `kind` discriminator")
                .to_string(),
        );
    }
    for expected in [
        "run-started",
        "node-ready",
        "node-admitted",
        "attempt-started",
        "attempt-succeeded",
        "attempt-failed",
        "attempt-outcome",
        "node-terminal",
        "zombie-at-exit",
        "run-finished",
    ] {
        assert!(
            kinds.iter().any(|k| k == expected),
            "the writer must emit a `{expected}` record; emitted kinds: {kinds:?}"
        );
    }

    // The assembly-failed path still emits a schema-valid two-record stream: the
    // fingerprint fields are present (the schema requires them) as the sentinel.
    let sink2 = VecSink::default();
    let mut w2 = EventStreamWriter::new(
        sink2.clone(),
        TickClock::default(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-fedcba987654"),
        "example-pipeline",
    )
    .with_wall_clock(fixed_wall);
    w2.run_started(RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: None,
        fingerprint_policy: None,
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: std::collections::BTreeMap::new(),
        data_interval: None,
        captured_env: std::collections::BTreeMap::new(),
        resumed_from: None,
    })
    .unwrap();
    w2.run_finished(RunOutcome::AssemblyFailed).unwrap();
    let lines2 = sink2.lines.lock().unwrap().clone();
    assert_eq!(lines2.len(), 2, "assembly-failed is a two-record stream");
    for line in &lines2 {
        let value: Value = serde_json::from_slice(line).unwrap();
        assert_valid(ArtifactKind::EventStream, 1, &value);
    }

    // The whole stream is readable by the tolerant reader (the fold's front door).
    let mut bytes: Vec<u8> = Vec::new();
    for line in &lines {
        bytes.extend_from_slice(line);
    }
    let parsed = read_records(&bytes).expect("the writer's stream reads back");
    assert_eq!(parsed.records.len(), lines.len());
    assert!(!parsed.trailing_partial_discarded);
}

#[test]
fn invalid_input_rejected_with_a_usable_error() {
    // A deliberately malformed artifact (schema-version field missing entirely
    // from the header) fails, and the error names the failing artifact and the
    // reason so tests and CI can assert on it.
    let mut malformed = graph_artifact();
    malformed["header"]
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let err = assert_invalid(ArtifactKind::Graph, 1, &malformed);
    let text = err.to_string();
    assert!(
        text.contains("graph") && text.contains("schema_version"),
        "error must name the artifact kind and the failing reason, got: {text}"
    );
}

// === writer-generated corpus fixtures (T39 corpus ← real writer) ==========
//
// The event-stream corpus fixtures (tests/fixtures/corpus/event-stream/v1/*.json)
// are generated from REAL `EventStreamWriter` output, so they double as a
// writer-conformance golden: if the writer's wire form ever drifts from the
// checked-in fixtures, `event_stream_corpus_matches_real_writer_output` fails —
// and `fixture_corpus_round_trip` (above) still proves every fixture is
// schema-valid. Regenerate with:
//   cargo test -p dagr-artifact --features schema-validation --test artifact_schemas \
//       regenerate_event_stream_corpus -- --ignored

/// The canonical corpus records, as `(fixture-file-stem, one canonical JSON
/// line)` — every one produced by a REAL writer through the capturing sink, with
/// a fixed wall stamp and a manual clock so the bytes are deterministic. One
/// representative fixture per interesting record kind (run-started with a full
/// header, the rich attempt-outcome, and zombie-at-exit).
fn writer_corpus_records() -> Vec<(String, Vec<u8>)> {
    let sink = VecSink::default();
    let mut w = EventStreamWriter::new(
        sink.clone(),
        TickClock::default(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-0123456789ab"),
        "example-pipeline",
    )
    .with_wall_clock(fixed_wall);

    let mut params = std::collections::BTreeMap::new();
    params.insert("date".to_string(), "2026-07-23".to_string());

    // 0: run-started (full assembly-succeeded header).
    w.run_started(RunStartedHeader {
        pipeline: "example-pipeline".to_string(),
        fingerprint_structural: Some(
            "blake3:1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        fingerprint_policy: Some(
            "blake3:2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: params,
        data_interval: Some([
            "2026-07-23T00:00:00Z".to_string(),
            "2026-07-24T00:00:00Z".to_string(),
        ]),
        captured_env: std::collections::BTreeMap::new(),
        resumed_from: None,
    })
    .unwrap();
    // 1: attempt-outcome (the fold's richest input).
    let mut outcome = AttemptOutcomeRecord::new("load", 1, "succeeded");
    outcome.worker = Some("compute#3".to_string());
    outcome.message = Some("ok".to_string());
    outcome.metrics = Some(json!({ "rows_read": 42 }));
    outcome.cost_declared = Some(json!({ "memory_bytes": 1024 }));
    outcome.cost_measured = Some(json!({ "memory_bytes": 900 }));
    w.attempt_outcome(outcome).unwrap();
    // 2: zombie-at-exit.
    let _ = w.emit_event(&Event::ZombieAtExit {
        node: "slow-node".to_string(),
        attempt: 1,
    });

    let lines = sink.lines.lock().unwrap().clone();
    vec![
        ("run-started".to_string(), lines[0].clone()),
        ("attempt-outcome".to_string(), lines[1].clone()),
        ("zombie-at-exit".to_string(), lines[2].clone()),
    ]
}

/// The corpus directory for event-stream v1 fixtures.
fn event_stream_corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(CORPUS_DIR)
        .join("event-stream")
        .join("v1")
}

/// Pretty-print a canonical record line as multi-line JSON (2-space indent) so a
/// checked-in fixture is human-readable; content is byte-equal in value.
fn pretty(line: &[u8]) -> Vec<u8> {
    let v: Value = serde_json::from_slice(line).expect("record parses");
    let mut s = serde_json::to_vec_pretty(&v).expect("pretty prints");
    s.push(b'\n');
    s
}

#[test]
#[ignore = "regenerates the checked-in event-stream corpus from real writer output"]
fn regenerate_event_stream_corpus() {
    let dir = event_stream_corpus_dir();
    std::fs::create_dir_all(&dir).expect("corpus dir");
    for (stem, line) in writer_corpus_records() {
        let path = dir.join(format!("{stem}.json"));
        std::fs::write(&path, pretty(&line)).expect("write fixture");
        // Each regenerated fixture is schema-valid.
        let v: Value = serde_json::from_slice(&line).unwrap();
        assert_valid(ArtifactKind::EventStream, 1, &v);
    }
}

#[test]
fn event_stream_corpus_matches_real_writer_output() {
    // The checked-in corpus is a writer-conformance golden: each fixture equals
    // (by JSON value) what a real writer emits for that record today. A drift in
    // the writer's wire form fails here — regenerate with the ignored test.
    let dir = event_stream_corpus_dir();
    for (stem, line) in writer_corpus_records() {
        let path = dir.join(format!("{stem}.json"));
        let on_disk = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("corpus fixture {} missing: {e}", path.display()));
        let disk_value: Value = serde_json::from_slice(&on_disk)
            .unwrap_or_else(|e| panic!("corpus fixture {} is not JSON: {e}", path.display()));
        let writer_value: Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(
            disk_value,
            writer_value,
            "corpus fixture {} drifted from real writer output — regenerate the \
             event-stream corpus (cargo test … regenerate_event_stream_corpus -- --ignored)",
            path.display()
        );
    }
}
