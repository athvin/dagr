//! C22 · Run-artifact **fold** schema round-trip — ticket T42 (053). Written
//! first, TDD.
//!
//! The load-bearing interlock with T39 (ticket 050): a **real folded** run
//! artifact validates against the published `schemas/run/v1.schema.json` via the
//! T39 validation helper (`dagr_artifact::schema`), across the full-run,
//! interrupted, and pre-execution-failure variants. This proves the fold emits
//! to the *published* contract, and that the fold-reader version declaration the
//! fold adds is additive (unknown fields validate, T0.10). A deliberately
//! corrupted copy is rejected — proving the check has teeth.
//!
//! Gated behind the `schema-validation` feature (default OFF), which pulls the
//! CI-/dev-scoped `jsonschema` validator (T4 ADR 017 §4); CI runs it with the
//! feature ON in a dedicated step, mirroring T39/T40. The shipped binary and the
//! bare `cargo test --workspace` never activate it.

#![cfg(feature = "schema-validation")]

use serde_json::{json, Value};

use dagr_artifact::fold::fold_stream;
use dagr_artifact::schema::{validate_value, ArtifactKind};

fn env(seq: u64, offset_ns: u64, kind: &str) -> Value {
    json!({
        "schema_version": "dagr.event-stream@1",
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "seq": seq,
        "wall": "2026-07-23T00:00:00.000Z",
        "offset_ns": offset_ns,
        "kind": kind,
    })
}

fn with(mut v: Value, fields: &[(&str, Value)]) -> Value {
    let o = v.as_object_mut().unwrap();
    for (k, val) in fields {
        o.insert((*k).to_string(), val.clone());
    }
    v
}

fn start_header() -> Value {
    json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
        "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
        "fingerprint_algorithm_version": 1,
        "parameters": { "date": "2026-07-23" },
        "data_interval": { "start": "2026-07-23T00:00:00Z", "end": "2026-07-24T00:00:00Z" },
        "captured_environment": { "DAGR_REGION": "us-east-1" },
        "resume_lineage": null,
    })
}

fn stream(records: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for r in records {
        out.push_str(&serde_json::to_string(r).unwrap());
        out.push('\n');
    }
    out.into_bytes()
}

fn full_run_stream() -> Vec<u8> {
    stream(&[
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(env(1, 100, "node-ready"), &[("node", json!("load"))]),
        with(env(2, 300, "node-admitted"), &[("node", json!("load"))]),
        with(
            env(3, 500, "attempt-started"),
            &[("node", json!("load")), ("attempt", json!(1))],
        ),
        with(
            env(4, 1000, "attempt-outcome"),
            &[
                ("node", json!("load")),
                ("attempt", json!(1)),
                ("status", json!("failed")),
                ("message", json!("transient failure")),
                ("error", json!({ "kind": "transient", "detail": "timeout" })),
                ("metrics", json!({ "rows_read": 0 })),
                ("cost_declared", json!({ "memory_bytes": 1024 })),
                ("cost_measured", json!({ "memory_bytes": 512 })),
            ],
        ),
        with(
            env(5, 1100, "attempt-started"),
            &[("node", json!("load")), ("attempt", json!(2))],
        ),
        with(
            env(6, 2000, "attempt-outcome"),
            &[
                ("node", json!("load")),
                ("attempt", json!(2)),
                ("status", json!("succeeded")),
                ("metrics", json!({ "rows_read": 1000 })),
                ("cost_declared", json!({ "memory_bytes": 1024 })),
                ("cost_measured", json!({ "memory_bytes": 900 })),
                ("retained", json!(true)),
                ("slot_residency", json!(2)),
                (
                    "durable_reference",
                    json!({ "storage_key": "file:///runs/example/load/output" }),
                ),
            ],
        ),
        with(
            env(7, 2000, "node-terminal"),
            &[("node", json!("load")), ("state", json!("succeeded"))],
        ),
        with(
            env(8, 2000, "node-terminal"),
            &[
                ("node", json!("sink")),
                ("state", json!("satisfied-from-prior")),
                (
                    "satisfied_from_run",
                    json!("018f0000-0000-7000-8000-000000000001"),
                ),
            ],
        ),
        with(
            env(9, 2000, "run-finished"),
            &[("outcome", json!("succeeded"))],
        ),
    ])
}

#[test]
fn folded_full_run_validates_against_published_schema() {
    let art = fold_stream(
        &full_run_stream(),
        &["load".to_string(), "sink".to_string()],
    )
    .expect("fold");
    let value = art.to_value();
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("REAL folded run artifact must validate: {e}"));

    // The fold-reader declaration is additive and validates (unknown fields
    // ignored, T0.10).
    assert!(
        value.get("fold_reader").is_some(),
        "fold declares its reader version"
    );

    // Teeth: a corrupted copy (non-integer phase duration) is rejected.
    let mut bad = value.clone();
    bad["attempts"][0]["phase_durations_ns"]["executing"] = json!("nope");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "the schema check has teeth"
    );
}

#[test]
fn folded_interrupted_run_validates() {
    // A crash-truncated stream (no run-finished + a trailing partial) folds to an
    // interrupted artifact that still validates.
    let mut bytes = stream(&[
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(
            env(1, 500, "attempt-started"),
            &[("node", json!("load")), ("attempt", json!(1))],
        ),
        with(
            env(2, 1000, "attempt-outcome"),
            &[
                ("node", json!("load")),
                ("attempt", json!(1)),
                ("status", json!("succeeded")),
            ],
        ),
        with(
            env(3, 1000, "node-terminal"),
            &[("node", json!("load")), ("state", json!("succeeded"))],
        ),
    ]);
    bytes.extend_from_slice(br#"{"schema_version":"dagr.event-stream@1","seq":4,"#);
    let art = fold_stream(&bytes, &["load".to_string()]).expect("fold");
    assert!(art.is_interrupted());
    validate_value(ArtifactKind::Run, 1, &art.to_value())
        .unwrap_or_else(|e| panic!("folded interrupted artifact must validate: {e}"));
}

#[test]
fn folded_assembly_failed_validates() {
    let mut header = start_header();
    let h = header.as_object_mut().unwrap();
    h.remove("fingerprint_structural");
    h.remove("fingerprint_policy");
    h.remove("fingerprint_algorithm_version");
    let bytes = stream(&[
        with(env(0, 0, "run-started"), &[("header", header)]),
        with(
            env(1, 0, "run-finished"),
            &[
                ("outcome", json!("assembly-failed")),
                ("errors", json!(["node `a` duplicates node `a`"])),
            ],
        ),
    ]);
    let art = fold_stream(&bytes, &[]).expect("fold");
    validate_value(ArtifactKind::Run, 1, &art.to_value())
        .unwrap_or_else(|e| panic!("folded assembly-failed artifact must validate: {e}"));
}
