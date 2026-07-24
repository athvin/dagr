//! C23 · Node metrics **reach the run artifact unmodified** — ticket T44 (055).
//! Written first, TDD.
//!
//! The C23↔C22 boundary: a collected metric set (task entries + framework
//! entries under the reserved `dagr.` prefix) rides in an `attempt-outcome`
//! record's `metrics` object, folds through T42's `fold_stream`, and appears in
//! the run artifact **byte-for-value identical** to what was collected — the
//! fold copies metrics unmodified (arch.md `### C22`, `### C23`). The
//! schema-round-trip half (behind the `schema-validation` feature) proves a REAL
//! metrics-carrying folded artifact validates against the published
//! `schemas/run/v1.schema.json` with the schema UNCHANGED (the metrics map is an
//! open numeric map the schema already declares).

use serde_json::{json, Value};

use dagr_artifact::fold::fold_stream;

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
        "parameters": {},
        "data_interval": null,
        "captured_environment": {},
        "resume_lineage": null,
    })
}

/// A stream whose single node's attempt carries a mix of task and framework
/// (reserved-prefix) metrics.
fn stream_with_metrics(metrics: &Value) -> Vec<u8> {
    let records = [
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(env(1, 10, "node-ready"), &[("node", json!("collector"))]),
        with(env(2, 20, "node-admitted"), &[("node", json!("collector"))]),
        with(
            env(3, 30, "attempt-started"),
            &[("node", json!("collector")), ("attempt", json!(1))],
        ),
        with(
            env(4, 130, "attempt-outcome"),
            &[
                ("node", json!("collector")),
                ("attempt", json!(1)),
                ("status", json!("succeeded")),
                ("worker", json!("compute#1")),
                ("metrics", metrics.clone()),
            ],
        ),
        with(
            env(5, 130, "node-terminal"),
            &[("node", json!("collector")), ("state", json!("succeeded"))],
        ),
        with(
            env(6, 140, "run-finished"),
            &[("outcome", json!("succeeded"))],
        ),
    ];
    let mut bytes = Vec::new();
    for r in records {
        bytes.extend_from_slice(serde_json::to_string(&r).unwrap().as_bytes());
        bytes.push(b'\n');
    }
    bytes
}

#[test]
fn collected_metrics_reach_the_artifact_byte_for_value_identical() {
    // A mix of task metrics (unit-suffixed names) and framework metrics under the
    // reserved `dagr.` prefix — exactly what C23's collector produces.
    let metrics = json!({
        "rows_read": 42,
        "bytes_spilled": 1_048_576,
        "dagr.peak_memory_bytes": 4_194_304,
        "dagr.permit.memory_bytes": 1024,
        "dagr.phase.executing_ns": 100,
        "dagr.metrics.dropped_entries_count": 0,
    });

    let bytes = stream_with_metrics(&metrics);
    let artifact = fold_stream(&bytes, &["collector".to_string()]).expect("folds");

    let attempt = artifact
        .attempts()
        .iter()
        .find(|a| a.node() == "collector")
        .expect("the collector attempt is present");

    // Every collected metric — task AND framework — appears unaltered.
    assert_eq!(
        attempt.metrics(),
        &metrics,
        "the fold carries the metric set unmodified (task + framework entries)"
    );
    // Spot-check individual entries survived name-and-value.
    let m = attempt.metrics().as_object().unwrap();
    assert_eq!(m["rows_read"], json!(42));
    assert_eq!(m["dagr.peak_memory_bytes"], json!(4_194_304));
    assert_eq!(m["dagr.permit.memory_bytes"], json!(1024));
}

#[cfg(feature = "schema-validation")]
mod schema {
    use super::*;
    use dagr_artifact::schema::{validate_value, ArtifactKind};

    #[test]
    fn a_real_metrics_carrying_folded_artifact_validates_against_the_published_run_schema() {
        let metrics = json!({
            "rows_read": 42,
            "bytes_spilled": 1_048_576,
            "dagr.peak_memory_bytes": 4_194_304,
            "dagr.permit.memory_bytes": 1024,
            "dagr.permit.compute_threads": 2,
            "dagr.phase.executing_ns": 100,
            "dagr.phase.permit_wait_ns": 10,
            "dagr.metrics.dropped_entries_count": 0,
            "dagr.metrics.dropped_bytes_count": 0,
            "dagr.metrics.truncated_count": 0,
        });
        let bytes = stream_with_metrics(&metrics);
        let artifact = fold_stream(&bytes, &["collector".to_string()]).expect("folds");
        let value = artifact.to_value();

        validate_value(ArtifactKind::Run, 1, &value).expect(
            "the metrics-carrying folded artifact validates against schemas/run/v1.schema.json",
        );

        // And the metrics survived into the validated document unmodified.
        let attempt = &value["attempts"][0];
        assert_eq!(&attempt["metrics"], &metrics);
    }
}
