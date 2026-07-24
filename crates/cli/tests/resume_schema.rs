//! C27 · Resumed-artifact **schema round-trip** — ticket T58 (070), written first
//! (TDD). Gated behind `schema-validation` (default OFF), the CI-/dev-scoped
//! validator (T4 ADR 017 §4), like the T39/T42/T57 schema round-trips.
//!
//! A REAL resumed run artifact produced by `resume_verb` — satisfied-from-prior
//! nodes recorded with their originating run identity, durable references copied
//! forward, and the header linked to both the immediate parent and the lineage
//! root — validates against the UNMODIFIED published `schemas/run/v1.schema.json`
//! (T39). T58 edits **no** schema: the `satisfied_from_run`, `resume_lineage`, and
//! `durable_reference` slots were all published by T39; this proves the resume
//! recording emits into them validly. Teeth: a corrupted copy is rejected.

#![cfg(feature = "schema-validation")]

use std::collections::BTreeMap;

use serde_json::json;

use dagr_artifact::schema::{validate_value, ArtifactKind};
use dagr_cli::contract::{resume_verb, ResumeOptions, ResumeOutcome};
use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::resume::ReferenceExistence;
use dagr_core::task::Task;
use dagr_core::{RehydrateError, RunContext, TaskError};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Blob(String);
impl DurableOutput for Blob {
    fn serialize_reference(&self) -> String {
        self.0.clone()
    }
    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        Ok(Blob(reference.to_string()))
    }
}
struct MakeBlob;
impl Task for MakeBlob {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        Ok(Blob("x".into()))
    }
}
struct Passthrough;
impl Task for Passthrough {
    type Input = Blob;
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, i: Blob) -> Result<Blob, TaskError> {
        Ok(i)
    }
}

fn durable_chain() -> Pipeline {
    let mut flow = Flow::new();
    let produce = flow.register_source_durable("produce", &MakeBlob, NodePolicy::new());
    let _consume = flow.register("consume", &Passthrough, produce);
    flow.finish()
}

#[test]
fn a_real_resumed_artifact_validates_against_the_unmodified_run_schema() {
    let pipeline = durable_chain();
    let fp = pipeline.fingerprint();
    let prior = json!({
        "header": {
            "run_id": "run-A",
            "pipeline": "example-pipeline",
            "fingerprint_structural": format!("fnv:{:016x}", fp.structural()),
            "fingerprint_policy": format!("fnv:{:016x}", fp.policy()),
            "fingerprint_algorithm_version": fp.algorithm_version(),
            "tool_version": "dagr@1",
            "parameters": { "region": "eu" },
            "data_interval": { "start": "2026-07-01", "end": "2026-07-02" },
            "captured_environment": {},
            "resume_lineage": { "parent_run_id": "run-ROOT", "lineage_root_run_id": "run-ROOT" },
            "overall_outcome": "failed",
        },
        "attempts": [
            {
                "node": "produce", "attempt": 1, "status": "succeeded",
                "phase_durations_ns": { "executing": 10 }, "worker": "compute#1",
                "durable_reference": "produce/out",
            },
            {
                "node": "consume", "attempt": 1, "status": "failed",
                "phase_durations_ns": { "executing": 5 }, "worker": "compute#1",
            },
        ],
        "summary": null,
    });

    let options = ResumeOptions {
        new_run_id: "run-B".to_string(),
        tool_version: "dagr@1".to_string(),
        store_present: true,
        force: false,
        param_overrides: BTreeMap::new(),
        interval_override: None,
    };
    let bytes = serde_json::to_vec(&prior).unwrap();
    let artifact = match resume_verb(&pipeline, &bytes, &options, |_n, _r| {
        ReferenceExistence::Present
    }) {
        ResumeOutcome::Resumed { artifact, .. } => artifact,
        ResumeOutcome::Refused { code, message } => {
            panic!("expected a resumed artifact, got refusal {code:?}: {message}")
        }
    };

    // The satisfied-from-prior producer carries its originating run + copied ref.
    let produce = artifact["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["node"] == json!("produce"))
        .expect("produce recorded");
    assert_eq!(produce["status"], json!("satisfied-from-prior"));
    assert_eq!(produce["satisfied_from_run"], json!("run-A"));
    assert_eq!(produce["durable_reference"], json!("produce/out"));

    // The REAL resumed artifact validates against the UNMODIFIED published schema.
    validate_value(ArtifactKind::Run, 1, &artifact).unwrap_or_else(|e| {
        panic!("REAL resumed artifact must validate against the run schema: {e}")
    });

    // Teeth: a satisfied-from-prior record WITHOUT its originating run identity is
    // rejected by the schema's conditional requirement (not vacuously passing).
    let mut bad = artifact.clone();
    let idx = bad["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .position(|a| a["node"] == json!("produce"))
        .unwrap();
    bad["attempts"][idx]
        .as_object_mut()
        .unwrap()
        .remove("satisfied_from_run");
    assert!(
        validate_value(ArtifactKind::Run, 1, &bad).is_err(),
        "a satisfied-from-prior record must carry its originating run identity (schema teeth)"
    );
}
