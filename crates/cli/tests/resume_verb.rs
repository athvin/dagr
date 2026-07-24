//! C27 · Resume **verb wiring** — ticket T58 (070). Written first, TDD.
//!
//! The `resume` verb was stubbed by T55 (068). T58 replaces the stub with the
//! real resume behaviour: it reads a prior run's folded artifact, runs the pure
//! seed/closure/demand plan (`dagr_core::resume`), derives parameters and the
//! data interval from the prior artifact (a conflict refuses; `--force`
//! overrides and is recorded), refuses a prior run whose run store is gone, and
//! produces a resumed run artifact that records satisfied-from-prior nodes with
//! their originating run identity, copies durable references forward so the
//! artifact is self-contained, and links the run to its immediate parent and its
//! lineage root.
//!
//! These exercise the REAL `dagr_cli::contract` resume entry against the REAL
//! `dagr_core::flow` pipeline surface and the REAL fold shape. Determinism +
//! refusal-exit-code alignment with the C26 table are asserted here; the
//! exhaustive behavioural matrix is T59.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use dagr_cli::contract::{resume_verb, ExitCode, ResumeOptions, ResumeOutcome};
use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::resume::ReferenceExistence;
use dagr_core::task::Task;
use dagr_core::{RehydrateError, RunContext, TaskError};

// === Fixtures ==============================================================

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

/// `produce` (durable) → `consume`.
fn durable_chain() -> Pipeline {
    let mut flow = Flow::new();
    let produce = flow.register_source_durable("produce", &MakeBlob, NodePolicy::new());
    let _consume = flow.register("consume", &Passthrough, produce);
    flow.finish()
}

/// A folded-style prior run artifact JSON for `pipeline`, matching its
/// fingerprint and tool version, one attempt per `(node, status, dref)`.
fn prior_artifact(
    pipeline: &Pipeline,
    run_id: &str,
    params: &[(&str, &str)],
    interval: Option<[&str; 2]>,
    resume_lineage: Value,
    attempts: &[(&str, &str, Option<&str>)],
) -> Value {
    let fp = pipeline.fingerprint();
    let attempts: Vec<Value> = attempts
        .iter()
        .map(|(node, status, dref)| {
            let mut a = json!({
                "node": node,
                "attempt": 1,
                "status": status,
                "phase_durations_ns": { "executing": 10 },
                "worker": "compute#1",
            });
            if let Some(r) = dref {
                a["durable_reference"] = json!(r);
            }
            a
        })
        .collect();
    let params_obj: serde_json::Map<String, Value> = params
        .iter()
        .map(|(k, v)| ((*k).to_string(), json!(v)))
        .collect();
    json!({
        "header": {
            "run_id": run_id,
            "pipeline": "example-pipeline",
            "fingerprint_structural": format!("fnv:{:016x}", fp.structural()),
            "fingerprint_policy": format!("fnv:{:016x}", fp.policy()),
            "fingerprint_algorithm_version": fp.algorithm_version(),
            "tool_version": "dagr@1",
            "parameters": params_obj,
            "data_interval": interval.map_or(Value::Null, |[s, e]| json!({ "start": s, "end": e })),
            "captured_environment": {},
            "resume_lineage": resume_lineage,
            "overall_outcome": "failed",
        },
        "attempts": attempts,
        "summary": null,
    })
}

fn present(_n: &str, _r: &str) -> ReferenceExistence {
    ReferenceExistence::Present
}

fn opts(run_id: &str) -> ResumeOptions {
    ResumeOptions {
        new_run_id: run_id.to_string(),
        tool_version: "dagr@1".to_string(),
        store_present: true,
        force: false,
        param_overrides: BTreeMap::new(),
        interval_override: None,
    }
}

fn run(pipeline: &Pipeline, prior: &Value, options: ResumeOptions) -> ResumeOutcome {
    let bytes = serde_json::to_vec(prior).unwrap();
    resume_verb(pipeline, &bytes, &options, present)
}

// ===========================================================================
// Missing run store — refused up front, before planning.
// ===========================================================================

#[test]
fn missing_run_store_is_refused_up_front() {
    let pipeline = durable_chain();
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[],
        None,
        Value::Null,
        &[("produce", "succeeded", Some("produce/out")), ("consume", "failed", None)],
    );
    let mut options = opts("run-B");
    options.store_present = false; // the prior run's store is gone

    let outcome = run(&pipeline, &prior, options);
    match outcome {
        ResumeOutcome::Refused { code, message } => {
            assert_eq!(code, ExitCode::ResumeRefusal);
            assert!(
                message.to_lowercase().contains("store"),
                "the refusal says the store is gone: {message}"
            );
        }
        other => panic!("expected a store-gone refusal, got {other:?}"),
    }
}

// ===========================================================================
// The gate — structural mismatch refuses; algorithm/tool distinct.
// ===========================================================================

#[test]
fn structural_mismatch_refuses_with_the_refusal_code_and_prints_the_diff() {
    let pipeline = durable_chain();
    // A prior recorded against a DIFFERENT graph: corrupt the structural hash.
    let mut prior = prior_artifact(
        &pipeline,
        "run-A",
        &[],
        None,
        Value::Null,
        &[("produce", "succeeded", Some("produce/out"))],
    );
    prior["header"]["fingerprint_structural"] = json!("fnv:0000000000000000");

    let outcome = run(&pipeline, &prior, opts("run-B"));
    match outcome {
        ResumeOutcome::Refused { code, message } => {
            assert_eq!(code, ExitCode::ResumeRefusal);
            assert!(
                message.to_lowercase().contains("structural"),
                "prints the structural diff: {message}"
            );
        }
        other => panic!("expected a structural-mismatch refusal, got {other:?}"),
    }
}

// ===========================================================================
// Invocation derivation — parameters + interval from the prior artifact.
// ===========================================================================

#[test]
fn parameters_and_interval_are_derived_from_the_prior_artifact() {
    let pipeline = durable_chain();
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[("region", "eu")],
        Some(["2026-07-01", "2026-07-02"]),
        Value::Null,
        &[("produce", "succeeded", Some("produce/out")), ("consume", "failed", None)],
    );
    let outcome = run(&pipeline, &prior, opts("run-B"));
    let artifact = outcome.expect_resumed();
    assert_eq!(
        artifact["header"]["parameters"]["region"],
        json!("eu"),
        "the resumed run uses the prior parameters"
    );
    assert_eq!(
        artifact["header"]["data_interval"],
        json!({ "start": "2026-07-01", "end": "2026-07-02" }),
        "and the prior data interval"
    );
}

#[test]
fn a_conflicting_parameter_without_force_refuses_with_a_diff() {
    let pipeline = durable_chain();
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[("region", "eu")],
        None,
        Value::Null,
        &[("produce", "succeeded", Some("produce/out")), ("consume", "failed", None)],
    );
    let mut options = opts("run-B");
    options
        .param_overrides
        .insert("region".to_string(), "us".to_string()); // conflicts with prior "eu"

    match run(&pipeline, &prior, options) {
        ResumeOutcome::Refused { code, message } => {
            assert_eq!(code, ExitCode::ResumeRefusal);
            assert!(message.contains("region"), "the diff names the conflicting parameter: {message}");
            assert!(message.contains("eu") && message.contains("us"), "and both values: {message}");
        }
        other => panic!("expected a parameter-conflict refusal, got {other:?}"),
    }
}

#[test]
fn force_overrides_a_conflicting_parameter_and_records_it() {
    let pipeline = durable_chain();
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[("region", "eu")],
        None,
        Value::Null,
        &[("produce", "succeeded", Some("produce/out")), ("consume", "failed", None)],
    );
    let mut options = opts("run-B");
    options.force = true;
    options
        .param_overrides
        .insert("region".to_string(), "us".to_string());

    let artifact = run(&pipeline, &prior, options).expect_resumed();
    assert_eq!(
        artifact["header"]["parameters"]["region"],
        json!("us"),
        "the override wins under --force"
    );
    assert_eq!(
        artifact["header"]["resume_forced"],
        json!(true),
        "and the resumed artifact records that force was used"
    );
}

// ===========================================================================
// Satisfied-from-prior recording + copy-forward + lineage.
// ===========================================================================

#[test]
fn a_durable_success_is_recorded_satisfied_from_prior_with_origin_and_ref_copied_forward() {
    let pipeline = durable_chain();
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[],
        None,
        Value::Null,
        &[
            ("produce", "succeeded", Some("produce/out")),
            ("consume", "failed", None),
        ],
    );
    let artifact = run(&pipeline, &prior, opts("run-B")).expect_resumed();

    let produce = attempt_for(&artifact, "produce");
    assert_eq!(
        produce["status"],
        json!("satisfied-from-prior"),
        "the durable success is recorded satisfied-from-prior"
    );
    assert_eq!(
        produce["satisfied_from_run"],
        json!("run-A"),
        "carrying its originating run identity"
    );
    assert_eq!(
        produce["durable_reference"],
        json!("produce/out"),
        "the durable reference is copied forward so the artifact is self-contained"
    );
}

#[test]
fn the_resumed_artifact_links_parent_and_lineage_root() {
    let pipeline = durable_chain();
    // A first resume of the original run-A: parent = root = run-A.
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[],
        None,
        Value::Null, // run-A is the original (not itself a resume)
        &[("produce", "succeeded", Some("produce/out")), ("consume", "failed", None)],
    );
    let artifact = run(&pipeline, &prior, opts("run-B")).expect_resumed();
    let lineage = &artifact["header"]["resume_lineage"];
    assert_eq!(lineage["parent_run_id"], json!("run-A"), "immediate parent is the prior run");
    assert_eq!(
        lineage["lineage_root_run_id"],
        json!("run-A"),
        "the lineage root of a first resume is the original run"
    );
    assert_eq!(artifact["header"]["run_id"], json!("run-B"));
}

#[test]
fn multi_generation_lineage_keeps_the_original_root() {
    let pipeline = durable_chain();
    // run-A resumed run-ROOT; now we resume run-A. Parent = run-A, root = run-ROOT.
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[],
        None,
        json!({ "parent_run_id": "run-ROOT", "lineage_root_run_id": "run-ROOT" }),
        &[("produce", "succeeded", Some("produce/out")), ("consume", "failed", None)],
    );
    let artifact = run(&pipeline, &prior, opts("run-B")).expect_resumed();
    let lineage = &artifact["header"]["resume_lineage"];
    assert_eq!(lineage["parent_run_id"], json!("run-A"), "immediate parent is the prior resumed run");
    assert_eq!(
        lineage["lineage_root_run_id"],
        json!("run-ROOT"),
        "the lineage root stays the original run across generations"
    );
}

// ===========================================================================
// Full-success resume is a no-op that exits successfully.
// ===========================================================================

#[test]
fn full_success_resume_is_a_noop_success() {
    let pipeline = durable_chain();
    let prior = prior_artifact(
        &pipeline,
        "run-A",
        &[],
        None,
        Value::Null,
        &[("produce", "succeeded", Some("produce/out")), ("consume", "succeeded", None)],
    );
    let outcome = run(&pipeline, &prior, opts("run-B"));
    // A no-op resume completes successfully and re-runs nothing.
    let artifact = outcome.expect_resumed();
    for node in ["produce", "consume"] {
        assert_eq!(
            attempt_for(&artifact, node)["status"],
            json!("satisfied-from-prior"),
            "{node} is satisfied-from-prior on a full-success resume"
        );
    }
}

// === helpers ===============================================================

fn attempt_for<'a>(artifact: &'a Value, node: &str) -> &'a Value {
    artifact["attempts"]
        .as_array()
        .expect("attempts array")
        .iter()
        .find(|a| a["node"] == json!(node))
        .unwrap_or_else(|| panic!("attempt for {node} present"))
}

impl ResumeOutcome {
    fn expect_resumed(self) -> Value {
        match self {
            ResumeOutcome::Resumed { artifact, .. } => artifact,
            other => panic!("expected a resumed artifact, got {other:?}"),
        }
    }
}
