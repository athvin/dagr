//! C22 · Run-artifact **fold** — fingerprint-joins-to-graph-artifact test
//! (ticket T42 / 053). Written first, TDD.
//!
//! The load-bearing join criterion: *"The run artifact's structural fingerprint
//! equals the graph artifact's."* This test produces a graph artifact from a
//! build fixture (via the T40 `emit_graph`), extracts its structural
//! fingerprint, builds an event stream whose run-started header carries that
//! same fingerprint, folds the stream into a run artifact, and asserts the two
//! structural fingerprints are equal — the artifacts join on the same build.
//!
//! It lives in the CLI crate because that is where the graph emitter and a real
//! assembled pipeline live; the fold itself is dependency-light and in
//! `dagr-artifact`.

use dagr_artifact::fold::fold_stream;
use dagr_cli::graph::{emit_graph, BuildProvenance};
use dagr_core::stable_name::StableName;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};
use serde_json::{json, Value};

struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

struct LoadRows;
impl StableName for LoadRows {
    const STABLE_NAME: &'static str = "load-rows-task";
}
impl Task for LoadRows {
    type Input = ();
    type Output = Rows;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

fn build_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    let _load = flow.register_source_named::<LoadRows>(
        "load",
        &LoadRows,
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

#[test]
fn run_artifact_structural_fingerprint_matches_graph_artifact() {
    let pipeline = build_pipeline();
    let provenance = BuildProvenance::embedded();
    let graph_json = emit_graph(
        &pipeline,
        "example-pipeline",
        "2026-07-23T00:00:00Z",
        &provenance,
    )
    .expect("emit graph");
    let graph: Value = serde_json::from_str(&graph_json).expect("graph is JSON");
    let graph_fp = graph["header"]["fingerprint_structural"]
        .as_str()
        .expect("graph carries a structural fingerprint")
        .to_string();

    // Build a stream whose run-started header carries the SAME fingerprint the
    // graph emitter computed (both derive from the same build fixture).
    let header = json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": graph_fp,
        "fingerprint_policy": graph["header"]["fingerprint_policy"],
        "fingerprint_algorithm_version": graph["header"]["fingerprint_algorithm_version"],
        "parameters": {},
        "data_interval": null,
        "captured_environment": {},
        "resume_lineage": null,
    });
    let mut out = String::new();
    for r in [
        json!({
            "schema_version": "dagr.event-stream@1",
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "seq": 0, "wall": "2026-07-23T00:00:00.000Z", "offset_ns": 0,
            "kind": "run-started", "header": header,
        }),
        json!({
            "schema_version": "dagr.event-stream@1",
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "seq": 1, "wall": "2026-07-23T00:00:00.100Z", "offset_ns": 100,
            "kind": "attempt-outcome", "node": "load", "attempt": 1, "status": "succeeded",
        }),
        json!({
            "schema_version": "dagr.event-stream@1",
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "seq": 2, "wall": "2026-07-23T00:00:00.100Z", "offset_ns": 100,
            "kind": "node-terminal", "node": "load", "state": "succeeded",
        }),
        json!({
            "schema_version": "dagr.event-stream@1",
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "seq": 3, "wall": "2026-07-23T00:00:00.100Z", "offset_ns": 100,
            "kind": "run-finished", "outcome": "succeeded",
        }),
    ] {
        out.push_str(&serde_json::to_string(&r).unwrap());
        out.push('\n');
    }

    let run_art = fold_stream(out.as_bytes(), &["load".to_string()]).expect("fold");
    let run_fp = run_art
        .header_fingerprint_structural()
        .expect("folded run artifact carries a structural fingerprint");

    assert_eq!(
        run_fp,
        graph["header"]["fingerprint_structural"].as_str().unwrap(),
        "the run artifact's structural fingerprint equals the graph artifact's"
    );
}
