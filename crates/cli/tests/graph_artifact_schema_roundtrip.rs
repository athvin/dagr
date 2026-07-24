//! C20 · Graph artifact **schema round-trip** — ticket T40. Written first, TDD.
//!
//! The load-bearing interlock with T39 (ticket 050): a **real emitted** graph
//! artifact validates against the published `schemas/graph/v1.schema.json` via the
//! T39 validation helper (`dagr_artifact::schema`), and a deliberately-corrupted
//! copy is rejected — proving the check has teeth. That is the whole point of
//! emitting to a *published* contract.
//!
//! This suite is gated behind the `schema-validation` feature (default OFF), which
//! pulls the CI-/dev-scoped `jsonschema` validator (T4 ADR 017 §4). CI runs it
//! with the feature ON in a dedicated step (mirroring T39's); the shipped binary
//! and the bare `cargo test --workspace` never activate it, so `cargo deny` never
//! sees `jsonschema`'s permissive-but-unlisted transitive licences.

#![cfg(feature = "schema-validation")]

use dagr_artifact::schema::{validate_value, ArtifactKind};
use dagr_cli::graph::{emit_graph, graph_verb, BuildProvenance, GRAPH_SCHEMA_MAJOR};
use dagr_core::stable_name::StableName;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};
use serde_json::Value;

struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
struct Schema;
impl StableName for Schema {
    const STABLE_NAME: &'static str = "Schema";
}
struct Report;
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
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
struct LoadSchema;
impl StableName for LoadSchema {
    const STABLE_NAME: &'static str = "LoadSchema";
}
impl Task for LoadSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}
struct BuildReport;
impl StableName for BuildReport {
    const STABLE_NAME: &'static str = "BuildReport";
}
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

fn fixture_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<LoadRows>(
        "load",
        &LoadRows,
        Some("ingest"),
        NodePolicy::new()
            .retries(2)
            .working_memory(4096)
            .output_residency(1024)
            .compute_threads(3),
    );
    let schema = flow.register_source_named::<LoadSchema>(
        "schema",
        &LoadSchema,
        None::<String>,
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (rows, schema),
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

fn provenance() -> BuildProvenance {
    BuildProvenance::new(
        "0.0.0",
        "0123456789abcdef0123456789abcdef01234567",
        "fnv1a-64:0011223344556677",
    )
}

fn emitted() -> Value {
    let out = emit_graph(
        &fixture_pipeline(),
        "example-pipeline",
        "2026-07-23T00:00:00Z",
        &provenance(),
    )
    .expect("fixture emits");
    serde_json::from_str(&out).expect("valid JSON")
}

/// **Schema validation (C20).** A real emitted artifact validates cleanly against
/// the published T39 graph schema via the validation helper; a deliberately
/// corrupted copy (a required field removed) fails — proving the check has teeth.
#[test]
fn emitted_artifact_validates_against_the_published_schema() {
    let artifact = emitted();

    validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &artifact)
        .expect("a real emitted artifact validates against schemas/graph/v1.schema.json");

    // Corrupt it: drop a required node field. Validation must now reject it.
    let mut corrupt = artifact.clone();
    corrupt["nodes"][0]
        .as_object_mut()
        .unwrap()
        .remove("output_type_name");
    assert!(
        validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &corrupt).is_err(),
        "a corrupted artifact (missing required field) is rejected — the check has teeth"
    );

    // Corrupt the header: drop build provenance. Rejected.
    let mut corrupt = artifact.clone();
    corrupt["header"]
        .as_object_mut()
        .unwrap()
        .remove("build_provenance");
    assert!(
        validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &corrupt).is_err(),
        "an artifact missing build provenance is rejected"
    );

    // Corrupt a data edge: drop its carried type name. Rejected (the schema
    // requires `type_name` on a data edge).
    let mut corrupt = artifact.clone();
    corrupt["edges"][0]
        .as_object_mut()
        .unwrap()
        .remove("type_name");
    assert!(
        validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &corrupt).is_err(),
        "a data edge missing its carried type name is rejected"
    );
}

/// **The graph verb's output validates against the published schema (C20 / C26).**
/// What the verb writes to a sink is itself a schema-valid artifact.
#[test]
fn graph_verb_output_validates_against_the_published_schema() {
    let mut buf: Vec<u8> = Vec::new();
    graph_verb(
        &fixture_pipeline(),
        "example-pipeline",
        "2026-07-23T00:00:00Z",
        &mut buf,
    )
    .expect("graph verb emits");
    let text = String::from_utf8(buf).unwrap();
    let artifact: Value = serde_json::from_str(text.trim_end()).unwrap();
    validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &artifact)
        .expect("the graph verb's output validates against the published schema");
}
