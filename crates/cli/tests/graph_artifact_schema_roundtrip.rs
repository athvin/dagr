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

/// A sourceless effect-only task with a stable name — the ordering-edge upstream.
struct Publish;
impl StableName for Publish {
    const STABLE_NAME: &'static str = "publish-task";
}
impl Task for Publish {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// A pipeline carrying both a data edge and an ordering edge (T50 / C4).
fn ordering_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<LoadRows>(
        "load",
        &LoadRows,
        None::<String>,
        NodePolicy::new(),
    );
    let publish = flow.register_source_named::<Publish>(
        "publish",
        &Publish,
        None::<String>,
        NodePolicy::new(),
    );
    let _report = flow.register_named_ordered_after::<BuildReportOne, _>(
        "report",
        &BuildReportOne,
        rows,
        &[],
        None::<String>,
        NodePolicy::new(),
    );
    let _cleanup = flow.register_source_named_ordered_after::<Publish>(
        "cleanup",
        &Publish,
        &[publish.ordering()],
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

/// A single-input `Rows -> Report` task for the ordering-edge fixture's data edge.
struct BuildReportOne;
impl StableName for BuildReportOne {
    const STABLE_NAME: &'static str = "BuildReportOne";
}
impl Task for BuildReportOne {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// **A real ordering-edge graph validates against the published schema (C20 / C4).**
/// The emitted artifact — carrying a `data` edge with a `type_name` and an
/// `ordering` edge without one — validates cleanly against
/// `schemas/graph/v1.schema.json`, and a corrupted copy is rejected.
#[test]
fn ordering_edge_artifact_validates_against_the_published_schema() {
    let out = emit_graph(
        &ordering_pipeline(),
        "ordering-pipeline",
        "2026-07-23T00:00:00Z",
        &provenance(),
    )
    .expect("ordering fixture emits");
    let artifact: Value = serde_json::from_str(&out).expect("valid JSON");

    validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &artifact)
        .expect("a real ordering-edge artifact validates against the published schema");

    // The ordering edge is present and carries no type_name — still schema-valid.
    let ordering = artifact["edges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "ordering")
        .expect("ordering edge present");
    assert!(ordering.get("type_name").is_none());

    // Corrupt the ordering edge: drop its `kind`. Rejected (kind is required).
    let mut corrupt = artifact.clone();
    let edges = corrupt["edges"].as_array_mut().unwrap();
    let idx = edges.iter().position(|e| e["kind"] == "ordering").unwrap();
    edges[idx].as_object_mut().unwrap().remove("kind");
    assert!(
        validate_value(ArtifactKind::Graph, GRAPH_SCHEMA_MAJOR, &corrupt).is_err(),
        "an edge missing its kind is rejected"
    );
}

/// **A real emitted ordering-edge graph renders distinctly in both formats (C24).**
/// Feeding a real emitted artifact (with one data and one ordering edge) through
/// the renderer produces DOT and Mermaid in which the two edge kinds carry disjoint
/// documented styling — the data edge solid/labelled, the ordering edge dashed.
#[test]
fn ordering_edge_artifact_renders_distinctly() {
    let out = emit_graph(
        &ordering_pipeline(),
        "ordering-pipeline",
        "2026-07-23T00:00:00Z",
        &provenance(),
    )
    .expect("ordering fixture emits");
    let art = dagr_render::GraphArtifact::from_json_str(&out)
        .expect("the emitted artifact parses for the renderer");

    let dot = dagr_render::render_dot(&art);
    // The data edge (load -> report) is solid; the ordering edge (publish ->
    // cleanup) is dashed. Both endpoints appear.
    assert!(
        dot.lines()
            .any(|l| l.contains("\"load\" -> \"report\"") && l.contains("style=solid")),
        "the data edge renders solid in DOT"
    );
    assert!(
        dot.lines()
            .any(|l| l.contains("\"publish\" -> \"cleanup\"") && l.contains("style=dashed")),
        "the ordering edge renders dashed in DOT"
    );

    let mmd = dagr_render::render_mermaid(&art);
    // The ordering link is the dashed Mermaid form `-.->`; the data link solid.
    assert!(
        mmd.lines()
            .any(|l| l.contains("-.->") && l.contains("cleanup")),
        "the ordering edge renders as a dashed Mermaid link"
    );
    assert!(
        mmd.lines()
            .any(|l| l.contains("report") && l.contains("-->") && !l.contains("-.->")),
        "the data edge renders as a solid Mermaid link"
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
