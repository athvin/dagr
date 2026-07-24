//! C24 · T46 — the 30-node render fixture is a **schema-valid** C20 graph
//! artifact. Written first, TDD.
//!
//! The renderer reads a graph artifact conforming to the published C20/T39
//! schema (arch.md C24; the T46 DoD). This proves the checked-in 30-node fixture
//! this ticket renders is itself schema-valid — it validates against
//! `schemas/graph/v1.schema.json` via the published T39 helper
//! (`dagr_artifact::schema::validate_value`) — and, for teeth, that a corrupted
//! copy (a required field removed) is rejected.
//!
//! This suite is gated behind the `schema-validation` feature (default OFF)
//! exactly like T39's own `artifact_schemas` and T40's `graph_artifact_schema_
//! roundtrip`: the `jsonschema` validator is CI-/dev-scoped (T4 ADR 017 §4), so
//! the shipped renderer and the bare `cargo test --workspace` never pull it. CI
//! runs it in a dedicated step.

#![cfg(feature = "schema-validation")]

use std::path::{Path, PathBuf};

use dagr_artifact::schema::{validate_value, ArtifactKind};
use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn load_value(name: &str) -> Value {
    let raw = std::fs::read_to_string(fixture(name)).expect("fixture readable");
    serde_json::from_str(&raw).expect("fixture is valid JSON")
}

/// The 30-node render fixture validates against the published graph schema v1.
#[test]
fn thirty_node_fixture_validates_against_the_published_graph_schema() {
    let value = load_value("thirty-node.graph.json");
    validate_value(ArtifactKind::Graph, 1, &value)
        .expect("the 30-node render fixture must validate against schemas/graph/v1.schema.json");
}

/// The stable-names fixture (informational `type_name` differing from the stable
/// declared name) is also schema-valid.
#[test]
fn stable_names_fixture_validates_against_the_published_graph_schema() {
    let value = load_value("stable-names.graph.json");
    validate_value(ArtifactKind::Graph, 1, &value)
        .expect("the stable-names render fixture must validate against the published schema");
}

/// Teeth: the deliberately-corrupted fixture (a node with the required
/// `output_type_name` removed) is REJECTED by the published schema, so the
/// reject-path test in `renderer.rs` is exercising a genuinely invalid artifact.
#[test]
fn the_schema_invalid_fixture_is_rejected_by_the_published_schema() {
    let value = load_value("schema-invalid.graph.json");
    let err = validate_value(ArtifactKind::Graph, 1, &value)
        .expect_err("the corrupted fixture must fail schema validation");
    // The failure names the missing required field.
    assert!(
        err.reason().contains("output_type_name") || err.reason().contains("required"),
        "the schema rejection must name the problem, got: {}",
        err.reason()
    );
}
