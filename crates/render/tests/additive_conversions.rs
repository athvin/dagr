//! Additive-conversion tests for the two artifact readers — ticket 111 (T96),
//! written first, TDD.
//!
//! `conv-tryfrom-fallible` / `api-from-not-into`: a fallible parse from a string
//! is spelled `TryFrom<&str>` in Rust, so `?`, `.try_into()`, and generic code
//! bounded on `TryFrom` all reach it. The inherent `from_json_str` constructors
//! stay exactly as they are — the impls are **additive**, so no call site
//! changes — which makes drift the only real risk. Every test below therefore
//! runs **one fixture through both paths** and asserts they agree, on the
//! success path and on the rejection path.
//!
//! `GraphArtifact` and `RunArtifact` are read-only views with no `PartialEq`, so
//! agreement is asserted through what the renderer actually does with them: two
//! parses of one document must render byte-identically.

use std::path::Path;

use dagr_render::overlay::{RunArtifact, render_dot_overlay};
use dagr_render::{GraphArtifact, render_dot, render_mermaid};

fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn graph_artifact_try_from_agrees_with_the_inherent_reader() {
    let json = fixture("thirty-node.graph.json");

    let inherent = GraphArtifact::from_json_str(&json).expect("the fixture is a valid artifact");
    let converted = GraphArtifact::try_from(json.as_str()).expect("the same document, converted");

    assert_eq!(
        render_dot(&inherent),
        render_dot(&converted),
        "`TryFrom<&str>` and `from_json_str` parse one document into one artifact"
    );
    assert_eq!(render_mermaid(&inherent), render_mermaid(&converted));

    // `.try_into()` at a call site resolves to the same conversion.
    let inferred: GraphArtifact = json.as_str().try_into().expect("inferred conversion");
    assert_eq!(render_dot(&inherent), render_dot(&inferred));
}

#[test]
fn graph_artifact_try_from_rejects_exactly_what_the_inherent_reader_rejects() {
    let json = fixture("schema-invalid.graph.json");

    let inherent =
        GraphArtifact::from_json_str(&json).expect_err("the schema-invalid fixture is refused");
    let converted =
        GraphArtifact::try_from(json.as_str()).expect_err("the conversion refuses it identically");

    assert_eq!(
        inherent.to_string(),
        converted.to_string(),
        "both paths produce the same diagnostic — the conversion adds no second \
         error vocabulary"
    );
}

#[test]
fn run_artifact_try_from_agrees_with_the_inherent_reader() {
    let graph = GraphArtifact::from_json_str(&fixture("thirty-node.graph.json"))
        .expect("the graph fixture is valid");
    // A minimal, published-shape run artifact: one attempt over a node the graph
    // fixture declares.
    let json = r#"{
      "schema": "dagr.run@1",
      "run_id": "t96",
      "attempts": [{"node": "n00", "attempt": 1, "status": "succeeded",
                    "phase_durations_ns": {"executing": 1000}}]
    }"#;

    let inherent = RunArtifact::from_json_str(json).expect("the run fixture is valid");
    let converted = RunArtifact::try_from(json).expect("the same document, converted");

    assert_eq!(
        render_dot_overlay(&graph, &inherent),
        render_dot_overlay(&graph, &converted),
        "`TryFrom<&str>` and `from_json_str` parse one document into one overlay"
    );

    let inferred: RunArtifact = json.try_into().expect("inferred conversion");
    assert_eq!(
        render_dot_overlay(&graph, &inherent),
        render_dot_overlay(&graph, &inferred)
    );
}

#[test]
fn run_artifact_try_from_rejects_exactly_what_the_inherent_reader_rejects() {
    // An attempt missing its required `status` field.
    let json = r#"{"schema":"dagr.run@1","attempts":[{"node":"n00"}]}"#;

    let inherent =
        RunArtifact::from_json_str(json).expect_err("a malformed run artifact is refused");
    let converted = RunArtifact::try_from(json).expect_err("the conversion refuses it identically");

    assert_eq!(
        inherent, converted,
        "both paths produce the same diagnostic string"
    );
}
