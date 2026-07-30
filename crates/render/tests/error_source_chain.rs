//! `RenderError` carries its real cause. Written first, TDD.
//!
//! `GraphArtifact::from_json_str` stringified its `serde_json::Error` at
//! construction (`.map_err(|e| RenderError::Malformed(e.to_string()))`), so the
//! deserializer's own error — the one carrying the offending **line and column** —
//! could not be recovered by any caller, even in principle. The renderer is a
//! read path; that line/column is precisely what an operator needs to fix a
//! hand-edited or truncated artifact.

use std::error::Error;

use dagr_render::GraphArtifact;

/// **The chain reaches the deserializer, with line and column.** A malformed
/// artifact must expose its `serde_json::Error` through `source()`, and that error
/// must report where in the document the problem is.
#[test]
fn render_error_exposes_the_deserializer_error_with_line_and_column() {
    // Valid JSON up to the third line, then a bare word where a value belongs.
    let malformed = "{\n  \"schema_version\": 1,\n  \"nodes\": oops\n}\n";
    let err = GraphArtifact::from_json_str(malformed)
        .expect_err("a document that is not valid JSON is refused");

    let source = err
        .source()
        .expect("RenderError::Malformed wraps a real deserializer error; source() must expose it");
    let json_err = source
        .downcast_ref::<serde_json::Error>()
        .expect("the cause must be the serde_json::Error itself, not a stringified copy");
    assert_eq!(
        json_err.line(),
        3,
        "the deserializer's line survives the wrap (that is why the error is carried, not \
         stringified)"
    );
    assert!(
        json_err.column() > 0,
        "the deserializer's column survives the wrap"
    );
}

/// **A schema-shaped mismatch keeps its naming.** The cause is carried for a
/// missing required field too, and the outer `Display` still frames the failure
/// against the published schema.
#[test]
fn a_missing_required_field_still_names_the_field_and_the_schema() {
    // A node object missing its required `output_type_name`.
    let missing_field = r#"{"schema_version":1,"nodes":[{"name":"a"}],"edges":[]}"#;
    let err = GraphArtifact::from_json_str(missing_field)
        .expect_err("a schema-invalid artifact is refused");

    assert!(
        err.to_string().contains("schemas/graph/v1.schema.json"),
        "the rendered diagnostic still references the published schema: {err}"
    );
    let source = err.source().expect("the deserializer error is carried");
    assert!(
        source.to_string().contains("output_type_name")
            || err.to_string().contains("output_type_name"),
        "the diagnostic still names the offending field: {err} / {source}"
    );
}

/// **Well-formed input is unaffected.** Carrying the cause must not change what
/// parses — the existing thirty-node fixture still round-trips.
#[test]
fn a_wellformed_artifact_still_parses() {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/thirty-node.graph.json"
    ))
    .expect("the thirty-node fixture is readable");
    assert!(
        GraphArtifact::from_json_str(&raw).is_ok(),
        "a well-formed artifact still parses"
    );
}
