//! C21 · Graph fingerprint in the graph artifact — ticket T41 (052). Written
//! first, TDD.
//!
//! These prove the two computed hashes (arch.md `### C21 · Graph fingerprint`;
//! T0.7 §7) are wired into the **graph artifact header** (T40, `dagr_cli::graph`)
//! in place of the reserved placeholder slots, that they equal the values
//! computed directly from the assembled `Pipeline`, that the algorithm version is
//! present and stable, and that **environmental inputs** (generation time, build
//! provenance) do not feed either hash. The structural change/no-change matrix
//! and the stable-name coverage live in the core suite
//! (`crates/core/tests/fingerprint.rs`); this suite is the artifact-surface half.

use dagr_cli::graph::{
    emit_graph, format_fingerprint_policy, format_fingerprint_structural, BuildProvenance,
};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError, FINGERPRINT_ALGORITHM_VERSION};
use serde_json::Value;

// === Fixture value + task types (author-declared stable names) =============

struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
struct Report;
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

struct MakeRows;
impl StableName for MakeRows {
    const STABLE_NAME: &'static str = "make-rows";
}
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

struct BuildReport;
impl StableName for BuildReport {
    const STABLE_NAME: &'static str = "build-report";
}
impl Task for BuildReport {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

fn fixture_pipeline() -> Pipeline {
    let mut f = Flow::new();
    let rows = f.register_source_named("rows", &MakeRows, Some("ingest"), NodePolicy::new());
    let _ = f.register_named(
        "report",
        &BuildReport,
        rows,
        None::<String>,
        NodePolicy::new().retries(2),
    );
    f.finish()
}

fn provenance_a() -> BuildProvenance {
    BuildProvenance::new(
        "0.0.0",
        "0123456789abcdef0123456789abcdef01234567",
        "fnv1a-64:0011223344556677",
    )
}

// Provenance that differs in every environmental field.
fn provenance_b() -> BuildProvenance {
    BuildProvenance::new(
        "9.9.9",
        "ffffffffffffffffffffffffffffffffffffffff",
        "fnv1a-64:8899aabbccddeeff",
    )
}

const GEN_A: &str = "2026-07-23T00:00:00Z";
const GEN_B: &str = "2027-01-01T23:59:59Z";

fn parse(json: &str) -> Value {
    serde_json::from_str(json).expect("emitted artifact is valid JSON")
}

fn header(pipeline: &Pipeline, gen: &str, prov: &BuildProvenance) -> Value {
    let out = emit_graph(pipeline, "example-pipeline", gen, prov).expect("emits");
    parse(&out)["header"].clone()
}

// === Computed values appear in the header, not the reserved placeholders =====

/// The header carries the **computed** structural fingerprint, policy hash, and
/// algorithm version — equal to the values computed directly from the flow — and
/// no longer the `reserved-t41:*` placeholders (T0.7 §7).
#[test]
fn header_carries_the_computed_fingerprints_equal_to_the_flow() {
    let pipeline = fixture_pipeline();
    let h = header(&pipeline, GEN_A, &provenance_a());

    let slot = pipeline.fingerprint();
    let expected_structural = format_fingerprint_structural(&slot);
    let expected_policy = format_fingerprint_policy(&slot);

    assert_eq!(
        h["fingerprint_structural"],
        Value::from(expected_structural.clone()),
        "header structural fp equals the value computed from the flow"
    );
    assert_eq!(
        h["fingerprint_policy"],
        Value::from(expected_policy.clone()),
        "header policy hash equals the value computed from the flow"
    );
    assert_eq!(
        h["fingerprint_algorithm_version"].as_u64().unwrap(),
        FINGERPRINT_ALGORITHM_VERSION,
        "header carries the declared algorithm version"
    );

    // The reserved placeholders are gone.
    assert_ne!(h["fingerprint_structural"], Value::from("reserved-t41:structural"));
    assert_ne!(h["fingerprint_policy"], Value::from("reserved-t41:policy"));
    assert_ne!(
        h["fingerprint_structural"], h["fingerprint_policy"],
        "the two hashes are distinct fields with distinct values"
    );
}

/// The formatted fingerprint strings are **version-prefixed** so a version
/// mismatch is legible (T0.7 §7 / C21) — the algorithm version is embedded in the
/// string as well as in the dedicated integer field.
#[test]
fn fingerprint_strings_are_version_prefixed() {
    let pipeline = fixture_pipeline();
    let slot = pipeline.fingerprint();
    let structural = format_fingerprint_structural(&slot);
    let policy = format_fingerprint_policy(&slot);
    let prefix = format!("v{FINGERPRINT_ALGORITHM_VERSION}:");
    assert!(
        structural.contains(&prefix),
        "structural fp string `{structural}` carries the algorithm version"
    );
    assert!(
        policy.contains(&prefix),
        "policy hash string `{policy}` carries the algorithm version"
    );
    assert!(!structural.is_empty() && !policy.is_empty());
}

// === Environmental inputs are excluded from both hashes ======================

/// Emitting the same flow under **different generation times** and **different
/// build provenance** yields the **same** structural fingerprint and policy hash
/// in the header — timestamps, provenance, and generation time do not feed either
/// hash (T0.7 §5).
#[test]
fn environmental_inputs_do_not_change_the_fingerprints() {
    let pipeline = fixture_pipeline();
    let h1 = header(&pipeline, GEN_A, &provenance_a());
    let h2 = header(&pipeline, GEN_B, &provenance_b());

    // The environmental fields genuinely differ between the two emissions.
    assert_ne!(h1["generated_at"], h2["generated_at"]);
    assert_ne!(h1["build_provenance"], h2["build_provenance"]);

    // ...but the two fingerprints do not.
    assert_eq!(
        h1["fingerprint_structural"], h2["fingerprint_structural"],
        "structural fp excludes generation time and provenance"
    );
    assert_eq!(
        h1["fingerprint_policy"], h2["fingerprint_policy"],
        "policy hash excludes generation time and provenance"
    );
    assert_eq!(
        h1["fingerprint_algorithm_version"], h2["fingerprint_algorithm_version"],
        "algorithm version is fixed"
    );
}

/// The header fingerprints are schema-shaped: non-empty strings for the two
/// hashes and an integer >= 1 for the algorithm version (so the emitter still
/// validates against `schemas/graph/v1.schema.json`).
#[test]
fn header_fingerprint_fields_are_schema_shaped() {
    let pipeline = fixture_pipeline();
    let h = header(&pipeline, GEN_A, &provenance_a());
    assert!(h["fingerprint_structural"].as_str().unwrap().len() >= 1);
    assert!(h["fingerprint_policy"].as_str().unwrap().len() >= 1);
    assert!(h["fingerprint_algorithm_version"].as_u64().unwrap() >= 1);
}
