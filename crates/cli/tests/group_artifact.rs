//! C6 groups in the graph artifact — ticket T51 (063). Written first, TDD.
//!
//! The group label is *presentation metadata* recorded on the node in the C20
//! graph artifact (arch.md `### C6 · Group`; C20) so downstream tooling — the C24
//! renderer (which clusters by it, T46) and the C28 structure comparison — can
//! read it, while it stays out of every field that feeds node identity or either
//! C21 fingerprint hash. This suite asserts the T51-owned artifact facets against
//! the **real** emitter [`dagr_cli::graph`] over a **real** assembled pipeline:
//!
//! * each node's record carries its group label (a documented empty-string marker
//!   for an ungrouped node), and the artifact round-trips through serialization
//!   stably;
//! * a group **rename** is **review-visible** — it produces a byte-different graph
//!   artifact (the node records differ) — yet leaves **both** header fingerprints
//!   byte-identical. This is the fingerprint-neutral-but-review-visible property
//!   C6 owns; the full C28 structure-diff harness is T61's, and this ticket wires
//!   the group label through the existing artifact surface it consumes (Out of
//!   scope: redesigning the C28 harness).
//!
//! The renderer-clustering half of C6 (groups render as clusters in DOT/Mermaid,
//! accepted by the reference tools) landed with T46 and is covered by
//! `crates/render/tests/renderer.rs` and `reference_tools.rs` over a real emitted
//! grouped artifact; this suite covers the recording and fingerprint-neutrality
//! halves.

use dagr_cli::graph::{emit_graph, mask_generated_at, BuildProvenance};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};
use serde_json::Value;

// === Fixture value + task types (author-declared stable names) =============

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

struct MakeRows;
impl StableName for MakeRows {
    const STABLE_NAME: &'static str = "MakeRows";
}
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

struct MakeSchema;
impl StableName for MakeSchema {
    const STABLE_NAME: &'static str = "MakeSchema";
}
impl Task for MakeSchema {
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

// === Fixtures ==============================================================

/// A four-node fixture with **two groups and an ungrouped node**: `rows` +
/// `schema` in `ingest`, `report` in `publish`, and `loose` ungrouped. Two data
/// edges feed `report`. `group_of` supplies each node's group so a rename variant
/// reuses the exact same shape.
fn fixture(group_of: impl Fn(&str) -> Option<&'static str>) -> Pipeline {
    let mut flow = Flow::new();
    let rows =
        flow.register_source_named::<MakeRows>("rows", &MakeRows, group_of("rows"), NodePolicy::new());
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        group_of("schema"),
        NodePolicy::new(),
    );
    // An ungrouped source — records the documented empty-string group marker.
    let _loose = flow.register_source_named::<MakeSchema>(
        "loose",
        &MakeSchema,
        group_of("loose"),
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (rows, schema),
        group_of("report"),
        NodePolicy::new(),
    );
    flow.finish()
}

fn base_groups(name: &str) -> Option<&'static str> {
    match name {
        "rows" | "schema" => Some("ingest"),
        "report" => Some("publish"),
        _ => None, // `loose` is ungrouped
    }
}

/// The same shape with `ingest` renamed to `landing` (members unchanged).
fn renamed_groups(name: &str) -> Option<&'static str> {
    match name {
        "rows" | "schema" => Some("landing"),
        "report" => Some("publish"),
        _ => None,
    }
}

fn test_provenance() -> BuildProvenance {
    BuildProvenance::new(
        "0.0.0",
        "0123456789abcdef0123456789abcdef01234567",
        "fnv1a-64:0011223344556677",
    )
}

const GEN: &str = "2026-07-24T00:00:00Z";

fn emit(pipeline: &Pipeline, generated_at: &str) -> String {
    emit_graph(pipeline, "grouped-pipeline", generated_at, &test_provenance())
        .expect("fixture pipeline emits")
}

fn parse(json: &str) -> Value {
    serde_json::from_str(json).expect("emitted artifact is valid JSON")
}

/// The `group` field of the node named `name` in a parsed artifact.
fn group_of_node(artifact: &Value, name: &str) -> String {
    artifact["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .find(|n| n["name"] == Value::from(name))
        .unwrap_or_else(|| panic!("node `{name}` present"))["group"]
        .as_str()
        .expect("group is a string")
        .to_string()
}

// === Tests =================================================================

/// **Group appears in the graph artifact.** Each node's record carries its group
/// label, and an ungrouped node records the documented empty-string marker
/// (arch.md C6; C20 "each node's record carries its group label or a documented
/// none marker for ungrouped nodes").
#[test]
fn each_node_record_carries_its_group_label() {
    let artifact = parse(&emit(&fixture(base_groups), GEN));

    assert_eq!(group_of_node(&artifact, "rows"), "ingest");
    assert_eq!(group_of_node(&artifact, "schema"), "ingest");
    assert_eq!(group_of_node(&artifact, "report"), "publish");
    // The ungrouped node records the documented "none" marker (the empty string).
    assert_eq!(
        group_of_node(&artifact, "loose"),
        "",
        "an ungrouped node records the documented empty-string group marker"
    );

    // Two distinct groups plus an ungrouped node are present (the C6 clustering
    // fixture shape the renderer consumes).
    let groups: std::collections::BTreeSet<String> = artifact["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["group"].as_str().unwrap().to_string())
        .collect();
    assert!(groups.contains("ingest") && groups.contains("publish") && groups.contains(""));
}

/// **The artifact round-trips through serialization stably.** Re-emitting the
/// same pipeline with the same clock is byte-identical, and parsing then
/// re-serializing the artifact (with generation time masked) is stable — the
/// group label participates in the deterministic canonical encoding (C20).
#[test]
fn grouped_artifact_round_trips_stably() {
    let first = emit(&fixture(base_groups), GEN);
    let second = emit(&fixture(base_groups), GEN);
    assert_eq!(
        first, second,
        "two emissions with the same clock are byte-identical, including group labels"
    );

    // Parse → re-serialize (mask generation time) is a stable identity: the parsed
    // value canonically re-serializes to the same masked bytes.
    let parsed = parse(&first);
    let masked = mask_generated_at(parsed.clone());
    let masked_again = mask_generated_at(parse(&second));
    assert_eq!(
        masked, masked_again,
        "the grouped artifact round-trips through serialization stably"
    );
}

/// **Group rename is review-visible in the artifact yet fingerprint-neutral.**
/// Renaming a group produces a byte-**different** graph artifact (the affected
/// node records carry the new label) — the change is visible to a reviewer /
/// structure comparison — while **both** header fingerprints stay byte-identical.
/// This is C6's load-bearing property: a rename shows up in the structure diff
/// (C28) but never breaks resume (C21/C27). The full C28 diff harness is T61's;
/// T51 wires the label through this existing artifact surface.
#[test]
fn group_rename_is_review_visible_but_fingerprint_neutral() {
    let base = parse(&emit(&fixture(base_groups), GEN));
    let renamed = parse(&emit(&fixture(renamed_groups), GEN));

    // The node records DIFFER — the rename is review-visible in the artifact.
    assert_ne!(
        mask_generated_at(base.clone()),
        mask_generated_at(renamed.clone()),
        "a group rename must be visible in the graph artifact (review-visible)"
    );
    assert_eq!(group_of_node(&base, "rows"), "ingest");
    assert_eq!(group_of_node(&renamed, "rows"), "landing");

    // Yet BOTH header fingerprints are byte-identical — the rename never moves a
    // hash, so resume is never broken by a regrouping (C21 line 465 / C27).
    for field in [
        "fingerprint_structural",
        "fingerprint_policy",
        "fingerprint_algorithm_version",
    ] {
        assert_eq!(
            base["header"][field], renamed["header"][field],
            "a group rename must not change the `{field}` header field"
        );
    }
}

/// **Removing every group is review-visible yet fingerprint-neutral, and the
/// dependency structure is unchanged.** Dropping all group labels yields a
/// byte-different artifact (every affected node now records the empty marker)
/// while both fingerprints and every node's recorded dependency list are
/// identical — grouping is presentation only and never re-partitions the graph.
#[test]
fn removing_groups_keeps_fingerprints_and_dependencies() {
    let grouped = parse(&emit(&fixture(base_groups), GEN));
    let bare = parse(&emit(&fixture(|_| None::<&'static str>), GEN));

    // The `rows` record's group changed (review-visible) …
    assert_eq!(group_of_node(&grouped, "rows"), "ingest");
    assert_eq!(group_of_node(&bare, "rows"), "");

    // … yet both fingerprints are unchanged …
    for field in ["fingerprint_structural", "fingerprint_policy"] {
        assert_eq!(
            grouped["header"][field], bare["header"][field],
            "removing groups must not change `{field}`"
        );
    }

    // … and every node's recorded dependency list is identical (grouping never
    // changes wiring).
    let deps = |artifact: &Value, name: &str| -> Value {
        artifact["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["name"] == Value::from(name))
            .unwrap()["dependencies"]
            .clone()
    };
    for name in ["rows", "schema", "loose", "report"] {
        assert_eq!(
            deps(&grouped, name),
            deps(&bare, name),
            "dependency list of `{name}` must be independent of grouping"
        );
    }
}
