//! C6 groups — the headline group-model tests, ticket T51 (063). Written first,
//! TDD.
//!
//! These exercise the **real** C6 group model (arch.md `### C6 · Group`) on top of
//! the T13 registration/builder surface (`dagr_core::flow`) and the T14/T29
//! assembly pass (`dagr_core::assembly`). A group label is *presentation metadata
//! only*: it is a flat label attached to a node (groups do **not** nest,
//! arch.md line 170), it is **excluded from node identity** and from **both**
//! graph-fingerprint hashes (C21, arch.md line 456), and it carries **no**
//! execution semantics. Removing or renaming every group changes no execution
//! behaviour (execution order, consumer/dependency counts) and neither hash.
//!
//! The dependency tickets already assert the pieces T51 sits on: the group slot
//! and its exclusion from identity (T13, `flow_builder.rs`), the single-node
//! neither-hash exclusion (T29, `node_policy.rs::group_is_in_neither_hash`), and
//! the diagram-clustering facet (T46, `render` crate). This suite adds the
//! *headline* C6 acceptance the coverage matrix defers to T51: whole-pipeline
//! fingerprint neutrality, removal-changes-no-behaviour, name uniqueness across
//! grouping (a duplicate name in different groups is an assembly error naming both
//! declarations), and reorder-stability under grouping.

use dagr_core::assembly::ProblemKind;
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::task::{RunContext, Task};
use dagr_core::TaskError;

// --- Illustrative value + task types ----------------------------------------
struct Rows;
struct Schema;
struct Report;

/// A sourceless task producing `Rows`.
struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

/// A sourceless task producing `Schema`.
struct MakeSchema;
impl Task for MakeSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}

/// A single-input consumer of `Rows`.
struct CountRows;
impl Task for CountRows {
    type Input = Rows;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<u64, TaskError> {
        Ok(0)
    }
}

/// A two-input join over `(Rows, Schema)`.
struct BuildReport;
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// The group label to attach to each node of the shared fixture, keyed by name;
/// `None` builds the same pipeline ungrouped. A small diamond exercising
/// multi-node grouping, a fan-out, and an ungrouped-vs-grouped contrast.
///
/// Diamond:  rows ─┐            (rows: group "ingest")
///                 ├─> report   (schema: group "ingest")
///        schema ─┘            (report: group "publish")
///        rows ────> count      (count: group "publish")
fn build_diamond(group_of: impl Fn(&str) -> Option<&'static str>) -> dagr_core::flow::Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_in_group("rows", &MakeRows, group_of("rows"));
    let schema = flow.register_source_in_group("schema", &MakeSchema, group_of("schema"));
    let _report = flow.register_in_group(
        "report",
        &BuildReport,
        (rows.shared(), schema),
        group_of("report"),
    );
    let _count = flow.register_in_group("count", &CountRows, rows.shared(), group_of("count"));
    flow.finish()
}

/// Group assignment for the fully-grouped variant.
fn grouped(name: &str) -> Option<&'static str> {
    match name {
        "rows" | "schema" => Some("ingest"),
        "report" | "count" => Some("publish"),
        _ => None,
    }
}

/// Group assignment where every group is *renamed* (fresh labels, same members).
fn renamed(name: &str) -> Option<&'static str> {
    match name {
        "rows" | "schema" => Some("landing"),
        "report" | "count" => Some("outputs"),
        _ => None,
    }
}

/// The all-ungrouped variant of the same pipeline.
fn ungrouped(_name: &str) -> Option<&'static str> {
    None
}

// ---------------------------------------------------------------------------
// Group excluded from the fingerprint (whole pipeline, grouped vs ungrouped).
// ---------------------------------------------------------------------------

/// **Group excluded from the fingerprint.** Two otherwise-identical pipelines —
/// one fully ungrouped, one where every node carries a group label — produce
/// byte-for-byte-equal structural fingerprints AND policy hashes (arch.md C6
/// line 456; C21). Grouping is presentation metadata in neither hash.
#[test]
fn group_is_excluded_from_both_fingerprint_hashes() {
    let bare = build_diamond(ungrouped).fingerprint();
    let grouped_fp = build_diamond(grouped).fingerprint();

    assert_eq!(
        bare.structural(),
        grouped_fp.structural(),
        "a group label must not move the structural fingerprint"
    );
    assert_eq!(
        bare.policy(),
        grouped_fp.policy(),
        "a group label must not move the policy hash"
    );
    assert_eq!(
        bare.algorithm_version(),
        grouped_fp.algorithm_version(),
        "same algorithm version"
    );
}

// ---------------------------------------------------------------------------
// Renaming every group changes no fingerprint.
// ---------------------------------------------------------------------------

/// **Rename changes no fingerprint.** Capture a grouped pipeline's fingerprint,
/// then rename every group label (same members, fresh labels) and reassemble:
/// both hashes are unchanged from the captured values (arch.md C6; C21 line 465
/// *"A group rename changes neither hash"*).
#[test]
fn renaming_every_group_changes_neither_hash() {
    let before = build_diamond(grouped).fingerprint();
    let after = build_diamond(renamed).fingerprint();

    assert_eq!(
        before.structural(),
        after.structural(),
        "a group rename must not move the structural fingerprint"
    );
    assert_eq!(
        before.policy(),
        after.policy(),
        "a group rename must not move the policy hash"
    );
}

// ---------------------------------------------------------------------------
// Removing every group changes no fingerprint AND no behaviour.
// ---------------------------------------------------------------------------

/// **Removal changes no fingerprint and no behaviour.** Remove all group labels
/// (leave every node ungrouped) and reassemble: both fingerprint hashes are
/// unchanged, and the precomputed execution order and consumer/dependency counts
/// are identical to the grouped version (arch.md C6 *"Removing … every group
/// changes no execution behaviour and no fingerprint"*). Groups are strictly
/// presentation — they never touch scheduling or readiness.
#[test]
fn removing_every_group_changes_no_fingerprint_and_no_behaviour() {
    let grouped_art = build_diamond(grouped).assemble().expect("grouped assembles");
    let bare_art = build_diamond(ungrouped)
        .assemble()
        .expect("ungrouped assembles");

    // Neither hash moved.
    assert_eq!(
        grouped_art.fingerprint().structural(),
        bare_art.fingerprint().structural(),
        "removing groups must not move the structural fingerprint"
    );
    assert_eq!(
        grouped_art.fingerprint().policy(),
        bare_art.fingerprint().policy(),
        "removing groups must not move the policy hash"
    );

    // Execution order is identical (precomputed topological order, C11).
    assert_eq!(
        grouped_art.execution_order(),
        bare_art.execution_order(),
        "removing groups must not change the execution order"
    );

    // Consumer and remaining-dependency counts are identical per node (C10/C11):
    // grouping never re-partitions the dependency graph.
    for name in ["rows", "schema", "report", "count"] {
        let id = NodeId::from_name(name);
        assert_eq!(
            grouped_art.consumer_count(id),
            bare_art.consumer_count(id),
            "consumer count of `{name}` must be independent of grouping"
        );
        assert_eq!(
            grouped_art.remaining_dependency_count(id),
            bare_art.remaining_dependency_count(id),
            "remaining-dependency count of `{name}` must be independent of grouping"
        );
    }
    // Sanity: the fixture actually has a fan-out (rows feeds report + count) so
    // the count comparison above is non-vacuous.
    assert_eq!(
        grouped_art.consumer_count(NodeId::from_name("rows")),
        Some(2),
        "fixture must exercise a real fan-out"
    );
}

// ---------------------------------------------------------------------------
// Group is not part of node identity: a duplicate name in DIFFERENT groups is an
// assembly error naming both declarations (no per-group name namespacing).
// ---------------------------------------------------------------------------

/// **Group is not part of node identity.** Registering a node with a given name
/// inside one group and a second node with the *same* name in a *different* group
/// in the same builder is a duplicate-node-name **assembly error** — names are
/// unique across the whole pipeline regardless of grouping (arch.md C6 *"Node
/// names are unique across the whole pipeline regardless of grouping"*). The
/// report names the duplicated name and states that both declarations collided.
#[test]
fn same_name_in_different_groups_is_a_duplicate_assembly_error() {
    let mut flow = Flow::new();
    // Same identity name `dup`, two different groups. The group is presentation
    // metadata, not a namespace, so these collide.
    let _ = flow.register_source_in_group("dup", &MakeRows, Some("ingest"));
    let _ = flow.register_source_in_group("dup", &MakeSchema, Some("staging"));
    // A distinct node so the pipeline is otherwise well-formed.
    let _ = flow.register_source_in_group("other", &MakeRows, Some("ingest"));

    let err = flow
        .finish()
        .assemble()
        .expect_err("a duplicate name across groups must fail assembly");

    let dups: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::DuplicateNodeName)
        .collect();
    assert_eq!(
        dups.len(),
        1,
        "exactly one duplicate-name problem for the collided name"
    );
    let msg = dups[0].message();
    assert!(
        msg.contains("dup"),
        "the report must name the duplicated node: {msg}"
    );
    // Both declarations collided (the "names both declarations" C7/C6 contract).
    assert_eq!(
        dups[0].declaration_count(),
        Some(2),
        "the report must state that both declarations collided"
    );
}

// ---------------------------------------------------------------------------
// Reorder-stability holds with groups: identities and both hashes are identical
// across two declaration orders of the same grouped nodes.
// ---------------------------------------------------------------------------

/// **Reorder-stability holds with groups.** Two builders register the *same*
/// grouped nodes in different declaration orders. Assembling both yields identical
/// node identities and identical structural fingerprints and policy hashes —
/// grouping does not reintroduce order sensitivity (arch.md C6; the C7/C21
/// registration-order-independence guarantee holds under grouping).
#[test]
fn reorder_stability_holds_with_groups() {
    // Order A: rows, schema, report, count (the fixture's natural order).
    let pipe_a = build_diamond(grouped);

    // Order B: register the two grouped sources in the opposite order; the
    // downstream bindings are otherwise identical. Declaration order differs; the
    // graph is the same.
    let pipe_b = {
        let mut flow = Flow::new();
        let schema = flow.register_source_in_group("schema", &MakeSchema, grouped("schema"));
        let rows = flow.register_source_in_group("rows", &MakeRows, grouped("rows"));
        let _count = flow.register_in_group("count", &CountRows, rows.shared(), grouped("count"));
        let _report = flow.register_in_group(
            "report",
            &BuildReport,
            (rows.shared(), schema),
            grouped("report"),
        );
        flow.finish()
    };

    // Node identities are order-independent (name-derived, group-independent).
    for name in ["rows", "schema", "report", "count"] {
        let id = NodeId::from_name(name);
        assert!(pipe_a.node(id).is_some(), "order A carries `{name}`");
        assert!(pipe_b.node(id).is_some(), "order B carries `{name}`");
        assert_eq!(
            pipe_a.node(id).unwrap().id(),
            pipe_b.node(id).unwrap().id(),
            "identity of `{name}` is order-independent"
        );
    }

    // Both hashes are identical across the two declaration orders.
    let fp_a = pipe_a.fingerprint();
    let fp_b = pipe_b.fingerprint();
    assert_eq!(
        fp_a.structural(),
        fp_b.structural(),
        "grouping must not reintroduce structural order sensitivity"
    );
    assert_eq!(
        fp_a.policy(),
        fp_b.policy(),
        "grouping must not reintroduce policy-hash order sensitivity"
    );

    // And the canonical byte forms match — the full order-independence guarantee.
    let art_a = pipe_a.assemble().expect("order A assembles");
    let art_b = pipe_b.assemble().expect("order B assembles");
    assert_eq!(
        art_a.canonical_bytes(),
        art_b.canonical_bytes(),
        "canonical byte form is registration-order-independent under grouping"
    );
}
