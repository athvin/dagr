//! C28 · **Structure-snapshot testing** — ticket T61 (074). Written first, TDD.
//!
//! The middle level of the C28 testing surface (arch.md `### C28 · Testing
//! surface`): a *shipped* library API that captures an assembled pipeline's
//! **structure** as a canonical, human-readable snapshot (built on the T40 graph
//! artifact / T41 fingerprint / T0.7 stable names) and asserts it against a
//! checked-in golden fixture, so unintended rewiring fails review rather than
//! production.
//!
//! These translate the T61 Test plan into executable tests against the **real**
//! structure-snapshot API [`dagr_cli::structure_snapshot`] over **real** assembled
//! [`Pipeline`]s built with the stable-name-aware registrars. Each test maps to
//! one arch.md C28 (and C21 / C6) acceptance criterion (see the per-test doc
//! comment). Every fixture is produced through the blessed update flow, never
//! hand-edited.

use std::path::PathBuf;

use dagr_cli::graph::BuildProvenance;
use dagr_cli::structure_snapshot::{
    assert_structure, bless_structure, StructureAssertError, StructureSnapshot,
};
use dagr_core::stable_name::StableName;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};

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
struct Digest;
impl StableName for Digest {
    const STABLE_NAME: &'static str = "Digest";
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

/// A task producing `Digest` from a `Report` — the extra node an add/remove test
/// wires in and out.
struct MakeDigest;
impl StableName for MakeDigest {
    const STABLE_NAME: &'static str = "MakeDigest";
}
impl Task for MakeDigest {
    type Input = Report;
    type Output = Digest;
    async fn run(&mut self, _c: &RunContext, _i: Report) -> Result<Digest, TaskError> {
        Ok(Digest)
    }
}

/// A `Rows`-typed producer that carries a **different** stable output type name
/// (used for the carried-type-change test): it produces `Schema` where the
/// baseline produced `Rows`.
struct MakeRowsAsSchema;
impl StableName for MakeRowsAsSchema {
    const STABLE_NAME: &'static str = "MakeRows"; // same TASK name as the baseline
}
impl Task for MakeRowsAsSchema {
    type Input = ();
    type Output = Schema; // DIFFERENT carried type on its outgoing edge
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}

/// A `report` variant consuming `(Schema, Schema)` so the carried-type-change
/// producer can wire into it with the node/edge endpoints otherwise identical.
struct BuildReportTwoSchema;
impl StableName for BuildReportTwoSchema {
    const STABLE_NAME: &'static str = "BuildReport"; // same TASK name
}
impl Task for BuildReportTwoSchema {
    type Input = (Schema, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Schema, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

// === Fixtures ==============================================================

fn test_provenance() -> BuildProvenance {
    BuildProvenance::new(
        "0.0.0",
        "0123456789abcdef0123456789abcdef01234567",
        "fnv1a-64:0011223344556677",
    )
}

/// The baseline three-node pipeline: two grouped sources (`rows`, `schema` in
/// `ingest`) feeding a two-input `report` node (in `publish`).
fn baseline(group_of: impl Fn(&str) -> Option<&'static str>) -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        group_of("rows"),
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        group_of("schema"),
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
        _ => None,
    }
}

/// The baseline, registered in a DIFFERENT order (report last still, but sources
/// swapped) — the snapshot must be independent of registration order.
fn baseline_reordered() -> Pipeline {
    let mut flow = Flow::new();
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (rows, schema),
        Some("publish"),
        NodePolicy::new(),
    );
    flow.finish()
}

/// Baseline + one extra node (`digest`) consuming `report` (with its edge).
fn added_node() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    let report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (rows, schema),
        Some("publish"),
        NodePolicy::new(),
    );
    let _digest = flow.register_named::<MakeDigest, _>(
        "digest",
        &MakeDigest,
        report,
        Some("publish"),
        NodePolicy::new(),
    );
    flow.finish()
}

/// Baseline with `report`'s node **renamed** to `summary`, wiring unchanged.
fn renamed_node() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReport, _>(
        "summary", // renamed node identity
        &BuildReport,
        (rows, schema),
        Some("publish"),
        NodePolicy::new(),
    );
    flow.finish()
}

/// Baseline with the `report` policy changed (retries bumped) — topology
/// unchanged, only an effective-policy field differs.
fn policy_changed() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (rows, schema),
        Some("publish"),
        NodePolicy::new().retries(3), // effective-policy change, topology unchanged
    );
    flow.finish()
}

/// Baseline with `report`'s group changed from `publish` to `landing` — a
/// regroup, no other change.
fn regrouped() -> Pipeline {
    baseline(|name| match name {
        "rows" | "schema" => Some("ingest"),
        "report" => Some("landing"), // moved out of `publish`
        _ => None,
    })
}

/// The carried-type-change variant: `rows` now produces `Schema` (not `Rows`),
/// and `report` consumes `(Schema, Schema)` — node identities and edge endpoints
/// unchanged, only the carried type on the `rows → report` edge differs.
fn carried_type_changed() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRowsAsSchema>(
        "rows",
        &MakeRowsAsSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReportTwoSchema, _>(
        "report",
        &BuildReportTwoSchema,
        (rows, schema),
        Some("publish"),
        NodePolicy::new(),
    );
    flow.finish()
}

/// A four-node pipeline with **two** `Rows` producers (`rows`, `rows_alt`) both
/// present, `schema`, and a `report` consuming `(Rows, Schema)`. `report`'s first
/// input is wired to whichever `Rows` producer `first_rows` names, so the golden and
/// the rewired variant share an **identical node set** — only one data edge's `from`
/// endpoint moves between the two existing producers (a pure rewire).
fn two_rows_producers(first_rows: &str) -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new(),
    );
    let rows_alt = flow.register_source_named::<MakeRows>(
        "rows_alt",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        Some("ingest"),
        NodePolicy::new(),
    );
    // Redirect `report`'s first (Rows) input to the chosen existing producer; the
    // node SET is unchanged across the two choices — only the edge endpoint moves.
    let chosen_rows = match first_rows {
        "rows" => rows,
        "rows_alt" => rows_alt,
        other => panic!("unknown Rows producer `{other}`"),
    };
    let _report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (chosen_rows, schema),
        Some("publish"),
        NodePolicy::new(),
    );
    flow.finish()
}

/// Baseline with `report` moved from its `publish` group to the **existing**
/// `ingest` group (where `rows` and `schema` already live) — a move **between two
/// existing groups**, distinct from the group-*rename* fixture ([`regrouped`], which
/// moves `report` into a brand-new `landing` group). The node set and wiring are
/// unchanged; only `report`'s group facet moves to a group that already exists.
fn moved_between_groups() -> Pipeline {
    // `rows`/`schema` stay in `ingest`; `report` moves OUT of `publish` and INTO the
    // pre-existing `ingest` group (so all three named nodes land in `ingest`).
    baseline(|name| match name {
        "rows" | "schema" | "report" => Some("ingest"),
        _ => None,
    })
}

fn snapshot(pipeline: &Pipeline) -> StructureSnapshot {
    StructureSnapshot::from_pipeline(pipeline, "example-pipeline")
        .expect("baseline pipeline snapshots")
}

/// Write a blessed fixture for `pipeline` under a unique temp path and return it.
fn bless_to_temp(pipeline: &Pipeline, tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "dagr-t61-{tag}-{}-{}.snapshot.json",
        std::process::id(),
        // A per-call nonce so parallel tests never collide.
        FIXTURE_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    bless_structure(pipeline, "example-pipeline", &path).expect("bless writes the fixture");
    path
}

static FIXTURE_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// === Tests =================================================================

/// **Baseline match passes.** A blessed fixture and a freshly snapshotted
/// pipeline of the same structure assert equal with no diff (arch.md C28).
#[test]
fn baseline_match_passes_with_no_diff() {
    let fixture = bless_to_temp(&baseline(base_groups), "baseline-match");
    assert_structure(&baseline(base_groups), "example-pipeline", &fixture)
        .expect("baseline matches its blessed fixture with no diff");
    let _ = std::fs::remove_file(&fixture);
}

/// **Rebuild does not fail.** Volatile header fields — generation time, build
/// provenance, tool version — are excluded from the snapshot, so a "rebuild"
/// (a different provenance and generation time) produces the identical snapshot
/// and the assertion still passes (arch.md C28: does not fail on a rebuild).
#[test]
fn rebuild_does_not_fail() {
    // Two "builds" differing only in provenance + generation time produce
    // byte-identical snapshots — the volatile header is excluded.
    let build_a = StructureSnapshot::from_pipeline(&baseline(base_groups), "example-pipeline")
        .unwrap()
        .to_canonical_string();
    // A second snapshot taken through the explicit-provenance/clock path with a
    // different provenance and instant.
    let build_b = StructureSnapshot::from_pipeline_with(
        &baseline(base_groups),
        "example-pipeline",
        "2999-01-01T00:00:00Z",
        &BuildProvenance::new(
            "9.9.9",
            "ffffffffffffffffffffffffffffffffffffffff",
            "fnv1a-64:ffffffffffffffff",
        ),
    )
    .unwrap()
    .to_canonical_string();
    assert_eq!(
        build_a, build_b,
        "the snapshot excludes volatile header fields, so a rebuild produces the identical snapshot"
    );

    // And the assertion against a fixture blessed by one build passes for the other.
    let fixture = bless_to_temp(&baseline(base_groups), "rebuild");
    assert_structure(&baseline(base_groups), "example-pipeline", &fixture)
        .expect("a rebuild does not fail the structure assertion");
    let _ = std::fs::remove_file(&fixture);
}

/// **Adding a node fails with a diff.** Adding one node (and its edge) without
/// re-blessing fails; the structural diff names the added node and its new edge
/// and no unrelated nodes (arch.md C28).
#[test]
fn adding_a_node_fails_with_a_diff() {
    let fixture = bless_to_temp(&baseline(base_groups), "add");
    let err = assert_structure(&added_node(), "example-pipeline", &fixture)
        .expect_err("adding a node must fail the assertion");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("adding a node is a structural mismatch, not an I/O error");
    };
    let report = diff.to_string();
    assert!(
        report.contains("digest"),
        "the diff names the added node `digest`: {report}"
    );
    assert!(
        report.contains("added") || report.contains('+'),
        "the diff marks `digest` as added: {report}"
    );
    // No unrelated node is reported as changed.
    assert!(
        !report.contains("schema"),
        "the diff must not name the unrelated `schema` node: {report}"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Removing a node fails with a diff.** Deleting a node and its incident edges
/// fails; the diff names the removed node and the removed edges (arch.md C28).
#[test]
fn removing_a_node_fails_with_a_diff() {
    // Bless the richer (added) pipeline, then assert the baseline (node removed).
    let fixture = bless_to_temp(&added_node(), "remove");
    let err = assert_structure(&baseline(base_groups), "example-pipeline", &fixture)
        .expect_err("removing a node must fail the assertion");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("removing a node is a structural mismatch");
    };
    let report = diff.to_string();
    assert!(
        report.contains("digest") && (report.contains("removed") || report.contains('-')),
        "the diff names `digest` as removed: {report}"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Renaming a node fails with a diff.** Changing a node's stable name (keeping
/// wiring identical) fails; the diff shows the old name removed and the new name
/// added so the identity change is review-visible (arch.md C28).
#[test]
fn renaming_a_node_fails_with_a_diff() {
    let fixture = bless_to_temp(&baseline(base_groups), "rename");
    let err = assert_structure(&renamed_node(), "example-pipeline", &fixture)
        .expect_err("renaming a node must fail the assertion");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("renaming a node is a structural mismatch");
    };
    let report = diff.to_string();
    assert!(
        report.contains("report"),
        "the old name `report` shows as removed: {report}"
    );
    assert!(
        report.contains("summary"),
        "the new name `summary` shows as added: {report}"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Carried-type change fails with a diff.** Changing the payload type carried on
/// one edge (node and edge endpoints unchanged) fails; the diff reports the edge's
/// carried-type change (arch.md C28, aligned with C21 carried-type coverage).
#[test]
fn carried_type_change_fails_with_a_diff() {
    let fixture = bless_to_temp(&baseline(base_groups), "carried");
    let err = assert_structure(&carried_type_changed(), "example-pipeline", &fixture)
        .expect_err("a carried-type change must fail the assertion");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("a carried-type change is a structural mismatch");
    };
    let report = diff.to_string();
    // The edge from `rows` to `report` carried `Rows`, now `Schema`.
    assert!(
        report.contains("edge") || report.contains("Rows") || report.contains("Schema"),
        "the diff reports the carried-type change on the rows→report edge: {report}"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Rewiring an edge fails with a diff naming the removed and added edge.**
/// Redirecting one existing data edge to a **different existing endpoint** — the
/// node set is *unchanged* (no node added or removed), only `report`'s first input
/// moves from producer `rows` to producer `rows_alt` — fails the assertion. Because
/// the edge key is `from -kind-> to [carried-type]`, a pure endpoint change reads as
/// exactly one edge **removed** (`rows -data-> report`) and one edge **added**
/// (`rows_alt -data-> report`), each carrying its type + kind. The test is
/// **non-vacuous**: it also asserts NO node is reported added/removed, so it proves a
/// true rewire distinct from an add/remove and would fail if the snapshot ignored an
/// edge's endpoint (arch.md C28 · "rewired", line 595; Test plan §34).
#[test]
fn rewiring_an_edge_fails_naming_removed_and_added_edge() {
    // Golden: `report`'s Rows input comes from `rows`.
    let fixture = bless_to_temp(&two_rows_producers("rows"), "rewire");
    // Variant: the SAME node set, but `report`'s Rows input is redirected to the
    // other existing producer `rows_alt` — a pure edge rewire.
    let err = assert_structure(
        &two_rows_producers("rows_alt"),
        "example-pipeline",
        &fixture,
    )
    .expect_err("rewiring an existing edge to a different existing endpoint must fail");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("an edge rewire is a structural mismatch");
    };
    let report = diff.to_string();

    // The removed edge names the old producer→consumer with its kind + carried type.
    assert!(
        report.contains("removed edge")
            && report.contains("`rows`")
            && report.contains("-data->")
            && report.contains("`report`")
            && report.contains("Rows"),
        "the diff names the REMOVED edge `rows` -data-> `report` carrying `Rows`: {report}"
    );
    // The added edge names the new producer with its kind + carried type.
    assert!(
        report.contains("added edge")
            && report.contains("`rows_alt`")
            && report.contains("-data->")
            && report.contains("`report`")
            && report.contains("Rows"),
        "the diff names the ADDED edge `rows_alt` -data-> `report` carrying `Rows`: {report}"
    );
    // NON-VACUOUS: it is a *pure* rewire — no node is added or removed. Were the
    // snapshot to ignore an edge's endpoint, this diff would be empty and the
    // assertion above would not have failed; and this guards the rewire from being
    // mistaken for (or implemented as) an add/remove.
    assert!(
        !report.contains("added node") && !report.contains("removed node"),
        "a pure rewire must report NO node added/removed (distinct from add/remove): {report}"
    );

    // Companion: the node set really is identical across the two structures — the
    // only difference is the one moved edge (so the diff carries exactly one removed
    // and one added edge, and no node add/remove).
    let golden = snapshot(&two_rows_producers("rows"));
    let variant = snapshot(&two_rows_producers("rows_alt"));
    let structural = variant.diff(&golden);
    assert!(
        !structural.is_empty(),
        "the rewire is a real structural difference (non-empty diff)"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Moving a node between two existing groups is review-visible yet
/// fingerprint-neutral.** Moving `report` from its `publish` group into the
/// **existing** `ingest` group (where `rows`/`schema` already live) — distinct from
/// the group-*rename* test, whose destination group is brand-new — fails the
/// structure assertion, and the diff reports `report`'s group facet change. As a
/// companion check, BOTH the structural fingerprint and the policy hash are
/// unchanged, because group is fingerprint-neutral (C6 / C21; T51). (Test plan §42.)
#[test]
fn moving_a_node_between_existing_groups_is_visible_but_fingerprint_neutral() {
    let fixture = bless_to_temp(&baseline(base_groups), "move-groups");
    let err = assert_structure(&moved_between_groups(), "example-pipeline", &fixture)
        .expect_err("moving a node between groups must fail the structure assertion");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("a between-groups move is a structural-snapshot mismatch");
    };
    let report = diff.to_string();
    // The diff names the moved node and its group-facet change (publish → ingest).
    assert!(
        report.contains("report")
            && report.contains("group")
            && report.contains("publish")
            && report.contains("ingest"),
        "the diff reports `report`'s group facet moving publish -> ingest: {report}"
    );
    // The move is a pure regroup: no node and no edge is added/removed.
    assert!(
        !report.contains("added node")
            && !report.contains("removed node")
            && !report.contains("added edge")
            && !report.contains("removed edge"),
        "moving a node between groups changes only the group facet, not the topology: {report}"
    );

    // Companion (T51 / C21): a between-groups move is fingerprint-neutral — BOTH
    // hashes are unchanged.
    let base_fp = snapshot(&baseline(base_groups));
    let moved_fp = snapshot(&moved_between_groups());
    assert_eq!(
        base_fp.structural_fingerprint(),
        moved_fp.structural_fingerprint(),
        "moving a node between existing groups must not move the structural fingerprint (C21)"
    );
    assert_eq!(
        base_fp.policy_fingerprint(),
        moved_fp.policy_fingerprint(),
        "moving a node between existing groups must not move the policy hash (C21)"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Effective-policy change fails with a diff.** Changing one node's effective
/// policy (a retry count) with topology unchanged fails; the diff names the node
/// and the changed policy field (arch.md C28).
#[test]
fn effective_policy_change_fails_with_a_diff() {
    let fixture = bless_to_temp(&baseline(base_groups), "policy");
    let err = assert_structure(&policy_changed(), "example-pipeline", &fixture)
        .expect_err("an effective-policy change must fail the assertion");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("a policy change is a structural mismatch in the semantic comparison");
    };
    let report = diff.to_string();
    assert!(
        report.contains("report"),
        "the diff names the `report` node whose policy changed: {report}"
    );
    assert!(
        report.contains("retries") || report.contains("policy"),
        "the diff names the changed policy field: {report}"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Group rename fails with a review-visible diff, yet the fingerprint is
/// unchanged.** Renaming a group (no other change) fails the structure assertion —
/// the regroup is review-visible — even though both the structural fingerprint and
/// the policy hash are unchanged (C6 / C21). This is the C6/C28 distinction proven
/// in one place (arch.md C28 line 595).
#[test]
fn group_rename_fails_but_fingerprint_unchanged() {
    let fixture = bless_to_temp(&baseline(base_groups), "regroup");
    let err = assert_structure(&regrouped(), "example-pipeline", &fixture)
        .expect_err("a group rename must fail the structure assertion (review-visible)");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("a group rename is a structural-snapshot mismatch");
    };
    let report = diff.to_string();
    assert!(
        report.contains("report") && (report.contains("publish") || report.contains("landing")),
        "the diff reports the regrouping of `report` (publish → landing): {report}"
    );

    // Companion fingerprint check (T41 / C21): BOTH hashes are unchanged by the
    // regroup — the review-visible-but-fingerprint-neutral property C6 owns.
    let base_fp = snapshot(&baseline(base_groups));
    let regrouped_fp = snapshot(&regrouped());
    assert_eq!(
        base_fp.structural_fingerprint(),
        regrouped_fp.structural_fingerprint(),
        "a group rename must not move the structural fingerprint (C21)"
    );
    assert_eq!(
        base_fp.policy_fingerprint(),
        regrouped_fp.policy_fingerprint(),
        "a group rename must not move the policy hash (C21)"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **Defaulted vs. written-out policy does not differ.** A pipeline authored with
/// a policy value left to its default snapshots identically to one that states the
/// same value explicitly — defaulted values compare identically to written-out
/// defaults (C5 / C21), so no spurious diff (arch.md C28).
#[test]
fn defaulted_and_written_out_policy_do_not_differ() {
    // Baseline `report` leaves retries at its default (0).
    let defaulted = snapshot(&baseline(base_groups)).to_canonical_string();
    // The same pipeline, but `report` writes the default (0) out explicitly.
    let written_out = {
        let mut flow = Flow::new();
        let rows = flow.register_source_named::<MakeRows>(
            "rows",
            &MakeRows,
            Some("ingest"),
            NodePolicy::new(),
        );
        let schema = flow.register_source_named::<MakeSchema>(
            "schema",
            &MakeSchema,
            Some("ingest"),
            NodePolicy::new(),
        );
        let _report = flow.register_named::<BuildReport, _>(
            "report",
            &BuildReport,
            (rows, schema),
            Some("publish"),
            NodePolicy::new().retries(0).timeout_off(), // defaults written out
        );
        snapshot(&flow.finish()).to_canonical_string()
    };
    assert_eq!(
        defaulted, written_out,
        "a defaulted policy value snapshots identically to a written-out default (C5)"
    );
}

/// **Canonical serialization is stable and order-independent.** The same
/// structure registered in a different order blesses to a byte-identical fixture —
/// the serialization is canonical and stably ordered, not dependent on
/// registration order (arch.md C28 / T0.7 §6).
#[test]
fn canonical_serialization_is_stable_and_order_independent() {
    let one = snapshot(&baseline(base_groups)).to_canonical_string();
    let other = snapshot(&baseline_reordered()).to_canonical_string();
    assert_eq!(
        one, other,
        "two registration orders of the same structure produce a byte-identical snapshot"
    );
    // And blessing each to a file yields byte-identical fixture contents.
    let f1 = bless_to_temp(&baseline(base_groups), "canon-a");
    let f2 = bless_to_temp(&baseline_reordered(), "canon-b");
    assert_eq!(
        std::fs::read_to_string(&f1).unwrap(),
        std::fs::read_to_string(&f2).unwrap(),
        "blessed fixtures are byte-identical across registration orders"
    );
    let _ = std::fs::remove_file(&f1);
    let _ = std::fs::remove_file(&f2);
}

/// **Bless flow regenerates the fixture deliberately and is idempotent.** For a
/// pipeline whose structure legitimately changed against a stale fixture, running
/// the documented update command rewrites the fixture to the new canonical
/// structure; a subsequent assertion passes; running the update again is a no-op
/// (byte-identical output) — idempotence (arch.md C28).
#[test]
fn bless_flow_regenerates_and_is_idempotent() {
    // A stale fixture blessed for the baseline …
    let fixture = bless_to_temp(&baseline(base_groups), "bless");
    // … the structure legitimately changed (a node added); the assertion now fails.
    assert!(
        assert_structure(&added_node(), "example-pipeline", &fixture).is_err(),
        "the stale fixture no longer matches the changed structure"
    );
    // Re-bless deliberately to the new structure.
    bless_structure(&added_node(), "example-pipeline", &fixture).expect("re-bless rewrites");
    let after_first = std::fs::read_to_string(&fixture).unwrap();
    // A subsequent assertion passes.
    assert_structure(&added_node(), "example-pipeline", &fixture)
        .expect("after re-blessing, the assertion passes");
    // Running the update again is a no-op — byte-identical output (idempotence).
    bless_structure(&added_node(), "example-pipeline", &fixture).expect("re-bless again");
    let after_second = std::fs::read_to_string(&fixture).unwrap();
    assert_eq!(
        after_first, after_second,
        "re-running the bless command is idempotent (byte-identical output)"
    );
    let _ = std::fs::remove_file(&fixture);
}

/// **No pipeline writes its own harness.** An example pipeline that only calls the
/// shipped assertion helper against its own snapshot and fixture path runs its
/// structure test with no bespoke comparison or serialization code — the entire
/// mechanism comes from the library (arch.md C28: no pipeline needs its own
/// harness).
#[test]
fn no_pipeline_writes_its_own_harness() {
    // The example's entire "structure test" is these two library calls.
    let fixture = bless_to_temp(&baseline(base_groups), "no-harness");
    assert_structure(&baseline(base_groups), "example-pipeline", &fixture)
        .expect("the shipped helper is the whole harness");
    let _ = std::fs::remove_file(&fixture);
}

/// **A missing fixture is a clear I/O error, not a structural mismatch.** Pointing
/// the assertion at a nonexistent fixture returns an [`StructureAssertError::Io`]
/// naming the path — so a first run before blessing is a clear "bless me" signal,
/// distinct from a structural difference.
#[test]
fn missing_fixture_is_an_io_error_not_a_mismatch() {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "dagr-t61-absent-{}.snapshot.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let err = assert_structure(&baseline(base_groups), "example-pipeline", &path)
        .expect_err("a missing fixture is an error");
    assert!(
        matches!(err, StructureAssertError::Io(_)),
        "a missing fixture is an I/O error, not a structural mismatch: {err:?}"
    );
}

/// **A structure snapshot is emittable only from stable-name-aware nodes.** A
/// pipeline whose node lacks author-declared stable names cannot be snapshotted —
/// the same C20 emittability contract the graph artifact enforces (T40).
#[test]
fn snapshot_requires_stable_names() {
    // A type-erased registration produces a node without stable names.
    struct Bare;
    impl StableName for Bare {
        const STABLE_NAME: &'static str = "Bare";
    }
    impl Task for Bare {
        type Input = ();
        type Output = ();
        const EXECUTION_CLASS: ExecutionClass = ExecutionClass::AwaitBound;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
            Ok(())
        }
    }
    let mut flow = Flow::new();
    // `register_source` (NOT the *_named variant) captures no stable names.
    let _ = flow.register_source::<Bare>("bare", &Bare);
    let pipeline = flow.finish();
    let err = StructureSnapshot::from_pipeline(&pipeline, "example-pipeline")
        .expect_err("a stable-name-less node is not snapshottable");
    // It surfaces as the same emit error the graph artifact raises.
    let msg = err.to_string();
    assert!(
        msg.contains("bare") || msg.contains("stable name"),
        "the error names the offending node / the stable-name requirement: {msg}"
    );
    let _ = test_provenance(); // silence unused in this fixture-free test path
}
