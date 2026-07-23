//! Positive (compiles + runtime-shape) tests for the C7 flow builder and node
//! identity — ticket T13 (023). Written first, TDD.
//!
//! These exercise the **real** flow builder in [`dagr_core::flow`]: a builder
//! that accumulates node registrations (each carrying an explicit
//! caller-supplied name), hands back the typed [`Handle`] from T10, and
//! finalizes into an **immutable** [`Pipeline`]. The one decision everything
//! downstream binds to is asserted here: **node identity is the explicit
//! registration name** — name-derived, reorder-stable, rename-sensitive, and
//! with the group label carried alongside identity but excluded from it.
//!
//! Assembly *validation* (duplicate-name reporting, empty-pipeline, class
//! overrides, fingerprint computation, consumer/dependency counts, execution
//! order) is deliberately **not** here — that is T14. This ticket lands only the
//! builder skeleton, node identity, and the immutable pipeline the seams read.

use dagr_core::flow::{Flow, Pipeline, PipelineNode};
use dagr_core::handle::{Handle, NodeId};
use dagr_core::task::Task;
use dagr_core::TaskError;

// --- Illustrative value + task types (distinct, so mismatches would show) ---
struct Rows;
struct Schema;
struct Report;

/// A sourceless task producing `Rows`.
struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

/// A sourceless task producing `Schema`.
struct MakeSchema;
impl Task for MakeSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}

/// A downstream task consuming exactly two inputs, in order: `(Rows, Schema)`.
struct BuildReport;
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(
        &mut self,
        _c: &dagr_core::RunContext,
        _i: (Rows, Schema),
    ) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// **Registration returns a usable handle.** A fresh builder, one registered
/// node under an explicit name, returns a handle of the node's output type that
/// can be copied and passed around. There is no separate API to fabricate a
/// handle: obtaining one requires registering.
#[test]
fn registration_returns_a_usable_handle() {
    let mut flow = Flow::new();
    let rows: Handle<Rows> = flow.register_source("rows", &MakeRows);

    // The handle is Copy and freely passable during construction.
    fn passthrough<T>(h: Handle<T>) -> Handle<T> {
        let taken = h; // a Copy, not a move
        let _reused = h; // original still usable
        taken
    }
    let copy = rows;
    let out = passthrough(rows);
    let still = rows;

    assert_eq!(copy.id(), still.id());
    assert_eq!(out.id(), rows.id());
    // Identity is the name.
    assert_eq!(rows.id(), NodeId::from_name("rows"));
}

/// **Identity is the registration name.** Register one node under a chosen
/// name, finalize, inspect the pipeline's node set: the node is found under
/// exactly that name and its recorded identity equals the supplied name
/// verbatim (no prefix, suffix, index, or normalization).
#[test]
fn identity_is_the_registration_name() {
    let mut flow = Flow::new();
    let _rows: Handle<Rows> = flow.register_source("rows", &MakeRows);
    let pipeline: Pipeline = flow.finish();

    let node: &PipelineNode = pipeline.node(NodeId::from_name("rows")).expect("node present");
    assert_eq!(node.name(), "rows");
    assert_eq!(node.id(), NodeId::from_name("rows"));
    // Found under exactly that name; no other node exists.
    assert_eq!(pipeline.len(), 1);
}

/// **Reordering registrations changes nothing.** Two builders register the same
/// set of nodes (same names, same bodies) in different orders. Finalized, the
/// two pipelines contain the same node identities associated with the same
/// nodes; identity does not depend on order, and the immutable content is equal.
#[test]
fn reordering_registrations_changes_nothing() {
    // Order one: rows, schema, report.
    let mut a = Flow::new();
    let rows_a: Handle<Rows> = a.register_source("rows", &MakeRows);
    let schema_a: Handle<Schema> = a.register_source("schema", &MakeSchema);
    let _report_a: Handle<Report> =
        a.register("report", &BuildReport, (rows_a, schema_a));
    let pa = a.finish();

    // Order two: report last but sources reversed — schema, rows, report.
    let mut b = Flow::new();
    let schema_b: Handle<Schema> = b.register_source("schema", &MakeSchema);
    let rows_b: Handle<Rows> = b.register_source("rows", &MakeRows);
    let _report_b: Handle<Report> =
        b.register("report", &BuildReport, (rows_b, schema_b));
    let pb = b.finish();

    // Same identities present.
    for name in ["rows", "schema", "report"] {
        let id = NodeId::from_name(name);
        assert_eq!(pa.node(id).unwrap().id(), pb.node(id).unwrap().id());
        assert_eq!(pa.node(id).unwrap().name(), pb.node(id).unwrap().name());
    }
    // Order-insensitive: the immutable content is equal regardless of order.
    assert_eq!(pa, pb);
    // Iteration order is deterministic (by name), not registration order.
    let names_a: Vec<&str> = pa.nodes().map(PipelineNode::name).collect();
    let names_b: Vec<&str> = pb.nodes().map(PipelineNode::name).collect();
    assert_eq!(names_a, names_b);
}

/// **Renaming changes identity.** Two pipelines identical except one node's
/// registration name differs: the node's identity differs between them.
#[test]
fn renaming_changes_identity() {
    let mut a = Flow::new();
    let _ = a.register_source::<MakeRows>("rows", &MakeRows);
    let pa = a.finish();

    let mut b = Flow::new();
    let _ = b.register_source::<MakeRows>("input-rows", &MakeRows);
    let pb = b.finish();

    // The renamed node has a different identity — and the pipelines differ.
    assert!(pa.node(NodeId::from_name("rows")).is_some());
    assert!(pa.node(NodeId::from_name("input-rows")).is_none());
    assert!(pb.node(NodeId::from_name("input-rows")).is_some());
    assert_ne!(
        NodeId::from_name("rows"),
        NodeId::from_name("input-rows")
    );
    assert_ne!(pa, pb);
}

/// **Group label is excluded from identity.** Two pipelines whose corresponding
/// nodes carry identical names but different group labels: identities are equal
/// — the group label is presentation metadata carried alongside identity, never
/// part of it.
#[test]
fn group_label_is_excluded_from_identity() {
    let mut a = Flow::new();
    let _ = a.register_source_in_group::<MakeRows>("rows", &MakeRows, Some("ingest"));
    let pa = a.finish();

    let mut b = Flow::new();
    let _ = b.register_source_in_group::<MakeRows>("rows", &MakeRows, Some("staging"));
    let pb = b.finish();

    let na = pa.node(NodeId::from_name("rows")).unwrap();
    let nb = pb.node(NodeId::from_name("rows")).unwrap();

    // Identities are equal despite different group labels.
    assert_eq!(na.id(), nb.id());
    // The label is carried alongside, and it does differ.
    assert_eq!(na.group(), Some("ingest"));
    assert_eq!(nb.group(), Some("staging"));
    // A node registered without a group has no label.
    let mut c = Flow::new();
    let _ = c.register_source::<MakeRows>("rows", &MakeRows);
    let pc = c.finish();
    assert_eq!(pc.node(NodeId::from_name("rows")).unwrap().group(), None);
}

/// **The finalized pipeline is immutable — read-only surface.** Once finalized,
/// the pipeline exposes only read access to its node set: iterate, look up by
/// id, resolve a handle. (The *inexpressibility* of mutation-after-finalize is
/// asserted by the checked-in compile-failure fixture
/// `tests/ui/flow_pipeline_immutable.rs`; this runtime test asserts the read
/// surface is present and complete.)
#[test]
fn the_finalized_pipeline_exposes_only_read_access() {
    let mut flow = Flow::new();
    let rows: Handle<Rows> = flow.register_source("rows", &MakeRows);
    let schema: Handle<Schema> = flow.register_source("schema", &MakeSchema);
    let pipeline = flow.finish();

    assert_eq!(pipeline.len(), 2);
    assert!(!pipeline.is_empty());
    assert_eq!(pipeline.nodes().count(), 2);
    assert!(pipeline.resolve(rows).is_some());
    assert!(pipeline.resolve(schema).is_some());
}

/// **Handle-to-node linkage survives finalization.** Register two nodes, keep
/// both handles, finalize; each handle resolves to exactly the node it was
/// returned for.
#[test]
fn handle_to_node_linkage_survives_finalization() {
    let mut flow = Flow::new();
    let rows: Handle<Rows> = flow.register_source("rows", &MakeRows);
    let schema: Handle<Schema> = flow.register_source("schema", &MakeSchema);
    let pipeline = flow.finish();

    let rows_node = pipeline.resolve(rows).expect("rows resolves");
    let schema_node = pipeline.resolve(schema).expect("schema resolves");

    assert_eq!(rows_node.id(), rows.id());
    assert_eq!(rows_node.name(), "rows");
    assert_eq!(schema_node.id(), schema.id());
    assert_eq!(schema_node.name(), "schema");
    // Each maps to a distinct node.
    assert_ne!(rows_node.id(), schema_node.id());
}

/// **Handle-to-node linkage records data edges.** A data-dependent node's
/// registration records one data edge per upstream, in input order — the seam
/// assembly (T14) reads. (This ticket records; it does not adjudicate.)
#[test]
fn data_dependent_node_records_its_edges() {
    let mut flow = Flow::new();
    let rows: Handle<Rows> = flow.register_source("rows", &MakeRows);
    let schema: Handle<Schema> = flow.register_source("schema", &MakeSchema);
    let report: Handle<Report> = flow.register("report", &BuildReport, (rows, schema));
    let pipeline = flow.finish();

    let node = pipeline.resolve(report).expect("report resolves");
    let edges = node.data_edges();
    assert_eq!(edges.len(), 2);
    assert_eq!(edges[0].upstream(), rows.id());
    assert_eq!(edges[1].upstream(), schema.id());
    assert_eq!(edges[0].position(), 0);
    assert_eq!(edges[1].position(), 1);
    // A source node records no data edges.
    assert!(pipeline
        .node(NodeId::from_name("rows"))
        .unwrap()
        .data_edges()
        .is_empty());
}

/// **Builder does not touch the environment.** Building and finalizing a small
/// pipeline completes with no filesystem, network, clock, credentials, or
/// parameters reachable. The builder+finalize path introduces no such
/// dependency — the type surface offers no parameter accessor at all (the full
/// empty-environment proof is T15; this asserts the API surface introduces no
/// dependency).
#[test]
fn builder_does_not_touch_the_environment() {
    // Pure construction: no I/O primitives are constructed or reachable here,
    // and the whole builder+finalize path is a pure value transformation.
    let mut flow = Flow::new();
    let rows: Handle<Rows> = flow.register_source("rows", &MakeRows);
    let schema: Handle<Schema> = flow.register_source("schema", &MakeSchema);
    let _report = flow.register("report", &BuildReport, (rows, schema));
    let pipeline = flow.finish();
    assert_eq!(pipeline.len(), 3);
    // No parameter, clock, fs, or network value was ever named — this test
    // constructing and finalizing is itself the assertion.
}
