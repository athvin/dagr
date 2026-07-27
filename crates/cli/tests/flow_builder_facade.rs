//! The `FlowBuilder` declaration façade over `RunnableFlow` — a curated
//! *declaration* surface (`source` / `node` / `source_erased` / `node_erased`)
//! that returns the real `Handle<T>` and never leaks the *execution* seam
//! (`run` / `into_pipeline`) into a DAG body. Written first (TDD).
//!
//! # What this suite proves
//!
//! - **Forwarding / structure parity.** A two-node `extract -> load` DAG built
//!   through [`FlowBuilder::source`] / [`FlowBuilder::node`] assembles to the
//!   *identical* pipeline structure (same nodes, same one data edge, same stable
//!   names) as the equivalent hand-written `register_source_named` /
//!   `register_named` calls — asserted through the T61 structure-snapshot kit.
//! - **Graph-emittable by default.** The `FlowBuilder`-built DAG emits a graph
//!   artifact (not an `AssemblyFailure` / emit error) recording the
//!   author-declared stable names — because `source` / `node` forward to the
//!   stable-name-aware registrars.
//!
//! The compile-time safety cases (wrong-type wiring, `StableName`-less `source`,
//! no `run` / `into_pipeline` on the façade) are pinned as `trybuild` compile-fail
//! samples under `tests/ui/` (see `tests/flow_builder_compile_fail.rs`).

use dagr_cli::prelude::FlowBuilder;
use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::structure_snapshot::StructureSnapshot;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// The example tasks — a two-node `extract -> load` DAG, stable-named so the DAG
// is emittable to the graph contract.
// ===========================================================================

/// The rows an extract produced — a payload whose declared stable name is what the
/// graph edge carrying it records.
#[derive(Clone)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// A loaded report.
#[derive(Clone)]
#[allow(dead_code)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// The source: reads some rows.
struct Extract {
    rows: u64,
}
impl StableName for Extract {
    const STABLE_NAME: &'static str = "Extract";
}
impl Task for Extract {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _ctx: &RunContext, _input: ()) -> Result<Rows, TaskError> {
        Ok(Rows(self.rows))
    }
}

/// The sink: loads the rows into a report.
struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "Load";
}
impl Task for Load {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _ctx: &RunContext, input: Rows) -> Result<Report, TaskError> {
        Ok(Report(input.0 * 2))
    }
}

/// Build the `extract -> load` DAG through the **`FlowBuilder` façade**.
fn build_via_facade() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    {
        let mut f = FlowBuilder::new(&mut flow);
        let rows = f.source("extract", Extract { rows: 100 });
        let _report = f.node::<Load, _>("load", Load, rows);
    }
    flow
}

/// Build the *same* DAG through the explicit `f.task(..).depends_on(..)` builder — the
/// primary wiring surface. It delegates to the positional `f.node(..)`, so it must
/// produce an identical structure.
fn build_via_depends_on() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    {
        let mut f = FlowBuilder::new(&mut flow);
        let rows = f.source("extract", Extract { rows: 100 });
        let _report = f.task("load", Load).depends_on(rows); // load DEPENDS ON extract
    }
    flow
}

/// Build the *same* DAG the hand-written, stable-name-aware way — the parity oracle.
fn build_via_register_named() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let rows = flow.register_source_named("extract", Extract { rows: 100 });
    let _report = flow.register_named::<Load, _>("load", Load, rows);
    flow
}

// ===========================================================================
// Forwarding / structure parity.
// ===========================================================================

/// A `FlowBuilder`-built two-node DAG assembles to the **identical** structure
/// (nodes, edges, stable names) as the equivalent `register_source_named` /
/// `register_named` calls — `source` / `node` forward to exactly those registrars.
#[test]
fn facade_built_dag_is_structurally_identical_to_hand_written_named_form() {
    let facade_pipeline = build_via_facade().into_pipeline();
    let hand_pipeline = build_via_register_named().into_pipeline();

    let facade_snapshot =
        StructureSnapshot::from_pipeline(&facade_pipeline, "etl").expect("façade DAG is emittable");
    let hand_snapshot = StructureSnapshot::from_pipeline(&hand_pipeline, "etl")
        .expect("hand-written DAG is emittable");

    let diff = facade_snapshot.diff(&hand_snapshot);
    assert!(
        diff.is_empty(),
        "a FlowBuilder-built DAG must be structurally identical to the hand-written \
         register_*_named form, but the structures differ:\n{diff}"
    );
    // The byte-canonical structure is identical too (belt-and-braces on the diff).
    assert_eq!(
        facade_snapshot.to_canonical_string(),
        hand_snapshot.to_canonical_string(),
        "the canonical structure bytes must match the hand-written form"
    );
}

/// The explicit `f.task(..).depends_on(..)` builder produces the **identical**
/// structure to the positional `f.node(..)` form it delegates to — the two spellings
/// are the same registration, so `depends_on` adds ergonomics and loses nothing.
#[test]
fn depends_on_builder_is_identical_to_the_positional_node_form() {
    let builder_pipeline = build_via_depends_on().into_pipeline();
    let node_pipeline = build_via_facade().into_pipeline();

    let builder_snapshot = StructureSnapshot::from_pipeline(&builder_pipeline, "etl")
        .expect("depends_on DAG is emittable");
    let node_snapshot =
        StructureSnapshot::from_pipeline(&node_pipeline, "etl").expect("node DAG is emittable");

    assert_eq!(
        builder_snapshot.to_canonical_string(),
        node_snapshot.to_canonical_string(),
        "`f.task(..).depends_on(..)` must build the identical structure as `f.node(..)`"
    );
}

/// The `FlowBuilder`-built DAG is **graph-emittable** and records the
/// author-declared stable names — `source` / `node` are the graph-emittable pair
/// (they carry the `StableName` bounds), so `graph <flow>` / `validate <flow>`
/// work over a DAG declared through the façade.
#[test]
fn facade_built_dag_is_graph_emittable_and_records_stable_names() {
    let pipeline = build_via_facade().into_pipeline();
    let mut out = Vec::new();
    dagr_cli::graph::graph_verb(&pipeline, "etl", "1970-01-01T00:00:00Z", &mut out)
        .expect("a façade-built DAG emits its graph artifact (not an emit failure)");

    let artifact: serde_json::Value =
        serde_json::from_slice(out.trim_ascii_end()).expect("graph verb emits a JSON artifact");

    // The two nodes are present under their author-declared identity names.
    let node_names: Vec<&str> = artifact["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|n| n["name"].as_str().expect("node name"))
        .collect();
    assert!(
        node_names.contains(&"extract") && node_names.contains(&"load"),
        "the façade DAG's two nodes are present, got {node_names:?}"
    );

    // The author-declared stable task names were recorded (graph-emittable proof).
    let task_names: Vec<&str> = artifact["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .filter_map(|n| n["task_name"].as_str())
        .collect();
    assert!(
        task_names.contains(&"Extract") && task_names.contains(&"Load"),
        "the façade DAG records the author-declared stable task names, got {task_names:?}"
    );

    // The single data edge carries the declared `Rows` stable type name.
    let edges = artifact["edges"].as_array().expect("edges array");
    assert_eq!(edges.len(), 1, "the DAG has exactly one data edge");
    assert_eq!(
        edges[0]["type_name"].as_str(),
        Some("Rows"),
        "the data edge records the declared stable carried-type name"
    );
}
