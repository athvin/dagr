//! Assembly validation and precomputation tests — ticket T14 (025). Written
//! first, TDD.
//!
//! These exercise the **real** assembly pass in [`dagr_core::assembly`]: the
//! total, pure validation-plus-precomputation that turns the immutable
//! [`Pipeline`](dagr_core::flow::Pipeline) from T13 into a validated,
//! runtime-ready [`AssemblyArtifact`](dagr_core::assembly::AssemblyArtifact),
//! reporting **every** problem it finds (never just the first) — governed by C7
//! (arch.md `### C7 · Flow assembly`), with criteria leaking in from C5 (invalid
//! class override), C10 (consumer counts), C17 (nonzero teardown cost), and C27
//! (durable-output contract).
//!
//! The assembly/bootstrap seam is the T0.5 ADR; the durable-output contract is
//! the T0.8 ADR; the fingerprint composition is the T0.7 ADR. Capacity/cost-fit
//! and the actual capture of allowlisted environment values are **bootstrap**
//! (T15/T24/T29), deliberately NOT here.

use dagr_core::assembly::{AssemblyArtifact, DurableOutput, NodePolicy, ProblemKind};
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::task::{ExecutionClass, RunContext, Task};
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

/// A downstream task consuming exactly two inputs: `(Rows, Schema)`.
struct BuildReport;
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
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

/// An await-bound (default) task — its work shape forbids a synchronous override.
struct AwaitBoundTask;
impl Task for AwaitBoundTask {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}

/// A synchronous (blocking) task — its work shape permits moving between the two
/// synchronous classes (blocking <-> compute).
struct BlockingTask;
impl Task for BlockingTask {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}

// --- A durable-contract-satisfying output type and one that lacks it ---------

/// An output type that IS a durable reference — implements the contract (T57).
struct DurableBlob;
impl DurableOutput for DurableBlob {
    fn serialize_reference(&self) -> String {
        "durable-blob/ref".to_string()
    }
    fn rehydrate(_reference: &str) -> Result<Self, dagr_core::RehydrateError> {
        Ok(DurableBlob)
    }
}

/// A sourceless task whose output satisfies the durable-output contract.
struct MakeDurable;
impl Task for MakeDurable {
    type Input = ();
    type Output = DurableBlob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<DurableBlob, TaskError> {
        Ok(DurableBlob)
    }
}

/// A sourceless task whose output is a plain in-memory value lacking the
/// durable-output contract.
struct MakeInMemory;
impl Task for MakeInMemory {
    type Input = ();
    type Output = Rows; // Rows does NOT implement DurableOutput
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

// ---------------------------------------------------------------------------
// Duplicate node name names BOTH declarations.
// ---------------------------------------------------------------------------

#[test]
fn duplicate_node_name_names_both_declarations() {
    let mut flow = Flow::new();
    // Two registrations under the identical name.
    let _ = flow.register_source("dup", &MakeRows);
    let _ = flow.register_source("dup", &MakeSchema);
    let _ = flow.register_source("other", &MakeRows);

    let err = flow
        .finish()
        .assemble()
        .expect_err("duplicate name must fail assembly");

    // Exactly one duplicate-name problem, and it names the duplicated name.
    let dups: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::DuplicateNodeName)
        .collect();
    assert_eq!(dups.len(), 1, "one duplicate-name problem expected");
    let msg = dups[0].message();
    // The report names the duplicated node name.
    assert!(msg.contains("dup"), "message must name the node: {msg}");
    // And it identifies that there are TWO declarations (both), not one — the
    // count of colliding declarations is surfaced.
    assert_eq!(dups[0].declaration_count(), Some(2));
}

// ---------------------------------------------------------------------------
// Empty pipeline is rejected.
// ---------------------------------------------------------------------------

#[test]
fn empty_pipeline_is_rejected() {
    let flow = Flow::new();
    let err = flow
        .finish()
        .assemble()
        .expect_err("empty pipeline must fail assembly");
    assert!(err
        .problems()
        .iter()
        .any(|p| p.kind() == ProblemKind::EmptyPipeline));
}

// ---------------------------------------------------------------------------
// Invalid execution-class override fails assembly; a compatible one assembles.
// ---------------------------------------------------------------------------

#[test]
fn invalid_execution_class_override_fails_assembly() {
    // Await-bound task overridden to a synchronous class (the disallowed
    // direction per C5).
    let mut flow = Flow::new();
    let _ = flow.register_source_with(
        "awaitish",
        &AwaitBoundTask,
        NodePolicy::new().execution_class(ExecutionClass::Blocking),
    );
    let err = flow
        .finish()
        .assemble()
        .expect_err("invalid override must fail assembly");
    let bad: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::InvalidExecutionClassOverride)
        .collect();
    assert_eq!(bad.len(), 1);
    assert!(bad[0].message().contains("awaitish"));
}

#[test]
fn compatible_execution_class_override_assembles() {
    // Synchronous (blocking) task moving to compute — the allowed direction.
    let mut flow = Flow::new();
    let _ = flow.register_source_with(
        "syncish",
        &BlockingTask,
        NodePolicy::new().execution_class(ExecutionClass::Compute),
    );
    let artifact = flow
        .finish()
        .assemble()
        .expect("compatible override assembles");
    assert_eq!(artifact.node_count(), 1);
}

// ---------------------------------------------------------------------------
// Durable node without the contract fails; a contract-satisfying one assembles.
// ---------------------------------------------------------------------------

#[test]
fn durable_node_without_the_contract_fails() {
    let mut flow = Flow::new();
    // `Rows` does not implement DurableOutput; marking the node durable fails.
    let _ = flow.register_source_with("snapshot", &MakeInMemory, NodePolicy::new().durable(true));
    let err = flow
        .finish()
        .assemble()
        .expect_err("durable without contract must fail");
    let bad: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::DurableWithoutContract)
        .collect();
    assert_eq!(bad.len(), 1);
    assert!(bad[0].message().contains("snapshot"));
}

#[test]
fn durable_node_with_the_contract_assembles() {
    let mut flow = Flow::new();
    // `register_source_durable`'s `T::Output: DurableOutput` bound captures the
    // Present witness; `DurableBlob` implements the contract.
    let _ = flow.register_source_durable("snapshot", &MakeDurable, NodePolicy::new());
    let artifact = flow
        .finish()
        .assemble()
        .expect("durable with contract assembles");
    assert_eq!(artifact.node_count(), 1);
}

// ---------------------------------------------------------------------------
// Ownership: owned demand on a multi-consumer value fails.
// ---------------------------------------------------------------------------

#[test]
fn owned_demand_on_a_multi_consumer_value_fails() {
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    // Two consumers of `rows`; the first takes ownership (the default bare-handle
    // binding is Owned), the second reads shared.
    let _ = flow.register("count-a", &CountRows, rows);
    let _ = flow.register("count-b", &CountRows, rows.shared());

    let err = flow
        .finish()
        .assemble()
        .expect_err("owned demand on a multi-consumer value must fail");
    let bad: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::OwnershipModeConflict)
        .collect();
    assert_eq!(bad.len(), 1, "one ownership conflict expected");
    let msg = bad[0].message();
    // Identifies the producer and both consumers.
    assert!(msg.contains("rows"), "names producer: {msg}");
    assert!(msg.contains("count-a"), "names offending consumer: {msg}");
    assert!(msg.contains("count-b"), "names the other consumer: {msg}");
}

// ---------------------------------------------------------------------------
// Ownership: owned edge into a retrying node without clone-on-read fails.
// ---------------------------------------------------------------------------

#[test]
fn owned_edge_into_a_retrying_node_without_clone_on_read_fails() {
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    // A sole consumer with retries taking an owned input edge, no clone-on-read.
    let _ = flow.register_with("counter", &CountRows, rows, NodePolicy::new().retries(3));

    let err = flow
        .finish()
        .assemble()
        .expect_err("owned edge into a retrying node must fail");
    let bad: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::OwnershipModeConflict)
        .collect();
    assert_eq!(bad.len(), 1);
    assert!(bad[0].message().contains("counter"));
}

#[test]
fn retrying_node_with_clone_on_read_assembles() {
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    let _ = flow.register_with(
        "counter",
        &CountRows,
        rows.clone_on_read(),
        NodePolicy::new().retries(3),
    );
    let artifact = flow
        .finish()
        .assemble()
        .expect("retries with clone-on-read assembles");
    assert_eq!(artifact.node_count(), 2);
}

#[test]
fn retrying_shared_consumer_assembles() {
    // A shared-access consumer with retries finds its input intact on every
    // attempt — no conflict.
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    let _ = flow.register_with(
        "counter",
        &CountRows,
        rows.shared(),
        NodePolicy::new().retries(3),
    );
    let artifact = flow
        .finish()
        .assemble()
        .expect("retries with shared assembles");
    assert_eq!(artifact.node_count(), 2);
}

// ---------------------------------------------------------------------------
// Nonzero teardown cost fails; a zero-cost teardown assembles.
// ---------------------------------------------------------------------------

#[test]
fn nonzero_teardown_cost_fails() {
    let mut flow = Flow::new();
    let _ = flow.register_source_with(
        "cleanup",
        &MakeRows,
        NodePolicy::new().teardown(true).working_memory(1024),
    );
    let err = flow
        .finish()
        .assemble()
        .expect_err("nonzero teardown cost must fail");
    let bad: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::NonzeroTeardownCost)
        .collect();
    assert_eq!(bad.len(), 1);
    assert!(bad[0].message().contains("cleanup"));
}

#[test]
fn zero_cost_teardown_assembles() {
    let mut flow = Flow::new();
    let _ = flow.register_source_with("cleanup", &MakeRows, NodePolicy::new().teardown(true));
    let artifact = flow
        .finish()
        .assemble()
        .expect("zero-cost teardown assembles");
    assert_eq!(artifact.node_count(), 1);
}

// ---------------------------------------------------------------------------
// Zero-consumer non-unit output is a WARNING, not an error.
// ---------------------------------------------------------------------------

#[test]
fn zero_consumer_non_unit_output_is_a_warning_not_an_error() {
    let mut flow = Flow::new();
    // `MakeRows` produces a non-() value; nothing consumes it; not retained, not
    // durable.
    let _ = flow.register_source("orphan", &MakeRows);
    let artifact = flow
        .finish()
        .assemble()
        .expect("zero-consumer non-unit output SUCCEEDS with a warning");
    let warns: Vec<_> = artifact
        .warnings()
        .iter()
        .filter(|w| w.message().contains("orphan"))
        .collect();
    assert_eq!(warns.len(), 1, "one zero-consumer warning expected");
}

/// An effect-only task whose output is `()`.
struct EffectOnly;
impl Task for EffectOnly {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

#[test]
fn unit_output_zero_consumer_produces_no_warning() {
    let mut flow = Flow::new();
    let _ = flow.register_source("effect", &EffectOnly);
    let artifact = flow.finish().assemble().expect("effect-only assembles");
    assert!(artifact.warnings().is_empty(), "no warning for () output");
}

#[test]
fn retained_or_durable_zero_consumer_produces_no_warning() {
    // Retained.
    let mut a = Flow::new();
    let _ = a.register_source_with("kept", &MakeRows, NodePolicy::new().retained(true));
    let art_a = a.finish().assemble().expect("retained assembles");
    assert!(
        art_a.warnings().is_empty(),
        "no warning for a retained node"
    );

    // Durable.
    let mut b = Flow::new();
    let _ = b.register_source_durable("durable", &MakeDurable, NodePolicy::new());
    let art_b = b.finish().assemble().expect("durable assembles");
    assert!(art_b.warnings().is_empty(), "no warning for a durable node");
}

// ---------------------------------------------------------------------------
// All problems are reported, not just the first.
// ---------------------------------------------------------------------------

#[test]
fn all_problems_are_reported_not_just_the_first() {
    let mut flow = Flow::new();
    // Defect 1 + 2: a duplicate name.
    let _ = flow.register_source("dup", &MakeRows);
    let _ = flow.register_source("dup", &MakeSchema);
    // Defect 3: a nonzero teardown cost.
    let _ = flow.register_source_with(
        "cleanup",
        &MakeRows,
        NodePolicy::new().teardown(true).working_memory(4096),
    );
    // Defect 4: a durable-without-contract node.
    let _ = flow.register_source_with("snapshot", &MakeInMemory, NodePolicy::new().durable(true));

    let err = flow.finish().assemble().expect_err("multiple defects fail");

    // Every defect present carries its own report.
    let kinds: Vec<ProblemKind> = err
        .problems()
        .iter()
        .map(dagr_core::Problem::kind)
        .collect();
    assert!(kinds.contains(&ProblemKind::DuplicateNodeName));
    assert!(kinds.contains(&ProblemKind::NonzeroTeardownCost));
    assert!(kinds.contains(&ProblemKind::DurableWithoutContract));
    // At least three distinct problems (not short-circuited to the first).
    assert!(
        err.problems().len() >= 3,
        "all problems reported: {kinds:?}"
    );
}

#[test]
fn fixing_one_defect_still_surfaces_the_rest() {
    // Same as above minus the durable defect: the duplicate and teardown-cost
    // problems still surface.
    let mut flow = Flow::new();
    let _ = flow.register_source("dup", &MakeRows);
    let _ = flow.register_source("dup", &MakeSchema);
    let _ = flow.register_source_with(
        "cleanup",
        &MakeRows,
        NodePolicy::new().teardown(true).working_memory(4096),
    );
    let err = flow.finish().assemble().expect_err("two defects remain");
    let kinds: Vec<ProblemKind> = err
        .problems()
        .iter()
        .map(dagr_core::Problem::kind)
        .collect();
    assert!(kinds.contains(&ProblemKind::DuplicateNodeName));
    assert!(kinds.contains(&ProblemKind::NonzeroTeardownCost));
    assert!(!kinds.contains(&ProblemKind::DurableWithoutContract));
}

// ---------------------------------------------------------------------------
// Consumer counts are exact before execution.
// ---------------------------------------------------------------------------

#[test]
fn consumer_counts_are_exact_before_execution() {
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    let _schema = flow.register_source("schema", &MakeSchema);
    // Three consumers of `rows` (shared so it is a legal fan-out).
    let _ = flow.register("a", &CountRows, rows.shared());
    let _ = flow.register("b", &CountRows, rows.shared());
    let _ = flow.register("c", &CountRows, rows.shared());

    let artifact = flow.finish().assemble().expect("valid fan-out assembles");

    // `rows` feeds three consumers; `schema` feeds none.
    assert_eq!(artifact.consumer_count(NodeId::from_name("rows")), Some(3));
    assert_eq!(
        artifact.consumer_count(NodeId::from_name("schema")),
        Some(0)
    );
    // Every node has a count present before any execution.
    for name in ["rows", "schema", "a", "b", "c"] {
        assert!(artifact.consumer_count(NodeId::from_name(name)).is_some());
    }
}

// ---------------------------------------------------------------------------
// Remaining-dependency counts match the graph.
// ---------------------------------------------------------------------------

#[test]
fn remaining_dependency_counts_match_the_graph() {
    // Diamond: root -> {mid1, mid2} -> join.
    let mut flow = Flow::new();
    let root = flow.register_source("root", &MakeRows);
    let mid1 = flow.register("mid1", &CountRows, root.shared());
    let mid2 = flow.register("mid2", &CountRows, root.shared());
    let _join = flow.register("join", &SumTwo, (mid1, mid2));

    let artifact = flow.finish().assemble().expect("diamond assembles");

    assert_eq!(
        artifact.remaining_dependency_count(NodeId::from_name("root")),
        Some(0)
    );
    assert_eq!(
        artifact.remaining_dependency_count(NodeId::from_name("mid1")),
        Some(1)
    );
    assert_eq!(
        artifact.remaining_dependency_count(NodeId::from_name("mid2")),
        Some(1)
    );
    assert_eq!(
        artifact.remaining_dependency_count(NodeId::from_name("join")),
        Some(2)
    );
}

/// A two-input join over `(u64, u64)`.
struct SumTwo;
impl Task for SumTwo {
    type Input = (u64, u64);
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: (u64, u64)) -> Result<u64, TaskError> {
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// Execution order is a valid topological order.
// ---------------------------------------------------------------------------

#[test]
fn execution_order_is_a_valid_topological_order() {
    let mut flow = Flow::new();
    let root = flow.register_source("root", &MakeRows);
    let mid1 = flow.register("mid1", &CountRows, root.shared());
    let mid2 = flow.register("mid2", &CountRows, root.shared());
    let _join = flow.register("join", &SumTwo, (mid1, mid2));

    let artifact = flow.finish().assemble().expect("diamond assembles");
    let order = artifact.execution_order();

    // Every node appears exactly once.
    assert_eq!(order.len(), 4);
    // Position of each node.
    let pos = |name: &str| {
        order
            .iter()
            .position(|id| *id == NodeId::from_name(name))
            .unwrap()
    };
    // Every node appears after all of its dependencies.
    assert!(pos("root") < pos("mid1"));
    assert!(pos("root") < pos("mid2"));
    assert!(pos("mid1") < pos("join"));
    assert!(pos("mid2") < pos("join"));
}

// ---------------------------------------------------------------------------
// Environment-capture allowlist is empty by default and declarative.
// ---------------------------------------------------------------------------

#[test]
fn environment_capture_allowlist_is_empty_by_default() {
    let mut flow = Flow::new();
    let _ = flow.register_source("rows", &MakeRows);
    let artifact = flow.finish().assemble().expect("assembles");
    assert!(artifact.env_allowlist().is_empty());
}

#[test]
fn environment_capture_allowlist_records_exactly_the_declared_names() {
    let mut flow = Flow::new();
    let _ = flow.register_source("rows", &MakeRows);
    flow.allow_env_capture(["DAGR_REGION", "DAGR_TIER"]);
    let artifact = flow.finish().assemble().expect("assembles");
    let allow = artifact.env_allowlist();
    assert_eq!(allow.len(), 2);
    assert!(allow.iter().any(|n| n == "DAGR_REGION"));
    assert!(allow.iter().any(|n| n == "DAGR_TIER"));
    // No value was captured — the allowlist holds names only.
}

// ---------------------------------------------------------------------------
// Assembly is pure — runs in an empty environment.
// ---------------------------------------------------------------------------

#[test]
fn assembly_is_pure_runs_in_an_empty_environment() {
    // No network reachable, no relevant files, no parameter values supplied —
    // this test process supplies none of them, and assembly touches none.
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    let schema = flow.register_source("schema", &MakeSchema);
    let _report = flow.register("report", &BuildReport, (rows.shared(), schema.shared()));
    let artifact: AssemblyArtifact = flow
        .finish()
        .assemble()
        .expect("a valid pipeline assembles with every external resource absent");
    assert_eq!(artifact.node_count(), 3);
    // The proof is that this executed with no external dependency present.
}

// ---------------------------------------------------------------------------
// Assembling twice yields byte-identical graph artifacts.
// ---------------------------------------------------------------------------

fn build_reference_pipeline() -> AssemblyArtifact {
    let mut flow = Flow::new();
    let root = flow.register_source("root", &MakeRows);
    let mid1 = flow.register("mid1", &CountRows, root.shared());
    let mid2 = flow.register("mid2", &CountRows, root.shared());
    let _join = flow.register("join", &SumTwo, (mid1, mid2));
    flow.allow_env_capture(["DAGR_REGION"]);
    flow.finish()
        .assemble()
        .expect("reference pipeline assembles")
}

#[test]
fn assembling_twice_yields_byte_identical_graph_artifacts() {
    let a = build_reference_pipeline();
    let b = build_reference_pipeline();
    // The structural fingerprint slot is deterministic.
    assert_eq!(a.fingerprint().structural(), b.fingerprint().structural());
    assert_eq!(a.fingerprint().policy(), b.fingerprint().policy());
    // The canonical byte form (the byte-identity comparison surface, generation
    // time aside) is identical.
    assert_eq!(a.canonical_bytes(), b.canonical_bytes());
}

#[test]
fn registration_order_does_not_change_the_fingerprint() {
    // Same pipeline, two registration orders — identical fingerprint + bytes.
    let mut a = Flow::new();
    let root_a = a.register_source("root", &MakeRows);
    let mid1_a = a.register("mid1", &CountRows, root_a.shared());
    let mid2_a = a.register("mid2", &CountRows, root_a.shared());
    let _ = a.register("join", &SumTwo, (mid1_a, mid2_a));
    let art_a = a.finish().assemble().unwrap();

    let mut b = Flow::new();
    // Reverse the source-consumer registration order where legal.
    let root_b = b.register_source("root", &MakeRows);
    let mid2_b = b.register("mid2", &CountRows, root_b.shared());
    let mid1_b = b.register("mid1", &CountRows, root_b.shared());
    let _ = b.register("join", &SumTwo, (mid1_b, mid2_b));
    let art_b = b.finish().assemble().unwrap();

    assert_eq!(art_a.canonical_bytes(), art_b.canonical_bytes());
    assert_eq!(
        art_a.fingerprint().structural(),
        art_b.fingerprint().structural()
    );
}
