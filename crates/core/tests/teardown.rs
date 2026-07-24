//! Registration/assembly tests for **C17 teardown nodes** (ticket T52 / 064),
//! written first (TDD). These exercise the *authoring* seam a teardown node uses:
//! a consume-nothing node registered with [`Flow::register_teardown`], ordered
//! after an explicit covered set, carrying the `all-terminal` rule and the
//! teardown policy flag — plus the [`Pipeline::teardown_covered_nodes`] query the
//! driver's teardown phase (and the future resume seed, C27) reads.
//!
//! The *runtime* half (the driver running teardown after the main graph under a
//! fresh signal, failure isolation, admission bypass) is exercised by the
//! `dagr-cli` integration tests (`teardown_nodes.rs`); the *nonzero-cost assembly
//! rejection* and the *covered-states context exposure* already have tests
//! (`assembly.rs`, `run_context.rs`) and are reused as-is per this ticket's Out of
//! scope.

use dagr_core::assembly::{NodePolicy, ProblemKind};
use dagr_core::binding::TriggerRule;
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

// A trivial consume-nothing task — the shape both a covered node and a teardown
// node take here (a teardown consumes nothing, C4/C17).
struct Unit;
impl Task for Unit {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// A teardown registered over a covered set is a **consume-nothing `all-terminal`
/// teardown-policy node ordered after exactly the covered nodes** — the four
/// facts C17 depends on, all set by one registrar so an author cannot get them
/// out of step.
#[test]
fn register_teardown_is_all_terminal_teardown_ordered_after_covered() {
    let mut flow = Flow::new();
    let setup = flow.register_source("setup", &Unit);
    let work = flow.register_source("work", &Unit);
    let _cleanup = flow.register_teardown("cleanup", &Unit, &[setup.ordering(), work.ordering()]);

    let pipeline = flow.finish();
    let node = pipeline
        .node(NodeId::from_name("cleanup"))
        .expect("teardown registered");

    // Teardown policy flag set, and cost is zero (so assembly's bypass invariant holds).
    assert!(node.policy().is_teardown(), "cleanup is a teardown node");
    assert!(
        node.policy().cost().is_zero(),
        "a teardown declares zero cost"
    );
    // Fires on all-terminal, so it runs after covered nodes end in ANY state.
    assert_eq!(node.trigger_rule(), TriggerRule::AllTerminal);
    // Ordered after exactly the covered nodes (backward-reference discipline, C4).
    let covered: Vec<NodeId> = node
        .ordering_edges()
        .iter()
        .map(dagr_core::binding::OrderingEdge::upstream)
        .collect();
    assert!(covered.contains(&NodeId::from_name("setup")));
    assert!(covered.contains(&NodeId::from_name("work")));
    assert_eq!(covered.len(), 2, "covers exactly the two named nodes");

    // A pipeline carrying a zero-cost teardown assembles cleanly.
    pipeline.assemble().expect("zero-cost teardown assembles");
}

/// The covered-set query the driver's teardown phase and the resume seed (C27)
/// read: every teardown maps to the names of the nodes it covers. A pipeline with
/// no teardown reports an empty map (backward-compat).
#[test]
fn teardown_covered_nodes_reports_each_teardowns_covered_set() {
    let mut flow = Flow::new();
    let a = flow.register_source("a", &Unit);
    let b = flow.register_source("b", &Unit);
    let _t = flow.register_teardown("t", &Unit, &[a.ordering(), b.ordering()]);
    let pipeline = flow.finish();

    let covered = pipeline.teardown_covered_nodes();
    let mut for_t: Vec<&str> = covered
        .get("t")
        .expect("teardown t present")
        .iter()
        .map(String::as_str)
        .collect();
    for_t.sort_unstable();
    assert_eq!(for_t, vec!["a", "b"]);

    // No teardown anywhere → empty map (a no-teardown pipeline is unaffected).
    let mut plain = Flow::new();
    let _ = plain.register_source("only", &Unit);
    assert!(plain.finish().teardown_covered_nodes().is_empty());
}

/// A teardown given a **nonzero** cost is still rejected at assembly with the
/// distinct, review-visible `NonzeroTeardownCost` problem naming the node — the
/// `register_teardown` seam does not smuggle around C12's capacity invariant.
#[test]
fn register_teardown_with_nonzero_cost_is_rejected_at_assembly() {
    let mut flow = Flow::new();
    let src = flow.register_source("src", &Unit);
    let _bad = flow.register_teardown_with(
        "cleanup",
        &Unit,
        &[src.ordering()],
        NodePolicy::new().working_memory(4096),
    );
    let err = flow
        .finish()
        .assemble()
        .expect_err("nonzero teardown cost must fail assembly");
    let hits: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::NonzeroTeardownCost)
        .collect();
    assert_eq!(hits.len(), 1, "exactly one nonzero-teardown-cost problem");
    assert!(
        hits[0].message().contains("cleanup"),
        "the problem names the offending teardown node"
    );
}

/// `register_teardown_with` lets an author state a policy (retries, timeout, …)
/// while the teardown flag, zero cost, and all-terminal rule are pinned by the
/// registrar — a nonzero cost is the only policy the assembler rejects.
#[test]
fn register_teardown_with_zero_cost_policy_assembles() {
    let mut flow = Flow::new();
    let src = flow.register_source("src", &Unit);
    let _t = flow.register_teardown_with(
        "cleanup",
        &Unit,
        &[src.ordering()],
        NodePolicy::new().retries(2),
    );
    let pipeline = flow.finish();
    let node = pipeline
        .node(NodeId::from_name("cleanup"))
        .expect("present");
    assert!(node.policy().is_teardown());
    assert_eq!(node.trigger_rule(), TriggerRule::AllTerminal);
    assert_eq!(node.policy().retry_count(), 2, "author policy is respected");
    pipeline.assemble().expect("zero-cost teardown assembles");
}
