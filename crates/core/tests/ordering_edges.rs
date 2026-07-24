//! C4 · Ordering-dependency tests — ticket T50 (062). Written first, TDD.
//!
//! These exercise the **real** ordering-edge authoring surface and its effect on
//! readiness, assembly, and the structural fingerprint (arch.md `### C4 · Ordering
//! dependency`; the T0.9 mechanics ADR). An ordering edge sequences two nodes
//! without carrying a value: the downstream waits for its ordering upstream's
//! terminal state and receives **no** value, exactly the cleanup-after-publish /
//! cache-warm-before-read shape.
//!
//! Split of coverage:
//! - **Authoring** (this file): declare ordering edges at registration against
//!   already-registered handles; a node may carry both data + ordering edges; an
//!   ordering-only node records zero data inputs; the graph records the ordering
//!   edge distinctly.
//! - **Readiness** (this file): default-rule failure / skip propagates across an
//!   ordering edge exactly as across a data edge; an `all-terminal` node ordered
//!   after a failure still runs.
//! - **Fingerprint** (this file): an ordering edge is part of the structural
//!   fingerprint; a graph with no ordering edge keeps its existing fingerprint.
//! - **Compile-fail** (tests/ui/): a cycle is inexpressible over ordering edges,
//!   and a non-default rule on a data-consuming node still fails to compile — even
//!   with an ordering edge added.

use dagr_core::binding::{EdgeKind, TriggerRule};
use dagr_core::context::TerminalState;
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::{Handle, NodeId};
use dagr_core::readiness::{Decision, ReadinessTracker};
use dagr_core::task::{RunContext, Task};
use dagr_core::{NodePolicy, TaskError};

// === Fixture value + task types ============================================

struct Rows;
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

/// A single-input consumer of `Rows`, producing `Report`.
struct FromRows;
impl Task for FromRows {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// An effect-only sourceless task (consumes nothing, produces `()`), the shape a
/// cleanup / notify node takes.
struct Effect;
impl Task for Effect {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

fn id(name: &str) -> NodeId {
    NodeId::from_name(name)
}

fn has_ready(decisions: &[Decision], name: &str) -> bool {
    decisions
        .iter()
        .any(|d| matches!(d, Decision::Ready(n) if *n == id(name)))
}

fn propagated(decisions: &[Decision], name: &str) -> Option<(TerminalState, NodeId)> {
    decisions.iter().find_map(|d| match d {
        Decision::PropagatedTerminal {
            node,
            state,
            origin,
        } if *node == id(name) => Some((*state, *origin)),
        _ => None,
    })
}

// === Authoring ==============================================================

/// **Ordering edge attaches at registration against an existing upstream.** A
/// downstream consume-nothing node ordered after one upstream records exactly one
/// ordering edge and zero data edges into itself.
#[test]
fn ordering_edge_attaches_against_an_existing_upstream() {
    let mut flow = Flow::new();
    let up: Handle<Rows> = flow.register_source("up", &MakeRows);
    let _down: Handle<()> = flow.register_source_ordered_after("down", &Effect, &[up.ordering()]);
    let pipeline = flow.finish();

    let down = pipeline.node(id("down")).expect("down registered");
    assert!(
        down.data_edges().is_empty(),
        "an ordering-only node records zero data edges"
    );
    assert_eq!(
        down.ordering_edges().len(),
        1,
        "exactly one ordering edge recorded"
    );
    assert_eq!(down.ordering_edges()[0].upstream(), up.id());
    assert_eq!(down.ordering_edges()[0].kind(), EdgeKind::Ordering);
}

/// **A node carries both a data dependency and an additional ordering edge.** The
/// same downstream node records one data edge (carrying the producer's value type)
/// and one ordering edge (carrying no value), into itself.
#[test]
fn node_carries_both_a_data_dep_and_an_ordering_edge() {
    let mut flow = Flow::new();
    let producer: Handle<Rows> = flow.register_source("producer", &MakeRows);
    let side_effect: Handle<()> = flow.register_source("side-effect", &Effect);
    let _consumer: Handle<Report> =
        flow.register_ordered_after("consumer", &FromRows, producer, &[side_effect.ordering()]);
    let pipeline = flow.finish();

    let consumer = pipeline.node(id("consumer")).expect("consumer registered");
    assert_eq!(
        consumer.data_edges().len(),
        1,
        "one data edge (the producer's value)"
    );
    assert_eq!(consumer.data_edges()[0].upstream(), producer.id());
    assert_eq!(
        consumer.ordering_edges().len(),
        1,
        "one ordering edge (the side-effect node)"
    );
    assert_eq!(consumer.ordering_edges()[0].upstream(), side_effect.id());
}

/// **An ordering-only node's recorded input arity is zero.** No value slot is
/// demanded from its ordering upstream; no data edge is recorded.
#[test]
fn ordering_only_node_records_zero_data_inputs() {
    let mut flow = Flow::new();
    let effect: Handle<()> = flow.register_source("effect", &Effect);
    let _cleanup: Handle<()> =
        flow.register_source_ordered_after("cleanup", &Effect, &[effect.ordering()]);
    let pipeline = flow.finish();

    let cleanup = pipeline.node(id("cleanup")).expect("cleanup registered");
    assert_eq!(cleanup.data_edges().len(), 0);
    assert_eq!(cleanup.ordering_edges().len(), 1);
}

/// **A non-default trigger rule is expressible on a consume-nothing node with
/// ordering edges.** A cleanup node ordered after two upstreams with `all-terminal`
/// compiles, assembles, and records both ordering edges and its effective rule.
#[test]
fn non_default_rule_expressible_on_a_consume_nothing_node_with_ordering_edges() {
    let mut flow = Flow::new();
    let a: Handle<()> = flow.register_source("a", &Effect);
    let b: Handle<()> = flow.register_source("b", &Effect);
    let _cleanup: Handle<()> = flow.register_source_ordered_after_with_trigger(
        "cleanup",
        &Effect,
        &[a.ordering(), b.ordering()],
        NodePolicy::new(),
        TriggerRule::AllTerminal,
    );
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let cleanup = pipeline.node(id("cleanup")).expect("cleanup registered");
    assert_eq!(cleanup.ordering_edges().len(), 2);
    assert_eq!(cleanup.trigger_rule(), TriggerRule::AllTerminal);
}

// === Readiness ==============================================================

/// A consume-nothing downstream `cleanup` (given `rule`) ordered after one upstream
/// `work` — the minimal ordering-edge shape readiness is driven over.
fn ordered_after_one(rule: TriggerRule) -> Pipeline {
    let mut flow = Flow::new();
    let work: Handle<()> = flow.register_source("work", &Effect);
    let _cleanup: Handle<()> = flow.register_source_ordered_after_with_trigger(
        "cleanup",
        &Effect,
        &[work.ordering()],
        NodePolicy::new(),
        rule,
    );
    flow.finish()
}

/// **Default-rule failure propagates across an ordering edge.** A default
/// (`all-succeeded`) downstream ordered after a `failed` upstream is marked
/// `upstream-failed` without executing — identical to a data edge.
#[test]
fn default_rule_failure_propagates_across_an_ordering_edge() {
    let pipeline = ordered_after_one(TriggerRule::AllSucceeded);
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    // `cleanup` waits on its ordering upstream — not in the initial frontier.
    assert!(!tracker.initial_ready().contains(&id("cleanup")));
    assert_eq!(tracker.remaining_dependencies(id("cleanup")), Some(1));

    let decisions = tracker.notify_terminal(id("work"), TerminalState::Failed);
    assert!(!has_ready(&decisions, "cleanup"), "cleanup must not run");
    let (state, origin) = propagated(&decisions, "cleanup").expect("cleanup propagated");
    assert_eq!(state, TerminalState::UpstreamFailed);
    assert_eq!(
        origin,
        id("work"),
        "carries the originating node's identity"
    );
}

/// **Default-rule skip propagates across an ordering edge.** A default downstream
/// ordered after a `skipped` upstream is marked `upstream-skipped`, carrying the
/// originating node's identity — identical to a data edge.
#[test]
fn default_rule_skip_propagates_across_an_ordering_edge() {
    let pipeline = ordered_after_one(TriggerRule::AllSucceeded);
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let decisions = tracker.notify_terminal(id("work"), TerminalState::Skipped);
    assert!(!has_ready(&decisions, "cleanup"));
    let (state, origin) = propagated(&decisions, "cleanup").expect("cleanup propagated");
    assert_eq!(state, TerminalState::UpstreamSkipped);
    assert_eq!(origin, id("work"));
}

/// **An `all-terminal` node ordered after a failure still runs.** The motivating
/// cleanup-after-failure case: `all-terminal` never propagates failure, so the
/// cleanup node becomes ready and executes.
#[test]
fn all_terminal_node_ordered_after_a_failure_still_runs() {
    let pipeline = ordered_after_one(TriggerRule::AllTerminal);
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let decisions = tracker.notify_terminal(id("work"), TerminalState::Failed);
    assert!(
        has_ready(&decisions, "cleanup"),
        "all-terminal fires regardless of the upstream's failure"
    );
    assert!(propagated(&decisions, "cleanup").is_none());
}

/// A data upstream driven to the same terminal state propagates identically — the
/// ordering edge is byte-for-byte equivalent in readiness to a data edge.
#[test]
fn ordering_edge_propagation_matches_a_data_edge() {
    // Data-edge shape: `cleanup` consumes `work`'s value directly.
    let data_pipeline = {
        let mut flow = Flow::new();
        let work: Handle<Rows> = flow.register_source("work", &MakeRows);
        let _cleanup: Handle<Report> = flow.register("cleanup", &FromRows, work);
        flow.finish()
    };
    let data_artifact = data_pipeline.assemble().expect("assembles");
    let mut data_tracker = ReadinessTracker::new(&data_pipeline, &data_artifact);
    let data_decisions = data_tracker.notify_terminal(id("work"), TerminalState::Failed);
    let (data_state, data_origin) =
        propagated(&data_decisions, "cleanup").expect("data cleanup propagated");

    // Ordering-edge shape: `cleanup` is ordered after `work`, receives no value.
    let ord_pipeline = ordered_after_one(TriggerRule::AllSucceeded);
    let ord_artifact = ord_pipeline.assemble().expect("assembles");
    let mut ord_tracker = ReadinessTracker::new(&ord_pipeline, &ord_artifact);
    let ord_decisions = ord_tracker.notify_terminal(id("work"), TerminalState::Failed);
    let (ord_state, ord_origin) =
        propagated(&ord_decisions, "cleanup").expect("ordering cleanup propagated");

    assert_eq!(
        data_state, ord_state,
        "same propagated state as a data edge"
    );
    assert_eq!(
        data_origin, ord_origin,
        "same originating identity as a data edge"
    );
}

// === Fingerprint ============================================================

/// Two pipelines identical except one has an ordering edge the other lacks.
fn with_and_without_ordering() -> (Pipeline, Pipeline) {
    let without = {
        let mut flow = Flow::new();
        let _a: Handle<()> = flow.register_source("a", &Effect);
        let _b: Handle<()> = flow.register_source("b", &Effect);
        flow.finish()
    };
    let with = {
        let mut flow = Flow::new();
        let a: Handle<()> = flow.register_source("a", &Effect);
        let _b: Handle<()> = flow.register_source_ordered_after("b", &Effect, &[a.ordering()]);
        flow.finish()
    };
    (with, without)
}

/// **Ordering edges participate in the structural fingerprint.** Adding an ordering
/// edge is a structural change a resume must notice, so the two fingerprints differ.
#[test]
fn ordering_edge_moves_the_structural_fingerprint() {
    let (with, without) = with_and_without_ordering();
    let fp_with = with.fingerprint();
    let fp_without = without.fingerprint();
    assert_ne!(
        fp_with.structural(),
        fp_without.structural(),
        "an ordering edge is part of the structural fingerprint"
    );
}

/// **A graph with no ordering edge keeps its existing structural fingerprint** — no
/// accidental churn from this ticket. A two-source, no-edge pipeline fingerprints
/// deterministically and identically across repeated assembly.
#[test]
fn no_ordering_edge_graph_fingerprint_is_stable() {
    let (_with, without) = with_and_without_ordering();
    // Same source, assembled twice: identical (no ordering-edge churn).
    let again = {
        let mut flow = Flow::new();
        let _a: Handle<()> = flow.register_source("a", &Effect);
        let _b: Handle<()> = flow.register_source("b", &Effect);
        flow.finish()
    };
    assert_eq!(
        without.fingerprint().structural(),
        again.fingerprint().structural(),
        "a no-ordering-edge graph fingerprints identically across assemblies"
    );
}

/// The ordering edge feeds only the **structural** fingerprint, not the policy
/// hash — it is graph shape, like a data edge.
#[test]
fn ordering_edge_does_not_move_the_policy_hash() {
    let (with, without) = with_and_without_ordering();
    assert_eq!(
        with.fingerprint().policy(),
        without.fingerprint().policy(),
        "an ordering edge is structure, not policy"
    );
}
