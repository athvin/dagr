//! C11 readiness-tracker tests — ticket T18 (028). Written first, TDD.
//!
//! These exercise the **real** readiness tracker in [`dagr_core::readiness`]: the
//! pure decision engine that, given upstream terminal-state notifications, decides
//! the next ready nodes and the immediate propagated-terminal assignments —
//! governed by C11 (arch.md `### C11 · Readiness tracker`) and evaluating against
//! the normative fires / can-never-fire decision table fixed by T0.4
//! (`docs/implementation/010-T0.4-trigger-rule-and-state-tables.md`).
//!
//! The tracker consumes T14's precomputed dependency structure
//! ([`dagr_core::assembly::AssemblyArtifact`]) and the immutable
//! [`Pipeline`](dagr_core::flow::Pipeline). It never spawns, schedules, times, or
//! writes events — every scenario drives it directly with synthetic pipelines and
//! injected terminal outcomes (no real task execution, no runtime, no clock).
//!
//! M1 wires and tests the `all-succeeded` path; the rule-evaluation seam accepts
//! all three T0.4 rules (`all-succeeded`, `all-terminal`, `any-failed`) so T34 can
//! enable the other two without reshaping the tracker (see the pure-seam tests at
//! the bottom).

use dagr_core::binding::TriggerRule;
use dagr_core::context::TerminalState;
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::{Handle, NodeId};
use dagr_core::readiness::{evaluate_rule, Decision, ReadinessTracker, RuleOutcome};
use dagr_core::task::{RunContext, Task};
use dagr_core::TaskError;

// --- Illustrative value + task types ----------------------------------------
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

/// A single-input consumer of `Report`, producing `Report` (a chain link).
struct Chain;
impl Task for Chain {
    type Input = Report;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: Report) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// A two-`Report`-input join, producing `Report` — a sink on two upstreams.
struct JoinTwo;
impl Task for JoinTwo {
    type Input = (Report, Report);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Report, Report)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// A three-`Report`-input join, producing `Report`.
struct JoinThree;
impl Task for JoinThree {
    type Input = (Report, Report, Report);
    type Output = Report;
    async fn run(
        &mut self,
        _c: &RunContext,
        _i: (Report, Report, Report),
    ) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

fn id(name: &str) -> NodeId {
    NodeId::from_name(name)
}

/// Assert a decision set contains a `Ready` decision for `name`.
fn has_ready(decisions: &[Decision], name: &str) -> bool {
    decisions
        .iter()
        .any(|d| matches!(d, Decision::Ready(n) if *n == id(name)))
}

/// Find the propagated-terminal decision for `name`, if any.
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

/// A source `S`, two `Report` producers `A` and `B` each depending on `S`, and a
/// sink `J` (a two-input join) depending on both `A` and `B`. The canonical
/// diamond used by several scenarios.
fn diamond() -> Pipeline {
    let mut flow = Flow::new();
    let s = flow.register_source("source", &MakeRows);
    let a: Handle<Report> = flow.register("mid-a", &FromRows, s);
    let b: Handle<Report> = flow.register("mid-b", &FromRows, s);
    let _j: Handle<Report> = flow.register("sink", &JoinTwo, (a, b));
    flow.finish()
}

/// Just the sink half of the diamond: two independent `Report` sources `up-a`,
/// `up-b`, and a join `sink` depending on both — nothing feeds the sources, so
/// each is its own countdown-zero root and the sink starts at countdown two.
fn join_of_two() -> Pipeline {
    let mut flow = Flow::new();
    let a: Handle<Report> = flow.register_source("up-a", &MakeReport);
    let b: Handle<Report> = flow.register_source("up-b", &MakeReport);
    let _j: Handle<Report> = flow.register("sink", &JoinTwo, (a, b));
    flow.finish()
}

/// A sourceless task producing `Report` directly.
struct MakeReport;
impl Task for MakeReport {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

// ===========================================================================
// Countdown seeding + source frontier.
// ===========================================================================

/// Countdown seeds from T14's precomputed dependency counts; sources appear in
/// the initial-ready frontier; nothing else is ready yet. (C11 · DoD line 1, 8.)
#[test]
fn countdown_seeds_from_precomputed_dependency_counts() {
    let pipeline = diamond();
    let artifact = pipeline.assemble().expect("diamond assembles");
    let tracker = ReadinessTracker::new(&pipeline, &artifact);

    assert_eq!(tracker.remaining_dependencies(id("source")), Some(0));
    assert_eq!(tracker.remaining_dependencies(id("mid-a")), Some(1));
    assert_eq!(tracker.remaining_dependencies(id("mid-b")), Some(1));
    assert_eq!(tracker.remaining_dependencies(id("sink")), Some(2));

    let init: Vec<NodeId> = tracker.initial_ready().to_vec();
    assert!(init.contains(&id("source")), "source ready from the start");
    assert_eq!(init.len(), 1, "only the source is initially ready");
}

/// Source nodes are ready without any notification: two independent sources are
/// both in the initial frontier and the sink is not. (C11 · DoD line 8.)
#[test]
fn source_nodes_are_ready_without_any_notification() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let tracker = ReadinessTracker::new(&pipeline, &artifact);

    let init: Vec<NodeId> = tracker.initial_ready().to_vec();
    assert!(init.contains(&id("up-a")));
    assert!(init.contains(&id("up-b")));
    assert!(!init.contains(&id("sink")), "sink is not ready yet");
    assert_eq!(init.len(), 2, "both sources, and only them, are ready");
}

// ===========================================================================
// Decrement on terminal, and the all-upstreams-terminal evaluation gate.
// ===========================================================================

/// Notifying the source `succeeded` decrements exactly its dependents (both
/// middles → ready) and nothing else; the source becomes decided. (C11 · DoD 1,3.)
#[test]
fn decrement_on_terminal_unlocks_the_exact_dependents() {
    let pipeline = diamond();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let decisions = tracker.notify_terminal(id("source"), TerminalState::Succeeded);

    assert!(has_ready(&decisions, "mid-a"), "mid-a becomes ready");
    assert!(has_ready(&decisions, "mid-b"), "mid-b becomes ready");
    assert!(!has_ready(&decisions, "sink"), "sink does not depend on source");
    // The sink did not decrement (it depends on the middles, not the source).
    assert_eq!(tracker.remaining_dependencies(id("sink")), Some(2));
    assert!(tracker.is_decided(id("source")), "source is now decided");
    assert_eq!(
        tracker.terminal_state(id("source")),
        Some(TerminalState::Succeeded)
    );
}

/// A rule is NOT evaluated on a partial result: notifying only the first of a
/// join's two upstreams drops the countdown to one and neither readies nor
/// propagates the sink. (C11 · DoD 2 — the all-upstreams-terminal gate.)
#[test]
fn rule_is_not_evaluated_on_a_partial_result() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let decisions = tracker.notify_terminal(id("up-a"), TerminalState::Succeeded);

    assert_eq!(tracker.remaining_dependencies(id("sink")), Some(1));
    assert!(!has_ready(&decisions, "sink"), "no early fire");
    assert!(propagated(&decisions, "sink").is_none(), "no early propagation");
    assert!(!tracker.is_decided(id("sink")), "sink is still pending");
}

/// `all-succeeded` fires when the LAST upstream completes: with one upstream
/// already succeeded (countdown one), notifying the second `succeeded` drops the
/// countdown to zero, the rule fires, and the sink is emitted ready. (C11 · DoD 3.)
#[test]
fn all_succeeded_fires_when_the_last_upstream_completes() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("up-a"), TerminalState::Succeeded);
    let decisions = tracker.notify_terminal(id("up-b"), TerminalState::Succeeded);

    assert_eq!(tracker.remaining_dependencies(id("sink")), Some(0));
    assert!(has_ready(&decisions, "sink"), "sink fires and is ready");
    assert!(
        propagated(&decisions, "sink").is_none(),
        "a firing node is not propagated"
    );
}

// ===========================================================================
// `all-succeeded` can-never-fire → propagated terminal (T0.4 §5a table).
// ===========================================================================

/// Can-never-fire → `upstream-failed`, carrying the failed upstream's identity:
/// a two-upstream `all-succeeded` node with one `failed` and one `succeeded`.
/// (C11 · DoD 4,5,6; T0.4 §5a "otherwise".)
#[test]
fn all_succeeded_can_never_fire_upstream_failed() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("up-a"), TerminalState::Failed);
    let decisions = tracker.notify_terminal(id("up-b"), TerminalState::Succeeded);

    assert!(!has_ready(&decisions, "sink"), "a failing join is not ready");
    let (state, origin) = propagated(&decisions, "sink").expect("sink is propagated");
    assert_eq!(state, TerminalState::UpstreamFailed);
    assert_eq!(origin, id("up-a"), "propagation carries the failed upstream");
    assert_eq!(
        tracker.terminal_state(id("sink")),
        Some(TerminalState::UpstreamFailed)
    );
}

/// Can-never-fire → `upstream-skipped` when every non-success upstream is
/// skip-like, carrying the originating skip node. One `skipped` (originated) and
/// one `succeeded`. (C11 · DoD 4,5,6; T0.4 §5a all-skip-like row.)
#[test]
fn all_succeeded_can_never_fire_upstream_skipped() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("up-a"), TerminalState::Skipped);
    let decisions = tracker.notify_terminal(id("up-b"), TerminalState::Succeeded);

    let (state, origin) = propagated(&decisions, "sink").expect("sink is propagated");
    assert_eq!(state, TerminalState::UpstreamSkipped);
    assert_eq!(origin, id("up-a"), "propagation carries the skip node");
}

/// A propagated `upstream-skipped` upstream is itself skip-like, so a downstream
/// `all-succeeded` still propagates `upstream-skipped`. (T0.4 §3 state classes.)
#[test]
fn propagated_upstream_skipped_counts_skip_like() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("up-a"), TerminalState::UpstreamSkipped);
    let decisions = tracker.notify_terminal(id("up-b"), TerminalState::Succeeded);

    let (state, _origin) = propagated(&decisions, "sink").expect("sink is propagated");
    assert_eq!(state, TerminalState::UpstreamSkipped);
}

/// Can-never-fire → `cancelled` when every non-success upstream is stop-like:
/// one `cancelled` and one `succeeded`. (C11 · DoD 4; T0.4 §5a all-stop-like row.)
#[test]
fn all_succeeded_can_never_fire_cancelled() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("up-a"), TerminalState::Cancelled);
    let decisions = tracker.notify_terminal(id("up-b"), TerminalState::Succeeded);

    assert!(!has_ready(&decisions, "sink"));
    let (state, _origin) = propagated(&decisions, "sink").expect("sink is propagated");
    assert_eq!(state, TerminalState::Cancelled, "all-stop-like → cancelled");
}

/// Mixed non-success classes → `upstream-failed` (the "otherwise" branch): three
/// upstreams ending `succeeded`, `skipped`, and `failed`. The non-success set is
/// neither all-skip-like nor all-stop-like. (C11 · DoD 5; T0.4 §5a "otherwise".)
#[test]
fn mixed_non_success_classes_propagate_upstream_failed() {
    let mut flow = Flow::new();
    let a: Handle<Report> = flow.register_source("succ", &MakeReport);
    let b: Handle<Report> = flow.register_source("skip", &MakeReport);
    let c: Handle<Report> = flow.register_source("fail", &MakeReport);
    let _j: Handle<Report> = flow.register("sink", &JoinThree, (a, b, c));
    let pipeline = flow.finish();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("succ"), TerminalState::Succeeded);
    let _ = tracker.notify_terminal(id("skip"), TerminalState::Skipped);
    let decisions = tracker.notify_terminal(id("fail"), TerminalState::Failed);

    let (state, _origin) = propagated(&decisions, "sink").expect("sink is propagated");
    assert_eq!(
        state,
        TerminalState::UpstreamFailed,
        "a cross-class non-success set is the otherwise branch"
    );
}

// ===========================================================================
// `satisfied-from-prior` counts success-like.
// ===========================================================================

/// A resumed prior success satisfies a downstream `all-succeeded`: one upstream
/// `succeeded`, the other `satisfied-from-prior`; the join fires. (C11 · DoD 7.)
#[test]
fn satisfied_from_prior_counts_success_like() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let _ = tracker.notify_terminal(id("up-a"), TerminalState::Succeeded);
    let decisions = tracker.notify_terminal(id("up-b"), TerminalState::SatisfiedFromPrior);

    assert!(
        has_ready(&decisions, "sink"),
        "satisfied-from-prior is success-like, so the join fires"
    );
    assert!(propagated(&decisions, "sink").is_none());
}

// ===========================================================================
// Propagated terminal cascades without intervening execution.
// ===========================================================================

/// A propagated-terminal assignment is itself a terminal notification that
/// cascades: A→B→C (all `all-succeeded`). Notifying A `failed` propagates
/// `upstream-failed` to B, and that in turn propagates it to C — no intervening
/// execution. (C11 · DoD 6.)
#[test]
fn propagated_terminal_cascades() {
    let mut flow = Flow::new();
    let a: Handle<Report> = flow.register_source("A", &MakeReport);
    let b: Handle<Report> = flow.register("B", &Chain, a);
    let _c: Handle<Report> = flow.register("C", &Chain, b);
    let pipeline = flow.finish();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    let decisions = tracker.notify_terminal(id("A"), TerminalState::Failed);

    // B is propagated upstream-failed, carrying A.
    let (b_state, b_origin) = propagated(&decisions, "B").expect("B is propagated");
    assert_eq!(b_state, TerminalState::UpstreamFailed);
    assert_eq!(b_origin, id("A"));
    // C is propagated in the SAME cascade, without executing.
    let (c_state, _c_origin) = propagated(&decisions, "C").expect("C is propagated in the cascade");
    assert_eq!(c_state, TerminalState::UpstreamFailed);
    // Nothing became ready; both are decided; the run drains.
    assert!(!has_ready(&decisions, "B"));
    assert!(!has_ready(&decisions, "C"));
    assert!(tracker.is_decided(id("B")) && tracker.is_decided(id("C")));
    assert_eq!(tracker.pending_count(), 0, "everything is decided");
}

// ===========================================================================
// No wave batching — the load-bearing diamond test.
// ===========================================================================

/// A diamond with one slow branch does not batch into waves: the fast branch's
/// independent descendant is emitted ready before the slow branch reaches any
/// terminal state, and before the join is eligible. (C11 · DoD 9,10 — acceptance.)
#[test]
fn diamond_proves_no_wave_batching() {
    // source S; fast branch F and slow branch W both depend on S; F has an
    // independent descendant Fd depending only on F; the join J depends on both F
    // and W.
    let mut flow = Flow::new();
    let s = flow.register_source("S", &MakeRows);
    let f: Handle<Report> = flow.register("F", &FromRows, s);
    let w: Handle<Report> = flow.register("W", &FromRows, s);
    let _fd: Handle<Report> = flow.register("Fd", &Chain, f);
    let _j: Handle<Report> = flow.register("J", &JoinTwo, (f, w));
    let pipeline = flow.finish();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    // S succeeds → F and W become ready.
    let _ = tracker.notify_terminal(id("S"), TerminalState::Succeeded);
    // F succeeds while W is still pending.
    let decisions = tracker.notify_terminal(id("F"), TerminalState::Succeeded);

    assert!(
        has_ready(&decisions, "Fd"),
        "Fd depends only on F and is ready immediately, before W terminates"
    );
    assert!(
        !has_ready(&decisions, "J"),
        "J is not ready — it still waits on the slow branch W"
    );
    assert_eq!(
        tracker.remaining_dependencies(id("J")),
        Some(1),
        "J stays at countdown one"
    );
    assert!(!tracker.is_decided(id("W")), "W is still pending");
}

// ===========================================================================
// Exactly-one-terminal-state + pending accounting.
// ===========================================================================

/// Every node ends in exactly one terminal state, assigned exactly once: drive
/// the cascade diamond to completion and confirm no node is decided twice.
/// (C11 · DoD 12 — Vocabulary "exactly one, exactly once".)
#[test]
fn every_node_ends_in_exactly_one_terminal_state() {
    let pipeline = diamond();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    // Drive to completion by notifying executed-terminal outcomes for ready nodes.
    let _ = tracker.notify_terminal(id("source"), TerminalState::Succeeded);
    let _ = tracker.notify_terminal(id("mid-a"), TerminalState::Succeeded);
    let decisions = tracker.notify_terminal(id("mid-b"), TerminalState::Succeeded);
    // The sink fired; the driver would run it. Notify its executed terminal.
    assert!(has_ready(&decisions, "sink"));
    let _ = tracker.notify_terminal(id("sink"), TerminalState::Succeeded);

    for name in ["source", "mid-a", "mid-b", "sink"] {
        assert_eq!(
            tracker.terminal_state(id(name)),
            Some(TerminalState::Succeeded),
            "{name} has exactly one terminal state"
        );
    }
    assert_eq!(tracker.pending_count(), 0);
}

/// Re-notifying an already-decided node is rejected (no double assignment across
/// executed-terminal and propagated-terminal paths). (C11 · DoD 12.)
#[test]
fn a_decided_node_is_not_assigned_a_terminal_state_twice() {
    let pipeline = join_of_two();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    // up-a fails → sink is propagated upstream-failed once up-b terminates.
    let _ = tracker.notify_terminal(id("up-a"), TerminalState::Failed);
    let _ = tracker.notify_terminal(id("up-b"), TerminalState::Succeeded);
    assert_eq!(
        tracker.terminal_state(id("sink")),
        Some(TerminalState::UpstreamFailed)
    );
    // The driver must NOT re-notify the sink (it was propagated, never ran). A
    // second notification for an already-decided node changes nothing.
    let again = tracker.notify_terminal(id("sink"), TerminalState::Succeeded);
    assert!(again.is_empty(), "re-notifying a decided node is a no-op");
    assert_eq!(
        tracker.terminal_state(id("sink")),
        Some(TerminalState::UpstreamFailed),
        "the first terminal state stands"
    );
}

/// Pending accounting reaches zero exactly when the last node becomes terminal,
/// and is nonzero before that. (C11 · DoD 11 — the "nothing pending" signal.)
#[test]
fn pending_accounting_reports_run_completion() {
    let pipeline = diamond();
    let artifact = pipeline.assemble().expect("assembles");
    let mut tracker = ReadinessTracker::new(&pipeline, &artifact);

    assert_eq!(tracker.pending_count(), 4, "four nodes pending at the start");

    let _ = tracker.notify_terminal(id("source"), TerminalState::Succeeded);
    assert_eq!(tracker.pending_count(), 3);
    let _ = tracker.notify_terminal(id("mid-a"), TerminalState::Succeeded);
    assert_eq!(tracker.pending_count(), 2);
    let _ = tracker.notify_terminal(id("mid-b"), TerminalState::Succeeded);
    assert_eq!(tracker.pending_count(), 1, "only the sink remains");
    let _ = tracker.notify_terminal(id("sink"), TerminalState::Succeeded);
    assert_eq!(tracker.pending_count(), 0, "run has nothing pending");
}

// ===========================================================================
// The pure rule-evaluation seam accepts all three T0.4 rules.
// ===========================================================================

/// `all-succeeded` fires when every upstream is success-like (including a
/// `satisfied-from-prior`), and propagates per the §5a table otherwise.
#[test]
fn evaluate_rule_all_succeeded_matches_the_table() {
    // Fires: all success-like (with a satisfied-from-prior).
    assert_eq!(
        evaluate_rule(
            TriggerRule::AllSucceeded,
            &[TerminalState::Succeeded, TerminalState::SatisfiedFromPrior],
        ),
        RuleOutcome::Fires
    );
    // All non-success skip-like → upstream-skipped.
    assert_eq!(
        evaluate_rule(
            TriggerRule::AllSucceeded,
            &[TerminalState::Succeeded, TerminalState::UpstreamSkipped],
        ),
        RuleOutcome::Propagate(TerminalState::UpstreamSkipped)
    );
    // All non-success stop-like → cancelled.
    assert_eq!(
        evaluate_rule(
            TriggerRule::AllSucceeded,
            &[TerminalState::Succeeded, TerminalState::Cancelled],
        ),
        RuleOutcome::Propagate(TerminalState::Cancelled)
    );
    // Any failure-like, or a cross-class mix → upstream-failed.
    assert_eq!(
        evaluate_rule(
            TriggerRule::AllSucceeded,
            &[TerminalState::TimedOut, TerminalState::Succeeded],
        ),
        RuleOutcome::Propagate(TerminalState::UpstreamFailed)
    );
    assert_eq!(
        evaluate_rule(
            TriggerRule::AllSucceeded,
            &[TerminalState::Skipped, TerminalState::Cancelled],
        ),
        RuleOutcome::Propagate(TerminalState::UpstreamFailed),
        "skip-like + stop-like mix is the otherwise branch"
    );
}

/// `all-terminal` always fires once every upstream is terminal, regardless of
/// class, and never propagates failure — the seam is present though M1 does not
/// wire it into a runtime node (T34 lights it up). (T0.4 §5b.)
#[test]
fn evaluate_rule_all_terminal_always_fires() {
    assert_eq!(
        evaluate_rule(
            TriggerRule::AllTerminal,
            &[
                TerminalState::Succeeded,
                TerminalState::Skipped,
                TerminalState::Failed,
                TerminalState::Cancelled,
            ],
        ),
        RuleOutcome::Fires,
        "all-terminal fires across all four classes — no can-never-fire case"
    );
}

/// `any-failed` fires when at least one upstream is failure-like (including a
/// transitively `upstream-failed` one), and is marked `skipped` when the
/// contingency never arose. (T0.4 §5c.)
#[test]
fn evaluate_rule_any_failed_matches_the_table() {
    assert_eq!(
        evaluate_rule(
            TriggerRule::AnyFailed,
            &[TerminalState::Succeeded, TerminalState::UpstreamFailed],
        ),
        RuleOutcome::Fires,
        "a transitively upstream-failed upstream counts as failure-like"
    );
    assert_eq!(
        evaluate_rule(
            TriggerRule::AnyFailed,
            &[TerminalState::Succeeded, TerminalState::Skipped],
        ),
        RuleOutcome::Propagate(TerminalState::Skipped),
        "no failure-like upstream → the contingency never arose → skipped"
    );
}
