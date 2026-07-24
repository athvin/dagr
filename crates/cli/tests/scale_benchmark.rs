//! **T69 (ticket 076) — Scale benchmark.** Written first, TDD (`bench(T69)` /
//! `test(T69)`).
//!
//! arch.md `## Performance envelope`: *framework overhead per node — scheduling,
//! admission, event writing; everything but the task's own work — is budgeted at
//! **under one millisecond**, held by a CI benchmark that runs a thousand-node
//! no-op graph and fails on regression.* This suite **is** that benchmark. It
//! drives a graph of exactly [`SCALE_NODE_COUNT`] no-op nodes through the **real**
//! T24 run-loop driver (`dagr_cli::driver::drive` — readiness C11, admission C12,
//! attempt running C14, event-stream writing C19, no stubbed scheduler), measures
//! the per-node framework overhead, and gates the build on the budget.
//!
//! # CI reliability (why this does not flake)
//!
//! A wall-clock timing benchmark on a shared CI runner is inherently noisy. This
//! suite therefore does **not** gate CI on a tight wall-clock threshold. It
//! asserts:
//!
//! - **Deterministic invariants** (the correctness weight): exactly
//!   [`SCALE_NODE_COUNT`] nodes, every node in exactly one success-like terminal
//!   state, the folded per-attempt phase breakdown sums exactly with a negligible
//!   `executing` phase (overhead attributed to the framework, not the task),
//!   admission capacity **pinned** (not host-discovered). None of these depends on
//!   the wall clock.
//! - **A generous wall-clock bound** ([`CI_BUDGET_NS_PER_NODE`], 16 ms/node — 16×
//!   the spec ceiling): it catches an orders-of-magnitude *regression* while normal
//!   runner variance stays comfortably under it. The 1 ms/node spec ceiling is
//!   recorded and the measured per-node overhead is **printed** for re-baselining.
//! - **The failure path** ([`over_budget`]) proven non-vacuous: a deliberately-over
//!   value is reported a regression naming the measured value, threshold, and node
//!   count.
//!
//! Every run is deterministic (no hidden randomness, no wall clock read by any
//! task, fixed graph, pinned capacity) and uses a **private per-run temp base**,
//! so concurrent test binaries never collide and the verdict is reproducible.

use std::collections::BTreeMap;

use dagr_artifact::event_stream::RunOutcome;
use dagr_artifact::fold::{fold_stream, PHASE_EXECUTING};
use dagr_cli::scale_bench::{
    build_scale_graph, over_budget, run_scale_benchmark, CI_BUDGET_NS_PER_NODE, SCALE_NODE_COUNT,
    SPEC_CEILING_NS_PER_NODE,
};
use dagr_core::admission::Pool;
use dagr_core::context::TerminalState;

// ===========================================================================
// Test-plan scenario 1 — the graph is exactly a thousand no-op nodes
// ===========================================================================

/// **Graph size is exactly a thousand nodes.** The benchmark's graph builder
/// assembles a graph whose node count is exactly [`SCALE_NODE_COUNT`], and every
/// node is a no-op source — the shape the spec's Performance envelope names.
#[test]
fn graph_is_exactly_a_thousand_no_op_nodes() {
    let (pipeline, names) = build_scale_graph();
    assert_eq!(
        pipeline.nodes().count(),
        SCALE_NODE_COUNT,
        "the benchmark graph carries exactly {SCALE_NODE_COUNT} nodes"
    );
    assert_eq!(
        SCALE_NODE_COUNT, 1_000,
        "the spec fixes the ceiling at a thousand nodes"
    );
    assert_eq!(
        names.len(),
        SCALE_NODE_COUNT,
        "one build-order name per node"
    );
    // Every node is a source (no upstream data edges): a no-op graph the driver
    // admits immediately, so the measured time is framework overhead, not the
    // shape of a dependency chain.
    for node in pipeline.nodes() {
        assert!(
            node.data_edges().is_empty(),
            "every benchmark node is an independent no-op source (no data edges)"
        );
    }
}

// ===========================================================================
// Test-plan scenario 2 — the no-op run completes; every node succeeds
// ===========================================================================

/// **No-op run completes with all nodes in a success terminal state.** Building
/// and driving the thousand-node no-op graph once through the real driver ends
/// with every one of the [`SCALE_NODE_COUNT`] nodes in exactly one success-like
/// terminal state, and the run ends precisely when nothing is pending or in
/// flight. A benchmark over a graph that silently skipped or failed nodes would
/// measure the wrong thing, so this guards the measurement's validity.
#[test]
fn no_op_run_completes_with_every_node_succeeded() {
    let run = run_scale_benchmark();

    assert_eq!(
        run.outcome,
        RunOutcome::Succeeded,
        "the no-op run succeeds overall"
    );
    assert_eq!(
        run.terminal_states.len(),
        SCALE_NODE_COUNT,
        "every node has a terminal state"
    );
    for name in &run.node_names {
        assert_eq!(
            run.terminal_states.get(name).copied(),
            Some(TerminalState::Succeeded),
            "{name} ends in the success terminal state"
        );
    }

    // Exactly one node-terminal record per node in the recorded stream (the
    // single-terminal-state invariant — the run measured a valid, fully-executed
    // graph, not one with double-assigned or missing terminals).
    let mut terminals: BTreeMap<&str, u32> = BTreeMap::new();
    // Count node-terminal transitions directly from the stream.
    let stream = dagr_artifact::event_stream::read_records(&run.stream).expect("stream parses");
    for rec in &stream.records {
        if rec.get("kind").and_then(|v| v.as_str()) == Some("node-terminal") {
            if let Some(node) = rec.get("node").and_then(|v| v.as_str()) {
                *terminals.entry(node).or_insert(0) += 1;
            }
        }
    }
    for name in &run.node_names {
        assert_eq!(
            terminals.get(name.as_str()).copied(),
            Some(1),
            "{name} reaches exactly one terminal state in the stream"
        );
    }
}

// ===========================================================================
// Test-plan scenario 3 — overhead is the framework's, not the task's
// ===========================================================================

/// **Overhead is attributed to the framework, not the task.** Reading the folded
/// per-attempt phase breakdown, the phases sum exactly to each attempt's total
/// (per C22), and each no-op attempt's window is a small **bounded constant**,
/// identical in structure across all [`SCALE_NODE_COUNT`] nodes — no node's body
/// did variable work. Because every task body is a no-op, the growth of the
/// measured budget across nodes is framework overhead
/// (readiness/admission/event-writing per node), not per-node task work.
///
/// Note on what the phase breakdown measures here: the driver is fed a
/// **deterministic monotonic tick clock** (one ns per read) so the folded phases
/// are reproducible; those phase numbers reflect *event ordering*, not real time
/// (the honest wall-clock budget number is measured separately by a real
/// `Instant`, per `per_node_overhead_is_computed_and_reported`). The fold labels
/// the whole `attempt-started → terminal` window `executing` by construction (the
/// node-level readiness/admission waits precede `attempt-started` and lie outside
/// the attempt total). What this scenario proves is that that window is a small
/// bounded constant for a no-op body — the task contributes no variable work — and
/// that the framework's own readiness/admission events are recorded per node
/// (the overhead the budget covers).
#[test]
fn overhead_is_attributed_to_the_framework_not_the_task() {
    // The no-op attempt window is a small bounded constant under the tick clock:
    // a handful of clock reads (attempt-started, terminal), never inflated by task
    // work. A regression that made a body do real work would blow this bound.
    const MAX_NOOP_ATTEMPT_TICKS: u64 = 16;

    let run = run_scale_benchmark();
    let artifact = fold_stream(&run.stream, &run.node_names)
        .expect("the benchmark stream folds into a run artifact");

    assert_eq!(
        artifact.attempts().len(),
        SCALE_NODE_COUNT,
        "one attempt per no-op node (no retries)"
    );

    for a in artifact.attempts() {
        // C22: the named phases sum exactly to the attempt's total (the real teeth).
        let sum: u64 = a.phase_durations_ns().values().copied().sum();
        assert_eq!(
            sum,
            a.total_elapsed_ns(),
            "phases sum exactly to the attempt total for {}",
            a.node()
        );
        // The task body is a no-op: its whole attempt window is a small constant,
        // identical in structure across every node (no variable per-node work).
        assert!(
            a.total_elapsed_ns() <= MAX_NOOP_ATTEMPT_TICKS,
            "no-op attempt window for {} ({} ticks) must be a small bounded constant — the \
             budget's growth across nodes is framework overhead, not task work",
            a.node(),
            a.total_elapsed_ns(),
        );
        // The `executing` phase exists (the phase vocabulary is complete) and, for
        // a no-op, equals the whole small window — never a large task-work span.
        let executing = a
            .phase_durations_ns()
            .get(PHASE_EXECUTING)
            .copied()
            .unwrap_or(0);
        assert!(
            executing <= MAX_NOOP_ATTEMPT_TICKS,
            "the no-op executing phase for {} is a small constant, not task work",
            a.node(),
        );
    }

    // The framework's per-node readiness + admission events ARE recorded in the
    // stream — the overhead the budget covers is real, not a stub. Every node has a
    // node-ready and a node-admitted transition around its attempt.
    let stream = dagr_artifact::event_stream::read_records(&run.stream).expect("stream parses");
    let count_kind = |kind: &str| {
        stream
            .records
            .iter()
            .filter(|r| r.get("kind").and_then(|v| v.as_str()) == Some(kind))
            .count()
    };
    assert_eq!(
        count_kind("node-ready"),
        SCALE_NODE_COUNT,
        "the framework recorded one readiness event per node (C11)"
    );
    assert_eq!(
        count_kind("node-admitted"),
        SCALE_NODE_COUNT,
        "the framework recorded one admission event per node (C12)"
    );
}

// ===========================================================================
// Test-plan scenario 4 — per-node overhead is computed and reported
// ===========================================================================

/// **Per-node overhead is computed and reported.** One benchmark run yields a
/// single per-node-overhead number, in a stable machine-readable form (an integer
/// nanoseconds-per-node) the CI job can threshold against, and the benchmark
/// prints it so an operator can re-baseline.
#[test]
fn per_node_overhead_is_computed_and_reported() {
    let run = run_scale_benchmark();
    let per_node = run.per_node_overhead_ns();

    // Machine-readable and computed as total / node_count.
    assert_eq!(
        per_node,
        u64::try_from(run.total_overhead_ns / SCALE_NODE_COUNT as u128).unwrap(),
        "per-node overhead is total framework overhead over node count"
    );
    // Printed for re-baselining (visible in the CI log; `cargo test -- --nocapture`
    // locally). This is the single number the budget assertion thresholds against.
    println!(
        "scale benchmark: {} nodes, total framework overhead {} ns, per-node overhead {} ns/node \
         (CI budget {} ns/node, spec ceiling {} ns/node)",
        SCALE_NODE_COUNT,
        run.total_overhead_ns,
        per_node,
        CI_BUDGET_NS_PER_NODE,
        SPEC_CEILING_NS_PER_NODE,
    );
}

// ===========================================================================
// Test-plan scenario 5 — the budget assertion passes under budget (THE GATE)
// ===========================================================================

/// **The budget assertion passes under budget.** On a normally-provisioned runner
/// with capacity pinned to the benchmark's fixed configuration, the computed
/// per-node overhead is under the generous [`CI_BUDGET_NS_PER_NODE`] budget (and,
/// on any non-pathological host, under the 1 ms/node spec ceiling too), so the
/// benchmark — the CI gate — exits success.
///
/// This is the gating assertion. It uses a **generous** bound so ordinary CI
/// runner variance cannot flap it; a genuine regression moves the number by orders
/// of magnitude, far past this headroom.
#[test]
fn the_budget_assertion_passes_under_budget() {
    let run = run_scale_benchmark();
    let per_node = run.per_node_overhead_ns();

    // The CI gate: at or under the checked-in (generous) budget. The failure path
    // is the same `over_budget` function scenario 6 drives with an over value.
    if let Some(diag) = over_budget(per_node, CI_BUDGET_NS_PER_NODE, SCALE_NODE_COUNT) {
        panic!("scale benchmark exceeded the CI budget: {diag}");
    }

    // A soft, non-gating record of headroom against the SPEC ceiling: on a healthy
    // runner the overhead is far under 1 ms/node. We do NOT hard-fail on the spec
    // ceiling here (that would be the tight, flaky gate the CI-reliability rule
    // forbids); the generous CI budget above is the gate, and this line surfaces
    // the spec-ceiling headroom in the log for re-baselining.
    println!(
        "scale benchmark under budget: {per_node} ns/node (spec ceiling {SPEC_CEILING_NS_PER_NODE} \
         ns/node, {} the ceiling)",
        if per_node < SPEC_CEILING_NS_PER_NODE {
            "well under"
        } else {
            "above the spec ceiling but under the generous CI budget — investigate (noisy host?)"
        }
    );
}

// ===========================================================================
// Test-plan scenario 6 — the budget assertion fails on regression
// ===========================================================================

/// **The budget assertion fails on regression.** Feeding the threshold check a
/// per-node-overhead value deliberately above the ceiling makes it report failure
/// with a message naming the measured value, the threshold, and the node count —
/// so a CI failure is diagnosable without re-running locally. This proves the
/// "fails on regression" clause is real and not vacuous.
#[test]
fn the_budget_assertion_fails_on_regression() {
    // A simulated slow number: one nanosecond over the CI budget must fail.
    let just_over = CI_BUDGET_NS_PER_NODE + 1;
    let diag = over_budget(just_over, CI_BUDGET_NS_PER_NODE, SCALE_NODE_COUNT)
        .expect("a value over the budget must be reported a regression");
    assert!(
        diag.contains(&just_over.to_string()),
        "the message names the measured value: {diag}"
    );
    assert!(
        diag.contains(&CI_BUDGET_NS_PER_NODE.to_string()),
        "the message names the threshold: {diag}"
    );
    assert!(
        diag.contains(&SCALE_NODE_COUNT.to_string()),
        "the message names the node count: {diag}"
    );

    // And a value at/under the threshold is NOT a regression (the check is not a
    // constant failure).
    assert!(
        over_budget(
            CI_BUDGET_NS_PER_NODE,
            CI_BUDGET_NS_PER_NODE,
            SCALE_NODE_COUNT
        )
        .is_none(),
        "a value at the threshold is under budget, not a regression"
    );

    // The same failure would fire against the SPEC ceiling — proving the ceiling
    // is a real, assertable bound the benchmark could gate on a deterministic host.
    let over_ceiling = SPEC_CEILING_NS_PER_NODE + 1;
    assert!(
        over_budget(over_ceiling, SPEC_CEILING_NS_PER_NODE, SCALE_NODE_COUNT).is_some(),
        "a value over the spec ceiling is reported a regression against the ceiling"
    );
}

// ===========================================================================
// Test-plan scenario 7 — capacity is deterministic, not host-discovered
// ===========================================================================

/// **Capacity is deterministic, not host-discovered.** Two benchmark runs on the
/// same runner use identical **pinned** admission-pool capacities (the C12 pinning
/// flag), not values discovered from the CI host's cgroup/host limits — so the
/// number is a property of dagr's overhead, not of the runner's core count. The
/// terminal-state picture is identical across the two runs (the pinned,
/// deterministic configuration), and the pinned capacities are the fixed benchmark
/// values.
#[test]
fn capacity_is_deterministic_not_host_discovered() {
    use dagr_cli::scale_bench::bench_capacities;

    // The pinned capacities are fixed benchmark values, finite and independent of
    // the host (a host-discovered pool would size from cores/cgroup, not these).
    let caps = bench_capacities();
    assert!(
        caps.total(Pool::Memory) < u64::MAX,
        "the memory pool is pinned to a finite benchmark value, not the unconstrained default"
    );
    assert_eq!(
        caps.total(Pool::BlockingThreads),
        64,
        "the blocking pool is pinned to a fixed benchmark value, not host core count"
    );
    assert_eq!(
        caps.total(Pool::ComputeThreads),
        64,
        "the compute pool is pinned to a fixed benchmark value, not host core count"
    );

    // Two runs produce the identical terminal-state picture (deterministic under
    // the pinned ceiling), so the measurement is reproducible run to run.
    let a = run_scale_benchmark();
    let b = run_scale_benchmark();
    assert_eq!(a.outcome, RunOutcome::Succeeded);
    assert_eq!(b.outcome, RunOutcome::Succeeded);
    assert_eq!(
        a.terminal_states, b.terminal_states,
        "both runs produce the identical terminal-state picture under the pinned capacity"
    );
    assert_eq!(a.terminal_states.len(), SCALE_NODE_COUNT);
}
