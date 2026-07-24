//! C28 · the **full-pipeline fakes harness** — ticket T62 (075). Written first,
//! TDD.
//!
//! These exercise the **shipped** harness ([`dagr_cli::full_pipeline`]) — the
//! third and last of C28's three testing levels (arch.md `### C28 · Testing
//! surface`). The harness assembles a small flow of **fake** tasks (declarative,
//! no real work), injects fake resources, and drives it through the **real** T24
//! run-loop driver ([`dagr_cli::driver::drive`]) with an injected deterministic
//! clock and a captured in-memory event sink — then exposes the run's outcome,
//! per-node terminal states, the raw event stream, and the folded run artifact
//! for assertion.
//!
//! The point is that **fakes run through real orchestration**: readiness,
//! admission, dispatch, failure propagation, trigger-rule evaluation, and
//! cancellation are the framework's (C11/C12/C13/C15), reproduced verbatim by the
//! harness rather than computed by the test. No test here reimplements the
//! scheduler, wires a `NodeRunner` by hand, or reaches into the driver.
//!
//! Determinism (the T65 contract): every outcome is scripted (a node succeeds,
//! fails permanently, retries then succeeds, or skips — keyed off the C8 attempt
//! number, never timing or randomness); the clock is a hand-stepped monotonic
//! counter; no wall-clock sleep is used; and if the harness touches the
//! filesystem it does so under a private per-run temp base. So CI is
//! reproducible run to run and machine to machine.

use std::time::{Duration, Instant};

use dagr_cli::full_pipeline::{FakeResource, FullPipelineTest, Outcome};
use dagr_core::binding::TriggerRule;
use dagr_core::context::TerminalState;

// ===========================================================================
// A fake resource, retrieved by type — no task edits needed to substitute it.
// ===========================================================================

/// A fake client a node retrieves by type from the C9 registry. In production a
/// node's task would call `ctx.resources().get::<ApiClient>()`; the harness
/// injects this fake through the registry with **no change to the task**.
#[derive(Clone, Default)]
struct ApiClient;

// ===========================================================================
// 1. Runs the real scheduler, not a stub.
// ===========================================================================

/// **Runs the real scheduler, not a stub.** A small assembled flow (a chain with
/// a fan-in) whose declared resources are all faked; every node scripted to
/// succeed. Every node reaches `succeeded`, the event stream contains
/// run-started + run-finished plus the per-node transitions the real driver
/// produces, and the folded run artifact reports exactly one terminal state per
/// node — confirming the genuine T24 run loop drove the run.
#[test]
fn runs_the_real_scheduler_not_a_stub() {
    // a → b, a → c, (b, c) → d : a chain with a fan-in (four nodes, a data
    // dependency, and a two-input join).
    let run = FullPipelineTest::new("real-scheduler")
        .source("a", Outcome::succeed())
        .node("b", &["a"], Outcome::succeed())
        .node("c", &["a"], Outcome::succeed())
        .node("d", &["b", "c"], Outcome::succeed())
        .run();

    assert_eq!(run.overall_outcome(), "succeeded", "the whole run succeeds");
    for node in ["a", "b", "c", "d"] {
        assert_eq!(
            run.terminal_state(node),
            Some(TerminalState::Succeeded),
            "node `{node}` reaches succeeded"
        );
        assert_eq!(
            run.terminal_count(node),
            1,
            "node `{node}` has exactly one terminal state"
        );
    }

    // The event stream is the REAL driver's: run-started first, run-finished last,
    // and each node's per-transition anchors present.
    assert_eq!(run.event_kinds().first().copied(), Some("run-started"));
    assert_eq!(run.event_kinds().last().copied(), Some("run-finished"));
    for node in ["a", "b", "c", "d"] {
        assert!(
            run.has_event("node-ready", Some(node)),
            "node `{node}` was made ready by the real tracker"
        );
        assert!(
            run.has_event("attempt-started", Some(node)),
            "node `{node}` had an attempt dispatched by the real driver"
        );
    }

    // The fan-in `d` becomes ready only after both `b` and `c` are terminal —
    // the framework's readiness, not the test's.
    let b_term = run.event_index("node-terminal", Some("b")).unwrap();
    let c_term = run.event_index("node-terminal", Some("c")).unwrap();
    let d_ready = run.event_index("node-ready", Some("d")).unwrap();
    assert!(
        d_ready > b_term && d_ready > c_term,
        "the fan-in `d` is ready only after both upstreams are terminal"
    );

    // The folded run artifact — one attempt record per node, overall succeeded.
    let artifact = run.artifact();
    assert_eq!(artifact.overall_outcome(), "succeeded");
    for node in ["a", "b", "c", "d"] {
        assert_eq!(
            artifact.attempts().iter().filter(|r| r.node() == node).count(),
            1,
            "the folded artifact carries exactly one attempt for `{node}`"
        );
    }
}

// ===========================================================================
// 2. No infrastructure required.
// ===========================================================================

/// **No infrastructure required.** The same flow, resources supplied only as
/// fakes, run with no network available and no database configured: the run
/// completes successfully; no code path attempts a live connection, and bootstrap
/// resource validation (C9) passes against the fakes.
#[test]
fn no_infrastructure_required() {
    let run = FullPipelineTest::new("no-infra")
        .register_fake(FakeResource::new(ApiClient))
        .source("load", Outcome::succeed())
        .node("use", &["load"], Outcome::succeed().requires::<ApiClient>())
        .run();

    assert!(
        run.bootstrap_resource_validation_passed(),
        "a flow whose declared resources are all faked passes C9 bootstrap validation"
    );
    assert_eq!(run.overall_outcome(), "succeeded");
    assert_eq!(run.terminal_state("use"), Some(TerminalState::Succeeded));
}

// ===========================================================================
// 3. Fake substitution needs no task edits.
// ===========================================================================

/// **Fake substitution needs no task edits.** A node whose scripted task
/// retrieves a resource by type receives the injected fake and completes; the fake
/// is injected purely through the registry, with the node's declared behaviour
/// unchanged.
#[test]
fn fake_substitution_needs_no_task_edits() {
    let run = FullPipelineTest::new("fake-substitution")
        .register_fake(FakeResource::new(ApiClient))
        .source(
            "reader",
            // The scripted outcome asserts the fake IS reachable by type at run
            // time — it succeeds iff `ctx.resources().get::<ApiClient>()` is Some.
            Outcome::succeed_if_resource::<ApiClient>().requires::<ApiClient>(),
        )
        .run();

    assert_eq!(
        run.terminal_state("reader"),
        Some(TerminalState::Succeeded),
        "the node received the injected fake by type and succeeded"
    );
    assert_eq!(run.overall_outcome(), "succeeded");
}

// ===========================================================================
// 4. Scripted permanent failure propagates through the real policy.
// ===========================================================================

/// **Scripted permanent failure propagates through the real policy.** `b` depends
/// on `a`; a consume-nothing contingency with a non-default `any-failed` rule is
/// ordered after `a`; `a` is scripted to fail permanently; run under
/// stop-on-first-failure. `a` is `failed`, `b` is `upstream-failed` without
/// executing, the contingency still executes, and an unrelated default-rule
/// pending node ends `cancelled` — the propagation decisions are the framework's
/// (C15), reproduced verbatim by the harness, not computed by the test.
#[test]
fn scripted_permanent_failure_propagates_through_the_real_policy() {
    let run = FullPipelineTest::new("perm-failure")
        .stop_on_first_failure()
        .source("a", Outcome::fail_permanent())
        .node("b", &["a"], Outcome::succeed())
        // A consume-nothing contingency ordered after `a`, firing on `a`'s failure.
        .contingency("notify", &["a"], TriggerRule::AnyFailed, Outcome::succeed())
        // An unrelated default-rule node with no dependency on `a`.
        .source("unrelated", Outcome::succeed())
        .run();

    assert_eq!(
        run.terminal_state("a"),
        Some(TerminalState::Failed),
        "`a` failed permanently"
    );
    assert_eq!(
        run.terminal_state("b"),
        Some(TerminalState::UpstreamFailed),
        "`b` is upstream-failed without executing"
    );
    assert!(
        !run.has_event("attempt-started", Some("b")),
        "`b` never executed an attempt"
    );
    assert_eq!(
        run.terminal_state("notify"),
        Some(TerminalState::Succeeded),
        "the any-failed contingency fired and executed"
    );
    assert!(
        run.has_event("attempt-started", Some("notify")),
        "the contingency executed a real attempt"
    );
    assert_eq!(
        run.terminal_state("unrelated"),
        Some(TerminalState::Cancelled),
        "the unrelated default-rule node ends cancelled under stop-on-first-failure"
    );
    assert_eq!(run.overall_outcome(), "failed", "the run failed overall");

    // Every node has exactly one terminal state, including the ones that never ran.
    for node in ["a", "b", "notify", "unrelated"] {
        assert_eq!(run.terminal_count(node), 1, "`{node}` has one terminal state");
    }
}

// ===========================================================================
// 5. Scripted retry-then-succeed.
// ===========================================================================

/// **Scripted retry-then-succeed.** A node scripted to fail its first attempt
/// (retry-eligibly) and succeed on the second, within its retry budget: the
/// attempt number increments across the two attempts (visible in the stream and
/// artifact), the node ends `succeeded`, and downstream data consumers run.
#[test]
fn scripted_retry_then_succeed() {
    let run = FullPipelineTest::new("retry-then-succeed")
        .source("flaky", Outcome::fail_then_succeed(1))
        .node("downstream", &["flaky"], Outcome::succeed())
        .run();

    assert_eq!(
        run.terminal_state("flaky"),
        Some(TerminalState::Succeeded),
        "the flaky node recovers on its second attempt"
    );
    // Two attempts started (initial + one retry).
    assert_eq!(
        run.event_count("attempt-started", "flaky"),
        2,
        "the flaky node started exactly two attempts"
    );
    assert_eq!(
        run.event_count("attempt-succeeded", "flaky"),
        1,
        "the flaky node's second attempt succeeded"
    );
    // The artifact carries both attempt numbers, incrementing.
    let attempts: Vec<u32> = run
        .artifact()
        .attempts()
        .iter()
        .filter(|r| r.node() == "flaky")
        .map(|r| r.attempt_number())
        .collect();
    assert_eq!(attempts, vec![1, 2], "attempt number increments across the retry");

    // The downstream data consumer ran after the recovery.
    assert_eq!(
        run.terminal_state("downstream"),
        Some(TerminalState::Succeeded),
        "the downstream data consumer runs after the retry recovers"
    );
    assert_eq!(run.overall_outcome(), "succeeded");
}

// ===========================================================================
// 6. Scripted deliberate skip.
// ===========================================================================

/// **Scripted deliberate skip.** A node scripted to skip: the skip propagates as
/// `upstream-skipped` carrying the originating node's identity to a default-rule
/// downstream, while an `all-terminal` contingency whose rule is satisfiable
/// despite the skip still runs, and a run whose only non-success outcomes are
/// skips reports overall success (C15).
#[test]
fn scripted_deliberate_skip() {
    let run = FullPipelineTest::new("deliberate-skip")
        .source("decide", Outcome::skip())
        // A default-rule downstream whose input `decide` skipped: unsatisfiable.
        .node("consumer", &["decide"], Outcome::succeed())
        // An all-terminal contingency ordered after `decide`: satisfiable despite
        // the skip (it fires once `decide` is terminal in any state).
        .contingency("cleanup", &["decide"], TriggerRule::AllTerminal, Outcome::succeed())
        .run();

    assert_eq!(
        run.terminal_state("decide"),
        Some(TerminalState::Skipped),
        "`decide` deliberately skipped"
    );
    assert_eq!(
        run.terminal_state("consumer"),
        Some(TerminalState::UpstreamSkipped),
        "the unsatisfiable default-rule downstream is upstream-skipped, not executed"
    );
    assert!(
        !run.has_event("attempt-started", Some("consumer")),
        "the upstream-skipped node never executed"
    );
    // The upstream-skipped mark carries the originating node's identity.
    assert_eq!(
        run.upstream_skip_origin("consumer").as_deref(),
        Some("decide"),
        "the upstream-skipped mark carries the originating node's identity"
    );
    assert_eq!(
        run.terminal_state("cleanup"),
        Some(TerminalState::Succeeded),
        "the all-terminal contingency runs despite the skip"
    );
    // A run whose only non-success outcomes are skips is a SUCCESSFUL run.
    assert_eq!(
        run.overall_outcome(),
        "succeeded",
        "a skip-only-non-success run reports overall success"
    );
}

// ===========================================================================
// 7. Completes in seconds — budget enforced.
// ===========================================================================

/// **Completes in seconds — budget enforced.** The harness's own CI fixture flow
/// runs under a wall-clock assertion: it completes well within the
/// completes-in-seconds budget; the test fails if elapsed time crosses the
/// configured threshold, so a future regression is caught rather than silently
/// tolerated.
#[test]
fn completes_in_seconds_budget_enforced() {
    // The completes-in-seconds budget (arch.md C28). A generous ceiling that a
    // healthy fake run clears by orders of magnitude, so it never flakes but a
    // real regression (a wall-clock sleep, a live connection) trips it.
    const BUDGET: Duration = Duration::from_secs(5);

    let start = Instant::now();
    let run = FullPipelineTest::new("budget-fixture")
        .source("a", Outcome::succeed())
        .node("b", &["a"], Outcome::fail_then_succeed(1))
        .node("c", &["b"], Outcome::succeed())
        .run();
    let elapsed = start.elapsed();

    assert_eq!(run.overall_outcome(), "succeeded", "the fixture flow succeeds");
    assert!(
        elapsed < BUDGET,
        "the full-pipeline fake run completed in {elapsed:?}, over the \
         completes-in-seconds budget of {BUDGET:?} — a regression"
    );
}

// ===========================================================================
// 8. Interpretive determinism (the T65 contract).
// ===========================================================================

/// **Interpretive determinism (the T65 contract).** One flow, one set of scripted
/// outcomes, fixed parameters and a fixed data interval, run twice: both runs
/// yield identical per-node terminal states, identical propagation decisions, and
/// byte-identical interpretive artifact content (volatile header fields excluded)
/// — demonstrating the harness is the deterministic replay surface T65 drives.
///
/// This is the covering test for system-acceptance criterion 4(b) (`SL4b`).
#[test]
fn interpretive_determinism_is_the_t65_replay_surface() {
    let build = || {
        FullPipelineTest::new("determinism")
            .run_id("fixed-run-id")
            .parameter("window", "P1D")
            .data_interval("2026-07-24T00:00:00Z", "2026-07-25T00:00:00Z")
            .source("a", Outcome::fail_then_succeed(1))
            .node("b", &["a"], Outcome::succeed())
            .contingency("guard", &["a"], TriggerRule::AllTerminal, Outcome::succeed())
            .source("skipper", Outcome::skip())
            .run()
    };

    let run_a = build();
    let run_b = build();

    // Identical per-node terminal states (the propagation decisions).
    for node in ["a", "b", "guard", "skipper"] {
        assert_eq!(
            run_a.terminal_state(node),
            run_b.terminal_state(node),
            "node `{node}` reaches the same terminal state both runs"
        );
    }
    assert_eq!(run_a.overall_outcome(), run_b.overall_outcome());

    // Byte-identical interpretive artifact content, volatile header fields (run
    // id, generation time) excluded — the harness normalizes them for the replay.
    assert_eq!(
        run_a.interpretive_artifact_json(),
        run_b.interpretive_artifact_json(),
        "the interpretive run artifact is byte-identical across repeated runs"
    );
}

// ===========================================================================
// 9. Await-bound tasks use only the provided test runtime.
// ===========================================================================

/// **Await-bound tasks use only the provided test runtime.** A flow containing an
/// await-bound task runs through the harness with no externally started async
/// runtime: the task executes on the runtime the surface provides, the run
/// completes, and the test needed no async setup of its own.
#[test]
fn await_bound_tasks_use_only_the_provided_runtime() {
    // No `#[tokio::main]`, no runtime built here — the harness provides it.
    let run = FullPipelineTest::new("await-bound")
        .source("awaiter", Outcome::succeed_after_yield())
        .node("next", &["awaiter"], Outcome::succeed())
        .run();

    assert_eq!(run.overall_outcome(), "succeeded");
    assert_eq!(run.terminal_state("awaiter"), Some(TerminalState::Succeeded));
    assert_eq!(run.terminal_state("next"), Some(TerminalState::Succeeded));
}

// ===========================================================================
// 10. No pipeline writes its own harness (the documentation deliverable).
// ===========================================================================

/// **No pipeline writes its own harness.** The example multi-node test drives
/// assembly, fake injection, scripting, execution, and assertions entirely
/// through the shipped library machinery — there is no bespoke `NodeRunner`, no
/// hand-built `RunPlan`, no direct `drive` call anywhere in this file. This test
/// is that evidence: a fork/join with a failure branch expressed purely through
/// the harness API.
#[test]
fn no_pipeline_writes_its_own_harness() {
    // A fork/join with a failure branch, expressed only through the harness.
    //         ┌─ left  (fails permanently) ─┐
    //  root ──┤                             ├── join
    //         └─ right (succeeds) ──────────┘
    // Under continue-independent mode, `left` fails, `join` is upstream-failed,
    // `right` still succeeds — all decided by the framework.
    let run = FullPipelineTest::new("fork-join-failure")
        .source("root", Outcome::succeed())
        .node("left", &["root"], Outcome::fail_permanent())
        .node("right", &["root"], Outcome::succeed())
        .node("join", &["left", "right"], Outcome::succeed())
        .run();

    assert_eq!(run.terminal_state("root"), Some(TerminalState::Succeeded));
    assert_eq!(run.terminal_state("left"), Some(TerminalState::Failed));
    assert_eq!(run.terminal_state("right"), Some(TerminalState::Succeeded));
    assert_eq!(
        run.terminal_state("join"),
        Some(TerminalState::UpstreamFailed),
        "the join is upstream-failed because `left` failed — the framework's decision"
    );
    assert_eq!(run.overall_outcome(), "failed");

    // Every node has exactly one terminal state (including the never-ran join).
    for node in ["root", "left", "right", "join"] {
        assert_eq!(run.terminal_count(node), 1);
    }
}
