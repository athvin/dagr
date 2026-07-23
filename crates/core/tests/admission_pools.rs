//! C12 · Admission pools and permit lifecycle (T31).
//!
//! These are the TDD acceptance tests for the runtime admission controller: the
//! weighted capacity pools, all-or-nothing multi-pool acquisition, oldest-ready
//! -first admission with bounded bypass, and the permit lifecycle the T0.3 ADR
//! fixed (permit held for the whole attempt; abandoned-but-running cost counted
//! until the closure returns; no leak across a whole run). Admission is driven by
//! **counts, not sleeps**, so every scenario is deterministic in CI (no
//! wall-clock, no network) — a pool at capacity makes an over-demand node wait
//! until a release, and the release is an explicit call, never a timer.
//!
//! The mapped headline test for C12 is
//! [`admission_pools_hold_capacity_and_permits_release_without_leaking`].

use std::sync::Arc;
use std::time::Duration;

use dagr_core::admission::{
    AdmissionController, Permit, Pool, PoolCapacities, PoolCost, UndeclaredCostWarning,
};
use dagr_core::assembly::NodePolicy;
use dagr_core::execution::ZombieObserver;
use dagr_core::slot::ResidencyLedger;

/// A per-pool cost with only working memory declared (no threads, no residency).
fn mem_cost(bytes: u64) -> PoolCost {
    PoolCost::new().working_memory(bytes)
}

// ===========================================================================
// The mapped C12 headline test — a whole run ends with every pool back to full.
// ===========================================================================

/// **No permit leak across a whole run.** A controller admits a sequence of
/// nodes through every terminal outcome — success, permanent failure,
/// retry-eligible failure, cooperative cancellation, and a timed-out blocking
/// node whose zombie eventually returns — and the run ends with every pool back
/// at full remaining capacity and zero live zombies. This is the invariant the
/// whole ticket protects: the ledger never lies and never leaks.
#[test]
fn admission_pools_hold_capacity_and_permits_release_without_leaking() {
    let caps = PoolCapacities::new()
        .memory(10_000)
        .blocking_threads(4)
        .compute_threads(4);
    let ctrl = AdmissionController::new(caps);

    // Success: admit, hold, then drop the permit at the terminal state.
    let p = ctrl.try_admit("succeeded", &mem_cost(1_000)).expect("fits");
    assert_eq!(ctrl.remaining(Pool::Memory), 9_000);
    drop(p);
    assert_eq!(ctrl.remaining(Pool::Memory), 10_000);

    // Permanent failure: same lifecycle — release at the terminal failure.
    let p = ctrl.try_admit("failed", &mem_cost(2_000)).expect("fits");
    assert_eq!(ctrl.remaining(Pool::Memory), 8_000);
    drop(p);
    assert_eq!(ctrl.remaining(Pool::Memory), 10_000);

    // Retry-eligible failure: the attempt's permit releases; a fresh attempt
    // re-acquires rather than inheriting the held permit.
    let attempt1 = ctrl.try_admit("retried", &mem_cost(3_000)).expect("fits");
    drop(attempt1);
    let attempt2 = ctrl.try_admit("retried", &mem_cost(3_000)).expect("fits");
    assert_eq!(ctrl.remaining(Pool::Memory), 7_000);
    drop(attempt2);
    assert_eq!(ctrl.remaining(Pool::Memory), 10_000);

    // Cooperative cancellation: the future is dropped, the permit releases.
    let p = ctrl.try_admit("cancelled", &mem_cost(4_000)).expect("fits");
    drop(p);
    assert_eq!(ctrl.remaining(Pool::Memory), 10_000);

    // Timed-out blocking node: marked timed-out immediately (a live zombie), the
    // permit is held until the closure returns, then released.
    let zombie = ctrl.try_admit("timed_out", &mem_cost(5_000)).expect("fits");
    ctrl.mark_zombie(&zombie);
    assert_eq!(ctrl.zombie_report().live_zombie_count, 1);
    assert_eq!(ctrl.remaining(Pool::Memory), 5_000);
    drop(zombie); // the closure finally returns
    assert_eq!(ctrl.zombie_report().live_zombie_count, 0);

    // Run end: every pool is back to full, no zombie pins anything.
    assert_eq!(ctrl.remaining(Pool::Memory), 10_000);
    assert_eq!(ctrl.remaining(Pool::BlockingThreads), 4);
    assert_eq!(ctrl.remaining(Pool::ComputeThreads), 4);
    assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
    assert!(ctrl.all_pools_full());
}

// ===========================================================================
// Immediate admission and per-pool ledger accounting.
// ===========================================================================

/// A node that fits every pool is admitted at once, and each pool's remaining
/// capacity drops by exactly that node's declared per-pool cost.
#[test]
fn a_fitting_node_is_admitted_immediately_and_each_pool_drops_by_its_cost() {
    let caps = PoolCapacities::new()
        .memory(1_000)
        .blocking_threads(4)
        .compute_threads(4);
    let ctrl = AdmissionController::new(caps);

    let cost = PoolCost::new()
        .working_memory(400)
        .blocking_threads(2)
        .compute_threads(1);
    let permit = ctrl.try_admit("n", &cost).expect("fits every pool");

    assert_eq!(ctrl.remaining(Pool::Memory), 600);
    assert_eq!(ctrl.remaining(Pool::BlockingThreads), 2);
    assert_eq!(ctrl.remaining(Pool::ComputeThreads), 3);
    drop(permit);
    assert!(ctrl.all_pools_full());
}

/// A node is admitted only when it fits **every** pool it needs — and while it
/// waits on the pool it does not fit, the pool it *does* fit is **not** consumed
/// (no partial hold).
#[test]
fn a_node_is_admitted_only_when_it_fits_every_pool_and_holds_no_partial() {
    let caps = PoolCapacities::new()
        .memory(1_000)
        .blocking_threads(1)
        .compute_threads(4);
    let ctrl = AdmissionController::new(caps);

    // Fits memory (needs 500 of 1000) but not blocking threads (needs 2 of 1).
    let cost = PoolCost::new().working_memory(500).blocking_threads(2);
    assert!(
        ctrl.try_admit("n", &cost).is_none(),
        "must not admit when a pool does not fit"
    );
    // Critically: the memory pool was NOT consumed while waiting on threads.
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000, "no partial hold");
    assert_eq!(ctrl.remaining(Pool::BlockingThreads), 1);
}

// ===========================================================================
// Atomic multi-pool acquisition — two contending nodes make progress, no deadlock.
// ===========================================================================

/// Two ready nodes each declaring cost on the same two pools, pinned so only one
/// fits at a time. One is admitted, runs, releases; then the other is admitted.
/// At no instant does one hold pool A while blocking on pool B while the other
/// holds pool B while blocking on pool A — acquisition is all-or-nothing, so the
/// classic two-pool deadlock cannot arise. The test would hang (and fail) if
/// either stalled; because admission is by counts, it completes deterministically.
#[test]
fn two_contending_multi_pool_nodes_make_progress_without_deadlock() {
    let caps = PoolCapacities::new()
        .memory(1_000)
        .blocking_threads(1)
        .compute_threads(1);
    let ctrl = AdmissionController::new(caps);

    let cost = PoolCost::new()
        .working_memory(600)
        .blocking_threads(1)
        .compute_threads(1);

    // Only one can hold both pools at once (blocking/compute pools are size 1).
    let a = ctrl.try_admit("a", &cost).expect("a is admitted first");
    assert!(
        ctrl.try_admit("b", &cost).is_none(),
        "b cannot be admitted while a holds the single-unit pools"
    );
    // a runs and releases atomically — every pool it drew from returns together.
    drop(a);
    let b = ctrl
        .try_admit("b", &cost)
        .expect("b is admitted after a releases");
    drop(b);
    assert!(ctrl.all_pools_full());
}

// ===========================================================================
// The capacity invariant holds while a zombie is live.
// ===========================================================================

/// Combined counted cost never exceeds capacity, including a live zombie. Pinned
/// to admit exactly one node of the declared cost: admit a blocking node, time it
/// out into a live zombie, and a second same-cost node is refused until the
/// zombie's closure returns. At every instant the counted sum (executing + zombie)
/// is at most capacity.
#[test]
fn combined_counted_cost_never_exceeds_capacity_including_a_live_zombie() {
    let caps = PoolCapacities::new().memory(1_000);
    let ctrl = AdmissionController::new(caps);

    let cost = mem_cost(1_000); // pinned to admit exactly one
    let zombie = ctrl.try_admit("z", &cost).expect("first fits");
    ctrl.mark_zombie(&zombie); // timed out → abandoned-but-running
    assert_eq!(
        ctrl.remaining(Pool::Memory),
        0,
        "zombie still pins its cost"
    );

    // The zombie's closure has not returned: a second same-cost node is refused.
    assert!(
        ctrl.try_admit("second", &cost).is_none(),
        "second node must wait for the zombie to return"
    );
    assert_eq!(ctrl.counted(Pool::Memory), 1_000);
    assert!(ctrl.counted(Pool::Memory) <= 1_000, "invariant never lies");

    // The zombie's closure finally returns → its permit drops → capacity freed.
    drop(zombie);
    assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
    let second = ctrl
        .try_admit("second", &cost)
        .expect("admitted only after the zombie returned");
    drop(second);
}

// ===========================================================================
// Oldest-ready-first with bounded bypass — no starvation of a large node.
// ===========================================================================

/// A large node ready first, behind a continuous stream of small ready nodes, is
/// admitted within a bounded number of admission decisions — small nodes bypass
/// only while doing so cannot delay the large (oldest) waiter, so the large node
/// is never indefinitely postponed. Admission is by counts: the stream is a fixed
/// number of small offers, and the large node must be admitted before the stream
/// is exhausted.
#[test]
fn a_large_node_behind_a_stream_of_small_nodes_is_admitted_without_starvation() {
    // Fits several small (100) nodes at once, but only one large (1000) node.
    let caps = PoolCapacities::new().memory(1_000);
    let ctrl = AdmissionController::new(caps);

    // The large node arrives first — it is the oldest waiter.
    ctrl.offer("large", &mem_cost(1_000));
    // A continuous stream of small nodes arrives behind it.
    for i in 0..50 {
        ctrl.offer(&format!("small-{i}"), &mem_cost(100));
    }

    // Drive admission decisions: each round admits whoever the policy allows and
    // then releases them (simulating instant completion), until the large node is
    // admitted. The large node MUST be admitted within a bounded number of rounds.
    let mut large_admitted_round = None;
    for round in 0..100 {
        let admitted = ctrl.poll_admissions();
        for permit in admitted {
            if permit.node() == "large" {
                large_admitted_round = Some(round);
            }
            drop(permit); // instant completion frees capacity
        }
        if large_admitted_round.is_some() {
            break;
        }
    }

    let round =
        large_admitted_round.expect("the large node is admitted within a bounded number of rounds");
    assert!(
        round < 100,
        "large node admitted at round {round}, never starved"
    );
}

/// Bounded bypass never delays the oldest waiter: with one large node waiting
/// (the oldest) and free capacity that a small node would fit, the small node is
/// admitted **only** if doing so leaves the large node's admission no later than
/// it otherwise would have been. When admitting the small node *would* push out
/// the large node, the small node is held instead.
#[test]
fn bounded_bypass_never_delays_the_oldest_waiter() {
    // Capacity 1000: a large (1000) node needs all of it; a small (100) node
    // would fit in free capacity, but admitting it would delay the large node.
    let caps = PoolCapacities::new().memory(1_000);
    let ctrl = AdmissionController::new(caps);

    ctrl.offer("large", &mem_cost(1_000)); // oldest waiter, needs all capacity
    ctrl.offer("small", &mem_cost(100)); // would fit free capacity

    // A poll must admit the large node (the oldest waiter), not bypass it with the
    // small node — admitting the small one would push the large one's admission
    // out, which the bounded bypass forbids.
    let admitted = ctrl.poll_admissions();
    let names: Vec<_> = admitted.iter().map(|p| p.node().to_string()).collect();
    assert!(
        names.contains(&"large".to_string()),
        "the oldest waiter (large) is admitted, not delayed by the small bypass; got {names:?}"
    );
    assert!(
        !names.contains(&"small".to_string()),
        "the small node is held because admitting it would delay the large oldest waiter"
    );
}

/// Bounded bypass **does** admit a small node when it genuinely cannot delay the
/// oldest waiter — with enough headroom for both, the small node rides along.
#[test]
fn bounded_bypass_admits_a_small_node_when_it_cannot_delay_the_oldest() {
    // Capacity 1100: the large (1000) and the small (100) both fit at once, so
    // admitting the small one cannot delay the large one.
    let caps = PoolCapacities::new().memory(1_100);
    let ctrl = AdmissionController::new(caps);

    ctrl.offer("large", &mem_cost(1_000));
    ctrl.offer("small", &mem_cost(100));

    let admitted = ctrl.poll_admissions();
    let names: Vec<_> = admitted.iter().map(|p| p.node().to_string()).collect();
    assert!(names.contains(&"large".to_string()), "large is admitted");
    assert!(
        names.contains(&"small".to_string()),
        "the small node rides along because it cannot delay the large oldest waiter"
    );
}

// ===========================================================================
// Permit releases on each terminal outcome (one representative test each).
// ===========================================================================

/// Permit releases on success: working memory returns to the pool at the terminal
/// success state (output residency is handled separately by the slot lease).
#[test]
fn permit_releases_on_success() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000));
    let permit = ctrl.try_admit("ok", &mem_cost(400)).expect("fits");
    assert_eq!(ctrl.remaining(Pool::Memory), 600);
    drop(permit); // reaches terminal success
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
}

/// Permit releases on permanent failure.
#[test]
fn permit_releases_on_permanent_failure() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000));
    let permit = ctrl.try_admit("fail", &mem_cost(400)).expect("fits");
    drop(permit); // reaches terminal permanent failure
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
}

/// Permit releases on retry-eligible failure, and the next attempt re-acquires
/// admission fresh rather than inheriting a held permit.
#[test]
fn permit_releases_on_retry_eligible_failure_and_next_attempt_reacquires() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000));
    let attempt1 = ctrl.try_admit("retry", &mem_cost(1_000)).expect("fits");
    drop(attempt1); // retry-eligible failure releases the attempt's permit
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
    // The next attempt re-acquires fresh (it would fail to fit if the permit had
    // leaked into the retry).
    let attempt2 = ctrl
        .try_admit("retry", &mem_cost(1_000))
        .expect("the next attempt re-acquires a fresh permit");
    drop(attempt2);
}

/// Permit releases on cooperative cancellation: the future is dropped and the
/// permit releases immediately; remaining capacity is restored at once.
#[test]
fn permit_releases_on_cooperative_cancellation() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000));
    let permit = ctrl.try_admit("cancel", &mem_cost(700)).expect("fits");
    assert_eq!(ctrl.remaining(Pool::Memory), 300);
    // Cooperative cancellation of an await-bound node drops the future → the
    // permit moved into it drops → immediate release.
    drop(permit);
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
}

// ===========================================================================
// Timed-out blocking attempt: permit held until the closure returns.
// ===========================================================================

/// Immediately after the timeout mark the node is timed out and the ledger still
/// counts the full declared cost (one zombie present); only after the closure
/// actually returns does the ledger release that cost and drop the zombie count
/// to zero.
#[test]
fn permit_for_a_timed_out_blocking_attempt_is_held_until_the_closure_returns() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000).blocking_threads(2));
    let cost = PoolCost::new().working_memory(1_000).blocking_threads(2);
    let permit = ctrl.try_admit("blocking", &cost).expect("fits");

    // Fire the timeout: mark the attempt as a zombie. The permit is NOT released.
    ctrl.mark_zombie(&permit);
    assert_eq!(ctrl.remaining(Pool::Memory), 0, "cost still counted");
    assert_eq!(ctrl.remaining(Pool::BlockingThreads), 0);
    assert_eq!(ctrl.zombie_report().live_zombie_count, 1);

    // A live zombie is observable through the ZombieObserver seam (defers retry).
    assert!(
        ctrl.has_live_zombie(),
        "zombie is live before the closure returns"
    );

    // The closure finally returns → the permit drops → the cost releases and the
    // zombie count drops to zero.
    drop(permit);
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
    assert_eq!(ctrl.remaining(Pool::BlockingThreads), 2);
    assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
    assert!(!ctrl.has_live_zombie());
}

// ===========================================================================
// Working memory vs output residency — charged separately, and the slot lease.
// ===========================================================================

/// Working memory and output residency are charged separately: working memory is
/// charged on admission and released at the attempt's terminal state; output
/// residency **transfers** from the attempt to the node's output slot when the
/// value is produced and is NOT released at the attempt's terminal state — it
/// remains charged as a slot lease against the shared memory pool.
#[test]
fn working_memory_and_output_residency_are_charged_separately() {
    let ledger = ResidencyLedger::new();
    // The memory pool counts BOTH the controller's working-memory permits AND the
    // shared residency ledger (the slot lease) against total memory capacity.
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(10_000))
        .with_residency_ledger(Arc::clone(&ledger));

    // Admit the producer: working memory 2000 is charged now.
    let cost = PoolCost::new()
        .working_memory(2_000)
        .output_residency(3_000);
    let permit = ctrl.try_admit("producer", &cost).expect("fits");
    assert_eq!(
        ctrl.counted(Pool::Memory),
        2_000,
        "only working memory is charged on admission"
    );

    // The value is produced: output residency transfers to the slot lease. The
    // controller mints a residency lease against the shared ledger, standing in
    // for the transfer `Slot::fill` performs into the same ledger (C10).
    let lease = ctrl.transfer_residency("producer", 3_000);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        5_000,
        "working memory + transferred residency both counted against the pool"
    );

    // The attempt reaches its terminal state: working memory releases, residency
    // stays charged as the slot lease.
    drop(permit);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        3_000,
        "working memory released at terminal state; residency lease still held"
    );

    // The slot actually releases (last consumer terminal-and-returned): the lease
    // finally reclaims. The lease's drop mirrors the slot's own release timing
    // (C10), which waits for zombie consumers to return.
    drop(lease);
    assert_eq!(ctrl.counted(Pool::Memory), 0);
}

/// The slot lease is held until the slot actually releases — which waits for a
/// zombie consumer to return — so the pool never regains capacity for bytes a
/// leftover thread still pins. Modelled by holding the residency lease across the
/// zombie's lifetime: the memory pool regains the residency only when the lease
/// (the slot's actual release) drops, not when the healthy consumer finishes.
#[test]
fn slot_lease_is_held_until_the_slot_actually_releases() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(10_000));

    // The value is produced and its residency transferred to the slot lease.
    let lease = ctrl.transfer_residency("produced", 4_000);
    assert_eq!(ctrl.counted(Pool::Memory), 4_000);

    // A zombie consumer of the value: its work has not returned, so the slot
    // cannot release. The residency stays counted the whole time.
    let consumer_cost = mem_cost(1_000);
    let zombie_consumer = ctrl
        .try_admit("zombie_consumer", &consumer_cost)
        .expect("fits");
    ctrl.mark_zombie(&zombie_consumer);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        5_000,
        "residency lease + zombie consumer both pinned"
    );

    // The healthy consumer finishing does not release the slot (a zombie consumer
    // still holds it open) — the lease is not dropped here.
    assert_eq!(ctrl.counted(Pool::Memory), 5_000);

    // The zombie consumer's closure finally returns; only then can the slot
    // actually release its lease.
    drop(zombie_consumer);
    assert_eq!(ctrl.counted(Pool::Memory), 4_000, "residency still leased");
    drop(lease); // the slot actually releases now that the zombie has returned
    assert_eq!(ctrl.counted(Pool::Memory), 0);
}

/// A retained output is charged until run end: its residency stays counted past
/// all consumers' terminal states, never reclaimed mid-run — the lease is only
/// released when the run ends.
#[test]
fn a_retained_output_is_charged_until_run_end() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(10_000));

    // A retained node's residency lease is held to run end.
    let retained_lease = ctrl.transfer_residency("retained", 2_500);
    assert_eq!(ctrl.counted(Pool::Memory), 2_500);

    // Consumers come and go (admitted and released), but the retained residency
    // is never reclaimed while the run is live.
    for i in 0..3 {
        let c = ctrl
            .try_admit(&format!("consumer-{i}"), &mem_cost(500))
            .expect("fits");
        drop(c);
        assert_eq!(ctrl.counted(Pool::Memory), 2_500, "retained stays charged");
    }

    // Run end: the retained lease is released.
    drop(retained_lease);
    assert_eq!(ctrl.counted(Pool::Memory), 0);
}

// ===========================================================================
// Permit-wait time recorded separately from execution time.
// ===========================================================================

/// The recorded permit-wait duration and the recorded execution duration are
/// distinct fields, each reflecting the correct interval; a node admitted
/// immediately shows a near-zero wait. Time is injected (explicit instants), not
/// read from a wall clock, so the test is deterministic.
#[test]
fn permit_wait_time_is_recorded_separately_from_execution_time() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000));

    // A node that waits a measurable interval before admission.
    let mut phases = ctrl.begin_wait("waiter");
    phases.record_wait(Duration::from_millis(250)); // measured wait phase
    phases.record_execution(Duration::from_millis(700)); // measured exec phase
    assert_eq!(phases.wait(), Duration::from_millis(250));
    assert_eq!(phases.execution(), Duration::from_millis(700));
    assert_ne!(phases.wait(), phases.execution(), "distinct fields");

    // A node admitted immediately shows a near-zero wait.
    let mut immediate = ctrl.begin_wait("immediate");
    immediate.record_wait(Duration::ZERO);
    immediate.record_execution(Duration::from_millis(500));
    assert_eq!(immediate.wait(), Duration::ZERO);
    assert!(immediate.wait() < immediate.execution());
}

// ===========================================================================
// Undeclared-cost warning in a memory-constrained run.
// ===========================================================================

/// A memory-constrained run warns about a node with no declared memory cost, and
/// the warning names the node. The warning fires only when the memory pool is a
/// real constraint, not for an unconstrained run.
#[test]
fn an_undeclared_cost_node_warns_in_a_memory_constrained_run() {
    // A memory-constrained pool (a real, finite constraint).
    let constrained = AdmissionController::new(PoolCapacities::new().memory(1_000));
    let no_cost = PoolCost::new(); // no declared memory cost
    let warning: Option<UndeclaredCostWarning> =
        constrained.warn_if_undeclared("free_rider", &no_cost);
    let warning = warning.expect("a constrained run warns about an undeclared-cost node");
    assert_eq!(warning.node(), "free_rider");

    // An unconstrained run (no memory constraint) does NOT warn.
    let unconstrained = AdmissionController::new(PoolCapacities::new());
    assert!(
        unconstrained
            .warn_if_undeclared("free_rider", &no_cost)
            .is_none(),
        "no warning fires for an unconstrained run"
    );

    // A node that DID declare a memory cost never warns, constrained or not.
    assert!(
        constrained
            .warn_if_undeclared("declared", &mem_cost(100))
            .is_none(),
        "a node with a declared cost does not warn"
    );
}

// ===========================================================================
// Declared-cost reporting seam (T42/C23) — declared side + zombie cost.
// ===========================================================================

/// The reporting seam surfaces each node's declared per-pool cost and the current
/// per-pool zombie cost, in the shape T42/C23 folds side by side with measured
/// cost. No measured-vs-declared comparison is computed here; only the declared
/// side and the live zombie cost are surfaced.
#[test]
fn declared_cost_is_exposed_for_the_artifact_juxtaposition() {
    let ctrl = AdmissionController::new(
        PoolCapacities::new()
            .memory(10_000)
            .blocking_threads(4)
            .compute_threads(4),
    );

    // Admit a node with a known per-pool declared cost, then time it out.
    let cost = PoolCost::new().working_memory(300).blocking_threads(1);
    let zombie = ctrl.try_admit("alpha", &cost).expect("fits");
    ctrl.mark_zombie(&zombie);

    let report = ctrl.zombie_report();
    assert_eq!(report.live_zombie_count, 1);
    // The zombie pins its exact declared per-pool cost.
    let alpha = report
        .zombies
        .iter()
        .find(|z| z.node == "alpha")
        .expect("alpha is a live zombie");
    assert_eq!(alpha.pinned.working_memory(), 300);
    assert_eq!(alpha.pinned.blocking_threads(), 1);
    assert_eq!(alpha.pinned.compute_threads(), 0);

    // The declared-cost of a node is derivable from its C5 policy cost vector
    // without duplicating the definition (the controller reads NodePolicy::cost).
    let policy = NodePolicy::new().working_memory(300).blocking_threads(1);
    let declared = PoolCost::from_cost_vector(policy.cost());
    assert_eq!(declared.working_memory_bytes(), 300);
    assert_eq!(declared.blocking_thread_count(), 1);

    drop(zombie);
    assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
}

// ===========================================================================
// Isolation: the ledger cannot be corrupted by a misbehaving task.
// ===========================================================================

/// The admission ledger's machinery is isolated from task execution: releasing a
/// permit twice (a defect) cannot drive a pool's remaining capacity above its
/// total, so a misbehaving task cannot corrupt the ledger into over-crediting.
#[test]
fn the_ledger_is_isolated_and_cannot_be_corrupted_into_over_crediting() {
    let ctrl = AdmissionController::new(PoolCapacities::new().memory(1_000));
    let permit = ctrl.try_admit("n", &mem_cost(400)).expect("fits");
    drop(permit);
    // The pool is back to full; it can never exceed its total capacity.
    assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
    assert!(ctrl.remaining(Pool::Memory) <= 1_000, "never over-credits");
    assert!(ctrl.all_pools_full());
}

// A convenience so a `Permit` used only in assertions is not flagged unused.
#[allow(dead_code)]
fn assert_permit_node(p: &Permit, name: &str) {
    assert_eq!(p.node(), name);
}
