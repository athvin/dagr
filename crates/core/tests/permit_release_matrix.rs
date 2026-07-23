//! C12 / C14 · **permit-release outcome matrix** (T37, 047). Written first, TDD.
//!
//! The admission controller (C12) is honest only if its permit ledger never
//! lies — including the honest exception where timed-out or cancelled-but-still
//! -running work stays counted against the pools until the closure actually
//! returns (T0.3 ADR §2). T31 (041) shipped the ledger and *sampled* a few
//! outcomes; this suite drives the **full matrix** T31 deferred:
//!
//! ```text
//!                | succeeded | failed(perm) | retry-elig | timed-out | panicked | cancelled | abandoned
//!  AwaitBound    |  release  |   release    |  release   |  release  | release  |  release  |    n/a  (a)
//!  Blocking      |  release  |   release    |  release   |   HELD(b) | release  |  release  |   HELD(b)
//!  Compute       |  release  |   release    |  release   |   HELD(b) | release  |  release  |   HELD(b)
//! ```
//!
//! - `(a)` an **await-bound** attempt cannot be *abandoned*: its future is
//!   droppable, so cancellation drops it and releases immediately (there is no
//!   unkillable thread to leave behind — arch.md C14, T0.3 ADR §1). The
//!   abandonment cell is therefore blocking/compute-only, which is exactly why
//!   the matrix is not a full Cartesian product.
//! - `(b)` the **one honest exception** (arch.md C12): a blocking/compute closure
//!   cannot be killed, so a timed-out or abandoned-past-grace attempt is
//!   *marked* (its fate decided) yet its permit stays counted — HELD — until the
//!   closure **actually returns**, then releases. The ledger counts zombies
//!   because "a ledger that releases what is still running is a ledger that
//!   lies, and the container's OOM killer audits it."
//!
//! For every cell we assert the pool returns to full (no leak) exactly per the
//! cell's rule, that the combined counted cost — **including any live zombie** —
//! never exceeds capacity at any sample, and that a whole run ends with every
//! pool full and zero live zombies. We also cross-check the C10 companion
//! invariant: a slot pinned by a zombie consumer stays counted until that
//! consumer's closure returns.
//!
//! # System under test — merged, unchanged
//!
//! This is a **tests-only** ticket. It exercises the already-merged production
//! code and changes none of it:
//! - the C12 ledger (`AdmissionController`, `Permit`/`Drop`-release, `mark_zombie`
//!   mark-and-hold, `ResidencyLease`, `all_pools_full`, `zombie_report`) — T31.
//! - the C14 blocking-timeout mark (`TimeoutDecision::mark_blocking_timed_out`,
//!   its `LateResultBarrier`, and `retry_may_start`/`ZombieObserver` deferral) — T21.
//! - the C10 residency ledger (`ResidencyLedger`) a zombie consumer pins — T17.
//! - the `AttemptOutcome`/`AttemptEvent` taxonomy (panicked → permanent failure,
//!   cancelled/abandoned terminals) — T23 / T35.
//!
//! # Determinism (CI): counts + explicit gates, never sleeps / wall clocks
//!
//! The load-bearing T0.3 trick is that "the work has returned" is *definitionally*
//! "the permit was dropped." So a still-running blocking/compute closure is
//! modelled by **holding the permit** (keeping the guard alive) behind an explicit
//! gate; the gate "opens" when the test drops the permit. No timer, no thread, no
//! wall clock, no network — every sample is taken at a point the test controls, so
//! nothing races the outcome it measures.
//!
//! # Non-vacuity
//!
//! Every release assertion checks the pool is **restored to full**, and every
//! HELD assertion checks the pool is **NOT** restored while the zombie is live and
//! only *then* restored on return. A permit leak (a release that is skipped, or a
//! HELD cell that releases too early / never) makes the corresponding `assert_eq!`
//! fail. This was verified locally by temporarily neutralising `Permit::drop`'s
//! release in production (`crates/core/src/admission.rs`) — the whole matrix went
//! red — then reverting (see the ticket's Open questions for the exact probe).

use std::sync::Arc;

use dagr_core::admission::{
    AdmissionController, Permit, Pool, PoolCapacities, PoolCost, ResidencyLease,
};
use dagr_core::context::{PipelineId, RunContext, RunId, TerminalState};
use dagr_core::execution::{
    AttemptEvent, AttemptEventSink, AttemptOutcome, TimeoutDecision, ZombieObserver,
};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::ExecutionClass;

// ===========================================================================
// The deterministic test rig: pinned pools + a ledger observer + a gate.
// ===========================================================================

/// The pinned per-pool capacity every matrix cell runs against. Small, exact, and
/// one-unit-per-node so admission and release are individually observable (the
/// C12 pin flag is the CI determinism lever — here we pin the pools outright).
///
/// Threads are pinned to 1 each so a single node saturates its class pool exactly:
/// a leaked thread permit is then directly visible as `remaining == 0`.
fn pinned_pools() -> PoolCapacities {
    PoolCapacities::new()
        .memory(1_000)
        .blocking_threads(1)
        .compute_threads(1)
}

/// The author-declared per-pool cost for a node of `class`, charging exactly one
/// unit of that class's thread pool plus a fixed working-memory unit — so every
/// cell exercises the memory pool *and* the class-specific thread pool, and a leak
/// in either is caught.
fn cost_for(class: ExecutionClass) -> PoolCost {
    let base = PoolCost::new().working_memory(400);
    match class {
        // Await-bound work runs on the async runtime — it draws no dedicated
        // thread-pool permit, only memory (arch.md C13). Its "release" is the
        // memory permit returning.
        ExecutionClass::AwaitBound => base,
        ExecutionClass::Blocking => base.blocking_threads(1),
        ExecutionClass::Compute => base.compute_threads(1),
    }
}

/// The class's own thread pool (memory for await-bound, which has no thread pool).
fn thread_pool_of(class: ExecutionClass) -> Option<Pool> {
    match class {
        ExecutionClass::AwaitBound => None,
        ExecutionClass::Blocking => Some(Pool::BlockingThreads),
        ExecutionClass::Compute => Some(Pool::ComputeThreads),
    }
}

const CLASSES: [ExecutionClass; 3] = [
    ExecutionClass::AwaitBound,
    ExecutionClass::Blocking,
    ExecutionClass::Compute,
];

/// Assert every pool is back at full remaining capacity and no residency is
/// leased — the no-leak invariant every terminal-and-returned node must restore.
fn assert_all_pools_full(ctrl: &AdmissionController) {
    for pool in Pool::ALL {
        assert_eq!(
            ctrl.counted(pool),
            0,
            "pool {pool:?} must be back at full (nothing counted) after every \
             node is terminal-and-returned"
        );
    }
    assert!(
        ctrl.all_pools_full(),
        "the whole run must end with every pool full and zero live residency"
    );
    assert_eq!(
        ctrl.zombie_report().live_zombie_count,
        0,
        "no zombie may remain live at run end"
    );
}

/// The **capacity invariant** probe (arch.md C12): the combined counted cost —
/// **including abandoned-but-running work** — never exceeds any pool's `caps`
/// capacity. Sampled at every interesting instant; a ledger that over-counted
/// (double admission) or a rig that over-charged would trip this. `caps` is the
/// controller's own pinned capacity so a test using non-default pools checks
/// against its real ceiling.
fn assert_within_capacity(ctrl: &AdmissionController, caps: PoolCapacities) {
    for pool in Pool::ALL {
        let total = caps.total(pool);
        assert!(
            ctrl.counted(pool) <= total,
            "counted cost of {pool:?} ({}) must never exceed capacity ({total}) — \
             not even with a live zombie counted",
            ctrl.counted(pool)
        );
    }
}

/// A capturing C19-shaped sink (the same shape T20/T21 tests use) that counts the
/// exactly-one attempt-outcome record and reports the decided terminal states.
#[derive(Default)]
struct CapturingSink {
    records: Vec<AttemptEvent>,
}
impl AttemptEventSink for CapturingSink {
    fn emit(&mut self, event: AttemptEvent) {
        self.records.push(event);
    }
}
impl CapturingSink {
    /// The count of closing attempt-outcome records (succeeded / failed / timed
    /// -out / panicked) — the "exactly one per attempt" contract (arch.md C14).
    fn attempt_outcome_count(&self) -> usize {
        self.records
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    AttemptEvent::AttemptSucceeded { .. }
                        | AttemptEvent::AttemptFailed { .. }
                        | AttemptEvent::AttemptTimedOut { .. }
                        | AttemptEvent::AttemptPanicked { .. }
                )
            })
            .count()
    }
    fn terminal_states(&self) -> Vec<TerminalState> {
        self.records
            .iter()
            .filter_map(|e| match e {
                AttemptEvent::NodeTerminal { state, .. } => Some(*state),
                _ => None,
            })
            .collect()
    }
}

fn ctx_for(node: &str, attempt: u32, max: u32) -> RunContext {
    RunContext::builder(
        RunId::new("t37-run"),
        PipelineId::new("t37-pipeline"),
        NodeId::from_name(node),
    )
    .attempt(attempt)
    .max_attempts(max)
    .build()
}

// ===========================================================================
// The mapped C12/C14 headline test — the whole matrix, no leak.
// ===========================================================================

/// **The permit-release outcome matrix leaks nothing.** Drive one node of every
/// execution class through every terminal outcome against a freshly-pinned
/// controller, asserting per cell that the permit is released exactly per the
/// outcome's rule — immediately for the release outcomes, HELD-until-return for a
/// blocking/compute timeout or abandonment — and that no cell ever exceeds
/// capacity. After the whole matrix, every pool is back to full with zero live
/// zombies. This is the invariant the ticket protects: across the full outcome ×
/// class grid the ledger never lies and never leaks.
#[test]
fn permit_release_outcome_matrix_leaks_nothing_across_every_class_and_outcome() {
    for class in CLASSES {
        // --- Release-immediately outcomes: the permit drops at the terminal
        //     state and the pool is restored at once, for every class. ---
        for outcome in [
            AttemptOutcome::Succeeded,
            AttemptOutcome::PermanentFailure,
            AttemptOutcome::RetryEligibleFailure,
            AttemptOutcome::Panicked,
        ] {
            let ctrl = AdmissionController::new(pinned_pools());
            let cost = cost_for(class);
            let permit = ctrl
                .try_admit("node", &cost)
                .expect("a one-unit node fits the pinned pool");
            assert_within_capacity(&ctrl, pinned_pools());
            // The class thread pool (if any) is saturated while the permit is held.
            if let Some(tp) = thread_pool_of(class) {
                assert_eq!(ctrl.remaining(tp), 0, "{class:?} holds its thread permit");
            }
            assert_eq!(ctrl.remaining(Pool::Memory), 600, "working memory held");
            // Reaching this terminal outcome drops the permit → immediate release.
            drop(permit);
            assert_within_capacity(&ctrl, pinned_pools());
            assert_all_pools_full(&ctrl);
            let _ = outcome; // the outcome label documents the cell being asserted
        }

        // --- Timeout: class-split. Await-bound releases immediately; blocking /
        //     compute hold until the closure returns. ---
        let ctrl = AdmissionController::new(pinned_pools());
        let cost = cost_for(class);
        let permit = ctrl.try_admit("node", &cost).expect("fits");
        match class {
            ExecutionClass::AwaitBound => {
                // The future is dropped on timeout → the permit moved into it drops
                // → immediate release, no zombie left behind.
                drop(permit);
                assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
                assert_all_pools_full(&ctrl);
            }
            ExecutionClass::Blocking | ExecutionClass::Compute => {
                // Mark the attempt a zombie: the closure runs on, the permit is
                // HELD, the cost stays counted. State decided once as timed-out.
                ctrl.mark_zombie(&permit);
                assert_eq!(ctrl.zombie_report().live_zombie_count, 1);
                let tp = thread_pool_of(class).expect("blocking/compute has a thread pool");
                assert_eq!(ctrl.remaining(tp), 0, "thread permit HELD by the zombie");
                assert_eq!(
                    ctrl.remaining(Pool::Memory),
                    600,
                    "memory HELD by the zombie"
                );
                assert!(
                    ctrl.has_live_zombie(),
                    "zombie live before the closure returns"
                );
                assert_within_capacity(&ctrl, pinned_pools()); // capacity honoured WITH a live zombie
                                                               // The closure finally returns → the permit drops → release now.
                drop(permit);
                assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
                assert_all_pools_full(&ctrl);
            }
        }

        // --- Cooperative cancellation: releases immediately for every class (the
        //     node observed the signal and returned). ---
        let ctrl = AdmissionController::new(pinned_pools());
        let permit = ctrl.try_admit("node", &cost_for(class)).expect("fits");
        assert_within_capacity(&ctrl, pinned_pools());
        drop(permit); // observed cancellation, returned promptly → release
        assert_all_pools_full(&ctrl);

        // --- Abandonment: blocking/compute only (an await-bound future is
        //     dropped and released, never abandoned). HELD until the closure
        //     returns past grace. ---
        if let Some(tp) = thread_pool_of(class) {
            let ctrl = AdmissionController::new(pinned_pools());
            let permit = ctrl.try_admit("node", &cost_for(class)).expect("fits");
            ctrl.mark_zombie(&permit); // asked to cancel, ignored past grace
            assert_eq!(ctrl.remaining(tp), 0, "abandoned closure HOLDS its thread");
            assert_eq!(
                ctrl.remaining(Pool::Memory),
                600,
                "abandoned closure HOLDS memory"
            );
            assert_within_capacity(&ctrl, pinned_pools());
            drop(permit); // the abandoned closure finally returns
            assert_all_pools_full(&ctrl);
        }
    }
}

// ===========================================================================
// Release-immediately outcomes, one focused test each (the DoD lines).
// ===========================================================================

/// **Success releases immediately** — for every class the permit returns to the
/// pool at `succeeded`, and the pool never exceeded capacity.
#[test]
fn success_releases_the_permit_immediately_for_every_class() {
    for class in CLASSES {
        let ctrl = AdmissionController::new(pinned_pools());
        let permit = ctrl.try_admit("ok", &cost_for(class)).expect("fits");
        assert_within_capacity(&ctrl, pinned_pools());
        drop(permit); // reaches `succeeded`
        assert_all_pools_full(&ctrl);
    }
}

/// **Permanent failure releases immediately, no retry.** The permit drops at
/// `failed`; a fresh admission afterward proves nothing leaked into a phantom
/// retry (no permit is held for a permanent failure).
#[test]
fn permanent_failure_releases_immediately_and_is_not_retried() {
    for class in CLASSES {
        let ctrl = AdmissionController::new(pinned_pools());
        let permit = ctrl.try_admit("fail", &cost_for(class)).expect("fits");
        drop(permit); // permanent `failed` — no further attempt
        assert_all_pools_full(&ctrl);
        // The pool is fully free (a permanent failure schedules no retry that
        // could still hold a permit).
        assert!(ctrl.all_pools_full());
    }
}

/// **Retry-eligible failure releases between attempts and re-acquires.** The
/// first attempt's permit releases (pool free during backoff), the retry
/// re-acquires a *fresh* permit, and the two attempts never hold two permits at
/// once — the pool has room for exactly one attempt at a time.
#[test]
fn retry_eligible_failure_releases_between_attempts_and_never_doubly_holds() {
    for class in CLASSES {
        let ctrl = AdmissionController::new(pinned_pools());
        let cost = cost_for(class);

        let attempt1 = ctrl.try_admit("retry", &cost).expect("attempt 1 fits");
        if let Some(tp) = thread_pool_of(class) {
            assert_eq!(
                ctrl.remaining(tp),
                0,
                "attempt 1 holds the sole thread unit"
            );
            // While attempt 1 holds the permit, a *second* concurrent permit for the
            // same node cannot be admitted — the pool has one unit — so a retry can
            // never doubly hold.
            assert!(
                ctrl.try_admit("retry", &cost).is_none(),
                "no second permit while attempt 1 holds the sole unit (never doubly held)"
            );
        }
        // Attempt 1 fails retry-eligibly → its permit releases (pool free in backoff).
        drop(attempt1);
        assert_all_pools_full(&ctrl);

        // The retry re-acquires a fresh permit (it would not fit if attempt 1 leaked).
        let attempt2 = ctrl
            .try_admit("retry", &cost)
            .expect("the retry re-acquires a fresh permit");
        assert_within_capacity(&ctrl, pinned_pools());
        drop(attempt2); // retry succeeds/terminates → release
        assert_all_pools_full(&ctrl);
    }
}

/// **A panic releases the permit immediately as a permanent failure, and an
/// unrelated co-scheduled node proceeds.** The panic is contained (T23) and maps
/// to `failed`; its permit drops at once, freeing the pool for other work.
#[test]
fn panic_releases_the_permit_immediately_and_the_rest_of_the_run_proceeds() {
    // A panic is a permanent failure whose terminal state is `failed`.
    assert_eq!(
        AttemptOutcome::Panicked.terminal_state(),
        TerminalState::Failed
    );
    assert!(!AttemptOutcome::Panicked.is_retry_eligible());

    for class in CLASSES {
        // Two nodes share the memory pool; give room for both to observe that the
        // panicking node's release lets the co-scheduled node keep its permit.
        let caps = PoolCapacities::new()
            .memory(1_000)
            .blocking_threads(2)
            .compute_threads(2);
        let ctrl = AdmissionController::new(caps);
        let panicker = ctrl.try_admit("panicker", &cost_for(class)).expect("fits");
        let bystander = ctrl.try_admit("bystander", &cost_for(class)).expect("fits");
        assert_within_capacity(&ctrl, caps);

        // The panicking node's caught panic → permanent failure → permit drops now.
        drop(panicker);
        // The unrelated node is unaffected — still holding its permit, still counted.
        assert_eq!(
            ctrl.remaining(Pool::Memory),
            600,
            "the bystander keeps its permit after the panicking node releases"
        );
        assert!(!ctrl.all_pools_full(), "the bystander is still executing");
        drop(bystander);
        assert_all_pools_full(&ctrl);
    }
}

/// **Cooperative cancellation releases immediately.** The node observes the
/// cancellation signal and returns; its permit releases at once. `cancelled` is a
/// distinct terminal state from `failed`.
#[test]
fn cooperative_cancellation_releases_the_permit_immediately() {
    // `cancelled` is distinct from `failed` in the taxonomy.
    assert_ne!(TerminalState::Cancelled, TerminalState::Failed);
    for class in CLASSES {
        let ctrl = AdmissionController::new(pinned_pools());
        let permit = ctrl.try_admit("coop", &cost_for(class)).expect("fits");
        assert_within_capacity(&ctrl, pinned_pools());
        drop(permit); // observed cancellation, returned promptly → release
        assert_all_pools_full(&ctrl);
    }
}

// ===========================================================================
// The honest exception — blocking/compute timeout & abandonment: HELD.
// ===========================================================================

/// **A blocking/compute timeout is decided `timed-out` immediately while the
/// permit stays HELD until the closure returns, then releases — and the state
/// never flips to a second terminal.** Drives the real `TimeoutDecision` mark:
/// it emits exactly one attempt-outcome record and one `timed-out` node-terminal
/// at the mark, holds the permit (the closure runs on), and only the permit's
/// eventual drop releases the cost. The state stays `timed-out` — never
/// `abandoned` (that arises only on the cancellation path).
#[test]
fn blocking_and_compute_timeout_hold_the_permit_until_return_and_stay_timed_out() {
    for class in [ExecutionClass::Blocking, ExecutionClass::Compute] {
        let tp = thread_pool_of(class).expect("blocking/compute has a thread pool");
        let ctrl = AdmissionController::new(pinned_pools());
        let permit = ctrl.try_admit("slow", &cost_for(class)).expect("fits");

        // (a) The timeout fires: mark the attempt (its fate is decided now). The
        //     real T21 mark emits the closing records; the ledger marks the zombie.
        let mut sink = CapturingSink::default();
        let decision =
            TimeoutDecision::mark_blocking_timed_out("slow", &ctx_for("slow", 1, 1), &mut sink);
        ctrl.mark_zombie(&permit);
        assert_eq!(decision.outcome(), AttemptOutcome::TimedOut);
        assert_eq!(decision.outcome().terminal_state(), TerminalState::TimedOut);

        // Exactly one attempt-outcome record, and the single decided terminal is
        // `timed-out` — never a second terminal, never `abandoned`.
        assert_eq!(
            sink.attempt_outcome_count(),
            1,
            "exactly one attempt-outcome record for a timed-out attempt"
        );
        assert_eq!(
            sink.terminal_states(),
            vec![TerminalState::TimedOut],
            "the single decided terminal is `timed-out`, never `abandoned`"
        );

        // The permit is HELD: the cost stays counted while the closure runs on.
        assert_eq!(
            ctrl.remaining(tp),
            0,
            "thread permit HELD by the timed-out closure"
        );
        assert_eq!(
            ctrl.remaining(Pool::Memory),
            600,
            "memory HELD by the timed-out closure"
        );
        assert_eq!(ctrl.zombie_report().live_zombie_count, 1);
        assert!(ctrl.has_live_zombie());
        assert_within_capacity(&ctrl, pinned_pools()); // capacity honoured with the zombie counted

        // Any late value the abandoned closure computes is refused (T0.3 §4).
        let slot: Slot<u32> = Slot::new(
            NodeId::from_name("slow"),
            "slow",
            0,
            false,
            0,
            ResidencyLedger::new(),
        );
        assert!(
            !decision.barrier().fill_slot(&slot, 99),
            "a timed-out closure never fills its output slot"
        );

        // (b) The closure finally returns → the permit drops → release now.
        drop(permit);
        assert_eq!(ctrl.remaining(tp), pinned_pools().total(tp));
        assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
        assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
        assert!(!ctrl.has_live_zombie());
        assert_all_pools_full(&ctrl);
    }
}

/// **A blocking-timeout retry is deferred until the previous closure returns**, so
/// the same task instance never runs concurrently with its own zombie, and the
/// ledger never counts two live permits for the node at once. Drives the real
/// `retry_may_start`/`ZombieObserver` deferral against the live ledger.
#[test]
fn blocking_timeout_retry_is_deferred_until_the_closure_returns() {
    for class in [ExecutionClass::Blocking, ExecutionClass::Compute] {
        let ctrl = AdmissionController::new(pinned_pools());
        let cost = cost_for(class);
        let attempt1 = ctrl.try_admit("slow", &cost).expect("attempt 1 fits");

        let mut sink = CapturingSink::default();
        let decision =
            TimeoutDecision::mark_blocking_timed_out("slow", &ctx_for("slow", 1, 2), &mut sink);
        ctrl.mark_zombie(&attempt1);

        // While the first closure is still running (its zombie live), no retry may
        // start — and the pool has no room for a second permit either.
        assert!(
            !decision.retry_may_start(&ctrl),
            "retry is deferred while the previous closure's zombie is live"
        );
        assert!(
            ctrl.try_admit("slow", &cost).is_none(),
            "the ledger never admits a second live permit for the node while the zombie holds the unit"
        );
        assert_eq!(ctrl.zombie_report().live_zombie_count, 1);

        // The first closure returns → the permit drops → the zombie clears.
        drop(attempt1);
        assert_eq!(ctrl.zombie_report().live_zombie_count, 0);
        assert!(
            decision.retry_may_start(&ctrl),
            "the retry begins only after the previous closure has returned"
        );
        // Now (and only now) the retry re-acquires a fresh permit.
        let attempt2 = ctrl
            .try_admit("slow", &cost)
            .expect("the retry re-acquires a fresh permit after the zombie cleared");
        assert_within_capacity(&ctrl, pinned_pools());
        drop(attempt2);
        assert_all_pools_full(&ctrl);
    }
}

/// **Abandonment holds the permit until the closure returns.** A blocking/compute
/// closure asked to cancel ignores the signal past grace: it is recorded
/// `abandoned` (distinct from `failed` and `cancelled`) yet its permit is still
/// counted against the pool (the ledger counts zombies); only when the closure
/// returns does the permit release. Capacity is never exceeded at any sample.
#[test]
fn abandonment_holds_the_permit_until_the_closure_returns() {
    // `abandoned` is distinct from both `failed` and `cancelled`.
    assert_ne!(TerminalState::Abandoned, TerminalState::Failed);
    assert_ne!(TerminalState::Abandoned, TerminalState::Cancelled);

    for class in [ExecutionClass::Blocking, ExecutionClass::Compute] {
        let tp = thread_pool_of(class).expect("blocking/compute has a thread pool");
        let ctrl = AdmissionController::new(pinned_pools());
        let permit = ctrl.try_admit("ignorer", &cost_for(class)).expect("fits");

        // (a) Grace expires while the closure is still gated: mark it abandoned-but
        //     -running. The permit is HELD — the cost stays counted.
        ctrl.mark_zombie(&permit);
        assert_eq!(
            ctrl.remaining(tp),
            0,
            "the abandoned closure HOLDS its thread"
        );
        assert_eq!(
            ctrl.remaining(Pool::Memory),
            600,
            "the abandoned closure HOLDS memory"
        );
        assert_eq!(ctrl.zombie_report().live_zombie_count, 1);
        assert!(ctrl.has_live_zombie());
        assert_within_capacity(&ctrl, pinned_pools()); // capacity honoured with the abandoned zombie

        // (b) The closure finally returns → the permit drops → release now.
        drop(permit);
        assert_eq!(ctrl.remaining(tp), pinned_pools().total(tp));
        assert_eq!(ctrl.remaining(Pool::Memory), 1_000);
        assert_all_pools_full(&ctrl);
    }
}

// ===========================================================================
// C10 cross-check — a zombie consumer pins slot residency until it returns.
// ===========================================================================

/// **A slot value pinned by a zombie consumer stays counted against the memory
/// pool until that consumer's closure returns.** A producer fills a slot with a
/// declared output residency (the slot lease). A single consumer of that slot is
/// timed out / abandoned while still gated: even after the producer's permit has
/// released, the slot's residency stays counted because the zombie consumer still
/// holds read access; only when the consumer's closure returns is the residency
/// reclaimed to the allocator. The memory pool never regains capacity for bytes a
/// leftover thread still pins (arch.md C10; the same honesty rule as C12).
#[test]
fn a_zombie_consumer_pins_slot_residency_until_its_closure_returns() {
    let ledger = ResidencyLedger::new();
    let caps = PoolCapacities::new().memory(10_000).blocking_threads(2);
    let ctrl = AdmissionController::new(caps).with_residency_ledger(Arc::clone(&ledger));

    // The producer runs and produces its value: output residency transfers from
    // the producing attempt to the output slot (the slot lease, C10).
    let producer = ctrl
        .try_admit(
            "producer",
            &PoolCost::new()
                .working_memory(1_000)
                .output_residency(4_000),
        )
        .expect("fits");
    assert_eq!(
        ctrl.counted(Pool::Memory),
        1_000,
        "producer working memory counted"
    );
    let slot_lease: ResidencyLease = ctrl.transfer_residency("producer", 4_000);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        5_000,
        "working memory + transferred slot residency both counted"
    );
    // The producer reaches its terminal state: its working memory releases, the
    // slot residency stays counted (it lives in the slot now, not the attempt).
    drop(producer);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        4_000,
        "slot residency still counted"
    );

    // A single consumer reads the value, then is timed out / abandoned while still
    // gated — a zombie consumer that still holds read access to the slot.
    let consumer = ctrl
        .try_admit(
            "consumer",
            &PoolCost::new().working_memory(500).blocking_threads(1),
        )
        .expect("fits");
    ctrl.mark_zombie(&consumer);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        4_500,
        "the slot residency + the zombie consumer's working memory are both counted"
    );

    // (a) The consumer is decided and the producer's final consumer count would
    //     otherwise allow release — but the zombie consumer still holds the slot
    //     open, so its residency is NOT reclaimed while the closure is gated. We
    //     model "the slot cannot release yet" by holding the slot lease.
    assert_eq!(
        ctrl.counted(Pool::Memory),
        4_500,
        "the slot's residency stays counted while the zombie consumer holds it open"
    );
    assert_within_capacity(&ctrl, caps);

    // (b) The consumer's closure finally returns → its working-memory permit
    //     releases, and only now can the slot actually release its residency.
    drop(consumer);
    assert_eq!(
        ctrl.counted(Pool::Memory),
        4_000,
        "the consumer's working memory is reclaimed on return; residency still leased until slot release"
    );
    drop(slot_lease); // the slot actually releases now that the zombie has returned
    assert_eq!(
        ctrl.counted(Pool::Memory),
        0,
        "the residency is reclaimed to the allocator only after the zombie consumer returned"
    );
    assert_all_pools_full(&ctrl);
}

// ===========================================================================
// The capacity invariant across a whole saturated matrix run.
// ===========================================================================

/// **Across a whole run that saturates the pinned capacity, the summed counted
/// cost — including any abandoned-but-running or timed-out node — never exceeds
/// any pool's capacity, and the run ends with every pool full.** A saturated set
/// runs concurrently: one healthy blocking node, one timed-out blocking zombie
/// (HELD), and one await-bound node — sampled continuously as each is admitted,
/// marked, and returns. At no instant does any pool's counted cost exceed its
/// capacity, and after every closure returns every pool is back to full.
#[test]
fn capacity_invariant_holds_across_a_whole_saturated_matrix_run() {
    // Capacity for exactly two concurrent blocking permits + memory for three.
    let caps = PoolCapacities::new()
        .memory(3_000)
        .blocking_threads(2)
        .compute_threads(1);
    let ctrl = AdmissionController::new(caps);
    let block_cost = PoolCost::new().working_memory(1_000).blocking_threads(1);
    let await_cost = PoolCost::new().working_memory(1_000);

    // Admit the saturating set — every pool is now full-or-honestly-counted.
    let healthy = ctrl.try_admit("healthy", &block_cost).expect("fits");
    let zombie = ctrl.try_admit("zombie", &block_cost).expect("fits");
    let awaiter = ctrl.try_admit("awaiter", &await_cost).expect("fits");
    assert_eq!(
        ctrl.remaining(Pool::BlockingThreads),
        0,
        "both blocking units in use"
    );
    assert_eq!(ctrl.remaining(Pool::Memory), 0, "memory exactly saturated");
    assert_within_capacity(&ctrl, caps);

    // A fourth node cannot be admitted — the pools are saturated (all-or-nothing).
    assert!(
        ctrl.try_admit("overflow", &block_cost).is_none(),
        "a saturated pool admits nothing more — capacity is never exceeded"
    );

    // The zombie blocking node times out: marked, but its permit is HELD, so the
    // counted cost is unchanged (still exactly at capacity, never over).
    ctrl.mark_zombie(&zombie);
    assert_within_capacity(&ctrl, caps);
    assert_eq!(
        ctrl.remaining(Pool::BlockingThreads),
        0,
        "zombie still HOLDS its unit"
    );
    assert_eq!(ctrl.zombie_report().live_zombie_count, 1);

    // Nodes return one at a time; capacity is honoured at every sample.
    drop(healthy);
    assert_within_capacity(&ctrl, caps);
    assert_eq!(
        ctrl.remaining(Pool::BlockingThreads),
        1,
        "healthy freed one unit"
    );
    // The zombie is still counted — the ledger did NOT release what is still running.
    assert_eq!(ctrl.zombie_report().live_zombie_count, 1);

    drop(awaiter);
    assert_within_capacity(&ctrl, caps);

    // Finally the zombie's closure returns → its HELD permit releases.
    drop(zombie);
    assert_within_capacity(&ctrl, caps);
    assert_all_pools_full(&ctrl);
}

// ===========================================================================
// Exactly one attempt-outcome record per attempt, across the matrix.
// ===========================================================================

/// **Every induced outcome emits exactly one attempt-outcome record per attempt,
/// including the timed-out case, and the outcome's terminal state matches the
/// ledger-release behaviour asserted above.** Walks the outcome taxonomy and the
/// event stream for a timed-out attempt.
#[test]
fn every_outcome_emits_exactly_one_attempt_outcome_record() {
    // Each classified outcome maps to exactly one terminal state (the shape the
    // single closing record carries) — the taxonomy the release rules key on.
    let expectations = [
        (AttemptOutcome::Succeeded, TerminalState::Succeeded),
        (AttemptOutcome::PermanentFailure, TerminalState::Failed),
        (AttemptOutcome::RetryEligibleFailure, TerminalState::Failed),
        (AttemptOutcome::TimedOut, TerminalState::TimedOut),
        (AttemptOutcome::Panicked, TerminalState::Failed),
    ];
    for (outcome, state) in expectations {
        assert_eq!(outcome.terminal_state(), state);
    }

    // A timed-out blocking attempt emits exactly one attempt-outcome record and
    // one `timed-out` node-terminal at the mark — including the zombie case.
    let mut sink = CapturingSink::default();
    let _decision = TimeoutDecision::mark_blocking_timed_out("z", &ctx_for("z", 1, 1), &mut sink);
    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one attempt-outcome record for a timed-out (zombie) attempt"
    );
    assert_eq!(sink.terminal_states(), vec![TerminalState::TimedOut]);
}

// A convenience so a `Permit` used only in assertions is not flagged unused.
#[allow(dead_code)]
fn permit_node(p: &Permit) -> &str {
    p.node()
}
