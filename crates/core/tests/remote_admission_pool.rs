//! **`Pool::RemoteSlots`** — the flat operator ceiling on in-flight remote work.
//! Written first, TDD.
//!
//! The admission controller sizes its pools from *this machine's* limits, which is
//! the right model for in-process work and the wrong one for a node running
//! somewhere else: a placed node consumes almost none of this machine's memory or
//! threads, and its real cost is cluster capacity dagr does not model. So a placed
//! node draws **one remote slot** and a near-zero local cost, and the remote pool
//! is a **flat operator ceiling** — not an attempt to mirror a cluster quota. The
//! cluster remains responsible for its own capacity.
//!
//! Every assertion here is against the **admission ledger** — counts, permits, and
//! the bootstrap over-demand check — never against timing.

use dagr_core::admission::{
    AdmissionController, PlacementHandling, Pool, PoolCapacities, PoolCost,
};
use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::limits::detect_capacities;

/// A placed node's policy: a genuinely large declared local cost, so "its local
/// cost is near zero once placed" is a visible change rather than a tautology.
fn placed_policy() -> NodePolicy {
    NodePolicy::new()
        .working_memory(64 * 1024 * 1024 * 1024)
        .compute_threads(8)
        .blocking_threads(4)
        .placement(Placement::new().cpu("500m").memory("64Gi"))
}

/// The same declared cost with **no** placement — the local node this ticket must
/// leave completely unchanged.
fn local_policy() -> NodePolicy {
    NodePolicy::new()
        .working_memory(64 * 1024 * 1024 * 1024)
        .compute_threads(8)
        .blocking_threads(4)
}

// ===========================================================================
// The pool exists, and it is the fourth one
// ===========================================================================

/// The remote pool joins the fixed pool set. `Pool::ALL` is the iteration order
/// every all-or-nothing acquisition and every bootstrap check walks, so the new
/// pool has to be in it or it is not a pool at all.
#[test]
fn remote_slots_is_a_pool_and_is_unconstrained_by_default() {
    assert!(
        Pool::ALL.contains(&Pool::RemoteSlots),
        "the remote pool must be in the fixed pool set: {:?}",
        Pool::ALL
    );
    assert_eq!(
        PoolCapacities::new().total(Pool::RemoteSlots),
        u64::from(u32::MAX),
        "an unpinned remote pool is unconstrained — dagr does not invent a cluster ceiling"
    );
    assert_eq!(
        PoolCapacities::new().remote_slots(2).total(Pool::RemoteSlots),
        2,
        "the operator's ceiling is the pool's total capacity"
    );
}

// ===========================================================================
// The cost mapping — one remote slot, near-zero local cost
// ===========================================================================

/// A placed node charged under an executor that **honours** its placement takes
/// exactly **one remote slot** and a near-zero local cost: no working memory and
/// no threads, because the attempt is not running in this process.
#[test]
fn a_placed_node_costs_one_remote_slot_and_no_local_working_capacity() {
    let cost = PoolCost::from_policy(placed_policy(), PlacementHandling::Honoured);
    assert_eq!(cost.remote_slot_count(), 1, "one placed node, one remote slot");
    assert_eq!(cost.working_memory_bytes(), 0);
    assert_eq!(cost.blocking_thread_count(), 0);
    assert_eq!(cost.compute_thread_count(), 0);
    assert_eq!(cost.demand_on(Pool::RemoteSlots), 1);
    assert_eq!(cost.demand_on(Pool::Memory), 0);
}

/// An **unplaced** node is charged exactly its declared vector and draws **no**
/// remote slot, whether or not the executor would honour a placement. Nothing
/// about an existing pipeline changes.
#[test]
fn an_unplaced_node_never_draws_a_remote_slot() {
    for handling in [PlacementHandling::Honoured, PlacementHandling::Ignored] {
        let cost = PoolCost::from_policy(local_policy(), handling);
        assert_eq!(cost.remote_slot_count(), 0);
        assert_eq!(cost, PoolCost::from_cost_vector(local_policy().cost()));
    }
}

/// **Recorded and ignored.** Under an executor that does not honour placement (the
/// local one), a placed node is charged its ordinary declared local cost — it
/// really is running in this process, and a ledger that pretended otherwise would
/// lie to the memory pool.
#[test]
fn under_a_non_honouring_executor_a_placed_node_pays_its_declared_local_cost() {
    let cost = PoolCost::from_policy(placed_policy(), PlacementHandling::Ignored);
    assert_eq!(
        cost,
        PoolCost::from_cost_vector(placed_policy().cost()),
        "the local executor charges the declared cost verbatim"
    );
    assert_eq!(cost.remote_slot_count(), 0);
}

// ===========================================================================
// The ceiling — asserted through the ledger, never through timing
// ===========================================================================

/// `--dagr.max-pods=2` with three placed nodes ready at once: two hold remote
/// slots, the third waits. When one releases, the third is admitted.
#[test]
fn a_two_slot_ceiling_admits_two_placed_nodes_and_holds_the_third() {
    let admission = AdmissionController::new(PoolCapacities::new().remote_slots(2));
    let cost = PoolCost::from_policy(placed_policy(), PlacementHandling::Honoured);

    for node in ["a", "b", "c"] {
        admission.offer(node, &cost);
    }
    let mut permits = admission.poll_admissions();
    assert_eq!(
        permits.len(),
        2,
        "the ceiling is two in-flight remote attempts, so only two are admitted"
    );
    assert_eq!(admission.counted(Pool::RemoteSlots), 2);
    assert_eq!(admission.remaining(Pool::RemoteSlots), 0);

    // The third is still waiting: nothing admits it while the pool is full.
    assert!(
        admission.poll_admissions().is_empty(),
        "a full remote pool admits nothing further"
    );

    // Release one, and exactly the third is admitted.
    drop(permits.pop().expect("two permits were held"));
    assert_eq!(admission.counted(Pool::RemoteSlots), 1);
    let next = admission.poll_admissions();
    assert_eq!(next.len(), 1, "the waiting node is admitted on release");
    assert_eq!(admission.counted(Pool::RemoteSlots), 2);
}

/// A placed node is admitted under a memory ceiling that would reject it outright
/// as a local node — because its local cost is near zero. This is the whole point
/// of the separate pool.
#[test]
fn a_placed_node_is_admitted_under_a_memory_ceiling_that_rejects_it_locally() {
    // One mebibyte of memory: far below the node's 64 GiB declared working set.
    let caps = PoolCapacities::new().memory(1024 * 1024);
    let admission = AdmissionController::new(caps);

    let local = PoolCost::from_policy(placed_policy(), PlacementHandling::Ignored);
    assert!(
        admission.try_admit("as-local", &local).is_none(),
        "as a local node it does not fit the memory pool"
    );
    assert!(
        !admission.can_ever_fit(&local),
        "and it can never fit — the pool's total is smaller than its declared cost"
    );

    let remote = PoolCost::from_policy(placed_policy(), PlacementHandling::Honoured);
    assert!(
        admission.try_admit("as-placed", &remote).is_some(),
        "placed, it draws no local working memory and is admitted"
    );
}

/// A run with **no** placed nodes never consults the remote pool: it stays at full
/// capacity throughout, and the ledger's behaviour is exactly what it was.
#[test]
fn with_no_placed_nodes_the_remote_pool_is_never_consulted() {
    let admission = AdmissionController::new(PoolCapacities::new().remote_slots(1));
    let cost = PoolCost::from_cost_vector(local_policy().cost());
    let permits: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|n| admission.try_admit(n, &cost).expect("unconstrained pools admit"))
        .collect();
    assert_eq!(
        admission.counted(Pool::RemoteSlots),
        0,
        "no unplaced node charges the remote pool, even three at once against a ceiling of one"
    );
    drop(permits);
    assert!(admission.all_pools_full());
}

// ===========================================================================
// Over-demand — refused at bootstrap, exactly as it is today
// ===========================================================================

/// A node demanding more remote slots than the ceiling can **ever** supply is
/// refused at bootstrap — the same terminal failure the memory and thread pools
/// produce, never a node stranded in the pending queue.
#[test]
fn a_remote_demand_beyond_the_ceiling_fails_bootstrap() {
    // An operator ceiling of zero: remote execution is switched off, so a placed
    // node can never be admitted no matter how much capacity releases.
    let caps = PoolCapacities::new().remote_slots(0);
    let cost = PoolCost::from_policy(placed_policy(), PlacementHandling::Honoured);

    let failure = detect_capacities(&caps, &[("placed".to_string(), cost)])
        .expect_err("a placed node against a zero remote ceiling must fail bootstrap");
    let errors = failure.errors();
    assert_eq!(errors.len(), 1, "one offending (node, pool) pair");
    assert_eq!(errors[0].node(), "placed");
    assert_eq!(errors[0].pool(), Pool::RemoteSlots);
    assert_eq!(errors[0].declared_cost(), 1);
    assert_eq!(errors[0].capacity(), 0);

    // And the driver-level guard agrees, with an honest reason naming the pool.
    let admission = AdmissionController::new(caps);
    assert!(!admission.can_ever_fit(&cost));
    let reason = admission
        .over_demand_reason(&cost)
        .expect("an over-demand has a reason");
    assert!(
        reason.contains("RemoteSlots") && reason.contains("remote slots"),
        "the refusal names the remote pool and its unit: {reason}"
    );
}

/// The bootstrap check is unchanged for every pipeline that declares no placement:
/// an unpinned remote pool admits any number of them.
#[test]
fn the_bootstrap_check_is_unchanged_for_unplaced_pipelines() {
    let caps = PoolCapacities::new();
    let costs = vec![
        (
            "a".to_string(),
            PoolCost::from_policy(local_policy(), PlacementHandling::Ignored),
        ),
        (
            "b".to_string(),
            PoolCost::from_policy(local_policy(), PlacementHandling::Honoured),
        ),
    ];
    assert!(detect_capacities(&caps, &costs).is_ok());
}
