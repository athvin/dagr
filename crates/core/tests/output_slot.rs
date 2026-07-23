//! Output-slot behavioral tests — ticket T17 (027). Written first, TDD.
//!
//! These exercise the **real** C10 output-slot substrate in
//! [`dagr_core::slot`]: a typed, once-writable slot per node, assembly-time
//! typed consumer references (lookup-free, type-check-free reads), the T0.2
//! three-mode delivery (owned move / shared read / clone-on-read), zombie-aware
//! release gated on *terminal-and-returned*, single-count output residency with
//! peak accounting hooks, the `retained` post-run redemption API, and the loud
//! read-before-fill framework defect that names the node.
//!
//! Governed by arch.md `### C10 · Output slot` and the T0.2 output-ownership ADR
//! (008). The **runner** that fills slots from real attempt outcomes is T20; the
//! authoritative hundred-node bounded-memory assertion is T26 — only a smaller
//! smoke test lives here.

use std::sync::Arc;

use dagr_core::handle::NodeId;
use dagr_core::slot::{RedeemError, ResidencyLedger, Slot, SlotRef};

// --- A deliberately non-`Clone` output type -------------------------------
// The T0.2 model must carry a non-`Clone` output through both the owned and the
// shared modes; only clone-on-read demands `Clone`.
#[derive(Debug, PartialEq, Eq)]
struct Payload {
    bytes: Vec<u8>,
}

impl Payload {
    fn of_len(n: usize) -> Self {
        Self {
            bytes: vec![7u8; n],
        }
    }
}

// A cloneable output for the clone-on-read scenarios.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Counter {
    n: u64,
}

/// Build a fresh slot for a named node with the given consumer count, retained
/// flag, and declared output residency in bytes, sharing one ledger.
fn slot_for<T: Send + Sync + 'static>(
    name: &str,
    consumers: u32,
    retained: bool,
    residency: u64,
    ledger: &Arc<ResidencyLedger>,
) -> Slot<T> {
    Slot::new(
        NodeId::from_name(name),
        name,
        consumers,
        retained,
        residency,
        Arc::clone(ledger),
    )
}

// ---------------------------------------------------------------------------
// Read before fill is a loud, node-named defect.
// ---------------------------------------------------------------------------
#[test]
fn read_before_fill_is_a_loud_node_named_defect() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("loader", 1, false, 0, &ledger);
    let consumer = slot.shared_ref();

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Reading an unfilled slot is a framework defect, not a task error.
        let _ = consumer.read();
    }))
    .expect_err("reading an unfilled slot must fail loudly");

    let msg = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_default();
    assert!(
        msg.contains("loader"),
        "the defect must name the offending node; got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Fill once, read the value through a consumer reference.
// ---------------------------------------------------------------------------
#[test]
fn fill_once_then_read_the_value() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 1, false, 0, &ledger);
    let consumer = slot.shared_ref();

    slot.fill(Payload::of_len(64)).expect("first fill succeeds");

    let value = consumer.read();
    assert_eq!(value.bytes.len(), 64);
    assert_eq!(&*value, &Payload::of_len(64));
}

// ---------------------------------------------------------------------------
// Second fill is rejected; the original value is unchanged.
// ---------------------------------------------------------------------------
#[test]
fn second_fill_is_rejected_original_unchanged() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 1, false, 0, &ledger);
    let consumer = slot.shared_ref();

    slot.fill(Payload::of_len(10)).expect("first fill succeeds");
    let err = slot
        .fill(Payload::of_len(999))
        .expect_err("a second fill must be refused (once-writable)");
    // The rejection returns the spurned value so the caller can discard it.
    assert_eq!(err.rejected().bytes.len(), 999);

    // The original value is unchanged.
    assert_eq!(consumer.read().bytes.len(), 10);
}

// ---------------------------------------------------------------------------
// Shared consumer reads, fails a retry-eligible attempt, and still finds its
// input on the next attempt. The slot is not released between attempts.
// ---------------------------------------------------------------------------
#[test]
fn shared_consumer_retry_finds_the_value_intact() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 1, false, 128, &ledger);
    let consumer = slot.shared_ref();
    slot.fill(Payload::of_len(64)).expect("fill");

    // Attempt 1: read the value, then "fail" a retry-eligible attempt. The
    // attempt did not reach a terminal state (a retry is coming), so no lease
    // is closed.
    {
        let v = consumer.read();
        assert_eq!(v.bytes.len(), 64);
    } // the Arc read-clone drops here; the slot still owns the value.

    // The slot is NOT released between attempts: residency stays counted.
    assert!(slot.is_filled());
    assert_eq!(ledger.current(), 128);

    // Attempt 2: the value is still present and readable.
    let v = consumer.read();
    assert_eq!(v.bytes.len(), 64);
}

// ---------------------------------------------------------------------------
// Release fires only after every consumer is terminal AND every closure has
// returned.
// ---------------------------------------------------------------------------
#[test]
fn release_fires_only_after_every_consumer_terminal_and_returned() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 2, false, 256, &ledger);
    let a = slot.shared_ref();
    let b = slot.shared_ref();
    slot.fill(Payload::of_len(200)).expect("fill");
    assert_eq!(ledger.current(), 256);

    // Consumer A enters, reads, reaches a terminal state, and its closure
    // returns (the lease guard drops).
    {
        let lease = a.enter();
        let _ = lease.read();
        // dropping the lease == the closure returned after a terminal decision.
    }
    // B is still in flight: the value stays reachable, residency stays counted.
    assert!(slot.is_filled());
    assert_eq!(ledger.current(), 256);

    // Drive B terminal and let its closure return.
    {
        let lease = b.enter();
        let _ = lease.read();
    }
    // Now every consumer is terminal-and-returned: the slot releases and the
    // memory returns to the allocator.
    assert!(!slot.is_filled());
    assert_eq!(ledger.current(), 0);
}

// ---------------------------------------------------------------------------
// Release waits on last read/return, not last terminal-signal.
// ---------------------------------------------------------------------------
#[test]
fn release_waits_on_closure_return_not_terminal_decision() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 1, false, 64, &ledger);
    let c = slot.shared_ref();
    slot.fill(Payload::of_len(64)).expect("fill");

    // The consumer enters (closure running) and its fate is decided terminal,
    // but its closure has NOT yet returned (the lease is still held).
    let mut lease = c.enter();
    lease.mark_terminal();

    // In the window between the terminal decision and the closure return the
    // value must NOT be reclaimed.
    assert!(slot.is_filled());
    assert_eq!(ledger.current(), 64);

    // The closure returns.
    drop(lease);

    assert!(!slot.is_filled());
    assert_eq!(ledger.current(), 0);
}

// ---------------------------------------------------------------------------
// Zombie consumer pins the value and its residency until the closure returns.
// ---------------------------------------------------------------------------
#[test]
fn zombie_consumer_pins_value_and_residency() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 1, false, 512, &ledger);
    let c = slot.shared_ref();
    slot.fill(Payload::of_len(400)).expect("fill");

    // The consumer is marked timed-out/abandoned — a terminal decision — but
    // its closure has NOT returned (abandoned-but-running: the lease is held).
    let mut lease = c.enter();
    lease.mark_terminal();

    // While the closure runs, the value is reachable and its residency stays
    // counted against the memory pool.
    assert!(slot.is_filled());
    let v = lease.read();
    assert_eq!(v.bytes.len(), 400);
    assert_eq!(ledger.current(), 512);

    // Only when the closure returns do both release.
    drop(lease);
    assert!(!slot.is_filled());
    assert_eq!(ledger.current(), 0);
}

// ---------------------------------------------------------------------------
// Retained value survives to run end and is redeemable.
// ---------------------------------------------------------------------------
#[test]
fn retained_value_survives_to_run_end_and_is_redeemable() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("kept", 1, true, 64, &ledger);
    let handle = slot.redemption_handle();
    let c = slot.shared_ref();
    slot.fill(Payload::of_len(64)).expect("fill");

    // The sole consumer reaches terminal-and-returned.
    {
        let lease = c.enter();
        let _ = lease.read();
    }

    // A retained node keeps its value: residency is counted through run end and
    // the slot is not released by consumer completion.
    assert!(slot.is_filled());
    assert_eq!(ledger.current(), 64);

    // After the run has ended, exchange the handle for the value.
    let value = handle.redeem().expect("a retained value is redeemable");
    assert_eq!(value.bytes.len(), 64);

    // Redemption consumes the value; residency is released exactly once at run
    // end.
    assert_eq!(ledger.current(), 0);
}

// ---------------------------------------------------------------------------
// Released value is not redeemable, distinct from read-before-fill.
// ---------------------------------------------------------------------------
#[test]
fn released_value_is_not_redeemable() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("transient", 1, false, 64, &ledger);
    let handle = slot.redemption_handle();
    let c = slot.shared_ref();
    slot.fill(Payload::of_len(64)).expect("fill");

    // The last consumer returns; a non-retained value is released.
    {
        let lease = c.enter();
        let _ = lease.read();
    }
    assert!(!slot.is_filled());

    // Post-run redemption reports no value available — distinct from a
    // read-before-fill defect (that panics loudly; this returns an error).
    let err = handle
        .redeem()
        .expect_err("a released value is not redeemable");
    assert_eq!(err, RedeemError::Released);
}

#[test]
fn redeeming_an_unfilled_slot_reports_never_filled_not_released() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("neverfilled", 0, true, 0, &ledger);
    let handle = slot.redemption_handle();
    // Never filled: redemption distinguishes "never produced" from "released".
    let err = handle
        .redeem()
        .expect_err("an unfilled retained slot has no value");
    assert_eq!(err, RedeemError::NeverFilled);
}

// ---------------------------------------------------------------------------
// Residency is counted once, not per consumer.
// ---------------------------------------------------------------------------
#[test]
fn residency_is_counted_once_not_per_consumer() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("shared", 4, false, 1000, &ledger);
    // Four consumers all reference the same slot.
    let refs: Vec<_> = (0..4).map(|_| slot.shared_ref()).collect();
    slot.fill(Payload::of_len(500)).expect("fill");

    // The declared output residency is counted exactly once, regardless of the
    // four consumers.
    assert_eq!(ledger.current(), 1000);
    // Every consumer sees the value.
    for r in &refs {
        assert_eq!(r.read().bytes.len(), 500);
    }
    assert_eq!(ledger.current(), 1000);
}

// ---------------------------------------------------------------------------
// Accounting hooks expose peak residency.
// ---------------------------------------------------------------------------
#[test]
fn accounting_hooks_expose_peak_residency() {
    let ledger = ResidencyLedger::new();

    // Fill and release several slots over time; peak is the max concurrent
    // counted residency observed.
    {
        let s1: Slot<Payload> = slot_for("n1", 1, false, 300, &ledger);
        let c1 = s1.shared_ref();
        s1.fill(Payload::of_len(1)).expect("fill");
        assert_eq!(ledger.current(), 300);

        {
            let s2: Slot<Payload> = slot_for("n2", 1, false, 500, &ledger);
            let c2 = s2.shared_ref();
            s2.fill(Payload::of_len(1)).expect("fill");
            // Concurrent: 300 + 500.
            assert_eq!(ledger.current(), 800);
            let lease = c2.enter();
            let _ = lease.read();
        }
        // s2 released.
        assert_eq!(ledger.current(), 300);
        let lease = c1.enter();
        let _ = lease.read();
    }
    assert_eq!(ledger.current(), 0);

    // Peak measured residency matches the maximum concurrent counted residency.
    assert_eq!(ledger.peak(), 800);
}

// ---------------------------------------------------------------------------
// Owned delivery consumes the slot; a non-`Clone` type flows through it.
// ---------------------------------------------------------------------------
#[test]
fn owned_delivery_moves_the_value_out() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Payload> = slot_for("producer", 1, false, 64, &ledger);
    let consumer = slot.owned_ref();
    slot.fill(Payload::of_len(64)).expect("fill");

    // The sole owner takes the value by move — the framework has no copy left.
    // `take` consumes the lease: taking the value IS the consumer's terminal-and-
    // returned point (a value can be moved out exactly once).
    let lease = consumer.enter();
    let owned: Payload = lease.take();
    assert_eq!(owned.bytes.len(), 64);

    // After the sole consumer returned, the value is released.
    assert!(!slot.is_filled());
    assert_eq!(ledger.current(), 0);
}

// ---------------------------------------------------------------------------
// Clone-on-read gives each attempt a fresh, independent value.
// ---------------------------------------------------------------------------
#[test]
fn clone_on_read_gives_each_attempt_a_fresh_value() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Counter> = slot_for("producer", 1, false, 8, &ledger);
    let consumer = slot.clone_on_read_ref();
    slot.fill(Counter { n: 0 }).expect("fill");

    // Attempt 1 gets a fresh clone and mutates it.
    let mut a1 = consumer.clone_value();
    a1.n = 100;
    assert_eq!(a1.n, 100);

    // Attempt 2 gets an independent fresh clone; the mutation did not leak.
    let a2 = consumer.clone_value();
    assert_eq!(a2.n, 0);
}

// ---------------------------------------------------------------------------
// No lookup, no runtime type check on the read path (by construction): two
// slots of different output types wired to their consumers; each read yields
// the correctly typed value directly, and a mismatched-type wiring is
// impossible to construct.
// ---------------------------------------------------------------------------
#[test]
fn no_lookup_no_type_check_on_the_read_path() {
    let ledger = ResidencyLedger::new();

    let a: Slot<Payload> = slot_for("a", 1, false, 0, &ledger);
    let b: Slot<Counter> = slot_for("b", 1, false, 0, &ledger);

    // A consumer ref carries the concrete output type; it is minted from a slot
    // of exactly that type. There is no name/index/key argument — the reference
    // is a direct link, not a lookup.
    let ra = a.shared_ref(); // SlotRef<Payload>
    let rb = b.shared_ref(); // SlotRef<Counter>

    a.fill(Payload::of_len(3)).expect("fill a");
    b.fill(Counter { n: 42 }).expect("fill b");

    // Each read yields the correctly typed value with no lookup argument.
    let pa: Arc<Payload> = ra.read();
    let pb: Arc<Counter> = rb.read();
    assert_eq!(pa.bytes.len(), 3);
    assert_eq!(pb.n, 42);

    // A mismatched-type wiring is impossible to construct: `a.shared_ref()` is a
    // `SlotRef<Payload>`, so no `SlotRef<Counter>` can be obtained from `a`.
    // (Asserting a type equality documents the by-construction guarantee.)
    let _: dagr_core::slot::SlotRef<Payload> = a.shared_ref();
}

// ---------------------------------------------------------------------------
// Chain peak does not grow with chain length (bounded-memory smoke). Each
// node's value is consumed by exactly one downstream node; nothing retained.
// ---------------------------------------------------------------------------
/// Run a synthetic linear chain of `len` nodes, each carrying `per_node` bytes of
/// declared residency and consumed by exactly one downstream node (the last has
/// zero consumers). Returns the **peak** counted residency observed. Because each
/// value is released the instant its single consumer's read returns, at most two
/// node values are ever concurrently live, so the peak is independent of `len`.
fn run_chain(len: usize, per_node: u64) -> u64 {
    let ledger = ResidencyLedger::new();

    // The "previous" slot + its downstream consumer, carried forward: node i
    // reads node i-1's value, which releases i-1 once node i's read returns.
    let mut prev_consumer: Option<SlotRef<Vec<u8>>> = None;
    let mut prev_slot: Option<Slot<Vec<u8>>> = None;

    for i in 0..len {
        let name = format!("node-{i}");
        let slot: Slot<Vec<u8>> = slot_for(&name, u32::from(i + 1 < len), false, per_node, &ledger);

        // Node i's downstream consumer reference (consumed by node i+1).
        let downstream = slot.shared_ref();

        // Node i "runs": it reads its upstream (node i-1), producing its own
        // value. Consuming the upstream releases node i-1's slot.
        if let (Some(pc), Some(_ps)) = (prev_consumer.take(), prev_slot.take()) {
            let lease = pc.enter();
            let _ = lease.read();
            // lease drops here: node i-1's sole consumer returned → released.
        }

        slot.fill(vec![0u8; 8]).expect("fill");
        prev_consumer = Some(downstream);
        prev_slot = Some(slot);
    }

    // Drain the final node.
    if let (Some(pc), Some(_ps)) = (prev_consumer.take(), prev_slot.take()) {
        let lease = pc.enter();
        let _ = lease.read();
    }

    ledger.peak()
}

#[test]
fn chain_peak_does_not_grow_with_chain_length() {
    let per_node: u64 = 1024;
    let short = run_chain(4, per_node);
    let long = run_chain(64, per_node);

    // Peak counted residency is bounded and does NOT scale with chain length.
    // At most two node values are ever concurrently live (the one being read and
    // the one just produced), so the peak matches for a short and a long chain.
    assert_eq!(short, long);
    assert!(
        long <= per_node * 2,
        "peak residency must stay bounded to a small constant, got {long}"
    );
}
