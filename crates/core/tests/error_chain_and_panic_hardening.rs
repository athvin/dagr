//! Error-chain, panic, and arithmetic hardening — the `dagr-core` half.
//! Written first, TDD.
//!
//! Three of the ticket's Test-plan groups land here, because the behaviour they
//! pin is `dagr-core`'s:
//!
//! - **The `fill` discard.** `crates/core/src/execution.rs` filled the output slot
//!   with `let _ = slot.fill(value)` and then reported `Succeeded`
//!   *unconditionally*, while the comment beside it claimed a rejected fill was
//!   "dropped rather than silently swallowed as success". These tests decide which
//!   of the two was right: an attempt whose fill was **rejected** did not deliver
//!   its output, so the recorded outcome must not say it succeeded.
//! - **Poisoning policy — the *recover* half.** `Slot`'s interior lock recovers
//!   from poisoning deliberately (a panicking consumer must not wedge the slot
//!   machinery for every other node). The test poisons it through the public API —
//!   a read-before-fill panics *while holding the lock* — and proves the slot is
//!   still usable afterwards.
//! - **Causal chains — the no-cause half.** A variant with genuinely no underlying
//!   error must keep `source() == None`; the fix must not fabricate a link.

use dagr_core::context::{PipelineId, RunContext, RunId};
use dagr_core::execution::{
    AttemptEvent, AttemptEventSink, AttemptOutcome, PanicStrategy, check_panic_strategy,
    run_attempt, run_attempt_caught,
};
use dagr_core::handle::NodeId;
use dagr_core::resume::ResumeRefusal;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{TaskError, TerminalState};

const NODE: &str = "producer";

// ===========================================================================
// Harness
// ===========================================================================

/// A capturing attempt-event sink: the emitted records are asserted directly.
#[derive(Default)]
struct CapturingSink {
    events: Vec<AttemptEvent>,
}

impl AttemptEventSink for CapturingSink {
    fn emit(&mut self, event: AttemptEvent) {
        self.events.push(event);
    }
}

impl CapturingSink {
    /// The kind tag of every captured event, in emission order.
    fn kinds(&self) -> Vec<&'static str> {
        self.events
            .iter()
            .map(|e| match e {
                AttemptEvent::NodeAdmitted { .. } => "node-admitted",
                AttemptEvent::AttemptStarted { .. } => "attempt-started",
                AttemptEvent::AttemptSucceeded { .. } => "attempt-succeeded",
                AttemptEvent::AttemptFailed { .. } => "attempt-failed",
                AttemptEvent::AttemptTimedOut { .. } => "attempt-timed-out",
                AttemptEvent::AttemptPanicked { .. } => "attempt-panicked",
                AttemptEvent::BackoffStarted { .. } => "backoff-started",
                AttemptEvent::NodeTerminal { .. } => "node-terminal",
                _ => "other",
            })
            .collect()
    }
}

/// A task that succeeds with a fixed value.
struct Produces(u64);
impl Task for Produces {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0)
    }
}

fn ctx() -> RunContext {
    RunContext::builder(
        RunId::new("test-run"),
        PipelineId::new("test-pipeline"),
        NodeId::from_name(NODE),
    )
    .attempt(1)
    .max_attempts(1)
    .build()
}

fn slot_for(name: &str) -> Slot<u64> {
    Slot::new(
        NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    )
}

/// A minimal dependency-free block-on: the attempt futures under test never
/// suspend, so a no-op waker is enough (`dagr-core` pulls in no runtime).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let mut cx = Context::from_waker(Waker::noop());
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

// ===========================================================================
// The `let _ = slot.fill(value)` discard
// ===========================================================================

/// **A rejected fill is not a success.** The slot is pre-filled, so the attempt's
/// own fill is refused by the once-writable invariant: the attempt did **not**
/// deliver its output, and the recorded outcome must reflect what actually
/// happened rather than reporting `Succeeded` over a value that went nowhere.
#[test]
fn a_rejected_fill_is_not_recorded_as_success() {
    let slot = slot_for(NODE);
    slot.fill(1).expect("the first fill takes");

    let mut sink = CapturingSink::default();
    let outcome = block_on(run_attempt(
        &mut Produces(2),
        NODE,
        &ctx(),
        &slot,
        &mut sink,
    ));

    assert_ne!(
        outcome,
        AttemptOutcome::Succeeded,
        "an attempt whose output slot rejected the fill produced no output; recording it \
         `Succeeded` is exactly the swallowing the code's own comment says it does not do"
    );
    assert_eq!(
        outcome,
        AttemptOutcome::PermanentFailure,
        "a rejected fill is a framework-invariant violation, never retry-eligible"
    );
    assert_eq!(
        outcome.terminal_state(),
        TerminalState::Failed,
        "the node's terminal state follows the honest outcome"
    );
    assert!(
        sink.kinds().contains(&"attempt-failed"),
        "the exactly-one attempt-outcome record must be the failure record, got {:?}",
        sink.kinds()
    );
    assert!(
        !sink.kinds().contains(&"attempt-succeeded"),
        "no success record may be emitted for an attempt that filled nothing"
    );
}

/// The **caught** (panic-containing) path classifies a rejected fill the same way
/// — the two paths share one fill decision, so they cannot drift.
#[test]
fn a_rejected_fill_is_not_a_success_on_the_caught_path_either() {
    let slot = slot_for(NODE);
    slot.fill(1).expect("the first fill takes");

    let mut sink = CapturingSink::default();
    let outcome = block_on(run_attempt_caught(
        &mut Produces(2),
        NODE,
        &ctx(),
        &slot,
        &mut sink,
    ));

    assert_eq!(
        outcome,
        AttemptOutcome::PermanentFailure,
        "the caught path must not report a success the uncaught path refuses"
    );
}

/// **An accepted fill still succeeds.** The hardening must not turn the ordinary
/// path into a failure: a fresh slot accepts the fill and the attempt succeeds
/// with the value visible in the slot.
#[test]
fn an_accepted_fill_still_succeeds_and_delivers_the_value() {
    let slot = slot_for(NODE);
    let mut sink = CapturingSink::default();
    let outcome = block_on(run_attempt(
        &mut Produces(7),
        NODE,
        &ctx(),
        &slot,
        &mut sink,
    ));

    assert_eq!(outcome, AttemptOutcome::Succeeded);
    assert_eq!(
        *slot.shared_ref().read(),
        7,
        "the delivered value is the one the task produced"
    );
    assert!(sink.kinds().contains(&"attempt-succeeded"));
}

// ===========================================================================
// Poisoning policy — the *recover* half
// ===========================================================================

/// **A poisoned slot lock recovers.** `Slot`'s chosen policy is *recover*: a
/// read-before-fill panics loudly **while holding the interior lock**, so the
/// lock is poisoned by the framework's own defect assertion. A recovering lock
/// means one node's defect does not wedge the slot machinery for the rest of the
/// run — the subsequent fill still succeeds.
#[test]
fn a_poisoned_slot_lock_recovers_so_a_later_fill_still_succeeds() {
    let slot = slot_for(NODE);
    let reference = slot.shared_ref();

    // Read before fill: panics loudly (framework defect) *inside* the lock, which
    // poisons it. Caught here so the test can continue.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = reference.read();
    }));
    assert!(panicked.is_err(), "read-before-fill must panic loudly");

    // The recover policy: the poisoned lock is still usable.
    slot.fill(5)
        .expect("a poisoned slot lock recovers, so the fill still takes");
    assert_eq!(*reference.read(), 5);
}

// ===========================================================================
// Causal chains — the no-cause half
// ===========================================================================

/// **No fabricated links.** Both refusal types are constructed from data, not from
/// an underlying error, so `source()` stays `None`. The chain fix must add a link
/// only where a real cause exists.
#[test]
fn refusals_with_no_underlying_cause_keep_a_none_source() {
    use std::error::Error;

    let bootstrap =
        check_panic_strategy(PanicStrategy::Abort).expect_err("abort must be refused at bootstrap");
    assert!(
        bootstrap.source().is_none(),
        "BootstrapRefusal wraps no error; a fabricated source would be a lie"
    );

    let refusal = ResumeRefusal::ToolVersionMismatch {
        prior: "0.0.0".to_string(),
        current: "0.0.1".to_string(),
    };
    assert!(
        refusal.source().is_none(),
        "ResumeRefusal carries structured data, not an underlying error"
    );
}
