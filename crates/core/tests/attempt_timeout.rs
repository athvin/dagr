//! C14 per-attempt timeout tests — ticket T21 (031). Written first, TDD.
//!
//! These exercise the **per-attempt timeout** facet the single-attempt runner
//! (T20) deferred: the C14 "Timeout semantics differ by class, honestly"
//! paragraph and the T0.3 ADR (009) it builds on. The timeout is **runtime-
//! agnostic**, exactly as the T20 core is: the runner races the attempt future
//! against a caller-provided *deadline* future (the framework's isolated timer,
//! C13/T33, drives the real one; tests drive a controllable one), so `dagr-core`
//! stays dependency-free (no tokio).
//!
//! Two class-shapes, one taxonomy (T0.3 ADR §1):
//!
//! - **await-bound** — the one shape Rust can cancel: on timeout the attempt
//!   future is **dropped** (true cancellation) and any permit-shaped guard it
//!   held releases **immediately** (guard moved into the future ⇒ drop drops it).
//! - **blocking / compute** — synchronous, unkillable closures: on timeout the
//!   attempt is **marked** `timed-out` immediately (fate decided, event emitted,
//!   late-result barrier up) while the closure runs on as *abandoned-but-running*
//!   work whose permit is **held until the closure actually returns**; a retry is
//!   deferred until that return (C1 exclusivity).
//!
//! Scope discipline (T21): timeout only. No retry loop (T22), no panic-catch
//! (T23), no run-loop driver (T24), no dispatch (T33). The permit here is a
//! test stand-in modelling the T31 ledger's release/hold contract, not the real
//! ledger.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use dagr_core::context::{PipelineId, RunContext, RunId};
use dagr_core::execution::{
    run_attempt, run_attempt_with_timeout, AttemptEvent, AttemptEventSink, AttemptOutcome,
    TimeoutDecision,
};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{TaskError, TerminalState};

// --- Typed value + task types ----------------------------------------------

/// A trivial typed output value.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Report {
    rows: u32,
}

/// An await-bound task whose work never resolves — it awaits a future that is
/// always `Pending`, so only a timeout can end it. This is the await-bound
/// "exceeds its timeout" case; dropping its future is the only way it stops.
struct NeverResolves {
    /// Set to `true` if the work ever runs to completion (it must not, under a
    /// timeout that fires first — the drop must prevent completion).
    completed: Arc<AtomicBool>,
}
impl Task for NeverResolves {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        // Await a permanently-pending future: the attempt never completes on its
        // own. Only dropping the future (timeout cancellation) ends it.
        std::future::pending::<()>().await;
        self.completed.store(true, Ordering::SeqCst);
        Ok(Report { rows: 1 })
    }
}

/// An await-bound task that completes promptly (well within any timeout).
struct ResolvesPromptly {
    value: Report,
}
impl Task for ResolvesPromptly {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Ok(self.value.clone())
    }
}

// --- A capturing event sink (C19-shaped, in-memory) -------------------------

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
    fn events(&self) -> &[AttemptEvent] {
        &self.records
    }
    /// The number of attempt-outcome records (succeeded / failed / timed-out) —
    /// the exactly-one contract counts every closing outcome record.
    fn attempt_outcome_count(&self) -> usize {
        self.records
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    AttemptEvent::AttemptSucceeded { .. }
                        | AttemptEvent::AttemptFailed { .. }
                        | AttemptEvent::AttemptTimedOut { .. }
                )
            })
            .count()
    }
    fn has_timed_out_record(&self) -> bool {
        self.records
            .iter()
            .any(|e| matches!(e, AttemptEvent::AttemptTimedOut { .. }))
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

// --- A permit-ledger stand-in (models the T31 release/hold contract) --------

/// A minimal stand-in for the C12 permit ledger (T31 owns the real one). It
/// counts a per-pool cost while a permit is live and returns it to zero when the
/// permit's guard is dropped — the load-bearing T0.3 trick: "the work returned"
/// is *definitionally* "the guard was dropped."
#[derive(Debug, Default)]
struct Ledger {
    counted: AtomicU64,
    live_zombies: AtomicU64,
}
impl Ledger {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn counted(&self) -> u64 {
        self.counted.load(Ordering::SeqCst)
    }
    fn live_zombies(&self) -> u64 {
        self.live_zombies.load(Ordering::SeqCst)
    }
    /// Admit `cost`, returning a guard whose `Drop` releases it.
    fn admit(self: &Arc<Self>, cost: u64) -> Permit {
        self.counted.fetch_add(cost, Ordering::SeqCst);
        Permit {
            ledger: Arc::clone(self),
            cost,
            zombie: false,
        }
    }
}

/// A permit-shaped guard: its cost is counted against the ledger for exactly as
/// long as the guard is alive. Dropping it releases the cost. This is the guard
/// the runner moves into an await-bound future (so future-drop releases it) or
/// hands to a blocking/compute closure (so the closure's return releases it).
struct Permit {
    ledger: Arc<Ledger>,
    cost: u64,
    zombie: bool,
}
impl Permit {
    /// Register this permit as abandoned-but-running (a live zombie): the cost
    /// stays counted, but the ledger now reports one live zombie until drop.
    fn mark_zombie(&mut self) {
        if !self.zombie {
            self.zombie = true;
            self.ledger.live_zombies.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.ledger.counted.fetch_sub(self.cost, Ordering::SeqCst);
        if self.zombie {
            self.ledger.live_zombies.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

// --- A controllable deadline (the caller-provided timer future) -------------

/// The shared state a [`Deadline`] future observes. A test flips
/// [`fire`](Clock::fire) to make the deadline resolve on the next poll — a
/// pinned/fake clock, so timing is reproducible in CI (no wall-clock sleep).
#[derive(Default)]
struct Clock {
    fired: AtomicBool,
}
impl Clock {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    /// Fire the deadline: the next poll of a [`Deadline`] built on this clock
    /// resolves. Models the framework's isolated timer (C13) elapsing.
    fn fire(&self) {
        self.fired.store(true, Ordering::SeqCst);
    }
    /// A deadline future that resolves once [`fire`](Clock::fire) is called.
    fn deadline(self: &Arc<Self>) -> Deadline {
        Deadline {
            clock: Arc::clone(self),
        }
    }
}

/// The caller-provided per-attempt timeout future. Resolving means "the deadline
/// elapsed" — the runner races the attempt future against it. In production this
/// is a `tokio::time` sleep on the framework runtime (T2/T33); here it is a
/// controllable pinned clock so timing is deterministic and no runtime is
/// needed.
struct Deadline {
    clock: Arc<Clock>,
}
impl std::future::Future for Deadline {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.clock.fired.load(Ordering::SeqCst) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}

/// A deadline that never fires (the well-behaved-attempt case).
struct NeverFires;
impl std::future::Future for NeverFires {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        std::task::Poll::Pending
    }
}

// --- Helpers ----------------------------------------------------------------

const NODE: &str = "timed-node";

fn fresh_slot() -> Slot<Report> {
    Slot::new(
        NodeId::from_name(NODE),
        NODE,
        1,
        false,
        0,
        ResidencyLedger::new(),
    )
}

fn ctx_for(attempt: u32, max: u32) -> RunContext {
    RunContext::builder(
        RunId::new("test-run"),
        PipelineId::new("test-pipeline"),
        NodeId::from_name(NODE),
    )
    .attempt(attempt)
    .max_attempts(max)
    .build()
}

/// A no-dependency, `unsafe`-free block-on for the returned future — the same
/// runtime-agnostic executor the T20 tests use, proving the timeout path needs
/// no runtime. The attempt/deadline futures here resolve on a poll once their
/// state is set, so a busy-poll loop drives them to completion.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: StdArc<Self>) {}
        fn wake_by_ref(self: &StdArc<Self>) {}
    }

    let waker = Waker::from(StdArc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

// --- Await-bound timeout: cancelled immediately, permit released now ---------

/// **Await-bound attempt exceeds its timeout → cancelled immediately.** The
/// node reaches `timed-out`; the attempt future is dropped (the awaited work
/// never runs to completion); the permit releases at the moment of timeout, with
/// no residual counted and no zombie.
#[test]
fn await_bound_timeout_cancels_immediately_and_releases_permit() {
    let ledger = Ledger::new();
    let clock = Clock::new();
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 3);
    let completed = Arc::new(AtomicBool::new(false));

    // Admit the attempt with a known cost, then move the permit into the future
    // so that dropping the future (timeout cancellation) releases it.
    let permit = ledger.admit(100);
    assert_eq!(ledger.counted(), 100, "permit counted while admitted");

    // Fire the deadline immediately so the timeout wins the race.
    clock.fire();

    let task = NeverResolves {
        completed: Arc::clone(&completed),
    };

    let outcome = block_on(run_attempt_with_timeout(
        task,
        NODE,
        &ctx,
        &slot,
        &mut sink,
        clock.deadline(),
        permit,
    ));

    assert_eq!(outcome, AttemptOutcome::TimedOut, "classifies timed-out");
    assert!(
        !completed.load(Ordering::SeqCst),
        "the awaited work did not run to completion — its future was dropped"
    );
    assert_eq!(
        ledger.counted(),
        0,
        "permit released immediately at timeout — no residual counted"
    );
    assert_eq!(ledger.live_zombies(), 0, "await-bound records no zombie");
    assert!(!slot.is_filled(), "a timed-out attempt never fills the slot");
    assert!(
        sink.has_timed_out_record(),
        "the timed-out attempt-outcome record was emitted"
    );
    assert_eq!(
        sink.terminal_states(),
        vec![TerminalState::TimedOut],
        "node terminal state is timed-out"
    );
}

// --- Blocking/compute timeout: marked now, permit held until return ---------

/// **Blocking attempt exceeds its timeout → marked immediately, permit held
/// until return.** At the timeout mark the node is `timed-out` and the event is
/// emitted immediately, while the permit remains counted (one live zombie) until
/// the closure actually returns; only then does the permit release.
#[test]
fn blocking_timeout_marks_immediately_and_holds_permit_until_return() {
    let ledger = Ledger::new();
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();

    // The blocking closure holds the permit; the runner is handed a synchronous
    // "the closure has not returned yet" observation. On timeout the runner
    // marks `timed-out` immediately WITHOUT dropping the permit.
    let mut permit = ledger.admit(200);
    assert_eq!(ledger.counted(), 200, "permit counted while running");

    // T21's blocking-class decision: mark timed-out now, hold the permit.
    let decision = TimeoutDecision::mark_blocking_timed_out(NODE, &ctx_for(1, 2), &mut sink);
    permit.mark_zombie();

    assert_eq!(decision.outcome(), AttemptOutcome::TimedOut);
    assert_eq!(
        ledger.counted(),
        200,
        "permit still counted at the timeout mark (abandoned-but-running)"
    );
    assert_eq!(ledger.live_zombies(), 1, "one live zombie at the mark");
    assert!(sink.has_timed_out_record(), "timed-out event emitted immediately");
    assert_eq!(sink.terminal_states(), vec![TerminalState::TimedOut]);
    assert!(!slot.is_filled(), "never fills the slot");

    // Now the closure finally returns — dropping the permit releases it.
    drop(permit);
    assert_eq!(
        ledger.counted(),
        0,
        "permit released only when the closure actually returned"
    );
    assert_eq!(ledger.live_zombies(), 0, "zombie cleared on return");
}

/// **Compute attempt exceeds its timeout → same held-permit semantics as
/// blocking.** Identical observable behaviour: marked `timed-out` immediately,
/// permit held until the closure returns (T0.3 ADR §1 — class-shape-driven).
#[test]
fn compute_timeout_behaves_identically_to_blocking() {
    let ledger = Ledger::new();
    let mut sink = CapturingSink::default();

    let mut permit = ledger.admit(200);
    let decision = TimeoutDecision::mark_blocking_timed_out(NODE, &ctx_for(1, 2), &mut sink);
    permit.mark_zombie();

    assert_eq!(decision.outcome(), AttemptOutcome::TimedOut);
    assert_eq!(ledger.counted(), 200, "compute permit held at the mark");
    assert_eq!(ledger.live_zombies(), 1);

    drop(permit);
    assert_eq!(ledger.counted(), 0, "released only on closure return");
    assert_eq!(ledger.live_zombies(), 0);
}

// --- Late-result barrier ----------------------------------------------------

/// **Late result of a timed-out attempt never fills the slot.** After the
/// timeout has fired, a blocking closure finishes and produces a value; the
/// output slot is never filled by that value, and the value is discarded.
#[test]
fn late_result_never_fills_the_slot() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();

    // Mark the attempt timed out (blocking) — this raises the late-result
    // barrier the runner returns.
    let decision = TimeoutDecision::mark_blocking_timed_out(NODE, &ctx_for(1, 1), &mut sink);
    let barrier = decision.barrier();

    // The abandoned closure runs on and eventually produces a value, then tries
    // to fill the slot THROUGH the barrier.
    let filled = barrier.fill_slot(&slot, Report { rows: 99 });

    assert!(
        !filled,
        "the late fill was refused — a timed-out attempt never fills its slot"
    );
    assert!(!slot.is_filled(), "slot stays empty; the value was discarded");
}

/// **Late result of a timed-out attempt never writes scratch.** After the
/// timeout, the abandoned closure attempts a scratch write through the barrier;
/// no scratch value attributable to it is persisted.
#[test]
fn late_result_never_writes_scratch() {
    let mut sink = CapturingSink::default();
    let decision = TimeoutDecision::mark_blocking_timed_out(NODE, &ctx_for(1, 1), &mut sink);
    let barrier = decision.barrier();

    // A post-timeout scratch write is refused by the barrier.
    let wrote = barrier.write_scratch();
    assert!(
        !wrote,
        "the late scratch write was refused — a timed-out attempt never writes scratch"
    );
}

// --- Deferred retry ---------------------------------------------------------

/// **Retry of a timed-out blocking node is deferred past zombie return.** No
/// second attempt of the same node may begin while the first closure is still
/// running; the retry begins only after the first closure has returned
/// (C1 exclusivity — the task instance never runs concurrently with its zombie).
#[test]
fn retry_of_a_timed_out_blocking_node_is_deferred_past_zombie_return() {
    let ledger = Ledger::new();
    let mut sink = CapturingSink::default();

    let mut permit = ledger.admit(50);
    let decision = TimeoutDecision::mark_blocking_timed_out(NODE, &ctx_for(1, 2), &mut sink);
    permit.mark_zombie();

    // The outcome is retry-eligible, so a retry WILL eventually run...
    assert!(
        decision.outcome().is_retry_eligible(),
        "timeout is retry-eligible by default"
    );
    // ...but not while the first closure's zombie is live.
    assert!(
        decision.retry_may_start(&ledger) == false,
        "a retry must not begin while the first closure is still running"
    );
    assert_eq!(ledger.live_zombies(), 1);

    // The first closure returns.
    drop(permit);

    assert!(
        decision.retry_may_start(&ledger),
        "the retry may begin only after the first closure has returned"
    );
    assert_eq!(ledger.live_zombies(), 0);
}

// --- Terminal state decided exactly once ------------------------------------

/// **Terminal state is decided exactly once.** A blocking timeout is and stays
/// `timed-out`; it never transitions to `abandoned`, and the lingering thread's
/// eventual return is not a second terminal state (only a zombie event, C19).
#[test]
fn terminal_state_is_decided_exactly_once() {
    let ledger = Ledger::new();
    let mut sink = CapturingSink::default();

    let mut permit = ledger.admit(10);
    let decision = TimeoutDecision::mark_blocking_timed_out(NODE, &ctx_for(1, 1), &mut sink);
    permit.mark_zombie();

    // Exactly one node-terminal record, and it is timed-out.
    assert_eq!(
        sink.terminal_states(),
        vec![TerminalState::TimedOut],
        "one terminal state, timed-out"
    );

    // The thread lingers, then returns — no new terminal state is recorded.
    drop(permit);
    assert_eq!(
        sink.terminal_states(),
        vec![TerminalState::TimedOut],
        "still exactly one terminal state after the thread returned; never abandoned"
    );
    assert!(
        !sink
            .terminal_states()
            .contains(&TerminalState::Abandoned),
        "a timed-out attempt never becomes abandoned (that is the C16 path)"
    );
}

// --- Timeout is retry-eligible by default -----------------------------------

/// **Timeout is retry-eligible by default.** The timed-out outcome classifies
/// retry-eligible (it enters the retry path), and its terminal state is
/// `timed-out` — which is the terminal fate on the final permitted attempt.
#[test]
fn timeout_is_retry_eligible_by_default() {
    assert!(
        AttemptOutcome::TimedOut.is_retry_eligible(),
        "timeout enters the retry path"
    );
    assert!(AttemptOutcome::TimedOut.is_failure(), "timeout is a failure");
    assert!(!AttemptOutcome::TimedOut.is_success());
    assert_eq!(
        AttemptOutcome::TimedOut.terminal_state(),
        TerminalState::TimedOut,
        "a timeout on the final permitted attempt yields terminal timed-out"
    );
}

// --- Exactly one attempt-outcome record for a timed-out attempt -------------

/// **Exactly one attempt-outcome record for a timed-out attempt.** Precisely one
/// timed-out outcome record exists, alongside the attempt-started and
/// node-terminal per-transition events — no duplicate, no missing outcome.
#[test]
fn exactly_one_attempt_outcome_record_for_a_timed_out_attempt() {
    let ledger = Ledger::new();
    let clock = Clock::new();
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 2);

    let permit = ledger.admit(1);
    clock.fire();
    let outcome = block_on(run_attempt_with_timeout(
        NeverResolves {
            completed: Arc::new(AtomicBool::new(false)),
        },
        NODE,
        &ctx,
        &slot,
        &mut sink,
        clock.deadline(),
        permit,
    ));

    assert_eq!(outcome, AttemptOutcome::TimedOut);
    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one attempt-outcome record for the timed-out attempt"
    );
    assert!(
        sink.has_timed_out_record(),
        "the single outcome record is a timeout"
    );
    // The per-transition companions are present: attempt-started + node-terminal.
    assert!(sink
        .events()
        .iter()
        .any(|e| matches!(e, AttemptEvent::AttemptStarted { .. })));
    assert!(sink
        .events()
        .iter()
        .any(|e| matches!(e, AttemptEvent::NodeTerminal { .. })));
}

// --- A well-behaved attempt within its timeout is unaffected ----------------

/// **A well-behaved await-bound attempt within its timeout is unaffected.** It
/// fills its slot and reaches `succeeded`; no timeout event is emitted; the
/// permit releases on the normal terminal path exactly as in T20.
#[test]
fn well_behaved_attempt_within_timeout_is_unaffected() {
    let ledger = Ledger::new();
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);
    let value = Report { rows: 7 };

    let permit = ledger.admit(30);
    // The deadline never fires: the attempt completes comfortably inside it.
    let outcome = block_on(run_attempt_with_timeout(
        ResolvesPromptly {
            value: value.clone(),
        },
        NODE,
        &ctx,
        &slot,
        &mut sink,
        NeverFires,
        permit,
    ));

    assert_eq!(outcome, AttemptOutcome::Succeeded, "unchanged T20 success");
    assert!(slot.is_filled(), "slot filled on the normal path");
    assert_eq!(*slot.shared_ref().read(), value);
    assert!(
        !sink.has_timed_out_record(),
        "no timeout event on the happy path"
    );
    assert_eq!(sink.terminal_states(), vec![TerminalState::Succeeded]);
    assert_eq!(
        ledger.counted(),
        0,
        "permit released on the normal terminal path (future dropped on return)"
    );
    // The plain (untimed) runner still behaves identically for the same task —
    // the timeout wrapper is a superset, not a behaviour change.
    {
        let slot2 = fresh_slot();
        let mut sink2 = CapturingSink::default();
        let mut task2 = ResolvesPromptly {
            value: value.clone(),
        };
        let outcome2 = block_on(run_attempt(&mut task2, NODE, &ctx, &slot2, &mut sink2));
        assert_eq!(outcome2, AttemptOutcome::Succeeded);
        assert!(slot2.is_filled());
    }
}

// --- Timeout fires even when the deadline is driven off-thread (isolation) ---

/// **The timeout fires regardless of task-worker availability.** The deadline
/// future is the framework's isolated timer (C13): the runner races the attempt
/// against it, so even an attempt whose work never yields is ended by the
/// deadline. Here the deadline is already fired before the race begins,
/// modelling a timer that elapsed on the isolated framework runtime while task
/// workers were saturated — the node is still marked `timed-out`.
#[test]
fn timeout_fires_independent_of_task_worker_availability() {
    let ledger = Ledger::new();
    let clock = Clock::new();
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);

    // The isolated timer already elapsed (task workers were jammed); the runner
    // must observe it and mark timed-out on the first race poll.
    clock.fire();
    let permit = ledger.admit(5);
    let outcome = block_on(run_attempt_with_timeout(
        NeverResolves {
            completed: Arc::new(AtomicBool::new(false)),
        },
        NODE,
        &ctx,
        &slot,
        &mut sink,
        clock.deadline(),
        permit,
    ));

    assert_eq!(
        outcome,
        AttemptOutcome::TimedOut,
        "the timeout fired from the isolated timer, not gated on task workers"
    );
    assert_eq!(ledger.counted(), 0, "await-bound release is immediate");
}
