//! C14 retry-with-jittered-exponential-backoff tests — ticket T22 (032).
//! Written first, TDD.
//!
//! These exercise the **retry loop** the single-attempt runner (T20) deferred:
//! the C14 "either fill the slot, schedule another attempt after a backoff, or
//! reach a terminal failure" paragraph and its "Backoff is exponential with
//! jitter and a cap" clause. The loop wraps the already-merged
//! [`dagr_core::execution::run_attempt`] core (T20) — it does not fork it — and
//! drives it up to the configured maximum, consulting the classification after
//! each attempt.
//!
//! Determinism (the load-bearing constraint): jitter needs randomness but tests
//! must be reproducible. The retry logic reads **no** global RNG and **no**
//! system clock. Jitter is an injected [`dagr_core::execution::Jitter`] source
//! (a tiny seeded PRNG for production; a pinned/zero source for tests), and the
//! backoff **wait** is a caller-provided timer future (the driver arms a real
//! `tokio::time` sleep off the isolated runtime, T24/T33; tests pass a
//! controllable timer that records the requested delay and resolves immediately)
//! — so the exact backoff sequence is assertable with no wall-clock flakiness.
//!
//! Scope discipline (T22): retry + backoff only. No run-loop driver (T24), no
//! dispatch/concurrency (T33), no panic-catch (T23), no C5 policy surface (T29).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_core::context::{PipelineId, RunContext, RunId};
use dagr_core::execution::{
    run_with_retries, AttemptEvent, AttemptEventSink, AttemptOutcome, Backoff, NoJitter,
    RetryConfig, SeededJitter,
};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::TaskError;

// --- Typed value + task types ----------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Report {
    rows: u32,
}

/// A task that returns a retry-eligible error on every attempt, counting how
/// many times its work was invoked.
struct AlwaysRetryable {
    invocations: Arc<AtomicU32>,
}
impl Task for AlwaysRetryable {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Err(TaskError::retryable("transient blip"))
    }
}

/// A task that fails retry-eligibly until `succeed_on` (1-based attempt), then
/// succeeds. Reads the attempt number from the context (so it also proves the
/// attempt number is threaded through).
struct SucceedsOnAttempt {
    succeed_on: u32,
    invocations: Arc<AtomicU32>,
    value: Report,
    /// Records the (attempt, max) pairs the work observed, in order.
    observed: Arc<Mutex<Vec<(u32, u32)>>>,
}
impl Task for SucceedsOnAttempt {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<Report, TaskError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        self.observed
            .lock()
            .unwrap()
            .push((ctx.attempt(), ctx.max_attempts()));
        if ctx.attempt() >= self.succeed_on {
            Ok(self.value.clone())
        } else {
            Err(TaskError::retryable("not yet"))
        }
    }
}

/// A task that returns a permanent (retry-ineligible) error, counting invocations.
struct PermanentlyFails {
    invocations: Arc<AtomicU32>,
}
impl Task for PermanentlyFails {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Err(TaskError::permanent("bad input"))
    }
}

/// A task that returns a deliberate skip, counting invocations.
struct DecidesToSkip {
    invocations: Arc<AtomicU32>,
}
impl Task for DecidesToSkip {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Err(TaskError::skip("nothing to do"))
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
    /// Count of attempt-outcome records (succeeded / failed / timed-out) — the
    /// exactly-one-per-attempt contract.
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
    /// The attempt numbers of the closing outcome records, in emission order.
    fn outcome_attempt_numbers(&self) -> Vec<u32> {
        self.records
            .iter()
            .filter_map(|e| match e {
                AttemptEvent::AttemptSucceeded { attempt, .. }
                | AttemptEvent::AttemptFailed { attempt, .. }
                | AttemptEvent::AttemptTimedOut { attempt, .. } => Some(*attempt),
                _ => None,
            })
            .collect()
    }
    fn node_terminal_count(&self) -> usize {
        self.records
            .iter()
            .filter(|e| matches!(e, AttemptEvent::NodeTerminal { .. }))
            .count()
    }
    /// The backoff-phase records, in order, each carrying the delay it waited.
    fn backoff_delays(&self) -> Vec<Duration> {
        self.records
            .iter()
            .filter_map(|e| match e {
                AttemptEvent::BackoffStarted { delay, .. } => Some(*delay),
                _ => None,
            })
            .collect()
    }
    fn last_terminal(&self) -> Option<dagr_core::TerminalState> {
        self.records.iter().rev().find_map(|e| match e {
            AttemptEvent::NodeTerminal { state, .. } => Some(*state),
            _ => None,
        })
    }
}

// --- A controllable timer that records requested delays ---------------------

/// The caller-provided backoff timer. The retry loop hands it the computed
/// backoff [`Duration`]; here it records that duration and returns a future that
/// resolves on the next poll — a deterministic stand-in for the driver's
/// isolated-runtime sleep, so tests assert the exact delays with no wall-clock
/// sleep. This is where the "actual sleeping is the driver's concern" seam sits.
#[derive(Clone, Default)]
struct RecordingTimer {
    waited: Arc<std::sync::Mutex<Vec<Duration>>>,
}
impl RecordingTimer {
    fn new() -> Self {
        Self::default()
    }
    fn waited(&self) -> Vec<Duration> {
        self.waited.lock().unwrap().clone()
    }
    /// Produce the timer future for `delay`, recording it. Resolves immediately
    /// (no real sleep): the *schedule* is what the test asserts, not wall time.
    fn timer(&self, delay: Duration) -> impl std::future::Future<Output = ()> {
        self.waited.lock().unwrap().push(delay);
        std::future::ready(())
    }
}

// --- Helpers ----------------------------------------------------------------

const NODE: &str = "retry-node";

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

/// A no-dependency, `unsafe`-free block-on for the returned future — the same
/// runtime-agnostic executor the T20/T21 tests use, proving the retry loop needs
/// no runtime. The attempt/timer futures resolve on a poll, so a busy-poll loop
/// drives them to completion.
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

// ===========================================================================
// Configuration + default
// ===========================================================================

/// **Default configuration performs exactly one attempt.** The conservative
/// default is "no retries": a single attempt, then the node fails.
#[test]
fn default_config_performs_exactly_one_attempt() {
    let cfg = RetryConfig::default();
    assert_eq!(
        cfg.max_attempts(),
        1,
        "conservative default: no retries → a single attempt"
    );

    let invocations = Arc::new(AtomicU32::new(0));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();

    let outcome = block_on(run_with_retries(
        AlwaysRetryable {
            invocations: Arc::clone(&invocations),
        },
        NODE,
        RunId::new("test-run"),
        PipelineId::new("test-pipeline"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(invocations.load(Ordering::SeqCst), 1, "exactly one attempt");
    assert!(outcome.is_retry_eligible());
    assert_eq!(sink.last_terminal(), Some(dagr_core::TerminalState::Failed));
    assert!(
        timer.waited().is_empty(),
        "no backoff scheduled under the no-retry default"
    );
}

// ===========================================================================
// Classification-gated retry
// ===========================================================================

/// **Retry-eligible error is retried up to the budget and no further.** Max 3
/// attempts, always retry-eligible: the work runs exactly 3 times, the node ends
/// `failed` (exhausted), and no 4th attempt occurs.
#[test]
fn retry_eligible_is_retried_up_to_the_budget_and_no_further() {
    let invocations = Arc::new(AtomicU32::new(0));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(3, Backoff::new(Duration::from_millis(10), 2.0, Duration::MAX));

    let outcome = block_on(run_with_retries(
        AlwaysRetryable {
            invocations: Arc::clone(&invocations),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(
        invocations.load(Ordering::SeqCst),
        3,
        "the work is invoked exactly max-attempts times, no 4th"
    );
    assert_eq!(outcome, AttemptOutcome::RetryEligibleFailure);
    assert_eq!(
        sink.last_terminal(),
        Some(dagr_core::TerminalState::Failed),
        "exhausted retry → failed terminal state"
    );
    // Exactly one node-terminal record for the whole loop, not one per attempt.
    assert_eq!(
        sink.node_terminal_count(),
        1,
        "exactly one node-terminal record for the retried node"
    );
    // Two backoffs (before attempt 2 and before attempt 3), none after the last.
    assert_eq!(timer.waited().len(), 2, "backoff before each retry, not after");
}

/// **A single successful retry stops the loop.** Fails retry-eligibly on attempt
/// 1, succeeds on attempt 2: the work runs exactly twice, the slot is filled, no
/// 3rd attempt occurs.
#[test]
fn a_single_successful_retry_stops_the_loop() {
    let invocations = Arc::new(AtomicU32::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(3, Backoff::new(Duration::from_millis(5), 2.0, Duration::MAX));
    let value = Report { rows: 7 };

    let outcome = block_on(run_with_retries(
        SucceedsOnAttempt {
            succeed_on: 2,
            invocations: Arc::clone(&invocations),
            value: value.clone(),
            observed: Arc::clone(&observed),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(invocations.load(Ordering::SeqCst), 2, "exactly two attempts");
    assert_eq!(outcome, AttemptOutcome::Succeeded);
    assert!(slot.is_filled(), "slot filled with the successful value");
    assert_eq!(*slot.shared_ref().read(), value);
    assert_eq!(
        sink.last_terminal(),
        Some(dagr_core::TerminalState::Succeeded)
    );
    assert_eq!(timer.waited().len(), 1, "exactly one backoff before the retry");
}

/// **Permanent error is never retried, regardless of remaining budget.** Max 5,
/// permanent on attempt 1: the work runs exactly once, the node fails, no backoff.
#[test]
fn permanent_error_is_never_retried() {
    let invocations = Arc::new(AtomicU32::new(0));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(5, Backoff::new(Duration::from_millis(5), 2.0, Duration::MAX));

    let outcome = block_on(run_with_retries(
        PermanentlyFails {
            invocations: Arc::clone(&invocations),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "permanent error runs exactly once even with 4 attempts remaining"
    );
    assert_eq!(outcome, AttemptOutcome::PermanentFailure);
    assert_eq!(sink.last_terminal(), Some(dagr_core::TerminalState::Failed));
    assert!(
        timer.waited().is_empty(),
        "no backoff scheduled for a permanent failure"
    );
}

/// **Deliberate skip is not retried.** Max 5, skip on attempt 1: the work runs
/// exactly once, the node ends `skipped`, no backoff, no further attempt.
#[test]
fn deliberate_skip_is_not_retried() {
    let invocations = Arc::new(AtomicU32::new(0));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(5, Backoff::new(Duration::from_millis(5), 2.0, Duration::MAX));

    let outcome = block_on(run_with_retries(
        DecidesToSkip {
            invocations: Arc::clone(&invocations),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(invocations.load(Ordering::SeqCst), 1, "skip runs exactly once");
    assert_eq!(outcome, AttemptOutcome::Skipped);
    assert_eq!(sink.last_terminal(), Some(dagr_core::TerminalState::Skipped));
    assert!(timer.waited().is_empty(), "no backoff scheduled for a skip");
}

// ===========================================================================
// Backoff schedule: exponential, capped, jittered
// ===========================================================================

/// **Backoff is exponential and capped.** With jitter disabled the successive
/// delays are exactly base·factor^n, each clamped to the cap (later delays sit
/// exactly at the cap).
#[test]
fn backoff_is_exponential_and_capped() {
    let base = Duration::from_millis(100);
    let cap = Duration::from_millis(500);
    let backoff = Backoff::new(base, 2.0, cap);

    // No jitter: assert the exact nominal sequence off the pure schedule fn.
    // Attempt index n (0-based, for the wait *after* attempt n+1):
    //   n=0 → 100ms, n=1 → 200ms, n=2 → 400ms, n=3 → 800ms→cap 500ms, ...
    assert_eq!(backoff.delay_for(0, &mut NoJitter), Duration::from_millis(100));
    assert_eq!(backoff.delay_for(1, &mut NoJitter), Duration::from_millis(200));
    assert_eq!(backoff.delay_for(2, &mut NoJitter), Duration::from_millis(400));
    assert_eq!(
        backoff.delay_for(3, &mut NoJitter),
        cap,
        "clamped to the cap"
    );
    assert_eq!(
        backoff.delay_for(9, &mut NoJitter),
        cap,
        "later delays sit exactly at the cap"
    );

    // And end-to-end through the loop: an always-retryable node with 5 attempts
    // records the same nominal, capped sequence in the timer.
    let invocations = Arc::new(AtomicU32::new(0));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(5, backoff);
    let _ = block_on(run_with_retries(
        AlwaysRetryable {
            invocations: Arc::clone(&invocations),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));
    assert_eq!(
        timer.waited(),
        vec![
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(500), // capped
        ],
        "4 backoffs for 5 attempts, exponential then capped"
    );
    // The backoff-phase records in the stream carry the same delays.
    assert_eq!(sink.backoff_delays(), timer.waited());
}

/// **Every scheduled delay never exceeds the cap, even with jitter.** With a
/// seeded jitter source the delay stays within the jitter window around the
/// nominal exponential value and is always clamped to the cap.
#[test]
fn jittered_delay_never_exceeds_the_cap() {
    let base = Duration::from_millis(100);
    let cap = Duration::from_millis(300);
    let backoff = Backoff::new(base, 2.0, cap);

    // Full jitter over a seeded source: draw many delays across attempt indices
    // and assert none exceeds the cap and each is nonnegative.
    let mut jitter = SeededJitter::new(0xC0FFEE);
    for n in 0..12u32 {
        for _ in 0..50 {
            let d = backoff.delay_for(n, &mut jitter);
            assert!(d <= cap, "jittered delay {d:?} exceeds cap {cap:?} at n={n}");
        }
    }
}

/// **Backoff is jittered — a fan-out does not resynchronize.** N identical
/// always-retry nodes entering their first backoff with a seeded jitter source
/// producing distinct draws yield wake delays that are NOT all identical (the
/// spread is nonzero), and each delay lies within the jitter window and never
/// exceeds the cap.
#[test]
fn jittered_backoff_does_not_resynchronize_a_fan_out() {
    let base = Duration::from_millis(100);
    let cap = Duration::from_secs(60); // high cap so the cap does not flatten the spread
    let backoff = Backoff::new(base, 2.0, cap);

    // Each of the N nodes draws its first backoff (attempt index 0) from a jitter
    // source seeded distinctly per node — modelling N nodes that entered backoff
    // at the same instant.
    let n_nodes = 16;
    let first_wakes: Vec<Duration> = (0..n_nodes)
        .map(|node_seed| {
            let mut jitter = SeededJitter::new(0x1234_0000 + node_seed as u64);
            backoff.delay_for(0, &mut jitter)
        })
        .collect();

    let all_identical = first_wakes.windows(2).all(|w| w[0] == w[1]);
    assert!(
        !all_identical,
        "a fan-out of simultaneous retries must not wake in lockstep (spread must be nonzero): {first_wakes:?}"
    );

    // Each first-backoff wake still lies within the jitter window around the
    // nominal base (100ms) and never exceeds the cap. With full jitter the window
    // is (0, nominal]; assert the loose, source-agnostic bounds.
    for d in &first_wakes {
        assert!(*d <= base, "jittered first backoff {d:?} exceeds nominal base");
        assert!(*d <= cap);
    }
}

/// The seeded jitter is **reproducible**: two sources with the same seed produce
/// the identical delay sequence (this is what makes the backoff tests
/// deterministic), while different seeds diverge.
#[test]
fn seeded_jitter_is_reproducible_and_seed_dependent() {
    let backoff = Backoff::new(Duration::from_millis(50), 2.0, Duration::from_secs(10));

    let seq = |seed: u64| -> Vec<Duration> {
        let mut j = SeededJitter::new(seed);
        (0..6).map(|n| backoff.delay_for(n, &mut j)).collect()
    };

    assert_eq!(seq(42), seq(42), "same seed → identical sequence");
    assert_ne!(seq(1), seq(2), "different seeds → different sequences");
}

// ===========================================================================
// Per-attempt events and phase recording
// ===========================================================================

/// **Exactly one attempt-outcome record per attempt.** Fails retry-eligibly
/// twice then succeeds (max 3): exactly 3 outcome records, attempt numbers
/// gapless and increasing (1,2,3), the first two failures and the last a
/// success, and the terminal node record appears once.
#[test]
fn exactly_one_attempt_outcome_record_per_attempt() {
    let invocations = Arc::new(AtomicU32::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(3, Backoff::new(Duration::from_millis(1), 2.0, Duration::MAX));

    let _ = block_on(run_with_retries(
        SucceedsOnAttempt {
            succeed_on: 3,
            invocations: Arc::clone(&invocations),
            value: Report { rows: 1 },
            observed: Arc::clone(&observed),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(
        sink.attempt_outcome_count(),
        3,
        "one outcome record per attempt"
    );
    assert_eq!(
        sink.outcome_attempt_numbers(),
        vec![1, 2, 3],
        "attempt numbers are gapless and increasing"
    );
    // First two are failures, last is a success.
    let outcome_events: Vec<&AttemptEvent> = sink
        .events()
        .iter()
        .filter(|e| {
            matches!(
                e,
                AttemptEvent::AttemptSucceeded { .. } | AttemptEvent::AttemptFailed { .. }
            )
        })
        .collect();
    assert!(matches!(outcome_events[0], AttemptEvent::AttemptFailed { .. }));
    assert!(matches!(outcome_events[1], AttemptEvent::AttemptFailed { .. }));
    assert!(matches!(
        outcome_events[2],
        AttemptEvent::AttemptSucceeded { .. }
    ));
    assert_eq!(
        sink.node_terminal_count(),
        1,
        "the terminal node record appears exactly once"
    );
}

/// **Attempt number is visible to the task.** With max 3 and failing until the
/// last attempt, the work observes attempt numbers 1, 2, 3 in order, each paired
/// with the maximum of 3 — the current attempt / max are wired into the context.
#[test]
fn attempt_number_is_visible_to_the_task() {
    let invocations = Arc::new(AtomicU32::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(3, Backoff::new(Duration::from_millis(1), 2.0, Duration::MAX));

    let _ = block_on(run_with_retries(
        SucceedsOnAttempt {
            succeed_on: 3,
            invocations: Arc::clone(&invocations),
            value: Report { rows: 1 },
            observed: Arc::clone(&observed),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    assert_eq!(
        *observed.lock().unwrap(),
        vec![(1, 3), (2, 3), (3, 3)],
        "the task observes attempt 1..3, each paired with max 3"
    );
}

/// **Backoff phase is a named, measurable interval.** Fails once retry-eligibly
/// then succeeds: a distinct backoff-phase record appears between the first
/// failed attempt and the second attempt start, and the recorded delay equals
/// the scheduled backoff delay (distinct from executing).
#[test]
fn backoff_phase_is_a_named_measurable_interval() {
    let invocations = Arc::new(AtomicU32::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let base = Duration::from_millis(80);
    let cfg = RetryConfig::new(3, Backoff::new(base, 2.0, Duration::MAX));

    let _ = block_on(run_with_retries(
        SucceedsOnAttempt {
            succeed_on: 2,
            invocations: Arc::clone(&invocations),
            value: Report { rows: 1 },
            observed: Arc::clone(&observed),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    // The backoff record sits between the first AttemptFailed and the second
    // AttemptStarted, and carries the scheduled delay.
    let events = sink.events();
    let first_failed = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptFailed { .. }))
        .expect("first failed present");
    let backoff_pos = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::BackoffStarted { .. }))
        .expect("backoff phase record present");
    let second_started = events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, AttemptEvent::AttemptStarted { .. }))
        .nth(1)
        .map(|(i, _)| i)
        .expect("second attempt-started present");
    assert!(
        first_failed < backoff_pos && backoff_pos < second_started,
        "the backoff phase falls between the failed attempt and the next attempt start"
    );
    assert_eq!(
        sink.backoff_delays(),
        vec![base],
        "the backoff-phase delay equals the scheduled backoff (nominal base for the first retry)"
    );
}

// ===========================================================================
// C1 exclusivity — no premature re-entry
// ===========================================================================

/// **No premature re-entry (C1 exclusivity).** Attempt N+1's work never begins
/// until attempt N's closure has returned — the same task instance is never
/// running concurrently with a prior attempt. The loop takes the task by
/// `&mut self`, so the borrow checker plus the sequential await already enforce
/// this; this test asserts the observable ordering (each attempt starts only
/// after the previous one fully returned).
#[test]
fn no_premature_re_entry_attempts_are_strictly_sequential() {
    // A task that records an "enter" and "exit" marker per attempt; if any
    // attempt entered before the previous exited, the recorded sequence would
    // interleave. Sequential execution yields strictly paired enter/exit.
    struct Sequential {
        log: Arc<Mutex<Vec<&'static str>>>,
        in_flight: Arc<AtomicU32>,
    }
    impl Task for Sequential {
        type Input = ();
        type Output = Report;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
            // No other attempt of this instance may be in flight (C1).
            let concurrent = self.in_flight.fetch_add(1, Ordering::SeqCst);
            assert_eq!(concurrent, 0, "a prior attempt was still running (C1 violated)");
            self.log.lock().unwrap().push("enter");
            self.log.lock().unwrap().push("exit");
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Err(TaskError::retryable("again"))
        }
    }

    let log = Arc::new(Mutex::new(Vec::new()));
    let in_flight = Arc::new(AtomicU32::new(0));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let timer = RecordingTimer::new();
    let cfg = RetryConfig::new(3, Backoff::new(Duration::from_millis(1), 2.0, Duration::MAX));

    let _ = block_on(run_with_retries(
        Sequential {
            log: Arc::clone(&log),
            in_flight: Arc::clone(&in_flight),
        },
        NODE,
        RunId::new("r"),
        PipelineId::new("p"),
        &slot,
        &mut sink,
        &cfg,
        &mut NoJitter,
        |d| timer.timer(d),
    ));

    // Strictly paired enter/exit for each of the 3 attempts — never two enters
    // in a row (which would mean a concurrent re-entry).
    assert_eq!(
        *log.lock().unwrap(),
        vec!["enter", "exit", "enter", "exit", "enter", "exit"],
        "attempts are strictly sequential; no attempt re-enters before the prior returned"
    );
}
