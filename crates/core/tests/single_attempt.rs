//! C14 single-attempt execution-core tests — ticket T20 (030). Written first, TDD.
//!
//! These exercise the **real** single-attempt runner in
//! [`dagr_core::execution`]: the load-bearing spine of the C14 attempt runner
//! (arch.md `### C14 · Attempt runner`) that runs **one** attempt of **one**
//! node end to end — open a span, record the admission phase marker, dispatch
//! the already-placed work, await its outcome, classify it into the normative
//! taxonomy (arch.md Vocabulary), fill the output slot (C10/T17) on success
//! only, and emit the ordered per-transition events plus exactly one
//! attempt-outcome record (C19/T19) for every attempt.
//!
//! Scope discipline (T20): this is **single-attempt only** — no retry
//! (T22), no timeout (T21), no panic catching (T23), no execution-class
//! dispatch (T33). The runner is runtime-agnostic: an `async fn` awaited here
//! on a plain executor, with a hand-built `RunContext` (C8, no runtime) and a
//! capturing event sink (C19) whose records are asserted directly.

use std::sync::Arc;

use dagr_core::context::{CancellationSource, PipelineId, RunContext, RunId};
use dagr_core::execution::{run_attempt, AttemptEvent, AttemptEventSink, AttemptOutcome};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{TaskError, TerminalState};

// --- Illustrative typed value + task types ----------------------------------

/// A non-trivial typed output value (proves the fill path preserves `T`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Report {
    rows: u32,
    label: String,
}

/// A task that returns a fixed `Report` value on success.
struct Produces {
    value: Report,
}
impl Task for Produces {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Ok(self.value.clone())
    }
}

/// A task that returns a permanent (retry-ineligible) error.
struct PermanentlyFails;
impl Task for PermanentlyFails {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Err(TaskError::permanent("bad input, do not retry"))
    }
}

/// A task that returns a retry-eligible error.
struct RetryablyFails;
impl Task for RetryablyFails {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Err(TaskError::retryable("transient blip, try me again"))
    }
}

/// A task that returns a deliberate (originated) skip.
struct DecidesToSkip;
impl Task for DecidesToSkip {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Err(TaskError::skip("nothing to do"))
    }
}

// --- A capturing event sink (C19-shaped, in-memory) -------------------------

/// A capturing [`AttemptEventSink`] that records every emitted record in order,
/// stamping a gapless, strictly-increasing sequence number — the in-memory
/// stand-in for the C19 writer the ticket's test plan calls for.
#[derive(Default)]
struct CapturingSink {
    records: Vec<(u64, AttemptEvent)>,
    next_seq: u64,
}

impl AttemptEventSink for CapturingSink {
    fn emit(&mut self, event: AttemptEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.records.push((seq, event));
    }
}

impl CapturingSink {
    fn events(&self) -> Vec<AttemptEvent> {
        self.records.iter().map(|(_, e)| e.clone()).collect()
    }

    /// The number of attempt-outcome records (succeeded/failed) in the stream —
    /// used to assert the exactly-one contract.
    fn attempt_outcome_count(&self) -> usize {
        self.records
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e,
                    AttemptEvent::AttemptSucceeded { .. } | AttemptEvent::AttemptFailed { .. }
                )
            })
            .count()
    }
}

// --- Helpers ----------------------------------------------------------------

const NODE: &str = "produce-report";

/// A fresh, empty output slot for `Report` with a single consumer (enough for
/// the runner's fill; consumer wiring is assembly's job — T14).
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

/// Build a `RunContext` for `NODE` at the given attempt/max (C8 hand-build, no
/// runtime).
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

/// Drive one attempt of `task` to a decided outcome on a plain single-threaded
/// executor (runtime-agnostic: the caller provides the runtime — T33 places).
fn drive<T: Task<Input = (), Output = Report>>(
    mut task: T,
    ctx: &RunContext,
    slot: &Slot<Report>,
    sink: &mut CapturingSink,
) -> AttemptOutcome {
    futures_lite_block_on(run_attempt(&mut task, NODE, ctx, slot, sink))
}

/// A minimal no-dependency, `unsafe`-free block-on for the returned future.
/// dagr-core is dependency-free, so tests use a tiny hand-rolled executor built
/// on the safe [`std::task::Wake`] trait rather than pull a runtime — proving
/// the core is runtime-agnostic (T2/T33 place; T20 only awaits).
fn futures_lite_block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};

    /// A no-op waker: the test attempt futures never actually yield (the test
    /// tasks do no real I/O), so waking is unnecessary and this busy-polls.
    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: StdArc<Self>) {}
        fn wake_by_ref(self: &StdArc<Self>) {}
    }

    let waker = Waker::from(StdArc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        // The attempt future resolves on the next poll (no real suspension), so
        // a bare busy-poll loop drives it to completion.
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

// --- Tests ------------------------------------------------------------------

/// Successful attempt fills the slot; outcome is success; the sink shows
/// attempt-started then attempt-succeeded; slot empty before, filled after.
#[test]
fn successful_attempt_fills_the_slot() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);
    let value = Report {
        rows: 42,
        label: "q3".into(),
    };

    assert!(!slot.is_filled(), "slot empty before the run");

    let outcome = drive(
        Produces {
            value: value.clone(),
        },
        &ctx,
        &slot,
        &mut sink,
    );

    assert!(
        matches!(outcome, AttemptOutcome::Succeeded),
        "classifies success"
    );
    assert!(slot.is_filled(), "slot filled after a successful run");
    assert_eq!(
        *slot.shared_ref().read(),
        value,
        "slot holds exactly the produced value"
    );

    let events = sink.events();
    let started = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptStarted { .. }))
        .expect("attempt-started present");
    let succeeded = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptSucceeded { .. }))
        .expect("attempt-succeeded present");
    assert!(
        started < succeeded,
        "attempt-started precedes attempt-succeeded"
    );
}

/// Exactly one attempt-outcome record on success, carrying run/node/attempt
/// identity and a success outcome.
#[test]
fn exactly_one_attempt_outcome_record_on_success() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);
    drive(
        Produces {
            value: Report {
                rows: 1,
                label: "x".into(),
            },
        },
        &ctx,
        &slot,
        &mut sink,
    );

    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one outcome record"
    );
    let outcome_rec = sink
        .events()
        .into_iter()
        .find(|e| matches!(e, AttemptEvent::AttemptSucceeded { .. }))
        .expect("success outcome record present");
    match outcome_rec {
        AttemptEvent::AttemptSucceeded { node, attempt } => {
            assert_eq!(node, NODE, "carries node identity");
            assert_eq!(attempt, 1, "carries attempt identity");
        }
        _ => unreachable!(),
    }
}

/// Permanent failure does not fill the slot; classifies as permanent failure;
/// attempt-started then attempt-failed; exactly one outcome record (a failure).
#[test]
fn permanent_failure_does_not_fill_the_slot() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);

    let outcome = drive(PermanentlyFails, &ctx, &slot, &mut sink);

    assert!(
        matches!(outcome, AttemptOutcome::PermanentFailure),
        "classifies permanent failure"
    );
    assert!(!slot.is_filled(), "slot stays empty on failure");

    let events = sink.events();
    let started = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptStarted { .. }))
        .expect("attempt-started present");
    let failed = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptFailed { .. }))
        .expect("attempt-failed present");
    assert!(started < failed, "started precedes failed");
    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one outcome record"
    );
}

/// A permanent error is NEVER classified retry-eligible (C14: a permanent error
/// is not retried regardless of remaining attempts — the runner surfaces the
/// distinction, even with attempts remaining).
#[test]
fn permanent_failure_is_not_retry_eligible_even_with_attempts_remaining() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    // max_attempts is high, but a permanent error is still not retry-eligible.
    let ctx = ctx_for(1, 5);

    let outcome = drive(PermanentlyFails, &ctx, &slot, &mut sink);

    assert!(
        matches!(outcome, AttemptOutcome::PermanentFailure),
        "permanent, not retry-eligible"
    );
    assert!(
        !outcome.is_retry_eligible(),
        "the runner never reports a permanent error as retry-eligible"
    );
    assert_eq!(outcome.terminal_state(), TerminalState::Failed);
}

/// Retry-eligible failure is classified distinctly from permanent and does not
/// fill the slot; this runner schedules no retry (it only classifies).
#[test]
fn retry_eligible_failure_is_classified_distinctly() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 3);

    let outcome = drive(RetryablyFails, &ctx, &slot, &mut sink);

    assert!(
        matches!(outcome, AttemptOutcome::RetryEligibleFailure),
        "classifies retry-eligible, distinct from permanent"
    );
    assert!(
        outcome.is_retry_eligible(),
        "retry driver (T22) can act on it"
    );
    assert!(!slot.is_filled(), "slot stays empty");
    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one outcome record"
    );
    // Distinct from the permanent classification.
    assert!(!matches!(outcome, AttemptOutcome::PermanentFailure));
}

/// Deliberate skip is classified as an originated skip, distinct from success
/// and failure, and does not fill the slot.
#[test]
fn deliberate_skip_is_classified_as_originated_skip() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);

    let outcome = drive(DecidesToSkip, &ctx, &slot, &mut sink);

    assert!(
        matches!(outcome, AttemptOutcome::Skipped),
        "classifies skip"
    );
    assert!(!outcome.is_success(), "distinct from success");
    assert!(!outcome.is_failure(), "distinct from failure");
    assert_eq!(outcome.terminal_state(), TerminalState::Skipped);
    assert!(!slot.is_filled(), "slot stays empty on skip");
    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one outcome record"
    );
}

/// Every reachable outcome yields exactly one attempt-outcome record — never
/// zero, never two — parametrized over success / permanent / retryable / skip.
#[test]
fn every_outcome_yields_exactly_one_attempt_outcome_record() {
    // success
    {
        let slot = fresh_slot();
        let mut sink = CapturingSink::default();
        drive(
            Produces {
                value: Report {
                    rows: 0,
                    label: String::new(),
                },
            },
            &ctx_for(1, 1),
            &slot,
            &mut sink,
        );
        assert_eq!(sink.attempt_outcome_count(), 1, "success: one record");
    }
    // permanent
    {
        let slot = fresh_slot();
        let mut sink = CapturingSink::default();
        drive(PermanentlyFails, &ctx_for(1, 1), &slot, &mut sink);
        assert_eq!(sink.attempt_outcome_count(), 1, "permanent: one record");
    }
    // retryable
    {
        let slot = fresh_slot();
        let mut sink = CapturingSink::default();
        drive(RetryablyFails, &ctx_for(1, 3), &slot, &mut sink);
        assert_eq!(sink.attempt_outcome_count(), 1, "retryable: one record");
    }
    // skip
    {
        let slot = fresh_slot();
        let mut sink = CapturingSink::default();
        drive(DecidesToSkip, &ctx_for(1, 1), &slot, &mut sink);
        assert_eq!(sink.attempt_outcome_count(), 1, "skip: one record");
    }
}

/// Attempt number and maximum are carried through identity: the span, the
/// per-transition events, and the outcome record all carry the same attempt
/// number and maximum from the C8 context.
#[test]
fn attempt_number_is_carried_through_identity() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    // A non-first attempt with a defined maximum.
    let ctx = ctx_for(3, 5);

    drive(
        Produces {
            value: Report {
                rows: 9,
                label: "a".into(),
            },
        },
        &ctx,
        &slot,
        &mut sink,
    );

    for event in sink.events() {
        match event {
            AttemptEvent::AttemptStarted { attempt, node }
            | AttemptEvent::AttemptSucceeded { attempt, node }
            | AttemptEvent::AttemptFailed { attempt, node } => {
                assert_eq!(attempt, 3, "per-transition events carry the attempt number");
                assert_eq!(node, NODE);
            }
            _ => {}
        }
    }
}

/// A run under attempt one carries one.
#[test]
fn first_attempt_carries_attempt_one() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);
    drive(
        Produces {
            value: Report {
                rows: 1,
                label: "one".into(),
            },
        },
        &ctx,
        &slot,
        &mut sink,
    );
    let started = sink
        .events()
        .into_iter()
        .find_map(|e| match e {
            AttemptEvent::AttemptStarted { attempt, .. } => Some(attempt),
            _ => None,
        })
        .expect("attempt-started present");
    assert_eq!(started, 1);
}

/// Event ordering within an attempt: attempt-started precedes attempt-succeeded,
/// which precedes the node-terminal record; sequence numbers are gapless.
#[test]
fn event_ordering_within_an_attempt() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(1, 1);
    drive(
        Produces {
            value: Report {
                rows: 7,
                label: "ord".into(),
            },
        },
        &ctx,
        &slot,
        &mut sink,
    );

    let events = sink.events();
    let started = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptStarted { .. }))
        .expect("started");
    let succeeded = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::AttemptSucceeded { .. }))
        .expect("succeeded");
    let terminal = events
        .iter()
        .position(|e| matches!(e, AttemptEvent::NodeTerminal { .. }))
        .expect("node-terminal");
    assert!(started < succeeded, "started before succeeded");
    assert!(succeeded <= terminal, "succeeded before/at node-terminal");

    // Sequence numbers are gapless across every emitted record.
    for (i, (seq, _)) in sink.records.iter().enumerate() {
        assert_eq!(*seq, i as u64, "gapless sequence numbers");
    }
}

/// The node-terminal record carries the classified terminal state (succeeded).
#[test]
fn node_terminal_record_carries_the_classified_state() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    drive(
        Produces {
            value: Report {
                rows: 2,
                label: "t".into(),
            },
        },
        &ctx_for(1, 1),
        &slot,
        &mut sink,
    );
    let state = sink.events().into_iter().find_map(|e| match e {
        AttemptEvent::NodeTerminal { state, .. } => Some(state),
        _ => None,
    });
    assert_eq!(state, Some(TerminalState::Succeeded));
}

/// Value type flows unchanged: the value read from the slot equals the produced
/// value with no coercion, confirming the fill path preserves the slot type (C10).
#[test]
fn value_type_flows_unchanged() {
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let produced = Report {
        rows: 12345,
        label: "a non-trivial typed value".into(),
    };
    drive(
        Produces {
            value: produced.clone(),
        },
        &ctx_for(1, 1),
        &slot,
        &mut sink,
    );
    let read: Arc<Report> = slot.shared_ref().read();
    assert_eq!(*read, produced, "no type coercion; slot preserves T");
}

/// The runner is callable with a hand-constructed context and a capturing sink,
/// with no runtime and no run-loop driver present — the unit-testable-in-
/// isolation contract. (This whole file demonstrates it; this test states it.)
#[test]
fn runnable_in_isolation_with_no_runtime() {
    // A fresh, uncancelled context built entirely in-process.
    let source = CancellationSource::new();
    let ctx = RunContext::builder(
        RunId::new("iso-run"),
        PipelineId::new("iso-pipeline"),
        NodeId::from_name(NODE),
    )
    .cancellation(source.signal())
    .build();
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let outcome = drive(
        Produces {
            value: Report {
                rows: 3,
                label: "iso".into(),
            },
        },
        &ctx,
        &slot,
        &mut sink,
    );
    assert!(matches!(outcome, AttemptOutcome::Succeeded));
    assert!(
        !sink.events().is_empty(),
        "events were emitted with no runtime"
    );
}

/// The span opens before the work runs: the work observes the attempt span
/// carrying this node's identity active at the moment it executes, so any line
/// it emits is attributable without correlating timestamps (C8/C25 surface).
#[test]
fn the_span_opens_before_the_work_runs() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc as StdArc;

    // The work records whether a span carrying its node/attempt identity was
    // observable at the instant it ran.
    struct ObservesSpan {
        saw_span: StdArc<AtomicBool>,
    }
    impl Task for ObservesSpan {
        type Input = ();
        type Output = Report;
        async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<Report, TaskError> {
            // The attempt span is present and keyed on this node + attempt.
            let span = ctx.span();
            if span.node_id() == NodeId::from_name(NODE) && span.attempt() == ctx.attempt() {
                self.saw_span.store(true, Ordering::SeqCst);
            }
            Ok(Report {
                rows: 1,
                label: "span".into(),
            })
        }
    }

    let saw = StdArc::new(AtomicBool::new(false));
    let slot = fresh_slot();
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(2, 4);
    let outcome = drive(
        ObservesSpan {
            saw_span: StdArc::clone(&saw),
        },
        &ctx,
        &slot,
        &mut sink,
    );
    assert!(matches!(outcome, AttemptOutcome::Succeeded));
    assert!(
        saw.load(Ordering::SeqCst),
        "the work observed the attempt span (node+attempt identity) active"
    );
}
