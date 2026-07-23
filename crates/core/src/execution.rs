//! The C14 **single-attempt execution core** — the load-bearing spine of the
//! attempt runner (arch.md `### C14 · Attempt runner`).
//!
//! This module runs **one** attempt of **one** node end to end:
//!
//! 1. open the attempt span (already carried on the [`RunContext`], keyed on
//!    run / node / attempt — the C25 surface T45 consumes) *before* the work
//!    runs, so everything beneath it is attributable;
//! 2. record the informational admission phase marker;
//! 3. emit the `attempt-started` per-transition event;
//! 4. dispatch the **already-placed** work — [`Task::run`] — and await its
//!    result (this runner is runtime-agnostic: the caller's runtime drives the
//!    future; execution-class placement is C13 / T33, not this ticket);
//! 5. **classify** the outcome into the normative taxonomy (arch.md
//!    Vocabulary) — success, permanent failure, retry-eligible failure, or a
//!    deliberate skip;
//! 6. on success **only**, fill the node's once-writable output slot (C10 /
//!    T17) with the produced value (declared output residency transfers to the
//!    slot at fill);
//! 7. emit the closing per-transition event (`attempt-succeeded` /
//!    `attempt-failed`) — which is the **exactly-one attempt-outcome record**
//!    for this attempt (arch.md C14) — and the `node-terminal` record carrying
//!    the classified terminal state.
//!
//! Every attempt, regardless of outcome, produces exactly one attempt-outcome
//! record alongside its per-transition events. A non-success attempt never
//! fills the slot.
//!
//! # Runtime-agnostic, dependency-free
//!
//! [`run_attempt`] is an `async fn` awaited by a caller-provided runtime; it
//! adds **no** async-runtime dependency to `dagr-core`. Per the T2 ADR (004)
//! await-bound work ultimately runs on tokio, but *placement* — choosing the
//! class and spawning onto the await / blocking / compute pool — is C13 / T33.
//! This core "receives work that is already on the correct thread/runtime" and
//! assumes the work returns (T20 Out of scope).
//!
//! # Event emission through an abstract port (keeps `dagr-core` dependency-free)
//!
//! The C19 event-stream writer lives in `dagr-artifact`, and `dagr-core` must
//! not depend on it (workspace ADR T1: core depends on nothing; the C24
//! renderer boundary). So the runner emits through the **abstract**
//! [`AttemptEventSink`] port defined here. The run-loop driver (T24, in
//! `dagr-cli`, which depends on both crates) adapts the concrete
//! `dagr_artifact::event_stream::EventStreamWriter` to this port; tests use a
//! plain capturing sink with no runtime. The [`AttemptEvent`] variants this
//! runner emits map one-to-one onto C19's closed event vocabulary.
//!
//! # What this ticket owns, and what it reserves
//!
//! T20 owns the **single-attempt** path only. Deliberately **not** here, each
//! with its owning ticket:
//!
//! - **retry / backoff / attempt loop (T22)** — this runner classifies an
//!   outcome as retry-eligible but never loops, schedules no second attempt,
//!   and computes no delay;
//! - **per-attempt timeout / abandonment (T21)** — no timer is started, no
//!   future is dropped, no zombie accounting; the work is assumed to return;
//! - **panic containment (T23)** — no `catch_unwind` boundary; a panicking task
//!   unwinds through this core (T23 wraps that boundary);
//! - **execution-class dispatch (C13 / T33)** — the runner does not choose the
//!   class or spawn onto a pool;
//! - **the run-loop driver (T24)** — admission, readiness feedback,
//!   run-started / run-finished events, and run-identity minting are the
//!   driver's.
//!
//! The [`AttemptOutcome`] enum is `#[non_exhaustive]` and its rustdoc names the
//! `TimedOut` (T21) and `Panicked` (T23) variants those tickets add **without
//! reshaping** the four T20 owns.

use crate::context::{PipelineId, RunContext, RunId, TerminalState};
use crate::error::{TaskError, TaskErrorClass};
use crate::handle::NodeId;
use crate::slot::Slot;
use crate::task::Task;

/// One record the single-attempt runner emits, mapped one-to-one onto the C19
/// event vocabulary (arch.md `### C19 · Event stream`; the writer is T19).
///
/// This is the **abstract** event shape the runner produces; the run-loop
/// driver (T24) translates each variant into the concrete
/// `dagr_artifact::event_stream::Event` and stamps the C19 envelope (run
/// identity, schema version, gapless sequence, wall stamp, monotonic offset).
/// Keeping it abstract is what lets `dagr-core` emit events without depending on
/// `dagr-artifact` (workspace ADR T1 / the C24 boundary).
///
/// Only the records a **single attempt** is responsible for appear here. The
/// `node-ready`, `run-started`, `run-finished`, and `zombie-at-exit` records
/// are the driver's / readiness tracker's (T24), not this runner's.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttemptEvent {
    /// Informational admission phase marker: the node moved from waiting into
    /// running (arch.md C14 *Behavior*: "record the waiting and admission
    /// phases"). Permit accounting itself is C12 / T31 — this marker is
    /// informational, not an admission decision.
    NodeAdmitted {
        /// The node's author-declared identity name (T13).
        node: String,
    },
    /// The attempt began — the opening per-transition event.
    AttemptStarted {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number, from the C8 context.
        attempt: u32,
    },
    /// The attempt returned a value — the closing per-transition event and the
    /// **exactly-one attempt-outcome record** for a successful attempt.
    AttemptSucceeded {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number, from the C8 context.
        attempt: u32,
    },
    /// The attempt failed (permanent, retry-eligible, or a deliberate skip) —
    /// the closing per-transition event and the **exactly-one attempt-outcome
    /// record** for a non-successful attempt.
    AttemptFailed {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number, from the C8 context.
        attempt: u32,
    },
    /// The attempt exceeded its per-attempt timeout (T21) — the closing
    /// per-transition event and the **exactly-one attempt-outcome record** for a
    /// timed-out attempt (arch.md C14: "Every attempt produces exactly one
    /// *attempt-outcome* record … including attempts that timed out").
    ///
    /// This is emitted **immediately at the timeout mark** for *every* execution
    /// class: an await-bound attempt whose future was dropped, and a
    /// blocking/compute attempt whose fate is decided while its closure runs on
    /// as abandoned-but-running work (T0.3 ADR §1). The node's terminal state is
    /// decided once here; a lingering thread's eventual return is a
    /// `zombie-at-exit` **event** (the driver's, C19 / T24), never a second
    /// terminal state.
    AttemptTimedOut {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number, from the C8 context.
        attempt: u32,
    },
    /// The node entered a **backoff/waiting phase** between two attempts (T22):
    /// a retry-eligible attempt failed and the retry loop is waiting `delay`
    /// before dispatching the next attempt. This names the backoff interval as a
    /// distinct, measurable phase (feeding the C23 phase timings — arch.md C14
    /// "record the waiting … phases"), so the interval between the failing
    /// attempt and the next attempt start is attributable to *backoff*, not to
    /// executing.
    ///
    /// It is emitted **after** the failing attempt's closing outcome record and
    /// **before** the next attempt's `attempt-started`. `attempt` is the number
    /// of the attempt that just failed (the one being backed off *from*); the
    /// next attempt is `attempt + 1`. Only the retry loop (T22) emits this; a
    /// single attempt (T20 / T21) never does.
    BackoffStarted {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based number of the attempt that just failed — the retry loop
        /// waits before dispatching attempt `attempt + 1`.
        attempt: u32,
        /// The scheduled backoff delay (base·factor^n, capped, with jitter) the
        /// loop waits before the next attempt. Recorded as the phase's measured
        /// interval; the actual sleeping is the driver's (T24/T33).
        delay: Duration,
    },
    /// The node reached a terminal state from the normative taxonomy (arch.md
    /// Vocabulary), carrying the classified state.
    NodeTerminal {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The normative terminal state this attempt decided.
        state: TerminalState,
    },
}

/// The **abstract event-emission port** the single-attempt runner writes
/// through (see the [module docs](self)).
///
/// The runner calls [`emit`](AttemptEventSink::emit) once per record, in order.
/// The run-loop driver (T24) implements this over the concrete C19
/// `EventStreamWriter` (stamping the envelope and appending to the run store);
/// tests implement it with an in-memory capturing collector. Defining the port
/// in `dagr-core` — rather than depending on `dagr-artifact` — is what keeps the
/// execution core dependency-free (workspace ADR T1) and the C24 boundary
/// intact.
///
/// # Note on fallibility
///
/// A real C19 sink can fault mid-run (an unwritable event stream — C19 / T0.6),
/// which the driver (T24) turns into run-level cancellation. That fault path is
/// the driver's to own; this single-attempt port is **infallible** so the T20
/// core stays focused on running one attempt. The driver's adaptor absorbs a
/// `SinkFault` and reacts per C19; the runner is not the place to decide the
/// run's fate on a sink error.
pub trait AttemptEventSink {
    /// Emit one attempt record. Called by the runner in emission order.
    fn emit(&mut self, event: AttemptEvent);
}

/// The classified outcome of **one** attempt, in the normative taxonomy
/// (arch.md Vocabulary; the runner's C14 outcome surface).
///
/// This is the framework-internal runner taxonomy — a strict superset of the
/// task-facing [`TaskError`] three-valued surface (T3 ADR §11) — restricted to
/// the four outcomes T20 owns plus the [`TimedOut`](AttemptOutcome::TimedOut)
/// variant T21 (031) adds:
///
/// - [`Succeeded`](AttemptOutcome::Succeeded) — the work returned a value; the
///   slot was filled.
/// - [`PermanentFailure`](AttemptOutcome::PermanentFailure) — a retry-ineligible
///   error; **never** treated as retry-eligible regardless of remaining
///   attempts (arch.md C14).
/// - [`RetryEligibleFailure`](AttemptOutcome::RetryEligibleFailure) — a
///   retry-eligible error; **distinct** from permanent so the retry driver (T22)
///   can act on it. This runner schedules no retry.
/// - [`Skipped`](AttemptOutcome::Skipped) — a deliberate (originated) skip;
///   distinct from both success and failure.
///
/// # The T21 timeout variant, and the T23 reservation
///
/// The enum is `#[non_exhaustive]` precisely so the operationally-hard siblings
/// extend it rather than rewrite it:
///
/// - **`TimedOut`** — per-attempt timeout (T21, added by **this** ticket).
///   Timeout is retry-eligible by default, subject to the node's budget (arch.md
///   C14). Its terminal state is [`TerminalState::TimedOut`]. It is a failure,
///   but a **distinct** one from the two T20 failure classes, because the
///   per-class abandonment semantics (await-bound future-drop vs blocking/compute
///   permit-held-until-return, T0.3 ADR §1) are decided at the timeout mark, not
///   by mapping to `PermanentFailure` / `RetryEligibleFailure`.
/// - **`Panicked`** — a caught panic converted to a permanent failure (T23,
///   still reserved). Its terminal state is [`TerminalState::Failed`] (arch.md
///   Vocabulary: "a caught panic" is `failed`).
///
/// T23 does not populate its variant yet (this runner catches no panic); naming
/// it here is the stable-shape contract T23 plugs into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttemptOutcome {
    /// The work returned a value; the output slot was filled (`succeeded`).
    Succeeded,
    /// A permanent, retry-ineligible failure (`failed`). Never retry-eligible.
    PermanentFailure,
    /// A retry-eligible failure (`failed` once the budget is exhausted). The
    /// retry driver (T22) may schedule another attempt; this runner does not.
    RetryEligibleFailure,
    /// A deliberate, originated skip (`skipped`).
    Skipped,
    /// The attempt exceeded its per-attempt timeout (`timed-out`, T21).
    ///
    /// **Retry-eligible by default**, subject to the node's retry budget (arch.md
    /// C14) — a timeout enters the retry path rather than terminating the node
    /// immediately; a timeout on the last permitted attempt yields terminal
    /// [`TerminalState::TimedOut`]. The per-class abandonment (an await-bound
    /// future is dropped and its permit released immediately; a blocking/compute
    /// closure runs on as abandoned-but-running work whose permit is held until
    /// it returns) is handled by the runner at the timeout mark (T0.3 ADR §1),
    /// not by this classification.
    TimedOut,
}

impl AttemptOutcome {
    /// The normative terminal state this outcome maps to (arch.md Vocabulary).
    ///
    /// Both failure classes map to [`TerminalState::Failed`] as a *single-attempt*
    /// terminal — a retry-eligible failure only becomes a non-`failed` fate when
    /// a *later* attempt succeeds, which is the retry driver's (T22) concern, not
    /// this single attempt's. The distinct [`RetryEligibleFailure`](AttemptOutcome::RetryEligibleFailure)
    /// classification is what lets T22 decide to retry *before* this maps to a
    /// terminal state.
    #[must_use]
    pub fn terminal_state(self) -> TerminalState {
        match self {
            AttemptOutcome::Succeeded => TerminalState::Succeeded,
            AttemptOutcome::PermanentFailure | AttemptOutcome::RetryEligibleFailure => {
                TerminalState::Failed
            }
            AttemptOutcome::Skipped => TerminalState::Skipped,
            // A timeout is and stays `timed-out` (arch.md Vocabulary; T0.3 ADR
            // §6) — it is decided once at the timeout mark and never becomes
            // `abandoned` (that state arises only on the cancellation path, C16).
            // On a non-final attempt this is a per-attempt terminal that the
            // retry driver (T22) may still turn into a later success; on the last
            // permitted attempt it is the node's terminal state.
            AttemptOutcome::TimedOut => TerminalState::TimedOut,
        }
    }

    /// Whether this attempt succeeded (the slot was filled).
    #[must_use]
    pub fn is_success(self) -> bool {
        matches!(self, AttemptOutcome::Succeeded)
    }

    /// Whether this outcome is **retry-eligible** — the signal the retry driver
    /// (T22) acts on. A permanent failure is **never** retry-eligible (arch.md
    /// C14), and a success or skip is not a failure at all. A
    /// [`TimedOut`](AttemptOutcome::TimedOut) attempt **is** retry-eligible by
    /// default (arch.md C14: "Timeout is retry-eligible by default, subject to
    /// the node's retry budget") — for a blocking/compute node that retry is
    /// deferred until the previous closure returns (T0.3 ADR §5), which the
    /// runner enforces via the [`TimeoutDecision`] barrier, not this predicate.
    #[must_use]
    pub fn is_retry_eligible(self) -> bool {
        matches!(
            self,
            AttemptOutcome::RetryEligibleFailure | AttemptOutcome::TimedOut
        )
    }

    /// Whether this attempt failed (permanent, retry-eligible, or timed out). A
    /// skip is **not** a failure (arch.md Vocabulary: `skipped` is skip-like),
    /// and a success is not. A [`TimedOut`](AttemptOutcome::TimedOut) attempt is
    /// failure-like (arch.md Vocabulary: `timed-out` is failure-like).
    #[must_use]
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            AttemptOutcome::PermanentFailure
                | AttemptOutcome::RetryEligibleFailure
                | AttemptOutcome::TimedOut
        )
    }
}

/// Run **one** attempt of one node to a decided outcome (arch.md `### C14`).
///
/// This is the single-attempt runner entry point. It drives the C14 order —
/// open span, record the admission phase marker, dispatch the work, await the
/// outcome, classify it, then fill the slot on success or report the classified
/// failure/skip — emitting the ordered per-transition events plus exactly one
/// attempt-outcome record through the injected [`AttemptEventSink`].
///
/// - `task` is taken by `&mut self` (C1), the shape that makes sequential
///   re-runs safe — this call performs **one** attempt; the loop is T22's.
/// - `node` is the node's author-declared name (C19 records key nodes by name;
///   `NodeId` is opaque with no route back to a name, so the caller supplies it
///   from assembly).
/// - `ctx` supplies the attempt number and maximum (C8) that appear identically
///   in the span, the events, and the outcome record, and the attempt span the
///   work observes.
/// - `slot` is the node's single once-writable output slot (C10 / T17); it is
///   filled **only** on success and left untouched on any non-success outcome.
/// - `sink` is the abstract C19 emission port (T24 adapts it to the real
///   writer; tests capture in memory).
///
/// The work is **already placed** on the correct runtime/thread (T33); this
/// runner only awaits it. Returns the classified [`AttemptOutcome`].
///
/// # Panics
///
/// This single-attempt core installs **no** `catch_unwind` boundary (that is
/// T23): a panic in the task's work unwinds through this future. T23 wraps the
/// boundary that converts a caught panic into [`AttemptOutcome`]'s reserved
/// `Panicked` variant.
pub async fn run_attempt<T, S>(
    task: &mut T,
    node: &str,
    ctx: &RunContext,
    slot: &Slot<T::Output>,
    sink: &mut S,
) -> AttemptOutcome
where
    T: Task<Input = ()>,
    S: AttemptEventSink + ?Sized,
{
    // A single attempt IS terminal: run the attempt (emitting the opening
    // events + the exactly-one attempt-outcome record) and then, because the
    // node ends here, emit the node-terminal record. The retry loop (T22)
    // instead calls the no-terminal helper per attempt and emits *one*
    // node-terminal record when the loop ends.
    let outcome = run_one_attempt(task, node, ctx, slot, sink).await;
    emit_node_terminal(node, outcome.terminal_state(), sink);
    outcome
}

/// Run one attempt end to end and emit its opening events plus the **exactly-one
/// attempt-outcome record**, but **not** the node-terminal record.
///
/// This is the shared body of [`run_attempt`] and the retry loop
/// ([`run_with_retries`], T22): the loop drives this once per attempt (so each
/// attempt gets its own outcome record) and emits the single node-terminal
/// record itself when the loop terminates. Splitting the terminal record out is
/// what lets a retried node have many attempt-outcome records but exactly one
/// node-terminal record, while keeping [`run_attempt`]'s single-attempt
/// behaviour byte-identical (it composes this with the terminal emission).
async fn run_one_attempt<T, S>(
    task: &mut T,
    node: &str,
    ctx: &RunContext,
    slot: &Slot<T::Output>,
    sink: &mut S,
) -> AttemptOutcome
where
    T: Task<Input = ()>,
    S: AttemptEventSink + ?Sized,
{
    let attempt = ctx.attempt();

    // (1) The attempt span is already open on the `RunContext` (C8/C25), keyed
    // on run/node/attempt — opened before the work runs so everything beneath
    // it is attributable. (2) Record the informational admission phase marker.
    sink.emit(AttemptEvent::NodeAdmitted { node: node.into() });

    // (3) Opening per-transition event, emitted before the work is dispatched.
    sink.emit(AttemptEvent::AttemptStarted {
        node: node.into(),
        attempt,
    });

    // (4) Dispatch the already-placed work and await its result. The runner is
    // runtime-agnostic; the caller's runtime drives this future (T33 places).
    let result = task.run(ctx, ()).await;

    // (5) Classify into the normative taxonomy, and (6) fill the slot on
    // success only.
    let outcome = match result {
        Ok(value) => {
            // Declared output residency transfers to the slot at fill (C10). A
            // fresh slot is empty, so this fill succeeds; a refused second fill
            // would be a framework defect (the runner fills a node's slot at
            // most once — sequential attempts do not overlap, C1), so a rejected
            // fill is dropped rather than silently swallowed as success.
            let _ = slot.fill(value);
            AttemptOutcome::Succeeded
        }
        Err(err) => match err.class() {
            TaskErrorClass::Permanent => AttemptOutcome::PermanentFailure,
            TaskErrorClass::Retryable => AttemptOutcome::RetryEligibleFailure,
            TaskErrorClass::Skip => AttemptOutcome::Skipped,
        },
    };

    // (7) The exactly-one attempt-outcome record for this attempt. The
    // node-terminal record is the caller's to emit (once), so a retried node
    // gets one outcome record per attempt but a single node-terminal record.
    emit_attempt_outcome_record(node, attempt, outcome, sink);

    outcome
}

/// Emit the **closing** per-transition record — the exactly-one attempt-outcome
/// record — followed by the node-terminal record carrying the classified state.
///
/// This is the one place the outcome→record mapping lives, so [`run_attempt`],
/// [`run_attempt_with_timeout`], and the blocking/compute [`TimeoutDecision`]
/// mark all emit the *same* records for the *same* outcome (the exactly-one
/// contract, arch.md C14 / C19). Each outcome maps to exactly one outcome
/// record: success → `attempt-succeeded`; timeout → `attempt-timed-out`;
/// permanent/retry-eligible/skip → `attempt-failed`.
fn emit_closing_events<S>(node: &str, attempt: u32, outcome: AttemptOutcome, sink: &mut S)
where
    S: AttemptEventSink + ?Sized,
{
    emit_attempt_outcome_record(node, attempt, outcome, sink);
    emit_node_terminal(node, outcome.terminal_state(), sink);
}

/// Emit **only** the exactly-one attempt-outcome record for one attempt (arch.md
/// C14 / C19), without the node-terminal record.
///
/// Split out of [`emit_closing_events`] so the retry loop ([`run_with_retries`],
/// T22) can emit one outcome record **per attempt** while deferring the *single*
/// node-terminal record to the moment the loop actually terminates — a retried
/// node has many attempt-outcome records but exactly one node-terminal record.
/// A single attempt (T20 / T21) composes this with [`emit_node_terminal`] via
/// [`emit_closing_events`], so their behaviour is byte-identical to before.
fn emit_attempt_outcome_record<S>(node: &str, attempt: u32, outcome: AttemptOutcome, sink: &mut S)
where
    S: AttemptEventSink + ?Sized,
{
    match outcome {
        AttemptOutcome::Succeeded => sink.emit(AttemptEvent::AttemptSucceeded {
            node: node.into(),
            attempt,
        }),
        AttemptOutcome::TimedOut => sink.emit(AttemptEvent::AttemptTimedOut {
            node: node.into(),
            attempt,
        }),
        AttemptOutcome::PermanentFailure
        | AttemptOutcome::RetryEligibleFailure
        | AttemptOutcome::Skipped => sink.emit(AttemptEvent::AttemptFailed {
            node: node.into(),
            attempt,
        }),
    }
}

/// Emit the single node-terminal record carrying the classified terminal state.
/// The retry loop calls this exactly once, at loop termination; a single attempt
/// calls it via [`emit_closing_events`].
fn emit_node_terminal<S>(node: &str, state: TerminalState, sink: &mut S)
where
    S: AttemptEventSink + ?Sized,
{
    sink.emit(AttemptEvent::NodeTerminal {
        node: node.into(),
        state,
    });
}

// ===========================================================================
// C14 · per-attempt timeout (T21)
// ===========================================================================
//
// The per-attempt timeout is **runtime-agnostic**, exactly like the T20 core:
// `dagr-core` adds **no** async-runtime dependency (workspace ADR T1). The runner
// *races* the attempt future against a **caller-provided deadline future** —
// whatever future resolves when the per-attempt timeout elapses. In production
// that deadline is a `tokio::time` sleep armed on the framework's **isolated**
// runtime (T2 ADR §5), so a saturated task pool cannot disable it (C13); in a
// unit test it is any controllable pinned-clock future (no runtime needed). The
// class fork is exactly the one the T0.3 ADR (009 §1) fixed:
//
// - **await-bound** — the one shape Rust can cancel. On timeout the runner drops
//   the attempt future (true cancellation, arch.md C14). A permit-shaped guard
//   moved **into** that future is dropped with it, so the permit releases
//   **immediately** and no zombie is recorded (T0.3 ADR §1, §2). This is
//   [`run_attempt_with_timeout`].
//
// - **blocking / compute** — synchronous, unkillable closures. The framework
//   *cannot* stop the thread, so it **marks** the attempt `timed-out` at once and
//   raises the late-result barrier, but the closure runs on as
//   *abandoned-but-running* work whose permit is **held until it actually
//   returns** (observed by the guard dropping when the closure body ends, off the
//   run loop — never joined synchronously). A retry is deferred until that return
//   (C1 exclusivity). This is [`TimeoutDecision::mark_blocking_timed_out`].
//
// This ticket owns **only** the per-attempt timeout facet. The retry *loop*
// (T22), panic containment (T23), the run-loop driver that arms the real timer
// and adapts the sink (T24), execution-class dispatch that places the closure on
// the blocking/compute pool (T33), and the concrete admission ledger the permit
// guard is a stand-in for (T31) are their own tickets.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Run one attempt of an **await-bound** node under a per-attempt timeout,
/// racing its future against a caller-provided `deadline` (T21; arch.md C14
/// "Timeout semantics differ by class, honestly").
///
/// This is the await-bound half of the per-class timeout — the one shape Rust
/// can truly cancel. It emits the same admission / `attempt-started` opening as
/// [`run_attempt`], then races the task future against `deadline`:
///
/// - **the attempt wins** (returns within its timeout): behaviour is **identical
///   to [`run_attempt`]** — classify the outcome, fill the slot on success only,
///   emit the closing outcome record and the node-terminal record. `permit` is
///   held for the whole attempt and released when this function returns (its
///   guard drops), i.e. on the normal terminal path.
/// - **the deadline wins** (the attempt exceeds its timeout): the attempt future
///   is **dropped** — true cancellation, so the awaited work does not run to
///   completion — which drops `permit` and releases its cost **immediately**
///   (T0.3 ADR §1). The node is marked [`AttemptOutcome::TimedOut`]: the
///   `attempt-timed-out` outcome record and the `timed-out` node-terminal record
///   are emitted, the slot is **never** filled, and no zombie is recorded (an
///   await-bound future that is dropped leaves no leftover thread).
///
/// `permit` is any guard whose `Drop` returns its cost to the admission ledger
/// (C12 / T31); moving it **into** the raced future is what makes future-drop
/// release the permit for free (the T0.3 ownership trick). A caller that tracks
/// no permit passes `()`.
///
/// # Runtime-agnostic
///
/// No async-runtime dependency is added: the caller's runtime drives this future
/// and supplies `deadline` (a framework-timer future in production — armed on the
/// isolated runtime so it fires even when task workers are saturated, C13 — or a
/// pinned-clock future in a unit test). See the [module docs](self).
///
/// # Panics
///
/// Like [`run_attempt`], this installs **no** `catch_unwind` boundary (T23): a
/// panic in the task's work unwinds through the future. Timeout and panic are
/// independent facets.
pub async fn run_attempt_with_timeout<T, S, D, P>(
    mut task: T,
    node: &str,
    ctx: &RunContext,
    slot: &Slot<T::Output>,
    sink: &mut S,
    deadline: D,
    permit: P,
) -> AttemptOutcome
where
    T: Task<Input = ()>,
    T::Output: Send,
    S: AttemptEventSink + ?Sized,
    D: Future<Output = ()> + Send,
    P: Send,
{
    let attempt = ctx.attempt();

    // Same opening as `run_attempt`: informational admission marker, then the
    // opening per-transition event, emitted before the work is dispatched.
    sink.emit(AttemptEvent::NodeAdmitted { node: node.into() });
    sink.emit(AttemptEvent::AttemptStarted {
        node: node.into(),
        attempt,
    });

    // Move `permit` INTO the raced work future so that dropping the work future
    // (the timeout branch below) drops the permit and releases it immediately —
    // the T0.3 ownership trick for await-bound cancellation. On the normal path
    // the work future completes and `permit` is dropped when it is consumed here.
    let work = async move {
        let result = task.run(ctx, ()).await;
        drop(permit); // released on the normal terminal path (explicit for clarity)
        result
    };

    // Race the work against the deadline. `Timed::attempt` returns the work's
    // result; `Timed::deadline` fires when the timeout elapses, at which point
    // the work future — and the permit it owns — is dropped without completing.
    let outcome = match race(work, deadline).await {
        Race::A(result) => classify_and_fill(result, slot),
        Race::B(()) => {
            // Timeout: the work future was dropped by the race (permit released
            // immediately, work not run to completion). Mark `timed-out`.
            AttemptOutcome::TimedOut
        }
    };

    emit_closing_events(node, attempt, outcome, sink);
    outcome
}

/// Classify one attempt's `Result` into the normative taxonomy and fill the slot
/// on success only — the shared body of the success/failure/skip classification
/// used by both timed and untimed await paths.
fn classify_and_fill<O>(result: Result<O, TaskError>, slot: &Slot<O>) -> AttemptOutcome
where
    O: Send + Sync + 'static,
{
    match result {
        Ok(value) => {
            // Fill the once-writable slot (C10). A fresh slot is empty, so this
            // succeeds; a refused fill is a framework defect, so the rejected
            // value is dropped rather than swallowed as success.
            let _ = slot.fill(value);
            AttemptOutcome::Succeeded
        }
        Err(err) => match err.class() {
            TaskErrorClass::Permanent => AttemptOutcome::PermanentFailure,
            TaskErrorClass::Retryable => AttemptOutcome::RetryEligibleFailure,
            TaskErrorClass::Skip => AttemptOutcome::Skipped,
        },
    }
}

/// The result of racing two futures: whichever resolved first. The **loser is
/// dropped** — for await-bound cancellation that drop is the cancellation.
enum Race<A, B> {
    A(A),
    B(B),
}

/// Race two futures, resolving as soon as **either** completes and dropping the
/// other — a minimal, dependency-free, **`unsafe`-free** `select` (no tokio). `a`
/// is polled first, so a future already ready when the race begins (e.g. a
/// deadline that elapsed while task workers were jammed) wins deterministically
/// on the first poll.
///
/// This is what keeps the per-attempt timeout runtime-agnostic: the caller's
/// executor drives this combinator exactly as it drives any other future, and
/// dropping the losing future is Rust's own cancellation. The two futures are
/// **heap-pinned** ([`Box::pin`]) so their fields never move — `Pin<Box<_>>` is
/// [`Unpin`], which is what lets the combinator poll them through safe pin
/// projection with **no `unsafe`** (dagr targets safe Rust; `unsafe_code` is
/// warned and warnings are denied). One allocation per attempt is negligible
/// against a per-attempt timeout.
fn race<'f, A, B>(a: A, b: B) -> RaceFuture<'f, A::Output, B::Output>
where
    A: Future + Send + 'f,
    B: Future + Send + 'f,
{
    RaceFuture {
        a: Some(Box::pin(a)),
        b: Some(Box::pin(b)),
    }
}

/// The [`Future`] returned by [`race`]. It owns both futures **heap-pinned**; on
/// completion it drops the loser (dropping the losing future is the
/// cancellation). Because `Pin<Box<_>>` is [`Unpin`], the combinator itself is
/// `Unpin` and needs no `unsafe` to poll its fields. The boxed futures are `Send`
/// so racing does not poison the `Send`-ness of the caller's attempt future.
struct RaceFuture<'f, A, B> {
    a: Option<Pin<Box<dyn Future<Output = A> + Send + 'f>>>,
    b: Option<Pin<Box<dyn Future<Output = B> + Send + 'f>>>,
}

impl<A, B> Future for RaceFuture<'_, A, B> {
    type Output = Race<A, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `Self` is `Unpin` (both fields are `Pin<Box<_>>`), so `get_mut` is safe.
        let this = self.as_mut().get_mut();
        if let Some(a) = this.a.as_mut() {
            // Poll `a` first: a deadline/attempt already ready wins on this poll.
            if let Poll::Ready(out) = a.as_mut().poll(cx) {
                this.a = None;
                this.b = None; // drop the loser (cancellation)
                return Poll::Ready(Race::A(out));
            }
        }
        if let Some(b) = this.b.as_mut() {
            if let Poll::Ready(out) = b.as_mut().poll(cx) {
                this.a = None; // drop the loser (cancellation)
                this.b = None;
                return Poll::Ready(Race::B(out));
            }
        }
        Poll::Pending
    }
}

/// The decision the runner reaches when a **blocking or compute** attempt exceeds
/// its per-attempt timeout (T21; arch.md C14; T0.3 ADR §1, §4, §5, §6).
///
/// Blocking and compute closures are synchronous and **unkillable** — the
/// framework cannot stop the thread, so it does not pretend to. Constructing a
/// `TimeoutDecision` via [`mark_blocking_timed_out`](TimeoutDecision::mark_blocking_timed_out)
/// **marks the attempt `timed-out` immediately**: the node's fate is decided and
/// the `attempt-timed-out` + `timed-out` node-terminal records are emitted at the
/// mark. What it deliberately does **not** do is release the permit or fill any
/// slot: the closure keeps running as *abandoned-but-running* work, and
///
/// - the permit is held until the closure **actually returns** — the caller keeps
///   the permit guard alive inside the closure so the guard's drop (on return) is
///   what releases the cost (C12; T0.3 ADR §2). The runner never joins the
///   closure synchronously.
/// - a **retry is deferred** until the previous closure returns: while the node
///   still has a live zombie, [`retry_may_start`](TimeoutDecision::retry_may_start)
///   is `false`, preventing the same task instance from running concurrently with
///   its own zombie (C1 exclusivity; T0.3 ADR §5).
/// - a **late-result barrier** ([`barrier`](TimeoutDecision::barrier)) refuses any
///   post-timeout slot fill or scratch write, so whatever the abandoned closure
///   computes after the mark is discarded (T0.3 ADR §4).
///
/// The terminal state is decided **exactly once** — `timed-out` — and never
/// becomes `abandoned` (that arises only on the cancellation path, C16; T0.3 ADR
/// §6). A leftover thread still running at process exit is a `zombie-at-exit`
/// **event** (the driver's, C19 / T24), not a second terminal state.
#[derive(Debug, Clone, Copy)]
pub struct TimeoutDecision {
    outcome: AttemptOutcome,
}

impl TimeoutDecision {
    /// Mark a **blocking or compute** attempt `timed-out` at the moment its
    /// per-attempt timeout fires (arch.md C14; T0.3 ADR §1).
    ///
    /// Emits the `attempt-timed-out` outcome record and the `timed-out`
    /// node-terminal record **immediately** — the node's fate is decided now —
    /// and returns the decision, which carries the retry-eligible
    /// [`TimedOut`](AttemptOutcome::TimedOut) outcome and mints the late-result
    /// [`barrier`](TimeoutDecision::barrier). It does **not** touch the permit or
    /// the slot: the caller holds the permit inside the still-running closure
    /// (released on the closure's actual return, T0.3 ADR §2) and the barrier
    /// bars any late fill/scratch.
    #[must_use]
    pub fn mark_blocking_timed_out<S>(node: &str, ctx: &RunContext, sink: &mut S) -> Self
    where
        S: AttemptEventSink + ?Sized,
    {
        let outcome = AttemptOutcome::TimedOut;
        // Decide the fate now: emit the timed-out outcome record + node-terminal.
        // The permit is untouched (held by the running closure); the slot is
        // untouched (a timed-out attempt never fills it).
        emit_closing_events(node, ctx.attempt(), outcome, sink);
        Self { outcome }
    }

    /// The classified outcome — always [`AttemptOutcome::TimedOut`], which is
    /// retry-eligible by default (arch.md C14).
    #[must_use]
    pub fn outcome(self) -> AttemptOutcome {
        self.outcome
    }

    /// The **late-result barrier** for the abandoned closure: any slot fill or
    /// scratch write it attempts after the timeout mark is refused (T0.3 ADR §4).
    #[must_use]
    pub fn barrier(self) -> LateResultBarrier {
        LateResultBarrier { _private: () }
    }

    /// Whether a retry of this timed-out node may begin yet.
    ///
    /// A blocking/compute retry is **deferred until the previous attempt's closure
    /// has returned** (C1 exclusivity; T0.3 ADR §5) — the alternative is the same
    /// task instance running concurrently with its own zombie. This returns
    /// `false` while `zombies` reports a live zombie for the node and `true` once
    /// the closure has returned (its permit dropped, clearing the zombie).
    ///
    /// `zombies` is any observer of the admission ledger's live-zombie state (the
    /// concrete ledger is T31); the runner consults it rather than joining the
    /// closure, so a live zombie never blocks the run loop.
    #[must_use]
    pub fn retry_may_start<Z: ZombieObserver + ?Sized>(self, zombies: &Z) -> bool {
        // Timeout is retry-eligible, but only after the zombie has cleared.
        self.outcome.is_retry_eligible() && !zombies.has_live_zombie()
    }
}

/// Observes whether abandoned-but-running (zombie) work is still live — the
/// signal that defers a timed-out blocking/compute node's retry until its
/// previous closure has returned (T0.3 ADR §5).
///
/// The concrete admission ledger (C12 / T31) implements this; the runner reads it
/// through this narrow port so the timeout facet does not depend on the ledger's
/// full surface. A retry is barred while [`has_live_zombie`](ZombieObserver::has_live_zombie)
/// is `true`.
pub trait ZombieObserver {
    /// Whether any abandoned-but-running closure of this node is still live (its
    /// permit not yet dropped). `true` bars a retry (C1 exclusivity).
    fn has_live_zombie(&self) -> bool;
}

/// The producer-side **late-result barrier** raised at a blocking/compute timeout
/// mark (arch.md C14; T0.3 ADR §4). A timed-out attempt's closure may run on and
/// compute a value, but that value must never escape: this barrier refuses any
/// slot fill and any scratch write the abandoned closure attempts **after** the
/// mark, so whatever it computes is discarded.
///
/// This is the *producer-side* mirror of the slot's *consumer-side* zombie rule
/// (C10 / T17): a timed-out producer cannot fill, just as an abandoned consumer
/// cannot reclaim.
#[derive(Debug, Clone, Copy)]
pub struct LateResultBarrier {
    // Barred by construction: obtainable only from a `TimeoutDecision`, which
    // exists only once the attempt is already marked timed-out. Every method is a
    // refusal — the barrier is a *deny* gate, not a store.
    _private: (),
}

impl LateResultBarrier {
    /// Attempt to fill `slot` with a late value the abandoned closure produced —
    /// **always refused**. Returns `false` (never filled) and drops `value`, so a
    /// timed-out attempt never fills its output slot (arch.md C14; T0.3 ADR §4).
    ///
    /// The `slot` is taken by shared reference and left **untouched**: the barrier
    /// discards `value` rather than writing it.
    #[must_use]
    pub fn fill_slot<O>(&self, _slot: &Slot<O>, value: O) -> bool
    where
        O: Send + Sync + 'static,
    {
        // A timed-out attempt never fills its slot. `value` is dropped here.
        let _discarded = value;
        false
    }

    /// Attempt a late scratch write the abandoned closure performs after the
    /// timeout — **always refused**. Returns `false` (nothing written), so no
    /// scratch value attributable to a timed-out attempt is persisted (arch.md
    /// C14; T0.3 ADR §4).
    #[must_use]
    pub fn write_scratch(&self) -> bool {
        // A timed-out attempt never writes scratch.
        false
    }
}

// ===========================================================================
// C14 · retry with jittered exponential backoff (T22)
// ===========================================================================
//
// This wraps the single-attempt core ([`run_one_attempt`], shared with
// [`run_attempt`], T20) in a **bounded retry loop**. After each failed attempt
// the loop consults the outcome classification and either schedules another
// attempt after a jittered exponential backoff or terminates the node (arch.md
// C14: "either fill the slot, schedule another attempt after a backoff, or reach
// a terminal failure"). It re-decides nothing T20/T21 owns: it reuses their
// classification ([`AttemptOutcome::is_retry_eligible`]) and event contract, and
// forks no attempt logic.
//
// # Determinism — no global RNG, no clock, inside the retry logic
//
// Jitter needs randomness but the retry logic must be reproducible in tests, so
// two things are **injected** rather than read from ambient state:
//
// - the **jitter source** is a caller-supplied [`Jitter`] (a tiny dependency-free
//   seeded PRNG for production, [`SeededJitter`]; a pinned/zero source for tests,
//   [`NoJitter`]). The loop never reads a thread/global RNG, so the exact backoff
//   sequence is assertable.
// - the **backoff wait** is a caller-supplied timer future factory
//   (`FnMut(Duration) -> impl Future`). The loop only *computes* the delay and
//   awaits the caller's future — it never reads the system clock. The driver
//   (T24 / T33) arms a real `tokio::time` sleep on the isolated framework runtime
//   there; a unit test passes a future that records the delay and resolves at
//   once. This keeps `dagr-core` runtime-agnostic and dependency-free, exactly as
//   the T21 timeout race did.
//
// # Interim M1 surface → migrates into C5 policy in M2 (T29)
//
// [`RetryConfig`] is a **deliberately small, self-contained interim knob**. Its
// conservative default is **no retries** (a single attempt), matching C5's
// stated default. In M2 this shape folds into the full C5 node-policy struct —
// that migration is T29's concern (which this ticket blocks); nothing here
// implements the policy surface, the defaults hash, or the graph-artifact
// disclosure of the effective policy.

use std::time::Duration;

/// An **injectable, deterministic** jitter source for backoff (T22).
///
/// Jitter spreads simultaneous retries so a fan-out does not resynchronize
/// (arch.md C14: "Backoff delays are jittered, so a fan-out of simultaneous
/// retries does not resynchronize"). But tests must be reproducible, so the
/// retry loop reads jitter **only** through this port — never a thread/global
/// RNG or the system clock. Production passes a seeded PRNG ([`SeededJitter`]);
/// tests pass a pinned source ([`NoJitter`], or a seeded one for the fan-out
/// spread test).
///
/// [`next_unit`](Jitter::next_unit) returns the next pseudo-random draw in the
/// half-open unit interval `[0, 1)`; [`Backoff`] maps it into the jitter window
/// around the nominal exponential delay.
pub trait Jitter {
    /// The next pseudo-random draw in `[0, 1)`. Deterministic given the source's
    /// state — a seeded source replays the identical sequence.
    fn next_unit(&mut self) -> f64;
}

/// A **no-jitter** source: every draw is `0.0`, so [`Backoff`] yields the exact
/// nominal `base·factor^n` (capped) schedule with no spread.
///
/// This is the pinned source the deterministic backoff tests use to assert the
/// exact sequence, and it is also the honest default when jitter is undesired.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoJitter;

impl Jitter for NoJitter {
    fn next_unit(&mut self) -> f64 {
        0.0
    }
}

/// A tiny **dependency-free, seeded** PRNG usable as a [`Jitter`] source
/// (`splitmix64`).
///
/// `dagr-core` is kept dependency-free (arch.md "Stability"), so rather than pull
/// `rand`/`fastrand` for the default jitter this uses `splitmix64` — a
/// well-known, small, fast, `unsafe`-free integer generator — to produce the
/// unit draws. A given seed **replays the identical sequence**, which is what
/// makes the backoff schedule assertable in tests (a distinct seed per node
/// produces distinct draws, which is what spreads a fan-out).
///
/// This is *not* a cryptographic RNG and does not need to be: jitter only needs
/// a well-spread, reproducible spread of retry wake times. The production driver
/// seeds one per node (e.g. from the node identity) so a fan-out of identical
/// nodes still draws distinct delays.
#[derive(Debug, Clone)]
pub struct SeededJitter {
    state: u64,
}

impl SeededJitter {
    /// A seeded jitter source. The same `seed` replays the identical draw
    /// sequence (deterministic); distinct seeds diverge (spreading a fan-out).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl Jitter for SeededJitter {
    #[allow(
        clippy::cast_precision_loss,
        reason = "exact by construction: `z >> 11` is a 53-bit integer (≤ f64's mantissa) and `1u64 << 53` is a power of two — the standard lossless [0,1) unit-float construction"
    )]
    fn next_unit(&mut self) -> f64 {
        // splitmix64: advance the state, then avalanche it into a well-mixed
        // 64-bit output. Dependency-free and `unsafe`-free.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Map the top 53 bits into [0, 1) — the standard f64 unit construction.
        // Both casts are exact: `z >> 11` fits in 53 bits (≤ f64's mantissa) and
        // `1u64 << 53` is a power of two, so no precision is actually lost.
        (z >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// The **backoff schedule** (T22): exponential in the attempt index, clamped to a
/// cap, and jittered so a fan-out does not resynchronize (arch.md C14).
///
/// For a 0-based attempt index `n` (the wait *after* the `n`-th failed attempt,
/// before attempt `n + 1`), the **nominal** delay is `base · factor^n`, clamped
/// so it never exceeds `cap` — later delays sit exactly at the cap. Jitter then
/// spreads the *actual* delay within `[0, nominal]` using a caller-supplied
/// [`Jitter`] draw (full jitter): `delay = nominal · draw` when the draw is in
/// `[0, 1)`, with [`NoJitter`] collapsing that to exactly `nominal`. The
/// jittered delay is itself clamped to the cap, so **no scheduled delay ever
/// exceeds the cap**, jitter or not.
///
/// # Why full jitter
///
/// Full jitter (`[0, nominal]`) maximises decorrelation of simultaneous retries
/// — the property the C14 acceptance criterion "a fan-out of simultaneous
/// retries does not resynchronize" demands — and keeps the delay bounded above
/// by the nominal (hence by the cap). The window is documented here as part of
/// the interim surface that migrates into C5 policy (T29).
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    base: Duration,
    factor: f64,
    cap: Duration,
}

impl Backoff {
    /// A backoff schedule with the given `base` delay, exponential growth
    /// `factor` (per attempt), and maximum `cap`. Pass [`Duration::MAX`] for an
    /// effectively-uncapped schedule.
    #[must_use]
    pub fn new(base: Duration, factor: f64, cap: Duration) -> Self {
        Self { base, factor, cap }
    }

    /// The **nominal** (pre-jitter) delay for 0-based attempt index `n`:
    /// `base · factor^n`, clamped to the cap. Later indices sit exactly at the
    /// cap. Public for testing and driver introspection; the loop uses
    /// [`delay_for`](Backoff::delay_for), which applies jitter on top.
    #[must_use]
    pub fn nominal_delay(&self, n: u32) -> Duration {
        // Compute in f64 seconds, guarding overflow by clamping to the cap. A
        // `factor` >= 1 grows without bound, so `saturating` behaviour falls out
        // of the min-with-cap below.
        let base_secs = self.base.as_secs_f64();
        // A huge `n` would overflow the schedule anyway; clamp the exponent to
        // `i32::MAX` (the delay is clamped to the cap below regardless).
        let exponent = i32::try_from(n).unwrap_or(i32::MAX);
        let scaled = base_secs * self.factor.powi(exponent);
        // A non-finite or overflowing product is clamped to the cap.
        if !scaled.is_finite() {
            return self.cap;
        }
        let nominal = Duration::try_from_secs_f64(scaled).unwrap_or(self.cap);
        nominal.min(self.cap)
    }

    /// The **scheduled** delay for 0-based attempt index `n`: the nominal
    /// exponential delay with full jitter applied from `jitter`, clamped to the
    /// cap.
    ///
    /// Jitter is **subtractive full jitter** relative to the nominal: a draw of
    /// `0` yields **exactly** the nominal (so [`NoJitter`] reproduces the exact
    /// nominal exponential-and-capped sequence), and a larger draw shortens the
    /// delay toward zero — the scheduled delay is `nominal · (1 − draw)`, lying in
    /// the half-open window `(0, nominal]`. Because the window's upper bound is
    /// the nominal (itself clamped to the cap), **no scheduled delay ever exceeds
    /// the cap**, jitter or not. Anchoring the window at the nominal (rather than
    /// scaling *up* from it) is what keeps the ticket's two facets consistent:
    /// "pinned to zero → nominal" and "within the jitter window around the
    /// nominal, never above the cap."
    #[must_use]
    pub fn delay_for<J: Jitter + ?Sized>(&self, n: u32, jitter: &mut J) -> Duration {
        let nominal = self.nominal_delay(n);
        // Subtractive full jitter: draw in [0, 1) removes up to the whole nominal.
        // Clamp the draw defensively so a misbehaving source can never lengthen
        // the delay past the nominal (and hence past the cap) or below zero.
        let draw = jitter.next_unit().clamp(0.0, 1.0);
        let jittered_secs = nominal.as_secs_f64() * (1.0 - draw);
        let jittered = Duration::try_from_secs_f64(jittered_secs).unwrap_or(nominal);
        jittered.min(self.cap)
    }
}

/// The **interim per-node retry configuration** (T22): maximum attempt count plus
/// the [`Backoff`] schedule, with a conservative default of **no retries**.
///
/// # Classification-gated retry
///
/// Only outcomes classified **retry-eligible** ([`AttemptOutcome::is_retry_eligible`]
/// — a retry-eligible failure or a timeout, arch.md C14) consume the budget and
/// trigger a backoff. A permanent failure, a deliberate skip, and success end the
/// loop immediately with no further attempts and no backoff — a permanent error
/// is never retried regardless of remaining budget.
///
/// # Conservative default
///
/// [`RetryConfig::default`] is **one attempt** (no retries) — the C5 stated
/// default ("no retries"). A node with the default config performs exactly one
/// attempt and then fails on a retry-eligible error, proving the default is
/// honestly non-retrying.
///
/// # Interim M1 surface → C5 policy in M2 (T29)
///
/// This is a deliberately small, self-contained knob introduced as an interim M1
/// surface. In M2 it **migrates into the C5 node-policy struct** (retries +
/// backoff shape live there alongside timeout, cost, trigger rule, …) — that
/// migration is **T29**'s concern (which this ticket blocks). Nothing here
/// implements the policy struct, its defaults hash, or the graph-artifact
/// disclosure of the effective policy.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    max_attempts: u32,
    backoff: Backoff,
}

impl RetryConfig {
    /// A retry configuration allowing up to `max_attempts` **total** attempts
    /// (not retries-beyond-the-first: `max_attempts == 1` is the no-retry case,
    /// `3` allows the initial attempt plus two retries) with the given
    /// [`Backoff`] schedule. `max_attempts` is clamped to at least one — a node
    /// always gets at least a single attempt.
    #[must_use]
    pub fn new(max_attempts: u32, backoff: Backoff) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            backoff,
        }
    }

    /// The maximum **total** number of attempts (initial attempt included).
    /// Always at least one.
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// The backoff schedule the loop waits between attempts.
    #[must_use]
    pub fn backoff(&self) -> &Backoff {
        &self.backoff
    }
}

impl Default for RetryConfig {
    /// The conservative default: **no retries** — a single attempt. The backoff
    /// is present but never consulted under a single attempt.
    fn default() -> Self {
        // A zero base means even a hypothetical retry would wait nothing; the
        // real guard is `max_attempts == 1`, which schedules no backoff at all.
        Self {
            max_attempts: 1,
            backoff: Backoff::new(Duration::ZERO, 2.0, Duration::MAX),
        }
    }
}

/// Run a node through the **bounded retry loop** (T22; arch.md `### C14`).
///
/// This is the retry driver that turns the single-attempt runner into what C14
/// describes: for each attempt in turn it runs the attempt (via the shared
/// single-attempt core, emitting the opening events and the exactly-one
/// attempt-outcome record per attempt), then:
///
/// - **success / permanent failure / deliberate skip** — the loop ends
///   immediately; no backoff, no further attempt. (Only success fills the slot.)
/// - **retry-eligible failure / timeout** — if the budget still has attempts
///   left, the loop enters a **named backoff phase** ([`AttemptEvent::BackoffStarted`],
///   feeding C23 phase timings), waits the jittered exponential delay by awaiting
///   the caller-provided `timer` future, then dispatches the next attempt; if the
///   budget is exhausted, the node reaches its terminal failure (`failed` for a
///   retry-eligible failure whose retries ran out, `timed-out` for a timeout on
///   the last permitted attempt).
///
/// Exactly **one** node-terminal record is emitted, when the loop terminates —
/// carrying the last attempt's classified terminal state — even though each
/// attempt emitted its own attempt-outcome record (gapless, increasing attempt
/// numbers).
///
/// # Determinism (no global RNG / clock in the loop)
///
/// - `jitter` is the injected [`Jitter`] source — the loop reads no thread/global
///   RNG. `SeededJitter` replays deterministically; `NoJitter` yields the exact
///   nominal schedule.
/// - `timer` is a caller-supplied factory `FnMut(Duration) -> impl Future<Output = ()>`.
///   The loop *computes* the delay and awaits the caller's future; it reads no
///   system clock. The driver (T24 / T33) arms a real isolated-runtime sleep
///   there; a test passes a future that records the delay and resolves at once.
///
/// # C1 exclusivity — no premature re-entry
///
/// `task` is taken by `&mut self` (C1) and each attempt is `await`ed to
/// completion **before** the next is dispatched, so attempt `n + 1` never begins
/// until attempt `n`'s closure has returned — the same task instance is never
/// running concurrently with a prior attempt. (The await-bound future-drop and
/// blocking/compute zombie-deferral of a *timed-out* attempt are T21's; this loop
/// composes with them via the outcome classification and does not re-decide
/// them.)
///
/// # Arguments
///
/// - `task` — the node's work, taken by value and driven `&mut` per attempt (C1).
/// - `node` — the node's author-declared name (keys every emitted record).
/// - `run` / `pipeline` — the run and pipeline identities the per-attempt
///   [`RunContext`] carries (C8); the loop mints a fresh context per attempt with
///   the incremented attempt number and the configured maximum, so the task
///   observes which attempt it is on.
/// - `slot` — the node's single once-writable output slot (C10), filled only on a
///   successful attempt.
/// - `sink` — the abstract C19 emission port (T24 adapts it; tests capture).
/// - `config` — the interim [`RetryConfig`] (max attempts + backoff shape).
/// - `jitter` — the injected deterministic [`Jitter`] source.
/// - `timer` — the caller-provided backoff timer factory (the sleeping seam).
///
/// Returns the **last** attempt's classified [`AttemptOutcome`] — the one whose
/// terminal state the node ends in.
#[allow(
    clippy::too_many_arguments,
    reason = "the interim retry surface threads \
    the run/pipeline identity, slot, sink, config, jitter, and timer explicitly; \
    these fold into the C5 policy + driver context in M2 (T29/T24)"
)]
pub async fn run_with_retries<T, S, F, Fut>(
    mut task: T,
    node: &str,
    run: RunId,
    pipeline: PipelineId,
    slot: &Slot<T::Output>,
    sink: &mut S,
    config: &RetryConfig,
    jitter: &mut (impl Jitter + ?Sized),
    mut timer: F,
) -> AttemptOutcome
where
    T: Task<Input = ()>,
    S: AttemptEventSink + ?Sized,
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    let node_id = NodeId::from_name(node);
    let max_attempts = config.max_attempts();

    // Attempt numbers are 1-based (C8); the backoff schedule is 0-based on the
    // *failed*-attempt index. The loop runs attempt 1..=max_attempts, stopping
    // early on any non-retry-eligible outcome or once the budget is spent.
    let mut attempt: u32 = 1;
    loop {
        // Mint a fresh per-attempt context carrying this attempt number and the
        // configured maximum (C8), so the task observes which attempt it is on.
        let ctx = RunContext::builder(run.clone(), pipeline.clone(), node_id)
            .attempt(attempt)
            .max_attempts(max_attempts)
            .build();

        // One attempt end to end (opening events + exactly-one outcome record),
        // driven `&mut` and awaited to completion before any next attempt (C1).
        let outcome = run_one_attempt(&mut task, node, &ctx, slot, sink).await;

        // Classification-gated: only a retry-eligible outcome with budget left
        // schedules another attempt; everything else terminates the node now.
        let budget_left = attempt < max_attempts;
        if outcome.is_retry_eligible() && budget_left {
            // Enter a named backoff phase: compute the jittered exponential delay
            // for this (0-based) failed-attempt index, record it as a distinct
            // measurable interval (C23), then await the caller's timer future.
            let delay = config.backoff().delay_for(attempt - 1, jitter);
            sink.emit(AttemptEvent::BackoffStarted {
                node: node.into(),
                attempt,
                delay,
            });
            // The wait: await the caller-provided timer future. The loop reads no
            // clock — the driver's future decides when the delay has elapsed.
            timer(delay).await;
            attempt += 1;
            continue;
        }

        // Terminal: emit the single node-terminal record and return the outcome.
        // (A retry-eligible failure whose budget ran out ends `failed`; a timeout
        // on the last attempt ends `timed-out`; success/permanent/skip end at
        // their own terminal state — all via the outcome's terminal_state.)
        emit_node_terminal(node, outcome.terminal_state(), sink);
        return outcome;
    }
}
