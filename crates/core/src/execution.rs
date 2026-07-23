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

use crate::context::{RunContext, TerminalState};
use crate::error::{TaskError, TaskErrorClass};
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

    // (7) Closing per-transition event = the exactly-one attempt-outcome record,
    // then the node-terminal record carrying the classified state.
    emit_closing_events(node, attempt, outcome, sink);

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
    sink.emit(AttemptEvent::NodeTerminal {
        node: node.into(),
        state: outcome.terminal_state(),
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
