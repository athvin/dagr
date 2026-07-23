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
use crate::error::TaskErrorClass;
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
/// task-facing [`TaskError`](crate::error::TaskError) three-valued surface (T3
/// ADR §11) — restricted to the outcomes **reachable in T20**:
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
/// # Reserved variants — added by T21 / T23 without reshaping
///
/// The enum is `#[non_exhaustive]` precisely so the operationally-hard siblings
/// extend it rather than rewrite it:
///
/// - **`TimedOut`** — per-attempt timeout (T21). Timeout is retry-eligible by
///   default, subject to the node's budget (arch.md C14). Its terminal state is
///   [`TerminalState::TimedOut`].
/// - **`Panicked`** — a caught panic converted to a permanent failure (T23). Its
///   terminal state is [`TerminalState::Failed`] (arch.md Vocabulary: "a caught
///   panic" is `failed`).
///
/// T20 does not populate those variants (it starts no timer and catches no
/// panic); naming them here is the stable-shape contract T21/T23 plug into.
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
        }
    }

    /// Whether this attempt succeeded (the slot was filled).
    #[must_use]
    pub fn is_success(self) -> bool {
        matches!(self, AttemptOutcome::Succeeded)
    }

    /// Whether this outcome is **retry-eligible** — the one signal the retry
    /// driver (T22) acts on. A permanent failure is **never** retry-eligible
    /// (arch.md C14), and a success or skip is not a failure at all.
    #[must_use]
    pub fn is_retry_eligible(self) -> bool {
        matches!(self, AttemptOutcome::RetryEligibleFailure)
    }

    /// Whether this attempt failed (permanent or retry-eligible). A skip is
    /// **not** a failure (arch.md Vocabulary: `skipped` is skip-like), and a
    /// success is not.
    #[must_use]
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            AttemptOutcome::PermanentFailure | AttemptOutcome::RetryEligibleFailure
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
    if outcome.is_success() {
        sink.emit(AttemptEvent::AttemptSucceeded {
            node: node.into(),
            attempt,
        });
    } else {
        sink.emit(AttemptEvent::AttemptFailed {
            node: node.into(),
            attempt,
        });
    }
    sink.emit(AttemptEvent::NodeTerminal {
        node: node.into(),
        state: outcome.terminal_state(),
    });

    outcome
}
