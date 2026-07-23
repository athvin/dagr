//! The C19 event-stream writer — the crash-proof, append-only record of a run.
//!
//! This module builds the append-only event-stream **writer** for a single run
//! (arch.md `### C19 · Event stream`). It emits one single-line JSON record per
//! state transition through the run-store [`EventSink`] (T0.6 / 012), encoded as
//! JSON Lines per the T4 serialization ADR (017), and stamps every record with
//! run identity, schema version, a gapless strictly-increasing sequence number,
//! an informational wall-clock stamp, and an authoritative monotonic offset from
//! run start.
//!
//! # What lives here (C19 writer) — and what does not
//!
//! This is the **writer** only: it serializes events and appends them to the
//! sink, and it ships the tolerant [`read_records`] the fold contract (C22 /
//! T42) and the crash-safety suite (T27) build on. It does **not** run tasks
//! (C14 / T20), drive the run loop that produces the transitions at runtime
//! (T24), read back a stream into a run artifact (the fold itself is C22 / T42),
//! nor construct the default local-file sink from an operator base path (the
//! sink and base-location surface are T0.6 / C18, injected here). The event
//! vocabulary is closed (arch.md "Vocabulary"); this module never mutates the
//! graph shape at runtime and pushes nothing over a network (arch.md C19: live
//! telemetry is an external tailer's job, not a framework exporter).
//!
//! # Governing decisions
//!
//! - **Encoding — T4 (017).** Each record is one compact JSON object per line,
//!   newline-delimited, written into the reserved `events.jsonl`. The
//!   `schema_version` field carries `dagr.event-stream@1`. Bytes are canonical
//!   (T4 §6): object keys sorted lexicographically, integers only, compact
//!   whitespace, minimal escaping — so a record is byte-deterministic.
//! - **Sink + header — T0.6 (012).** The stream is written through the injected
//!   two-operation [`EventSink`] (append a line, flush). Every record carries
//!   the T0.6 §7 header: run identity, schema version, gapless sequence,
//!   informational wall stamp, authoritative monotonic offset.
//! - **Node identity — T13 (023).** Records name nodes by their author-declared
//!   registration name (node identity is the name, verbatim).

use std::collections::BTreeMap;
use std::fmt;
use std::io;

use serde::Serialize;
use uuid::Uuid;

/// The schema-version string stamped on every event record (T4 §3).
///
/// A `<name>@<version>` string: the `<name>` self-identifies the event-stream
/// schema among the three co-located schemas (`dagr.event-stream`, `dagr.graph`,
/// `dagr.run`), and `@1` is the single monotonically-increasing major version.
pub const EVENT_STREAM_SCHEMA_VERSION: &str = "dagr.event-stream@1";

/// The reserved file name of the C19 event stream under a run directory
/// (`<base>/<pipeline>/<run-id>/events.jsonl`, T0.6 §3).
pub const EVENTS_FILE_NAME: &str = "events.jsonl";

// === Sink =================================================================

/// The two-operation sink the event stream is written through (T0.6 §1).
///
/// Exactly two operations, no more: append one complete record as a single
/// line, and flush. The writer never hands the sink a partial or interleaved
/// line (line atomicity at the framework boundary); what the concrete sink does
/// with the bytes — and whether `flush` fsyncs — is the sink's business. The
/// **default local-file sink** (a local file under the run directory, which does
/// not fsync per append; its `flush` fsyncs) is **owned and constructed by the
/// run store — T0.6 / C18 — and injected here, not built by this crate** (see
/// the module-level "what lives here" note). The injection seam is a
/// bootstrap-time parameter (T0.6 §1): the crash-safety suite (T27) supplies a
/// failing sink here, and an operator points the stream at a different local
/// target, without touching the pipeline.
pub trait EventSink: Send {
    /// Append one complete record as a single line (bytes ending in `\n`).
    ///
    /// Line-atomic at the framework boundary: the caller never passes a partial
    /// or interleaved line.
    ///
    /// # Errors
    /// Returns any I/O error from the underlying target; the writer treats it as
    /// a run-level "event stream unwritable" fault (T0.6 §5).
    fn append_line(&mut self, line: &[u8]) -> io::Result<()>;

    /// Ensure everything already appended has been handed to the sink and (for
    /// durable sinks) made durable. No user-space buffering remains.
    ///
    /// # Errors
    /// Returns any I/O error from the underlying target.
    fn flush(&mut self) -> io::Result<()>;
}

// === Monotonic clock ======================================================

/// The authoritative monotonic clock the writer reads offsets from (T0.6 §7).
///
/// Durations are computed from offsets, never from the wall clock, so this
/// source must be **monotonic** (non-decreasing, immune to wall-clock steps).
/// It is injectable so tests can drive offsets deterministically, including a
/// backward wall-clock step that must not move any offset.
pub trait MonotonicClock {
    /// Nanoseconds elapsed since run start (the instant the writer captured at
    /// construction). Non-decreasing across successive calls.
    fn elapsed_ns(&self) -> u64;
}

// === Envelope + events ====================================================

/// The full run-artifact header known at run start, carried by `run-started`.
///
/// Everything a `run.json` header holds *except* the overall outcome and
/// summary, which exist only at run end (C19). A stream that ends one record
/// after `run-started` still identifies its run completely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStartedHeader {
    /// Pipeline identity — its stable name (C20/C22 header).
    pub pipeline: String,
    /// Structural fingerprint — present only when assembly succeeded (C21).
    pub fingerprint_structural: Option<String>,
    /// Policy hash — present only when assembly succeeded (C21).
    pub fingerprint_policy: Option<String>,
    /// The run's parameters (C7), as name→value strings.
    pub parameters: BTreeMap<String, String>,
    /// The run's optional data interval (C7), as `[start, end]` strings.
    pub data_interval: Option<[String; 2]>,
    /// Allowlisted captured environment values (C7 / C22), name→value.
    pub captured_env: BTreeMap<String, String>,
    /// Resume lineage — the originating run id when this run resumed one (C27).
    pub resumed_from: Option<String>,
}

/// One event kind per state transition in the closed C19 vocabulary.
///
/// The set is closed: run started, node became ready, node admitted, attempt
/// started, attempt succeeded, attempt failed, node reached terminal state,
/// zombie-at-exit (C14), run finished. Terminal records carry the normative
/// terminal state from the arch.md Vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The run started; carries the full run-artifact header known at start.
    RunStarted(RunStartedHeader),
    /// A node's upstreams are all terminal and its rule fired — it is ready.
    NodeReady {
        /// The node's author-declared identity name (T13).
        node: String,
    },
    /// The node was admitted through the admission controller (C12).
    NodeAdmitted {
        /// The node's author-declared identity name (T13).
        node: String,
    },
    /// An attempt for the node began (attempt numbers are 1-based).
    AttemptStarted {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number.
        attempt: u32,
    },
    /// An attempt for the node returned a value.
    AttemptSucceeded {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number.
        attempt: u32,
    },
    /// An attempt for the node failed (retryable or permanent).
    AttemptFailed {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The 1-based attempt number.
        attempt: u32,
    },
    /// The node reached a terminal state from the C19 vocabulary.
    NodeTerminal {
        /// The node's author-declared identity name (T13).
        node: String,
        /// The normative terminal state from arch.md "Vocabulary".
        state: TerminalState,
    },
    /// A leftover thread was still running at process exit (C14 zombie).
    ZombieAtExit {
        /// The node's author-declared identity name (T13).
        node: String,
    },
    /// The run finished; carries the run outcome.
    RunFinished {
        /// The overall run outcome.
        outcome: RunOutcome,
    },
}

/// The normative terminal states from arch.md "Vocabulary".
///
/// Every node ends a run in exactly one of these. The wire form (below) is the
/// exact kebab-case spelling arch.md uses, so artifacts and diagrams read the
/// same word the spec does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalState {
    /// The task returned a value; the slot was filled.
    Succeeded,
    /// Permanent failure, retries exhausted, or a caught panic.
    Failed,
    /// The final attempt exceeded its per-attempt timeout.
    TimedOut,
    /// The task itself returned a deliberate (originated) skip.
    Skipped,
    /// Never ran because an upstream skip propagated to it.
    UpstreamSkipped,
    /// Never ran because its trigger rule can no longer be satisfied.
    UpstreamFailed,
    /// Observed cancellation and returned, or was never admitted after cancel.
    Cancelled,
    /// Asked to cancel and never returned within the grace period.
    Abandoned,
    /// Not executed this run; resume carried its prior success forward.
    SatisfiedFromPrior,
}

impl TerminalState {
    /// The normative kebab-case wire spelling from arch.md "Vocabulary".
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TerminalState::Succeeded => "succeeded",
            TerminalState::Failed => "failed",
            TerminalState::TimedOut => "timed-out",
            TerminalState::Skipped => "skipped",
            TerminalState::UpstreamSkipped => "upstream-skipped",
            TerminalState::UpstreamFailed => "upstream-failed",
            TerminalState::Cancelled => "cancelled",
            TerminalState::Abandoned => "abandoned",
            TerminalState::SatisfiedFromPrior => "satisfied-from-prior",
        }
    }
}

/// The overall run outcome carried by the `run-finished` record.
///
/// This is the *outcome*, not the summary (metrics/critical path are C22/C23 and
/// fold-time — T42/T43). The `assembly-failed` variant is how an assembly
/// failure records itself in a two-record stream (C19: even an assembly failure
/// has a place to record itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The run completed; no node ended failure-like.
    Succeeded,
    /// The run completed with at least one failure-like terminal state.
    Failed,
    /// The run was cancelled (signal, or an "event stream unwritable" fault).
    Cancelled,
    /// Assembly failed before execution; the stream has no fingerprints.
    AssemblyFailed,
}

impl RunOutcome {
    /// The kebab-case wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RunOutcome::Succeeded => "succeeded",
            RunOutcome::Failed => "failed",
            RunOutcome::Cancelled => "cancelled",
            RunOutcome::AssemblyFailed => "assembly-failed",
        }
    }
}

// === Writer ================================================================

/// A run-level fault surfaced when the sink cannot record a transition (C19).
///
/// A mid-run sink failure is a run-level fault: the run moves to cancelling with
/// reason "event stream unwritable" and exits with the distinct sink-failure
/// code (T0.6 §5). This is the error the writer returns from a record method
/// when the sink's append/flush fails; the run loop (T24) reacts to it.
#[derive(Debug)]
pub struct SinkFault {
    /// The stable cancellation reason surfaced to the run loop.
    pub reason: &'static str,
    /// The underlying sink I/O error.
    pub source: io::Error,
}

/// The cancellation reason a sink fault carries (arch.md C19, verbatim).
pub const EVENT_STREAM_UNWRITABLE: &str = "event stream unwritable";

impl fmt::Display for SinkFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.reason, self.source)
    }
}

impl std::error::Error for SinkFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The append-only event-stream writer for a single run (C19).
///
/// Constructed at bootstrap from an injected [`EventSink`], a run identity
/// (`UUIDv7`, operator-overridable), a pipeline identity, and a
/// [`MonotonicClock`] whose zero is the captured run-start instant. It stamps a
/// gapless strictly-increasing sequence (starting at `0` on `run-started`), an
/// informational wall stamp, and an authoritative monotonic offset onto every
/// record, then appends and flushes it to the sink before the transition is
/// treated as recorded (write-through, no user-space buffering — T0.6 §6).
#[allow(dead_code)] // fields consumed by the implementation commit (TDD skeleton)
pub struct EventStreamWriter<S: EventSink, C: MonotonicClock> {
    sink: S,
    clock: C,
    run_id: String,
    pipeline: String,
    next_seq: u64,
    wall: Box<dyn FnMut() -> u64 + Send>,
    faulted: bool,
}

#[allow(
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::unused_self,
    clippy::needless_pass_by_value,
    missing_docs,
    unused_variables
)]
impl<S: EventSink, C: MonotonicClock> EventStreamWriter<S, C> {
    // Skeleton signatures so the C19 test suite compiles and FAILS (TDD: tests
    // fail first). The real bodies land in the implementation commit.

    /// Construct the writer at bootstrap from an injected sink, clock, run id,
    /// and pipeline identity. The clock's zero is the captured run-start instant.
    pub fn new(sink: S, clock: C, run_id: RunId, pipeline: impl Into<String>) -> Self {
        let _ = (&sink, &clock, &run_id);
        let _ = pipeline.into();
        todo!("implemented after failing tests are committed")
    }

    /// Emit the `run-started` record carrying the full run-artifact header.
    pub fn run_started(&mut self, header: RunStartedHeader) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit a `node-ready` record.
    pub fn node_ready(&mut self, node: &str) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit a `node-admitted` record.
    pub fn node_admitted(&mut self, node: &str) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit an `attempt-started` record.
    pub fn attempt_started(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit an `attempt-succeeded` record.
    pub fn attempt_succeeded(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit an `attempt-failed` record.
    pub fn attempt_failed(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit a `node-terminal` record carrying the normative terminal state.
    pub fn node_terminal(&mut self, node: &str, state: TerminalState) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit a `zombie-at-exit` record (C14).
    pub fn zombie_at_exit(&mut self, node: &str) -> Result<(), SinkFault> {
        todo!()
    }

    /// Emit the `run-finished` record carrying the run outcome.
    pub fn run_finished(&mut self, outcome: RunOutcome) -> Result<(), SinkFault> {
        todo!()
    }

    /// Flush (fsync via the sink) at run end or cancellation.
    pub fn finish(&mut self) -> Result<(), SinkFault> {
        todo!()
    }

    /// The stream file path this writer writes under, given the resolved base
    /// location: `<base>/<pipeline>/<run-id>/events.jsonl` (T0.6 §3).
    #[must_use]
    pub fn stream_path(&self, base: &str) -> String {
        todo!()
    }
}

// === Run identity ==========================================================

/// A run identity (T0.6 §4): a `UUIDv7` by default, operator-overridable verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunId(String);

impl RunId {
    /// Mint a fresh time-ordered `UUIDv7` run id (the natural-sort default).
    #[must_use]
    pub fn generate() -> Self {
        RunId(Uuid::now_v7().to_string())
    }

    /// Use an operator-supplied id **verbatim** — not validated into `UUIDv7`
    /// shape, re-hashed, or prefixed (T0.6 §4).
    #[must_use]
    pub fn from_operator(id: impl Into<String>) -> Self {
        RunId(id.into())
    }

    /// The id as it appears in the run directory path and every record.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// === Tolerant reader =======================================================

/// The outcome of tolerantly reading a (possibly crash-truncated) stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadStream {
    /// Every fully-parsed record line, in order, as `serde_json::Value`.
    pub records: Vec<serde_json::Value>,
    /// Whether a single trailing partial record was tolerated and discarded.
    pub trailing_partial_discarded: bool,
}

/// Tolerantly read a JSONL event stream, discarding at most one trailing partial
/// record (C19 / T4 §1). Skeleton — implemented after the failing tests land.
#[allow(clippy::missing_errors_doc, dead_code, unused_variables)]
pub fn read_records(_bytes: &[u8]) -> Result<ReadStream, ReadError> {
    todo!("implemented after failing tests are committed")
}

/// A corruption error: a **non-final** line failed to parse (not the tolerated
/// trailing partial).
#[derive(Debug)]
pub struct ReadError {
    /// The zero-based index of the line that failed to parse.
    pub line: usize,
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "non-final record at line {} failed to parse", self.line)
    }
}

impl std::error::Error for ReadError {}

// A private marker so the skeleton's `Serialize`-bound helper compiles; the
// real canonicalization lands with the implementation commit.
#[allow(dead_code)]
fn _canonical_placeholder<T: Serialize>(_value: &T) -> String {
    String::new()
}
