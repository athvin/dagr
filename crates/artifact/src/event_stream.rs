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
    /// Structural fingerprint — a real `blake3:…` hash when assembly succeeded
    /// (C21); [`FINGERPRINT_UNAVAILABLE`] on the assembly-failed path (a stream
    /// with no assembled graph still records its start, and the published
    /// event-stream schema requires the field on every `run-started` header).
    pub fingerprint_structural: Option<String>,
    /// Policy hash — a real `blake3:…` hash when assembly succeeded (C21);
    /// [`FINGERPRINT_UNAVAILABLE`] on the assembly-failed path (see
    /// [`fingerprint_structural`](Self::fingerprint_structural)).
    pub fingerprint_policy: Option<String>,
    /// The fingerprint-algorithm version the two fingerprints were computed
    /// under (C21), so a consumer knows which algorithm produced the hashes.
    pub fingerprint_algorithm_version: u32,
    /// The run's parameters (C7), as name→value strings.
    pub parameters: BTreeMap<String, String>,
    /// The run's optional data interval (C7), as `{start, end}` strings.
    pub data_interval: Option<[String; 2]>,
    /// Allowlisted captured environment values (C7 / C22), name→value.
    pub captured_env: BTreeMap<String, String>,
    /// Resume lineage — the originating run id when this run resumed one (C27).
    pub resumed_from: Option<String>,
}

/// The sentinel a `run-started` header carries for a fingerprint that does not
/// exist because assembly failed before a graph was built (C21). The published
/// event-stream schema requires `fingerprint_structural`/`fingerprint_policy` on
/// every `run-started` header (each a non-empty string), so the assembly-failed
/// stream — which has no fingerprints — records this sentinel; the C22 fold
/// treats it as "no fingerprint" and omits it from the run artifact.
pub const FINGERPRINT_UNAVAILABLE: &str = "unavailable";

/// The default fingerprint-algorithm version stamped on a header when the caller
/// does not pin one (C21: the fingerprints are BLAKE3-based, algorithm version 1).
pub const FINGERPRINT_ALGORITHM_VERSION: u32 = 1;

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
    /// The single rich **attempt-outcome** record for one attempt (arch.md
    /// l.331: "Every attempt produces exactly one attempt-outcome record in the
    /// event stream, alongside its per-transition events"). It carries the
    /// attempt's terminal status plus the fold's richest input — the
    /// task-reported metrics, declared-vs-measured cost, structured error and
    /// message, the worker that ran it, and the durable-output reference — so the
    /// C22 fold (T42) reconstructs the run artifact from the stream alone.
    AttemptOutcome(AttemptOutcomeRecord),
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
        /// The 1-based attempt number whose thread was left behind (the schema
        /// keys pinned-time accounting off `(node, attempt)`; C14 / C22 fold).
        attempt: u32,
    },
    /// The run finished; carries the run outcome.
    RunFinished {
        /// The overall run outcome.
        outcome: RunOutcome,
    },
}

/// The rich payload of the single `attempt-outcome` record one attempt emits
/// (arch.md l.331). Its field names, status token, phase-friendly worker string,
/// and optional-field defaults match **both** the published event-stream schema
/// (`schemas/event-stream/v1.schema.json`) and the C22 fold's reader
/// (`crates/artifact/src/fold.rs`), so a real writer stream folds end-to-end
/// (C19 ↔ C22).
///
/// Only `node`, `attempt`, and `status` are always present; every other field is
/// emitted only when the caller has a value for it (the schema is additive and
/// the fold defaults a missing field), keeping the record minimal for the M1/M2
/// callers that do not yet measure cost or metrics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AttemptOutcomeRecord {
    /// The node's author-declared identity name (T13).
    pub node: String,
    /// The 1-based attempt number.
    pub attempt: u32,
    /// The attempt's terminal status — the normative kebab-case taxonomy the
    /// fold reads under `status` (the same tokens [`TerminalState::as_str`]
    /// produces).
    pub status: String,
    /// The worker that ran the attempt, as the fold's single unambiguous
    /// `"<pool>#<thread>"` string; `None` when the caller does not name a worker
    /// (the fold defaults to `"unknown"`).
    pub worker: Option<String>,
    /// A human message (e.g. a panic payload or a skip reason), when recorded.
    pub message: Option<String>,
    /// Structured error detail, reproduced verbatim into the artifact.
    pub error: Option<serde_json::Value>,
    /// Task-reported metrics (C23), copied unmodified by the fold; `None` emits
    /// no `metrics` field (the fold defaults to an empty object).
    pub metrics: Option<serde_json::Value>,
    /// The declared cost vector (C5), juxtaposed against the measured cost.
    pub cost_declared: Option<serde_json::Value>,
    /// The measured cost, juxtaposed against the declared cost (C5/C10).
    pub cost_measured: Option<serde_json::Value>,
    /// The durable-output reference a durable node's succeeded attempt records
    /// (C10/C27), or that a resume copies forward.
    pub durable_reference: Option<serde_json::Value>,
    /// The originating run identity a `satisfied-from-prior` attempt carries (C27).
    pub satisfied_from_run: Option<String>,
    /// The node that decided an `upstream-skipped`/`upstream-failed` propagation
    /// (arch.md Vocabulary).
    pub originating_node: Option<String>,
}

impl AttemptOutcomeRecord {
    /// A minimal outcome record naming only the node, attempt, and terminal
    /// status — the shape the M1/M2 driver emits, since it does not yet measure
    /// metrics or cost. Optional fields default to absent (the fold defaults each
    /// missing field).
    #[must_use]
    pub fn new(node: impl Into<String>, attempt: u32, status: impl Into<String>) -> Self {
        Self {
            node: node.into(),
            attempt,
            status: status.into(),
            ..Self::default()
        }
    }
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
/// has a place to record itself); the `bootstrap-failed` variant is the parallel
/// fail-fast startup outcome (a missing resource — C9/T30 — or a too-big node —
/// C12/T32), **distinct** from an assembly failure.
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
    /// Bootstrap failed a fail-fast startup check (a missing declared resource —
    /// C9/T30 — or a node whose declared cost exceeds a pool's total capacity —
    /// C12/T32) before any node executed. **Distinct** from an assembly failure.
    BootstrapFailed,
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
            RunOutcome::BootstrapFailed => "bootstrap-failed",
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
pub struct EventStreamWriter<S: EventSink, C: MonotonicClock> {
    /// The injected two-operation sink (T0.6 §1).
    sink: S,
    /// The authoritative monotonic clock; its zero is the run-start instant.
    clock: C,
    /// The run identity, stamped on every record (T0.6 §7).
    run_id: String,
    /// The pipeline identity, stamped on every record.
    pipeline: String,
    /// The next sequence number to assign — gapless, strictly increasing,
    /// starting at `0` on `run-started` (T0.6 §7).
    next_seq: u64,
    /// The informational wall-clock source (an RFC3339 string, per the published
    /// schema's `wall`). Never used for durations — the monotonic `offset_ns` is
    /// authoritative (T0.6 §7).
    wall: fn() -> String,
    /// Once a sink append/flush has faulted, the run is unwritable; the writer
    /// refuses to keep appending (a run that cannot record should stop).
    faulted: bool,
}

impl<S: EventSink, C: MonotonicClock> EventStreamWriter<S, C> {
    /// Construct the writer at bootstrap from an injected sink, clock, run id,
    /// and pipeline identity.
    ///
    /// The clock's zero is the captured run-start instant, so its
    /// [`elapsed_ns`](MonotonicClock::elapsed_ns) is the authoritative offset. No
    /// record is written by construction — the first record is emitted by
    /// [`run_started`](Self::run_started). The wall-clock stamp defaults to the
    /// current time as an RFC3339 string and is informational only.
    #[must_use]
    pub fn new(sink: S, clock: C, run_id: RunId, pipeline: impl Into<String>) -> Self {
        Self {
            sink,
            clock,
            run_id: run_id.0,
            pipeline: pipeline.into(),
            next_seq: 0,
            wall: rfc3339_now,
            faulted: false,
        }
    }

    /// Override the informational wall-clock source (default: the current time as
    /// an RFC3339 string).
    ///
    /// The wall stamp is **informational only** — never used for durations
    /// (offsets are authoritative — T0.6 §7) — so overriding it changes no
    /// behavior that matters to a consumer. It exists to make the record bytes
    /// fully deterministic under test (the wall stamp is a record's analog of an
    /// artifact's excluded generation-time field, T4 §6): holding it fixed lets
    /// two emissions of the same record be byte-identical.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: fn() -> String) -> Self {
        self.wall = wall;
        self
    }

    /// The stream file path this writer writes under, given the resolved base
    /// location: `<base>/<pipeline>/<run-id>/events.jsonl` (T0.6 §3).
    ///
    /// Because the path embeds both the pipeline identity and the run-unique
    /// run id, two concurrent runs — even of the same binary and pipeline —
    /// write disjoint files (T0.6 §3; C19 concurrent-run disjointness).
    #[must_use]
    pub fn stream_path(&self, base: &str) -> String {
        format!(
            "{base}/{}/{}/{EVENTS_FILE_NAME}",
            self.pipeline, self.run_id
        )
    }

    /// Emit the `run-started` record carrying the full run-artifact header.
    ///
    /// The record carries every header field known at start — run identity,
    /// pipeline identity, both fingerprints *when assembly succeeded*, parameters,
    /// data interval, allowlisted captured environment, and resume lineage — and
    /// omits overall outcome and summary, which exist only at run end (C19). A
    /// stream that ends immediately after it still identifies its run completely.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn run_started(&mut self, header: RunStartedHeader) -> Result<(), SinkFault> {
        self.emit(&Event::RunStarted(header))
    }

    /// Emit a `node-ready` record.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn node_ready(&mut self, node: &str) -> Result<(), SinkFault> {
        self.emit(&Event::NodeReady { node: node.into() })
    }

    /// Emit a `node-admitted` record.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn node_admitted(&mut self, node: &str) -> Result<(), SinkFault> {
        self.emit(&Event::NodeAdmitted { node: node.into() })
    }

    /// Emit an `attempt-started` record (attempt numbers are 1-based).
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn attempt_started(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        self.emit(&Event::AttemptStarted {
            node: node.into(),
            attempt,
        })
    }

    /// Emit an `attempt-succeeded` record.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn attempt_succeeded(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        self.emit(&Event::AttemptSucceeded {
            node: node.into(),
            attempt,
        })
    }

    /// Emit an `attempt-failed` record.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn attempt_failed(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        self.emit(&Event::AttemptFailed {
            node: node.into(),
            attempt,
        })
    }

    /// Emit the single rich `attempt-outcome` record for one attempt (arch.md
    /// l.331), carrying the terminal status and whatever metrics/cost/error/
    /// worker/durable-reference the caller measured. This is emitted **alongside**
    /// the per-transition `attempt-succeeded`/`attempt-failed` record, not
    /// instead of it, and is the fold's richest input (C22 / T42).
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn attempt_outcome(&mut self, record: AttemptOutcomeRecord) -> Result<(), SinkFault> {
        self.emit(&Event::AttemptOutcome(record))
    }

    /// Emit a `node-terminal` record carrying the normative terminal state.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn node_terminal(&mut self, node: &str, state: TerminalState) -> Result<(), SinkFault> {
        self.emit(&Event::NodeTerminal {
            node: node.into(),
            state,
        })
    }

    /// Emit a `zombie-at-exit` record (a leftover thread still running at process
    /// exit — C14; it changes no node's terminal state).
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn zombie_at_exit(&mut self, node: &str, attempt: u32) -> Result<(), SinkFault> {
        self.emit(&Event::ZombieAtExit {
            node: node.into(),
            attempt,
        })
    }

    /// Emit the `run-finished` record carrying the overall run outcome.
    ///
    /// This records only the *outcome*; the summary (metrics, critical path) is
    /// C22/C23 fold-time work (T42/T43), deliberately not here.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn run_finished(&mut self, outcome: RunOutcome) -> Result<(), SinkFault> {
        self.emit(&Event::RunFinished { outcome })
    }

    /// Emit an arbitrary [`Event`], the same path every typed helper takes.
    ///
    /// The run loop (T24) that produces transitions may name the [`Event`]
    /// variants directly; the typed helpers above are the ergonomic surface.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink cannot record the transition.
    pub fn emit_event(&mut self, event: &Event) -> Result<(), SinkFault> {
        self.emit(event)
    }

    /// Flush (fsync, via the sink's `flush`) at run end or cancellation.
    ///
    /// This is the single fsync boundary the spec promises (T0.6 §6): the writer
    /// does **not** fsync per event; it asks the sink to make its accepted bytes
    /// durable exactly once, at the known-complete boundary. Call it at
    /// `run-finished` and at cancellation.
    ///
    /// # Errors
    /// Returns a [`SinkFault`] if the sink's flush fails.
    pub fn finish(&mut self) -> Result<(), SinkFault> {
        self.sink.flush().map_err(|source| {
            self.faulted = true;
            SinkFault {
                reason: EVENT_STREAM_UNWRITABLE,
                source,
            }
        })
    }

    /// Whether a prior append/flush faulted (the run is unwritable).
    #[must_use]
    pub fn is_faulted(&self) -> bool {
        self.faulted
    }

    /// The gapless sequence number the next record will carry.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Build the canonical envelope, append-and-record it, then advance the
    /// sequence. Write-through: the record reaches the sink before this returns
    /// (no user-space buffering — T0.6 §6). A sink error is a run-level fault.
    fn emit(&mut self, event: &Event) -> Result<(), SinkFault> {
        let seq = self.next_seq;
        let offset_ns = self.clock.elapsed_ns();
        let wall = (self.wall)();
        let line = self.canonical_line(seq, &wall, offset_ns, event);
        self.sink.append_line(line.as_bytes()).map_err(|source| {
            self.faulted = true;
            SinkFault {
                reason: EVENT_STREAM_UNWRITABLE,
                source,
            }
        })?;
        // Only advance after a successful append: a faulted record leaves no gap.
        self.next_seq += 1;
        Ok(())
    }

    /// Serialize one record to its canonical single-line JSON bytes plus the
    /// terminating newline (T4 §1, §6).
    ///
    /// The wire form is the **published** event-stream schema
    /// (`schemas/event-stream/v1.schema.json`): the T0.6/C19 header
    /// (`schema_version`, `run_id`, `seq`, `wall` as an RFC3339 string,
    /// `offset_ns`), the `kind` discriminator, and the per-kind payload **spread
    /// at the top level** (never nested under a `body`), so the C22 fold reads it
    /// directly.
    fn canonical_line(&self, seq: u64, wall: &str, offset_ns: u64, event: &Event) -> String {
        let (kind, fields) = event_wire(event, &self.run_id);
        // Build the record as a serde_json::Map: the shared header envelope plus
        // the per-kind payload fields spread top-level, then emit canonically
        // (sorted keys, compact) — serde_json does not sort keys by default.
        let mut record = serde_json::Map::new();
        record.insert(
            "schema_version".into(),
            serde_json::Value::from(EVENT_STREAM_SCHEMA_VERSION),
        );
        record.insert(
            "run_id".into(),
            serde_json::Value::from(self.run_id.clone()),
        );
        record.insert("seq".into(), serde_json::Value::from(seq));
        record.insert("wall".into(), serde_json::Value::from(wall));
        record.insert("offset_ns".into(), serde_json::Value::from(offset_ns));
        record.insert("kind".into(), serde_json::Value::from(kind));
        // Spread the per-kind fields top-level. A field never collides with a
        // header key (the vocabulary keys are `node`/`attempt`/`header`/… ).
        for (k, v) in fields {
            record.insert(k, v);
        }
        let value = serde_json::Value::Object(record);
        let mut out = String::new();
        // The T4 §6 canonicalization is shared with the graph-artifact emitter
        // (T40) so both rest on one authoritative canonicalizer (see
        // `crate::canonical`).
        crate::canonical::write_canonical(&value, &mut out);
        out.push('\n');
        out
    }
}

/// One top-level payload field name and value, spread beside the C19 header.
type WireField = (String, serde_json::Value);

/// Map an [`Event`] to its `(wire-kind-name, top-level-fields)` pair. The fields
/// are spread beside the shared header (never nested under a `body`), matching
/// the published schema's per-kind shapes: `run-started` carries `header`,
/// per-node events carry `node` (+`attempt`), `node-terminal` carries `state`,
/// `run-finished` carries `outcome`, and `attempt-outcome` carries the rich
/// fold payload.
fn event_wire(event: &Event, run_id: &str) -> (&'static str, Vec<WireField>) {
    fn f(name: &str, v: serde_json::Value) -> WireField {
        (name.to_string(), v)
    }
    match event {
        Event::RunStarted(h) => (
            "run-started",
            vec![f("header", run_started_header(h, run_id))],
        ),
        Event::NodeReady { node } => ("node-ready", vec![f("node", node.clone().into())]),
        Event::NodeAdmitted { node } => ("node-admitted", vec![f("node", node.clone().into())]),
        Event::AttemptStarted { node, attempt } => (
            "attempt-started",
            vec![
                f("node", node.clone().into()),
                f("attempt", (*attempt).into()),
            ],
        ),
        Event::AttemptSucceeded { node, attempt } => (
            "attempt-succeeded",
            vec![
                f("node", node.clone().into()),
                f("attempt", (*attempt).into()),
            ],
        ),
        Event::AttemptFailed { node, attempt } => (
            "attempt-failed",
            vec![
                f("node", node.clone().into()),
                f("attempt", (*attempt).into()),
            ],
        ),
        Event::AttemptOutcome(record) => ("attempt-outcome", attempt_outcome_fields(record)),
        Event::NodeTerminal { node, state } => (
            "node-terminal",
            vec![
                f("node", node.clone().into()),
                f("state", state.as_str().into()),
            ],
        ),
        Event::ZombieAtExit { node, attempt } => (
            "zombie-at-exit",
            vec![
                f("node", node.clone().into()),
                f("attempt", (*attempt).into()),
            ],
        ),
        Event::RunFinished { outcome } => {
            ("run-finished", vec![f("outcome", outcome.as_str().into())])
        }
    }
}

/// Build the `run-started` header **object** (spread under the `header` key): the
/// full run-artifact header known at start, matching the published schema's
/// required set (`run_id` is stamped by the writer's envelope, so it is copied in
/// here too). The fingerprints are always present — a real hash when assembly
/// succeeded, else [`FINGERPRINT_UNAVAILABLE`] — because the schema requires
/// them on every `run-started`; `data_interval` and `resume_lineage` are `null`
/// when absent (the schema types them `object|null`).
fn run_started_header(h: &RunStartedHeader, run_id: &str) -> serde_json::Value {
    let mut header = serde_json::Map::new();
    header.insert("run_id".into(), serde_json::Value::from(run_id.to_owned()));
    header.insert(
        "pipeline".into(),
        serde_json::Value::from(h.pipeline.clone()),
    );
    header.insert(
        "fingerprint_structural".into(),
        serde_json::Value::from(
            h.fingerprint_structural
                .clone()
                .unwrap_or_else(|| FINGERPRINT_UNAVAILABLE.to_string()),
        ),
    );
    header.insert(
        "fingerprint_policy".into(),
        serde_json::Value::from(
            h.fingerprint_policy
                .clone()
                .unwrap_or_else(|| FINGERPRINT_UNAVAILABLE.to_string()),
        ),
    );
    header.insert(
        "fingerprint_algorithm_version".into(),
        serde_json::Value::from(h.fingerprint_algorithm_version),
    );
    header.insert("parameters".into(), string_map(&h.parameters));
    header.insert(
        "data_interval".into(),
        match &h.data_interval {
            Some([start, end]) => serde_json::json!({ "start": start, "end": end }),
            None => serde_json::Value::Null,
        },
    );
    header.insert("captured_environment".into(), string_map(&h.captured_env));
    header.insert(
        "resume_lineage".into(),
        match &h.resumed_from {
            Some(from) => serde_json::json!({ "run_id": from }),
            None => serde_json::Value::Null,
        },
    );
    serde_json::Value::Object(header)
}

/// Build the top-level fields of an `attempt-outcome` record: always `node`,
/// `attempt`, and `status`; every richer field (worker, message, error, metrics,
/// cost, durable reference, resume/propagation lineage) only when the caller
/// supplied it — the fold defaults each missing field.
fn attempt_outcome_fields(r: &AttemptOutcomeRecord) -> Vec<WireField> {
    let mut fields: Vec<WireField> = vec![
        ("node".to_string(), r.node.clone().into()),
        ("attempt".to_string(), r.attempt.into()),
        ("status".to_string(), r.status.clone().into()),
    ];
    // Optional string fields — emitted only when present.
    for (name, opt) in [
        ("worker", &r.worker),
        ("message", &r.message),
        ("satisfied_from_run", &r.satisfied_from_run),
        ("originating_node", &r.originating_node),
    ] {
        if let Some(s) = opt {
            fields.push((name.to_string(), s.clone().into()));
        }
    }
    // Optional structured (JSON value) fields — emitted only when present.
    for (name, opt) in [
        ("error", &r.error),
        ("metrics", &r.metrics),
        ("cost_declared", &r.cost_declared),
        ("cost_measured", &r.cost_measured),
        ("durable_reference", &r.durable_reference),
    ] {
        if let Some(val) = opt {
            fields.push((name.to_string(), val.clone()));
        }
    }
    fields
}

/// Convert a name→value `BTreeMap` into a JSON object value.
fn string_map(map: &BTreeMap<String, String>) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
        .collect();
    serde_json::Value::Object(obj)
}

/// The current time as an **RFC3339 UTC** string (`YYYY-MM-DDTHH:MM:SS.mmmZ`) —
/// the informational wall-clock stamp the published schema types as a non-empty
/// string. Never used for durations (the monotonic `offset_ns` is authoritative
/// — T0.6 §7), so this stamp's only job is to be a valid, human-readable RFC3339
/// instant. Computed dependency-free from `SystemTime` (no `chrono`/`time` — the
/// runtime writer stays on `serde_json` + `uuid` only, per the T4 ADR
/// supply-chain posture).
fn rfc3339_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    rfc3339_from_unix_millis(unix_ms)
}

/// Format Unix milliseconds as an RFC3339 UTC string. Uses Howard Hinnant's
/// public-domain days-from-civil inverse (`civil_from_days`) so the conversion is
/// exact for any date, dependency-free.
fn rfc3339_from_unix_millis(unix_ms: u128) -> String {
    // Unix millis fit i64 for any realistic instant (i64 millis span ±292M years);
    // an out-of-range value saturates rather than panicking (the stamp is only
    // informational).
    let secs = i64::try_from(unix_ms / 1000).unwrap_or(i64::MAX);
    let millis = u64::try_from(unix_ms % 1000).unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    // civil_from_days (Hinnant): days since 1970-01-01 -> (year, month, day).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
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

/// Tolerantly read a JSONL event stream, discarding **at most one trailing
/// partial record** (C19 / T4 §1).
///
/// This is the reader half of the writer/reader contract the crash-safety suite
/// (T27) and the run-artifact fold (C22 / T42) build on. It parses each physical
/// line independently:
///
/// - Every complete line (terminated by `\n`) must parse; a **non-final** line
///   that fails to parse is a corruption, reported as a [`ReadError`].
/// - An **unterminated final line** (the bytes after the last `\n`) is the single
///   tolerated trailing partial: it is discarded and
///   [`trailing_partial_discarded`](ReadStream::trailing_partial_discarded) is
///   set — this is the abrupt-kill tolerance, because the default sink does not
///   fsync per event (T0.6 §6).
///
/// It needs nothing but the bytes — no live writer, no run object — which is what
/// makes the stream self-contained for folding (C22).
///
/// # Errors
/// Returns a [`ReadError`] if a non-final (fully terminated) line fails to parse.
pub fn read_records(bytes: &[u8]) -> Result<ReadStream, ReadError> {
    let mut records = Vec::new();
    let mut trailing_partial_discarded = false;

    // Split on '\n'. A trailing '\n' yields a final empty segment (no partial);
    // a missing trailing '\n' yields a non-empty final segment (the partial).
    let mut segments: Vec<&[u8]> = bytes.split(|&b| b == b'\n').collect();
    // The element after the last '\n' is the "tail": empty if the stream ended
    // on a newline (a clean boundary), non-empty if the final record was cut.
    let tail = segments.pop().unwrap_or(&[]);
    let has_partial_tail = !tail.is_empty();

    for (i, seg) in segments.iter().enumerate() {
        // A blank line between records is not a record; skip it defensively.
        if seg.is_empty() {
            continue;
        }
        match serde_json::from_slice::<serde_json::Value>(seg) {
            Ok(v) => records.push(v),
            // A terminated line that does not parse is genuine corruption, not
            // the tolerated trailing partial.
            Err(_) => return Err(ReadError { line: i }),
        }
    }

    if has_partial_tail {
        // The one tolerated trailing partial. If it happens to parse as valid
        // JSON (a complete-but-unterminated final record), keep it; otherwise
        // discard it. Either way we tolerate exactly one unterminated tail.
        match serde_json::from_slice::<serde_json::Value>(tail) {
            Ok(v) => records.push(v),
            Err(_) => trailing_partial_discarded = true,
        }
    }

    Ok(ReadStream {
        records,
        trailing_partial_discarded,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_write_sorts_object_keys() {
        // Build with keys deliberately out of order.
        let mut map = serde_json::Map::new();
        map.insert("zebra".into(), serde_json::Value::from(1));
        map.insert("alpha".into(), serde_json::Value::from(2));
        map.insert("middle".into(), serde_json::Value::from(3));
        let value = serde_json::Value::Object(map);
        let mut out = String::new();
        crate::canonical::write_canonical(&value, &mut out);
        assert_eq!(out, r#"{"alpha":2,"middle":3,"zebra":1}"#);
    }

    #[test]
    fn canonical_write_is_compact_and_recursive() {
        let value = serde_json::json!({
            "b": {"y": 1, "x": 2},
            "a": [3, 2, 1],
        });
        let mut out = String::new();
        crate::canonical::write_canonical(&value, &mut out);
        // Outer keys sorted (a<b), inner keys sorted (x<y), arrays preserve
        // order (arrays are ordered, not sorted), compact whitespace.
        assert_eq!(out, r#"{"a":[3,2,1],"b":{"x":2,"y":1}}"#);
    }

    #[test]
    fn string_escaping_is_minimal_and_utf8() {
        let mut out = String::new();
        crate::canonical::write_json_string("a\"b\\c\nd\te—é", &mut out);
        // Quote/backslash/control escaped; the em dash and é stay literal UTF-8.
        assert_eq!(out, "\"a\\\"b\\\\c\\nd\\te—é\"");
    }

    #[test]
    fn generate_produces_a_uuidv7() {
        let id = RunId::generate();
        // Parses as a UUID and carries version 7.
        let parsed = Uuid::parse_str(id.as_str()).expect("valid UUID");
        assert_eq!(parsed.get_version_num(), 7, "run id is a UUIDv7");
    }

    #[test]
    fn operator_id_is_verbatim() {
        let id = RunId::from_operator("job-42/attempt-1");
        assert_eq!(
            id.as_str(),
            "job-42/attempt-1",
            "honored verbatim (T0.6 §4)"
        );
    }

    #[test]
    fn reader_reports_nonfinal_corruption() {
        // A terminated line that does not parse (in the middle) is corruption,
        // not the tolerated trailing partial.
        let bytes = b"{\"a\":1}\nnot json\n{\"b\":2}\n";
        let err = read_records(bytes).unwrap_err();
        assert_eq!(err.line, 1);
    }

    #[test]
    fn reader_handles_empty_and_newline_only() {
        assert_eq!(read_records(b"").unwrap().records.len(), 0);
        assert!(!read_records(b"").unwrap().trailing_partial_discarded);
        // A lone record with no trailing newline is the tolerated partial only if
        // it fails to parse; a complete-but-unterminated final record is kept.
        let r = read_records(b"{\"a\":1}").unwrap();
        assert_eq!(r.records.len(), 1);
        assert!(!r.trailing_partial_discarded);
    }

    #[test]
    fn rfc3339_from_unix_millis_is_correct() {
        // The Unix epoch.
        assert_eq!(rfc3339_from_unix_millis(0), "1970-01-01T00:00:00.000Z");
        // A known instant: 2026-07-23T00:00:00.123Z. 2026-07-23 is 20657 days
        // after the epoch (verified against a reference calendar).
        let ms = (20_657_u128 * 86_400) * 1000 + 123;
        assert_eq!(rfc3339_from_unix_millis(ms), "2026-07-23T00:00:00.123Z");
        // A leap-year date with a nonzero time of day: 2024-02-29T13:45:07.500Z.
        // 2024-02-29 is 19782 days after the epoch.
        let ms = ((19_782_u128 * 86_400) + 13 * 3600 + 45 * 60 + 7) * 1000 + 500;
        assert_eq!(rfc3339_from_unix_millis(ms), "2024-02-29T13:45:07.500Z");
    }

    #[test]
    fn attempt_outcome_spreads_rich_fields_and_omits_absent() {
        let record = AttemptOutcomeRecord {
            node: "n".into(),
            attempt: 2,
            status: "succeeded".into(),
            worker: Some("compute#3".into()),
            metrics: Some(serde_json::json!({ "rows": 42 })),
            ..AttemptOutcomeRecord::default()
        };
        let (kind, fields) = event_wire(&Event::AttemptOutcome(record), "run-xyz");
        assert_eq!(kind, "attempt-outcome");
        let map: std::collections::BTreeMap<String, serde_json::Value> =
            fields.into_iter().collect();
        assert_eq!(map["node"], serde_json::json!("n"));
        assert_eq!(map["attempt"], serde_json::json!(2));
        assert_eq!(map["status"], serde_json::json!("succeeded"));
        assert_eq!(map["worker"], serde_json::json!("compute#3"));
        assert_eq!(map["metrics"], serde_json::json!({ "rows": 42 }));
        // Absent optionals are omitted (the fold defaults them).
        assert!(!map.contains_key("error"));
        assert!(!map.contains_key("cost_declared"));
        assert!(!map.contains_key("message"));
    }

    #[test]
    fn assembly_failed_header_carries_fingerprint_sentinels() {
        // A run-started header with no fingerprints (assembly failed) still emits
        // the schema-required fingerprint fields, as the documented sentinel.
        let h = RunStartedHeader {
            pipeline: "p".into(),
            fingerprint_structural: None,
            fingerprint_policy: None,
            fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
            parameters: BTreeMap::new(),
            data_interval: None,
            captured_env: BTreeMap::new(),
            resumed_from: None,
        };
        let header = run_started_header(&h, "run-xyz");
        assert_eq!(header["run_id"], serde_json::json!("run-xyz"));
        assert_eq!(
            header["fingerprint_structural"],
            serde_json::json!(FINGERPRINT_UNAVAILABLE)
        );
        assert_eq!(
            header["fingerprint_policy"],
            serde_json::json!(FINGERPRINT_UNAVAILABLE)
        );
        assert_eq!(header["data_interval"], serde_json::Value::Null);
        assert_eq!(header["resume_lineage"], serde_json::Value::Null);
    }

    #[test]
    fn terminal_and_outcome_wire_names_are_normative() {
        // Spot-check the exact arch.md "Vocabulary" spellings.
        assert_eq!(TerminalState::UpstreamSkipped.as_str(), "upstream-skipped");
        assert_eq!(
            TerminalState::SatisfiedFromPrior.as_str(),
            "satisfied-from-prior"
        );
        assert_eq!(RunOutcome::AssemblyFailed.as_str(), "assembly-failed");
        // The bootstrap-failed outcome is distinct from assembly-failed (C12/T32).
        assert_eq!(RunOutcome::BootstrapFailed.as_str(), "bootstrap-failed");
        assert_ne!(
            RunOutcome::BootstrapFailed.as_str(),
            RunOutcome::AssemblyFailed.as_str()
        );
    }
}
