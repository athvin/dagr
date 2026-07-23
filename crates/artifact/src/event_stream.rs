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
    /// The informational wall-clock source (Unix milliseconds). Never used for
    /// durations — offsets are authoritative (T0.6 §7).
    wall: fn() -> u64,
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
    /// [`run_started`](Self::run_started). The wall-clock stamp defaults to Unix
    /// milliseconds and is informational only.
    #[must_use]
    pub fn new(sink: S, clock: C, run_id: RunId, pipeline: impl Into<String>) -> Self {
        Self {
            sink,
            clock,
            run_id: run_id.0,
            pipeline: pipeline.into(),
            next_seq: 0,
            wall: unix_millis,
            faulted: false,
        }
    }

    /// Override the informational wall-clock source (default: Unix
    /// milliseconds).
    ///
    /// The wall stamp is **informational only** — never used for durations
    /// (offsets are authoritative — T0.6 §7) — so overriding it changes no
    /// behavior that matters to a consumer. It exists to make the record bytes
    /// fully deterministic under test (the wall stamp is a record's analog of an
    /// artifact's excluded generation-time field, T4 §6): holding it fixed lets
    /// two emissions of the same record be byte-identical.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: fn() -> u64) -> Self {
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
    pub fn zombie_at_exit(&mut self, node: &str) -> Result<(), SinkFault> {
        self.emit(&Event::ZombieAtExit { node: node.into() })
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
        let line = self.canonical_line(seq, wall, offset_ns, event);
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
    fn canonical_line(&self, seq: u64, wall: u64, offset_ns: u64, event: &Event) -> String {
        let (kind, body) = event_wire(event);
        // Build the envelope as a serde_json::Value, then emit canonically
        // (sorted keys, compact) — serde_json does not sort keys by default.
        let mut envelope = serde_json::Map::new();
        envelope.insert(
            "schema_version".into(),
            serde_json::Value::from(EVENT_STREAM_SCHEMA_VERSION),
        );
        envelope.insert(
            "run_id".into(),
            serde_json::Value::from(self.run_id.clone()),
        );
        envelope.insert("seq".into(), serde_json::Value::from(seq));
        envelope.insert("wall".into(), serde_json::Value::from(wall));
        envelope.insert("offset_ns".into(), serde_json::Value::from(offset_ns));
        envelope.insert("event".into(), serde_json::Value::from(kind));
        envelope.insert("body".into(), body);
        let value = serde_json::Value::Object(envelope);
        let mut out = String::new();
        canonical_write(&value, &mut out);
        out.push('\n');
        out
    }
}

// === Canonicalization (T4 §6) =============================================

/// Write a JSON value in the T4 canonical form: object keys sorted
/// lexicographically by byte order, compact (no insignificant whitespace),
/// integers only. This is what makes two emissions of the same record
/// byte-identical.
fn canonical_write(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(map) => {
            out.push('{');
            // BTreeMap gives lexicographic (byte-order) key ordering.
            let sorted: BTreeMap<&String, &serde_json::Value> = map.iter().collect();
            for (i, (k, v)) in sorted.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                canonical_write(v, out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_write(v, out);
            }
            out.push(']');
        }
        serde_json::Value::String(s) => write_json_string(s, out),
        // Booleans, integers, and null render identically to serde_json's compact
        // form; all dagr numeric fields are integers (T4 §6), so no float
        // formatting hazard arises.
        other => out.push_str(&other.to_string()),
    }
}

/// Emit a JSON string with minimal, deterministic escaping (T4 §6): escape only
/// what JSON requires (`"`, `\`, and control chars U+0000–U+001F); non-ASCII
/// printable characters are emitted literally as UTF-8, never `\u`-escaped.
fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Map an [`Event`] to its `(wire-kind-name, body)` pair. The body is a
/// `serde_json` object; `run-started` and `run-finished` carry structured
/// bodies, and per-node events carry `{node, ...}`.
fn event_wire(event: &Event) -> (&'static str, serde_json::Value) {
    use serde_json::json;
    match event {
        Event::RunStarted(h) => ("run-started", run_started_body(h)),
        Event::NodeReady { node } => ("node-ready", json!({ "node": node })),
        Event::NodeAdmitted { node } => ("node-admitted", json!({ "node": node })),
        Event::AttemptStarted { node, attempt } => (
            "attempt-started",
            json!({ "node": node, "attempt": attempt }),
        ),
        Event::AttemptSucceeded { node, attempt } => (
            "attempt-succeeded",
            json!({ "node": node, "attempt": attempt }),
        ),
        Event::AttemptFailed { node, attempt } => (
            "attempt-failed",
            json!({ "node": node, "attempt": attempt }),
        ),
        Event::NodeTerminal { node, state } => (
            "node-terminal",
            json!({ "node": node, "state": state.as_str() }),
        ),
        Event::ZombieAtExit { node } => ("zombie-at-exit", json!({ "node": node })),
        Event::RunFinished { outcome } => ("run-finished", json!({ "outcome": outcome.as_str() })),
    }
}

/// Build the `run-started` body: every header field known at start, omitting the
/// fingerprints and resume lineage when they are absent (assembly-failed variant).
fn run_started_body(h: &RunStartedHeader) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert(
        "pipeline".into(),
        serde_json::Value::from(h.pipeline.clone()),
    );
    if let Some(fp) = &h.fingerprint_structural {
        body.insert(
            "fingerprint_structural".into(),
            serde_json::Value::from(fp.clone()),
        );
    }
    if let Some(fp) = &h.fingerprint_policy {
        body.insert(
            "fingerprint_policy".into(),
            serde_json::Value::from(fp.clone()),
        );
    }
    body.insert("parameters".into(), string_map(&h.parameters));
    if let Some([start, end]) = &h.data_interval {
        body.insert(
            "data_interval".into(),
            serde_json::Value::Array(vec![
                serde_json::Value::from(start.clone()),
                serde_json::Value::from(end.clone()),
            ]),
        );
    }
    body.insert("captured_env".into(), string_map(&h.captured_env));
    if let Some(from) = &h.resumed_from {
        body.insert("resumed_from".into(), serde_json::Value::from(from.clone()));
    }
    serde_json::Value::Object(body)
}

/// Convert a name→value `BTreeMap` into a JSON object value.
fn string_map(map: &BTreeMap<String, String>) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
        .collect();
    serde_json::Value::Object(obj)
}

/// Unix milliseconds — the informational wall-clock stamp. Never used for
/// durations (offsets are authoritative — T0.6 §7).
fn unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
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
        canonical_write(&value, &mut out);
        assert_eq!(out, r#"{"alpha":2,"middle":3,"zebra":1}"#);
    }

    #[test]
    fn canonical_write_is_compact_and_recursive() {
        let value = serde_json::json!({
            "b": {"y": 1, "x": 2},
            "a": [3, 2, 1],
        });
        let mut out = String::new();
        canonical_write(&value, &mut out);
        // Outer keys sorted (a<b), inner keys sorted (x<y), arrays preserve
        // order (arrays are ordered, not sorted), compact whitespace.
        assert_eq!(out, r#"{"a":[3,2,1],"b":{"x":2,"y":1}}"#);
    }

    #[test]
    fn string_escaping_is_minimal_and_utf8() {
        let mut out = String::new();
        write_json_string("a\"b\\c\nd\te—é", &mut out);
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
    fn terminal_and_outcome_wire_names_are_normative() {
        // Spot-check the exact arch.md "Vocabulary" spellings.
        assert_eq!(TerminalState::UpstreamSkipped.as_str(), "upstream-skipped");
        assert_eq!(
            TerminalState::SatisfiedFromPrior.as_str(),
            "satisfied-from-prior"
        );
        assert_eq!(RunOutcome::AssemblyFailed.as_str(), "assembly-failed");
    }
}
