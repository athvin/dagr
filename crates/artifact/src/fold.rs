//! The C22 **fold** — the standalone reader that turns a C19 event stream into a
//! run artifact (arch.md `### C22 · Run artifact`).
//!
//! # What the fold is
//!
//! The run artifact is *derived, never authored*: every field comes from folding
//! the append-only event stream (C19). [`fold_stream`] is that fold — a pure
//! function from stream bytes (plus the graph's node roster, for coverage) to a
//! [`RunArtifact`]. It takes **no run store, no live graph, and no network**
//! (arch.md C19: the fold "needs no access to the original run"), so it runs both
//! at natural run end and, crucially, by a *later* invocation over a crashed
//! run's stream — the dead run cannot fold itself.
//!
//! The fold is a **reader only**: it never re-runs a task, never mutates
//! execution, and never changes the graph's shape. It reads the bytes it is
//! given and produces a static artifact. Cross-run analysis stays "concatenate
//! streams partitioned by run identity" (C19), not a queryable store.
//!
//! # Input: the published event-stream wire form (C19 / T39)
//!
//! The fold reads the **published** event-stream schema
//! (`schemas/event-stream/v1.schema.json`, T39): `kind`-discriminated JSON-Lines
//! records carrying the T0.6/C19 header (run identity, `schema_version`, gapless
//! `seq`, informational `wall`, authoritative `offset_ns`). The
//! **`attempt-outcome`** record is the fold's richest input: it carries the
//! attempt's terminal status, the task-reported metrics, declared-vs-measured
//! cost, structured error and message, the durable-output reference, the
//! retention flag, and the measured slot residency. Durations are computed from
//! `offset_ns` **only** — never from the informational `wall` stamp (C19).
//!
//! The fold **declares which stream schema versions it reads**
//! ([`ACCEPTED_STREAM_SCHEMA_VERSIONS`]) and its own reader version
//! ([`FOLD_READER_VERSION`]); both are recorded on the produced artifact under
//! `fold_reader`. Within a stream version, evolution is additive-only: the fold
//! **ignores unknown fields and defaults missing ones** (T0.10).
//!
//! # Determinism & truncation tolerance
//!
//! The fold is deterministic — folding the same stream twice yields byte-
//! identical artifacts (attempts are ordered by `(seq)`, maps serialize
//! canonically). It reuses the C19 tolerant reader
//! ([`crate::event_stream::read_records`]): it discards **at most one** trailing
//! partial record (the abrupt-kill tolerance, since the default sink does not
//! fsync per event) and marks the artifact `interrupted`. A **non-final**
//! corruption, or a *second* trailing corruption, is a hard [`FoldError`], never
//! silently dropped (the C19 tolerance boundary).
//!
//! # Interrupted representation (why `cancelled` + a first-class `interrupted`)
//!
//! A crash-truncated stream — one with no `run-finished` record — has no
//! terminal outcome to fold. The published run schema's `overall_outcome` enum
//! (`schemas/run/v1.schema.json`) is **closed** (`succeeded`/`failed`/
//! `cancelled`/`assembly-failed`/`bootstrap-failed`) and carries **no dedicated
//! `interrupted` token**, so `overall_outcome` is set to `cancelled` (an
//! interrupted run terminated without completing) — staying inside the closed
//! enum keeps the artifact schema-valid.
//!
//! To keep a consumer from conflating a *crash-truncation* with a *deliberate
//! cancellation* (both read `overall_outcome = "cancelled"`), the interrupted
//! signal is promoted to a **first-class, top-level artifact field**
//! `interrupted` (a boolean) on the folded artifact — mirrored on
//! [`RunArtifact::is_interrupted`] and (for a reader that reads only that block)
//! `fold_reader.interrupted`. This is safe and additive: the run schema is
//! **open-world at every level** — no object sets `additionalProperties:false`
//! (the schema's own header note; T0.10 additive evolution) — so a top-level
//! `interrupted` field validates against the *unmodified* published schema. A
//! downstream consumer therefore distinguishes crash-truncation from deliberate
//! cancellation by reading one boolean, without any provenance archaeology.
//!
//! Promoting the distinction into `overall_outcome` itself (a dedicated
//! `interrupted` enum token) is **deferred to a schema revision** (`dagr.run@2`),
//! since it would widen a *closed* enum and is out of this ticket's scope.
//!
//! # Open-question resolutions (recorded here, per the ticket)
//!
//! - **Canonical phase list** — the fold names four phases that *partition* an
//!   attempt's elapsed time so they sum to the total exactly (C22 phase
//!   criterion), aligned to the C14 waiting/admission/dispatch/backoff phases:
//!   `ready-wait` (node-ready → node-admitted), `permit-wait` (node-admitted →
//!   attempt-started), `executing` (attempt-started → attempt terminal), and
//!   `backoff` (the inter-attempt wait a retry records, folded onto the *later*
//!   attempt so per-attempt phases still sum to that attempt's own total). Each
//!   phase is a non-negative `offset_ns` delta; every attempt's phases sum
//!   bit-exactly to its total elapsed. See [`PHASE_READY_WAIT`] and siblings.
//! - **Worker identity** — recorded as **both** the pool name and the thread id,
//!   as a single unambiguous string `"<pool>#<thread>"` (e.g.
//!   `"compute#3"`), so capacity analysis reads one field. When the stream
//!   supplies an explicit `worker` string it is used verbatim; otherwise the
//!   fold synthesizes one from the `pool`/`thread` fields, defaulting to
//!   [`UNKNOWN_WORKER`] when neither is present.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::event_stream::read_records;

/// The fold-reader version declared on every produced artifact (C22 schema
/// discipline). Bumped when the fold's *reading* behavior changes in a way a
/// downstream consumer must notice; it is independent of the artifact
/// `schema_version` (which the run schema governs).
pub const FOLD_READER_VERSION: &str = "dagr.run-fold@1";

/// The event-stream schema versions this fold declares it can read (C22: "the
/// folding function declares which stream schema versions it reads"). Recorded
/// on the artifact under `fold_reader.accepts`.
pub const ACCEPTED_STREAM_SCHEMA_VERSIONS: &[&str] = &["dagr.event-stream@1"];

/// The `ready-wait` phase: node-ready → node-admitted (C14 waiting).
pub const PHASE_READY_WAIT: &str = "ready-wait";
/// The `permit-wait` phase: node-admitted → attempt-started (C14 admission).
pub const PHASE_PERMIT_WAIT: &str = "permit-wait";
/// The `executing` phase: attempt-started → attempt terminal (C14 dispatch).
pub const PHASE_EXECUTING: &str = "executing";
/// The `backoff` phase: the inter-attempt wait a retry records (C14 backoff).
pub const PHASE_BACKOFF: &str = "backoff";

/// The worker string used when the stream names neither a pool nor a thread.
pub const UNKNOWN_WORKER: &str = "unknown";

// === Errors ================================================================

/// A fold failure. The fold is tolerant of exactly one *trailing* partial
/// record (C19); anything else that prevents producing a faithful artifact is
/// one of these.
#[derive(Debug)]
pub enum FoldError {
    /// A record could not be parsed and it was **not** the single tolerated
    /// trailing partial — a non-final corruption, or a second trailing
    /// corruption (the C19 tolerance boundary).
    CorruptRecord {
        /// The zero-based physical line index that failed to parse.
        line: usize,
    },
    /// The stream carried no `run-started` record, so the header cannot be
    /// assembled — there is nothing to fold.
    MissingRunStarted,
}

impl std::fmt::Display for FoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FoldError::CorruptRecord { line } => {
                write!(f, "corrupt event-stream record at line {line} (not the tolerated trailing partial)")
            }
            FoldError::MissingRunStarted => {
                write!(f, "event stream has no run-started record; nothing to fold")
            }
        }
    }
}

impl std::error::Error for FoldError {}

// === The produced artifact =================================================

/// One attempt record in the run artifact body — one per *attempt*, never
/// collapsed per node (C22).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    node: String,
    attempt: u32,
    status: String,
    phase_durations_ns: BTreeMap<String, u64>,
    worker: String,
    message: Option<String>,
    error: Option<Value>,
    metrics: Value,
    cost_declared: Option<Value>,
    cost_measured: Option<Value>,
    durable_reference: Option<Value>,
    satisfied_from_run: Option<String>,
    originating_node: Option<String>,
}

impl AttemptRecord {
    /// The node's author-declared identity (T13).
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }
    /// The 1-based attempt number.
    #[must_use]
    pub fn attempt_number(&self) -> u32 {
        self.attempt
    }
    /// The terminal status from the normative taxonomy (arch.md Vocabulary).
    #[must_use]
    pub fn status(&self) -> &str {
        &self.status
    }
    /// The named phase durations (nanoseconds), which sum to
    /// [`total_elapsed_ns`](Self::total_elapsed_ns) exactly.
    #[must_use]
    pub fn phase_durations_ns(&self) -> &BTreeMap<String, u64> {
        &self.phase_durations_ns
    }
    /// The attempt's total elapsed time — the sum of its phase durations
    /// (bit-exact by construction, since both derive from monotonic offsets).
    #[must_use]
    pub fn total_elapsed_ns(&self) -> u64 {
        self.phase_durations_ns.values().copied().sum()
    }
    /// The worker that ran this attempt (`"<pool>#<thread>"`, or a verbatim
    /// stream-supplied string; see the module-level worker-identity note).
    #[must_use]
    pub fn worker(&self) -> &str {
        &self.worker
    }
    /// The human message, when the stream recorded one.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
    /// The structured error detail, reproduced unmodified from the stream.
    #[must_use]
    pub fn error(&self) -> Option<&Value> {
        self.error.as_ref()
    }
    /// The task-reported metrics, byte/value-identical to the stream (C23: the
    /// fold copies whatever metrics the stream recorded, unmodified).
    #[must_use]
    pub fn metrics(&self) -> &Value {
        &self.metrics
    }
    /// The declared cost vector, juxtaposed against [`cost_measured`](Self::cost_measured).
    #[must_use]
    pub fn cost_declared(&self) -> Option<&Value> {
        self.cost_declared.as_ref()
    }
    /// The measured cost, juxtaposed against [`cost_declared`](Self::cost_declared).
    #[must_use]
    pub fn cost_measured(&self) -> Option<&Value> {
        self.cost_measured.as_ref()
    }
    /// The durable-output reference, present only when the stream recorded one
    /// (a durable node's succeeded attempt, or copied forward on resume — C27).
    #[must_use]
    pub fn durable_reference(&self) -> Option<&Value> {
        self.durable_reference.as_ref()
    }
    /// The originating run identity a `satisfied-from-prior` record carries (C27).
    #[must_use]
    pub fn satisfied_from_run(&self) -> Option<&str> {
        self.satisfied_from_run.as_deref()
    }
    /// The originating node identity an `upstream-skipped` record carries — the
    /// node that decided to skip (arch.md Vocabulary).
    #[must_use]
    pub fn originating_node(&self) -> Option<&str> {
        self.originating_node.as_deref()
    }

    fn to_value(&self) -> Value {
        let mut o = serde_json::Map::new();
        o.insert("node".into(), Value::from(self.node.clone()));
        o.insert("attempt".into(), Value::from(self.attempt));
        o.insert("status".into(), Value::from(self.status.clone()));
        let phases: serde_json::Map<String, Value> = self
            .phase_durations_ns
            .iter()
            .map(|(k, v)| (k.clone(), Value::from(*v)))
            .collect();
        o.insert("phase_durations_ns".into(), Value::Object(phases));
        o.insert("worker".into(), Value::from(self.worker.clone()));
        o.insert(
            "message".into(),
            self.message.clone().map_or(Value::Null, Value::from),
        );
        o.insert("error".into(), self.error.clone().unwrap_or(Value::Null));
        o.insert("metrics".into(), self.metrics.clone());
        o.insert(
            "cost_declared".into(),
            self.cost_declared.clone().unwrap_or(Value::Null),
        );
        o.insert(
            "cost_measured".into(),
            self.cost_measured.clone().unwrap_or(Value::Null),
        );
        if let Some(dref) = &self.durable_reference {
            o.insert("durable_reference".into(), dref.clone());
        }
        if let Some(run) = &self.satisfied_from_run {
            o.insert("satisfied_from_run".into(), Value::from(run.clone()));
        }
        if let Some(node) = &self.originating_node {
            o.insert("originating_node".into(), Value::from(node.clone()));
        }
        Value::Object(o)
    }
}

/// The run summary the fold assembles from the fields the stream supplies (C22).
///
/// The critical-path/structure-vs-resource enrichment is **T43**; this fold
/// wires only the fields the stream already carries — total elapsed, peak
/// measured slot residency, retained values, and the abandoned-work-pinned time
/// and capacity. `critical_path_ns` is populated with the total elapsed as a
/// conservative lower bound the fold *can* derive from the stream; the true
/// dependency-aware critical path is T43's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    total_elapsed_ns: u64,
    critical_path_ns: u64,
    peak_slot_residency: u64,
    retained_values: Vec<String>,
    abandoned_pinned_time_ns: u64,
    abandoned_pinned_capacity: u64,
}

impl RunSummary {
    fn to_value(&self) -> Value {
        serde_json::json!({
            "total_elapsed_ns": self.total_elapsed_ns,
            "critical_path_ns": self.critical_path_ns,
            "peak_slot_residency": self.peak_slot_residency,
            "retained_values": self.retained_values,
            "abandoned_pinned_time_ns": self.abandoned_pinned_time_ns,
            "abandoned_pinned_capacity": self.abandoned_pinned_capacity,
        })
    }
}

/// A folded C22 run artifact: the outcome of one execution, joinable to the
/// structure. Produced only by [`fold_stream`]; every field is derived from the
/// event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArtifact {
    header: Value,
    overall_outcome: String,
    interrupted: bool,
    trailing_partial_discarded: bool,
    errors: Vec<String>,
    attempts: Vec<AttemptRecord>,
    summary: Option<RunSummary>,
}

impl RunArtifact {
    /// The attempt records — one per attempt, in stream (`seq`) order.
    #[must_use]
    pub fn attempts(&self) -> &[AttemptRecord] {
        &self.attempts
    }
    /// The overall run outcome (`succeeded`/`failed`/`cancelled`/
    /// `assembly-failed`/`bootstrap-failed`). A crash-truncated run with no
    /// `run-finished` is reported `cancelled` (the closed enum has no dedicated
    /// interrupted token) and flagged [`is_interrupted`](Self::is_interrupted) —
    /// **read `is_interrupted` to tell a crash-truncation apart from a deliberate
    /// cancellation**, since both read `cancelled` here (see the module-level
    /// "interrupted representation" note).
    #[must_use]
    pub fn overall_outcome(&self) -> &str {
        &self.overall_outcome
    }
    /// Whether the stream ended without a `run-finished` record — a crash-
    /// truncated (interrupted) run (C22). This is the **first-class** distinction
    /// between a crash-truncation and a deliberate cancellation (both carry
    /// [`overall_outcome`](Self::overall_outcome) `cancelled`); it is serialized
    /// as a top-level `interrupted` boolean on [`to_value`](Self::to_value), so a
    /// consumer of the folded artifact detects it without reading the
    /// `fold_reader` provenance block.
    #[must_use]
    pub fn is_interrupted(&self) -> bool {
        self.interrupted
    }
    /// Whether the tolerant reader discarded a single trailing partial record
    /// (the abrupt-kill tolerance; C19).
    #[must_use]
    pub fn trailing_partial_discarded(&self) -> bool {
        self.trailing_partial_discarded
    }
    /// The complete error list of a pre-execution-failure variant
    /// (`assembly-failed`/`bootstrap-failed`); empty otherwise.
    #[must_use]
    pub fn errors(&self) -> &[String] {
        &self.errors
    }
    /// The run identity from the header.
    #[must_use]
    pub fn header_run_id(&self) -> &str {
        self.header
            .get("run_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
    /// The pipeline identity from the header.
    #[must_use]
    pub fn header_pipeline(&self) -> &str {
        self.header
            .get("pipeline")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
    /// The structural fingerprint (present only when assembly succeeded; C21).
    /// Joins to a graph artifact from the same build.
    #[must_use]
    pub fn header_fingerprint_structural(&self) -> Option<&str> {
        self.header
            .get("fingerprint_structural")
            .and_then(Value::as_str)
    }
    /// The policy-hash fingerprint (present only when assembly succeeded; C21).
    #[must_use]
    pub fn header_fingerprint_policy(&self) -> Option<&str> {
        self.header
            .get("fingerprint_policy")
            .and_then(Value::as_str)
    }
    /// The invocation parameters from the header.
    #[must_use]
    pub fn header_parameters(&self) -> &serde_json::Map<String, Value> {
        static EMPTY: std::sync::OnceLock<serde_json::Map<String, Value>> =
            std::sync::OnceLock::new();
        self.header
            .get("parameters")
            .and_then(Value::as_object)
            .unwrap_or_else(|| EMPTY.get_or_init(serde_json::Map::new))
    }
    /// The data interval from the header, if any.
    #[must_use]
    pub fn header_data_interval(&self) -> Option<&Value> {
        self.header.get("data_interval").filter(|v| !v.is_null())
    }
    /// The allowlisted captured environment values from the header — verbatim,
    /// and **only** these (the fold sources env from nowhere else; C22).
    #[must_use]
    pub fn header_captured_environment(&self) -> &Value {
        static EMPTY: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
        self.header
            .get("captured_environment")
            .unwrap_or_else(|| EMPTY.get_or_init(|| Value::Object(serde_json::Map::new())))
    }
    /// The fold-reader version this artifact was produced by ([`FOLD_READER_VERSION`]).
    #[must_use]
    pub fn fold_reader_version(&self) -> &str {
        FOLD_READER_VERSION
    }

    /// The summary, when present (absent for the pre-execution failure variants).
    #[must_use]
    pub fn summary(&self) -> Option<&RunSummary> {
        self.summary.as_ref()
    }
    /// The values still retained at run end (summary; C10). Empty when there is
    /// no summary.
    #[must_use]
    pub fn summary_retained_values(&self) -> Vec<String> {
        self.summary
            .as_ref()
            .map(|s| s.retained_values.clone())
            .unwrap_or_default()
    }
    /// The peak measured slot residency (summary; C10).
    #[must_use]
    pub fn summary_peak_slot_residency(&self) -> u64 {
        self.summary.as_ref().map_or(0, |s| s.peak_slot_residency)
    }
    /// The run's **total elapsed time** — the authoritative monotonic wall of the
    /// run (last event offset minus the run-start offset, which is 0), never from
    /// the informational `wall` stamps (summary; C22 · T43). Zero when there is no
    /// summary.
    #[must_use]
    pub fn summary_total_elapsed_ns(&self) -> u64 {
        self.summary.as_ref().map_or(0, |s| s.total_elapsed_ns)
    }
    /// The run's **critical-path time** — the longest dependency-respecting chain
    /// of node executing contributions (summary; C22 · T43). See the module-level
    /// critical-path note and `docs/adr/0001-critical-path-definition.md` for
    /// exactly what it includes and excludes. Zero when there is no summary.
    #[must_use]
    pub fn summary_critical_path_ns(&self) -> u64 {
        self.summary.as_ref().map_or(0, |s| s.critical_path_ns)
    }
    /// The time pinned by abandoned-but-running (zombie) work (summary; C10/C14).
    #[must_use]
    pub fn summary_abandoned_pinned_time_ns(&self) -> u64 {
        self.summary
            .as_ref()
            .map_or(0, |s| s.abandoned_pinned_time_ns)
    }
    /// The capacity pinned by abandoned-but-running (zombie) work (summary).
    #[must_use]
    pub fn summary_abandoned_pinned_capacity(&self) -> u64 {
        self.summary
            .as_ref()
            .map_or(0, |s| s.abandoned_pinned_capacity)
    }

    /// The artifact as a `serde_json::Value`, conforming to the published
    /// `schemas/run/v1.schema.json` (T39) plus the additive top-level
    /// `interrupted` boolean and `fold_reader` declaration (both validate against
    /// the unmodified schema — it is open-world at every level, T0.10).
    #[must_use]
    pub fn to_value(&self) -> Value {
        let mut o = serde_json::Map::new();
        // Header: the recorded run-started header, plus the end-only
        // overall_outcome, plus the error list on a pre-execution failure.
        let mut header = self.header.as_object().cloned().unwrap_or_default();
        header.insert(
            "overall_outcome".into(),
            Value::from(self.overall_outcome.clone()),
        );
        if !self.errors.is_empty() {
            header.insert("errors".into(), Value::from(self.errors.clone()));
        }
        o.insert("header".into(), Value::Object(header));
        o.insert(
            "attempts".into(),
            Value::Array(self.attempts.iter().map(AttemptRecord::to_value).collect()),
        );
        o.insert(
            "summary".into(),
            self.summary
                .as_ref()
                .map_or(Value::Null, RunSummary::to_value),
        );
        // First-class crash-truncation signal, promoted to a **top-level artifact
        // field** (not buried in the reader-provenance block) so a consumer keying
        // off the artifact detects an interrupted run without reading
        // `fold_reader`, while `overall_outcome` stays inside its closed enum
        // (`cancelled`). This is additive and schema-valid because the run schema
        // is open-world at every level — no object sets `additionalProperties:false`
        // (`schemas/run/v1.schema.json` header note; T0.10). See the module-level
        // "interrupted representation" note for the full rationale.
        o.insert("interrupted".into(), Value::from(self.interrupted));
        // Additive fold-reader declaration (T0.10: unknown fields validate). The
        // `interrupted` flag is mirrored here too, so the reader-provenance block
        // stays self-describing for a consumer that reads only it.
        o.insert(
            "fold_reader".into(),
            serde_json::json!({
                "version": FOLD_READER_VERSION,
                "accepts": ACCEPTED_STREAM_SCHEMA_VERSIONS,
                "interrupted": self.interrupted,
            }),
        );
        Value::Object(o)
    }

    /// The artifact in canonical JSON (T4 §6): sorted keys, compact, integers —
    /// so folding the same stream twice is byte-identical.
    #[must_use]
    pub fn to_canonical_json(&self) -> String {
        crate::canonical::to_canonical_string(&self.to_value())
    }
}

// === The fold ==============================================================

/// Fold a C19 event stream into a C22 [`RunArtifact`].
///
/// `stream_bytes` is the raw event-stream bytes (JSON Lines, published wire
/// form); `graph_nodes` is the graph's node roster used for **node coverage** —
/// every node in it appears at least once in the artifact, so never-ran nodes
/// carry their propagated terminal state even when the stream held no record for
/// them. Pass an empty slice for the pre-execution-failure variants (no graph).
///
/// The fold is a **reader**: it touches no run store, no live graph, and no
/// network — only the bytes given. It tolerates and discards at most one
/// trailing partial record and marks such a stream `interrupted`.
///
/// # Errors
///
/// Returns [`FoldError::CorruptRecord`] on a non-final (or a second trailing)
/// corruption, and [`FoldError::MissingRunStarted`] if the stream carries no
/// `run-started` record.
pub fn fold_stream(stream_bytes: &[u8], graph_nodes: &[String]) -> Result<RunArtifact, FoldError> {
    let read = read_records(stream_bytes).map_err(|e| FoldError::CorruptRecord { line: e.line })?;
    let records = read.records;

    // The header comes from run-started alone (C19: it carries every start
    // header field). Find it; without it there is nothing to fold.
    let run_started = records
        .iter()
        .find(|r| kind_of(r) == Some("run-started"))
        .ok_or(FoldError::MissingRunStarted)?;
    let header = run_started
        .get("header")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));

    // The overall outcome + error list come from run-finished (if present).
    let run_finished = records.iter().find(|r| kind_of(r) == Some("run-finished"));
    let interrupted = run_finished.is_none();
    let (overall_outcome, errors) = match run_finished {
        Some(rec) => {
            let outcome = rec
                .get("outcome")
                .and_then(Value::as_str)
                .unwrap_or("succeeded")
                .to_string();
            let errors = rec
                .get("errors")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            (outcome, errors)
        }
        // No run-finished ⇒ crash-truncated. arch.md marks such a run
        // `interrupted`; the schema's `overall_outcome` enum has no dedicated
        // token, so the outcome is `cancelled` (an interrupted run terminated
        // without completing) and `is_interrupted()` carries the distinction.
        None => ("cancelled".to_string(), Vec::new()),
    };

    // A pre-execution failure (assembly/bootstrap-failed) has zero attempts and
    // no summary — the stream carries no attempt/lifecycle events.
    let pre_execution = matches!(
        overall_outcome.as_str(),
        "assembly-failed" | "bootstrap-failed"
    );

    let (attempts, summary) = if pre_execution {
        (Vec::new(), None)
    } else {
        let attempts = assemble_attempts(&records, graph_nodes);
        let summary = Some(assemble_summary(&records, &attempts));
        (attempts, summary)
    };

    Ok(RunArtifact {
        header,
        overall_outcome,
        interrupted,
        trailing_partial_discarded: read.trailing_partial_discarded,
        errors,
        attempts,
        summary,
    })
}

/// The `kind` discriminator of a published-wire-form record.
fn kind_of(record: &Value) -> Option<&str> {
    record.get("kind").and_then(Value::as_str)
}

/// The authoritative monotonic offset of a record.
fn offset_of(record: &Value) -> u64 {
    record.get("offset_ns").and_then(Value::as_u64).unwrap_or(0)
}

/// Assemble the one-record-per-attempt body, plus the never-ran node coverage.
fn assemble_attempts(records: &[Value], graph_nodes: &[String]) -> Vec<AttemptRecord> {
    // Phase timing scaffolding: the last-seen node-ready / node-admitted /
    // attempt-started offset per node, so an attempt-outcome can break its
    // elapsed time into phases from monotonic offsets. Retries: the offset of a
    // node's previous attempt terminal becomes the start of the next attempt's
    // backoff phase.
    let mut ready_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut admitted_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut started_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut prev_terminal_at: BTreeMap<String, u64> = BTreeMap::new();

    let mut attempts: Vec<AttemptRecord> = Vec::new();
    // Track which nodes produced an attempt record (from attempt-outcome), so we
    // know which node-terminal records are "never ran" (no matching attempt).
    let mut node_has_attempt: BTreeMap<String, bool> = BTreeMap::new();
    // The node-terminal record per node (for never-ran coverage + statuses that
    // arrive only as a terminal, e.g. satisfied-from-prior / upstream-*).
    let mut terminal: BTreeMap<String, &Value> = BTreeMap::new();

    for rec in records {
        let Some(kind) = kind_of(rec) else { continue };
        let node = rec.get("node").and_then(Value::as_str).map(String::from);
        let off = offset_of(rec);
        match kind {
            "node-ready" => {
                if let Some(n) = node {
                    ready_at.insert(n, off);
                }
            }
            "node-admitted" => {
                if let Some(n) = node {
                    admitted_at.insert(n, off);
                }
            }
            "attempt-started" => {
                if let Some(n) = node {
                    started_at.insert(n, off);
                }
            }
            "attempt-outcome" => {
                if let Some(n) = node.clone() {
                    let record = build_attempt_record(
                        rec,
                        &n,
                        off,
                        &ready_at,
                        &admitted_at,
                        &started_at,
                        &prev_terminal_at,
                    );
                    // The next retry of this node starts its backoff after this
                    // attempt's outcome.
                    prev_terminal_at.insert(n.clone(), off);
                    node_has_attempt.insert(n, true);
                    attempts.push(record);
                }
            }
            "node-terminal" => {
                if let Some(n) = node {
                    terminal.insert(n, rec);
                }
            }
            _ => {}
        }
    }

    // Never-ran node coverage: for every node-terminal without a matching
    // attempt record, synthesize a zero-elapsed attempt record carrying its
    // propagated terminal state (upstream-failed / upstream-skipped / skipped /
    // satisfied-from-prior / cancelled).
    for (n, rec) in &terminal {
        if node_has_attempt.get(n).copied().unwrap_or(false) {
            continue;
        }
        attempts.push(synthesize_never_ran(n, rec));
    }

    // Graph node roster coverage: any graph node with neither an attempt nor a
    // node-terminal record still appears at least once, marked upstream-failed
    // (the conservative propagated state for a node the run never reached).
    for n in graph_nodes {
        let seen = attempts.iter().any(|a| a.node == *n);
        if !seen {
            attempts.push(AttemptRecord {
                node: n.clone(),
                attempt: 1,
                status: "upstream-failed".to_string(),
                phase_durations_ns: zero_phases(),
                worker: UNKNOWN_WORKER.to_string(),
                message: None,
                error: None,
                metrics: Value::Object(serde_json::Map::new()),
                cost_declared: None,
                cost_measured: None,
                durable_reference: None,
                satisfied_from_run: None,
                originating_node: None,
            });
        }
    }

    attempts
}

/// Build one attempt record from an `attempt-outcome` event, breaking its
/// elapsed time into phases from the monotonic offsets of the preceding
/// lifecycle events. Phases sum bit-exactly to the attempt total.
fn build_attempt_record(
    rec: &Value,
    node: &str,
    outcome_off: u64,
    ready_at: &BTreeMap<String, u64>,
    admitted_at: &BTreeMap<String, u64>,
    started_at: &BTreeMap<String, u64>,
    prev_terminal_at: &BTreeMap<String, u64>,
) -> AttemptRecord {
    let attempt = rec
        .get("attempt")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(1);
    let status = rec
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("failed")
        .to_string();

    // Phase breakdown (open-question 1). The **attempt total** is anchored at
    // its own `attempt-started` → its terminal `attempt-outcome`, so
    // `total = outcome_off − start` (C22: "the total equals the offset delta
    // between attempt start and terminal event"). The four canonical phases
    // partition *that* window and always sum to it bit-exactly:
    //
    //   - `executing`   — the always-present body of the attempt.
    //   - `permit-wait` — an admission wait that fell *inside* the attempt
    //     window (node-admitted at/after attempt-started); usually zero, since
    //     admission precedes the attempt.
    //   - `ready-wait`  — a readiness wait inside the window; usually zero.
    //   - `backoff`     — a retry's post-outcome backoff that fell inside the
    //     window; usually zero (backoff precedes the next attempt-started).
    //
    // ready-wait, permit-wait, and backoff measure node-level waits that
    // normally precede `attempt-started` and so lie *outside* the attempt total
    // — they contribute zero here and never a negative span, while the phase
    // vocabulary remains declared for streams that carry in-window sub-phase
    // offsets (additive, T0.10). `executing` absorbs the remainder so the four
    // phases sum to the attempt total exactly.
    let start = started_at
        .get(node)
        .copied()
        .unwrap_or(outcome_off)
        .min(outcome_off);
    let total = outcome_off.saturating_sub(start);

    // Only offsets that fall *within* [start, outcome_off] carve a phase out of
    // the attempt total; earlier per-node waits are outside it.
    let admitted_in = admitted_at
        .get(node)
        .copied()
        .filter(|&a| a > start && a <= outcome_off)
        .map_or(0, |a| a - start);
    let ready_in = ready_at
        .get(node)
        .copied()
        .filter(|&r| r > start && r <= outcome_off)
        .map_or(0, |r| r - start);
    let backoff = match prev_terminal_at.get(node) {
        Some(&prev) if attempt > 1 && prev > start && prev <= outcome_off => prev - start,
        _ => 0,
    };
    // Partition [start, outcome_off]: the pre-executing carve-outs are bounded by
    // the total, and `executing` is the remainder.
    let ready_wait = ready_in.min(total);
    let permit_wait = admitted_in.min(total.saturating_sub(ready_wait));
    let backoff = backoff.min(total.saturating_sub(ready_wait + permit_wait));
    let executing = total - ready_wait - permit_wait - backoff;

    let mut phases = BTreeMap::new();
    phases.insert(PHASE_BACKOFF.to_string(), backoff);
    phases.insert(PHASE_READY_WAIT.to_string(), ready_wait);
    phases.insert(PHASE_PERMIT_WAIT.to_string(), permit_wait);
    phases.insert(PHASE_EXECUTING.to_string(), executing);

    AttemptRecord {
        node: node.to_string(),
        attempt,
        status,
        phase_durations_ns: phases,
        worker: worker_of(rec),
        message: rec.get("message").and_then(Value::as_str).map(String::from),
        error: rec.get("error").filter(|v| !v.is_null()).cloned(),
        metrics: rec
            .get("metrics")
            .filter(|v| v.is_object())
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new())),
        cost_declared: rec.get("cost_declared").filter(|v| !v.is_null()).cloned(),
        cost_measured: rec.get("cost_measured").filter(|v| !v.is_null()).cloned(),
        durable_reference: rec
            .get("durable_reference")
            .filter(|v| !v.is_null())
            .cloned(),
        satisfied_from_run: rec
            .get("satisfied_from_run")
            .and_then(Value::as_str)
            .map(String::from),
        originating_node: rec
            .get("originating_node")
            .and_then(Value::as_str)
            .map(String::from),
    }
}

/// Synthesize a zero-elapsed attempt record for a never-ran node whose only
/// stream presence is a `node-terminal` (propagated state).
fn synthesize_never_ran(node: &str, terminal: &Value) -> AttemptRecord {
    let status = terminal
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("upstream-failed")
        .to_string();
    AttemptRecord {
        node: node.to_string(),
        attempt: 1,
        status,
        phase_durations_ns: zero_phases(),
        worker: UNKNOWN_WORKER.to_string(),
        message: terminal
            .get("message")
            .and_then(Value::as_str)
            .map(String::from),
        error: terminal.get("error").filter(|v| !v.is_null()).cloned(),
        metrics: Value::Object(serde_json::Map::new()),
        cost_declared: None,
        cost_measured: None,
        durable_reference: terminal
            .get("durable_reference")
            .filter(|v| !v.is_null())
            .cloned(),
        satisfied_from_run: terminal
            .get("satisfied_from_run")
            .and_then(Value::as_str)
            .map(String::from),
        originating_node: terminal
            .get("originating_node")
            .and_then(Value::as_str)
            .map(String::from),
    }
}

/// The four canonical phases, all zero — for a never-ran node (its phases sum to
/// its zero total).
fn zero_phases() -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    m.insert(PHASE_BACKOFF.to_string(), 0);
    m.insert(PHASE_READY_WAIT.to_string(), 0);
    m.insert(PHASE_PERMIT_WAIT.to_string(), 0);
    m.insert(PHASE_EXECUTING.to_string(), 0);
    m
}

/// The worker identity for an attempt (open-question 2): a verbatim
/// stream-supplied `worker` string, else `"<pool>#<thread>"` synthesized from
/// the `pool`/`thread` fields, else [`UNKNOWN_WORKER`].
fn worker_of(rec: &Value) -> String {
    if let Some(w) = rec.get("worker").and_then(Value::as_str) {
        return w.to_string();
    }
    let pool = rec.get("pool").and_then(Value::as_str);
    let thread = rec.get("thread").and_then(|v| {
        v.as_u64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    match (pool, thread) {
        (Some(p), Some(t)) => format!("{p}#{t}"),
        (Some(p), None) => p.to_string(),
        (None, Some(t)) => format!("{UNKNOWN_WORKER}#{t}"),
        (None, None) => UNKNOWN_WORKER.to_string(),
    }
}

/// Assemble the run summary from the fields the stream supplies (C22). The
/// dependency-aware critical path is T43; here `critical_path_ns` mirrors the
/// total elapsed as the fold's derivable lower bound.
fn assemble_summary(records: &[Value], attempts: &[AttemptRecord]) -> RunSummary {
    // Total elapsed = the maximum monotonic offset seen (run start is offset 0).
    let total_elapsed_ns = records.iter().map(offset_of).max().unwrap_or(0);

    // Peak measured slot residency: the max `slot_residency` any attempt-outcome
    // recorded.
    let peak_slot_residency = records
        .iter()
        .filter(|r| kind_of(r) == Some("attempt-outcome"))
        .filter_map(|r| r.get("slot_residency").and_then(Value::as_u64))
        .max()
        .unwrap_or(0);

    // Retained values: nodes whose latest attempt-outcome flagged `retained`.
    let mut retained: BTreeMap<String, bool> = BTreeMap::new();
    for r in records
        .iter()
        .filter(|r| kind_of(r) == Some("attempt-outcome"))
    {
        if let (Some(node), Some(flag)) = (
            r.get("node").and_then(Value::as_str),
            r.get("retained").and_then(Value::as_bool),
        ) {
            retained.insert(node.to_string(), flag);
        }
    }
    let retained_values: Vec<String> = retained
        .into_iter()
        .filter_map(|(n, f)| f.then_some(n))
        .collect();

    // Zombie-pinned time and capacity (C10/C14): for each zombie-at-exit event,
    // the pinned capacity it carries, and the pinned time between the abandoned
    // attempt's terminal (timed-out) offset and the zombie-at-exit offset.
    let terminal_off: BTreeMap<(String, u64), u64> = records
        .iter()
        .filter(|r| kind_of(r) == Some("attempt-outcome"))
        .filter_map(|r| {
            let node = r.get("node").and_then(Value::as_str)?.to_string();
            let attempt = r.get("attempt").and_then(Value::as_u64)?;
            Some(((node, attempt), offset_of(r)))
        })
        .collect();
    let mut abandoned_pinned_capacity = 0u64;
    let mut abandoned_pinned_time_ns = 0u64;
    for z in records
        .iter()
        .filter(|r| kind_of(r) == Some("zombie-at-exit"))
    {
        abandoned_pinned_capacity = abandoned_pinned_capacity.saturating_add(
            z.get("pinned_capacity")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        );
        let zoff = offset_of(z);
        if let (Some(node), Some(attempt)) = (
            z.get("node").and_then(Value::as_str),
            z.get("attempt").and_then(Value::as_u64),
        ) {
            if let Some(&toff) = terminal_off.get(&(node.to_string(), attempt)) {
                abandoned_pinned_time_ns =
                    abandoned_pinned_time_ns.saturating_add(zoff.saturating_sub(toff));
            }
        }
    }

    // Critical-path *lower bound* the fold can derive without dependency
    // structure: the longest single attempt total, capped at the run's total
    // elapsed. The true dependency-aware critical path is T43.
    let longest_attempt = attempts
        .iter()
        .map(AttemptRecord::total_elapsed_ns)
        .max()
        .unwrap_or(0);
    let critical_path_ns = longest_attempt.min(total_elapsed_ns);

    RunSummary {
        total_elapsed_ns,
        critical_path_ns,
        peak_slot_residency,
        retained_values,
        abandoned_pinned_time_ns,
        abandoned_pinned_capacity,
    }
}
