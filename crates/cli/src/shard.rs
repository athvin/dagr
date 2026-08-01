//! The **attempt shard** — one remote attempt's slice of the event stream, plus
//! everything the orchestrator needs to stamp that attempt's record without having
//! watched it run.
//!
//! # Why it is a fragment of the event stream and not a new format
//!
//! An attempt that ran somewhere else still produced exactly the records an attempt
//! that ran here produces. So the shard's body is **the same records**, in the same
//! JSON Lines shape the event-stream writer emits, for exactly one attempt. The
//! orchestrator replays them into the sink the driver injected, which keeps `seq`
//! gapless and owned by the single writer, and means the fold needs no new record
//! kind. The shard's own numbering is attempt-local and rewritten on replay.
//!
//! Around that body sit two lines the event stream has no place for:
//!
//! * a **header**, which makes the shard self-identifying — run, node, attempt, both
//!   fingerprints, tool version, image digest, and the ordered input references the
//!   attempt was actually given;
//! * a **trailer**, which carries the outcome — the terminal state, the durable
//!   output reference and its metadata, and diagnostic strings.
//!
//! Both carry their own `schema_version` (`dagr.attempt-shard@1`), never the event
//! stream's, because they are not event-stream records and must never be mistaken
//! for some.
//!
//! # Why the trailer is last, and why the write is atomic
//!
//! The shard is written **after** the output blob and **last**, so a shard's
//! existence implies its output exists. It is written to a temporary file, fsynced,
//! and renamed into place, so a reader never observes a half-written shard at all —
//! and the trailer, which names the record count, catches the case a reader gets
//! truncated bytes by some other route. A partial shard is therefore
//! [`Incomplete`](ShardError::Incomplete), never a complete one missing records.
//!
//! # Where it lives
//!
//! In the **blob store's container**, at a deterministic path derived from the
//! identity triple `(run_id, node, attempt)` — [`shard_path`]. That is the one place
//! both sides provably reach (the run store is local to the orchestrator), and a
//! deterministic address is what lets the orchestrator find the shard with no
//! callback from the pod. The node name is addressed by its **digest** rather than
//! its text, for the same reason identity is annotated rather than labelled: a node
//! name is author-chosen and need not be a legal path segment. The full name is
//! recorded inside the header.

use std::fmt;
use std::path::{Path, PathBuf};

use dagr_blob::BlobKey;

/// The shard's own schema version — deliberately **not** the event stream's, because
/// the header and trailer are not event-stream records.
pub const SHARD_SCHEMA_VERSION: &str = "dagr.attempt-shard@1";

/// The container-relative directory every shard lives under.
pub const SHARD_DIR: &str = "attempt-shards";

/// The header line's `kind`.
const KIND_HEADER: &str = "shard-header";

/// The trailer line's `kind`.
const KIND_TRAILER: &str = "shard-trailer";

// ===========================================================================
// Addressing
// ===========================================================================

/// The container-relative path of the shard for `(run, node, attempt)`.
///
/// Deterministic in all three, so the orchestrator computes the same address the pod
/// wrote to without being told — which is what makes the no-callback state path
/// (ADR 115 §3) work. Attempt and run are both in the address, so a retry never
/// overwrites its predecessor's record.
#[must_use]
pub fn shard_relative_path(run_id: &str, node: &str, attempt: u32) -> String {
    format!(
        "{SHARD_DIR}/{run_id}/{}/{attempt}.jsonl",
        BlobKey::of(node.as_bytes()).hex()
    )
}

/// [`shard_relative_path`] resolved against a blob container `root`.
#[must_use]
pub fn shard_path(root: impl AsRef<Path>, run_id: &str, node: &str, attempt: u32) -> PathBuf {
    root.as_ref()
        .join(shard_relative_path(run_id, node, attempt))
}

// ===========================================================================
// The model
// ===========================================================================

/// Who wrote this shard, and for which attempt — the facts an orchestrator checks
/// **before** replaying anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardIdentity {
    run_id: String,
    node: String,
    attempt: u32,
    structural_fingerprint: String,
    policy_hash: String,
    tool_version: String,
    image_digest: Option<String>,
}

impl ShardIdentity {
    /// Build an identity for one attempt of `node` in `run_id`.
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        node: impl Into<String>,
        attempt: u32,
        structural_fingerprint: impl Into<String>,
        policy_hash: impl Into<String>,
        tool_version: impl Into<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            node: node.into(),
            attempt,
            structural_fingerprint: structural_fingerprint.into(),
            policy_hash: policy_hash.into(),
            tool_version: tool_version.into(),
            image_digest: None,
        }
    }

    /// Record the image digest the attempt ran under, when the invoker supplied one.
    #[must_use]
    pub fn image_digest(mut self, digest: impl Into<String>) -> Self {
        self.image_digest = Some(digest.into());
        self
    }

    /// The run this attempt belongs to.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// The node this attempt ran.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The attempt number — part of the idempotency key, so try 1 and try 3 are
    /// never confused.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The writing build's structural fingerprint.
    #[must_use]
    pub fn structural_fingerprint(&self) -> &str {
        &self.structural_fingerprint
    }

    /// The writing build's policy hash.
    #[must_use]
    pub fn policy_hash(&self) -> &str {
        &self.policy_hash
    }

    /// The writing build's tool version.
    #[must_use]
    pub fn tool_version(&self) -> &str {
        &self.tool_version
    }

    /// The image digest the attempt ran under, when one was supplied.
    #[must_use]
    pub fn recorded_image_digest(&self) -> Option<&str> {
        self.image_digest.as_deref()
    }
}

/// One durable reference an attempt **was actually given**, with the content hash
/// that names its bytes.
///
/// Recorded positionally: order is load-bearing (dagr binds inputs positionally),
/// and a consume-nothing source records an **empty list, never a null**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedRef {
    uri: String,
    content_hash: Option<String>,
}

impl ConsumedRef {
    /// A consumed reference and, when known, the content hash of its bytes.
    #[must_use]
    pub fn new(uri: impl Into<String>, content_hash: Option<String>) -> Self {
        Self {
            uri: uri.into(),
            content_hash,
        }
    }

    /// The reference text, verbatim as the pod received it.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// The content hash of the referent, when recorded.
    #[must_use]
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
}

/// The durable output a succeeded attempt produced: its reference plus the
/// [`DurableReferenceMeta`](dagr_core::assembly::DurableReferenceMeta) fields the
/// orchestrator stamps onto the attempt record exactly as a local durable node's are.
///
/// The metadata's fourth field, `produced_at_offset_ns`, is deliberately **not**
/// here: an offset is measured from a run's start, and the pod's clock starts at its
/// own attempt. Carrying an attempt-local offset into the orchestrator's stream would
/// be a number that looks comparable and is not. The orchestrator stamps its own when
/// it replays, exactly as it does for a locally-produced output.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShardOutput {
    uri: String,
    content_hash: Option<String>,
    size_bytes: Option<u64>,
    scheme: Option<String>,
}

impl ShardOutput {
    /// The output stored at `uri`.
    #[must_use]
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            ..Self::default()
        }
    }

    /// Record the content hash of the stored bytes.
    #[must_use]
    pub fn content_hash(mut self, hash: impl Into<String>) -> Self {
        self.content_hash = Some(hash.into());
        self
    }

    /// Record the stored size in bytes.
    #[must_use]
    pub fn size_bytes(mut self, bytes: u64) -> Self {
        self.size_bytes = Some(bytes);
        self
    }

    /// Record the reference's scheme.
    #[must_use]
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = Some(scheme.into());
        self
    }

    /// The reference naming the stored output.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// The recorded content hash.
    #[must_use]
    pub fn recorded_content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }

    /// The recorded size.
    #[must_use]
    pub fn recorded_size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    /// The recorded scheme.
    #[must_use]
    pub fn recorded_scheme(&self) -> Option<&str> {
        self.scheme.as_deref()
    }
}

/// One attempt's shard: identity, the references it consumed, its event-stream
/// records, its terminal state, its output, and its diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptShard {
    identity: ShardIdentity,
    inputs: Vec<ConsumedRef>,
    records: Vec<serde_json::Value>,
    terminal_state: String,
    output: Option<ShardOutput>,
    diagnostics: Vec<String>,
}

impl AttemptShard {
    /// Begin a shard for `identity` whose attempt ended in `terminal_state`.
    #[must_use]
    pub fn new(identity: ShardIdentity, terminal_state: impl Into<String>) -> Self {
        Self {
            identity,
            inputs: Vec::new(),
            records: Vec::new(),
            terminal_state: terminal_state.into(),
            output: None,
            diagnostics: Vec::new(),
        }
    }

    /// Record the ordered references the attempt was given.
    #[must_use]
    pub fn with_inputs(mut self, inputs: Vec<ConsumedRef>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Record the attempt's event-stream records, in emission order.
    #[must_use]
    pub fn with_records(mut self, records: Vec<serde_json::Value>) -> Self {
        self.records = records;
        self
    }

    /// Record the durable output the attempt produced.
    #[must_use]
    pub fn with_output(mut self, output: ShardOutput) -> Self {
        self.output = Some(output);
        self
    }

    /// Record a diagnostic string — a task message, a panic payload, a platform
    /// reason. Diagnostics never change a terminal state; the taxonomy stays closed.
    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: impl Into<String>) -> Self {
        self.diagnostics.push(diagnostic.into());
        self
    }

    /// Who wrote this shard, and for which attempt.
    #[must_use]
    pub fn identity(&self) -> &ShardIdentity {
        &self.identity
    }

    /// The ordered references the attempt was given (empty for a source).
    #[must_use]
    pub fn inputs(&self) -> &[ConsumedRef] {
        &self.inputs
    }

    /// The attempt's event-stream records, in emission order — what the orchestrator
    /// replays into the driver's sink.
    #[must_use]
    pub fn records(&self) -> &[serde_json::Value] {
        &self.records
    }

    /// The attempt's terminal state, in the normative vocabulary's spelling.
    #[must_use]
    pub fn terminal_state(&self) -> &str {
        &self.terminal_state
    }

    /// The durable output, when the attempt produced one.
    #[must_use]
    pub fn output(&self) -> Option<&ShardOutput> {
        self.output.as_ref()
    }

    /// The diagnostic strings.
    #[must_use]
    pub fn diagnostics(&self) -> &[String] {
        &self.diagnostics
    }

    /// Refuse a shard written by a **different build**, naming both values.
    ///
    /// Replaying records produced by a different program is worse than refusing
    /// them: the node set may differ, the policy may differ, and the attempt may not
    /// mean what the record says. So the orchestrator checks first.
    ///
    /// # Errors
    ///
    /// [`ShardError::BuildMismatch`] naming the field, the expected value, and the
    /// value the shard carries.
    pub fn verify_build(
        &self,
        expected_structural: &str,
        expected_tool_version: &str,
    ) -> Result<(), ShardError> {
        if self.identity.structural_fingerprint != expected_structural {
            return Err(ShardError::BuildMismatch {
                field: "structural fingerprint",
                expected: expected_structural.to_string(),
                found: self.identity.structural_fingerprint.clone(),
            });
        }
        if self.identity.tool_version != expected_tool_version {
            return Err(ShardError::BuildMismatch {
                field: "tool version",
                expected: expected_tool_version.to_string(),
                found: self.identity.tool_version.clone(),
            });
        }
        Ok(())
    }

    /// Serialize to the shard's JSON Lines bytes: header, records, trailer.
    ///
    /// Every line is canonical (sorted keys, compact), so the same shard serializes
    /// to the same bytes on any machine.
    #[must_use]
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        out.push_str(&dagr_artifact::canonical::to_canonical_string(
            &self.header_value(),
        ));
        out.push('\n');
        for record in &self.records {
            out.push_str(&dagr_artifact::canonical::to_canonical_string(record));
            out.push('\n');
        }
        out.push_str(&dagr_artifact::canonical::to_canonical_string(
            &self.trailer_value(),
        ));
        out.push('\n');
        out
    }

    /// Parse shard bytes, refusing anything that is not a **complete** shard.
    ///
    /// # Errors
    ///
    /// [`ShardError::Incomplete`] when the trailer is absent or the record count
    /// disagrees with the body (the truncation case), and
    /// [`ShardError::Malformed`] when a line is not JSON or a required field is
    /// missing or of the wrong type.
    pub fn parse(bytes: &[u8]) -> Result<Self, ShardError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| ShardError::malformed(format!("the shard is not UTF-8: {e}")))?;
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.len() < 2 {
            return Err(ShardError::incomplete(format!(
                "a complete shard has a header and a trailer; found {} line(s)",
                lines.len()
            )));
        }
        let header = parse_line(lines[0], 0)?;
        require_kind(&header, KIND_HEADER)?;
        let trailer = parse_line(lines[lines.len() - 1], lines.len() - 1)
            // A truncated shard's last line is usually a partial record, not a
            // trailer: report it as incomplete rather than malformed, because that
            // is the fact a reader must act on.
            .map_err(|_| ShardError::incomplete("the shard's last line is not a trailer"))?;
        if kind_of(&trailer) != Some(KIND_TRAILER) {
            return Err(ShardError::incomplete(
                "the shard has no trailer — it was truncated or never finished",
            ));
        }

        let records: Vec<serde_json::Value> = lines[1..lines.len() - 1]
            .iter()
            .enumerate()
            .map(|(i, line)| parse_line(line, i + 1))
            .collect::<Result<_, _>>()?;
        let declared = u64_field(&trailer, "record_count")?;
        if declared != records.len() as u64 {
            return Err(ShardError::incomplete(format!(
                "the trailer declares {declared} record(s) and the shard carries {}",
                records.len()
            )));
        }

        let (identity, inputs) = parse_header(&header)?;

        let output = trailer.get("output").and_then(|value| {
            let uri = value.get("uri")?.as_str()?;
            let mut out = ShardOutput::new(uri);
            if let Some(hash) = value
                .get("content_hash")
                .and_then(serde_json::Value::as_str)
            {
                out = out.content_hash(hash);
            }
            if let Some(size) = value.get("size_bytes").and_then(serde_json::Value::as_u64) {
                out = out.size_bytes(size);
            }
            if let Some(scheme) = value.get("scheme").and_then(serde_json::Value::as_str) {
                out = out.scheme(scheme);
            }
            Some(out)
        });

        let diagnostics = trailer
            .get("diagnostics")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            identity,
            inputs,
            records,
            terminal_state: str_field(&trailer, "terminal_state")?.to_string(),
            output,
            diagnostics,
        })
    }

    /// Read the shard for `(run_id, node, attempt)` from a blob container `root`.
    ///
    /// # Errors
    ///
    /// [`ShardError::Missing`] when no shard is there (which is what an interrupted
    /// write leaves, because the write is atomic), [`ShardError::Io`] when it cannot
    /// be read, and the [`parse`](Self::parse) errors otherwise.
    pub fn read(
        root: impl AsRef<Path>,
        run_id: &str,
        node: &str,
        attempt: u32,
    ) -> Result<Self, ShardError> {
        let path = shard_path(root, run_id, node, attempt);
        match std::fs::read(&path) {
            Ok(bytes) => Self::parse(&bytes),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(ShardError::Missing { path })
            }
            Err(err) => Err(ShardError::Io {
                path,
                message: err.to_string(),
            }),
        }
    }

    /// Write this shard **atomically** into a blob container `root`, returning the
    /// path it landed at.
    ///
    /// Temp file, fsync, rename — so a reader either sees the complete shard or no
    /// shard, never a prefix. `stop_before_rename` is the fault-injection seam the
    /// atomic-write discipline is asserted through: it performs every step except
    /// the rename, which is exactly what a process killed mid-write leaves behind.
    ///
    /// # Errors
    ///
    /// The underlying [`std::io::Error`], with the path it was attempted at.
    pub fn write_atomically(
        &self,
        root: impl AsRef<Path>,
        stop_before_rename: bool,
    ) -> Result<PathBuf, ShardError> {
        use std::io::Write;

        let path = shard_path(
            root,
            self.identity.run_id(),
            self.identity.node(),
            self.identity.attempt,
        );
        let dir = path
            .parent()
            .ok_or_else(|| ShardError::Io {
                path: path.clone(),
                message: "a shard path always has a parent directory".to_string(),
            })?
            .to_path_buf();
        let io = |message: String| ShardError::Io {
            path: path.clone(),
            message,
        };
        std::fs::create_dir_all(&dir).map_err(|e| io(e.to_string()))?;

        // A hidden, pid- and counter-tagged temp name in the SAME directory, so the
        // rename is atomic (same filesystem) and concurrent writers never collide.
        let tmp = dir.join(format!(
            ".{}.tmp.{}.{}",
            self.identity.attempt,
            std::process::id(),
            next_tmp_counter()
        ));
        let bytes = self.to_jsonl();
        let write = (|| -> std::io::Result<()> {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(bytes.as_bytes())?;
            file.sync_all()?;
            if stop_before_rename {
                return Ok(());
            }
            std::fs::rename(&tmp, &path)
        })();
        if write.is_err() || stop_before_rename {
            // Best-effort cleanup: the discipline promises no debris survives a
            // failed or interrupted write.
            let _ = std::fs::remove_file(&tmp);
        }
        write.map_err(|e| io(e.to_string()))?;
        if stop_before_rename {
            return Err(ShardError::Io {
                path,
                message: "the shard write was interrupted before the rename".to_string(),
            });
        }
        if let Ok(handle) = std::fs::File::open(&dir) {
            let _ = handle.sync_all();
        }
        Ok(path)
    }

    fn header_value(&self) -> serde_json::Value {
        let mut header = serde_json::Map::new();
        header.insert("schema_version".into(), SHARD_SCHEMA_VERSION.into());
        header.insert("kind".into(), KIND_HEADER.into());
        header.insert("run_id".into(), self.identity.run_id.clone().into());
        header.insert("node".into(), self.identity.node.clone().into());
        header.insert("attempt".into(), self.identity.attempt.into());
        header.insert(
            "structural_fingerprint".into(),
            self.identity.structural_fingerprint.clone().into(),
        );
        header.insert(
            "policy_hash".into(),
            self.identity.policy_hash.clone().into(),
        );
        header.insert(
            "tool_version".into(),
            self.identity.tool_version.clone().into(),
        );
        if let Some(digest) = &self.identity.image_digest {
            header.insert("image_digest".into(), digest.clone().into());
        }
        header.insert(
            "inputs".into(),
            serde_json::Value::Array(
                self.inputs
                    .iter()
                    .map(|consumed| {
                        let mut entry = serde_json::Map::new();
                        entry.insert("uri".into(), consumed.uri.clone().into());
                        if let Some(hash) = &consumed.content_hash {
                            entry.insert("content_hash".into(), hash.clone().into());
                        }
                        serde_json::Value::Object(entry)
                    })
                    .collect(),
            ),
        );
        serde_json::Value::Object(header)
    }

    fn trailer_value(&self) -> serde_json::Value {
        let mut trailer = serde_json::Map::new();
        trailer.insert("schema_version".into(), SHARD_SCHEMA_VERSION.into());
        trailer.insert("kind".into(), KIND_TRAILER.into());
        trailer.insert("terminal_state".into(), self.terminal_state.clone().into());
        trailer.insert("record_count".into(), (self.records.len() as u64).into());
        if let Some(output) = &self.output {
            let mut value = serde_json::Map::new();
            value.insert("uri".into(), output.uri.clone().into());
            if let Some(hash) = &output.content_hash {
                value.insert("content_hash".into(), hash.clone().into());
            }
            if let Some(size) = output.size_bytes {
                value.insert("size_bytes".into(), size.into());
            }
            if let Some(scheme) = &output.scheme {
                value.insert("scheme".into(), scheme.clone().into());
            }
            trailer.insert("output".into(), serde_json::Value::Object(value));
        }
        trailer.insert(
            "diagnostics".into(),
            serde_json::Value::Array(
                self.diagnostics
                    .iter()
                    .map(|d| serde_json::Value::from(d.clone()))
                    .collect(),
            ),
        );
        serde_json::Value::Object(trailer)
    }
}

// ===========================================================================
// Attempt records, in event-stream shape
// ===========================================================================

/// Stamp an attempt's abstract [`AttemptEvent`]s into the **event-stream records**
/// the shard carries.
///
/// This is deliberately the *same* translation the driver performs for a locally-run
/// attempt — the shard's records are the local records, produced by the same code,
/// so "equivalent for the same outcome" is true by construction rather than by
/// review. The `seq` numbers are attempt-local (they start at zero for every shard)
/// and are rewritten when the orchestrator replays the records into its own
/// single-writer stream, which is what keeps that stream gapless. `offset_ns` is
/// attempt-local for the same reason and is likewise restamped on replay — and it
/// carries no less information than a local attempt's does, because the abstract
/// attempt-event port has no clock on it either: the driver stamps a local attempt's
/// records when it *drains* them, after the attempt has returned.
#[must_use]
pub fn records_for(
    run_id: &str,
    events: &[dagr_core::execution::AttemptEvent],
) -> Vec<serde_json::Value> {
    let sink = LineSink::default();
    let mut writer = dagr_artifact::event_stream::EventStreamWriter::new(
        sink.clone(),
        crate::run_store::SystemClock::new(),
        dagr_artifact::event_stream::RunId::from_operator(run_id),
        // The shard is attempt-scoped; the pipeline identity on the envelope is the
        // orchestrator's to supply when it replays, so this writer's is never read.
        String::new(),
    );
    for event in events {
        // A sink that cannot fail has no failure to report.
        let _ = crate::driver::write_attempt_event(&mut writer, event);
    }
    sink.lines()
        .iter()
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect()
}

/// An in-memory [`EventSink`](dagr_artifact::event_stream::EventSink) collecting the
/// canonical record lines the writer emits.
#[derive(Clone, Default)]
struct LineSink {
    lines: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl LineSink {
    /// The collected lines.
    ///
    /// Poison policy: recover. This buffer is written and read by one thread inside
    /// [`records_for`], so it cannot in practice be poisoned at all; recovering
    /// rather than panicking keeps a panic *elsewhere* in the attempt from also
    /// destroying the shard that is supposed to report it.
    fn lines(&self) -> Vec<Vec<u8>> {
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl dagr_artifact::event_stream::EventSink for LineSink {
    /// Poison policy: recover — the same buffer and the same reason as
    /// [`lines`](LineSink::lines).
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(line.to_vec());
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ===========================================================================
// Errors
// ===========================================================================

/// Why a shard could not be read, parsed, or trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShardError {
    /// No shard at that address. Because the write is atomic, this is what an
    /// interrupted write leaves — never a partial shard.
    Missing {
        /// The address that has no shard.
        path: PathBuf,
    },
    /// The shard is there but is **not complete**: no trailer, or fewer records than
    /// the trailer declares. Never mistaken for a complete shard.
    Incomplete {
        /// What was missing.
        reason: String,
    },
    /// The shard is complete but unreadable — a line that is not JSON, a missing or
    /// mistyped field, an unknown schema version.
    Malformed {
        /// What was wrong.
        reason: String,
    },
    /// The shard was written by a **different build**. Names both values so the
    /// refusal is actionable.
    BuildMismatch {
        /// Which identity field diverged.
        field: &'static str,
        /// What the reader expected.
        expected: String,
        /// What the shard carries.
        found: String,
    },
    /// The shard could not be read or written.
    Io {
        /// The path involved.
        path: PathBuf,
        /// The underlying message.
        message: String,
    },
}

impl ShardError {
    fn incomplete(reason: impl Into<String>) -> Self {
        Self::Incomplete {
            reason: reason.into(),
        }
    }

    fn malformed(reason: impl Into<String>) -> Self {
        Self::Malformed {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ShardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { path } => {
                write!(f, "no attempt shard at `{}`", path.display())
            }
            Self::Incomplete { reason } => {
                write!(f, "the attempt shard is incomplete: {reason}")
            }
            Self::Malformed { reason } => {
                write!(f, "the attempt shard is malformed: {reason}")
            }
            Self::BuildMismatch {
                field,
                expected,
                found,
            } => write!(
                f,
                "refusing an attempt shard from a different build: expected {field} \
                 `{expected}`, the shard carries `{found}`"
            ),
            Self::Io { path, message } => {
                write!(f, "attempt shard I/O at `{}`: {message}", path.display())
            }
        }
    }
}

impl std::error::Error for ShardError {}

// ===========================================================================
// Line helpers
// ===========================================================================

/// Parse the shard header into its identity and its ordered consumed references.
///
/// Split out of [`AttemptShard::parse`] so each half stays readable: this one is the
/// identity question ("who wrote this, for which attempt, over which references?"),
/// and what remains there is the structural one ("is this a complete shard?").
fn parse_header(
    header: &serde_json::Value,
) -> Result<(ShardIdentity, Vec<ConsumedRef>), ShardError> {
    let schema = str_field(header, "schema_version")?;
    if schema != SHARD_SCHEMA_VERSION {
        return Err(ShardError::malformed(format!(
            "this build reads `{SHARD_SCHEMA_VERSION}` shards and this one is `{schema}`"
        )));
    }

    let mut identity = ShardIdentity::new(
        str_field(header, "run_id")?,
        str_field(header, "node")?,
        u32::try_from(u64_field(header, "attempt")?)
            .map_err(|_| ShardError::malformed("the attempt number does not fit in u32"))?,
        str_field(header, "structural_fingerprint")?,
        str_field(header, "policy_hash")?,
        str_field(header, "tool_version")?,
    );
    if let Some(digest) = header
        .get("image_digest")
        .and_then(serde_json::Value::as_str)
    {
        identity = identity.image_digest(digest);
    }

    let inputs = header
        .get("inputs")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ShardError::malformed("the header carries no `inputs` array"))?
        .iter()
        .map(|entry| {
            let uri = entry
                .get("uri")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ShardError::malformed("an input entry carries no `uri`"))?;
            Ok(ConsumedRef::new(
                uri,
                entry
                    .get("content_hash")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ))
        })
        .collect::<Result<Vec<_>, ShardError>>()?;
    Ok((identity, inputs))
}

fn parse_line(line: &str, index: usize) -> Result<serde_json::Value, ShardError> {
    serde_json::from_str(line)
        .map_err(|e| ShardError::malformed(format!("line {index} is not JSON: {e}")))
}

fn kind_of(value: &serde_json::Value) -> Option<&str> {
    value.get("kind").and_then(serde_json::Value::as_str)
}

fn require_kind(value: &serde_json::Value, expected: &str) -> Result<(), ShardError> {
    match kind_of(value) {
        Some(kind) if kind == expected => Ok(()),
        other => Err(ShardError::malformed(format!(
            "expected a `{expected}` line, found `{}`",
            other.unwrap_or("<no kind>")
        ))),
    }
}

fn str_field<'v>(value: &'v serde_json::Value, field: &str) -> Result<&'v str, ShardError> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ShardError::malformed(format!("`{field}` is missing or not a string")))
}

fn u64_field(value: &serde_json::Value, field: &str) -> Result<u64, ShardError> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ShardError::malformed(format!("`{field}` is missing or not an integer")))
}

/// A process-monotonic counter, so two shard writes in one process never pick the
/// same temp name (mirroring the scratch store's and the blob backend's discipline).
fn next_tmp_counter() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{AttemptShard, ConsumedRef, ShardError, ShardIdentity, ShardOutput};

    fn shard() -> AttemptShard {
        AttemptShard::new(
            ShardIdentity::new("run-1", "node/with slash", 3, "fp-s", "fp-p", "dagr@1")
                .image_digest("sha256:beef"),
            "succeeded",
        )
        .with_inputs(vec![ConsumedRef::new(
            "dagr-blob+file:///r/sha256/aa",
            None,
        )])
        .with_records(vec![serde_json::json!({"kind": "attempt-started"})])
        .with_output(ShardOutput::new("dagr-blob+file:///r/sha256/bb").size_bytes(4))
        .with_diagnostic("all good")
    }

    #[test]
    fn a_shard_round_trips_through_its_own_bytes() {
        let original = shard();
        let parsed = AttemptShard::parse(original.to_jsonl().as_bytes()).expect("round trip");
        assert_eq!(parsed, original);
    }

    #[test]
    fn a_truncated_shard_is_incomplete_never_complete() {
        let bytes = shard().to_jsonl();
        let cut = &bytes.as_bytes()[..bytes.len() - 20];
        assert!(matches!(
            AttemptShard::parse(cut),
            Err(ShardError::Incomplete { .. })
        ));
    }

    #[test]
    fn a_foreign_build_is_refused_naming_both_values() {
        let err = shard()
            .verify_build("other-fp", "dagr@1")
            .expect_err("a foreign fingerprint is refused");
        let message = err.to_string();
        assert!(
            message.contains("other-fp") && message.contains("fp-s"),
            "{message}"
        );
    }

    #[test]
    fn the_address_is_deterministic_in_all_three_of_run_node_and_attempt() {
        let one = super::shard_relative_path("r", "n", 1);
        assert_eq!(one, super::shard_relative_path("r", "n", 1));
        assert_ne!(one, super::shard_relative_path("r", "n", 2));
        assert_ne!(one, super::shard_relative_path("r2", "n", 1));
        assert_ne!(one, super::shard_relative_path("r", "n2", 1));
        // A node name that is not a legal path segment still addresses cleanly.
        assert!(!super::shard_relative_path("r", "a/b c", 1).contains("a/b c"));
    }
}
