//! The T39 (ticket 050) published-artifact-schema validation helper.
//!
//! This module is the shared validation helper the rest of M3 leans on
//! (arch.md C19 event stream, C20 graph artifact, C22 run artifact). It
//! validates a candidate artifact or event-stream record against its
//! **published, versioned JSON Schema** and returns an actionable error naming
//! the artifact and the reason on failure. It is used by the emitters (T40
//! graph, T42 fold) and the compatibility CI (T48), so those target one
//! authoritative definition rather than inventing their own.
//!
//! # Where the schemas live (T4 §5)
//!
//! The published schemas are checked in at the repo-root path the T4 ADR (017)
//! fixed: `schemas/<kind>/v<version>.schema.json` — one frozen file per artifact
//! kind per released schema version. A version bump adds a new file beside the
//! old one, which is never edited, so old readers keep validating old artifacts
//! (C22 "prior-version fixtures remain parseable forever"). The fixture corpus
//! (T0.10) sits at `tests/fixtures/corpus/<kind>/v<version>/`.
//!
//! # Evolution posture (T0.10)
//!
//! Each family carries its own `schema_version` (`dagr.<kind>@<major>`, T4 §3).
//! Within a version, evolution is **additive-only**: no published schema sets
//! `additionalProperties: false` on an evolving object, so an unknown future
//! field validates (the reader ignores it) and a missing additively-introduced
//! field is defaulted by the reader (T0.10 / Stability). The published `run`
//! schema records that the fold reader (T42) declares which stream schema
//! versions it reads (C22) — that declaration is T42's; this helper validates.
//!
//! # Dependency posture (T4 §4)
//!
//! Validation uses the `jsonschema` crate (draft 2020-12), which the T4 ADR
//! scopes to **CI/tests only**, not the runtime. This whole module is therefore
//! behind the `schema-validation` cargo feature (default OFF); the runtime
//! writers (T19/T40/T42) never pull `jsonschema`. `dagr-core` stays entirely
//! dependency-free — schemas and validation live here, in `dagr-artifact`.
//!
//! # Scope (T39)
//!
//! This module **publishes and validates** the schemas. It emits nothing (T40),
//! folds nothing (T42), computes no fingerprint (T41), and seeds no scale corpus
//! (T48) — those are named in the ticket's Out of scope.

#![cfg(feature = "schema-validation")]

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// The repo-root directory holding the published JSON Schema documents (T4 §5),
/// relative to the workspace root: `schemas/`.
pub const SCHEMA_DIR: &str = "schemas";

/// The repo-root directory holding the fixture corpus (T0.10), relative to the
/// workspace root: `tests/fixtures/corpus/`.
pub const CORPUS_DIR: &str = "tests/fixtures/corpus";

/// The three durable artifact families dagr publishes a schema for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactKind {
    /// The C19 event stream (`dagr.event-stream@N`).
    EventStream,
    /// The C20 graph artifact (`dagr.graph@N`).
    Graph,
    /// The C22 run artifact (`dagr.run@N`).
    Run,
}

impl ArtifactKind {
    /// Every published artifact family, for corpus-wide iteration.
    pub const ALL: [ArtifactKind; 3] = [
        ArtifactKind::EventStream,
        ArtifactKind::Graph,
        ArtifactKind::Run,
    ];

    /// The on-disk directory segment for this kind under `schemas/` and
    /// `tests/fixtures/corpus/` (T4 §5).
    #[must_use]
    pub fn dir_name(self) -> &'static str {
        match self {
            ArtifactKind::EventStream => "event-stream",
            ArtifactKind::Graph => "graph",
            ArtifactKind::Run => "run",
        }
    }

    /// The `schema_version` name for this kind (T4 §3), e.g. `dagr.graph`.
    #[must_use]
    pub fn schema_name(self) -> &'static str {
        match self {
            ArtifactKind::EventStream => "dagr.event-stream",
            ArtifactKind::Graph => "dagr.graph",
            ArtifactKind::Run => "dagr.run",
        }
    }

    /// The full `schema_version` string a `version`-numbered artifact of this
    /// kind carries, e.g. `dagr.run@1` (T4 §3).
    #[must_use]
    pub fn schema_version_string(self, version: u32) -> String {
        format!("{}@{version}", self.schema_name())
    }
}

/// The workspace-root path (the directory that contains `schemas/`), resolved
/// from this crate's manifest directory (`crates/artifact` → two levels up).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(Path::parent) // <workspace root>
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// The published-schema file path for `kind`@`version` (T4 §5):
/// `schemas/<kind>/v<version>.schema.json`.
#[must_use]
pub fn schema_path(kind: ArtifactKind, version: u32) -> PathBuf {
    workspace_root()
        .join(SCHEMA_DIR)
        .join(kind.dir_name())
        .join(format!("v{version}.schema.json"))
}

/// The released schema versions published for `kind`, ascending — discovered by
/// scanning `schemas/<kind>/` for `v<N>.schema.json` files. Empty if the family
/// directory is missing.
#[must_use]
pub fn published_schema_versions(kind: ArtifactKind) -> Vec<u32> {
    let dir = workspace_root().join(SCHEMA_DIR).join(kind.dir_name());
    let mut versions: Vec<u32> = fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let stem = name.strip_prefix('v')?.strip_suffix(".schema.json")?;
            stem.parse::<u32>().ok()
        })
        .collect();
    versions.sort_unstable();
    versions
}

/// The outcome of a failed schema validation, naming the artifact and the
/// reason so tests and CI can assert on it (T39 "returns an actionable error
/// identifying the artifact and reason on failure").
#[derive(Debug)]
pub struct SchemaValidationError {
    /// Human-readable identity of the artifact that failed (kind + version, and
    /// the source path when validating a corpus fixture).
    artifact: String,
    /// The failing reason (the JSON-pointer instance path and the validator's
    /// message), or an I/O / schema-load reason.
    reason: String,
}

impl SchemaValidationError {
    /// The artifact identity this error names (kind@version, plus a fixture path
    /// when applicable).
    #[must_use]
    pub fn artifact(&self) -> &str {
        &self.artifact
    }

    /// The failing reason (instance path + validator message, or a load error).
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed validation: {}", self.artifact, self.reason)
    }
}

impl std::error::Error for SchemaValidationError {}

/// Load and compile the published schema for `kind`@`version`.
fn load_validator(
    kind: ArtifactKind,
    version: u32,
    artifact_id: &str,
) -> Result<jsonschema::Validator, SchemaValidationError> {
    let path = schema_path(kind, version);
    let bytes = fs::read(&path).map_err(|e| SchemaValidationError {
        artifact: artifact_id.to_string(),
        reason: format!("cannot read published schema {}: {e}", path.display()),
    })?;
    let schema: Value = serde_json::from_slice(&bytes).map_err(|e| SchemaValidationError {
        artifact: artifact_id.to_string(),
        reason: format!("published schema {} is not valid JSON: {e}", path.display()),
    })?;
    jsonschema::validator_for(&schema).map_err(|e| SchemaValidationError {
        artifact: artifact_id.to_string(),
        reason: format!("published schema {} does not compile: {e}", path.display()),
    })
}

/// Validate a candidate `instance` against the published schema for
/// `kind`@`version`.
///
/// Returns `Ok(())` when the instance validates, or a [`SchemaValidationError`]
/// naming the artifact (`dagr.<kind>@<version>`) and the failing reason (the
/// JSON-pointer instance path and the validator's message) on failure — the
/// actionable error T40/T42/T48 assert on.
///
/// # Errors
///
/// Returns [`SchemaValidationError`] if the instance does not conform, or if the
/// published schema cannot be read/parsed/compiled.
pub fn validate_value(
    kind: ArtifactKind,
    version: u32,
    instance: &Value,
) -> Result<(), SchemaValidationError> {
    let artifact_id = kind.schema_version_string(version);
    validate_named(kind, version, instance, &artifact_id)
}

/// Validate `instance`, attributing any failure to `artifact_id` (used by the
/// corpus walker to name the offending fixture file).
fn validate_named(
    kind: ArtifactKind,
    version: u32,
    instance: &Value,
    artifact_id: &str,
) -> Result<(), SchemaValidationError> {
    let validator = load_validator(kind, version, artifact_id)?;
    if let Err(error) = validator.validate(instance) {
        return Err(SchemaValidationError {
            artifact: artifact_id.to_string(),
            reason: format!("at `{}`: {}", error.instance_path(), error),
        });
    }
    Ok(())
}

/// Parse `bytes` as JSON and [`validate_value`] the result — the entry point a
/// CI step or a caller with raw bytes (a single event-stream record line, an
/// artifact file) uses.
///
/// # Errors
///
/// Returns [`SchemaValidationError`] if the bytes are not valid JSON, or if the
/// parsed value fails validation.
pub fn validate_bytes(
    kind: ArtifactKind,
    version: u32,
    bytes: &[u8],
) -> Result<(), SchemaValidationError> {
    let artifact_id = kind.schema_version_string(version);
    let value: Value = serde_json::from_slice(bytes).map_err(|e| SchemaValidationError {
        artifact: artifact_id.clone(),
        reason: format!("input is not valid JSON: {e}"),
    })?;
    validate_named(kind, version, &value, &artifact_id)
}

/// Validate every checked-in corpus fixture against its declared version's
/// published schema (T0.10 / Stability: "a corpus parsed in CI forever after").
///
/// Walks `tests/fixtures/corpus/<kind>/v<version>/*.json`, validating each file
/// against `schemas/<kind>/v<version>.schema.json`. On the first failure it
/// returns an error **naming the offending fixture file and the reason** so the
/// CI step fails loudly. Returns `Ok(())` only if every corpus fixture
/// validates.
///
/// This is the library half of the CI validation step T39 ships; T48 owns the
/// enduring compatibility gate and the ten-thousand-attempt scale artifact
/// (both named in the ticket's Out of scope) — this walker is the seed.
///
/// # Errors
///
/// Returns [`SchemaValidationError`] naming the first fixture that fails to read,
/// parse, or validate.
pub fn check_corpus() -> Result<(), SchemaValidationError> {
    let root = workspace_root().join(CORPUS_DIR);
    for kind in ArtifactKind::ALL {
        for version in published_schema_versions(kind) {
            let dir = root.join(kind.dir_name()).join(format!("v{version}"));
            // No fixtures for this version is not a failure here; the
            // "at least one per released version" completeness check is a
            // separate assertion in the test suite.
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            let mut files: Vec<PathBuf> = entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
                .collect();
            files.sort();
            for file in files {
                let artifact_id = format!(
                    "{} fixture {}",
                    kind.schema_version_string(version),
                    file.display()
                );
                let bytes = fs::read(&file).map_err(|e| SchemaValidationError {
                    artifact: artifact_id.clone(),
                    reason: format!("cannot read corpus fixture: {e}"),
                })?;
                let value: Value =
                    serde_json::from_slice(&bytes).map_err(|e| SchemaValidationError {
                        artifact: artifact_id.clone(),
                        reason: format!("corpus fixture is not valid JSON: {e}"),
                    })?;
                validate_named(kind, version, &value, &artifact_id)?;
            }
        }
    }
    Ok(())
}
