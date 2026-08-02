//! **Intermediate-blob garbage collection, by reachability.**
//!
//! Every remote node attempt writes its output to the blob store. Until this
//! module existed, nothing ever removed one: a pipeline run nightly leaked its
//! whole intermediate set every night, forever. `prune` (C26) already owns
//! retention — it deletes run-store directories and scratch — so it is where this
//! belongs, rather than in a second retention verb that would be a second place to
//! get wrong.
//!
//! # Why the criterion is reachability and can never be age
//!
//! A blob's key is the digest of its bytes and nothing else, so **the same value
//! produced by two runs is one blob**. Reclaiming "the blobs of runs older than
//! T" would therefore delete a blob a *newer* run still references — including
//! one that run's resume needs to rehydrate. There is no run directory to walk,
//! either: content addressing means a blob belongs to no run in particular.
//!
//! So a blob is reclaimable exactly when **no retained run artifact references
//! it**, and the reclaim is a set difference: everything the store holds, minus
//! everything the artifacts still under the run store point at.
//!
//! # The asymmetry that shapes every judgement here
//!
//! Getting reachability wrong is not symmetric. Missing a reference deletes a
//! blob a run still needs — silent, permanent data loss whose first symptom is a
//! resume refused months later. Counting something as reachable that is not
//! merely keeps a dead blob until the next prune. Every uncertain case therefore
//! resolves toward *keeping*:
//!
//! * reference extraction walks the **whole** artifact document and takes every
//!   string that parses as a blob reference, rather than reading the three fields
//!   that carry one today. A schema that grows a fourth cannot silently make live
//!   blobs collectable.
//! * an artifact that cannot be **read** is a refusal, not a zero-reference
//!   artifact. Unknown reachability is not an excuse to guess.
//! * a run directory with an event stream and **no folded artifact** is a refusal
//!   too. Its references are in the stream, so `fold` makes them readable — and
//!   until it runs, that run's blobs look unreferenced when they are not.
//! * a reference naming a **different container** contributes nothing and is not
//!   evidence about this one.
//!
//! # Safe by default
//!
//! The reclaim is opt-in twice over: `prune` touches no blob without
//! `--reclaim-blobs`, and `--reclaim-blobs dry-run` lists exactly what
//! `--reclaim-blobs delete` would remove without removing any of it. The listing
//! is emitted by the same code path in both modes, so "the dry run matched" is
//! structural rather than a promise.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};

use dagr_artifact::event_stream::EVENTS_FILE_NAME;
use dagr_blob::{BlobKey, BlobReclaim, BlobRef, BlobStore, LocalFsBlob};

use crate::contract::ExitCode;
use crate::run_store::{DEFAULT_STORE_BASE, RUN_ARTIFACT_FILE_NAME};

/// The flag that opts a `prune` invocation into touching the blob store at all.
pub const RECLAIM_BLOBS_FLAG: &str = "--reclaim-blobs";

/// The prefix a `--blob-store` value carries to name an object-store container
/// rather than a filesystem root.
pub const OBJECT_STORE_SCHEME: &str = "s3://";

/// The line prefix every reclaimable blob is listed under, **identical in both
/// modes** so a dry run and a real one are comparable line for line.
const LISTING_PREFIX: &str = "blob-reclaim ";

// ===========================================================================
// The plan.
// ===========================================================================

/// What a reclaim would remove, and the evidence it was computed from.
///
/// Producing a plan performs no deletion. It is the dry run, and applying it is
/// the only thing that removes anything.
#[derive(Debug, Clone)]
pub struct ReclaimPlan {
    container: String,
    artifacts: Vec<PathBuf>,
    reachable: BTreeSet<String>,
    reclaimable: Vec<BlobKey>,
}

impl ReclaimPlan {
    /// The container this plan is about.
    #[must_use]
    pub fn container(&self) -> &str {
        &self.container
    }

    /// Every run artifact that was read to compute reachability.
    #[must_use]
    pub fn artifacts_read(&self) -> &[PathBuf] {
        &self.artifacts
    }

    /// How many distinct blobs in this container a retained artifact references.
    #[must_use]
    pub fn reachable_count(&self) -> usize {
        self.reachable.len()
    }

    /// The blobs no retained artifact references, sorted — the reclaim set.
    #[must_use]
    pub fn reclaimable(&self) -> &[BlobKey] {
        &self.reclaimable
    }

    /// Write the listing: one line per reclaimable blob, plus a summary.
    ///
    /// The per-blob lines are byte-identical between a dry run and a real one; the
    /// summary line is what differs, because it is the one that says whether
    /// anything was deleted.
    pub fn render<W: Write>(&self, out: &mut W, deleted: bool) {
        for key in &self.reclaimable {
            let _ = writeln!(out, "{LISTING_PREFIX}{key}");
        }
        let _ = writeln!(
            out,
            "prune: {} blob(s) reclaimable in `{}`; {} reachable from {} retained run artifact(s); {}",
            self.reclaimable.len(),
            self.container,
            self.reachable.len(),
            self.artifacts.len(),
            if deleted {
                "deleted"
            } else {
                "dry run — nothing was deleted (re-run with `--reclaim-blobs delete`)"
            }
        );
    }
}

/// Why a reclaim refused to run.
///
/// Every variant is a case where reachability is **unknown**. None of them is a
/// reason to delete cautiously or to delete a subset: a reaper that half-knows
/// which blobs are live is a reaper that deletes live blobs.
#[derive(Debug)]
pub enum ReclaimRefusal {
    /// The run store base does not exist. Almost always a mistyped `--store`, and
    /// treating it as "no artifacts" would make every blob in the container look
    /// unreferenced.
    MissingStoreBase {
        /// The base that was not there.
        path: PathBuf,
    },
    /// A run artifact exists and could not be read or parsed.
    UnreadableArtifact {
        /// The artifact.
        path: PathBuf,
        /// Why it could not be read.
        reason: String,
    },
    /// A run directory has an event stream but no folded run artifact: a crashed
    /// run whose references have not been folded out of the stream yet.
    UnfoldedRun {
        /// The run directory.
        path: PathBuf,
    },
    /// The blob container could not be enumerated.
    StoreUnreadable {
        /// The container.
        container: String,
        /// Why it could not be enumerated.
        reason: String,
    },
    /// A blob could not be removed once the plan was applied.
    DeleteFailed {
        /// The blob.
        key: BlobKey,
        /// Why.
        reason: String,
    },
    /// `--blob-store` named a backend this build has no client for.
    UnsupportedBackend {
        /// What was named.
        container: String,
        /// The feature that would supply it.
        feature: &'static str,
    },
}

impl fmt::Display for ReclaimRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingStoreBase { path } => write!(
                f,
                "the run store base `{}` does not exist, so no run artifact could be read. \
                 Reclaiming against it would treat every blob as unreferenced — check `--store`",
                path.display()
            ),
            Self::UnreadableArtifact { path, reason } => write!(
                f,
                "the run artifact `{}` could not be read ({reason}), so which blobs it \
                 references is unknown. Nothing was reclaimed: unknown reachability is not an \
                 excuse to guess",
                path.display()
            ),
            Self::UnfoldedRun { path } => write!(
                f,
                "the run directory `{}` has an event stream but no `{RUN_ARTIFACT_FILE_NAME}`, \
                 so its durable references are still only in the stream. Run `fold` on it \
                 first; until then its blobs would look unreferenced when they are not",
                path.display()
            ),
            Self::StoreUnreadable { container, reason } => write!(
                f,
                "the blob container `{container}` could not be enumerated ({reason}). Nothing \
                 was reclaimed: an empty listing from a store that could not be read is \
                 indistinguishable from an empty store"
            ),
            Self::DeleteFailed { key, reason } => write!(
                f,
                "`{key}` could not be reclaimed ({reason}). The reclaim stopped there; the \
                 blobs it had already removed were unreferenced and re-running is safe"
            ),
            Self::UnsupportedBackend { container, feature } => write!(
                f,
                "`{container}` names an object store, and this build has no client for one — \
                 rebuild `dagr-cli` with the `{feature}` feature"
            ),
        }
    }
}

impl std::error::Error for ReclaimRefusal {}

// ===========================================================================
// Planning.
// ===========================================================================

/// Compute what is reclaimable in `store`, given the run artifacts retained under
/// `store_base`.
///
/// Deletes nothing.
///
/// # Errors
///
/// [`ReclaimRefusal`] whenever reachability could not be established completely —
/// a missing base, an unreadable artifact, an unfolded run, or a container that
/// could not be enumerated.
pub fn plan_reclaim<S: BlobStore + BlobReclaim + ?Sized>(
    store_base: &Path,
    store: &S,
) -> Result<ReclaimPlan, ReclaimRefusal> {
    if !store_base.is_dir() {
        return Err(ReclaimRefusal::MissingStoreBase {
            path: store_base.to_path_buf(),
        });
    }

    let backend = store.backend().to_string();
    let container = store.container();
    let mut artifacts = Vec::new();
    let mut reachable = BTreeSet::new();
    walk_run_store(
        store_base,
        &backend,
        &container,
        &mut artifacts,
        &mut reachable,
    )?;

    let held = store
        .list()
        .map_err(|err| ReclaimRefusal::StoreUnreadable {
            container: container.clone(),
            reason: err.to_string(),
        })?;
    let mut reclaimable: Vec<BlobKey> = held
        .into_iter()
        .filter(|key| !reachable.contains(key.hex()))
        .collect();
    reclaimable.sort();
    artifacts.sort();

    Ok(ReclaimPlan {
        container,
        artifacts,
        reachable,
        reclaimable,
    })
}

/// Delete every blob `plan` found reclaimable, returning what was removed.
///
/// # Errors
///
/// [`ReclaimRefusal::DeleteFailed`] naming the first blob that could not be
/// removed. Deletion is idempotent per blob, so re-running after a failure is
/// safe.
pub fn apply_reclaim<S: BlobReclaim + ?Sized>(
    store: &S,
    plan: &ReclaimPlan,
) -> Result<Vec<BlobKey>, ReclaimRefusal> {
    let mut deleted = Vec::with_capacity(plan.reclaimable.len());
    for key in &plan.reclaimable {
        store
            .delete(key)
            .map_err(|err| ReclaimRefusal::DeleteFailed {
                key: key.clone(),
                reason: err.to_string(),
            })?;
        deleted.push(key.clone());
    }
    Ok(deleted)
}

/// Walk `<base>/**` for run directories, reading each retained artifact.
///
/// A **run directory** is one holding a run artifact or an event stream. Anything
/// else — the pipeline directories above them, a scratch subtree, an operator's
/// notes — is descended through and otherwise ignored.
fn walk_run_store(
    dir: &Path,
    backend: &str,
    container: &str,
    artifacts: &mut Vec<PathBuf>,
    reachable: &mut BTreeSet<String>,
) -> Result<(), ReclaimRefusal> {
    let artifact = dir.join(RUN_ARTIFACT_FILE_NAME);
    if artifact.is_file() {
        let text = std::fs::read_to_string(&artifact).map_err(|err| {
            ReclaimRefusal::UnreadableArtifact {
                path: artifact.clone(),
                reason: err.to_string(),
            }
        })?;
        let value: serde_json::Value =
            serde_json::from_str(&text).map_err(|err| ReclaimRefusal::UnreadableArtifact {
                path: artifact.clone(),
                reason: err.to_string(),
            })?;
        collect_references(&value, backend, container, reachable);
        artifacts.push(artifact);
        // A run directory holds no run directories, so there is nothing below it
        // to walk — and its `scratch/` subtree must not be mistaken for one.
        return Ok(());
    }
    if dir.join(EVENTS_FILE_NAME).is_file() {
        return Err(ReclaimRefusal::UnfoldedRun {
            path: dir.to_path_buf(),
        });
    }

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            return Err(ReclaimRefusal::UnreadableArtifact {
                path: dir.to_path_buf(),
                reason: err.to_string(),
            });
        }
    };
    let mut children: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        if entry.path().is_dir() {
            children.push(entry.path());
        }
    }
    children.sort();
    for child in children {
        walk_run_store(&child, backend, container, artifacts, reachable)?;
    }
    Ok(())
}

/// Collect every blob reference in `value` that names this store's container.
///
/// It walks the **whole document** rather than the three fields that carry a
/// reference today (`outputs[].uri`, `attempts[].durable_reference`,
/// `attempts[].inputs[].uri`). That is deliberate, and it is the asymmetry
/// argument in code: reading a fixed field list means a schema that grows a fourth
/// place silently turns live blobs into garbage, while over-collecting only ever
/// retains a blob one prune longer.
fn collect_references(
    value: &serde_json::Value,
    backend: &str,
    container: &str,
    reachable: &mut BTreeSet<String>,
) {
    match value {
        serde_json::Value::String(text) => {
            if let Ok(reference) = BlobRef::parse(text)
                && reference.backend() == backend
                && reference.container() == container
            {
                reachable.insert(reference.key().hex().to_string());
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_references(item, backend, container, reachable);
            }
        }
        serde_json::Value::Object(fields) => {
            for field in fields.values() {
                collect_references(field, backend, container, reachable);
            }
        }
        _ => {}
    }
}

// ===========================================================================
// The verb body.
// ===========================================================================

/// What `--reclaim-blobs` was asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimMode {
    /// List what would be reclaimed and delete nothing.
    DryRun,
    /// Delete what the same listing names.
    Delete,
}

impl ReclaimMode {
    /// Parse the flag's value. Unrecognized values are refused rather than
    /// defaulted — the safe default is already "no flag at all", and a typo that
    /// resolved to `delete` would be unrecoverable.
    fn parse(value: &str) -> Option<Self> {
        match value {
            "dry-run" => Some(Self::DryRun),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

/// `prune`'s **blob half**: reclaim intermediate blobs by reachability.
///
/// Reads `--store <base>` (the run store whose retained artifacts define
/// reachability), `--blob-store <container>` (the container to reclaim from), and
/// `--reclaim-blobs <dry-run|delete>`.
///
/// With no `--reclaim-blobs`, it does nothing at all and exits `Success` — a bare
/// `prune` must be exactly as it was, and touching a blob store because a flag was
/// *absent* would be the opposite of opt-in.
///
/// The pipeline-specific `prune` body calls this after its own run-directory
/// retention, so reachability is computed against what retention decided to keep.
#[must_use]
pub fn reclaim_blobs_verb<W: Write>(argv: &[OsString], out: &mut W) -> ExitCode {
    let Some(raw_mode) = flag_value(argv, RECLAIM_BLOBS_FLAG) else {
        return ExitCode::Success;
    };
    let Some(mode) = ReclaimMode::parse(&raw_mode) else {
        let _ = writeln!(
            out,
            "dagr prune: `{RECLAIM_BLOBS_FLAG} {raw_mode}` is not a reclaim mode — use \
             `dry-run` (list what would be reclaimed) or `delete` (reclaim it). Nothing was \
             deleted"
        );
        return ExitCode::InvalidUsage;
    };
    let Some(container) = flag_value(argv, "--blob-store") else {
        let _ = writeln!(
            out,
            "dagr prune: `{RECLAIM_BLOBS_FLAG}` needs `--blob-store <container>` — the run \
             store says which blobs are still referenced, but not where they are"
        );
        return ExitCode::InvalidUsage;
    };
    let base = flag_value(argv, "--store").unwrap_or_else(|| DEFAULT_STORE_BASE.to_string());

    match run_reclaim(Path::new(&base), &container, mode, out) {
        Ok(code) => code,
        Err(refusal) => {
            let _ = writeln!(out, "dagr prune: {refusal}");
            // The verb checked its preconditions and refused before acting, which
            // is what the bootstrap-failure code means. No new number is minted:
            // the C26 exit-code table is closed.
            ExitCode::BootstrapFailure
        }
    }
}

/// Open the named container and run the plan (and, in `Delete` mode, apply it).
fn run_reclaim<W: Write>(
    base: &Path,
    container: &str,
    mode: ReclaimMode,
    out: &mut W,
) -> Result<ExitCode, ReclaimRefusal> {
    if let Some(rest) = container.strip_prefix(OBJECT_STORE_SCHEME) {
        return object_store_reclaim(base, rest, mode, out);
    }
    let store = LocalFsBlob::open(container);
    let plan = plan_reclaim(base, &store)?;
    finish(&store, &plan, mode, out)
}

/// The object-store arm, which exists only when a client was compiled in.
#[cfg(feature = "blob-s3")]
fn object_store_reclaim<W: Write>(
    base: &Path,
    container: &str,
    mode: ReclaimMode,
    out: &mut W,
) -> Result<ExitCode, ReclaimRefusal> {
    let store = crate::blob_s3::open_ambient(container).map_err(|reason| {
        ReclaimRefusal::StoreUnreadable {
            container: container.to_string(),
            reason,
        }
    })?;
    let plan = plan_reclaim(base, &store)?;
    finish(&store, &plan, mode, out)
}

/// Without the client feature the arm refuses by name rather than pretending the
/// container is a directory — which is what would happen if `s3://…` fell through
/// to the filesystem backend.
#[cfg(not(feature = "blob-s3"))]
fn object_store_reclaim<W: Write>(
    _base: &Path,
    container: &str,
    _mode: ReclaimMode,
    _out: &mut W,
) -> Result<ExitCode, ReclaimRefusal> {
    Err(ReclaimRefusal::UnsupportedBackend {
        container: format!("{OBJECT_STORE_SCHEME}{container}"),
        feature: "blob-s3",
    })
}

/// Render the listing and, in `Delete` mode, apply the plan.
fn finish<S: BlobStore + BlobReclaim + ?Sized, W: Write>(
    store: &S,
    plan: &ReclaimPlan,
    mode: ReclaimMode,
    out: &mut W,
) -> Result<ExitCode, ReclaimRefusal> {
    match mode {
        ReclaimMode::DryRun => plan.render(out, false),
        ReclaimMode::Delete => {
            apply_reclaim(store, plan)?;
            plan.render(out, true);
        }
    }
    Ok(ExitCode::Success)
}

/// Read a value-taking flag out of a raw invocation, in both the `--flag=value`
/// and `--flag value` grammars.
fn flag_value(argv: &[OsString], flag: &str) -> Option<String> {
    let prefix = format!("{flag}=");
    let mut remaining = argv.iter();
    while let Some(arg) = remaining.next() {
        let text = arg.to_string_lossy();
        if let Some(value) = text.strip_prefix(&prefix) {
            return Some(value.to_string());
        }
        if text == flag {
            return remaining.next().map(|v| v.to_string_lossy().into_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{ReclaimMode, collect_references, flag_value};
    use std::collections::BTreeSet;
    use std::ffi::OsString;

    fn argv(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn both_flag_grammars_are_accepted() {
        assert_eq!(
            flag_value(&argv(&["--reclaim-blobs", "delete"]), "--reclaim-blobs"),
            Some("delete".to_string())
        );
        assert_eq!(
            flag_value(&argv(&["--reclaim-blobs=dry-run"]), "--reclaim-blobs"),
            Some("dry-run".to_string())
        );
        assert_eq!(
            flag_value(&argv(&["--store", "b"]), "--reclaim-blobs"),
            None
        );
    }

    #[test]
    fn only_the_two_documented_modes_parse() {
        assert_eq!(ReclaimMode::parse("dry-run"), Some(ReclaimMode::DryRun));
        assert_eq!(ReclaimMode::parse("delete"), Some(ReclaimMode::Delete));
        for typo in ["", "Delete", "yes", "true", "dryrun"] {
            assert!(
                ReclaimMode::parse(typo).is_none(),
                "`{typo}` must not resolve to a mode"
            );
        }
    }

    /// The whole-document walk finds a reference wherever it is recorded, and
    /// ignores one naming another container.
    #[test]
    fn references_are_collected_from_anywhere_in_the_document() {
        let value = serde_json::json!({
            "outputs": [{ "uri": "dagr-blob+file:///c/sha256/aa" }],
            "attempts": [{
                "durable_reference": "dagr-blob+file:///c/sha256/bb",
                "inputs": [{ "uri": "dagr-blob+file:///c/sha256/cc" }],
                "nested": { "somewhere_new": "dagr-blob+file:///c/sha256/dd" },
            }],
            "elsewhere": "dagr-blob+file:///other/sha256/ee",
            "not_a_reference": "just a string",
        });
        // 64-hex is required by the grammar, so build real digests.
        let text = serde_json::to_string(&value).expect("json");
        let text =
            ["aa", "bb", "cc", "dd", "ee"]
                .iter()
                .enumerate()
                .fold(text, |acc, (i, short)| {
                    acc.replace(
                        &format!("sha256/{short}"),
                        &format!("sha256/{}", "0".repeat(63) + &i.to_string()),
                    )
                });
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");

        let mut reachable = BTreeSet::new();
        collect_references(&value, "file", "/c", &mut reachable);
        assert_eq!(
            reachable.len(),
            4,
            "every reference in this container, wherever it sits: {reachable:?}"
        );
        assert!(
            !reachable.contains(&("0".repeat(63) + "4")),
            "a reference into another container says nothing about this one"
        );
    }
}
