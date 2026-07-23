//! The C16 **per-run temp-directory convention** — where a run confines its local
//! debris, and how it is reclaimed (arch.md `### C16`; T0.6 §3; ticket T36).
//!
//! # The convention
//!
//! Everything a task writes *locally* — scratch files, intermediate outputs a task
//! materializes on the local filesystem before uploading a reference (C2 output
//! ownership) — goes under the **run's own per-run temp directory**, reached
//! through the [`RunContext`](dagr_core::context::RunContext::temp_dir). The temp
//! directory lives under the run-store base at
//! `<base>/<pipeline>/<run-id>/tmp/` (T0.6 §3 reserves the run directory; the
//! `tmp/` subtree is this convention's, sitting alongside the reserved
//! `events.jsonl`/`graph.json`/`run.json`/`scratch/` names, **never** colliding
//! with them). Because the path embeds both the pipeline identity and the
//! run-unique id, **two runs — even of the same binary and pipeline — get disjoint
//! temp directories** (the confinement guarantee).
//!
//! # What C16 actually promises about cleanup — and what it does not
//!
//! Cleanup guarantees are scoped to what is *enforceable* (arch.md C16):
//!
//! - A **cooperative** task that observes cancellation within grace cleans up its
//!   own temp artifacts (it removes what it wrote under the temp dir before
//!   returning) — this module gives it the confined place to write.
//! - At **run end** (normal *or* cancelled), the driver removes the whole per-run
//!   temp directory via [`cleanup_temp_dir`]. This is **best-effort by design**: an
//!   abandoned thread may race the deletion, and after grace the process exits
//!   promptly rather than waiting on a zombie.
//! - The **next invocation** reclaims leftover per-run temp directories via
//!   [`reclaim_leftover_temp_dirs`] **regardless of how the prior process ended** —
//!   an abrupt `SIGKILL` leaves the temp dir behind, and the following run of the
//!   same binary+pipeline sweeps the stale `tmp/` subtrees while leaving every
//!   reserved run output (`events.jsonl`, artifacts, `scratch/`) untouched, per
//!   T0.6 (nothing is deleted implicitly *except* this temp-dir reclamation and the
//!   prune verb).
//!
//! This module is pure filesystem plumbing over **injected paths** — no clock, no
//! network, no ambient state — so it is deterministic under test.

use std::io;
use std::path::{Path, PathBuf};

/// The reserved per-run temp-directory subtree name under a run directory. Sits
/// alongside the T0.6 §3 reserved names (`events.jsonl`, `graph.json`, `run.json`,
/// `scratch/`) and never collides with them.
pub const TEMP_DIR_NAME: &str = "tmp";

/// The per-run temp directory for one run:
/// `<base>/<pipeline>/<run-id>/tmp` (T0.6 §3; arch.md C16).
///
/// Because the path embeds both the pipeline identity and the run-unique id, two
/// runs — even of the same binary and pipeline — resolve to **disjoint** temp
/// directories. This is a pure path computation; it does not touch the filesystem
/// (use [`create_temp_dir`] to materialize it).
#[must_use]
pub fn per_run_temp_dir(base: &str, pipeline: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base)
        .join(pipeline)
        .join(run_id)
        .join(TEMP_DIR_NAME)
}

/// The run directory `<base>/<pipeline>/<run-id>` a per-run temp dir sits under.
#[must_use]
fn run_dir(base: &str, pipeline: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base).join(pipeline).join(run_id)
}

/// Create the per-run temp directory (and any missing parents), idempotently.
///
/// Called by the driver at bootstrap so a task always has a confined place to
/// write. Creating an already-existing directory is not an error.
///
/// # Errors
/// Returns any I/O error from creating the directory tree (e.g. an unwritable
/// base). The driver treats a temp-dir creation failure as non-fatal to the run's
/// record-keeping — a task that needs the temp dir will surface its own error.
pub fn create_temp_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)
}

/// Remove a per-run temp directory and everything under it, **best-effort**.
///
/// Called at run end (normal or cancelled) and — per the C16 convention — the
/// deletion is best-effort by design: a missing directory is a harmless no-op (the
/// abrupt-end / already-cleaned path), and an I/O error is swallowed rather than
/// held against the run, because after grace the process exits promptly rather than
/// blocking on a zombie that may be racing the deletion (arch.md C16).
pub fn cleanup_temp_dir(path: &Path) {
    // A missing directory is the common, expected case (nothing was written, or a
    // prior sweep already removed it) and any I/O error is likewise swallowed:
    // best-effort by design (C16) — a racing zombie thread may hold a file open, and
    // the process exits promptly rather than blocking or failing the run over
    // residual debris.
    let _ = std::fs::remove_dir_all(path);
}

/// Reclaim leftover per-run temp directories under `<base>/<pipeline>/`,
/// **keeping** the current run's, regardless of how the prior process ended
/// (arch.md C16; T0.6).
///
/// For every sibling run directory `<base>/<pipeline>/<run-id>/` other than
/// `keep_run_id`, remove its `tmp/` subtree if present. This sweeps the debris an
/// abrupt kill (`SIGKILL`, power loss) left behind on the *previous* run without
/// touching any reserved run output — `events.jsonl`, `graph.json`, `run.json`, and
/// `scratch/` are left intact (retention stays operator-owned via prune, T0.6 §8;
/// only the ephemeral `tmp/` subtree is this convention's to reclaim). A missing
/// pipeline directory (the first ever run) is a no-op.
///
/// This is deliberately scoped to the *same pipeline*: dagr does not become a
/// distributed-cleanup service reaping other pipelines' or other processes' debris
/// (arch.md permanent non-goals; the ticket's Out of scope). Residual debris beyond
/// this enforceable next-invocation sweep is the province of the operator.
pub fn reclaim_leftover_temp_dirs(base: &str, pipeline: &str, keep_run_id: &str) {
    let pipeline_dir = PathBuf::from(base).join(pipeline);
    let keep = run_dir(base, pipeline, keep_run_id);
    // No pipeline directory yet (the first-ever run) — nothing to reclaim.
    let Ok(entries) = std::fs::read_dir(&pipeline_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let run_path = entry.path();
        if !run_path.is_dir() || run_path == keep {
            continue;
        }
        // Remove ONLY the ephemeral `tmp/` subtree; every reserved run output stays.
        let leftover_temp = run_path.join(TEMP_DIR_NAME);
        if leftover_temp.exists() {
            cleanup_temp_dir(&leftover_temp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_run_temp_dir_embeds_pipeline_and_run() {
        let p = per_run_temp_dir("/base", "pipe", "run-1");
        assert!(p.ends_with("pipe/run-1/tmp"));
        assert!(p.starts_with("/base"));
    }

    #[test]
    fn distinct_runs_are_disjoint() {
        let a = per_run_temp_dir("/base", "pipe", "a");
        let b = per_run_temp_dir("/base", "pipe", "b");
        assert_ne!(a, b);
    }

    #[test]
    fn cleanup_missing_is_a_noop() {
        // A never-created directory: cleanup must not panic and must not error out.
        let p = std::env::temp_dir().join(format!("dagr-temp-unit-{}", std::process::id()));
        let missing = p.join("pipe/never/tmp");
        cleanup_temp_dir(&missing);
        assert!(!missing.exists());
    }
}
