//! The **C18 durable scratch store** (local) — a per-run, per-node key-value
//! store of opaque bytes, backed by the run store on local disk (arch.md
//! `### C18 · Durable scratch store`; the placement/isolation/lifecycle contract
//! is fixed by the T0.6 run-store ADR §9).
//!
//! # What it is (and is not)
//!
//! Scratch lets a task **remember something across its own retries** — a cursor,
//! a high-water mark, an "I already finished the first half" checkpoint. A value
//! written on attempt one is readable on attempt two. Keys and values are
//! **opaque `byte`-strings**: serialization is the task's affair and the store
//! imposes no schema. There is **no hard size bound**, but the store is designed
//! for values measured in **kilobytes** — cursors and checkpoints, not datasets.
//!
//! Scratch is **not a channel for passing data between nodes** — that is what
//! typed data edges (C10) are for. It is scoped to **one node within one run**;
//! there is deliberately no API by which one node can name, reach, or read
//! another node's scratch (see [enforced isolation](#enforced-cross-node-isolation)).
//!
//! # Physical layout (T0.6 §3, §9)
//!
//! A node's scratch lives under the run store, inside that run's directory:
//!
//! ```text
//! <base>/<pipeline>/<run-id>/scratch/<node>/
//! ```
//!
//! The `<node>` segment is derived from the node's opaque [`NodeId`]
//! (a stable hex fingerprint), so two distinct nodes resolve to **distinct**
//! directories and cannot collide, while the same node under two different runs
//! resolves under two different `<run-id>` directories and cannot collide either.
//! The `scratch/` subtree sits alongside the reserved `events.jsonl` /
//! `graph.json` / `run.json` / `tmp/` names (T0.6 §3) and never touches them.
//!
//! # Durability (atomic writes)
//!
//! A write is **crash-safe** by the same discipline the event stream and the
//! artifacts use (T0.6 §6; T27): the value is written to a **temporary file in
//! the same directory**, the temp file is **fsynced**, then **atomically renamed**
//! into place, and finally the **containing directory is fsynced** so the rename
//! itself is durable. A crash at any point leaves either the old value or the new
//! value on disk — never a torn one. A read observes the last completed write.
//!
//! When the operator has pointed the run-store base at storage that survives the
//! container, scratch survives with it (the basis for T54a); on ephemeral local
//! disk it does not, and such a run is simply not resumable — that is the
//! operator's one infrastructure choice (arch.md "The shape of a run"), not this
//! store's concern.
//!
//! # Survives a process restart (T54a)
//!
//! Because a write lands **through the run store on disk**, a value written by one
//! process is readable, **byte-for-byte, by a later, separate process** that opens
//! the same run directory — the store keeps no live in-process state a value
//! depends on. This is the durability foundation resume (C27 / T58) stands on: a
//! checkpoint written before a process ends is still there after it. It is proven
//! by `crates/core/tests/scratch_survives_restart.rs` against a **real** separate
//! writing process (the `dagr-scratch-run` test-support harness) whose scratch a
//! *different* process reads back after the writer has exited — and it holds only
//! as far as the operator's base itself survives (see above).
//!
//! # Enforced cross-node isolation
//!
//! Isolation is **enforced by construction, not by convention**. A [`ScratchStore`]
//! handle carries only the resolved path of **its own** node's namespace and
//! exposes no method that takes a foreign node, run, or absolute path. There is no
//! API surface — none — by which the handle a task receives can address another
//! node's namespace. A key that another node wrote is simply **absent** through
//! this handle.
//!
//! # Lifecycle (T0.6 §8, §9)
//!
//! When a node reaches **terminal success**, its scratch is deleted (the
//! checkpoints have served their purpose) via [`ScratchStore::remove_on_success`].
//! Scratch of a node that did **not** succeed is **left in place** — nothing is
//! deleted implicitly at run end; that retained scratch is exactly what a later
//! resume (C27 / T54b) copies forward, and it is reclaimed only by the prune verb
//! (C26). Neither resume copy-forward nor prune is this ticket's concern.
//!
//! **Retention is the operator's, and prune's, alone (T54a / T0.6 §8).** Stated
//! as the operator-facing guarantee: after a run ends, a **succeeded** node's
//! scratch is **gone**; a **non-succeeded** node's scratch (failed, timed-out,
//! cancelled, skipped, or never-terminal) **survives the process restart** on
//! disk and is **the operator's to prune**. The run-finished path performs no
//! blanket cleanup, and no timer or subsequent run reclaims it — **prune (C26) is
//! the sole implicit-deletion path**, and it reclaims retained scratch by removing
//! the **whole per-run directory** `<base>/<pipeline>/<run-id>/`.
//!
//! # Failure classification (C4)
//!
//! Any read or write failure caused by the underlying store surfaces as a
//! [`ScratchError::Io`], which converts to a **retry-eligible**
//! [`TaskError`] — disk trouble is transient more often
//! than not, so the node's retry budget absorbs it. This is **distinct** from the
//! "absent key" outcome, which is `Ok(None)` and **not** a failure.
//!
//! # Hand-construction for tests (C8)
//!
//! A store is reachable from a [`RunContext`](crate::context::RunContext) built
//! entirely by hand under a temp run-store base, with **no runtime, admission, or
//! event stream** present — the single-task test path C8 guarantees.

use std::io;
use std::path::{Path, PathBuf};

use crate::context::{PipelineId, RunId};
use crate::error::TaskError;
use crate::handle::NodeId;

/// The reserved scratch subtree name under a run directory (T0.6 §3). Sits
/// alongside the reserved `events.jsonl`, `graph.json`, `run.json`, and `tmp/`
/// names and never collides with them.
pub const SCRATCH_DIR_NAME: &str = "scratch";

/// The scratch operation that failed, carried by [`ScratchError::Io`] so the
/// error identifies which operation surfaced the fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScratchOp {
    /// A [`ScratchStore::get`] read failed at the store level.
    Read,
    /// A [`ScratchStore::put`] write failed at the store level.
    Write,
}

impl ScratchOp {
    /// A short label for the failing operation, for the error message.
    #[must_use]
    fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// The error a [`ScratchStore`] read or write reports (arch.md `### C18`; T0.6
/// §9 fixes it is a **retry-eligible** task failure).
///
/// # This is not the "absent" outcome
///
/// A key that was never written is **not** an error: [`ScratchStore::get`]
/// returns `Ok(None)` for it. `ScratchError` is raised **only** when the
/// underlying store fails — an unwritable directory, a read fault on an existing
/// value. It converts to a retry-eligible [`TaskError`] via [`From`], never to a
/// permanent failure and never to a panic.
#[derive(Debug)]
#[non_exhaustive]
pub enum ScratchError {
    /// The underlying store failed on a scratch operation. Carries which
    /// operation failed and the underlying I/O error, so the caller can identify
    /// the failing operation. Classified **retry-eligible** (C4).
    Io {
        /// Which scratch operation surfaced the fault.
        op: ScratchOp,
        /// The underlying I/O error.
        source: io::Error,
    },
}

impl std::fmt::Display for ScratchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { op, source } => {
                write!(f, "scratch {} failed: {source}", op.label())
            }
        }
    }
}

impl std::error::Error for ScratchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
        }
    }
}

impl From<ScratchError> for TaskError {
    /// A scratch I/O failure is a **retry-eligible** task failure (C4; T0.6 §9):
    /// disk trouble is transient more often than not, so the node's retry budget
    /// absorbs it. The underlying I/O error is preserved as the error source.
    fn from(err: ScratchError) -> Self {
        match err {
            ScratchError::Io { op, source } => {
                TaskError::retryable_from(format!("durable scratch {} failed", op.label()), source)
            }
        }
    }
}

/// The durable, per-run, per-node **scratch store** handle (arch.md `### C18`).
///
/// A handle addresses **exactly one** node's namespace under one run:
/// `<base>/<pipeline>/<run-id>/scratch/<node>/`. It exposes opaque-byte
/// [`get`](Self::get) / [`put`](Self::put) / [`remove`](Self::remove) plus the
/// success-lifecycle [`remove_on_success`](Self::remove_on_success) hook — and
/// **nothing** that could name another node's namespace, which is what makes
/// cross-node isolation a guarantee rather than a convention (see the
/// [module docs](self)).
///
/// Cheap to clone (a clone shares the resolved path); the store keeps no open
/// file handles between calls, so a `RunContext` carrying one stays `Send + Sync`
/// and hand-constructable.
#[derive(Debug, Clone)]
pub struct ScratchStore {
    // The resolved directory for THIS node's namespace, and only this node's:
    // `<base>/<pipeline>/<run-id>/scratch/<node>/`. The handle exposes no method
    // taking a foreign node/run/path, so there is no route out of this directory
    // — cross-node isolation is enforced by construction (module docs).
    //
    // `None` is the honestly-unwired seam: a `RunContext` built with no run store
    // (the C8 hand-built path that supplies no scratch root) carries a store that
    // reads absent and writes error, never pretending to persist.
    dir: Option<PathBuf>,
}

impl ScratchStore {
    /// Resolve the store for one node under a run-store base (T0.6 §3, §9).
    ///
    /// The namespace directory is `<base>/<pipeline>/<run-id>/scratch/<node>/`.
    /// The `<node>` segment is derived from the opaque [`NodeId`] as a stable hex
    /// fingerprint, so two distinct nodes never collide and the segment is always
    /// filesystem-safe. This is a pure path computation — it does **not** touch
    /// the filesystem (a write creates the directory lazily).
    #[must_use]
    pub fn for_node(base: &Path, pipeline: &PipelineId, run: &RunId, node: NodeId) -> Self {
        Self {
            dir: Some(namespace_path(base, pipeline, run, node)),
        }
    }

    /// The honestly-unwired store a `RunContext` carries when it was built with no
    /// run store (the C8 hand-built path). Reads report absent and writes report a
    /// retry-eligible I/O error — it never pretends to persist. The real store is
    /// wired via [`for_node`](Self::for_node).
    #[must_use]
    pub(crate) fn unwired() -> Self {
        Self { dir: None }
    }

    /// This node's scratch namespace directory
    /// (`<base>/<pipeline>/<run-id>/scratch/<node>/`), or [`None`] for an
    /// unwired store. Exposed so a test can assert the physical layout (T53 Test
    /// plan: "Physical layout is inside the run directory and namespaced"); it is
    /// **not** a route to another node's namespace — it is this handle's own dir.
    #[must_use]
    pub fn namespace_dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// The on-disk path of one key's value file within this node's namespace. The
    /// key's opaque bytes are hex-encoded into a filesystem-safe filename, so any
    /// key bytes — including path separators, dots, or non-UTF-8 — are stored
    /// safely and can never escape the namespace directory (no path traversal).
    fn key_path(dir: &Path, key: &[u8]) -> PathBuf {
        dir.join(encode_key(key))
    }

    /// Read the value previously written under `key`, or `Ok(None)` if no value
    /// was ever written under it (the **absent** outcome, distinct from a failure).
    ///
    /// A value written on one attempt is readable on the next (arch.md C18). The
    /// bytes are returned **exactly** as written, byte-for-byte.
    ///
    /// # Errors
    ///
    /// Returns [`ScratchError::Io`] (which converts to a **retry-eligible**
    /// [`TaskError`], C4) if the underlying store fails to
    /// read an existing value. A **missing** key is **not** an error — it is
    /// `Ok(None)`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ScratchError> {
        let Some(dir) = self.dir.as_deref() else {
            // Unwired seam: nothing was persisted, and there is nowhere to read
            // from. Surface a retry-eligible fault rather than a fabricated absent
            // — an unwired store is a misconfiguration, not "key absent".
            return Err(ScratchError::Io {
                op: ScratchOp::Read,
                source: io::Error::new(
                    io::ErrorKind::NotFound,
                    "scratch store has no run-store base (context built with no run store)",
                ),
            });
        };
        let path = Self::key_path(dir, key);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            // A never-written key is the well-defined ABSENT outcome, not a fault.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(ScratchError::Io {
                op: ScratchOp::Read,
                source,
            }),
        }
    }

    /// Write `value` under `key`, durably and atomically (write-temp, fsync,
    /// rename, fsync-dir — see the [module docs](self)). Overwrites any prior
    /// value under the same key. The value is opaque bytes; serialization is the
    /// task's affair.
    ///
    /// # Errors
    ///
    /// Returns [`ScratchError::Io`] (which converts to a **retry-eligible**
    /// [`TaskError`], C4) if the underlying store cannot
    /// persist the value — an unwritable namespace, a full disk. Never a permanent
    /// failure and never a panic.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ScratchError> {
        let Some(dir) = self.dir.as_deref() else {
            return Err(ScratchError::Io {
                op: ScratchOp::Write,
                source: io::Error::new(
                    io::ErrorKind::NotFound,
                    "scratch store has no run-store base (context built with no run store)",
                ),
            });
        };
        let path = Self::key_path(dir, key);
        atomic_write(dir, &path, value).map_err(|source| ScratchError::Io {
            op: ScratchOp::Write,
            source,
        })
    }

    /// Remove the value stored under `key`, if any. Removing a key that was never
    /// written is **not** an error (the absent outcome is idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`ScratchError::Io`] (retry-eligible, C4) if the underlying store
    /// fails to remove an existing value.
    pub fn remove(&self, key: &[u8]) -> Result<(), ScratchError> {
        let Some(dir) = self.dir.as_deref() else {
            // No run store: nothing to remove; idempotent no-op.
            return Ok(());
        };
        let path = Self::key_path(dir, key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ScratchError::Io {
                op: ScratchOp::Write,
                source,
            }),
        }
    }

    /// The **on-success lifecycle hook** (arch.md C18; T0.6 §9): delete this
    /// node's entire scratch namespace, invoked when the node reaches terminal
    /// **success**. After it, every key this node wrote reads back absent and the
    /// node's scratch storage location no longer exists on disk.
    ///
    /// This is called **only** for a succeeded node. Scratch of a node that did
    /// not succeed is **left in place** (nothing is deleted implicitly at run end)
    /// — that retained scratch is what a later resume copies forward and prune
    /// (C26) reclaims. Removing an already-absent namespace is a harmless no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ScratchError::Io`] (retry-eligible, C4) if the underlying store
    /// fails to remove a present namespace directory.
    pub fn remove_on_success(&self) -> Result<(), ScratchError> {
        let Some(dir) = self.dir.as_deref() else {
            return Ok(());
        };
        match std::fs::remove_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ScratchError::Io {
                op: ScratchOp::Write,
                source,
            }),
        }
    }

    /// **Resume scratch carry-forward** (arch.md `### C18`, line 391; C27 / T54b):
    /// copy **one node's retained scratch** from the linked `prior` run's namespace
    /// into the `resumed` run's namespace, under the same run-store `base`, so a
    /// re-executing node **continues from its checkpoint** rather than starting over.
    ///
    /// This is the scratch half of resume. The resume core (C27 / T58) computes the
    /// re-execution set — the nodes it does *not* mark `satisfied-from-prior` — and
    /// the driver calls this **once per node in that set**, before the node is
    /// admitted. The re-executing node then reads its checkpoint through the
    /// ordinary [`get`](Self::get) context API, with no awareness that a copy
    /// happened and **no route to the prior run's directory**: the value is written
    /// under the resumed run id's own per-node namespace
    /// (`<base>/<pipeline>/<resumed>/scratch/<node>/`), preserving the T53/T54a
    /// per-run/per-node namespacing.
    ///
    /// # Copy, not move — the prior run is retained
    ///
    /// The prior run's scratch is **retained** (T54a: a non-succeeded node's scratch
    /// is not deleted at run end) and is reclaimed only by prune (C26). Carry-forward
    /// **copies** it and never consumes it: after this call the prior namespace still
    /// holds every value it did.
    ///
    /// # Isolation is preserved by construction
    ///
    /// The source and destination namespaces are both derived from **this same
    /// `node`**'s opaque identity fingerprint, so a node's carried-forward scratch
    /// lands only in its own resumed namespace — one node can never receive another's
    /// (the C18 isolation criterion, enforced not conventional). Copying one node
    /// reads and writes **only** that node's two directories.
    ///
    /// # Missing prior scratch is a clean empty carry, not an error
    ///
    /// A re-executing node whose prior scratch is **absent** (it succeeded — so its
    /// scratch was deleted — or simply never wrote any) is an **empty** carry:
    /// nothing is copied, no resumed namespace is created, and this returns `Ok(())`.
    /// The resumed node then reads an empty namespace and proceeds normally. Absence
    /// of prior scratch is never a resume failure.
    ///
    /// # Guard boundary
    ///
    /// This performs no resumability check of its own — it presupposes the prior
    /// run's store survived. A resume whose store is gone is refused up front by the
    /// C27 gate (T58) and never reaches carry-forward, so this never fabricates a
    /// missing prior directory.
    ///
    /// # Errors
    ///
    /// Returns [`ScratchError::Io`] — which converts to a **retry-eligible**
    /// [`TaskError`] (C4; arch.md line 393) — if reading a prior value or writing it
    /// into the resumed namespace fails at the store level. The failure is the
    /// affected node's alone; it is neither a silent skip nor a whole-resume abort.
    pub fn carry_forward(
        base: &Path,
        pipeline: &PipelineId,
        prior: &RunId,
        resumed: &RunId,
        node: NodeId,
    ) -> Result<(), ScratchError> {
        let prior_dir = namespace_path(base, pipeline, prior, node);
        let resumed_dir = namespace_path(base, pipeline, resumed, node);

        // Read the prior node's retained namespace. An ABSENT prior namespace is a
        // clean empty carry (the node succeeded, or never wrote any scratch) — not a
        // fault; nothing to copy, nothing to create.
        let entries = match std::fs::read_dir(&prior_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(ScratchError::Io {
                    op: ScratchOp::Read,
                    source,
                });
            }
        };

        for entry in entries {
            let entry = entry.map_err(|source| ScratchError::Io {
                op: ScratchOp::Read,
                source,
            })?;
            let file_name = entry.file_name();
            // Skip any leftover write-temp debris (`.<name>.tmp.<pid>.<n>`) so a
            // crashed prior write never becomes a carried-forward value; a value
            // file is the atomically-renamed final name (a hex-encoded key), never a
            // dotfile. This mirrors the atomic-write discipline `put` uses.
            if is_write_temp(&file_name) {
                continue;
            }
            let src = entry.path();
            // A value file is a regular file; ignore anything that is not (the
            // namespace holds only value files — this is belt-and-suspenders).
            let is_file = entry
                .file_type()
                .map_err(|source| ScratchError::Io {
                    op: ScratchOp::Read,
                    source,
                })?
                .is_file();
            if !is_file {
                continue;
            }

            // Read the prior value, then write it into the resumed namespace through
            // the same crash-safe atomic discipline `put` uses (create-dir-lazily,
            // write-temp, fsync, rename, fsync-dir). The destination file name is the
            // SAME encoded-key file name — identity-preserving, so the resumed node
            // reads it back under the identical key.
            let value = std::fs::read(&src).map_err(|source| ScratchError::Io {
                op: ScratchOp::Read,
                source,
            })?;
            let dest = resumed_dir.join(&file_name);
            atomic_write(&resumed_dir, &dest, &value).map_err(|source| ScratchError::Io {
                op: ScratchOp::Write,
                source,
            })?;
        }
        Ok(())
    }
}

/// Is this directory entry a write-temp file left by [`atomic_write`]
/// (`.<final>.tmp.<pid>.<counter>`)? Value files are the atomically-renamed final
/// names (hex-encoded keys, never a leading dot), so a leading-dot `.tmp.` name is
/// crashed-write debris that carry-forward must not promote into a resumed value.
fn is_write_temp(file_name: &std::ffi::OsStr) -> bool {
    file_name
        .to_str()
        .is_some_and(|s| s.starts_with('.') && s.contains(".tmp."))
}

/// Resolve the on-disk namespace directory for one node under one run
/// (`<base>/<pipeline>/<run-id>/scratch/<node>/`, T0.6 §3, §9). A pure path
/// computation shared by [`ScratchStore::for_node`] and
/// [`ScratchStore::carry_forward`]; it touches no filesystem.
fn namespace_path(base: &Path, pipeline: &PipelineId, run: &RunId, node: NodeId) -> PathBuf {
    base.join(path_segment(pipeline.as_str()))
        .join(path_segment(run.as_str()))
        .join(SCRATCH_DIR_NAME)
        .join(node_segment(node))
}

/// Derive a stable, filesystem-safe segment for a node's namespace from its
/// opaque [`NodeId`]. The id is an opaque `u64` fingerprint (name-derived,
/// reorder-stable, rename-sensitive — T0.7); rendering it as fixed-width hex
/// gives a segment that is always valid on disk, is disjoint for distinct ids,
/// and reveals no name (identity stays opaque — C2).
fn node_segment(node: NodeId) -> String {
    format!("{:016x}", node.namespace_fingerprint())
}

/// Render an identity string (pipeline / run) into a single path segment. The
/// run-store layout (T0.6 §3) uses the identity strings directly as directory
/// names; this maps any character that is not filesystem-safe (a separator or a
/// bare `.`/`..`) to `_` so an opaque, operator-supplied identity can never
/// escape its directory or collide with a parent reference, while ordinary ids
/// pass through readable. A leading `.` is escaped so no identity resolves to a
/// hidden dir or the `.`/`..` specials.
fn path_segment(id: &str) -> String {
    if id.is_empty() {
        return "_".to_string();
    }
    let mut out = String::with_capacity(id.len());
    for (i, ch) in id.chars().enumerate() {
        // Keep the run-store layout readable for ordinary ids while making a
        // hostile one inert: separators, NULs, and a leading dot become `_`.
        let safe =
            matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_') || (ch == '.' && i != 0);
        if safe {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

/// Hex-encode an opaque key into a filesystem-safe filename. A key is arbitrary
/// bytes (path separators, dots, non-UTF-8 all allowed); hex-encoding makes every
/// key a distinct, safe filename that cannot traverse out of the namespace
/// directory and cannot collide with another key. An empty key encodes to a
/// fixed non-empty sentinel so it still names a file.
fn encode_key(key: &[u8]) -> String {
    if key.is_empty() {
        return "00".to_string(); // fixed sentinel: the empty key still names a file
    }
    let mut s = String::with_capacity(key.len() * 2);
    for byte in key {
        s.push(char::from_digit(u32::from(byte >> 4), 16).unwrap());
        s.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap());
    }
    s
}

/// Write `value` to `final_path` **durably and atomically** under `dir`
/// (mirroring the event-stream / artifact discipline, T0.6 §6, T27):
///
/// 1. write the bytes to a fresh temp file **in the same directory** (so the
///    rename is same-filesystem and therefore atomic),
/// 2. `fsync` the temp file so its bytes are durable before it is named,
/// 3. atomically `rename` the temp file over the final path,
/// 4. `fsync` the containing directory so the rename entry itself is durable.
///
/// A crash at any step leaves either the old value or the new value at
/// `final_path` — never a torn one — because the reader only ever observes the
/// atomically-renamed final file.
fn atomic_write(dir: &Path, final_path: &Path, value: &[u8]) -> io::Result<()> {
    use std::io::Write as _;

    // Create the namespace directory lazily on first write.
    std::fs::create_dir_all(dir)?;

    // A temp name unique to this write, in the SAME directory as the final file
    // so the rename stays on one filesystem (atomic). Uniqueness across concurrent
    // writers within one process is not required by the contract (one live attempt
    // writes a node's scratch — T0.3), but a pid+counter base keeps two writes to
    // different keys from sharing a temp path.
    let tmp_name = format!(
        ".{}.tmp.{}.{}",
        final_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("scratch"),
        std::process::id(),
        next_tmp_counter(),
    );
    let tmp_path = dir.join(tmp_name);

    // Write + fsync the temp file, then rename it into place. On any error, remove
    // the temp file so a failed write leaves no debris.
    let write_result = (|| -> io::Result<()> {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(value)?;
        file.sync_all()?; // fsync the bytes before the rename names them
        std::fs::rename(&tmp_path, final_path)?; // atomic replace
        Ok(())
    })();

    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp_path); // best-effort cleanup of the temp
        return write_result;
    }

    // fsync the directory so the rename entry itself is durable across a crash.
    // A directory that cannot be opened for fsync is not fatal to the write (the
    // bytes and the rename already landed); swallow only that, not the write.
    if let Ok(dir_handle) = std::fs::File::open(dir) {
        let _ = dir_handle.sync_all();
    }
    Ok(())
}

/// A per-process monotonic counter so two temp files in one directory never share
/// a name within a process. Deterministic and lock-free.
fn next_tmp_counter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_key_is_distinct_and_safe() {
        assert_eq!(encode_key(b""), "00");
        assert_eq!(encode_key(b"A"), "41");
        assert_ne!(encode_key(b"../escape"), encode_key(b"escape"));
        // No path separators, dots, or traversal in an encoded key.
        let enc = encode_key(b"../../etc/passwd");
        assert!(!enc.contains('/') && !enc.contains('.'));
    }

    #[test]
    fn path_segment_neutralizes_traversal() {
        assert_eq!(path_segment(""), "_");
        // A leading dot is escaped, so neither `.` nor `..` survives as a
        // self/parent-reference segment; an interior dot (legitimate in ids like
        // `T0.6`) is kept.
        assert_eq!(path_segment("."), "_");
        assert_ne!(path_segment(".."), "..");
        assert!(!path_segment("..").starts_with('.'));
        assert_eq!(path_segment("a/b"), "a_b");
        assert_eq!(path_segment(".hidden"), "_hidden");
        assert_eq!(path_segment("T0.6"), "T0.6");
        assert_eq!(path_segment("ok-run_1"), "ok-run_1");
    }

    #[test]
    fn distinct_nodes_get_distinct_segments() {
        let a = node_segment(NodeId::from_name("alpha"));
        let b = node_segment(NodeId::from_name("beta"));
        assert_ne!(a, b);
        // Stable: same name → same segment.
        assert_eq!(a, node_segment(NodeId::from_name("alpha")));
    }
}
