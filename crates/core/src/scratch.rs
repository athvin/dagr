//! The **C18 durable scratch store** (local) — a per-run, per-node key-value
//! store of opaque bytes, backed by the run store on local disk (arch.md
//! `### C18 · Durable scratch store`; the placement/isolation/lifecycle contract
//! is fixed by the T0.6 run-store ADR §9).
//!
//! TDD skeleton (T53): the API surface, namespacing, and error classification
//! are present so the T53 test plan compiles and fails; the durable read/write
//! bodies land in the implementation commit.

use std::io;
use std::path::{Path, PathBuf};

use crate::context::{PipelineId, RunId};
use crate::error::TaskError;
use crate::handle::NodeId;

/// The reserved scratch subtree name under a run directory (T0.6 §3).
pub const SCRATCH_DIR_NAME: &str = "scratch";

/// The scratch operation that failed, carried by [`ScratchError::Io`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScratchOp {
    /// A [`ScratchStore::get`] read failed at the store level.
    Read,
    /// A [`ScratchStore::put`] write failed at the store level.
    Write,
}

impl ScratchOp {
    fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// The error a [`ScratchStore`] read or write reports (retry-eligible — C4).
#[derive(Debug)]
#[non_exhaustive]
pub enum ScratchError {
    /// The underlying store failed on a scratch operation. Retry-eligible (C4).
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
            Self::Io { op, source } => write!(f, "scratch {} failed: {source}", op.label()),
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
    fn from(err: ScratchError) -> Self {
        match err {
            ScratchError::Io { op, source } => {
                TaskError::retryable_from(format!("durable scratch {} failed", op.label()), source)
            }
        }
    }
}

/// The durable, per-run, per-node **scratch store** handle (arch.md `### C18`).
#[derive(Debug, Clone)]
pub struct ScratchStore {
    dir: Option<PathBuf>,
}

impl ScratchStore {
    /// Resolve the store for one node under a run-store base (T0.6 §3, §9):
    /// `<base>/<pipeline>/<run-id>/scratch/<node>/`.
    #[must_use]
    pub fn for_node(base: &Path, pipeline: &PipelineId, run: &RunId, node: NodeId) -> Self {
        let dir = base
            .join(path_segment(pipeline.as_str()))
            .join(path_segment(run.as_str()))
            .join(SCRATCH_DIR_NAME)
            .join(node_segment(node));
        Self { dir: Some(dir) }
    }

    /// The honestly-unwired store a `RunContext` built with no run store carries.
    #[must_use]
    pub(crate) fn unwired() -> Self {
        Self { dir: None }
    }

    /// This node's scratch namespace directory, or [`None`] for an unwired store.
    #[must_use]
    pub fn namespace_dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// Read the value under `key`, or `Ok(None)` if absent.
    ///
    /// # Errors
    /// [`ScratchError::Io`] (retry-eligible) on a store fault. TDD stub.
    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, ScratchError> {
        Err(not_implemented(ScratchOp::Read))
    }

    /// Write `value` under `key`, durably and atomically.
    ///
    /// # Errors
    /// [`ScratchError::Io`] (retry-eligible) on a store fault. TDD stub.
    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), ScratchError> {
        Err(not_implemented(ScratchOp::Write))
    }

    /// Remove the value under `key`, if any (idempotent).
    ///
    /// # Errors
    /// [`ScratchError::Io`] (retry-eligible) on a store fault. TDD stub.
    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub fn remove(&self, _key: &[u8]) -> Result<(), ScratchError> {
        Err(not_implemented(ScratchOp::Write))
    }

    /// The on-success lifecycle hook: delete this node's scratch namespace.
    ///
    /// # Errors
    /// [`ScratchError::Io`] (retry-eligible) on a store fault. TDD stub.
    #[allow(clippy::unused_self)]
    pub fn remove_on_success(&self) -> Result<(), ScratchError> {
        Err(not_implemented(ScratchOp::Write))
    }
}

fn not_implemented(op: ScratchOp) -> ScratchError {
    ScratchError::Io {
        op,
        source: io::Error::other("T53 scratch store not implemented yet"),
    }
}

fn node_segment(node: NodeId) -> String {
    format!("{:016x}", node.namespace_fingerprint())
}

fn path_segment(id: &str) -> String {
    if id.is_empty() {
        return "_".to_string();
    }
    let mut out = String::with_capacity(id.len());
    for (i, ch) in id.chars().enumerate() {
        let safe =
            matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_') || (ch == '.' && i != 0);
        out.push(if safe { ch } else { '_' });
    }
    out
}
