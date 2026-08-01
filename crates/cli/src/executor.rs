//! **Executor selection** — which machine runs a node's attempt.
//!
//! A dagr binary is meant to be *both*: iterate locally at full speed until the
//! pipeline is correct, then give each task the infrastructure its work actually
//! needs. That pair is selected per invocation with `--dagr.executor`
//! (`DAGR_EXECUTOR`), resolved by the standard `flag > env > default` precedence in
//! [`crate::config::resolve_executor`], and defaulting to
//! [`Local`](ExecutorKind::Local).
//!
//! # What this module ships, and what it does not
//!
//! It ships the **selection**: the closed set of executor names, the parse, and the
//! availability check. It ships **no** remote execution — the
//! [`Kubernetes`](ExecutorKind::Kubernetes) variant is a **recognized stub**
//! ([`ensure_available`](ExecutorKind::ensure_available) refuses it, naming the
//! ticket that implements it), exactly the shape the metastore's reserved-but-
//! unbuilt open modes already use. There is no Kubernetes client here, no pod spec,
//! and no cluster call.
//!
//! That refusal is deliberately **loud**. Silently falling back to a local run
//! would be the worst possible behaviour: an operator who asked for placed
//! execution would get a laptop-shaped run reported as a success.
//!
//! # Placement is separate from selection
//!
//! A node's [`Placement`](dagr_core::assembly::Placement) says *where it wants to
//! run*; the executor says *what this invocation can actually do about it*. Under
//! the local executor a placement is **recorded and ignored** — it is in the graph
//! artifact and the policy hash, and the node runs in-process with its ordinary
//! declared cost. That is what makes one binary genuinely both, and it is why a
//! placed pipeline still runs on a laptop with no cluster and no warning.

use std::fmt;
use std::str::FromStr;

/// The ticket that implements the remote executor. Named in the refusal so the
/// diagnostic points somewhere rather than merely saying "no".
pub const REMOTE_EXECUTOR_TICKET: &str = "T108";

/// Which executor runs this invocation's node attempts.
///
/// A **closed** set: an unrecognized name is a loud parse failure, never a silent
/// fallback to the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutorKind {
    /// Run every node **in this process** — the default, and the mode the whole
    /// testing surface is built around. A node's placement is recorded and ignored.
    #[default]
    Local,
    /// Run **placed** nodes on Kubernetes. A **recognized stub**: this build
    /// refuses it (see [`ensure_available`](Self::ensure_available)); the executor
    /// itself is [`REMOTE_EXECUTOR_TICKET`]'s.
    Kubernetes,
}

impl ExecutorKind {
    /// Every executor name, in the order the diagnostics list them — so the
    /// accepted set is written down exactly once.
    pub const ALL: [ExecutorKind; 2] = [ExecutorKind::Local, ExecutorKind::Kubernetes];

    /// The stable flag/env spelling of this executor (`local` / `k8s`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutorKind::Local => "local",
            ExecutorKind::Kubernetes => "k8s",
        }
    }

    /// Whether this executor **honours** a node's declared placement — i.e.
    /// actually runs a placed node somewhere else, so the node draws a remote slot
    /// and near-zero local capacity from the admission pools.
    ///
    /// `false` for [`Local`](Self::Local), which records a placement and runs the
    /// node in-process anyway; its declared local cost therefore stands.
    #[must_use]
    pub fn honours_placement(self) -> bool {
        match self {
            ExecutorKind::Local => false,
            ExecutorKind::Kubernetes => true,
        }
    }

    /// The [`PlacementHandling`](dagr_core::admission::PlacementHandling) the
    /// admission ledger charges a node's cost under, for this executor.
    #[must_use]
    pub fn placement_handling(self) -> dagr_core::admission::PlacementHandling {
        if self.honours_placement() {
            dagr_core::admission::PlacementHandling::Honoured
        } else {
            dagr_core::admission::PlacementHandling::Ignored
        }
    }

    /// Check that this build can actually run under the selected executor.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorRefusal`] for an executor that is a **recognized stub** in
    /// this build. The caller must not proceed: it prints the refusal and exits
    /// with the bootstrap-failure code, because running locally instead would be a
    /// silent substitution the operator never asked for.
    pub fn ensure_available(self) -> Result<(), ExecutorRefusal> {
        match self {
            ExecutorKind::Local => Ok(()),
            ExecutorKind::Kubernetes => Err(ExecutorRefusal { kind: self }),
        }
    }
}

impl fmt::Display for ExecutorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExecutorKind {
    type Err = ExecutorParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ExecutorKind::ALL
            .into_iter()
            .find(|kind| kind.as_str() == s.trim())
            .ok_or_else(|| ExecutorParseError {
                input: s.to_string(),
            })
    }
}

/// A string was not one of the recognized executor names. Implements
/// [`Display`](fmt::Display) so it flows through
/// [`resolve`](crate::config::resolve) into an
/// [`EnvParseError`](crate::config::EnvParseError) that names the offending
/// variable and value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorParseError {
    /// The offending input verbatim.
    pub input: String,
}

impl fmt::Display for ExecutorParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let accepted: Vec<&str> = ExecutorKind::ALL.iter().map(|k| k.as_str()).collect();
        write!(
            f,
            "`{}` is not a known executor — expected one of {}",
            self.input,
            accepted.join(" | ")
        )
    }
}

impl std::error::Error for ExecutorParseError {}

/// The selected executor is a **recognized stub** this build does not implement.
///
/// A distinct type from a parse failure: the name was valid and understood, and the
/// build simply cannot honour it yet. It names the executor, the ticket that
/// implements it, and the flag to change — the three facts an operator needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRefusal {
    /// The executor that was selected and refused.
    pub kind: ExecutorKind,
}

impl fmt::Display for ExecutorRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "executor `{}` is a recognized stub reserved behind the executor seam and is not \
             implemented in this build (it is implemented by ticket {}); refusing rather than \
             running every node locally behind your back — pass `{}={}` to run in-process",
            self.kind,
            REMOTE_EXECUTOR_TICKET,
            crate::config::EXECUTOR_FLAG,
            ExecutorKind::Local,
        )
    }
}

impl std::error::Error for ExecutorRefusal {
    /// Refused from **data** (a selected name this build does not implement), not
    /// wrapped from an underlying failure — so there is no cause to expose, and
    /// fabricating one would be a lie about where the refusal came from.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
