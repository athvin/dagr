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
//! availability check. There is no Kubernetes client here, no pod spec, and no
//! cluster call — the executor itself lives behind the default-off
//! [`k8s`](REMOTE_EXECUTOR_FEATURE) feature, in [`crate::k8s_runner`] and
//! [`crate::remote`], and this module only asks whether it was linked.
//!
//! [`ensure_available`](ExecutorKind::ensure_available) refuses the
//! [`Kubernetes`](ExecutorKind::Kubernetes) executor in a build that did not compile
//! it, naming the feature and the flag. That refusal is deliberately **loud**:
//! silently falling back to a local run would be the worst possible behaviour, since
//! an operator who asked for placed execution would get a laptop-shaped run reported
//! as a success. The complementary check — a build that *has* the executor but was
//! handed no cluster — is [`crate::remote_guard`]'s.
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

/// The ticket that wired the remote executor into the run path. Named in the
/// refusal so the diagnostic points somewhere rather than merely saying "no".
pub const REMOTE_EXECUTOR_TICKET: &str = "T112";

/// The cargo feature a build needs in order to run under the remote executor.
///
/// Named in the refusal, because "this build cannot" and "dagr cannot" are very
/// different sentences and only one of them is true.
pub const REMOTE_EXECUTOR_FEATURE: &str = "k8s";

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
    /// Run **placed** nodes on Kubernetes — one pod per node attempt, with the
    /// node's declared resource requests, selectors and tolerations (ADR 115).
    ///
    /// Available only in a build that compiled the default-off
    /// [`k8s`](REMOTE_EXECUTOR_FEATURE) feature; a build without it refuses saying
    /// so rather than running every node locally
    /// (see [`ensure_available`](Self::ensure_available)).
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

    /// Check that **this build** can run under the selected executor.
    ///
    /// This is a question about the binary, not about the run: it answers "was the
    /// executor compiled in at all". The separate question — "was this *run* given a
    /// cluster to place its placed nodes on" — is
    /// [`crate::remote_guard`]'s, and is what actually decides whether a node would
    /// silently run in this process. Splitting them is what lets a build with the
    /// executor compiled in run a pipeline that declares no placement under
    /// `--dagr.executor=k8s` without theatre, while still refusing the one shape that
    /// matters.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorRefusal`] when the selected executor is not compiled into
    /// this build. The caller must not proceed: it prints the refusal and exits with
    /// the bootstrap-failure code, because running locally instead would be a silent
    /// substitution the operator never asked for.
    pub fn ensure_available(self) -> Result<(), ExecutorRefusal> {
        match self {
            ExecutorKind::Local => Ok(()),
            #[cfg(feature = "k8s")]
            ExecutorKind::Kubernetes => Ok(()),
            #[cfg(not(feature = "k8s"))]
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

/// The selected executor is not compiled into this build.
///
/// A distinct type from a parse failure: the name was valid and understood, and this
/// binary simply has no implementation linked. It names the executor, the feature
/// that would supply it, the ticket that wired it, and the flag to change — the four
/// facts an operator needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRefusal {
    /// The executor that was selected and refused.
    pub kind: ExecutorKind,
}

impl fmt::Display for ExecutorRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "executor `{}` is not compiled into this build: it lives behind the default-off \
             `{}` cargo feature, wired into the run path by ticket {}. Rebuild `dagr-cli` \
             with `--features {}`, or pass `{}={}` to run every node in this process — \
             refusing rather than running them locally behind your back",
            self.kind,
            REMOTE_EXECUTOR_FEATURE,
            REMOTE_EXECUTOR_TICKET,
            REMOTE_EXECUTOR_FEATURE,
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
