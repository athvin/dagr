//! **The placement-wiring guard** — the refusal that replaced T105's bootstrap stub.
//!
//! T105 refused `--dagr.executor=k8s` outright, for a stated reason: *"a
//! `RunConfig::executor(Kubernetes)` would run every node in-process while the
//! operator believed their placement was honoured — the one outcome worse than
//! refusing."* T112 lifted that refusal, because the executor now exists — but the
//! outcome it prevented has not become acceptable, so the check moved rather than
//! vanished, and it moved to where it can be *precise*:
//!
//! - [`ExecutorKind::ensure_available`](crate::executor::ExecutorKind::ensure_available)
//!   now answers "did this build compile the executor in at all", which is a
//!   question about the binary.
//! - This module answers "was this *run* given a cluster to place its placed nodes
//!   on", which is a question about the run — and it is the one that decides whether
//!   a node would silently run here.
//!
//! The narrowing is the point. A pipeline with **no** placed node runs perfectly
//! well under a remote executor: there is nothing to place, so there is nothing to
//! substitute, and refusing it would be theatre.
//!
//! It lives outside the `k8s`-gated `crate::remote` module on purpose: the guard
//! must exist in **every** build, including the ones that compiled no executor,
//! because the diagnostic those builds print is the one an operator reads first.

use dagr_core::flow::Pipeline;

use crate::executor::ExecutorKind;

/// Every node in `pipeline` that declares a [placement](dagr_core::assembly::Placement),
/// in the order the pipeline holds them.
#[must_use]
pub fn placed_node_names(pipeline: &Pipeline) -> Vec<String> {
    pipeline
        .nodes()
        .filter(|node| node.policy().placement_spec().is_some())
        .map(|node| node.name().to_string())
        .collect()
}

/// The refusal a run gets when it selects an executor that honours placement, has
/// placed nodes, and was handed no cluster to place them on.
///
/// It names all three facts an operator needs: which executor, which nodes, and the
/// two ways forward.
#[must_use]
pub fn unwired_refusal(executor: ExecutorKind, placed: &[String]) -> String {
    format!(
        "executor `{executor}` honours placement and {} node(s) declare one ({}), but this \
         run was given no cluster to place them on. Drive it through \
         `RunnableFlow::run_placed` with a `RemoteCluster`, or pass `{}=local` to run every \
         node in this process — refusing rather than running them here behind your back.",
        placed.len(),
        placed.join(", "),
        crate::config::EXECUTOR_FLAG,
    )
}
