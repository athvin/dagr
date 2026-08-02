//! Shared fixtures for the pod-observer task's suites: one canonical identity,
//! and pod snapshots built from it.
//!
//! Both suites speak about the same run, so the fixture lives once and the tests
//! differ only in what happens to the pods.
//!
//! `allow(dead_code)`: this module is compiled separately into each integration
//! test binary, and no single one of them uses every fixture. The alternative is
//! two near-identical copies of the same run.

#![allow(dead_code)]

use std::collections::BTreeMap;

use dagr_k8s::api::{PodPhase, PodSnapshot};
use dagr_k8s::identity::{AttemptIdentity, AttemptKey};
use dagr_k8s::observer::RunSelector;

/// The run every suite observes. A real 36-character `UUIDv7`, because the
/// 63-character label ceiling only bites at that length.
pub const RUN_ID: &str = "0197f3a8-6b21-7c4e-9d55-1f2a3b4c5d6e";

/// The structural fingerprint the observer is told to expect. A pod annotated
/// with anything else is foreign.
pub const FINGERPRINT: &str = "sf-2f1c9a4b7e0d5638";

/// The policy hash carried alongside it.
pub const POLICY_HASH: &str = "ph-88c1042ea9b3f7d0";

/// The selector under test.
pub fn selector() -> RunSelector {
    RunSelector::new(RUN_ID, FINGERPRINT)
}

/// A full identity for one attempt of one node.
pub fn identity(node: &str, attempt: u32) -> AttemptIdentity {
    AttemptIdentity {
        key: AttemptKey::new(RUN_ID, node, attempt),
        pipeline: "nightly-etl".to_string(),
        structural_fingerprint: FINGERPRINT.to_string(),
        policy_hash: POLICY_HASH.to_string(),
        tool_version: "dagr 0.0.0".to_string(),
        image_digest: "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
            .to_string(),
        owner: "orchestrator-01".to_string(),
    }
}

/// A pod for `node`/`attempt` in `phase`, named after the two, carrying the
/// canonical labels and annotations.
pub fn pod(node: &str, attempt: u32, phase: PodPhase, resource_version: &str) -> PodSnapshot {
    let id = identity(node, attempt);
    let mut snapshot = PodSnapshot::new(
        format!("dagr-{node}-{attempt}"),
        resource_version.to_string(),
        phase,
        &id,
    );
    if phase == PodPhase::Failed {
        snapshot.container_reason = Some("Error".to_string());
        snapshot.exit_code = Some(1);
    }
    snapshot
}

/// The same pod, but with one annotation overridden — the only way to build a
/// pod that is *shaped* like ours and is not ours.
pub fn pod_with_annotation(
    node: &str,
    attempt: u32,
    phase: PodPhase,
    resource_version: &str,
    key: &str,
    value: &str,
) -> PodSnapshot {
    let mut snapshot = pod(node, attempt, phase, resource_version);
    snapshot
        .annotations
        .insert(key.to_string(), value.to_string());
    snapshot
}

/// A pod carrying arbitrary labels and annotations — used to prove that an
/// unidentifiable pod is reported rather than attributed.
pub fn bare_pod(
    name: &str,
    resource_version: &str,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
) -> PodSnapshot {
    PodSnapshot {
        name: name.to_string(),
        resource_version: resource_version.to_string(),
        phase: PodPhase::Running,
        labels,
        annotations,
        pod_reason: None,
        container_reason: None,
        exit_code: None,
        uid: None,
        host: None,
        waiting_reason: None,
        scheduling_refusal: None,
    }
}
