//! Shared fixtures for this crate's suites: one canonical identity for one run.
//!
//! The pod *snapshots* built from it live with the suites that drive the observer
//! task — `crates/cli/tests/k8s_support/` — because that is where the task and its
//! runtime are. What is shared across the two is the run itself, so a reader
//! comparing them sees the same ids.
//!
//! `allow(dead_code)`: this module is compiled separately into each integration
//! test binary, and no single one of them uses every fixture.

#![allow(dead_code)]

use dagr_k8s::identity::{AttemptIdentity, AttemptKey};

/// The run every suite observes. A real 36-character `UUIDv7`, because the
/// 63-character label ceiling only bites at that length.
pub const RUN_ID: &str = "0197f3a8-6b21-7c4e-9d55-1f2a3b4c5d6e";

/// The structural fingerprint the observer is told to expect. A pod annotated
/// with anything else is foreign.
pub const FINGERPRINT: &str = "sf-2f1c9a4b7e0d5638";

/// The policy hash carried alongside it.
pub const POLICY_HASH: &str = "ph-88c1042ea9b3f7d0";

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
