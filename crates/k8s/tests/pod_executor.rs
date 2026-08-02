//! T108 · the **executor's pure half**: the pod spec, the never-retry invariant,
//! the three pre-start surfaces, and terminal classification from pod status.
//! Written first (TDD).
//!
//! Everything here is a decision with no I/O in it, which is why it lives in
//! `dagr-k8s` and is tested without a runtime, a cluster or a feature. The half
//! that submits, waits and replays is `dagr_cli::k8s_runner`, where the process
//! and its runtime are (ADR 004).
//!
//! Two findings from T101's spike are load-bearing and are pinned here:
//!
//! - **A pre-start failure never reaches a terminal phase**, and it shows up on
//!   *three* surfaces, not one — a waiting reason, an unschedulable `PodScheduled`
//!   condition, and a pod-level reason on an already-started pod. A runner awaiting
//!   `Succeeded`/`Failed` for an unpullable image waits forever.
//! - **Both `reason` fields must be read.** An out-of-memory kill appears on the
//!   *container* with an empty pod reason; an eviction appears on the *pod* with the
//!   container reporting a generic `Error`. Exit code `137` is produced by both an
//!   OOM kill and an external termination, so it disambiguates nothing on its own.

use std::collections::BTreeMap;

use dagr_k8s::api::{PodPhase, PodSnapshot};
use dagr_k8s::executor::{
    ClusterRetry, FATAL_WAITING_REASONS, PodOutcome, PodPlacement, PodRequest, PodStatusFacts,
    PreStartSurface, build_pod, classify_pre_start, classify_terminal, pod_name,
    reject_credential_bearing,
};
use dagr_k8s::identity::{
    ANNOTATION_IMAGE_DIGEST, ANNOTATION_NODE, ANNOTATION_POLICY_HASH,
    ANNOTATION_STRUCTURAL_FINGERPRINT, AttemptIdentity, AttemptKey, LABEL_ATTEMPT, LABEL_RUN_ID,
};

const RUN_ID: &str = "0197f3a8-6b21-7c4e-9d55-1f2a3b4c5d6e";
const FINGERPRINT: &str = "sf-2f1c9a4b7e0d5638";
const POLICY_HASH: &str = "ph-88c1042ea9b3f7d0";
const IMAGE: &str = "registry.example/dagr@sha256:cafebabe";

fn identity(node: &str, attempt: u32) -> AttemptIdentity {
    AttemptIdentity {
        key: AttemptKey::new(RUN_ID, node, attempt),
        pipeline: "example-pipeline".to_string(),
        structural_fingerprint: FINGERPRINT.to_string(),
        policy_hash: POLICY_HASH.to_string(),
        tool_version: "dagr@1".to_string(),
        image_digest: "sha256:cafebabe".to_string(),
        owner: "orchestrator-1".to_string(),
    }
}

fn request(node: &str, attempt: u32) -> PodRequest {
    PodRequest {
        identity: identity(node, attempt),
        namespace: "dagr".to_string(),
        image: IMAGE.to_string(),
        command: vec![
            "dagr".to_string(),
            "exec-node".to_string(),
            "--node".to_string(),
            node.to_string(),
        ],
        placement: PodPlacement::default(),
    }
}

// ---------------------------------------------------------------------------
// The pod spec, and the invariant that Kubernetes never retries
// ---------------------------------------------------------------------------

#[test]
fn the_pod_carries_its_identity_as_selector_labels_and_authoritative_annotations() {
    let spec = build_pod(&request("extract", 2), ClusterRetry::Disabled).expect("a spec is built");

    assert_eq!(
        spec.labels.get(LABEL_RUN_ID).map(String::as_str),
        Some(RUN_ID)
    );
    assert_eq!(
        spec.labels.get(LABEL_ATTEMPT).map(String::as_str),
        Some("2")
    );
    assert_eq!(
        spec.annotations.get(ANNOTATION_NODE).map(String::as_str),
        Some("extract"),
        "the FULL node name is authoritative and lives in an annotation"
    );
    assert_eq!(
        spec.annotations
            .get(ANNOTATION_STRUCTURAL_FINGERPRINT)
            .map(String::as_str),
        Some(FINGERPRINT)
    );
    assert_eq!(
        spec.annotations
            .get(ANNOTATION_POLICY_HASH)
            .map(String::as_str),
        Some(POLICY_HASH)
    );
    assert_eq!(
        spec.annotations
            .get(ANNOTATION_IMAGE_DIGEST)
            .map(String::as_str),
        Some("sha256:cafebabe")
    );
    assert_eq!(spec.image, IMAGE);
    assert_eq!(spec.namespace, "dagr");
}

#[test]
fn the_pod_never_restarts_itself_so_kubernetes_can_never_duplicate_an_attempt() {
    let spec = build_pod(&request("extract", 1), ClusterRetry::Disabled).expect("a spec is built");
    assert_eq!(
        spec.restart_policy, "Never",
        "ADR 115 §2: dagr owns retry through NodePolicy; a cluster-side restart \
         running alongside a dagr-level retry duplicates the same attempt"
    );
}

#[test]
fn a_configuration_enabling_cluster_side_retry_is_refused_naming_the_duplicate_execution_hazard() {
    let refusal =
        build_pod(&request("extract", 1), ClusterRetry::Enabled).expect_err("the executor refuses");
    let message = refusal.to_string();
    assert!(
        message.contains("retry"),
        "the refusal names what was configured: {message}"
    );
    assert!(
        message.to_lowercase().contains("duplicat"),
        "the refusal names the duplicate-execution hazard rather than just saying \
         no: {message}"
    );
}

#[test]
fn a_placement_travels_as_opaque_strings_the_engine_never_interprets() {
    let mut req = request("heavy", 1);
    req.placement = PodPlacement {
        cpu: Some("500m".to_string()),
        memory: Some("2Gi".to_string()),
        node_selectors: vec![("disktype".to_string(), "ssd".to_string())],
        tolerations: vec!["spot".to_string()],
    };
    let spec = build_pod(&req, ClusterRetry::Disabled).expect("a spec is built");
    assert_eq!(spec.cpu.as_deref(), Some("500m"));
    assert_eq!(spec.memory.as_deref(), Some("2Gi"));
    assert_eq!(
        spec.node_selectors,
        vec![("disktype".to_string(), "ssd".to_string())]
    );
    assert_eq!(spec.tolerations, vec!["spot".to_string()]);
}

#[test]
fn the_pod_name_is_a_pure_function_of_the_attempt_key_so_a_resubmission_addresses_the_same_pod() {
    let key = AttemptKey::new(RUN_ID, "extract", 3);
    assert_eq!(pod_name(&key), pod_name(&key), "deterministic");
    assert_ne!(
        pod_name(&key),
        pod_name(&AttemptKey::new(RUN_ID, "extract", 4)),
        "a retry addresses a DIFFERENT pod — a resubmission of attempt 3 must not \
         adopt attempt 4's work"
    );
    assert_ne!(
        pod_name(&key),
        pod_name(&AttemptKey::new(RUN_ID, "load", 3)),
        "two nodes of one run never collide"
    );
    let name = pod_name(&key);
    assert!(
        name.len() <= 63
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !name.starts_with('-')
            && !name.ends_with('-'),
        "an author-chosen node name must not be able to produce an illegal object \
         name: {name}"
    );
}

#[test]
fn a_node_name_that_is_not_a_legal_object_name_still_produces_one() {
    let key = AttemptKey::new(RUN_ID, "Extract/Rows — stage 1", 1);
    let name = pod_name(&key);
    assert!(
        name.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "the name is derived, not copied: {name}"
    );
}

// ---------------------------------------------------------------------------
// Pre-start detection — the T101 correction
// ---------------------------------------------------------------------------

fn pending_with_waiting(reason: &str) -> PodStatusFacts<'_> {
    PodStatusFacts {
        phase: PodPhase::Pending,
        waiting_reason: Some(reason),
        scheduling_refusal: None,
        pod_reason: None,
        container_reason: None,
        exit_code: None,
    }
}

#[test]
fn every_known_fatal_waiting_reason_is_a_pre_start_failure_while_the_pod_is_still_pending() {
    assert!(
        FATAL_WAITING_REASONS.contains(&"ImagePullBackOff")
            && FATAL_WAITING_REASONS.contains(&"ErrImagePull")
            && FATAL_WAITING_REASONS.contains(&"CreateContainerConfigError")
            && FATAL_WAITING_REASONS.contains(&"InvalidImageName"),
        "the set T101 measured is the set the runner bounds on"
    );
    for reason in FATAL_WAITING_REASONS {
        let failure = classify_pre_start(&pending_with_waiting(reason))
            .unwrap_or_else(|| panic!("`{reason}` is a pre-start failure"));
        assert_eq!(failure.surface, PreStartSurface::WaitingReason);
        assert_eq!(failure.reason, *reason);
    }
}

#[test]
fn a_transient_waiting_reason_is_not_a_pre_start_failure() {
    for reason in ["ContainerCreating", "PodInitializing"] {
        assert!(
            classify_pre_start(&pending_with_waiting(reason)).is_none(),
            "`{reason}` is the platform working, not the platform refusing"
        );
    }
}

#[test]
fn an_unschedulable_pod_is_a_pre_start_failure_even_though_it_has_no_container_status_at_all() {
    // T101 Run B: `0/1 nodes are available: 1 Insufficient memory` reports through
    // `conditions[PodScheduled].reason` and has NO containerStatuses entry, so a
    // waiting-reason check alone never sees it.
    let facts = PodStatusFacts {
        phase: PodPhase::Pending,
        waiting_reason: None,
        scheduling_refusal: Some("Unschedulable"),
        pod_reason: None,
        container_reason: None,
        exit_code: None,
    };
    let failure = classify_pre_start(&facts).expect("an unschedulable pod never starts");
    assert_eq!(failure.surface, PreStartSurface::Unschedulable);
    assert_eq!(failure.reason, "Unschedulable");
}

#[test]
fn a_pending_pod_with_nothing_wrong_is_not_yet_a_failure() {
    let facts = PodStatusFacts {
        phase: PodPhase::Pending,
        waiting_reason: None,
        scheduling_refusal: None,
        pod_reason: None,
        container_reason: None,
        exit_code: None,
    };
    assert!(
        classify_pre_start(&facts).is_none(),
        "a pod that is merely still pending is the normal case"
    );
}

#[test]
fn a_pod_that_started_is_never_reclassified_as_a_pre_start_failure() {
    // The budgets are only separable if "the container ran" is decidable. A running
    // or terminal pod's troubles are the NODE's retry budget, whatever they are.
    for phase in [PodPhase::Running, PodPhase::Succeeded, PodPhase::Failed] {
        let facts = PodStatusFacts {
            phase,
            waiting_reason: Some("ImagePullBackOff"),
            scheduling_refusal: Some("Unschedulable"),
            pod_reason: Some("Evicted"),
            container_reason: Some("OOMKilled"),
            exit_code: Some(137),
        };
        assert!(
            classify_pre_start(&facts).is_none(),
            "{phase} is past the pre-start window"
        );
    }
}

// ---------------------------------------------------------------------------
// Terminal classification — evidence, not invention
// ---------------------------------------------------------------------------

#[test]
fn a_pod_that_succeeded_classifies_as_a_succeeded_attempt_with_no_diagnostics() {
    let verdict = classify_terminal(&PodStatusFacts {
        phase: PodPhase::Succeeded,
        waiting_reason: None,
        scheduling_refusal: None,
        pod_reason: None,
        container_reason: None,
        exit_code: Some(0),
    });
    assert_eq!(verdict.outcome, PodOutcome::Succeeded);
    assert!(verdict.diagnostics.is_empty());
}

#[test]
fn an_out_of_memory_kill_is_a_failed_attempt_carrying_a_diagnostic_and_not_a_new_state() {
    // T101: OOMKilled appears on the CONTAINER with an empty pod reason.
    let verdict = classify_terminal(&PodStatusFacts {
        phase: PodPhase::Failed,
        waiting_reason: None,
        scheduling_refusal: None,
        pod_reason: None,
        container_reason: Some("OOMKilled"),
        exit_code: Some(137),
    });
    assert_eq!(verdict.outcome, PodOutcome::Failed);
    assert!(
        verdict.diagnostics.iter().any(|d| d.contains("OOMKilled")),
        "the platform's reason is carried as a diagnostic string: {:?}",
        verdict.diagnostics
    );
}

#[test]
fn an_eviction_is_read_off_the_pod_reason_which_a_container_only_reader_would_miss() {
    // T101: Evicted appears on the POD, with the container reporting a generic Error.
    let verdict = classify_terminal(&PodStatusFacts {
        phase: PodPhase::Failed,
        waiting_reason: None,
        scheduling_refusal: None,
        pod_reason: Some("Evicted"),
        container_reason: Some("Error"),
        exit_code: Some(137),
    });
    assert_eq!(verdict.outcome, PodOutcome::Failed);
    assert!(
        verdict.diagnostics.iter().any(|d| d.contains("Evicted")),
        "both reason fields are read: {:?}",
        verdict.diagnostics
    );
}

#[test]
fn exit_code_137_alone_never_decides_between_an_oom_kill_and_an_external_termination() {
    let oom = classify_terminal(&PodStatusFacts {
        phase: PodPhase::Failed,
        waiting_reason: None,
        scheduling_refusal: None,
        pod_reason: None,
        container_reason: Some("OOMKilled"),
        exit_code: Some(137),
    });
    let killed = classify_terminal(&PodStatusFacts {
        phase: PodPhase::Failed,
        waiting_reason: None,
        scheduling_refusal: None,
        pod_reason: None,
        container_reason: Some("Error"),
        exit_code: Some(137),
    });
    assert_eq!(oom.outcome, killed.outcome, "the same code, both failures");
    assert_ne!(
        oom.diagnostics, killed.diagnostics,
        "…and only the reason separates them, which is why both are carried"
    );
}

// ---------------------------------------------------------------------------
// No credential ever reaches a record, a label, or an annotation
// ---------------------------------------------------------------------------

#[test]
fn an_opaque_blob_reference_carries_no_credential_and_is_accepted() {
    reject_credential_bearing("dagr-blob+local://blobs/sha256/2c26b46b")
        .expect("an opaque content-addressed reference is exactly what may be recorded");
    reject_credential_bearing("s3://bucket/key").expect("a bare object path is fine");
}

#[test]
fn a_presigned_or_otherwise_secret_bearing_url_is_rejected_before_it_can_be_recorded() {
    for uri in [
        "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=deadbeef&X-Amz-Expires=900",
        "https://store.example/key?signature=abc",
        "https://user:hunter2@store.example/key",
        "https://store.example/key?token=abc",
        "https://store.example/key?access_key_id=AKIA",
    ] {
        let err =
            reject_credential_bearing(uri).expect_err("a credential-bearing reference is refused");
        let message = err.to_string();
        assert!(
            !message.contains("deadbeef")
                && !message.contains("hunter2")
                && !message.contains("AKIA"),
            "the refusal must not leak the credential it refused: {message}"
        );
    }
}

// ---------------------------------------------------------------------------
// The snapshot carries the facts the three surfaces need
// ---------------------------------------------------------------------------

#[test]
fn a_snapshot_carries_the_platform_identity_and_the_pre_start_surfaces() {
    let mut snap = PodSnapshot::new(
        "dagr-extract-1",
        "100",
        PodPhase::Pending,
        &identity("extract", 1),
    );
    snap.uid = Some("6f0f1b2c".to_string());
    snap.host = Some("kind-worker2".to_string());
    snap.waiting_reason = Some("ImagePullBackOff".to_string());
    snap.scheduling_refusal = None;

    let facts = PodStatusFacts::from(&snap);
    assert_eq!(facts.waiting_reason, Some("ImagePullBackOff"));
    assert!(
        classify_pre_start(&facts).is_some(),
        "the facts a snapshot yields are the facts the classifier reads"
    );

    // A pod with no diagnostics at all still yields readable facts.
    let clean = PodSnapshot::new(
        "dagr-load-1",
        "101",
        PodPhase::Running,
        &identity("load", 1),
    );
    assert!(classify_pre_start(&PodStatusFacts::from(&clean)).is_none());
    assert_eq!(clean.labels.len(), 4);
    assert!(!clean.annotations.is_empty());
    let _: &BTreeMap<String, String> = &clean.annotations;
}
