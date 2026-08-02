//! Identity: labels are lossy selectors, annotations are authoritative.
//!
//! Kubernetes caps a label *value* at 63 characters. dagr's run ids are
//! 36-character `UUIDv7` values before any node name is appended, so a label cannot
//! carry identity without truncating — which is why annotations exist and why the
//! observer reads them on every path. These are the tests that make that claim
//! mean something: a truncation collision is *constructed*, and the two colliding
//! nodes are still told apart.

mod support;

use dagr_k8s::identity::{
    ANNOTATION_IMAGE_DIGEST, ANNOTATION_NODE, ANNOTATION_PIPELINE, ANNOTATION_POLICY_HASH,
    ANNOTATION_STRUCTURAL_FINGERPRINT, ANNOTATION_TOOL_VERSION, IdentityError, LABEL_ATTEMPT,
    LABEL_COMPLETE, LABEL_NODE, LABEL_OWNER, LABEL_RUN_ID, LABEL_VALUE_MAX, TOMBSTONE_VALUE,
    identify, is_valid_label_value, node_label,
};
use support::{FINGERPRINT, POLICY_HASH, RUN_ID, identity};

/// A node name long enough that its label must truncate, with the characters a
/// label value may not contain.
const LONG_NODE: &str =
    "warehouse::ingest::partition_by_day::normalise_customer_events::write_parquet_shards";

/// **Test-plan scenario: every emitted label value is ≤63 characters and valid.**
/// Setup: the 36-character run id and a very long node name. Action: emit the
/// label set. Expected: every value is inside the platform ceiling and is
/// syntactically a legal label value.
#[test]
fn every_emitted_label_value_is_within_the_ceiling_and_syntactically_valid() {
    let id = identity(LONG_NODE, 7);
    let labels = id.labels();

    assert_eq!(
        RUN_ID.len(),
        36,
        "the fixture must exercise a real UUID length"
    );
    assert!(
        LONG_NODE.len() > LABEL_VALUE_MAX,
        "the fixture node name must be longer than a label value can hold"
    );

    for (key, value) in &labels {
        assert!(
            value.len() <= LABEL_VALUE_MAX,
            "label {key}={value} is {} characters, over the {LABEL_VALUE_MAX} ceiling",
            value.len()
        );
        assert!(
            is_valid_label_value(value),
            "label {key}={value} is not a syntactically valid label value"
        );
    }

    // The five selector keys ADR 115 §4 names, and nothing else: a label set that
    // grew a sixth key would be identity leaking back into the lossy half.
    let keys: Vec<&str> = labels.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        vec![LABEL_ATTEMPT, LABEL_NODE, LABEL_OWNER, LABEL_RUN_ID]
    );
    assert_eq!(labels.get(LABEL_RUN_ID).map(String::as_str), Some(RUN_ID));
    assert_eq!(labels.get(LABEL_ATTEMPT).map(String::as_str), Some("7"));

    // The completion tombstone key is shipped as a *key* — the mechanism that
    // writes it belongs to the adoption ticket — so it must not be on a live pod.
    assert!(!labels.contains_key(LABEL_COMPLETE));
    assert!(is_valid_label_value(TOMBSTONE_VALUE));
}

/// **Test-plan scenario: two distinct node names that collide under label
/// truncation stay distinguishable via annotations.** This is the test that
/// justifies annotations existing at all.
#[test]
fn node_names_that_collide_under_label_truncation_stay_distinguishable_by_annotation() {
    let shared_prefix = "a".repeat(LABEL_VALUE_MAX + 4);
    let left = format!("{shared_prefix}::left");
    let right = format!("{shared_prefix}::right");
    assert_ne!(left, right);

    assert_eq!(
        node_label(&left),
        node_label(&right),
        "the fixture must actually collide under truncation, or the test proves nothing"
    );

    let left_id = identity(&left, 1);
    let right_id = identity(&right, 1);
    assert_eq!(
        left_id.labels().get(LABEL_NODE),
        right_id.labels().get(LABEL_NODE)
    );

    let left_back = identify(&left_id.labels(), &left_id.annotations()).expect("left identifies");
    let right_back =
        identify(&right_id.labels(), &right_id.annotations()).expect("right identifies");
    assert_eq!(left_back.key.node, left);
    assert_eq!(right_back.key.node, right);
    assert_ne!(left_back.key, right_back.key);
}

/// **Test-plan scenario: a pod's annotations round-trip the full node name,
/// pipeline name, both fingerprints, tool version, and image digest.**
#[test]
fn annotations_round_trip_the_authoritative_identity() {
    let id = identity(LONG_NODE, 3);
    let annotations = id.annotations();

    let keys: Vec<&str> = annotations.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        vec![
            ANNOTATION_IMAGE_DIGEST,
            ANNOTATION_NODE,
            ANNOTATION_PIPELINE,
            ANNOTATION_POLICY_HASH,
            ANNOTATION_STRUCTURAL_FINGERPRINT,
            ANNOTATION_TOOL_VERSION,
        ],
        "annotations carry exactly the six authoritative fields ADR 115 §4 names"
    );

    let back = identify(&id.labels(), &annotations).expect("a well-formed pod identifies");
    assert_eq!(back.key, id.key);
    assert_eq!(back.key.node, LONG_NODE, "the FULL node name survives");
    assert_eq!(back.pipeline, id.pipeline);
    assert_eq!(back.structural_fingerprint, FINGERPRINT);
    assert_eq!(back.policy_hash, POLICY_HASH);
    assert_eq!(back.tool_version, id.tool_version);
    assert_eq!(back.image_digest, id.image_digest);
    assert_eq!(back.owner, id.owner);
    assert!(!back.complete, "a live pod carries no completion tombstone");
}

/// A tombstoned pod reads back as complete — the key ships here, the mechanism
/// that writes it does not.
#[test]
fn a_tombstoned_pod_reads_back_as_complete() {
    let id = identity("extract", 1);
    let mut labels = id.labels();
    labels.insert(LABEL_COMPLETE.to_string(), TOMBSTONE_VALUE.to_string());

    let back = identify(&labels, &id.annotations()).expect("a tombstoned pod still identifies");
    assert!(back.complete);
}

/// A pod missing an authoritative annotation is not identifiable — and says which
/// one, because the alternative is attributing work to the wrong waiter.
#[test]
fn a_missing_annotation_is_a_named_identity_error() {
    let id = identity("extract", 1);
    let mut annotations = id.annotations();
    annotations.remove(ANNOTATION_NODE);

    let err = identify(&id.labels(), &annotations).expect_err("an unnamed pod cannot identify");
    match err {
        IdentityError::MissingAnnotation { key } => assert_eq!(key, ANNOTATION_NODE),
        other => panic!("expected a missing-annotation error, got {other:?}"),
    }

    let mut labels = id.labels();
    labels.remove(LABEL_ATTEMPT);
    let err =
        identify(&labels, &id.annotations()).expect_err("an attempt-less pod cannot identify");
    match err {
        IdentityError::MissingLabel { key } => assert_eq!(key, LABEL_ATTEMPT),
        other => panic!("expected a missing-label error, got {other:?}"),
    }

    let mut labels = id.labels();
    labels.insert(LABEL_ATTEMPT.to_string(), "second".to_string());
    let err = identify(&labels, &id.annotations()).expect_err("a non-numeric attempt is malformed");
    assert!(matches!(err, IdentityError::MalformedAttempt { .. }));
}

/// The label is derived from the annotation, so a pod whose two halves disagree
/// is malformed rather than "authoritative wins": disagreement means someone
/// edited one half, and dagr should say so instead of guessing.
#[test]
fn a_label_that_disagrees_with_its_annotation_is_malformed() {
    let id = identity("extract", 1);
    let mut labels = id.labels();
    labels.insert(LABEL_NODE.to_string(), "transform".to_string());

    let err = identify(&labels, &id.annotations()).expect_err("a disagreeing pod cannot identify");
    match err {
        IdentityError::NodeLabelMismatch { label, derived } => {
            assert_eq!(label, "transform");
            assert_eq!(derived, node_label("extract"));
        }
        other => panic!("expected a node-label mismatch, got {other:?}"),
    }
}

/// Label values are sanitized, not merely truncated: a node name is Rust source
/// identity and contains characters (`:`, `<`, `>`, spaces) a label value may not.
#[test]
fn label_values_sanitize_characters_a_label_may_not_carry() {
    for raw in [
        "warehouse::ingest",
        "map<String, Vec<u8>>",
        "node with spaces",
        "-leading-and-trailing-",
        "___",
    ] {
        let value = node_label(raw);
        assert!(
            is_valid_label_value(&value),
            "node_label({raw:?}) produced {value:?}, which is not a valid label value"
        );
        assert!(value.len() <= LABEL_VALUE_MAX);
    }

    // Sanitization is deterministic and non-empty even for a name with nothing a
    // label can keep, so a selector is always constructible.
    assert!(!node_label("///").is_empty());
    assert!(is_valid_label_value(&node_label("///")));
}

/// The owner key is a label, so it must survive the ceiling too — adoption
/// rewrites it, and a value that cannot be written is a mechanism that cannot run.
#[test]
fn the_owner_key_is_a_label_value_within_the_ceiling() {
    let mut id = identity("extract", 1);
    id.owner = "orchestrator-".to_string() + &"z".repeat(120);
    let labels = id.labels();
    let owner = labels.get(LABEL_OWNER).expect("the owner key is emitted");
    assert!(owner.len() <= LABEL_VALUE_MAX);
    assert!(is_valid_label_value(owner));
}
