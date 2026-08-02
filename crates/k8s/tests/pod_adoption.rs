//! T109 · **orphan adoption, tombstones and ownership revocation — the pure
//! half.** Written first (TDD).
//!
//! Everything here is a function of values: which pods a restart may reclaim,
//! which label patch each act writes, how several pods claiming one attempt are
//! resolved, and how a watcher tells an orchestrator-initiated teardown from an
//! external deletion. No I/O, no clock, no cluster — the half that lists, patches
//! and deletes is `dagr_cli::adoption`, and its suite is
//! `crates/cli/tests/orphan_adoption_and_ownership.rs`.
//!
//! The property the whole ticket exists for: an orchestrator that dies leaves pods
//! running, and the next process must **reclaim** them rather than duplicate the
//! work (and, for a task with side effects, duplicate the side effects) or leak
//! them.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use dagr_k8s::adoption::{
    AdoptionRefusal, AdoptionVerdict, BuildIdentity, DeletionOrigin, adoption_patch,
    adoption_selector, classify, deletion_origin, plan, revocation_patch, tombstone_patch,
};
use dagr_k8s::api::{PodPhase, PodSnapshot};
use dagr_k8s::identity::{
    AttemptIdentity, AttemptKey, LABEL_COMPLETE, LABEL_OWNER, TOMBSTONE_VALUE,
};
use support::{FINGERPRINT, RUN_ID, identity};

/// The digest the shared fixture's identity annotates. The build a restart
/// compares against has to be *this* one, or every pod would be refused for the
/// wrong reason.
const IMAGE_DIGEST: &str =
    "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
/// Likewise the tool version.
const TOOL_VERSION: &str = "dagr 0.0.0";

/// This binary, as a discovered pod's annotations must describe it.
fn build() -> BuildIdentity {
    BuildIdentity::new(FINGERPRINT, TOOL_VERSION, IMAGE_DIGEST)
}

/// A pod named `name` running `node`'s attempt, carrying the canonical labels and
/// annotations.
fn pod(name: &str, node: &str, attempt: u32, phase: PodPhase) -> PodSnapshot {
    PodSnapshot::new(name, "100", phase, &identity(node, attempt))
}

/// The same pod with one annotation rewritten — the only way to build a pod that
/// is shaped like ours and belongs to a different program.
fn pod_with_annotation(name: &str, key: &str, value: &str) -> PodSnapshot {
    let mut snapshot = pod(name, "extract", 1, PodPhase::Running);
    snapshot
        .annotations
        .insert(key.to_string(), value.to_string());
    snapshot
}

/// Every node in the graph runs, which is the ordinary (non-resume) case.
fn everything() -> BTreeSet<String> {
    BTreeSet::from(["extract".to_string(), "load".to_string()])
}

// ===========================================================================
// Discovery narrows to the run, and excludes what has already been consumed
// ===========================================================================

/// **Definition of done: startup discovers the run's pods by label, excluding
/// tombstoned ones.**
#[test]
fn the_discovery_selector_narrows_to_this_run_and_excludes_the_completion_tombstone() {
    let selector = adoption_selector(RUN_ID);

    assert!(
        selector.contains(&format!("dagr.io/run-id={RUN_ID}")),
        "the run is the one term a selector can rely on: {selector}"
    );
    assert!(
        selector.contains("dagr.io/complete!=true"),
        "a pod whose outcome has been consumed is excluded by the SAME key the \
         tombstone writes, so a finished attempt is never adopted twice: {selector}"
    );
    assert!(
        selector.contains(LABEL_COMPLETE),
        "…and it is that key, not a second one: {selector}"
    );
}

#[test]
fn a_tombstoned_pod_is_refused_even_when_the_selector_did_not_filter_it_out() {
    // Belt and braces on purpose: the selector is a request to a server, and a
    // server that ignored it (or a caller that listed without it) must not be able
    // to make a consumed outcome adoptable.
    let mut tombstoned = pod("dagr-extract-1", "extract", 1, PodPhase::Succeeded);
    tombstoned
        .labels
        .insert(LABEL_COMPLETE.to_string(), TOMBSTONE_VALUE.to_string());

    match classify(&tombstoned, RUN_ID, &build()) {
        AdoptionVerdict::Refuse(AdoptionRefusal::Tombstoned) => {}
        other => panic!("a consumed outcome is never adopted again: {other:?}"),
    }

    let plan = plan(&[tombstoned], RUN_ID, &build(), &everything());
    assert!(plan.adopt.is_empty(), "nothing is adopted");
    assert!(
        plan.revoke.is_empty(),
        "and nothing is revoked either — its outcome is already in the record, so \
         deleting it is a cleanup decision, not this pass's"
    );
    assert_eq!(
        plan.tombstoned.keys().collect::<Vec<_>>(),
        vec![&AttemptKey::new(RUN_ID, "extract", 1)],
        "it is reported as an already-consumed outcome"
    );
}

// ===========================================================================
// Adoption, and the three ways a pod belongs to a different program
// ===========================================================================

#[test]
fn a_pod_annotated_with_this_build_is_adoptable() {
    let live = pod("dagr-extract-1", "extract", 1, PodPhase::Running);
    match classify(&live, RUN_ID, &build()) {
        AdoptionVerdict::Adopt(identity) => {
            assert_eq!(identity.key, AttemptKey::new(RUN_ID, "extract", 1));
        }
        other => panic!("this run's pod, from this build, is adoptable: {other:?}"),
    }
}

/// **Definition of done: a pod whose annotated fingerprint, tool version, or image
/// digest differs is refused, left untouched, and reported with both values
/// named.**
#[test]
fn a_pod_from_a_different_structural_fingerprint_is_refused_naming_both_fingerprints() {
    let foreign = pod_with_annotation(
        "dagr-extract-1",
        "dagr.io/structural-fingerprint",
        "sf-a-different-graph",
    );

    let refusal = match classify(&foreign, RUN_ID, &build()) {
        AdoptionVerdict::Refuse(refusal) => refusal,
        other => panic!("a different program's pod is never adopted: {other:?}"),
    };
    assert!(
        matches!(refusal, AdoptionRefusal::StructuralFingerprint { .. }),
        "the surface that disagreed is named: {refusal:?}"
    );
    let message = refusal.to_string();
    assert!(
        message.contains(FINGERPRINT) && message.contains("sf-a-different-graph"),
        "both values are named, so an operator can see which program it belongs \
         to: {message}"
    );
}

#[test]
fn a_pod_from_a_different_tool_version_is_refused_naming_both_versions() {
    let foreign = pod_with_annotation("dagr-extract-1", "dagr.io/tool-version", "dagr 0.0.1");

    let refusal = match classify(&foreign, RUN_ID, &build()) {
        AdoptionVerdict::Refuse(refusal) => refusal,
        other => panic!("a different tool version's pod is never adopted: {other:?}"),
    };
    assert!(matches!(refusal, AdoptionRefusal::ToolVersion { .. }));
    let message = refusal.to_string();
    assert!(
        message.contains(TOOL_VERSION) && message.contains("dagr 0.0.1"),
        "both versions are named: {message}"
    );
}

#[test]
fn a_pod_from_a_different_image_digest_is_refused_naming_both_digests() {
    let foreign = pod_with_annotation("dagr-extract-1", "dagr.io/image-digest", "sha256:decafbad");

    let refusal = match classify(&foreign, RUN_ID, &build()) {
        AdoptionVerdict::Refuse(refusal) => refusal,
        other => panic!("a different image's pod is never adopted: {other:?}"),
    };
    assert!(matches!(refusal, AdoptionRefusal::ImageDigest { .. }));
    let message = refusal.to_string();
    assert!(
        message.contains(IMAGE_DIGEST) && message.contains("sha256:decafbad"),
        "both digests are named: {message}"
    );
}

#[test]
fn a_refused_pod_is_reported_against_its_attempt_and_never_revoked() {
    // "Report it, LEAVE IT ALONE, and fail the node." Deleting another program's
    // pod is not dagr's call, so a refusal produces no revocation.
    let foreign = pod_with_annotation(
        "dagr-extract-1",
        "dagr.io/structural-fingerprint",
        "sf-a-different-graph",
    );

    let plan = plan(&[foreign], RUN_ID, &build(), &everything());
    assert!(plan.adopt.is_empty());
    assert!(
        plan.revoke.is_empty(),
        "a foreign pod is left running, not deleted"
    );
    let key = AttemptKey::new(RUN_ID, "extract", 1);
    let refused = plan
        .refuse
        .get(&key)
        .expect("the refusal is keyed by attempt");
    assert_eq!(refused.name, "dagr-extract-1");
}

#[test]
fn a_foreign_build_alongside_one_of_ours_reports_rather_than_failing_the_node() {
    // A refusal exists because the attempt's object name is occupied by work dagr
    // did not launch and cannot submit through. When the attempt ALSO has a pod of
    // ours, that reasoning does not apply: object names are unique, so the two are
    // different objects, and ours is genuinely ours.
    let ours = pod("dagr-aaaa-extract-1", "extract", 1, PodPhase::Running);
    let theirs = pod_with_annotation(
        "dagr-zzzz-extract-1",
        "dagr.io/structural-fingerprint",
        "sf-a-different-graph",
    );

    let plan = plan(&[ours, theirs], RUN_ID, &build(), &everything());
    assert_eq!(
        plan.adopt
            .get(&AttemptKey::new(RUN_ID, "extract", 1))
            .map(|p| p.name.as_str()),
        Some("dagr-aaaa-extract-1"),
        "our own pod is adopted"
    );
    assert!(
        plan.refuse.is_empty(),
        "the node is not failed on account of somebody else's object"
    );
    assert!(plan.revoke.is_empty(), "and theirs is not deleted");
    assert_eq!(plan.ignored.len(), 1, "it is reported, and left running");
}

#[test]
fn a_pod_from_another_run_is_not_this_runs_business_at_all() {
    let mut other_run = pod("dagr-extract-1", "extract", 1, PodPhase::Running);
    other_run.labels.insert(
        "dagr.io/run-id".to_string(),
        "0197ffff-0000-7000-8000-000000000000".to_string(),
    );

    let plan = plan(&[other_run], RUN_ID, &build(), &everything());
    assert!(plan.adopt.is_empty(), "adoption is scoped to one run id");
    assert!(plan.revoke.is_empty(), "and touches nothing outside it");
    assert!(plan.refuse.is_empty(), "…and fails no node of this run's");
    assert_eq!(plan.ignored.len(), 1, "it is reported, and left alone");
}

// ===========================================================================
// The three patches, each of which writes exactly one key
// ===========================================================================

/// **Definition of done: adoption patches *only* the owner label.**
#[test]
fn the_adoption_patch_rewrites_the_owner_key_and_nothing_else() {
    let patch = adoption_patch("orchestrator-02");

    assert_eq!(
        patch.keys().collect::<Vec<_>>(),
        vec![LABEL_OWNER],
        "one key. Adoption is a labels-only patch of the owner — never a pod \
         recreation, and never a rewrite of anything the pod is running"
    );
    assert_eq!(
        patch.get(LABEL_OWNER),
        Some(&Some("orchestrator-02".to_string()))
    );
}

#[test]
fn the_revocation_patch_clears_the_owner_key_and_nothing_else() {
    let patch = revocation_patch();

    assert_eq!(patch.keys().collect::<Vec<_>>(), vec![LABEL_OWNER]);
    assert_eq!(
        patch.get(LABEL_OWNER),
        Some(&None),
        "a merge patch removes a key by setting it to null — the owner is CLEARED, \
         which is what a watcher reads to tell our teardown from someone else's \
         delete"
    );
}

#[test]
fn the_tombstone_patch_writes_the_completion_key_the_selector_excludes() {
    let patch = tombstone_patch();

    assert_eq!(patch.keys().collect::<Vec<_>>(), vec![LABEL_COMPLETE]);
    assert_eq!(
        patch.get(LABEL_COMPLETE),
        Some(&Some(TOMBSTONE_VALUE.to_string()))
    );
    assert!(
        adoption_selector(RUN_ID).contains(LABEL_COMPLETE),
        "the key written is the key discovery filters on — one constant, not two"
    );
}

// ===========================================================================
// Ambiguity: several pods claiming one attempt
// ===========================================================================

/// **Definition of done: two pods for one attempt key resolve deterministically to
/// one adoption and one revocation.**
#[test]
fn two_pods_for_one_attempt_key_resolve_to_one_adoption_and_one_revocation() {
    let first = pod("dagr-aaaa-extract-1", "extract", 1, PodPhase::Running);
    let second = pod("dagr-zzzz-extract-1", "extract", 1, PodPhase::Running);

    let plan = plan(
        &[first.clone(), second.clone()],
        RUN_ID,
        &build(),
        &everything(),
    );
    let key = AttemptKey::new(RUN_ID, "extract", 1);
    assert_eq!(
        plan.adopt.get(&key).map(|p| p.name.as_str()),
        Some("dagr-aaaa-extract-1"),
        "exactly one is adopted, and which one is a total order over the object \
         name rather than the order the API happened to list them in"
    );
    assert_eq!(
        plan.revoke,
        vec!["dagr-zzzz-extract-1".to_string()],
        "the other is revoked, so the attempt has exactly one live pod"
    );
}

#[test]
fn the_resolution_does_not_depend_on_the_order_the_api_listed_them() {
    let first = pod("dagr-aaaa-extract-1", "extract", 1, PodPhase::Running);
    let second = pod("dagr-zzzz-extract-1", "extract", 1, PodPhase::Running);

    let forwards = plan(
        &[first.clone(), second.clone()],
        RUN_ID,
        &build(),
        &everything(),
    );
    let backwards = plan(&[second, first], RUN_ID, &build(), &everything());

    assert_eq!(
        forwards
            .adopt
            .get(&AttemptKey::new(RUN_ID, "extract", 1))
            .map(|p| p.name.clone()),
        backwards
            .adopt
            .get(&AttemptKey::new(RUN_ID, "extract", 1))
            .map(|p| p.name.clone()),
        "deterministic means deterministic: a listing is a set, and the answer \
         cannot depend on its enumeration"
    );
    assert_eq!(forwards.revoke, backwards.revoke);
}

// ===========================================================================
// Composition with resume: a node that will not run seeks no pod
// ===========================================================================

/// **Definition of done: `satisfied-from-prior` nodes seek no pod.**
#[test]
fn a_pod_for_a_node_that_will_not_run_is_neither_adopted_nor_revoked() {
    let live = pod("dagr-extract-1", "extract", 1, PodPhase::Running);
    // Resume marked `extract` satisfied-from-prior: it has no runner at all, so
    // there is no attempt to reclaim and nothing to wait on.
    let must_run = BTreeSet::from(["load".to_string()]);

    let plan = plan(&[live], RUN_ID, &build(), &must_run);
    assert!(
        plan.adopt.is_empty(),
        "a node that will not run seeks no pod"
    );
    assert!(
        plan.revoke.is_empty(),
        "and dagr does not delete a pod it never intended to wait for"
    );
    assert_eq!(
        plan.unclaimed.keys().collect::<Vec<_>>(),
        vec![&AttemptKey::new(RUN_ID, "extract", 1)],
        "it is reported so the operator can see it"
    );
}

// ===========================================================================
// A revoked pod is distinguishable from an externally deleted one
// ===========================================================================

/// **Definition of done: revocation clears the owner label *then* deletes.** This
/// is the half that says why the ordering matters: the cleared label is the only
/// thing a watcher can read off the deletion.
#[test]
fn a_revoked_pod_is_distinguishable_from_an_externally_deleted_one() {
    let ours = identity("extract", 1);
    let external: BTreeMap<String, String> = ours.labels();
    let mut revoked = external.clone();
    revoked.remove(LABEL_OWNER);

    assert_eq!(
        deletion_origin(&revoked),
        DeletionOrigin::Revoked,
        "we cleared the owner before deleting, so this teardown is ours"
    );
    assert_eq!(
        deletion_origin(&external),
        DeletionOrigin::External,
        "a pod that still carries an owner when it disappears was deleted by \
         somebody else — which is a different event and a different diagnosis"
    );
}

#[test]
fn the_identity_of_a_revoked_pod_no_longer_attributes_it_to_a_waiter() {
    // The consequence of clearing the owner, stated as a test rather than left to
    // be discovered: a revoked pod stops being attributable, so its deletion can
    // never retire the waiter of the pod that WAS adopted for the same attempt.
    let ours = identity("extract", 1);
    let mut revoked = ours.labels();
    revoked.remove(LABEL_OWNER);

    assert!(
        dagr_k8s::identity::identify(&revoked, &ours.annotations()).is_err(),
        "an unowned pod is not identified as an attempt's, which is what keeps a \
         revoked duplicate from deciding the adopted pod's fate"
    );
}

// ===========================================================================
// The whole plan over a mixed listing
// ===========================================================================

#[test]
fn one_pass_over_a_mixed_listing_sorts_every_pod_into_exactly_one_outcome() {
    let mut tombstoned = pod("dagr-load-1", "load", 1, PodPhase::Succeeded);
    tombstoned
        .labels
        .insert(LABEL_COMPLETE.to_string(), TOMBSTONE_VALUE.to_string());
    let mut foreign = pod("dagr-foreign-load-2", "load", 2, PodPhase::Running);
    foreign.annotations.insert(
        "dagr.io/image-digest".to_string(),
        "sha256:decafbad".to_string(),
    );
    let pods = vec![
        pod("dagr-aaaa-extract-1", "extract", 1, PodPhase::Running),
        pod("dagr-zzzz-extract-1", "extract", 1, PodPhase::Running),
        tombstoned,
        foreign,
    ];

    let plan = plan(&pods, RUN_ID, &build(), &everything());

    assert_eq!(plan.adopt.len(), 1, "one adoption");
    assert_eq!(plan.revoke.len(), 1, "one revocation");
    assert_eq!(plan.tombstoned.len(), 1, "one consumed outcome");
    assert_eq!(plan.refuse.len(), 1, "one refusal");
    assert_eq!(
        plan.adopt.len() + plan.revoke.len() + plan.tombstoned.len() + plan.refuse.len(),
        pods.len(),
        "every discovered pod lands in exactly one outcome — a pod that fell \
         through would be one nobody is waiting for and nobody will clean up"
    );
}

#[test]
fn an_unidentifiable_pod_is_reported_rather_than_attributed() {
    let bare = PodSnapshot {
        name: "someone-elses-pod".to_string(),
        resource_version: "1".to_string(),
        phase: PodPhase::Running,
        labels: BTreeMap::from([("dagr.io/run-id".to_string(), RUN_ID.to_string())]),
        annotations: BTreeMap::new(),
        pod_reason: None,
        container_reason: None,
        exit_code: None,
        uid: None,
        host: None,
        waiting_reason: None,
        scheduling_refusal: None,
    };

    let plan = plan(&[bare], RUN_ID, &build(), &everything());
    assert!(plan.adopt.is_empty());
    assert!(plan.revoke.is_empty(), "guessing is worse than reporting");
    assert_eq!(plan.ignored.len(), 1);
    assert!(matches!(
        plan.ignored[0].refusal,
        AdoptionRefusal::Unidentifiable(_)
    ));
}

/// A node's *identity* is what a build comparison reads, so the fixture the whole
/// suite rests on has to actually carry this build.
#[test]
fn the_fixture_identity_annotates_the_build_the_suite_compares_against() {
    let annotations = AttemptIdentity::annotations(&identity("extract", 1));
    assert_eq!(
        annotations.get("dagr.io/tool-version").map(String::as_str),
        Some(TOOL_VERSION)
    );
    assert_eq!(
        annotations.get("dagr.io/image-digest").map(String::as_str),
        Some(IMAGE_DIGEST)
    );
}
