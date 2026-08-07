//! T112 · **the complete least-privilege RBAC the orchestrator needs.** Written
//! first (TDD).
//!
//! T107 shipped the *observer's* grant — `get`/`list`/`watch` on pods in one
//! namespace — and its own test fails if a write verb appears there. That file is
//! deliberately unchanged: an operator who only wants to observe still applies it
//! alone.
//!
//! What was missing, and what T108/T109 both routed here, is the grant the
//! **orchestrator** needs once it actually submits and reclaims pods: `create` and
//! `delete` (the executor's), and `patch` (adoption's ownership label). Those three
//! plus the observer's three are the whole set, in **one** namespace, on **one**
//! resource, and nothing else.
//!
//! These assertions are that "nothing else". A manifest is documentation until
//! something fails when it widens.

/// The manifest as shipped. `include_str!` rather than a path read, so the test
/// cannot pass against a file that is missing from the package.
const MANIFEST: &str = include_str!("../manifests/dagr-orchestrator-rbac.yaml");

/// The observer-only manifest, which this one does **not** replace.
const OBSERVER_MANIFEST: &str = include_str!("../manifests/pod-observer-rbac.yaml");

/// Every non-comment, non-empty line — comments explain what is *absent*, and an
/// absence assertion that matched a comment would be worthless.
fn body(manifest: &str) -> String {
    manifest
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

/// **Definition of done: create, get, list, watch, delete and patch on pods in ONE
/// namespace, and nothing else.**
#[test]
fn the_orchestrator_role_grants_exactly_the_six_verbs_on_pods_in_one_namespace() {
    let body = body(MANIFEST);

    assert!(
        body.contains("kind: Role"),
        "the grant is a namespaced Role"
    );
    assert!(
        body.contains("kind: RoleBinding"),
        "the binding is namespaced too"
    );
    assert!(
        body.contains("kind: ServiceAccount"),
        "the identity the binding names ships with it"
    );

    assert!(
        body.contains(r#"resources: ["pods"]"#),
        "pods, and nothing else"
    );
    assert!(
        body.contains(r#"apiGroups: [""]"#),
        "the core API group, explicitly"
    );

    // The verb list is written once, in a fixed order, so a reviewer can diff it.
    assert!(
        body.contains(r#"verbs: ["create", "delete", "get", "list", "patch", "watch"]"#),
        "exactly the six verbs the orchestrator uses, in sorted order: {body}"
    );

    // Exactly one rule. A second rule is a second grant, whatever it says.
    assert_eq!(
        body.matches("- apiGroups:").count(),
        1,
        "the Role has exactly one rule"
    );
    assert_eq!(
        body.matches("verbs:").count(),
        1,
        "…and therefore exactly one verb list"
    );

    // Every object is namespaced: a manifest with a cluster-scoped object would
    // grant across namespaces however narrow its verbs are.
    let namespaced = body.matches("namespace: dagr").count();
    assert!(
        namespaced >= 4,
        "each of the three objects (and the binding subject) is namespaced; found {namespaced}"
    );
}

/// The absences are the point. Each of these would be a real widening.
#[test]
fn the_orchestrator_manifest_grants_nothing_cluster_wide_and_no_second_resource() {
    let body = body(MANIFEST);

    for forbidden in [
        "kind: ClusterRole",
        "kind: ClusterRoleBinding",
        // Sub-resources dagr does not read or drive.
        "pods/log",
        "pods/exec",
        "pods/portforward",
        "pods/attach",
        "pods/eviction",
        // Other resources: a Job or a CronJob would be cluster-side retry and a
        // control plane outliving the run, both of which ADR 115 forbids.
        "jobs",
        "cronjobs",
        "configmaps",
        "secrets",
        "events",
        "nodes",
        "persistentvolumeclaims",
        // Verbs beyond the six.
        "\"update\"",
        "\"deletecollection\"",
        "\"*\"",
        "\"bind\"",
        "\"escalate\"",
        "\"impersonate\"",
    ] {
        assert!(
            !body.contains(forbidden),
            "the orchestrator's manifest must not contain {forbidden:?} — the grant is \
             six verbs on pods in one namespace and nothing else"
        );
    }
}

/// The observer's narrower manifest is **not** widened or deleted by this ticket:
/// the two ports exist precisely to keep the read grant and the write grant
/// separable, and an operator who wants only the read half still has a file to
/// apply.
#[test]
fn the_observer_only_manifest_is_still_read_only_and_still_ships() {
    let observer = body(OBSERVER_MANIFEST);
    assert!(
        observer.contains(r#"verbs: ["get", "list", "watch"]"#),
        "the observer's manifest is unchanged: three read verbs"
    );
    for write_verb in ["\"create\"", "\"delete\"", "\"patch\""] {
        assert!(
            !observer.contains(write_verb),
            "the observer's manifest must not acquire {write_verb} — the wider grant is a \
             separate file"
        );
    }
}

/// The two manifests must not collide when both are applied: they bind the **same**
/// `ServiceAccount` (an operator who applies the wide one alone still has an
/// identity), but they must be distinct `Role`/`RoleBinding` objects or the second
/// apply silently replaces the first.
#[test]
fn the_two_manifests_name_the_same_service_account_and_different_roles() {
    assert!(
        MANIFEST.contains("name: dagr-orchestrator")
            && OBSERVER_MANIFEST.contains("name: dagr-orchestrator"),
        "both manifests bind the same ServiceAccount identity"
    );
    assert!(
        MANIFEST.contains("name: dagr-orchestrator-pods"),
        "the wide Role has its own name"
    );
    assert!(
        !OBSERVER_MANIFEST.contains("dagr-orchestrator-pods"),
        "…which the observer-only file does not use"
    );
}

/// The scan is not vacuous: the strings it looks for are the ones the file actually
/// uses, so a renamed field cannot turn every absence assertion green.
#[test]
fn the_orchestrator_manifest_scan_is_not_vacuous() {
    let body = body(MANIFEST);
    assert!(body.contains("rbac.authorization.k8s.io/v1"));
    assert!(body.contains("rules:"));
    assert!(body.contains("roleRef:"));
    assert!(body.contains("subjects:"));
    assert!(
        MANIFEST
            .lines()
            .filter(|l| l.trim_start().starts_with('#'))
            .count()
            > 5,
        "the manifest explains why each verb is there, or the next reader widens it by accident"
    );
    // Every verb the classifier knows must be named somewhere in the file, so the
    // shipped grant and the shipped diagnostic cannot drift apart.
    for verb in dagr_k8s::rbac::PodVerb::ALL {
        assert!(
            body.contains(&format!("\"{}\"", verb.as_str())),
            "the manifest grants `{verb}`, which the diagnostic can name"
        );
    }
}
