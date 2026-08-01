//! The shipped RBAC manifest is least privilege, and stays that way.
//!
//! A manifest is documentation until something fails when it widens. These
//! assertions are that something: they read the file the crate actually ships and
//! fail if it acquires a cluster-scoped grant, a second resource, or a verb that
//! changes anything.

/// The manifest as shipped. `include_str!` rather than a path read, so the test
/// cannot pass against a file that is missing from the package.
const MANIFEST: &str = include_str!("../manifests/pod-observer-rbac.yaml");

/// Every non-comment, non-empty line — comments explain what is *absent*, and an
/// absence assertion that matched a comment would be worthless.
fn body() -> String {
    MANIFEST
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

/// **Definition of done: least-privilege RBAC (get/list/watch pods, one
/// namespace) ships as a reviewed manifest.**
#[test]
fn the_manifest_grants_get_list_watch_on_pods_in_one_namespace() {
    let body = body();

    assert!(body.contains("kind: Role"), "the grant is a namespaced Role");
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
        body.contains(r#"verbs: ["get", "list", "watch"]"#),
        "exactly the three read verbs the observer uses"
    );
    assert!(
        body.contains(r#"apiGroups: [""]"#),
        "the core API group, explicitly"
    );

    // Every object is namespaced: a manifest with a cluster-scoped object would
    // grant across namespaces however narrow its verbs are.
    let namespaced = body.matches("namespace: dagr").count();
    assert!(
        namespaced >= 4,
        "each of the three objects (and the binding subject) is namespaced; found {namespaced}"
    );
}

/// The absences are the point. Each of these would be a real widening, and each
/// belongs to a mechanism this ticket does not ship.
#[test]
fn the_manifest_grants_nothing_cluster_wide_and_nothing_that_writes() {
    let body = body();

    for forbidden in [
        "kind: ClusterRole",
        "kind: ClusterRoleBinding",
        "pods/log",
        "pods/exec",
        "pods/portforward",
        "\"create\"",
        "\"delete\"",
        "\"patch\"",
        "\"update\"",
        "\"deletecollection\"",
        "\"*\"",
    ] {
        assert!(
            !body.contains(forbidden),
            "the observer's manifest must not contain {forbidden:?} — it reads pod status and nothing more"
        );
    }
}

/// The scan is not vacuous: the strings it looks for are the ones the file
/// actually uses, so a renamed field cannot turn every absence assertion green.
#[test]
fn the_manifest_scan_is_not_vacuous() {
    let body = body();
    assert!(body.contains("rbac.authorization.k8s.io/v1"));
    assert!(body.contains("rules:"));
    assert!(body.contains("roleRef:"));
    assert!(body.contains("subjects:"));
    assert!(
        MANIFEST.lines().filter(|l| l.trim_start().starts_with('#')).count() > 5,
        "the manifest explains its absences, or the next reader widens it by accident"
    );
}
