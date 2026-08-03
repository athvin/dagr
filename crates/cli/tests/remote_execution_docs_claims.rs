//! T112 · **the remote-execution documentation says what dagr actually does.**
//! Written first (TDD).
//!
//! Deliberately **un-gated**: it carries no `#![cfg(feature = ...)]`, so a plain
//! `cargo test --workspace` reds the moment the prose and the shipped behaviour part
//! company. That is the point of the pattern `crates/cli/tests/metastore_docs_claims.rs`
//! established — documentation about an opt-in feature is exactly the documentation
//! nobody runs, so its guard must run everywhere.
//!
//! Two duties, in two bands below:
//!
//! 1. **The docs teach the shipped shape** — every flag, environment variable,
//!    default and verb list is compared against the shipped constant rather than
//!    retyped, so a renamed knob fails here instead of misleading an operator.
//! 2. **The docs claim nothing unshipped** — ADR 115's carve-out is narrow, and a
//!    README that promises a scheduler, a control plane, or cluster-side retry would
//!    widen it in the only place that matters, which is the reader's head.

use std::path::{Path, PathBuf};

use dagr_cli::config::{
    DAGR_EXECUTOR, DAGR_MAX_PODS, DAGR_POD_LAUNCH_RETRIES, EXECUTOR_FLAG, MAX_PODS_FLAG,
    POD_LAUNCH_RETRIES_DEFAULT, POD_LAUNCH_RETRIES_FLAG,
};
use dagr_cli::executor::ExecutorKind;

// ===========================================================================
// Helpers
// ===========================================================================

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

fn read_doc(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"))
}

/// Collapse whitespace, so a Markdown line-wrap cannot split a phrase across source
/// lines and quietly break a prose claim.
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The cookbook's remote-execution section, and only it — an unrelated mention
/// elsewhere in the file must never satisfy a claim about this one.
const COOKBOOK_HEADING: &str = "## Placing a node on remote compute";

fn cookbook_remote_section() -> String {
    let md = read_doc("docs/cookbook.md");
    let start = md
        .find(COOKBOOK_HEADING)
        .unwrap_or_else(|| panic!("the cookbook has a '{COOKBOOK_HEADING}' section"));
    let rest = &md[start..];
    // Skip this section's own `## ` so `find` lands on the *next* heading.
    let end = rest[3..].find("\n## ").map_or(rest.len(), |i| i + 3);
    rest[..end].to_string()
}

/// The README's remote-execution section.
const README_HEADING: &str = "## Remote execution";

fn readme_remote_section() -> String {
    let md = read_doc("README.md");
    let start = md
        .find(README_HEADING)
        .unwrap_or_else(|| panic!("the README has a '{README_HEADING}' section"));
    let rest = &md[start..];
    let end = rest[3..].find("\n## ").map_or(rest.len(), |i| i + 3);
    rest[..end].to_string()
}

// ===========================================================================
// 1. The docs teach the SHIPPED shape
// ===========================================================================

/// Every knob the section names is the knob that ships, spelled the way it ships.
#[test]
fn the_cookbook_documents_the_shipped_knobs_and_their_defaults() {
    let section = normalize_ws(&cookbook_remote_section());

    for spelling in [
        EXECUTOR_FLAG,
        DAGR_EXECUTOR,
        MAX_PODS_FLAG,
        DAGR_MAX_PODS,
        POD_LAUNCH_RETRIES_FLAG,
        DAGR_POD_LAUNCH_RETRIES,
    ] {
        assert!(
            section.contains(spelling),
            "the cookbook's remote section documents `{spelling}`"
        );
    }

    // The closed executor set, both names.
    for kind in ExecutorKind::ALL {
        assert!(
            section.contains(kind.as_str()),
            "the section names the `{}` executor",
            kind.as_str()
        );
    }

    // The defaults an operator relies on: local, unlimited pods, two launch retries.
    let lower = section.to_lowercase();
    assert!(
        lower.contains("default") && lower.contains("local"),
        "the section says the default executor is `local`"
    );
    assert!(
        section.contains(&POD_LAUNCH_RETRIES_DEFAULT.to_string()),
        "the section states the launch-retry default ({POD_LAUNCH_RETRIES_DEFAULT})"
    );
    assert!(
        lower.contains("unlimited") || lower.contains("unconstrained") || lower.contains("uncapped"),
        "the section says the pod ceiling is unpinned by default"
    );
}

/// The section teaches the two things that are genuinely surprising: placement is
/// **policy**, so it never refuses a resume; and remote start latency is measured in
/// seconds, so a graph of sub-second nodes should not be placed node by node.
#[test]
fn the_cookbook_states_the_latency_caveat_and_the_policy_not_class_rule() {
    let section = normalize_ws(&cookbook_remote_section());
    let lower = section.to_lowercase();

    assert!(
        lower.contains("second"),
        "the latency caveat is stated in seconds, which is the unit T101 measured"
    );
    assert!(
        lower.contains("policy diff") || lower.contains("policy hash"),
        "placement feeds the policy hash, so a resume prints a diff and proceeds"
    );
    assert!(
        lower.contains("structural fingerprint"),
        "…and does not move the structural fingerprint"
    );
    assert!(
        lower.contains("image pull") || lower.contains("pre-pull") || lower.contains("pre-pulled"),
        "the dominant cost is the image pull, not the scheduler — T101's headline"
    );
}

/// The storage choice an operator has to make before the first remote run is stated,
/// because a placed node cannot report without one.
#[test]
fn the_cookbook_states_the_shared_volume_versus_object_store_choice() {
    let section = normalize_ws(&cookbook_remote_section()).to_lowercase();
    assert!(
        section.contains("rwx") || section.contains("readwritemany") || section.contains("shared volume"),
        "the shared-volume option is named"
    );
    assert!(
        section.contains("s3") || section.contains("object store") || section.contains("object storage"),
        "…and so is the object-store option"
    );
}

/// The RBAC an operator must apply is named, verb for verb, and the file is named
/// too — so applying it is a copy rather than a reconstruction.
#[test]
fn the_docs_name_the_rbac_verbs_and_the_manifest_to_apply() {
    let cookbook = normalize_ws(&cookbook_remote_section());
    assert!(
        cookbook.contains(dagr_k8s::rbac::ORCHESTRATOR_RBAC_MANIFEST),
        "the cookbook names the manifest an operator applies"
    );
    for verb in dagr_k8s::rbac::PodVerb::ALL {
        assert!(
            cookbook.contains(verb.as_str()),
            "the cookbook names the `{verb}` grant"
        );
    }
    let lower = cookbook.to_lowercase();
    assert!(
        lower.contains("one namespace") || lower.contains("single namespace"),
        "…and says the grant is scoped to one namespace"
    );
}

/// The README's own section says the same three things in short form: opt-in, which
/// flag turns it on, and that no dagr server exists either way.
#[test]
fn the_readme_documents_remote_execution_as_opt_in_and_serverless() {
    let section = normalize_ws(&readme_remote_section());
    let lower = section.to_lowercase();

    assert!(
        section.contains(EXECUTOR_FLAG),
        "the README names the flag that selects the executor"
    );
    assert!(
        lower.contains("opt-in") || lower.contains("opt in"),
        "…says it is opt-in"
    );
    assert!(
        lower.contains("adr 115"),
        "…and cites the decision that permits it"
    );
    assert!(
        lower.contains("no listener") || lower.contains("opens no listener"),
        "…and restates the unconditional half: dagr opens no listener under either executor"
    );
    assert!(
        section.contains("k8s") && lower.contains("default-off"),
        "…and that the feature is default-off"
    );
}

// ===========================================================================
// 2. The docs claim NOTHING unshipped
// ===========================================================================

/// The forbidden-claim scan. Each entry names something ADR 115 explicitly does not
/// permit; a document that promises one has widened the carve-out in the only place
/// a reader can see.
#[test]
fn the_remote_docs_claim_nothing_unshipped() {
    let cookbook = cookbook_remote_section();
    let readme = readme_remote_section();

    // (a) Hard-forbidden substrings: never acceptable, in any framing.
    let forbidden: &[(&str, &str)] = &[
        ("tcplistener", "the orchestrator opens no listener (ADR 115)"),
        ("::bind(", "no socket is bound anywhere in the run path"),
        (".serve(", "there is no server to serve"),
        ("backofflimit", "cluster-side retry is refused: two retry loops duplicate an attempt"),
        ("restartpolicy: always", "the pod's restartPolicy is pinned to Never"),
        ("helm", "dagr is invoked, not installed — no chart, no operator, no CRD"),
        ("crd", "a CRD plus a controller is a control plane outliving a run"),
        ("webhook", "nothing calls dagr inbound"),
    ];
    for (haystack, name) in [(&cookbook, "cookbook"), (&readme, "README")] {
        let lower = haystack.to_lowercase();
        for (needle, why) in forbidden {
            assert!(
                !lower.contains(needle),
                "the {name}'s remote section must not reference `{needle}` — {why}"
            );
        }
    }

    // (b) Words that may appear ONLY in their negated form.
    let conditional: &[(&str, &[&str])] = &[
        (
            "scheduler",
            &["not a scheduler", "no scheduler", "never a scheduler", "is not a scheduler"],
        ),
        (
            "control plane",
            &["no control plane", "not a control plane", "never a control plane"],
        ),
        (
            "distributed execution",
            &[
                "not a distributed execution",
                "no distributed execution",
                "distributed execution system\" here means",
                "narrowed",
            ],
        ),
        (
            "credential",
            &["no credential", "never a credential", "holds no credential", "carries no credential"],
        ),
    ];
    for (haystack, name) in [(&cookbook, "cookbook"), (&readme, "README")] {
        let lower = normalize_ws(haystack).to_lowercase();
        for (term, negated) in conditional {
            if lower.contains(term) {
                assert!(
                    negated.iter().any(|n| lower.contains(n)),
                    "the {name}'s remote section mentions `{term}` — it may only do so to say \
                     there is none; expected one of {negated:?}"
                );
            }
        }
    }

    // (c) The one promise that would be a lie about the shipped code: that a plain
    // build can do this. It cannot; the feature is default-off and a build without
    // it refuses saying so.
    let lower = normalize_ws(&cookbook).to_lowercase();
    assert!(
        lower.contains("feature") && lower.contains("k8s"),
        "the cookbook says which cargo feature a remote-capable build needs"
    );
    assert!(
        lower.contains("refus"),
        "…and that a build without it refuses rather than running locally behind your back"
    );
}

/// Non-vacuity: the section extractors really do return the sections, and the
/// scans above are looking at prose rather than at an empty string.
#[test]
fn the_section_extractors_are_not_vacuous() {
    let cookbook = cookbook_remote_section();
    let readme = readme_remote_section();
    assert!(
        cookbook.len() > 800,
        "the cookbook's remote section is substantive, not a stub ({} bytes)",
        cookbook.len()
    );
    assert!(
        readme.len() > 400,
        "the README's remote section is substantive ({} bytes)",
        readme.len()
    );
    assert!(cookbook.starts_with(COOKBOOK_HEADING));
    assert!(readme.starts_with(README_HEADING));
    assert!(
        !cookbook.contains("\n## Querying run state"),
        "the extractor stops at the next heading rather than swallowing the rest of the file"
    );
}
