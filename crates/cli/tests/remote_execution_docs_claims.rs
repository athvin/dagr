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
        lower.contains("unlimited")
            || lower.contains("unconstrained")
            || lower.contains("uncapped"),
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

/// The storage a placed attempt needs is stated **as it actually stands**, which is
/// that it is not wireable yet.
///
/// The earlier wording offered the operator a choice — an RWX volume "mounted at the
/// same path in the orchestrator and in every pod", or the `blob-s3` backend — and
/// neither is reachable from the shipped code: the pod side writes through
/// `LocalFsBlob` and `exec-node` refuses any reference naming another backend, while
/// the pod spec has no volume, no volumeMount and no environment field to attach one
/// with. Documenting a choice that cannot be made frames a blocking defect as
/// operator provisioning, so the section must name the gap instead.
#[test]
fn the_cookbook_says_the_pod_storage_seam_is_not_wired_yet() {
    let section = normalize_ws(&cookbook_remote_section()).to_lowercase();

    assert!(
        section.contains("not wired") || section.contains("cannot yet"),
        "the section says the pod-storage seam is not wired yet"
    );
    assert!(
        section.contains("no volume"),
        "…and names the missing field: the pod spec carries no volume"
    );
    assert!(
        section.contains("volumemount"),
        "…and no volumeMount either"
    );
    assert!(
        section.contains("local backend") || section.contains("local one"),
        "…and that `exec-node` refuses any reference naming a non-local backend, \
         which closes the object-store route too"
    );
    // The choice may still be described — as the shape the seam will take, never as
    // something an operator can pick today.
    if section.contains("rwx") || section.contains("s3") {
        assert!(
            section.contains("when it lands") || section.contains("once"),
            "the volume/object-store choice may only be described as future shape"
        );
    }
}

/// The four deployment facts the demo requires are the four the README prints. The
/// list is read out of the example's own source, so renaming one fails here rather
/// than leaving a README invocation that exits `BootstrapFailure` as printed.
#[test]
fn the_readme_invocation_carries_every_variable_the_demo_requires() {
    let demo = read_doc("crates/cli/examples/placed_pipeline.rs");
    let names = demo_env_vars(&demo);
    assert_eq!(
        names.len(),
        4,
        "the demo reads four `DAGR_DEMO_*` deployment facts; found {names:?}"
    );

    let readme = readme_remote_section();
    let index = read_doc("crates/cli/examples/README.md");
    for name in &names {
        assert!(
            readme.contains(name),
            "the README's remote invocation sets `{name}` — without it the demo exits \
             at bootstrap, and the printed command is a lie"
        );
        assert!(
            index.contains(name) || index.contains("DAGR_DEMO_*"),
            "the examples index names `{name}` too"
        );
    }
}

/// Every `DAGR_DEMO_*` identifier the example's source mentions, in first-seen order.
fn demo_env_vars(source: &str) -> Vec<String> {
    const PREFIX: &str = "DAGR_DEMO_";
    let mut names: Vec<String> = Vec::new();
    let mut rest = source;
    while let Some(at) = rest.find(PREFIX) {
        let tail = &rest[at..];
        let end = tail
            .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
            .unwrap_or(tail.len());
        let name = tail[..end].to_string();
        if name != PREFIX && !names.contains(&name) {
            names.push(name);
        }
        rest = &tail[end..];
    }
    names
}

/// The README's latency figures are **T101's spike measurements**, of the spike's own
/// client against a cluster — not a measured property of the shipped executor, which
/// has never been run against one. Printing them unattributed reads as the latter.
#[test]
fn the_readme_attributes_its_latency_figures_to_the_spike_that_measured_them() {
    let section = normalize_ws(&readme_remote_section());
    let lower = section.to_lowercase();
    let quotes_a_figure =
        lower.contains(" s co-located") || lower.contains(" s across") || lower.contains("latency");
    if quotes_a_figure {
        assert!(
            lower.contains("spike"),
            "the README attributes its numbers to the spike that measured them"
        );
        assert!(
            lower.contains("not") && lower.contains("shipped executor"),
            "…and says they are not a measurement of the shipped executor"
        );
    }
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
        // Delimited, whole-token. Bare substring matching made two of the six
        // assertions vacuous: `get` was satisfied by the word "budget" elsewhere in
        // the section, and `delete` by the `deletecollection` sitting in the
        // section's own list of what is deliberately NOT granted.
        let token = format!("`{}`", verb.as_str());
        assert!(
            cookbook.contains(&token),
            "the cookbook names the `{verb}` grant as a code token of its own"
        );
    }
    let lower = cookbook.to_lowercase();
    assert!(
        lower.contains("one namespace") || lower.contains("single namespace"),
        "…and says the grant is scoped to one namespace"
    );

    // What a removed verb actually buys the operator, stated at the strength the
    // code supports: the classifier is proven against a pinned fixture of the API
    // server's denial message, and nothing here has been run against a live cluster.
    assert!(
        lower.contains("fixture"),
        "the missing-verb claim says the denial shape it parses is a pinned fixture"
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
        (
            "tcplistener",
            "the orchestrator opens no listener (ADR 115)",
        ),
        ("::bind(", "no socket is bound anywhere in the run path"),
        (".serve(", "there is no server to serve"),
        (
            "backofflimit",
            "cluster-side retry is refused: two retry loops duplicate an attempt",
        ),
        (
            "restartpolicy: always",
            "the pod's restartPolicy is pinned to Never",
        ),
        (
            "helm",
            "dagr is invoked, not installed — no chart, no operator, no CRD",
        ),
        (
            "crd",
            "a CRD plus a controller is a control plane outliving a run",
        ),
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
            &[
                "not a scheduler",
                "no scheduler",
                "never a scheduler",
                "is not a scheduler",
            ],
        ),
        (
            "control plane",
            &[
                "no control plane",
                "not a control plane",
                "never a control plane",
            ],
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
            &[
                "no credential",
                "never a credential",
                "holds no credential",
                "carries no credential",
            ],
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
