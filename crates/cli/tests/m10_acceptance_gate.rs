//! **M10 acceptance gate.** Written first, TDD.
//!
//! M10 moved a *permanent* boundary. ADR 115 narrowed "distributed execution
//! system" to mean an engine that distributes the graph and its control, and
//! permitted **one** orchestrator process placing individual node attempts on remote
//! compute it owns for **one** run. The only thing keeping that carve-out at exactly
//! the width it was granted is a test that fails when it widens — which is what this
//! file is, on ADR 097's precedent (T88 asserted its boundary with `cargo tree` and a
//! diff-scanning script, not with prose).
//!
//! Two kinds of assertion live here, and they are different in character.
//!
//! **The boundary proof** is structural and it matters most: the orchestrator opens
//! no listener, pods link no metastore and hold no database credential,
//! `dagr-core`'s runtime dependency set is still empty, a build that does not ask
//! for remote execution compiles no HTTP/TLS stack and no Kubernetes or S3 client,
//! the terminal-state taxonomy still has exactly nine members and the trigger rules
//! exactly three, and `OpenMode::RemoteSqld` / `SyncedReplica` are still
//! `ModeNotImplemented`.
//!
//! **The capability proof** an operator can read is the runnable example: a pipeline
//! with a placed node declaring CPU and memory, which runs to completion under
//! `--dagr.executor=local` with **no cluster present and no warning about one** —
//! the property that makes one binary genuinely both.
//!
//! # Where the rest of the milestone's proof lives
//!
//! - The wired remote run path, adoption across a restart, dual-mode parity and the
//!   RBAC failure modes are in `crates/cli/tests/m10_remote_execution.rs`, which
//!   needs the default-off `k8s` feature and drives T107's fake API surface.
//! - The crate-graph and diff-surface invariants that `cargo test` cannot reach are
//!   in `scripts/check-m10-acceptance-boundary.sh`, which this file runs.
//! - The docs' claims are pinned by `crates/cli/tests/remote_execution_docs_claims.rs`.
//!
//! # No cluster
//!
//! Nothing here talks to a cluster, and that is a property of the gate rather than a
//! limitation of it: every invariant above is a fact about the *build*, and a
//! cluster cannot make one of them true or false.
//!
//! The cluster-requiring half of the ticket's test plan — a placed node running end
//! to end on kind, the kill-restart proof against real pods, the OOM-kill case — is
//! **not covered anywhere yet**, and the ticket file records why: the shipped pod
//! spec (`dagr_k8s::executor::PodSpec`, translated by `dagr_k8s::client::pod_object`)
//! has no volume field, so a pod cannot be given the blob container it must write
//! its output and shard into. That is T108's mechanism to add, not this gate's, and
//! a cluster job that asserted less than it claimed would be worse than none.

use std::path::{Path, PathBuf};
use std::process::Command;

// ===========================================================================
// Helpers
// ===========================================================================

/// The repository root — `crates/cli`'s two-level ancestor.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"))
}

/// The resolved runtime dependency package names of `pkg`, via `cargo tree -e
/// normal` — the resolution cargo would actually build, so a build that merely
/// *declares* an optional dependency is not mistaken for one that compiles it.
fn resolved_deps(pkg: &str, extra: &[&str]) -> Vec<String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let mut cmd = Command::new(cargo);
    cmd.current_dir(repo_root())
        .args(["tree", "-p", pkg, "-e", "normal", "--prefix", "none"])
        .args(extra);
    let out = cmd.output().expect("cargo tree runs");
    assert!(
        out.status.success(),
        "cargo tree -p {pkg} {extra:?} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let names: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter(|n| !n.is_empty())
        .map(str::to_owned)
        .collect();
    assert!(
        !names.is_empty(),
        "cargo tree reported nothing for {pkg} — the assertion would be vacuous"
    );
    names
}

/// The whole-workspace resolution, at a given feature setting.
fn workspace_deps(extra: &[&str]) -> Vec<String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let out = Command::new(cargo)
        .current_dir(repo_root())
        .args(["tree", "--workspace", "-e", "normal", "--prefix", "none"])
        .args(extra)
        .output()
        .expect("cargo tree runs");
    assert!(
        out.status.success(),
        "cargo tree --workspace {extra:?} failed"
    );
    let names: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter(|n| !n.is_empty())
        .map(str::to_owned)
        .collect();
    assert!(!names.is_empty(), "the workspace resolution is empty");
    names
}

/// Every crate that constitutes an HTTP or TLS stack, a Kubernetes client, or an
/// object-store client. A default build compiling **any** of these is the M10
/// containment guarantee broken, whichever crate pulled it in.
const REMOTE_STACK: &[&str] = &[
    // HTTP / TLS
    "hyper",
    "hyper-util",
    "hyper-rustls",
    "rustls",
    "rustls-pki-types",
    "rustls-webpki",
    "rustls-native-certs",
    "ring",
    "aws-lc-rs",
    "tower",
    "tower-http",
    "h2",
    "tokio-rustls",
    "ureq",
    // Kubernetes
    "kube",
    "kube-client",
    "kube-core",
    "kube-runtime",
    "k8s-openapi",
];

// ===========================================================================
// 1. The boundary — crate graph
// ===========================================================================

/// **Test plan: `cargo tree -p dagr-core -e normal --no-default-features` shows an
/// empty runtime dependency set.** M10 added a Kubernetes client, an object-store
/// client and a pod-side verb; none of them may have reached the core.
#[test]
fn dagr_cores_runtime_dependency_set_is_still_empty() {
    let deps = resolved_deps("dagr-core", &["--no-default-features"]);
    let others: Vec<&String> = deps.iter().filter(|n| *n != "dagr-core").collect();
    assert!(
        others.is_empty(),
        "dagr-core acquired runtime dependencies: {others:?}"
    );

    // And at the adversarial setting, where every optional feature in the workspace
    // is on.
    let all = resolved_deps("dagr-core", &["--all-features"]);
    for forbidden in [
        "dagr-k8s",
        "dagr-blob",
        "dagr-metastore",
        "kube",
        "rustls",
        "ureq",
    ] {
        assert!(
            !all.iter().any(|n| n == forbidden),
            "dagr-core reached {forbidden} under --all-features"
        );
    }
}

/// **Test plan: `cargo build --all` and `cargo build --no-default-features` compile
/// no HTTP/TLS stack and no Kubernetes or S3 client — asserted against the built
/// dependency list, not the manifest.**
#[test]
fn a_build_that_does_not_ask_for_remote_execution_compiles_none_of_it() {
    for leg in [vec![], vec!["--no-default-features"]] {
        let deps = workspace_deps(&leg);
        let hits: Vec<&&str> = REMOTE_STACK
            .iter()
            .filter(|f| deps.iter().any(|n| n == **f))
            .collect();
        assert!(
            hits.is_empty(),
            "the whole-workspace resolution {leg:?} compiled a remote stack: {hits:?}"
        );
    }
}

/// Non-vacuity for the scan above: the very same query **does** see the stack when a
/// build asks for it. Without this control, a mistyped crate name would turn the
/// guarantee green for the wrong reason.
#[test]
fn the_remote_stack_scan_is_not_vacuous() {
    let deps = resolved_deps("dagr-cli", &["--features", "k8s"]);
    assert!(
        deps.iter().any(|n| n == "kube"),
        "`--features k8s` does reach kube — the absence checks are looking at something"
    );
    let deps = resolved_deps("dagr-cli", &["--features", "blob-s3"]);
    assert!(
        deps.iter().any(|n| n == "ureq"),
        "`--features blob-s3` does reach the object-store transport"
    );
}

// ===========================================================================
// 2. The boundary — no listener, no served database, no scheduler
// ===========================================================================

/// The structural gate script. It holds the invariants a unit test cannot reach:
/// the M9→M10 shipped-source diff introduces no listener, no server framework and no
/// scheduler type; the pod path links no metastore and passes no database
/// credential; and the reserved open-mode seam is still a stub.
#[test]
fn the_m10_acceptance_boundary_script_passes() {
    let script = repo_root().join("scripts/check-m10-acceptance-boundary.sh");
    assert!(
        script.is_file(),
        "the M10 acceptance-gate script ships at scripts/check-m10-acceptance-boundary.sh"
    );
    let out = Command::new("bash")
        .current_dir(repo_root())
        .arg(&script)
        .output()
        .expect("the gate script runs");
    assert!(
        out.status.success(),
        "scripts/check-m10-acceptance-boundary.sh failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The forbidden-surface scan the M7 gate established still passes, **extended**
/// rather than relaxed — M10's source is inside its diff window too.
#[test]
fn the_metastore_forbidden_surface_scan_still_passes() {
    let script = repo_root().join("scripts/check-metastore-acceptance-boundary.sh");
    let out = Command::new("bash")
        .current_dir(repo_root())
        .arg(&script)
        .output()
        .expect("the M7 gate script runs");
    assert!(
        out.status.success(),
        "the M7 acceptance-boundary script must still pass unmodified:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// The reserved-but-unbuilt open modes are still stubs. Read from the source rather
/// than from a running store, because the `metastore` feature is default-off and
/// this file runs on the default feature set.
#[test]
fn the_reserved_open_modes_are_still_unimplemented_stubs() {
    let store = read("crates/metastore/src/store.rs");
    for variant in ["RemoteSqld", "SyncedReplica"] {
        assert!(
            store.contains(variant),
            "the {variant} seam is still declared"
        );
    }
    assert!(
        store.contains("ModeNotImplemented"),
        "a non-local open mode still resolves to ModeNotImplemented, never a server client"
    );
    for wired in [
        "embedded_replica(",
        "sync_from_remote",
        "Database::open_remote",
    ] {
        assert!(
            !store.contains(wired),
            "the seam must not have acquired a real remote client call ({wired})"
        );
    }
}

// ===========================================================================
// 3. The vocabulary is closed
// ===========================================================================

/// **Test plan: the terminal-state taxonomy has exactly nine members and the
/// trigger-rule set exactly three.** M10 added an executor, two retry budgets, a
/// pre-start failure class, an adoption refusal and an OOM diagnostic — and not one
/// of them is a new terminal state. That is the whole claim: remote execution
/// changes *where* a node runs and nothing else.
#[test]
fn the_terminal_taxonomy_and_trigger_rules_are_unchanged() {
    use dagr_core::TriggerRule;
    use dagr_core::context::TerminalState;

    let terminal = [
        TerminalState::Succeeded,
        TerminalState::Failed,
        TerminalState::TimedOut,
        TerminalState::Skipped,
        TerminalState::UpstreamSkipped,
        TerminalState::UpstreamFailed,
        TerminalState::Cancelled,
        TerminalState::Abandoned,
        TerminalState::SatisfiedFromPrior,
    ];
    assert_eq!(terminal.len(), 9, "nine terminal states, and only nine");
    // Exhaustiveness, so a tenth variant is a compile error here rather than a
    // silently-uncounted one: the match below must cover the enum.
    for state in terminal {
        let _class = match state {
            TerminalState::Succeeded
            | TerminalState::SatisfiedFromPrior
            | TerminalState::Skipped
            | TerminalState::UpstreamSkipped
            | TerminalState::Failed
            | TerminalState::TimedOut
            | TerminalState::UpstreamFailed
            | TerminalState::Cancelled
            | TerminalState::Abandoned => (),
        };
    }

    let rules = [
        TriggerRule::AllSucceeded,
        TriggerRule::AllTerminal,
        TriggerRule::AnyFailed,
    ];
    assert_eq!(rules.len(), 3, "three trigger rules, and only three");
    for rule in rules {
        match rule {
            TriggerRule::AllSucceeded | TriggerRule::AllTerminal | TriggerRule::AnyFailed => (),
        }
    }
}

/// The executor set is closed too, and `local` is the default — so a build that
/// says nothing runs everything in-process.
#[test]
fn the_executor_set_is_closed_and_defaults_to_local() {
    use dagr_cli::executor::ExecutorKind;
    let names: Vec<&str> = ExecutorKind::ALL.iter().map(|k| k.as_str()).collect();
    assert_eq!(names, vec!["local", "k8s"], "two executors, and only two");
    assert_eq!(ExecutorKind::default(), ExecutorKind::Local);
    assert!(
        !ExecutorKind::Local.honours_placement(),
        "under the local executor a placement is recorded and ignored"
    );
}

// ===========================================================================
// 4. RBAC ships, and is least privilege
// ===========================================================================

/// Both manifests ship: the observer's read-only grant (unchanged) and the
/// orchestrator's complete one. Their contents are asserted in
/// `crates/k8s/tests/`; what this gate adds is that the milestone ships them at all
/// and that the wide one is the one the diagnostic points at.
#[test]
fn the_least_privilege_rbac_manifests_ship() {
    for rel in [
        "crates/k8s/manifests/pod-observer-rbac.yaml",
        "crates/k8s/manifests/dagr-orchestrator-rbac.yaml",
    ] {
        assert!(
            repo_root().join(rel).is_file(),
            "the reviewed manifest {rel} ships with the crate"
        );
    }
    assert_eq!(
        dagr_k8s::rbac::ORCHESTRATOR_RBAC_MANIFEST,
        "crates/k8s/manifests/dagr-orchestrator-rbac.yaml",
        "the diagnostic points at the file that actually ships"
    );
}

// ===========================================================================
// 5. The criteria and coverage matrices
// ===========================================================================

/// **Test plan: every M10 numbered `arch.md` criterion appears exactly once in
/// `docs/criteria-matrix.md` with a covering row in `docs/coverage-matrix.md`.**
///
/// M10 introduced **no new numbered criterion**: ADR 115 amended C5, C12, C26, the
/// operational model, the performance envelope and system-level criterion 7 in
/// place, and arch.md's criterion id set is still exactly `C1`–`C28` plus the eight
/// system-level ids. So the duty this DoD line creates is to prove that claim rather
/// than to add rows — a claim that is false the moment somebody adds a `C29`, which
/// is precisely when a row would be owed.
#[test]
fn m10_added_no_numbered_criterion_and_both_matrices_are_still_complete() {
    let arch = read("docs/arch.md");
    let mut ids: Vec<u32> = Vec::new();
    for line in arch.lines() {
        if let Some(rest) = line.strip_prefix("### C") {
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if let Ok(n) = digits.parse::<u32>() {
                ids.push(n);
            }
        }
    }
    ids.sort_unstable();
    assert_eq!(
        ids,
        (1..=28).collect::<Vec<u32>>(),
        "arch.md still numbers exactly C1..=C28 — a new id here owes a matrix row"
    );

    let criteria = read("docs/criteria-matrix.md");
    let coverage = read("docs/coverage-matrix.md");
    for n in 1..=28u32 {
        let row = format!("| C{n} |");
        assert_eq!(
            criteria.matches(&row).count(),
            1,
            "C{n} appears exactly once in the criteria partition"
        );
        assert_eq!(
            coverage.matches(&row).count(),
            1,
            "C{n} has exactly one covering row in the coverage matrix"
        );
    }
    for sl in [
        "SL1",
        "SL2",
        "SL3",
        "SL4a",
        "SL4b",
        "SL4c",
        "SL5",
        "SL6",
        "SL7",
        "SL8machine",
        "SL8human",
    ] {
        let row = format!("| {sl} |");
        assert_eq!(
            criteria.matches(&row).count(),
            1,
            "{sl} appears exactly once in the criteria partition"
        );
        assert_eq!(
            coverage.matches(&row).count(),
            1,
            "{sl} has exactly one covering row in the coverage matrix"
        );
    }
    assert!(
        !coverage.contains("| unmapped |"),
        "no machine criterion is left unmapped at the end of M10"
    );
}

// ===========================================================================
// 6. The capability proof an operator can read
// ===========================================================================

/// The example's name, so the assertions below and the docs cannot drift.
const EXAMPLE: &str = "placed_pipeline";

/// **Test plan: a runnable example ships a pipeline with a placed node, runnable
/// under both executors from one binary** — and it is indexed where a reader will
/// find it.
#[test]
fn the_placed_pipeline_example_ships_and_declares_a_size() {
    let src = read(&format!("crates/cli/examples/{EXAMPLE}.rs"));
    assert!(
        src.contains("register_source_placed") || src.contains("register_placed"),
        "the example registers a placed node through the Payload-bounded registrar"
    );
    assert!(
        src.contains(".cpu(") && src.contains(".memory("),
        "the placed node declares CPU and memory — the size the pod is created with"
    );
    assert!(
        src.contains("--dagr.executor"),
        "the example documents the flag that selects between the two executors"
    );

    let index = read("crates/cli/examples/README.md");
    assert!(
        index.contains(&format!("`{EXAMPLE}.rs`")),
        "the example is listed in the examples index"
    );
    assert!(
        index.contains(&format!("cargo run --example {EXAMPLE}")),
        "…with the exact invocation a reader copies"
    );
}

/// **Test plan: given `--dagr.executor=local` on the demo, it runs with no cluster
/// present and no warning about one.**
///
/// This is the dual-mode promise's cheap half, and it is the one an author relies on
/// every day: the same binary that can place a node on a pod runs the whole pipeline
/// in-process, at full speed, on a laptop that has never heard of Kubernetes.
#[test]
fn the_demo_runs_locally_with_no_cluster_and_says_nothing_about_one() {
    let store = repo_root().join("target/m10-gate-local-demo");
    let _ = std::fs::remove_dir_all(&store);
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let out = Command::new(cargo)
        .current_dir(repo_root())
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--example",
            EXAMPLE,
            "--",
            "run",
            EXAMPLE,
            "--store",
        ])
        .arg(&store)
        .arg("--dagr.executor=local")
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `cargo run --example {EXAMPLE}`: {e}"));

    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "the demo runs to completion under the local executor:\n{stdout}\n{stderr}"
    );

    // No warning about a cluster, a kubeconfig, or a missing namespace. A placement
    // under the local executor is recorded and ignored, silently.
    let said = format!("{stdout}\n{stderr}").to_lowercase();
    for noise in [
        "kubeconfig",
        "no cluster",
        "cluster is not",
        "namespace",
        "kubernetes",
    ] {
        assert!(
            !said.contains(noise),
            "the local run must say nothing about a cluster, but mentioned {noise:?}:\n{said}"
        );
    }

    // And it left a real run behind: an event stream that folds, with both of the
    // demo's nodes in it — the placed one having run **here**, which is what
    // "recorded and ignored" means.
    let events = find_events(&store).expect("the demo wrote an events.jsonl");
    let bytes = std::fs::read(&events).expect("the stream is readable");
    let graph_nodes = ["sample".to_string(), "summarise".to_string()];
    let artifact =
        dagr_artifact::fold::fold_stream(&bytes[..], &graph_nodes).expect("the stream folds");
    let attempted: std::collections::BTreeSet<&str> =
        artifact.attempts().iter().map(|a| a.node()).collect();
    assert!(
        attempted.contains("sample") && attempted.contains("summarise"),
        "the folded artifact carries the demo's nodes: {attempted:?}"
    );
    let _ = std::fs::remove_dir_all(&store);
}

/// The one `events.jsonl` under a freshly-created store.
fn find_events(store: &Path) -> Option<PathBuf> {
    let mut stack = vec![store.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n == "events.jsonl") {
                return Some(path);
            }
        }
    }
    None
}
