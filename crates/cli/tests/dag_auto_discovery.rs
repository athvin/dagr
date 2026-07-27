//! `dagr_cli::run` — `inventory`-backed DAG auto-discovery + registry delegation.
//! Written first (TDD).
//!
//! These exercise the `dagr_cli::run(argv)` entrypoint (T79): it iterates
//! `inventory::iter::<DagRegistration>()`, sorts records by name, rejects a
//! duplicate name with `ExitCode::InvalidUsage` before any flow is built, builds a
//! `FlowRegistry` (single-flow default for exactly one DAG), and delegates to
//! `run_registry`.
//!
//! Per ADR 092's leaf-binary contract, `inventory` reliably collects a submission
//! only when it is compiled into the final linked binary, so the discovery corpus
//! is a set of **example binaries** (`examples/many_dags.rs`, `examples/one_dag.rs`,
//! `examples/dup_dags.rs`) — each submitting `DagRegistration` records directly and
//! calling `dagr_cli::run`. These tests spawn those examples via
//! `cargo run --example` and assert on their output / exit codes, so discovery is
//! exercised through a real linked binary (mirroring the `multi_flow` example test),
//! and runs on both `ubuntu-latest` and `macos-latest`.

use std::path::{Path, PathBuf};
use std::process::Command;

use dagr_cli::graph::mask_generated_at;

/// The `InvalidUsage` exit code (`ExitCode::as_u8`), asserted numerically so this
/// test needs no `dag`-feature-gated symbols of its own.
const INVALID_USAGE: i32 = 2;

/// Locate the repository root from this crate's manifest directory.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// The captured outcome of one example invocation.
struct Run {
    /// The process exit code (numeric, so we compare against `ExitCode::as_u8`).
    code: i32,
    /// Everything the example wrote to stdout (the `list` names, the `graph`
    /// artifact, the `validate` report, or a selection diagnostic).
    stdout: String,
    /// Everything the example wrote to stderr.
    stderr: String,
}

/// Run the compiled `example` binary with `args`, returning its captured outcome.
/// This spawns a real linked binary so the `inventory` submissions in the example
/// crate are actually collected (the leaf-binary contract).
fn run_example(example: &str, args: &[&str]) -> Run {
    let out = Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--example",
            example,
            "--",
        ])
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `cargo run --example {example}`: {e}"));
    Run {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// The non-empty lines of a `list` output, in order.
fn list_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect()
}

// ===========================================================================
// Discovery, ordering, dedup
// ===========================================================================

/// Three DAGs submitted out of alphabetical order (`gamma`, `alpha`, `beta`) print
/// **sorted** on `list`, and the order is **stable** across repeated runs —
/// `inventory::iter`'s unspecified order is neutralised by `run`'s sort.
#[test]
fn list_is_sorted_and_stable_regardless_of_submission_order() {
    let first = run_example("many_dags", &["list"]);
    assert_eq!(
        first.code, 0,
        "list exits Success, stderr:\n{}",
        first.stderr
    );
    let names = list_lines(&first.stdout);
    assert_eq!(
        names,
        ["alpha", "beta", "gamma"],
        "list prints the three DAG names sorted, got {names:?} (stdout:\n{})",
        first.stdout
    );

    // Stable across a repeated run: same sorted order, no dependence on the
    // (unspecified) submission/iteration order.
    let second = run_example("many_dags", &["list"]);
    assert_eq!(second.code, 0);
    assert_eq!(
        list_lines(&second.stdout),
        names,
        "list output is stable across repeated runs"
    );
}

/// Two records submitted under the **same** name make `run` exit `InvalidUsage`
/// with a diagnostic **naming** the duplicated DAG, *before* any flow is built.
#[test]
fn duplicate_name_is_invalid_usage_naming_the_duplicate() {
    // `list` alone would exercise the discovery/dedup path before dispatch; use it
    // so the failure is attributable to duplicate rejection, not a verb body.
    let run = run_example("dup_dags", &["list"]);
    assert_eq!(
        run.code, INVALID_USAGE,
        "a duplicate DAG name is InvalidUsage (2), got {} (stdout:\n{}\nstderr:\n{})",
        run.code, run.stdout, run.stderr
    );
    let combined = format!("{}{}", run.stdout, run.stderr);
    assert!(
        combined.contains("dup"),
        "the duplicate diagnostic names the duplicated DAG `dup`, got:\n{combined}"
    );
}

// ===========================================================================
// Delegation parity with the existing registry
// ===========================================================================

/// With exactly one discovered DAG, `run run --store DIR` with the **name omitted**
/// dispatches the sole DAG (single-flow default) and exits `Success`, writing a real
/// event stream under the store — the same outcome as a hand-wired
/// `FlowRegistry::single_flow` + `run_registry`.
#[test]
fn single_discovered_dag_runs_with_no_name() {
    let base = temp_base("single");
    let run = run_example("one_dag", &["run", "--store", &base]);
    assert_eq!(
        run.code, 0,
        "a single discovered DAG serves `run` with the name omitted, stderr:\n{}",
        run.stderr
    );
    // The sole DAG really ran: a single-flow registry dispatches under the synthetic
    // `flow` identity, so its stream lives under `<base>/flow/<run-id>/`.
    assert!(
        run_store_has_a_stream(&base, "flow"),
        "the sole DAG wrote an event stream under {base}/flow/<run-id>/ (stdout:\n{})",
        run.stdout
    );
}

/// `graph <name>` and `validate <name>` over discovered records produce the same
/// artifact / exit code they would through a hand-wired registry — delegation is
/// transparent. Two distinct DAGs (`alpha`, `beta`) emit distinct graphs.
#[test]
fn graph_and_validate_delegate_transparently() {
    let alpha = run_example("many_dags", &["graph", "alpha"]);
    assert_eq!(
        alpha.code, 0,
        "graph alpha succeeds, stderr:\n{}",
        alpha.stderr
    );
    let alpha_json: serde_json::Value = serde_json::from_str(alpha.stdout.trim())
        .unwrap_or_else(|e| panic!("graph alpha emits a JSON artifact: {e}\n{}", alpha.stdout));
    let alpha_nodes: Vec<&str> = alpha_json["nodes"]
        .as_array()
        .expect("alpha nodes")
        .iter()
        .map(|n| n["name"].as_str().unwrap_or_default())
        .collect();
    assert!(
        alpha_nodes.contains(&"extract") && alpha_nodes.contains(&"load"),
        "graph alpha carries alpha's two nodes, got {alpha_nodes:?}"
    );

    let beta = run_example("many_dags", &["graph", "beta"]);
    assert_eq!(
        beta.code, 0,
        "graph beta succeeds, stderr:\n{}",
        beta.stderr
    );
    let beta_json: serde_json::Value = serde_json::from_str(beta.stdout.trim())
        .unwrap_or_else(|e| panic!("graph beta emits a JSON artifact: {e}\n{}", beta.stdout));
    assert_ne!(
        mask_generated_at(alpha_json.clone()),
        mask_generated_at(beta_json),
        "the two discovered DAGs emit distinct graph artifacts"
    );

    // `validate <name>` runs assembly only and reports its clean result.
    let validate = run_example("many_dags", &["validate", "alpha"]);
    assert_eq!(
        validate.code, 0,
        "validate alpha exits Success on a clean assembly, stderr:\n{}",
        validate.stderr
    );
    assert!(
        validate.stdout.to_lowercase().contains("valid")
            || validate.stdout.to_lowercase().contains("succeeded"),
        "validate alpha reports the clean assembly, got:\n{}",
        validate.stdout
    );
}

// ===========================================================================
// Test-support
// ===========================================================================

/// A unique run-store base under the OS temp dir, so concurrent test binaries never
/// collide and each test's stores are inspectable in isolation.
fn temp_base(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "dagr-t79-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    dir.to_string_lossy().into_owned()
}

/// Whether the run store under `<base>/<pipeline>/` holds at least one
/// `<run-id>/events.jsonl` — i.e. a DAG really ran and wrote its stream.
fn run_store_has_a_stream(base: &str, pipeline: &str) -> bool {
    let dir = Path::new(base).join(pipeline);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    entries
        .flatten()
        .any(|e| e.path().join("events.jsonl").exists())
}
