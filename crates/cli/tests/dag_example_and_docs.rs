//! **The `#[dag]` example + docs are teachable and provably correct** (T81).
//! Written first (TDD).
//!
//! T78–T80 shipped the `#[dag]` authoring surface (`FlowBuilder` façade, the
//! `inventory`-backed `dagr_cli::run` discovery entrypoint, and the `#[dag]` macro).
//! This suite is the M6 counterpart to `crates/cli/examples/multi_flow.rs`'s tests:
//! it proves the copyable `#[dag]` example (`crates/cli/examples/many_dags.rs`)
//! runs and graph-emits **through the sugar**, and that the cookbook/README teach the
//! pattern with the load-bearing contract lines ADR 092 pins — so the docs cannot
//! claim a behaviour the shipped example does not have, and cannot silently drop the
//! leaf-binary / `inventory = "0.3"` / cross-crate-out-of-scope obligations.
//!
//! The example is a real linked **example binary** (per ADR 092's leaf-binary
//! contract, `inventory` reliably collects a submission only in the final linked
//! binary), so the runnable scenarios spawn it via `cargo run --example` — exercising
//! discovery through a real binary, on both `ubuntu-latest` and `macos-latest` under
//! `cargo test --workspace`. The sort/dedup/single-flow discovery behaviour itself is
//! covered by `dag_auto_discovery.rs` (T79) and `dag_macro.rs` (T80); this suite adds
//! the T81-owned run-end-to-end, graph-through-the-sugar, and docs-truthfulness
//! guards.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Locate the repository root from this crate's manifest directory (`crates/cli`).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// The captured outcome of one example invocation.
struct Run {
    /// The process exit code.
    code: i32,
    /// Everything the example wrote to stdout.
    stdout: String,
    /// Everything the example wrote to stderr.
    stderr: String,
}

/// Run the compiled `example` binary with `args`, returning its captured outcome.
/// Spawning a real linked binary is what makes the example's `#[dag]`-emitted
/// `inventory` submissions actually collected (the leaf-binary contract).
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

/// A unique run-store base under the OS temp dir, so concurrent test binaries never
/// collide and each test's stores are inspectable in isolation.
fn temp_base(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "dagr-t81-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    dir.to_string_lossy().into_owned()
}

/// Whether the run store under `<base>/<pipeline>/` holds at least one
/// `<run-id>/events.jsonl` — i.e. the selected DAG really ran and wrote its stream.
fn run_store_has_a_stream(base: &str, pipeline: &str) -> bool {
    let dir = Path::new(base).join(pipeline);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    entries
        .flatten()
        .any(|e| e.path().join("events.jsonl").exists())
}

fn read_doc(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"))
}

// ===========================================================================
// The example runs and graph-emits THROUGH THE SUGAR
// ===========================================================================

/// `run <dag> --store DIR` over the `#[dag]`-declared example drives the selected DAG
/// to a real on-disk event stream and exits with the run's own code (`Success`) —
/// delegation through `run_registry`, on both CI OSes. A multi-flow registry keys the
/// store under the DAG name, so the stream lives at `<base>/alpha/<run-id>/`.
#[test]
fn many_dags_run_writes_an_on_disk_event_stream() {
    let base = temp_base("run");
    let run = run_example("many_dags", &["run", "alpha", "--store", &base]);
    assert_eq!(
        run.code, 0,
        "run alpha exits with the run's own Success code, stderr:\n{}",
        run.stderr
    );
    assert!(
        run_store_has_a_stream(&base, "alpha"),
        "the selected DAG wrote an event stream under {base}/alpha/<run-id>/ (stdout:\n{})",
        run.stdout
    );
}

/// A DAG declared through `FlowBuilder::source` / `node` (the graph-emittable
/// variants the `#[dag]` example uses) really **graph-emits**: `graph <dag>` produces
/// a real graph artifact recording the author-declared stable node names, **not**
/// `AssemblyFailure`. This is the load-bearing "graph-emittable through the sugar"
/// proof.
#[test]
fn many_dags_graph_emits_through_the_sugar() {
    let graph = run_example("many_dags", &["graph", "alpha"]);
    assert_eq!(
        graph.code, 0,
        "graph alpha emits cleanly (not AssemblyFailure), stderr:\n{}",
        graph.stderr
    );
    let json: serde_json::Value = serde_json::from_str(graph.stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "graph alpha emits a JSON graph artifact: {e}\n{}",
            graph.stdout
        )
    });
    let nodes: Vec<&str> = json["nodes"]
        .as_array()
        .expect("the artifact carries a nodes array")
        .iter()
        .map(|n| n["name"].as_str().unwrap_or_default())
        .collect();
    assert!(
        nodes.contains(&"extract") && nodes.contains(&"load"),
        "the sugar records the author-declared stable node names `extract`/`load`, got {nodes:?}"
    );
}

// ===========================================================================
// The docs teach the pattern AND its ADR-092 contract (docs cannot rot / overclaim)
// ===========================================================================

/// The cookbook's `#[dag]` section documents the three load-bearing obligations ADR
/// 092 pins — the `inventory = "0.3"` Cargo.toml requirement, the leaf-binary
/// contract, and cross-crate DAG libraries as out of scope — and keeps the hand-wired
/// `FlowRegistry` documented as the explicit fallback. A section that drops any of
/// these reds the build.
#[test]
fn cookbook_teaches_the_dag_contract() {
    let md = read_doc("docs/cookbook.md");
    assert!(md.contains("#[dag]"), "the cookbook has a #[dag] section");
    assert!(
        md.contains("inventory = \"0.3\""),
        "the cookbook states the required `inventory = \"0.3\"` Cargo.toml line"
    );
    let lower = md.to_lowercase();
    assert!(
        lower.contains("leaf") && lower.contains("binary"),
        "the cookbook states the leaf-binary contract"
    );
    assert!(
        lower.contains("cross-crate"),
        "the cookbook calls out cross-crate DAG libraries as out of scope"
    );
    assert!(
        md.contains("FlowRegistry"),
        "the cookbook keeps the hand-wired FlowRegistry documented as the fallback"
    );
    // The example is the compiled, run backing for the section, so a reader copies
    // code the build actually compiles (docs-are-runnable).
    assert!(
        md.contains("many_dags"),
        "the cookbook points at the compiled `many_dags` example that backs the section"
    );
}

/// The README's `#[dag]` pointer teaches the one-line `main` over `dagr_cli::run`,
/// notes the `use dagr_cli as dagr;` short spelling (there is no facade crate — ADR
/// 092 rejected `crates/dagr`), and points at the compiled `many_dags` example.
#[test]
fn readme_points_at_the_dag_example_and_the_short_spelling() {
    let md = read_doc("README.md");
    assert!(
        md.contains("#[dag]"),
        "the README teaches the #[dag] declarative-DAG option"
    );
    assert!(
        md.contains("dagr_cli::run"),
        "the README shows the one-line `dagr_cli::run` main"
    );
    assert!(
        md.contains("use dagr_cli as dagr"),
        "the README notes the `use dagr_cli as dagr;` short spelling (no facade crate)"
    );
    assert!(
        md.contains("many_dags"),
        "the README points at the compiled `many_dags` example"
    );
}
