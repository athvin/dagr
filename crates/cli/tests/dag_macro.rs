//! The `#[dag]` attribute macro (T80) — expansion + discovery, exercised through a
//! real linked leaf binary. Written first (TDD).
//!
//! `#[dag]` keeps the user's flow-builder fn, generates a `fn() -> RunnableFlow`
//! factory that drives it through a `FlowBuilder`, and emits an `inventory::submit!`
//! of a `DagRegistration` so `dagr_cli::run` discovers it. Per ADR 092's leaf-binary
//! contract, `inventory` reliably collects a submission only when it is compiled into
//! the final linked binary, so these tests spawn the `dag_macro_smoke` example via
//! `cargo run --example` (mirroring the T79 `dag_auto_discovery` tests) and assert on
//! its `list` output — discovery through a real linked binary, on ubuntu and macOS.
//!
//! The example declares:
//! - `#[dag] fn etl(..)`             -> discovered as `etl` (default = fn name),
//! - `#[dag(name = "nightly")] fn foo(..)` -> discovered as `nightly`, not `foo`,
//! - both in the **same module**    -> no item-name collision, both discovered.

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
    /// Everything the example wrote to stdout (the `list` names).
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

/// The non-empty, trimmed lines of a `list` output, in order.
fn list_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect()
}

/// A `#[dag]`-declared DAG with **no** attribute argument is discovered under its
/// **fn name** (`etl`), a `#[dag(name = "nightly")]` on `fn foo` is discovered as
/// `nightly` (**not** `foo`), and two `#[dag]`s in the **same module** both expand
/// without an item-name collision and are both discovered. `list` prints them sorted.
#[test]
fn dag_macro_declares_and_discovers_by_name() {
    let run = run_example("dag_macro_smoke", &["list"]);
    assert_eq!(
        run.code, 0,
        "list over the #[dag]-declared example exits Success, stderr:\n{}",
        run.stderr
    );
    let names = list_lines(&run.stdout);

    // Default name = fn name; the `name = "…"` override wins over the fn name; both
    // same-module DAGs are present. `run` sorts, so the expected order is fixed.
    assert_eq!(
        names,
        ["etl", "nightly"],
        "expected the fn-named `etl` and the renamed `nightly` (not `foo`), got {names:?} \
         (stdout:\n{}\nstderr:\n{})",
        run.stdout,
        run.stderr
    );

    // The override truly renamed it — the fn name `foo` never leaks to discovery.
    assert!(
        !names.contains(&"foo"),
        "#[dag(name = \"nightly\")] on `fn foo` registers `nightly`, never `foo`; got {names:?}"
    );
}
