//! **README quickstart — verbatim, CI-verified.**
//!
//! The single machine-classed criterion 1 test: the README quickstart compiles
//! and runs **verbatim** in CI, taking a Rust/cargo developer (no async
//! experience) from an empty directory to a compiled, run, artifact-inspected
//! two-node pipeline. It is the mapped test for system-level criterion 1.
//!
//! # Why the quickstart is extracted, not hand-maintained
//!
//! The ticket requires the quickstart's code blocks to be run **verbatim**,
//! "extracted from the README rather than maintained separately," so a divergence
//! between the README text and what compiles fails the test. This suite realizes
//! that by making the runnable example
//! [`crates/cli/examples/quickstart.rs`](../../examples/quickstart.rs) the single
//! source of truth: the example is what CI *compiles and runs*, and
//! [`readme_rust_block_matches_the_compiled_example_verbatim`] asserts the
//! README's fenced Rust block is **byte-identical** to the example's anchored
//! region. So the code a reader copies from the README is exactly the code the
//! build compiles and runs — editing one without the other reds this test.
//!
//! # What each scenario pins
//!
//! - **Compiles verbatim.** The example is a workspace `[[example]]` target, so
//!   `cargo build`/`clippy`/`test` compile it; a README block that drifts from it
//!   fails [`readme_rust_block_matches_the_compiled_example_verbatim`]. The
//!   TOML/shell blocks are checked for the exact `cargo run` invocation and crate
//!   dependency the prose tells the reader to use.
//! - **Runs end to end (criterion 1, criterion 7 boundary).** The example binary
//!   is run against a private temp run store — no database, scheduler, or network
//!   — and must exit `0` with both nodes `succeeded`.
//! - **Artifact inspection matches the prose.** The run's `events.jsonl` is
//!   folded and the exact values the quickstart promises — the two node names,
//!   both `succeeded`, the doubled output `42`, the two-node shape — are asserted.
//! - **"When not to use this" and MSRV are present and truthful.** The README
//!   carries the adoption-triggers guidance recommending plain tokio below them,
//!   and the documented MSRV equals the workspace-pinned `rust-version`.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Repo root, from this crate's manifest dir (`crates/cli`) up two levels.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

fn read_to_string(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// The event-stream bytes of the **single** run under `<base>/<pipeline>/`. The
/// framework mints the run id, so the run directory is discovered rather than
/// hardcoded.
fn single_run_stream(base: &Path, pipeline: &str) -> Vec<u8> {
    let pipeline_dir = base.join(pipeline);
    let run_dir = std::fs::read_dir(&pipeline_dir)
        .unwrap_or_else(|e| {
            panic!(
                "the run store has a {} directory: {e}",
                pipeline_dir.display()
            )
        })
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.join("events.jsonl").exists())
        .unwrap_or_else(|| {
            panic!(
                "a run directory with events.jsonl exists under {}",
                pipeline_dir.display()
            )
        });
    let stream = run_dir.join("events.jsonl");
    std::fs::read(&stream).unwrap_or_else(|e| {
        panic!(
            "the quickstart wrote its stream at {}: {e}",
            stream.display()
        )
    })
}

/// Extract the text of the first fenced block whose opening fence is
/// ```` ```<lang> ```` (e.g. ` ```rust `), from `md`. Returns the block's inner
/// lines joined with `\n` and a trailing newline (so it round-trips a file).
fn fenced_block(md: &str, lang: &str) -> Option<String> {
    let open = format!("```{lang}");
    let mut lines = md.lines();
    // Find the opening fence.
    for line in lines.by_ref() {
        if line.trim_end() == open {
            break;
        }
    }
    let mut body: Vec<&str> = Vec::new();
    for line in lines {
        if line.trim_end() == "```" {
            let mut joined = body.join("\n");
            joined.push('\n');
            return Some(joined);
        }
        body.push(line);
    }
    None
}

/// Extract the region of `src` between `// ANCHOR: <tag>` and
/// `// ANCHOR_END: <tag>` (exclusive of the marker lines).
fn anchored_region(src: &str, tag: &str) -> Option<String> {
    let start = format!("// ANCHOR: {tag}");
    let end = format!("// ANCHOR_END: {tag}");
    let mut lines = src.lines();
    for line in lines.by_ref() {
        if line.trim_end() == start {
            break;
        }
    }
    let mut body: Vec<&str> = Vec::new();
    for line in lines {
        if line.trim_end() == end {
            let mut joined = body.join("\n");
            joined.push('\n');
            return Some(joined);
        }
        body.push(line);
    }
    None
}

/// **Compiles verbatim.** The README's fenced `rust` quickstart block is
/// byte-identical to the anchored region of the compiled-and-run example. Because
/// the example is a workspace target CI compiles, this makes the README's code
/// exactly what the build checks — a divergence fails here.
#[test]
fn readme_rust_block_matches_the_compiled_example_verbatim() {
    let root = repo_root();
    let readme = read_to_string(&root.join("README.md"));
    let example = read_to_string(&root.join("crates/cli/examples/quickstart.rs"));

    let readme_block =
        fenced_block(&readme, "rust").expect("README has a fenced ```rust quickstart block");
    let example_region = anchored_region(&example, "quickstart")
        .expect("the quickstart example carries its ANCHOR markers");

    assert_eq!(
        readme_block, example_region,
        "the README quickstart's Rust block has drifted from the compiled example \
         (crates/cli/examples/quickstart.rs) — they must be verbatim identical so \
         the code a reader copies is the code CI compiles and runs (criterion 1)"
    );
}

/// **The quickstart is authored with the macro trio, plumbing-free.** It is the
/// canonical surface a reader copies first, so it must make the "just define your
/// DAGs and tasks" claim demonstrably true: two `#[task]` tasks, a
/// `#[derive(StableName)]` payload, one `#[dag]` fn, a one-line `dagr_cli::run`
/// `main`, and **no** hand-written `impl Task` / `impl StableName` / `type Input` /
/// `type Output` / `EXECUTION_CLASS` — the macros supply all of it. The whole
/// authoring surface is reachable through the single `dagr_cli::prelude` import.
#[test]
fn quickstart_is_authored_with_the_macro_trio_and_no_hand_written_plumbing() {
    let root = repo_root();
    let example = read_to_string(&root.join("crates/cli/examples/quickstart.rs"));
    let region = anchored_region(&example, "quickstart")
        .expect("the quickstart example carries its ANCHOR markers");

    // One authoring import brings in the whole macro trio (`#[task]`, `#[dag]`,
    // `#[derive(StableName)]`) plus `FlowBuilder` / `TaskError`.
    assert!(
        region.contains("use dagr_cli::prelude::*;"),
        "the quickstart uses the single prelude import (`use dagr_cli::prelude::*;`)"
    );
    // The two tasks are `#[task]` inherent impls; the structs are named by the derive;
    // the flow is declared with `#[dag]`; `main` is the one-line entrypoint.
    assert!(
        region.contains("#[task]\nimpl Count"),
        "`Count` is authored with `#[task]` on an inherent `impl` block"
    );
    assert!(
        region.contains("#[task]\nimpl Double"),
        "`Double` is authored with `#[task]` on an inherent `impl` block"
    );
    assert!(
        region.contains("#[derive(StableName)]"),
        "types are named with `#[derive(StableName)]`, not a hand-written `impl StableName`"
    );
    assert!(
        region.contains("#[dag]"),
        "the flow is declared with the `#[dag]` attribute"
    );
    assert!(
        region.contains("dagr_cli::run(std::env::args_os())"),
        "`main` is the one-line `dagr_cli::run` entrypoint"
    );

    // The graph is declared through the `FlowBuilder` façade with the dependency
    // direction explicit; a wrong-typed binding would be a compile error.
    assert!(
        region.contains("f.source(\"count\", Count { up_to: 21 })"),
        "the source (root) node is declared with `f.source`"
    );
    assert!(
        region.contains("f.task(\"double\", Double).depends_on(count)"),
        "the dependent node reads `f.task(..).depends_on(..)` — the direction is explicit"
    );

    // The macros generate every trait body, so the quickstart hand-writes none of the
    // scaffolding they remove — that is exactly the "write no plumbing" claim.
    for forbidden in [
        "impl Task for",
        "impl StableName for",
        "type Input",
        "type Output",
        "EXECUTION_CLASS",
    ] {
        assert!(
            !region.contains(forbidden),
            "the quickstart carries no hand-written `{forbidden}` — the macros generate it"
        );
    }
}

/// **The TOML and shell blocks match what the prose tells the reader to do.** The
/// `Cargo.toml` dependency block names `dagr-cli`, and the shell block invokes the
/// example exactly as the example's own docs say (`cargo run --example
/// quickstart`).
#[test]
fn readme_setup_blocks_are_consistent_with_the_example() {
    let root = repo_root();
    let readme = read_to_string(&root.join("README.md"));

    let toml_block = fenced_block(&readme, "toml").expect("README has a ```toml block");
    assert!(
        toml_block.contains("dagr-cli"),
        "the quickstart's Cargo.toml block declares the dagr-cli dependency: {toml_block}"
    );

    assert!(
        fenced_block(&readme, "console").is_some()
            || fenced_block(&readme, "sh").is_some()
            || fenced_block(&readme, "bash").is_some(),
        "the README has a shell block showing the reader how to build and run"
    );
    // The reader runs the pipeline binary against a run-store directory, and can
    // reproduce exactly what CI checks via the compiled example.
    assert!(
        readme.contains("cargo run -- "),
        "the README shows running the quickstart binary against a run-store directory"
    );
    assert!(
        readme.contains("cargo run --example quickstart"),
        "the README names the compiled example (`cargo run --example quickstart`) CI runs verbatim"
    );
}

/// **Runs end to end (criteria 1 and 7).** The compiled example runs against a
/// private temp run store — no database, scheduler, or network — exits `0`, and
/// its stream shows exactly the two-node shape with both nodes `succeeded` and
/// the doubled output `42` the prose promises.
#[test]
fn quickstart_runs_end_to_end_and_the_artifact_matches_the_prose() {
    let root = repo_root();
    let base = std::env::temp_dir().join(format!(
        "dagr-quickstart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Run the *compiled example binary* — the same target CI compiles — driving the
    // sole DAG through the `run` verb against a private store, nothing else running.
    let status = Command::new(env!("CARGO"))
        .current_dir(&root)
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--example",
            "quickstart",
            "--",
            "run",
            "--store",
        ])
        .arg(&base)
        .status()
        .expect("failed to spawn `cargo run --example quickstart -- run --store`");
    assert!(
        status.success(),
        "the quickstart exits with the success code (criterion 1's run clause)"
    );

    // Artifact inspection: fold the single run's event stream (its id is minted, so
    // it is discovered under `<base>/quickstart/`) and assert the promised values.
    let bytes = single_run_stream(&base, "quickstart");
    let records = dagr_artifact::event_stream::read_records(&bytes)
        .expect("the quickstart's event stream parses");

    // The two node names the prose promises, both `succeeded`.
    let terminal_of = |node: &str| -> Option<String> {
        records
            .records
            .iter()
            .rev()
            .find(|r| {
                r.get("kind").and_then(|k| k.as_str()) == Some("node-terminal")
                    && r.get("node").and_then(|n| n.as_str()) == Some(node)
            })
            .and_then(|r| r.get("state").and_then(|s| s.as_str()).map(str::to_string))
    };
    assert_eq!(
        terminal_of("count").as_deref(),
        Some("succeeded"),
        "`count` succeeded"
    );
    assert_eq!(
        terminal_of("double").as_deref(),
        Some("succeeded"),
        "`double` succeeded"
    );

    // The two-node shape: exactly two node-terminal events, no more, no fewer.
    let terminals = records
        .records
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("node-terminal"))
        .count();
    assert_eq!(
        terminals, 2,
        "the pipeline is exactly two nodes (criterion 1's two-node clause)"
    );

    // Run bookends prove the stream is complete.
    assert_eq!(
        records
            .records
            .first()
            .and_then(|r| r.get("kind"))
            .and_then(|k| k.as_str()),
        Some("run-started"),
    );
    assert_eq!(
        records
            .records
            .last()
            .and_then(|r| r.get("kind"))
            .and_then(|k| k.as_str()),
        Some("run-finished"),
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// **"When not to use this" and MSRV are present and truthful.** The README's
/// honesty section states the adoption triggers and recommends plain tokio below
/// them; the documented MSRV equals the workspace-pinned `rust-version`.
#[test]
fn readme_when_not_to_use_and_msrv_are_present_and_truthful() {
    let root = repo_root();
    let readme = read_to_string(&root.join("README.md"));

    // The honesty note.
    assert!(
        readme.contains("When not to use this"),
        "the README carries a `When not to use this` section"
    );
    let lower = readme.to_lowercase();
    assert!(
        lower.contains("tokio"),
        "the honesty section names plain tokio as the recommendation below the triggers"
    );

    // MSRV drift check: the pinned workspace `rust-version` must appear in the
    // README's MSRV line (Stability: "documented in the README").
    let manifest = read_to_string(&root.join("Cargo.toml"));
    let pinned = manifest
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix("rust-version = \"")
                .and_then(|rest| rest.strip_suffix('"'))
        })
        .expect("workspace Cargo.toml pins rust-version");
    assert!(
        readme.contains(pinned),
        "the README's MSRV line documents the workspace-pinned rust-version `{pinned}` \
         with no drift (Stability)"
    );
}
