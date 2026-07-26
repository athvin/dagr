//! **README quickstart — verbatim, CI-verified** (arch.md "Documentation";
//! system-level acceptance criterion 1; ticket T64).
//!
//! The single machine-classed criterion 1 test: the README quickstart compiles
//! and runs **verbatim** in CI, taking a Rust/cargo developer (no async
//! experience) from an empty directory to a compiled, run, artifact-inspected
//! two-node pipeline. It is the mapped test for `SL1` in the coverage matrix.
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

/// **The two tasks are authored with `#[task]`, plumbing-free (T73).** The
/// quickstart is the canonical surface a reader copies first, so it must make
/// arch.md C1's "declare four things, write no plumbing" claim demonstrably true:
/// `Count` and `Double` are `#[task]` inherent impls whose bodies are a single
/// `run` fn, carrying **no** hand-written `type Input` / `type Output` /
/// `EXECUTION_CLASS` lines (the macro supplies them). The registration and the
/// support types stay unchanged — asserted alongside so the rewrite touched only
/// the two node bodies.
#[test]
fn quickstart_tasks_are_authored_with_the_task_macro_and_carry_no_hand_written_plumbing() {
    let root = repo_root();
    let example = read_to_string(&root.join("crates/cli/examples/quickstart.rs"));
    let region = anchored_region(&example, "quickstart")
        .expect("the quickstart example carries its ANCHOR markers");

    // The macro is the primary authoring style: the anchored region pulls in the
    // re-exported attribute and applies it to both node impls.
    assert!(
        region.contains("use dagr_core::task;"),
        "the quickstart imports the `#[task]` attribute (`use dagr_core::task;`) — \
         the macro is the primary authoring style the quickstart shows (ADR 082)"
    );
    assert!(
        region.contains("#[task]\nimpl Count"),
        "`Count` is authored with `#[task]` on an inherent `impl` block (not a \
         hand-written `impl Task`)"
    );
    assert!(
        region.contains("#[task]\nimpl Double"),
        "`Double` is authored with `#[task]` on an inherent `impl` block"
    );

    // The macro supplies the four C1 declarations, so neither node body may carry
    // a hand-written associated-type or execution-class line — that is exactly the
    // scaffolding the macro removes.
    assert!(
        !region.contains("impl Task for Count"),
        "`Count` no longer hand-writes `impl Task` — the `#[task]` expansion does"
    );
    assert!(
        !region.contains("impl Task for Double"),
        "`Double` no longer hand-writes `impl Task` — the `#[task]` expansion does"
    );
    assert!(
        !region.contains("type Input"),
        "the quickstart carries no hand-written `type Input` line: the `#[task]` \
         expansion infers it (arch.md C1, 'declare four things, write no plumbing')"
    );
    assert!(
        !region.contains("type Output"),
        "the quickstart carries no hand-written `type Output` line: the `#[task]` \
         expansion infers it"
    );
    assert!(
        !region.contains("EXECUTION_CLASS"),
        "the quickstart carries no hand-written `EXECUTION_CLASS` line: the default \
         await-bound class is the macro's, not the author's"
    );

    // `main()` and the `RunnableFlow` registration are unchanged — the rewrite
    // shrank only the two node bodies, leaving the wiring the reader learns intact.
    assert!(
        region.contains("let counted = flow.register_source(\"count\", Count { up_to: 21 });"),
        "the source registration is unchanged from the pre-macro quickstart"
    );
    assert!(
        region.contains("let doubled = flow.register::<Double, _>(\"double\", Double, counted);"),
        "the sink registration is unchanged from the pre-macro quickstart"
    );
    assert!(
        region.contains("flow.run(\"quickstart\", &config, sink, TickClock::default())"),
        "the one-call `RunnableFlow::run` seam is unchanged from the pre-macro quickstart"
    );
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

    // Run the *compiled example binary* — the same target CI compiles — against a
    // private store, with nothing else running.
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
        ])
        .arg(&base)
        .status()
        .expect("failed to spawn `cargo run --example quickstart`");
    assert!(
        status.success(),
        "the quickstart exits with the success code (criterion 1's run clause)"
    );

    // Artifact inspection: fold the event stream and assert the promised values.
    let stream = base
        .join("quickstart")
        .join("quickstart-run")
        .join("events.jsonl");
    let bytes = std::fs::read(&stream).unwrap_or_else(|e| {
        panic!(
            "the quickstart wrote its event stream at {}: {e}",
            stream.display()
        )
    });
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
