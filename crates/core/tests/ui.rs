//! dagr's compile-failure (UI-test) harness — ticket T8 (007).
//!
//! # What this is
//!
//! The framework tests itself the way it asks pipelines to test themselves: a
//! wrong-type binding must fail to *compile*, and the error message must name
//! both the expected and the supplied type (arch.md **C3 · Data dependency**,
//! **C28 · Testing surface**). This file is the single, canonical entry point
//! of the harness that proves it. Every compile-failure case is a `.rs` sample
//! in [`tests/ui/`](./ui) with a sibling `.stderr` snapshot; this runner
//! discovers all of them, so later tickets (notably **T12**, the full wiring
//! compile-fail suite) add cases by dropping files into that directory with **no
//! changes to this harness**.
//!
//! At this milestone exactly one seed sample ships:
//! [`tests/ui/wrong_type_binding.rs`](./ui/wrong_type_binding.rs) — a throwaway,
//! non-compiling snippet whose diagnostic names two distinct type names. It is
//! not a use of dagr's real authoring API (that lands in T9+).
//!
//! # The assertion: both type names, nothing more
//!
//! A `.stderr` snapshot is **not** an exact copy of the compiler output. It is a
//! canonical, reviewable list of the type-name **substrings** that must appear
//! in the sample's diagnostic (one per line; `#` comments and blank lines
//! ignored). The harness asserts only that:
//!
//! 1. the sample **fails to compile** — a sample that unexpectedly compiles is a
//!    hard failure, so a no-op case can never masquerade as coverage (C2); and
//! 2. **every** substring in the snapshot appears somewhere in the diagnostic.
//!
//! It never asserts exact prose, wording, note count, or span layout, so
//! ordinary compiler-message churn does not break the suite (C3 message-quality
//! clause; C28 "assert only that both type names appear").
//!
//! # Pinned toolchain
//!
//! Snapshots are pinned to the single workspace toolchain
//! (`rust-toolchain.toml`, currently 1.95.0). The harness compiles each sample
//! with the `rustc` that sits beside the `cargo` running the test (`$CARGO`),
//! which under this workspace is exactly the pinned toolchain — so diagnostic
//! output is deterministic. Multi-toolchain or multi-platform snapshot matrices
//! are explicitly out of scope (T8 Out of scope).
//!
//! # Regenerating (blessing) a snapshot
//!
//! Snapshots are rewritten **deliberately, never silently**. The single
//! documented command, run from the repo root, is:
//!
//! ```text
//! DAGR_BLESS=1 cargo test -p dagr-core --test ui
//! ```
//!
//! Blessing is opt-in via the `DAGR_BLESS` environment variable and is kept out
//! of the default test path: a plain `cargo test` never writes a snapshot, so a
//! stale snapshot fails the build (nonzero exit) rather than being silently
//! overwritten. The regenerated file is a reviewable diff, not a binary blob.
//!
//! # Why this harness is hand-built rather than `trybuild`
//!
//! The ticket names a "trybuild/UI-test harness." `trybuild` (and `ui_test`)
//! match a captured `.stderr` **exactly**, line by line, against a checked-in
//! snapshot. That is directly incompatible with what C3 and C28 require and
//! what the T8 Test plan demands ("prose churn does not break the suite"): the
//! assertion must key **only** on the two type-name substrings and tolerate
//! wording, spans, and note count. There is no trybuild configuration that
//! turns its exact-match into substring-only matching, so a trybuild-driven
//! snapshot would either be brittle (full exact prose) or fail (substrings
//! only). This harness is therefore a small, self-contained trybuild-*style*
//! runner — a dedicated `tests/ui/` directory, one entry point, checked-in
//! snapshots, a single-command blessing flow — with the both-type-names
//! assertion the spec actually asks for. A welcome consequence: it adds **no**
//! third-party dependency (the trybuild tree pulls `unicode-ident`, whose
//! `(MIT OR Apache-2.0) AND Unicode-3.0` license would force a `Unicode-3.0`
//! exception into `deny.toml`), keeping the core dependency set minimal
//! (arch.md "Stability"). Recorded as a T8 design-decision resolution.
//!
//! # Toolchain-bump contract
//!
//! When the pinned toolchain in `rust-toolchain.toml` (a T7 policy artifact)
//! changes, regenerate the snapshots **deliberately** through the blessing
//! command above and review the resulting diff before committing — the same
//! way a source change is reviewed. Because the assertion keys only on the
//! two type names and tolerates prose churn, most bumps require no
//! regeneration at all; when one does, this is the flow.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The environment variable that opts a run into the blessing (snapshot-update)
/// flow. Kept out of the default test path so snapshots are never silently
/// overwritten.
const BLESS_ENV: &str = "DAGR_BLESS";

/// Directory (relative to this crate's manifest) holding the UI samples and
/// their snapshots. A single directory keeps the harness discoverable and lets
/// T12 add cases with no wiring changes.
const UI_DIR: &str = "tests/ui";

/// Resolve the `rustc` that belongs to the toolchain running this test. Cargo
/// exports `$CARGO` pointing at its own binary; the matching `rustc` is its
/// sibling. Under this workspace that is the toolchain pinned in
/// `rust-toolchain.toml`, which is what makes the diagnostics deterministic.
fn pinned_rustc() -> PathBuf {
    let cargo: OsString = std::env::var_os("CARGO")
        .expect("CARGO is set by `cargo test`; the harness must be run through cargo");
    PathBuf::from(&cargo).with_file_name("rustc")
}

/// This crate's manifest directory (`crates/core`).
fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The `target/<profile>` directory holding the built `dagr_core` rlib and the
/// `deps/` dependency dir. Derived from the running test executable's own path:
/// an integration-test binary lives at `<target>/<profile>/deps/<name>-<hash>`,
/// so two levels up is exactly `<target>/<profile>` — profile-agnostic (debug or
/// release) and independent of the workspace-level `CARGO_TARGET_TMPDIR`. A
/// real-API sample (one that imports `dagr_core`) is linked against the rlib
/// there; a self-contained sample needs none of this.
fn target_profile_dir() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    // .../target/<profile>/deps/ui-<hash>  ->  .../target/<profile>
    exe.parent().and_then(Path::parent).map(Path::to_path_buf)
}

/// Whether `sample` references the real `dagr_core` crate (so it must be linked
/// against the built rlib rather than compiled standalone). A self-contained
/// throwaway sketch (the T5 fixtures) does not, and is compiled bare.
fn needs_dagr_core(sample: &Path) -> bool {
    fs::read_to_string(sample).is_ok_and(|src| src.contains("dagr_core"))
}

/// Compile `sample` with the pinned `rustc`, emitting no artifact. Returns the
/// combined diagnostic text and whether compilation *succeeded*. A sample that
/// imports `dagr_core` is linked against the workspace-built rlib so the real
/// authoring-API compile-fail cases (T11/T12) resolve their imports and produce
/// the *intended* diagnostic — never a spurious unresolved-import error.
fn compile_sample(sample: &Path) -> (String, bool) {
    // A unique throwaway output path under the target dir so parallel/repeat
    // runs never collide and nothing is left behind on disk.
    let mut out = std::env::temp_dir();
    out.push(format!(
        "dagr-ui-{}-{}.out",
        sample.file_stem().unwrap().to_string_lossy(),
        std::process::id()
    ));

    let mut cmd = Command::new(pinned_rustc());
    cmd.arg("--edition")
        .arg("2021")
        .arg("--crate-type")
        .arg("bin")
        .arg("--color")
        .arg("never")
        .arg("-o")
        .arg(&out);

    // Link the real `dagr_core` rlib for samples that use the shipped API.
    if needs_dagr_core(sample) {
        let profile_dir = target_profile_dir().expect(
            "the running test executable resolves to a target/<profile> dir; a real-API \
             UI sample needs the built dagr_core rlib beside it",
        );
        let rlib = profile_dir.join("libdagr_core.rlib");
        assert!(
            rlib.exists(),
            "expected the built dagr_core rlib at {} — run `cargo test` (which builds \
             the lib first) rather than invoking this harness in isolation",
            rlib.display(),
        );
        cmd.arg("--extern")
            .arg(format!("dagr_core={}", rlib.display()))
            .arg("-L")
            .arg(format!("dependency={}", profile_dir.join("deps").display()));
    }

    let result = cmd
        .arg(sample)
        .output()
        .expect("failed to invoke the pinned rustc");

    // rustc writes diagnostics to stderr; keep stdout too for completeness.
    let mut diagnostic = String::from_utf8_lossy(&result.stderr).into_owned();
    diagnostic.push_str(&String::from_utf8_lossy(&result.stdout));
    let _ = fs::remove_file(&out); // best-effort cleanup; absent on failure.
    (diagnostic, result.status.success())
}

/// Read the required type-name substrings from a snapshot: every non-blank line
/// that is not a `#` comment, trimmed.
fn required_substrings(snapshot_body: &str) -> Vec<String> {
    snapshot_body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect()
}

/// The header comment prepended to a freshly-blessed snapshot so a regenerated
/// file stays canonical and self-documenting.
fn snapshot_header(stem: &str) -> String {
    format!(
        "# Compile-failure snapshot for `{stem}.rs` — regenerated by the T8 harness.\n\
         #\n\
         # Each non-comment, non-blank line is a type-name substring that MUST appear in\n\
         # the sample's compiler diagnostic. The harness asserts ONLY that both appear —\n\
         # never exact prose, wording, note count, or spans. Rewritten deliberately via:\n\
         #\n\
         #     DAGR_BLESS=1 cargo test -p dagr-core --test ui\n\
         #\n\
         # (see tests/ui.rs). Review this diff on a pinned-toolchain bump.\n\n"
    )
}

/// Extract the two type names an E0308 `expected .., found ..` diagnostic
/// carries (the words after the `expected` and `found` backtick markers). Used
/// only by the blessing flow to seed a snapshot's substring list; it is
/// intentionally simple — a human reviews the blessed diff.
fn extract_type_names(diagnostic: &str) -> Vec<String> {
    let mut names = Vec::new();
    for marker in ["expected `", "found `"] {
        if let Some(start) = diagnostic.find(marker) {
            let rest = &diagnostic[start + marker.len()..];
            if let Some(end) = rest.find('`') {
                let name = rest[..end].to_owned();
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
    }
    names
}

/// Discover every `tests/ui/*.rs` sample, sorted for a stable order.
fn discover_samples(ui_dir: &Path) -> Vec<PathBuf> {
    let mut samples: Vec<PathBuf> = fs::read_dir(ui_dir)
        .expect("tests/ui directory exists")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    samples.sort();
    samples
}

/// The single UI-test entry point. Runs every sample under the pinned toolchain
/// and enforces the both-type-names contract (or, under `DAGR_BLESS`, rewrites
/// the snapshots deliberately).
#[test]
fn ui() {
    let ui_dir = manifest_dir().join(UI_DIR);
    let samples = discover_samples(&ui_dir);
    assert!(
        !samples.is_empty(),
        "no UI samples found in {} — the harness ships at least one seed case",
        ui_dir.display()
    );

    let blessing = std::env::var_os(BLESS_ENV).is_some();

    for sample in &samples {
        let stem = sample.file_stem().unwrap().to_string_lossy().into_owned();
        let snapshot_path = sample.with_extension("stderr");

        let (diagnostic, compiled) = compile_sample(sample);

        // (C2) A compile-fail sample that unexpectedly compiles is a hard
        // failure — a no-op case must never pass as coverage. This check runs
        // even under blessing: we never bless a passing compile.
        assert!(
            !compiled,
            "UI sample {} was expected to FAIL compilation but it compiled cleanly; \
             a compile-fail case that no longer fails cannot count as coverage",
            sample.display()
        );

        if blessing {
            // Deliberate regeneration: rewrite the snapshot's substring list
            // from the current diagnostic's type names, keeping it canonical
            // and reviewable.
            let names = extract_type_names(&diagnostic);
            assert!(
                names.len() >= 2,
                "blessing {}: expected to extract at least two type names from the \
                 diagnostic, found {:?}",
                sample.display(),
                names
            );
            let body = format!("{}{}\n", snapshot_header(&stem), names.join("\n"));
            fs::write(&snapshot_path, body).unwrap_or_else(|e| {
                panic!(
                    "blessing {}: could not write snapshot: {e}",
                    snapshot_path.display()
                )
            });
            eprintln!("blessed {}", snapshot_path.display());
            continue;
        }

        // Frozen (default) run: the snapshot must exist and every substring it
        // names must appear in the diagnostic. Both together.
        let snapshot = fs::read_to_string(&snapshot_path).unwrap_or_else(|e| {
            panic!(
                "missing snapshot {} for sample {} ({e}); bless it with \
                 `DAGR_BLESS=1 cargo test -p dagr-core --test ui`",
                snapshot_path.display(),
                sample.display()
            )
        });
        let required = required_substrings(&snapshot);
        assert!(
            required.len() >= 2,
            "snapshot {} must name at least two distinct type-name substrings (found {:?})",
            snapshot_path.display(),
            required
        );
        for needle in &required {
            assert!(
                diagnostic.contains(needle.as_str()),
                "snapshot {} requires the substring `{needle}`, but the diagnostic for {} \
                 did not contain it.\n--- diagnostic ---\n{diagnostic}\n--- end ---",
                snapshot_path.display(),
                sample.display(),
            );
        }
    }
}

/// The resolved toolchain equals the pin in `rust-toolchain.toml`, establishing
/// the determinism the larger T12 suite relies on (T8 Test plan: "Pinned
/// toolchain governs output"). We read the pin from the workspace file and ask
/// the resolved `rustc` for its version; the pinned channel must appear in it.
#[test]
fn pinned_toolchain_governs_output() {
    // repo root = crates/core -> crates -> root
    let repo_root = manifest_dir()
        .parent()
        .and_then(Path::parent)
        .expect("crates/core has a two-level ancestor")
        .to_path_buf();
    let toolchain_toml = repo_root.join("rust-toolchain.toml");
    let pin_body = fs::read_to_string(&toolchain_toml).expect("rust-toolchain.toml is readable");

    // Parse the pinned `channel = "X"` line without pulling in a TOML crate.
    let pinned = pin_body
        .lines()
        .find_map(|line| {
            let line = line.trim();
            let rest = line
                .strip_prefix("channel")?
                .trim_start()
                .strip_prefix('=')?;
            Some(rest.trim().trim_matches('"').to_owned())
        })
        .expect("rust-toolchain.toml declares a pinned channel");

    let version = Command::new(pinned_rustc())
        .arg("--version")
        .output()
        .expect("the pinned rustc reports its version");
    let version = String::from_utf8_lossy(&version.stdout);

    assert!(
        version.contains(&pinned),
        "resolved rustc `{}` does not report the pinned channel `{pinned}` from {}",
        version.trim(),
        toolchain_toml.display(),
    );
}
