//! Integration test that covers system-level acceptance criterion 8's machine
//! part (`SL8machine`): *coverage of every machine-classed criterion is itself
//! verified in CI from a checked-in criteria matrix* (arch.md "System-level
//! acceptance", criterion 8; ticket 006 / T7).
//!
//! This test IS the covering test the coverage matrix maps `SL8machine` to. It
//! runs the checked-in coverage-matrix verifier (`scripts/check-coverage-matrix.sh`)
//! against the real matrix and the real workspace test suite and asserts it
//! passes. Because this test itself lives in the cargo suite, `SL8machine`'s
//! mapping is a real, existing, non-dangling test id — the enforcement is
//! self-consistent: the matrix that maps `SL8machine` is verified by the very
//! script `SL8machine` names.
//!
//! It is hosted in `dagr-cli` because cli is the workspace's integration crate
//! (it is the one place the live pipeline, artifacts, and rendering meet, per
//! the T1 ADR), so an integration test that spans the whole repo belongs here.

use std::path::PathBuf;
use std::process::Command;

/// Locate the repository root from this crate's manifest directory
/// (`crates/cli`) by walking up two levels.
fn repo_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// The coverage-matrix verifier passes against the real checked-in matrix and
/// the real test suite. This is the mapped test for `SL8machine`.
#[test]
fn verifier_passes_against_the_checked_in_matrix() {
    let root = repo_root();
    let verifier = root.join("scripts/check-coverage-matrix.sh");
    assert!(
        verifier.is_file(),
        "coverage-matrix verifier is missing at {}",
        verifier.display()
    );

    let output = Command::new("bash")
        .arg(&verifier)
        .current_dir(&root)
        .output()
        .expect("failed to spawn the coverage-matrix verifier");

    assert!(
        output.status.success(),
        "coverage-matrix verifier failed (SL8machine).\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
