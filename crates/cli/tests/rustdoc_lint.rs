//! **Rustdoc-on-public-items lint — present at head, fails on a missing doc**
//! (arch.md "Documentation"; ticket T64).
//!
//! The Documentation deliverable requires that *every public item* carry rustdoc,
//! enforced by a CI lint that fails on any missing-docs finding. dagr wires this
//! as a **rust** lint in the workspace manifest — `missing_docs = "warn"` under
//! `[workspace.lints.rust]`, escalated to an error by the gate's
//! `cargo clippy --workspace --all-targets -- -D warnings` (and the CI
//! `RUSTFLAGS: -D warnings`). Every member crate opts in with
//! `[lints] workspace = true`, so the whole public surface is covered from one
//! place.
//!
//! This suite pins the lint two ways:
//!
//! 1. **The policy is configured.** The workspace manifest and the single-source
//!    lint policy both declare `missing_docs`, so the enforcement is not folklore.
//! 2. **The lint has teeth (the negative control).** A throwaway source with an
//!    *undocumented* public item, compiled under `-D missing_docs`, is **rejected**
//!    by `rustc` and the diagnostic names the missing-docs lint — and the same
//!    source with the doc comment restored **compiles**. This is the executable
//!    form of the Test plan's "rustdoc lint fails on an undocumented public item,
//!    restoring the doc comment makes it pass."
//!
//! The "every public item is documented at head" half is enforced continuously by
//! the gate's clippy/rustdoc steps over the real workspace (a missing doc reds the
//! build), so it needs no separate test here — it is the running state of the
//! merge gate itself.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// **The policy is configured.** The workspace manifest and the lint-policy
/// source both declare `missing_docs`, so the rustdoc-on-public-items lint is
/// applied workspace-wide from one place.
#[test]
fn missing_docs_is_configured_workspace_wide() {
    let root = repo_root();
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");
    assert!(
        manifest.contains("missing_docs"),
        "the workspace manifest declares the missing_docs lint under [workspace.lints.rust]"
    );
    // Every member opts into the workspace lint table; spot-check dagr-cli.
    let cli_manifest =
        std::fs::read_to_string(root.join("crates/cli/Cargo.toml")).expect("read cli Cargo.toml");
    assert!(
        cli_manifest.contains("[lints]") && cli_manifest.contains("workspace = true"),
        "dagr-cli opts into the workspace lint policy (`[lints] workspace = true`)"
    );
}

/// Compile a one-line throwaway crate under `-D missing_docs` and return whether
/// `rustc` accepted it, plus its stderr. The source is written to a temp file so
/// the invocation is hermetic and needs no fixture on disk.
fn compile_under_missing_docs(source: &str, tag: &str) -> (bool, String) {
    let dir = std::env::temp_dir().join(format!(
        "dagr-rustdoc-lint-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let src = dir.join("lib.rs");
    std::fs::write(&src, source).unwrap();

    // Use the rustc that sits beside the cargo running this test — under this
    // workspace that is the pinned toolchain (rust-toolchain.toml), so the lint's
    // behaviour is deterministic.
    let output = Command::new("rustc")
        .args([
            "--crate-type",
            "lib",
            "--edition",
            "2021",
            "-D",
            "missing_docs",
        ])
        .arg(&src)
        .arg("--out-dir")
        .arg(&dir)
        .output()
        .expect("failed to spawn rustc");

    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let _ = std::fs::remove_dir_all(&dir);
    (output.status.success(), stderr)
}

/// **The lint has teeth (the negative control).** An undocumented public item
/// compiled under `-D missing_docs` is REJECTED and the diagnostic names the
/// missing-docs lint; restoring the doc comment makes it COMPILE.
#[test]
fn missing_docs_rejects_an_undocumented_public_item_and_accepts_a_documented_one() {
    // Both sources carry a crate-level doc (`//!`), so ONLY the item-level doc
    // comment on `public_item` distinguishes them — isolating the missing-docs
    // finding to the public item, which is what the lint enforces.
    let crate_doc = "//! A throwaway crate.\n#![allow(dead_code)]\n";

    // Undocumented public item — must be rejected under `-D missing_docs`.
    let undocumented = format!("{crate_doc}pub fn public_item() {{}}\n");
    let (accepted, stderr) = compile_under_missing_docs(&undocumented, "undoc");
    assert!(
        !accepted,
        "an undocumented public item is rejected under -D missing_docs; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("missing-docs")
            || stderr.contains("missing_docs")
            || stderr.to_lowercase().contains("missing documentation"),
        "the diagnostic names the missing-docs lint: {stderr}"
    );
    assert!(
        stderr.contains("function"),
        "the diagnostic names the undocumented public function: {stderr}"
    );

    // The same item WITH a doc comment compiles — restoring the doc makes it pass.
    let documented =
        format!("{crate_doc}/// A documented public function.\npub fn public_item() {{}}\n");
    let (accepted, stderr) = compile_under_missing_docs(&documented, "doc");
    assert!(
        accepted,
        "the documented public item compiles under -D missing_docs; stderr:\n{stderr}"
    );
}
