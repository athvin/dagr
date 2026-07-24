//! Build script embedding **build provenance** into the pipeline binary at build
//! time (arch.md `### C20 · Graph artifact`; "Stability · Supply chain": *"build
//! provenance (tool version, git commit, lockfile hash) is embedded in every
//! binary and every artifact"*).
//!
//! It resolves three provenance values **once, at build time**, and exposes each
//! as a `cargo::rustc-env` variable the crate reads through `env!` (so the value
//! is compiled into the binary and is fixed per binary, identical across every
//! emission from it):
//!
//! - **tool version** — the workspace package version (`CARGO_PKG_VERSION`).
//! - **git commit** — the `HEAD` commit SHA of the source tree the binary was
//!   built from, or a stable `unknown` sentinel when git is unavailable (a source
//!   tarball build, a shallow checkout without `.git`). Provenance must never fail
//!   the build.
//! - **lockfile hash** — a content hash of `Cargo.lock` (the resolved dependency
//!   set), so the same locked dependency graph yields the same hash and a
//!   dependency bump changes it. The hash is a hex FNV-1a digest with a `fnv1a-64:`
//!   prefix — dependency-free (the build script pulls in no crate) and
//!   deterministic; the concrete algorithm is provenance metadata, not a security
//!   boundary.
//!
//! The build script is deliberately **network-free and credential-free**: it runs
//! `git rev-parse HEAD` if git is on `PATH` (no fetch, no remote), reads a local
//! file for the lockfile hash, and otherwise emits stable sentinels — so a build
//! in an empty environment still produces a complete, byte-stable header, which is
//! what lets the graph artifact emit in CI on every pull request (C20).

use std::path::PathBuf;
use std::process::Command;

fn main() {
    // The tool version is the crate's own package version (workspace-inherited).
    let tool_version = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".into());
    println!("cargo:rustc-env=DAGR_BUILD_TOOL_VERSION={tool_version}");

    // The git commit SHA of HEAD, or a stable sentinel. Never fail the build.
    let git_commit = git_head_commit().unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=DAGR_BUILD_GIT_COMMIT={git_commit}");

    // The lockfile hash over the resolved dependency set (Cargo.lock at the
    // workspace root). A missing lockfile yields a stable sentinel.
    let lockfile_hash = lockfile_hash().unwrap_or_else(|| "fnv1a-64:unknown".into());
    println!("cargo:rustc-env=DAGR_BUILD_LOCKFILE_HASH={lockfile_hash}");

    // Re-run when the resolved dependency set changes, so the lockfile hash stays
    // truthful. (Git-commit staleness across a rebuild without a source change is
    // acceptable provenance drift, not a correctness bug; the value is fixed per
    // *built* binary, which is the C20 requirement.)
    if let Some(lock) = workspace_lockfile_path() {
        println!("cargo:rerun-if-changed={}", lock.display());
    }
}

/// The `HEAD` commit SHA via `git rev-parse HEAD`, or `None` when git is
/// unavailable or the tree is not a git repo. Network-free (no fetch).
fn git_head_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if sha.is_empty() {
        None
    } else {
        Some(sha)
    }
}

/// The workspace `Cargo.lock` path — two levels up from this crate's manifest
/// dir (`crates/cli` → workspace root).
fn workspace_lockfile_path() -> Option<PathBuf> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let root = PathBuf::from(manifest).parent()?.parent()?.to_path_buf();
    Some(root.join("Cargo.lock"))
}

/// A `fnv1a-64:`-prefixed hex FNV-1a digest of the workspace `Cargo.lock` bytes,
/// or `None` when the lockfile is absent. Dependency-free and deterministic.
fn lockfile_hash() -> Option<String> {
    let path = workspace_lockfile_path()?;
    let bytes = std::fs::read(path).ok()?;
    Some(format!("fnv1a-64:{:016x}", fnv1a_64(&bytes)))
}

/// FNV-1a over bytes — the dependency-free, deterministic digest the rest of the
/// tree already uses for stable non-cryptographic hashing.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
