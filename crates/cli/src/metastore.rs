//! The `dagr metastore init` CLI verb (M7, T83) — behind the default-off
//! `metastore` feature.
//!
//! `dagr metastore init [--store <path>]` creates/opens the libSQL run-index
//! store at `<path>` (default [`default_metastore_path`](crate::metastore::default_metastore_path))
//! and applies the ordered idempotent migrations, exiting `0` on success.
//! Re-running is a no-op success (the migrations are `CREATE … IF NOT EXISTS`).
//!
//! This module is compiled only when `--features metastore` is set; the whole
//! edge onto `dagr-metastore` (and thus `libsql`) is absent from a default build.
//! It is intentionally the CLI's *only* metastore surface in T83: the reader
//! (`sync`, T84) and the live tee (T86) are separate tickets.

use std::ffi::OsString;
use std::path::PathBuf;

use dagr_metastore::store::OpenMode;
use dagr_metastore::MetaStore;

use crate::contract::ExitCode;

/// The default store path when `--store` is not given: a `metastore.db` under the
/// default run-store base (so the index sits alongside the runs it indexes).
#[must_use]
pub fn default_metastore_path() -> PathBuf {
    PathBuf::from(crate::run_store::DEFAULT_STORE_BASE).join("metastore.db")
}

/// Parse the `metastore` sub-argv (everything after the `metastore` token) and run
/// the requested subcommand. Returns the verb's exit code.
///
/// Only `init` is implemented in T83. An unknown or missing subcommand is an
/// invalid-usage error.
#[must_use]
pub fn metastore_verb(sub_argv: &[OsString]) -> ExitCode {
    match sub_argv.first().and_then(|s| s.to_str()) {
        Some("init") => init_verb(&sub_argv[1..]),
        Some(other) => {
            eprintln!(
                "dagr metastore: unknown subcommand `{other}` (the only subcommand is `init`)"
            );
            ExitCode::InvalidUsage
        }
        None => {
            eprintln!("dagr metastore: a subcommand is required (the only subcommand is `init`)");
            ExitCode::InvalidUsage
        }
    }
}

/// `dagr metastore init [--store <path>]`.
fn init_verb(args: &[OsString]) -> ExitCode {
    let path = match parse_store_flag(args) {
        Ok(p) => p.unwrap_or_else(default_metastore_path),
        Err(msg) => {
            eprintln!("dagr metastore init: {msg}");
            return ExitCode::InvalidUsage;
        }
    };

    // Ensure the parent directory exists so a fresh default base works.
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                eprintln!(
                    "dagr metastore init: cannot create store directory `{}`: {e}",
                    parent.display()
                );
                return ExitCode::SinkFailure;
            }
        }
    }

    // Build a small current-thread runtime just for the open+migrate; the verb
    // does no other async work, so it does not need the driver's dual runtimes.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("dagr metastore init: cannot build a runtime: {e}");
            return ExitCode::SinkFailure;
        }
    };

    match rt.block_on(MetaStore::open(OpenMode::LocalFile(path.clone()))) {
        Ok(_store) => {
            // Success: the store is created/opened and migrations are applied.
            // (Idempotent — a second init reaches here too.)
            println!("metastore initialized at {}", path.display());
            ExitCode::Success
        }
        Err(e) => {
            eprintln!("dagr metastore init: {e}");
            ExitCode::SinkFailure
        }
    }
}

/// Parse an optional `--store <path>` from the sub-argv. Returns `Ok(None)` when
/// absent, `Ok(Some(path))` when present, and `Err(msg)` on a malformed flag or an
/// unexpected extra argument.
fn parse_store_flag(args: &[OsString]) -> Result<Option<PathBuf>, String> {
    let mut store: Option<PathBuf> = None;
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.to_str() {
            Some("--store") => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| "`--store` requires a path argument".to_string())?;
                store = Some(PathBuf::from(value));
                i += 2;
            }
            Some(other) if other.starts_with("--store=") => {
                let value = other.trim_start_matches("--store=");
                if value.is_empty() {
                    return Err("`--store=` requires a non-empty path".to_string());
                }
                store = Some(PathBuf::from(value));
                i += 1;
            }
            _ => {
                return Err(format!(
                    "unexpected argument `{}` (usage: metastore init [--store <path>])",
                    arg.to_string_lossy()
                ));
            }
        }
    }
    Ok(store)
}
