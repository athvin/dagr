//! The `dagr metastore` CLI verbs (M7) — behind the default-off `metastore`
//! feature.
//!
//! - `dagr metastore init [--store <path>]` (T83) creates/opens the libSQL
//!   run-index store at `<path>` (default
//!   [`default_metastore_path`](crate::metastore::default_metastore_path)) and
//!   applies the ordered idempotent migrations, exiting `0` on success. Re-running
//!   is a no-op success (the migrations are `CREATE … IF NOT EXISTS`).
//! - `dagr metastore sync [--store <path>] [--follow] <run-store-base>` (T84)
//!   walks the T0.6 run-store layout (`<base>/<pipeline>/<run-id>/events.jsonl`),
//!   folds each stream via `fold_stream`, and UPSERTs it idempotently through the
//!   store's write discipline. A run with no readable/foldable stream is reported
//!   and skipped, never aborting the batch; the command exits `0` with a summary
//!   count. `--follow` re-runs the pass on an interval, consolidating new/finalized
//!   runs incrementally until interrupted.
//!
//! This module is compiled only when `--features metastore` is set; the whole
//! edge onto `dagr-metastore` (and thus `libsql`) is absent from a default build.
//! The live tee sink that writes during a run (T86) is a separate ticket; `sync`
//! reads finished/observed streams only.

use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use dagr_metastore::mapping::{sync_run_store, SyncSummary};
use dagr_metastore::store::OpenMode;
use dagr_metastore::MetaStore;

use crate::contract::ExitCode;

/// The interval between `--follow` passes. Short enough that a newly-finalized run
/// is picked up promptly, long enough that idle polling is cheap.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(500);

/// The default store path when `--store` is not given: a `metastore.db` under the
/// default run-store base (so the index sits alongside the runs it indexes).
#[must_use]
pub fn default_metastore_path() -> PathBuf {
    PathBuf::from(crate::run_store::DEFAULT_STORE_BASE).join("metastore.db")
}

/// Parse the `metastore` sub-argv (everything after the `metastore` token) and run
/// the requested subcommand. Returns the verb's exit code.
///
/// `init` (T83) and `sync` (T84) are implemented. An unknown or missing subcommand
/// is an invalid-usage error.
#[must_use]
pub fn metastore_verb(sub_argv: &[OsString]) -> ExitCode {
    match sub_argv.first().and_then(|s| s.to_str()) {
        Some("init") => init_verb(&sub_argv[1..]),
        Some("sync") => sync_verb(&sub_argv[1..]),
        Some(other) => {
            eprintln!(
                "dagr metastore: unknown subcommand `{other}` (subcommands are `init`, `sync`)"
            );
            ExitCode::InvalidUsage
        }
        None => {
            eprintln!(
                "dagr metastore: a subcommand is required (subcommands are `init`, `sync`)"
            );
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

/// `dagr metastore sync [--store <path>] [--follow] <run-store-base>`.
///
/// Walks the run store, folds each `events.jsonl`, and UPSERTs it idempotently.
/// A bad run is skipped and reported; the batch still exits `0`. `--follow` loops
/// the pass on an interval until interrupted.
fn sync_verb(args: &[OsString]) -> ExitCode {
    let parsed = match parse_sync_args(args) {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("dagr metastore sync: {msg}");
            return ExitCode::InvalidUsage;
        }
    };
    let store_path = parsed.store.unwrap_or_else(default_metastore_path);
    let base = parsed.base;

    // Ensure the store's parent directory exists so a fresh default base works.
    if let Some(parent) = store_path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                eprintln!(
                    "dagr metastore sync: cannot create store directory `{}`: {e}",
                    parent.display()
                );
                return ExitCode::SinkFailure;
            }
        }
    }

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("dagr metastore sync: cannot build a runtime: {e}");
            return ExitCode::SinkFailure;
        }
    };

    rt.block_on(async {
        let store = match MetaStore::open(OpenMode::LocalFile(store_path.clone())).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("dagr metastore sync: {e}");
                return ExitCode::SinkFailure;
            }
        };

        // The initial pass. A store-write failure is hard (exit non-zero); a
        // per-run fold/read failure is a *skip* counted in the summary, not a
        // failure — so one bad run never aborts the batch.
        let first = match sync_run_store(&store, &base).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("dagr metastore sync: write failure: {e}");
                return ExitCode::SinkFailure;
            }
        };
        report_pass(&first);

        if !parsed.follow {
            return ExitCode::Success;
        }

        // --follow: consolidate incrementally. Each pass re-folds every run
        // (idempotent UPSERT) but only NEW runs add rows, so already-synced runs
        // are not reprocessed into duplicates. Loops until interrupted (Ctrl-C /
        // SIGTERM terminates the process; there is no clean in-loop stop signal in
        // this ticket's scope).
        let mut total = first;
        loop {
            tokio::time::sleep(FOLLOW_INTERVAL).await;
            match sync_run_store(&store, &base).await {
                Ok(pass) => {
                    if pass.newly_synced > 0 {
                        report_new(&pass);
                    }
                    total.add(pass);
                }
                Err(e) => {
                    // A transient store-write failure in follow mode is reported but
                    // does not tear down the consolidator; the next pass retries.
                    eprintln!("dagr metastore sync --follow: write failure (will retry): {e}");
                }
            }
        }
    })
}

/// Print the summary line for a completed pass.
fn report_pass(summary: &SyncSummary) {
    println!(
        "metastore sync: {} run(s) indexed, {} skipped (unreadable stream)",
        summary.synced, summary.skipped
    );
}

/// Print the incremental line for a follow pass that consolidated new runs.
fn report_new(summary: &SyncSummary) {
    println!(
        "metastore sync --follow: {} new run(s) indexed",
        summary.newly_synced
    );
}

/// The parsed `sync` arguments.
struct SyncArgs {
    store: Option<PathBuf>,
    follow: bool,
    base: PathBuf,
}

/// Parse `sync`'s args: an optional `--store <path>`, an optional `--follow`, and
/// exactly one positional `<run-store-base>`.
fn parse_sync_args(args: &[OsString]) -> Result<SyncArgs, String> {
    let mut store: Option<PathBuf> = None;
    let mut follow = false;
    let mut base: Option<PathBuf> = None;
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
            Some("--follow") => {
                follow = true;
                i += 1;
            }
            Some(flag) if flag.starts_with("--") => {
                return Err(format!(
                    "unexpected flag `{flag}` (usage: metastore sync [--store <path>] [--follow] <run-store-base>)"
                ));
            }
            _ => {
                if base.is_some() {
                    return Err(format!(
                        "unexpected extra argument `{}` (exactly one <run-store-base> is expected)",
                        arg.to_string_lossy()
                    ));
                }
                base = Some(PathBuf::from(arg));
                i += 1;
            }
        }
    }
    let base = base.ok_or_else(|| {
        "a <run-store-base> argument is required (usage: metastore sync [--store <path>] [--follow] <run-store-base>)"
            .to_string()
    })?;
    Ok(SyncArgs {
        store,
        follow,
        base,
    })
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
