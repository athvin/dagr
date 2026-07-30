//! `write_worker` — a **real OS-process** metastore writer, spawned by the T85
//! multi-process harness (`crates/metastore/tests/multiprocess_writes.rs`).
//!
//! The T85 claim is that **many OS processes** can write to one embedded libSQL
//! file safely when their rows are disjoint (ADR 097: WAL single-writer +
//! `busy_timeout` + `BEGIN IMMEDIATE` + bounded `SQLITE_BUSY` retry). Proving that
//! needs genuine *processes*, not threads sharing one address space — so the
//! harness spawns N copies of this binary via `std::process::Command`, each
//! opening the **same** `LocalFile` store through the production
//! [`MetaStore`](dagr_metastore::MetaStore) seam and writing its own disjoint
//! rows through [`with_write_txn`](dagr_metastore::MetaStore::with_write_txn) —
//! the exact production write discipline, exercised across a process boundary.
//!
//! This is a **test-support binary only**. Nothing depends on it; it exists so a
//! test in this crate can spawn it. `dagr-metastore` is a normal workspace member
//! whose tests run under `cargo test --workspace` on both `ubuntu-latest` and
//! `macos-latest`, so the harness — and this worker — run on both platforms with
//! no extra CI wiring (POSIX byte-range locks differ across the two, which is the
//! point).
//!
//! # Contract (argv, all `--flag value`)
//!
//! - `--store <path>` — the shared `LocalFile` store every worker opens.
//! - `--run-prefix <s>` — the per-worker namespace for every `run_id` this worker
//!   writes; distinct across workers, so **no two workers ever touch the same
//!   row** (disjoint by construction).
//! - `--runs <N>` — how many `dag_run` rows this worker writes.
//! - `--attempts <M>` — how many `node_attempt` rows per run.
//! - `--barrier <path>` — a rendezvous file: the worker creates
//!   `<barrier>.ready.<prefix>`, then spins until `<barrier>.go` exists, so all
//!   workers begin their write window together (genuine, observable overlap — no
//!   sleep, no assumption of order). Optional; absent means "start immediately".
//! - `--max-attempts <k>` — optional override of the write-txn retry cap (used by
//!   the over-contention test to force a wedge under a tiny cap).
//! - `--hold-ms <ms>` — optional: hold each `BEGIN IMMEDIATE` write lock this long
//!   inside the txn (used by the wedger to create pathological contention the
//!   retry cap cannot outlast).
//! - `--hold-until <path>` — optional: instead of holding each write lock for a
//!   *fixed* duration, hold it (inside the txn, after the INSERT) until this
//!   observable release-marker file appears, then commit. This is the deterministic
//!   contention rendezvous for the over-contention test: the harness holds the WAL
//!   writer until the contending writer has *provably* exhausted its retry cap and
//!   exited, then creates the marker to release the holder — so the wedge lasts
//!   exactly as long as it must, never a fixed sleep racing the contender's
//!   (unbounded, on a starved CI runner) scheduling latency. A generous bounded
//!   safety cap still applies so a harness bug cannot hang a CI job forever.
//! - `--acquired-marker <path>` — optional: the instant this worker's first write
//!   transaction has acquired the WAL writer (its INSERT succeeded, before the
//!   `--hold-ms`/`--hold-until` hold begins), create this observable marker file.
//!   The over-contention harness waits on it to release the contending writer only
//!   once the holder *provably* owns the writer, so the wedge is deterministic
//!   regardless of macOS's slower process-spawn timing (no `-wal`-exists guess,
//!   no sleep).
//!
//! # Exit code
//!
//! `0` on success (every intended write committed). A non-zero exit with a
//! `write_worker:` diagnostic on stderr on any failure — in particular exit code
//! `3` specifically means a write hit the **bounded-retry cap** and surfaced a
//! hard [`WriteError::BusyRetriesExhausted`](dagr_metastore::WriteError), so the
//! over-contention test can assert "a wedged writer is observable, not silently
//! dropped".

use std::path::PathBuf;
use std::time::Duration;

use dagr_metastore::MetaStore;
use dagr_metastore::store::{OpenMode, RetryPolicy, WriteError};

/// Exit code meaning "a write exhausted the bounded `SQLITE_BUSY` retry cap and
/// surfaced a hard error" — a *visible wedge*, distinct from any other failure.
const EXIT_BUSY_EXHAUSTED: u8 = 3;
/// Exit code for a usage / non-busy error.
const EXIT_ERROR: u8 = 1;

/// One parsed worker invocation.
struct Args {
    store: PathBuf,
    run_prefix: String,
    runs: u64,
    attempts: u64,
    barrier: Option<PathBuf>,
    max_attempts: Option<u32>,
    hold: Duration,
    hold_until: Option<PathBuf>,
    acquired_marker: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("write_worker: {msg}");
            return std::process::ExitCode::from(EXIT_ERROR);
        }
    };

    // A private current-thread runtime: the worker is a single process doing a
    // bounded amount of async I/O, so it needs no multi-thread scheduler and no
    // `#[tokio::main]` macro (the crate's non-dev tokio features are `rt`/`time`/
    // `sync` only, deliberately no `macros`).
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("write_worker: cannot build a runtime: {e}");
            return std::process::ExitCode::from(EXIT_ERROR);
        }
    };

    rt.block_on(run(args))
}

/// Open the shared store, rendezvous at the barrier if one was given, then write
/// this worker's disjoint rows through the production write discipline.
async fn run(args: Args) -> std::process::ExitCode {
    // Open through the SAME production seam the driver/sync path uses: WAL +
    // synchronous=NORMAL + busy_timeout on open, and BEGIN IMMEDIATE + bounded
    // retry on every `with_write_txn`. `open` also (idempotently) applies the
    // migrations, so a worker racing to be first to create the schema is fine.
    let retry = args
        .max_attempts
        .map_or_else(RetryPolicy::default, |max_attempts| RetryPolicy {
            max_attempts,
            ..RetryPolicy::default()
        });
    let store =
        match MetaStore::open_with_retry(OpenMode::LocalFile(args.store.clone()), retry).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("write_worker[{}]: open failed: {e}", args.run_prefix);
                return std::process::ExitCode::from(EXIT_ERROR);
            }
        };

    // Rendezvous: announce readiness, then spin until the harness fires the go
    // signal, so every worker enters its write window at (near) the same instant.
    // Observable-signal synchronisation, not a sleep — no ordering is assumed.
    if let Some(barrier) = &args.barrier {
        if let Err(e) = rendezvous(barrier, &args.run_prefix) {
            eprintln!("write_worker[{}]: barrier failed: {e}", args.run_prefix);
            return std::process::ExitCode::from(EXIT_ERROR);
        }
    }

    for run_idx in 0..args.runs {
        // The run_id is namespaced by this worker's prefix, so the whole
        // (run_id, node_id, try_number) key space this worker writes is disjoint
        // from every other worker's — zero row overlap by construction.
        let run_id = format!("{}-run-{run_idx}", args.run_prefix);
        // The acquired-marker fires exactly once, on the FIRST run's dag_run write,
        // the instant this worker holds the writer (see `write_one_run`).
        // The acquired-marker and the `--hold-until` release-marker both apply to
        // the FIRST run's dag_run write only — the single write the holder makes
        // in the over-contention test — so the holder owns the writer across
        // exactly the window the harness observes and releases.
        let (acquired, hold_until) = if run_idx == 0 {
            (args.acquired_marker.as_deref(), args.hold_until.as_deref())
        } else {
            (None, None)
        };
        if let Err(code) = write_one_run(&store, &args, &run_id, acquired, hold_until).await {
            return code;
        }
    }

    std::process::ExitCode::SUCCESS
}

/// Write one full run: a `dag_run` row plus `attempts` `node_attempt` rows, each
/// in its own `BEGIN IMMEDIATE` transaction (so contention is per-row, maximising
/// the overlap the harness is testing). On a busy-cap exhaustion, exit with the
/// dedicated `EXIT_BUSY_EXHAUSTED` code so the wedge is observable.
async fn write_one_run(
    store: &MetaStore,
    args: &Args,
    run_id: &str,
    acquired_marker: Option<&std::path::Path>,
    hold_until: Option<&std::path::Path>,
) -> Result<(), std::process::ExitCode> {
    // The dag_run row (state='running', started at offset 0 — the mapping's
    // convention). One transaction.
    let rid = run_id.to_string();
    let hold = args.hold;
    let marker = acquired_marker.map(std::path::Path::to_path_buf);
    let release = hold_until.map(std::path::Path::to_path_buf);
    let res = store
        .with_write_txn(move |c| {
            let rid = rid.clone();
            let marker = marker.clone();
            let release = release.clone();
            Box::pin(async move {
                c.execute(
                    &format!(
                        "INSERT INTO dag_run (run_id, dag_id, state, started_ms) \
                         VALUES ('{rid}', 'multiproc-dag', 'running', 0)"
                    ),
                    (),
                )
                .await?;
                // The writer is now provably held (BEGIN IMMEDIATE succeeded and this
                // INSERT committed to the txn). Announce it BEFORE the hold so the
                // harness releases the contending writer only once this txn owns the
                // WAL writer — a deterministic rendezvous, not a `-wal`-exists guess.
                announce_acquired(marker.as_deref());
                // Hold the writer: either until an observable release marker appears
                // (`--hold-until`, the deterministic over-contention rendezvous) or
                // for a fixed duration (`--hold-ms`). `hold-until` wins when both are
                // given.
                if let Some(release) = release.as_deref() {
                    hold_until_marker(release).await;
                } else {
                    hold_lock(hold).await;
                }
                Ok(())
            })
        })
        .await;
    classify(res, &args.run_prefix)?;

    // The node_attempt rows, one transaction each.
    for try_number in 0..args.attempts {
        let rid = run_id.to_string();
        let hold = args.hold;
        let res = store
            .with_write_txn(move |c| {
                let rid = rid.clone();
                Box::pin(async move {
                    c.execute(
                        &format!(
                            "INSERT INTO node_attempt (run_id, node_id, try_number, state) \
                             VALUES ('{rid}', 'n0', {try_number}, 'succeeded')"
                        ),
                        (),
                    )
                    .await?;
                    hold_lock(hold).await;
                    Ok(())
                })
            })
            .await;
        classify(res, &args.run_prefix)?;
    }
    Ok(())
}

/// Create the `--acquired-marker` file (best-effort) the instant the writer is
/// held, so the over-contention harness can release the contending writer on an
/// observable signal rather than guessing from the `-wal` sidecar (which exists
/// well before the writer lock is taken). `None` (or a marker already present) is
/// a no-op. A write error here does not fail the worker — the marker is a test
/// rendezvous, not part of the write correctness.
fn announce_acquired(marker: Option<&std::path::Path>) {
    if let Some(path) = marker {
        let _ = std::fs::write(path, b"acquired");
    }
}

/// Optionally hold the write lock open for `hold` inside the transaction, to
/// manufacture pathological contention (the wedger). A zero duration is a no-op.
async fn hold_lock(hold: Duration) {
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }
}

/// Hold the write lock open (inside the transaction, before commit) until the
/// observable `release` marker appears — the **deterministic** contention hold the
/// over-contention harness uses. The harness creates the marker only after the
/// contending writer has provably exhausted its retry cap and exited, so the WAL
/// writer stays held for exactly the wedge window regardless of how long the
/// contender took to get scheduled on a starved CI runner (no fixed sleep racing
/// an unbounded scheduling latency). A generous bounded cap (`HOLD_UNTIL_CAP`)
/// backstops a harness bug so a CI job cannot hang forever; hitting it is a
/// harness fault, not a normal path.
async fn hold_until_marker(release: &std::path::Path) {
    let deadline = tokio::time::Instant::now() + HOLD_UNTIL_CAP;
    while !release.exists() {
        if tokio::time::Instant::now() >= deadline {
            eprintln!(
                "write_worker: hold-until cap reached before the release marker `{}` \
                 appeared (harness fault — releasing the writer)",
                release.display()
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The bounded safety cap on a `--hold-until` hold: generous enough that it never
/// bounds a legitimate wedge (the contender exhausts in well under a second), but
/// finite so a harness bug that never fires the release marker cannot wedge a CI
/// job indefinitely.
const HOLD_UNTIL_CAP: Duration = Duration::from_mins(1);

/// Map a `with_write_txn` result to a worker exit decision: `Ok` continues; a
/// busy-cap exhaustion is the *visible* wedge (`EXIT_BUSY_EXHAUSTED`); any other
/// error is a generic failure.
fn classify(res: Result<(), WriteError>, prefix: &str) -> Result<(), std::process::ExitCode> {
    match res {
        Ok(()) => Ok(()),
        Err(WriteError::BusyRetriesExhausted { attempts }) => {
            eprintln!(
                "write_worker[{prefix}]: SQLITE_BUSY retry cap exhausted after {attempts} attempts \
                 (visible wedge, not a silent drop)"
            );
            Err(std::process::ExitCode::from(EXIT_BUSY_EXHAUSTED))
        }
        Err(WriteError::Libsql(e)) => {
            eprintln!("write_worker[{prefix}]: write failed (non-busy): {e}");
            Err(std::process::ExitCode::from(EXIT_ERROR))
        }
    }
}

/// Announce readiness by creating `<barrier>.ready.<prefix>`, then spin (with a
/// tiny yield) until the harness creates `<barrier>.go`. No sleep-based timing
/// assumption: the wait is on an observable file, exactly like T67's barrier is on
/// an observable rendezvous.
fn rendezvous(barrier: &std::path::Path, prefix: &str) -> std::io::Result<()> {
    let ready = ready_marker(barrier, prefix);
    std::fs::write(&ready, b"ready")?;
    let go = go_marker(barrier);
    // Bounded spin: if the go signal never arrives (harness bug), give up rather
    // than hang a CI job forever.
    for _ in 0..600_000u64 {
        if go.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "go signal never arrived",
    ))
}

/// The readiness marker path a worker with `prefix` creates.
fn ready_marker(barrier: &std::path::Path, prefix: &str) -> PathBuf {
    let mut p = barrier.as_os_str().to_os_string();
    p.push(format!(".ready.{prefix}"));
    PathBuf::from(p)
}

/// The go-signal path the harness creates to release all workers.
fn go_marker(barrier: &std::path::Path) -> PathBuf {
    let mut p = barrier.as_os_str().to_os_string();
    p.push(".go");
    PathBuf::from(p)
}

/// Parse the `--flag value` argv into [`Args`]. Every flag except `--barrier`,
/// `--max-attempts`, and `--hold-ms` is required.
fn parse_args() -> Result<Args, String> {
    let mut store: Option<PathBuf> = None;
    let mut run_prefix: Option<String> = None;
    let mut runs: Option<u64> = None;
    let mut attempts: Option<u64> = None;
    let mut barrier: Option<PathBuf> = None;
    let mut max_attempts: Option<u32> = None;
    let mut hold_ms: u64 = 0;
    let mut hold_until: Option<PathBuf> = None;
    let mut acquired_marker: Option<PathBuf> = None;

    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--store" => store = Some(PathBuf::from(it_next(&mut it, &flag)?)),
            "--run-prefix" => run_prefix = Some(it_next(&mut it, &flag)?),
            "--runs" => runs = Some(parse_u64(&it_next(&mut it, &flag)?, "--runs")?),
            "--attempts" => attempts = Some(parse_u64(&it_next(&mut it, &flag)?, "--attempts")?),
            "--barrier" => barrier = Some(PathBuf::from(it_next(&mut it, &flag)?)),
            "--max-attempts" => {
                max_attempts = Some(
                    u32::try_from(parse_u64(&it_next(&mut it, &flag)?, "--max-attempts")?)
                        .map_err(|_| "--max-attempts too large".to_string())?,
                );
            }
            "--hold-ms" => hold_ms = parse_u64(&it_next(&mut it, &flag)?, "--hold-ms")?,
            "--hold-until" => hold_until = Some(PathBuf::from(it_next(&mut it, &flag)?)),
            "--acquired-marker" => acquired_marker = Some(PathBuf::from(it_next(&mut it, &flag)?)),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }

    Ok(Args {
        store: store.ok_or("missing --store")?,
        run_prefix: run_prefix.ok_or("missing --run-prefix")?,
        runs: runs.ok_or("missing --runs")?,
        attempts: attempts.ok_or("missing --attempts")?,
        barrier,
        max_attempts,
        hold: Duration::from_millis(hold_ms),
        hold_until,
        acquired_marker,
    })
}

/// Pull the next argv token as a flag's value, erroring if it is absent.
fn it_next(it: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    it.next()
        .ok_or_else(|| format!("flag `{flag}` needs a value"))
}

/// Parse a `u64` flag value with a flag-named error.
fn parse_u64(s: &str, flag: &str) -> Result<u64, String> {
    s.parse::<u64>()
        .map_err(|_| format!("flag `{flag}` needs a non-negative integer, got `{s}`"))
}
