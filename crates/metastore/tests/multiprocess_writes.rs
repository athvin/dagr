//! T85 — **multi-process** write validation for the metastore.
//!
//! Written FIRST (TDD), failing before the `write_worker` binary and any T83-seam
//! hardening land. These tests exercise the **already-merged** T83 connection seam
//! ([`MetaStore::open`] / [`with_write_txn`]: WAL + `busy_timeout` +
//! `BEGIN IMMEDIATE` + bounded `SQLITE_BUSY` retry) across a **real process
//! boundary** — the claim ADR 097 bets the live-write design on is that *many OS
//! processes* can write one embedded libSQL file, so the harness spawns genuine
//! processes (`std::process::Command` over the crate's `write_worker` binary),
//! never just threads.
//!
//! ## Why this mirrors T67, not a thread test
//!
//! T67 proved run-store isolation across *concurrent* runs; but its `drive()`
//! calls were threads in one process (a single address space). The metastore
//! claim is specifically about **cross-process file access** — POSIX byte-range
//! locks, the WAL's single-writer arbitration, and `busy_timeout`'s reliability in
//! the libSQL binding are all *process*-level properties a thread test cannot
//! reach. So every writer here is a separate OS process.
//!
//! ## Genuine, deterministic overlap (no race flake)
//!
//! Overlap is forced by an **observable file rendezvous**, never a sleep: each
//! worker writes a readiness marker and spins until the harness fires a single
//! `go` file, so every worker enters its write window together. The assertions are
//! on *row presence and counts* (order-independent: rows are keyed disjointly), so
//! there is no interleaving assumption to flake — the classic T35 cancellation-
//! ordering race is structurally impossible here.
//!
//! ## Each store path is private
//!
//! Every test uses a private per-test store path under the OS temp dir (pid +
//! nanos + counter), so the suite is collision-proof under CI parallelism and the
//! documented shared-`/tmp` flake class cannot bite.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dagr_metastore::store::OpenMode;
use dagr_metastore::MetaStore;

/// The compiled `write_worker` binary (built for this crate's test run), the real
/// OS process each writer spawns.
const WORKER: &str = env!("CARGO_BIN_EXE_write_worker");

/// A private per-test store path: `<tmp>/dagr-metastore-t85-<tag>-<pid>-<nanos>-<n>.db`.
/// Collision-proof across tests, processes, and re-runs.
fn temp_store_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!(
        "dagr-metastore-t85-{tag}-{}-{nanos}-{n}.db",
        std::process::id()
    ))
}

/// The barrier base path for a store (marker files are `<barrier>.ready.<prefix>`
/// and `<barrier>.go`, created beside the store).
fn barrier_path_for(store: &Path) -> PathBuf {
    let mut p = store.as_os_str().to_os_string();
    p.push(".barrier");
    PathBuf::from(p)
}

/// Spawn one `write_worker` OS process. `extra` carries optional flags
/// (`--max-attempts`, `--hold-ms`).
fn spawn_worker(
    store: &Path,
    prefix: &str,
    runs: u64,
    attempts: u64,
    barrier: Option<&Path>,
    extra: &[&str],
) -> Child {
    let mut cmd = Command::new(WORKER);
    cmd.arg("--store")
        .arg(store)
        .arg("--run-prefix")
        .arg(prefix)
        .arg("--runs")
        .arg(runs.to_string())
        .arg("--attempts")
        .arg(attempts.to_string());
    if let Some(b) = barrier {
        cmd.arg("--barrier").arg(b);
    }
    for e in extra {
        cmd.arg(e);
    }
    cmd.spawn()
        .expect("write_worker launches as a real OS process")
}

/// The readiness-marker path a worker with `prefix` writes beside `barrier`.
fn ready_marker(barrier: &Path, prefix: &str) -> PathBuf {
    let mut p = barrier.as_os_str().to_os_string();
    p.push(format!(".ready.{prefix}"));
    PathBuf::from(p)
}

/// Fire the single `<barrier>.go` file that releases every waiting worker at once.
fn fire_go(barrier: &Path) {
    let mut go = barrier.as_os_str().to_os_string();
    go.push(".go");
    std::fs::write(PathBuf::from(go), b"go").expect("write the go marker");
}

/// Block until every worker with one of `prefixes` has written its readiness
/// marker, then fire the go signal to release them all at once — the observable
/// rendezvous that forces a genuinely overlapping write window across processes.
fn release_when_all_ready(barrier: &Path, prefixes: &[String]) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let all_ready = prefixes
            .iter()
            .all(|prefix| ready_marker(barrier, prefix).exists());
        if all_ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "workers never all reached the barrier (a worker failed to start)"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    fire_go(barrier);
}

/// Block until the single worker `prefix` has written its readiness marker, WITHOUT
/// firing the go signal (the caller fires it later, after arranging contention).
fn release_when_all_ready_wait_only(barrier: &Path, prefix: &str) {
    let deadline = Instant::now() + Duration::from_mins(1);
    while !ready_marker(barrier, prefix).exists() {
        assert!(
            Instant::now() < deadline,
            "worker `{prefix}` never reached the barrier (it failed to start)"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Wait for a child and return its exit code (panicking if it was killed by a
/// signal instead of exiting).
fn wait_code(mut child: Child) -> i32 {
    let status = child.wait().expect("worker process is reaped");
    status
        .code()
        .expect("worker exited with a code (not killed by a signal)")
}

/// Clean up a store path and its WAL/SHM sidecars plus barrier markers, so the
/// temp dir does not accumulate across a CI run.
fn cleanup(store: &Path) {
    let _ = std::fs::remove_file(store);
    for suffix in ["-wal", "-shm"] {
        let mut p = store.as_os_str().to_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(p));
    }
    // Best-effort sweep of the sidecar rendezvous markers (barrier ready/go files
    // and the holder-acquired marker), so the temp dir does not accumulate.
    if let Some(dir) = store.parent() {
        if let Some(stem) = store.file_name().and_then(|s| s.to_str()) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with(stem)
                            && (name.contains(".barrier")
                                || name.contains(".holder-acquired")
                                || name.contains(".holder-release"))
                        {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }
    }
}

/// A tiny current-thread runtime for the harness's own read-side integrity
/// checks (opening the store the workers wrote and counting rows).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a current-thread runtime")
        .block_on(fut)
}

/// Count the `i64` a scalar `SELECT count(*)` (or similar) returns.
async fn scalar_i64(conn: &libsql::Connection, sql: &str) -> i64 {
    let mut rows = conn.query(sql, ()).await.expect("query runs");
    let row = rows.next().await.expect("row read").expect("one row");
    row.get::<i64>(0).expect("i64 column")
}

/// Open the store the workers wrote and assert its structural integrity: a
/// `PRAGMA integrity_check` reports `ok` and the WAL journal mode survived. Then
/// return the total `dag_run` and `node_attempt` counts.
async fn integrity_and_counts(store: &Path) -> (i64, i64) {
    let meta = MetaStore::open(OpenMode::LocalFile(store.to_path_buf()))
        .await
        .expect("re-open the store the workers wrote");
    let conn = meta.connection();

    // The DB opens and passes SQLite's own integrity check — no corruption from
    // the concurrent multi-process writes.
    let mut rows = conn
        .query("PRAGMA integrity_check", ())
        .await
        .expect("integrity_check runs");
    let row = rows.next().await.expect("a row").expect("one result row");
    let verdict = row.get_str(0).expect("text verdict").to_string();
    assert_eq!(
        verdict, "ok",
        "the store passes SQLite's integrity check after the concurrent multi-process writes"
    );

    // WAL mode survived the concurrent open/writes.
    let mut jm = conn
        .query("PRAGMA journal_mode", ())
        .await
        .expect("journal_mode query");
    let jrow = jm.next().await.expect("row").expect("a mode row");
    let mode = jrow.get_str(0).expect("text").to_ascii_lowercase();
    assert_eq!(mode, "wal", "journal_mode is still WAL");

    let runs = scalar_i64(conn, "SELECT count(*) FROM dag_run").await;
    let attempts = scalar_i64(conn, "SELECT count(*) FROM node_attempt").await;
    (runs, attempts)
}

// ===========================================================================
// (a) N processes, disjoint rows, zero lost writes, exactly-once, integrity
// ===========================================================================

/// **Headline multi-process correctness.** N real OS processes, each writing M
/// disjoint runs (each with A attempts) to ONE `LocalFile` store, all released
/// together at a barrier. Asserted: every worker exits 0; the store passes an
/// integrity check afterward and is still WAL; and the row counts are *exactly*
/// N×M `dag_run` rows and N×M×A `node_attempt` rows — no lost write, no
/// duplicate, exactly-once. Because every worker's `(run_id, node_id, try)` key
/// space is disjoint by construction, the count IS the exactly-once proof.
#[test]
fn n_processes_writing_disjoint_rows_lose_nothing() {
    let store = temp_store_path("disjoint");
    let barrier = barrier_path_for(&store);

    let n: u64 = 5;
    let runs_per: u64 = 4;
    let attempts_per: u64 = 3;

    let prefixes: Vec<String> = (0..n).map(|w| format!("w{w}")).collect();
    let children: Vec<Child> = prefixes
        .iter()
        .map(|prefix| spawn_worker(&store, prefix, runs_per, attempts_per, Some(&barrier), &[]))
        .collect();

    // Release all workers at once — genuine overlapping write windows.
    release_when_all_ready(&barrier, &prefixes);

    for (child, prefix) in children.into_iter().zip(&prefixes) {
        let code = wait_code(child);
        assert_eq!(
            code, 0,
            "worker `{prefix}` writing disjoint rows exits 0 (every write landed)"
        );
    }

    let (dag_runs, node_attempts) = block_on(integrity_and_counts(&store));
    assert_eq!(
        dag_runs,
        i64::try_from(n * runs_per).unwrap(),
        "exactly N×M dag_run rows — zero lost writes, no duplicate"
    );
    assert_eq!(
        node_attempts,
        i64::try_from(n * runs_per * attempts_per).unwrap(),
        "exactly N×M×A node_attempt rows — every attempt present exactly once"
    );

    // Exactly-once at the key level too: no (run_id,node_id,try) key appears twice
    // (the UNIQUE constraint would have rejected a dup, but assert the distinct
    // count equals the total to prove exactly-once directly).
    let distinct_keys = block_on(async {
        let meta = MetaStore::open(OpenMode::LocalFile(store.clone()))
            .await
            .expect("re-open");
        scalar_i64(
            meta.connection(),
            "SELECT count(*) FROM (SELECT DISTINCT run_id, node_id, try_number FROM node_attempt)",
        )
        .await
    });
    assert_eq!(
        distinct_keys, node_attempts,
        "every node_attempt key is distinct — exactly-once, no phantom duplicate"
    );

    cleanup(&store);
}

// ===========================================================================
// (b) Heavy overlap: SQLITE_BUSY is retried within bound; every write lands
// ===========================================================================

/// **Heavy overlap still loses nothing.** More processes than the first test,
/// each holding its write lock briefly inside the transaction (`--hold-ms`) so
/// the WAL's single-writer arbitration is genuinely contended and `SQLITE_BUSY`
/// is *forced* — but the rows stay disjoint, so the T83 bounded retry must absorb
/// every busy and land every write. Asserted: all workers exit 0 (no spurious
/// hard error for disjoint rows — the busy was retried within the cap) and the
/// full N×M / N×M×A counts are present with a passing integrity check.
#[test]
fn heavy_overlap_retries_busy_and_lands_every_write() {
    let store = temp_store_path("overlap");
    let barrier = barrier_path_for(&store);

    let n: u64 = 6;
    let runs_per: u64 = 3;
    let attempts_per: u64 = 4;

    let prefixes: Vec<String> = (0..n).map(|w| format!("h{w}")).collect();
    // Each worker holds the write lock 3ms inside every txn — long enough that
    // simultaneous writers collide on the single WAL writer and hit SQLITE_BUSY,
    // which the T83 busy_timeout + bounded retry must transparently absorb.
    let children: Vec<Child> = prefixes
        .iter()
        .map(|prefix| {
            spawn_worker(
                &store,
                prefix,
                runs_per,
                attempts_per,
                Some(&barrier),
                &["--hold-ms", "3"],
            )
        })
        .collect();

    release_when_all_ready(&barrier, &prefixes);

    for (child, prefix) in children.into_iter().zip(&prefixes) {
        let code = wait_code(child);
        assert_eq!(
            code, 0,
            "worker `{prefix}` under heavy overlap exits 0 — SQLITE_BUSY was retried \
             within bound, never surfaced as a spurious hard error for disjoint rows"
        );
    }

    let (dag_runs, node_attempts) = block_on(integrity_and_counts(&store));
    assert_eq!(
        dag_runs,
        i64::try_from(n * runs_per).unwrap(),
        "every dag_run landed despite forced contention (no lost update, no partial run)"
    );
    assert_eq!(
        node_attempts,
        i64::try_from(n * runs_per * attempts_per).unwrap(),
        "every node_attempt landed under heavy overlap"
    );

    cleanup(&store);
}

// ===========================================================================
// (d) Over-contention past the retry cap: a hard, visible error (no silent drop)
// ===========================================================================

/// **A wedge is observable, never a silent drop.** One process opens a write and
/// holds the WAL writer lock for a long time; a second process, pinned to a tiny
/// retry cap and negligible backoff, tries to write and cannot outlast the
/// wedger. Asserted: the wedged writer exits with the dedicated
/// `EXIT_BUSY_EXHAUSTED` code (3) — the bounded retry surfaced a *hard*
/// `WriteError::BusyRetriesExhausted`, not a silent drop — while the holder exits
/// 0 and its write is present. This is the retry-cap escape hatch proven under a
/// real cross-process wedge.
#[test]
fn over_contention_past_the_cap_surfaces_a_hard_visible_error() {
    let store = temp_store_path("wedge");
    let barrier = barrier_path_for(&store);

    // First, initialise the schema so neither worker's OPEN is what races — the
    // wedge we want to prove is a *write* contention past the retry cap, not an
    // open contention (open is hardened to be robust; that is the separate (a)/(b)
    // property).
    block_on(async {
        MetaStore::open(OpenMode::LocalFile(store.clone()))
            .await
            .expect("pre-create the schema");
    });

    // The wedger opens FIRST (uncontended — schema present, holder not yet started →
    // its open + migration no-op cannot race) and then waits at the barrier before
    // its data write. A tiny retry cap (2 attempts) + negligible backoff, so once
    // the holder owns the writer the wedger cannot outlast it and MUST exhaust its
    // budget on the *write* (its open already completed uncontended). It writes a
    // disjoint run, so any failure is pure contention, not a key conflict.
    let wedger = spawn_worker(
        &store,
        "wedger",
        1,
        0,
        Some(&barrier),
        &["--max-attempts", "2"],
    );

    // Wait for the wedger to open and reach the barrier (so its open is safely done
    // BEFORE the holder grabs the writer).
    release_when_all_ready_wait_only(&barrier, "wedger");

    // Now the holder grabs and holds the single WAL writer (one run, held inside
    // the txn) monopolising it while the wedger tries to write. No barrier — it
    // starts immediately, and its open is a no-op over the existing schema. It
    // writes an `--acquired-marker` the instant its BEGIN IMMEDIATE txn owns the
    // writer (its INSERT landed, before the hold), so we release the wedger on a
    // deterministic *observable* signal — not on a `-wal`-exists guess or a sleep.
    //
    // The hold length is itself a **deterministic rendezvous**, not a fixed sleep:
    // the holder holds `--hold-until <release>` and blocks (inside its txn) until
    // the harness creates that marker, which it does only AFTER the wedger has
    // provably exhausted its cap and exited. A prior fixed `--hold-ms 2000` raced
    // the wedger's post-release scheduling latency: on a starved macOS CI runner
    // the wedger could fail to get scheduled to run its two ~250ms BEGIN IMMEDIATE
    // attempts until after the holder's 2s already elapsed and released the writer,
    // so the wedger slipped its write in and exited 0 (the observed macOS flake).
    // Holding until the wedger is *observably* done removes that race entirely
    // while the assertions below are byte-for-byte unchanged.
    let acquired = holder_acquired_marker(&store);
    let release = holder_release_marker(&store);
    let _ = std::fs::remove_file(&acquired);
    let _ = std::fs::remove_file(&release);
    let acquired_arg = acquired
        .to_str()
        .expect("the acquired-marker temp path is valid UTF-8");
    let release_arg = release
        .to_str()
        .expect("the release-marker temp path is valid UTF-8");
    let holder = spawn_worker(
        &store,
        "holder",
        1,
        0,
        None,
        &[
            "--hold-until",
            release_arg,
            "--acquired-marker",
            acquired_arg,
        ],
    );
    wait_until_holder_holds_the_writer(&acquired);

    // Release the wedger into its data write against the held writer.
    fire_go(&barrier);

    // The wedger runs against the still-held writer and MUST exhaust its bounded
    // retry cap — the holder does not release until we say so, so no scheduling
    // latency can let the wedger outlast the hold.
    let wedger_code = wait_code(wedger);
    // The wedger has provably exhausted (it exited); release the holder now.
    std::fs::write(&release, b"release").expect("write the holder release marker");
    assert_eq!(
        wedger_code, 3,
        "the over-contended writer exhausts its bounded retry cap and exits with the \
         dedicated BUSY-exhausted code (a hard, VISIBLE error — not a silent drop)"
    );

    // The holder still commits its own write successfully.
    let holder_code = wait_code(holder);
    assert_eq!(holder_code, 0, "the lock holder commits its write");

    // The holder's row is present; the wedger's is absent (it never committed) —
    // the failure was surfaced, and no phantom partial row was left behind.
    let (holder_present, wedger_present) = block_on(async {
        let meta = MetaStore::open(OpenMode::LocalFile(store.clone()))
            .await
            .expect("re-open");
        let h = scalar_i64(
            meta.connection(),
            "SELECT count(*) FROM dag_run WHERE run_id='holder-run-0'",
        )
        .await;
        let w = scalar_i64(
            meta.connection(),
            "SELECT count(*) FROM dag_run WHERE run_id='wedger-run-0'",
        )
        .await;
        (h, w)
    });
    assert_eq!(holder_present, 1, "the holder's committed write is present");
    assert_eq!(
        wedger_present, 0,
        "the wedged writer left no phantom row — its hard error means no partial write"
    );

    cleanup(&store);
}

/// The observable marker the holder creates the instant its `BEGIN IMMEDIATE`
/// transaction owns the WAL writer (its INSERT landed, before the hold). The
/// harness releases the wedger only after this appears.
fn holder_acquired_marker(store: &Path) -> PathBuf {
    let mut p = store.as_os_str().to_os_string();
    p.push(".holder-acquired");
    PathBuf::from(p)
}

/// The observable marker the harness creates to *release* the holder's
/// `--hold-until` hold — created only AFTER the wedger has provably exhausted its
/// retry cap and exited, so the WAL writer is held for exactly the wedge window
/// (deterministic, not a fixed sleep the wedger's scheduling latency could race).
fn holder_release_marker(store: &Path) -> PathBuf {
    let mut p = store.as_os_str().to_os_string();
    p.push(".holder-release");
    PathBuf::from(p)
}

/// Block until the holder *provably* holds the WAL writer, signalled by its
/// `--acquired-marker` file (written inside its transaction, after `BEGIN
/// IMMEDIATE` + the INSERT, before the hold). This is a **deterministic**
/// rendezvous, not a timing guess: it does not fire the wedger until the holder
/// genuinely owns the writer, so the wedge holds regardless of macOS's slower
/// process-spawn timing (the prior `-wal`-exists heuristic could fire before the
/// writer lock was taken — the file exists from the pre-created schema — letting
/// a slowly-spawned holder lose the race and the wedger slip its write in). The
/// holder then holds the writer (via `--hold-until`) until the harness releases
/// it *after* the wedger has exhausted and exited, so the exhaustion is guaranteed
/// no matter how long the wedger takes to get scheduled.
fn wait_until_holder_holds_the_writer(acquired: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !acquired.exists() {
        assert!(
            Instant::now() < deadline,
            "the holder never signalled it acquired the WAL writer (its --acquired-marker \
             never appeared — the holder process failed to start or to take the writer)"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}
