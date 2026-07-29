//! T85 discipline guard — the `DEFERRED`-then-upgrade instant-`SQLITE_BUSY`
//! footgun, pinning the `BEGIN IMMEDIATE` requirement against regression.
//!
//! Written FIRST (TDD). This is the empirical answer to the question ADR 097 §3
//! flagged: a write transaction that opens `DEFERRED` (a read transaction) and
//! then *upgrades* to a write when another connection already holds the writer
//! hits an **instant `SQLITE_BUSY` that `busy_timeout` will NOT retry** — because
//! the upgrade would deadlock, `SQLite` refuses it immediately rather than waiting.
//! `BEGIN IMMEDIATE` (which the T83 [`with_write_txn`] uses for *every* write)
//! avoids this by acquiring the writer up front, so `busy_timeout` + the app-level
//! bounded retry can do their job.
//!
//! The test passes by **reproducing the failure mode** on two raw libSQL
//! connections to one file (the footgun the production path must avoid), then
//! showing the production [`with_write_txn`] path — which begins `IMMEDIATE` —
//! does not exhibit it. A regression that quietly changed the helper to
//! `BEGIN DEFERRED` would flip the second assertion and fail this test.
//!
//! Both connections are in one process on purpose: the footgun is a property of
//! the *transaction mode*, not of the process boundary, and two connections to one
//! file reproduce it deterministically without any cross-process timing. (The
//! *multi-process* correctness claim is proven separately in
//! `multiprocess_writes.rs`.)

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dagr_metastore::MetaStore;
use dagr_metastore::store::OpenMode;

/// A private per-test store path.
fn temp_store_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!(
        "dagr-metastore-t85-footgun-{tag}-{}-{nanos}-{n}.db",
        std::process::id()
    ))
}

/// Whether a libSQL error is a busy/locked failure (`SQLITE_BUSY`=5 / `SQLITE_LOCKED`=6),
/// masking the extended code — the same classification the store seam uses.
fn is_busy(e: &libsql::Error) -> bool {
    matches!(e, libsql::Error::SqliteFailure(code, _) if (code & 0xff) == 5 || (code & 0xff) == 6)
}

/// Open a second **independent** libSQL connection to the same file, with the same
/// WAL + short `busy_timeout` pragmas the store uses — but WITHOUT the store's
/// `BEGIN IMMEDIATE` discipline, so a test can drive a raw `BEGIN DEFERRED`.
async fn raw_conn(path: &std::path::Path, busy_ms: u64) -> libsql::Connection {
    let db = libsql::Builder::new_local(path)
        .build()
        .await
        .expect("open raw connection db");
    let conn = db.connect().expect("connect");
    conn.busy_timeout(Duration::from_millis(busy_ms))
        .expect("busy_timeout");
    // Drain the row-returning WAL pragma via query.
    let mut rows = conn
        .query("PRAGMA journal_mode=WAL", ())
        .await
        .expect("wal");
    let _ = rows.next().await.expect("drain");
    // Keep the Database alive for the connection's lifetime by leaking it: this is
    // a short-lived test process and the OS reclaims everything on exit. (Boxing +
    // leaking avoids threading the Database handle back through the signature.)
    Box::leak(Box::new(db));
    conn
}

/// **The footgun reproduces.** Connection A holds the WAL writer
/// (`BEGIN IMMEDIATE` + an INSERT, not yet committed). Connection B opens
/// `BEGIN DEFERRED` (a read txn), does a read, then attempts a write — the
/// *upgrade*. Asserted: the upgrade fails with an **instant `SQLITE_BUSY`** that a
/// generous `busy_timeout` does not cure (it returns fast, well under the
/// timeout, because `SQLite` refuses the deadlocking upgrade immediately). This is
/// exactly the failure ADR 097 §3 warns about — the reason every production write
/// must use `BEGIN IMMEDIATE`.
#[tokio::test]
async fn deferred_upgrade_hits_instant_busy_that_busy_timeout_cannot_cure() {
    let path = temp_store_path("reproduce");

    // Initialise the schema via the production seam.
    {
        MetaStore::open(OpenMode::LocalFile(path.clone()))
            .await
            .expect("init schema");
    }

    // A: acquire and HOLD the writer (BEGIN IMMEDIATE + insert, no commit yet).
    let a = raw_conn(&path, 250).await;
    a.execute("BEGIN IMMEDIATE", ())
        .await
        .expect("A begins immediate");
    a.execute(
        "INSERT INTO dag (dag_id, name, created_ms) VALUES ('a', 'holder', 0)",
        (),
    )
    .await
    .expect("A holds the writer");

    // B: a DEFERRED (read) transaction that then tries to UPGRADE to a write while
    // A holds the writer. B has a GENEROUS busy_timeout (2s) — if busy_timeout
    // could cure this, B would block up to 2s and eventually error or succeed.
    let b = raw_conn(&path, 2000).await;
    b.execute("BEGIN DEFERRED", ())
        .await
        .expect("B begins deferred");
    // A read first, so the txn is genuinely a read-then-upgrade (the footgun shape).
    let mut rows = b
        .query("SELECT count(*) FROM dag", ())
        .await
        .expect("B reads");
    let _ = rows.next().await.expect("B read row");

    let started = std::time::Instant::now();
    let upgrade = b
        .execute(
            "INSERT INTO dag (dag_id, name, created_ms) VALUES ('b', 'upgrader', 0)",
            (),
        )
        .await;
    let elapsed = started.elapsed();

    // Roll back both to leave the file clean.
    let _ = b.execute("ROLLBACK", ()).await;
    let _ = a.execute("ROLLBACK", ()).await;

    let err = upgrade.expect_err(
        "the DEFERRED→write upgrade must FAIL while another connection holds the writer",
    );
    assert!(
        is_busy(&err),
        "the upgrade fails with SQLITE_BUSY/LOCKED (the deadlock-avoidance refusal), got: {err:?}"
    );
    // The refusal is INSTANT: it returns far under B's 2s busy_timeout, proving
    // busy_timeout cannot cure a DEFERRED upgrade (the whole reason for IMMEDIATE).
    assert!(
        elapsed < Duration::from_millis(1500),
        "the busy is instant — it returned in {elapsed:?}, well under the 2s busy_timeout, \
         so busy_timeout did NOT retry it"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

/// **The production path avoids the footgun.** A holds the writer as above; a
/// second `MetaStore` on the same file writes a DISJOINT row through the real
/// [`with_write_txn`](dagr_metastore::MetaStore::with_write_txn) — which begins
/// `IMMEDIATE` and retries `SQLITE_BUSY` with backoff. Once A commits (released on
/// a background task after a short hold), the second store's write lands. Asserted:
/// the `with_write_txn` write ultimately SUCCEEDS (it did not hit the instant,
/// uncurable DEFERRED-upgrade busy — it waited for the writer and retried). A
/// regression changing the helper to `BEGIN DEFERRED` would reintroduce the
/// instant busy and, under a bounded cap, fail this write.
#[tokio::test]
async fn with_write_txn_begins_immediate_and_does_not_hit_the_footgun() {
    let path = temp_store_path("avoid");

    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("init store");

    // A holds the writer briefly, then commits on a background task, releasing it.
    let a = raw_conn(&path, 250).await;
    a.execute("BEGIN IMMEDIATE", ())
        .await
        .expect("A begins immediate");
    a.execute(
        "INSERT INTO dag (dag_id, name, created_ms) VALUES ('a', 'holder', 0)",
        (),
    )
    .await
    .expect("A holds the writer");

    // Release A after a short hold, on a separate task, so the store's IMMEDIATE
    // write must wait+retry (not fail instantly) and then succeed.
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = a.execute("COMMIT", ()).await;
    });

    // The production write: BEGIN IMMEDIATE + bounded busy retry. It contends with
    // A's held writer, so it MUST retry (not instant-fail), and once A commits it
    // lands. A disjoint row, so no key conflict — any error would be pure contention.
    let write = store
        .with_write_txn(|c| {
            Box::pin(async move {
                c.execute(
                    "INSERT INTO dag (dag_id, name, created_ms) VALUES ('b', 'immediate', 0)",
                    (),
                )
                .await
                .map(|_| ())
            })
        })
        .await;

    releaser.await.expect("releaser task joins");

    write.expect(
        "the BEGIN IMMEDIATE production write waits out the held writer and succeeds — it did \
         NOT hit the instant, uncurable DEFERRED-upgrade busy the footgun test reproduced",
    );

    // Both rows are present: A's holder row and the production immediate write.
    let mut rows = store
        .connection()
        .query("SELECT count(*) FROM dag", ())
        .await
        .expect("count");
    let row = rows.next().await.expect("row").expect("one");
    assert_eq!(
        row.get::<i64>(0).expect("i64"),
        2,
        "both the held-writer row and the IMMEDIATE production write are present"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
