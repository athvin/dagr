//! T83 acceptance tests for the `dagr-metastore` schema + connection seam.
//!
//! These are the ticket's Test-plan scenarios (schema/migrations, the CHECK-enum
//! acceptance/rejection over the canonical states, and the `with_write_txn`
//! `BEGIN IMMEDIATE` + bounded-retry shape), written FIRST and failing before the
//! implementation lands. Each test opens a private per-test temp file so the suite
//! is collision-proof under CI parallelism.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dagr_metastore::MetaStore;
use dagr_metastore::schema::{NODE_TERMINAL_STATES, RUN_STATES, TABLES, migrations};
use dagr_metastore::store::{OpenMode, RetryPolicy, WriteError};

/// A private per-test store path under the OS temp dir: `<tmp>/dagr-metastore-t83
/// -<pid>-<nanos>-<counter>.db`. Collision-proof across tests, processes, and
/// re-runs.
fn temp_store_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!(
        "dagr-metastore-t83-{tag}-{}-{nanos}-{n}.db",
        std::process::id()
    ))
}

/// Count rows a `SELECT count(*)` query returns as an `i64`.
async fn scalar_i64(conn: &libsql::Connection, sql: &str) -> i64 {
    let mut rows = conn.query(sql, ()).await.expect("query runs");
    let row = rows.next().await.expect("row read").expect("one row");
    row.get::<i64>(0).expect("i64 column")
}

// === Schema + migrations ===================================================

/// Scenario 1: a fresh open creates all five tables, the file is a valid
/// SQLite/libSQL database, and `journal_mode` reports `wal`.
#[tokio::test]
async fn fresh_open_creates_all_tables_and_enables_wal() {
    let path = temp_store_path("fresh");
    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("open a fresh store");
    let conn = store.connection();

    for table in TABLES {
        let present = scalar_i64(
            conn,
            &format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'"),
        )
        .await;
        assert_eq!(present, 1, "table `{table}` must exist after open");
    }

    // journal_mode reports wal (WAL was set on open, ADR 097 §3).
    let mut rows = conn
        .query("PRAGMA journal_mode", ())
        .await
        .expect("pragma query");
    let row = rows.next().await.expect("row").expect("a mode row");
    let mode = row.get_str(0).expect("text").to_ascii_lowercase();
    assert_eq!(mode, "wal", "journal_mode must be WAL");

    // The file exists on disk (a real local-file store).
    assert!(path.exists(), "the store file is created on disk");
    let _ = std::fs::remove_file(&path);
}

/// Scenario 2: re-opening an initialized store is idempotent — no error, no
/// duplicate objects, and existing rows survive.
#[tokio::test]
async fn reopen_is_idempotent_and_preserves_rows() {
    let path = temp_store_path("idem");

    // First open + insert a dag row.
    {
        let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
            .await
            .expect("first open");
        store
            .with_write_txn(|c| {
                Box::pin(async move {
                    c.execute(
                        "INSERT INTO dag (dag_id, name, created_ms) VALUES ('d1', 'demo', 1)",
                        (),
                    )
                    .await
                    .map(|_| ())
                })
            })
            .await
            .expect("insert a dag row");
    }

    // Second open: migrations re-run without error and the row survives.
    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("second open is a no-op success");
    let conn = store.connection();

    // No duplicate tables (each of the five appears exactly once in sqlite_master).
    for table in TABLES {
        let count = scalar_i64(
            conn,
            &format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'"),
        )
        .await;
        assert_eq!(
            count, 1,
            "table `{table}` must not be duplicated on re-open"
        );
    }

    let surviving = scalar_i64(conn, "SELECT count(*) FROM dag WHERE dag_id='d1'").await;
    assert_eq!(surviving, 1, "the row inserted before re-open survives");

    let _ = std::fs::remove_file(&path);
}

/// Scenario 3a: a `dag_run` insert with a `state` outside the allowed set is
/// rejected by the CHECK constraint.
#[tokio::test]
async fn dag_run_check_rejects_unknown_state() {
    let path = temp_store_path("run-reject");
    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("open");

    let result = store
        .with_write_txn(|c| {
            Box::pin(async move {
                c.execute(
                    "INSERT INTO dag_run (run_id, dag_id, state, started_ms) \
                     VALUES ('r1', 'd1', 'not-a-real-state', 1)",
                    (),
                )
                .await
                .map(|_| ())
            })
        })
        .await;

    assert!(
        result.is_err(),
        "the CHECK constraint must reject a dag_run state outside the run-level set"
    );

    let _ = std::fs::remove_file(&path);
}

/// Scenario 3b: every one of the nine canonical node terminal states is accepted
/// by the `node_attempt.state` CHECK.
#[tokio::test]
async fn node_attempt_check_accepts_all_nine_canonical_states() {
    let path = temp_store_path("node-accept");
    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("open");

    for (i, state) in NODE_TERMINAL_STATES.iter().enumerate() {
        let state = (*state).to_string();
        let label = state.clone();
        let try_number = i64::try_from(i).expect("small index fits i64");
        store
            .with_write_txn(move |c| {
                let state = state.clone();
                Box::pin(async move {
                    c.execute(
                        &format!(
                            "INSERT INTO node_attempt (run_id, node_id, try_number, state) \
                             VALUES ('r1', 'n1', {try_number}, '{state}')"
                        ),
                        (),
                    )
                    .await
                    .map(|_| ())
                })
            })
            .await
            .unwrap_or_else(|e| panic!("canonical node state `{label}` must be accepted: {e}"));
    }

    let conn = store.connection();
    let count = scalar_i64(conn, "SELECT count(*) FROM node_attempt").await;
    assert_eq!(
        count,
        i64::try_from(NODE_TERMINAL_STATES.len()).expect("nine fits i64"),
        "all nine canonical node states were inserted"
    );

    // And an unknown node state is rejected.
    let bad = store
        .with_write_txn(|c| {
            Box::pin(async move {
                c.execute(
                    "INSERT INTO node_attempt (run_id, node_id, try_number, state) \
                     VALUES ('r1', 'n2', 0, 'bogus')",
                    (),
                )
                .await
                .map(|_| ())
            })
        })
        .await;
    assert!(bad.is_err(), "an unknown node_attempt state is rejected");

    let _ = std::fs::remove_file(&path);
}

/// The migration set names all five tables and the run/node state lists are the
/// expected sizes (a cheap guard the schema constants do not drift).
#[test]
fn migration_set_and_state_lists_are_well_formed() {
    let m = migrations();
    assert!(!m.is_empty(), "there are migrations");
    for table in TABLES {
        assert!(
            m.iter()
                .any(|s| s.contains(&format!("TABLE IF NOT EXISTS {table} "))
                    || m.iter()
                        .any(|s| s.contains(&format!("TABLE IF NOT EXISTS {table}(")))),
            "a migration creates table `{table}`"
        );
    }
    assert_eq!(
        NODE_TERMINAL_STATES.len(),
        9,
        "dagr has nine terminal states"
    );
    assert_eq!(RUN_STATES.len(), 6, "there are six run-level states");
}

// === Write discipline ======================================================

/// Scenario 4a (helper shape): two writers contending on the same file both
/// commit — the loser retries under `BEGIN IMMEDIATE` + `busy_timeout` and no
/// write is lost. (Deep multi-process coverage is T85; this asserts the helper's
/// single-file shape.)
#[tokio::test]
async fn two_contending_writers_both_commit_no_lost_write() {
    let path = temp_store_path("contend");
    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("open");
    store
        .with_write_txn(|c| {
            Box::pin(async move {
                c.execute(
                    "INSERT INTO dag (dag_id, name, created_ms) VALUES ('d', 'x', 0)",
                    (),
                )
                .await
                .map(|_| ())
            })
        })
        .await
        .expect("seed row");

    // Two write transactions issued back-to-back against the same connection;
    // each increments created_ms. Both must land (no lost update).
    let w1 = store.with_write_txn(|c| {
        Box::pin(async move {
            c.execute(
                "UPDATE dag SET created_ms = created_ms + 10 WHERE dag_id='d'",
                (),
            )
            .await
            .map(|_| ())
        })
    });
    let w2 = store.with_write_txn(|c| {
        Box::pin(async move {
            c.execute(
                "UPDATE dag SET created_ms = created_ms + 100 WHERE dag_id='d'",
                (),
            )
            .await
            .map(|_| ())
        })
    });
    let (r1, r2) = tokio::join!(w1, w2);
    r1.expect("first writer commits");
    r2.expect("second writer commits");

    let total = scalar_i64(
        store.connection(),
        "SELECT created_ms FROM dag WHERE dag_id='d'",
    )
    .await;
    assert_eq!(total, 110, "both writes committed — neither was lost");

    let _ = std::fs::remove_file(&path);
}

/// Scenario 4b: when the closure keeps returning a busy error past the retry cap,
/// a hard `WriteError::BusyRetriesExhausted` is returned — never a silent drop.
#[tokio::test]
async fn write_txn_returns_hard_error_past_retry_cap() {
    let path = temp_store_path("cap");
    // A tiny cap (2 attempts) with negligible backoff so the test is fast.
    let store = MetaStore::open_with_retry(
        OpenMode::LocalFile(path.clone()),
        RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        },
    )
    .await
    .expect("open");

    // A closure that always fails with a synthesized SQLITE_BUSY (code 5), so the
    // retry loop must exhaust its cap and surface a hard error.
    let result = store
        .with_write_txn(|_c| {
            Box::pin(async move {
                Err(libsql::Error::SqliteFailure(
                    5,
                    "database is locked".to_string(),
                ))
            })
        })
        .await;

    match result {
        Err(WriteError::BusyRetriesExhausted { attempts }) => {
            assert_eq!(attempts, 2, "the bounded cap (2 attempts) is surfaced");
        }
        other => panic!("expected a hard BusyRetriesExhausted, got {other:?}"),
    }

    let _ = std::fs::remove_file(&path);
}

/// A non-busy error from the closure is surfaced immediately as
/// `WriteError::Libsql` (not retried, not swallowed).
#[tokio::test]
async fn write_txn_surfaces_non_busy_error_immediately() {
    let path = temp_store_path("nonbusy");
    let store = MetaStore::open(OpenMode::LocalFile(path.clone()))
        .await
        .expect("open");

    let result = store
        .with_write_txn(|c| {
            Box::pin(async move {
                // A constraint violation (unknown dag_run state) is not a busy error.
                c.execute(
                    "INSERT INTO dag_run (run_id, dag_id, state, started_ms) \
                     VALUES ('r', 'd', 'bogus', 0)",
                    (),
                )
                .await
                .map(|_| ())
            })
        })
        .await;

    assert!(
        matches!(result, Err(WriteError::Libsql(_))),
        "a non-busy error is surfaced as WriteError::Libsql immediately"
    );

    let _ = std::fs::remove_file(&path);
}

// === Recognized stubs ======================================================

/// The `RemoteSqld` / `SyncedReplica` modes are recognized stubs (ADR 097 §5,
/// ticket-conventions §7): `open` returns a clear "not implemented" error rather
/// than pretending to connect.
#[tokio::test]
async fn recognized_stub_modes_return_not_implemented() {
    let remote = MetaStore::open(OpenMode::RemoteSqld {
        url: "libsql://example".to_string(),
        auth_token: "t".to_string(),
    })
    .await;
    assert!(
        remote.is_err(),
        "RemoteSqld is a recognized stub, not implemented"
    );

    let replica = MetaStore::open(OpenMode::SyncedReplica {
        path: temp_store_path("replica"),
        url: "libsql://example".to_string(),
        auth_token: "t".to_string(),
    })
    .await;
    assert!(
        replica.is_err(),
        "SyncedReplica is a recognized stub, not implemented"
    );
}
