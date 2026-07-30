//! `OpenError` and `WriteError` carry their real cause. Written first, TDD.
//!
//! Both types wrap a genuine `libsql::Error` and both left `impl Error for X {}`
//! empty, so `source()` returned `None` and the substrate's own diagnostic — the
//! only thing that says *why* a store could not be opened or a transaction could
//! not commit — was invisible to any caller walking the chain.
//!
//! Each test opens a private per-test path so the suite is collision-proof under
//! CI parallelism, matching the rest of this crate's tests.

use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_metastore::MetaStore;
use dagr_metastore::store::{OpenError, OpenMode, WriteError};

/// A private per-test store path under the OS temp dir. Collision-proof across
/// tests, processes, and re-runs.
fn temp_store_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!(
        "dagr-metastore-t95-{tag}-{}-{nanos}-{n}.db",
        std::process::id()
    ))
}

/// **`OpenError::Libsql` exposes the substrate error.** Opening a path that cannot
/// be a database file (a directory) fails inside libSQL; the wrapper must hand
/// that error back through `source()`.
#[tokio::test]
async fn open_error_exposes_the_libsql_error_through_source() {
    // A directory can never be opened as a database file.
    let dir = temp_store_path("open-dir");
    std::fs::create_dir_all(&dir).expect("temp dir creates");

    // `MetaStore` is not `Debug`, so the refutable-let form stands in for
    // `expect_err` here.
    let Err(err) = MetaStore::open(OpenMode::LocalFile(dir)).await else {
        panic!("opening a directory as a database file must fail");
    };

    let OpenError::Libsql(_) = &err else {
        panic!("expected a libsql-backed open failure, got {err}");
    };
    let source = err
        .source()
        .expect("OpenError::Libsql wraps a real libsql::Error; source() must expose it");
    assert!(
        source.downcast_ref::<libsql::Error>().is_some(),
        "the cause must be the libsql::Error itself: {source}"
    );
    assert!(
        err.to_string().contains(&source.to_string()),
        "the Display form quotes the cause it now also exposes structurally"
    );
}

/// **A variant with no cause keeps `source() == None`.** The recognized-stub
/// modes are refused from data, not from an underlying error; the fix must not
/// fabricate a link.
#[tokio::test]
async fn open_error_with_no_cause_has_no_source() {
    let Err(err) = MetaStore::open(OpenMode::RemoteSqld {
        url: "http://example.invalid".to_string(),
        auth_token: String::new(),
    })
    .await
    else {
        panic!("a recognized stub mode is refused");
    };

    assert!(
        err.source().is_none(),
        "ModeNotImplemented wraps no error; a fabricated source would be a lie"
    );
}

/// **`WriteError::Libsql` exposes the substrate error.** A write transaction whose
/// closure hands back a real `libsql::Error` (invalid SQL) must expose it through
/// `source()` — a caller that logs only the chain would otherwise see nothing but
/// "write transaction failed".
#[tokio::test]
async fn write_error_exposes_the_libsql_error_through_source() {
    let path = temp_store_path("write");
    let store = MetaStore::open(OpenMode::LocalFile(path))
        .await
        .expect("a fresh local file opens");

    let err = store
        .with_write_txn(|conn| {
            Box::pin(async move {
                conn.execute("THIS IS NOT SQL", ()).await?;
                Ok(())
            })
        })
        .await
        .expect_err("invalid SQL fails the transaction");

    let WriteError::Libsql(_) = &err else {
        panic!("expected a libsql-backed write failure, got {err}");
    };
    let source = err
        .source()
        .expect("WriteError::Libsql wraps a real libsql::Error; source() must expose it");
    assert!(
        source.downcast_ref::<libsql::Error>().is_some(),
        "the cause must be the libsql::Error itself: {source}"
    );
}

/// **The retry-exhausted variant has no wrapped cause.** It is a bounded-budget
/// refusal built from a count, so `source()` stays `None`.
#[test]
fn write_error_with_no_cause_has_no_source() {
    let err = WriteError::BusyRetriesExhausted { attempts: 8 };
    assert!(
        err.source().is_none(),
        "BusyRetriesExhausted carries a count, not an error"
    );
}
