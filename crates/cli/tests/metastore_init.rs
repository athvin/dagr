//! T83 acceptance test for the `dagr metastore init` CLI verb.
//!
//! The verb lives behind the **default-off** `metastore` feature, so this whole
//! test file is `#[cfg(feature = "metastore")]` — a bare `cargo test --workspace`
//! (metastore off) compiles it to nothing, and CI's dedicated
//! `--features metastore` step is what exercises it. It drives the real compiled
//! `dagr` binary as a subprocess and asserts the observable contract: `init`
//! creates the store with all five tables and exits 0, and a second `init` is a
//! no-op success.
#![cfg(feature = "metastore")]

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

/// The compiled reference binary (built with `--features metastore` for this test
/// run, so it carries the `metastore` verb).
const DAGR: &str = env!("CARGO_BIN_EXE_dagr");

/// A private per-test store path under the OS temp dir.
fn temp_store_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!(
        "dagr-cli-metastore-init-{tag}-{}-{nanos}-{n}.db",
        std::process::id()
    ))
}

fn run(args: &[&str]) -> Output {
    Command::new(DAGR)
        .args(args)
        .env("DAGR_NO_BANNER", "1")
        .stdin(Stdio::null())
        .output()
        .expect("the dagr binary launches as a separate OS process and is reaped")
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("child exited with a code")
}

/// `dagr metastore init --store <tmp>` creates the DB with all tables and exits 0;
/// a second `init` is a no-op success.
#[test]
fn metastore_init_creates_store_and_is_idempotent() {
    let path = temp_store_path("init");
    let store = path.to_str().expect("utf-8 temp path");

    let first = run(&["metastore", "init", "--store", store]);
    assert_eq!(
        code(&first),
        0,
        "first `metastore init` exits 0.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr),
    );
    assert!(path.exists(), "the store file was created on disk");

    // The file is a real SQLite/libSQL database with the five tables. Open it
    // through the metastore crate and confirm.
    let tables_present = tokio_open_and_count_tables(&path);
    assert_eq!(
        tables_present, 5,
        "all five M7 tables exist after `metastore init`"
    );

    // A second init is a no-op success (idempotent migrations).
    let second = run(&["metastore", "init", "--store", store]);
    assert_eq!(
        code(&second),
        0,
        "a second `metastore init` is a no-op success.\nstderr: {}",
        String::from_utf8_lossy(&second.stderr),
    );

    let _ = std::fs::remove_file(&path);
}

/// Open the store the CLI created and count how many of the five M7 tables exist,
/// using the metastore crate directly (a small current-thread runtime).
fn tokio_open_and_count_tables(path: &std::path::Path) -> usize {
    use dagr_metastore::MetaStore;
    use dagr_metastore::schema::TABLES;
    use dagr_metastore::store::OpenMode;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a current-thread runtime");
    rt.block_on(async {
        let store = MetaStore::open(OpenMode::LocalFile(path.to_path_buf()))
            .await
            .expect("re-open the store the CLI created");
        let conn = store.connection();
        let mut present = 0;
        for table in TABLES {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'"
                    ),
                    (),
                )
                .await
                .expect("query sqlite_master");
            let row = rows.next().await.expect("row").expect("one row");
            if row.get::<i64>(0).expect("i64") == 1 {
                present += 1;
            }
        }
        present
    })
}
