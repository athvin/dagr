//! T84 acceptance test for the `dagr metastore sync` CLI verb.
//!
//! Like the `init` verb, `sync` lives behind the **default-off** `metastore`
//! feature, so this whole file is `#[cfg(feature = "metastore")]` — a bare
//! `cargo test --workspace` compiles it to nothing, and CI's dedicated
//! `--features metastore` step exercises it. It drives the real compiled `dagr`
//! binary as a subprocess: it writes a run store on disk (via the real driver's
//! run-store path convention), runs `metastore sync <base>`, and asserts the
//! observable contract — the run is indexed, the command exits 0, a bad run is
//! skipped without aborting, and re-syncing is a no-op success.
#![cfg(feature = "metastore")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const DAGR: &str = env!("CARGO_BIN_EXE_dagr");

fn temp_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let path = std::env::temp_dir().join(format!(
        "dagr-cli-metastore-sync-{tag}-{}-{nanos}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("mk temp dir");
    path
}

fn run(args: &[&str]) -> Output {
    Command::new(DAGR)
        .args(args)
        .env("DAGR_NO_BANNER", "1")
        .stdin(Stdio::null())
        .output()
        .expect("dagr launches as a subprocess")
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("child exited with a code")
}

/// Write a minimal but real events.jsonl for a run under
/// `<base>/<pipeline>/<run_id>/events.jsonl` using the writer via the artifact
/// crate. A one-node succeeded run is enough to prove the CLI walk indexes it.
fn write_minimal_run(base: &Path, pipeline: &str, run_id: &str, finished: bool) {
    use dagr_artifact::event_stream::{
        AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
        RunStartedHeader, TerminalState,
    };
    use std::collections::BTreeMap;

    struct FileSink {
        f: std::fs::File,
    }
    impl EventSink for FileSink {
        fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
            use std::io::Write;
            self.f.write_all(line)?;
            self.f.flush()
        }
        fn flush(&mut self) -> std::io::Result<()> {
            use std::io::Write;
            self.f.flush()
        }
    }
    #[derive(Default)]
    struct Tick {
        n: AtomicU64,
    }
    impl MonotonicClock for Tick {
        fn elapsed_ns(&self) -> u64 {
            self.n.fetch_add(1, Ordering::SeqCst)
        }
    }

    let path = base.join(pipeline).join(run_id).join("events.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).expect("mk run dir");
    let file = std::fs::File::create(&path).expect("create events.jsonl");
    let mut w = EventStreamWriter::new(
        FileSink { f: file },
        Tick::default(),
        RunId::from_operator(run_id),
        pipeline,
    )
    .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());

    let header = RunStartedHeader {
        pipeline: pipeline.into(),
        fingerprint_structural: Some("blake3:aaa".into()),
        fingerprint_policy: Some("blake3:bbb".into()),
        fingerprint_algorithm_version: 1,
        parameters: BTreeMap::new(),
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    };
    w.run_started(header).unwrap();
    w.node_ready("a").unwrap();
    w.node_admitted("a").unwrap();
    w.attempt_started("a", 1).unwrap();
    w.attempt_succeeded("a", 1).unwrap();
    w.attempt_outcome(AttemptOutcomeRecord::new("a", 1, "succeeded"))
        .unwrap();
    w.node_terminal("a", TerminalState::Succeeded).unwrap();
    if finished {
        w.run_finished(RunOutcome::Succeeded).unwrap();
        w.finish().unwrap();
    }
}

/// Count how many `dag_run` rows the store the CLI wrote holds.
fn count_dag_runs(db: &Path) -> i64 {
    use dagr_metastore::MetaStore;
    use dagr_metastore::store::OpenMode;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(async {
        let store = MetaStore::open(OpenMode::LocalFile(db.to_path_buf()))
            .await
            .expect("open the store the CLI wrote");
        let mut rows = store
            .connection()
            .query("SELECT count(*) FROM dag_run", ())
            .await
            .expect("query");
        let row = rows.next().await.expect("row").expect("one row");
        row.get::<i64>(0).expect("i64")
    })
}

/// `dagr metastore sync <base>` indexes every run under the base, exits 0, skips a
/// bad run without aborting, and re-syncing is a no-op success.
#[test]
fn metastore_sync_indexes_runs_and_skips_bad_ones() {
    let dir = temp_dir("sync");
    let base = dir.join("runs");
    let db = dir.join("index.db");
    write_minimal_run(&base, "pipe", "run-1", true);
    write_minimal_run(&base, "pipe", "run-2", true);
    // A bad run directory with an unfoldable stream (no run-started).
    let bad = base.join("pipe").join("bad");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("events.jsonl"), b"garbage not json\n").unwrap();

    let base_s = base.to_str().unwrap();
    let db_s = db.to_str().unwrap();

    let out = run(&["metastore", "sync", "--store", db_s, base_s]);
    assert_eq!(
        code(&out),
        0,
        "sync exits 0 even with a bad run.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    assert_eq!(count_dag_runs(&db), 2, "the two good runs are indexed");

    // Re-sync: still exits 0, still exactly two runs (idempotent UPSERTs).
    let again = run(&["metastore", "sync", "--store", db_s, base_s]);
    assert_eq!(code(&again), 0, "re-sync is a no-op success");
    assert_eq!(count_dag_runs(&db), 2, "no duplicate runs after re-sync");

    let _ = std::fs::remove_dir_all(&dir);
}
