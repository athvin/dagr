//! T86 acceptance tests: the guaranteed live metastore tee sink.
//!
//! These are the ticket's Test-plan scenarios (live rows observable mid-run,
//! guaranteed-not-best-effort on a write failure, and live == reconcile parity),
//! written FIRST and failing before the `MetastoreSink` lands. They drive the
//! **real** event-stream writer ([`dagr_artifact::event_stream::EventStreamWriter`])
//! through a `MetastoreSink`, so the sink is proven against exactly the canonical
//! bytes a run emits, and the live rows are checked against the `RunArtifact` that
//! `fold_stream` folds from the same bytes and against a post-hoc `sync`.
//!
//! Each test uses a private per-test temp dir so the suite is collision-proof
//! under CI parallelism (ubuntu + macOS).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState,
};
use dagr_artifact::fold::fold_stream;

use dagr_metastore::mapping::sync_run;
use dagr_metastore::store::OpenMode;
use dagr_metastore::{MetaStore, MetastoreSink};

// === Test scaffolding ======================================================

/// A private per-test temp directory that is created fresh and removed on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "dagr-metastore-t86-{tag}-{}-{nanos}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A deterministic per-read counter clock, so the fixture bytes are reproducible.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// An in-memory capturing sink (clone-shared), so a test can read back the exact
/// bytes a run wrote to feed a post-hoc `sync` fold.
#[derive(Clone, Default)]
struct CaptureSink {
    bytes: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().unwrap().clone()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A two-sink tee (like the cli's tee) so a test can drive one writer that writes
/// both to the capture and the metastore, and prove the metastore path does not
/// perturb the captured bytes.
struct Tee<A: EventSink, B: EventSink> {
    a: A,
    b: B,
}

impl<A: EventSink, B: EventSink> EventSink for Tee<A, B> {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.a.append_line(line)?;
        self.b.append_line(line)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.a.flush()?;
        self.b.flush()
    }
}

fn header(pipeline: &str) -> RunStartedHeader {
    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-27".to_string());
    RunStartedHeader {
        pipeline: pipeline.to_string(),
        fingerprint_structural: Some("blake3:aaa".to_string()),
        fingerprint_policy: Some("blake3:bbb".to_string()),
        fingerprint_algorithm_version: 1,
        parameters: params,
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    }
}

fn store_path(dir: &Path) -> PathBuf {
    dir.join("metastore.db")
}

/// Open a read handle to the store for assertions.
async fn open_store(dir: &Path) -> MetaStore {
    MetaStore::open(OpenMode::LocalFile(store_path(dir)))
        .await
        .expect("open the metastore for reads")
}

fn scalar_i64(store: &MetaStore, sql: &str) -> i64 {
    RT.block_on(async {
        let mut rows = store.connection().query(sql, ()).await.expect("query");
        let row = rows.next().await.expect("row").expect("one row");
        row.get::<i64>(0).expect("i64")
    })
}

fn opt_string(store: &MetaStore, sql: &str) -> Option<String> {
    RT.block_on(async {
        let mut rows = store.connection().query(sql, ()).await.expect("query");
        let row = rows.next().await.expect("row")?;
        row.get::<String>(0).ok()
    })
}

// A shared current-thread runtime for the read-side assertions (the sink owns its
// own writer thread; these blocks are only reads against the finished file). A
// current-thread runtime matches the crate's minimal tokio feature set (`rt`).
static RT: once_lock::Lazy<tokio::runtime::Runtime> = once_lock::Lazy::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a read runtime")
});

// A minimal `Lazy` (no external dep beyond what the crate's dev-deps carry).
mod once_lock {
    use std::sync::OnceLock;
    pub struct Lazy<T> {
        cell: OnceLock<T>,
        init: fn() -> T,
    }
    impl<T> Lazy<T> {
        pub const fn new(init: fn() -> T) -> Self {
            Self {
                cell: OnceLock::new(),
                init,
            }
        }
    }
    impl<T> std::ops::Deref for Lazy<T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.cell.get_or_init(self.init)
        }
    }
}

// === Live rows observable mid-run ==========================================

/// With the sink live, `dag_run(state='running')` appears at run-started, a
/// `node_attempt` row appears as an attempt finishes (observable BEFORE the run
/// ends), and `dag_run` finalizes to its terminal state at run-finished.
#[test]
fn live_rows_are_observable_mid_run_running_then_attempts_then_terminal() {
    let dir = TempDir::new("live");
    let sink = MetastoreSink::open(store_path(dir.path()), Some("events.jsonl".to_string()))
        .expect("open the live sink");
    let mut writer = EventStreamWriter::new(
        sink,
        TickClock::default(),
        RunId::from_operator("run-live"),
        "pipe",
    )
    .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());

    // run-started ⇒ dag_run(running), no attempts yet, observable NOW.
    writer
        .run_started(header("pipe"))
        .expect("run-started projects");
    {
        let store = RT.block_on(open_store(dir.path()));
        let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-live'");
        assert_eq!(
            state.as_deref(),
            Some("running"),
            "dag_run is 'running' at run-started, mid-run"
        );
        let attempts = scalar_i64(
            &store,
            "SELECT count(*) FROM node_attempt WHERE run_id='run-live'",
        );
        assert_eq!(attempts, 0, "no attempts recorded yet at run-started");
    }

    // node a completes ⇒ a node_attempt row appears while the run is still going.
    writer.node_ready("a").unwrap();
    writer.node_admitted("a").unwrap();
    writer.attempt_started("a", 1).unwrap();
    writer.attempt_succeeded("a", 1).unwrap();
    writer
        .attempt_outcome(AttemptOutcomeRecord::new("a", 1, "succeeded"))
        .unwrap();
    writer.node_terminal("a", TerminalState::Succeeded).unwrap();
    {
        let store = RT.block_on(open_store(dir.path()));
        let a_attempts = scalar_i64(
            &store,
            "SELECT count(*) FROM node_attempt WHERE run_id='run-live' AND node_id='a'",
        );
        assert_eq!(a_attempts, 1, "node a's attempt row is visible mid-run");
        let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-live'");
        assert_eq!(
            state.as_deref(),
            Some("running"),
            "dag_run is STILL 'running' while node b has not run"
        );
    }

    // run-finished ⇒ dag_run finalizes to the terminal outcome.
    writer.run_finished(RunOutcome::Succeeded).unwrap();
    writer.finish().unwrap();
    drop(writer);
    {
        let store = RT.block_on(open_store(dir.path()));
        let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-live'");
        assert_eq!(
            state.as_deref(),
            Some("succeeded"),
            "dag_run finalizes to 'succeeded' at run-finished"
        );
        let interrupted = scalar_i64(
            &store,
            "SELECT interrupted FROM dag_run WHERE run_id='run-live'",
        );
        assert_eq!(interrupted, 0, "a finished run is not interrupted");
    }
}

/// The tee does not perturb the event stream: a run driven through
/// `Tee<Capture, MetastoreSink>` produces byte-identical `events.jsonl` bytes to a
/// run driven through the capture alone.
#[test]
fn the_tee_does_not_perturb_the_captured_event_bytes() {
    // Toggle-OFF reference: capture only.
    let off_capture = CaptureSink::default();
    {
        let mut writer = EventStreamWriter::new(
            off_capture.clone(),
            TickClock::default(),
            RunId::from_operator("run-x"),
            "pipe",
        )
        .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
        emit_two_node_run(&mut writer);
    }
    let off_bytes = off_capture.bytes();

    // Toggle-ON: tee the capture with the live metastore sink.
    let dir = TempDir::new("byteid");
    let on_capture = CaptureSink::default();
    {
        let metastore = MetastoreSink::open(store_path(dir.path()), None).expect("open live sink");
        let tee = Tee {
            a: on_capture.clone(),
            b: metastore,
        };
        let mut writer = EventStreamWriter::new(
            tee,
            TickClock::default(),
            RunId::from_operator("run-x"),
            "pipe",
        )
        .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
        emit_two_node_run(&mut writer);
    }
    let on_bytes = on_capture.bytes();

    assert_eq!(
        off_bytes, on_bytes,
        "the tee's metastore path must not change a single byte of the event stream"
    );
    assert!(!off_bytes.is_empty(), "the reference run wrote a stream");
}

/// Live-produced rows equal reconcile(`sync`)-produced rows for the same run: drive
/// the run once live, drive the identical run once to a capture then `sync` that
/// finished stream into a second store, and assert the row sets are identical.
#[test]
fn live_rows_equal_reconcile_rows_for_the_same_run() {
    // --- Live: drive through the sink.
    let live_dir = TempDir::new("live-parity");
    let capture = CaptureSink::default();
    {
        let metastore = MetastoreSink::open(
            store_path(live_dir.path()),
            Some("events.jsonl".to_string()),
        )
        .expect("open live sink");
        let tee = Tee {
            a: capture.clone(),
            b: metastore,
        };
        let mut writer = EventStreamWriter::new(
            tee,
            TickClock::default(),
            RunId::from_operator("run-p"),
            "pipe",
        )
        .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
        emit_two_node_run(&mut writer);
    }
    let stream = capture.bytes();

    // --- Reconcile: `sync` the identical finished stream into a fresh store.
    let recon_dir = TempDir::new("recon-parity");
    RT.block_on(async {
        let store = MetaStore::open(OpenMode::LocalFile(store_path(recon_dir.path())))
            .await
            .expect("open recon store");
        let artifact = fold_stream(&stream, &[]).expect("fold the finished stream");
        sync_run(&store, &artifact, Some("events.jsonl"))
            .await
            .expect("sync the finished stream");
    });

    // --- Compare the two stores' rows for the run.
    let live = RT.block_on(open_store(live_dir.path()));
    let recon = RT.block_on(open_store(recon_dir.path()));

    let dag_run_live = dump_row(
        &live,
        "SELECT state, interrupted, finished_ms, events_path FROM dag_run WHERE run_id='run-p'",
    );
    let dag_run_recon = dump_row(
        &recon,
        "SELECT state, interrupted, finished_ms, events_path FROM dag_run WHERE run_id='run-p'",
    );
    assert_eq!(
        dag_run_live, dag_run_recon,
        "the live dag_run row equals the reconcile dag_run row"
    );

    let attempts_live = dump_rows(
        &live,
        "SELECT node_id, try_number, state, metrics_json FROM node_attempt WHERE run_id='run-p' ORDER BY node_id, try_number",
    );
    let attempts_recon = dump_rows(
        &recon,
        "SELECT node_id, try_number, state, metrics_json FROM node_attempt WHERE run_id='run-p' ORDER BY node_id, try_number",
    );
    assert_eq!(
        attempts_live, attempts_recon,
        "the live node_attempt rows equal the reconcile node_attempt rows"
    );

    let terminals_live = dump_rows(
        &live,
        "SELECT node_id, state FROM node_terminal WHERE run_id='run-p' ORDER BY node_id",
    );
    let terminals_recon = dump_rows(
        &recon,
        "SELECT node_id, state FROM node_terminal WHERE run_id='run-p' ORDER BY node_id",
    );
    assert_eq!(
        terminals_live, terminals_recon,
        "the live node_terminal rows equal the reconcile node_terminal rows"
    );
}

// === Guaranteed, not best-effort ===========================================

/// An injected metastore write failure surfaces as an error from the sink (which
/// the writer folds into a `SinkFault`) and is NOT swallowed. We force a failure by
/// pointing the sink at an unwritable store location, so the very first projected
/// event fails a durable write.
#[test]
fn an_injected_write_failure_surfaces_and_is_not_swallowed() {
    let dir = TempDir::new("fault");
    // Make the store PATH be a directory (not a file): libsql cannot open a
    // directory as a database file, so the sink's open fails loudly.
    let bad = dir.path().join("as-a-dir");
    std::fs::create_dir_all(&bad).expect("make a directory where the db file should be");

    let opened = MetastoreSink::open(bad, None);
    assert!(
        opened.is_err(),
        "opening the live sink against an unusable store path fails loudly (guaranteed, not swallowed)"
    );
}

/// A genuine mid-run durable-write failure surfaces and STICKS: with the sink's
/// store pinned to a one-attempt retry cap and a competing writer holding the
/// single-writer WAL lock (`BEGIN IMMEDIATE`), the sink's projection deterministically
/// hits `SQLITE_BUSY` past the cap — a hard write failure. That failure surfaces as
/// an error (which the writer folds into a `SinkFault`), is NOT swallowed, and once
/// it happens the sink stays faulted: `flush` keeps failing so the driver's bounded
/// final flush reliably reports the sink failure.
#[test]
fn a_mid_run_write_failure_surfaces_and_sticks() {
    use dagr_metastore::store::RetryPolicy;

    let dir = TempDir::new("sticky");
    let path = store_path(dir.path());

    // Pin the sink's store to a ONE-attempt cap: no retry absorbs the contention,
    // so a held write lock deterministically produces a hard busy failure.
    let one_shot = RetryPolicy {
        max_attempts: 1,
        base_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(1),
    };
    let mut sink = MetastoreSink::open_with_retry(path.clone(), None, one_shot)
        .expect("open the live sink against the store");

    // A competing writer on a SECOND connection to the same file, holding the WAL's
    // single write lock via BEGIN IMMEDIATE for the duration of the sink's write.
    let contender = RT.block_on(open_store(dir.path()));
    RT.block_on(async {
        contender
            .connection()
            .execute("BEGIN IMMEDIATE", ())
            .await
            .expect("the contender takes the write lock");
    });

    // Build a canonical run-started line and project it: the sink's write txn hits
    // the held write lock, gets SQLITE_BUSY, exhausts its one-attempt cap, and fails.
    let capture = CaptureSink::default();
    {
        let mut w2 = EventStreamWriter::new(
            capture.clone(),
            TickClock::default(),
            RunId::from_operator("run-s"),
            "pipe",
        )
        .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
        w2.run_started(header("pipe")).unwrap();
    }
    let bytes = capture.bytes();
    let first = bytes
        .split_inclusive(|&b| b == b'\n')
        .next()
        .expect("run-started line");

    let write_res = sink.append_line(first);
    assert!(
        write_res.is_err(),
        "a contended durable write surfaces a hard fault (guaranteed, not swallowed)"
    );
    // Sticky: the fault re-surfaces from flush, so the bounded final flush reports it.
    assert!(
        sink.flush().is_err(),
        "once faulted, the sink stays faulted: flush keeps failing"
    );

    // Release the contender's lock so the temp dir cleans up.
    RT.block_on(async {
        let _ = contender.connection().execute("ROLLBACK", ()).await;
    });
}

// === Fixtures + helpers ====================================================

/// Emit a deterministic two-node run: `a` succeeds; `b` fails once then succeeds
/// (a retry), with metrics — enough to exercise attempts, terminal, and finalize.
fn emit_two_node_run<S: EventSink, C: MonotonicClock>(w: &mut EventStreamWriter<S, C>) {
    w.run_started(header("pipe")).unwrap();
    w.node_ready("a").unwrap();
    w.node_admitted("a").unwrap();
    w.attempt_started("a", 1).unwrap();
    w.attempt_succeeded("a", 1).unwrap();
    w.attempt_outcome(AttemptOutcomeRecord {
        node: "a".into(),
        attempt: 1,
        status: "succeeded".into(),
        worker: Some("compute#1".into()),
        metrics: Some(serde_json::json!({ "rows": 10 })),
        ..AttemptOutcomeRecord::default()
    })
    .unwrap();
    w.node_terminal("a", TerminalState::Succeeded).unwrap();
    w.node_ready("b").unwrap();
    w.node_admitted("b").unwrap();
    w.attempt_started("b", 1).unwrap();
    w.attempt_failed("b", 1).unwrap();
    w.attempt_outcome(AttemptOutcomeRecord {
        node: "b".into(),
        attempt: 1,
        status: "failed".into(),
        worker: Some("compute#2".into()),
        message: Some("transient".into()),
        ..AttemptOutcomeRecord::default()
    })
    .unwrap();
    w.attempt_started("b", 2).unwrap();
    w.attempt_succeeded("b", 2).unwrap();
    w.attempt_outcome(AttemptOutcomeRecord {
        node: "b".into(),
        attempt: 2,
        status: "succeeded".into(),
        worker: Some("compute#2".into()),
        metrics: Some(serde_json::json!({ "rows": 20 })),
        ..AttemptOutcomeRecord::default()
    })
    .unwrap();
    w.node_terminal("b", TerminalState::Succeeded).unwrap();
    w.run_finished(RunOutcome::Succeeded).unwrap();
    w.finish().unwrap();
}

/// Dump one row as a `|`-joined string of its column values (NULL → "∅").
fn dump_row(store: &MetaStore, sql: &str) -> String {
    RT.block_on(async {
        let mut rows = store.connection().query(sql, ()).await.expect("query");
        let row = rows.next().await.expect("row").expect("one row");
        row_to_string(&row)
    })
}

/// Dump every row as a newline-joined list of `|`-joined column values.
fn dump_rows(store: &MetaStore, sql: &str) -> String {
    RT.block_on(async {
        let mut rows = store.connection().query(sql, ()).await.expect("query");
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.expect("row") {
            out.push(row_to_string(&row));
        }
        out.join("\n")
    })
}

/// Render a libsql row as a stable `|`-joined string, coercing each value to text.
fn row_to_string(row: &libsql::Row) -> String {
    let mut cols = Vec::new();
    for i in 0..row.column_count() {
        let value = row.get_value(i).expect("column value");
        cols.push(match value {
            libsql::Value::Null => "∅".to_string(),
            libsql::Value::Integer(n) => n.to_string(),
            libsql::Value::Real(r) => r.to_string(),
            libsql::Value::Text(t) => t,
            libsql::Value::Blob(b) => format!("blob:{}", b.len()),
        });
    }
    cols.join("|")
}
