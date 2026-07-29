//! T84 acceptance tests: the event→row mapping and the `sync` reconcile walk.
//!
//! These are the ticket's Test-plan scenarios (mapping fidelity, crash-truncated
//! / assembly-failed / bootstrap-failed representation, idempotency, batch
//! robustness, and follow), written FIRST and failing before the mapping module
//! and the run-store walk land. Each test uses a private per-test temp dir so the
//! suite is collision-proof under CI parallelism.
//!
//! The fixtures are produced by the **real** event-stream writer
//! ([`dagr_artifact::event_stream::EventStreamWriter`]) — a real producer→fold→
//! row round-trip, not a hand-authored JSONL — so the mapping is proven against
//! exactly the bytes a run emits, and everything is checked against the
//! `RunArtifact` that [`dagr_artifact::fold::fold_stream`] folds from the same
//! bytes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState,
};
use dagr_artifact::fold::fold_stream;

use dagr_metastore::MetaStore;
use dagr_metastore::mapping::sync_run_store;
use dagr_metastore::store::OpenMode;

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
            "dagr-metastore-t84-{tag}-{}-{nanos}-{n}",
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

/// A file-backed sink that appends each record to a run's `events.jsonl` on disk,
/// so the fixture lands in the real `<base>/<pipeline>/<run-id>/events.jsonl`
/// layout the run-store walk discovers.
struct FileSink {
    file: std::fs::File,
}

impl FileSink {
    fn create(path: &Path) -> Self {
        std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mk run dir");
        let file = std::fs::File::create(path).expect("create events.jsonl");
        Self { file }
    }
}

impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        self.file.write_all(line)?;
        self.file.flush()
    }
    fn flush(&mut self) -> std::io::Result<()> {
        use std::io::Write;
        self.file.flush()
    }
}

/// A deterministic per-read counter clock (like the cli's `TickClock`), so the
/// fixture bytes are reproducible.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
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

/// Build a real event-stream and write it to `<base>/<pipeline>/<run_id>/events.jsonl`.
/// `emit` gets the writer to script the run's events; `finish` decides whether a
/// `run-finished` is emitted (a crash-truncated run omits it).
fn write_run(
    base: &Path,
    pipeline: &str,
    run_id: &str,
    emit: impl FnOnce(&mut EventStreamWriter<FileSink, TickClock>),
) -> Vec<u8> {
    let path = base.join(pipeline).join(run_id).join("events.jsonl");
    let mut writer = EventStreamWriter::new(
        FileSink::create(&path),
        TickClock::default(),
        RunId::from_operator(run_id),
        pipeline,
    )
    .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
    emit(&mut writer);
    // The writer owns the sink, so drop it to flush/close, then read the stream
    // back off disk for the fold comparison.
    drop(writer);
    std::fs::read(&path).expect("read back the written stream")
}

/// Open a metastore under `dir`.
async fn open_store(dir: &Path) -> MetaStore {
    let db = dir.join("metastore.db");
    MetaStore::open(OpenMode::LocalFile(db))
        .await
        .expect("open the metastore")
}

async fn scalar_i64(store: &MetaStore, sql: &str) -> i64 {
    let mut rows = store.connection().query(sql, ()).await.expect("query");
    let row = rows.next().await.expect("row").expect("one row");
    row.get::<i64>(0).expect("i64")
}

async fn opt_string(store: &MetaStore, sql: &str) -> Option<String> {
    let mut rows = store.connection().query(sql, ()).await.expect("query");
    let row = rows.next().await.expect("row")?;
    row.get::<String>(0).ok()
}

// === A helper to write a fixture directly to the run store =================

/// Emit a two-node run with a retry on `b`: `a` succeeds, `b` fails once then
/// succeeds (durable reference on the success), and a `run-finished(succeeded)`.
fn emit_multi_node_with_retry(w: &mut EventStreamWriter<FileSink, TickClock>) {
    w.run_started(header("pipe")).unwrap();
    // node a: ready, admitted, one attempt, succeeded, terminal.
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
        durable_reference: Some(serde_json::json!("s3://bucket/a")),
        ..AttemptOutcomeRecord::default()
    })
    .unwrap();
    w.node_terminal("a", TerminalState::Succeeded).unwrap();
    // node b: ready, admitted, attempt 1 fails, attempt 2 succeeds.
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
        error: Some(serde_json::json!({ "kind": "io" })),
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

// === Mapping fidelity ======================================================

/// Given a fixture `events.jsonl` for a multi-node run with a retry, when `sync`
/// folds and upserts it, then `dag_run` state/timing and every `node_attempt`
/// (one row per attempt, correct `try_number`, phase durations, durable
/// reference, `satisfied_from_run`) match the `RunArtifact` `fold_stream` produces,
/// and `node_terminal` holds the latest state per node.
#[tokio::test]
async fn mapping_matches_fold_stream_for_a_multi_node_run_with_retry() {
    let dir = TempDir::new("fidelity");
    let base = dir.path();
    let bytes = write_run(base, "pipe", "run-1", emit_multi_node_with_retry);

    // The fold is the authority we compare against.
    let artifact = fold_stream(&bytes, &[]).expect("fold the fixture");
    let expected_attempts = artifact.attempts().len();

    let store = open_store(base).await;
    let summary = sync_run_store(&store, base)
        .await
        .expect("sync the run store");
    assert_eq!(summary.synced, 1, "one run synced");
    assert_eq!(summary.skipped, 0, "no run skipped");

    // dag_run: identity + state + it is present.
    let run_rows = scalar_i64(&store, "SELECT count(*) FROM dag_run WHERE run_id='run-1'").await;
    assert_eq!(run_rows, 1, "exactly one dag_run row for the run");
    let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-1'").await;
    assert_eq!(
        state.as_deref(),
        Some(artifact.overall_outcome()),
        "dag_run.state is the folded overall_outcome"
    );
    let interrupted = scalar_i64(
        &store,
        "SELECT interrupted FROM dag_run WHERE run_id='run-1'",
    )
    .await;
    assert_eq!(interrupted, 0, "a finished run is not interrupted");

    // node_attempt: one row per AttemptRecord the fold produced.
    let attempt_rows = scalar_i64(
        &store,
        "SELECT count(*) FROM node_attempt WHERE run_id='run-1'",
    )
    .await;
    assert_eq!(
        attempt_rows,
        i64::try_from(expected_attempts).unwrap(),
        "one node_attempt row per folded AttemptRecord"
    );

    // b has two attempts (a retry); the second is succeeded.
    let b_attempts = scalar_i64(
        &store,
        "SELECT count(*) FROM node_attempt WHERE run_id='run-1' AND node_id='b'",
    )
    .await;
    assert_eq!(b_attempts, 2, "node b recorded both attempts (the retry)");
    let b2_state = opt_string(
        &store,
        "SELECT state FROM node_attempt WHERE run_id='run-1' AND node_id='b' AND try_number=2",
    )
    .await;
    assert_eq!(b2_state.as_deref(), Some("succeeded"));
    let b1_state = opt_string(
        &store,
        "SELECT state FROM node_attempt WHERE run_id='run-1' AND node_id='b' AND try_number=1",
    )
    .await;
    assert_eq!(b1_state.as_deref(), Some("failed"));

    // Durable reference carried onto a's succeeded attempt.
    let a_dref = opt_string(
        &store,
        "SELECT durable_reference FROM node_attempt WHERE run_id='run-1' AND node_id='a' AND try_number=1",
    )
    .await;
    assert!(
        a_dref.is_some_and(|s| s.contains("s3://bucket/a")),
        "the durable reference is carried onto node a"
    );

    // Phase durations JSON carried (sums to the attempt total, by fold construction).
    let phases = opt_string(
        &store,
        "SELECT phase_durations_json FROM node_attempt WHERE run_id='run-1' AND node_id='a' AND try_number=1",
    )
    .await;
    assert!(
        phases.is_some_and(|s| s.contains("executing")),
        "phase durations are carried as JSON"
    );

    // node_terminal: latest terminal state per node.
    let terminals = scalar_i64(
        &store,
        "SELECT count(*) FROM node_terminal WHERE run_id='run-1'",
    )
    .await;
    assert_eq!(terminals, 2, "one node_terminal row per node (a and b)");
    let a_term = opt_string(
        &store,
        "SELECT state FROM node_terminal WHERE run_id='run-1' AND node_id='a'",
    )
    .await;
    assert_eq!(a_term.as_deref(), Some("succeeded"));
    let b_term = opt_string(
        &store,
        "SELECT state FROM node_terminal WHERE run_id='run-1' AND node_id='b'",
    )
    .await;
    assert_eq!(b_term.as_deref(), Some("succeeded"));
}

/// A crash-truncated stream (no `run-finished`): `dag_run.interrupted = 1`, the
/// partial attempts are recorded without error, and the state is the folded
/// `cancelled` (the closed-enum representation of an interrupted run).
#[tokio::test]
async fn crash_truncated_run_is_interrupted_with_partial_attempts() {
    let dir = TempDir::new("crash");
    let base = dir.path();
    let bytes = write_run(base, "pipe", "run-crash", |w| {
        w.run_started(header("pipe")).unwrap();
        w.node_ready("a").unwrap();
        w.node_admitted("a").unwrap();
        w.attempt_started("a", 1).unwrap();
        w.attempt_succeeded("a", 1).unwrap();
        w.attempt_outcome(AttemptOutcomeRecord::new("a", 1, "succeeded"))
            .unwrap();
        w.node_terminal("a", TerminalState::Succeeded).unwrap();
        // No run-finished — the process died here.
    });

    let artifact = fold_stream(&bytes, &[]).expect("fold");
    assert!(
        artifact.is_interrupted(),
        "the fixture folds as interrupted"
    );

    let store = open_store(base).await;
    let summary = sync_run_store(&store, base).await.expect("sync");
    assert_eq!(summary.synced, 1);

    let interrupted = scalar_i64(
        &store,
        "SELECT interrupted FROM dag_run WHERE run_id='run-crash'",
    )
    .await;
    assert_eq!(interrupted, 1, "a crash-truncated run is interrupted=1");
    let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-crash'").await;
    assert_eq!(
        state.as_deref(),
        Some("cancelled"),
        "the closed-enum outcome of an interrupted run is cancelled"
    );
    let a_attempts = scalar_i64(
        &store,
        "SELECT count(*) FROM node_attempt WHERE run_id='run-crash' AND node_id='a'",
    )
    .await;
    assert_eq!(a_attempts, 1, "the partial attempt was recorded");
}

/// An assembly-failed run maps its state faithfully (zero attempts, distinct from
/// bootstrap-failed).
#[tokio::test]
async fn assembly_failed_run_maps_its_state() {
    let dir = TempDir::new("assembly");
    let base = dir.path();
    let bytes = write_run(base, "pipe", "run-af", |w| {
        w.run_started(header("pipe")).unwrap();
        w.run_finished(RunOutcome::AssemblyFailed).unwrap();
        w.finish().unwrap();
    });
    let artifact = fold_stream(&bytes, &[]).expect("fold");
    assert_eq!(artifact.overall_outcome(), "assembly-failed");

    let store = open_store(base).await;
    sync_run_store(&store, base).await.expect("sync");
    let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-af'").await;
    assert_eq!(state.as_deref(), Some("assembly-failed"));
    let attempts = scalar_i64(
        &store,
        "SELECT count(*) FROM node_attempt WHERE run_id='run-af'",
    )
    .await;
    assert_eq!(attempts, 0, "a pre-execution failure has zero attempts");
}

/// A bootstrap-failed run maps its distinct state.
#[tokio::test]
async fn bootstrap_failed_run_maps_its_state() {
    let dir = TempDir::new("bootstrap");
    let base = dir.path();
    write_run(base, "pipe", "run-bf", |w| {
        w.run_started(header("pipe")).unwrap();
        w.run_finished(RunOutcome::BootstrapFailed).unwrap();
        w.finish().unwrap();
    });
    let store = open_store(base).await;
    sync_run_store(&store, base).await.expect("sync");
    let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-bf'").await;
    assert_eq!(state.as_deref(), Some("bootstrap-failed"));
}

// === Idempotency ===========================================================

/// Given a run already synced, re-syncing creates no duplicate rows and leaves row
/// counts unchanged (deterministic fold + UPSERT).
#[tokio::test]
async fn resync_is_idempotent_no_dupes_stable_counts() {
    let dir = TempDir::new("idem");
    let base = dir.path();
    write_run(base, "pipe", "run-1", emit_multi_node_with_retry);
    let store = open_store(base).await;

    sync_run_store(&store, base).await.expect("first sync");
    let runs1 = scalar_i64(&store, "SELECT count(*) FROM dag_run").await;
    let attempts1 = scalar_i64(&store, "SELECT count(*) FROM node_attempt").await;
    let terminals1 = scalar_i64(&store, "SELECT count(*) FROM node_terminal").await;

    // Sync again — everything is an UPSERT, so counts must be identical.
    let summary = sync_run_store(&store, base).await.expect("second sync");
    assert_eq!(summary.synced, 1, "the run is folded and upserted again");
    let runs2 = scalar_i64(&store, "SELECT count(*) FROM dag_run").await;
    let attempts2 = scalar_i64(&store, "SELECT count(*) FROM node_attempt").await;
    let terminals2 = scalar_i64(&store, "SELECT count(*) FROM node_terminal").await;

    assert_eq!(runs1, runs2, "no duplicate dag_run rows");
    assert_eq!(attempts1, attempts2, "no duplicate node_attempt rows");
    assert_eq!(terminals1, terminals2, "no duplicate node_terminal rows");
}

// === Batch robustness ======================================================

/// A run store with three run directories, one of which has no readable stream:
/// the two good runs are upserted, the bad one is reported and skipped, and the
/// walk reports the skip (never aborting the batch).
#[tokio::test]
async fn batch_skips_one_unreadable_run_and_syncs_the_rest() {
    let dir = TempDir::new("batch");
    let base = dir.path();
    write_run(base, "pipe", "good-1", emit_multi_node_with_retry);
    write_run(base, "pipe", "good-2", emit_multi_node_with_retry);
    // A third run directory with an events.jsonl that has no run-started record —
    // an unfoldable/unreadable stream that must be skipped, not fatal.
    let bad_dir = base.join("pipe").join("bad-3");
    std::fs::create_dir_all(&bad_dir).expect("mk bad dir");
    std::fs::write(bad_dir.join("events.jsonl"), b"{\"kind\":\"node-ready\"}\n")
        .expect("write a stream with no run-started");

    let store = open_store(base).await;
    let summary = sync_run_store(&store, base).await.expect("sync exits Ok");
    assert_eq!(summary.synced, 2, "the two good runs synced");
    assert_eq!(summary.skipped, 1, "the unreadable run was skipped");

    let runs = scalar_i64(&store, "SELECT count(*) FROM dag_run").await;
    assert_eq!(runs, 2, "only the two good runs produced rows");
    let bad_present = scalar_i64(&store, "SELECT count(*) FROM dag_run WHERE run_id='bad-3'").await;
    assert_eq!(bad_present, 0, "the bad run produced no rows");
}

// === Follow (incremental) ==================================================

/// After an initial sync, a newly-appeared finalized run is picked up by a second
/// sync pass without reprocessing already-synced runs (the follow consolidator's
/// incremental step; the CLI `--follow` loops this pass).
#[tokio::test]
async fn a_new_run_is_picked_up_without_reprocessing_synced_runs() {
    let dir = TempDir::new("follow");
    let base = dir.path();
    write_run(base, "pipe", "run-1", emit_multi_node_with_retry);
    let store = open_store(base).await;

    let first = sync_run_store(&store, base).await.expect("first pass");
    assert_eq!(first.synced, 1);
    assert_eq!(
        first.newly_synced, 1,
        "the first run is newly synced on the first pass"
    );

    // A new run appears after the first pass.
    write_run(base, "pipe", "run-2", emit_multi_node_with_retry);
    let second = sync_run_store(&store, base).await.expect("second pass");
    // Both runs are folded+upserted (idempotent), but only the new one is *newly*
    // synced — the already-synced run is not a new row.
    assert_eq!(second.synced, 2, "both runs are folded on the second pass");
    assert_eq!(
        second.newly_synced, 1,
        "only the new run is newly synced (the old one already existed)"
    );

    let runs = scalar_i64(&store, "SELECT count(*) FROM dag_run").await;
    assert_eq!(runs, 2, "both runs are present, no duplicates");
}
