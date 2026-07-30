//! T91 acceptance tests: lineage projection into the metastore (+ optional asset
//! identity).
//!
//! These are the ticket's Test-plan scenarios (projection via reconcile AND the
//! live tee, cross-run query + no-FK discipline, and migration/additivity),
//! written FIRST and failing before the schema/mapping extension lands. They drive
//! the **real** event-stream writer
//! ([`dagr_artifact::event_stream::EventStreamWriter`]) — a real producer→fold→row
//! round-trip — so the lineage projection is proven against exactly the bytes a
//! run emits, checked against the `RunArtifact` that
//! [`dagr_artifact::fold::fold_stream`] folds from the same bytes.
//!
//! Each test uses a private per-test temp dir so the suite is collision-proof
//! under CI parallelism (ubuntu + macOS).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, ConsumedInput, DurableReferenceMeta, EventSink, EventStreamWriter,
    MonotonicClock, OutputProducedRecord, RunId, RunOutcome, RunStartedHeader, TerminalState,
    record_consumed_inputs, record_durable_reference, record_durable_reference_meta,
};
use dagr_artifact::fold::fold_stream;

use dagr_metastore::mapping::{sync_run, sync_run_store};
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
            "dagr-metastore-t91-{tag}-{}-{nanos}-{n}",
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

/// Open a metastore under `dir`.
async fn open_store_at(db: &Path) -> MetaStore {
    MetaStore::open(OpenMode::LocalFile(db.to_path_buf()))
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

// === Fixture: a run that produces and consumes durable lineage =============

/// Emit through a writer that shares a clock handle, so the test can set exact
/// offsets. Returns the on-disk bytes.
fn write_run_shared_clock(base: &Path, run_id: &str, a_uri: &str, a_hash: &str) -> Vec<u8> {
    let path = base.join("pipe").join(run_id).join("events.jsonl");
    // The writer needs to OWN its clock, so wrap a settable clock in an Arc-shared
    // adapter: the writer holds one handle, the test holds another.
    let shared = SharedClock::default();
    let mut writer = EventStreamWriter::new(
        FileSink::create(&path),
        shared.clone(),
        RunId::from_operator(run_id),
        "pipe",
    )
    .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
    emit_producer_consumer_shared(&mut writer, &shared, run_id, a_uri, a_hash);
    drop(writer);
    std::fs::read(&path).expect("read back the written stream")
}

/// A clock whose current value is shared (Arc), so a test can `set` offsets while
/// the writer owns its own clone.
#[derive(Clone, Default)]
struct SharedClock {
    n: std::sync::Arc<AtomicU64>,
}

impl SharedClock {
    fn set(&self, v: u64) {
        self.n.store(v, Ordering::SeqCst);
    }
}

impl MonotonicClock for SharedClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.load(Ordering::SeqCst)
    }
}

fn emit_producer_consumer_shared<S: EventSink>(
    w: &mut EventStreamWriter<S, SharedClock>,
    clock: &SharedClock,
    run_id: &str,
    a_uri: &str,
    a_hash: &str,
) {
    clock.set(0);
    w.run_started(header("pipe")).unwrap();

    clock.set(50);
    w.node_ready("a").unwrap();
    w.node_admitted("a").unwrap();
    clock.set(60);
    w.attempt_started("a", 1).unwrap();
    clock.set(100);
    w.attempt_succeeded("a", 1).unwrap();
    let mut a = AttemptOutcomeRecord::new("a", 1, TerminalState::Succeeded.as_str());
    a.worker = Some("compute#1".into());
    record_durable_reference(&mut a, Some(a_uri.to_string()));
    record_durable_reference_meta(
        &mut a,
        Some(
            DurableReferenceMeta::new()
                .content_hash(a_hash)
                .size_bytes(11)
                .scheme("s3")
                .produced_at_offset_ns(100),
        ),
    );
    w.attempt_outcome(a).unwrap();
    w.output_produced(OutputProducedRecord {
        node: "a".to_string(),
        attempt: 1,
        uri: a_uri.to_string(),
        content_hash: Some(a_hash.to_string()),
        size_bytes: Some(11),
        kind: Some("s3".to_string()),
        produced_at_offset_ns: 100,
        originating_run: run_id.to_string(),
    })
    .unwrap();
    w.node_terminal("a", TerminalState::Succeeded).unwrap();

    clock.set(150);
    w.node_ready("b").unwrap();
    w.node_admitted("b").unwrap();
    clock.set(160);
    w.attempt_started("b", 1).unwrap();
    clock.set(200);
    w.attempt_succeeded("b", 1).unwrap();
    let mut b = AttemptOutcomeRecord::new("b", 1, TerminalState::Succeeded.as_str());
    b.worker = Some("compute#2".into());
    record_consumed_inputs(
        &mut b,
        vec![ConsumedInput {
            uri: a_uri.to_string(),
            content_hash: Some(a_hash.to_string()),
        }],
    );
    w.attempt_outcome(b).unwrap();
    w.node_terminal("b", TerminalState::Succeeded).unwrap();

    clock.set(210);
    w.run_finished(RunOutcome::Succeeded).unwrap();
    w.finish().unwrap();
}

// === (a) PROJECTION via reconcile ==========================================

/// A run with produced/consumed lineage, when `sync`ed, yields `output_produced` /
/// `input_consumed` rows and `node_attempt.durable_reference_meta` columns matching
/// the folded `RunArtifact`'s `outputs[]`/`inputs[]`/`AttemptRecord`.
#[tokio::test]
async fn reconcile_projects_lineage_matching_the_fold() {
    let dir = TempDir::new("reconcile");
    let base = dir.path();
    let bytes = write_run_shared_clock(base, "run-1", "s3://bucket/a", "sha256:aaaa");

    // The fold is the authority we compare against.
    let artifact = fold_stream(&bytes, &[]).expect("fold the fixture");
    assert_eq!(artifact.outputs().len(), 1, "one produced output folded");
    let o = &artifact.outputs()[0];

    let store = open_store_at(&base.join("metastore.db")).await;
    let summary = sync_run_store(&store, base).await.expect("sync");
    assert_eq!(summary.synced, 1);

    // output_produced: one row matching outputs[0] field-for-field.
    let out = "FROM output_produced WHERE run_id='run-1'";
    assert_eq!(
        scalar_i64(&store, &q("count(*)", out)).await,
        1,
        "one output_produced row"
    );
    for (col, expected) in [
        ("uri", Some(o.uri())),
        ("content_hash", o.content_hash()),
        ("kind", o.kind()),
        ("originating_run", Some(o.originating_run())),
        ("node_id", Some("a")),
    ] {
        assert_eq!(
            opt_string(&store, &q(col, out)).await.as_deref(),
            expected,
            "output_produced.{col} carried by value"
        );
    }
    assert_eq!(
        scalar_i64(&store, &q("size_bytes", out)).await,
        11,
        "size_bytes by value"
    );

    // input_consumed: one row for b's consumed input (uri + hash by value).
    let inn = "FROM input_consumed WHERE run_id='run-1' AND node_id='b'";
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM input_consumed WHERE run_id='run-1'"
        )
        .await,
        1,
        "one input_consumed row (b consumed a's output)"
    );
    for (col, expected) in [("uri", "s3://bucket/a"), ("content_hash", "sha256:aaaa")] {
        assert_eq!(
            opt_string(&store, &q(col, inn)).await.as_deref(),
            Some(expected),
            "input_consumed.{col} by value"
        );
    }

    // node_attempt.durable_reference_meta columns: from a's producing attempt.
    let a_meta = "FROM node_attempt WHERE run_id='run-1' AND node_id='a' AND try_number=1";
    assert_eq!(
        opt_string(&store, &q("content_hash", a_meta))
            .await
            .as_deref(),
        Some("sha256:aaaa")
    );
    assert_eq!(scalar_i64(&store, &q("size_bytes", a_meta)).await, 11);
    assert_eq!(
        opt_string(&store, &q("scheme", a_meta)).await.as_deref(),
        Some("s3")
    );
    assert_eq!(
        scalar_i64(&store, &q("produced_at_offset_ns", a_meta)).await,
        100
    );

    // A non-producing attempt (b) reads NULL meta columns.
    let b_meta = "FROM node_attempt WHERE run_id='run-1' AND node_id='b' AND try_number=1";
    assert_eq!(
        opt_string(&store, &q("content_hash", b_meta)).await,
        None,
        "a non-producing attempt has NULL meta columns"
    );
}

/// `SELECT <col> <from_where>` — a tiny query builder that keeps the field-by-field
/// lineage assertions compact.
fn q(col: &str, from_where: &str) -> String {
    format!("SELECT {col} {from_where}")
}

/// Re-syncing a run with lineage is idempotent: no duplicate `output_produced` /
/// `input_consumed` / `asset` rows (the UPSERTs are keyed on the natural lineage
/// identity, and the fold is deterministic).
#[tokio::test]
async fn reconcile_lineage_resync_is_idempotent_no_dupes() {
    let dir = TempDir::new("reconcile-idem");
    let base = dir.path();
    write_run_shared_clock(base, "run-1", "s3://bucket/a", "sha256:aaaa");

    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("first sync");
    let produced1 = scalar_i64(&store, "SELECT count(*) FROM output_produced").await;
    let consumed1 = scalar_i64(&store, "SELECT count(*) FROM input_consumed").await;
    let assets1 = scalar_i64(&store, "SELECT count(*) FROM asset").await;

    sync_run_store(&store, base).await.expect("re-sync");
    assert_eq!(
        scalar_i64(&store, "SELECT count(*) FROM output_produced").await,
        produced1,
        "re-sync does not duplicate output_produced"
    );
    assert_eq!(
        scalar_i64(&store, "SELECT count(*) FROM input_consumed").await,
        consumed1,
        "re-sync does not duplicate input_consumed"
    );
    assert_eq!(
        scalar_i64(&store, "SELECT count(*) FROM asset").await,
        assets1,
        "re-sync does not duplicate asset rows"
    );
}

// === (b) PROJECTION via live tee ===========================================

/// The same run executed with the live tee produces the same lineage rows, and
/// live == reconcile for lineage.
#[tokio::test]
async fn live_tee_projects_the_same_lineage_as_reconcile() {
    // --- Reconcile path: write to disk, then sync. ---
    let recon_dir = TempDir::new("live-recon");
    let recon_base = recon_dir.path();
    write_run_shared_clock(recon_base, "run-live", "s3://bucket/a", "sha256:aaaa");
    let recon_store = open_store_at(&recon_base.join("metastore.db")).await;
    sync_run_store(&recon_store, recon_base)
        .await
        .expect("reconcile sync");

    // --- Live path: drive the same run through a MetastoreSink. ---
    let live_dir = TempDir::new("live-tee");
    let live_db = live_dir.path().join("metastore.db");
    {
        let sink = MetastoreSink::open(live_db.clone(), Some("events.jsonl".to_string()))
            .expect("open the live sink");
        let shared = SharedClock::default();
        let mut writer = EventStreamWriter::new(
            sink,
            shared.clone(),
            RunId::from_operator("run-live"),
            "pipe",
        )
        .with_wall_clock(|| "2026-07-27T00:00:00.000Z".to_string());
        emit_producer_consumer_shared(
            &mut writer,
            &shared,
            "run-live",
            "s3://bucket/a",
            "sha256:aaaa",
        );
        drop(writer);
    }
    let live_store = open_store_at(&live_db).await;

    // Live == reconcile: same lineage rows in both.
    for (label, store) in [("reconcile", &recon_store), ("live", &live_store)] {
        let produced = scalar_i64(
            store,
            "SELECT count(*) FROM output_produced WHERE run_id='run-live'",
        )
        .await;
        assert_eq!(produced, 1, "{label}: one output_produced row");
        let consumed = scalar_i64(
            store,
            "SELECT count(*) FROM input_consumed WHERE run_id='run-live'",
        )
        .await;
        assert_eq!(consumed, 1, "{label}: one input_consumed row");
        let uri = opt_string(
            store,
            "SELECT uri FROM output_produced WHERE run_id='run-live'",
        )
        .await;
        assert_eq!(uri.as_deref(), Some("s3://bucket/a"), "{label}: uri");
        let meta_hash = opt_string(
            store,
            "SELECT content_hash FROM node_attempt WHERE run_id='run-live' AND node_id='a' AND try_number=1",
        )
        .await;
        assert_eq!(
            meta_hash.as_deref(),
            Some("sha256:aaaa"),
            "{label}: node_attempt.content_hash"
        );
    }
}

// === (c) NO-FK discipline + cross-run join by value ========================

/// Two runs producing outputs at the same `uri` with different content hashes both
/// join to a single `asset` row BY VALUE, and deleting/absent `asset` rows never
/// orphans `output_produced` (no foreign key).
#[tokio::test]
async fn two_runs_same_uri_join_one_asset_by_value_no_fk() {
    let dir = TempDir::new("no-fk");
    let base = dir.path();
    // Two runs produce at the SAME uri with DIFFERENT content hashes.
    write_run_shared_clock(base, "run-a", "s3://shared/dataset", "sha256:1111");
    write_run_shared_clock(base, "run-b", "s3://shared/dataset", "sha256:2222");

    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync both runs");

    // Both runs' output_produced rows exist at the shared uri.
    let produced = scalar_i64(
        &store,
        "SELECT count(*) FROM output_produced WHERE uri='s3://shared/dataset'",
    )
    .await;
    assert_eq!(produced, 2, "both runs produced at the shared uri");

    // Exactly ONE asset row for the shared uri (populated by value, first sight).
    let assets = scalar_i64(
        &store,
        "SELECT count(*) FROM asset WHERE uri='s3://shared/dataset'",
    )
    .await;
    assert_eq!(
        assets, 1,
        "one asset identity row for the shared uri (by value)"
    );

    // The cross-run join answers "which runs produced dataset X" — BY VALUE on uri.
    let runs = scalar_i64(
        &store,
        "SELECT count(DISTINCT p.run_id) FROM output_produced p \
         JOIN asset a ON a.uri = p.uri WHERE a.uri='s3://shared/dataset'",
    )
    .await;
    assert_eq!(runs, 2, "both runs join to the single asset row by value");

    // The two rows carry DISTINCT content hashes (append-only, not overwritten).
    let hashes = scalar_i64(
        &store,
        "SELECT count(DISTINCT content_hash) FROM output_produced WHERE uri='s3://shared/dataset'",
    )
    .await;
    assert_eq!(
        hashes, 2,
        "two distinct content hashes at the same uri (append-only)"
    );

    // NO FK: delete the asset row; output_produced is untouched (survives-GC).
    store
        .with_write_txn(|c| {
            Box::pin(async move {
                c.execute("DELETE FROM asset WHERE uri='s3://shared/dataset'", ())
                    .await
                    .map(|_| ())
            })
        })
        .await
        .expect("delete the asset row (no FK blocks it)");
    let produced_after = scalar_i64(
        &store,
        "SELECT count(*) FROM output_produced WHERE uri='s3://shared/dataset'",
    )
    .await;
    assert_eq!(
        produced_after, 2,
        "deleting the asset row does NOT orphan or delete output_produced (no FK)"
    );
}

// === (d) MIGRATION + additivity ============================================

/// A metastore created at M7 (T83 shape: the five tables, no lineage) opens after
/// T91 with forward migrations that ADD the lineage tables/columns without
/// disturbing existing rows.
#[tokio::test]
async fn m7_store_upgrades_in_place_adding_lineage_without_disturbing_rows() {
    let dir = TempDir::new("migrate");
    let db = dir.path().join("metastore.db");

    // 1) Create an M7-shape store WITHOUT the lineage tables/columns, and seed a row.
    {
        let m7 = build_m7_store(&db).await;
        m7.seed(&[
            "INSERT INTO dag (dag_id, name, created_ms) VALUES ('pipe', 'pipe', 7)",
            "INSERT INTO dag_run (run_id, dag_id, state, started_ms) \
             VALUES ('old-run', 'pipe', 'succeeded', 0)",
            "INSERT INTO node_attempt (run_id, node_id, try_number, state) \
             VALUES ('old-run', 'n', 1, 'succeeded')",
        ])
        .await;

        // The lineage tables do NOT exist yet on the M7 store.
        assert!(
            !m7.table_exists("output_produced").await,
            "M7 store has no output_produced table yet"
        );
    }

    // 2) Open the SAME file with the full T91 MetaStore — forward migrations run.
    let upgraded = open_store_at(&db).await;

    // The lineage tables now exist.
    for table in ["output_produced", "input_consumed", "asset"] {
        let present = scalar_i64(
            &upgraded,
            &format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'"),
        )
        .await;
        assert_eq!(present, 1, "T91 forward migration added `{table}`");
    }
    // The new node_attempt lineage columns exist (an ALTER-added column reads).
    for col in [
        "content_hash",
        "size_bytes",
        "scheme",
        "produced_at_offset_ns",
    ] {
        let _ = scalar_i64(&upgraded, &format!("SELECT count({col}) FROM node_attempt")).await;
    }

    // The existing rows are UNDISTURBED.
    let dag_name = opt_string(&upgraded, "SELECT name FROM dag WHERE dag_id='pipe'").await;
    assert_eq!(dag_name.as_deref(), Some("pipe"));
    let run_state = opt_string(
        &upgraded,
        "SELECT state FROM dag_run WHERE run_id='old-run'",
    )
    .await;
    assert_eq!(run_state.as_deref(), Some("succeeded"));
    let attempt_state = opt_string(
        &upgraded,
        "SELECT state FROM node_attempt WHERE run_id='old-run' AND node_id='n'",
    )
    .await;
    assert_eq!(attempt_state.as_deref(), Some("succeeded"));
    // The seeded old row's new lineage columns read as NULL (additive, undisturbed).
    let old_hash = opt_string(
        &upgraded,
        "SELECT content_hash FROM node_attempt WHERE run_id='old-run' AND node_id='n'",
    )
    .await;
    assert_eq!(
        old_hash, None,
        "the old row's added column is NULL, not disturbed"
    );

    // A store with no lineage data has EMPTY lineage tables and behaves as before.
    let empty_produced = scalar_i64(&upgraded, "SELECT count(*) FROM output_produced").await;
    assert_eq!(
        empty_produced, 0,
        "no-lineage store has an empty output_produced"
    );
    let empty_consumed = scalar_i64(&upgraded, "SELECT count(*) FROM input_consumed").await;
    assert_eq!(
        empty_consumed, 0,
        "no-lineage store has an empty input_consumed"
    );
    let empty_asset = scalar_i64(&upgraded, "SELECT count(*) FROM asset").await;
    assert_eq!(empty_asset, 0, "no-lineage store has an empty asset table");

    // And a real run projects into it cleanly after the upgrade.
    let base = dir.path();
    write_run_shared_clock(base, "run-new", "s3://bucket/new", "sha256:new0");
    // sync_run against the same store file (folded off disk).
    let bytes = std::fs::read(base.join("pipe").join("run-new").join("events.jsonl")).unwrap();
    let artifact = fold_stream(&bytes, &[]).unwrap();
    sync_run(&upgraded, &artifact, None)
        .await
        .expect("sync new run");
    let new_produced = scalar_i64(
        &upgraded,
        "SELECT count(*) FROM output_produced WHERE run_id='run-new'",
    )
    .await;
    assert_eq!(
        new_produced, 1,
        "a new run projects lineage into the upgraded store"
    );
}

/// A thin M7-shape store built from **raw** DDL — the exact T83 tables and
/// columns, WITHOUT any lineage tables or the `durable_reference_meta` columns — so
/// the upgrade test proves the real T91 forward migration adds them in place over a
/// genuine pre-T91 file. It exposes a `with_write_txn`-like shim and a
/// `count`/`scalar` reader over a raw libSQL connection; the file it writes is then
/// re-opened by the real [`MetaStore`], which runs the T91 migrations.
struct M7Store {
    conn: libsql::Connection,
    _db: libsql::Database,
}

impl M7Store {
    async fn seed(&self, statements: &[&str]) {
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .expect("begin");
        for stmt in statements {
            self.conn.execute(stmt, ()).await.expect("seed stmt");
        }
        self.conn.execute("COMMIT", ()).await.expect("commit");
    }

    async fn table_exists(&self, table: &str) -> bool {
        let mut rows = self
            .conn
            .query(
                &format!(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'"
                ),
                (),
            )
            .await
            .expect("query");
        let row = rows.next().await.expect("row").expect("one row");
        row.get::<i64>(0).expect("i64") == 1
    }
}

/// Lay down the pre-T91 (M7 / T83) schema — the five tables with the pre-lineage
/// column set only — on a fresh file via raw libSQL DDL.
async fn build_m7_store(db: &Path) -> M7Store {
    let dbh = libsql::Builder::new_local(db)
        .build()
        .await
        .expect("build m7 db");
    let conn = dbh.connect().expect("connect m7");
    // A minimal M7 schema: the five tables T83 shipped, with the T84 rich columns
    // but WITHOUT the T91 lineage tables or the node_attempt.durable_reference_meta
    // columns. The state CHECKs are omitted here (the M7 shape detail that matters
    // for this test is column presence, not constraint text).
    let m7_ddl = [
        "CREATE TABLE dag (dag_id TEXT PRIMARY KEY, name TEXT NOT NULL, created_ms INTEGER NOT NULL)",
        "CREATE TABLE dag_version (dag_version_id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, \
         structural_fingerprint TEXT NOT NULL, created_ms INTEGER NOT NULL, meta_json TEXT)",
        "CREATE TABLE dag_run (run_id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, dag_version_id TEXT, \
         state TEXT NOT NULL, started_ms INTEGER NOT NULL, finished_ms INTEGER, \
         interrupted INTEGER NOT NULL DEFAULT 0, resumed_from TEXT, events_path TEXT, \
         params_json TEXT, env_json TEXT, meta_json TEXT)",
        "CREATE TABLE node_attempt (run_id TEXT NOT NULL, node_id TEXT NOT NULL, \
         try_number INTEGER NOT NULL, state TEXT NOT NULL, started_ms INTEGER, finished_ms INTEGER, \
         message TEXT, metrics_json TEXT, worker TEXT, phase_durations_json TEXT, error_json TEXT, \
         cost_declared_json TEXT, cost_measured_json TEXT, durable_reference TEXT, \
         satisfied_from_run TEXT, originating_node TEXT, UNIQUE (run_id, node_id, try_number))",
        "CREATE TABLE node_terminal (run_id TEXT NOT NULL, node_id TEXT NOT NULL, \
         state TEXT NOT NULL, finished_ms INTEGER, PRIMARY KEY (run_id, node_id))",
    ];
    for stmt in m7_ddl {
        conn.execute(stmt, ()).await.expect("m7 ddl");
    }
    M7Store { conn, _db: dbh }
}
