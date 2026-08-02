//! T111 acceptance tests: the **submission projection and audit surface**.
//! Written FIRST (TDD), failing before the fold/schema/mapping extension lands.
//!
//! The ticket's reason for existing is a guarantee, not a table: **the index is a
//! projection of the event stream and nothing else**. So the first thing asserted
//! here is that the new rows are produced by the *same* `build_statements` path
//! from the *same* folded artifact, reachable identically by the live tee and by a
//! post-hoc `sync` — byte-identical rows either way. There is no path where an
//! executor writes SQL directly.
//!
//! Everything drives the **real** event-stream writer
//! ([`dagr_artifact::event_stream::EventStreamWriter`]) — a genuine
//! producer→fold→row round trip — so the projection is proven against exactly the
//! bytes a placed run emits.
//!
//! Each test uses a private per-test temp dir so the suite is collision-proof under
//! CI parallelism (ubuntu + macOS).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, AttemptSubmittedRecord, ConsumedInput, EventSink, EventStreamWriter,
    MonotonicClock, RunId, RunStartedHeader, TerminalState, record_consumed_inputs,
};
use dagr_artifact::fold::fold_stream;

use dagr_metastore::mapping::{sync_run, sync_run_store};
use dagr_metastore::store::OpenMode;
use dagr_metastore::{MetaStore, MetastoreSink};

// === Test scaffolding ======================================================

/// A private per-test temp directory, created fresh and removed on drop.
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
            "dagr-metastore-t111-{tag}-{}-{nanos}-{n}",
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

/// A file-backed sink writing into the real `<base>/<pipeline>/<run-id>/events.jsonl`
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

/// A clock whose value is shared, so a test can set exact offsets while the writer
/// owns its own clone.
#[derive(Clone, Default)]
struct SharedClock {
    n: Arc<AtomicU64>,
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

fn header(pipeline: &str) -> RunStartedHeader {
    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-31".to_string());
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

fn consumed(uri: &str, hash: Option<&str>) -> ConsumedInput {
    ConsumedInput {
        uri: uri.to_string(),
        content_hash: hash.map(String::from),
    }
}

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

/// Every row of a single-column `TEXT` query, in the query's own order.
async fn rows_text(store: &MetaStore, sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rows = store.connection().query(sql, ()).await.expect("query");
    while let Some(row) = rows.next().await.expect("row") {
        out.push(row.get::<String>(0).expect("text"));
    }
    out
}

/// A `SELECT` rendering every named column of `table` into one `|`-joined text
/// column (NULLs made visible), ordered deterministically — the byte-identity
/// comparison the live-equals-reconcile assertion is built on.
fn row_dump_sql(table: &str, cols: &[&str], order: &str) -> String {
    let parts: Vec<String> = cols
        .iter()
        .map(|c| format!("coalesce(CAST({c} AS TEXT),'<null>')"))
        .collect();
    format!(
        "SELECT {} FROM {table} ORDER BY {order}",
        parts.join("||'|'||")
    )
}

/// Every column of `attempt_submitted`, in schema order.
const SUBMISSION_COLS: &[&str] = &[
    "run_id",
    "node_id",
    "attempt",
    "executor",
    "target_name",
    "observed_name",
    "observed_uid",
    "observed_host",
    "structural_fingerprint",
    "policy_hash",
    "tool_version",
    "image_digest",
    "input_count",
    "submitted_at_offset_ns",
    "completed",
    "outcome_state",
];

/// Every column of `attempt_submitted_input`, in schema order.
const SUBMISSION_INPUT_COLS: &[&str] = &[
    "run_id",
    "node_id",
    "attempt",
    "position",
    "uri",
    "content_hash",
];

fn submission_dump() -> String {
    row_dump_sql(
        "attempt_submitted",
        SUBMISSION_COLS,
        "run_id, node_id, attempt",
    )
}

fn submission_input_dump() -> String {
    row_dump_sql(
        "attempt_submitted_input",
        SUBMISSION_INPUT_COLS,
        "run_id, node_id, attempt, position",
    )
}

// === The fixture run =======================================================

/// A placed run: `extract` is submitted (intent), observed additively, and
/// completes; `load` is submitted with three ordered inputs and **never** reports;
/// `source` is a consume-nothing placed source that completes.
///
/// `read_hash` is the content hash `extract`'s attempt-outcome reports having
/// actually read for `blob://in-0`, so a test can make it agree with or diverge
/// from the submitted hash.
fn write_placed_run(base: &Path, run_id: &str, read_hash: &str) -> Vec<u8> {
    let path = base.join("pipe").join(run_id).join("events.jsonl");
    let clock = SharedClock::default();
    let mut w = EventStreamWriter::new(
        FileSink::create(&path),
        clock.clone(),
        RunId::from_operator(run_id),
        "pipe",
    )
    .with_wall_clock(|| "2026-07-31T00:00:00.000Z".to_string());
    emit_placed_run(&mut w, &clock, read_hash);
    drop(w);
    std::fs::read(&path).expect("read back the written stream")
}

fn emit_placed_run<S: EventSink>(
    w: &mut EventStreamWriter<S, SharedClock>,
    clock: &SharedClock,
    read_hash: &str,
) {
    clock.set(0);
    w.run_started(header("pipe")).unwrap();

    // --- source: a consume-nothing placed source ---------------------------
    clock.set(10);
    w.node_ready("source").unwrap();
    w.node_admitted("source").unwrap();
    w.attempt_submitted(
        AttemptSubmittedRecord::new("source", 1)
            .executor("k8s")
            .target_name("dagr-source-1"),
    )
    .unwrap();
    clock.set(20);
    w.attempt_started("source", 1).unwrap();
    clock.set(30);
    w.attempt_succeeded("source", 1).unwrap();
    w.attempt_outcome(AttemptOutcomeRecord::new("source", 1, "succeeded"))
        .unwrap();
    w.node_terminal("source", TerminalState::Succeeded).unwrap();

    // --- extract: submitted, observed additively, completed ----------------
    clock.set(50);
    w.node_ready("extract").unwrap();
    w.node_admitted("extract").unwrap();
    clock.set(60);
    let base = AttemptSubmittedRecord::new("extract", 1)
        .inputs(vec![
            consumed("blob://in-0", Some("sha256:0000")),
            consumed("blob://in-1", Some("sha256:1111")),
        ])
        .executor("k8s")
        .target_name("dagr-extract-1")
        .structural_fingerprint("blake3:aaa")
        .policy_hash("blake3:bbb")
        .tool_version("dagr 0.0.0")
        .image_digest("sha256:image");
    w.attempt_submitted(base.clone()).unwrap();
    clock.set(65);
    w.attempt_submitted(
        base.observed_name("dagr-extract-1-adopted")
            .observed_uid("uid-9f")
            .observed_host("node-7"),
    )
    .unwrap();
    clock.set(70);
    w.attempt_started("extract", 1).unwrap();
    clock.set(100);
    w.attempt_succeeded("extract", 1).unwrap();
    let mut outcome = AttemptOutcomeRecord::new("extract", 1, "succeeded");
    outcome.worker = Some("compute#1".into());
    // What the attempt reports having actually READ — the shard's own record of
    // its inputs, which is what a divergence query compares against.
    record_consumed_inputs(
        &mut outcome,
        vec![
            consumed("blob://in-0", Some(read_hash)),
            consumed("blob://in-1", Some("sha256:1111")),
        ],
    );
    w.attempt_outcome(outcome).unwrap();
    w.node_terminal("extract", TerminalState::Succeeded).unwrap();

    // --- load: submitted with ordered inputs, never reports ---------------
    clock.set(150);
    w.node_ready("load").unwrap();
    w.node_admitted("load").unwrap();
    clock.set(160);
    w.attempt_submitted(
        AttemptSubmittedRecord::new("load", 1)
            .inputs(vec![
                consumed("blob://zeta", Some("sha256:z")),
                consumed("blob://alpha", Some("sha256:a")),
                consumed("blob://mid", None),
            ])
            .executor("k8s")
            .target_name("dagr-load-1"),
    )
    .unwrap();
    // No outcome, no node-terminal, no run-finished: this is what a crashed
    // orchestrator leaves behind.
}

// === (a) THE PROJECTION GUARANTEE ==========================================

/// The rows a **live tee** writes and the rows a post-hoc **`sync`** of the same
/// stream writes are **byte-identical**, for both new tables — the existing
/// live-equals-reconcile assertion, extended.
#[tokio::test]
async fn live_tee_and_sync_produce_byte_identical_submission_rows() {
    // --- Reconcile: write the stream to disk, then sync it. ---
    let recon_dir = TempDir::new("parity-recon");
    let recon_base = recon_dir.path();
    write_placed_run(recon_base, "run-parity", "sha256:0000");
    let recon_store = open_store_at(&recon_base.join("metastore.db")).await;
    sync_run_store(&recon_store, recon_base)
        .await
        .expect("reconcile sync");

    // --- Live: drive the identical run through a MetastoreSink. ---
    let live_dir = TempDir::new("parity-live");
    let live_db = live_dir.path().join("metastore.db");
    {
        let sink = MetastoreSink::open(live_db.clone(), Some("events.jsonl".to_string()))
            .expect("open the live sink");
        let clock = SharedClock::default();
        let mut w = EventStreamWriter::new(
            sink,
            clock.clone(),
            RunId::from_operator("run-parity"),
            "pipe",
        )
        .with_wall_clock(|| "2026-07-31T00:00:00.000Z".to_string());
        emit_placed_run(&mut w, &clock, "sha256:0000");
        drop(w);
    }
    let live_store = open_store_at(&live_db).await;

    let recon_rows = rows_text(&recon_store, &submission_dump()).await;
    let live_rows = rows_text(&live_store, &submission_dump()).await;
    assert!(
        !recon_rows.is_empty(),
        "the fixture really does project submission rows"
    );
    assert_eq!(
        live_rows, recon_rows,
        "the live attempt_submitted rows are byte-identical to the reconcile rows"
    );

    let recon_inputs = rows_text(&recon_store, &submission_input_dump()).await;
    let live_inputs = rows_text(&live_store, &submission_input_dump()).await;
    assert!(!recon_inputs.is_empty(), "and the child input rows exist");
    assert_eq!(
        live_inputs, recon_inputs,
        "the live attempt_submitted_input rows are byte-identical to the reconcile rows"
    );
}

/// `sync` run twice over the same stream leaves the rows unchanged — the idempotent
/// UPSERT every other table already uses.
#[tokio::test]
async fn resyncing_the_same_stream_is_idempotent() {
    let dir = TempDir::new("idempotent");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");

    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("first sync");
    let first = rows_text(&store, &submission_dump()).await;
    let first_inputs = rows_text(&store, &submission_input_dump()).await;

    sync_run_store(&store, base).await.expect("re-sync");
    assert_eq!(
        rows_text(&store, &submission_dump()).await,
        first,
        "re-syncing does not duplicate or perturb attempt_submitted rows"
    );
    assert_eq!(
        rows_text(&store, &submission_input_dump()).await,
        first_inputs,
        "re-syncing does not duplicate or perturb attempt_submitted_input rows"
    );
}

/// A pre-T111 store gains the new tables **in place**, with no existing row
/// disturbed, and converges on the identical shape a fresh store gets.
#[tokio::test]
async fn a_pre_t111_store_upgrades_in_place_and_converges_with_a_fresh_one() {
    let dir = TempDir::new("migrate");
    let db = dir.path().join("metastore.db");

    // 1) A pre-T111 store: the M7+M8 shape, no submission tables, with rows in it.
    {
        let old = build_pre_t111_store(&db).await;
        old.seed(&[
            "INSERT INTO dag (dag_id, name, created_ms) VALUES ('pipe', 'pipe', 7)",
            "INSERT INTO dag_run (run_id, dag_id, state, started_ms) \
             VALUES ('old-run', 'pipe', 'succeeded', 0)",
            "INSERT INTO node_attempt (run_id, node_id, try_number, state) \
             VALUES ('old-run', 'n', 1, 'succeeded')",
            "INSERT INTO output_produced (run_id, node_id, attempt, uri) \
             VALUES ('old-run', 'n', 1, 'blob://old')",
        ])
        .await;
        assert!(
            !old.table_exists("attempt_submitted").await,
            "the pre-T111 store has no attempt_submitted table yet"
        );
        assert!(!old.table_exists("attempt_submitted_input").await);
    }

    // 2) Open the SAME file with the real MetaStore — forward migrations run.
    let upgraded = open_store_at(&db).await;
    for table in ["attempt_submitted", "attempt_submitted_input"] {
        assert_eq!(
            scalar_i64(
                &upgraded,
                &format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'")
            )
            .await,
            1,
            "the forward migration added `{table}` in place"
        );
        assert_eq!(
            scalar_i64(&upgraded, &format!("SELECT count(*) FROM {table}")).await,
            0,
            "`{table}` starts empty on an upgraded store"
        );
    }

    // No existing row was disturbed.
    assert_eq!(
        opt_string(&upgraded, "SELECT name FROM dag WHERE dag_id='pipe'")
            .await
            .as_deref(),
        Some("pipe")
    );
    assert_eq!(
        opt_string(&upgraded, "SELECT state FROM dag_run WHERE run_id='old-run'")
            .await
            .as_deref(),
        Some("succeeded")
    );
    assert_eq!(
        opt_string(
            &upgraded,
            "SELECT uri FROM output_produced WHERE run_id='old-run'"
        )
        .await
        .as_deref(),
        Some("blob://old")
    );

    // 3) A FRESH store converges on the identical column shape.
    let fresh_dir = TempDir::new("fresh");
    let fresh = open_store_at(&fresh_dir.path().join("metastore.db")).await;
    for table in ["attempt_submitted", "attempt_submitted_input"] {
        let shape = format!(
            "SELECT name||' '||type FROM pragma_table_info('{table}') ORDER BY cid"
        );
        assert_eq!(
            rows_text(&upgraded, &shape).await,
            rows_text(&fresh, &shape).await,
            "an upgraded store and a fresh one converge on the same `{table}` shape"
        );
        assert!(
            !rows_text(&fresh, &shape).await.is_empty(),
            "`{table}` really has columns"
        );
    }

    // 4) And a real run projects into the upgraded store cleanly.
    let base = dir.path();
    write_placed_run(base, "run-new", "sha256:0000");
    let bytes = std::fs::read(base.join("pipe").join("run-new").join("events.jsonl")).unwrap();
    let artifact = fold_stream(&bytes, &[]).unwrap();
    sync_run(&upgraded, &artifact, None)
        .await
        .expect("sync the new run");
    assert_eq!(
        scalar_i64(
            &upgraded,
            "SELECT count(*) FROM attempt_submitted WHERE run_id='run-new'"
        )
        .await,
        3,
        "the upgraded store projects the new run's three submissions"
    );
}

/// Neither new table carries a foreign key — an audit row must outlive garbage
/// collection of anything it references, the same discipline the lineage tables
/// already keep.
#[tokio::test]
async fn the_submission_tables_carry_no_foreign_keys() {
    let dir = TempDir::new("no-fk-pragma");
    let store = open_store_at(&dir.path().join("metastore.db")).await;
    for table in ["attempt_submitted", "attempt_submitted_input"] {
        assert_eq!(
            scalar_i64(
                &store,
                &format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'")
            )
            .await,
            1,
            "`{table}` exists (an absent table would trivially have no keys)"
        );
        assert_eq!(
            scalar_i64(
                &store,
                &format!("SELECT count(*) FROM pragma_foreign_key_list('{table}')")
            )
            .await,
            0,
            "`{table}` declares no foreign key"
        );
    }
}

// === (b) THE CASE THE RECORD EXISTS FOR ====================================

/// A submission with no matching attempt-outcome is a row, is identifiable as
/// submitted-but-never-completed, and is not represented as a failure.
#[tokio::test]
async fn submitted_but_never_completed_is_a_first_class_queryable_state() {
    let dir = TempDir::new("never-completed");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    // The documented "which attempts were submitted but never completed" query.
    let stranded = rows_text(
        &store,
        "SELECT run_id||'/'||node_id||'/'||attempt FROM attempt_submitted \
         WHERE completed = 0 ORDER BY node_id",
    )
    .await;
    assert_eq!(
        stranded,
        vec!["run-1/load/1".to_string()],
        "exactly the attempt that never reported"
    );

    // It is not silently dropped, and not a failure.
    assert_eq!(
        opt_string(
            &store,
            "SELECT coalesce(outcome_state,'<null>') FROM attempt_submitted \
             WHERE run_id='run-1' AND node_id='load'"
        )
        .await
        .as_deref(),
        Some("<null>"),
        "no terminal state is invented for an attempt that never produced one"
    );
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM node_attempt WHERE run_id='run-1' AND node_id='load'"
        )
        .await,
        0,
        "and there is genuinely no attempt row it could have joined to"
    );
}

/// A submission followed by a successful outcome joins to its `node_attempt` row on
/// `(run_id, node_id, try_number)`.
#[tokio::test]
async fn a_completed_submission_joins_to_its_node_attempt_row() {
    let dir = TempDir::new("join");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    let joined = rows_text(
        &store,
        "SELECT s.node_id||'/'||s.attempt||'/'||a.state \
         FROM attempt_submitted s \
         JOIN node_attempt a \
           ON a.run_id = s.run_id AND a.node_id = s.node_id AND a.try_number = s.attempt \
         ORDER BY s.node_id",
    )
    .await;
    assert_eq!(
        joined,
        vec!["extract/1/succeeded".to_string(), "source/1/succeeded".to_string()],
        "the completed submissions join on (run_id, node_id, try_number)"
    );
    assert_eq!(
        opt_string(
            &store,
            "SELECT outcome_state FROM attempt_submitted \
             WHERE run_id='run-1' AND node_id='extract'"
        )
        .await
        .as_deref(),
        Some("succeeded"),
        "and the outcome is carried on the audit row itself"
    );
}

// === (c) ORDERING AND SHAPE ================================================

/// The projected inputs preserve **positional order**, and a query recovers the
/// reference at position *k*.
#[tokio::test]
async fn positional_input_order_is_preserved_and_queryable() {
    let dir = TempDir::new("order");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    let ordered = rows_text(
        &store,
        "SELECT uri FROM attempt_submitted_input \
         WHERE run_id='run-1' AND node_id='load' AND attempt=1 ORDER BY position",
    )
    .await;
    assert_eq!(
        ordered,
        vec![
            "blob://zeta".to_string(),
            "blob://alpha".to_string(),
            "blob://mid".to_string()
        ],
        "positional order is preserved verbatim — not sorted by uri"
    );
    assert_eq!(
        opt_string(
            &store,
            "SELECT uri FROM attempt_submitted_input \
             WHERE run_id='run-1' AND node_id='load' AND attempt=1 AND position=1"
        )
        .await
        .as_deref(),
        Some("blob://alpha"),
        "the reference at position k is directly queryable"
    );
    assert_eq!(
        opt_string(
            &store,
            "SELECT coalesce(content_hash,'<null>') FROM attempt_submitted_input \
             WHERE run_id='run-1' AND node_id='load' AND position=2"
        )
        .await
        .as_deref(),
        Some("<null>"),
        "a reference whose producer supplied no hash keeps a NULL hash"
    );
}

/// A consume-nothing source records **zero** inputs, and that is distinguishable
/// from a submission whose inputs are unknown.
#[tokio::test]
async fn zero_inputs_is_distinguishable_from_unknown_inputs() {
    let dir = TempDir::new("zero-vs-unknown");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");

    // Append a hand-built submission record that omits `inputs` entirely — the
    // "unknown" shape. The published writer always emits the array, so this is the
    // only way to produce it, and the projection must not read it as zero.
    let events = base.join("pipe").join("run-1").join("events.jsonl");
    let mut bytes = std::fs::read(&events).expect("read the stream");
    bytes.extend_from_slice(
        br#"{"schema_version":"dagr.event-stream@1","run_id":"run-1","seq":9001,"wall":"2026-07-31T00:00:00.000Z","offset_ns":200,"kind":"attempt-submitted","node":"mystery","attempt":1}"#,
    );
    bytes.push(b'\n');
    std::fs::write(&events, &bytes).expect("rewrite the stream");

    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    assert_eq!(
        scalar_i64(
            &store,
            "SELECT input_count FROM attempt_submitted WHERE node_id='source'"
        )
        .await,
        0,
        "a consume-nothing source is KNOWN to have zero inputs"
    );
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM attempt_submitted_input WHERE node_id='source'"
        )
        .await,
        0,
        "and it has no child input rows"
    );
    assert_eq!(
        opt_string(
            &store,
            "SELECT coalesce(CAST(input_count AS TEXT),'<null>') FROM attempt_submitted \
             WHERE node_id='mystery'"
        )
        .await
        .as_deref(),
        Some("<null>"),
        "an unknown input list is NULL — never silently 0"
    );
    // The distinguishing query the audit needs.
    assert_eq!(
        rows_text(
            &store,
            "SELECT node_id FROM attempt_submitted WHERE input_count = 0 ORDER BY node_id"
        )
        .await,
        vec!["source".to_string()],
        "`input_count = 0` selects the consume-nothing source and nothing else"
    );
}

/// Intended and observed target identity are **separate columns**, both present,
/// and they may differ without either being lost.
#[tokio::test]
async fn intended_and_observed_target_identity_are_separate_columns() {
    let dir = TempDir::new("intent-vs-reality");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    let where_extract = "FROM attempt_submitted WHERE run_id='run-1' AND node_id='extract'";
    for (col, expected) in [
        ("target_name", "dagr-extract-1"),
        ("observed_name", "dagr-extract-1-adopted"),
        ("observed_uid", "uid-9f"),
        ("observed_host", "node-7"),
        ("executor", "k8s"),
        ("structural_fingerprint", "blake3:aaa"),
        ("policy_hash", "blake3:bbb"),
        ("tool_version", "dagr 0.0.0"),
        ("image_digest", "sha256:image"),
    ] {
        assert_eq!(
            opt_string(&store, &format!("SELECT {col} {where_extract}"))
                .await
                .as_deref(),
            Some(expected),
            "attempt_submitted.{col}"
        );
    }
    assert_eq!(
        scalar_i64(&store, &format!("SELECT count(*) {where_extract}")).await,
        1,
        "the write-ahead record and the additive observed record are ONE row"
    );
    assert_eq!(
        scalar_i64(
            &store,
            &format!("SELECT submitted_at_offset_ns {where_extract}")
        )
        .await,
        60,
        "stamped at the write-ahead point, not at the observation"
    );
    // A submission the platform never acknowledged has intent and no reality.
    assert_eq!(
        opt_string(
            &store,
            "SELECT coalesce(observed_name,'<null>') FROM attempt_submitted \
             WHERE run_id='run-1' AND node_id='load'"
        )
        .await
        .as_deref(),
        Some("<null>"),
        "intent without reality is representable"
    );
}

// === (d) THE AUDIT QUERIES ACTUALLY ANSWER THE QUESTION ====================

/// "What was attempt N of node X launched with?" — answered by one documented
/// query, content hashes included.
#[tokio::test]
async fn the_launch_query_returns_exactly_what_an_attempt_was_launched_with() {
    let dir = TempDir::new("launch-query");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    let launched = rows_text(
        &store,
        "SELECT i.position||' '||i.uri||' '||coalesce(i.content_hash,'-')||' '||s.image_digest \
         FROM attempt_submitted s \
         JOIN attempt_submitted_input i \
           ON i.run_id = s.run_id AND i.node_id = s.node_id AND i.attempt = s.attempt \
         WHERE s.run_id='run-1' AND s.node_id='extract' AND s.attempt=1 \
         ORDER BY i.position",
    )
    .await;
    assert_eq!(
        launched,
        vec![
            "0 blob://in-0 sha256:0000 sha256:image".to_string(),
            "1 blob://in-1 sha256:1111 sha256:image".to_string(),
        ],
        "the launch query returns the ordered references with their content hashes"
    );
}

/// An attempt whose recorded inputs differ from its submitted inputs is surfaced by
/// a documented join — the divergence T106's shard records make detectable.
#[tokio::test]
async fn the_divergence_query_surfaces_a_submitted_versus_read_mismatch() {
    // `extract` reports having read a DIFFERENT content hash at blob://in-0 than
    // the one it was submitted with — an out-of-band overwrite between submission
    // and read.
    let dir = TempDir::new("divergence");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:OVERWRITTEN");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    let diverged = rows_text(
        &store,
        "SELECT s.node_id||' '||s.attempt||' '||s.uri||' submitted='||coalesce(s.content_hash,'-') \
                ||' read='||coalesce(c.content_hash,'-') \
         FROM attempt_submitted_input s \
         JOIN input_consumed c \
           ON c.run_id = s.run_id AND c.node_id = s.node_id \
          AND c.attempt = s.attempt AND c.uri = s.uri \
         WHERE c.content_hash IS NOT s.content_hash \
         ORDER BY s.node_id, s.position",
    )
    .await;
    assert_eq!(
        diverged,
        vec![
            "extract 1 blob://in-0 submitted=sha256:0000 read=sha256:OVERWRITTEN".to_string()
        ],
        "exactly the one reference whose content hash moved under the attempt"
    );

    // The agreeing reference is NOT reported.
    let agreeing = scalar_i64(
        &store,
        "SELECT count(*) FROM attempt_submitted_input s \
         JOIN input_consumed c \
           ON c.run_id = s.run_id AND c.node_id = s.node_id \
          AND c.attempt = s.attempt AND c.uri = s.uri \
         WHERE c.content_hash IS NOT s.content_hash AND s.uri='blob://in-1'",
    )
    .await;
    assert_eq!(agreeing, 0, "a reference that matches is not a divergence");
}

/// The audit rows join to the existing lineage tables, and an audit row still
/// resolves after its referent has been garbage-collected — the no-foreign-key
/// property the lineage tables already guarantee.
#[tokio::test]
async fn audit_rows_join_to_lineage_and_survive_a_collected_referent() {
    let dir = TempDir::new("gc");
    let base = dir.path();
    write_placed_run(base, "run-1", "sha256:0000");
    let store = open_store_at(&base.join("metastore.db")).await;
    sync_run_store(&store, base).await.expect("sync");

    // "Which runs were launched against a given uri" — the submission side, joined
    // to the consumed-lineage side by value.
    let touched = rows_text(
        &store,
        "SELECT s.run_id||' '||s.node_id||' consumed='||CAST(count(c.uri) AS TEXT) \
         FROM attempt_submitted_input s \
         LEFT JOIN input_consumed c ON c.uri = s.uri AND c.run_id = s.run_id \
         WHERE s.uri = 'blob://in-0' \
         GROUP BY s.run_id, s.node_id",
    )
    .await;
    assert_eq!(
        touched,
        vec!["run-1 extract consumed=1".to_string()],
        "the submission joins to the consumed-lineage row by value on uri"
    );

    // Garbage-collect the referent: drop the asset identity row AND the lineage
    // rows that named it. The audit row is untouched — no foreign key broke.
    store
        .with_write_txn(|c| {
            Box::pin(async move {
                c.execute("DELETE FROM asset", ()).await?;
                c.execute("DELETE FROM input_consumed", ()).await?;
                c.execute("DELETE FROM output_produced", ()).await?;
                c.execute("DELETE FROM node_attempt WHERE node_id='extract'", ())
                    .await
                    .map(|_| ())
            })
        })
        .await
        .expect("collect the referents (no FK blocks it)");

    assert_eq!(
        rows_text(
            &store,
            "SELECT uri FROM attempt_submitted_input \
             WHERE run_id='run-1' AND node_id='extract' ORDER BY position"
        )
        .await,
        vec!["blob://in-0".to_string(), "blob://in-1".to_string()],
        "the audit row still resolves after its referent was collected"
    );
    assert_eq!(
        opt_string(
            &store,
            "SELECT target_name FROM attempt_submitted WHERE run_id='run-1' AND node_id='extract'"
        )
        .await
        .as_deref(),
        Some("dagr-extract-1"),
        "and so does the submission row whose attempt row is gone"
    );
}

// === A pre-T111 store, built from raw DDL ==================================

/// A store at the pre-T111 (M7 + M8 lineage) shape, laid down with **raw** DDL, so
/// the migration test proves the real forward migration adds the submission tables
/// in place over a genuine older file.
struct PreT111Store {
    conn: libsql::Connection,
    _db: libsql::Database,
}

impl PreT111Store {
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

async fn build_pre_t111_store(db: &Path) -> PreT111Store {
    let dbh = libsql::Builder::new_local(db)
        .build()
        .await
        .expect("build the pre-T111 db");
    let conn = dbh.connect().expect("connect");
    let ddl = [
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
         satisfied_from_run TEXT, originating_node TEXT, content_hash TEXT, size_bytes INTEGER, \
         scheme TEXT, produced_at_offset_ns INTEGER, UNIQUE (run_id, node_id, try_number))",
        "CREATE TABLE node_terminal (run_id TEXT NOT NULL, node_id TEXT NOT NULL, \
         state TEXT NOT NULL, finished_ms INTEGER, PRIMARY KEY (run_id, node_id))",
        "CREATE TABLE output_produced (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, \
         node_id TEXT NOT NULL, attempt INTEGER NOT NULL, uri TEXT NOT NULL, content_hash TEXT, \
         size_bytes INTEGER, kind TEXT, produced_at_offset_ns INTEGER, originating_run TEXT, \
         UNIQUE (run_id, node_id, attempt, uri, content_hash))",
        "CREATE TABLE input_consumed (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, \
         node_id TEXT NOT NULL, attempt INTEGER NOT NULL, uri TEXT NOT NULL, content_hash TEXT, \
         UNIQUE (run_id, node_id, attempt, uri, content_hash))",
        "CREATE TABLE asset (uri TEXT PRIMARY KEY, extra TEXT)",
    ];
    for stmt in ddl {
        conn.execute(stmt, ()).await.expect("pre-T111 ddl");
    }
    PreT111Store { conn, _db: dbh }
}
