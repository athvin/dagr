//! T86 acceptance tests: the guaranteed live metastore tee sink, driven through
//! the **real** `drive` loop via `RunnableFlow::run`.
//!
//! These are the ticket's Test-plan scenarios that live at the driver boundary:
//! the tee does not perturb the `events.jsonl` stream, a metastore `SinkFault`
//! flows through the driver's shutdown exit-code precedence (the distinct
//! sink-failure code, NOT swallowed), and the toggle default-off ⇒ no `libsql`
//! activity / no behavior change. They are written FIRST and failing before the
//! `metastore_tee` wiring lands.
//!
//! The whole file is gated behind the default-off `metastore` feature: a
//! `--no-default-features` build (which omits the wiring entirely) simply does not
//! compile or run it.
//!
//! Determinism/portability: each test uses a PRIVATE temp base under the OS temp
//! dir (disjoint per process + nanos), the registry's deterministic `TickClock`,
//! and no wall-clock sleeps. macOS + ubuntu are in CI.
#![cfg(feature = "metastore")]

use dagr_artifact::event_stream::read_records;
use dagr_cli::driver::{RunConfig, ShutdownExit};
use dagr_cli::metastore_tee::{RunSink, build_run_sink, stream_path};
use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::run_store::TickClock;
use dagr_core::TaskError;
use dagr_core::context::RunContext;
use dagr_core::task::Task;

use dagr_metastore::MetaStore;
use dagr_metastore::store::OpenMode;

// === Fixtures ==============================================================

/// A source producing a fixed number.
struct Count {
    up_to: u64,
}
impl Task for Count {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.up_to)
    }
}

/// A sink doubling its input.
struct Double;
impl Task for Double {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

/// Build a fresh two-node flow (count → double).
fn two_node_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let counted = flow.register_source("count", Count { up_to: 21 });
    let _doubled = flow.register::<Double, _>("double", Double, counted);
    flow
}

/// A private, disjoint temp base under the OS temp dir.
fn temp_base(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "dagr-t86-tee-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Normalize a stream's per-record **informational** `wall` stamp to a fixed value
/// so two runs at different wall times compare on everything the event stream
/// actually carries (the `wall` field is informational and deliberately excluded
/// from byte-identity — durations use the authoritative `offset_ns`). Every other
/// byte — including the monotonic `offset_ns`, driven by the deterministic
/// `TickClock` — is left exactly as written, so this proves the tee changes no
/// meaningful byte of the stream.
fn normalize_wall(bytes: &[u8]) -> Vec<u8> {
    let text = String::from_utf8(bytes.to_vec()).expect("utf8 stream");
    let mut out = String::new();
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches('\n');
        if trimmed.is_empty() {
            out.push_str(line);
            continue;
        }
        let mut record: serde_json::Value = serde_json::from_str(trimmed).expect("record parses");
        if let Some(obj) = record.as_object_mut() {
            if obj.contains_key("wall") {
                obj.insert("wall".to_string(), serde_json::Value::from("<wall>"));
            }
        }
        out.push_str(&record.to_string());
        if line.ends_with('\n') {
            out.push('\n');
        }
    }
    out.into_bytes()
}

/// A blocking read of a scalar `i64` from the store.
fn scalar_i64(store_path: &std::path::Path, sql: &str) -> i64 {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("read runtime");
    rt.block_on(async {
        let store = MetaStore::open(OpenMode::LocalFile(store_path.to_path_buf()))
            .await
            .expect("open store for reads");
        let mut rows = store.connection().query(sql, ()).await.expect("query");
        let row = rows.next().await.expect("row").expect("one row");
        row.get::<i64>(0).expect("i64")
    })
}

/// A blocking read of an optional string.
fn opt_string(store_path: &std::path::Path, sql: &str) -> Option<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("read runtime");
    rt.block_on(async {
        let store = MetaStore::open(OpenMode::LocalFile(store_path.to_path_buf()))
            .await
            .expect("open store for reads");
        let mut rows = store.connection().query(sql, ()).await.expect("query");
        let row = rows.next().await.expect("row")?;
        row.get::<String>(0).ok()
    })
}

// === Byte-identical stream (the tee does not perturb) ======================

/// A run driven with the metastore toggle ON writes an `events.jsonl` byte-identical
/// to the same run with the toggle OFF (the tee's metastore path never touches the
/// event stream), AND the ON run produces a populated metastore while the OFF run
/// creates none.
#[test]
fn toggle_on_stream_is_byte_identical_to_toggle_off_and_metastore_only_appears_on() {
    // --- OFF run (default): plain FileSink, no metastore.
    let off_base = temp_base("off");
    let off_base_str = off_base.to_string_lossy().to_string();
    let off_run_id = "run-off";
    let off_stream = stream_path(&off_base_str, "pipe", off_run_id);
    let off_sink = build_run_sink(&off_stream, &off_base_str, &[]).expect("off sink builds");
    assert!(
        matches!(off_sink, RunSink::File(_)),
        "the default (no toggle) run sink is a plain FileSink"
    );
    let off_config = RunConfig::new(&off_base_str).run_id(off_run_id);
    let _off = two_node_flow()
        .run("pipe", &off_config, off_sink, TickClock::default())
        .expect("off run assembles + runs");
    let off_bytes = std::fs::read(&off_stream).expect("off stream written");
    // The OFF run created NO metastore db (no libsql activity).
    assert!(
        !off_base.join("metastore.db").exists(),
        "toggle-off ⇒ no metastore.db is created (no libsql activity)"
    );

    // --- ON run: tee FileSink + MetastoreSink. Same pipeline, same run id, same
    // deterministic clock ⇒ same bytes.
    let on_base = temp_base("on");
    let on_base_str = on_base.to_string_lossy().to_string();
    let on_run_id = "run-off"; // same id so the stream bytes match byte-for-byte
    let on_stream = stream_path(&on_base_str, "pipe", on_run_id);
    let argv = vec![std::ffi::OsString::from("--dagr.metastore")];
    let on_sink = build_run_sink(&on_stream, &on_base_str, &argv).expect("on sink builds");
    assert!(
        matches!(on_sink, RunSink::Tee { .. }),
        "with the toggle on, the run sink is a tee of FileSink + MetastoreSink"
    );
    let on_config = RunConfig::new(&on_base_str).run_id(on_run_id);
    let _on = two_node_flow()
        .run("pipe", &on_config, on_sink, TickClock::default())
        .expect("on run assembles + runs");
    let on_bytes = std::fs::read(&on_stream).expect("on stream written");

    // Compare with the informational `wall` stamp normalized (it is excluded from
    // byte-identity by design; the authoritative `offset_ns` — driven by the
    // deterministic TickClock — IS compared verbatim). Every meaningful byte is
    // identical: the tee's metastore path only READS the bytes, never rewrites them.
    assert_eq!(
        normalize_wall(&off_bytes),
        normalize_wall(&on_bytes),
        "the tee does not perturb the event stream: ON and OFF bytes are identical"
    );

    // The ON run populated the metastore: a terminal dag_run + both nodes' terminals.
    let store = on_base.join("metastore.db");
    assert!(store.exists(), "toggle-on ⇒ the metastore.db is created");
    let state = opt_string(&store, "SELECT state FROM dag_run WHERE run_id='run-off'");
    assert_eq!(
        state.as_deref(),
        Some("succeeded"),
        "the finished live run finalized dag_run to succeeded"
    );
    let terminals = scalar_i64(
        &store,
        "SELECT count(*) FROM node_terminal WHERE run_id='run-off'",
    );
    assert_eq!(terminals, 2, "both nodes have a node_terminal row");

    let _ = std::fs::remove_dir_all(&off_base);
    let _ = std::fs::remove_dir_all(&on_base);
}

// === Guaranteed: a metastore SinkFault ⇒ the sink-failure exit ============

/// A metastore write failure (an unwritable index) surfaces through the driver's
/// shutdown exit-code precedence as the distinct sink-failure selection — the
/// failure is NOT swallowed. We inject the failure by holding the store's single
/// WAL write lock from a competing connection while the run projects, with the
/// sink's store pinned to a one-attempt retry cap so the contention is a hard fault.
#[test]
fn a_metastore_write_failure_surfaces_as_the_sink_failure_exit() {
    use dagr_metastore::MetastoreSink;
    use dagr_metastore::store::RetryPolicy;

    let base = temp_base("fault");
    let base_str = base.to_string_lossy().to_string();
    std::fs::create_dir_all(&base).expect("mk base");
    let store_path = base.join("metastore.db");
    let run_id = "run-fault";
    let stream = stream_path(&base_str, "pipe", run_id);

    // Open the live sink with a ONE-attempt cap so a held write lock is a hard fault.
    let one_shot = RetryPolicy {
        max_attempts: 1,
        base_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(1),
    };
    let events_path = stream.to_string_lossy().into_owned();
    let metastore = MetastoreSink::open_with_retry(store_path.clone(), Some(events_path), one_shot)
        .expect("open the live sink");
    let file = dagr_cli::run_store::FileSink::create(&stream).expect("open events.jsonl");
    let sink = RunSink::Tee { file, metastore };

    // A competing writer holds the WAL write lock for the whole run, so every
    // projection the live sink attempts hits SQLITE_BUSY past its one-attempt cap.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("contender runtime");
    let contender = rt.block_on(async {
        let s = MetaStore::open(OpenMode::LocalFile(store_path.clone()))
            .await
            .expect("open contender");
        s.connection()
            .execute("BEGIN IMMEDIATE", ())
            .await
            .expect("contender takes the write lock");
        s
    });

    let config = RunConfig::new(&base_str).run_id(run_id);
    let report = two_node_flow()
        .run("pipe", &config, sink, TickClock::default())
        .expect("the flow assembles (the failure is a SINK fault, not assembly)");

    // The driver's shutdown exit selection is SinkFailure: a metastore write fault is
    // a sink fault (event stream unwritable), reported through the bounded final
    // flush — NOT swallowed, NOT reported as success.
    assert_eq!(
        report.driver_report().shutdown_exit,
        ShutdownExit::SinkFailure,
        "a metastore write failure surfaces as the distinct sink-failure exit (guaranteed)"
    );

    // Release the lock so the temp dir cleans up.
    rt.block_on(async {
        let _ = contender.connection().execute("ROLLBACK", ()).await;
    });
    let _ = std::fs::remove_dir_all(&base);
}

// === Toggle default-off (no libsql activity) ===============================

/// With no `--dagr.metastore` flag and no `DAGR_METASTORE` env, `build_run_sink`
/// returns a plain `FileSink` and opens no store — no `libsql` activity.
#[test]
#[allow(
    unsafe_code,
    reason = "std::env::remove_var/set_var are unsafe fns in edition 2024; the \
              toggle resolver reads the REAL DAGR_METASTORE name, so asserting the \
              default-off path requires guaranteeing it unset"
)]
fn default_off_builds_a_plain_file_sink_and_opens_no_store() {
    // Ensure DAGR_METASTORE is not set for this assertion (unique-name isolation is
    // not possible here — the resolver reads the real name — so remove it). A
    // process-global lock covers the whole unset → build → restore window so the
    // mutation cannot overlap another test's read of the same name.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    let base = temp_base("default");
    let base_str = base.to_string_lossy().to_string();
    let stream = stream_path(&base_str, "pipe", "run-default");

    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let prior = std::env::var_os("DAGR_METASTORE");
    // SAFETY: mutating the process environment races a concurrent `getenv` (the
    // update can reallocate `environ` under a reader). `_guard` is held across the
    // whole window, and this is the only test in the suite that touches the
    // variable; the prior value is restored so the process is left as it was found.
    unsafe { std::env::remove_var("DAGR_METASTORE") };
    let sink = build_run_sink(&stream, &base_str, &[]).expect("default sink builds");
    if let Some(v) = prior {
        // SAFETY: still inside the same `_guard` window as the removal above.
        unsafe { std::env::set_var("DAGR_METASTORE", v) };
    }

    assert!(
        matches!(sink, RunSink::File(_)),
        "default-off ⇒ a plain FileSink (no metastore, no libsql)"
    );
    assert!(
        !base.join("metastore.db").exists(),
        "default-off opens no store"
    );
    // The stream file was created (the FileSink opened it).
    assert!(stream.exists(), "the events.jsonl sink was opened");

    let _ = std::fs::remove_dir_all(&base);
}

// === Live rows are observable mid-run (through the tee) =====================

/// Driving a real run through the tee leaves a metastore whose `dag_run` finalized
/// to its terminal state and whose `node_attempt` rows equal the folded stream's —
/// i.e. the live projection is the same guaranteed projection `sync` would produce.
#[test]
fn live_projection_through_the_tee_matches_the_folded_stream() {
    use dagr_artifact::fold::fold_stream;

    let base = temp_base("live");
    let base_str = base.to_string_lossy().to_string();
    let run_id = "run-live";
    let stream = stream_path(&base_str, "pipe", run_id);
    let argv = vec![std::ffi::OsString::from("--dagr.metastore=true")];
    let sink = build_run_sink(&stream, &base_str, &argv).expect("tee builds");

    let config = RunConfig::new(&base_str).run_id(run_id);
    let _report = two_node_flow()
        .run("pipe", &config, sink, TickClock::default())
        .expect("live run");

    let bytes = std::fs::read(&stream).expect("stream written");
    // The stream carries one run-started + one run-finished bookend.
    let records = read_records(&bytes).expect("stream parses");
    let started = records
        .records
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("run-started"))
        .count();
    let finished = records
        .records
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("run-finished"))
        .count();
    assert_eq!((started, finished), (1, 1), "one run bookend pair");

    // The live-produced attempt rows equal the folded stream's attempt count.
    let artifact = fold_stream(&bytes, &[]).expect("fold the finished stream");
    let store = base.join("metastore.db");
    let attempt_rows = scalar_i64(
        &store,
        "SELECT count(*) FROM node_attempt WHERE run_id='run-live'",
    );
    assert_eq!(
        attempt_rows,
        i64::try_from(artifact.attempts().len()).unwrap(),
        "live-produced node_attempt rows equal the folded stream's attempts (live == reconcile)"
    );

    let _ = std::fs::remove_dir_all(&base);
}
