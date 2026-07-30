//! T97: the live sink's **queue depth** and **copy volume**. Written first, TDD.
//!
//! Two properties of [`MetastoreSink`] that the guaranteed-write design implies but
//! nothing pinned:
//!
//! * **Queue depth.** The sink hands work to its worker over an *unbounded*
//!   `std::sync::mpsc` channel, but the producer blocks on a **rendezvous** reply
//!   channel before it can send again — so the queue can never hold more than one
//!   request, and a slow writer applies backpressure to `append_line` instead of
//!   growing a queue. `async-bounded-channel` is satisfied structurally, by a
//!   stronger mechanism than a capacity.
//! * **Copy volume.** The sink used to hand the worker `self.buffer.clone()` — the
//!   *whole accumulated stream* — on **every** appended line, which is O(n) bytes
//!   per event and O(n²) over a run. The worker now owns the accumulated buffer and
//!   each request carries only the newly appended bytes, so the total volume copied
//!   across a run is exactly the stream's length. These tests measure that volume;
//!   they fail loudly against the per-append full-buffer clone.
//!
//! Both are measured through instrumentation on the sink itself
//! ([`MetastoreSink::peak_queue_depth`], [`MetastoreSink::bytes_handed_to_worker`]),
//! not inferred, and each test also asserts the guaranteed-write contract still
//! holds — every event reaches the store — so a "cheaper" sink that dropped writes
//! could not pass.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState,
};

use dagr_metastore::MetaStore;
use dagr_metastore::MetastoreSink;
use dagr_metastore::store::OpenMode;

// === Test scaffolding ======================================================

/// A private per-test temp directory, removed on drop.
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
            "dagr-metastore-t97-{tag}-{}-{nanos}-{n}",
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

#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// An in-memory capturing sink, so a test can produce the exact canonical bytes a
/// run emits and then replay them through the sink under test one line at a time.
#[derive(Clone, Default)]
struct CaptureSink {
    bytes: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().expect("capture not poisoned").clone()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.bytes
            .lock()
            .expect("capture not poisoned")
            .extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn store_path(dir: &Path) -> PathBuf {
    dir.join("metastore.db")
}

/// How many nodes the sustained-load run emits. Large enough that the difference
/// between a linear and a quadratic copy volume is orders of magnitude, small
/// enough that the run stays a fast test (each event is its own committed write
/// transaction).
const NODES: usize = 24;

/// The canonical bytes of a run of [`NODES`] one-attempt nodes, each succeeding.
fn canonical_run_bytes() -> Vec<u8> {
    let capture = CaptureSink::default();
    let mut w = EventStreamWriter::new(
        capture.clone(),
        TickClock::default(),
        RunId::from_operator("t97-load-run"),
        "t97-load",
    )
    .with_wall_clock(|| "2026-07-30T00:00:00.000Z".to_string());
    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-30".to_string());
    w.run_started(RunStartedHeader {
        pipeline: "t97-load".to_string(),
        fingerprint_structural: Some("blake3:aaa".to_string()),
        fingerprint_policy: Some("blake3:bbb".to_string()),
        fingerprint_algorithm_version: 1,
        parameters: params,
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    })
    .expect("run-started");
    for i in 0..NODES {
        let node = format!("node-{i:02}");
        w.node_ready(&node).expect("ready");
        w.node_admitted(&node).expect("admitted");
        w.attempt_started(&node, 1).expect("started");
        w.attempt_succeeded(&node, 1).expect("succeeded");
        w.attempt_outcome(AttemptOutcomeRecord {
            node: node.clone(),
            attempt: 1,
            status: "succeeded".into(),
            worker: Some("compute#1".into()),
            metrics: Some(serde_json::json!({ "rows": 10 })),
            ..AttemptOutcomeRecord::default()
        })
        .expect("outcome");
        w.node_terminal(&node, TerminalState::Succeeded)
            .expect("terminal");
    }
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("finish");
    capture.bytes()
}

/// Split canonical JSONL bytes into newline-terminated lines, exactly as the writer
/// hands them to a sink.
fn lines_of(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push(bytes[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < bytes.len() {
        out.push(bytes[start..].to_vec());
    }
    out
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

async fn open_store(dir: &Path) -> MetaStore {
    MetaStore::open(OpenMode::LocalFile(store_path(dir)))
        .await
        .expect("open the metastore for reads")
}

// A shared current-thread runtime for the read-side assertions.
static RT: once_lock::Lazy<tokio::runtime::Runtime> = once_lock::Lazy::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a read runtime")
});

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

// === The tests =============================================================

/// **Test-plan scenario: the live sink's queue depth is bounded under sustained
/// write load, and a slow writer applies backpressure.**
///
/// The channel carries at most one outstanding request at any moment, because
/// `append_line` blocks on a zero-capacity reply channel until the worker has
/// **committed** the write. Backpressure is asserted directly rather than by
/// timing: the moment `append_line` returns, the row is already readable through a
/// *separate* connection — the producer physically cannot outrun the worker.
#[test]
fn the_projection_queue_holds_at_most_one_request_and_backpressures_the_writer() {
    let dir = TempDir::new("queue");
    let bytes = canonical_run_bytes();
    let lines = lines_of(&bytes);
    let mut sink = MetastoreSink::open(store_path(dir.path()), Some("events.jsonl".to_string()))
        .expect("open the live sink");

    // The very first append must be visible before the second one starts: this is
    // backpressure, stated as a fact about ordering rather than about timing.
    sink.append_line(&lines[0]).expect("run-started projects");
    let reader = RT.block_on(open_store(dir.path()));
    assert_eq!(
        opt_string(&reader, "SELECT state FROM dag_run LIMIT 1").as_deref(),
        Some("running"),
        "append_line returned before the write was committed — the sink is not \
         applying backpressure"
    );
    drop(reader);

    for line in &lines[1..] {
        sink.append_line(line).expect("append projects");
    }
    sink.flush().expect("final flush");

    assert_eq!(
        sink.peak_queue_depth(),
        1,
        "the sink's request queue peaked at {} — the producer blocks on a \
         rendezvous reply, so it can never have more than one request outstanding",
        sink.peak_queue_depth()
    );

    // The guaranteed-write contract is unchanged: every attempt reached the store.
    drop(sink);
    let store = RT.block_on(open_store(dir.path()));
    assert_eq!(
        scalar_i64(&store, "SELECT COUNT(*) FROM node_attempt"),
        i64::try_from(NODES).expect("node count fits an i64"),
        "every attempt in the stream reached the store"
    );
    assert_eq!(
        opt_string(&store, "SELECT state FROM dag_run LIMIT 1").as_deref(),
        Some("succeeded")
    );
}

/// **Test-plan scenario: the total bytes copied grow linearly, not quadratically,
/// in the number of appended lines.**
///
/// Each request now carries only the bytes appended since the previous one, so the
/// volume handed to the worker over a whole run is **exactly the stream's length**.
/// Against the old per-append full-buffer clone the volume is the sum of the
/// stream's prefixes — for this run roughly `NODES * 3` times larger — so this
/// assertion fails against that code rather than merely passing more elegantly.
#[test]
fn the_bytes_handed_to_the_worker_grow_linearly_in_the_stream_length() {
    let dir = TempDir::new("volume");
    let bytes = canonical_run_bytes();
    let lines = lines_of(&bytes);
    let line_count = lines.len();
    assert!(
        line_count > 100,
        "the load run must be long enough for the quadratic term to dominate; got \
         {line_count} lines"
    );

    let mut sink = MetastoreSink::open(store_path(dir.path()), Some("events.jsonl".to_string()))
        .expect("open the live sink");
    for line in &lines {
        sink.append_line(line).expect("append projects");
    }
    sink.flush().expect("final flush");

    let total = u64::try_from(bytes.len()).expect("stream length fits a u64");
    let handed = sink.bytes_handed_to_worker();

    // The quadratic figure the old implementation produced, for the failure message
    // (and to prove the two are not accidentally close at this size).
    let quadratic: u64 = lines
        .iter()
        .scan(0u64, |acc, l| {
            *acc += u64::try_from(l.len()).expect("line length fits a u64");
            Some(*acc)
        })
        .sum();
    assert!(
        quadratic > total * 10,
        "the fixture is too small to distinguish linear from quadratic: linear \
         {total}, quadratic {quadratic}"
    );

    assert_eq!(
        handed, total,
        "the worker was handed {handed} bytes for a {total}-byte stream; a linear \
         sink hands over exactly the stream (the per-append full-buffer clone would \
         hand over {quadratic})"
    );

    // Still guaranteed, not best-effort: the linear path writes everything.
    drop(sink);
    let store = RT.block_on(open_store(dir.path()));
    assert_eq!(
        scalar_i64(&store, "SELECT COUNT(*) FROM node_attempt"),
        i64::try_from(NODES).expect("node count fits an i64"),
    );
}
