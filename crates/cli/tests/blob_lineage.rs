#![cfg(all(feature = "blob", feature = "metastore"))]
//! T104 acceptance test: M8 lineage rows carry the blob URI and content hash
//! through the **existing** projection, with no mapping change.
//!
//! The point of the ticket is that the bridge gets lineage for free: a blob
//! reference is just a URI and its digest is just a hash, so the T90/T91
//! `output_produced` / `input_consumed` projection carries them the way it
//! carries any other reference. This test proves that end to end — real payload →
//! real store → real event-stream writer → real fold → real metastore sync — and
//! is the reason `crates/metastore/src/mapping.rs` is untouched by this ticket.
//!
//! It needs BOTH default-off features (`blob` for the bridge, `metastore` for the
//! index), so a bare `cargo test --workspace` compiles it to nothing; CI runs it
//! under `--features blob,metastore`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, ConsumedInput, EventSink, EventStreamWriter, MonotonicClock,
    OutputProducedRecord, RunId, RunOutcome, RunStartedHeader, TerminalState,
    record_consumed_inputs, record_durable_reference, record_durable_reference_meta,
};
use dagr_blob::LocalFsBlob;
use dagr_cli::blob_bridge::{Blob, wire_reference_meta};
use dagr_core::assembly::DurableOutput;
use dagr_core::{Payload, StableName};
use dagr_metastore::MetaStore;
use dagr_metastore::mapping::sync_run_store;
use dagr_metastore::store::OpenMode;

/// The payload the durable stage boundary produces.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Manifest {
    rows: u64,
    label: String,
}

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
            "dagr-cli-t104-lineage-{tag}-{}-{nanos}-{n}",
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

struct FileSink {
    file: std::fs::File,
}

impl FileSink {
    fn create(path: &Path) -> Self {
        std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mk run dir");
        Self {
            file: std::fs::File::create(path).expect("create events.jsonl"),
        }
    }
}

impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        use std::io::Write as _;
        self.file.write_all(line)?;
        self.file.flush()
    }
    fn flush(&mut self) -> std::io::Result<()> {
        use std::io::Write as _;
        self.file.flush()
    }
}

struct ZeroClock;
impl MonotonicClock for ZeroClock {
    fn elapsed_ns(&self) -> u64 {
        0
    }
}

async fn scalar_i64(store: &MetaStore, sql: &str) -> i64 {
    let mut rows = store.connection().query(sql, ()).await.expect("query");
    let row = rows.next().await.expect("row").expect("one row");
    row.get::<i64>(0).expect("i64")
}

async fn scalar_string(store: &MetaStore, sql: &str) -> String {
    let mut rows = store.connection().query(sql, ()).await.expect("query");
    let row = rows.next().await.expect("row").expect("one row");
    row.get::<String>(0).expect("string")
}

/// Emit a real run into `base`: `produce` publishes the blob at `uri`, `consume`
/// reads it back. Exactly the records the driver writes for a durable node — the
/// T89/T90 recording path, unchanged.
fn write_blob_run(
    base: &Path,
    uri: &str,
    hash: &str,
    wire: &dagr_artifact::event_stream::DurableReferenceMeta,
) {
    let events = base.join("pipe").join("run-blob").join("events.jsonl");
    let mut writer = EventStreamWriter::new(
        FileSink::create(&events),
        ZeroClock,
        RunId::from_operator("run-blob".to_string()),
        "pipe",
    )
    .with_wall_clock(|| "2026-07-31T00:00:00.000Z".to_string());
    writer
        .run_started(RunStartedHeader {
            pipeline: "pipe".to_string(),
            fingerprint_structural: None,
            fingerprint_policy: None,
            fingerprint_algorithm_version: 1,
            parameters: BTreeMap::new(),
            data_interval: None,
            captured_env: BTreeMap::new(),
            resumed_from: None,
        })
        .unwrap();

    writer.node_ready("produce").unwrap();
    writer.node_admitted("produce").unwrap();
    writer.attempt_started("produce", 1).unwrap();
    writer.attempt_succeeded("produce", 1).unwrap();
    let mut produce = AttemptOutcomeRecord::new("produce", 1, TerminalState::Succeeded.as_str());
    record_durable_reference(&mut produce, Some(uri.to_string()));
    record_durable_reference_meta(&mut produce, Some(wire.clone()));
    writer.attempt_outcome(produce).unwrap();
    writer
        .output_produced(OutputProducedRecord {
            node: "produce".to_string(),
            attempt: 1,
            uri: uri.to_string(),
            content_hash: wire.content_hash.clone(),
            size_bytes: wire.size_bytes,
            kind: wire.scheme.clone(),
            produced_at_offset_ns: 0,
            originating_run: "run-blob".to_string(),
        })
        .unwrap();
    writer
        .node_terminal("produce", TerminalState::Succeeded)
        .unwrap();

    writer.node_ready("consume").unwrap();
    writer.node_admitted("consume").unwrap();
    writer.attempt_started("consume", 1).unwrap();
    writer.attempt_succeeded("consume", 1).unwrap();
    let mut consume = AttemptOutcomeRecord::new("consume", 1, TerminalState::Succeeded.as_str());
    record_consumed_inputs(
        &mut consume,
        vec![ConsumedInput {
            uri: uri.to_string(),
            content_hash: Some(hash.to_string()),
        }],
    );
    writer.attempt_outcome(consume).unwrap();
    writer
        .node_terminal("consume", TerminalState::Succeeded)
        .unwrap();
    writer.run_finished(RunOutcome::Succeeded).unwrap();
    writer.finish().unwrap();
}

#[test]
fn lineage_rows_carry_the_blob_uri_and_content_hash_through_the_existing_projection() {
    let dir = TempDir::new("rows");
    let base = dir.path();

    // A real blob, produced through the real store and the real bridge.
    let store = LocalFsBlob::open(base.join("blobs"));
    let value = Manifest {
        rows: 4_211,
        label: "shipments/2026-07-31".to_string(),
    };
    let blob = Blob::put(&store, value).expect("put the payload");
    let uri = blob.serialize_reference();
    let hash = blob.content_hash();
    let wire = wire_reference_meta(&blob.durable_reference_meta().expect("metadata"));
    write_blob_run(base, &uri, &hash, &wire);

    // The existing projection, unchanged.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime for the metastore");
    runtime.block_on(async {
        let meta = MetaStore::open(OpenMode::LocalFile(base.join("metastore.db")))
            .await
            .expect("open the metastore");
        let summary = sync_run_store(&meta, base).await.expect("sync");
        assert_eq!(summary.synced, 1, "the run indexed");

        assert_eq!(
            scalar_i64(
                &meta,
                "SELECT count(*) FROM output_produced WHERE run_id='run-blob'"
            )
            .await,
            1,
            "one produced-output lineage row"
        );
        assert_eq!(
            scalar_string(
                &meta,
                "SELECT uri FROM output_produced WHERE run_id='run-blob'"
            )
            .await,
            uri,
            "the row carries the blob URI verbatim"
        );
        assert_eq!(
            scalar_string(
                &meta,
                "SELECT content_hash FROM output_produced WHERE run_id='run-blob'"
            )
            .await,
            hash,
            "and the blob's content hash"
        );
        assert_eq!(
            scalar_string(
                &meta,
                "SELECT uri FROM input_consumed WHERE run_id='run-blob'"
            )
            .await,
            uri,
            "the consuming attempt's input row names the same blob"
        );
        assert_eq!(
            scalar_string(
                &meta,
                "SELECT content_hash FROM input_consumed WHERE run_id='run-blob'"
            )
            .await,
            hash,
            "with the same hash — cross-run reachability over a blob URI"
        );
        assert_eq!(
            scalar_i64(
                &meta,
                &format!("SELECT count(*) FROM asset WHERE uri='{uri}'")
            )
            .await,
            1,
            "the by-value asset identity row is populated on first sight"
        );
    });
}
