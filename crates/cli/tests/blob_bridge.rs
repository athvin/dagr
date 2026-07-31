#![cfg(feature = "blob")]
//! T104 acceptance tests: the blanket `DurableOutput` bridge over `Payload`.
//!
//! The port and its local backend are proven in `crates/blob/tests/blob_port.rs`;
//! this suite proves the **bridge** — the part that hands a remote output content
//! hashes, existence probes, resume rehydration, and M8 lineage rows for free,
//! through machinery that is already tested:
//!
//! * a `Payload` output produced through the bridge round-trips
//!   `serialize_reference` → `rehydrate`;
//! * its `durable_reference_meta` carries a content hash and a size, and those
//!   reach an `attempt-outcome` record through the unchanged T89 path;
//! * the existence probe answers all four `ReferenceExistence` cases;
//! * resume is satisfied-from-prior on an intact blob, refuses `DanglingReference`
//!   on a deleted one, and refuses `MutatedReference` on one overwritten
//!   out-of-band — the existing gates, now reachable through a shipped backend.
//!
//! Written FIRST and failing before the bridge lands. Each test uses a private
//! per-test temp root so the suite is collision-proof under CI parallelism.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, OutputProducedRecord,
    RunId as WireRunId, RunOutcome, RunStartedHeader, TerminalState as WireTerminalState,
    record_durable_reference, record_durable_reference_meta,
};
use dagr_artifact::fold::fold_stream;
use dagr_blob::{BlobKey, BlobStore, LocalFsBlob};
use dagr_cli::blob_bridge::{Blob, reference_existence, wire_reference_meta};
use dagr_core::assembly::NodePolicy;
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::resume::{
    PriorNode, PriorRun, ReferenceExistence, ResumeRefusal, plan_resume, DurableOutput,
};
use dagr_core::task::Task;
use dagr_core::{Payload, RunContext, StableName, TaskError, TerminalState};

// === Fixtures ==============================================================

/// The payload a durable stage boundary produces: a small, derived `Payload` with
/// an author-declared stable name.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Manifest {
    rows: u64,
    label: String,
}

/// A durable source whose output is a blob-backed `Manifest`. The value is written
/// through the store at registration time (the task only names what it wrote),
/// which is exactly the shape the durable-output contract describes.
struct PublishManifest(Blob<Manifest>);
impl Task for PublishManifest {
    type Input = ();
    type Output = Blob<Manifest>;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob<Manifest>, TaskError> {
        Ok(self.0.clone())
    }
}

/// A consumer of the blob-backed manifest: passes its input through, so what it
/// receives is observable.
struct Consume;
impl Task for Consume {
    type Input = Blob<Manifest>;
    type Output = Blob<Manifest>;
    async fn run(
        &mut self,
        _c: &RunContext,
        i: Blob<Manifest>,
    ) -> Result<Blob<Manifest>, TaskError> {
        Ok(i)
    }
}

/// A private per-test temp root, removed on drop.
struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "dagr-cli-t104-{tag}-{}-{nanos}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp root");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn manifest() -> Manifest {
    Manifest {
        rows: 4_211,
        label: "shipments/2026-07-31".to_string(),
    }
}

/// `produce` (durable, blob-backed) → `consume`. The durable stage boundary the
/// resume scenarios plan against.
fn durable_chain(blob: &Blob<Manifest>) -> Pipeline {
    let mut flow = Flow::new();
    let produce = flow.register_source_durable(
        "produce",
        &PublishManifest(blob.clone()),
        NodePolicy::new(),
    );
    let _consume = flow.register("consume", &Consume, produce);
    flow.finish()
}

/// A prior run matching `pipeline`: `produce` succeeded durably with `blob`'s
/// reference + recorded hash, `consume` failed (so it re-runs and demands the
/// producer's value — which is what makes the probe fire).
fn prior_with(pipeline: &Pipeline, blob: &Blob<Manifest>) -> PriorRun {
    let fp = pipeline.fingerprint();
    let mut nodes = BTreeMap::new();
    nodes.insert(
        "produce".to_string(),
        PriorNode {
            terminal: TerminalState::Succeeded,
            durable_reference: Some(blob.serialize_reference()),
            durable_reference_content_hash: Some(blob.content_hash().to_string()),
            originating_run: "run-A".to_string(),
        },
    );
    nodes.insert(
        "consume".to_string(),
        PriorNode {
            terminal: TerminalState::Failed,
            durable_reference: None,
            durable_reference_content_hash: None,
            originating_run: "run-A".to_string(),
        },
    );
    PriorRun {
        structural_fingerprint: fp.structural(),
        policy_hash: fp.policy(),
        algorithm_version: fp.algorithm_version(),
        tool_version: "dagr@1".to_string(),
        nodes,
    }
}

// ===========================================================================
// The round trip: a Payload value through the store and back.
// ===========================================================================

#[test]
fn a_payload_produced_through_the_bridge_rehydrates_to_an_equal_value() {
    let root = TempRoot::new("round-trip");
    let store = LocalFsBlob::open(root.path());

    let blob = Blob::put(&store, manifest()).expect("put the payload");
    let reference = blob.serialize_reference();

    let rehydrated = Blob::<Manifest>::rehydrate(&reference).expect("rehydrate");
    assert_eq!(
        rehydrated.value(),
        &manifest(),
        "the reference round-trips to an EQUAL value"
    );
    assert_eq!(
        rehydrated.serialize_reference(),
        reference,
        "and names the same blob"
    );
    assert_eq!(*blob, manifest(), "the produced blob derefs to its value");
}

#[test]
fn the_same_payload_written_twice_names_one_blob() {
    let root = TempRoot::new("dedup");
    let store = LocalFsBlob::open(root.path());

    let first = Blob::put(&store, manifest()).expect("put 1");
    let second = Blob::put(&store, manifest()).expect("put 2");

    assert_eq!(
        first.serialize_reference(),
        second.serialize_reference(),
        "a deterministic codec + content addressing collapse the two writes"
    );
}

#[test]
fn durable_reference_meta_carries_the_content_hash_and_the_size() {
    let root = TempRoot::new("meta");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");

    let meta = blob
        .durable_reference_meta()
        .expect("the bridge always supplies metadata");
    assert_eq!(
        meta.recorded_content_hash(),
        Some(blob.content_hash()),
        "the digest the store computed lands in content_hash"
    );
    let encoded_len = manifest().encode_to_vec().len() as u64;
    assert_eq!(
        meta.recorded_size_bytes(),
        Some(encoded_len),
        "size_bytes is the encoded length"
    );
    assert!(
        meta.recorded_scheme().is_some(),
        "the scheme names where the value lives"
    );

    // The content hash is the store's own key for those bytes — self-verifying.
    let key = BlobKey::of(&manifest().encode_to_vec());
    assert_eq!(blob.content_hash(), key.to_string());
}

#[test]
fn a_rehydrate_of_a_deleted_blob_is_absent_and_of_a_damaged_one_is_corrupt() {
    let root = TempRoot::new("classify");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let reference = blob.serialize_reference();
    let key = BlobKey::of(&manifest().encode_to_vec());

    std::fs::write(store.object_path(&key), b"not a payload").expect("damage");
    let err = Blob::<Manifest>::rehydrate(&reference).expect_err("damaged blob");
    assert!(err.is_corruption(), "a damaged blob is CORRUPT: {err}");

    std::fs::remove_file(store.object_path(&key)).expect("delete");
    let err = Blob::<Manifest>::rehydrate(&reference).expect_err("deleted blob");
    assert!(err.is_absent(), "a deleted blob is ABSENT: {err}");

    let err = Blob::<Manifest>::rehydrate("not a blob reference at all")
        .expect_err("unparseable reference");
    assert!(
        err.is_transient(),
        "an unreadable reference is not evidence the referent is gone: {err}"
    );
}

// ===========================================================================
// The existence probe: all four ReferenceExistence cases.
// ===========================================================================

#[test]
fn the_probe_reports_present_for_an_intact_blob() {
    let root = TempRoot::new("probe-present");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");

    assert_eq!(
        reference_existence("produce", &blob.serialize_reference(), Some(blob.content_hash())),
        ReferenceExistence::Present
    );
}

#[test]
fn the_probe_reports_absent_for_a_deleted_blob() {
    let root = TempRoot::new("probe-absent");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let key = BlobKey::of(&manifest().encode_to_vec());
    std::fs::remove_file(store.object_path(&key)).expect("delete");

    assert_eq!(
        reference_existence("produce", &blob.serialize_reference(), Some(blob.content_hash())),
        ReferenceExistence::Absent
    );
}

#[test]
fn the_probe_reports_changed_with_the_actual_hash_for_an_overwritten_blob() {
    let root = TempRoot::new("probe-changed");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let key = BlobKey::of(&manifest().encode_to_vec());
    std::fs::write(store.object_path(&key), b"overwritten out-of-band").expect("overwrite");

    let actual = BlobKey::of(b"overwritten out-of-band").to_string();
    assert_eq!(
        reference_existence("produce", &blob.serialize_reference(), Some(blob.content_hash())),
        ReferenceExistence::Changed { actual },
        "the probe measures the referent's ACTUAL hash and reports the mismatch"
    );
}

#[test]
fn the_probe_cannot_determine_a_reference_it_cannot_reach() {
    // An unparseable reference, and a backend this binary has no store for, are
    // both "cannot determine" — NOT absent. Claiming absent would refuse a resume
    // for a value that is very probably still there.
    assert_eq!(
        reference_existence("produce", "nonsense://whatever", None),
        ReferenceExistence::CannotDetermine
    );
    assert_eq!(
        reference_existence("produce", "dagr-blob+s3://bucket/sha256/abcdef", None),
        ReferenceExistence::CannotDetermine
    );

    // So is an object path that exists but cannot be read as a file.
    let root = TempRoot::new("probe-unreachable");
    let store = LocalFsBlob::open(root.path());
    let key = BlobKey::of(b"unreadable");
    std::fs::create_dir_all(store.object_path(&key)).expect("occupy the object path");
    assert_eq!(
        reference_existence("produce", &store.reference(&key).to_string(), None),
        ReferenceExistence::CannotDetermine
    );
}

// ===========================================================================
// Resume: the existing gates, reached through a shipped backend.
// ===========================================================================

#[test]
fn a_resume_with_an_intact_blob_satisfies_the_producer_from_prior() {
    let root = TempRoot::new("resume-present");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let pipeline = durable_chain(&blob);
    let prior = prior_with(&pipeline, &blob);

    let plan = plan_resume(&pipeline, &prior, "dagr@1", reference_existence)
        .expect("an intact blob resumes");

    assert!(
        plan.satisfied_from_prior().contains_key("produce"),
        "the durable producer is satisfied-from-prior: {:?}",
        plan.satisfied_from_prior()
    );
    assert!(
        !plan.must_run().contains("produce"),
        "and is not re-executed"
    );
    assert_eq!(
        plan.rehydrate().get("produce").map(String::as_str),
        Some(blob.serialize_reference().as_str()),
        "its value is rehydrated from the blob reference"
    );
    assert!(
        plan.must_run().contains("consume"),
        "the failed consumer re-runs"
    );
}

#[test]
fn a_resume_whose_blob_was_deleted_refuses_with_a_dangling_reference() {
    let root = TempRoot::new("resume-dangling");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let pipeline = durable_chain(&blob);
    let prior = prior_with(&pipeline, &blob);

    // The blob is deleted between runs.
    let key = BlobKey::of(&manifest().encode_to_vec());
    std::fs::remove_file(store.object_path(&key)).expect("delete");

    let refusal = plan_resume(&pipeline, &prior, "dagr@1", reference_existence)
        .expect_err("a dangling reference fails the plan up front");
    match refusal {
        ResumeRefusal::DanglingReference { node, reference } => {
            assert_eq!(node, "produce", "the refusal names the node");
            assert_eq!(reference, blob.serialize_reference());
        }
        other => panic!("expected DanglingReference, got {other}"),
    }
}

#[test]
fn a_resume_whose_blob_was_overwritten_refuses_with_a_mutated_reference() {
    let root = TempRoot::new("resume-mutated");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let pipeline = durable_chain(&blob);
    let prior = prior_with(&pipeline, &blob);

    // The blob is overwritten out-of-band: it still exists, and it is not the
    // value the prior run produced.
    let key = BlobKey::of(&manifest().encode_to_vec());
    std::fs::write(store.object_path(&key), b"someone else's bytes").expect("overwrite");

    let refusal = plan_resume(&pipeline, &prior, "dagr@1", reference_existence)
        .expect_err("a mutated referent fails the plan up front");
    match refusal {
        ResumeRefusal::MutatedReference {
            node,
            reference,
            expected_hash,
            actual_hash,
        } => {
            assert_eq!(node, "produce");
            assert_eq!(reference, blob.serialize_reference());
            assert_eq!(expected_hash, blob.content_hash(), "names the recorded hash");
            assert_eq!(
                actual_hash,
                BlobKey::of(b"someone else's bytes").to_string(),
                "and the actual one"
            );
        }
        other => panic!("expected MutatedReference, got {other}"),
    }
}

// ===========================================================================
// The T89 path, unchanged: the attempt-outcome record carries hash and size.
// ===========================================================================

/// A sink collecting the emitted stream in memory.
#[derive(Clone, Default)]
struct MemSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl MemSink {
    fn take(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}
impl EventSink for MemSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A hand-stepped monotonic clock — deterministic offsets, no wall clock.
struct ZeroClock;
impl MonotonicClock for ZeroClock {
    fn elapsed_ns(&self) -> u64 {
        0
    }
}

#[test]
fn the_attempt_outcome_record_carries_the_blob_reference_hash_and_size() {
    let root = TempRoot::new("t89");
    let store = LocalFsBlob::open(root.path());
    let blob = Blob::put(&store, manifest()).expect("put");
    let meta = blob.durable_reference_meta().expect("metadata");
    let wire = wire_reference_meta(&meta);

    let sink = MemSink::default();
    let mut writer = EventStreamWriter::new(
        sink.clone(),
        ZeroClock,
        WireRunId::from_operator("run-blob".to_string()),
        "blob-pipeline",
    );
    writer
        .run_started(RunStartedHeader {
            pipeline: "blob-pipeline".to_string(),
            fingerprint_structural: None,
            fingerprint_policy: None,
            fingerprint_algorithm_version: 1,
            parameters: BTreeMap::new(),
            data_interval: None,
            captured_env: BTreeMap::new(),
            resumed_from: None,
        })
        .expect("header");
    writer.node_ready("produce").expect("ready");
    writer.node_admitted("produce").expect("admitted");
    writer.attempt_started("produce", 1).expect("started");
    writer.attempt_succeeded("produce", 1).expect("succeeded");

    // Exactly the driver's calls (T89), with the bridge's values.
    let mut record = AttemptOutcomeRecord {
        node: "produce".into(),
        attempt: 1,
        status: WireTerminalState::Succeeded.as_str().into(),
        ..AttemptOutcomeRecord::default()
    };
    record_durable_reference(&mut record, Some(blob.serialize_reference()));
    record_durable_reference_meta(&mut record, Some(wire.clone()));
    writer.attempt_outcome(record).expect("outcome");
    writer
        .output_produced(OutputProducedRecord {
            node: "produce".to_string(),
            attempt: 1,
            uri: blob.serialize_reference(),
            content_hash: wire.content_hash.clone(),
            size_bytes: wire.size_bytes,
            kind: wire.scheme.clone(),
            produced_at_offset_ns: 0,
            originating_run: "run-blob".to_string(),
        })
        .expect("output-produced");
    writer
        .node_terminal("produce", WireTerminalState::Succeeded)
        .expect("terminal");
    writer.run_finished(RunOutcome::Succeeded).expect("finished");
    writer.finish().expect("flush");

    let artifact = fold_stream(&sink.take(), &["produce".to_string()]).expect("the stream folds");
    let attempt = artifact
        .attempts()
        .iter()
        .find(|a| a.node() == "produce")
        .expect("the produce attempt");
    assert_eq!(
        attempt.durable_reference(),
        Some(blob.serialize_reference().as_str()),
        "the attempt record carries the blob reference"
    );

    let produced = artifact.outputs().first().expect("one produced output");
    assert_eq!(produced.uri(), blob.serialize_reference());
    assert_eq!(
        produced.content_hash(),
        Some(blob.content_hash()),
        "and the content hash, through the unchanged T89 path"
    );
    assert_eq!(
        produced.size_bytes(),
        Some(manifest().encode_to_vec().len() as u64),
        "and the size"
    );
}
