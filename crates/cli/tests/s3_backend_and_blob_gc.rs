//! T110 acceptance tests: the **object-store backend through the bridge**, and
//! **intermediate-blob garbage collection by reachability**.
//!
//! `crates/blob/tests/s3_backend.rs` proves the backend against the port. This
//! suite proves the two things that only exist above the port:
//!
//! * the `DurableOutput` bridge and all four `ReferenceExistence` outcomes work
//!   against `S3Blob` exactly as they do against `LocalFsBlob` — and, the case
//!   that matters most, a store that is merely *unreachable* never turns a
//!   healthy resume into a spurious `DanglingReference` refusal;
//! * `prune` reclaims intermediate blobs by **reachability, not age**. Content
//!   addressing means the same value produced by two runs is *one* blob, so an
//!   age-based reaper would delete a blob a newer run still references. The
//!   criterion is therefore "no retained run artifact references it", and an
//!   artifact that cannot be read is a refusal rather than a guess.
//!
//! No network service anywhere: the object store is the in-process
//! S3-compatible fixture `dagr_blob::s3::fake::FakeS3`.
//!
//! Written FIRST and failing before any of it lands.
#![cfg(feature = "blob")]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dagr_blob::retry::RetryBudget;
use dagr_blob::s3::fake::FakeS3;
use dagr_blob::s3::{S3Blob, S3Config, S3Credentials};
use dagr_blob::{BlobKey, BlobReclaim, BlobStore, LocalFsBlob};
use dagr_cli::blob_bridge::{Blob, reference_existence_in};
use dagr_cli::blob_gc::{ReclaimRefusal, apply_reclaim, plan_reclaim, reclaim_blobs_verb};
use dagr_cli::contract::{ExitCode, reserved_flag_names};
use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::execution::Backoff;
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::resume::{PriorNode, PriorRun, ReferenceExistence, ResumeRefusal, plan_resume};
use dagr_core::task::Task;
use dagr_core::{Payload, RunContext, StableName, TaskError, TerminalState};

// === Fixtures ==============================================================

#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Manifest {
    rows: u64,
    label: String,
}

struct PublishManifest(Blob<Manifest>);
impl Task for PublishManifest {
    type Input = ();
    type Output = Blob<Manifest>;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob<Manifest>, TaskError> {
        Ok(self.0.clone())
    }
}

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

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "dagr-cli-t110-{tag}-{}-{n}",
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

fn manifest(rows: u64) -> Manifest {
    Manifest {
        rows,
        label: "shipments/2026-08-01".to_string(),
    }
}

fn s3(bucket: &str) -> (S3Blob<FakeS3>, FakeS3) {
    let fake = FakeS3::new(bucket);
    let store = S3Blob::new(
        S3Config::new(bucket).with_region("eu-west-2"),
        S3Credentials::new("AKIAIOSFODNN7EXAMPLE", "a-fabricated-secret"),
        fake.clone(),
    )
    .with_retry(RetryBudget::new(1, Duration::ZERO, 2.0, Duration::ZERO));
    (store, fake)
}

/// `produce` (durable, blob-backed) → `consume`.
fn durable_chain(blob: &Blob<Manifest>) -> Pipeline {
    let mut flow = Flow::new();
    let produce =
        flow.register_source_durable("produce", &PublishManifest(blob.clone()), NodePolicy::new());
    let _consume = flow.register("consume", &Consume, produce);
    flow.finish()
}

fn prior_with(pipeline: &Pipeline, blob: &Blob<Manifest>) -> PriorRun {
    let fp = pipeline.fingerprint();
    let mut nodes = BTreeMap::new();
    nodes.insert(
        "produce".to_string(),
        PriorNode {
            terminal: TerminalState::Succeeded,
            durable_reference: Some(blob.serialize_reference()),
            durable_reference_content_hash: Some(blob.content_hash()),
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

// === Run-store fixtures for the GC =========================================

/// Write a run artifact at `<base>/<pipeline>/<run-id>/run.json` recording that
/// `produce` output `reference` (with `content_hash`), in both places a folded
/// artifact records it: the top-level `outputs` lineage array and the attempt's
/// `durable_reference`.
fn plant_run_artifact(base: &Path, pipeline: &str, run_id: &str, outputs: &[(&str, &Blob<Manifest>)]) {
    let dir = base.join(pipeline).join(run_id);
    std::fs::create_dir_all(&dir).expect("create the run directory");
    std::fs::write(dir.join("events.jsonl"), b"").expect("plant an event stream");

    let produced: Vec<serde_json::Value> = outputs
        .iter()
        .map(|(node, blob)| {
            serde_json::json!({
                "node": node,
                "attempt": 1,
                "uri": blob.serialize_reference(),
                "content_hash": blob.content_hash(),
                "size_bytes": blob.size_bytes(),
                "produced_at_offset_ns": 0,
                "originating_run": run_id,
            })
        })
        .collect();
    let attempts: Vec<serde_json::Value> = outputs
        .iter()
        .map(|(node, blob)| {
            serde_json::json!({
                "node": node,
                "attempt": 1,
                "terminal_state": "succeeded",
                "durable_reference": blob.serialize_reference(),
                "durable_reference_meta": { "content_hash": blob.content_hash() },
            })
        })
        .collect();
    let artifact = serde_json::json!({
        "schema": "dagr.run@1.2",
        "run_id": run_id,
        "pipeline": pipeline,
        "overall_outcome": "succeeded",
        "attempts": attempts,
        "outputs": produced,
    });
    std::fs::write(
        dir.join("run.json"),
        serde_json::to_vec_pretty(&artifact).expect("serialize"),
    )
    .expect("plant the run artifact");
}

/// Remove a run directory the way `prune`'s retention half does.
fn prune_run_directory(base: &Path, pipeline: &str, run_id: &str) {
    std::fs::remove_dir_all(base.join(pipeline).join(run_id)).expect("prune the run directory");
}

fn hexes(keys: &[BlobKey]) -> Vec<String> {
    let mut out: Vec<String> = keys.iter().map(|k| k.hex().to_string()).collect();
    out.sort();
    out
}

// ===========================================================================
// The bridge, over the object store.
// ===========================================================================

#[test]
fn a_payload_stored_in_the_object_store_round_trips_through_the_bridge() {
    let (store, _fake) = s3("bridge-bucket");
    let published = Blob::put(&store, manifest(4_211)).expect("publish");

    let reference = published.serialize_reference();
    assert!(
        reference.starts_with("dagr-blob+s3://bridge-bucket/sha256/"),
        "the reference names the object-store backend: {reference}"
    );

    let same = Blob::<Manifest>::rehydrate_from(&store, &reference).expect("rehydrate");
    assert_eq!(same.value(), published.value());

    let meta = published
        .durable_reference_meta()
        .expect("a blob-backed output always carries metadata");
    assert_eq!(meta.recorded_content_hash(), Some(published.content_hash().as_str()));
    assert_eq!(meta.recorded_size_bytes(), Some(published.size_bytes()));
    assert_eq!(meta.recorded_scheme(), Some("dagr-blob+s3"));
}

#[test]
fn the_probe_answers_all_four_outcomes_against_the_object_store() {
    let (store, fake) = s3("probe-bucket");
    let published = Blob::put(&store, manifest(7)).expect("publish");
    let reference = published.serialize_reference();
    let recorded = published.content_hash();

    // Present.
    assert_eq!(
        reference_existence_in(&store, &reference, Some(&recorded)),
        ReferenceExistence::Present
    );

    // Changed — the object is there and hashes to something else.
    fake.overwrite_object(
        &store.object_key(published.blob_ref().key()),
        b"overwritten out of band".to_vec(),
    );
    match reference_existence_in(&store, &reference, Some(&recorded)) {
        ReferenceExistence::Changed { actual } => {
            assert_ne!(actual, recorded, "the actual hash is reported");
        }
        other => panic!("expected Changed, got {other:?}"),
    }

    // Absent — the object is gone.
    fake.remove_object(&store.object_key(published.blob_ref().key()));
    assert_eq!(
        reference_existence_in(&store, &reference, Some(&recorded)),
        ReferenceExistence::Absent
    );

    // CannotDetermine — the store could not be reached, so the probe could not
    // look. A probe that could not look is not evidence of deletion.
    fake.set_unreachable(true);
    assert_eq!(
        reference_existence_in(&store, &reference, Some(&recorded)),
        ReferenceExistence::CannotDetermine
    );
}

#[test]
fn a_resume_over_object_store_blobs_satisfies_the_producer_from_prior() {
    let (store, _fake) = s3("resume-bucket");
    let published = Blob::put(&store, manifest(11)).expect("publish");
    let pipeline = durable_chain(&published);
    let prior = prior_with(&pipeline, &published);

    let plan = plan_resume(&pipeline, &prior, "dagr@1", |_node, reference, hash| {
        reference_existence_in(&store, reference, hash)
    })
    .expect("an intact object-store blob is a resumable prior run");
    assert!(
        plan.satisfied_from_prior().contains_key("produce"),
        "the durable producer is carried forward: {:?}",
        plan.satisfied_from_prior()
    );
}

#[test]
fn an_unreachable_object_store_does_not_turn_a_resume_into_a_dangling_reference() {
    let (store, fake) = s3("down-bucket");
    let published = Blob::put(&store, manifest(13)).expect("publish");
    let pipeline = durable_chain(&published);
    let prior = prior_with(&pipeline, &published);

    fake.set_unreachable(true);
    let outcome = plan_resume(&pipeline, &prior, "dagr@1", |_node, reference, hash| {
        reference_existence_in(&store, reference, hash)
    });
    match outcome {
        Ok(_plan) => {}
        Err(ResumeRefusal::DanglingReference { node, .. }) => panic!(
            "an unreachable store must never refuse `{node}` as dangling — the value is \
             very probably still there"
        ),
        Err(other) => panic!("unexpected refusal: {other}"),
    }
}

#[test]
fn a_genuinely_missing_object_refuses_the_resume_naming_the_node() {
    let (store, fake) = s3("gone-bucket");
    let published = Blob::put(&store, manifest(17)).expect("publish");
    let pipeline = durable_chain(&published);
    let prior = prior_with(&pipeline, &published);

    fake.remove_object(&store.object_key(published.blob_ref().key()));
    let refusal = plan_resume(&pipeline, &prior, "dagr@1", |_node, reference, hash| {
        reference_existence_in(&store, reference, hash)
    })
    .expect_err("a deleted object is a dangling reference");
    match refusal {
        ResumeRefusal::DanglingReference { node, .. } => assert_eq!(node, "produce"),
        other => panic!("expected DanglingReference, got {other}"),
    }
}

// ===========================================================================
// GC by reachability, not age.
// ===========================================================================

/// **The test that would fail under age-based reclaim.** Two runs produce the
/// *same* value, so content addressing collapses them onto one blob. The older
/// run is pruned; the blob must survive, because the newer run still references
/// it — and a resume of the newer run still needs it.
#[test]
fn a_blob_two_runs_produced_survives_pruning_the_older_run() {
    let base = TempRoot::new("shared-blob");
    let blobs = TempRoot::new("shared-blob-store");
    let store = LocalFsBlob::open(blobs.path());

    let shared = Blob::put(&store, manifest(99)).expect("publish");
    let again = Blob::put(&store, manifest(99)).expect("publish the identical value again");
    assert_eq!(
        shared.blob_ref().key(),
        again.blob_ref().key(),
        "identical values collapse to one blob — this is the whole hazard"
    );

    plant_run_artifact(base.path(), "etl", "run-old", &[("produce", &shared)]);
    plant_run_artifact(base.path(), "etl", "run-new", &[("produce", &again)]);

    // Retention removed the older run; the blob is now referenced only by the
    // newer artifact.
    prune_run_directory(base.path(), "etl", "run-old");

    let plan = plan_reclaim(base.path(), &store).expect("the artifacts are readable");
    assert!(
        plan.reclaimable().is_empty(),
        "a blob a retained run still references is never reclaimable: {:?}",
        hexes(plan.reclaimable())
    );

    apply_reclaim(&store, &plan).expect("nothing to do");
    assert_eq!(
        store.get(shared.blob_ref().key()).expect("still there"),
        manifest(99).encode_to_vec()
    );
}

#[test]
fn the_blobs_of_a_retained_run_are_never_reclaimed() {
    let base = TempRoot::new("retained");
    let blobs = TempRoot::new("retained-store");
    let store = LocalFsBlob::open(blobs.path());

    let one = Blob::put(&store, manifest(1)).expect("publish");
    let two = Blob::put(&store, manifest(2)).expect("publish");
    plant_run_artifact(
        base.path(),
        "etl",
        "run-1",
        &[("produce", &one), ("second", &two)],
    );

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert!(plan.reclaimable().is_empty(), "both blobs are reachable");
    assert_eq!(plan.reachable_count(), 2);
}

#[test]
fn blobs_of_a_pruned_run_that_nothing_else_references_are_reclaimed() {
    let base = TempRoot::new("pruned-run");
    let blobs = TempRoot::new("pruned-run-store");
    let store = LocalFsBlob::open(blobs.path());

    let kept = Blob::put(&store, manifest(1)).expect("publish");
    let orphaned = Blob::put(&store, manifest(2)).expect("publish");
    plant_run_artifact(base.path(), "etl", "run-keep", &[("produce", &kept)]);
    plant_run_artifact(base.path(), "etl", "run-drop", &[("produce", &orphaned)]);
    prune_run_directory(base.path(), "etl", "run-drop");

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert_eq!(
        hexes(plan.reclaimable()),
        vec![orphaned.blob_ref().key().hex().to_string()],
        "exactly the blob no retained artifact references"
    );

    let deleted = apply_reclaim(&store, &plan).expect("delete");
    assert_eq!(hexes(&deleted), hexes(plan.reclaimable()));
    assert!(
        store.head(orphaned.blob_ref().key()).expect_err("gone").is_absent(),
        "the reclaimed blob is gone"
    );
    assert!(
        store.head(kept.blob_ref().key()).is_ok(),
        "and the retained one is not"
    );
}

#[test]
fn a_blob_no_artifact_references_at_all_is_reclaimed() {
    let base = TempRoot::new("orphan");
    let blobs = TempRoot::new("orphan-store");
    let store = LocalFsBlob::open(blobs.path());

    // An abandoned run's leftover: bytes in the store, no artifact anywhere.
    let leftover = store.put(b"an abandoned attempt's output").expect("put");
    std::fs::create_dir_all(base.path().join("etl")).expect("an empty pipeline directory");

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert_eq!(hexes(plan.reclaimable()), vec![leftover.hex().to_string()]);
}

#[test]
fn an_input_reference_recorded_by_an_attempt_keeps_its_blob_reachable() {
    let base = TempRoot::new("inputs");
    let blobs = TempRoot::new("inputs-store");
    let store = LocalFsBlob::open(blobs.path());

    let consumed = Blob::put(&store, manifest(5)).expect("publish");
    let dir = base.path().join("etl").join("run-1");
    std::fs::create_dir_all(&dir).expect("create");
    std::fs::write(dir.join("events.jsonl"), b"").expect("plant");
    // An artifact whose attempt CONSUMED a reference but produced nothing durable:
    // the blob is still live, because a replay of that node needs it.
    let artifact = serde_json::json!({
        "schema": "dagr.run@1.2",
        "run_id": "run-1",
        "pipeline": "etl",
        "overall_outcome": "succeeded",
        "attempts": [{
            "node": "consume",
            "attempt": 1,
            "terminal_state": "succeeded",
            "inputs": [{ "uri": consumed.serialize_reference(), "content_hash": consumed.content_hash() }],
        }],
        "outputs": [],
    });
    std::fs::write(dir.join("run.json"), serde_json::to_vec(&artifact).unwrap()).expect("write");

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert!(
        plan.reclaimable().is_empty(),
        "a consumed input reference keeps its blob reachable: {:?}",
        hexes(plan.reclaimable())
    );
}

#[test]
fn attempt_shards_sharing_the_container_are_never_reclaimed() {
    let base = TempRoot::new("shards");
    let blobs = TempRoot::new("shards-store");
    let store = LocalFsBlob::open(blobs.path());
    let orphan = store.put(b"an unreferenced blob").expect("put");

    let shard = blobs.path().join("attempt-shards").join("run-1").join("aa");
    std::fs::create_dir_all(&shard).expect("create");
    std::fs::write(shard.join("1.jsonl"), b"{\"kind\":\"shard-header\"}\n").expect("plant");

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert_eq!(hexes(plan.reclaimable()), vec![orphan.hex().to_string()]);
    apply_reclaim(&store, &plan).expect("delete");
    assert!(
        shard.join("1.jsonl").is_file(),
        "an attempt shard is not a blob and the reaper never touches it"
    );
}

#[test]
fn an_unreadable_run_artifact_makes_the_reclaim_refuse_and_say_why() {
    let base = TempRoot::new("unreadable");
    let blobs = TempRoot::new("unreadable-store");
    let store = LocalFsBlob::open(blobs.path());
    let blob = Blob::put(&store, manifest(3)).expect("publish");
    plant_run_artifact(base.path(), "etl", "run-1", &[("produce", &blob)]);

    // Corrupt the artifact: reachability is now unknown, and unknown is not an
    // excuse to guess.
    std::fs::write(
        base.path().join("etl").join("run-1").join("run.json"),
        b"{ this is not json",
    )
    .expect("corrupt the artifact");

    let refusal = plan_reclaim(base.path(), &store).expect_err("unknown reachability refuses");
    match &refusal {
        ReclaimRefusal::UnreadableArtifact { path, .. } => {
            assert!(path.ends_with("run.json"), "the refusal names the artifact: {path:?}");
        }
        other => panic!("expected UnreadableArtifact, got {other}"),
    }
    assert!(
        refusal.to_string().contains("run.json"),
        "and says why: {refusal}"
    );
    assert!(
        store.head(blob.blob_ref().key()).is_ok(),
        "nothing was deleted"
    );
}

#[test]
fn an_unfolded_run_makes_the_reclaim_refuse_rather_than_guess() {
    let base = TempRoot::new("unfolded");
    let blobs = TempRoot::new("unfolded-store");
    let store = LocalFsBlob::open(blobs.path());
    store.put(b"a blob whose run was never folded").expect("put");

    // A crashed run: an event stream, no artifact. Its references are in the
    // stream, not in an artifact — so reachability is unknown until `fold` runs.
    let dir = base.path().join("etl").join("run-crashed");
    std::fs::create_dir_all(&dir).expect("create");
    std::fs::write(dir.join("events.jsonl"), b"{}\n").expect("plant a stream");

    let refusal = plan_reclaim(base.path(), &store).expect_err("an unfolded run refuses");
    match &refusal {
        ReclaimRefusal::UnfoldedRun { path } => {
            assert!(path.ends_with("run-crashed"), "names the run: {path:?}");
        }
        other => panic!("expected UnfoldedRun, got {other}"),
    }
    assert!(
        refusal.to_string().contains("fold"),
        "and points at the verb that makes it readable: {refusal}"
    );
}

#[test]
fn the_dry_run_listing_is_exactly_what_the_real_run_deletes() {
    let base = TempRoot::new("dryrun");
    let blobs = TempRoot::new("dryrun-store");
    let store = LocalFsBlob::open(blobs.path());
    let kept = Blob::put(&store, manifest(1)).expect("publish");
    let a = store.put(b"orphan a").expect("put");
    let b = store.put(b"orphan b").expect("put");
    plant_run_artifact(base.path(), "etl", "run-1", &[("produce", &kept)]);

    let mut dry = Vec::new();
    let code = reclaim_blobs_verb(
        &argv(&[
            "--store",
            base.path().to_str().unwrap(),
            "--blob-store",
            blobs.path().to_str().unwrap(),
            "--reclaim-blobs",
            "dry-run",
        ]),
        &mut dry,
    );
    assert_eq!(code, ExitCode::Success);
    let dry = String::from_utf8(dry).expect("utf-8");

    // Nothing was deleted by the dry run.
    assert!(store.head(&a).is_ok() && store.head(&b).is_ok(), "dry run deletes nothing");
    assert_eq!(
        hexes(&store.list().expect("list")).len(),
        3,
        "all three blobs are still present after the dry run"
    );

    let mut real = Vec::new();
    let code = reclaim_blobs_verb(
        &argv(&[
            "--store",
            base.path().to_str().unwrap(),
            "--blob-store",
            blobs.path().to_str().unwrap(),
            "--reclaim-blobs",
            "delete",
        ]),
        &mut real,
    );
    assert_eq!(code, ExitCode::Success);
    let real = String::from_utf8(real).expect("utf-8");

    let listed = |text: &str| -> Vec<String> {
        let mut v: Vec<String> = text
            .lines()
            .filter(|l| l.starts_with("blob-reclaim "))
            .map(str::to_string)
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        listed(&dry),
        listed(&real),
        "the dry-run listing is exactly what the real run deletes"
    );
    assert!(!listed(&dry).is_empty(), "and it is not vacuously empty");

    assert_eq!(
        hexes(&store.list().expect("list")),
        vec![kept.blob_ref().key().hex().to_string()],
        "the real run deleted exactly the listed blobs"
    );
}

#[test]
fn the_reclaim_is_opt_in_and_a_bare_prune_touches_no_blob() {
    let base = TempRoot::new("optin");
    let blobs = TempRoot::new("optin-store");
    let store = LocalFsBlob::open(blobs.path());
    store.put(b"an unreferenced blob").expect("put");
    std::fs::create_dir_all(base.path().join("etl")).expect("create");

    // No `--reclaim-blobs`: the verb does nothing to the store and says so.
    let mut out = Vec::new();
    let code = reclaim_blobs_verb(
        &argv(&["--store", base.path().to_str().unwrap()]),
        &mut out,
    );
    assert_eq!(code, ExitCode::Success);
    assert_eq!(
        store.list().expect("list").len(),
        1,
        "a prune without --reclaim-blobs never touches the blob store"
    );

    // An unrecognized mode is invalid usage, never a silent delete.
    let mut out = Vec::new();
    let code = reclaim_blobs_verb(
        &argv(&[
            "--store",
            base.path().to_str().unwrap(),
            "--blob-store",
            blobs.path().to_str().unwrap(),
            "--reclaim-blobs",
            "yes-please",
        ]),
        &mut out,
    );
    assert_eq!(code, ExitCode::InvalidUsage);
    assert_eq!(store.list().expect("list").len(), 1, "and nothing was deleted");
}

#[test]
fn a_refusal_exits_non_zero_and_deletes_nothing() {
    let base = TempRoot::new("refuse-exit");
    let blobs = TempRoot::new("refuse-exit-store");
    let store = LocalFsBlob::open(blobs.path());
    store.put(b"an unreferenced blob").expect("put");
    let dir = base.path().join("etl").join("run-1");
    std::fs::create_dir_all(&dir).expect("create");
    std::fs::write(dir.join("events.jsonl"), b"").expect("plant");
    std::fs::write(dir.join("run.json"), b"not json at all").expect("plant");

    let mut out = Vec::new();
    let code = reclaim_blobs_verb(
        &argv(&[
            "--store",
            base.path().to_str().unwrap(),
            "--blob-store",
            blobs.path().to_str().unwrap(),
            "--reclaim-blobs",
            "delete",
        ]),
        &mut out,
    );
    assert_ne!(code, ExitCode::Success, "a refusal is not a success");
    assert_eq!(store.list().expect("list").len(), 1, "and deletes nothing");
    let text = String::from_utf8(out).expect("utf-8");
    assert!(text.contains("run.json"), "the diagnostic names the artifact: {text}");
}

#[test]
fn a_resume_that_needs_a_reclaimed_blob_refuses_with_a_dangling_reference() {
    let base = TempRoot::new("reclaim-resume");
    let blobs = TempRoot::new("reclaim-resume-store");
    let store = LocalFsBlob::open(blobs.path());

    let published = Blob::put(&store, manifest(23)).expect("publish");
    let pipeline = durable_chain(&published);
    let prior = prior_with(&pipeline, &published);

    // The operator prunes the run and reclaims its blobs. This is the honest
    // consequence, tested so it is not a surprise.
    std::fs::create_dir_all(base.path().join("etl")).expect("create");
    let plan = plan_reclaim(base.path(), &store).expect("plan");
    apply_reclaim(&store, &plan).expect("reclaim");

    let refusal = plan_resume(&pipeline, &prior, "dagr@1", |_node, reference, hash| {
        reference_existence_in(&store, reference, hash)
    })
    .expect_err("the blob the resume needed was reclaimed");
    match refusal {
        ResumeRefusal::DanglingReference { node, .. } => assert_eq!(node, "produce"),
        other => panic!("expected DanglingReference, got {other}"),
    }
}

#[test]
fn the_reclaim_walks_the_object_store_backend_too() {
    let base = TempRoot::new("s3-gc");
    let (store, _fake) = s3("gc-bucket");

    let kept = Blob::put(&store, manifest(1)).expect("publish");
    let orphan = store.put(b"nothing references this").expect("put");
    plant_run_artifact(base.path(), "etl", "run-1", &[("produce", &kept)]);

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert_eq!(hexes(plan.reclaimable()), vec![orphan.hex().to_string()]);
    apply_reclaim(&store, &plan).expect("reclaim");
    assert!(store.head(&orphan).expect_err("gone").is_absent());
    assert!(store.head(kept.blob_ref().key()).is_ok());
}

#[test]
fn a_reference_into_a_different_container_is_ignored_rather_than_reclaimed() {
    let base = TempRoot::new("other-container");
    let mine = TempRoot::new("other-container-mine");
    let theirs = TempRoot::new("other-container-theirs");
    let store = LocalFsBlob::open(mine.path());
    let other = LocalFsBlob::open(theirs.path());

    let elsewhere = Blob::put(&other, manifest(1)).expect("publish into another container");
    let orphan = store.put(b"unreferenced here").expect("put");
    plant_run_artifact(base.path(), "etl", "run-1", &[("produce", &elsewhere)]);

    let plan = plan_reclaim(base.path(), &store).expect("plan");
    assert_eq!(
        hexes(plan.reclaimable()),
        vec![orphan.hex().to_string()],
        "a reference naming another container says nothing about this one"
    );
}

// ===========================================================================
// Operator configuration.
// ===========================================================================

#[test]
fn the_object_store_knobs_live_in_the_reserved_flag_namespace() {
    for flag in [
        "dagr.blob.endpoint",
        "dagr.blob.bucket",
        "dagr.blob.region",
        "dagr.blob.prefix",
        "reclaim-blobs",
    ] {
        assert!(
            reserved_flag_names().contains(&flag),
            "`{flag}` must be reserved so a pipeline parameter can never shadow it"
        );
    }
}

#[test]
fn endpoint_bucket_region_and_prefix_follow_flag_over_env_over_default() {
    use dagr_cli::config::{
        BLOB_REGION_DEFAULT, resolve_blob_bucket, resolve_blob_endpoint, resolve_blob_prefix,
        resolve_blob_region,
    };

    // Default, with nothing set anywhere.
    let cleared: &dyn Fn(&str) -> Option<String> = &|_name: &str| None;
    assert_eq!(
        resolve_blob_region(None, cleared).expect("default"),
        BLOB_REGION_DEFAULT
    );
    assert_eq!(resolve_blob_endpoint(None, cleared).expect("default"), None);
    assert_eq!(resolve_blob_bucket(None, cleared).expect("default"), None);
    assert_eq!(
        resolve_blob_prefix(None, cleared).expect("default"),
        String::new()
    );

    // Env supplies it when no flag does.
    let env: &dyn Fn(&str) -> Option<String> = &|name: &str| match name {
        "DAGR_BLOB_ENDPOINT" => Some("https://minio.internal:9000".to_string()),
        "DAGR_BLOB_BUCKET" => Some("from-env".to_string()),
        "DAGR_BLOB_REGION" => Some("eu-west-2".to_string()),
        "DAGR_BLOB_PREFIX" => Some("env-prefix".to_string()),
        _ => None,
    };
    assert_eq!(
        resolve_blob_endpoint(None, env).expect("env"),
        Some("https://minio.internal:9000".to_string())
    );
    assert_eq!(
        resolve_blob_bucket(None, env).expect("env"),
        Some("from-env".to_string())
    );
    assert_eq!(resolve_blob_region(None, env).expect("env"), "eu-west-2");
    assert_eq!(resolve_blob_prefix(None, env).expect("env"), "env-prefix");

    // The flag wins, and the environment is not even read.
    assert_eq!(
        resolve_blob_bucket(Some("from-flag".to_string()), env).expect("flag"),
        Some("from-flag".to_string())
    );
    assert_eq!(
        resolve_blob_region(Some("us-east-2".to_string()), env).expect("flag"),
        "us-east-2"
    );

    // A bad value fails LOUDLY and names the variable — never silently ignored,
    // and never silently guessed at.
    let bad: &dyn Fn(&str) -> Option<String> = &|name: &str| match name {
        "DAGR_BLOB_ENDPOINT" => Some("minio.internal:9000".to_string()),
        "DAGR_BLOB_BUCKET" => Some("bucket/with/a/prefix".to_string()),
        _ => None,
    };
    let err = resolve_blob_endpoint(None, bad).expect_err("a schemeless endpoint");
    assert!(err.to_string().contains("DAGR_BLOB_ENDPOINT"), "{err}");
    let err = resolve_blob_bucket(None, bad).expect_err("a bucket with a prefix in it");
    assert!(err.to_string().contains("DAGR_BLOB_BUCKET"), "{err}");
}

#[test]
fn an_s3_container_is_named_on_the_blob_store_flag_by_scheme() {
    use dagr_cli::blob_gc::OBJECT_STORE_SCHEME;
    assert_eq!(OBJECT_STORE_SCHEME, "s3://");
    // Without the client feature, an `s3://` container is refused BY NAME rather
    // than silently treated as a directory path — which is what would happen if it
    // fell through to the filesystem backend, and would make the reclaim compute
    // reachability against an empty store.
    #[cfg(not(feature = "blob-s3"))]
    {
        let base = TempRoot::new("s3-flag");
        std::fs::create_dir_all(base.path().join("etl")).expect("create");
        let mut out = Vec::new();
        let code = reclaim_blobs_verb(
            &argv(&[
                "--store",
                base.path().to_str().unwrap(),
                "--blob-store",
                "s3://bucket/prefix",
                "--reclaim-blobs",
                "dry-run",
            ]),
            &mut out,
        );
        assert_ne!(code, ExitCode::Success);
        let text = String::from_utf8(out).expect("utf-8");
        assert!(text.contains("blob-s3"), "names the missing feature: {text}");
    }
}

#[test]
fn an_s3_container_resolves_to_a_config_naming_its_bucket_and_prefix() {
    let config = S3Config::new("dagr-blobs").with_prefix("intermediates");
    assert_eq!(config.container(), "dagr-blobs/intermediates");
    let parsed = S3Config::from_container("dagr-blobs/intermediates")
        .expect("a container names a bucket and an optional prefix");
    assert_eq!(parsed.bucket(), "dagr-blobs");
    assert_eq!(parsed.prefix(), "intermediates");
    assert!(
        S3Config::from_container("").is_none(),
        "an empty container names no bucket"
    );
}

// ===========================================================================
// The backoff shape, and the credential boundary.
// ===========================================================================

/// The backend retries with the **engine's** backoff shape rather than a second
/// policy. `dagr-blob` cannot depend on `dagr-core` (that boundary is the whole
/// point of the crate), so the shape is reproduced there and pinned here — the
/// one crate where both are visible — for a matrix of parameters and attempts.
#[test]
fn the_backends_retry_budget_is_the_engines_backoff_shape() {
    for (base_ms, factor, cap_ms) in [(100u64, 2.0f64, 30_000u64), (5, 3.0, 40), (1, 1.0, 10)] {
        let engine = Backoff::new(
            Duration::from_millis(base_ms),
            factor,
            Duration::from_millis(cap_ms),
        );
        let backend = RetryBudget::new(
            8,
            Duration::from_millis(base_ms),
            factor,
            Duration::from_millis(cap_ms),
        );
        for n in 0..12u32 {
            assert_eq!(
                backend.nominal_delay(n),
                engine.nominal_delay(n),
                "backoff shape parity at n={n} for ({base_ms}ms, {factor}, {cap_ms}ms)"
            );
        }
    }
}

#[test]
fn no_credential_value_reaches_a_captured_log_line() {
    const SENTINEL: &str = "SENTINEL-b41c7e-DO-NOT-LEAK-93af2";
    let fake = FakeS3::new("logging-bucket");
    let store = S3Blob::new(
        S3Config::new("logging-bucket"),
        S3Credentials::new("AKIAIOSFODNN7EXAMPLE", SENTINEL),
        fake.clone(),
    )
    .with_retry(RetryBudget::new(1, Duration::ZERO, 2.0, Duration::ZERO));

    let mut captured = String::new();
    let published = Blob::put(&store, manifest(2)).expect("publish");
    captured.push_str(&format!("{:?}", published.durable_reference_meta()));
    captured.push_str(&published.serialize_reference());
    captured.push_str(&format!("{store:?}"));

    fake.set_unreachable(true);
    let err = Blob::<Manifest>::rehydrate_from(&store, &published.serialize_reference())
        .expect_err("unreachable");
    captured.push_str(&format!("{err}{err:?}"));
    let plan_err = plan_reclaim(Path::new("/nonexistent-run-store"), &store);
    captured.push_str(&format!("{plan_err:?}"));

    assert!(
        !captured.contains(SENTINEL),
        "no credential value may appear in a reference, an event record, or an error: {captured}"
    );
}

fn argv(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}
