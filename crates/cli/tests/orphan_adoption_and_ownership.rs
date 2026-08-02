//! T109 · **orphan adoption, tombstones and ownership revocation.** Written first
//! (TDD). Needs the default-off `k8s` feature (which turns on `blob`, because the
//! shard's address is a blob-container path).
//!
//! T108's submission is idempotent inside one live orchestrator process. This
//! suite is about the process **dying**: pods keep running on other machines,
//! unaware their submitter is gone, and the next invocation has to decide what
//! they are. Resubmitting duplicates work that is already running — and, for a
//! task with side effects, duplicates the side effects; ignoring them leaks them
//! and double-counts cluster capacity.
//!
//! The mechanics are ADR 115 §5's, and each is asserted as an **ordering or an
//! absence** rather than as an end state:
//!
//! - adoption is a labels-only patch of the owner key — never a recreation, and
//!   the pod is compared field by field afterwards;
//! - a consumed outcome is tombstoned with the very key the discovery selector
//!   excludes, so a finished attempt cannot be adopted twice;
//! - revocation clears the owner **then** deletes, so a watcher can tell an
//!   orchestrator-initiated teardown from an external deletion.
//!
//! Every test drives T107's fake API surface; the real-cluster kill-restart is
//! T112's.

#![cfg(feature = "k8s")]

mod k8s_support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_cli::adoption::{AdoptionConfig, Adoptions, DiscoveryReport, discover};
use dagr_cli::driver::NodeRunner;
use dagr_cli::k8s_runner::{K8sNodeRunner, RemoteAttemptConfig};
use dagr_cli::pod_observer::PodObserver;
use dagr_cli::run_flow::AttemptTimer;
use dagr_cli::shard::{AttemptShard, ConsumedRef, ShardIdentity, ShardOutput};
use dagr_cli::submission_log::SubmissionLog;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{AttemptEvent, AttemptEventSink, Backoff, RetryConfig};
use dagr_core::resume::{PriorNode, PriorRun, ReferenceExistence, ResumeRefusal, plan_resume};
use dagr_k8s::adoption::BuildIdentity;
use dagr_k8s::api::{
    ApiFailure, CreatedPod, PodLifecycle, PodPhase, PodSnapshot, WatchDelivery,
};
use dagr_k8s::executor::{ClusterRetry, PodPlacement, PodSpec, pod_name};
use dagr_k8s::fake::{FakeApi, FakeControl, fake_api};
use dagr_k8s::identity::{
    AttemptIdentity, AttemptKey, LABEL_COMPLETE, LABEL_OWNER, TOMBSTONE_VALUE,
};
use dagr_k8s::observer::ObserverLimits;
use k8s_support::{FINGERPRINT, POLICY_HASH, RUN_ID, selector};
use serde_json::Value;

/// The tool version this suite's build claims, in the annotation and in the shard.
const TOOL_VERSION: &str = "dagr@1";
const IMAGE: &str = "registry.example/dagr@sha256:cafebabe";
const IMAGE_DIGEST: &str = "sha256:cafebabe";
/// The orchestrator that submitted the pods, and then died.
const FIRST_OWNER: &str = "orchestrator-1";
/// The one that comes back and reclaims them.
const SECOND_OWNER: &str = "orchestrator-2";

/// A full identity for one attempt of one node, annotated with **this** build —
/// the shared `k8s_support` fixture annotates a different image digest, and a
/// suite about build comparison cannot borrow a fixture that fails the comparison
/// for an unrelated reason.
fn identity(node: &str, attempt: u32, owner: &str) -> AttemptIdentity {
    AttemptIdentity {
        key: AttemptKey::new(RUN_ID, node, attempt),
        pipeline: "example-pipeline".to_string(),
        structural_fingerprint: FINGERPRINT.to_string(),
        policy_hash: POLICY_HASH.to_string(),
        tool_version: TOOL_VERSION.to_string(),
        image_digest: IMAGE_DIGEST.to_string(),
        owner: owner.to_string(),
    }
}

/// The build a restart compares a discovered pod's annotations against.
fn build() -> BuildIdentity {
    BuildIdentity::new(FINGERPRINT, TOOL_VERSION, IMAGE_DIGEST)
}

// ===========================================================================
// The fake platform: a `PodLifecycle` that records the ORDER of every call
// ===========================================================================

/// One call the orchestrator made to the platform, in order.
///
/// The order is the deliverable: "revocation clears the owner label **then**
/// deletes" is an ordering claim, and an end-state assertion would pass against an
/// implementation that deleted first.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Call {
    verb: &'static str,
    pod: String,
    labels: BTreeMap<String, Option<String>>,
}

#[derive(Default)]
struct LifecycleState {
    live: BTreeMap<String, PodSnapshot>,
    calls: Vec<Call>,
    next_uid: u32,
    next_version: u32,
    failing_patches: BTreeSet<String>,
}

/// A `PodLifecycle` over T107's fake world.
#[derive(Clone)]
struct Lifecycle {
    state: Arc<Mutex<LifecycleState>>,
    control: FakeControl,
}

impl Lifecycle {
    fn new(control: FakeControl) -> Self {
        Self {
            state: Arc::new(Mutex::new(LifecycleState::default())),
            control,
        }
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, LifecycleState> {
        self.state.lock().expect("lifecycle mutex")
    }

    /// Put a pod into the world without going through `create` — the pods a dead
    /// orchestrator left behind.
    fn seed(&self, pod: PodSnapshot) {
        self.guard().live.insert(pod.name.clone(), pod.clone());
        self.control.upsert(pod);
    }

    fn calls(&self) -> Vec<Call> {
        self.guard().calls.clone()
    }

    /// The call log as `(verb, pod)` pairs — what the ordering assertions read.
    fn trace(&self) -> Vec<(&'static str, String)> {
        self.calls()
            .iter()
            .map(|c| (c.verb, c.pod.clone()))
            .collect()
    }

    fn verb(&self, verb: &str) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|c| c.verb == verb)
            .map(|c| c.pod)
            .collect()
    }

    fn created(&self) -> Vec<String> {
        self.verb("create")
    }

    fn deleted(&self) -> Vec<String> {
        self.verb("delete")
    }

    fn patches(&self) -> Vec<Call> {
        self.calls().into_iter().filter(|c| c.verb == "patch").collect()
    }

    fn pod(&self, name: &str) -> Option<PodSnapshot> {
        self.guard().live.get(name).cloned()
    }

    /// Script a patch failure for one pod — a transient API error on the one call
    /// adoption cannot do without.
    fn fail_patch_of(&self, name: &str) {
        self.guard().failing_patches.insert(name.to_string());
    }
}

impl PodLifecycle for Lifecycle {
    fn create(
        &self,
        spec: &PodSpec,
    ) -> impl std::future::Future<Output = Result<CreatedPod, ApiFailure>> + Send {
        let (pod, uid) = {
            let mut guard = self.guard();
            guard.calls.push(Call {
                verb: "create",
                pod: spec.name.clone(),
                labels: BTreeMap::new(),
            });
            guard.next_uid += 1;
            let uid = format!("uid-{}", guard.next_uid);
            let mut pod = PodSnapshot::new(&spec.name, "100", PodPhase::Pending, &spec.identity);
            pod.uid = Some(uid.clone());
            pod.host = Some("kind-worker2".to_string());
            guard.live.insert(spec.name.clone(), pod.clone());
            (pod, uid)
        };
        self.control.upsert(pod.clone());
        let name = spec.name.clone();
        async move {
            Ok(CreatedPod {
                name,
                uid: Some(uid),
                host: pod.host.clone(),
            })
        }
    }

    fn delete(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        {
            let mut guard = self.guard();
            // The labels the pod carried **at the instant it was deleted** — what a
            // watch's `Deleted` delivery hands a reader, and the only thing that
            // distinguishes our teardown from somebody else's.
            let labels = guard.live.get(name).map_or_else(BTreeMap::new, |pod| {
                pod.labels
                    .iter()
                    .map(|(k, v)| (k.clone(), Some(v.clone())))
                    .collect()
            });
            guard.calls.push(Call {
                verb: "delete",
                pod: name.to_string(),
                labels,
            });
            guard.live.remove(name);
        }
        self.control.remove(name);
        async move { Ok(()) }
    }

    fn get(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<PodSnapshot>, ApiFailure>> + Send {
        let found = {
            let mut guard = self.guard();
            guard.calls.push(Call {
                verb: "get",
                pod: name.to_string(),
                labels: BTreeMap::new(),
            });
            guard.live.get(name).cloned()
        };
        async move { Ok(found) }
    }

    fn patch_labels(
        &self,
        name: &str,
        labels: &BTreeMap<String, Option<String>>,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        let (patched, refused) = {
            let mut guard = self.guard();
            guard.calls.push(Call {
                verb: "patch",
                pod: name.to_string(),
                labels: labels.clone(),
            });
            if guard.failing_patches.contains(name) {
                (None, true)
            } else {
                guard.next_version += 1;
                let version = format!("2{:03}", guard.next_version);
                let updated = guard.live.get_mut(name).map(|pod| {
                    for (key, value) in labels {
                        match value {
                            Some(value) => {
                                pod.labels.insert(key.clone(), value.clone());
                            }
                            None => {
                                pod.labels.remove(key);
                            }
                        }
                    }
                    // A real patch is a write, and a write moves the object
                    // version. Keeping that honest matters: the observer keys its
                    // de-duplication on `resourceVersion`.
                    pod.resource_version = version;
                    pod.clone()
                });
                (updated, false)
            }
        };
        if let Some(pod) = patched {
            self.control.upsert(pod);
        }
        async move {
            if refused {
                return Err(ApiFailure::api(500, "InternalError", "etcdserver: too many requests"));
            }
            Ok(())
        }
    }
}

// ===========================================================================
// The rest of the scaffolding
// ===========================================================================

#[derive(Clone, Default)]
struct RecordingTimer {
    waits: Arc<Mutex<Vec<Duration>>>,
}

impl AttemptTimer for RecordingTimer {
    fn sleep(
        &self,
        delay: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.waits.lock().expect("timer mutex").push(delay);
        Box::pin(std::future::ready(()))
    }
}

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<AttemptEvent>>>,
}

impl Recorder {
    fn events(&self) -> Vec<AttemptEvent> {
        self.events.lock().expect("recorder mutex").clone()
    }

    fn attempt_numbers(&self) -> Vec<u32> {
        self.events()
            .iter()
            .filter_map(|e| match e {
                AttemptEvent::AttemptStarted { attempt, .. } => Some(*attempt),
                _ => None,
            })
            .collect()
    }

    fn terminals(&self) -> Vec<TerminalState> {
        self.events()
            .iter()
            .filter_map(|e| match e {
                AttemptEvent::NodeTerminal { state, .. } => Some(*state),
                _ => None,
            })
            .collect()
    }
}

impl AttemptEventSink for Recorder {
    fn emit(&mut self, event: AttemptEvent) {
        self.events.lock().expect("recorder mutex").push(event);
    }
}

/// A private per-test temp root, removed on drop.
struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "dagr-cli-t109-{tag}-{}-{nanos}-{n}",
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

/// The fake platform plus the run's blob container and event stream.
struct World {
    _tmp: TempRoot,
    root: PathBuf,
    stream_path: PathBuf,
    api: FakeApi,
    control: FakeControl,
    lifecycle: Lifecycle,
}

fn world(tag: &str) -> World {
    let tmp = TempRoot::new(tag);
    let root = tmp.path().join("blobs");
    std::fs::create_dir_all(&root).expect("blob container");
    let stream_path = tmp.path().join("events.jsonl");
    let (api, control) = fake_api();
    let lifecycle = Lifecycle::new(control.clone());
    World {
        _tmp: tmp,
        root,
        stream_path,
        api,
        control,
        lifecycle,
    }
}

fn limits() -> ObserverLimits {
    ObserverLimits {
        stall_bound: Duration::from_secs(90),
        backoff_initial: Duration::from_millis(250),
        backoff_max: Duration::from_secs(30),
        max_consecutive_failures: 4,
        failure_window: Duration::from_mins(5),
        watch_timeout_secs: 270,
    }
}

fn config(root: &Path, owner: &str) -> RemoteAttemptConfig {
    RemoteAttemptConfig {
        pipeline: "example-pipeline".to_string(),
        namespace: "dagr".to_string(),
        image: IMAGE.to_string(),
        image_digest: IMAGE_DIGEST.to_string(),
        structural_fingerprint: FINGERPRINT.to_string(),
        policy_hash: POLICY_HASH.to_string(),
        tool_version: TOOL_VERSION.to_string(),
        owner: owner.to_string(),
        blob_container: root.to_path_buf(),
        inputs: Vec::new(),
        declared_arity: 0,
        placement: PodPlacement::default(),
        cluster_retry: ClusterRetry::Disabled,
        launch_retries: 0,
        pre_start_bound: Duration::from_millis(80),
        retry: RetryConfig::new(1, Backoff::new(Duration::ZERO, 2.0, Duration::MAX)),
        timeout: None,
        command: vec!["dagr".to_string(), "exec-node".to_string()],
    }
}

fn adoption_config(owner: &str, must_run: &[&str]) -> AdoptionConfig {
    AdoptionConfig {
        run_id: RUN_ID.to_string(),
        owner: owner.to_string(),
        build: build(),
        must_run: must_run.iter().map(|n| (*n).to_string()).collect(),
        prior_run_id: None,
    }
}

fn runner(
    w: &World,
    node: &str,
    observer: &PodObserver,
    log: &SubmissionLog,
    adoptions: Adoptions,
    owner: &str,
) -> K8sNodeRunner<Lifecycle> {
    K8sNodeRunner::new(
        node,
        RUN_ID,
        w.lifecycle.clone(),
        observer.handle(),
        log.handle(),
        Arc::new(RecordingTimer::default()) as Arc<dyn AttemptTimer>,
        config(&w.root, owner),
    )
    .with_adoptions(adoptions)
}

fn shard_identity(node: &str, attempt: u32) -> ShardIdentity {
    ShardIdentity::new(
        RUN_ID,
        node,
        attempt,
        FINGERPRINT,
        POLICY_HASH,
        TOOL_VERSION,
    )
    .image_digest(IMAGE_DIGEST)
}

/// Write the shard a pod-side attempt would have left behind.
fn write_shard(root: &Path, node: &str, attempt: u32, state: TerminalState) {
    let closing = if state == TerminalState::Succeeded {
        AttemptEvent::AttemptSucceeded {
            node: node.to_string(),
            attempt,
        }
    } else {
        AttemptEvent::AttemptFailed {
            node: node.to_string(),
            attempt,
        }
    };
    let events = vec![
        AttemptEvent::AttemptStarted {
            node: node.to_string(),
            attempt,
        },
        closing,
        AttemptEvent::NodeTerminal {
            node: node.to_string(),
            state,
        },
    ];
    let token = if state == TerminalState::Succeeded {
        "succeeded"
    } else {
        "failed"
    };
    AttemptShard::new(shard_identity(node, attempt), token)
        .with_inputs(Vec::<ConsumedRef>::new())
        .with_records(dagr_cli::shard::records_for(RUN_ID, &events))
        .with_output(ShardOutput::new("dagr-blob+local://blobs/sha256/abc").content_hash("sha256:abc"))
        .write_atomically(root, false)
        .expect("the shard is written");
}

/// Move the pod named `name` to `phase` and deliver the modification.
async fn transition(control: &FakeControl, pod: &PodSnapshot, phase: PodPhase) {
    static VERSION: AtomicU32 = AtomicU32::new(300);
    let version = VERSION.fetch_add(1, Ordering::SeqCst).to_string();
    let mut next = pod.clone();
    next.resource_version = version;
    next.phase = phase;
    control.upsert(next.clone());
    control.deliver(WatchDelivery::Modified(next)).await;
}

fn stream_records(path: &Path) -> Vec<Value> {
    let bytes = std::fs::read(path).unwrap_or_default();
    dagr_artifact::event_stream::read_records(&bytes)
        .expect("the stream parses")
        .records
}

/// A live pod for one attempt, as a dead orchestrator would have left it.
fn orphan(node: &str, attempt: u32, phase: PodPhase, name: Option<&str>) -> PodSnapshot {
    let id = identity(node, attempt, FIRST_OWNER);
    let name = name.map_or_else(|| pod_name(&id.key), str::to_string);
    let mut pod = PodSnapshot::new(name, "100", phase, &id);
    pod.uid = Some(format!("uid-orphan-{node}-{attempt}"));
    pod.host = Some("kind-worker7".to_string());
    pod
}

// ===========================================================================
// Adoption happens instead of duplication
// ===========================================================================

/// **Definition of done: adoption patches *only* the owner label; the pod is
/// otherwise unmodified and is never recreated.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_pod_is_adopted_by_a_fresh_orchestrator_and_the_object_is_otherwise_untouched() {
    let w = world("adopt");
    let before = orphan("extract", 1, PodPhase::Running, None);
    w.lifecycle.seed(before.clone());

    let report: DiscoveryReport = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");

    assert_eq!(
        report.adopted,
        vec![before.name.clone()],
        "the orphan is adopted"
    );
    assert!(
        w.lifecycle.created().is_empty(),
        "no second pod is created — the work is already running"
    );
    assert!(
        w.lifecycle.deleted().is_empty(),
        "and the running pod is not deleted and resubmitted"
    );

    let patches = w.lifecycle.patches();
    assert_eq!(patches.len(), 1, "one labels-only patch: {patches:?}");
    assert_eq!(patches[0].pod, before.name);
    assert_eq!(
        patches[0].labels.keys().collect::<Vec<_>>(),
        vec![LABEL_OWNER],
        "ONLY the owner key — adoption never rewrites anything the pod is running"
    );

    let after = w.lifecycle.pod(&before.name).expect("the pod is still there");
    assert_eq!(after.uid, before.uid, "the same object: never recreated");
    assert_eq!(after.phase, before.phase, "its phase is untouched");
    assert_eq!(after.host, before.host, "it is still on the same node");
    assert_eq!(
        after.annotations, before.annotations,
        "annotations are authoritative and adoption does not rewrite them"
    );
    let mut expected = before.labels.clone();
    expected.insert(LABEL_OWNER.to_string(), SECOND_OWNER.to_string());
    assert_eq!(
        after.labels, expected,
        "exactly one label moved, and it is the owner"
    );
}

/// **Definition of done: an adopted pod's terminal transition and shard flow
/// through the same path as a submitted pod's.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adopted_pods_shard_is_replayed_and_the_node_reaches_the_shards_terminal_state() {
    let w = world("replay");
    let pod = orphan("extract", 1, PodPhase::Running, None);
    w.lifecycle.seed(pod.clone());

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");

    let log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut r = runner(&w, "extract", &observer, &log, report.adoptions, SECOND_OWNER);
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();

    let control = w.control.clone();
    let root = w.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded);
        transition(&control, &pod, PodPhase::Succeeded).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert!(
        w.lifecycle.created().is_empty(),
        "the adopted pod's outcome was consumed; nothing was resubmitted"
    );
    let events = sink.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, AttemptEvent::AttemptSucceeded { attempt: 1, .. })),
        "the shard's records were replayed through the injected sink, exactly as a \
         freshly submitted pod's would be: {events:?}"
    );
    assert_eq!(
        r.durable_reference().as_deref(),
        Some("dagr-blob+local://blobs/sha256/abc"),
        "…and its output reference reached the hook the driver already reads"
    );

    let observed: Vec<Value> = stream_records(&w.stream_path)
        .into_iter()
        .filter(|r| r["kind"] == "attempt-submitted" && r.get("observed_uid").is_some())
        .collect();
    assert_eq!(observed.len(), 1, "one submission record, additively completed");
    assert_eq!(
        observed[0]["observed_uid"], "uid-orphan-extract-1",
        "the ADOPTED pod's real identity is what the record carries — intent and \
         reality are two facts, and reality here is a pod this process did not create"
    );
    let _ = observer.shutdown(Duration::from_secs(5)).await;
}

/// **Definition of done: `seq` stays gapless and the stream folds.**
#[test]
fn a_run_that_adopted_a_pod_still_has_gapless_seq_and_folds_cleanly() {
    use dagr_core::flow::Flow;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime for the observer");
    let _entered = rt.enter();

    let w = world("fold");
    let store = w._tmp.path().join("store");
    std::fs::create_dir_all(&store).expect("run store");
    let pod = orphan("extract", 1, PodPhase::Running, None);
    w.lifecycle.seed(pod.clone());

    let report = rt
        .block_on(discover(
            &w.api,
            &w.lifecycle,
            &adoption_config(SECOND_OWNER, &["extract"]),
        ))
        .expect("discovery lists the run's pods");

    let stream = StreamSink::default();
    let log = SubmissionLog::over(stream.clone(), RUN_ID, "example-pipeline");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());

    let mut flow = Flow::new();
    let _ = flow.register_source("extract", &Seven);
    let pipeline = flow.finish();

    let remote = K8sNodeRunner::new(
        "extract",
        RUN_ID,
        w.lifecycle.clone(),
        observer.handle(),
        log.handle(),
        Arc::new(RecordingTimer::default()) as Arc<dyn AttemptTimer>,
        config(&w.root, SECOND_OWNER),
    )
    .with_adoptions(report.adoptions);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert("extract".to_string(), Box::new(remote));
    let plan = dagr_cli::driver::RunPlan::new(pipeline, runners);

    let control = w.control.clone();
    let root = w.root.clone();
    rt.spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded);
        transition(&control, &pod, PodPhase::Succeeded).await;
    });

    let report_out = dagr_cli::driver::drive(
        &dagr_cli::driver::RunConfig::new(store.to_string_lossy().to_string()).run_id(RUN_ID),
        "example-pipeline",
        Ok(plan),
        &[],
        log.sink(),
        TickClock::default(),
    );
    assert_eq!(
        report_out.terminal_states.get("extract"),
        Some(&TerminalState::Succeeded)
    );

    let bytes = stream.bytes.lock().expect("stream mutex").clone();
    let records = dagr_artifact::event_stream::read_records(&bytes)
        .expect("the stream parses")
        .records;
    for (i, record) in records.iter().enumerate() {
        assert_eq!(
            record["seq"].as_u64(),
            Some(u64::try_from(i).expect("a record index fits a u64")),
            "gapless, strictly increasing seq — the orchestrator is still the \
             single writer, adoption or not"
        );
    }
    let artifact = dagr_artifact::fold::fold_stream(&bytes, &["extract".to_string()])
        .expect("the stream folds cleanly");
    assert_eq!(artifact.overall_outcome(), "succeeded");
    assert_eq!(artifact.attempts().len(), 1, "one attempt, not two");

    rt.block_on(async { observer.shutdown(Duration::from_secs(5)).await })
        .ok();
}

// ===========================================================================
// Tombstones prevent double adoption
// ===========================================================================

/// **Definition of done: consumed outcomes are tombstoned with the key discovery
/// excludes.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumed_outcome_is_tombstoned_and_a_later_discovery_excludes_it() {
    let w = world("tombstone");
    let pod = orphan("extract", 1, PodPhase::Running, None);
    w.lifecycle.seed(pod.clone());

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");

    let log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut r = runner(&w, "extract", &observer, &log, report.adoptions, SECOND_OWNER);
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();

    let control = w.control.clone();
    let root = w.root.clone();
    let name = pod.name.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded);
        transition(&control, &pod, PodPhase::Succeeded).await;
    });
    assert_eq!(r.run(&ctx, &mut sink).await, TerminalState::Succeeded);
    driver.await.expect("the driver task completes");

    let tombstoned = w.lifecycle.pod(&name).expect("the pod is still there");
    assert_eq!(
        tombstoned.labels.get(LABEL_COMPLETE).map(String::as_str),
        Some(TOMBSTONE_VALUE),
        "the outcome has been consumed, so the pod carries the completion key"
    );

    // A second discovery — the next restart — sees nothing to adopt.
    let again = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config("orchestrator-3", &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");
    assert!(
        again.adopted.is_empty(),
        "a finished attempt is never adopted a second time"
    );
    assert_eq!(again.tombstoned, vec![name], "it is reported as consumed");
    let _ = observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tombstoned_pod_whose_deletion_was_deferred_is_never_adopted_by_the_submission_probe() {
    // The tombstone is the belt to deletion's braces: a delete that was deferred or
    // failed leaves a pod sitting on the attempt's own object name, and T108's
    // in-process idempotency probe would otherwise "adopt" it and consume its
    // stale shard a second time.
    let w = world("stale");
    let mut stale = orphan("extract", 1, PodPhase::Succeeded, None);
    stale
        .labels
        .insert(LABEL_COMPLETE.to_string(), TOMBSTONE_VALUE.to_string());
    w.lifecycle.seed(stale.clone());
    // The shard the earlier attempt left behind, which must NOT be re-consumed.
    write_shard(&w.root, "extract", 1, TerminalState::Failed);

    let log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut r = runner(&w, "extract", &observer, &log, Adoptions::none(), SECOND_OWNER);
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();

    let control = w.control.clone();
    let root = w.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded);
        let fresh = PodSnapshot::new(
            pod_name(&AttemptKey::new(RUN_ID, "extract", 1)),
            "400",
            PodPhase::Succeeded,
            &identity("extract", 1, SECOND_OWNER),
        );
        transition(&control, &fresh, PodPhase::Succeeded).await;
    });
    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert_eq!(
        w.lifecycle.created().len(),
        1,
        "the tombstoned pod was not adopted; a fresh one was submitted for the \
         attempt instead"
    );
    let trace = w.lifecycle.trace();
    let stale_calls: Vec<_> = trace.iter().filter(|(_, p)| *p == stale.name).collect();
    assert!(
        stale_calls
            .iter()
            .any(|(verb, _)| *verb == "patch" || *verb == "delete"),
        "the consumed pod squatting on the attempt's name is revoked to free it: \
         {trace:?}"
    );
    let _ = observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// Refusal on mismatch
// ===========================================================================

/// **Definition of done: a pod whose annotated fingerprint, tool version, or image
/// digest differs is refused, left untouched, and reported with both values
/// named.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_from_a_different_build_is_left_alone_and_fails_its_node_naming_both_values() {
    let w = world("foreign");
    let mut foreign = orphan("extract", 1, PodPhase::Running, None);
    foreign.annotations.insert(
        "dagr.io/structural-fingerprint".to_string(),
        "sf-a-different-graph".to_string(),
    );
    w.lifecycle.seed(foreign.clone());

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");

    assert!(report.adopted.is_empty(), "a different program's pod is never adopted");
    assert_eq!(report.refused, vec![foreign.name.clone()], "it is reported");
    assert!(
        w.lifecycle.patches().is_empty() && w.lifecycle.deleted().is_empty(),
        "and LEFT ALONE — deleting another program's pod is not dagr's call: {:?}",
        w.lifecycle.trace()
    );
    let after = w.lifecycle.pod(&foreign.name).expect("still running");
    assert_eq!(after.labels, foreign.labels, "not one label was touched");

    let log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut r = runner(&w, "extract", &observer, &log, report.adoptions, SECOND_OWNER);
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();
    let state = r.run(&ctx, &mut sink).await;

    assert_eq!(
        state,
        TerminalState::Failed,
        "the node fails with a classified error rather than guessing"
    );
    let failure = r.last_failure().expect("a classified failure").to_string();
    assert!(
        failure.contains(FINGERPRINT) && failure.contains("sf-a-different-graph"),
        "the error names BOTH fingerprints: {failure}"
    );
    assert!(
        w.lifecycle.created().is_empty(),
        "and nothing was launched alongside it"
    );
    let _ = observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mismatched_tool_version_or_image_digest_is_refused_likewise() {
    for (key, value, expected) in [
        ("dagr.io/tool-version", "dagr@2", TOOL_VERSION),
        ("dagr.io/image-digest", "sha256:decafbad", IMAGE_DIGEST),
    ] {
        let w = world("foreign-build");
        let mut foreign = orphan("extract", 1, PodPhase::Running, None);
        foreign
            .annotations
            .insert(key.to_string(), value.to_string());
        w.lifecycle.seed(foreign.clone());

        let report = discover(
            &w.api,
            &w.lifecycle,
            &adoption_config(SECOND_OWNER, &["extract"]),
        )
        .await
        .expect("discovery lists the run's pods");

        assert!(report.adopted.is_empty(), "{key}: never adopted");
        assert!(
            w.lifecycle.patches().is_empty(),
            "{key}: left untouched"
        );
        let named = report.report_for(&AttemptKey::new(RUN_ID, "extract", 1));
        assert!(
            named.as_ref().is_some_and(|r| r.contains(value) && r.contains(expected)),
            "{key}: both values are named: {named:?}"
        );
    }
}

// ===========================================================================
// Revocation ordering
// ===========================================================================

/// **Definition of done: revocation clears the owner label *then* deletes, with
/// the ordering asserted.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_clears_the_owner_label_before_it_issues_the_delete() {
    let w = world("revoke");
    // Two pods claiming the same attempt: one is adopted, the loser is revoked.
    let keeper = orphan("extract", 1, PodPhase::Running, Some("dagr-aaaa-extract-1"));
    let loser = orphan("extract", 1, PodPhase::Running, Some("dagr-zzzz-extract-1"));
    w.lifecycle.seed(keeper.clone());
    w.lifecycle.seed(loser.clone());

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");
    assert_eq!(report.revoked, vec![loser.name.clone()]);

    let trace: Vec<(&str, String)> = w
        .lifecycle
        .trace()
        .into_iter()
        .filter(|(_, pod)| *pod == loser.name)
        .collect();
    assert_eq!(
        trace,
        vec![
            ("patch", loser.name.clone()),
            ("delete", loser.name.clone()),
        ],
        "patch THEN delete — an end-state assertion would pass against an \
         implementation that deleted first, and a watcher would then read the \
         disappearance as somebody else's"
    );

    let cleared = w
        .lifecycle
        .calls()
        .into_iter()
        .find(|c| c.verb == "patch" && c.pod == loser.name)
        .expect("the revocation patch");
    assert_eq!(
        cleared.labels.get(LABEL_OWNER),
        Some(&None),
        "the owner is CLEARED, which is what makes the delete legible"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watcher_distinguishes_a_revoked_pod_from_an_externally_deleted_one() {
    use dagr_k8s::adoption::{DeletionOrigin, deletion_origin};

    let w = world("origin");
    let keeper = orphan("extract", 1, PodPhase::Running, Some("dagr-aaaa-extract-1"));
    let loser = orphan("extract", 1, PodPhase::Running, Some("dagr-zzzz-extract-1"));
    w.lifecycle.seed(keeper.clone());
    w.lifecycle.seed(loser.clone());

    discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");

    // The labels the loser carried at the instant of the delete — exactly what a
    // watch's `Deleted` delivery hands a reader.
    let at_delete = w
        .lifecycle
        .calls()
        .into_iter()
        .find(|c| c.verb == "delete" && c.pod == loser.name)
        .expect("the loser was deleted");
    let observed: BTreeMap<String, String> = at_delete
        .labels
        .iter()
        .filter_map(|(key, value)| value.clone().map(|value| (key.clone(), value)))
        .collect();

    assert!(
        !observed.is_empty(),
        "the pod still carried its other labels — revocation clears the OWNER, not \
         the identity"
    );
    assert_eq!(
        deletion_origin(&observed),
        DeletionOrigin::Revoked,
        "it had already lost its owner when it disappeared, so the teardown reads \
         as ours: {observed:?}"
    );
    assert_eq!(
        deletion_origin(&keeper.labels),
        DeletionOrigin::External,
        "a pod that still carries an owner when it goes was deleted by somebody else"
    );
}

// ===========================================================================
// Ambiguity
// ===========================================================================

/// **Definition of done: two pods for one attempt key resolve deterministically to
/// one adoption and one revocation, with exactly one terminal state.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_live_pods_for_one_attempt_produce_one_adoption_one_revocation_and_one_terminal() {
    let w = world("ambiguous");
    let keeper = orphan("extract", 1, PodPhase::Running, Some("dagr-aaaa-extract-1"));
    let loser = orphan("extract", 1, PodPhase::Running, Some("dagr-zzzz-extract-1"));
    w.lifecycle.seed(keeper.clone());
    w.lifecycle.seed(loser.clone());

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");
    assert_eq!(report.adopted, vec![keeper.name.clone()]);
    assert_eq!(report.revoked, vec![loser.name.clone()]);

    let log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut r = runner(&w, "extract", &observer, &log, report.adoptions, SECOND_OWNER);
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();

    let control = w.control.clone();
    let root = w.root.clone();
    let keeper_pod = keeper.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded);
        transition(&control, &keeper_pod, PodPhase::Succeeded).await;
    });
    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert_eq!(
        sink.terminals().len(),
        1,
        "exactly one terminal state, however many pods claimed the attempt: {:?}",
        sink.events()
    );
    assert!(w.lifecycle.created().is_empty(), "and nothing was resubmitted");
    let _ = observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_terminal_with_a_readable_shard_at_discovery_is_consumed_without_re_running() {
    let w = world("already-done");
    // The pod finished while the orchestrator was dead. Its shard is on disk.
    let pod = orphan("extract", 1, PodPhase::Succeeded, None);
    w.lifecycle.seed(pod.clone());
    write_shard(&w.root, "extract", 1, TerminalState::Succeeded);

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");
    assert_eq!(report.adopted, vec![pod.name.clone()]);

    let log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut r = runner(&w, "extract", &observer, &log, report.adoptions, SECOND_OWNER);
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();

    let state = r.run(&ctx, &mut sink).await;

    assert_eq!(
        state,
        TerminalState::Succeeded,
        "the outcome is consumed from the shard the pod already wrote"
    );
    assert!(
        w.lifecycle.created().is_empty(),
        "the node is NOT re-run — the whole point"
    );
    assert_eq!(sink.attempt_numbers(), vec![1], "one attempt, replayed");
    let _ = observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// Composition with resume
// ===========================================================================

/// A two-node pipeline, for the resume-composition tests.
fn resumable() -> dagr_core::flow::Pipeline {
    use dagr_core::flow::Flow;
    let mut flow = Flow::new();
    let _ = flow.register_source("extract", &Seven);
    let _ = flow.register_source("load", &Seven);
    flow.finish()
}

fn prior_run(pipeline: &dagr_core::flow::Pipeline, load_state: TerminalState) -> PriorRun {
    let fingerprint = pipeline.fingerprint();
    PriorRun {
        structural_fingerprint: fingerprint.structural(),
        policy_hash: fingerprint.policy(),
        algorithm_version: fingerprint.algorithm_version(),
        tool_version: TOOL_VERSION.to_string(),
        nodes: BTreeMap::from([
            (
                "extract".to_string(),
                PriorNode {
                    terminal: TerminalState::Succeeded,
                    durable_reference: None,
                    durable_reference_content_hash: None,
                    originating_run: "prior-run".to_string(),
                },
            ),
            (
                "load".to_string(),
                PriorNode {
                    terminal: load_state,
                    durable_reference: None,
                    durable_reference_content_hash: None,
                    originating_run: "prior-run".to_string(),
                },
            ),
        ]),
    }
}

/// **Definition of done: `satisfied-from-prior` nodes seek no pod.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_satisfied_from_prior_node_seeks_no_pod_and_a_must_run_one_is_adopted() {
    let pipeline = resumable();
    let prior = prior_run(&pipeline, TerminalState::Failed);
    let plan = plan_resume(&pipeline, &prior, TOOL_VERSION, |_, _, _| {
        ReferenceExistence::Present
    })
    .expect("the prior run resumes");
    assert!(plan.satisfied_from_prior().contains_key("extract"));
    assert!(plan.must_run().contains("load"));

    let w = world("resume");
    let satisfied = orphan("extract", 1, PodPhase::Running, None);
    let must_run = orphan("load", 1, PodPhase::Running, None);
    w.lifecycle.seed(satisfied.clone());
    w.lifecycle.seed(must_run.clone());

    let mut config = adoption_config(SECOND_OWNER, &[]);
    config.must_run = plan.must_run().iter().cloned().collect();
    let report = discover(&w.api, &w.lifecycle, &config)
        .await
        .expect("discovery lists the run's pods");

    assert_eq!(
        report.adopted,
        vec![must_run.name.clone()],
        "the must-run node's live pod is adopted rather than resubmitted"
    );
    assert!(
        !report.adopted.contains(&satisfied.name),
        "a satisfied-from-prior node has no runner at all, so no pod is sought for it"
    );
    assert!(
        w.lifecycle
            .trace()
            .iter()
            .all(|(_, pod)| *pod != satisfied.name),
        "and nothing was done to it: {:?}",
        w.lifecycle.trace()
    );
    assert_eq!(report.unclaimed, vec![satisfied.name], "it is reported");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resume_refusal_attempts_no_adoption() {
    let pipeline = resumable();
    let mut prior = prior_run(&pipeline, TerminalState::Failed);
    prior.structural_fingerprint ^= 0xdead_beef;

    let w = world("refused-resume");
    w.lifecycle
        .seed(orphan("load", 1, PodPhase::Running, None));

    // The composition, in the order the run path has it: resume plans first, and
    // discovery is downstream of a plan that succeeded.
    let refusal = match plan_resume(&pipeline, &prior, TOOL_VERSION, |_, _, _| {
        ReferenceExistence::Present
    }) {
        Ok(plan) => {
            let mut config = adoption_config(SECOND_OWNER, &[]);
            config.must_run = plan.must_run().iter().cloned().collect();
            discover(&w.api, &w.lifecycle, &config)
                .await
                .expect("discovery lists the run's pods");
            panic!("a structural mismatch must refuse");
        }
        Err(refusal) => refusal,
    };

    assert!(
        matches!(refusal, ResumeRefusal::StructuralMismatch { .. }),
        "the refusal path is unchanged: {refusal}"
    );
    assert!(
        w.lifecycle.calls().is_empty(),
        "no adoption was attempted — not a list, not a patch, not a delete: {:?}",
        w.lifecycle.trace()
    );
    assert_eq!(w.control.lists(), 0, "the API was never even listed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_run_revokes_the_prior_runs_pods_rather_than_adopting_them() {
    // The recorded resolution of this ticket's first open question: adoption
    // requires the SAME run id, because resume mints a new one and a resumed run
    // adopting the killed run's pods would blur two runs' event streams. The
    // consequence is stated rather than hidden — the prior run's pods are revoked.
    const PRIOR_RUN: &str = "0197aaaa-1111-7222-8333-444455556666";

    let w = world("prior-run");
    let mut stale = orphan("load", 1, PodPhase::Running, Some("dagr-prior-load-1"));
    stale
        .labels
        .insert("dagr.io/run-id".to_string(), PRIOR_RUN.to_string());
    w.lifecycle.seed(stale.clone());
    let ours = orphan("load", 1, PodPhase::Running, None);
    w.lifecycle.seed(ours.clone());

    let mut config = adoption_config(SECOND_OWNER, &["load"]);
    config.prior_run_id = Some(PRIOR_RUN.to_string());
    let report = discover(&w.api, &w.lifecycle, &config)
        .await
        .expect("discovery lists the run's pods");

    assert_eq!(
        report.adopted,
        vec![ours.name.clone()],
        "only this run's pods are adopted"
    );
    assert_eq!(
        report.prior_revoked,
        vec![stale.name.clone()],
        "the killed run's pods are revoked, not adopted"
    );
    let trace: Vec<(&str, String)> = w
        .lifecycle
        .trace()
        .into_iter()
        .filter(|(_, pod)| *pod == stale.name)
        .collect();
    assert_eq!(
        trace,
        vec![("patch", stale.name.clone()), ("delete", stale.name)],
        "…by the same patch-then-delete every revocation uses"
    );
}

// ===========================================================================
// Kill and restart, for real
// ===========================================================================

/// **Definition of done: a kill-and-restart test shows every node executed exactly
/// once.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_orchestrator_killed_mid_run_restarts_and_the_node_is_executed_exactly_once() {
    let w = world("restart");

    // --- process 1: submits the pod, then dies -----------------------------
    let first_log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens");
    let first_observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut first = runner(
        &w,
        "extract",
        &first_observer,
        &first_log,
        Adoptions::none(),
        FIRST_OWNER,
    );
    let ctx = RunContext::for_test();
    let killed = tokio::spawn(async move {
        let mut sink = Recorder::default();
        first.run(&ctx, &mut sink).await
    });
    w.control.await_watch().await;
    // Wait until the pod exists — the orchestrator is killed with work live.
    let name = pod_name(&AttemptKey::new(RUN_ID, "extract", 1));
    while w.lifecycle.pod(&name).is_none() {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    killed.abort();
    let _ = first_observer.shutdown(Duration::from_secs(5)).await;
    assert_eq!(w.lifecycle.created(), vec![name.clone()], "one pod exists");

    // --- process 2: a fresh orchestrator, same run id ----------------------
    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery lists the run's pods");
    assert_eq!(report.adopted, vec![name.clone()], "the orphan is reclaimed");
    assert_eq!(
        w.lifecycle
            .pod(&name)
            .and_then(|p| p.labels.get(LABEL_OWNER).cloned())
            .as_deref(),
        Some(SECOND_OWNER),
        "ownership moved to the process that is now waiting on it"
    );

    let second_log = SubmissionLog::open(&w.stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log reopens");
    let second_observer = PodObserver::spawn(w.api.clone(), selector(), limits());
    let mut second = runner(
        &w,
        "extract",
        &second_observer,
        &second_log,
        report.adoptions,
        SECOND_OWNER,
    );
    let mut sink = Recorder::default();
    let ctx = RunContext::for_test();

    let control = w.control.clone();
    let root = w.root.clone();
    let live = w.lifecycle.pod(&name).expect("the adopted pod");
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded);
        transition(&control, &live, PodPhase::Succeeded).await;
    });
    let state = second.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert_eq!(
        w.lifecycle.created(),
        vec![name],
        "ONE pod across both processes — the node executed exactly once"
    );
    assert_eq!(
        sink.attempt_numbers(),
        vec![1],
        "and exactly one attempt record: {:?}",
        sink.events()
    );
    let _ = second_observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// A failed ownership patch degrades to T108's in-process idempotency
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ownership_patch_that_fails_leaves_the_pod_unadopted_and_reported() {
    let w = world("patch-fails");
    let pod = orphan("extract", 1, PodPhase::Running, None);
    w.lifecycle.seed(pod.clone());
    w.lifecycle.fail_patch_of(&pod.name);

    let report = discover(
        &w.api,
        &w.lifecycle,
        &adoption_config(SECOND_OWNER, &["extract"]),
    )
    .await
    .expect("discovery still completes — one pod's patch is not the run's failure");

    assert!(report.adopted.is_empty(), "ownership was not taken");
    assert_eq!(
        report.unpatched,
        vec![pod.name.clone()],
        "and the pod is named, so the degradation is visible rather than silent"
    );
    assert!(
        w.lifecycle.deleted().is_empty(),
        "the running pod is emphatically not deleted because a label write failed"
    );
}

// ===========================================================================
// Local scaffolding the driver-level test needs
// ===========================================================================

/// A local source, so the driver-level test has a real pipeline to assemble.
struct Seven;

impl dagr_core::stable_name::StableName for Seven {
    const STABLE_NAME: &'static str = "t109.Seven";
}

impl dagr_core::task::Task for Seven {
    type Input = ();
    type Output = u64;
    async fn run(
        &mut self,
        _ctx: &RunContext,
        _i: (),
    ) -> Result<u64, dagr_core::TaskError> {
        Ok(7)
    }
}

/// A monotonic clock that ticks once per read — enough for the driver's offsets.
#[derive(Default)]
struct TickClock(std::sync::atomic::AtomicU64);

impl dagr_artifact::event_stream::MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.0.fetch_add(1_000, Ordering::SeqCst)
    }
}

#[derive(Clone, Default)]
struct StreamSink {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl dagr_artifact::event_stream::EventSink for StreamSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.bytes
            .lock()
            .expect("stream mutex")
            .extend_from_slice(line);
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
