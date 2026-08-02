//! T108 · **the Kubernetes node runner and the two retry budgets.** Written first
//! (TDD). Needs the default-off `k8s` feature (which turns on `blob`, because the
//! shard's address is a blob-container path).
//!
//! ADR 115's central claim is that `NodeRunner` is *already* the "where does this
//! node run" seam, so remote execution is one more implementation of it and no
//! driver code path is special-cased. This suite drives the real `drive()` loop
//! with a `K8sNodeRunner` in the runner map and asserts the properties the ticket
//! names, against T107's fake API surface — no cluster is involved anywhere (the
//! real-cluster run is T112).
//!
//! The two properties that are genuinely new:
//!
//! - **Write-ahead ordering.** The submission record is durably flushed *before*
//!   the create call. Asserted by ordering, not by content: the fake's create hook
//!   reads the stream off disk and fails if the record is not already there.
//! - **Two retry budgets.** A pod that never *started* is an infrastructure
//!   failure charged to `--dagr.pod-launch-retries`, emitting **no** user-visible
//!   attempt; a pod that started and whose task failed consumes
//!   `NodePolicy::retries` with T102's real backoff.

#![cfg(feature = "k8s")]

mod k8s_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::ConsumedInput;
use dagr_cli::k8s_runner::{K8sNodeRunner, RemoteAttemptConfig, RemoteLaunchError};
use dagr_cli::pod_observer::PodObserver;
use dagr_cli::run_flow::AttemptTimer;
use dagr_cli::shard::{AttemptShard, ConsumedRef, ShardIdentity, ShardOutput};
use dagr_cli::submission_log::SubmissionLog;
use dagr_core::context::TerminalState;
use dagr_core::execution::{AttemptEvent, AttemptEventSink, Backoff, RetryConfig};
use dagr_k8s::api::{ApiFailure, CreatedPod, PodApi, PodLifecycle, PodPhase, PodSnapshot};
use dagr_k8s::executor::{ClusterRetry, PodPlacement, PodSpec, pod_name};
use dagr_k8s::fake::{FakeControl, fake_api};
use dagr_k8s::identity::AttemptKey;
use dagr_k8s::observer::ObserverLimits;
use k8s_support::{FINGERPRINT, POLICY_HASH, RUN_ID, identity, selector};
use serde_json::Value;

const TOOL_VERSION: &str = "dagr@1";
const IMAGE: &str = "registry.example/dagr@sha256:cafebabe";
const IMAGE_DIGEST: &str = "sha256:cafebabe";

// ===========================================================================
// Test scaffolding: a scripted pod lifecycle over T107's fake world
// ===========================================================================

type CreateHook = Arc<dyn Fn(&PodSpec) + Send + Sync>;

/// What the fake platform does with the next `create`.
#[derive(Clone)]
enum CreateOutcome {
    /// The pod is admitted and enters `Pending`.
    Admitted,
    /// The API call itself fails (a quota rejection, an admission webhook).
    Rejected(ApiFailure),
}

#[derive(Default)]
struct LifecycleState {
    created: Vec<String>,
    deleted: Vec<String>,
    script: Vec<CreateOutcome>,
    live: BTreeMap<String, PodSnapshot>,
}

/// A `PodLifecycle` over the fake world: `create` upserts a `Pending` pod that the
/// observer's watch can then be driven through, `delete` removes it, and `get`
/// answers the adoption probe.
#[derive(Clone)]
struct ScriptedLifecycle {
    state: Arc<Mutex<LifecycleState>>,
    control: FakeControl,
    on_create: Option<CreateHook>,
    next_uid: Arc<AtomicU32>,
}

impl ScriptedLifecycle {
    fn new(control: FakeControl) -> Self {
        Self {
            state: Arc::new(Mutex::new(LifecycleState::default())),
            control,
            on_create: None,
            next_uid: Arc::new(AtomicU32::new(1)),
        }
    }

    fn with_create_hook(mut self, hook: CreateHook) -> Self {
        self.on_create = Some(hook);
        self
    }

    fn script(&self, outcomes: Vec<CreateOutcome>) {
        self.state.lock().expect("lifecycle mutex").script = outcomes;
    }

    /// Pre-seed a live pod for the adoption test, without going through `create`.
    fn adopt_target(&self, pod: PodSnapshot) {
        self.state
            .lock()
            .expect("lifecycle mutex")
            .live
            .insert(pod.name.clone(), pod.clone());
        self.control.upsert(pod);
    }

    fn created(&self) -> Vec<String> {
        self.state.lock().expect("lifecycle mutex").created.clone()
    }

    fn deleted(&self) -> Vec<String> {
        self.state.lock().expect("lifecycle mutex").deleted.clone()
    }
}

impl PodLifecycle for ScriptedLifecycle {
    fn create(
        &self,
        spec: &PodSpec,
    ) -> impl std::future::Future<Output = Result<CreatedPod, ApiFailure>> + Send {
        if let Some(hook) = &self.on_create {
            hook(spec);
        }
        let outcome = {
            let mut guard = self.state.lock().expect("lifecycle mutex");
            guard.created.push(spec.name.clone());
            if guard.script.is_empty() {
                CreateOutcome::Admitted
            } else {
                guard.script.remove(0)
            }
        };
        let name = spec.name.clone();
        let mut pod = PodSnapshot::new(&name, "100", PodPhase::Pending, &spec.identity);
        let uid = format!("uid-{}", self.next_uid.fetch_add(1, Ordering::SeqCst));
        pod.uid = Some(uid.clone());
        pod.host = Some("kind-worker2".to_string());
        let state = Arc::clone(&self.state);
        let control = self.control.clone();
        async move {
            match outcome {
                CreateOutcome::Rejected(failure) => Err(failure),
                CreateOutcome::Admitted => {
                    state
                        .lock()
                        .expect("lifecycle mutex")
                        .live
                        .insert(name.clone(), pod.clone());
                    control.upsert(pod);
                    Ok(CreatedPod {
                        name,
                        uid: Some(uid),
                        host: Some("kind-worker2".to_string()),
                    })
                }
            }
        }
    }

    fn delete(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        let name = name.to_string();
        let state = Arc::clone(&self.state);
        let control = self.control.clone();
        async move {
            {
                let mut guard = state.lock().expect("lifecycle mutex");
                guard.deleted.push(name.clone());
                guard.live.remove(&name);
            }
            control.remove(&name);
            Ok(())
        }
    }

    fn get(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<PodSnapshot>, ApiFailure>> + Send {
        let found = self
            .state
            .lock()
            .expect("lifecycle mutex")
            .live
            .get(name)
            .cloned();
        async move { Ok(found) }
    }
}

// ---------------------------------------------------------------------------
// A timer that records what it was asked to wait, and waits nothing.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct RecordingTimer {
    waits: Arc<Mutex<Vec<Duration>>>,
}

impl RecordingTimer {
    fn waits(&self) -> Vec<Duration> {
        self.waits.lock().expect("timer mutex").clone()
    }
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

// ---------------------------------------------------------------------------
// A buffering attempt sink, the shape the driver injects.
// ---------------------------------------------------------------------------

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
}

impl AttemptEventSink for Recorder {
    fn emit(&mut self, event: AttemptEvent) {
        self.events.lock().expect("recorder mutex").push(event);
    }
}

// ---------------------------------------------------------------------------
// Shard fixtures: what the pod would have written.
// ---------------------------------------------------------------------------

fn shard_identity(node: &str, attempt: u32) -> ShardIdentity {
    ShardIdentity::new(RUN_ID, node, attempt, FINGERPRINT, POLICY_HASH, TOOL_VERSION)
        .image_digest(IMAGE_DIGEST)
}

/// Write the shard a pod-side attempt would have left behind.
fn write_shard(
    root: &Path,
    node: &str,
    attempt: u32,
    state: TerminalState,
    output: Option<ShardOutput>,
) {
    let events = match state {
        TerminalState::Succeeded => vec![
            AttemptEvent::AttemptStarted {
                node: node.to_string(),
                attempt,
            },
            AttemptEvent::AttemptSucceeded {
                node: node.to_string(),
                attempt,
            },
            AttemptEvent::NodeTerminal {
                node: node.to_string(),
                state,
            },
        ],
        _ => vec![
            AttemptEvent::AttemptStarted {
                node: node.to_string(),
                attempt,
            },
            AttemptEvent::AttemptFailed {
                node: node.to_string(),
                attempt,
            },
            AttemptEvent::NodeTerminal {
                node: node.to_string(),
                state,
            },
        ],
    };
    let mut shard = AttemptShard::new(shard_identity(node, attempt), terminal_token(state))
        .with_inputs(Vec::<ConsumedRef>::new())
        .with_records(dagr_cli::shard::records_for(RUN_ID, &events));
    if let Some(out) = output {
        shard = shard.with_output(out);
    }
    shard
        .write_atomically(root, false)
        .expect("the shard is written");
}

fn terminal_token(state: TerminalState) -> &'static str {
    match state {
        TerminalState::Succeeded => "succeeded",
        TerminalState::Failed => "failed",
        TerminalState::TimedOut => "timed-out",
        TerminalState::Cancelled => "cancelled",
        _ => "failed",
    }
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

struct Harness {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    stream_path: PathBuf,
    log: SubmissionLog,
    control: FakeControl,
    lifecycle: ScriptedLifecycle,
    observer: PodObserver,
    timer: Arc<RecordingTimer>,
}

fn limits() -> ObserverLimits {
    ObserverLimits {
        stall_bound: Duration::from_secs(90),
        backoff_initial: Duration::from_millis(250),
        backoff_max: Duration::from_secs(30),
        max_consecutive_failures: 4,
        failure_window: Duration::from_secs(300),
        watch_timeout_secs: 270,
    }
}

fn harness(create_hook: Option<CreateHook>) -> Harness {
    let tmp = tempfile::tempdir().expect("a temp dir");
    let root = tmp.path().join("blobs");
    std::fs::create_dir_all(&root).expect("blob container");
    let stream_path = tmp.path().join("events.jsonl");

    let log = SubmissionLog::open(&stream_path, RUN_ID, "example-pipeline")
        .expect("the submission log opens over the run's event stream");

    let (api, control) = fake_api();
    let mut lifecycle = ScriptedLifecycle::new(control.clone());
    if let Some(hook) = create_hook {
        lifecycle = lifecycle.with_create_hook(hook);
    }
    let observer = PodObserver::spawn(api, selector(), limits());
    Harness {
        _tmp: tmp,
        root,
        stream_path,
        log,
        control,
        lifecycle,
        observer,
        timer: Arc::new(RecordingTimer::default()),
    }
}

fn config(node: &str, root: &Path) -> RemoteAttemptConfig {
    RemoteAttemptConfig {
        pipeline: "example-pipeline".to_string(),
        namespace: "dagr".to_string(),
        image: IMAGE.to_string(),
        image_digest: IMAGE_DIGEST.to_string(),
        structural_fingerprint: FINGERPRINT.to_string(),
        policy_hash: POLICY_HASH.to_string(),
        tool_version: TOOL_VERSION.to_string(),
        owner: "orchestrator-1".to_string(),
        blob_container: root.to_path_buf(),
        inputs: Vec::new(),
        declared_arity: 0,
        placement: PodPlacement::default(),
        cluster_retry: ClusterRetry::Disabled,
        launch_retries: 0,
        pre_start_bound: Duration::from_secs(30),
        retry: RetryConfig::new(1, Backoff::new(Duration::ZERO, 2.0, Duration::MAX)),
        command: vec!["dagr".to_string(), "exec-node".to_string()],
        _node: node.to_string(),
    }
}

fn runner(h: &Harness, node: &str, config: RemoteAttemptConfig) -> K8sNodeRunner<ScriptedLifecycle> {
    K8sNodeRunner::new(
        node,
        RUN_ID,
        h.lifecycle.clone(),
        h.observer.handle(),
        h.log.handle(),
        Arc::clone(&h.timer) as Arc<dyn AttemptTimer>,
        config,
    )
}

fn stream_records(path: &Path) -> Vec<Value> {
    let bytes = std::fs::read(path).unwrap_or_default();
    dagr_artifact::event_stream::read_records(&bytes)
        .expect("the stream parses")
        .records
}

/// Drive the fake so the pod for `key` reaches `phase`, carrying the given
/// diagnostics.
async fn transition(
    control: &FakeControl,
    node: &str,
    attempt: u32,
    phase: PodPhase,
    mutate: impl FnOnce(&mut PodSnapshot),
) {
    let key = AttemptKey::new(RUN_ID, node, attempt);
    let mut pod = PodSnapshot::new(pod_name(&key), "200", phase, &identity(node, attempt));
    mutate(&mut pod);
    control.upsert(pod.clone());
    control
        .deliver(dagr_k8s::api::WatchDelivery::Modified(pod))
        .await;
}

// ===========================================================================
// The submission record is write-ahead — the ordering, not just the content
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_submission_record_is_durably_flushed_before_the_create_call_is_issued() {
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let stream_for_hook: Arc<Mutex<Option<PathBuf>>> = Arc::new(Mutex::new(None));
    let hook_seen = Arc::clone(&seen);
    let hook_path = Arc::clone(&stream_for_hook);
    let h = harness(Some(Arc::new(move |_spec: &PodSpec| {
        // The whole point: read the stream OFF DISK at the instant the create call
        // is issued. A record written after creation is not here yet.
        let path = hook_path.lock().expect("path mutex").clone();
        if let Some(path) = path {
            *hook_seen.lock().expect("seen mutex") = stream_records(&path);
        }
    })));
    *stream_for_hook.lock().expect("path mutex") = Some(h.stream_path.clone());

    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        // Give the runner its terminal transition once the pod exists.
        write_shard(&root, "extract", 1, TerminalState::Succeeded, None);
        transition(&control, "extract", 1, PodPhase::Succeeded, |_| {}).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");
    assert_eq!(state, TerminalState::Succeeded);

    let at_create = seen.lock().expect("seen mutex").clone();
    let submitted: Vec<&Value> = at_create
        .iter()
        .filter(|r| r["kind"] == "attempt-submitted")
        .collect();
    assert_eq!(
        submitted.len(),
        1,
        "the write-ahead record is already durable on disk when the create call is \
         issued — this fails if the record is written after creation"
    );
    assert_eq!(submitted[0]["node"], "extract");
    assert_eq!(submitted[0]["attempt"], 1);
    assert!(
        submitted[0].get("observed_uid").is_none(),
        "the record that precedes creation cannot know the platform's identity"
    );
    assert_eq!(
        submitted[0]["target_name"],
        pod_name(&AttemptKey::new(RUN_ID, "extract", 1)).as_str(),
        "it names the pod it is ABOUT to create"
    );

    // …and the observed identity is recorded additively once creation returns.
    let after = stream_records(&h.stream_path);
    let observed: Vec<&Value> = after
        .iter()
        .filter(|r| r["kind"] == "attempt-submitted" && r.get("observed_uid").is_some())
        .collect();
    assert_eq!(observed.len(), 1, "reality is recorded as its own fact");
    assert_eq!(observed[0]["observed_uid"], "uid-1");
    assert_eq!(observed[0]["observed_host"], "kind-worker2");
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_between_the_record_and_the_create_leaves_the_intent_and_no_pod() {
    // The create call never returns a pod: the API rejects it, which stands in for
    // the orchestrator dying between the two — either way the record is on disk and
    // nothing was created.
    let h = harness(None);
    h.lifecycle.script(vec![CreateOutcome::Rejected(
        ApiFailure::api(403, "Forbidden", "exceeded quota: pods"),
    )]);

    let mut cfg = config("extract", &h.root);
    cfg.launch_retries = 0;
    let mut r = runner(&h, "extract", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();
    let state = r.run(&ctx, &mut sink).await;

    assert_eq!(state, TerminalState::Failed);
    let recs = stream_records(&h.stream_path);
    let submitted: Vec<&Value> = recs
        .iter()
        .filter(|r| r["kind"] == "attempt-submitted")
        .collect();
    assert_eq!(
        submitted.len(),
        1,
        "the intent survives even though the work never existed — the crash window \
         this record exists to cover"
    );
    assert!(
        submitted[0].get("observed_name").is_none(),
        "nothing was observed, because nothing was created"
    );
    assert!(
        h.lifecycle.deleted().is_empty(),
        "there is no pod to delete"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_arity_that_disagrees_with_the_assembled_references_fails_before_launching() {
    let h = harness(None);
    let mut cfg = config("join", &h.root);
    cfg.declared_arity = 2;
    cfg.inputs = vec![ConsumedInput {
        uri: "dagr-blob+local://blobs/sha256/aaa".to_string(),
        content_hash: Some("sha256:aaa".to_string()),
    }];

    let mut r = runner(&h, "join", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();
    let state = r.run(&ctx, &mut sink).await;

    assert_eq!(state, TerminalState::Failed);
    assert!(
        h.lifecycle.created().is_empty(),
        "nothing was launched — the empty-array-plus-known-arity encoding is what \
         makes the mismatch detectable"
    );
    let failure = r.last_failure().expect("a classified failure");
    assert!(
        matches!(failure, RemoteLaunchError::ArityMismatch { declared: 2, assembled: 1, .. }),
        "the error names both counts: {failure}"
    );
    assert!(
        stream_records(&h.stream_path)
            .iter()
            .all(|r| r["kind"] != "attempt-submitted"),
        "a submission that cannot be made is never recorded as one"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_credential_bearing_reference_is_rejected_before_it_can_reach_a_record() {
    let h = harness(None);
    let mut cfg = config("load", &h.root);
    cfg.declared_arity = 1;
    cfg.inputs = vec![ConsumedInput {
        uri: "https://bucket.s3.amazonaws.com/k?X-Amz-Signature=deadbeefdeadbeef".to_string(),
        content_hash: Some("sha256:aaa".to_string()),
    }];

    let mut r = runner(&h, "load", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();
    assert_eq!(r.run(&ctx, &mut sink).await, TerminalState::Failed);

    assert!(h.lifecycle.created().is_empty(), "nothing was launched");
    let on_disk = std::fs::read(&h.stream_path).unwrap_or_default();
    assert!(
        !String::from_utf8_lossy(&on_disk).contains("deadbeefdeadbeef"),
        "no credential ever reaches an event record"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// The two retry budgets
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_image_pull_backoff_is_bounded_by_the_runner_and_never_waits_for_a_terminal_phase() {
    let h = harness(None);
    let mut cfg = config("extract", &h.root);
    cfg.launch_retries = 0;
    cfg.pre_start_bound = Duration::from_millis(50);

    let mut r = runner(&h, "extract", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        // T101: an unpullable image sits in Pending with a waiting reason and NEVER
        // reaches a terminal phase. A runner that awaited one would hang here.
        for _ in 0..3 {
            transition(&control, "extract", 1, PodPhase::Pending, |pod| {
                pod.waiting_reason = Some("ImagePullBackOff".to_string());
            })
            .await;
        }
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Failed);
    let failure = r.last_failure().expect("a classified failure");
    assert!(
        failure.to_string().contains("ImagePullBackOff"),
        "the infrastructure cause is named: {failure}"
    );
    assert!(
        sink.events().is_empty(),
        "a pod that never started produced NO user-visible attempt: {:?}",
        sink.events()
    );
    assert_eq!(
        h.lifecycle.deleted(),
        vec![pod_name(&AttemptKey::new(RUN_ID, "extract", 1))],
        "the stuck pod is cleaned up rather than left behind"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unschedulable_pod_is_retried_against_the_launch_budget_and_consumes_no_attempt() {
    let h = harness(None);
    let mut cfg = config("extract", &h.root);
    cfg.launch_retries = 2;
    cfg.pre_start_bound = Duration::from_millis(30);
    // A node with a real retry budget, so "untouched" means something.
    cfg.retry = RetryConfig::new(3, Backoff::new(Duration::from_secs(1), 2.0, Duration::MAX));

    let mut r = runner(&h, "extract", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let driver = tokio::spawn(async move {
        for _ in 0..3 {
            control.await_watch().await;
            transition(&control, "extract", 1, PodPhase::Pending, |pod| {
                pod.scheduling_refusal = Some("Unschedulable".to_string());
            })
            .await;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.abort();

    assert_eq!(state, TerminalState::Failed);
    assert_eq!(
        h.lifecycle.created().len(),
        3,
        "one initial submission plus two launch retries"
    );
    assert!(
        sink.events().is_empty(),
        "the node's own retry budget is UNTOUCHED — the artifact shows no extra \
         attempt: {:?}",
        sink.events()
    );
    let failure = r.last_failure().expect("a classified failure");
    assert!(
        failure.to_string().contains("Unschedulable"),
        "the refusal names the infrastructure cause: {failure}"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successful_launch_after_a_pre_start_failure_is_still_try_number_one() {
    let h = harness(None);
    let mut cfg = config("extract", &h.root);
    cfg.launch_retries = 1;
    cfg.pre_start_bound = Duration::from_millis(30);

    let mut r = runner(&h, "extract", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        transition(&control, "extract", 1, PodPhase::Pending, |pod| {
            pod.waiting_reason = Some("ErrImagePull".to_string());
        })
        .await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded, None);
        transition(&control, "extract", 1, PodPhase::Succeeded, |_| {}).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert_eq!(
        sink.attempt_numbers(),
        vec![1],
        "the failed launch did not advance the try number"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_started_attempt_that_fails_consumes_the_nodes_retry_budget_with_real_backoff() {
    let h = harness(None);
    let mut cfg = config("extract", &h.root);
    cfg.retry = RetryConfig::new(2, Backoff::new(Duration::from_secs(4), 2.0, Duration::MAX));

    let mut r = runner(&h, "extract", cfg);
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Failed, None);
        transition(&control, "extract", 1, PodPhase::Failed, |pod| {
            pod.container_reason = Some("Error".to_string());
            pod.exit_code = Some(1);
        })
        .await;
        control.await_watch().await;
        write_shard(&root, "extract", 2, TerminalState::Succeeded, None);
        transition(&control, "extract", 2, PodPhase::Succeeded, |_| {}).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert_eq!(
        sink.attempt_numbers(),
        vec![1, 2],
        "each attempt is a distinct try number in the artifact"
    );
    assert_eq!(
        h.lifecycle.created().len(),
        2,
        "two pods, one per user-visible attempt"
    );
    let waits = h.timer.waits();
    assert_eq!(waits.len(), 1, "one backoff, between the two attempts");
    assert!(
        waits[0] > Duration::ZERO && waits[0] <= Duration::from_secs(4),
        "T102's real jittered backoff elapsed between attempts: {waits:?}"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// Classification, not invention
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oom_killed_pod_is_a_failed_attempt_carrying_a_diagnostic() {
    let h = harness(None);
    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Failed, None);
        transition(&control, "extract", 1, PodPhase::Failed, |pod| {
            pod.container_reason = Some("OOMKilled".to_string());
            pod.exit_code = Some(137);
        })
        .await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(
        state,
        TerminalState::Failed,
        "an OOM kill is a `failed` attempt — the nine-state taxonomy gains no member"
    );
    assert!(
        r.diagnostics().iter().any(|d| d.contains("OOMKilled")),
        "the platform's reason travels as a diagnostic string: {:?}",
        r.diagnostics()
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// The shard: replay, absence, and refusal
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shards_records_are_replayed_and_its_output_reference_reaches_the_existing_hooks() {
    let h = harness(None);
    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(
            &root,
            "extract",
            1,
            TerminalState::Succeeded,
            Some(
                ShardOutput::new("dagr-blob+local://blobs/sha256/abc")
                    .content_hash("sha256:abc")
                    .size_bytes(42)
                    .scheme("dagr-blob+local"),
            ),
        );
        transition(&control, "extract", 1, PodPhase::Succeeded, |_| {}).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");
    assert_eq!(state, TerminalState::Succeeded);

    let events = sink.events();
    assert!(
        events.iter().any(|e| matches!(
            e,
            AttemptEvent::AttemptStarted { attempt: 1, .. }
        )) && events.iter().any(|e| matches!(
            e,
            AttemptEvent::AttemptSucceeded { attempt: 1, .. }
        )),
        "the shard's records were replayed through the INJECTED sink, so the \
         orchestrator stays the single writer: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, AttemptEvent::NodeTerminal { .. }))
            .count(),
        1,
        "exactly one node-terminal, whatever the shard carried"
    );

    assert_eq!(
        r.durable_reference().as_deref(),
        Some("dagr-blob+local://blobs/sha256/abc"),
        "reported through the hook the driver already reads"
    );
    let meta = r.durable_reference_meta().expect("metadata is reported");
    assert_eq!(meta.content_hash.as_deref(), Some("sha256:abc"));
    assert_eq!(meta.size_bytes, Some(42));
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_terminal_pod_with_no_readable_shard_is_a_classified_failure_naming_the_pod() {
    let h = harness(None);
    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        // No shard written at all: the pod died before it wrote one.
        transition(&control, "extract", 1, PodPhase::Failed, |pod| {
            pod.container_reason = Some("Error".to_string());
            pod.exit_code = Some(137);
        })
        .await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(
        state,
        TerminalState::Failed,
        "never a silent success, and never a hang"
    );
    let failure = r.last_failure().expect("a classified failure").to_string();
    let name = pod_name(&AttemptKey::new(RUN_ID, "extract", 1));
    assert!(
        failure.contains(&name),
        "the failure names the pod: {failure}"
    );
    assert!(
        failure.contains("Failed") || failure.contains("Error"),
        "…and its status: {failure}"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shard_from_a_different_build_is_refused_naming_both_fingerprints() {
    let h = harness(None);
    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        AttemptShard::new(
            ShardIdentity::new(
                RUN_ID,
                "extract",
                1,
                "sf-a-different-build",
                POLICY_HASH,
                TOOL_VERSION,
            ),
            "succeeded",
        )
        .write_atomically(&root, false)
        .expect("the foreign shard is written");
        transition(&control, "extract", 1, PodPhase::Succeeded, |_| {}).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Failed);
    let failure = r.last_failure().expect("a classified failure").to_string();
    assert!(
        failure.contains(FINGERPRINT) && failure.contains("sf-a-different-build"),
        "both fingerprints are named: {failure}"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

// ===========================================================================
// Idempotency and cancellation
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_submission_for_an_attempt_key_with_a_live_pod_adopts_it_rather_than_creating_a_second() {
    let h = harness(None);
    let key = AttemptKey::new(RUN_ID, "extract", 1);
    let mut existing = PodSnapshot::new(pod_name(&key), "100", PodPhase::Running, &identity("extract", 1));
    existing.uid = Some("uid-existing".to_string());
    existing.host = Some("kind-worker9".to_string());
    h.lifecycle.adopt_target(existing);

    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();
    let ctx = dagr_core::context::RunContext::for_test();

    let control = h.control.clone();
    let root = h.root.clone();
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        write_shard(&root, "extract", 1, TerminalState::Succeeded, None);
        transition(&control, "extract", 1, PodPhase::Succeeded, |_| {}).await;
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Succeeded);
    assert!(
        h.lifecycle.created().is_empty(),
        "the live pod for that attempt key was adopted, not duplicated"
    );
    let observed: Vec<Value> = stream_records(&h.stream_path)
        .into_iter()
        .filter(|r| r["kind"] == "attempt-submitted" && r.get("observed_uid").is_some())
        .collect();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0]["observed_uid"], "uid-existing",
        "the adopted pod's real identity is what gets recorded"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_attempt_deletes_its_pod_and_records_cancelled() {
    let h = harness(None);
    let mut r = runner(&h, "extract", config("extract", &h.root));
    let mut sink = Recorder::default();

    let cancel = dagr_core::execution::CancellationSource::new();
    let ctx = dagr_core::context::RunContext::builder(
        dagr_core::context::RunId::new(RUN_ID),
        dagr_core::context::PipelineId::new("example-pipeline"),
        dagr_core::context::NodeId::from_name("extract"),
    )
    .cancellation(cancel.signal())
    .build();

    let control = h.control.clone();
    let cancel_handle = cancel;
    let driver = tokio::spawn(async move {
        control.await_watch().await;
        // The pod is running and healthy; the operator cancels the run.
        transition(&control, "extract", 1, PodPhase::Running, |_| {}).await;
        cancel_handle.cancel();
    });

    let state = r.run(&ctx, &mut sink).await;
    driver.await.expect("the driver task completes");

    assert_eq!(state, TerminalState::Cancelled);
    assert_eq!(
        h.lifecycle.deleted(),
        vec![pod_name(&AttemptKey::new(RUN_ID, "extract", 1))],
        "cancellation deletes the pod rather than orphaning it"
    );
    let _ = h.observer.shutdown(Duration::from_secs(5)).await;
}
