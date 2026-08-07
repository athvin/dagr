//! T112 · **the wired remote run path.** Written first (TDD). Needs the default-off
//! `k8s` feature.
//!
//! This is the half of the M10 gate that needs the executor compiled in: the
//! deferrals T108 and T109 both routed here.
//!
//! - **T108 deferred lifting the bootstrap refusal**, because *"lifting the refusal
//!   requires the flow-level wiring that turns a `RunnableFlow`'s placed nodes into
//!   `K8sNodeRunner`s"*. That wiring is `RunnableFlow::run_placed`, and these tests
//!   drive it.
//! - **T109 deferred wiring adoption discovery into the run path** for the same
//!   reason. The kill-and-restart guarantee is asserted through `run_placed`'s own
//!   startup pass here, not through a hand-called `discover`.
//! - **T108/T109 both deferred the complete RBAC.** A missing verb must fail
//!   *naming the verb*, not hang and not report a generic error.
//!
//! Every test drives T107's fake API surface, and what that buys differs by test —
//! so read each claim at its own strength rather than the module's:
//!
//! - **The RBAC classification and the dual-mode parity** are decidable from the
//!   API's observable behaviour. A real cluster would make them slower and less
//!   deterministic without making them stronger.
//! - **Adoption across a restart is weaker here than it reads.** Pod identity is a
//!   test-controlled struct: the fake hands back the labels and annotations the
//!   submission put there, so "the restart recognised its own pod" is asserted
//!   against data this file wrote. Against real pods the same assertion would also
//!   cover the API server's own round-tripping of labels, annotations and UIDs, and
//!   the timing of a live watch. That is a genuinely stronger test, and it is not
//!   this one. (What it does prove, and what no cluster is needed for, is that the
//!   run path *calls* `list` and `patch_labels` instead of creating a second pod.)
//! - **Nothing here observes a pod actually running.** The attempt shard a placed
//!   node reports through is written by the test, into the orchestrator's own blob
//!   directory. See the note below on why no pod can write one.
//!
//! What genuinely needs a cluster — an image pull, a real OOM kill, real scheduling
//! latency — is **not covered yet**: the shipped pod spec cannot mount the blob
//! container a pod must write into, so no real pod can report. The ticket file
//! records the gap and names T108 as its owner.

#![cfg(feature = "k8s")]

mod k8s_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_blob::{BlobStore, LocalFsBlob};
use dagr_cli::driver::RunConfig;
use dagr_cli::executor::ExecutorKind;
use dagr_cli::remote::{RemoteCluster, RemoteTarget};
use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::shard::{AttemptShard, ConsumedRef, ShardIdentity, ShardOutput};
use dagr_core::context::TerminalState;
use dagr_core::error::TaskError;
use dagr_core::execution::AttemptEvent;
use dagr_core::{Payload, StableName};
use dagr_k8s::api::{ApiFailure, CreatedPod, PodLifecycle, PodPhase, PodSnapshot};
use dagr_k8s::executor::PodSpec;
use dagr_k8s::fake::{FakeControl, fake_api};
use dagr_k8s::observer::ObserverLimits;
use dagr_k8s::rbac::PodVerb;

use k8s_support::RUN_ID;

const TOOL_VERSION: &str = "dagr@t112";
const IMAGE: &str = "registry.example/dagr@sha256:cafebabe";
const IMAGE_DIGEST: &str = "sha256:cafebabe";
const NAMESPACE: &str = "dagr";
const PIPELINE: &str = "placed-demo";
const PLACED: &str = "extract";
const LOCAL: &str = "report";

// ===========================================================================
// The pipeline under test: one placed source, one local consumer
// ===========================================================================

/// The value the placed node produces. `Payload`, because that is what makes a node
/// remote-eligible at compile time (ADR 115 §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, StableName, Payload)]
struct Reading {
    n: u64,
}

#[derive(StableName)]
struct Extract;

impl dagr_core::task::Task for Extract {
    type Input = ();
    type Output = Reading;
    async fn run(
        &mut self,
        _ctx: &dagr_core::context::RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Ok(Reading { n: 7 })
    }
}

#[derive(StableName)]
struct Report;

impl dagr_core::task::Task for Report {
    type Input = Reading;
    type Output = Reading;
    async fn run(
        &mut self,
        _ctx: &dagr_core::context::RunContext,
        input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Ok(Reading { n: input.n * 2 })
    }
}

/// The demo pipeline: `extract` is **placed** with a declared size; `report` runs
/// wherever the invocation runs. One binary, both executors.
fn build_flow() -> RunnableFlow {
    use dagr_core::assembly::{NodePolicy, Placement};
    let mut flow = RunnableFlow::new();
    let extract = flow.register_source_placed(
        PLACED,
        Extract,
        NodePolicy::new(),
        Placement::new().cpu("500m").memory("512Mi"),
    );
    let _ = flow.register_payload(LOCAL, Report, extract);
    flow
}

/// This build's fingerprints, rendered exactly as a shard records them.
fn fingerprints() -> (String, String) {
    let fp = build_flow().into_pipeline().fingerprint();
    (
        dagr_cli::graph::format_fingerprint_structural(&fp),
        dagr_cli::graph::format_fingerprint_policy(&fp),
    )
}

// ===========================================================================
// Scaffolding: a scripted pod lifecycle over the fake world
// ===========================================================================

#[derive(Default)]
struct LifecycleState {
    created: Vec<String>,
    deleted: Vec<String>,
    patched: Vec<String>,
    live: BTreeMap<String, PodSnapshot>,
    /// Every spec the platform was asked to create, so a test can read the declared
    /// size back off the object rather than out of the runner's configuration.
    specs: Vec<PodSpec>,
    /// A verb the fake platform refuses with a `403`, as a Role with that verb
    /// removed would.
    forbidden: Option<PodVerb>,
}

#[derive(Clone)]
struct ScriptedLifecycle {
    state: Arc<Mutex<LifecycleState>>,
    control: FakeControl,
    next_uid: Arc<AtomicU32>,
}

impl ScriptedLifecycle {
    fn new(control: FakeControl) -> Self {
        Self {
            state: Arc::new(Mutex::new(LifecycleState::default())),
            control,
            next_uid: Arc::new(AtomicU32::new(1)),
        }
    }

    /// Remove one verb from the grant, the way an operator's edited Role would.
    fn without(self, verb: PodVerb) -> Self {
        self.state.lock().expect("lifecycle mutex").forbidden = Some(verb);
        self
    }

    /// Pre-seed a live pod, without going through `create` — a pod a previous
    /// orchestrator left running.
    fn preexisting(&self, pod: PodSnapshot) {
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

    fn patched(&self) -> Vec<String> {
        self.state.lock().expect("lifecycle mutex").patched.clone()
    }

    fn specs(&self) -> Vec<PodSpec> {
        self.state.lock().expect("lifecycle mutex").specs.clone()
    }

    fn refuses(&self, verb: PodVerb) -> Option<ApiFailure> {
        let guard = self.state.lock().expect("lifecycle mutex");
        (guard.forbidden == Some(verb)).then(|| forbidden_failure(verb))
    }
}

/// The **fixture** denial message for a missing verb: the documented shape of a
/// Kubernetes RBAC `403`, hand-written, byte-identical to the one
/// `crates/k8s/tests/rbac_missing_verb.rs` builds. Not recorded from a live API
/// server — nothing here has contacted one — so what the test below proves is that
/// dagr's classifier parses *this pinned shape*, not that it parses whatever a
/// particular cluster emits.
fn forbidden_failure(verb: PodVerb) -> ApiFailure {
    ApiFailure::api(
        dagr_k8s::rbac::FORBIDDEN_CODE,
        "Forbidden",
        format!(
            "pods is forbidden: User \"system:serviceaccount:dagr:dagr-orchestrator\" cannot \
             {verb} resource \"pods\" in API group \"\" in the namespace \"{NAMESPACE}\""
        ),
    )
}

impl PodLifecycle for ScriptedLifecycle {
    fn create(
        &self,
        spec: &PodSpec,
    ) -> impl std::future::Future<Output = Result<CreatedPod, ApiFailure>> + Send {
        let refusal = self.refuses(PodVerb::Create);
        let name = spec.name.clone();
        let mut pod = PodSnapshot::new(&name, "100", PodPhase::Pending, &spec.identity);
        let uid = format!("uid-{}", self.next_uid.fetch_add(1, Ordering::SeqCst));
        pod.uid = Some(uid.clone());
        pod.host = Some("kind-worker".to_string());
        {
            let mut guard = self.state.lock().expect("lifecycle mutex");
            guard.specs.push(spec.clone());
            if refusal.is_none() {
                guard.created.push(name.clone());
                guard.live.insert(name.clone(), pod.clone());
            }
        }
        let control = self.control.clone();
        async move {
            if let Some(failure) = refusal {
                return Err(failure);
            }
            control.upsert(pod);
            Ok(CreatedPod {
                name,
                uid: Some(uid),
                host: Some("kind-worker".to_string()),
            })
        }
    }

    fn delete(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        let refusal = self.refuses(PodVerb::Delete);
        let name = name.to_string();
        let state = Arc::clone(&self.state);
        let control = self.control.clone();
        async move {
            if let Some(failure) = refusal {
                return Err(failure);
            }
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
        let refusal = self.refuses(PodVerb::Get);
        let found = self
            .state
            .lock()
            .expect("lifecycle mutex")
            .live
            .get(name)
            .cloned();
        async move {
            match refusal {
                Some(failure) => Err(failure),
                None => Ok(found),
            }
        }
    }

    fn patch_labels(
        &self,
        name: &str,
        labels: &BTreeMap<String, Option<String>>,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        let refusal = self.refuses(PodVerb::Patch);
        let patched = {
            let mut guard = self.state.lock().expect("lifecycle mutex");
            guard.patched.push(name.to_string());
            guard.live.get_mut(name).map(|pod| {
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
                pod.clone()
            })
        };
        let control = self.control.clone();
        async move {
            if let Some(failure) = refusal {
                return Err(failure);
            }
            if let Some(pod) = patched {
                control.upsert(pod);
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Temp roots, clocks, sinks
// ---------------------------------------------------------------------------

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "dagr-cli-t112-{tag}-{}-{nanos}-{n}",
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

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<String>>>,
}

impl MemorySink {
    fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("sink mutex").clone()
    }
}

impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .expect("sink mutex")
            .push(String::from_utf8_lossy(line).into_owned());
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct TickClock {
    now: Arc<std::sync::atomic::AtomicU64>,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.now.fetch_add(1_000, Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// The shard the pod would have written
// ---------------------------------------------------------------------------

fn write_shard(root: &Path, run_id: &str, node: &str, attempt: u32, state: TerminalState) {
    let (structural, policy) = fingerprints();
    let events = vec![
        AttemptEvent::AttemptStarted {
            node: node.to_string(),
            attempt,
        },
        if state == TerminalState::Succeeded {
            AttemptEvent::AttemptSucceeded {
                node: node.to_string(),
                attempt,
            }
        } else {
            AttemptEvent::AttemptFailed {
                node: node.to_string(),
                attempt,
            }
        },
        AttemptEvent::NodeTerminal {
            node: node.to_string(),
            state,
        },
    ];
    let identity = ShardIdentity::new(run_id, node, attempt, structural, policy, TOOL_VERSION)
        .image_digest(IMAGE_DIGEST);
    let mut shard = AttemptShard::new(
        identity,
        if state == TerminalState::Succeeded {
            "succeeded"
        } else {
            "failed"
        },
    )
    .with_inputs(Vec::<ConsumedRef>::new())
    .with_records(dagr_cli::shard::records_for(run_id, &events));
    if state == TerminalState::Succeeded {
        // The pod really does store the encoded value and record a reference to it
        // (`exec_node.rs`: `store.put(&bytes)` then `store.reference(&key)`), and the
        // orchestrator really does fetch it back to fill the node's slot — otherwise
        // a local consumer downstream of a placed node reads an unfilled slot. A
        // fixture that recorded a reference to bytes nobody wrote would assert the
        // return half of the data path away, so this writes the bytes.
        let bytes = Reading { n: 7 }.encode_to_vec();
        let store = LocalFsBlob::open(root);
        let key = store.put(&bytes).expect("the value is stored");
        shard = shard.with_output(
            ShardOutput::new(store.reference(&key).to_string())
                .content_hash(key.to_string())
                .size_bytes(bytes.len() as u64),
        );
    }
    shard
        .write_atomically(root, false)
        .expect("the shard is written");
}

/// Drive the fake platform's pod for `node`/`attempt` to `phase`.
async fn transition(control: &FakeControl, name: &str, phase: PodPhase, identity_node: &str) {
    let (structural, policy) = fingerprints();
    let id = dagr_k8s::identity::AttemptIdentity {
        key: dagr_k8s::identity::AttemptKey::new(RUN_ID, identity_node, 1),
        pipeline: PIPELINE.to_string(),
        structural_fingerprint: structural,
        policy_hash: policy,
        tool_version: TOOL_VERSION.to_string(),
        image_digest: IMAGE_DIGEST.to_string(),
        owner: "orchestrator-t112".to_string(),
    };
    let mut pod = PodSnapshot::new(name, "200", phase, &id);
    pod.uid = Some("uid-1".to_string());
    control
        .deliver(dagr_k8s::api::WatchDelivery::Modified(pod.clone()))
        .await;
    control.upsert(pod);
}

fn target(blobs: &Path) -> RemoteTarget {
    RemoteTarget::new(NAMESPACE, IMAGE, IMAGE_DIGEST, blobs)
        .owner("orchestrator-t112")
        .tool_version(TOOL_VERSION)
        .command(["dagr".to_string(), "exec-node".to_string()])
        .observer_limits(ObserverLimits {
            stall_bound: Duration::from_secs(5),
            backoff_initial: Duration::from_millis(1),
            backoff_max: Duration::from_millis(2),
            max_consecutive_failures: 2,
            failure_window: Duration::from_secs(30),
            watch_timeout_secs: 5,
        })
}

// ===========================================================================
// 1. The bootstrap refusal, lifted — and the guard that replaces it
// ===========================================================================

/// **T108's deferral: `--dagr.executor=k8s` no longer refuses at bootstrap.** A
/// build that compiled the executor in can run under it.
#[test]
fn the_kubernetes_executor_is_available_in_a_build_that_compiled_it() {
    ExecutorKind::Kubernetes
        .ensure_available()
        .expect("a build with the `k8s` feature has the remote executor");
    assert!(
        ExecutorKind::Kubernetes.honours_placement(),
        "…and it honours a node's declared placement"
    );
}

/// The refusal that **replaces** it, and the reason T105 wanted one at all: a run
/// that selects the remote executor for a pipeline with placed nodes, but hands the
/// run path no cluster to place them on, must not run those nodes in-process while
/// the operator believes their placement was honoured.
#[test]
fn a_placed_pipeline_with_no_cluster_target_still_fails_bootstrap() {
    let store = TempRoot::new("no-target");
    let sink = MemorySink::default();
    let config = RunConfig::new(store.path().to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let report = build_flow()
        .run(PIPELINE, &config, sink.clone(), TickClock::default())
        .expect("the flow assembles");

    assert_eq!(
        report.outcome(),
        RunOutcome::BootstrapFailed,
        "an unwired remote executor is a bootstrap failure, never a quiet local run"
    );
    assert!(
        report.driver_report().terminal_states.is_empty(),
        "and not one placed node ran locally behind the operator's back"
    );
}

/// …but a pipeline with **no** placed node runs fine under the remote executor:
/// there is nothing to place, so there is nothing to substitute.
#[test]
fn an_unplaced_pipeline_runs_under_the_remote_executor_with_no_cluster() {
    let store = TempRoot::new("unplaced");
    let sink = MemorySink::default();
    let config = RunConfig::new(store.path().to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let mut flow = RunnableFlow::new();
    let _ = flow.register_source_payload(LOCAL, Extract);
    let report = flow
        .run(PIPELINE, &config, sink, TickClock::default())
        .expect("the flow assembles");
    assert_eq!(report.outcome(), RunOutcome::Succeeded);
}

// ===========================================================================
// 2. The capability proof: a placed node runs through the wired path
// ===========================================================================

/// **What this proves:** a placed pipeline drives to completion through the wired
/// run path, one pod is submitted per placed node attempt and none for an unplaced
/// one, and the `PodSpec` handed to `create` carries the declared size verbatim.
///
/// **What it does not prove**, and what the ticket's own wording — *"the pods carried
/// the declared resource requests (read back from the API)"* — asks for: the size is
/// read off the spec this process submitted, not off a `Pod` object fetched back from
/// an API server, and the attempt shard the placed node reports through is written
/// **by this test** (`write_shard`) into the orchestrator's own blob directory. No
/// pod writes it, because no pod can: the spec has no volume field to mount that
/// directory with. So this is the wired path end to end, not the *system* end to end.
#[test]
fn a_placed_pipeline_drives_to_completion_and_its_submitted_spec_carries_the_declared_size() {
    let tmp = TempRoot::new("e2e");
    let blobs = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs).expect("blob container");
    let store = tmp.path().join("store");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime for the platform driver");

    let (api, control) = fake_api();
    let lifecycle = ScriptedLifecycle::new(control.clone());

    let shard_root = blobs.clone();
    let driver = {
        let control = control.clone();
        rt.spawn(async move {
            control.await_watch().await;
            write_shard(&shard_root, RUN_ID, PLACED, 1, TerminalState::Succeeded);
            let name = dagr_k8s::executor::pod_name(&dagr_k8s::identity::AttemptKey::new(
                RUN_ID, PLACED, 1,
            ));
            transition(&control, &name, PodPhase::Succeeded, PLACED).await;
        })
    };

    let sink = MemorySink::default();
    let config = RunConfig::new(store.to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let cluster = RemoteCluster::new(api, lifecycle.clone(), target(&blobs));

    let placed = build_flow()
        .run_placed(
            PIPELINE,
            &config,
            sink.clone(),
            TickClock::default(),
            cluster,
        )
        .expect("the placed run drives");

    rt.block_on(driver).expect("the platform driver finishes");

    assert_eq!(
        placed.report.terminal_state(PLACED),
        Some(TerminalState::Succeeded),
        "the placed node reached a terminal state through the ordinary loop"
    );
    assert_eq!(
        placed.report.terminal_state(LOCAL),
        Some(TerminalState::Succeeded),
        "…and so did its local consumer, in the same run"
    );

    // One pod per placed node attempt — and only for the placed node.
    let created = lifecycle.created();
    assert_eq!(created.len(), 1, "one pod, for one attempt: {created:?}");
    assert!(
        !created.iter().any(|n| n.contains(LOCAL)),
        "the unplaced node was never submitted anywhere"
    );

    // The declared size travelled, verbatim, as an opaque string.
    let specs = lifecycle.specs();
    let spec = specs.first().expect("the create carried a spec");
    assert_eq!(spec.cpu.as_deref(), Some("500m"));
    assert_eq!(spec.memory.as_deref(), Some("512Mi"));
    assert_eq!(
        spec.restart_policy, "Never",
        "cluster-side retry is unrepresentable, not merely unset"
    );
    assert_eq!(spec.namespace, NAMESPACE, "one namespace");

    // The stream folds, and its `seq` is gapless — the single-writer guarantee holds
    // with a submission record interleaved into it.
    let joined = sink.lines().join("\n") + "\n";
    let artifact = dagr_artifact::fold::fold_stream(
        joined.as_bytes(),
        &[PLACED.to_string(), LOCAL.to_string()],
    )
    .expect("the event stream folds");
    let attempted: std::collections::BTreeSet<&str> = artifact
        .attempts()
        .iter()
        .map(dagr_artifact::fold::AttemptRecord::node)
        .collect();
    assert!(
        attempted.contains(PLACED) && attempted.contains(LOCAL),
        "the folded artifact carries both nodes' attempts: {attempted:?}"
    );
}

// ===========================================================================
// 3. The kill-restart guarantee
// ===========================================================================

/// **Test plan: given the orchestrator killed mid-run with pods live, when it
/// restarts with the same run id, then live pods are adopted (not resubmitted), the
/// run completes, and the artifact shows every node executed exactly once. Given
/// that restart, no pod was recreated (asserted from pod UIDs).**
///
/// The restart is modelled by starting a run whose placed attempt's pod **already
/// exists** — which is exactly the state a killed orchestrator leaves behind, and
/// the only state the restarting process can observe.
///
/// Read the strength carefully. The pod this recognises is a struct this test built,
/// so what is proven is that the run path *takes the adoption branch*: it lists,
/// matches on identity, patches the ownership label, and creates nothing. Against
/// real pods the same assertion would additionally cover the API server's own
/// round-tripping of labels, annotations and UIDs, and a live watch's timing — and
/// that half needs no shared blob container, so it is provable on a cluster the day
/// one is wired up, unlike the "runs to completion from the artifact" half.
#[test]
fn a_restart_adopts_the_live_pod_instead_of_resubmitting_it() {
    let tmp = TempRoot::new("adopt");
    let blobs = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs).expect("blob container");
    let store = tmp.path().join("store");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");

    let (api, control) = fake_api();
    let lifecycle = ScriptedLifecycle::new(control.clone());

    // The pod the killed orchestrator left running, under the attempt's own object
    // name, carrying this build's identity.
    let key = dagr_k8s::identity::AttemptKey::new(RUN_ID, PLACED, 1);
    let name = dagr_k8s::executor::pod_name(&key);
    let (structural, policy) = fingerprints();
    let identity = dagr_k8s::identity::AttemptIdentity {
        key: key.clone(),
        pipeline: PIPELINE.to_string(),
        structural_fingerprint: structural,
        policy_hash: policy,
        tool_version: TOOL_VERSION.to_string(),
        image_digest: IMAGE_DIGEST.to_string(),
        owner: "orchestrator-that-died".to_string(),
    };
    let mut survivor = PodSnapshot::new(&name, "50", PodPhase::Running, &identity);
    survivor.uid = Some("uid-survivor".to_string());
    lifecycle.preexisting(survivor);

    let shard_root = blobs.clone();
    let driver = {
        let control = control.clone();
        let name = name.clone();
        rt.spawn(async move {
            control.await_watch().await;
            write_shard(&shard_root, RUN_ID, PLACED, 1, TerminalState::Succeeded);
            transition(&control, &name, PodPhase::Succeeded, PLACED).await;
        })
    };

    let sink = MemorySink::default();
    let config = RunConfig::new(store.to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let cluster = RemoteCluster::new(api, lifecycle.clone(), target(&blobs));

    let placed = build_flow()
        .run_placed(PIPELINE, &config, sink, TickClock::default(), cluster)
        .expect("the restarted run drives");
    rt.block_on(driver).expect("the platform driver finishes");

    // Adopted, by an ownership patch — never resubmitted.
    assert_eq!(
        placed.discovery.adopted,
        vec![name.clone()],
        "the live pod was reclaimed at startup"
    );
    assert!(
        lifecycle.created().is_empty(),
        "no pod was created: the survivor was waited on, not replaced ({:?})",
        lifecycle.created()
    );
    assert!(
        lifecycle.patched().contains(&name),
        "adoption is one label patch on the pod that already exists"
    );

    // Every node executed exactly once.
    assert_eq!(
        placed.report.terminal_state(PLACED),
        Some(TerminalState::Succeeded)
    );
    assert_eq!(
        placed.report.terminal_state(LOCAL),
        Some(TerminalState::Succeeded)
    );
}

// ===========================================================================
// 4. Dual mode
// ===========================================================================

/// **Test plan: the same pipeline run locally and remotely produces artifacts equal
/// except for policy-derived fields; terminal states and node outputs match.**
///
/// The structural fingerprint is the load-bearing half: placement is *policy*, so
/// moving a node between executors must not move it. If it did, a run started under
/// one executor could never be resumed under the other, which is the payoff ADR 115
/// §7 exists for.
#[test]
fn the_same_pipeline_run_locally_and_remotely_agrees_on_everything_but_policy() {
    // --- the local leg -----------------------------------------------------
    let local_tmp = TempRoot::new("dual-local");
    let local_sink = MemorySink::default();
    let local_config = RunConfig::new(local_tmp.path().to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Local);
    let local = build_flow()
        .run(
            PIPELINE,
            &local_config,
            local_sink.clone(),
            TickClock::default(),
        )
        .expect("the flow assembles");

    // --- the remote leg ----------------------------------------------------
    let tmp = TempRoot::new("dual-remote");
    let blobs = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs).expect("blob container");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let (api, control) = fake_api();
    let lifecycle = ScriptedLifecycle::new(control.clone());
    let shard_root = blobs.clone();
    let driver = {
        let control = control.clone();
        rt.spawn(async move {
            control.await_watch().await;
            write_shard(&shard_root, RUN_ID, PLACED, 1, TerminalState::Succeeded);
            let name = dagr_k8s::executor::pod_name(&dagr_k8s::identity::AttemptKey::new(
                RUN_ID, PLACED, 1,
            ));
            transition(&control, &name, PodPhase::Succeeded, PLACED).await;
        })
    };
    let remote_sink = MemorySink::default();
    let remote_config = RunConfig::new(tmp.path().join("store").to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let remote = build_flow()
        .run_placed(
            PIPELINE,
            &remote_config,
            remote_sink.clone(),
            TickClock::default(),
            RemoteCluster::new(api, lifecycle, target(&blobs)),
        )
        .expect("the placed run drives");
    rt.block_on(driver).expect("the platform driver finishes");

    // Terminal states match, node for node.
    for node in [PLACED, LOCAL] {
        assert_eq!(
            local.terminal_state(node),
            remote.report.terminal_state(node),
            "`{node}` ended the same way under both executors"
        );
    }
    assert_eq!(local.outcome(), remote.report.outcome());

    // The structural fingerprint is identical; the policy hash is where placement
    // lives, and both runs declare the same placement, so it matches too — what the
    // test pins is that placement never reaches the *structural* half. Read out of
    // the two headers rather than compared against a recomputed constant, so the
    // assertion is about the two runs agreeing rather than about a format.
    let local_header = local_sink.lines().first().expect("a header").clone();
    let remote_header = remote_sink.lines().first().expect("a header").clone();
    let local_structural = header_field(&local_header, "fingerprint_structural");
    let remote_structural = header_field(&remote_header, "fingerprint_structural");
    assert_eq!(
        local_structural, remote_structural,
        "both legs record the same structural fingerprint"
    );
    assert_eq!(
        header_field(&local_header, "fingerprint_policy"),
        header_field(&remote_header, "fingerprint_policy"),
        "…and the same policy hash, because both legs declare the same placement"
    );
    // Non-vacuity: the extractor really found the field, and it is this build's.
    let fp = build_flow().into_pipeline().fingerprint();
    assert!(
        dagr_cli::graph::format_fingerprint_structural(&fp).ends_with(&local_structural),
        "the recorded structural fingerprint is this build's ({local_structural})"
    );
}

/// One string field of a `run-started` header line.
fn header_field(line: &str, key: &str) -> String {
    let needle = format!("\"{key}\":\"");
    let start = line
        .find(&needle)
        .unwrap_or_else(|| panic!("the header carries `{key}`: {line}"))
        + needle.len();
    let end = line[start..]
        .find('"')
        .expect("the field's value is terminated")
        + start;
    line[start..end].to_string()
}

// ===========================================================================
// 5. RBAC: a missing verb fails informatively
// ===========================================================================

/// **Test plan: given the Role with `create` removed, the failure names the missing
/// permission rather than hanging or reporting a generic error.**
#[test]
fn a_missing_create_grant_fails_the_node_naming_the_verb() {
    let tmp = TempRoot::new("rbac-create");
    let blobs = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs).expect("blob container");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let _entered = rt.enter();

    let (api, control) = fake_api();
    let lifecycle = ScriptedLifecycle::new(control).without(PodVerb::Create);

    let sink = MemorySink::default();
    let config = RunConfig::new(tmp.path().join("store").to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let placed = build_flow()
        .run_placed(
            PIPELINE,
            &config,
            sink,
            TickClock::default(),
            RemoteCluster::new(api, lifecycle, target(&blobs)),
        )
        .expect("the run drives even though the node cannot be launched");

    assert_eq!(
        placed.report.terminal_state(PLACED),
        Some(TerminalState::Failed),
        "a node whose pod cannot be created fails; it does not hang"
    );
    let said = placed.diagnostics.join("\n");
    assert!(
        said.contains("create") && said.contains("pods"),
        "the diagnostic names the missing verb and the resource: {said}"
    );
    assert!(
        said.contains(dagr_k8s::rbac::ORCHESTRATOR_RBAC_MANIFEST),
        "…and the manifest that grants it: {said}"
    );
}

/// **…likewise for `watch`.** The observer classifies a `403` as a transient API
/// failure and retries it with backoff — correct for a server that is coming back,
/// and exactly wrong for a permission that will never appear. The run must end,
/// bounded, with a diagnostic naming the verb.
#[test]
fn a_missing_watch_grant_ends_the_run_naming_the_verb() {
    let tmp = TempRoot::new("rbac-watch");
    let blobs = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs).expect("blob container");

    let (api, control) = fake_api();
    // Every watch this run opens is refused, the way a Role without `watch` refuses.
    for _ in 0..8 {
        control.fail_next_watch(forbidden_failure(PodVerb::Watch));
    }
    let lifecycle = ScriptedLifecycle::new(control);

    let sink = MemorySink::default();
    let config = RunConfig::new(tmp.path().join("store").to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let placed = build_flow()
        .run_placed(
            PIPELINE,
            &config,
            sink,
            TickClock::default(),
            RemoteCluster::new(api, lifecycle, target(&blobs)),
        )
        .expect("the run drives to a conclusion rather than hanging");

    assert_ne!(
        placed.report.terminal_state(PLACED),
        Some(TerminalState::Succeeded),
        "a run that cannot watch its pods must not report success"
    );
    let said = placed.diagnostics.join("\n");
    assert!(
        said.contains("watch") && said.contains("pods"),
        "the diagnostic names the missing verb rather than a generic API failure: {said}"
    );
}

/// **…and for `patch`**, which is adoption's only write. Losing it must not lose the
/// run: T109's recorded resolution is that an unpatchable pod is reported and
/// degrades to the submission probe, never abandoned.
#[test]
fn a_missing_patch_grant_is_reported_and_never_silently_ignored() {
    let tmp = TempRoot::new("rbac-patch");
    let blobs = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs).expect("blob container");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");

    let (api, control) = fake_api();
    let lifecycle = ScriptedLifecycle::new(control.clone()).without(PodVerb::Patch);

    let key = dagr_k8s::identity::AttemptKey::new(RUN_ID, PLACED, 1);
    let name = dagr_k8s::executor::pod_name(&key);
    let (structural, policy) = fingerprints();
    let identity = dagr_k8s::identity::AttemptIdentity {
        key,
        pipeline: PIPELINE.to_string(),
        structural_fingerprint: structural,
        policy_hash: policy,
        tool_version: TOOL_VERSION.to_string(),
        image_digest: IMAGE_DIGEST.to_string(),
        owner: "orchestrator-that-died".to_string(),
    };
    let mut survivor = PodSnapshot::new(&name, "50", PodPhase::Running, &identity);
    survivor.uid = Some("uid-survivor".to_string());
    lifecycle.preexisting(survivor);

    let shard_root = blobs.clone();
    let driver = {
        let control = control.clone();
        let name = name.clone();
        rt.spawn(async move {
            control.await_watch().await;
            write_shard(&shard_root, RUN_ID, PLACED, 1, TerminalState::Succeeded);
            transition(&control, &name, PodPhase::Succeeded, PLACED).await;
        })
    };

    let sink = MemorySink::default();
    let config = RunConfig::new(tmp.path().join("store").to_string_lossy().to_string())
        .run_id(RUN_ID)
        .executor(ExecutorKind::Kubernetes);
    let placed = build_flow()
        .run_placed(
            PIPELINE,
            &config,
            sink,
            TickClock::default(),
            RemoteCluster::new(api, lifecycle.clone(), target(&blobs)),
        )
        .expect("the run drives");
    rt.block_on(driver).expect("the platform driver finishes");

    assert_eq!(
        placed.discovery.unpatched,
        vec![name],
        "the pod dagr could not take ownership of is named, not silently adopted"
    );
    assert!(
        placed.discovery.adopted.is_empty(),
        "an ownership that was never taken is never recorded as taken"
    );
    let said = placed.diagnostics.join("\n");
    assert!(
        said.contains("patch"),
        "the missing verb is named in the run's diagnostics: {said}"
    );
    assert!(
        lifecycle.deleted().is_empty(),
        "a label write that did not land says nothing about the work in flight"
    );
}
