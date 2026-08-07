//! **The remote executor, wired into the run path** (M10, T112, ADR 115).
//!
//! T105 made `--dagr.executor=k8s` a recognized stub that refused at bootstrap, and
//! T108 and T109 both recorded, in as many words, why they left it refusing:
//! *"lifting the refusal requires the flow-level wiring that turns a
//! `RunnableFlow`'s placed nodes into `K8sNodeRunner`s — which needs cluster access,
//! the orchestrator's own published image digest, a namespace, and a blob container
//! both sides can reach."* This module is that wiring, and
//! [`RunnableFlow::run_placed`](crate::run_flow::RunnableFlow::run_placed) is its
//! entry point.
//!
//! # What it does, in order
//!
//! 1. **Discovers** the run's existing pods and reclaims what this build may
//!    ([`crate::adoption::discover`], one pass, before any node is submitted). This
//!    is T109's deferral: the kill-restart guarantee is a property of the *run path*,
//!    not of a function a test calls.
//! 2. **Opens one watch** for the whole run ([`crate::pod_observer::PodObserver`]),
//!    which every placed node's runner then registers an attempt waiter against.
//! 3. **Builds the runner map**: a [`K8sNodeRunner`](crate::k8s_runner::K8sNodeRunner)
//!    for each placed node, the flow's own local runner for everything else. One
//!    `RunPlan`, one `drive()` call, no driver change — which is ADR 115's central
//!    claim, now exercised by the shipped path rather than only by a test.
//! 4. **Drives** the run, then shuts the observer down and reports what it saw.
//!
//! # The refusal that replaces the old one
//!
//! Lifting the bootstrap refusal did not lift the reason for it. T105 refused
//! because *"a `RunConfig::executor(Kubernetes)` would run every node in-process
//! while the operator believed their placement was honoured — the one outcome worse
//! than refusing"*, and that is still true of a run that selects the remote executor
//! and hands the run path no cluster. So the check moved rather than vanished: the
//! driver refuses at bootstrap when the executor honours placement, the pipeline has
//! placed nodes, and the plan carries no remote executor. A pipeline with **no**
//! placed node runs fine under `--dagr.executor=k8s` — there is nothing to place, so
//! there is nothing to substitute.
//!
//! # One runtime owns every cluster call
//!
//! The driver builds its own runtimes and the observer needs one before the driver
//! exists, so there are two. Every call to the cluster is therefore forwarded onto
//! **one** of them — the observer's — by [`Pinned`], which clones the arguments into
//! a `'static` future and spawns it on that runtime's handle. An I/O resource in
//! `tokio` belongs to the reactor it was created on, so a client built for the
//! observer and then polled on the driver's task runtime is a class of failure that
//! only appears against a real API server; pinning removes the class instead of
//! documenting it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use dagr_core::assembly::{AssemblyError, Placement};
use dagr_k8s::adoption::{BuildIdentity, LabelPatch};
use dagr_k8s::api::{ApiFailure, CreatedPod, PodApi, PodLifecycle, PodSnapshot};
use dagr_k8s::executor::{ClusterRetry, PodPlacement, PodSpec};
use dagr_k8s::observer::{ObserverLimits, RunSelector};
use dagr_k8s::rbac::{MissingPermission, PodVerb};

use crate::adoption::{AdoptionConfig, DiscoveryReport};
use crate::driver::NodeRunner;
use crate::k8s_runner::{K8sNodeRunner, RemoteAttemptConfig};
use crate::pod_observer::PodObserver;
use crate::run_flow::RunReport;

/// How long the observer is given to stop once the run has ended.
const OBSERVER_SHUTDOWN_BUDGET: Duration = Duration::from_secs(5);

// ===========================================================================
// The target
// ===========================================================================

/// Everything about the cluster that is the **operator's** to state, not the
/// pipeline's.
///
/// It is a value the binary supplies rather than a set of `--dagr.*` knobs, and that
/// is deliberate. A namespace, an image reference and a blob container are
/// deployment facts: the binary that was built into that image is the thing that
/// knows them, and inventing three more environment variables would put the
/// pipeline's *deployment* into the same tier as its *runtime knobs*, which
/// `arch.md`'s configuration surface deliberately keeps separate.
#[derive(Debug, Clone)]
pub struct RemoteTarget {
    namespace: String,
    image: String,
    image_digest: String,
    blob_container: PathBuf,
    owner: String,
    tool_version: String,
    command: Vec<String>,
    launch_retries: u32,
    pre_start_bound: Duration,
    cluster_retry: ClusterRetry,
    observer_limits: ObserverLimits,
    prior_run_id: Option<String>,
}

impl RemoteTarget {
    /// The four facts with no defensible default: where pods go, what image they
    /// run, which digest that image is, and where both sides exchange bytes.
    #[must_use]
    pub fn new(
        namespace: impl Into<String>,
        image: impl Into<String>,
        image_digest: impl Into<String>,
        blob_container: impl AsRef<Path>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            image: image.into(),
            image_digest: image_digest.into(),
            blob_container: blob_container.as_ref().to_path_buf(),
            owner: default_owner(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            command: vec!["dagr".to_string(), "exec-node".to_string()],
            launch_retries: crate::config::POD_LAUNCH_RETRIES_DEFAULT,
            pre_start_bound: crate::k8s_runner::DEFAULT_PRE_START_BOUND,
            // The platform must not retry, because dagr does: two retry loops
            // duplicate an attempt. `build_pod` refuses the other value outright.
            cluster_retry: ClusterRetry::Disabled,
            observer_limits: ObserverLimits::default(),
            prior_run_id: None,
        }
    }

    /// This orchestrator's owner key — the value adoption's label patch writes.
    #[must_use]
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// The comparability token a shard must agree with. Defaults to this build's
    /// package version.
    #[must_use]
    pub fn tool_version(mut self, version: impl Into<String>) -> Self {
        self.tool_version = version.into();
        self
    }

    /// The container's argv prefix — the pod-side `exec-node` invocation, before the
    /// per-attempt arguments the runner appends.
    #[must_use]
    pub fn command<I, S>(mut self, command: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.command = command.into_iter().map(Into::into).collect();
        self
    }

    /// The **infrastructure** retry budget: how many extra launches a pod that never
    /// started may have. Distinct from `NodePolicy::retries`, which is the node's.
    #[must_use]
    pub fn launch_retries(mut self, retries: u32) -> Self {
        self.launch_retries = retries;
        self
    }

    /// How long a pre-start signal must persist before it is charged as a launch
    /// failure.
    #[must_use]
    pub fn pre_start_bound(mut self, bound: Duration) -> Self {
        self.pre_start_bound = bound;
        self
    }

    /// The observer's reconnect discipline. The default is T107's.
    #[must_use]
    pub fn observer_limits(mut self, limits: ObserverLimits) -> Self {
        self.observer_limits = limits;
        self
    }

    /// The run this one **resumes**. Its still-live pods are revoked at startup,
    /// never adopted — a resumed run mints a new id, and adopting across that
    /// boundary would blur two runs' event streams (T109's recorded resolution).
    #[must_use]
    pub fn resuming(mut self, prior_run_id: impl Into<String>) -> Self {
        self.prior_run_id = Some(prior_run_id.into());
        self
    }

    /// The namespace pods are created in.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// A stable-enough owner key for one orchestrator process: the host and the pid.
///
/// It only has to distinguish *this* process from the one that died, which is what
/// adoption compares, so a name a human can also read in `kubectl get pods -L` is
/// worth more than a UUID here.
fn default_owner() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "orchestrator".to_string());
    format!("{host}-{}", std::process::id())
}

// ===========================================================================
// The cluster
// ===========================================================================

/// The two ports plus the target: everything the run path needs to place work.
///
/// The ports are separate types because the grants are separate: [`PodApi`] is the
/// read half the observer holds, [`PodLifecycle`] is the write half the executor and
/// the adoption pass hold. A single value may implement both (the shipped client
/// does), and passing the same value twice is the ordinary case.
pub struct RemoteCluster<A: PodApi, L: PodLifecycle + Clone> {
    api: A,
    lifecycle: L,
    target: RemoteTarget,
}

impl<A: PodApi, L: PodLifecycle + Clone> RemoteCluster<A, L> {
    /// Bind a read port, a write port and a target together.
    #[must_use]
    pub fn new(api: A, lifecycle: L, target: RemoteTarget) -> Self {
        Self {
            api,
            lifecycle,
            target,
        }
    }
}

// ===========================================================================
// Pinning every cluster call to one runtime
// ===========================================================================

/// A [`PodLifecycle`] that forwards every call onto one runtime.
///
/// See the module docs: the observer's runtime and the driver's task runtime are
/// different, and a `tokio` I/O resource belongs to the reactor it was created on.
/// Every argument is cloned into a `'static` future so it can be spawned; the
/// clones are a pod name, a label map and a pod spec, all small and all made once
/// per API call — next to a pod's ~1 s placement latency the cost is not measurable.
#[derive(Clone)]
pub(crate) struct Pinned<L: PodLifecycle + Clone> {
    inner: L,
    handle: tokio::runtime::Handle,
}

impl<L: PodLifecycle + Clone> Pinned<L> {
    fn new(inner: L, handle: tokio::runtime::Handle) -> Self {
        Self { inner, handle }
    }
}

impl<L: PodLifecycle + Clone> PodLifecycle for Pinned<L> {
    fn create(
        &self,
        spec: &PodSpec,
    ) -> impl std::future::Future<Output = Result<CreatedPod, ApiFailure>> + Send {
        let inner = self.inner.clone();
        let spec = spec.clone();
        let joined = self.handle.spawn(async move { inner.create(&spec).await });
        async move { join(joined).await }
    }

    fn delete(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        let inner = self.inner.clone();
        let name = name.to_string();
        let joined = self.handle.spawn(async move { inner.delete(&name).await });
        async move { join(joined).await }
    }

    fn get(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<PodSnapshot>, ApiFailure>> + Send {
        let inner = self.inner.clone();
        let name = name.to_string();
        let joined = self.handle.spawn(async move { inner.get(&name).await });
        async move { join(joined).await }
    }

    fn patch_labels(
        &self,
        name: &str,
        labels: &LabelPatch,
    ) -> impl std::future::Future<Output = Result<(), ApiFailure>> + Send {
        let inner = self.inner.clone();
        let name = name.to_string();
        let labels = labels.clone();
        let joined = self
            .handle
            .spawn(async move { inner.patch_labels(&name, &labels).await });
        async move { join(joined).await }
    }
}

/// Await a spawned cluster call, turning a panic in it into an `ApiFailure` rather
/// than a second panic on this side.
async fn join<T>(handle: tokio::task::JoinHandle<Result<T, ApiFailure>>) -> Result<T, ApiFailure> {
    match handle.await {
        Ok(result) => result,
        Err(join) => Err(ApiFailure::transport(format!(
            "the cluster call did not complete: {join}"
        ))),
    }
}

// ===========================================================================
// Errors
// ===========================================================================

/// Why a placed run could not start.
///
/// Every variant is a **bootstrap** condition: nothing has been submitted and no
/// node has run. A failure *during* the run is reported through the run's outcome
/// and terminal states like any other, because remoteness changes where a node runs
/// and not how failure is reported.
#[derive(Debug)]
pub enum RemoteRunError {
    /// The flow does not assemble.
    Assembly(AssemblyError),
    /// The run configuration carries no explicit run id.
    NoRunId,
    /// The runtime the observer needs could not be built.
    Runtime(std::io::Error),
    /// A placed node consumes values from upstream nodes, which this executor cannot
    /// yet bind: an attempt's inputs are references it is *given*, and nothing in the
    /// shipped path turns a local upstream's in-memory value into one.
    UnboundInputs {
        /// The node.
        node: String,
        /// How many inputs it declares.
        arity: usize,
    },
    /// A placed node whose output type has no codec. Its value provably cannot cross
    /// a process boundary, so submitting it would produce bytes nothing could decode.
    NotRemoteEligible {
        /// The node.
        node: String,
    },
    /// The startup discovery pass could not list the run's pods, so the run cannot
    /// tell what is already in flight. Submitting anyway would duplicate every pod it
    /// could not see.
    Discovery {
        /// What the platform said.
        failure: ApiFailure,
        /// The classified permission, when that is what it was.
        permission: Option<MissingPermission>,
    },
}

impl std::fmt::Display for RemoteRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Assembly(err) => write!(f, "the flow does not assemble: {err}"),
            Self::NoRunId => write!(
                f,
                "a placed run needs an explicit run id (`RunConfig::run_id`): adoption after \
                 a restart is scoped to one, so a minted id would make the kill-restart \
                 guarantee unstatable"
            ),
            Self::Runtime(err) => write!(f, "cannot build the observer's runtime: {err}"),
            Self::UnboundInputs { node, arity } => write!(
                f,
                "refusing to place `{node}`: it consumes {arity} upstream value(s), and a \
                 remote attempt is bound to references it is given rather than to values in \
                 this process. Place a consume-nothing node, or run this pipeline under \
                 `--dagr.executor=local`."
            ),
            Self::NotRemoteEligible { node } => write!(
                f,
                "refusing to place `{node}`: its output type has no codec, so the value a \
                 pod produced could never be decoded back into this process. Register it \
                 through `register_source_placed` (whose `Payload` bound makes this a \
                 compile error), or run this pipeline under `--dagr.executor=local`."
            ),
            Self::Discovery {
                failure,
                permission,
            } => match permission {
                Some(missing) => write!(
                    f,
                    "cannot list this run's pods, so what is already in flight is unknown: \
                     {missing}"
                ),
                None => write!(
                    f,
                    "cannot list this run's pods, so what is already in flight is unknown: \
                     {failure}"
                ),
            },
        }
    }
}

impl std::error::Error for RemoteRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Assembly(err) => Some(err),
            Self::Runtime(err) => Some(err),
            Self::Discovery { failure, .. } => Some(failure),
            Self::NoRunId | Self::UnboundInputs { .. } | Self::NotRemoteEligible { .. } => None,
        }
    }
}

// ===========================================================================
// The result
// ===========================================================================

/// What a placed run produced.
///
/// The run's own [`RunReport`] is the same value a local run returns — that is the
/// dual-mode promise. The other two fields are the operator-facing account of the
/// *placement*: what discovery reclaimed, and every diagnostic the cluster produced
/// that an operator can act on.
pub struct PlacedRun {
    /// The ordinary run report: outcome, per-node terminal states, typed outputs.
    pub report: RunReport,
    /// What the startup discovery pass did, in pod names.
    pub discovery: DiscoveryReport,
    /// Actionable diagnostics: a missing RBAC verb, a pod the platform refused, an
    /// observer that stopped. Empty for a run that went well.
    pub diagnostics: Vec<String>,
}

/// Decode the bytes a placed attempt produced into that node's own output slot.
///
/// Built by [`crate::run_flow`], where the node's concrete output type — and so its
/// codec — is still known. It is the **return half** of the data path: the pod
/// encoded the value into the blob container, and a local consumer downstream of the
/// placed node reads it out of the slot exactly as it would after a local run. The
/// error is already a sentence, because classifying it needs the codec's error type
/// and that type is not in scope here.
pub(crate) type SlotFill = Box<dyn Fn(&[u8]) -> Result<(), String> + Send>;

/// A diagnostics collector shared by every placed node's runner and the run path.
#[derive(Clone, Default)]
pub(crate) struct Diagnostics {
    lines: Arc<Mutex<Vec<String>>>,
}

impl Diagnostics {
    /// Record one operator-facing line.
    ///
    /// Poison policy: **recover**. This list is a report, not run state — a node
    /// whose attempt panicked is exactly the run that most needs its diagnostics
    /// printed, so poisoning here must not escalate into a second panic and take
    /// the report down with it.
    fn push(&self, line: String) {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }

    /// Take everything collected so far, leaving the list empty.
    ///
    /// Poison policy: **recover**, for the same reason as
    /// [`push`](Self::push) — the drain happens at run end, which is the one
    /// moment the lines are about to be shown to somebody.
    fn drain(&self) -> Vec<String> {
        let mut guard = self.lines.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *guard)
    }
}

// ===========================================================================
// The wiring
// ===========================================================================

/// The pieces `RunnableFlow::run_placed` needs from this module, built once.
pub(crate) struct RemoteWiring<L: PodLifecycle + Clone> {
    pub(crate) runtime: tokio::runtime::Runtime,
    pub(crate) observer: PodObserver,
    pub(crate) lifecycle: Pinned<L>,
    pub(crate) discovery: DiscoveryReport,
    pub(crate) diagnostics: Diagnostics,
    pub(crate) target: RemoteTarget,
    pub(crate) run_id: String,
}

/// Stand the remote executor up: a runtime, one discovery pass, and one watch.
///
/// # Errors
///
/// [`RemoteRunError::Runtime`] or [`RemoteRunError::Discovery`].
pub(crate) fn stand_up<A: PodApi, L: PodLifecycle + Clone>(
    cluster: RemoteCluster<A, L>,
    run_id: &str,
    structural_fingerprint: &str,
    must_run: BTreeSet<String>,
) -> Result<RemoteWiring<L>, RemoteRunError> {
    let RemoteCluster {
        api,
        lifecycle,
        target,
    } = cluster;

    // Two workers: one for the watch's read loop and one for the cluster calls the
    // node runners forward here. The driver builds its own runtimes for the run
    // itself; this one exists only for the cluster.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("dagr-cluster")
        .build()
        .map_err(RemoteRunError::Runtime)?;

    let diagnostics = Diagnostics::default();
    let pinned = Pinned::new(lifecycle, runtime.handle().clone());

    // --- 1. Discovery, before anything is submitted (T109 wired into the run path).
    let adoption = AdoptionConfig {
        run_id: run_id.to_string(),
        owner: target.owner.clone(),
        build: BuildIdentity::new(
            structural_fingerprint,
            target.tool_version.clone(),
            target.image_digest.clone(),
        ),
        must_run,
        prior_run_id: target.prior_run_id.clone(),
    };
    let discovery = runtime
        .block_on(crate::adoption::discover(&api, &pinned, &adoption))
        .map_err(|failure| {
            let permission = dagr_k8s::rbac::classify(PodVerb::List, &target.namespace, &failure);
            RemoteRunError::Discovery {
                failure,
                permission,
            }
        })?;

    // A pod this build would have reclaimed and could not is a *degradation*, not a
    // failure — the node falls back to the submission probe. It is named here so the
    // degradation is visible, with the missing verb when that is what it was.
    for pod in &discovery.unpatched {
        let reason = discovery.unpatched_reason(pod).unwrap_or_default();
        match dagr_k8s::rbac::classify_text(&target.namespace, reason) {
            Some(missing) => diagnostics.push(format!(
                "could not take ownership of pod `{pod}`: {missing}"
            )),
            None => diagnostics.push(format!(
                "could not `patch` the ownership label on pod `{pod}` ({reason}); the node \
                 falls back to the submission probe"
            )),
        }
    }
    for pod in &discovery.refused {
        if let Some(report) = discovery
            .refusal_messages()
            .get(pod.as_str())
            .map(String::as_str)
        {
            diagnostics.push(report.to_string());
        }
    }

    // --- 2. One watch for the whole run.
    let guard = runtime.enter();
    let observer = PodObserver::spawn(
        api,
        RunSelector::new(run_id, structural_fingerprint),
        target.observer_limits.clone(),
    );
    drop(guard);

    Ok(RemoteWiring {
        runtime,
        observer,
        lifecycle: pinned,
        discovery,
        diagnostics,
        target,
        run_id: run_id.to_string(),
    })
}

/// Everything about **one placed node** that its runner needs and the wiring does
/// not already hold.
///
/// A struct rather than eight more parameters: the run-wide facts (namespace, image,
/// owner, observer, lifecycle) live on the [`RemoteWiring`] and the per-node ones
/// live here, which is also the boundary a reader has to know anyway.
pub(crate) struct PlacedNode<'a> {
    /// The node itself — its name, policy, and declared input arity.
    pub(crate) node: &'a dagr_core::flow::PipelineNode,
    /// The pipeline's stable name, recorded as an annotation.
    pub(crate) pipeline_name: &'a str,
    /// The run's structural fingerprint. A shard reporting a different one is
    /// refused rather than replayed.
    pub(crate) structural_fingerprint: &'a str,
    /// The run's policy hash.
    pub(crate) policy_hash: &'a str,
    /// The author-declared placement, carried through as opaque strings.
    pub(crate) placement: Placement,
    /// The write-ahead submission log's handle.
    pub(crate) submissions: crate::submission_log::SubmissionHandle,
    /// The injected wait seam the retry backoff and the attempt deadline measure
    /// through.
    pub(crate) timer: Arc<dyn crate::run_flow::AttemptTimer>,
    /// The return half of the data path: decode what the pod produced into this
    /// node's own output slot.
    pub(crate) fill: SlotFill,
}

impl<L: PodLifecycle + Clone> RemoteWiring<L> {
    /// Build the runner for one placed node.
    pub(crate) fn runner(&self, placed: PlacedNode<'_>) -> Box<dyn NodeRunner> {
        let PlacedNode {
            node,
            pipeline_name,
            structural_fingerprint,
            policy_hash,
            placement,
            submissions,
            timer,
            fill,
        } = placed;
        let policy = node.policy();
        let config = RemoteAttemptConfig {
            pipeline: pipeline_name.to_string(),
            namespace: self.target.namespace.clone(),
            image: self.target.image.clone(),
            image_digest: self.target.image_digest.clone(),
            structural_fingerprint: structural_fingerprint.to_string(),
            policy_hash: policy_hash.to_string(),
            tool_version: self.target.tool_version.clone(),
            owner: self.target.owner.clone(),
            blob_container: self.target.blob_container.clone(),
            // A consume-nothing node: the empty array is the encoding, and the
            // arity check below is what makes an accidental mismatch detectable.
            inputs: Vec::new(),
            declared_arity: node.data_edges().len(),
            placement: to_pod_placement(&placement),
            cluster_retry: self.target.cluster_retry,
            launch_retries: self.target.launch_retries,
            pre_start_bound: self.target.pre_start_bound,
            retry: policy.retry_config(),
            timeout: policy.timeout_budget(),
            command: self.target.command.clone(),
        };
        let runner = K8sNodeRunner::new(
            node.name(),
            self.run_id.clone(),
            self.lifecycle.clone(),
            self.observer.handle(),
            submissions,
            timer,
            config,
        )
        .with_adoptions(self.discovery.adoptions.clone());
        Box::new(Reporting {
            inner: runner,
            diagnostics: self.diagnostics.clone(),
            namespace: self.target.namespace.clone(),
            blob_container: self.target.blob_container.clone(),
            fill,
        })
    }

    /// Shut the watch down and fold everything the cluster said into one list.
    pub(crate) fn finish(self) -> (DiscoveryReport, Vec<String>) {
        let RemoteWiring {
            runtime,
            observer,
            discovery,
            diagnostics,
            target,
            ..
        } = self;
        let report = runtime.block_on(observer.shutdown(OBSERVER_SHUTDOWN_BUDGET));
        match report {
            Ok(report) => {
                if let Some(failure) = &report.failure {
                    match dagr_k8s::rbac::classify_termination(
                        &target.namespace,
                        &failure.cause,
                        &failure.last_message,
                    ) {
                        Some(missing) => {
                            diagnostics.push(format!("the pod observer stopped: {missing}"));
                        }
                        None => diagnostics.push(format!(
                            "the pod observer stopped after {} consecutive failures over {:?}: {}",
                            failure.consecutive_failures, failure.elapsed, failure.last_message
                        )),
                    }
                }
                for foreign in &report.foreign {
                    diagnostics.push(format!(
                        "pod `{}` matched this run's selector and is not this run's: {}",
                        foreign.pod_name, foreign.reason
                    ));
                }
            }
            Err(overrun) => diagnostics.push(format!(
                "the pod observer did not stop inside its shutdown budget: {overrun}"
            )),
        }
        let lines = diagnostics.drain();
        // The runtime is dropped last, after the observer's task has been joined.
        drop(runtime);
        (discovery, lines)
    }
}

/// The author-declared placement, as opaque strings the executor never parses.
fn to_pod_placement(placement: &Placement) -> PodPlacement {
    PodPlacement {
        cpu: placement.cpu_request().map(str::to_string),
        memory: placement.memory_request().map(str::to_string),
        node_selectors: placement
            .node_selector_pairs()
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
        tolerations: placement
            .toleration_strings()
            .iter()
            .map(|t| (*t).to_string())
            .collect(),
    }
}

// ===========================================================================
// The reporting wrapper
// ===========================================================================

/// A [`NodeRunner`] that delegates to a [`K8sNodeRunner`], collects what the cluster
/// said on the way past, and lands the value the pod produced in the node's own
/// output slot.
///
/// The classified failure and the platform's own reason strings are the runner's,
/// and they are gone once the plan is dropped — so they are read here, at the one
/// moment both exist: immediately after the attempt returns.
///
/// # Why the slot fill lives here
///
/// A local runner writes into its slot as the last thing an attempt does. A placed
/// one cannot: the value was produced in another process and exists as bytes in the
/// blob container. Somewhere between "the attempt succeeded" and "a local consumer
/// reads the slot" those bytes have to be fetched and decoded, and this is the only
/// place that has both the durable reference and the node's codec. Skipping it is
/// not a degradation — a consumer reading an unfilled slot is a framework-defect
/// panic — so a fetch or decode that fails **fails the node**, loudly, rather than
/// returning a success nothing downstream can use.
struct Reporting<L: PodLifecycle> {
    inner: K8sNodeRunner<L>,
    diagnostics: Diagnostics,
    namespace: String,
    blob_container: PathBuf,
    fill: SlotFill,
}

impl<L: PodLifecycle> Reporting<L> {
    /// Fetch the bytes the attempt produced and decode them into the node's slot.
    ///
    /// Called only after a **success**, so a shard that recorded no output reference
    /// is an error rather than a no-op: a node that succeeded and produced nothing
    /// its consumers can read has not, from the graph's point of view, succeeded.
    fn land_output(&self) -> Result<(), String> {
        let Some(uri) = self.inner.durable_reference() else {
            return Err(format!(
                "`{}` succeeded remotely but its attempt shard recorded no output \
                 reference, so there is nothing for its consumers to read",
                self.inner.name()
            ));
        };
        let reference = dagr_blob::BlobRef::parse(&uri)
            .map_err(|err| format!("the produced reference `{uri}` is unusable: {err}"))?;
        // The container the pod wrote into is the one this orchestrator was told
        // about, not the one the reference names: a reference is data, and trusting
        // its container would let a shard redirect the read. The key is the
        // reference's, and `get` verifies the bytes against it.
        let store = dagr_blob::LocalFsBlob::open(&self.blob_container);
        let bytes = dagr_blob::BlobStore::get(&store, reference.key())
            .map_err(|err| format!("cannot read the produced value `{uri}`: {err}"))?;
        (self.fill)(&bytes)
    }
}

impl<L: PodLifecycle> NodeRunner for Reporting<L> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn run<'a>(
        &'a mut self,
        ctx: &'a dagr_core::context::RunContext,
        sink: &'a mut (dyn dagr_core::execution::AttemptEventSink + Send),
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = dagr_core::context::TerminalState> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut state = self.inner.run(ctx, sink).await;
            if let Some(failure) = self.inner.last_failure() {
                let text = failure.to_string();
                match dagr_k8s::rbac::classify_text(&self.namespace, &text) {
                    Some(missing) => self
                        .diagnostics
                        .push(format!("`{}` could not run: {missing}", self.inner.name())),
                    None => self.diagnostics.push(text),
                }
            }
            for diagnostic in self.inner.diagnostics() {
                self.diagnostics
                    .push(format!("`{}`: {diagnostic}", self.inner.name()));
            }
            // The return half of the data path. Only a success produces a value, and
            // a success whose value cannot be landed is reported as a failure rather
            // than left for a consumer to trip over.
            if state == dagr_core::context::TerminalState::Succeeded
                && let Err(why) = self.land_output()
            {
                self.diagnostics
                    .push(format!("`{}`: {why}", self.inner.name()));
                state = dagr_core::context::TerminalState::Failed;
            }
            state
        })
    }

    fn durable_reference(&self) -> Option<String> {
        self.inner.durable_reference()
    }

    fn durable_reference_meta(&self) -> Option<dagr_artifact::event_stream::DurableReferenceMeta> {
        self.inner.durable_reference_meta()
    }
}
