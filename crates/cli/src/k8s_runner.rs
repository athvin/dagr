//! The **Kubernetes node runner** (M10, T108, ADR 115): one more implementation of
//! `NodeRunner`, which happens to submit a pod.
//!
//! ADR 115's central claim is that [`NodeRunner`](crate::driver::NodeRunner) is
//! *already* the "where does this node run" seam: it is type-erased, it emits
//! through an **injected** `AttemptEventSink`, it returns only a `TerminalState`,
//! and it already has `durable_reference` / `durable_reference_meta` hooks the
//! driver reads after a success. So the run loop, readiness cascade, admission
//! ledger, teardown phase, resume path, sink-fault handling and exit-code precedence
//! are untouched by remoteness. This module adds the implementation; it changes
//! nothing about the driver.
//!
//! One attempt is: **record → submit → await → read shard → replay →
//! `TerminalState`**.
//!
//! # The two retry budgets
//!
//! A pod that never *started* — unschedulable, an image that will not pull, a quota
//! rejection — is an **infrastructure** failure. Charging it against
//! `NodePolicy::retries` would let a cluster at capacity burn a node's entire retry
//! budget without executing anything, so it is retried against
//! [`launch_retries`](RemoteAttemptConfig::launch_retries) instead and emits **no
//! user-visible attempt** at all: the driver never sees a failed attempt, no retry
//! is consumed, and the artifact shows no phantom try. A pod whose container *ran*
//! and whose task failed consumes `NodePolicy::retries` exactly as a local node
//! does, with T102's real backoff between attempts.
//!
//! # Why a pre-start failure is not detected by awaiting a terminal phase
//!
//! Because none arrives. T101 ran an unpullable image against a real cluster and the
//! pod **never reached a terminal phase**: it sat in `Pending` with
//! `waiting.reason=ImagePullBackOff` and no terminated state, because the platform
//! retries the pull indefinitely. An unschedulable pod is the same shape and does
//! not even have a container status to read. So this runner watches the two
//! pre-start surfaces `dagr_k8s::executor` classifies and applies **its own bound**
//! — the bound is what ends the wait, because the platform will not.
//!
//! # Ownership, across a restart (T109)
//!
//! One attempt is one pod, and a pod outlives the process that submitted it. So
//! three acts here are about **ownership** rather than execution:
//!
//! - an attempt whose pod was **adopted** by [`crate::adoption`]'s startup pass
//!   waits on that pod instead of creating one, through the same code path a
//!   freshly created pod takes;
//! - a pod whose outcome has been **consumed** is tombstoned, so a later discovery
//!   excludes it and the same attempt cannot be adopted twice;
//! - every orchestrator-initiated delete — cancellation, the node's timeout, a
//!   pre-start failure — is a **revocation**: the owner label is cleared *before*
//!   the delete, so a watcher can tell dagr's teardown from an external deletion.
//!
//! What stays elsewhere: the metastore projection of the `attempt-submitted`
//! records written here is T111's; nothing here writes SQL. The real-cluster proof
//! is T112's; every test here drives T107's fake.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dagr_artifact::event_stream::{
    AttemptSubmittedRecord, ConsumedInput, DurableReferenceMeta as WireMeta,
};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{AttemptEvent, AttemptEventSink, NoJitter, RetryConfig};
use dagr_k8s::adoption::{AdoptionRefusal, DiscoveredPod, is_tombstoned};
use dagr_k8s::api::{ApiFailure, CreatedPod, PodLifecycle, PodSnapshot};
use dagr_k8s::executor::{
    ClusterRetry, ClusterRetryRefused, CredentialInReference, PodOutcome, PodPlacement, PodRequest,
    PodSpec, PodStatusFacts, PreStartFailure, build_pod, classify_pre_start, classify_terminal,
};
use dagr_k8s::identity::{AttemptIdentity, AttemptKey};
use dagr_k8s::observer::PodObservation;

use crate::adoption::{Adoptions, Claim};
use crate::driver::NodeRunner;
use crate::pod_observer::{AttemptWaiter, ObserverHandle, WaiterEvent};
use crate::run_flow::AttemptTimer;
use crate::shard::{AttemptShard, ShardError};
use crate::submission_log::SubmissionHandle;

/// The executor's name, as it appears on an `attempt-submitted` record.
pub const EXECUTOR_NAME: &str = "k8s";

/// How often the await loop wakes to observe the cancellation flag.
///
/// Cancellation is an observe-only flag rather than an awaitable token
/// (`dagr_core::context::CancellationSignal`), so the loop polls it. The interval is
/// short enough that a per-attempt cancel deletes its pod well inside the grace
/// period, and long enough to be free next to a pod's ~1 s placement latency.
const CANCEL_POLL: Duration = Duration::from_millis(50);

/// The default bound the runner applies to a pre-start signal before charging it as
/// an infrastructure failure.
///
/// It exists because a fatal-looking waiting reason can still resolve: a registry
/// that was briefly unreachable produces `ErrImagePull` and then succeeds. Waiting a
/// little absorbs that; waiting indefinitely is the hang T101 warned about. Settable
/// per run and deliberately **not** a `--dagr.*` knob, on T107's stall-bound
/// precedent — it becomes one only if the acceptance demo shows it needs to be.
pub const DEFAULT_PRE_START_BOUND: Duration = Duration::from_mins(1);

// ===========================================================================
// Configuration
// ===========================================================================

/// Everything one placed node needs in order to run its attempts remotely.
///
/// The references are supplied rather than resolved here: what a node's inputs *are*
/// is the flow's business, and keeping the runner a function of them is what makes
/// the arity check possible at all.
#[derive(Debug, Clone)]
pub struct RemoteAttemptConfig {
    /// The pipeline's stable name, recorded as an annotation.
    pub pipeline: String,
    /// The namespace to submit into.
    pub namespace: String,
    /// The image reference — the orchestrator's own, so the pod runs this program.
    pub image: String,
    /// The image digest, recorded on the submission record and as an annotation.
    pub image_digest: String,
    /// The run's structural fingerprint. A shard reporting a different one is
    /// refused rather than replayed.
    pub structural_fingerprint: String,
    /// The run's policy hash.
    pub policy_hash: String,
    /// The tool version — the comparability token, not the package version.
    pub tool_version: String,
    /// This orchestrator's owner key.
    pub owner: String,
    /// The blob container the pod writes its output and shard into, and this runner
    /// reads them out of.
    pub blob_container: PathBuf,
    /// The **ordered, positional** references handed to the attempt. Empty for a
    /// consume-nothing source.
    pub inputs: Vec<ConsumedInput>,
    /// The node's declared input arity. Checked against `inputs` before anything is
    /// launched — the detectability the empty-array encoding buys.
    pub declared_arity: usize,
    /// The author-declared placement, carried through as opaque strings.
    pub placement: PodPlacement,
    /// Whether the platform may retry. [`ClusterRetry::Enabled`] is refused.
    pub cluster_retry: ClusterRetry,
    /// The **infrastructure** retry budget: how many extra launches a pod that never
    /// started may have. Distinct from `NodePolicy::retries`.
    pub launch_retries: u32,
    /// How long a pre-start signal must persist before it is charged as a launch
    /// failure. See [`DEFAULT_PRE_START_BOUND`].
    pub pre_start_bound: Duration,
    /// The node's own retry discipline, from `NodePolicy`.
    pub retry: RetryConfig,
    /// The node's declared **per-attempt timeout**, from `NodePolicy`.
    ///
    /// A remote attempt is await-bound by construction — the orchestrator is doing
    /// nothing but waiting on a watch — so C14's honest timeout semantics take their
    /// *cancellable* form: the wait is abandoned, the pod is **deleted**, and the
    /// attempt is `timed-out` immediately. There is no unkillable closure to hand
    /// off to the driver's isolated deadline, and nothing is left running: deleting
    /// the pod is the one cancellation dagr can actually perform on remote work.
    ///
    /// It bounds the whole attempt, launch retries included. A node that declares
    /// thirty seconds means thirty seconds, not thirty seconds per submission.
    pub timeout: Option<Duration>,
    /// The container's argv.
    pub command: Vec<String>,
}

// ===========================================================================
// Errors
// ===========================================================================

/// Why a remote attempt could not be launched, or could not be believed.
///
/// Every variant names something an operator can act on. None of them is a new
/// terminal state: they all resolve to `failed`, carrying their text as the node's
/// classified failure.
#[derive(Debug)]
pub enum RemoteLaunchError {
    /// The references assembled for the node disagree with its declared arity.
    ArityMismatch {
        /// The node.
        node: String,
        /// What the node declares it consumes.
        declared: usize,
        /// What was actually assembled for it.
        assembled: usize,
    },
    /// A reference carried a credential and was refused before being recorded.
    CredentialBearing {
        /// The node.
        node: String,
        /// The marker that matched — never the reference, and never the secret.
        source: CredentialInReference,
    },
    /// The configuration asked the platform to retry as well as dagr.
    ClusterRetry(ClusterRetryRefused),
    /// The write-ahead record could not be made durable, so nothing was submitted.
    /// Refusing here is the point: an unrecorded submission is the state the record
    /// exists to prevent.
    Unrecordable {
        /// The node.
        node: String,
        /// The underlying failure.
        message: String,
    },
    /// The infrastructure budget was spent without the pod ever starting.
    LaunchExhausted {
        /// The node.
        node: String,
        /// How many launches were attempted.
        launches: u32,
        /// The infrastructure cause, in the platform's own words.
        cause: String,
    },
    /// The pod reached a terminal phase and left no readable shard — never a silent
    /// success, and never a hang (T101's rule).
    ShardUnreadable {
        /// The node.
        node: String,
        /// The pod, by name.
        pod: String,
        /// The pod's status, as the platform reported it.
        status: String,
        /// Why the shard could not be read.
        message: String,
    },
    /// The observer stopped before the attempt reached a conclusion.
    ObserverStopped {
        /// The node.
        node: String,
        /// What the observer said.
        message: String,
    },
    /// A pod already occupies this attempt's object name and belongs to a
    /// different program — a different graph, tool version, or image. It is left
    /// running (deleting somebody else's work is not dagr's call) and the node
    /// fails rather than guessing.
    AdoptionRefused {
        /// The node.
        node: String,
        /// The pod, by name — left exactly as it was found.
        pod: String,
        /// Which surface disagreed, and both values.
        refusal: AdoptionRefusal,
    },
}

impl fmt::Display for RemoteLaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArityMismatch {
                node,
                declared,
                assembled,
            } => write!(
                f,
                "refusing to submit `{node}`: it declares {declared} input(s) and \
                 {assembled} reference(s) were assembled for it. An attempt is bound \
                 positionally, so a count that does not match cannot be bound safely."
            ),
            Self::CredentialBearing { node, source } => {
                write!(f, "refusing to submit `{node}`: {source}")
            }
            Self::ClusterRetry(refusal) => write!(f, "{refusal}"),
            Self::Unrecordable { node, message } => write!(
                f,
                "refusing to submit `{node}`: the write-ahead submission record \
                 could not be made durable ({message}), and submitting without it \
                 would leave exactly the unrecoverable state the record exists to \
                 prevent"
            ),
            Self::LaunchExhausted {
                node,
                launches,
                cause,
            } => write!(
                f,
                "`{node}` could not be launched after {launches} submission(s): \
                 {cause}. This is an infrastructure failure — the node's own retry \
                 budget was not consumed."
            ),
            Self::ShardUnreadable {
                node,
                pod,
                status,
                message,
            } => write!(
                f,
                "`{node}` ran in pod `{pod}` ({status}) and left no readable attempt \
                 shard: {message}"
            ),
            Self::ObserverStopped { node, message } => write!(
                f,
                "`{node}` lost its pod observer before the attempt concluded: {message}"
            ),
            Self::AdoptionRefused { node, pod, refusal } => write!(
                f,
                "refusing to run `{node}`: pod `{pod}` already holds this attempt \
                 and {refusal}. It is left running — deleting another program's pod \
                 is not dagr's call — and this node fails rather than guessing."
            ),
        }
    }
}

impl std::error::Error for RemoteLaunchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CredentialBearing { source, .. } => Some(source),
            Self::ClusterRetry(refusal) => Some(refusal),
            Self::AdoptionRefused { refusal, .. } => Some(refusal),
            _ => None,
        }
    }
}

// ===========================================================================
// The runner
// ===========================================================================

/// How one launch of one attempt ended.
enum LaunchEnd {
    /// The container ran and the pod reached a terminal phase.
    Terminal(Box<PodObservation>),
    /// The pod never started, and the runner's own bound expired.
    PreStart(PreStartFailure),
    /// The node's declared per-attempt timeout elapsed while the pod was still
    /// running. The pod is deleted; nothing is left behind.
    TimedOut,
    /// The API refused the create outright.
    Rejected(ApiFailure),
    /// The pod vanished before it reached a phase.
    Vanished,
    /// Cancellation was observed.
    Cancelled,
    /// Something the runner refuses to proceed past.
    Refused(RemoteLaunchError),
}

/// How one *attempt* ended, after its launch retries.
enum AttemptEnd {
    /// The attempt has a terminal state, replayed from its shard.
    Terminal(TerminalState),
    /// The attempt never launched. No user-visible attempt was emitted.
    NeverLaunched(RemoteLaunchError),
    /// Cancellation was observed.
    Cancelled,
}

/// A [`NodeRunner`] that runs each attempt of its node as one Kubernetes pod.
pub struct K8sNodeRunner<L: PodLifecycle> {
    node: String,
    run_id: String,
    lifecycle: L,
    observer: ObserverHandle,
    submissions: SubmissionHandle,
    timer: Arc<dyn AttemptTimer>,
    config: RemoteAttemptConfig,
    adoptions: Adoptions,
    durable_reference: Option<String>,
    durable_reference_meta: Option<WireMeta>,
    diagnostics: Vec<String>,
    last_failure: Option<RemoteLaunchError>,
}

impl<L: PodLifecycle> fmt::Debug for K8sNodeRunner<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("K8sNodeRunner")
            .field("node", &self.node)
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

impl<L: PodLifecycle> K8sNodeRunner<L> {
    /// Wire one placed node to the cluster.
    #[must_use]
    pub fn new(
        node: impl Into<String>,
        run_id: impl Into<String>,
        lifecycle: L,
        observer: ObserverHandle,
        submissions: SubmissionHandle,
        timer: Arc<dyn AttemptTimer>,
        config: RemoteAttemptConfig,
    ) -> Self {
        Self {
            node: node.into(),
            run_id: run_id.into(),
            lifecycle,
            observer,
            submissions,
            timer,
            config,
            adoptions: Adoptions::none(),
            durable_reference: None,
            durable_reference_meta: None,
            diagnostics: Vec::new(),
            last_failure: None,
        }
    }

    /// Let this node reclaim a pod a previous orchestrator left running.
    ///
    /// The registry comes from [`crate::adoption::discover`], one pass at startup
    /// for the whole run. An attempt whose key has a claim waits on the pod that
    /// already exists instead of creating a second one; every attempt without a
    /// claim submits exactly as it always did — which is why there is **one** code
    /// path after adoption rather than two.
    #[must_use]
    pub fn with_adoptions(mut self, adoptions: Adoptions) -> Self {
        self.adoptions = adoptions;
        self
    }

    /// The classified failure this node ended on, if it failed.
    #[must_use]
    pub fn last_failure(&self) -> Option<&RemoteLaunchError> {
        self.last_failure.as_ref()
    }

    /// The platform's own reason strings, carried verbatim (`OOMKilled`, `Evicted`,
    /// …). Diagnostics, never terminal states.
    #[must_use]
    pub fn diagnostics(&self) -> &[String] {
        &self.diagnostics
    }

    /// The checks that must pass before anything is launched **or recorded**.
    ///
    /// # Errors
    ///
    /// [`RemoteLaunchError::ArityMismatch`] or
    /// [`RemoteLaunchError::CredentialBearing`].
    pub fn validate(&self) -> Result<(), RemoteLaunchError> {
        if self.config.inputs.len() != self.config.declared_arity {
            return Err(RemoteLaunchError::ArityMismatch {
                node: self.node.clone(),
                declared: self.config.declared_arity,
                assembled: self.config.inputs.len(),
            });
        }
        for input in &self.config.inputs {
            dagr_k8s::executor::reject_credential_bearing(&input.uri).map_err(|source| {
                RemoteLaunchError::CredentialBearing {
                    node: self.node.clone(),
                    source,
                }
            })?;
        }
        Ok(())
    }

    /// The attempt's identity, as labels and annotations.
    fn identity(&self, attempt: u32) -> AttemptIdentity {
        AttemptIdentity {
            key: AttemptKey::new(self.run_id.clone(), self.node.clone(), attempt),
            pipeline: self.config.pipeline.clone(),
            structural_fingerprint: self.config.structural_fingerprint.clone(),
            policy_hash: self.config.policy_hash.clone(),
            tool_version: self.config.tool_version.clone(),
            image_digest: self.config.image_digest.clone(),
            owner: self.config.owner.clone(),
        }
    }

    /// The submission record's common fields — what the attempt was launched with.
    fn base_record(&self, attempt: u32, spec: &PodSpec) -> AttemptSubmittedRecord {
        AttemptSubmittedRecord::new(self.node.clone(), attempt)
            .inputs(self.config.inputs.clone())
            .executor(EXECUTOR_NAME)
            .target_name(spec.name.clone())
            .structural_fingerprint(self.config.structural_fingerprint.clone())
            .policy_hash(self.config.policy_hash.clone())
            .tool_version(self.config.tool_version.clone())
            .image_digest(self.config.image_digest.clone())
    }

    /// Drive one node to its terminal state.
    async fn drive(
        &mut self,
        ctx: &RunContext,
        sink: &mut (dyn AttemptEventSink + Send),
    ) -> TerminalState {
        if let Err(err) = self.validate() {
            self.last_failure = Some(err);
            sink.emit(AttemptEvent::NodeTerminal {
                node: self.node.clone(),
                state: TerminalState::Failed,
            });
            return TerminalState::Failed;
        }

        let max_attempts = self.config.retry.max_attempts();
        let mut attempt: u32 = 1;
        let state = loop {
            match self.run_attempt(ctx, sink, attempt).await {
                AttemptEnd::Cancelled => break TerminalState::Cancelled,
                AttemptEnd::NeverLaunched(err) => {
                    self.last_failure = Some(err);
                    break TerminalState::Failed;
                }
                AttemptEnd::Terminal(state) => {
                    let retry_eligible =
                        matches!(state, TerminalState::Failed | TerminalState::TimedOut);
                    if !retry_eligible || attempt >= max_attempts {
                        break state;
                    }
                    // T102's real backoff, waited through the injected seam so the
                    // emitted delay is a claim about elapsed time.
                    let delay = self
                        .config
                        .retry
                        .backoff()
                        .delay_for(attempt - 1, &mut NoJitter);
                    sink.emit(AttemptEvent::BackoffStarted {
                        node: self.node.clone(),
                        attempt,
                        delay,
                    });
                    self.timer.sleep(delay).await;
                    attempt += 1;
                }
            }
        };
        sink.emit(AttemptEvent::NodeTerminal {
            node: self.node.clone(),
            state,
        });
        state
    }

    /// One user-visible attempt, including its launch retries.
    async fn run_attempt(
        &mut self,
        ctx: &RunContext,
        sink: &mut (dyn AttemptEventSink + Send),
        attempt: u32,
    ) -> AttemptEnd {
        let mut cause = String::from("no submission was attempted");
        let launches = self.config.launch_retries.saturating_add(1);
        // The attempt's deadline is taken once, here, so the launch retries below
        // spend the same budget the attempt declares rather than each getting a
        // fresh one.
        let deadline = self.config.timeout.map(|budget| Instant::now() + budget);
        for _ in 0..launches {
            match self.launch_once(ctx, attempt, deadline).await {
                LaunchEnd::Cancelled => return AttemptEnd::Cancelled,
                LaunchEnd::Refused(err) => return AttemptEnd::NeverLaunched(err),
                LaunchEnd::Rejected(failure) => {
                    // The API refused: nothing ran, so this is the infrastructure
                    // budget's, not the node's.
                    cause = failure.to_string();
                }
                LaunchEnd::PreStart(failure) => {
                    cause = failure.to_string();
                }
                LaunchEnd::Vanished => {
                    cause = "the pod vanished before it reached a phase".to_string();
                }
                LaunchEnd::TimedOut => {
                    // The container ran (or the platform was still trying to start
                    // it) and the node's own budget is what ended the wait, so this
                    // IS a user-visible attempt: it is `timed-out`, retry-eligible
                    // under the node's retry budget, exactly as a local one is.
                    sink.emit(AttemptEvent::AttemptStarted {
                        node: self.node.clone(),
                        attempt,
                    });
                    sink.emit(AttemptEvent::AttemptTimedOut {
                        node: self.node.clone(),
                        attempt,
                    });
                    return AttemptEnd::Terminal(TerminalState::TimedOut);
                }
                LaunchEnd::Terminal(observation) => {
                    let state = self.conclude(sink, attempt, &observation);
                    // The outcome is now in the run's record, so the pod is spent.
                    // Tombstoning it is what stops the next restart adopting an
                    // attempt whose result this run already has.
                    self.tombstone(&observation.pod_name).await;
                    return AttemptEnd::Terminal(state);
                }
            }
        }
        AttemptEnd::NeverLaunched(RemoteLaunchError::LaunchExhausted {
            node: self.node.clone(),
            launches,
            cause,
        })
    }

    /// One submission: record, create (or adopt), and await.
    async fn launch_once(
        &mut self,
        ctx: &RunContext,
        attempt: u32,
        deadline: Option<Instant>,
    ) -> LaunchEnd {
        let identity = self.identity(attempt);
        let spec = match build_pod(
            &PodRequest {
                identity: identity.clone(),
                namespace: self.config.namespace.clone(),
                image: self.config.image.clone(),
                command: self.config.command.clone(),
                placement: self.config.placement.clone(),
            },
            self.config.cluster_retry,
        ) {
            Ok(spec) => spec,
            Err(refusal) => return LaunchEnd::Refused(RemoteLaunchError::ClusterRetry(refusal)),
        };

        // Ownership, before anything else: a pod this attempt's key was refused
        // belongs to a different program and occupies the object name this build
        // derives. There is nothing to submit through, so the node fails naming
        // both values rather than colliding.
        let reclaimed = match self.adoptions.take(&identity.key) {
            Some(Claim::Refused(refused)) => {
                return LaunchEnd::Refused(RemoteLaunchError::AdoptionRefused {
                    node: self.node.clone(),
                    pod: refused.name,
                    refusal: refused.refusal,
                });
            }
            Some(Claim::Adopted(pod)) => Some(pod),
            None => None,
        };

        // Register the waiter BEFORE anything is created, so a transition cannot
        // land in the gap between creating and watching.
        let waiter = match self.observer.watch_attempt(identity.key.clone()).await {
            Ok(waiter) => waiter,
            Err(stopped) => {
                return LaunchEnd::Refused(RemoteLaunchError::ObserverStopped {
                    node: self.node.clone(),
                    message: stopped.to_string(),
                });
            }
        };

        // Idempotency on the attempt key, in two layers. An orchestrator restart
        // resolved this attempt at startup (T109) and hands the pod over here; a
        // *duplicate submission inside one live process* (T108) is caught by the
        // probe below. Either way the answer is the same: adopt, never duplicate
        // (ADR 115 §5). A read is not a create, so this precedes the write-ahead
        // record without weakening it.
        let existing = match reclaimed {
            Some(pod) => Some(adopted_pod(&pod)),
            None => match self.lifecycle.get(&spec.name).await {
                Ok(Some(pod)) if is_tombstoned(&pod.labels) => {
                    // Its outcome has already been consumed. Adopting it would
                    // re-consume the same attempt, so the name is freed instead —
                    // by the ordinary two-step revocation, because this is an
                    // orchestrator-initiated delete like any other.
                    crate::adoption::revoke(&self.lifecycle, &pod.name).await;
                    None
                }
                Ok(found) => found.as_ref().map(adopted),
                Err(failure) => return LaunchEnd::Rejected(failure),
            },
        };

        // === The write-ahead point =========================================
        // Durable BEFORE the create call. Recording after would lose exactly the
        // crash window this record exists to cover (ADR 115 §9).
        if let Err(err) = self.submissions.record(self.base_record(attempt, &spec)) {
            return LaunchEnd::Refused(RemoteLaunchError::Unrecordable {
                node: self.node.clone(),
                message: err.to_string(),
            });
        }

        let created = match existing {
            Some(pod) => pod,
            None => match self.lifecycle.create(&spec).await {
                Ok(created) => created,
                Err(failure) => return LaunchEnd::Rejected(failure),
            },
        };

        // Reality, recorded additively as its own fact: intent and reality diverge
        // and a post-mortem needs both.
        let mut observed = self
            .base_record(attempt, &spec)
            .observed_name(&created.name);
        if let Some(uid) = &created.uid {
            observed = observed.observed_uid(uid);
        }
        if let Some(host) = &created.host {
            observed = observed.observed_host(host);
        }
        if let Err(err) = self.submissions.record(observed) {
            return LaunchEnd::Refused(RemoteLaunchError::Unrecordable {
                node: self.node.clone(),
                message: err.to_string(),
            });
        }

        // The **observed** name, not the intended one: an adopted pod need not
        // carry the name this build derives, and everything from here on — the
        // wait, the delete, the tombstone — is about the object that actually
        // exists.
        let pod = created.name.clone();
        self.await_pod(ctx, waiter, &pod, deadline).await
    }

    /// Wait for the pod to start and finish — or for the runner's own bound to
    /// decide that it never will.
    async fn await_pod(
        &mut self,
        ctx: &RunContext,
        mut waiter: AttemptWaiter,
        pod: &str,
        deadline: Option<Instant>,
    ) -> LaunchEnd {
        let mut fatal_since: Option<(PreStartFailure, Instant)> = None;
        loop {
            if ctx.cancellation().is_cancelled() {
                self.revoke(pod).await;
                return LaunchEnd::Cancelled;
            }
            // The node's declared budget. A pod that starts and never reports is
            // bounded here — the run does not hang waiting for a report that is not
            // coming — and the pod is deleted rather than abandoned.
            if deadline.is_some_and(|at| Instant::now() >= at) {
                self.revoke(pod).await;
                return LaunchEnd::TimedOut;
            }
            // The bound: once a pre-start surface has reported, the platform is not
            // going to produce a terminal event, so the runner's own clock is what
            // ends the wait.
            if let Some((failure, since)) = &fatal_since
                && since.elapsed() >= self.config.pre_start_bound
            {
                let failure = failure.clone();
                self.revoke(pod).await;
                return LaunchEnd::PreStart(failure);
            }
            match tokio::time::timeout(CANCEL_POLL, waiter.next()).await {
                // Nothing this tick: go round, re-observe cancellation, re-check
                // the bound. The poll is the loop's only clock.
                Err(_elapsed) => {}
                Ok(None) => {
                    return LaunchEnd::Refused(RemoteLaunchError::ObserverStopped {
                        node: self.node.clone(),
                        message: "the attempt's waiter closed".to_string(),
                    });
                }
                Ok(Some(WaiterEvent::ObserverFailed(failure))) => {
                    return LaunchEnd::Refused(RemoteLaunchError::ObserverStopped {
                        node: self.node.clone(),
                        message: failure.to_string(),
                    });
                }
                Ok(Some(WaiterEvent::Observed(observation))) => {
                    // Several pods can carry one attempt key — two orchestrators
                    // that both submitted, or a duplicate discovery revoked at
                    // startup. The waiter is keyed by the attempt, so it hears about
                    // all of them; this attempt's fate is decided by the object it
                    // is actually waiting on and by nothing else.
                    if observation.pod_name != pod {
                        continue;
                    }
                    if observation.vanished {
                        return LaunchEnd::Vanished;
                    }
                    if observation.terminal {
                        return LaunchEnd::Terminal(observation);
                    }
                    match classify_pre_start(&PodStatusFacts::from(observation.as_ref())) {
                        Some(failure) => {
                            // Re-arm only when the reason changed, so a repeated
                            // report does not keep resetting the bound.
                            let rearm = fatal_since
                                .as_ref()
                                .is_none_or(|(previous, _)| previous != &failure);
                            if rearm {
                                fatal_since = Some((failure, Instant::now()));
                            }
                        }
                        // The pod recovered (a registry that was briefly
                        // unreachable): the bound is stood down.
                        None => fatal_since = None,
                    }
                }
            }
        }
    }

    /// **Revoke** a pod: clear the owner label, then delete it.
    ///
    /// Every delete this runner issues is an orchestrator-initiated teardown —
    /// cancellation, the node's own timeout, a pod that will never start — so every
    /// one of them goes through the two-step form, and whatever is watching reads
    /// the disappearance as dagr's rather than as somebody else's
    /// (`dagr_k8s::adoption::deletion_origin`).
    ///
    /// Best effort. A revocation that fails leaves the pod behind and is worth
    /// saying so, but it is never a reason to fail a node twice.
    async fn revoke(&self, pod: &str) {
        crate::adoption::revoke(&self.lifecycle, pod).await;
    }

    /// **Tombstone** a pod whose outcome this runner has consumed.
    ///
    /// The key is the one a later discovery's selector excludes, so the attempt
    /// cannot be adopted a second time — independently of whether the pod is ever
    /// deleted, which is the protection that can be lost.
    async fn tombstone(&self, pod: &str) {
        crate::adoption::tombstone(&self.lifecycle, pod).await;
    }

    /// Read the shard, replay it, and decide the attempt's terminal state.
    fn conclude(
        &mut self,
        sink: &mut (dyn AttemptEventSink + Send),
        attempt: u32,
        observation: &PodObservation,
    ) -> TerminalState {
        let verdict = classify_terminal(&PodStatusFacts::from(observation));
        self.diagnostics.clone_from(&verdict.diagnostics);

        let shard = match AttemptShard::read(
            &self.config.blob_container,
            &self.run_id,
            &self.node,
            attempt,
        )
        .and_then(|shard| {
            shard
                .verify_build(
                    &self.config.structural_fingerprint,
                    &self.config.tool_version,
                )
                .map(|()| shard)
        }) {
            Ok(shard) => shard,
            Err(err) => {
                return self.without_shard(
                    sink,
                    attempt,
                    observation,
                    &verdict_status(observation),
                    &err,
                );
            }
        };

        for diagnostic in shard.diagnostics() {
            self.diagnostics.push(diagnostic.clone());
        }

        // Decide the state BEFORE replaying, so the records emitted and the terminal
        // reported can never disagree. The pod's own status is evidence, and it
        // outranks a shard claiming a success the platform did not see — a pod
        // killed after its task returned but before it exited cleanly is a failed
        // attempt, however cheerful its trailer.
        let claimed = terminal_from_token(shard.terminal_state());
        let disbelieved =
            verdict.outcome == PodOutcome::Failed && claimed == TerminalState::Succeeded;

        if disbelieved {
            self.diagnostics.push(format!(
                "the attempt shard reports `{}` but pod `{}` did not succeed",
                shard.terminal_state(),
                observation.pod_name
            ));
            sink.emit(AttemptEvent::AttemptStarted {
                node: self.node.clone(),
                attempt,
            });
            sink.emit(AttemptEvent::AttemptFailed {
                node: self.node.clone(),
                attempt,
            });
            return TerminalState::Failed;
        }

        // Replay the shard's own records through the INJECTED sink, so the
        // orchestrator stays the single writer and `seq` stays gapless. The shard is
        // parsed and re-emitted, never byte-concatenated: concatenating a shard whose
        // trailing record was truncated would turn a tolerated trailing partial into
        // a non-final corruption and lose the whole run's artifact (T101).
        for record in shard.records() {
            if let Some(event) = attempt_event_from(record) {
                sink.emit(event);
            }
        }

        if claimed == TerminalState::Succeeded
            && let Some(output) = shard.output()
        {
            self.durable_reference = Some(output.uri().to_string());
            let mut meta = WireMeta::new();
            meta.content_hash = output.recorded_content_hash().map(str::to_string);
            meta.size_bytes = output.recorded_size_bytes();
            meta.scheme = output.recorded_scheme().map(str::to_string);
            if !meta.is_empty() {
                self.durable_reference_meta = Some(meta);
            }
        }
        claimed
    }

    /// A terminal pod with no readable — or no believable — shard.
    ///
    /// T101's rule: synthesise the outcome from the pod's status alone, name the pod
    /// and its status, and fail. Never a silent success (the shard is the only record
    /// of what the task produced) and never a hang.
    fn without_shard(
        &mut self,
        sink: &mut (dyn AttemptEventSink + Send),
        attempt: u32,
        observation: &PodObservation,
        status: &str,
        err: &ShardError,
    ) -> TerminalState {
        self.last_failure = Some(RemoteLaunchError::ShardUnreadable {
            node: self.node.clone(),
            pod: observation.pod_name.clone(),
            status: status.to_string(),
            message: err.to_string(),
        });
        // The attempt did run, so it is a user-visible attempt: the container
        // started, and the node's own retry budget is the right one to charge.
        sink.emit(AttemptEvent::AttemptStarted {
            node: self.node.clone(),
            attempt,
        });
        sink.emit(AttemptEvent::AttemptFailed {
            node: self.node.clone(),
            attempt,
        });
        TerminalState::Failed
    }
}

/// The pod's status, rendered for a diagnostic.
fn verdict_status(observation: &PodObservation) -> String {
    let mut parts = vec![format!("phase {}", observation.phase)];
    if let Some(reason) = &observation.pod_reason {
        parts.push(format!("pod reason {reason}"));
    }
    if let Some(reason) = &observation.container_reason {
        parts.push(format!("container reason {reason}"));
    }
    if let Some(code) = observation.exit_code {
        parts.push(format!("exit code {code}"));
    }
    parts.join(", ")
}

/// Treat an already-live pod as the created one.
fn adopted(pod: &PodSnapshot) -> CreatedPod {
    CreatedPod {
        name: pod.name.clone(),
        uid: pod.uid.clone(),
        host: pod.host.clone(),
    }
}

/// The same, for a pod the startup discovery pass reclaimed.
fn adopted_pod(pod: &DiscoveredPod) -> CreatedPod {
    CreatedPod {
        name: pod.name.clone(),
        uid: pod.uid.clone(),
        host: pod.host.clone(),
    }
}

/// The normative terminal state a shard's trailer names.
fn terminal_from_token(token: &str) -> TerminalState {
    match token {
        "succeeded" => TerminalState::Succeeded,
        "timed-out" => TerminalState::TimedOut,
        "skipped" => TerminalState::Skipped,
        "cancelled" => TerminalState::Cancelled,
        "abandoned" => TerminalState::Abandoned,
        "upstream-skipped" => TerminalState::UpstreamSkipped,
        "upstream-failed" => TerminalState::UpstreamFailed,
        "satisfied-from-prior" => TerminalState::SatisfiedFromPrior,
        _ => TerminalState::Failed,
    }
}

/// Rebuild the abstract attempt record one shard line represents.
///
/// The shard's records were produced by the *same* translation the driver uses
/// (`crate::shard::records_for` drives `driver::write_attempt_event`), so this is its
/// inverse over the attempt-scoped kinds. `node-terminal` is deliberately **not**
/// rebuilt: a node with retries replays several shards, and the node's single
/// terminal is decided once, by the runner, after the last of them.
fn attempt_event_from(record: &serde_json::Value) -> Option<AttemptEvent> {
    let kind = record.get("kind")?.as_str()?;
    let node = record.get("node")?.as_str()?.to_string();
    let attempt = || -> Option<u32> { u32::try_from(record.get("attempt")?.as_u64()?).ok() };
    match kind {
        "node-admitted" => Some(AttemptEvent::NodeAdmitted { node }),
        "attempt-started" => Some(AttemptEvent::AttemptStarted {
            node,
            attempt: attempt()?,
        }),
        "attempt-succeeded" => Some(AttemptEvent::AttemptSucceeded {
            node,
            attempt: attempt()?,
        }),
        "attempt-failed" => Some(AttemptEvent::AttemptFailed {
            node,
            attempt: attempt()?,
        }),
        _ => None,
    }
}

impl<L: PodLifecycle> NodeRunner for K8sNodeRunner<L> {
    fn name(&self) -> &str {
        &self.node
    }

    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        Box::pin(self.drive(ctx, sink))
    }

    fn durable_reference(&self) -> Option<String> {
        self.durable_reference.clone()
    }

    fn durable_reference_meta(&self) -> Option<WireMeta> {
        self.durable_reference_meta.clone()
    }
}
