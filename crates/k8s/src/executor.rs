//! The **executor's pure half**: what pod to ask for, when a pod has already
//! failed before it started, and what a terminal pod's status means.
//!
//! Every decision here is a function of values, with no I/O, no clock and no
//! runtime — which is why it lives in this crate and is testable without a cluster.
//! The half that submits, waits, reads a shard and replays it is
//! `dagr_cli::k8s_runner`, where the orchestrator process and its runtime are
//! (ADR 004).
//!
//! # Three rules this module exists to make unbreakable
//!
//! **Kubernetes must never retry.** ADR 115 §2 makes that an *invariant, not a
//! default*: a cluster-level restart running alongside a dagr-level retry duplicates
//! execution of the same attempt. [`build_pod`] therefore pins `restartPolicy:
//! Never` and **refuses** a configuration that asks for cluster-side retry, naming
//! the hazard.
//!
//! **A pre-start failure never reaches a terminal phase.** T101 measured this on two
//! substrates: an unpullable image sits in `Pending` with a waiting reason and *no
//! terminated state at all*, because the platform retries the pull indefinitely
//! rather than failing the pod; an unschedulable pod reports through
//! `conditions[PodScheduled]` and has **no container status entry whatsoever**. So
//! [`classify_pre_start`] reads *both* surfaces and the caller applies its own
//! bound — there is no platform-side terminal event coming.
//!
//! **Terminal state is read off pod status, and both `reason` fields are read.** An
//! out-of-memory kill appears on the *container* with an empty pod reason; an
//! eviction appears on the *pod* with the container reporting a generic `Error`.
//! Exit code `137` is produced by both an OOM kill and an external termination, so
//! it disambiguates nothing on its own. The reasons travel as **diagnostic strings**
//! — the nine-state terminal taxonomy (`arch.md` "Vocabulary") is closed and gains
//! no member from anything the platform says.

use std::collections::BTreeMap;
use std::fmt;

use crate::api::{PodPhase, PodSnapshot};
use crate::identity::{AttemptIdentity, AttemptKey};
use crate::observer::PodObservation;

/// The container waiting reasons that mean the container will **never** start
/// without intervention.
///
/// Measured, not guessed: T101 ran each against a real cluster and confirmed the
/// pod stays `Pending` indefinitely. Anything not on this list — `ContainerCreating`,
/// `PodInitializing` — is the platform *working*, and treating it as fatal would
/// abort every pod that takes a moment to start.
pub const FATAL_WAITING_REASONS: &[&str] = &[
    "ImagePullBackOff",
    "ErrImagePull",
    "CreateContainerConfigError",
    "InvalidImageName",
];

/// The scheduler's own refusal reason, reported on the `PodScheduled` condition.
pub const UNSCHEDULABLE_REASON: &str = "Unschedulable";

/// The maximum length of a Kubernetes object name.
const NAME_MAX: usize = 63;

// ===========================================================================
// Placement — opaque strings, never interpreted
// ===========================================================================

/// The author-declared placement, carried through as **opaque strings**.
///
/// `dagr-core` never learns what Kubernetes is (ADR 115 §7): a `Placement` is a set
/// of strings the engine records, hashes into the policy digest, and hands on. This
/// is the executor's owned mirror of it, so the pure half can be exercised without
/// a `dagr-core` edge — which this crate deliberately does not have.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodPlacement {
    /// The CPU request, verbatim.
    pub cpu: Option<String>,
    /// The memory request, verbatim.
    pub memory: Option<String>,
    /// Node selector pairs, verbatim and in declared order.
    pub node_selectors: Vec<(String, String)>,
    /// Tolerations, verbatim and in declared order.
    pub tolerations: Vec<String>,
}

// ===========================================================================
// The request and the spec
// ===========================================================================

/// Everything the executor needs in order to ask for one pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRequest {
    /// The attempt's identity: the key, and the provenance that travels as
    /// annotations.
    pub identity: AttemptIdentity,
    /// The namespace to create in.
    pub namespace: String,
    /// The image reference, carrying the digest the orchestrator itself is running.
    pub image: String,
    /// The container's argv — the pod-side `exec-node` invocation.
    pub command: Vec<String>,
    /// The author-declared placement.
    pub placement: PodPlacement,
}

/// Whether the platform is allowed to restart the work on its own.
///
/// An enum rather than a `bool` because the value is a *decision with a rationale*,
/// and because a caller reading `ClusterRetry::Enabled` at a call site is told what
/// it is asking for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClusterRetry {
    /// The invariant: dagr owns retry, the platform runs the work exactly once.
    #[default]
    Disabled,
    /// Asking the platform to retry as well — which [`build_pod`] refuses.
    Enabled,
}

/// The executor refused to build a pod because the configuration asked the platform
/// to retry as well as dagr.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRetryRefused {
    /// The node whose submission was refused.
    pub node: String,
}

impl fmt::Display for ClusterRetryRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "refusing to submit `{}` with cluster-side retry enabled: dagr owns \
             retry through NodePolicy, and a platform-level retry running alongside \
             a dagr-level retry duplicates execution of the same attempt. Submit \
             bare pods, or pin the job's backoff limit to zero.",
            self.node
        )
    }
}

impl std::error::Error for ClusterRetryRefused {}

/// One pod, as the executor asks for it.
///
/// Deliberately **not** a Kubernetes API type: this crate's client is quarantined
/// behind a default-off feature, and a spec that named `k8s_openapi::api::core::v1`
/// would drag the whole HTTP/TLS tree into every build that wanted to reason about
/// placement. The adapter translates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodSpec {
    /// The object name — a pure function of the attempt key, so a resubmission
    /// addresses the same object.
    pub name: String,
    /// The namespace to create in.
    pub namespace: String,
    /// Selector labels (lossy by design; each value is ≤63 characters).
    pub labels: BTreeMap<String, String>,
    /// Authoritative annotations (unbounded).
    pub annotations: BTreeMap<String, String>,
    /// The image reference, carrying its digest.
    pub image: String,
    /// The container's argv.
    pub command: Vec<String>,
    /// Always `"Never"`. Pinned as a field rather than implied so a reader — and a
    /// test — can see the invariant rather than trust it.
    pub restart_policy: &'static str,
    /// The CPU request, verbatim.
    pub cpu: Option<String>,
    /// The memory request, verbatim.
    pub memory: Option<String>,
    /// Node selector pairs, verbatim.
    pub node_selectors: Vec<(String, String)>,
    /// Tolerations, verbatim.
    pub tolerations: Vec<String>,
    /// The identity this pod carries, retained so a caller that observes the pod
    /// again can compare what it asked for against what it got.
    pub identity: AttemptIdentity,
}

/// The object name for one attempt, a **pure function of the attempt key**.
///
/// Determinism is what makes submission idempotent: a resubmission for
/// `(run_id, node, attempt)` computes the same name, finds the live pod, and adopts
/// it instead of creating a second one. Names are *derived* rather than copied
/// because a node name is author-chosen and need not be a legal object name — the
/// full name lives in an annotation, exactly as ADR 115 §4 puts identity there.
#[must_use]
pub fn pod_name(key: &AttemptKey) -> String {
    format!(
        "dagr-{}-{}-{}",
        &short_digest(&key.run_id)[..8],
        &short_digest(&key.node)[..12],
        key.attempt
    )
}

/// A 16-hex-character FNV-1a digest — the same hash family the fingerprints use,
/// chosen here for the same reason: no dependency, and identity data does not need
/// a cryptographic hash to be a stable, collision-improbable *name*.
fn short_digest(value: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Build the pod for `request`, refusing a configuration in which the platform
/// would retry as well.
///
/// # Errors
///
/// [`ClusterRetryRefused`] when `cluster_retry` is [`ClusterRetry::Enabled`], with a
/// message naming the duplicate-execution hazard.
pub fn build_pod(
    request: &PodRequest,
    cluster_retry: ClusterRetry,
) -> Result<PodSpec, ClusterRetryRefused> {
    if cluster_retry == ClusterRetry::Enabled {
        return Err(ClusterRetryRefused {
            node: request.identity.key.node.clone(),
        });
    }
    let mut name = pod_name(&request.identity.key);
    name.truncate(NAME_MAX);
    Ok(PodSpec {
        name,
        namespace: request.namespace.clone(),
        labels: request.identity.labels(),
        annotations: request.identity.annotations(),
        image: request.image.clone(),
        command: request.command.clone(),
        restart_policy: "Never",
        cpu: request.placement.cpu.clone(),
        memory: request.placement.memory.clone(),
        node_selectors: request.placement.node_selectors.clone(),
        tolerations: request.placement.tolerations.clone(),
        identity: request.identity.clone(),
    })
}

// ===========================================================================
// Status facts, and the two classifications over them
// ===========================================================================

/// The pod-status facts every classification here reads, borrowed from whatever
/// observed them.
///
/// A borrowed view rather than a second owned struct, so a `PodSnapshot` (from a
/// list or a get) and a `PodObservation` (from the watch) feed exactly the same
/// decision and cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodStatusFacts<'a> {
    /// The lifecycle phase.
    pub phase: PodPhase,
    /// The container's waiting reason, when it is waiting.
    pub waiting_reason: Option<&'a str>,
    /// The scheduler's refusal reason, when it refused.
    pub scheduling_refusal: Option<&'a str>,
    /// The **pod**-level reason, e.g. an eviction.
    pub pod_reason: Option<&'a str>,
    /// The **container**-level reason, e.g. an out-of-memory kill.
    pub container_reason: Option<&'a str>,
    /// The container's exit code, when it ran at all.
    pub exit_code: Option<i32>,
}

impl<'a> From<&'a PodSnapshot> for PodStatusFacts<'a> {
    fn from(pod: &'a PodSnapshot) -> Self {
        Self {
            phase: pod.phase,
            waiting_reason: pod.waiting_reason.as_deref(),
            scheduling_refusal: pod.scheduling_refusal.as_deref(),
            pod_reason: pod.pod_reason.as_deref(),
            container_reason: pod.container_reason.as_deref(),
            exit_code: pod.exit_code,
        }
    }
}

impl<'a> From<&'a PodObservation> for PodStatusFacts<'a> {
    fn from(observation: &'a PodObservation) -> Self {
        Self {
            phase: observation.phase,
            waiting_reason: observation.waiting_reason.as_deref(),
            scheduling_refusal: observation.scheduling_refusal.as_deref(),
            pod_reason: observation.pod_reason.as_deref(),
            container_reason: observation.container_reason.as_deref(),
            exit_code: observation.exit_code,
        }
    }
}

/// Which of the pre-start surfaces reported the failure.
///
/// Carried because the two mean different things operationally — one is an image or
/// container-configuration problem, the other is capacity — and an operator reading
/// a diagnostic acts on them differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreStartSurface {
    /// A container waiting reason in [`FATAL_WAITING_REASONS`].
    WaitingReason,
    /// The scheduler declined to place the pod.
    Unschedulable,
}

impl fmt::Display for PreStartSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PreStartSurface::WaitingReason => "container waiting reason",
            PreStartSurface::Unschedulable => "scheduling refusal",
        })
    }
}

/// A pod that will never start on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreStartFailure {
    /// Where the evidence came from.
    pub surface: PreStartSurface,
    /// The platform's machine-readable reason, verbatim.
    pub reason: String,
}

impl fmt::Display for PreStartFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} `{}`", self.surface, self.reason)
    }
}

/// Whether `facts` describe a pod that **never started**, and never will.
///
/// Returns [`None`] for a pod that is merely still pending (the normal case) and for
/// any pod past `Pending` — once a container has run, whatever goes wrong afterwards
/// belongs to the node's own retry budget, and the two budgets are only separable
/// because "the container ran" is decidable.
///
/// The caller applies its **own bound** to a `Some`: the platform retries a pull
/// indefinitely rather than failing the pod, so a transient `ErrImagePull` that
/// resolves must not be charged as a launch failure, and a persistent one must not
/// wait for a terminal event that is never coming.
#[must_use]
pub fn classify_pre_start(facts: &PodStatusFacts<'_>) -> Option<PreStartFailure> {
    if facts.phase != PodPhase::Pending {
        return None;
    }
    if let Some(reason) = facts.waiting_reason
        && FATAL_WAITING_REASONS.contains(&reason)
    {
        return Some(PreStartFailure {
            surface: PreStartSurface::WaitingReason,
            reason: reason.to_string(),
        });
    }
    if let Some(reason) = facts.scheduling_refusal {
        return Some(PreStartFailure {
            surface: PreStartSurface::Unschedulable,
            reason: reason.to_string(),
        });
    }
    None
}

/// What a terminal pod's status says about the attempt that ran in it.
///
/// Two members, because that is all the platform can tell you: the container
/// finished cleanly, or it did not. Everything else the platform reports is a
/// **diagnostic string** on the attempt record — the terminal taxonomy is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodOutcome {
    /// Every container terminated successfully.
    Succeeded,
    /// A container terminated in failure, or the pod was evicted or lost.
    Failed,
}

/// A terminal pod's classification: the outcome, and the platform's own words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodVerdict {
    /// Succeeded or failed. Never a new terminal state.
    pub outcome: PodOutcome,
    /// The platform's reason strings, carried verbatim for the attempt record.
    pub diagnostics: Vec<String>,
}

/// Classify a terminal pod from its status.
///
/// **Both** reason fields are read, and neither is inferred from the exit code:
/// `137` is produced by an out-of-memory kill *and* by an external termination, and
/// only the reason separates them (T101).
#[must_use]
pub fn classify_terminal(facts: &PodStatusFacts<'_>) -> PodVerdict {
    let mut diagnostics = Vec::new();
    if let Some(reason) = facts.pod_reason {
        diagnostics.push(format!("pod reason: {reason}"));
    }
    if let Some(reason) = facts.container_reason {
        diagnostics.push(format!("container reason: {reason}"));
    }
    let outcome = match facts.phase {
        PodPhase::Succeeded => PodOutcome::Succeeded,
        _ => PodOutcome::Failed,
    };
    if outcome == PodOutcome::Failed
        && let Some(code) = facts.exit_code
    {
        diagnostics.push(format!("exit code: {code}"));
    }
    PodVerdict {
        outcome,
        diagnostics,
    }
}

// ===========================================================================
// Reference hygiene
// ===========================================================================

/// A reference carried a credential and was refused before it could be recorded.
///
/// The refusal deliberately reports **only the marker it matched** — never the URL
/// and never the secret. A diagnostic that quotes the credential it refused has
/// leaked it into exactly the record the refusal exists to keep clean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialInReference {
    /// The credential-shaped marker that matched.
    pub marker: &'static str,
}

impl fmt::Display for CredentialInReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "refusing to record a reference carrying `{}`: a credential-bearing URL \
             must never reach an event record, a label, or an annotation \
             (ADR 115 §9) — credentials come from the backend's ambient \
             environment, not from the reference",
            self.marker
        )
    }
}

impl std::error::Error for CredentialInReference {}

/// The markers that make a reference credential-bearing. Matched case-insensitively
/// against the query string, plus the URL's own userinfo separator.
const CREDENTIAL_MARKERS: &[&str] = &[
    "x-amz-signature",
    "x-amz-credential",
    "x-amz-security-token",
    "signature=",
    "sig=",
    "token=",
    "access_key_id",
    "accesskeyid",
    "password=",
    "secret=",
    "sas=",
];

/// Refuse a reference that carries a credential.
///
/// # Errors
///
/// [`CredentialInReference`] naming the marker — and only the marker.
pub fn reject_credential_bearing(uri: &str) -> Result<(), CredentialInReference> {
    // Userinfo: `scheme://user:secret@host/…`. Checked on the authority only, so a
    // path or query containing `@` is not mistaken for one.
    if let Some(rest) = uri.split_once("://").map(|(_, rest)| rest) {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
        if authority.contains('@') {
            return Err(CredentialInReference {
                marker: "userinfo credentials",
            });
        }
    }
    if let Some(query) = uri.split_once('?').map(|(_, q)| q) {
        let lowered = query.to_ascii_lowercase();
        for marker in CREDENTIAL_MARKERS {
            if lowered.contains(marker) {
                return Err(CredentialInReference { marker });
            }
        }
    }
    Ok(())
}
