//! The **port** the observer watches through: list, watch, and the shapes those
//! two hand back.
//!
//! Everything above this line is dagr's discipline; everything below it is
//! somebody's API. Keeping the seam here is what lets the reconnect discipline be
//! tested against a fake that can produce a `resourceVersion` expiry, a silent
//! stall and a duplicate delivery on demand — none of which a real cluster
//! produces reliably, and two of which a fresh cluster cannot produce at all.
//!
//! The port is deliberately **`list` + `watch`**, the two primitive calls, rather
//! than "hand me a self-healing stream". That is the level the spike's error
//! taxonomy is written in terms of — an expiry arrives as a *decoded event* with
//! `type: ERROR`, not as a transport failure — and it is the level at which
//! re-list-then-watch is a decision dagr makes rather than one it inherits.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::identity::AttemptIdentity;

/// A pod's lifecycle phase, as the platform reports it.
///
/// The set is the platform's and is closed here on purpose: dagr's own terminal
/// taxonomy has nine members and gains none from this, because a pod phase is
/// evidence about an attempt, not a verdict on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PodPhase {
    /// Accepted by the cluster; not yet running. A pod that never starts stays
    /// here — which is why a caller must not wait on a terminal phase alone.
    Pending,
    /// At least one container is running.
    Running,
    /// Every container terminated successfully.
    Succeeded,
    /// A container terminated in failure, or the pod was evicted.
    Failed,
    /// The platform could not determine the phase.
    Unknown,
}

impl PodPhase {
    /// Whether this phase is one a pod does not leave.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, PodPhase::Succeeded | PodPhase::Failed)
    }

    /// The platform's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            PodPhase::Pending => "Pending",
            PodPhase::Running => "Running",
            PodPhase::Succeeded => "Succeeded",
            PodPhase::Failed => "Failed",
            PodPhase::Unknown => "Unknown",
        }
    }

    /// Read the platform's spelling. Anything unrecognized — including an absent
    /// phase — reads as [`Unknown`](Self::Unknown) rather than as a guess.
    #[must_use]
    pub fn from_api(raw: &str) -> Self {
        match raw {
            "Pending" => PodPhase::Pending,
            "Running" => PodPhase::Running,
            "Succeeded" => PodPhase::Succeeded,
            "Failed" => PodPhase::Failed,
            _ => PodPhase::Unknown,
        }
    }
}

impl fmt::Display for PodPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One pod, as of one observation.
///
/// Both `reason` fields are carried, and that is not redundancy: an out-of-memory
/// kill appears on the **container** status with an empty pod reason, while an
/// eviction appears on the **pod** with the container reporting a generic error.
/// A reader that consults only one of the two loses half the diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodSnapshot {
    /// The pod's name.
    pub name: String,
    /// The object's `resourceVersion` — the resume point for a watch that ends.
    pub resource_version: String,
    /// The phase as of this observation.
    pub phase: PodPhase,
    /// The pod's labels: the selector half of identity.
    pub labels: BTreeMap<String, String>,
    /// The pod's annotations: the authoritative half.
    pub annotations: BTreeMap<String, String>,
    /// The **pod**-level reason, e.g. an eviction.
    pub pod_reason: Option<String>,
    /// The **container**-level reason, e.g. an out-of-memory kill.
    pub container_reason: Option<String>,
    /// The container's exit code, when it ran at all. Note that one code is
    /// produced by both an out-of-memory kill and an external termination, so it
    /// never disambiguates them on its own — the reasons above do.
    pub exit_code: Option<i32>,
}

impl PodSnapshot {
    /// A snapshot carrying `identity`'s labels and annotations and no diagnostics
    /// — the shape a pod has before anything has gone wrong with it.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        resource_version: impl Into<String>,
        phase: PodPhase,
        identity: &AttemptIdentity,
    ) -> Self {
        Self {
            name: name.into(),
            resource_version: resource_version.into(),
            phase,
            labels: identity.labels(),
            annotations: identity.annotations(),
            pod_reason: None,
            container_reason: None,
            exit_code: None,
        }
    }
}

/// The result of a `list`: current state, plus the version to watch from.
///
/// A stale `resourceVersion` expires a *watch* but is accepted by a *list*, which
/// returns current state rather than a delta — which is exactly why
/// re-list-then-watch cannot lose a transition, and why it is the recovery for an
/// expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodListing {
    /// The collection's `resourceVersion`: where the watch that follows starts.
    pub resource_version: String,
    /// Every pod matching the selector, right now.
    pub pods: Vec<PodSnapshot>,
}

/// One item off a watch.
///
/// Five of the six are ordinary; [`ApiError`](Self::ApiError) is the one that
/// matters most, because an expiry arrives as a **decoded event** rather than as
/// a transport failure. A client that only handles transport errors never notices
/// one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchDelivery {
    /// A pod entered the selector's scope.
    Added(PodSnapshot),
    /// A pod already in scope changed.
    Modified(PodSnapshot),
    /// A pod left the selector's scope.
    Deleted(PodSnapshot),
    /// A periodic marker carrying a resume point and nothing else. Useful as a
    /// **cheap reconnect point** and useless as a liveness signal: cadence tracks
    /// cluster activity, so an idle namespace emits one roughly per minute and a
    /// busy one many times faster.
    Bookmark {
        /// The version to resume from.
        resource_version: String,
    },
    /// An in-stream error frame. `410`/`Expired` is the expiry.
    ApiError {
        /// The HTTP status the frame carries.
        code: u16,
        /// The platform's machine-readable reason.
        reason: String,
        /// The platform's human-readable message. For an expiry this contains a
        /// resource version, and that number is the watch cache's **oldest
        /// retained** bound rather than the head — resuming from it would skip or
        /// replay transitions.
        message: String,
    },
    /// The stream failed below the protocol.
    Transport {
        /// What the client said.
        message: String,
    },
}

/// A call to the API did not produce a stream or a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiFailure {
    /// The HTTP status, when there was one. `None` means it never got that far.
    pub code: Option<u16>,
    /// The platform's machine-readable reason, or a client-side category.
    pub reason: String,
    /// What the client said, verbatim. Kept because a classified failure that
    /// paraphrases the platform is a diagnostic nobody can act on.
    pub message: String,
}

impl ApiFailure {
    /// A failure below the protocol: no status, no reason from the server.
    #[must_use]
    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            code: None,
            reason: "Transport".to_string(),
            message: message.into(),
        }
    }

    /// A failure the server described.
    #[must_use]
    pub fn api(code: u16, reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: Some(code),
            reason: reason.into(),
            message: message.into(),
        }
    }

    /// Whether this is an expiry — the one failure whose recovery is a full
    /// re-list rather than a resume.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.code == Some(410) || self.reason == "Expired"
    }
}

impl fmt::Display for ApiFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code {
            Some(code) => write!(f, "{} ({code}): {}", self.reason, self.message),
            None => write!(f, "{}: {}", self.reason, self.message),
        }
    }
}

impl std::error::Error for ApiFailure {}

/// An open watch, and the guarantee that closing it closes everything behind it.
///
/// A watch may be backed by a task pumping somebody's stream into this channel.
/// Dropping the handle aborts that task, so "the observer's watch is torn down
/// and its task does not outlive the run" is a property of the type rather than
/// of remembering to call something.
#[derive(Debug)]
pub struct PodWatch {
    receiver: mpsc::Receiver<WatchDelivery>,
    pump: Option<JoinHandle<()>>,
}

impl PodWatch {
    /// A watch fed directly, with no task behind it.
    #[must_use]
    pub fn from_channel(receiver: mpsc::Receiver<WatchDelivery>) -> Self {
        Self {
            receiver,
            pump: None,
        }
    }

    /// A watch fed by `pump`, which is aborted when this handle is dropped.
    #[must_use]
    pub fn pumped(receiver: mpsc::Receiver<WatchDelivery>, pump: JoinHandle<()>) -> Self {
        Self {
            receiver,
            pump: Some(pump),
        }
    }

    /// The next delivery, or `None` when the stream has ended.
    pub async fn next(&mut self) -> Option<WatchDelivery> {
        self.receiver.recv().await
    }
}

impl Drop for PodWatch {
    fn drop(&mut self) {
        if let Some(pump) = self.pump.take() {
            pump.abort();
        }
    }
}

/// How many deliveries a watch buffers before the producer waits.
///
/// The observer drains promptly and a delivery is small, so this exists to keep a
/// burst from blocking rather than to smooth a sustained rate. Bounded rather
/// than unbounded on purpose: an unbounded channel turns a wedged consumer into a
/// memory leak, which is a worse failure than back-pressure on a watch.
pub const WATCH_BUFFER: usize = 64;

/// Read access to one namespace's pods: the two calls the observer makes, and
/// nothing else.
///
/// Both are **outbound**. Nothing in this trait accepts a connection, serves a
/// request, or is handed anything by another process — the observer talks to an
/// API it does not own, which is why remote execution adds no coordination
/// surface.
pub trait PodApi: Send + Sync + 'static {
    /// Every pod matching `selector`, right now, with the version to watch from.
    ///
    /// # Errors
    ///
    /// Returns [`ApiFailure`] when the call did not produce a listing.
    fn list(&self, selector: &str) -> impl Future<Output = Result<PodListing, ApiFailure>> + Send;

    /// A watch over `selector`, starting after `resource_version`.
    ///
    /// # Errors
    ///
    /// Returns [`ApiFailure`] when the watch could not be opened. Note that an
    /// **expiry** usually arrives inside the stream instead, as
    /// [`WatchDelivery::ApiError`] — both paths exist and both are classified.
    fn watch(
        &self,
        selector: &str,
        resource_version: &str,
    ) -> impl Future<Output = Result<PodWatch, ApiFailure>> + Send;
}
