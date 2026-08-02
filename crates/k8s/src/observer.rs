//! **The shared pod observer** — one watch per orchestrator process, and the
//! discipline that keeps it alive.
//!
//! Two halves, deliberately separated, and separated across a *crate* boundary:
//!
//! - [`ObserverCore`], **here**, is a deterministic state machine. It takes an
//!   [`ObserverInput`] and a monotonic offset and returns the deliveries to route
//!   and the [`ObserverAction`] to take next. It performs no I/O, reads no clock,
//!   and spawns nothing — so every reconnect scenario is a sequence of function
//!   calls, and the reconnect discipline is testable without a cluster, a
//!   network, or a sleep.
//! - The **task** that wires that machine to a [`PodApi`](crate::api::PodApi) and
//!   a timer, owns the one watch, and demultiplexes to per-attempt waiters is
//!   `dagr_cli::pod_observer`. It lives there because that is where the
//!   orchestrator process — and, by ADR 004's placement, the async runtime — is.
//!   "One watch per orchestrator *process*" is a property of the process, and
//!   this crate deliberately owns no runtime: it describes the port, the
//!   identity encoding, the cluster-access decision and the discipline, and every
//!   one of those is a value or a function rather than a task.
//!
//! # Why one watch
//!
//! A watch per pod multiplies API-server connections by graph width and gives
//! every node its own reconnect bug. Both reviewed implementations shipped
//! per-job watching and then removed it; one of them still carries the
//! configuration fields for the mechanism it deleted, which is its own cautionary
//! tale.
//!
//! # The reconnect discipline
//!
//! A Kubernetes watch is not a reliable stream, and the four ways it ends need
//! four different answers:
//!
//! | Class | How it appears | Answer |
//! |---|---|---|
//! | **Expired** | a *decoded* event with `code: 410`, `reason: Expired` — **not** a transport error | re-**LIST**, reconcile, watch from the listing's version |
//! | **Transport / stream end** | one error item then the end of the stream | resume from the last version actually seen |
//! | **Transient API (`403`, `5xx`)** | a failure while the server is coming back | retry with backoff; never fatal without a repeat count and an elapsed bound |
//! | **Silence** | no event, no error | indistinguishable from idle — only a **client-side bound** finds it |
//!
//! Four things follow, and each is a defect somewhere if it is left out.
//!
//! 1. **Decoded events are inspected for errors.** A handler that only looks at
//!    transport failures misses every expiry, because an expiry is a frame.
//! 2. **Recovery from an expiry is a re-LIST, never the version in the message.**
//!    That number is the watch cache's *oldest retained* bound, not the head;
//!    resuming from it would skip or replay transitions. A stale version on a
//!    *list* is accepted and returns current state, which is why the re-list
//!    cannot lose anything.
//! 3. **Reconnection backs off, and is bounded.** An unwrapped watch loop will
//!    issue hundreds of reconnects per second at an API server that is trying to
//!    come back — the client hammering the thing it is waiting for. Here the
//!    delay grows and the retries are capped, and exhausting the cap produces a
//!    **classified** failure rather than an infinite quiet retry.
//! 4. **Silence is bounded and reconciled.** Bookmarks arrive roughly once a
//!    minute on an idle namespace and many times faster on a busy one, so their
//!    cadence tracks cluster activity and cannot serve as a heartbeat. The
//!    reliable liveness signal is a client-side bound that forces a periodic
//!    LIST reconciliation regardless of the watch — which also repairs any
//!    transition lost to a bug. Because a remote attempt's real outcome is
//!    reconstructed from what it wrote rather than from the event, a late
//!    reconciliation costs latency and not correctness.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use crate::api::{ApiFailure, PodListing, PodPhase, PodSnapshot, WatchDelivery};
use crate::identity::{AttemptKey, IdentityError, ObservedIdentity, identify, run_selector};

/// The default bound on silence: how long a watch may say nothing before the
/// observer stops believing it.
///
/// **90 seconds**, and the number is a trade between two measured facts rather
/// than a round figure.
///
/// - It must be **longer than an idle cluster's quiet stretch**, or an idle graph
///   churns. Bookmarks — the only thing an idle watch emits — were measured at
///   roughly one per 40–60 seconds on an idle namespace, with the longest
///   observed gap just under a minute. Ninety seconds clears that with margin.
/// - It must be **short enough that a hung run is noticed well inside a human's
///   patience**. Placing a pod costs about a second warm and up to twenty-four
///   seconds at the tail of a wide cold fan-out, so ninety seconds is several
///   times the worst measured startup and still under two minutes of silence
///   before dagr acts.
///
/// The cost of being wrong on the high side is a late reconciliation; on the low
/// side it is one extra LIST of one namespace per bound. Both are cheap, which is
/// why this is a constant with a settable field rather than an operator knob —
/// it becomes one only if a real deployment shows it needs to be.
pub const DEFAULT_STALL_BOUND: Duration = Duration::from_secs(90);

/// The client-side ceiling on a watch timeout. Anything at or above this is
/// rejected before the request leaves the process, so the default sits below it.
pub const WATCH_TIMEOUT_CEILING_SECS: u32 = 295;

/// Which run's pods this observer is watching, and which program's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSelector {
    /// The run id every pod must carry.
    pub run_id: String,
    /// The structural fingerprint every pod must claim. A pod annotated with
    /// anything else was launched by a different program and is reported foreign.
    pub structural_fingerprint: String,
}

impl RunSelector {
    /// Build a selector for one run of one program.
    #[must_use]
    pub fn new(run_id: impl Into<String>, structural_fingerprint: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            structural_fingerprint: structural_fingerprint.into(),
        }
    }

    /// The label selector the watch and the list are narrowed by.
    #[must_use]
    pub fn label_selector(&self) -> String {
        run_selector(&self.run_id)
    }
}

/// The bounds the discipline runs under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverLimits {
    /// How long the watch may be silent before it is reconnected. See
    /// [`DEFAULT_STALL_BOUND`].
    pub stall_bound: Duration,
    /// The delay after the *second* consecutive failure. The first retries
    /// immediately — a single expiry is the ordinary consequence of an outage and
    /// deserves no penalty — and each subsequent one doubles.
    pub backoff_initial: Duration,
    /// The ceiling the doubling stops at.
    pub backoff_max: Duration,
    /// How many consecutive failures end the observer with a classified failure.
    pub max_consecutive_failures: u32,
    /// How long a run of consecutive failures may last before it ends the
    /// observer, whichever bound trips first.
    pub failure_window: Duration,
    /// The watch's own server-side timeout, below
    /// [`WATCH_TIMEOUT_CEILING_SECS`].
    pub watch_timeout_secs: u32,
}

impl Default for ObserverLimits {
    fn default() -> Self {
        Self {
            stall_bound: DEFAULT_STALL_BOUND,
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            max_consecutive_failures: 12,
            failure_window: Duration::from_mins(5),
            watch_timeout_secs: 270,
        }
    }
}

/// What happened, from the core's point of view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverInput {
    /// A list succeeded.
    Listed(PodListing),
    /// A list failed.
    ListFailed(ApiFailure),
    /// A watch was opened.
    WatchEstablished,
    /// A watch could not be opened.
    WatchFailed(ApiFailure),
    /// The open watch produced an item.
    Delivered(WatchDelivery),
    /// The open watch ended.
    StreamEnded,
    /// The silence bound elapsed with nothing delivered.
    Tick,
}

/// What to do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverAction {
    /// Read the open watch. If nothing arrives within `stall_deadline`, report a
    /// [`Tick`](ObserverInput::Tick).
    Read {
        /// How much of the silence bound is left.
        stall_deadline: Duration,
    },
    /// Re-list, reconcile, then watch from the listing's version.
    Relist {
        /// How long to wait first.
        delay: Duration,
    },
    /// Open a watch from a known-good version.
    Resume {
        /// The version to resume from.
        resource_version: String,
        /// How long to wait first.
        delay: Duration,
    },
    /// Stop, with a classified reason.
    Fail(ObserverFailure),
}

/// Why the observer stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationCause {
    /// The watch's starting version had aged out of the cache.
    Expired,
    /// The stream failed below the protocol.
    Transport,
    /// The server described a failure that is ordinarily transient.
    TransientApi {
        /// The status it reported.
        code: u16,
    },
    /// The watch said nothing at all for longer than the bound.
    Silence,
}

impl fmt::Display for TerminationCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TerminationCause::Expired => f.write_str("the watch's resource version expired"),
            TerminationCause::Transport => f.write_str("the watch stream failed"),
            TerminationCause::TransientApi { code } => {
                write!(f, "the API server reported {code}")
            }
            TerminationCause::Silence => {
                f.write_str("the watch delivered nothing inside the silence bound")
            }
        }
    }
}

/// The observer gave up, and says why.
///
/// This type existing is the point: the alternative to a classified failure is an
/// infinite quiet retry, which presents as a hung run with nothing in the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverFailure {
    /// The class of the last failure in the run that exhausted the budget.
    pub cause: TerminationCause,
    /// How many consecutive failures there were.
    pub consecutive_failures: u32,
    /// How long the run of failures lasted.
    pub elapsed: Duration,
    /// What the platform said, verbatim.
    pub last_message: String,
}

impl fmt::Display for ObserverFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the pod observer stopped after {} consecutive failures over {:?}: {} — {}",
            self.consecutive_failures, self.elapsed, self.cause, self.last_message
        )
    }
}

impl std::error::Error for ObserverFailure {}

/// A pod that is not this run's, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignPod {
    /// The pod's name.
    pub pod_name: String,
    /// Why it was not attributed.
    pub reason: ForeignReason,
}

/// Why a pod matching the selector was not attributed to a waiter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForeignReason {
    /// The annotated structural fingerprint is a different program's.
    ForeignFingerprint {
        /// The fingerprint this observer watches for.
        expected: String,
        /// The fingerprint the pod claims.
        found: String,
    },
    /// The pod's run id is not this run's — which a correct selector should have
    /// excluded, so seeing one is worth recording rather than ignoring.
    ForeignRun {
        /// The run this observer watches.
        expected: String,
        /// The run the pod claims.
        found: String,
    },
    /// The pod could not be identified at all.
    Unidentifiable(IdentityError),
}

impl fmt::Display for ForeignReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForeignReason::ForeignFingerprint { expected, found } => write!(
                f,
                "structural fingerprint `{found}` is not this run's `{expected}` — \
                 the pod was launched by a different program"
            ),
            ForeignReason::ForeignRun { expected, found } => {
                write!(f, "run `{found}` is not this observer's `{expected}`")
            }
            ForeignReason::Unidentifiable(err) => write!(f, "{err}"),
        }
    }
}

/// One attributed observation of one attempt's pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodObservation {
    /// The attempt this pod runs.
    pub key: AttemptKey,
    /// The pod's name.
    pub pod_name: String,
    /// The phase as of this observation.
    pub phase: PodPhase,
    /// Whether the phase is one the pod does not leave.
    pub terminal: bool,
    /// Whether the pod is gone — deleted, or absent from a reconciliation that
    /// had previously seen it. A waiter is retired by this too, because a pod
    /// that no longer exists will never reach a phase.
    pub vanished: bool,
    /// The **pod**-level reason, e.g. an eviction.
    pub pod_reason: Option<String>,
    /// The **container**-level reason, e.g. an out-of-memory kill.
    pub container_reason: Option<String>,
    /// The container's exit code, when it ran.
    pub exit_code: Option<i32>,
    /// The authoritative identity read off the pod's annotations.
    pub identity: ObservedIdentity,
}

impl PodObservation {
    /// Whether this observation retires its waiter: nothing further will come.
    #[must_use]
    pub fn is_final(&self) -> bool {
        self.terminal || self.vanished
    }
}

/// One thing the core produced for the driver to route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// An attributed observation, for the waiter on its attempt key.
    ///
    /// Boxed: an observation carries a full authoritative identity and is an
    /// order of magnitude larger than the other variant, so an unboxed enum
    /// would cost every foreign report the same space as an observation.
    Observed(Box<PodObservation>),
    /// A pod that is not this run's.
    Foreign(ForeignPod),
}

/// The result of one [`ObserverCore::step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// What to route, in order.
    pub deliveries: Vec<Delivery>,
    /// What to do next.
    pub action: ObserverAction,
}

/// What the observer did, for the record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObserverStats {
    /// How many LIST calls were made — one at start, one per expiry, one per
    /// stall.
    pub relists: u32,
    /// How many watches were opened.
    pub resumes: u32,
    /// How many times silence tripped the bound.
    pub stalls: u32,
    /// How many watches were opened after the first.
    pub reconnects: u32,
}

/// The deterministic half: the reconnect discipline, the demultiplexing, and the
/// exactly-once bookkeeping, with no I/O and no clock of its own.
#[derive(Debug)]
pub struct ObserverCore {
    selector: RunSelector,
    limits: ObserverLimits,
    /// The last phase delivered per attempt, so an unchanged phase is not
    /// re-delivered on every reconciliation.
    phases: BTreeMap<AttemptKey, PodPhase>,
    /// Attempts whose final observation has been delivered. This is what makes
    /// delivery idempotent on the **attempt key** rather than on the event, so a
    /// duplicate from the API, a repeat across a reconnect, and a re-read during
    /// a resync all collapse to one notification.
    finished: BTreeSet<AttemptKey>,
    /// Attempts whose pod has been seen at least once, so a later absence is a
    /// disappearance rather than a pod that has not been created yet — with the
    /// pod name and identity last read off it, so a disappearance can be reported
    /// with the identity the pod actually carried rather than a fabricated one.
    seen: BTreeMap<AttemptKey, (String, ObservedIdentity)>,
    /// The most recent version actually observed: a bookmark, or the object
    /// version on a delivered event. Both advance it; the bookmark exists for the
    /// idle case, and is a cheap resume point rather than a heartbeat.
    last_version: Option<String>,
    last_progress: Duration,
    consecutive_failures: u32,
    first_failure_at: Option<Duration>,
    stats: ObserverStats,
}

impl ObserverCore {
    /// A core watching `selector` under `limits`, before anything has happened.
    #[must_use]
    pub fn new(selector: RunSelector, limits: ObserverLimits) -> Self {
        Self {
            selector,
            limits,
            phases: BTreeMap::new(),
            finished: BTreeSet::new(),
            seen: BTreeMap::new(),
            last_version: None,
            last_progress: Duration::ZERO,
            consecutive_failures: 0,
            first_failure_at: None,
            stats: ObserverStats::default(),
        }
    }

    /// What the observer has done so far.
    #[must_use]
    pub fn stats(&self) -> ObserverStats {
        self.stats
    }

    /// The label selector to list and watch with.
    #[must_use]
    pub fn label_selector(&self) -> String {
        self.selector.label_selector()
    }

    /// The first thing to do: there is no watch and no version, so list.
    #[must_use]
    pub fn initial_action() -> ObserverAction {
        ObserverAction::Relist {
            delay: Duration::ZERO,
        }
    }

    /// Advance the machine.
    ///
    /// `now` is a monotonic offset from the observer's start — never a wall
    /// clock, so the discipline behaves identically under a clock that jumps.
    pub fn step(&mut self, now: Duration, input: ObserverInput) -> Step {
        match input {
            ObserverInput::Listed(listing) => {
                self.stats.relists += 1;
                self.note_progress(now);
                let deliveries = self.reconcile(&listing.pods);
                self.last_version = Some(listing.resource_version.clone());
                Step {
                    deliveries,
                    action: ObserverAction::Resume {
                        resource_version: listing.resource_version,
                        delay: Duration::ZERO,
                    },
                }
            }
            ObserverInput::ListFailed(failure) => {
                self.stats.relists += 1;
                self.fail(now, failure, true)
            }
            ObserverInput::WatchEstablished => {
                self.stats.resumes += 1;
                if self.stats.resumes > 1 {
                    self.stats.reconnects += 1;
                }
                // Establishing a connection is not evidence the server will keep
                // it: a watch that is accepted and immediately closed would reset
                // a counter that exists to notice exactly that. Only a delivery or
                // a successful list clears the failure run. The silence clock does
                // restart, because silence is measured from the live stream.
                self.last_progress = now;
                Step {
                    deliveries: Vec::new(),
                    action: self.read_action(now),
                }
            }
            ObserverInput::WatchFailed(failure) => self.fail(now, failure, false),
            ObserverInput::StreamEnded => {
                self.fail(now, ApiFailure::transport("the watch stream ended"), false)
            }
            ObserverInput::Delivered(delivery) => self.deliver(now, delivery),
            ObserverInput::Tick => {
                if now.saturating_sub(self.last_progress) < self.limits.stall_bound {
                    return Step {
                        deliveries: Vec::new(),
                        action: self.read_action(now),
                    };
                }
                // Silence is not a failure — an idle cluster is silent too — so it
                // does not spend the retry budget. It does force a full
                // reconciliation, which is the only way to tell the two apart.
                self.stats.stalls += 1;
                self.last_progress = now;
                Step {
                    deliveries: Vec::new(),
                    action: ObserverAction::Relist {
                        delay: Duration::ZERO,
                    },
                }
            }
        }
    }

    /// How long is left of the silence bound.
    fn read_action(&self, now: Duration) -> ObserverAction {
        ObserverAction::Read {
            stall_deadline: self
                .limits
                .stall_bound
                .saturating_sub(now.saturating_sub(self.last_progress)),
        }
    }

    /// Health: the stall clock restarts and the failure run is forgotten.
    ///
    /// Called for a successful list, a delivered object, and a bookmark — the
    /// three things that are evidence the server is answering. It is deliberately
    /// **not** called when a watch is merely established, because a watch that is
    /// accepted and immediately closed is exactly the failure the run counter
    /// exists to notice.
    ///
    /// One consequence was considered and accepted: because a successful list
    /// clears the run, a hypothetical server that expired every watch immediately
    /// after the list that fed it would re-list without ever backing off. It needs
    /// the watch cache to turn over *entirely* between a list and the watch that
    /// follows it microseconds later, and a cluster in that state is beyond any
    /// client-side pacing; the alternative — not clearing the run on a successful
    /// list — would make an ordinary run's *isolated* transient failures
    /// accumulate over hours until the elapsed bound tripped on a healthy cluster,
    /// which is a worse failure and a far likelier one.
    fn note_progress(&mut self, now: Duration) {
        self.last_progress = now;
        self.consecutive_failures = 0;
        self.first_failure_at = None;
    }

    /// Classify a failure, spend the budget, and choose the recovery.
    ///
    /// `from_list` distinguishes a failed LIST (whose recovery is another LIST)
    /// from a failed watch (whose recovery depends on the class).
    fn fail(&mut self, now: Duration, failure: ApiFailure, from_list: bool) -> Step {
        self.consecutive_failures += 1;
        let first = *self.first_failure_at.get_or_insert(now);
        let elapsed = now.saturating_sub(first);

        let cause = if failure.is_expired() {
            TerminationCause::Expired
        } else if let Some(code) = failure.code {
            TerminationCause::TransientApi { code }
        } else {
            TerminationCause::Transport
        };

        if self.consecutive_failures >= self.limits.max_consecutive_failures
            || elapsed >= self.limits.failure_window
        {
            return Step {
                deliveries: Vec::new(),
                action: ObserverAction::Fail(ObserverFailure {
                    cause,
                    consecutive_failures: self.consecutive_failures,
                    elapsed,
                    last_message: failure.message,
                }),
            };
        }

        let delay = self.backoff();
        // An expiry is recovered by re-listing, and the version inside its message
        // is never the resume point. A transport end resumes from the last version
        // actually seen, which is the cheap reconnect; with nothing seen there is
        // nothing to resume from, so it lists.
        let action = match (&cause, from_list, self.last_version.clone()) {
            (TerminationCause::Expired, _, _) | (_, true, _) | (_, _, None) => {
                ObserverAction::Relist { delay }
            }
            (_, false, Some(resource_version)) => ObserverAction::Resume {
                resource_version,
                delay,
            },
        };
        Step {
            deliveries: Vec::new(),
            action,
        }
    }

    /// The delay before the next attempt: none after the first failure, then
    /// doubling to the ceiling.
    fn backoff(&self) -> Duration {
        if self.consecutive_failures <= 1 {
            return Duration::ZERO;
        }
        let shift = (self.consecutive_failures - 2).min(20);
        let scaled = self.limits.backoff_initial.saturating_mul(1_u32 << shift);
        scaled.min(self.limits.backoff_max)
    }

    /// One item off the watch.
    fn deliver(&mut self, now: Duration, delivery: WatchDelivery) -> Step {
        match delivery {
            WatchDelivery::ApiError {
                code,
                reason,
                message,
            } => {
                // The expiry path. It arrives HERE — as a decoded frame — and not
                // as a transport error, which is why a handler that only inspects
                // transport failures never notices one.
                let failure = ApiFailure::api(code, reason, message);
                self.fail(now, failure, false)
            }
            WatchDelivery::Transport { message } => {
                self.fail(now, ApiFailure::transport(message), false)
            }
            WatchDelivery::Bookmark { resource_version } => {
                self.note_progress(now);
                self.last_version = Some(resource_version);
                Step {
                    deliveries: Vec::new(),
                    action: self.read_action(now),
                }
            }
            WatchDelivery::Added(pod) | WatchDelivery::Modified(pod) => {
                self.note_progress(now);
                self.last_version = Some(pod.resource_version.clone());
                let deliveries = self.observe(&pod, false);
                Step {
                    deliveries,
                    action: self.read_action(now),
                }
            }
            WatchDelivery::Deleted(pod) => {
                self.note_progress(now);
                self.last_version = Some(pod.resource_version.clone());
                let deliveries = self.observe(&pod, true);
                Step {
                    deliveries,
                    action: self.read_action(now),
                }
            }
        }
    }

    /// Re-read the world: this is what makes a reconnect lossless.
    ///
    /// A LIST returns current state rather than a delta, so a transition that
    /// happened while the watch was down is still here — and the finished set is
    /// what keeps it from being delivered twice.
    fn reconcile(&mut self, pods: &[PodSnapshot]) -> Vec<Delivery> {
        let mut deliveries = Vec::new();
        let mut present: BTreeSet<AttemptKey> = BTreeSet::new();

        for pod in pods {
            if let Ok(identity) = self.attribute(pod) {
                present.insert(identity.key.clone());
            }
            deliveries.extend(self.observe(pod, false));
        }

        // A pod this observer had seen and that a full reconciliation no longer
        // reports is gone. Reported once, so a waiter cannot hang on a pod that no
        // longer exists — and only for pods actually seen, because an attempt
        // registered before its pod is created is the ordinary case.
        let vanished: Vec<(AttemptKey, String, ObservedIdentity)> = self
            .seen
            .iter()
            .filter(|(key, _)| !present.contains(*key) && !self.finished.contains(*key))
            .map(|(key, (name, identity))| (key.clone(), name.clone(), identity.clone()))
            .collect();
        for (key, pod_name, identity) in vanished {
            self.finished.insert(key.clone());
            deliveries.push(Delivery::Observed(Box::new(PodObservation {
                key,
                pod_name,
                phase: PodPhase::Unknown,
                terminal: false,
                vanished: true,
                pod_reason: None,
                container_reason: None,
                exit_code: None,
                identity,
            })));
        }

        deliveries
    }

    /// Read a pod's identity and check it is this run's, this program's.
    fn attribute(&self, pod: &PodSnapshot) -> Result<ObservedIdentity, ForeignPod> {
        let identity = identify(&pod.labels, &pod.annotations).map_err(|err| ForeignPod {
            pod_name: pod.name.clone(),
            reason: ForeignReason::Unidentifiable(err),
        })?;
        if identity.key.run_id != self.selector.run_id {
            return Err(ForeignPod {
                pod_name: pod.name.clone(),
                reason: ForeignReason::ForeignRun {
                    expected: self.selector.run_id.clone(),
                    found: identity.key.run_id,
                },
            });
        }
        if identity.structural_fingerprint != self.selector.structural_fingerprint {
            return Err(ForeignPod {
                pod_name: pod.name.clone(),
                reason: ForeignReason::ForeignFingerprint {
                    expected: self.selector.structural_fingerprint.clone(),
                    found: identity.structural_fingerprint,
                },
            });
        }
        Ok(identity)
    }

    /// Turn one pod into zero or one deliveries.
    fn observe(&mut self, pod: &PodSnapshot, deleted: bool) -> Vec<Delivery> {
        let identity = match self.attribute(pod) {
            Ok(identity) => identity,
            Err(foreign) => return vec![Delivery::Foreign(foreign)],
        };
        let key = identity.key.clone();
        self.seen
            .insert(key.clone(), (pod.name.clone(), identity.clone()));

        if self.finished.contains(&key) {
            // Delivered already. This is the whole of exactly-once: a duplicate
            // from the API, a repeat across a reconnect, and a re-read during a
            // resync are all the same event as far as the waiter is concerned.
            return Vec::new();
        }

        let terminal = pod.phase.is_terminal();
        if !terminal && !deleted && self.phases.get(&key) == Some(&pod.phase) {
            return Vec::new();
        }
        self.phases.insert(key.clone(), pod.phase);
        if terminal || deleted {
            self.finished.insert(key.clone());
        }

        vec![Delivery::Observed(Box::new(PodObservation {
            key,
            pod_name: pod.name.clone(),
            phase: pod.phase,
            terminal,
            vanished: deleted,
            pod_reason: pod.pod_reason.clone(),
            container_reason: pod.container_reason.clone(),
            exit_code: pod.exit_code,
            identity,
        }))]
    }
}
