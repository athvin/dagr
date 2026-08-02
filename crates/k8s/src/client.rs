//! **The kube-rs adapter** — the one module here that speaks to a real API
//! server, and the only one that pulls an HTTP or TLS crate.
//!
//! It implements [`PodApi`] over the two primitive calls, `list` and `watch`,
//! and translates what they hand back into this crate's shapes. That is the whole
//! of its job: the reconnect discipline lives in [`crate::observer`], where it can
//! be driven by a fake, and this module makes no decision about what to do when a
//! watch ends.
//!
//! # Why the primitive calls, and not the client's own self-healing watcher
//!
//! kube-rs ships a `runtime::watcher` that re-lists-then-watches on its own, and
//! it does so correctly. It is not used here, for three reasons that all point
//! the same way:
//!
//! - **The classification would be unobservable.** The spike's taxonomy is
//!   written in terms of what the *primitive* stream produces — an expiry as a
//!   decoded `type: ERROR` frame, a transport failure as one error item followed
//!   by the end of the stream. A self-healing watcher consumes exactly those and
//!   reports neither, so "the observer classified an expiry and re-listed" would
//!   be a claim about somebody else's code with no way to assert it.
//! - **It could not be tested.** A self-healing watcher takes a concrete `Api`
//!   backed by a real client, so standing a fake behind it means standing up an
//!   HTTP fixture — in a project whose whole boundary claim is that it opens no
//!   inbound port, even in a test. Over the primitive calls, the fake is a trait
//!   implementation.
//! - **The discipline needs more than reconnection anyway.** Backoff, a silence
//!   bound and a periodic reconciliation are all dagr's regardless; the spike's
//!   own recipe says so, and permits dagr's own backoff explicitly. Having
//!   written those, inheriting the middle third would leave the discipline split
//!   across two owners.
//!
//! What is *not* re-derived is the behaviour: the spike established, against a
//! real cluster, exactly how each termination appears at this level, and the
//! classification here is aimed at those observations. The recorded frames in
//! this module's tests are the ones it captured.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

use futures_util::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, WatchParams};
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use kube::core::WatchEvent;

use crate::access::ClusterAccess;
use crate::api::{ApiFailure, PodApi, PodListing, PodPhase, PodSnapshot, PodWatch, WatchDelivery};
use crate::observer::{ObserverLimits, WATCH_TIMEOUT_CEILING_SECS};

/// The crypto provider is installed once per process, and the outcome is
/// remembered so a second client does not re-run it.
static PROVIDER: OnceLock<()> = OnceLock::new();

/// Install the process-level TLS crypto provider.
///
/// Enabling a TLS backend does **not** install one, and the omission does not
/// surface until the first handshake — as a *panic*, from inside the TLS crate.
/// A panic on first use is not an acceptable failure mode for an opt-in library
/// feature, so exactly one provider is named at the manifest and installed here,
/// before any client is built.
///
/// Installing twice is not an error: a host application that installed its own
/// provider keeps it, and this call is a no-op.
pub fn install_crypto_provider() {
    PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Building a client did not work.
#[derive(Debug)]
pub enum ClientError {
    /// The kubeconfig could not be read or parsed.
    Kubeconfig {
        /// The file that was read.
        path: std::path::PathBuf,
        /// What went wrong.
        source: kube::config::KubeconfigError,
    },
    /// The in-cluster environment did not produce a configuration, even though
    /// the files were present.
    InCluster(kube::config::InClusterError),
    /// The configuration was fine and the client would not build.
    Client(kube::Error),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Kubeconfig { path, .. } => {
                write!(f, "the kubeconfig at {} could not be used", path.display())
            }
            ClientError::InCluster(_) => f.write_str(
                "the in-cluster service account is mounted but did not produce a configuration",
            ),
            ClientError::Client(_) => f.write_str("the Kubernetes client could not be built"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Kubeconfig { source, .. } => Some(source),
            ClientError::InCluster(source) => Some(source),
            ClientError::Client(source) => Some(source),
        }
    }
}

/// Read access to one namespace's pods, through a real API server.
#[derive(Debug, Clone)]
pub struct KubePodApi {
    pods: Api<Pod>,
    watch_timeout_secs: u32,
}

impl KubePodApi {
    /// Build a client for `namespace` from an already-resolved [`ClusterAccess`].
    ///
    /// The resolution is separate on purpose: which cluster to talk to is a
    /// decision with a precedence and an actionable failure, and it is settled
    /// before any HTTP or TLS code is reached.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the configuration cannot be read or the
    /// client cannot be built.
    pub async fn connect(
        access: &ClusterAccess,
        namespace: &str,
        limits: &ObserverLimits,
    ) -> Result<Self, ClientError> {
        install_crypto_provider();
        let config = match access {
            ClusterAccess::OutOfCluster { kubeconfig, .. } => {
                let raw = Kubeconfig::read_from(kubeconfig).map_err(|source| {
                    ClientError::Kubeconfig {
                        path: kubeconfig.clone(),
                        source,
                    }
                })?;
                Config::from_custom_kubeconfig(raw, &KubeConfigOptions::default())
                    .await
                    .map_err(|source| ClientError::Kubeconfig {
                        path: kubeconfig.clone(),
                        source,
                    })?
            }
            ClusterAccess::InCluster { .. } => {
                Config::incluster().map_err(ClientError::InCluster)?
            }
        };
        let client = kube::Client::try_from(config).map_err(ClientError::Client)?;
        Ok(Self {
            pods: Api::namespaced(client, namespace),
            watch_timeout_secs: clamped_watch_timeout(limits),
        })
    }

    /// Wrap an already-built `Api`. The seam a caller uses when it owns the
    /// client for other reasons.
    #[must_use]
    pub fn from_api(pods: Api<Pod>, limits: &ObserverLimits) -> Self {
        Self {
            pods,
            watch_timeout_secs: clamped_watch_timeout(limits),
        }
    }
}

/// The watch's server-side timeout, held below the client-side ceiling.
///
/// Anything at or above the ceiling fails the request before it leaves the
/// process, so a caller who sets a large timeout gets a long watch rather than a
/// dead one.
fn clamped_watch_timeout(limits: &ObserverLimits) -> u32 {
    limits
        .watch_timeout_secs
        .min(WATCH_TIMEOUT_CEILING_SECS - 1)
}

/// One open watch over a real API server: kube's own stream, translated item by
/// item.
///
/// The stream is held **directly** rather than pumped into a channel by a
/// spawned task. That is what lets this crate name no runtime: there is nothing
/// here to spawn onto, and the future the observer awaits is the client's own.
/// Teardown keeps its guarantee for free — dropping this value drops the
/// underlying connection, so a torn-down watch closes rather than being asked to.
pub struct KubeWatch {
    stream: Pin<Box<dyn Stream<Item = Result<WatchEvent<Pod>, kube::Error>> + Send>>,
}

impl std::fmt::Debug for KubeWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeWatch").finish_non_exhaustive()
    }
}

impl PodWatch for KubeWatch {
    async fn next(&mut self) -> Option<WatchDelivery> {
        let item = self.stream.next().await?;
        Some(match item {
            Ok(event) => delivery(event),
            Err(error) => {
                let classified = failure(error);
                match classified.code {
                    Some(code) => WatchDelivery::ApiError {
                        code,
                        reason: classified.reason,
                        message: classified.message,
                    },
                    None => WatchDelivery::Transport {
                        message: classified.message,
                    },
                }
            }
        })
    }
}

impl PodApi for KubePodApi {
    type Watch = KubeWatch;

    fn list(
        &self,
        selector: &str,
    ) -> impl std::future::Future<Output = Result<PodListing, ApiFailure>> + Send {
        let pods = self.pods.clone();
        let params = ListParams::default().labels(selector);
        async move {
            let listed = pods.list(&params).await.map_err(failure)?;
            Ok(PodListing {
                // A listing always carries a version; treating an absent one as
                // "0" would silently restart the watch from the beginning of the
                // cache, so it is treated as no version at all and the observer
                // lists again.
                resource_version: listed.metadata.resource_version.unwrap_or_default(),
                pods: listed.items.iter().map(snapshot).collect(),
            })
        }
    }

    fn watch(
        &self,
        selector: &str,
        resource_version: &str,
    ) -> impl Future<Output = Result<Self::Watch, ApiFailure>> + Send {
        let pods = self.pods.clone();
        // Bookmarks are requested (they are on by default) because they are the
        // cheap resume point during an idle stretch. Nothing is scheduled off
        // their arrival — their cadence tracks cluster activity, so they are not
        // a heartbeat.
        let params = WatchParams::default()
            .labels(selector)
            .timeout(self.watch_timeout_secs);
        let resource_version = resource_version.to_string();
        async move {
            let stream = pods
                .watch(&params, &resource_version)
                .await
                .map_err(failure)?;
            Ok(KubeWatch {
                stream: Box::pin(stream),
            })
        }
    }
}

/// Translate one decoded watch event.
///
/// The [`WatchEvent::Error`] arm is the load-bearing one: an expiry arrives
/// **here**, as a decoded frame, and never as a transport failure.
fn delivery(event: WatchEvent<Pod>) -> WatchDelivery {
    match event {
        WatchEvent::Added(pod) => WatchDelivery::Added(snapshot(&pod)),
        WatchEvent::Modified(pod) => WatchDelivery::Modified(snapshot(&pod)),
        WatchEvent::Deleted(pod) => WatchDelivery::Deleted(snapshot(&pod)),
        WatchEvent::Bookmark(bookmark) => WatchDelivery::Bookmark {
            resource_version: bookmark.metadata.resource_version,
        },
        WatchEvent::Error(response) => WatchDelivery::ApiError {
            code: response.code,
            reason: response.reason,
            message: response.message,
        },
    }
}

/// Classify a client error into the port's shape, keeping the platform's words.
fn failure(error: kube::Error) -> ApiFailure {
    match error {
        kube::Error::Api(response) => {
            ApiFailure::api(response.code, response.reason, response.message)
        }
        other => ApiFailure::transport(other.to_string()),
    }
}

/// Read one pod into a snapshot.
///
/// Both reason fields are read, because they carry different failures: an
/// out-of-memory kill appears on the **container** status with an empty pod
/// reason, while an eviction appears on the **pod** with the container reporting
/// a generic error. A reader that consults one loses half the diagnostics.
fn snapshot(pod: &Pod) -> PodSnapshot {
    let status = pod.status.as_ref();
    let container = status
        .and_then(|status| status.container_statuses.as_ref())
        .and_then(|statuses| statuses.first());
    let state = container.and_then(|container| container.state.as_ref());
    let terminated = state.and_then(|state| state.terminated.as_ref());
    // A container that never started reports its trouble as a *waiting* reason —
    // an image that will not pull sits there indefinitely and reaches no terminal
    // phase — so the waiting reason stands in when there is no terminated state.
    let waiting = state.and_then(|state| state.waiting.as_ref());

    PodSnapshot {
        name: pod.metadata.name.clone().unwrap_or_default(),
        resource_version: pod.metadata.resource_version.clone().unwrap_or_default(),
        phase: status
            .and_then(|status| status.phase.as_deref())
            .map_or(PodPhase::Unknown, PodPhase::from_api),
        labels: pod.metadata.labels.clone().unwrap_or_default(),
        annotations: pod.metadata.annotations.clone().unwrap_or_default(),
        pod_reason: status.and_then(|status| status.reason.clone()),
        container_reason: terminated
            .and_then(|terminated| terminated.reason.clone())
            .or_else(|| waiting.and_then(|waiting| waiting.reason.clone())),
        exit_code: terminated.map(|terminated| terminated.exit_code),
        uid: pod.metadata.uid.clone(),
        host: pod.spec.as_ref().and_then(|spec| spec.node_name.clone()),
        // Carried in its own field as well as folded into `container_reason`: the
        // executor's pre-start classification must be able to tell "the container is
        // WAITING for this reason" from "the container TERMINATED for this reason",
        // and the merged field cannot (T101 — a pod that never starts reaches no
        // terminal phase, so the waiting reason is the only signal there will be).
        waiting_reason: waiting.and_then(|waiting| waiting.reason.clone()),
        // The second pre-start surface: an unschedulable pod reports through the
        // `PodScheduled` condition and has no container status entry at all, so a
        // waiting-reason check alone never sees it.
        scheduling_refusal: status
            .and_then(|status| status.conditions.as_ref())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.type_ == "PodScheduled" && condition.status == "False"
                })
            })
            .and_then(|condition| condition.reason.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Push a recorded frame through the client's own deserializer, exactly as
    /// the live path does, and then through this module's translation.
    fn decode(frame: &str) -> WatchDelivery {
        let event: WatchEvent<Pod> =
            serde_json::from_str(frame).expect("a recorded frame deserializes");
        delivery(event)
    }

    /// **Test-plan scenario: an expiry is classified.** The body is the one the
    /// spike captured off a real API server — including the resource version in
    /// the message, which is the watch cache's oldest retained bound and never a
    /// resume point.
    #[test]
    fn a_recorded_expiry_frame_decodes_as_an_in_stream_api_error() {
        let frame = r#"{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","metadata":{},
            "status":"Failure","message":"too old resource version: 1 (9371596)",
            "reason":"Expired","code":410}}"#;
        match decode(frame) {
            WatchDelivery::ApiError {
                code,
                reason,
                message,
            } => {
                assert_eq!(code, 410);
                assert_eq!(reason, "Expired");
                assert!(message.contains("too old resource version"));
                assert!(
                    ApiFailure::api(code, reason, message).is_expired(),
                    "the classifier must recognise a real expiry body"
                );
            }
            other => panic!("an expiry must decode as an in-stream error, got {other:?}"),
        }
    }

    /// A terminal pod decodes with both reason fields and the exit code.
    #[test]
    fn a_recorded_terminal_pod_frame_carries_both_reasons() {
        let frame = r#"{"type":"MODIFIED","object":{"kind":"Pod","apiVersion":"v1",
            "metadata":{"name":"dagr-extract-1","resourceVersion":"2534",
              "labels":{"dagr.io/run-id":"r1"},
              "annotations":{"dagr.io/node-name":"extract"}},
            "status":{"phase":"Failed","reason":"Evicted",
              "containerStatuses":[{"name":"task","ready":false,"restartCount":0,"image":"busybox",
                "imageID":"","state":{"terminated":{"exitCode":137,"reason":"OOMKilled"}}}]}}}"#;
        match decode(frame) {
            WatchDelivery::Modified(pod) => {
                assert_eq!(pod.name, "dagr-extract-1");
                assert_eq!(pod.resource_version, "2534");
                assert_eq!(pod.phase, PodPhase::Failed);
                assert!(pod.phase.is_terminal());
                assert_eq!(pod.pod_reason.as_deref(), Some("Evicted"));
                assert_eq!(pod.container_reason.as_deref(), Some("OOMKilled"));
                assert_eq!(pod.exit_code, Some(137));
                assert_eq!(
                    pod.labels.get("dagr.io/run-id").map(String::as_str),
                    Some("r1")
                );
                assert_eq!(
                    pod.annotations.get("dagr.io/node-name").map(String::as_str),
                    Some("extract")
                );
            }
            other => panic!("expected a modified pod, got {other:?}"),
        }
    }

    /// A pod whose image will not pull never reaches a terminal phase, and
    /// reports its trouble as a *waiting* reason. Carrying it is what lets a
    /// caller notice at all.
    #[test]
    fn a_recorded_pre_start_failure_carries_the_waiting_reason() {
        let frame = r#"{"type":"MODIFIED","object":{"kind":"Pod","apiVersion":"v1",
            "metadata":{"name":"dagr-extract-1","resourceVersion":"2535"},
            "status":{"phase":"Pending",
              "containerStatuses":[{"name":"task","ready":false,"restartCount":0,"image":"nope",
                "imageID":"","state":{"waiting":{"reason":"ImagePullBackOff",
                  "message":"Back-off pulling image"}}}]}}}"#;
        match decode(frame) {
            WatchDelivery::Modified(pod) => {
                assert_eq!(pod.phase, PodPhase::Pending);
                assert!(!pod.phase.is_terminal());
                assert_eq!(pod.container_reason.as_deref(), Some("ImagePullBackOff"));
                assert_eq!(pod.exit_code, None);
            }
            other => panic!("expected a modified pod, got {other:?}"),
        }
    }

    /// A bookmark decodes to a resume point and nothing else.
    #[test]
    fn a_recorded_bookmark_frame_decodes_to_a_resume_point() {
        let frame = r#"{"type":"BOOKMARK","object":{"kind":"Pod","apiVersion":"v1",
            "metadata":{"resourceVersion":"2600"}}}"#;
        assert_eq!(
            decode(frame),
            WatchDelivery::Bookmark {
                resource_version: "2600".to_string()
            }
        );
    }

    /// The watch timeout stays under the client-side ceiling, whatever the limits
    /// say — anything at or above it fails the request before it leaves the
    /// process.
    #[test]
    fn the_watch_timeout_is_held_below_the_client_side_ceiling() {
        let absurd = ObserverLimits {
            watch_timeout_secs: 100_000,
            ..ObserverLimits::default()
        };
        assert!(clamped_watch_timeout(&absurd) < WATCH_TIMEOUT_CEILING_SECS);
        assert_eq!(
            clamped_watch_timeout(&ObserverLimits::default()),
            270,
            "an ordinary timeout is passed through unchanged"
        );
    }

    /// Installing the provider twice is a no-op rather than a failure.
    #[test]
    fn installing_the_crypto_provider_is_idempotent() {
        install_crypto_provider();
        install_crypto_provider();
    }
}
