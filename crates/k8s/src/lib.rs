#![doc = include_str!("../README.md")]
//!
//! # Module index
//!
//! The orientation above comes from the crate's `README.md`, inlined here so the
//! crates.io landing page and this front page are one file. What follows is the
//! map of where each piece lives.
//!
//! - [`observer`] — the deterministic [`ObserverCore`](observer::ObserverCore):
//!   the reconnect discipline, the demultiplexing and the exactly-once
//!   bookkeeping, as a state machine with no I/O and no clock of its own. The
//!   task that drives it — one watch per orchestrator process, its timer and its
//!   per-attempt waiters — is `dagr_cli::pod_observer`, because that is where the
//!   process and its async runtime are (ADR 004).
//! - [`identity`] — the label/annotation encoding and its inverse: labels are
//!   lossy selectors, annotations are authoritative.
//! - [`api`] — the [`PodApi`] port the observer watches through,
//!   and the shapes a list and a watch hand back, plus the
//!   [`PodLifecycle`] port the executor submits through.
//! - [`executor`] — the executor's **pure half** (M10, T108): the pod spec, the
//!   never-retry invariant, the three pre-start surfaces T101 measured, and
//!   terminal classification from pod status. No I/O, no clock, no runtime; the
//!   half that submits, waits and replays is `dagr_cli::k8s_runner`.
//! - [`access`] — out-of-cluster and in-cluster resolution, as a pure decision.
//! - `client` — the kube-rs adapter, behind the default-off `client` feature.
//!   Not linked, deliberately: it does not exist in a default build, and a link
//!   to it would be a broken intra-doc link in the very resolution the
//!   quarantine exists to produce.
//!   The only module here that speaks to a real API server, and the only one that
//!   pulls an HTTP or TLS crate.
//! - [`fake`] — the in-process fake API surface, behind the default-on
//!   `test-kit` feature, whose failures are scripted.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod access;
pub mod api;
/// The **kube-rs adapter** — the one place this crate speaks to a real API
/// server. Behind the default-off `client` feature, which is the quarantine: a
/// build without it compiles no HTTP or TLS crate at all.
#[cfg(feature = "client")]
pub mod client;
pub mod executor;
/// The **fake API surface** the observer's own suites drive: an in-process
/// [`PodApi`](api::PodApi) whose expiries, silences, duplicates and failures are
/// scripted. Behind the default-on `test-kit` feature, mirroring `dagr-core`'s,
/// so `--no-default-features` drops it and a pipeline binary never links it.
#[cfg(feature = "test-kit")]
pub mod fake;
pub mod identity;
pub mod observer;

pub use access::{AccessProbe, ClusterAccess, NoClusterAccess, resolve};
pub use api::{
    CreatedPod, PodApi, PodLifecycle, PodListing, PodPhase, PodSnapshot, PodWatch, WatchDelivery,
};
pub use executor::{
    ClusterRetry, PodOutcome, PodPlacement, PodRequest, PodSpec, PodStatusFacts, PodVerdict,
    PreStartFailure, PreStartSurface, build_pod, classify_pre_start, classify_terminal, pod_name,
    reject_credential_bearing,
};
pub use identity::{AttemptIdentity, AttemptKey, ObservedIdentity};
pub use observer::{
    Delivery, ObserverAction, ObserverCore, ObserverFailure, ObserverInput, ObserverLimits,
    ObserverStats, PodObservation, RunSelector, Step,
};
