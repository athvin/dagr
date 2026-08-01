#![doc = include_str!("../README.md")]
//!
//! # Module index
//!
//! The orientation above comes from the crate's `README.md`, inlined here so the
//! crates.io landing page and this front page are one file. What follows is the
//! map of where each piece lives.
//!
//! - [`observer`] — the shared [`PodObserver`](observer::PodObserver), its
//!   deterministic [`ObserverCore`](observer::ObserverCore), the reconnect
//!   discipline, and the per-attempt waiters.
//! - [`identity`] — the label/annotation encoding and its inverse: labels are
//!   lossy selectors, annotations are authoritative.
//! - [`api`] — the [`PodApi`](api::PodApi) port the observer watches through,
//!   and the shapes a list and a watch hand back.
//! - [`access`] — out-of-cluster and in-cluster resolution, as a pure decision.
//! - [`client`] — the kube-rs adapter, behind the default-off `client` feature.
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
/// The **fake API surface** the observer's own suites drive: an in-process
/// [`PodApi`](api::PodApi) whose expiries, silences, duplicates and failures are
/// scripted. Behind the default-on `test-kit` feature, mirroring `dagr-core`'s,
/// so `--no-default-features` drops it and a pipeline binary never links it.
#[cfg(feature = "test-kit")]
pub mod fake;
pub mod identity;
pub mod observer;

pub use access::{AccessProbe, ClusterAccess, NoClusterAccess, resolve};
pub use api::{PodApi, PodListing, PodPhase, PodSnapshot, WatchDelivery};
pub use identity::{AttemptIdentity, AttemptKey, ObservedIdentity};
pub use observer::{
    AttemptWaiter, ObserverFailure, ObserverLimits, ObserverReport, PodObservation, PodObserver,
    RunSelector,
};
