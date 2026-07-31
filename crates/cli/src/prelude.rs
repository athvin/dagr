//! The **authoring prelude** — one glob import (`use dagr_cli::prelude::*;`) that
//! brings the declaration surface a DAG author needs into scope.
//!
//! It re-exports the [`FlowBuilder`] declaration façade (the curated `source` /
//! `node` surface a DAG body is handed), the [`RunnableFlow`] it wraps, and the
//! core authoring trio ([`Task`], [`RunContext`], [`TaskError`]) plus the
//! [`StableName`] / [`StableInputNames`] traits the graph-emittable declaration
//! verbs require — so a task-and-DAG source file imports exactly one path.
//!
//! # The auto-discovery surface (the `dag` feature)
//!
//! Under the default-on `dag` feature the prelude also carries the DAG
//! auto-discovery surface (M6, ADR 092): the [`macro@dag`] attribute (declares a DAG
//! over the [`FlowBuilder`] façade and auto-registers it), [`run()`], the one-call
//! entrypoint a DAG-hosting binary's `main` delegates to, and [`DagRegistration`],
//! the record `#[dag]` submits per DAG. All three are absent under
//! `--no-default-features`, which drops the `inventory` runtime dependency the
//! discovery mechanism uses.
//!
//! # The one authoring import
//!
//! `use dagr_cli::prelude::*;` is *the* single authoring import (ADR 092): declare
//! tasks with [`Task`] (or the `#[task]` macro, re-exported by `dagr-core`), declare a
//! DAG's nodes through [`FlowBuilder`], register the flow through [`RunnableFlow`],
//! and — under the `dag` feature — declare DAGs with the [`macro@dag`] attribute and
//! run them with the one-line [`run()`].

pub use crate::flow_builder::FlowBuilder;
#[cfg(feature = "dag")]
pub use crate::run::{DagRegistration, run};
pub use crate::run_flow::RunnableFlow;
/// The run-store defaults a hand-written driver reaches for: the default local-file
/// [`FileSink`], the wall-clock [`SystemClock`], and the [`DEFAULT_STORE_BASE`] — so
/// [`RunnableFlow::run`](crate::run_flow::RunnableFlow::run) needs no hand-written
/// sink/clock. (The one-call [`run_to_store`](crate::run_flow::RunnableFlow::run_to_store)
/// builds them for you; these are here for the explicit path.)
pub use crate::run_store::{DEFAULT_STORE_BASE, FileSink, SystemClock};
/// The `#[dag]` attribute macro (M6, T80) — the DAG-authoring sibling of `#[task]`.
/// Carried in the prelude under the default-on `dag` feature so `use
/// dagr_cli::prelude::*;` brings it into scope; absent under `--no-default-features`.
#[cfg(feature = "dag")]
pub use dagr_macros::dag;
/// The `#[task]` attribute and `#[derive(StableName)]` derive, re-exported from the
/// build-time proc-macro crate so `use dagr_cli::prelude::*;` is the **single**
/// authoring import that brings in the whole macro trio — `#[task]` (author a task),
/// `#[dag]` (declare a DAG), `#[derive(StableName)]` (name a task/payload so the
/// graph-emittable registrars accept it). The `StableName` *derive* (macro namespace)
/// coexists with the `StableName` *trait* re-exported below (type namespace) — the
/// standard trait+derive pairing. A proc-macro is never linked into the shipped
/// binary, so this adds no runtime dependency.
pub use dagr_macros::{Payload, StableName, task};

pub use dagr_core::TaskError;
pub use dagr_core::context::RunContext;
/// The **payload codec**: the [`Payload`] trait (a value that can cross a process
/// boundary as bytes) and its classified [`CodecError`], alongside the
/// `#[derive(Payload)]` derive re-exported above — the standard trait+derive pairing,
/// so one authoring import declares a payload type and encodes it. [`Codec`] is the
/// body half a derived impl provides; an author names it only when hand-writing a
/// codec.
pub use dagr_core::payload::{Codec, CodecError, Payload};
pub use dagr_core::stable_name::{StableInputNames, StableName};
pub use dagr_core::task::Task;
