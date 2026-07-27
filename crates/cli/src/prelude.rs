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
//! auto-discovery surface (M6, ADR 092): [`run()`], the one-call entrypoint a
//! DAG-hosting binary's `main` delegates to, and [`DagRegistration`], the record a
//! binary submits per DAG (the `#[dag]` macro that emits those submissions lands in a
//! later ticket). Both are absent under `--no-default-features`, which drops the
//! `inventory` runtime dependency the discovery mechanism uses.
//!
//! # What is deliberately *not* here yet
//!
//! The `#[dag]` attribute macro belongs to a later ticket; when it lands it joins
//! this prelude (the ADR pins `use dagr_cli::prelude::*;` as *the* one authoring
//! import). Until then this prelude carries: declare tasks with [`Task`] (or the
//! `#[task]` macro, re-exported by `dagr-core`), declare a DAG's nodes through
//! [`FlowBuilder`], register the flow through [`RunnableFlow`], and — under the `dag`
//! feature — declare DAGs with [`DagRegistration`] and run them with [`run()`].

pub use crate::flow_builder::FlowBuilder;
#[cfg(feature = "dag")]
pub use crate::run::{run, DagRegistration};
pub use crate::run_flow::RunnableFlow;

pub use dagr_core::context::RunContext;
pub use dagr_core::stable_name::{StableInputNames, StableName};
pub use dagr_core::task::Task;
pub use dagr_core::TaskError;
