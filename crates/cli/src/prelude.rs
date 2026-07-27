//! The **authoring prelude** — one glob import (`use dagr_cli::prelude::*;`) that
//! brings the declaration surface a DAG author needs into scope.
//!
//! It re-exports the [`FlowBuilder`] declaration façade (the curated `source` /
//! `node` surface a DAG body is handed), the [`RunnableFlow`] it wraps, and the
//! core authoring trio ([`Task`], [`RunContext`], [`TaskError`]) plus the
//! [`StableName`] / [`StableInputNames`] traits the graph-emittable declaration
//! verbs require — so a task-and-DAG source file imports exactly one path.
//!
//! # What is deliberately *not* here yet
//!
//! The `#[dag]` attribute macro and the auto-discovery `dagr_cli::run` entrypoint
//! belong to later tickets; when they land they join this prelude (the ADR pins
//! `use dagr_cli::prelude::*;` as *the* one authoring import). Until then this
//! prelude carries only what ships: declare tasks with [`Task`] (or the `#[task]`
//! macro, re-exported by `dagr-core`), declare a DAG's nodes through
//! [`FlowBuilder`], and register the flow through [`RunnableFlow`].

pub use crate::flow_builder::FlowBuilder;
pub use crate::run_flow::RunnableFlow;

pub use dagr_core::context::RunContext;
pub use dagr_core::stable_name::{StableInputNames, StableName};
pub use dagr_core::task::Task;
pub use dagr_core::TaskError;
