//! `dagr-core` — the dagr execution core (the live-pipeline surface).
//!
//! This crate holds dagr's authoring surface and execution core: the task
//! abstraction, typed handles, dependency binding, flow assembly, and the
//! run-loop machinery — the code that *is* a running pipeline. It is the crate
//! whose dependency set is kept minimal and review-gated (arch.md "Stability"),
//! and it is the "live pipeline" surface that renderers must never reach into
//! (arch.md C24 · Renderers).
//!
//! # What lives here so far
//!
//! The **C1 task abstraction** (ticket T9): the atomic unit of work.
//!
//! - [`task::Task`] — the trait an author implements on a configuration-holding
//!   `struct`, declaring C1's four elements: the consumed input type, the
//!   produced output type, the execution class ([`task::ExecutionClass`],
//!   default await-bound), and the `&mut self` work over a [`task::RunContext`]
//!   reference.
//! - [`TaskError`] — the task-facing classified error, three-valued
//!   (retry-eligible / permanent / deliberate skip) per the T3 ADR.
//!
//! The typed handle (C2 / T10), dependency binding (C3 / T11), flow builder
//! (C7 / T13), assembly (T14), the run context's real capabilities (C8 / T16),
//! and the M1+ execution tickets land later; this crate grows one component at a
//! time.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod error;
pub mod task;

pub use error::{TaskError, TaskErrorClass};
pub use task::{ExecutionClass, RunContext, Task};
