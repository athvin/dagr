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
//! The **C2 typed handle** (ticket T10): a typed claim on a value that does not
//! exist yet.
//!
//! - [`handle::Handle<T>`] — the cheap, freely copyable handle a node
//!   registration returns; it carries the node's [identity](handle::NodeId) plus
//!   the value type the node will produce, and it is the *only* way to refer to
//!   another node's output. It has no public constructor and no lookup by
//!   name/index/string key — a handle is obtained solely by registering a node.
//! - [`handle::NodeId`] — the opaque, name-derived identity token a handle
//!   carries (identity comes from the registration name, never from order).
//!
//! The **C3 typed data-dependency binding** (ticket T11): how a downstream node
//! declares its data dependencies at registration.
//!
//! - [`binding::Deps`] — the sealed positional encoding that binds one or more
//!   already-registered [`Handle`]s to a task, exact value-type and arity
//!   matching enforced at compile time (single input is a bare `Handle<T>`,
//!   multi-input a tuple up to [`binding::MAX_INPUT_ARITY`]).
//! - [`binding::ReceiveMode`] / [`binding::DataEdge`] — the per-edge receive mode
//!   (owned / shared / clone-on-read) recorded verbatim and left un-adjudicated
//!   (mode conflicts are assembly's job, C7 / T14).
//! - [`binding::NodeBinding`] — the trigger-rule typestate that makes any rule
//!   other than `all-succeeded` inexpressible on a data-dependent node.
//!
//! The **C8 run context** (ticket T16): what every task invocation is told about
//! the run it is part of.
//!
//! - [`context::RunContext`] — the read-only, hand-constructable handle passed
//!   into [`task::Task::run`], carrying run/pipeline/node identity, the current
//!   attempt and the maximum, opaque parameters, an optional
//!   [`context::DataInterval`], an observe-only [`context::CancellationSignal`], a
//!   [`context::LogSpan`], and the [`context::ResourceRegistry`] /
//!   [`context::ScratchStore`] accessor seams (registry substance is T30, scratch
//!   is T53). Built by hand with [`context::RunContext::builder`] /
//!   [`context::RunContext::for_test`] — no runtime, store, registry, clock, or
//!   network — which feeds the single-task test kit (C28 / T60).
//! - [`context::ResourceRequirements`] — the resource-requirement *declaration*
//!   plumbing a node records at registration, queryable for bootstrap validation
//!   (T30) and later graph-artifact rendering (C20).
//! - [`context::CoveredNodeStates`] / [`context::TerminalState`] — the
//!   teardown-only view of covered nodes' terminal states (C17), from arch.md's
//!   normative taxonomy; the runtime-side population is C17 / T52.
//!
//! The **C7 flow builder and node identity** (ticket T13): accumulating node
//! registrations into an immutable pipeline.
//!
//! - [`flow::Flow`] — the builder that accepts node registrations (each carrying
//!   an explicit caller-supplied name), hands back the typed [`Handle`] from T10,
//!   and finalizes into an immutable [`flow::Pipeline`]. Node identity is derived
//!   **solely** from the registration name — never from order — so renaming a
//!   node changes its identity while reordering registrations changes nothing.
//! - [`flow::Pipeline`] — the immutable, read-only pipeline finalization yields;
//!   once produced, no registration or mutation is possible (a compile-time
//!   fact). Its node set is keyed by identity name, so lookup and content
//!   comparison are order-insensitive.
//! - [`flow::PipelineNode`] — a node's preserved record: its identity, its
//!   handle linkage, its recorded data edges and trigger rule, and the
//!   group-label slot (C6 / T51) carried alongside identity but **excluded** from
//!   it.
//!
//! Assembly *validation and precomputation* (C7 / T14) — duplicate-name
//! reporting, the empty-pipeline check, class-override validation, the
//! zero-consumer warning, consumer/dependency counts, execution order, and the
//! fingerprint — and the M1+ execution tickets land later; this crate grows one
//! component at a time.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod binding;
pub mod context;
pub mod error;
pub mod flow;
pub mod handle;
pub mod task;

pub use binding::{
    BoundInput, CloneOnRead, DataEdge, Deps, EdgeKind, NodeBinding, ReceiveMode, RegisteredNode,
    Shared, TriggerRule, MAX_INPUT_ARITY,
};
pub use context::{
    CancellationSignal, CancellationSource, CoveredNodeStates, DataInterval, LogSpan, PipelineId,
    ResourceRegistry, ResourceRequirement, ResourceRequirements, RunContext, RunContextBuilder,
    RunId, ScratchError, ScratchStore, TerminalState,
};
pub use error::{TaskError, TaskErrorClass};
pub use flow::{Flow, Pipeline, PipelineNode};
pub use handle::{Handle, NodeId};
pub use task::{ExecutionClass, Task};
