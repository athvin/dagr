//! The **`FlowBuilder`** declaration façade — a minimal, graph-emittable-by-default
//! *declaration* surface over [`RunnableFlow`].
//!
//! # Why this type exists
//!
//! A flow can be declared directly against [`RunnableFlow`]
//! ([`crate::run_flow`]): its `register_source_named` / `register_named` pair are
//! the **graph-emittable** registrars (they carry [`StableName`] bounds so
//! `graph <flow>` / `validate <flow>` record author-declared stable names), and
//! `register_source` / `register` are the type-erased ones (a flow built with them
//! is *not* graph-emittable). That surface also carries the **execution** seam —
//! [`run`](RunnableFlow::run) and [`into_pipeline`](RunnableFlow::into_pipeline),
//! both of which **consume** the flow.
//!
//! `FlowBuilder` is a thin newtype over `&mut RunnableFlow` that hands a DAG author
//! exactly the *declaration* verbs and nothing else:
//!
//! - [`source`](FlowBuilder::source) / [`node`](FlowBuilder::node) — the
//!   graph-emittable pair (the right default for a framework where `graph` /
//!   `validate` are expected to work), forwarding to `register_source_named` /
//!   `register_named`.
//! - [`source_erased`](FlowBuilder::source_erased) /
//!   [`node_erased`](FlowBuilder::node_erased) — the type-erased escape hatches,
//!   forwarding to `register_source` / `register`; a DAG built with them is **not
//!   graph-emittable**.
//!
//! Each method forwards to the underlying registrar and returns the **real**
//! [`Handle<T>`], so the existing `D: Deps<Inputs = T::Input>` bound keeps
//! mis-wiring a **compile error** — the façade adds no runtime check and loses no
//! compile-time guarantee.
//!
//! # Why a newtype, not a type alias
//!
//! Making `FlowBuilder` a newtype (rather than an alias for `RunnableFlow`) is
//! deliberate: an alias would expose the *full* `RunnableFlow` surface — including
//! the consuming `run` / `into_pipeline` — inside a DAG body, letting a declaration
//! consume itself. The newtype exposes exactly the four declaration methods and no
//! consuming/execution method at all.

use dagr_core::binding::Deps;
use dagr_core::handle::Handle;
use dagr_core::stable_name::{StableInputNames, StableName};
use dagr_core::task::Task;

use crate::run_flow::{InputWiring, RunnableFlow};

/// A curated **declaration façade** over a [`RunnableFlow`]: a thin newtype that
/// hands a DAG author the *declaration* verbs ([`source`](Self::source) /
/// [`node`](Self::node), graph-emittable; [`source_erased`](Self::source_erased) /
/// [`node_erased`](Self::node_erased), type-erased) and **no** consuming/execution
/// method.
///
/// It borrows the flow mutably for its lifetime, so the author declares nodes
/// against it and the caller (the `#[dag]` factory in a later ticket, or a hand-
/// written builder) retains the [`RunnableFlow`] to finish or run. Because every
/// method forwards to the real registrar and returns the true [`Handle<T>`],
/// wiring stays exact-typed: a `Handle<T>` bound where a different `Input` is
/// declared is a compile error, exactly as on `RunnableFlow`.
pub struct FlowBuilder<'a>(&'a mut RunnableFlow);

impl<'a> FlowBuilder<'a> {
    /// Wrap a mutable borrow of a [`RunnableFlow`] in the declaration façade.
    ///
    /// This is the constructor the `#[dag]` factory (a later ticket) uses to hand a
    /// DAG-builder fn a `&mut FlowBuilder`; a hand-written builder can use it too.
    /// The borrow lasts the façade's lifetime, after which the caller finishes or
    /// runs the underlying flow.
    #[must_use]
    pub fn new(flow: &'a mut RunnableFlow) -> Self {
        Self(flow)
    }

    /// Declare a **graph-emittable source** node (one whose task consumes nothing)
    /// under `name`, returning its output [`Handle`].
    ///
    /// Forwards to [`RunnableFlow::register_source_named`], so the built pipeline
    /// records `T`'s and `T::Output`'s author-declared stable names — a DAG declared
    /// through `source` is emittable to the graph artifact (`graph <flow>` /
    /// `validate <flow>` work over it). For a `StableName`-less source use
    /// [`source_erased`](Self::source_erased) instead (not graph-emittable).
    #[must_use]
    pub fn source<T>(&mut self, name: impl Into<String>, task: T) -> Handle<T::Output>
    where
        T: Task<Input = ()> + StableName + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
    {
        self.0.register_source_named::<T>(name, task)
    }

    /// Declare a **graph-emittable data-dependent** node under `name`, binding `deps`
    /// (whose value types must **exactly match** `T::Input` — the compile-time
    /// `D: Deps<Inputs = T::Input>` check), returning its output [`Handle`].
    ///
    /// Forwards to [`RunnableFlow::register_named`], so the built pipeline records
    /// the stable task name, the ordered stable input type names, and the stable
    /// output type name — a DAG declared through `node` is emittable to the graph
    /// artifact. For a `StableName`-less node use [`node_erased`](Self::node_erased)
    /// instead (not graph-emittable). Mis-wiring (a `Handle` of the wrong type) is a
    /// **compile error** here, exactly as on `RunnableFlow`.
    #[must_use]
    pub fn node<T, D>(&mut self, name: impl Into<String>, task: T, deps: D) -> Handle<T::Output>
    where
        T: Task + StableName + Send + 'static,
        T::Input: StableInputNames + Clone + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.0.register_named::<T, D>(name, task, deps)
    }

    /// Declare a **type-erased source** node under `name`, returning its output
    /// [`Handle`].
    ///
    /// The escape hatch for a task **without** [`StableName`]: it forwards to the
    /// type-erased [`RunnableFlow::register_source`], so the node carries **no**
    /// author-declared stable names and a DAG built with it is **not
    /// graph-emittable** (`graph <flow>` / `validate <flow>` cannot emit it). Prefer
    /// [`source`](Self::source) for the graph-emittable default; reach for this only
    /// when the task genuinely cannot implement `StableName`.
    #[must_use]
    pub fn source_erased<T>(&mut self, name: impl Into<String>, task: T) -> Handle<T::Output>
    where
        T: Task<Input = ()> + Send + 'static,
        T::Output: Send + Sync + 'static,
    {
        self.0.register_source::<T>(name, task)
    }

    /// Declare a **type-erased data-dependent** node under `name`, binding `deps`,
    /// returning its output [`Handle`].
    ///
    /// The escape hatch for a task **without** [`StableName`]: it forwards to the
    /// type-erased [`RunnableFlow::register`], so a DAG built with it is **not
    /// graph-emittable**. The dependency binding is unchanged — the exact-type
    /// `D: Deps<Inputs = T::Input>` check still makes mis-wiring a compile error and
    /// the returned [`Handle`] is the real one. Prefer [`node`](Self::node) for the
    /// graph-emittable default; reach for this only when the task genuinely cannot
    /// implement `StableName`.
    #[must_use]
    pub fn node_erased<T, D>(
        &mut self,
        name: impl Into<String>,
        task: T,
        deps: D,
    ) -> Handle<T::Output>
    where
        T: Task + Send + 'static,
        T::Input: Clone + Send + 'static,
        T::Output: Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.0.register::<T, D>(name, task, deps)
    }
}
