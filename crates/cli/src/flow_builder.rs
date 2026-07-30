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
//! - [`source`](FlowBuilder::source) — a **root** node (no upstream), returning its
//!   output [`Handle`].
//! - [`task`](FlowBuilder::task) + [`depends_on`](NodeBuilder::depends_on) — the
//!   primary way to declare a **dependent** node, with the dependency direction
//!   explicit: `f.task("double", Double).depends_on(count)` reads as *double depends
//!   on count*. Because a [`Handle`] has no `depends_on`, edges point only backward —
//!   a cycle is unrepresentable.
//! - [`node`](FlowBuilder::node) — the equivalent **positional** form,
//!   `node(name, task, deps)`, kept as a low-level fallback.
//! - [`source_erased`](FlowBuilder::source_erased) /
//!   [`node_erased`](FlowBuilder::node_erased) — the type-erased escape hatches for a
//!   `StableName`-less task; a DAG built with them is **not graph-emittable**.
//!
//! `source` / `task` / `node` are the **graph-emittable** surface (the right default
//! where `graph` / `validate` are expected to work). Each forwards to the underlying
//! registrar and returns the **real** [`Handle<T>`], so the exact-typed
//! `D: Deps<Inputs = T::Input>` bound keeps mis-wiring a **compile error** — the
//! façade adds no runtime check and loses no compile-time guarantee.
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

    /// Begin declaring a **data-dependent node** under `name`, returning a
    /// [`NodeBuilder`] whose [`depends_on`](NodeBuilder::depends_on) names the node's
    /// upstream(s) explicitly and completes the registration.
    ///
    /// This is the primary, reads-like-English wiring surface:
    ///
    /// ```
    /// # use dagr_cli::prelude::FlowBuilder;
    /// # use dagr_cli::run_flow::RunnableFlow;
    /// # use dagr_core::{TaskError, context::RunContext, stable_name::StableName, task::Task};
    /// # #[derive(Clone)] struct Count(u64);
    /// # impl StableName for Count { const STABLE_NAME: &'static str = "Count"; }
    /// # #[derive(Clone)] struct Doubled(u64);
    /// # impl StableName for Doubled { const STABLE_NAME: &'static str = "Doubled"; }
    /// # struct CountTo { up_to: u64 }
    /// # impl StableName for CountTo { const STABLE_NAME: &'static str = "CountTo"; }
    /// # impl Task for CountTo {
    /// #     type Input = ();
    /// #     type Output = Count;
    /// #     async fn run(&mut self, _: &RunContext, _: ()) -> Result<Count, TaskError> { Ok(Count(self.up_to)) }
    /// # }
    /// # struct Double;
    /// # impl StableName for Double { const STABLE_NAME: &'static str = "Double"; }
    /// # impl Task for Double {
    /// #     type Input = Count;
    /// #     type Output = Doubled;
    /// #     async fn run(&mut self, _: &RunContext, c: Count) -> Result<Doubled, TaskError> { Ok(Doubled(c.0 * 2)) }
    /// # }
    /// # let mut flow = RunnableFlow::new();
    /// # let mut f = FlowBuilder::new(&mut flow);
    /// let count  = f.source("count", CountTo { up_to: 21 });
    /// let double = f.task("double", Double).depends_on(count); // double DEPENDS ON count
    /// # drop(double);
    /// ```
    ///
    /// A [`Handle`] has no `depends_on`, so an edge can only point **backward** — a
    /// cycle is unrepresentable, no runtime cycle check needed. The type checks (the
    /// exact-typed `Deps<Inputs = T::Input>` binding, the `StableName` bounds) all
    /// land on [`depends_on`](NodeBuilder::depends_on), which is where a mis-wiring's
    /// compile error points. It is equivalent to
    /// [`node(name, task, deps)`](Self::node) — the positional form stays a low-level
    /// fallback; `task(name, task).depends_on(deps)` is the same registration written
    /// so the dependency direction is explicit. A **source** (no upstream) uses
    /// [`source`](Self::source), not this.
    pub fn task<T>(&mut self, name: impl Into<String>, task: T) -> NodeBuilder<'_, 'a, T> {
        NodeBuilder {
            builder: self,
            name: name.into(),
            task,
        }
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

/// A **half-declared node**, awaiting its upstream(s): the value
/// [`FlowBuilder::task`] returns, completed by [`depends_on`](Self::depends_on).
///
/// It captures the node's `name` and `task` but registers **nothing** until
/// [`depends_on`](Self::depends_on) names its upstream(s) — so a node is wired
/// exactly once, and because a [`Handle`] carries no `depends_on`, edges point only
/// backward (cycles are unrepresentable). It borrows the [`FlowBuilder`] for its
/// lifetime; each `f.task(..).depends_on(..)` statement fully consumes it, releasing
/// the borrow before the next declaration.
#[must_use = "a NodeBuilder registers nothing until you call `.depends_on(..)` to name its upstream(s)"]
pub struct NodeBuilder<'b, 'a, T> {
    builder: &'b mut FlowBuilder<'a>,
    name: String,
    task: T,
}

impl<T> NodeBuilder<'_, '_, T> {
    /// Name this node's **upstream(s)** and complete the registration, returning its
    /// output [`Handle`].
    ///
    /// `deps` is the upstream [`Handle`] (or, for a fan-in, a tuple of handles) whose
    /// value types must **exactly match** the task's `Input` — the compile-time
    /// `D: Deps<Inputs = T::Input>` check, so a wrong-typed or wrong-arity upstream is
    /// a **compile error** here, never a runtime surprise:
    ///
    /// ```
    /// # use dagr_cli::prelude::FlowBuilder;
    /// # use dagr_cli::run_flow::RunnableFlow;
    /// # use dagr_core::{TaskError, context::RunContext, stable_name::StableName, task::Task};
    /// # #[derive(Clone)] struct Count(u64);
    /// # impl StableName for Count { const STABLE_NAME: &'static str = "Count"; }
    /// # #[derive(Clone)] struct Doubled(u64);
    /// # impl StableName for Doubled { const STABLE_NAME: &'static str = "Doubled"; }
    /// # struct CountTo { up_to: u64 }
    /// # impl StableName for CountTo { const STABLE_NAME: &'static str = "CountTo"; }
    /// # impl Task for CountTo {
    /// #     type Input = ();
    /// #     type Output = Count;
    /// #     async fn run(&mut self, _: &RunContext, _: ()) -> Result<Count, TaskError> { Ok(Count(self.up_to)) }
    /// # }
    /// # struct Double;
    /// # impl StableName for Double { const STABLE_NAME: &'static str = "Double"; }
    /// # impl Task for Double {
    /// #     type Input = Count;
    /// #     type Output = Doubled;
    /// #     async fn run(&mut self, _: &RunContext, c: Count) -> Result<Doubled, TaskError> { Ok(Doubled(c.0 * 2)) }
    /// # }
    /// # let mut flow = RunnableFlow::new();
    /// # let mut f = FlowBuilder::new(&mut flow);
    /// let count  = f.source("count", CountTo { up_to: 21 });
    /// let double = f.task("double", Double).depends_on(count); // one upstream
    /// # drop(double);
    /// ```
    ///
    /// A fan-in names its upstreams as a **tuple**, and the tuple's value types
    /// must match the task's `Input` tuple exactly:
    /// `f.task("join", Join).depends_on((left, right))`.
    ///
    /// Equivalent to [`FlowBuilder::node(name, task, deps)`](FlowBuilder::node); this
    /// spelling makes the dependency direction explicit. For a `StableName`-less task
    /// use [`FlowBuilder::node_erased`](FlowBuilder::node_erased) (not graph-emittable).
    #[must_use]
    pub fn depends_on<D>(self, deps: D) -> Handle<T::Output>
    where
        T: Task + StableName + Send + 'static,
        T::Input: StableInputNames + Clone + Send + 'static,
        T::Output: StableName + Send + Sync + 'static,
        D: Deps<Inputs = T::Input> + InputWiring + Clone,
    {
        self.builder.node(self.name, self.task, deps)
    }
}
