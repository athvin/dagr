//! The C1 task abstraction — the atomic unit of work.
//!
//! A [`Task`] declares **exactly four things** (arch.md `### C1 · Task`):
//!
//! 1. the type of value it **consumes** — [`Task::Input`],
//! 2. the type of value it **produces** — [`Task::Output`],
//! 3. its **execution class** — [`Task::EXECUTION_CLASS`] (default
//!    [`ExecutionClass::AwaitBound`]),
//! 4. the **work** itself — [`Task::run`].
//!
//! The input and output types are readable from the declaration alone, without
//! reading the work body: they are associated types on the trait
//! implementation. The work receives a [`RunContext`] reference and the declared
//! input, and returns either the produced value or a classified [`TaskError`].
//!
//! # A task is a *value*, not a bare function
//!
//! A task is a `struct` holding configuration captured when it was constructed —
//! a threshold, a target location, a rule set. Registration (C2 / T10, a later
//! ticket) moves the task value into the flow; "the same task type appearing
//! several times" means constructing several values, each registered as its own
//! node. Constructing one value affects no other.
//!
//! ```
//! use dagr_core::task::{RunContext, Task};
//! use dagr_core::TaskError;
//!
//! /// A task holding constructor-captured configuration (a threshold).
//! struct ThresholdGate {
//!     threshold: u32,
//! }
//!
//! impl Task for ThresholdGate {
//!     type Input = u32;
//!     type Output = bool;
//!
//!     async fn run(
//!         &mut self,
//!         _ctx: &RunContext,
//!         input: Self::Input,
//!     ) -> Result<Self::Output, TaskError> {
//!         Ok(input >= self.threshold)
//!     }
//! }
//! ```
//!
//! # The bounds — enforced by the type system
//!
//! - **Task values are `Send + 'static`.** A task must be sendable to another
//!   worker thread and free of borrowed data; the work takes the task
//!   **exclusively** (`&mut self`), which is what makes sequential attempts safe
//!   without any synchronization written by the author (attempt *n+1* never
//!   overlaps attempt *n*).
//! - **Output values are `Send + Sync + 'static`.** An output lives in a shared
//!   slot (C10) that concurrent consumers read, so it must additionally be
//!   `Sync`.
//!
//! These bounds are supertrait / associated-type bounds on [`Task`], so
//! violating either is a **compile error**.
//!
//! ## The most common first-hour error: capturing a non-`Send` value
//!
//! The single most common first-hour mistake is capturing a non-`Send` value —
//! an [`Rc`](std::rc::Rc), a `RefCell` of non-`Send` data, a raw pointer, a
//! `MutexGuard` — in a task. Because task values are `Send`, the compiler
//! catches it at registration:
//!
//! ```compile_fail
//! use std::rc::Rc;
//! use dagr_core::task::{RunContext, Task};
//! use dagr_core::TaskError;
//!
//! struct CapturesRc {
//!     // `Rc<u32>` is NOT `Send`, so `CapturesRc` is not `Send` — and a task
//!     // value must be `Send + 'static`. This does not compile.
//!     shared: Rc<u32>,
//! }
//!
//! impl Task for CapturesRc {
//!     type Input = ();
//!     type Output = u32;
//!
//!     async fn run(
//!         &mut self,
//!         _ctx: &RunContext,
//!         _input: Self::Input,
//!     ) -> Result<Self::Output, TaskError> {
//!         Ok(*self.shared)
//!     }
//! }
//!
//! fn assert_task<T: Task + Send + 'static>() {}
//! // `CapturesRc` is not `Send`, so this line fails to compile.
//! assert_task::<CapturesRc>();
//! ```
//!
//! **The fix** (cookbook, arch.md "Documentation"): construct the non-`Send`
//! value *inside* the task's work rather than capturing it, or place a genuinely
//! shared, thread-safe resource in the C9 resource registry (which holds
//! `Send + Sync` clients) rather than in the task value.
//!
//! # What lives elsewhere
//!
//! This ticket delivers only the *declaration surface* an author writes against.
//! Nothing here schedules, retries, times out, permits, or logs:
//!
//! - The **receive mode** (owned vs shared-read vs clone-on-read) of an input is
//!   part of the task's signature but its whole-graph delivery and mode-conflict
//!   checking are C3 / assembly (T11 / T14); this ticket enforces only the
//!   type-level bounds.
//! - The **run context**'s capabilities are C8 / T16 — [`RunContext`] is
//!   re-exported here from [`crate::context`]; T9 fixed only that the work
//!   receives a `&RunContext`, and T16 fleshed the type out (identities, attempt,
//!   parameters, data interval, cancellation, span, registry/scratch seams)
//!   without changing this signature.
//! - The **attempt runner**, retries, timeouts, and the runner's richer outcome
//!   taxonomy (timeout, panic) are C14 (T20+); this ticket guarantees only the
//!   *shape* (`&mut self`) that makes sequential re-runs safe.
//! - The **durable-output reference contract** (C5 / C27) is left as an
//!   unimplemented seam: nothing here forecloses a later durable marker adding
//!   its bound to a node's output type.

use std::future::Future;

use crate::error::TaskError;

// The **run context** every task invocation receives (arch.md C8). Its full
// field set and capabilities live in [`crate::context`] (C8 / T16); it is
// re-exported here so `dagr_core::task::RunContext` — the path T9's tests and the
// task signature use — continues to resolve. T9 fixed only that the work receives
// a `&RunContext`; T16 fleshed the type out without changing [`Task::run`].
pub use crate::context::RunContext;

/// The execution class a task declares — the fourth of C1's four declared
/// elements. It says *which kind of thread* the work belongs on (C13); the
/// task declares it, and node policy may later override it within limits (C5).
///
/// The default is [`AwaitBound`](ExecutionClass::AwaitBound): a task that states
/// no class is await-bound. This ticket declares and carries the class only —
/// **dispatch** onto the await / blocking / compute pools is C13 / T33.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionClass {
    /// Await-bound work — network calls, waiting — runs on the async runtime.
    /// This is the **default** when a task states no class.
    AwaitBound,
    /// Blocking work — a synchronous database call — runs on a dedicated pool so
    /// it cannot starve the async runtime.
    Blocking,
    /// Compute-bound work runs on a fixed pool sized to the container's CPU
    /// allocation.
    Compute,
}

impl Default for ExecutionClass {
    /// The default execution class is [`AwaitBound`](ExecutionClass::AwaitBound)
    /// (arch.md C1: *"default await-bound"*).
    fn default() -> Self {
        Self::AwaitBound
    }
}

/// The atomic unit of work (arch.md `### C1 · Task`).
///
/// Implement `Task` on a `struct` that holds the task's constructor-captured
/// configuration. The implementation declares the four elements of C1: the
/// consumed [`Input`](Task::Input) type, the produced [`Output`](Task::Output)
/// type, the [`EXECUTION_CLASS`](Task::EXECUTION_CLASS) (defaulting to
/// await-bound), and the [`run`](Task::run) work. See the [module
/// docs](self) for the bounds, the worked first-hour error, and what lives
/// elsewhere.
///
/// # Bounds
///
/// - `Self: Send + 'static` — the task value is sendable to a worker thread and
///   free of borrowed data.
/// - [`Output`](Task::Output)`: Send + Sync + 'static` — the produced value
///   lives in a shared slot (C10) that concurrent consumers read.
///
/// The work takes the task **exclusively** (`&mut self`): the author writes no
/// synchronization, and the type system rejects invoking the work through a
/// shared reference.
pub trait Task: Send + 'static {
    /// The type of value the task **consumes**. A task that consumes nothing
    /// declares `type Input = ()` — the no-input case, which needs no
    /// placeholder dance. Readable from the declaration without reading the body.
    type Input;

    /// The type of value the task **produces**. Bounded `Send + Sync + 'static`
    /// so the produced value can live in a shared output slot (C10) that
    /// concurrent consumers read. Readable from the declaration without reading
    /// the body.
    ///
    /// A node marked durable (C5 / C27, a later ticket) additionally requires its
    /// output type to implement the durable-reference contract; that bound is a
    /// seam left open here, not foreclosed.
    type Output: Send + Sync + 'static;

    /// The task's **execution class** — the fourth declared element. Defaults to
    /// [`ExecutionClass::AwaitBound`] when a task states no class; a task states
    /// a different class by overriding this associated constant (e.g.
    /// `const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;`).
    ///
    /// This ticket declares and carries the class only; dispatch onto pools is
    /// C13 / T33.
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::AwaitBound;

    /// The **work** — the task's business logic, and nothing else.
    ///
    /// It takes the task **exclusively** (`&mut self`), a [`RunContext`]
    /// reference, and the declared [`Input`](Task::Input); it returns either the
    /// produced [`Output`](Task::Output) or a classified [`TaskError`]. The
    /// returned future is `Send` so the runner can drive it on a worker thread.
    ///
    /// A task body contains business logic only — never scheduling, retry,
    /// permit, timeout, or logging code. Removing the framework's retry, timeout,
    /// and logging features would require no change to any task body. The unit is
    /// therefore safely re-runnable in shape, which is exactly what a retry (C14,
    /// a later ticket) relies on.
    fn run(
        &mut self,
        ctx: &RunContext,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, TaskError>> + Send;
}
