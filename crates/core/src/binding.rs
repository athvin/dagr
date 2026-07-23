//! The C3 typed data-dependency binding — declaring that one task consumes
//! another's output (arch.md `### C3 · Data dependency`).
//!
//! A data dependency is declared at a downstream node's registration by
//! **binding one or more already-registered upstream [`Handle`]s** whose value
//! types must **exactly match** the consuming task's declared input types. This
//! module delivers the binding vocabulary layered on the C2 typed handles
//! ([`crate::handle`], T10) and the C1 task abstraction ([`crate::task`], T9):
//! the sealed positional [`Deps`] encoding, the per-edge receive-mode recording,
//! and the trigger-rule typestate. It implements the encoding **exactly** as the
//! T5 design-spike ADR
//! (`docs/implementation/018-T5-typed-handle-encoding-spike.md`) fixed it.
//!
//! # The split that defines C3
//!
//! **Value-type and arity mismatches are compile errors here.** Receive-*mode*
//! conflicts are **not** — they are whole-graph facts (consumer counts, retry
//! policy) that only exist once every registration is in, so they are left to
//! assembly (C7 / T14). C3 *records* the declared mode on each edge and never
//! adjudicates it (T0.2 output-ownership ADR). This module raises no error about
//! consumer counts or retries.
//!
//! # Exact type + arity matching, and the ceiling
//!
//! Binding is encoded by a **sealed positional [`Deps`] trait** that maps a
//! handle tuple to the task's declared input tuple, so **count, order, and
//! types are all compile-checked at once** (T5 ADR §3). A **single-input** task
//! binds a **bare [`Handle<T>`](Handle)** — never a one-tuple `(T,)` (T5 ADR §5);
//! multi-input tasks bind a **tuple** of handles. Tuple arities from **2 through
//! the documented ceiling of [8](MAX_INPUT_ARITY)** are supported (T5 ADR §4);
//! binding more than that hits a single curated
//! [`#[diagnostic::on_unimplemented]`][Deps] message directing the author to
//! aggregate the upstream values into a struct produced by an intermediate node,
//! rather than a wall of trait errors.
//!
//! # Fan-out
//!
//! Because [`Handle<T>`](Handle) is [`Copy`], one producer handle fans out to
//! **any number** of downstream tasks by reuse; binding never moves or
//! invalidates the handle (T5 ADR §7). The assembled structure records one
//! distinct [`DataEdge`] per consumer.
//!
//! # Worked example — a two-input binding
//!
//! A downstream task declaring two inputs binds a **tuple** of two upstream
//! handles, in declaration order. The maximum input arity is
//! [`MAX_INPUT_ARITY`] (**8**); beyond it, aggregate the upstream values into a
//! struct produced by an intermediate node and depend on that one handle.
//!
//! ```
//! use dagr_core::binding::{NodeBinding, EdgeKind, MAX_INPUT_ARITY};
//! use dagr_core::handle::Handle;
//! use dagr_core::task::{RunContext, Task};
//! use dagr_core::TaskError;
//!
//! struct Rows;
//! struct Schema;
//! struct Report;
//!
//! // Upstream producers (sourceless): each registration mints a typed handle.
//! struct MakeRows;
//! impl Task for MakeRows {
//!     type Input = ();
//!     type Output = Rows;
//!     async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> { Ok(Rows) }
//! }
//! struct MakeSchema;
//! impl Task for MakeSchema {
//!     type Input = ();
//!     type Output = Schema;
//!     async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> { Ok(Schema) }
//! }
//!
//! // A downstream task consuming EXACTLY two inputs, in order: (Rows, Schema).
//! struct BuildReport;
//! impl Task for BuildReport {
//!     type Input = (Rows, Schema);
//!     type Output = Report;
//!     async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
//!         Ok(Report)
//!     }
//! }
//!
//! // Register the upstreams, obtaining their handles.
//! let rows: Handle<Rows> = NodeBinding::consuming_nothing("rows").finish::<Rows>();
//! let schema: Handle<Schema> = NodeBinding::consuming_nothing("schema").finish::<Schema>();
//!
//! // Bind the two handles as a tuple, in the task's declared input order.
//! let (report, node) = NodeBinding::consuming_nothing("report")
//!     .depends_on::<BuildReport, _>(&BuildReport, (rows, schema))
//!     .finish::<Report>();
//!
//! // Two data edges are recorded, preserving input order.
//! assert_eq!(node.data_edges().len(), 2);
//! assert_eq!(node.data_edges()[0].upstream(), rows.id());
//! assert_eq!(node.data_edges()[1].upstream(), schema.id());
//! assert_eq!(node.data_edges()[0].kind(), EdgeKind::Data);
//!
//! // The ceiling is documented at the point of use.
//! assert_eq!(MAX_INPUT_ARITY, 8);
//! let _ = report; // the report handle can be bound downstream in turn
//! ```
//!
//! # Trigger-rule typestate
//!
//! A node that has bound any data dependency **cannot** be given a trigger rule
//! other than `all-succeeded` — the [`NodeBinding`] typestate makes it
//! *inexpressible* (a compile error, not a runtime check): [`trigger_rule`] is
//! offered only in the [`ConsumesNothing`] state, and binding a dependency
//! transitions to [`ConsumesData`], which offers no such method (arch.md §126,
//! §52; T5 ADR §8).
//!
//! [`trigger_rule`]: NodeBinding::trigger_rule
//!
//! # What lives elsewhere
//!
//! - **Receive-mode conflict adjudication** (owned demand on a multi-consumer
//!   value; owned edge into a retrying node without clone-on-read; naming both
//!   consumers) is **assembly's** job — C7 / T14. C3 records the mode; it never
//!   rejects.
//! - **Ordering (no-data) edges** and the `all-terminal` rule for ordering-only
//!   nodes are **C4 / T50**; this module records data edges distinctly (via
//!   [`EdgeKind`]) so T50 can slot ordering edges in.
//! - **The flow builder, duplicate-name checking, and the immutable pipeline**
//!   are **C7 / T13**; [`NodeBinding`] is the binding-focused registration seam
//!   T13 wraps, not the whole-flow accumulator.
//! - **Consumer counts, execution order, and the fingerprint** are precomputed
//!   at assembly — **T14**.

use std::marker::PhantomData;

use crate::handle::{Handle, NodeId};
use crate::task::Task;

/// The maximum number of inputs a task may bind as a tuple (arch.md §121, §128;
/// T5 ADR §4). Binding more than this hits the curated [`Deps`]
/// `on_unimplemented` diagnostic; the remedy is to **aggregate the upstream
/// values into a struct produced by an intermediate node** and depend on that
/// one handle.
///
/// The ceiling is **8**: it covers every realistic fan-in without an unwieldy
/// macro expansion, and the struct-aggregation escape hatch handles anything
/// larger. This constant is the documented ceiling, stated at the point of use.
pub const MAX_INPUT_ARITY: usize = 8;

/// How a bound edge delivers its upstream value to the consuming attempt — the
/// **declared receive mode**, recorded verbatim and **not adjudicated** here
/// (T0.2 output-ownership ADR; arch.md §81, §119).
///
/// The three modes are the whole T0.2 model: [`Owned`](ReceiveMode::Owned)
/// (sole-consumer-owns), [`Shared`](ReceiveMode::Shared)
/// (multi-consumer-shared-read), and [`CloneOnRead`](ReceiveMode::CloneOnRead)
/// (the per-edge opt-in giving each attempt a fresh clone). C3 records which one
/// the author declared; whether that declaration is *legal* given whole-graph
/// facts (consumer counts, retry policy) is an **assembly** check (C7 / T14),
/// never made here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReceiveMode {
    /// The consumer takes **ownership** of the value (sole-consumer-owns). The
    /// default for a bare handle binding. Whether the value actually has a
    /// single consumer is a whole-graph fact adjudicated at assembly (T14), not
    /// here — C3 records the demand, it does not reject it.
    Owned,
    /// The consumer receives **shared read access** for the duration of its
    /// attempt (multi-consumer-shared-read). Stated per-edge via
    /// [`Handle::shared`].
    Shared,
    /// The edge opts into **clone-on-read**: each attempt receives a fresh clone
    /// of the value (requiring the value's type to be cloneable, with the memory
    /// multiplication that implies — enforced at assembly, T14). Stated per-edge
    /// via [`Handle::clone_on_read`].
    CloneOnRead,
}

/// The kind of a recorded dependency edge — kept distinct so ordering edges
/// (C4 / T50) can be recorded separately from data edges (arch.md §143).
///
/// This ticket records only [`Data`](EdgeKind::Data) edges. The enum is
/// `#[non_exhaustive]` so T50 can add an `Ordering` variant without a breaking
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EdgeKind {
    /// A **data** dependency: the downstream consumes the upstream's output
    /// value. Implies both ordering *and* that the upstream must have succeeded
    /// (if it didn't, there is no value — arch.md §119).
    Data,
}

/// A recorded data-dependency edge from a downstream node to one upstream node
/// (arch.md `### C3 · Data dependency`).
///
/// A data edge implies **both ordering and upstream success**: the downstream's
/// input cannot be formed unless the upstream succeeded and produced a value
/// (arch.md §119). It is recorded distinctly from an ordering-only edge (T50) so
/// that readiness (C11) and slot wiring (C10) can rely on that invariant. The
/// edge carries the upstream's [identity](DataEdge::upstream), the consumer's
/// [input position](DataEdge::position), and the declared
/// [receive mode](DataEdge::mode) — recorded verbatim, never adjudicated (T14
/// owns mode conflicts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataEdge {
    upstream: NodeId,
    position: usize,
    mode: ReceiveMode,
}

impl DataEdge {
    /// The identity of the upstream node this edge depends on.
    #[must_use]
    pub fn upstream(&self) -> NodeId {
        self.upstream
    }

    /// The consumer's input position this edge fills (0-based, in binding order).
    #[must_use]
    pub fn position(&self) -> usize {
        self.position
    }

    /// The declared [receive mode](ReceiveMode) of this edge — recorded verbatim,
    /// not adjudicated (T14).
    #[must_use]
    pub fn mode(&self) -> ReceiveMode {
        self.mode
    }

    /// The [kind](EdgeKind) of this edge. Always [`EdgeKind::Data`] for a
    /// data-dependency edge; recorded distinctly from ordering edges (T50).
    #[must_use]
    pub fn kind(&self) -> EdgeKind {
        EdgeKind::Data
    }

    /// A data edge implies **upstream success**: there is no value to consume
    /// unless the upstream succeeded (arch.md §119). Always `true` for a data
    /// edge — this is intrinsic to the kind, the invariant readiness (C11) and
    /// slot wiring (C10) later rely on.
    #[must_use]
    pub fn implies_success(&self) -> bool {
        true
    }

    /// A data edge implies **ordering**: the downstream runs after the upstream.
    /// Always `true` for a data edge (arch.md §119).
    #[must_use]
    pub fn implies_ordering(&self) -> bool {
        true
    }
}

/// The closed, normative trigger-rule set (arch.md Vocabulary). Data-dependent
/// nodes are restricted to [`AllSucceeded`](TriggerRule::AllSucceeded) at compile
/// time via the [`NodeBinding`] typestate; a consume-nothing node may set any of
/// these (C4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerRule {
    /// Default: fires when every upstream is success-like (arch.md Vocabulary).
    /// The **only** rule expressible on a data-dependent node.
    AllSucceeded,
    /// Fires when every upstream is terminal, regardless of class; never
    /// propagates failure. Only settable on a node that consumes nothing (C4).
    AllTerminal,
    /// Fires when every upstream is terminal and at least one is failure-like.
    /// Only settable on a node that consumes nothing (C4).
    AnyFailed,
}

impl Default for TriggerRule {
    /// The default trigger rule is [`AllSucceeded`](TriggerRule::AllSucceeded)
    /// (arch.md Vocabulary).
    fn default() -> Self {
        Self::AllSucceeded
    }
}

/// One bound input position — a handle plus its declared [`ReceiveMode`].
///
/// Sealed (crate-private supertrait): the only types that implement it are a
/// bare [`Handle<T>`](Handle) (mode [`Owned`](ReceiveMode::Owned)), a
/// [`Shared<T>`] wrapper ([`Shared`](ReceiveMode::Shared)), and a
/// [`CloneOnRead<T>`] wrapper ([`CloneOnRead`](ReceiveMode::CloneOnRead)). This
/// is what lets each position in a [`Deps`] tuple carry its own receive mode
/// while the [`Value`](BoundInput::Value) type drives the exact-match check.
pub trait BoundInput: sealed::Sealed {
    /// The value type this input delivers — matched against the task's declared
    /// input type by [`Deps`].
    type Value;
    /// The upstream identity plus the declared receive mode. Crate-internal.
    #[doc(hidden)]
    fn resolve(self) -> (NodeId, ReceiveMode);
}

/// A handle bound with the explicit **shared-read** receive mode
/// ([`ReceiveMode::Shared`]). Obtain one with [`Handle::shared`]. `Copy` (like
/// the handle it wraps), so a shared-read binding still fans out freely.
pub struct Shared<T>(Handle<T>);

/// A handle bound with the explicit per-edge **clone-on-read** opt-in
/// ([`ReceiveMode::CloneOnRead`]). Obtain one with [`Handle::clone_on_read`]. The
/// value type's `Clone` requirement is an **assembly** check (T14), not part of
/// this compile-time type match. `Copy` (like the handle it wraps).
pub struct CloneOnRead<T>(Handle<T>);

// Manual `Clone`/`Copy` (not `#[derive]`): a derive would emit `impl<T: Clone>`,
// wrongly requiring `T: Clone`. `Handle<T>` is `Copy` for every `T`, so these
// wrappers are too — a clone-on-read/shared binding fans out just like a bare
// handle.
impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Shared<T> {}
impl<T> Clone for CloneOnRead<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for CloneOnRead<T> {}

impl<T> Handle<T> {
    /// Bind this handle with the explicit **shared-read** receive mode — the
    /// consumer receives shared read access for the duration of its attempt
    /// (T0.2 multi-consumer-shared-read). Recorded verbatim on the edge; not
    /// adjudicated here (T14).
    #[must_use]
    pub fn shared(self) -> Shared<T> {
        Shared(self)
    }

    /// Bind this handle with the per-edge **clone-on-read** opt-in — each attempt
    /// receives a fresh clone of the value (T0.2 clone-on-read). The `Clone`
    /// requirement on the value type and any memory-multiplication concern are
    /// **assembly** checks (T14); C3 only records the opt-in.
    #[must_use]
    pub fn clone_on_read(self) -> CloneOnRead<T> {
        CloneOnRead(self)
    }
}

impl<T> BoundInput for Handle<T> {
    type Value = T;
    fn resolve(self) -> (NodeId, ReceiveMode) {
        (self.id(), ReceiveMode::Owned)
    }
}
impl<T> BoundInput for Shared<T> {
    type Value = T;
    fn resolve(self) -> (NodeId, ReceiveMode) {
        (self.0.id(), ReceiveMode::Shared)
    }
}
impl<T> BoundInput for CloneOnRead<T> {
    type Value = T;
    fn resolve(self) -> (NodeId, ReceiveMode) {
        (self.0.id(), ReceiveMode::CloneOnRead)
    }
}

/// The **sealed positional binding** trait: maps a set of bound handles to the
/// consuming task's declared input tuple, so **count, order, and value types are
/// all compile-checked at once** (T5 ADR §3). The exact-match bound lives at the
/// registration seam as `D: Deps<Inputs = T::Input>` — a wrong-type or
/// wrong-arity `deps` argument cannot satisfy it, so the mis-wiring is a
/// **compile error**.
///
/// A **single input** is a bare [`Handle<T>`](Handle) (or a
/// [`Shared`]/[`CloneOnRead`] wrapper), delivering the bare value `T` — never a
/// one-tuple `(T,)` (T5 ADR §5). Multi-input tuples are supported for arities
/// **2 through [`MAX_INPUT_ARITY`] (8)**. Binding more than the ceiling matches
/// no impl and surfaces this trait's curated `on_unimplemented` message.
#[diagnostic::on_unimplemented(
    message = "too many inputs bound to one task: the maximum input arity is 8",
    label = "this binds more than 8 handles",
    note = "aggregate the upstream values into a struct produced by an intermediate node, then depend on that one handle"
)]
pub trait Deps: sealed::Sealed {
    /// The tuple of value types these bound handles deliver, matched against the
    /// task's declared [`Task::Input`] by the registration seam.
    type Inputs;

    /// Consume the dep-set into recorded edges: `(upstream id, declared mode)` in
    /// input order. Crate-internal — the public surface never exposes raw ids.
    #[doc(hidden)]
    fn into_edges(self) -> Vec<(NodeId, ReceiveMode)>;
}

/// Sealed-trait guard: [`Deps`] and [`BoundInput`] are crate-private supertraits
/// so no downstream crate can add an impl (and so the arity ceiling is a finite,
/// curated set of impls — the whole point of the `on_unimplemented` cliff).
mod sealed {
    use super::{CloneOnRead, Handle, Shared};

    pub trait Sealed {}

    impl<T> Sealed for Handle<T> {}
    impl<T> Sealed for Shared<T> {}
    impl<T> Sealed for CloneOnRead<T> {}

    // Tuple impls are generated alongside the `Deps` impls by the `deps_tuple!`
    // macro in the parent module.
    macro_rules! seal_tuple {
        ($($ty:ident),+) => {
            impl<$($ty: super::BoundInput),+> Sealed for ($($ty,)+) {}
        };
    }
    seal_tuple!(I0, I1);
    seal_tuple!(I0, I1, I2);
    seal_tuple!(I0, I1, I2, I3);
    seal_tuple!(I0, I1, I2, I3, I4);
    seal_tuple!(I0, I1, I2, I3, I4, I5);
    seal_tuple!(I0, I1, I2, I3, I4, I5, I6);
    seal_tuple!(I0, I1, I2, I3, I4, I5, I6, I7);
}

// Arity 1: a single bound input delivers the bare value `T`, NOT `(T,)` (T5 ADR
// §5). This blanket impl covers a bare `Handle<T>`, `Shared<T>`, and
// `CloneOnRead<T>` uniformly — each is a `BoundInput` whose `Value` is `T`.
impl<D: BoundInput> Deps for D {
    type Inputs = D::Value;
    fn into_edges(self) -> Vec<(NodeId, ReceiveMode)> {
        vec![self.resolve()]
    }
}

/// Generate a tuple `Deps` impl for arity N (2..=8): `Inputs` is the tuple of
/// each position's [`BoundInput::Value`], and `into_edges` collects each
/// position's `(id, mode)` in order.
macro_rules! deps_tuple {
    ($($ty:ident => $idx:tt),+) => {
        impl<$($ty: BoundInput),+> Deps for ($($ty,)+) {
            type Inputs = ($($ty::Value,)+);
            fn into_edges(self) -> Vec<(NodeId, ReceiveMode)> {
                vec![$(self.$idx.resolve()),+]
            }
        }
    };
}
deps_tuple!(I0 => 0, I1 => 1);
deps_tuple!(I0 => 0, I1 => 1, I2 => 2);
deps_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3);
deps_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4);
deps_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4, I5 => 5);
deps_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4, I5 => 5, I6 => 6);
deps_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4, I5 => 5, I6 => 6, I7 => 7);

/// A registered node's recorded structure: its [identity](RegisteredNode::id),
/// the [data edges](RegisteredNode::data_edges) it declared (in input order),
/// and its [trigger rule](RegisteredNode::trigger_rule).
///
/// This is the construction-time artifact the binding produces — the seam
/// assembly (T14), readiness (C11), and slot wiring (C10) read. It records data
/// edges distinctly from the ordering edges T50 adds.
#[derive(Debug, Clone)]
pub struct RegisteredNode {
    id: NodeId,
    edges: Vec<DataEdge>,
    trigger_rule: TriggerRule,
}

impl RegisteredNode {
    /// The identity of this node (name-derived — T0.7).
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The declared **data** edges, in input order — distinct from ordering
    /// edges (T50). Each names its upstream, position, and declared receive mode.
    #[must_use]
    pub fn data_edges(&self) -> &[DataEdge] {
        &self.edges
    }

    /// This node's trigger rule. A node with any data edge is necessarily
    /// [`AllSucceeded`](TriggerRule::AllSucceeded) — the typestate makes any other
    /// rule inexpressible on it (arch.md §126).
    #[must_use]
    pub fn trigger_rule(&self) -> TriggerRule {
        self.trigger_rule
    }
}

/// Typestate marker: a node that so far consumes **no** value. Non-default
/// trigger rules are settable only in this state (C4 / Vocabulary).
#[derive(Debug)]
pub struct ConsumesNothing;

/// Typestate marker: a node that has bound at least one **data** dependency. This
/// state deliberately offers **no** `trigger_rule` method — the restriction to
/// `all-succeeded` *is* the method's absence (arch.md §126; T5 ADR §8).
#[derive(Debug)]
pub struct ConsumesData;

/// The binding-focused node registration builder, parameterized by a
/// consume-state typestate (arch.md `### C3 · Data dependency`; T5 ADR §8).
///
/// It starts in [`ConsumesNothing`] via [`consuming_nothing`](Self::consuming_nothing),
/// where [`trigger_rule`](NodeBinding::trigger_rule) is offered; binding a data
/// dependency with [`depends_on`](NodeBinding::depends_on) transitions it to
/// [`ConsumesData`], which offers no such method — so a non-default rule on a
/// data-dependent node is a **compile error**, not a runtime check.
///
/// This is the **binding seam** the flow builder (C7 / T13) wraps, not the
/// whole-flow accumulator: it records one node's edges and rule and mints its
/// output handle. Duplicate-name checking, node accumulation, and the immutable
/// pipeline are T13's; assembly validation is T14's.
#[derive(Debug)]
pub struct NodeBinding<S> {
    name: String,
    edges: Vec<DataEdge>,
    trigger_rule: TriggerRule,
    _state: PhantomData<S>,
}

impl NodeBinding<ConsumesNothing> {
    /// Begin registering a node under `name` that so far consumes nothing. In
    /// this state a non-default [`trigger_rule`](Self::trigger_rule) may be set;
    /// binding a data dependency transitions the typestate.
    #[must_use]
    pub fn consuming_nothing(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            edges: Vec::new(),
            trigger_rule: TriggerRule::AllSucceeded,
            _state: PhantomData,
        }
    }

    /// Set a trigger rule — available **only** while the node consumes nothing
    /// (C4 / Vocabulary). Once a data dependency is bound the node is
    /// [`ConsumesData`], a typestate that offers no `trigger_rule` method, so a
    /// non-default rule on a data-dependent node cannot be written.
    #[must_use]
    pub fn trigger_rule(mut self, rule: TriggerRule) -> Self {
        self.trigger_rule = rule;
        self
    }

    /// Bind this node's data dependencies, transitioning to [`ConsumesData`].
    ///
    /// The `deps` value types must **exactly match** `T::Input` — the
    /// `D: Deps<Inputs = T::Input>` bound is the compile-time choke point where a
    /// wrong-type or wrong-arity binding fails to compile. The `task` argument
    /// pins `T` (its declared input drives the match); it is not consumed here.
    /// Each bound edge records its declared receive mode verbatim (not
    /// adjudicated — T14).
    #[must_use]
    pub fn depends_on<T, D>(self, _task: &T, deps: D) -> NodeBinding<ConsumesData>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        let edges = deps
            .into_edges()
            .into_iter()
            .enumerate()
            .map(|(position, (upstream, mode))| DataEdge {
                upstream,
                position,
                mode,
            })
            .collect();
        NodeBinding {
            name: self.name,
            edges,
            trigger_rule: self.trigger_rule,
            _state: PhantomData,
        }
    }

    /// Finalize a consume-nothing node, minting its output handle. (A
    /// data-dependent node finalizes through [`NodeBinding::<ConsumesData>::finish`].)
    #[must_use]
    pub fn finish<Out>(self) -> Handle<Out> {
        Handle::for_registration(&self.name)
    }
}

impl NodeBinding<ConsumesData> {
    /// Finalize a data-dependent node, minting its output handle and returning
    /// the recorded node structure alongside it. Deliberately offers no
    /// `trigger_rule` method — that is the compile-time enforcement of the
    /// `all-succeeded`-only restriction on data-dependent nodes (arch.md §126).
    #[must_use]
    pub fn finish<Out>(self) -> (Handle<Out>, RegisteredNode) {
        let handle = Handle::for_registration(&self.name);
        let node = RegisteredNode {
            id: handle.id(),
            edges: self.edges,
            trigger_rule: self.trigger_rule,
        };
        (handle, node)
    }
}

/// Ergonomic registration helpers mirroring what the flow builder (T13) will do:
/// register a source node (mint a handle) and register a data-dependent node
/// (bind deps, mint a handle, return the recorded node). These are thin wrappers
/// over the public [`NodeBinding`] typestate — the registration act *is* how a
/// handle is obtained (C2), so they forge nothing. T13 supersedes them with the
/// real flow builder (duplicate-name checking, node accumulation); they are
/// `#[doc(hidden)]` so they do not advertise a competing surface in the interim.
#[doc(hidden)]
pub mod test_support {
    use super::{Deps, Handle, NodeBinding, RegisteredNode};
    use crate::task::Task;

    /// Register a sourceless (consume-nothing) node under `name`, minting its
    /// output handle — as the builder (T13) will for a `Task<Input = ()>`.
    #[must_use]
    pub fn source<T: Task<Input = ()>>(name: impl Into<String>, _task: &T) -> Handle<T::Output> {
        NodeBinding::consuming_nothing(name).finish::<T::Output>()
    }

    /// Register a data-dependent node under `name`, binding `deps` (whose value
    /// types must exactly match `T::Input`) and returning the node's output
    /// handle plus its recorded structure. The exact-match / arity / cycle checks
    /// all live in the `D: Deps<Inputs = T::Input>` bound.
    #[must_use]
    pub fn register<T, D>(
        name: impl Into<String>,
        task: &T,
        deps: D,
    ) -> (Handle<T::Output>, RegisteredNode)
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        NodeBinding::consuming_nothing(name)
            .depends_on::<T, D>(task, deps)
            .finish::<T::Output>()
    }
}
