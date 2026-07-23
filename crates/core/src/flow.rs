//! The C7 flow builder and node identity — accumulating node registrations into
//! an **immutable pipeline** (arch.md `### C7 · Flow assembly`).
//!
//! A [`Flow`] builder accepts node registrations — each carrying an **explicit,
//! caller-supplied node name** — and hands back the typed [`Handle`] from T10
//! for each. Finalization ([`Flow::finish`]) **consumes** the builder and yields
//! an immutable [`Pipeline`]: once produced, no further registration or mutation
//! is possible (that immutability is enforced at compile time — the pipeline has
//! no registration method, and `finish` takes the builder by value).
//!
//! # Node identity is the registration name — never the order
//!
//! The one decision everything downstream binds to (T0.7, C2): **a node's
//! identity is derived solely from its explicit registration name.** It is
//! *never* derived from registration order, an insertion index, or any implicit
//! counter. Two consequences the tests pin:
//!
//! - **Renaming a node changes its identity** (and, downstream, its structural
//!   fingerprint — C21).
//! - **Reordering registrations changes nothing**: the same set of names, in any
//!   registration order, yields the same node identities and the same immutable
//!   pipeline content. The node set is keyed by name, so identity comparison and
//!   lookup are order-insensitive.
//!
//! Node names are **unique across the whole pipeline** — that uniqueness is the
//! identity contract this ticket establishes and preserves. *Detecting* a
//! duplicate (and reporting both declarations) is an **assembly** check deferred
//! to T14; this module assumes uniqueness and does not diagnose a violation.
//!
//! # The group label is carried alongside identity, never part of it
//!
//! Each node reserves a **group-label slot** (C6 / T51). A group label is
//! **presentation metadata**: it feeds artifact organization and diagram
//! clustering only, and is **excluded from node identity** (and from both graph
//! fingerprints — C21). Two nodes with the same name but different group labels
//! have the *same* identity. This ticket exposes only a minimal seam for the
//! label ([`Flow::register_source_in_group`]); the real group-labelling API is
//! T51's.
//!
//! # Assembly is pure
//!
//! Registration and finalization perform **no I/O** and reach **no parameter
//! value** — no network, filesystem, clock, credentials, or parameters. The type
//! surface offers no parameter accessor at all: parameters are a bootstrap
//! concern that arrives *after* assembly (C7), so the graph provably cannot
//! depend on them. The full mechanical empty-environment proof is T14/T15; this
//! module guarantees only that the builder+finalize path introduces no such
//! dependency.
//!
//! # What lives elsewhere
//!
//! This ticket lands the builder skeleton, node identity, and the immutable
//! pipeline **only**. Deferred to **T14** (assembly validation and
//! precomputation), for which this module lays only the data *seams*:
//!
//! - **Duplicate-name reporting** (naming both declarations), the empty-pipeline
//!   check, execution-class-override validation, duplicate stable-name checking,
//!   the durable-without-contract check, and the zero-consumer warning.
//! - **Precomputation**: consumer counts, remaining-dependency counts, execution
//!   order, and the graph fingerprint. This module computes none of them; it only
//!   preserves the identity, handle linkage, group-label slot, and recorded edges
//!   those checks read.
//!
//! Data/ordering **dependency binding** (the exact-type match, tuple arities,
//! fan-out, ordering edges) is C3/C4 (T11/T12); this module *drives* the T11
//! [`NodeBinding`] seam to record a data-dependent node's edges, it does not
//! re-implement the binding. Groups (the real API), fingerprints, the graph
//! artifact, and the event-stream writer are C6/C21/C20/C19 (T51/T41/T40/T19).
//!
//! # Node identity here vs the stable *type* name (T0.7)
//!
//! **Node identity** — this module's subject — is the explicit **registration
//! name**, a per-node `String` the author supplies at each registration. It is
//! *distinct* from the **stable task/payload *type* name** the T0.7 ADR fixes: a
//! `StableName` associated-constant carried by task and payload *types*, feeding
//! the graph artifact (C20 / T40) and the two fingerprints (C21 / T41). Nothing
//! in this ticket consumes a stable *type* name — node identity, the immutable
//! pipeline, and its read surface are all built on the registration *name* alone
//! — so the `StableName` trait and its one-line derive land with their first
//! consumer, the fingerprint/artifact tickets (T40/T41), not here. Duplicate
//! *stable-type-name* checking (like duplicate *node-name* checking) is an
//! assembly concern deferred to T14.

use std::collections::BTreeMap;

use crate::binding::{DataEdge, NodeBinding, TriggerRule};
use crate::handle::{Handle, NodeId};
use crate::task::Task;
use crate::Deps;

/// A single node inside the immutable [`Pipeline`] — its identity, its
/// group-label slot, and the structure downstream tickets read.
///
/// Every field a downstream ticket reads is preserved here: the identity
/// [`name`](PipelineNode::name) (and its derived [`id`](PipelineNode::id)), the
/// [`group`](PipelineNode::group) label slot (presentation metadata, **not**
/// identity — C6/T51), and the recorded [`data_edges`](PipelineNode::data_edges)
/// and [`trigger_rule`](PipelineNode::trigger_rule) the binding produced (the
/// seam assembly (T14), readiness (C11), and slot wiring (C10) consume).
///
/// It is a **read-only** record: it exposes accessors and no mutators, so a node
/// cannot be altered once the pipeline is finalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineNode {
    /// The explicit registration name — the node's identity (T0.7 / C2).
    name: String,
    /// The name-derived identity token (opaque; T10). Redundant with `name` but
    /// kept so identity comparison never re-hashes.
    id: NodeId,
    /// The group label (C6), or `None`. Presentation metadata — **excluded** from
    /// identity and from both fingerprints (C21). T51 supplies the real API.
    group: Option<String>,
    /// The declared data edges, in input order (T11). Empty for a source node.
    edges: Vec<DataEdge>,
    /// The node's trigger rule (T0.4 / T11). `AllSucceeded` for a data-dependent
    /// node (the typestate makes any other rule inexpressible on it).
    trigger_rule: TriggerRule,
}

impl PipelineNode {
    /// This node's **identity name** — the explicit name supplied at
    /// registration, recorded verbatim (no prefix, suffix, index, or
    /// normalization). This *is* node identity (T0.7 / C2).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This node's opaque, name-derived [identity token](NodeId) (T10). Equal for
    /// two nodes registered under the same name, regardless of order; different
    /// for two different names. The group label is **not** part of it.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// This node's **group label** (C6), or `None` if it belongs to no group.
    ///
    /// The group label is **presentation metadata carried alongside identity** —
    /// it feeds artifact organization and diagram clustering only, and is
    /// **excluded** from node identity and from both fingerprints (C21). Renaming
    /// or removing a group changes neither this node's [`id`](PipelineNode::id)
    /// nor the pipeline's structural identity. This is the seam T51 populates
    /// through its real group-labelling API.
    #[must_use]
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    /// This node's declared **data** edges, in input order (T11) — empty for a
    /// source node. Each names its upstream, input position, and declared receive
    /// mode, recorded verbatim (mode conflicts are adjudicated at assembly, T14).
    #[must_use]
    pub fn data_edges(&self) -> &[DataEdge] {
        &self.edges
    }

    /// This node's [trigger rule](TriggerRule) (T0.4 / T11). A node with any data
    /// edge is necessarily [`AllSucceeded`](TriggerRule::AllSucceeded) — the T11
    /// typestate makes any other rule inexpressible on a data-dependent node.
    #[must_use]
    pub fn trigger_rule(&self) -> TriggerRule {
        self.trigger_rule
    }
}

/// The flow builder — it accumulates node registrations and produces an
/// immutable [`Pipeline`] (arch.md `### C7 · Flow assembly`).
///
/// A registration supplies an **explicit node name** and the task value, and
/// hands back the typed [`Handle`] from T10. There is **no** other way to obtain
/// a handle for a node (C2): a handle is the return value of a registration, and
/// registration is the only route into the flow. Identity is the name
/// ([module docs](self)); the builder never derives identity from the order in
/// which registrations arrive.
///
/// [`finish`](Flow::finish) **consumes** the builder and yields the immutable
/// [`Pipeline`]. There is no route by which the graph shape can change after
/// that — the pipeline exposes only read access, and the builder is moved out of
/// scope (both compile-time facts; see the checked-in UI fixtures
/// `flow_pipeline_immutable` and `flow_finish_consumes_builder`).
///
/// # What the builder does *not* do
///
/// It performs **no** assembly validation and **no** precomputation — duplicate
/// names, empty pipelines, class overrides, stable-name uniqueness, the
/// durable-contract check, the zero-consumer warning, consumer/dependency counts,
/// execution order, and the fingerprint are all **T14's**. It only accumulates
/// the identity, handle linkage, group-label slot, and recorded edges those
/// checks will later read.
#[derive(Debug, Default)]
pub struct Flow {
    /// Nodes keyed by identity name, so accumulation is order-insensitive:
    /// iteration and lookup are by name (unique across the pipeline, C7), never
    /// by the order registrations arrived. Assembly (T14) diagnoses a duplicate
    /// name; this map simply records the last write under a name.
    nodes: BTreeMap<String, PipelineNode>,
}

impl Flow {
    /// Begin a fresh flow with no nodes registered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }

    /// Register a **source** node (one whose task consumes nothing) under an
    /// explicit `name`, returning its output [`Handle`].
    ///
    /// The name is the node's identity (T0.7 / C2), recorded verbatim. The task
    /// value pins the output type `T::Output`; it is not consumed here (execution
    /// is a later ticket). The returned handle is the **only** way to refer to
    /// this node's output downstream.
    #[must_use]
    pub fn register_source<T>(&mut self, name: impl Into<String>, task: &T) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        self.register_source_in_group::<T>(name, task, None::<String>)
    }

    /// Register a **source** node under `name` **in a group**, returning its
    /// output [`Handle`].
    ///
    /// The `group` argument is the minimal **group-label seam** this ticket
    /// exposes (C6); T51 supplies the real group-labelling API. The label is
    /// presentation metadata carried alongside identity and **excluded** from it
    /// ([module docs](self)) — passing a different label leaves the node's
    /// [identity](NodeId) unchanged.
    #[must_use]
    pub fn register_source_in_group<T>(
        &mut self,
        name: impl Into<String>,
        _task: &T,
        group: Option<impl Into<String>>,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        let name = name.into();
        // Route through the T11 binding seam so a handle is obtained only by
        // registration (C2), and the source node's trigger rule defaults
        // correctly (AllSucceeded).
        let handle: Handle<T::Output> = NodeBinding::consuming_nothing(&name).finish::<T::Output>();
        let node = PipelineNode {
            id: handle.id(),
            name: name.clone(),
            group: group.map(Into::into),
            edges: Vec::new(),
            trigger_rule: TriggerRule::AllSucceeded,
        };
        self.nodes.insert(name, node);
        handle
    }

    /// Register a **data-dependent** node under `name`, binding `deps` (whose
    /// value types must **exactly match** `T::Input`), and returning its output
    /// [`Handle`].
    ///
    /// The exact-type / arity / cycle checks all live in the T11
    /// `D: Deps<Inputs = T::Input>` bound — a wrong-type, wrong-arity, or
    /// forward-referencing binding is a **compile error**, not something this
    /// builder validates at run time. Each bound upstream is recorded as one
    /// [`DataEdge`] in input order; the receive mode is recorded verbatim and
    /// adjudicated at assembly (T14), never here.
    #[must_use]
    pub fn register<T, D>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        deps: D,
    ) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        self.register_in_group::<T, D>(name, task, deps, None::<String>)
    }

    /// Register a **data-dependent** node under `name` **in a group**, binding
    /// `deps`, and returning its output [`Handle`].
    ///
    /// As [`register`](Flow::register), plus the minimal group-label seam (C6 /
    /// T51). The label is excluded from identity ([module docs](self)).
    #[must_use]
    pub fn register_in_group<T, D>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        deps: D,
        group: Option<impl Into<String>>,
    ) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        let name = name.into();
        // Drive the T11 binding seam: it records the edges (in input order) and
        // the trigger rule, and mints the output handle. We copy the recorded
        // structure into the pipeline node, adding the identity name and the
        // group-label slot this ticket owns.
        let (handle, node) = NodeBinding::consuming_nothing(&name)
            .depends_on::<T, D>(task, deps)
            .finish::<T::Output>();
        let pipeline_node = PipelineNode {
            id: node.id(),
            name: name.clone(),
            group: group.map(Into::into),
            edges: node.data_edges().to_vec(),
            trigger_rule: node.trigger_rule(),
        };
        self.nodes.insert(name, pipeline_node);
        handle
    }

    /// **Finalize** the flow: consume the builder and yield the immutable
    /// [`Pipeline`].
    ///
    /// This is the point after which the graph shape is fixed permanently. The
    /// builder is taken **by value** (consumed), and the returned [`Pipeline`]
    /// exposes only read access — so no registration or mutation is possible once
    /// the pipeline exists (C7). This performs **no** assembly validation and
    /// **no** precomputation (T14); it simply freezes the accumulated node set.
    #[must_use]
    pub fn finish(self) -> Pipeline {
        Pipeline { nodes: self.nodes }
    }
}

/// The **immutable pipeline** a finalized [`Flow`] produces (arch.md `### C7 ·
/// Flow assembly`).
///
/// It exposes **only read access** to its node set: iterate the nodes, look one
/// up by [identity](NodeId), or resolve a [`Handle`] to the node it names. There
/// is deliberately **no** registration or mutation method — the graph shape is
/// fixed at finalization and never changes afterward, which is a compile-time
/// fact (the checked-in UI fixture `flow_pipeline_immutable` asserts a
/// registration call on a `Pipeline` fails to compile).
///
/// Node identity being name-derived (T0.7 / C2), the node set is keyed by name,
/// so two pipelines assembled from the same registrations in different orders
/// compare **equal** (order-insensitive content) and iterate their nodes in the
/// same deterministic order. The group label is presentation metadata carried on
/// each node and excluded from identity ([`Flow`] docs).
///
/// # What it does *not* carry yet
///
/// This ticket lands the node set and its read surface only. Assembly validation
/// and precomputation — consumer counts, dependency counts, execution order, the
/// fingerprint slot, and every diagnostic — are **T14's**; this pipeline is the
/// value T14 bolts those onto, and the value the graph-artifact writer (T40) and
/// event-stream writer (T19) later read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline {
    /// Nodes keyed by identity name — a total, deterministic, order-insensitive
    /// ordering independent of registration order (the canonical sort key of
    /// T0.7 §6). Read-only from outside this module.
    nodes: BTreeMap<String, PipelineNode>,
}

impl Pipeline {
    /// The number of nodes in the pipeline.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the pipeline has no nodes. (The empty-pipeline *check* — treating
    /// an empty pipeline as an assembly error — is T14's, not here.)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Iterate the pipeline's nodes in a **deterministic, order-insensitive**
    /// order (by identity name — independent of registration order).
    pub fn nodes(&self) -> impl Iterator<Item = &PipelineNode> {
        self.nodes.values()
    }

    /// Look up a node by its opaque [identity](NodeId), returning `None` if no
    /// node carries that identity.
    ///
    /// This is **not** a runtime node-output lookup and **not** a route to forge
    /// a handle (C2): it maps an identity a registration already stamped to the
    /// read-only node record. There is deliberately no lookup by *name*, *index*,
    /// or *string key* into node outputs — the graph shape is fixed and outputs
    /// are never addressed by name.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&PipelineNode> {
        self.nodes.values().find(|node| node.id() == id)
    }

    /// Resolve a [`Handle`] to the node it was returned for, returning `None` if
    /// that node is not in this pipeline.
    ///
    /// The linkage established at registration — the handle carries the node's
    /// identity — survives finalization intact: `resolve(h)` maps `h` to exactly
    /// the node whose registration returned it. The handle's value type `T` is
    /// irrelevant to the lookup (identity is name-derived); it is accepted by
    /// value because [`Handle`] is [`Copy`].
    #[must_use]
    pub fn resolve<T>(&self, handle: Handle<T>) -> Option<&PipelineNode> {
        self.node(handle.id())
    }
}
