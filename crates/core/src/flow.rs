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

use crate::assembly::{output_is_unit, DurableOutput, DurableWitness, EffectivePolicy, NodePolicy};
use crate::binding::{DataEdge, NodeBinding, TriggerRule};
use crate::handle::{Handle, NodeId};
use crate::task::{ExecutionClass, Task};
use crate::Deps;

/// The run-level **failure mode** — what happens to the *rest* of the run when a
/// node reaches a failure-like terminal state (arch.md `### C15 · Failure policy
/// and propagation`; C15 / T34).
///
/// It is a **run policy**, not a graph fact: it is excluded from node identity and
/// from both graph fingerprints (C21), so flipping it re-runs the same graph under
/// a different failure discipline. It is selected at the builder/assembly seam
/// ([`Flow::failure_mode`] / [`Pipeline::failure_mode`]); the operator/CLI
/// override that also sets it is deferred to T55 (C26) and slots into the same
/// seam without a signature change.
///
/// In **both** modes propagation is governed by trigger rules (Vocabulary): a node
/// is marked `upstream-failed` only when its rule can no longer be satisfied, so an
/// `all-terminal` cleanup node downstream of a failure still runs regardless of
/// mode. The mode governs only the *scheduling* of still-eligible work after the
/// first failure, never the propagated-state table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FailureMode {
    /// **Continue independent** (the default): a failure cancels nothing; branches
    /// with no ancestral relationship to the failure run to completion. This is the
    /// least-surprising default and the behaviour the M1 run loop already had (it
    /// never cancelled anything), so selecting nothing changes nothing.
    #[default]
    ContinueIndependent,
    /// **Stop on first failure**: on the first terminal failure, admit no further
    /// default-rule non-teardown work; the in-flight drain completes, then every
    /// consume-nothing node with a *non-default* trigger rule whose rule fires on
    /// the resulting terminal picture still executes (a notify/cleanup contingency
    /// is exactly the work a failure is meant to trigger). Pending default-rule
    /// nodes unrelated to the failure end `cancelled`. Teardown ordering (C17) is a
    /// documented, deliberately-unimplemented carve-out left to T52.
    StopOnFirstFailure,
}

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
    /// The minimal assembly-validation policy seam (C5 / T14) — the fields
    /// assembly reads to validate this registration, with conservative defaults.
    /// The full C5 policy struct is T29's; this is what T14 must read.
    policy: NodePolicy,
    /// The execution class the task *declared* (its work shape, C1) — the
    /// baseline the class-override validity check (C5) is judged against.
    declared_class: ExecutionClass,
    /// The [durable-contract witness](DurableWitness) captured at registration —
    /// whether the node's statically-known output type is proven to implement the
    /// [`DurableOutput`] contract (T0.8 §5). A node whose policy marks it durable
    /// but whose witness is [`Absent`](DurableWitness::Absent) fails assembly.
    durable_witness: DurableWitness,
    /// Whether the node's output type is `()` — captured at registration so the
    /// zero-consumer warning (C7) never fires on a legitimate effect-only node.
    output_is_unit: bool,
    /// How many registrations collided under this node's name — 1 normally, >1
    /// when a duplicate name was registered. The pipeline's name-keyed map
    /// collapses duplicates to one node, so this count is where assembly reads
    /// that both declarations existed (C7).
    registration_count: usize,
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

    /// This node's [policy](NodePolicy) — the full C5 node-policy value (T29)
    /// carrying every author-settable field (durability, retention, retries,
    /// backoff, per-attempt timeout, teardown, cost, class override), each with
    /// its conservative default. The trigger rule and group label are carried on
    /// the node (not this value) and appear on the resolved
    /// [`effective_policy`](PipelineNode::effective_policy).
    #[must_use]
    pub fn policy(&self) -> NodePolicy {
        self.policy
    }

    /// This node's **full effective policy** (C5) — every policy field resolved to
    /// its concrete value with defaults written out, plus the effective
    /// [execution class](PipelineNode::effective_class) (override applied), the
    /// binding [trigger rule](PipelineNode::trigger_rule), and the
    /// [group](PipelineNode::group) label. This is the complete effective policy
    /// that reaches the graph artifact (arch.md C5) and that the two hashes (C21)
    /// run over; a no-policy node and an all-defaults node produce field-for-field
    /// equal effective policies.
    #[must_use]
    pub fn effective_policy(&self) -> EffectivePolicy {
        EffectivePolicy::resolve(
            self.policy,
            self.effective_class(),
            self.trigger_rule,
            self.group.as_deref(),
        )
    }

    /// The execution class the task **declared** (its work shape, C1) — before any
    /// C5 override. The class-override validity check (C5) is judged against this.
    #[must_use]
    pub fn declared_class(&self) -> ExecutionClass {
        self.declared_class
    }

    /// The node's **effective** execution class: its policy override if one is
    /// set, else the class the task declared (C5). This is the class the policy
    /// hash (C21 / T0.7) covers.
    #[must_use]
    pub fn effective_class(&self) -> ExecutionClass {
        self.policy.class_override().unwrap_or(self.declared_class)
    }

    /// Whether the node's statically-known output type is proven to implement the
    /// [`DurableOutput`] contract (T0.8 §5) — the witness captured at
    /// registration. A node marked durable whose output type does not satisfy
    /// this fails assembly.
    #[must_use]
    pub fn output_is_durable(&self) -> bool {
        matches!(self.durable_witness, DurableWitness::Present)
    }

    /// Whether the node's output type is `()` (an effect-only node). The
    /// zero-consumer warning (C7) never fires on such a node.
    #[must_use]
    pub fn output_is_unit(&self) -> bool {
        self.output_is_unit
    }

    /// How many registrations collided under this node's name — >1 signals a
    /// duplicate name that assembly rejects, naming both declarations (C7).
    #[must_use]
    pub fn registration_count(&self) -> usize {
        self.registration_count
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
    /// name; this map records the last write under a name and counts collisions
    /// (see [`PipelineNode::registration_count`]) so assembly can name both.
    nodes: BTreeMap<String, PipelineNode>,
    /// The declared **environment-capture allowlist** — the names bootstrap is
    /// permitted to capture later (C7 / C22). Empty by default; a pure
    /// *declaration* — assembly captures no values. Recorded in declared order.
    env_allowlist: Vec<String>,
    /// The run-level [failure mode](FailureMode) (C15 / T34) — the mode-selection
    /// seam. A run policy, excluded from identity and both fingerprints. Defaults
    /// to [`ContinueIndependent`](FailureMode::ContinueIndependent).
    failure_mode: FailureMode,
}

impl Flow {
    /// Begin a fresh flow with no nodes registered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            env_allowlist: Vec::new(),
            failure_mode: FailureMode::default(),
        }
    }

    /// Declare that bootstrap may later capture the given **environment variable
    /// names** into the run artifact (arch.md C7 / C22). This is a **pure
    /// declaration**: assembly stores the names and captures **no** value itself
    /// (the actual capture is bootstrap's, T24/T29). The allowlist is empty by
    /// default; each call appends the given names, recording exactly them.
    pub fn allow_env_capture<I, S>(&mut self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_allowlist.extend(names.into_iter().map(Into::into));
    }

    /// Select the run-level [failure mode](FailureMode) (C15 / T34) — the
    /// builder/assembly mode-selection seam.
    ///
    /// The mode is a **run policy**, not a graph fact: it is excluded from node
    /// identity and from both graph fingerprints (C21), so the same graph runs
    /// under either mode without changing its structural fingerprint. Defaults to
    /// [`ContinueIndependent`](FailureMode::ContinueIndependent); the operator/CLI
    /// override deferred to T55 (C26) sets this same value through the same seam
    /// without a signature change.
    pub fn failure_mode(&mut self, mode: FailureMode) {
        self.failure_mode = mode;
    }

    /// Register a **source** node (one whose task consumes nothing) under an
    /// explicit `name`, returning its output [`Handle`].
    ///
    /// The name is the node's identity (T0.7 / C2), recorded verbatim. The task
    /// value pins the output type `T::Output`; it is not consumed here (execution
    /// is a later ticket). The returned handle is the **only** way to refer to
    /// this node's output downstream. The node carries the **default**
    /// [`NodePolicy`] — use [`register_source_with`](Flow::register_source_with)
    /// to state a policy.
    #[must_use]
    pub fn register_source<T>(&mut self, name: impl Into<String>, task: &T) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        self.register_source_in_group::<T>(name, task, None::<String>)
    }

    /// Register a **source** node under `name` with an explicit [`NodePolicy`],
    /// returning its output [`Handle`].
    ///
    /// The policy is the minimal assembly-validation seam (C5 / T14): assembly
    /// reads its durability, retention, retries, teardown, cost, and
    /// class-override fields to validate the registration (an invalid override, a
    /// durable node lacking the contract, a nonzero teardown cost, …). The full
    /// C5 policy struct is T29's.
    ///
    /// This path records the durable-contract witness as
    /// [`Absent`](DurableWitness::Absent): to mark a node durable **and** prove
    /// its output type implements the contract, register it through
    /// [`register_source_durable`](Flow::register_source_durable), whose bound
    /// captures a [`Present`](DurableWitness::Present) witness. Marking a node
    /// durable here (without the bound) is precisely the durable-without-contract
    /// case assembly rejects (T0.8 §5).
    #[must_use]
    pub fn register_source_with<T>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        self.register_source_in_group_with::<T>(
            name,
            task,
            None::<String>,
            policy,
            DurableWitness::Absent,
            TriggerRule::AllSucceeded,
        )
    }

    /// Register a **durable** source node under `name`, returning its output
    /// [`Handle`].
    ///
    /// The `T::Output: DurableOutput` bound is what **captures the durable
    /// witness** (T0.8 §5): only a node whose output type proves the durable
    /// contract can be registered here, so assembly sees a
    /// [`Present`](DurableWitness::Present) witness and the durability flag is
    /// honored. `policy` is applied with its durability flag forced on. (A node
    /// marked durable whose output type does **not** implement the contract is
    /// registered through [`register_source_with`](Flow::register_source_with)
    /// instead, and fails assembly.)
    #[must_use]
    pub fn register_source_durable<T>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
        T::Output: DurableOutput,
    {
        self.register_source_in_group_with::<T>(
            name,
            task,
            None::<String>,
            policy.durable(true),
            DurableWitness::Present,
            TriggerRule::AllSucceeded,
        )
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
        task: &T,
        group: Option<impl Into<String>>,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        self.register_source_in_group_with::<T>(
            name,
            task,
            group,
            NodePolicy::new(),
            DurableWitness::Absent,
            TriggerRule::AllSucceeded,
        )
    }

    /// Register a **source** (consume-nothing) node under `name` with an explicit
    /// [`NodePolicy`] **and a non-default trigger rule** (T0.4 / C5), returning its
    /// output [`Handle`].
    ///
    /// A non-default trigger rule (`all-terminal`, `any-failed`) is expressible
    /// **only** on a node that consumes nothing (arch.md Vocabulary; C4) — a source
    /// is exactly such a node, so this registrar exposes the rule for sources
    /// without weakening the compile-time constraint: the data-dependent
    /// registrars ([`register`](Flow::register) / [`register_with`](Flow::register_with))
    /// offer **no** trigger-rule parameter, so a data node is still forced to
    /// `all-succeeded` at compile time. The trigger rule feeds the **structural
    /// fingerprint** (C21), not the policy hash.
    #[must_use]
    pub fn register_source_with_trigger<T>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        policy: NodePolicy,
        trigger_rule: TriggerRule,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        self.register_source_in_group_with::<T>(
            name,
            task,
            None::<String>,
            policy,
            DurableWitness::Absent,
            trigger_rule,
        )
    }

    /// Register a **source** node under `name`, in an optional group, with an
    /// explicit [`NodePolicy`], durable-contract [`witness`](DurableWitness), and
    /// [trigger rule](TriggerRule) — the full source-registration surface the other
    /// source registrars delegate to. The witness is
    /// [`Present`](DurableWitness::Present) only when the caller proved
    /// `T::Output: DurableOutput` ([`register_source_durable`](Flow::register_source_durable));
    /// the trigger rule is `all-succeeded` for every registrar except
    /// [`register_source_with_trigger`](Flow::register_source_with_trigger).
    #[must_use]
    fn register_source_in_group_with<T>(
        &mut self,
        name: impl Into<String>,
        _task: &T,
        group: Option<impl Into<String>>,
        policy: NodePolicy,
        durable_witness: DurableWitness,
        trigger_rule: TriggerRule,
    ) -> Handle<T::Output>
    where
        T: Task<Input = ()>,
    {
        let name = name.into();
        // Route through the T11 binding seam so a handle is obtained only by
        // registration (C2). A source consumes nothing, so the binding's
        // consume-nothing typestate is where a non-default trigger rule is
        // legitimately expressible (C4 / Vocabulary); a default source keeps
        // `AllSucceeded`.
        let handle: Handle<T::Output> = NodeBinding::consuming_nothing(&name)
            .trigger_rule(trigger_rule)
            .finish::<T::Output>();
        let node = PipelineNode {
            id: handle.id(),
            name: name.clone(),
            group: group.map(Into::into),
            edges: Vec::new(),
            trigger_rule,
            policy,
            declared_class: T::EXECUTION_CLASS,
            durable_witness,
            output_is_unit: output_is_unit::<T::Output>(),
            registration_count: 1,
        };
        self.insert_node(name, node);
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
    /// adjudicated at assembly (T14), never here. The node carries the **default**
    /// [`NodePolicy`] — use [`register_with`](Flow::register_with) to state one.
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

    /// Register a **data-dependent** node under `name`, binding `deps`, with an
    /// explicit [`NodePolicy`], returning its output [`Handle`].
    ///
    /// As [`register`](Flow::register), plus the assembly-validation policy seam
    /// (C5 / T14). A retrying node taking an owned input edge with no
    /// clone-on-read opt-in, for example, fails assembly (arch.md C1 "Ownership of
    /// inputs"). Records an [`Absent`](DurableWitness::Absent) durable witness;
    /// use [`register_durable`](Flow::register_durable) for a durable node.
    #[must_use]
    pub fn register_with<T, D>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        deps: D,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        self.register_in_group_with::<T, D>(
            name,
            task,
            deps,
            None::<String>,
            policy,
            DurableWitness::Absent,
        )
    }

    /// Register a **durable** data-dependent node under `name`, binding `deps`,
    /// returning its output [`Handle`].
    ///
    /// The `T::Output: DurableOutput` bound captures a
    /// [`Present`](DurableWitness::Present) durable witness (T0.8 §5); `policy` is
    /// applied with its durability flag forced on. A node marked durable whose
    /// output type does **not** implement the contract is registered through
    /// [`register_with`](Flow::register_with) and fails assembly.
    #[must_use]
    pub fn register_durable<T, D>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        deps: D,
        policy: NodePolicy,
    ) -> Handle<T::Output>
    where
        T: Task,
        T::Output: DurableOutput,
        D: Deps<Inputs = T::Input>,
    {
        self.register_in_group_with::<T, D>(
            name,
            task,
            deps,
            None::<String>,
            policy.durable(true),
            DurableWitness::Present,
        )
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
        self.register_in_group_with::<T, D>(
            name,
            task,
            deps,
            group,
            NodePolicy::new(),
            DurableWitness::Absent,
        )
    }

    /// Register a **data-dependent** node under `name`, in an optional group, with
    /// an explicit [`NodePolicy`] and durable-contract [`witness`](DurableWitness)
    /// — the full data-node-registration surface the other data registrars
    /// delegate to.
    #[must_use]
    fn register_in_group_with<T, D>(
        &mut self,
        name: impl Into<String>,
        task: &T,
        deps: D,
        group: Option<impl Into<String>>,
        policy: NodePolicy,
        durable_witness: DurableWitness,
    ) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        let name = name.into();
        // Drive the T11 binding seam: it records the edges (in input order) and
        // the trigger rule, and mints the output handle. We copy the recorded
        // structure into the pipeline node, adding the identity name, the
        // group-label slot, and the assembly-validation policy seam this ticket
        // owns.
        let (handle, node) = NodeBinding::consuming_nothing(&name)
            .depends_on::<T, D>(task, deps)
            .finish::<T::Output>();
        let pipeline_node = PipelineNode {
            id: node.id(),
            name: name.clone(),
            group: group.map(Into::into),
            edges: node.data_edges().to_vec(),
            trigger_rule: node.trigger_rule(),
            policy,
            declared_class: T::EXECUTION_CLASS,
            durable_witness,
            output_is_unit: output_is_unit::<T::Output>(),
            registration_count: 1,
        };
        self.insert_node(name, pipeline_node);
        handle
    }

    /// Insert a node under its name, **counting** a name collision.
    ///
    /// The node set is keyed by name (order-insensitive, C7), so a duplicate name
    /// would silently overwrite. Instead we carry the collision count on the
    /// surviving node ([`PipelineNode::registration_count`]) so **assembly** can
    /// diagnose the duplicate and name that both declarations existed (C7) — the
    /// builder still records only the identity/edges/policy, it does not itself
    /// diagnose (that is T14's).
    fn insert_node(&mut self, name: String, mut node: PipelineNode) {
        if let Some(existing) = self.nodes.get(&name) {
            node.registration_count = existing.registration_count + 1;
        }
        self.nodes.insert(name, node);
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
        Pipeline {
            nodes: self.nodes,
            env_allowlist: self.env_allowlist,
            failure_mode: self.failure_mode,
        }
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
    /// The declared environment-capture allowlist (names only, C7 / C22),
    /// carried forward from the builder for assembly to freeze into its artifact.
    /// A pure declaration — no value was ever captured.
    env_allowlist: Vec<String>,
    /// The run-level [failure mode](FailureMode) (C15 / T34), carried from the
    /// builder. A run policy excluded from identity and both fingerprints — two
    /// pipelines that differ *only* in mode still have identical graph
    /// fingerprints (the fingerprint reads node/edge/rule/policy fields, never
    /// this one).
    failure_mode: FailureMode,
}

impl Pipeline {
    /// The number of nodes in the pipeline.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the pipeline has no nodes. (The empty-pipeline *check* — treating
    /// an empty pipeline as an assembly error — is [`assemble`](Pipeline::assemble)'s,
    /// which reports it as an [`EmptyPipeline`](crate::assembly::ProblemKind::EmptyPipeline)
    /// problem.)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The declared **environment-capture allowlist** — the names bootstrap may
    /// later capture (C7 / C22), carried verbatim from the builder. Empty by
    /// default; a pure declaration, no value captured. Assembly freezes this into
    /// its [artifact](crate::assembly::AssemblyArtifact::env_allowlist).
    #[must_use]
    pub fn env_allowlist(&self) -> &[String] {
        &self.env_allowlist
    }

    /// The run-level [failure mode](FailureMode) selected at the builder/assembly
    /// seam (C15 / T34). A run policy excluded from identity and both graph
    /// fingerprints — the run-loop driver (T24/T34) reads it to decide how to treat
    /// still-eligible work after the first failure. Defaults to
    /// [`ContinueIndependent`](FailureMode::ContinueIndependent).
    #[must_use]
    pub fn failure_mode(&self) -> FailureMode {
        self.failure_mode
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
