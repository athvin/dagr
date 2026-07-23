//! C7 assembly — the total, pure validation-plus-precomputation pass that turns
//! the immutable [`Pipeline`] (T13) into a validated, runtime-ready
//! [`AssemblyArtifact`] (arch.md `### C7 · Flow assembly`).
//!
//! Assembly performs the checks the compiler cannot, and it reports **every**
//! problem it finds — never just the first (arch.md C7: *"Assembly reports all
//! problems it finds, not only the first"*). It then precomputes what the
//! runtime consumes and freezes it into the immutable artifact.
//!
//! # The assembly/bootstrap seam (T0.5)
//!
//! Assembly is **pure**: it touches **no network, no filesystem, no clock, no
//! credentials, and no parameter values**, so the graph is provably
//! parameter-independent and emittable in an empty environment (arch.md C7;
//! T0.5 ADR §2). The checks that need the actual machine — capacity/cost-fit, a
//! missing declared resource, an invalid parameter — belong to **bootstrap**
//! (T15/T24/T29), not here (T0.5 ADR §5, §7). This module deliberately makes
//! **no** capacity/cost-fit check.
//!
//! The [`AssemblyArtifact`] exposes **no** path to a parameter value — there is
//! no field or method that returns one — which is what makes "no parameter value
//! is reachable during assembly" a structural fact rather than a convention
//! (T0.5 ADR §2).
//!
//! # What assembly validates (the assembly-side partition rows, T0.5 §7)
//!
//! Each problem is reported as a distinct, complete [`Problem`]; assembly never
//! short-circuits on the first:
//!
//! - **Duplicate node name** — the report names the duplicated name and how many
//!   declarations collided (both).
//! - **Empty pipeline** — no nodes registered.
//! - **Invalid execution-class override** — an await-bound task overridden to a
//!   synchronous class (the disallowed direction per C5).
//! - **Durable-without-contract** — a node marked durable whose output type does
//!   not implement the [`DurableOutput`] contract (C27 / T0.8).
//! - **Ownership-mode conflict** — an owned (moved) demand on a value with more
//!   than one consumer (naming producer, offending edge, and consumers), or an
//!   owned edge into a retrying node with no clone-on-read opt-in (C3 / T0.2).
//! - **Nonzero teardown cost** — a teardown node with a nonzero declared cost in
//!   any pool (C17).
//!
//! The **zero-consumer non-`()` output** condition is emitted as a [`Warning`],
//! not an error: a node whose non-`()` output has zero consumers and is neither
//! retained nor durable is usually a wiring mistake, but a legitimate effect-only
//! node is common enough that it is not a failure (arch.md C7).
//!
//! # What assembly precomputes (T0.5 §1)
//!
//! Frozen into the [`AssemblyArtifact`], computed once: per-node
//! [consumer count](AssemblyArtifact::consumer_count), per-node
//! [remaining-dependency count](AssemblyArtifact::remaining_dependency_count)
//! (the readiness countdown seed, C11), a valid
//! [execution order](AssemblyArtifact::execution_order) (topological), and the
//! [fingerprint slot](AssemblyArtifact::fingerprint) (structural fingerprint plus
//! policy hash per T0.7).
//!
//! # The fingerprint slot vs the fingerprint algorithm
//!
//! This module **populates** the fingerprint slot using the T0.7 field
//! composition (structural fingerprint over the node set / edge set / trigger
//! rules; policy hash over the residual effective-policy values) and a
//! deterministic, registration-order-independent canonical byte encoding — enough
//! to make "assemble twice → byte-identical artifact" true. The **artifact schema
//! and renderers** (C20 / T40) and the **BLAKE3-v1 hash algorithm and its
//! versioning** (C21 / T41) are downstream; this module does not own them (T14
//! Out of scope). The digest here is a dependency-free, deterministic hash (the
//! same FNV-1a family the name-derived [`NodeId`] already uses), which T41
//! replaces with the versioned BLAKE3-v1 algorithm.
//!
//! # The node-policy seam vs the full policy struct (T29)
//!
//! [`NodePolicy`] here is the **minimal assembly-validation seam**: it carries
//! exactly the fields assembly *reads* to validate — the durability flag,
//! retention flag, retry count, teardown flag, declared cost vector, and
//! execution-class override — with the conservative C5 defaults. The **full C5
//! node-policy struct** (backoff shape, per-attempt timeout, trigger rule, group,
//! its defaults and emission, and its policy-hash participation) is **T29's**,
//! which comes *after* T14 and expands this seam. This module defines only what
//! it must read (T14 Out of scope: *"reads policy fields … but does not define
//! them"*, read against the dependency order T29-after-T14).

use std::collections::BTreeMap;

use crate::binding::{DataEdge, ReceiveMode};
use crate::flow::{Pipeline, PipelineNode};
use crate::handle::NodeId;
use crate::task::ExecutionClass;

/// The **durable-output reference contract** a node's output type must implement
/// to be marked durable (arch.md C27; T0.8 ADR §4).
///
/// A durable node's output value *is* a reference to where the value durably
/// lives; the full contract (serialize-reference, existence-probe, rehydrate) is
/// **T57's** to define — this module lands only the **marker seam** assembly
/// needs: whether a node's statically-known output type satisfies the contract.
/// A durable-marked node whose output type does **not** implement `DurableOutput`
/// fails assembly with a [`ProblemKind::DurableWithoutContract`] problem naming
/// the node (T0.8 ADR §5); a non-durable node demands nothing of its output type.
///
/// This trait sits on the **output type**, not the task, so any durable value is
/// reconstructable regardless of which node produced it (T0.8 ADR §4). T57
/// supersedes this marker with the full trait pair (serialize-reference /
/// existence-probe / rehydrate); assembly only reads the "implements the
/// contract" witness.
pub trait DurableOutput {}

/// The **durable-contract witness** a node carries: whether its
/// statically-known output type implements the [`DurableOutput`] contract (T0.8
/// ADR §5).
///
/// Stable Rust has no specialization, so a generic registrar cannot ask "does
/// `T::Output` implement `DurableOutput`?" through its type parameter. The
/// witness is therefore captured **at the typed registration site** and threaded
/// in as this value: the flow builder's durable-registration path is bounded on
/// `T::Output: DurableOutput` and passes [`DurableWitness::Present`], while the
/// ordinary policy path passes [`DurableWitness::Absent`]. A node whose policy
/// marks it durable but whose witness is [`Absent`](DurableWitness::Absent) is an
/// **assembly** failure (not a compile error) — exactly the partition T0.8 §5
/// fixes: the durable flag can be set on any node, but only a node whose output
/// type proves the contract carries a `Present` witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableWitness {
    /// The output type is proven to implement the [`DurableOutput`] contract.
    Present,
    /// The output type is not proven to implement the contract (the default).
    Absent,
}

/// Detect whether a node's output type `T` is the unit type `()`, so assembly can
/// skip the zero-consumer warning for a legitimate effect-only node (arch.md C7).
///
/// This uses [`TypeId`](std::any::TypeId) equality — a stable, generic way to
/// recognize a concrete type through a type parameter (unlike a trait-bound
/// probe, which specialization would be needed for).
#[doc(hidden)]
#[must_use]
pub fn output_is_unit<T: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>()
}

/// The declared **per-pool cost vector** for a node (T0.5 ADR §4).
///
/// One entry per admission pool in that pool's native unit: **bytes** for the
/// memory pool (split into working memory and output residency), and a **thread
/// count** for each thread pool (blocking, compute — T2). The conservative
/// default is **zero across every pool** (T0.5 ADR §5), so a node with no stated
/// cost behaves identically to one with an all-zero cost written out.
///
/// Assembly reads this vector only to enforce the **nonzero-teardown-cost** rule
/// (C17): a teardown node's declared cost must be zero. The **capacity/cost-fit**
/// check (a cost no pool can satisfy) is **bootstrap's**, not assembly's — the
/// machine is absent here (T0.5 ADR §5). T29 owns the full cost struct; this is
/// the minimal shape assembly validates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CostVector {
    /// Working memory in **bytes** — held for the attempt, released at its
    /// terminal state (T0.5 ADR §4).
    working_memory: u64,
    /// Output residency in **bytes** — transferred to the output slot on
    /// production, released when the last consumer is terminal (C10; T0.5 §4).
    output_residency: u64,
    /// Thread count drawn from the **blocking** pool (T2).
    blocking_threads: u32,
    /// Thread count drawn from the **compute** pool (T2).
    compute_threads: u32,
}

impl CostVector {
    /// Whether every pool's entry is zero — the conservative default (T0.5 §5).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.working_memory == 0
            && self.output_residency == 0
            && self.blocking_threads == 0
            && self.compute_threads == 0
    }
}

/// The **minimal assembly-validation policy seam** — exactly the C5 node-policy
/// fields assembly *reads* to validate a registration (arch.md C5; T14).
///
/// It carries the durability flag, the retention flag, the retry count, the
/// teardown flag, the declared [cost vector](CostVector), and the
/// execution-class override — each with its conservative C5 default (not durable,
/// not retained, no retries, not a teardown, zero cost, class as declared). The
/// **full** C5 policy struct (backoff, timeout, trigger rule, group, its defaults
/// and its policy-hash participation) is **T29's**, which expands this seam; this
/// type defines only what T14 must read.
///
/// Set it fluently at registration with [`Flow::register_source_with`] /
/// [`Flow::register_with`](crate::flow::Flow::register_with).
///
/// [`Flow::register_source_with`]: crate::flow::Flow::register_source_with
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePolicy {
    durable: bool,
    retained: bool,
    retries: u32,
    teardown: bool,
    cost: CostVector,
    class_override: Option<ExecutionClass>,
}

impl Default for NodePolicy {
    /// The conservative C5 defaults, applied uniformly (arch.md C5: *"no retries,
    /// … zero declared cost, … no group, release the output once consumed, not
    /// durable"*): not durable, not retained, no retries, not a teardown, zero
    /// cost, no class override (the class the task declared stands).
    fn default() -> Self {
        Self {
            durable: false,
            retained: false,
            retries: 0,
            teardown: false,
            cost: CostVector::default(),
            class_override: None,
        }
    }
}

impl NodePolicy {
    /// A fresh policy carrying every conservative C5 default.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the node's output **durable** (C27 / C5). Assembly rejects a durable
    /// node whose output type does not implement [`DurableOutput`] (T0.8 §5). The
    /// default is **not durable**.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Mark the node's output **retained** after its consumers finish (C10 / C5).
    /// A retained zero-consumer node produces no zero-consumer warning. The
    /// default is **not retained**.
    #[must_use]
    pub fn retained(mut self, retained: bool) -> Self {
        self.retained = retained;
        self
    }

    /// Set the node's **retry count** (C5 / C14). An owned input edge into a node
    /// with a nonzero retry count fails assembly unless that edge opts into
    /// clone-on-read (arch.md C1 "Ownership of inputs"). The default is **no
    /// retries**.
    #[must_use]
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Mark the node a **teardown** node (C17). A teardown node's declared cost
    /// must be zero — assembly rejects a nonzero-cost teardown (C17). The default
    /// is **not a teardown**.
    #[must_use]
    pub fn teardown(mut self, teardown: bool) -> Self {
        self.teardown = teardown;
        self
    }

    /// Set the declared **working-memory** cost in bytes (T0.5 §4).
    #[must_use]
    pub fn working_memory(mut self, bytes: u64) -> Self {
        self.cost.working_memory = bytes;
        self
    }

    /// Set the declared **output-residency** cost in bytes (T0.5 §4).
    #[must_use]
    pub fn output_residency(mut self, bytes: u64) -> Self {
        self.cost.output_residency = bytes;
        self
    }

    /// Set the declared **blocking-pool** thread count (T0.5 §4 / T2).
    #[must_use]
    pub fn blocking_threads(mut self, threads: u32) -> Self {
        self.cost.blocking_threads = threads;
        self
    }

    /// Set the declared **compute-pool** thread count (T0.5 §4 / T2).
    #[must_use]
    pub fn compute_threads(mut self, threads: u32) -> Self {
        self.cost.compute_threads = threads;
        self
    }

    /// Override the node's **execution class** (C5). Synchronous work may move
    /// between the blocking and compute classes; await-bound work **cannot** be
    /// overridden to a synchronous class — an invalid override fails assembly
    /// (C5). The default is **no override** (the class the task declared stands).
    #[must_use]
    pub fn execution_class(mut self, class: ExecutionClass) -> Self {
        self.class_override = Some(class);
        self
    }

    /// Whether the node is marked durable.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.durable
    }

    /// Whether the node's output is retained after its consumers finish.
    #[must_use]
    pub fn is_retained(&self) -> bool {
        self.retained
    }

    /// The node's retry count.
    #[must_use]
    pub fn retry_count(&self) -> u32 {
        self.retries
    }

    /// Whether the node is a teardown node.
    #[must_use]
    pub fn is_teardown(&self) -> bool {
        self.teardown
    }

    /// The node's declared per-pool [cost vector](CostVector).
    #[must_use]
    pub fn cost(&self) -> CostVector {
        self.cost
    }

    /// The node's execution-class override, or `None` if the declared class
    /// stands.
    #[must_use]
    pub fn class_override(&self) -> Option<ExecutionClass> {
        self.class_override
    }
}

/// The **kind** of an assembly [`Problem`] — one variant per assembly-side check
/// (T0.5 ADR §7 partition table).
///
/// The enum is [`non_exhaustive`](https://doc.rust-lang.org/reference/attributes/type_system.html)
/// so a later check can add a variant without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProblemKind {
    /// Two or more registrations collided under one node name. The [`Problem`]'s
    /// [`declaration_count`](Problem::declaration_count) reports how many
    /// declarations collided (both), and the message names the duplicated name
    /// (arch.md C7: *"names both declarations"*).
    DuplicateNodeName,
    /// The pipeline registered no nodes at all (arch.md C7).
    EmptyPipeline,
    /// A node's execution-class override is incompatible with the task's declared
    /// work shape — an await-bound task overridden to a synchronous class (C5).
    InvalidExecutionClassOverride,
    /// A node is marked durable but its output type does not implement the
    /// [`DurableOutput`] contract (C27 / T0.8).
    DurableWithoutContract,
    /// A receive-mode conflict: an owned (moved) demand on a value with more than
    /// one consumer, or an owned edge into a retrying node with no clone-on-read
    /// opt-in (C3 / T0.2). The message identifies the node(s) and edge involved.
    OwnershipModeConflict,
    /// A teardown node declared a nonzero cost in some pool; a teardown's cost
    /// must be zero so its admission bypass stays consistent with the capacity
    /// invariant (C17).
    NonzeroTeardownCost,
}

impl ProblemKind {
    /// A short, stable human label for this kind — used in [`Problem`] messages.
    const fn label(self) -> &'static str {
        match self {
            Self::DuplicateNodeName => "duplicate node name",
            Self::EmptyPipeline => "empty pipeline",
            Self::InvalidExecutionClassOverride => "invalid execution-class override",
            Self::DurableWithoutContract => "durable node without the durable-output contract",
            Self::OwnershipModeConflict => "ownership-mode conflict",
            Self::NonzeroTeardownCost => "nonzero teardown cost",
        }
    }
}

/// One complete, distinct assembly problem (arch.md C7). Assembly collects every
/// problem it finds into an [`AssemblyError`]; each carries its
/// [`kind`](Problem::kind) and a complete human-readable
/// [`message`](Problem::message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    kind: ProblemKind,
    message: String,
    declaration_count: Option<usize>,
}

impl Problem {
    fn new(kind: ProblemKind, message: String) -> Self {
        Self {
            kind,
            message,
            declaration_count: None,
        }
    }

    /// This problem's [kind](ProblemKind).
    #[must_use]
    pub fn kind(&self) -> ProblemKind {
        self.kind
    }

    /// The complete human-readable message — it names the offending node(s) and,
    /// for a duplicate name, states that both declarations collided.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// For a [`ProblemKind::DuplicateNodeName`], how many declarations collided
    /// under the name (2 or more — *both*); `None` for other kinds.
    #[must_use]
    pub fn declaration_count(&self) -> Option<usize> {
        self.declaration_count
    }
}

/// One assembly **warning** — a condition assembly reports without failing
/// (arch.md C7). Currently the sole warning is the zero-consumer non-`()` output:
/// a node whose non-`()` output has zero consumers and is neither retained nor
/// durable (usually a wiring mistake, but a legitimate effect-only node is common
/// enough that it is not an error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    message: String,
}

impl Warning {
    /// The complete human-readable message, naming the node the warning concerns.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// The failure an [`assemble`](Pipeline::assemble) returns — the **complete**
/// list of every problem assembly found, never just the first (arch.md C7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyError {
    problems: Vec<Problem>,
}

impl AssemblyError {
    /// Every problem assembly found, each distinct and complete (arch.md C7:
    /// *"Assembly reports all problems it finds, not only the first"*).
    #[must_use]
    pub fn problems(&self) -> &[Problem] {
        &self.problems
    }
}

impl std::fmt::Display for AssemblyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "assembly failed with {} problem(s):",
            self.problems.len()
        )?;
        for p in &self.problems {
            writeln!(f, "  - {}", p.message())?;
        }
        Ok(())
    }
}

impl std::error::Error for AssemblyError {}

/// The **fingerprint slot** frozen into an [`AssemblyArtifact`] — the structural
/// fingerprint and the policy hash (arch.md C21; T0.7 ADR §3–§4).
///
/// The **structural fingerprint** covers the node set (by name), the edge set
/// (upstream/downstream/kind), and per-node trigger rules — the shape-determining
/// inputs that gate resume (C27). The **policy hash** covers the residual
/// effective-policy values (retries, cost, effective class, retention,
/// durability). Group labels and everything environmental are in **neither**
/// (C6). Both are computed over a deterministic, registration-order-independent
/// canonical encoding, so assembling the same pipeline twice yields identical
/// values.
///
/// The concrete **BLAKE3-v1 algorithm and its versioning** are T41's (C21); this
/// slot holds a deterministic dependency-free digest T41 supersedes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FingerprintSlot {
    structural: u64,
    policy: u64,
}

impl FingerprintSlot {
    /// The **structural fingerprint** digest (node set, edge set, trigger rules —
    /// T0.7 §3). Gates resume (C27); a structural change moves it.
    #[must_use]
    pub fn structural(&self) -> u64 {
        self.structural
    }

    /// The **policy hash** digest (residual effective policy — T0.7 §4). A
    /// policy-only change moves this and not the structural fingerprint; a
    /// divergence is a proceed-with-diff at resume, never a refusal (C21 / C27).
    #[must_use]
    pub fn policy(&self) -> u64 {
        self.policy
    }
}

/// The immutable, machine-independent output of pure assembly (arch.md C7; T0.5
/// ADR §1).
///
/// It carries the validated graph plus everything assembly precomputes — per-node
/// [consumer counts](AssemblyArtifact::consumer_count), per-node
/// [remaining-dependency counts](AssemblyArtifact::remaining_dependency_count), a
/// valid [execution order](AssemblyArtifact::execution_order), the
/// [fingerprint slot](AssemblyArtifact::fingerprint), the
/// [environment-capture allowlist](AssemblyArtifact::env_allowlist) (names only,
/// nothing captured), and any non-fatal [warnings](AssemblyArtifact::warnings).
///
/// It is **constructible with every external resource absent** and carries **no**
/// parameter value, clock reading, filesystem or network state, or credential —
/// there is deliberately **no** accessor that returns any of those (T0.5 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyArtifact {
    /// Precomputed per-node consumer count, keyed by node name for determinism.
    consumer_counts: BTreeMap<String, u32>,
    /// Precomputed per-node remaining-dependency count (the C11 countdown seed).
    remaining_deps: BTreeMap<String, u32>,
    /// A valid topological execution order.
    order: Vec<NodeId>,
    /// The fingerprint slot (structural + policy).
    fingerprint: FingerprintSlot,
    /// The declared environment-capture allowlist — names only, captured nothing.
    env_allowlist: Vec<String>,
    /// Non-fatal warnings (the zero-consumer non-`()` output warning).
    warnings: Vec<Warning>,
    /// The deterministic canonical byte form (the byte-identity comparison
    /// surface, generation time aside — C20).
    canonical: Vec<u8>,
}

impl AssemblyArtifact {
    /// The number of nodes in the assembled pipeline.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.order.len()
    }

    /// The precomputed **consumer count** for the node with this identity —
    /// exact for every node before any execution begins (C10) — or `None` if no
    /// node carries that identity.
    #[must_use]
    pub fn consumer_count(&self, id: NodeId) -> Option<u32> {
        self.consumer_counts
            .iter()
            .find(|(name, _)| NodeId::from_name(name) == id)
            .map(|(_, c)| *c)
    }

    /// The precomputed **remaining-dependency count** for the node with this
    /// identity — the readiness countdown seed (C11) — or `None` if no node
    /// carries that identity.
    #[must_use]
    pub fn remaining_dependency_count(&self, id: NodeId) -> Option<u32> {
        self.remaining_deps
            .iter()
            .find(|(name, _)| NodeId::from_name(name) == id)
            .map(|(_, c)| *c)
    }

    /// The precomputed **execution order** — a valid topological order in which
    /// every node appears after all of its dependencies (frozen at assembly).
    #[must_use]
    pub fn execution_order(&self) -> &[NodeId] {
        &self.order
    }

    /// The [fingerprint slot](FingerprintSlot) — structural fingerprint plus
    /// policy hash (C21 / T0.7).
    #[must_use]
    pub fn fingerprint(&self) -> FingerprintSlot {
        self.fingerprint
    }

    /// The declared **environment-capture allowlist** — the set of environment
    /// variable names bootstrap is permitted to capture later. Empty by default;
    /// assembly captured **no** values (arch.md C7 / C22). The actual capture is
    /// bootstrap's (T24/T29).
    #[must_use]
    pub fn env_allowlist(&self) -> &[String] {
        &self.env_allowlist
    }

    /// The non-fatal [warnings](Warning) assembly reported (the zero-consumer
    /// non-`()` output warning). Assembly still succeeded.
    #[must_use]
    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }

    /// The deterministic **canonical byte form** — the surface over which
    /// byte-identity is defined (C20). Assembling the same pipeline twice in one
    /// process yields identical bytes (the generation-time field, owned by the
    /// artifact writer T40, is not part of this pure-assembly slice). Registration
    /// order does not affect it.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
}

impl Pipeline {
    /// Run the C7 **assembly** pass over this immutable pipeline: validate every
    /// registration and precompute what the runtime needs, returning the
    /// immutable [`AssemblyArtifact`] (arch.md `### C7 · Flow assembly`).
    ///
    /// Assembly is **total and pure** — it reports **every** problem it finds
    /// (never just the first) and touches no network, filesystem, clock,
    /// credentials, or parameter values. It performs **no** capacity/cost-fit
    /// check; that is deferred to bootstrap (T0.5 §5).
    ///
    /// # Errors
    ///
    /// Returns an [`AssemblyError`] carrying the **complete** list of problems
    /// when any assembly-side check fails: a duplicate node name (naming both
    /// declarations), an empty pipeline, an invalid execution-class override, a
    /// durable node lacking the [`DurableOutput`] contract, an ownership-mode
    /// conflict, or a nonzero teardown cost.
    pub fn assemble(&self) -> Result<AssemblyArtifact, AssemblyError> {
        assemble(self)
    }
}

/// The assembly pass. Collects every problem before returning, so a failure
/// carries the complete list (arch.md C7).
fn assemble(pipeline: &Pipeline) -> Result<AssemblyArtifact, AssemblyError> {
    let mut problems: Vec<Problem> = Vec::new();

    // --- Empty-pipeline check ------------------------------------------------
    if pipeline.is_empty() {
        problems.push(Problem::new(
            ProblemKind::EmptyPipeline,
            format!(
                "{}: the pipeline registered no nodes",
                ProblemKind::EmptyPipeline.label()
            ),
        ));
    }

    // --- Duplicate node names ------------------------------------------------
    // The BTreeMap in the pipeline collapses duplicate names to one entry, so the
    // authoritative duplicate count travels on each node (the number of
    // registrations that collided under the name — recorded by the builder).
    for node in pipeline.nodes() {
        let dups = node.registration_count();
        if dups > 1 {
            let mut p = Problem::new(
                ProblemKind::DuplicateNodeName,
                format!(
                    "{}: node name `{}` was registered by {} declarations; both declarations \
                     must use distinct names",
                    ProblemKind::DuplicateNodeName.label(),
                    node.name(),
                    dups
                ),
            );
            p.declaration_count = Some(dups);
            problems.push(p);
        }
    }

    // --- Per-node policy checks (class override, durable contract, teardown) --
    for node in pipeline.nodes() {
        check_execution_class_override(node, &mut problems);
        check_durable_contract(node, &mut problems);
        check_teardown_cost(node, &mut problems);
    }

    // --- Ownership-mode conflicts -------------------------------------------
    check_ownership_conflicts(pipeline, &mut problems);

    if !problems.is_empty() {
        return Err(AssemblyError { problems });
    }

    // --- Precomputation (only reached once the graph is valid) ---------------
    let consumer_counts = precompute_consumer_counts(pipeline);
    let remaining_deps = precompute_remaining_deps(pipeline);
    let order = precompute_execution_order(pipeline);
    let warnings = collect_warnings(pipeline, &consumer_counts);
    let canonical = canonical_encoding(pipeline);
    let fingerprint = compute_fingerprint(pipeline);

    Ok(AssemblyArtifact {
        consumer_counts,
        remaining_deps,
        order,
        fingerprint,
        env_allowlist: pipeline.env_allowlist().to_vec(),
        warnings,
        canonical,
    })
}

/// C5 invalid-override check: await-bound work cannot move to a synchronous
/// class; synchronous work may move between blocking and compute.
fn check_execution_class_override(node: &PipelineNode, problems: &mut Vec<Problem>) {
    let Some(target) = node.policy().class_override() else {
        return;
    };
    let declared = node.declared_class();
    let ok = match declared {
        // Await-bound work may not be overridden to a synchronous class; a
        // (redundant) override back to await-bound is harmless.
        ExecutionClass::AwaitBound => target == ExecutionClass::AwaitBound,
        // Synchronous work moves freely between the two synchronous classes, but
        // not back to await-bound (its work shape is synchronous).
        ExecutionClass::Blocking | ExecutionClass::Compute => {
            matches!(target, ExecutionClass::Blocking | ExecutionClass::Compute)
        }
    };
    if !ok {
        problems.push(Problem::new(
            ProblemKind::InvalidExecutionClassOverride,
            format!(
                "{}: node `{}` declares {declared:?} work but overrides its execution class to \
                 {target:?}; await-bound work cannot be moved to a synchronous class",
                ProblemKind::InvalidExecutionClassOverride.label(),
                node.name(),
            ),
        ));
    }
}

/// C27 / T0.8 durable-without-contract check.
fn check_durable_contract(node: &PipelineNode, problems: &mut Vec<Problem>) {
    if node.policy().is_durable() && !node.output_is_durable() {
        problems.push(Problem::new(
            ProblemKind::DurableWithoutContract,
            format!(
                "{}: node `{}` is marked durable, but its output type does not implement the \
                 durable-output contract; either implement the contract on the output type or \
                 drop durability on `{}`",
                ProblemKind::DurableWithoutContract.label(),
                node.name(),
                node.name(),
            ),
        ));
    }
}

/// C17 nonzero-teardown-cost check.
fn check_teardown_cost(node: &PipelineNode, problems: &mut Vec<Problem>) {
    if node.policy().is_teardown() && !node.policy().cost().is_zero() {
        problems.push(Problem::new(
            ProblemKind::NonzeroTeardownCost,
            format!(
                "{}: teardown node `{}` declares a nonzero cost; a teardown bypasses admission \
                 and its declared cost must be zero in every pool",
                ProblemKind::NonzeroTeardownCost.label(),
                node.name(),
            ),
        ));
    }
}

/// C3 / T0.2 ownership-mode conflicts: (1) an owned demand on a multi-consumer
/// value, and (2) an owned edge into a retrying node with no clone-on-read.
fn check_ownership_conflicts(pipeline: &Pipeline, problems: &mut Vec<Problem>) {
    // Build, per producer NAME (Ord, so the map is deterministic), the list of
    // (consumer name, mode) demands. `NodeId` is opaque (not Ord), so we key by
    // the producer's registration name — resolved once per edge.
    let mut demands: BTreeMap<String, Vec<(String, ReceiveMode)>> = BTreeMap::new();
    for node in pipeline.nodes() {
        for edge in node.data_edges() {
            let producer = producer_name(pipeline, edge.upstream());
            demands
                .entry(producer)
                .or_default()
                .push((node.name().to_string(), edge.mode()));
        }
    }
    for (producer_name, mut consumers) in demands {
        // Sort by (consumer name, mode) for a deterministic, order-insensitive
        // report.
        consumers.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| mode_key(a.1).cmp(&mode_key(b.1)))
        });
        // (1) An owned demand where the value has more than one consumer.
        if consumers.len() > 1 {
            for (consumer_name, mode) in &consumers {
                if *mode == ReceiveMode::Owned {
                    let others: Vec<&str> = consumers.iter().map(|(n, _)| n.as_str()).collect();
                    problems.push(Problem::new(
                        ProblemKind::OwnershipModeConflict,
                        format!(
                            "{}: consumer `{consumer_name}` demands ownership of the value \
                             produced by `{producer_name}`, but that value has {} consumers \
                             ({}); a multiply-consumed value must be received shared, or the \
                             edge must opt into clone-on-read",
                            ProblemKind::OwnershipModeConflict.label(),
                            consumers.len(),
                            others.join(", "),
                        ),
                    ));
                }
            }
        }
    }

    // (2) An owned edge into a retrying node with no clone-on-read opt-in.
    for node in pipeline.nodes() {
        if node.policy().retry_count() == 0 {
            continue;
        }
        for edge in node.data_edges() {
            if edge.mode() == ReceiveMode::Owned {
                let producer_name = producer_name(pipeline, edge.upstream());
                problems.push(Problem::new(
                    ProblemKind::OwnershipModeConflict,
                    format!(
                        "{}: node `{}` has {} retries but takes an owned input edge from \
                         `{producer_name}`; an owned-input edge into a retrying node must opt \
                         into clone-on-read (each attempt gets a fresh clone), or the node must \
                         drop its retries",
                        ProblemKind::OwnershipModeConflict.label(),
                        node.name(),
                        node.policy().retry_count(),
                    ),
                ));
            }
        }
    }
}

/// Resolve a producer id to its registration name (falling back to the opaque id
/// if — impossibly for a bound edge — it is not in the pipeline).
fn producer_name(pipeline: &Pipeline, id: NodeId) -> String {
    pipeline
        .node(id)
        .map_or_else(|| format!("{id:?}"), |n| n.name().to_string())
}

/// Exact per-node consumer count (C10): how many downstream edges name this node
/// as their upstream. Keyed by node name for a deterministic map.
fn precompute_consumer_counts(pipeline: &Pipeline) -> BTreeMap<String, u32> {
    let mut counts: BTreeMap<String, u32> = pipeline
        .nodes()
        .map(|n| (n.name().to_string(), 0))
        .collect();
    for node in pipeline.nodes() {
        for edge in node.data_edges() {
            if let Some(producer) = pipeline.node(edge.upstream()) {
                if let Some(c) = counts.get_mut(producer.name()) {
                    *c += 1;
                }
            }
        }
    }
    counts
}

/// Per-node remaining-dependency count (C11 countdown seed): the number of
/// distinct upstream nodes each node depends on.
fn precompute_remaining_deps(pipeline: &Pipeline) -> BTreeMap<String, u32> {
    let mut deps: BTreeMap<String, u32> = BTreeMap::new();
    for node in pipeline.nodes() {
        let mut upstreams: Vec<NodeId> = node.data_edges().iter().map(DataEdge::upstream).collect();
        upstreams.sort_by_key(|id| id.sort_key());
        upstreams.dedup();
        // Only count upstreams that are actually present in the pipeline.
        let count = upstreams
            .iter()
            .filter(|id| pipeline.node(**id).is_some())
            .count();
        deps.insert(
            node.name().to_string(),
            u32::try_from(count).unwrap_or(u32::MAX),
        );
    }
    deps
}

/// A valid topological execution order: every node appears after all of its
/// dependencies. Kahn's algorithm, breaking ties by node name so the order is
/// deterministic and registration-order-independent.
fn precompute_execution_order(pipeline: &Pipeline) -> Vec<NodeId> {
    // Adjacency by name (nodes are unique by name, order-insensitive).
    let names: Vec<String> = pipeline.nodes().map(|n| n.name().to_string()).collect();
    // Remaining in-degree per node name.
    let mut indegree: BTreeMap<String, usize> = names.iter().map(|n| (n.clone(), 0)).collect();
    // Forward edges: producer name -> consumer names.
    let mut forward: BTreeMap<String, Vec<String>> =
        names.iter().map(|n| (n.clone(), Vec::new())).collect();
    for node in pipeline.nodes() {
        let mut ups: Vec<NodeId> = node.data_edges().iter().map(DataEdge::upstream).collect();
        ups.sort_by_key(|id| id.sort_key());
        ups.dedup();
        for up in ups {
            if let Some(producer) = pipeline.node(up) {
                *indegree.get_mut(node.name()).unwrap() += 1;
                forward
                    .get_mut(producer.name())
                    .unwrap()
                    .push(node.name().to_string());
            }
        }
    }
    // Ready set = nodes with in-degree 0, popped in name order (BTree gives it).
    let mut ready: std::collections::BTreeSet<String> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| n.clone())
        .collect();
    let mut order: Vec<NodeId> = Vec::with_capacity(names.len());
    while let Some(name) = ready.iter().next().cloned() {
        ready.remove(&name);
        order.push(NodeId::from_name(&name));
        for consumer in forward.get(&name).cloned().unwrap_or_default() {
            let d = indegree.get_mut(&consumer).unwrap();
            *d -= 1;
            if *d == 0 {
                ready.insert(consumer);
            }
        }
    }
    order
}

/// Collect non-fatal warnings: a node whose non-`()` output has zero consumers
/// and is neither retained nor durable (arch.md C7).
fn collect_warnings(pipeline: &Pipeline, consumer_counts: &BTreeMap<String, u32>) -> Vec<Warning> {
    let mut warnings = Vec::new();
    for node in pipeline.nodes() {
        let count = consumer_counts.get(node.name()).copied().unwrap_or(0);
        if count == 0
            && !node.output_is_unit()
            && !node.policy().is_retained()
            && !node.policy().is_durable()
        {
            warnings.push(Warning {
                message: format!(
                    "node `{}` produces a non-() output with zero consumers and is neither \
                     retained nor durable; this is usually a wiring mistake (a legitimate \
                     effect-only node should produce `()`)",
                    node.name(),
                ),
            });
        }
    }
    warnings
}

// ---------------------------------------------------------------------------
// Canonicalization + fingerprint (dependency-free, deterministic).
//
// A single, fixed, unambiguously-framed byte encoding over the author-declared
// data, ordered by a total, registration-order-independent key (node name; edge
// (producer, consumer, position, kind, mode)). This is the surface over which
// byte-identity is defined (C20) and the input the fingerprint digest runs on
// (T0.7 §6). The concrete BLAKE3-v1 algorithm and the artifact wire schema are
// T41/T40; this is the deterministic dependency-free placeholder they replace.
// ---------------------------------------------------------------------------

/// FNV-1a over bytes — the same dependency-free family `NodeId::from_name` uses.
/// The fingerprint's real hash function (BLAKE3-v1) is T41's; this is a
/// deterministic stand-in that makes "assemble twice → identical" true today.
fn fnv1a(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Length-prefix a field into `out` so two distinct field structures can never
/// serialize to the same bytes (unambiguous framing — T0.7 §6).
fn push_framed(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// The canonical byte encoding of the whole graph (structure + policy) — the
/// byte-identity surface (C20). Deterministic and registration-order-independent.
fn canonical_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    push_framed(&mut out, b'S', &structural_encoding(pipeline));
    push_framed(&mut out, b'P', &policy_encoding(pipeline));
    push_framed(&mut out, b'E', &env_allowlist_encoding(pipeline));
    out
}

/// The structural encoding (T0.7 §3): node set (by name), edge set, trigger
/// rules — the resume-gating shape. Nodes and edges are emitted in name order.
fn structural_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    // Node set, ordered by name (pipeline.nodes() is already name-ordered).
    for node in pipeline.nodes() {
        push_framed(&mut out, b'n', node.name().as_bytes());
        // Trigger rule is shape (T0.7 §3), so it lives in the structural half.
        push_framed(&mut out, b'r', &[trigger_rule_code(node)]);
    }
    // Edge set, ordered by (consumer name, position) — a total, order-independent
    // key. Each edge frames (consumer, producer name, position, kind).
    let mut edges: Vec<(String, u64, String)> = Vec::new();
    for node in pipeline.nodes() {
        for edge in node.data_edges() {
            let producer = pipeline.node(edge.upstream()).map_or_else(
                || format!("{:?}", edge.upstream()),
                |n| n.name().to_string(),
            );
            edges.push((node.name().to_string(), edge.position() as u64, producer));
        }
    }
    edges.sort();
    for (consumer, position, producer) in edges {
        push_framed(&mut out, b'c', consumer.as_bytes());
        push_framed(&mut out, b'p', producer.as_bytes());
        out.extend_from_slice(&position.to_le_bytes());
        // Edge kind: data (the only kind recorded today — T50 adds ordering).
        out.push(b'd');
    }
    out
}

/// The policy encoding (T0.7 §4): the residual effective-policy values per node —
/// retries, cost, effective class, retention, durability — ordered by node name.
/// Group labels are excluded (C6). Defaulted policy encodes identically to a
/// written-out default because both resolve to the same effective values.
fn policy_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    for node in pipeline.nodes() {
        push_framed(&mut out, b'n', node.name().as_bytes());
        let policy = node.policy();
        out.extend_from_slice(&policy.retry_count().to_le_bytes());
        let cost = policy.cost();
        out.extend_from_slice(&cost.working_memory.to_le_bytes());
        out.extend_from_slice(&cost.output_residency.to_le_bytes());
        out.extend_from_slice(&cost.blocking_threads.to_le_bytes());
        out.extend_from_slice(&cost.compute_threads.to_le_bytes());
        out.push(execution_class_code(node.effective_class()));
        out.push(u8::from(policy.is_retained()));
        out.push(u8::from(policy.is_durable()));
        // Teardown is a shape-adjacent operational flag; keep it in the policy
        // half (it is not a resume-gating topology input).
        out.push(u8::from(policy.is_teardown()));
    }
    out
}

/// The env-allowlist encoding — names only, in declared order. It is neither in
/// the structural nor the policy hash (both hashes exclude everything
/// environmental — T0.7 §5); it lives in the canonical byte form only so the
/// artifact's byte-identity surface reflects the declared allowlist.
fn env_allowlist_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    for name in pipeline.env_allowlist() {
        push_framed(&mut out, b'v', name.as_bytes());
    }
    out
}

/// Compute both fingerprint digests over the canonical structural/policy
/// encodings (T0.7 §3–§6). BLAKE3-v1 is T41's; FNV-1a is the deterministic
/// dependency-free stand-in.
fn compute_fingerprint(pipeline: &Pipeline) -> FingerprintSlot {
    FingerprintSlot {
        structural: fnv1a(&structural_encoding(pipeline)),
        policy: fnv1a(&policy_encoding(pipeline)),
    }
}

fn trigger_rule_code(node: &PipelineNode) -> u8 {
    use crate::binding::TriggerRule::{AllSucceeded, AllTerminal, AnyFailed};
    match node.trigger_rule() {
        AllSucceeded => 0,
        AllTerminal => 1,
        AnyFailed => 2,
    }
}

fn execution_class_code(class: ExecutionClass) -> u8 {
    match class {
        ExecutionClass::AwaitBound => 0,
        ExecutionClass::Blocking => 1,
        ExecutionClass::Compute => 2,
    }
}

/// A total sort key over the (non-`Ord`) [`ReceiveMode`] so a consumer list can
/// be ordered deterministically for a stable ownership-conflict report.
fn mode_key(mode: ReceiveMode) -> u8 {
    match mode {
        ReceiveMode::Owned => 0,
        ReceiveMode::Shared => 1,
        ReceiveMode::CloneOnRead => 2,
    }
}
