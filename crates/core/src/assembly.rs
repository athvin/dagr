//! Flow assembly — the total, pure validation-plus-precomputation pass that turns
//! the immutable [`Pipeline`] into a validated, runtime-ready
//! [`AssemblyArtifact`].
//!
//! Assembly performs the checks the compiler cannot, and it reports **every**
//! problem it finds — never just the first. It then precomputes what the
//! runtime consumes and freezes it into the immutable artifact.
//!
//! # The assembly/bootstrap seam
//!
//! Assembly is **pure**: it touches **no network, no filesystem, no clock, no
//! credentials, and no parameter values**, so the graph is provably
//! parameter-independent and emittable in an empty environment. The checks that
//! need the actual machine — capacity/cost-fit, a missing declared resource, an
//! invalid parameter — belong to **bootstrap**, not here. This module
//! deliberately makes **no** capacity/cost-fit check.
//!
//! The [`AssemblyArtifact`] exposes **no** path to a parameter value — there is
//! no field or method that returns one — which is what makes "no parameter value
//! is reachable during assembly" a structural fact rather than a convention.
//!
//! # What assembly validates
//!
//! Each problem is reported as a distinct, complete [`Problem`]; assembly never
//! short-circuits on the first:
//!
//! - **Duplicate node name** — the report names the duplicated name and how many
//!   declarations collided (both).
//! - **Empty pipeline** — no nodes registered.
//! - **Invalid execution-class override** — an await-bound task overridden to a
//!   synchronous class (the disallowed direction).
//! - **Durable-without-contract** — a node marked durable whose output type does
//!   not implement the [`DurableOutput`] contract.
//! - **Ownership-mode conflict** — an owned (moved) demand on a value with more
//!   than one consumer (naming producer, offending edge, and consumers), or an
//!   owned edge into a retrying node with no clone-on-read opt-in.
//! - **Nonzero teardown cost** — a teardown node with a nonzero declared cost in
//!   any pool.
//!
//! The **zero-consumer non-`()` output** condition is emitted as a [`Warning`],
//! not an error: a node whose non-`()` output has zero consumers and is neither
//! retained nor durable is usually a wiring mistake, but a legitimate effect-only
//! node is common enough that it is not a failure.
//!
//! # What assembly precomputes
//!
//! Frozen into the [`AssemblyArtifact`], computed once: per-node
//! [consumer count](AssemblyArtifact::consumer_count), per-node
//! [remaining-dependency count](AssemblyArtifact::remaining_dependency_count)
//! (the readiness countdown seed), a valid
//! [execution order](AssemblyArtifact::execution_order) (topological), and the
//! [fingerprint slot](AssemblyArtifact::fingerprint) (structural fingerprint plus
//! policy hash).
//!
//! # The fingerprint slot vs the fingerprint algorithm
//!
//! This module **computes** the fingerprint slot from the field composition — the
//! structural fingerprint over the node set (identity names **and**
//! author-declared stable task / input / output type names), the edge set
//! (with each data edge's carried type stable name and kind), and trigger rules;
//! the policy hash over the residual effective-policy values — over a
//! deterministic, registration-order-independent canonical byte encoding, stamped
//! with the [`FINGERPRINT_ALGORITHM_VERSION`]. The **artifact schema and
//! renderers** live downstream. The digest is the dependency-free, deterministic
//! FNV-1a the name-derived [`NodeId`] already uses; an earlier plan named BLAKE3,
//! but the MIT-only supply-chain policy rules BLAKE3 out (see [`FingerprintSlot`]),
//! so **algorithm v1 is FNV-1a** — which satisfies every determinism /
//! cross-toolchain requirement.
//!
//! # The full node-policy value
//!
//! [`NodePolicy`] is the **full node-policy value**: the durability flag,
//! retention flag, retry count and [backoff](Backoff) shape, per-attempt timeout,
//! teardown flag, declared [cost vector](CostVector), and constrained
//! execution-class override — each with its single conservative default. The
//! trigger rule (set through the binding typestate) and the group label are
//! policy-adjacent knobs carried on the node rather than in this value; the
//! resolved [`EffectivePolicy`] — the complete, defaults-written-out view —
//! surfaces them alongside the policy fields and is what reaches the graph
//! artifact. The concrete artifact *schema* / renderers remain downstream; this
//! module resolves the effective policy and feeds the right inputs into the
//! fingerprint slot.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::binding::{DataEdge, OrderingEdge, ReceiveMode, TriggerRule};
use crate::error::RehydrateError;
use crate::execution::{Backoff, RetryConfig};
use crate::flow::{Pipeline, PipelineNode};
use crate::handle::NodeId;
use crate::task::ExecutionClass;

/// The **fingerprint algorithm version**.
///
/// Both graph hashes are stamped with this identifier so a later reader (resume;
/// the structure-assertion API) can distinguish a genuine topology difference
/// from **"cannot compare"** — a fingerprint produced by a different algorithm
/// version. Changing the canonical byte encoding, the structural / policy field
/// split, or the hash function is an **algorithm-version bump**: it changes what
/// the digest *means*, so this constant must move with it, and the change is a
/// deliberate, reviewed act — never a silent swap.
///
/// **v1** is the composition of the structural fingerprint (over the node set +
/// stable type names + edge set with carried types + trigger rules) and the
/// policy hash (over the residual effective policy), computed with the
/// dependency-free FNV-1a digest this module carries (see [`FingerprintSlot`] for
/// the hash-choice note).
pub const FINGERPRINT_ALGORITHM_VERSION: u64 = 1;

/// The **durable-output reference contract** a node's output type must implement
/// to be marked durable.
///
/// A durable node's output value *is* a reference to where the value durably
/// lives. Marking a node durable requires its output type to implement this
/// contract; a durable-marked node whose output type does **not** implement
/// `DurableOutput` fails assembly with a [`ProblemKind::DurableWithoutContract`]
/// problem naming the node, reported alongside every other assembly problem. A
/// non-durable node demands nothing of its output type.
///
/// # The two operations
///
/// - [`serialize_reference`](DurableOutput::serialize_reference) — produce a
///   **self-describing** reference to where the value durably lives (a storage
///   key, a URL, a content hash — the task's choice). **Infallible**: it is called
///   *on success*, after the task has already durably written the value, so it only
///   *names* an already-written value. The reference is an owned UTF-8 `String`
///   (dagr-core is dependency-free — no `serde`); a `String` is trivially
///   serde-serializable downstream and round-trips through the artifact schema's
///   opaque `durable_reference` slot. dagr never interprets the reference's bytes
///   — it serializes, records, existence-checks, and rehydrates them.
/// - [`rehydrate`](DurableOutput::rehydrate) — reconstruct the typed value from a
///   deserialized reference **later**, possibly in a different process (resume /
///   single-node replay). **Fallible** ([`RehydrateError`]) because the referent
///   may be gone, unreachable, or corrupt. The contract's async wrapping
///   (reconstruction is I/O) is applied at the resume call site where the runtime
///   lives; the fallibility the contract fixes is here.
///
/// # On the OUTPUT TYPE, not the task
///
/// The contract sits on the **output type** so any durable value is
/// reconstructable **regardless of which node produced it** — single-node replay
/// and demand-driven resume must rehydrate an input from a reference *without
/// running the producing task*. The durability policy flag plus the
/// [`DurableWitness`] captured at the typed registration site arm assembly's
/// durable-without-contract check.
///
/// # Scope boundaries this contract deliberately preserves
///
/// - **Teardown-deleted outputs are not resume-safe.** If a teardown that covers a
///   durable node executed in the prior run, the node's durable output may have
///   been destroyed; resume treats such nodes as not satisfiable and re-executes
///   them. Do not rely on a reference whose referent a teardown deletes.
/// - **In-memory outputs cannot be rehydrated.** A non-durable node produces an
///   in-memory value with no reference; a re-running consumer that demands it forces
///   the producer to re-execute. This is why the contract is required
///   **only** on durable-marked nodes — it creates useful authoring pressure to make
///   expensive stage boundaries produce durable, addressable outputs.
///
/// The cheap existence **probe** (present / absent / cannot-determine) and the
/// plan-time dangling refusal that consume references at resume live at the
/// resume call site; declaration, recording, and the serialize/rehydrate
/// round-trip are this module's.
pub trait DurableOutput {
    /// Produce the self-describing durable reference for an **already-written**
    /// value. Infallible: the value is in external storage by the time this is
    /// called; this only *names* it. The returned `String` is recorded verbatim
    /// in the attempt's artifact record.
    fn serialize_reference(&self) -> String;

    /// Reconstruct the typed value from a durable reference later.
    ///
    /// Fallible: the referent may be [absent](RehydrateError::is_absent) (a
    /// dangling reference), transiently unreachable, or corrupt. Given the same
    /// reference [`serialize_reference`](DurableOutput::serialize_reference)
    /// produced, this yields a value **equal** to the original (a lossless
    /// round-trip through a serialized reference). The async wrapping (I/O) is
    /// applied at the resume call site.
    ///
    /// # Errors
    ///
    /// Returns a classified [`RehydrateError`] when the referent cannot be turned
    /// back into a value.
    fn rehydrate(reference: &str) -> Result<Self, RehydrateError>
    where
        Self: Sized;

    /// **Optionally** supply metadata about the durable reference —
    /// [`DurableReferenceMeta`] `{ content_hash, size_bytes, scheme,
    /// produced_at_offset_ns }` — alongside the opaque reference itself.
    ///
    /// The default is [`None`]: an impl that supplies no metadata is unchanged,
    /// the recorded attempt carries no metadata field, and the run is
    /// byte-identical to a pre-metadata run. The reference itself stays the task's
    /// opaque string (whatever [`serialize_reference`](DurableOutput::serialize_reference)
    /// produced); the metadata is a separate, additive enrichment.
    ///
    /// # What each field is for
    ///
    /// - **`content_hash`** — the impl's own opaque digest of the referent's
    ///   bytes. dagr never computes it (that would force a hashing dependency on
    ///   core and on every impl) and never interprets it; it only carries it and,
    ///   at resume, hands it to the existence probe so a referent that still exists
    ///   but was **overwritten out-of-band** refuses the plan up front (a fingerprint
    ///   mismatch) instead of rehydrating stale bytes — the same discipline the
    ///   graph fingerprint gives structure, applied to data.
    /// - **`size_bytes`** / **`scheme`** / **`produced_at_offset_ns`** — descriptive
    ///   only in v1: recorded on the attempt for change-detection and later
    ///   lineage, never load-bearing for the resume gate.
    ///
    /// Every field is itself optional, so an impl may supply a content hash alone,
    /// or size alone, and leave the rest absent.
    fn durable_reference_meta(&self) -> Option<DurableReferenceMeta> {
        None
    }
}

/// Optional, additive metadata a [`DurableOutput`] impl may supply about its
/// durable reference: a content hash, size, scheme, and produced-at offset.
///
/// Every field is optional. The value is **opaque per-field data the impl chose**
/// — dagr records it verbatim and, for the `content_hash`, uses it only to harden
/// resume (see [`DurableOutput::durable_reference_meta`]). `dagr-core` computes no
/// hash and pulls no hashing dependency; an impl that wants a content hash
/// computes it however it likes and passes the resulting string here.
///
/// The **fluent setters** ([`content_hash`](Self::content_hash),
/// [`size_bytes`](Self::size_bytes), [`scheme`](Self::scheme),
/// [`produced_at_offset_ns`](Self::produced_at_offset_ns)) build the value; the
/// distinctly-named **read accessors** ([`get_content_hash`](Self::get_content_hash),
/// [`get_size_bytes`](Self::get_size_bytes), [`get_scheme`](Self::get_scheme),
/// [`get_produced_at_offset_ns`](Self::get_produced_at_offset_ns)) read it back —
/// the same builder/getter split [`NodePolicy`] uses.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DurableReferenceMeta {
    content_hash: Option<String>,
    size_bytes: Option<u64>,
    scheme: Option<String>,
    produced_at_offset_ns: Option<u64>,
}

impl DurableReferenceMeta {
    /// A fresh, empty metadata value — every field absent.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the reference's **content hash** — the impl's own opaque digest of the
    /// referent's bytes (dagr computes and interprets nothing). This is the one
    /// field resume verifies: a recorded hash that no longer matches the referent's
    /// current hash refuses the resume plan.
    #[must_use]
    pub fn content_hash(mut self, hash: impl Into<String>) -> Self {
        self.content_hash = Some(hash.into());
        self
    }

    /// Set the referent's **size in bytes** (descriptive; not load-bearing for the
    /// resume gate in v1).
    #[must_use]
    pub fn size_bytes(mut self, bytes: u64) -> Self {
        self.size_bytes = Some(bytes);
        self
    }

    /// Set the reference's **scheme** — a short tag for where the value lives
    /// (`"s3"`, `"file"`, …), the impl's choice (descriptive only).
    #[must_use]
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = Some(scheme.into());
        self
    }

    /// Set the **produced-at monotonic offset** (nanoseconds from run start) the
    /// reference was written at (descriptive only).
    #[must_use]
    pub fn produced_at_offset_ns(mut self, offset_ns: u64) -> Self {
        self.produced_at_offset_ns = Some(offset_ns);
        self
    }

    /// The recorded content hash, if any — the impl's opaque digest resume verifies.
    #[must_use]
    pub fn get_content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }

    /// The recorded size in bytes, if any.
    #[must_use]
    pub fn get_size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    /// The recorded scheme, if any.
    #[must_use]
    pub fn get_scheme(&self) -> Option<&str> {
        self.scheme.as_deref()
    }

    /// The recorded produced-at monotonic offset (nanoseconds), if any.
    #[must_use]
    pub fn get_produced_at_offset_ns(&self) -> Option<u64> {
        self.produced_at_offset_ns
    }

    /// Whether every field is absent (nothing was supplied).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.content_hash.is_none()
            && self.size_bytes.is_none()
            && self.scheme.is_none()
            && self.produced_at_offset_ns.is_none()
    }
}

/// The **durable-contract witness** a node carries: whether its
/// statically-known output type implements the [`DurableOutput`] contract.
///
/// Stable Rust has no specialization, so a generic registrar cannot ask "does
/// `T::Output` implement `DurableOutput`?" through its type parameter. The
/// witness is therefore captured **at the typed registration site** and threaded
/// in as this value: the flow builder's durable-registration path is bounded on
/// `T::Output: DurableOutput` and passes [`DurableWitness::Present`], while the
/// ordinary policy path passes [`DurableWitness::Absent`]. A node whose policy
/// marks it durable but whose witness is [`Absent`](DurableWitness::Absent) is an
/// **assembly** failure (not a compile error): the durable flag can be set on any
/// node, but only a node whose output type proves the contract carries a
/// `Present` witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableWitness {
    /// The output type is proven to implement the [`DurableOutput`] contract.
    Present,
    /// The output type is not proven to implement the contract (the default).
    Absent,
}

/// Detect whether a node's output type `T` is the unit type `()`, so assembly can
/// skip the zero-consumer warning for a legitimate effect-only node.
///
/// This uses [`TypeId`](std::any::TypeId) equality — a stable, generic way to
/// recognize a concrete type through a type parameter (unlike a trait-bound
/// probe, which specialization would be needed for).
#[doc(hidden)]
#[must_use]
pub fn output_is_unit<T: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>()
}

/// The declared **per-pool cost vector** for a node.
///
/// One entry per admission pool in that pool's native unit: **bytes** for the
/// memory pool (split into working memory and output residency), and a **thread
/// count** for each thread pool (blocking, compute). The conservative default is
/// **zero across every pool**, so a node with no stated cost behaves identically
/// to one with an all-zero cost written out.
///
/// Assembly reads this vector only to enforce the **nonzero-teardown-cost** rule:
/// a teardown node's declared cost must be zero. The **capacity/cost-fit** check
/// (a cost no pool can satisfy) is **bootstrap's**, not assembly's — the machine
/// is absent here. This is the minimal shape assembly validates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CostVector {
    /// Working memory in **bytes** — held for the attempt, released at its
    /// terminal state.
    working_memory: u64,
    /// Output residency in **bytes** — transferred to the output slot on
    /// production, released when the last consumer is terminal.
    output_residency: u64,
    /// Thread count drawn from the **blocking** pool.
    blocking_threads: u32,
    /// Thread count drawn from the **compute** pool.
    compute_threads: u32,
}

impl CostVector {
    /// Whether every pool's entry is zero — the conservative default.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.working_memory == 0
            && self.output_residency == 0
            && self.blocking_threads == 0
            && self.compute_threads == 0
    }

    /// The declared **working-memory** cost in bytes — held for the attempt,
    /// released at its terminal state. Zero by default.
    #[must_use]
    pub fn working_memory(&self) -> u64 {
        self.working_memory
    }

    /// The declared **output-residency** cost in bytes — transferred to the
    /// output slot on production, released when the last consumer is terminal.
    /// Zero by default; distinct from working memory (the memory pool's two-way
    /// split).
    #[must_use]
    pub fn output_residency(&self) -> u64 {
        self.output_residency
    }

    /// The declared **blocking-pool** thread count. Zero by default.
    #[must_use]
    pub fn blocking_threads(&self) -> u32 {
        self.blocking_threads
    }

    /// The declared **compute-pool** thread count. Zero by default.
    #[must_use]
    pub fn compute_threads(&self) -> u32 {
        self.compute_threads
    }
}

/// The **full node-policy value** — the immutable per-node operational knobs,
/// attached at registration and kept out of the task's logic.
///
/// It carries every author-settable policy field, each with its single
/// documented conservative default applied uniformly: **no retries**
/// ([`retries`](NodePolicy::retries)), the retry [backoff](NodePolicy::backoff)
/// shape (consulted only when retries are granted), **no** per-attempt
/// [timeout](NodePolicy::timeout), **zero** declared [cost](CostVector) on every
/// pool (working memory / output residency / blocking / compute), the constrained
/// execution-class [override](NodePolicy::execution_class) (default: the class the
/// task declared), **not** [retained](NodePolicy::retained) (release the output
/// once consumed), and **not** [durable](NodePolicy::durable). The teardown flag
/// ([`teardown`](NodePolicy::teardown)) is carried alongside for the
/// nonzero-cost check.
///
/// # The trigger rule and the group label live *beside* the policy value
///
/// Two policy-adjacent knobs are **not** fields of this struct, and deliberately
/// so:
///
/// - The **trigger rule** is set through the binding typestate
///   ([`NodeBinding`](crate::binding::NodeBinding)) so that a non-default rule is
///   *inexpressible* on a data-dependent node (a compile error, not a runtime
///   check). Putting a settable `trigger_rule` on `NodePolicy` would weaken that
///   constraint. The **effective** trigger rule is exposed on the resolved
///   [`EffectivePolicy`], sourced from the node's binding.
/// - The **group label** is presentation metadata attached at registration
///   (`register_*_in_group`) and excluded from node identity and both hashes; it
///   too surfaces on [`EffectivePolicy`], sourced from the node.
///
/// # Which hash each field feeds
///
/// The policy values (retries, backoff, timeout, costs, effective class,
/// retention, durability) feed the **policy hash**; the trigger rule feeds the
/// **structural fingerprint**; the group label feeds **neither**. A node with no
/// stated policy hashes **identically** to one with every default written out,
/// because both resolve to the same effective values.
///
/// Set it fluently at registration with [`Flow::register_source_with`] /
/// [`Flow::register_with`](crate::flow::Flow::register_with); the value is
/// immutable once assembled.
///
/// [`Flow::register_source_with`]: crate::flow::Flow::register_source_with
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePolicy {
    durable: bool,
    retained: bool,
    retries: u32,
    backoff: Backoff,
    timeout: Option<Duration>,
    teardown: bool,
    cost: CostVector,
    class_override: Option<ExecutionClass>,
}

impl Default for NodePolicy {
    /// The conservative defaults, applied uniformly: not durable, not retained,
    /// no retries, the default backoff shape (never consulted under no retries),
    /// no per-attempt timeout, not a teardown, zero cost, no class override (the
    /// class the task declared stands).
    fn default() -> Self {
        Self {
            durable: false,
            retained: false,
            retries: 0,
            backoff: default_backoff(),
            timeout: None,
            teardown: false,
            cost: CostVector::default(),
            class_override: None,
        }
    }
}

/// The default retry [`Backoff`] shape carried by a policy with no retries — a
/// small base with exponential growth and an effectively-uncapped ceiling. It is
/// never consulted under the no-retry default (the single attempt schedules no
/// wait); it is the starting point an author refines with [`NodePolicy::backoff`].
fn default_backoff() -> Backoff {
    Backoff::new(Duration::from_millis(100), 2.0, Duration::MAX)
}

impl NodePolicy {
    /// A fresh policy carrying every conservative default.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the node's output **durable**. Assembly rejects a durable node whose
    /// output type does not implement [`DurableOutput`]. The default is **not
    /// durable**.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Mark the node's output **retained** after its consumers finish. A retained
    /// zero-consumer node produces no zero-consumer warning. The default is **not
    /// retained**.
    #[must_use]
    pub fn retained(mut self, retained: bool) -> Self {
        self.retained = retained;
        self
    }

    /// Set the node's **retry count**: the number of retries *beyond* the first
    /// attempt, so `retries(0)` is a single attempt (the default) and
    /// `retries(2)` allows three attempts total. An owned input edge into a node
    /// with a nonzero retry count fails assembly unless that edge opts into
    /// clone-on-read. The default is **no retries**. Retry configuration lives in
    /// exactly this one home — the attempt runner reads it via
    /// [`retry_config`](NodePolicy::retry_config).
    #[must_use]
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set the retry **backoff shape**: the base delay, exponential growth factor,
    /// and cap the retry loop waits between attempts (backoff is exponential with
    /// jitter and a cap). It is consulted only when
    /// [`retries`](NodePolicy::retries) grants a retry (a single attempt waits
    /// nothing). The default is a small base, exponential growth, and an
    /// effectively-uncapped ceiling.
    #[must_use]
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Set the node's **per-attempt timeout**: the wall-clock budget each attempt
    /// has before it is [`timed-out`](crate::TerminalState::TimedOut). The default
    /// is **no timeout** ([`None`]); use [`timeout_off`](NodePolicy::timeout_off)
    /// to write the default out explicitly. The timeout is a policy value (it
    /// feeds the policy hash); *arming* the real timer is the driver's, which
    /// reads this budget.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Write out the **no-timeout** default explicitly: the node has no
    /// per-attempt timeout. Equivalent to leaving [`timeout`](NodePolicy::timeout)
    /// unset — a node with the default and one with the default written out here
    /// behave identically, including under the policy hash.
    #[must_use]
    pub fn timeout_off(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Mark the node a **teardown** node. A teardown node's declared cost must be
    /// zero — assembly rejects a nonzero-cost teardown. The default is **not a
    /// teardown**.
    #[must_use]
    pub fn teardown(mut self, teardown: bool) -> Self {
        self.teardown = teardown;
        self
    }

    /// Set the declared **working-memory** cost in bytes.
    #[must_use]
    pub fn working_memory(mut self, bytes: u64) -> Self {
        self.cost.working_memory = bytes;
        self
    }

    /// Set the declared **output-residency** cost in bytes.
    #[must_use]
    pub fn output_residency(mut self, bytes: u64) -> Self {
        self.cost.output_residency = bytes;
        self
    }

    /// Set the declared **blocking-pool** thread count.
    #[must_use]
    pub fn blocking_threads(mut self, threads: u32) -> Self {
        self.cost.blocking_threads = threads;
        self
    }

    /// Set the declared **compute-pool** thread count.
    #[must_use]
    pub fn compute_threads(mut self, threads: u32) -> Self {
        self.cost.compute_threads = threads;
        self
    }

    /// Override the node's **execution class**. Synchronous work may move between
    /// the blocking and compute classes; await-bound work **cannot** be overridden
    /// to a synchronous class — an invalid override fails assembly. The default is
    /// **no override** (the class the task declared stands).
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

    /// The node's retry count — retries *beyond* the first attempt (`0` is a
    /// single attempt, the default).
    #[must_use]
    pub fn retry_count(&self) -> u32 {
        self.retries
    }

    /// The node's retry [backoff](Backoff) shape — the base/factor/cap the retry
    /// loop waits between attempts (consulted only when retries are granted). Named
    /// distinctly from the [`backoff`](NodePolicy::backoff) builder (which shares
    /// the fluent-setter convention with [`retries`](NodePolicy::retries)).
    #[must_use]
    pub fn backoff_shape(&self) -> Backoff {
        self.backoff
    }

    /// The node's **per-attempt timeout** budget, or [`None`] for the no-timeout
    /// default. Named distinctly from the [`timeout`](NodePolicy::timeout)
    /// builder.
    #[must_use]
    pub fn timeout_budget(&self) -> Option<Duration> {
        self.timeout
    }

    /// The [`RetryConfig`] the attempt runner ([`run_with_retries`]) reads —
    /// **derived** from this policy's [`retries`](NodePolicy::retries) and
    /// [`backoff`](NodePolicy::backoff), so retry configuration has exactly one
    /// authoring home (the policy) and the runner never carries a second,
    /// independently-authored knob.
    ///
    /// `retries(n)` maps to `n + 1` total attempts (the initial attempt plus `n`
    /// retries), so the no-retry default yields a single-attempt config.
    ///
    /// [`run_with_retries`]: crate::execution::run_with_retries
    #[must_use]
    pub fn retry_config(&self) -> RetryConfig {
        // `retries` counts retries beyond the first attempt; `RetryConfig` counts
        // total attempts. `saturating_add(1)` keeps a `u32::MAX` retry count from
        // wrapping (it is clamped to at least one by `RetryConfig::new` anyway).
        RetryConfig::new(self.retries.saturating_add(1), self.backoff)
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

/// The **full effective policy** of a node — every policy field resolved to
/// its concrete value, defaulted fields **written out**, plus the two
/// policy-adjacent knobs the [`NodePolicy`] value does not itself carry: the
/// effective [trigger rule](EffectivePolicy::trigger_rule) (from the node's
/// binding) and the [group label](EffectivePolicy::group).
///
/// This is what reaches the **graph artifact** — every node's full effective
/// policy appears there, including defaulted values: a no-policy node and an
/// all-defaults node produce field-for-field equal effective policies. The
/// concrete artifact *schema* and its renderers are out of scope here; this is
/// the resolved value the artifact writer serializes and the two hashes run over.
///
/// # Which hash each field feeds
///
/// The policy values — [retries](EffectivePolicy::retry_count),
/// [backoff](EffectivePolicy::backoff), [timeout](EffectivePolicy::timeout),
/// [cost](EffectivePolicy::cost), effective
/// [class](EffectivePolicy::execution_class),
/// [retention](EffectivePolicy::is_retained),
/// [durability](EffectivePolicy::is_durable) — feed the **policy hash**; the
/// [trigger rule](EffectivePolicy::trigger_rule) feeds the **structural
/// fingerprint**; the [group](EffectivePolicy::group) feeds **neither**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePolicy {
    policy: NodePolicy,
    effective_class: ExecutionClass,
    trigger_rule: TriggerRule,
    group: Option<String>,
}

impl EffectivePolicy {
    /// Resolve a node's full effective policy from its [`NodePolicy`] value, its
    /// effective execution class (override applied), its binding trigger rule, and
    /// its group label. Crate-internal — produced by
    /// [`PipelineNode::effective_policy`](crate::flow::PipelineNode::effective_policy).
    pub(crate) fn resolve(
        policy: NodePolicy,
        effective_class: ExecutionClass,
        trigger_rule: TriggerRule,
        group: Option<&str>,
    ) -> Self {
        Self {
            policy,
            effective_class,
            trigger_rule,
            group: group.map(str::to_owned),
        }
    }

    /// The node's retry count — retries beyond the first attempt; `0` (a single
    /// attempt) by default. Feeds the policy hash.
    #[must_use]
    pub fn retry_count(&self) -> u32 {
        self.policy.retry_count()
    }

    /// The retry [backoff](Backoff) shape (base/factor/cap), consulted only when
    /// retries are granted. Feeds the policy hash.
    #[must_use]
    pub fn backoff(&self) -> Backoff {
        self.policy.backoff_shape()
    }

    /// The [`RetryConfig`] the attempt runner reads — derived from the effective
    /// [retry count](EffectivePolicy::retry_count) and
    /// [backoff](EffectivePolicy::backoff), so retries have one home.
    #[must_use]
    pub fn retry_config(&self) -> RetryConfig {
        self.policy.retry_config()
    }

    /// The per-attempt timeout budget, or [`None`] for the no-timeout default.
    /// Feeds the policy hash.
    #[must_use]
    pub fn timeout(&self) -> Option<Duration> {
        self.policy.timeout_budget()
    }

    /// The declared per-pool [cost vector](CostVector) — one entry per admission
    /// pool in its native unit, memory split into working / output residency.
    /// Zero on every pool by default. Feeds the policy hash.
    #[must_use]
    pub fn cost(&self) -> CostVector {
        self.policy.cost()
    }

    /// The node's **effective** execution class — the override if one was set,
    /// else the class the task declared (await-bound by default). Feeds the policy
    /// hash.
    #[must_use]
    pub fn execution_class(&self) -> ExecutionClass {
        self.effective_class
    }

    /// The node's **effective trigger rule**, sourced from the node's binding —
    /// [`AllSucceeded`](TriggerRule::AllSucceeded) by default and the only rule a
    /// data-dependent node can carry. Feeds the **structural fingerprint**, not
    /// the policy hash.
    #[must_use]
    pub fn trigger_rule(&self) -> TriggerRule {
        self.trigger_rule
    }

    /// The node's **group label**, or [`None`] for the no-group default.
    /// Presentation metadata only — it feeds **neither** hash and is visible only
    /// as artifact/diagram organization.
    #[must_use]
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    /// Whether the node's output is **retained** after its consumers finish:
    /// kept until run end and redeemable by the embedding program. `false`
    /// (release once consumed) by default. Feeds the policy hash.
    #[must_use]
    pub fn is_retained(&self) -> bool {
        self.policy.is_retained()
    }

    /// Whether the node's output is **durable**: its output type implements the
    /// durable-reference contract and its reference survives the run. `false` by
    /// default. Feeds the policy hash; arms the assembly durable-without-contract
    /// check.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.policy.is_durable()
    }

    /// Whether the node is a **teardown** node. Carried on the effective policy
    /// for completeness; its declared cost must be zero (assembly rejects a
    /// nonzero-cost teardown).
    #[must_use]
    pub fn is_teardown(&self) -> bool {
        self.policy.is_teardown()
    }
}

/// The **kind** of an assembly [`Problem`] — one variant per assembly-side check.
///
/// The enum is [`non_exhaustive`](https://doc.rust-lang.org/reference/attributes/type_system.html)
/// so a later check can add a variant without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProblemKind {
    /// Two or more registrations collided under one node name. The [`Problem`]'s
    /// [`declaration_count`](Problem::declaration_count) reports how many
    /// declarations collided (both), and the message names the duplicated name.
    DuplicateNodeName,
    /// The pipeline registered no nodes at all.
    EmptyPipeline,
    /// A node's execution-class override is incompatible with the task's declared
    /// work shape — an await-bound task overridden to a synchronous class.
    InvalidExecutionClassOverride,
    /// A node is marked durable but its output type does not implement the
    /// [`DurableOutput`] contract.
    DurableWithoutContract,
    /// A receive-mode conflict: an owned (moved) demand on a value with more than
    /// one consumer, or an owned edge into a retrying node with no clone-on-read
    /// opt-in. The message identifies the node(s) and edge involved.
    OwnershipModeConflict,
    /// A teardown node declared a nonzero cost in some pool; a teardown's cost
    /// must be zero so its admission bypass stays consistent with the capacity
    /// invariant.
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

/// One complete, distinct assembly problem. Assembly collects every problem it
/// finds into an [`AssemblyError`]; each carries its [`kind`](Problem::kind) and
/// a complete human-readable [`message`](Problem::message).
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

/// One assembly **warning** — a condition assembly reports without failing.
/// Currently the sole warning is the zero-consumer non-`()` output: a node whose
/// non-`()` output has zero consumers and is neither retained nor durable
/// (usually a wiring mistake, but a legitimate effect-only node is common enough
/// that it is not an error).
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
/// list of every problem assembly found, never just the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyError {
    problems: Vec<Problem>,
}

impl AssemblyError {
    /// Every problem assembly found, each distinct and complete.
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

/// The **fingerprint slot** — the graph fingerprint: the structural fingerprint
/// and the policy hash, each stamped with the [`FINGERPRINT_ALGORITHM_VERSION`].
///
/// The **structural fingerprint** covers the node set — each node's identity name
/// **and** its author-declared stable task / input / output type names — the edge
/// set (each data edge's endpoints, position, kind, and **carried type stable
/// name**; each **ordering** edge's endpoints and kind, with no carried type),
/// and per-node trigger rules. These are the shape-determining inputs that gate
/// resume; a structural change (node add/remove/rename, rewire, add or remove an
/// ordering edge, carried-type change, trigger-rule change) moves it. The
/// **policy hash** covers the residual effective-policy values (retries, backoff,
/// timeout, cost, effective class, retention, durability). Group labels and
/// everything environmental — timestamps, hostnames, compiler/tool versions,
/// generation time, git commit, lockfile hash — are in **neither** hash. Both are
/// computed over a deterministic, registration-order-independent canonical byte
/// encoding, so assembling the same source twice — on any machine or toolchain —
/// yields identical digests, because every hashed input is author-declared.
///
/// # Limitation — internal-logic changes are not detected
///
/// The fingerprint is composed from author-declared names, edges, trigger rules,
/// and policy — **never** from a task's function body. **Changing a task's
/// internal logic without changing its interface (its stable name, input/output
/// types, edges, trigger rule, and policy) does NOT change the fingerprint.**
/// This is a real limitation with no cheap fix in a compiled language, and it is
/// deliberate: an automatic content hash of task bodies silently under-detects
/// (inlining, monomorphization, and dependency bumps perturb the bytes without a
/// semantic change) and lies about what it covers. Where node-level change
/// detection is genuinely needed, the honest answer is a **hand-maintained
/// version marker on the task** (a version constant that *is* part of the
/// declared interface and therefore *does* move the fingerprint) — visible,
/// reviewable, obviously manual. This note is surfaced again for the readers that
/// meet it: the resume verb and the structure-assertion API.
///
/// # Hash function — dependency-free FNV-1a (algorithm v1)
///
/// An earlier plan named BLAKE3 as the v1 hash function, on the condition that it
/// be revisited if BLAKE3 proved **unavailable under the supply-chain policy**
/// rather than worked around locally. That is the case here: dagr's `deny.toml`
/// allows the **MIT** license only, and `blake3` and its transitive dependencies
/// (`arrayref` is BSD-2-Clause; `blake3` / `constant_time_eq` offer only
/// CC0-1.0 / Apache-2.0 / MIT-0) cannot resolve to MIT and pull a `cc` build-time
/// C dependency. So **algorithm v1 uses FNV-1a** — the dependency-free digest
/// already in the tree ([`crate::handle::NodeId`], the artifact build script),
/// which satisfies every fingerprint requirement (determinism, cross-toolchain
/// byte-identity, and the change/no-change matrix) because it is pure integer
/// arithmetic with no float, locale, or platform dependence. Adopting a different
/// hash later is an [algorithm-version](FINGERPRINT_ALGORITHM_VERSION) bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FingerprintSlot {
    structural: u64,
    policy: u64,
    algorithm_version: u64,
}

impl FingerprintSlot {
    /// The **structural fingerprint** digest (node set + stable type names, edge
    /// set + carried types, trigger rules). Gates resume; a structural change
    /// moves it.
    #[must_use]
    pub fn structural(&self) -> u64 {
        self.structural
    }

    /// The **policy hash** digest (residual effective policy). A policy-only
    /// change moves this and not the structural fingerprint; a divergence is a
    /// proceed-with-diff at resume, never a refusal.
    #[must_use]
    pub fn policy(&self) -> u64 {
        self.policy
    }

    /// The **algorithm version** these digests were computed under
    /// ([`FINGERPRINT_ALGORITHM_VERSION`]). Carried alongside the hashes wherever
    /// they appear so a later reader distinguishes "cannot compare" (a version
    /// mismatch) from a genuine topology difference.
    #[must_use]
    pub fn algorithm_version(&self) -> u64 {
        self.algorithm_version
    }
}

/// The immutable, machine-independent output of pure assembly.
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
/// there is deliberately **no** accessor that returns any of those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyArtifact {
    /// Precomputed per-node consumer count, keyed by node name for determinism.
    consumer_counts: BTreeMap<String, u32>,
    /// Precomputed per-node remaining-dependency count (the countdown seed).
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
    /// surface, generation time aside).
    canonical: Vec<u8>,
}

impl AssemblyArtifact {
    /// The number of nodes in the assembled pipeline.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.order.len()
    }

    /// The precomputed **consumer count** for the node with this identity —
    /// exact for every node before any execution begins — or `None` if no node
    /// carries that identity.
    #[must_use]
    pub fn consumer_count(&self, id: NodeId) -> Option<u32> {
        self.consumer_counts
            .iter()
            .find(|(name, _)| NodeId::from_name(name) == id)
            .map(|(_, c)| *c)
    }

    /// The precomputed **remaining-dependency count** for the node with this
    /// identity — the readiness countdown seed — or `None` if no node carries that
    /// identity.
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
    /// policy hash.
    #[must_use]
    pub fn fingerprint(&self) -> FingerprintSlot {
        self.fingerprint
    }

    /// The declared **environment-capture allowlist** — the set of environment
    /// variable names bootstrap is permitted to capture later. Empty by default;
    /// assembly captured **no** values. The actual capture is bootstrap's.
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
    /// byte-identity is defined. Assembling the same pipeline twice in one process
    /// yields identical bytes (the generation-time field, owned by the artifact
    /// writer, is not part of this pure-assembly slice). Registration order does
    /// not affect it.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
}

impl Pipeline {
    /// Run the **assembly** pass over this immutable pipeline: validate every
    /// registration and precompute what the runtime needs, returning the
    /// immutable [`AssemblyArtifact`].
    ///
    /// Assembly is **total and pure** — it reports **every** problem it finds
    /// (never just the first) and touches no network, filesystem, clock,
    /// credentials, or parameter values. It performs **no** capacity/cost-fit
    /// check; that is deferred to bootstrap.
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

    /// The graph [fingerprint](FingerprintSlot) — the structural fingerprint, the
    /// policy hash, and the [algorithm version](FINGERPRINT_ALGORITHM_VERSION) —
    /// computed directly from this pipeline.
    ///
    /// This is the **reuse surface** downstream consumers bind against without
    /// reaching into internals: the graph-artifact emitter, the run artifact, and
    /// resume all read the same digests from here rather than re-deriving the
    /// composition. It matches the slot [`AssemblyArtifact::fingerprint`] carries —
    /// assembling and reading the slot, or calling this directly, yield identical
    /// values.
    ///
    /// Computation is **pure**: it needs no credentials, no network, and no run
    /// store — every hashed input is author-declared and available from the
    /// assembled pipeline. Unlike [`assemble`](Pipeline::assemble) it performs no
    /// validation; a caller that needs the validated artifact assembles.
    #[must_use]
    pub fn fingerprint(&self) -> FingerprintSlot {
        compute_fingerprint(self)
    }
}

/// The assembly pass. Collects every problem before returning, so a failure
/// carries the complete list.
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

/// Invalid-override check: await-bound work cannot move to a synchronous class;
/// synchronous work may move between blocking and compute.
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

/// Durable-without-contract check.
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

/// Nonzero-teardown-cost check.
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

/// Ownership-mode conflicts: (1) an owned demand on a multi-consumer value, and
/// (2) an owned edge into a retrying node with no clone-on-read.
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

/// Exact per-node consumer count: how many downstream edges name this node
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

/// Per-node remaining-dependency count (the countdown seed): the number of
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
        // A node runs after BOTH its data upstreams and its ordering upstreams:
        // an ordering edge sequences without a value, but it still constrains
        // topological order. Combine both, deduplicated.
        let mut ups: Vec<NodeId> = node
            .data_edges()
            .iter()
            .map(DataEdge::upstream)
            .chain(node.ordering_edges().iter().map(OrderingEdge::upstream))
            .collect();
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
/// and is neither retained nor durable.
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
// byte-identity is defined and the input the fingerprint digest runs on.
// ---------------------------------------------------------------------------

/// FNV-1a over bytes — the same dependency-free family `NodeId::from_name` uses,
/// and the fingerprint's algorithm-v1 hash function (an earlier plan named
/// BLAKE3, ruled out by the MIT-only supply-chain policy — see
/// [`FingerprintSlot`]). It is deterministic, making "assemble twice → identical"
/// hold on any machine.
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
/// serialize to the same bytes (unambiguous framing).
fn push_framed(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// The canonical byte encoding of the whole graph (structure + policy) — the
/// byte-identity surface. Deterministic and registration-order-independent.
fn canonical_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    push_framed(&mut out, b'S', &structural_encoding(pipeline));
    push_framed(&mut out, b'P', &policy_encoding(pipeline));
    push_framed(&mut out, b'E', &env_allowlist_encoding(pipeline));
    out
}

/// The structural encoding: the node set — each node's identity name **and** its
/// author-declared stable task / input / output type names — the edge set (each
/// **data** edge with its carried type stable name and kind, each **ordering**
/// edge with its endpoints and kind but no carried type), and per-node trigger
/// rules. This is exactly the resume-gating shape and nothing else: no policy
/// value, no group label, no environmental input. Nodes and edges are emitted in
/// a total, registration-order-independent order (node name; edge
/// `(consumer, position, producer)`), so two assemblies of the same source yield
/// identical bytes.
fn structural_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    // Node set, ordered by name (pipeline.nodes() is already name-ordered).
    for node in pipeline.nodes() {
        push_framed(&mut out, b'n', node.name().as_bytes());
        // Author-declared stable type names: the stable task name and the stable
        // input/output type names are part of the resume-gating shape, so renaming
        // a stable name — even with the Rust interface unchanged — moves the
        // structural fingerprint. A node registered through a type-erased
        // registrar carries none; its absence is framed distinctly (empty frames)
        // from a present-but-empty name so the two never collide.
        match node.stable_names() {
            Some(names) => {
                push_framed(&mut out, b't', names.task().as_bytes());
                for input in names.inputs() {
                    push_framed(&mut out, b'i', input.as_bytes());
                }
                // A terminator frames the end of the (variable-length) input list
                // so two different input splits can never serialize alike.
                push_framed(&mut out, b'I', &[]);
                push_framed(&mut out, b'o', names.output().as_bytes());
            }
            None => {
                // Distinct marker: this node declared no stable names.
                push_framed(&mut out, b'T', &[]);
            }
        }
        // Trigger rule is shape, so it lives in the structural half.
        push_framed(&mut out, b'r', &[trigger_rule_code(node)]);
    }
    // Edge set, ordered by (consumer name, position) — a total, order-independent
    // key. Each edge frames (consumer, producer name, position, kind, carried
    // type stable name). The carried type is the producer's declared stable output
    // name — the stable name of the value type flowing along the edge — so
    // changing a data edge's carried type moves the structural fingerprint.
    let mut edges: Vec<(String, u64, String, Option<String>)> = Vec::new();
    for node in pipeline.nodes() {
        for edge in node.data_edges() {
            let (producer, carried) = pipeline.node(edge.upstream()).map_or_else(
                || (format!("{:?}", edge.upstream()), None),
                |n| {
                    (
                        n.name().to_string(),
                        n.stable_names().map(|s| s.output().to_string()),
                    )
                },
            );
            edges.push((
                node.name().to_string(),
                edge.position() as u64,
                producer,
                carried,
            ));
        }
    }
    edges.sort();
    for (consumer, position, producer, carried) in edges {
        push_framed(&mut out, b'c', consumer.as_bytes());
        push_framed(&mut out, b'p', producer.as_bytes());
        out.extend_from_slice(&position.to_le_bytes());
        // Edge kind: data. Ordering edges are encoded distinctly in the separate
        // section below, so a data edge's byte shape is unchanged.
        out.push(b'd');
        // Carried type stable name, present-or-absent framed distinctly.
        match carried {
            Some(name) => push_framed(&mut out, b'y', name.as_bytes()),
            None => push_framed(&mut out, b'Y', &[]),
        }
    }
    // Ordering-edge set, ordered by (consumer name, producer name) — a total,
    // order-independent key. An ordering edge is part of the graph SHAPE, so it
    // feeds the structural fingerprint: adding or removing one moves it, and a
    // resume notices. It carries NO position and NO carried type (it sequences
    // without a value), and its kind byte `O` differs from a data edge's `d`, so a
    // data and an ordering edge between the same pair never collide. This section
    // is appended only for edges that exist, so a graph with NO ordering edges
    // produces byte-identical structural bytes to a graph that predates ordering
    // edges — no accidental fingerprint churn.
    let mut ordering: Vec<(String, String)> = Vec::new();
    for node in pipeline.nodes() {
        for edge in node.ordering_edges() {
            let producer = pipeline.node(edge.upstream()).map_or_else(
                || format!("{:?}", edge.upstream()),
                |n| n.name().to_string(),
            );
            ordering.push((node.name().to_string(), producer));
        }
    }
    ordering.sort();
    ordering.dedup();
    for (consumer, producer) in ordering {
        push_framed(&mut out, b'c', consumer.as_bytes());
        push_framed(&mut out, b'p', producer.as_bytes());
        // Edge kind: ordering — distinct from data's `d`. No position, no carried
        // type (an ordering edge carries no value).
        out.push(b'O');
    }
    out
}

/// The policy encoding: the residual effective-policy values per node — retries,
/// backoff shape, per-attempt timeout, cost, effective class, retention,
/// durability — ordered by node name. Group labels are excluded, as is the
/// trigger rule (it lives in the structural half). Defaulted policy encodes
/// identically to a written-out default because both resolve to the same
/// effective values.
fn policy_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    for node in pipeline.nodes() {
        push_framed(&mut out, b'n', node.name().as_bytes());
        let policy = node.policy();
        out.extend_from_slice(&policy.retry_count().to_le_bytes());
        // Backoff shape: base + cap as nanos, factor as its raw bits — the same
        // deterministic, total treatment `Backoff`'s own equality/hash uses (a
        // config `f64` is compared by bits, never by IEEE value).
        let backoff = policy.backoff_shape();
        out.extend_from_slice(&duration_nanos(backoff.base()).to_le_bytes());
        out.extend_from_slice(&duration_nanos(backoff.cap()).to_le_bytes());
        out.extend_from_slice(&backoff.factor().to_bits().to_le_bytes());
        // Per-attempt timeout: a present/absent tag then the budget in nanos. The
        // no-timeout default (absent) encodes as tag 0 with a zero budget, so a
        // node with the default and one with the default written out coincide.
        match policy.timeout_budget() {
            None => {
                out.push(0);
                out.extend_from_slice(&0u128.to_le_bytes());
            }
            Some(d) => {
                out.push(1);
                out.extend_from_slice(&duration_nanos(d).to_le_bytes());
            }
        }
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

/// A [`Duration`] as total nanoseconds — a total, deterministic scalar for the
/// canonical encoding. `Duration::MAX` (the effectively-uncapped backoff cap)
/// saturates to `u128::MAX`, which is fine: it is a fixed sentinel, so every
/// uncapped schedule encodes identically.
fn duration_nanos(d: Duration) -> u128 {
    d.as_nanos()
}

/// The env-allowlist encoding — names only, in declared order. It is neither in
/// the structural nor the policy hash (both hashes exclude everything
/// environmental); it lives in the canonical byte form only so the artifact's
/// byte-identity surface reflects the declared allowlist.
fn env_allowlist_encoding(pipeline: &Pipeline) -> Vec<u8> {
    let mut out = Vec::new();
    for name in pipeline.env_allowlist() {
        push_framed(&mut out, b'v', name.as_bytes());
    }
    out
}

/// Compute the [`FingerprintSlot`] over the canonical structural / policy
/// encodings, stamped with the [`FINGERPRINT_ALGORITHM_VERSION`].
///
/// Algorithm v1 uses the dependency-free FNV-1a digest (see [`FingerprintSlot`]
/// for the hash-choice note). The encodings are total and
/// registration-order-independent, so the same source yields the same digests on
/// any machine or toolchain.
pub(crate) fn compute_fingerprint(pipeline: &Pipeline) -> FingerprintSlot {
    FingerprintSlot {
        structural: fnv1a(&structural_encoding(pipeline)),
        policy: fnv1a(&policy_encoding(pipeline)),
        algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
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
