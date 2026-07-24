//! The C8 run context — what every task invocation is told about the run it is
//! part of (arch.md `### C8 · Run context`).
//!
//! [`RunContext`] is a **read-only, hand-constructable handle** passed into every
//! [`Task::run`](crate::task::Task::run). It carries everything a task may know
//! about its run and **nothing it may change**: run / pipeline / node identity,
//! the current attempt number and the configured maximum, the run's parameters,
//! an optional [data interval](DataInterval), a [cancellation signal](CancellationSignal),
//! a [logging span](LogSpan), and accessors for the [resource registry](ResourceRegistry)
//! and the [durable scratch store](ScratchStore). A teardown node's context
//! additionally exposes the [terminal states](CoveredNodeStates) of the nodes it
//! covers (C17), so cleanup can no-op when setup never ran.
//!
//! # It is a capability surface, not an execution engine
//!
//! Every public method is a **read**. There is no API here to modify the graph,
//! reorder work, register or rescind a resource, or influence scheduling — and no
//! route back to the runtime or scheduler. The context holds **no mutable shared
//! state** the task can reach. This is C8's no-authority contract, and it is
//! load-bearing: dagr is not a scheduler and the graph's shape never changes at
//! runtime (arch.md "What this is not, permanently").
//!
//! # The data interval is caller-supplied and tool-opaque
//!
//! The [data interval](DataInterval) is a **caller-supplied, tool-opaque pair of
//! values recorded verbatim**. The tool **never** computes an interval, **never**
//! advances one, and **never** persists one between runs — a backfill is the
//! *caller* looping over invocations with different intervals. **This is the
//! boundary with "backfill orchestrator,"** stated here so nobody rediscovers it
//! in a design meeting: no framework code path in this module (or any other)
//! parses, orders, validates, or normalizes an interval's contents.
//!
//! # Hand-construction for tests
//!
//! A `RunContext` can be built by hand in a plain unit test — **no runtime, no
//! store, no registry, no clock, no network** — via [`RunContext::builder`] (full
//! control of every field) or [`RunContext::for_test`] (a fully-populated
//! zero-argument default). This is the C8 acceptance criterion that feeds the
//! single-task test kit (C28 / T60): a single task can be exercised in isolation
//! with a context constructed entirely in-process.
//!
//! # Seams landing with later tickets
//!
//! Two accessors are **additive seams** whose *substance* arrives with later
//! tickets, marked inline:
//!
//! - [`RunContext::resources`] — the [`ResourceRegistry`] (C9). Landed here as a
//!   stable, honestly-empty seam; type-keyed retrieval, newtype disambiguation,
//!   secret wrapping, and bootstrap validation are **T30**'s.
//! - [`RunContext::scratch`] — the [`ScratchStore`] (C18). The **local durable
//!   store** now lands under **T53**: opaque-byte key-value persistence,
//!   run/node namespacing with enforced cross-node isolation, atomic
//!   crash-safe writes, and the on-success cleanup hook, physically under the run
//!   store at `<base>/<pipeline>/<run-id>/scratch/<node>/`. A context built with
//!   **no run store** (the C8 hand-built path) carries an honestly-unwired store
//!   that never pretends to persist. Resume copy-forward is **T54b**'s.
//!
//! The [`CoveredNodeStates`] shape is defined here; the **runtime-side population**
//! of covered states (teardown ordering, the fresh uncancelled signal, the
//! teardown deadline) is finished under **C17 / T52**.
//!
//! The [`ResourceRequirements`] declaration plumbing is also landed here: a node
//! records the resource types it requires at registration in a form bootstrap
//! (T30) can validate against a registry and a graph artifact (C20) can later
//! render.

use std::any::{type_name, Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::handle::NodeId;

/// A run's identity (arch.md `### C8`; C19 mints a `UUIDv7` at bootstrap,
/// operator-overridable). A dagr-owned, opaque newtype so task authors program
/// against a dagr type; the framework does not interpret its contents here.
///
/// Hand-constructable in tests via [`RunId::new`]; the runtime mints the real
/// value at bootstrap (T-later), which is **not** this ticket's concern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(String);

impl RunId {
    /// Wrap an already-minted run identity verbatim. dagr owns the type; the
    /// content is opaque to the framework.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identity as a string slice, exactly as supplied.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A pipeline's identity (arch.md `### C8`). A dagr-owned, opaque newtype;
/// hand-constructable in tests via [`PipelineId::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineId(String);

impl PipelineId {
    /// Wrap a pipeline identity verbatim. dagr owns the type; the content is
    /// opaque to the framework.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identity as a string slice, exactly as supplied.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A **caller-supplied, tool-opaque** pair of values recorded verbatim
/// (arch.md `### C8`, "The data interval").
///
/// The framework **never** parses, orders, validates, normalizes, computes,
/// advances, or persists an interval — a backfill is the *caller* looping over
/// invocations with different intervals. The two endpoints are opaque strings
/// whose meaning is entirely the caller's; naming them `start` and `end` is a
/// convenience for the caller, **not** a claim that the framework treats one as
/// earlier than the other. **This is the boundary with "backfill orchestrator."**
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataInterval {
    start: String,
    end: String,
}

impl DataInterval {
    /// Record an opaque interval verbatim. The endpoints are stored **exactly**
    /// as supplied — reversed order, identical endpoints, empty content, and
    /// bytes nonsensical as a timestamp are all recorded unchanged, because no
    /// framework code path interprets them.
    #[must_use]
    pub fn new(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// The first opaque endpoint, exactly as supplied. "Start" is the caller's
    /// label; the framework attaches no ordering meaning to it.
    #[must_use]
    pub fn start(&self) -> &str {
        &self.start
    }

    /// The second opaque endpoint, exactly as supplied. "End" is the caller's
    /// label; the framework attaches no ordering meaning to it.
    #[must_use]
    pub fn end(&self) -> &str {
        &self.end
    }
}

/// The **read-only** cancellation signal a task observes (arch.md `### C8`,
/// `### C16`).
///
/// This is the **task-facing** half: it offers **only** observation
/// ([`is_cancelled`](Self::is_cancelled)) — there is deliberately **no** method to
/// cancel the run from here, consistent with C8's "no route back to the
/// scheduler." The run-scoped token and its per-attempt children, future-drop
/// cancellation of await-bound work, and cooperative-only marking of
/// blocking/compute work are wired by the runner (C14 / C16, T20 / T21 / T35) via
/// a [`CancellationSource`]; per the T2 async-runtime ADR the eventual backing is
/// `tokio_util::sync::CancellationToken`, but the type task authors see is this
/// dagr-owned wrapper, never a bare tokio type.
///
/// # T20/T35 seam
///
/// The internal representation here is a simple shared flag, enough to satisfy
/// C8's hand-constructability and observation contract with **no runtime**. When
/// the runner lands (T20/T35) the backing becomes the real cancellation token;
/// this task-facing surface — observe-only, no lever — does not change.
#[derive(Debug, Clone)]
pub struct CancellationSignal {
    flag: Arc<AtomicBool>,
    // The parent run token, present when this signal came from a per-attempt
    // child (C16 / T35): a task observes cancellation when its own attempt token
    // is cancelled OR the run token it descends from is cancelled.
    parent: Option<Arc<CancellationSource>>,
}

impl CancellationSignal {
    /// Whether cancellation has been signalled. This is the **only** thing a task
    /// may do with the signal: observe it and return promptly (recorded
    /// `cancelled`) or not (recorded `abandoned` — C16). There is no lever to
    /// cancel the run from the task side.
    ///
    /// A signal derived from a per-attempt [child](CancellationSource::child)
    /// observes cancellation when either its own attempt token or the run token it
    /// descends from has been cancelled — a run cancel reaches every attempt.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst) || self.parent.as_ref().is_some_and(|p| p.is_cancelled())
    }
}

/// The **runtime/test-side** handle that can *raise* a [`CancellationSignal`].
///
/// This is held by the runner (or a test), **never** handed to a task: the split
/// between this source and the observe-only [`CancellationSignal`] is exactly
/// what makes the task-facing side a read channel and not a lever. A test flips
/// cancellation with [`cancel`](Self::cancel) to exercise a task's observation of
/// it; the runner does the same on the cancellation path (C16).
///
/// # Run-scoped token with per-attempt children (C16 / T35)
///
/// A source is the **run-scoped token** the driver owns; [`child`](Self::child)
/// hands each spawned attempt its **per-attempt child**. Cancelling the run
/// ([`cancel`](Self::cancel) on the run source) cancels **every live child**
/// exactly once (the children observe the same flip), and a second cancel changes
/// nothing ([`is_cancelled`](Self::is_cancelled) is idempotent). Cancelling a
/// **child** ([`cancel`](Self::cancel) on the child — the per-attempt path a C12
/// timeout uses) cancels **only that child**: its siblings and the parent run
/// source stay uncancelled, so a single attempt's cancellation is never mistaken
/// for the run being cancelled.
#[derive(Debug, Clone, Default)]
pub struct CancellationSource {
    // This source's own flag: set by `cancel()` on this source, or observed set
    // because a parent's cancel propagated (see `parent`). A child observes
    // cancelled when *either* its own flag or any ancestor's flag is set, so a run
    // cancel reaches every child while a child cancel touches only its own flag.
    flag: Arc<AtomicBool>,
    // The parent run token, if this is a per-attempt child. A child is cancelled
    // when its own `flag` is set OR its `parent` is cancelled; the parent is
    // never reached back through here, so a child cancel cannot cancel the parent.
    parent: Option<Arc<CancellationSource>>,
}

impl CancellationSource {
    /// A fresh, uncancelled source.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A **per-attempt child** of this run source (C16 / T35). The child is
    /// cancelled when *this* source is cancelled (a run cancel reaches every live
    /// child) or when the child itself is cancelled; cancelling the child does
    /// **not** cancel this parent or any sibling. Any number of children may be
    /// derived; each is independent on its own flag but shares the parent's.
    #[must_use]
    pub fn child(&self) -> CancellationSource {
        CancellationSource {
            flag: Arc::new(AtomicBool::new(false)),
            parent: Some(Arc::new(self.clone())),
        }
    }

    /// Whether cancellation has been raised for this source — its own flag, or any
    /// ancestor's (a run cancel observed through a child). Idempotent: a second
    /// [`cancel`](Self::cancel) leaves this `true` with no further effect.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst) || self.parent.as_ref().is_some_and(|p| p.is_cancelled())
    }

    /// The observe-only [`CancellationSignal`] this source drives — the one a
    /// [`RunContext`] carries. Any number of signals may share one source; they
    /// all observe the same flip, including a parent run cancel observed through a
    /// per-attempt child.
    #[must_use]
    pub fn signal(&self) -> CancellationSignal {
        CancellationSignal {
            flag: Arc::clone(&self.flag),
            parent: self.parent.clone(),
        }
    }

    /// Raise cancellation on **this** source. Every [`CancellationSignal`] derived
    /// from this source — and, if this is the run source, every per-attempt
    /// [`child`](Self::child) — now observes cancellation. On a child, this cancels
    /// only that child; the parent run source and siblings are untouched.
    /// Idempotent.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }
}

/// Why the run was cancelled (arch.md `### C16`; C26 exit-code precedence).
///
/// The cancellation core (T35) records the **origin** of a cancellation so the
/// later exit-code logic (C26 / T55) can prefer *run failure over cancellation*:
/// a cancellation triggered by a failure under stop-on-first-failure must not mask
/// the failure, whereas an externally-originated interrupt with no run failure is
/// reported as a cancellation. This ticket only *records* the origin; it does not
/// own the exit-code mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CancellationOrigin {
    /// A node ended failure-like under [stop-on-first-failure](crate::flow::FailureMode)
    /// (C15 / T34), which routed through the cancellation core. The run failure
    /// wins over the cancellation in the C26 precedence.
    FailureUnderStop,
    /// An external interrupt (an operator/orchestrator termination signal). The
    /// **wiring** of an OS signal to this origin is **T36**; T35 records the origin
    /// value so the entry point exists and is exercised.
    ExternalInterrupt,
}

/// The **dagr-owned** logging span a task's attempt runs inside (arch.md
/// `### C8`, `### C25`).
///
/// Every attempt runs beneath a span carrying run / node / attempt identity, so
/// every line emitted under it is attributable without timestamp correlation
/// (C25). This is a dagr-owned handle (per the T2 ADR: context-exposed types are
/// dagr-owned wherever practical), carrying the identity the span is keyed on.
///
/// # C25 seam
///
/// The **subscriber integration** — structured-vs-human output, third-party line
/// capture, secret scrubbing on framework paths — is C25's, not this ticket's.
/// This type fixes only the span's *identity payload* and its placement on the
/// context; the tracing wiring lands with logging integration.
#[derive(Debug, Clone)]
pub struct LogSpan {
    run: RunId,
    node: NodeId,
    attempt: u32,
}

impl LogSpan {
    /// The run this span is attributed to.
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run
    }

    /// The node this span is attributed to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node
    }

    /// The attempt this span is attributed to.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// A value **marked secret** when placed in the resource registry (arch.md
/// `### C9`, "Secrets"): credentials, tokens, keys, or any material that must
/// never leak onto a framework-controlled output path.
///
/// # No `Debug` / `Display` path — by construction
///
/// `Secret<T>` implements **neither** [`Debug`](std::fmt::Debug) nor
/// [`Display`](std::fmt::Display), so the framework — which reaches values only
/// through those formatters — cannot render the wrapped material even by
/// accident. This is the *type-system* half of the C9 secret guarantee: a
/// framework `{:?}` or `{}` on a `Secret` (or on a registry holding one) fails to
/// **compile**, not merely at runtime. The compile-fail cases
/// `tests/ui/secret_no_debug.rs` and `tests/ui/secret_no_display.rs` pin it.
///
/// # The guarantee boundary (per C25)
///
/// The guarantee covers **framework-controlled** output paths. A task author who
/// pulls the inner value out with [`expose`](Self::expose) and formats it into
/// **their own** log line is **outside** the guarantee — dagr cannot scrub a
/// string the author built themselves. That boundary is stated in arch.md `### C25`;
/// end-to-end framework log-line redaction is T45's, and it builds on this
/// wrapper and the [`redacted`](Self::redacted) sentinel hook.
///
/// # Example
///
/// ```
/// use dagr_core::context::{ResourceRegistry, Secret};
///
/// // A newtype so two secrets of the same underlying type stay distinct.
/// struct ApiToken(Secret<String>);
///
/// let registry = ResourceRegistry::builder()
///     .register(ApiToken(Secret::new("s3cr3t".to_string())))
///     .expect("unambiguous")
///     .build();
///
/// // Authorized code exposes it deliberately; the framework never can via Debug.
/// let token = registry.get::<ApiToken>().unwrap();
/// assert_eq!(token.0.expose(), "s3cr3t");
/// ```
pub struct Secret<T> {
    inner: T,
}

impl<T> Secret<T> {
    /// Mark `value` as secret. The value is stored verbatim and only handed back
    /// through [`expose`](Self::expose); it can never reach a framework formatter.
    pub const fn new(value: T) -> Self {
        Self { inner: value }
    }

    /// Deliberately expose the wrapped value to **authorized** code. Calling this
    /// is the author's explicit act of stepping outside the redaction guarantee
    /// for this value (arch.md C25); the framework never calls it.
    pub const fn expose(&self) -> &T {
        &self.inner
    }

    /// The **sentinel** the framework substitutes for a secret on any output path
    /// it controls — the redaction hook C25 / T45 emits in place of the value. It
    /// is a fixed marker that contains none of the secret's bytes.
    #[must_use]
    #[allow(
        clippy::unused_self,
        reason = "the redaction marker is a property of the secret wrapper, invoked on an instance"
    )]
    pub fn redacted(&self) -> &'static str {
        "<redacted secret>"
    }
}

/// The error registry **construction** reports (arch.md `### C9`).
///
/// The registry keys resources by their concrete type, so registering a second
/// resource of the **literally identical** type is ambiguous — the framework
/// cannot know which one a `get::<T>()` should return. Rather than silently
/// replacing the first (a three-a.m. surprise) or keeping both (impossible under
/// a type key), construction fails here; two resources of the same *underlying*
/// type are kept distinct via the newtype pattern (see [`ResourceRegistry`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegistryError {
    /// A resource of this type was already registered — an ambiguous duplicate.
    /// Carries the offending type's name for the operator-facing message.
    Duplicate {
        /// The concrete type registered twice.
        type_name: &'static str,
    },
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate { type_name } => write!(
                f,
                "ambiguous resource registration: a resource of type `{type_name}` is already \
                 registered — distinguish two same-typed resources with newtype wrappers"
            ),
        }
    }
}

impl std::error::Error for RegistryError {}

/// The immutable, type-keyed **resource registry** (arch.md `### C9 · Resource
/// registry`) — dependency injection for long-lived external clients.
///
/// # What it is
///
/// A registry is built **once, in the developer's own `main`**, from clients the
/// developer constructed themselves — the framework fetches nothing from anywhere
/// to populate it, there is no lookup service, no per-task credential fetch, and
/// no network round trip. Once built it is **immutable** and cheaply
/// **shared for the whole run** (a clone is a shared handle, not a copy of the
/// contents); a task reaches it read-only through
/// [`RunContext::resources`](crate::context::RunContext::resources).
///
/// # Type-keyed, no string lookup (the C2 philosophy)
///
/// Resources are keyed by their **concrete type** and retrieved by type with
/// [`get`](Self::get) — no string key, no runtime type check on the happy path.
/// Registering a second resource of the **literally identical type** fails
/// construction as ambiguous ([`RegistryError::Duplicate`]) rather than silently
/// replacing the first.
///
/// # Newtype disambiguation (worked example)
///
/// Two resources of the *same underlying type* are distinguished by wrapping each
/// in a distinct **newtype** — the same no-string-lookup pattern C2 uses:
///
/// ```
/// use dagr_core::context::ResourceRegistry;
///
/// // Two HTTP clients of the same underlying type, kept distinct by newtype.
/// #[derive(Clone)]
/// struct HttpClient { base_url: String }
/// struct BillingClient(HttpClient);
/// struct AnalyticsClient(HttpClient);
///
/// let registry = ResourceRegistry::builder()
///     .register(BillingClient(HttpClient { base_url: "https://billing".into() }))
///     .expect("BillingClient is a distinct type")
///     .register(AnalyticsClient(HttpClient { base_url: "https://analytics".into() }))
///     .expect("AnalyticsClient is a distinct type despite the shared inner type")
///     .build();
///
/// assert_eq!(registry.get::<BillingClient>().unwrap().0.base_url, "https://billing");
/// assert_eq!(registry.get::<AnalyticsClient>().unwrap().0.base_url, "https://analytics");
/// ```
///
/// # Thread-safety bound and the owning-worker escape hatch
///
/// Stored resources are **`Send + Sync + 'static`** so the immutable registry can
/// be shared across the worker threads attempts run on. A client that is **not**
/// thread-safe is **not** registered directly (that fails to compile — see
/// `tests/ui/registry_non_send_resource.rs`); instead it is placed behind the
/// documented **owning-worker channel pattern**: one dedicated thread owns the
/// non-thread-safe client, and the `Send + Sync` handle registered here is a
/// channel sender other tasks use to reach it. dagr documents this pattern; it
/// implements no worker here (that is task-author guidance, not framework code).
///
/// # Secrets
///
/// Secret material is wrapped in [`Secret`] before registration, which has no
/// `Debug`/`Display` path, so the framework never renders it onto a
/// framework-controlled output path (the guarantee boundary is stated on
/// [`Secret`] and in arch.md C25).
///
/// # Backward-compatible empty registry (T16 seam)
///
/// [`ResourceRegistry::default`] yields the **honestly-empty** registry T16's
/// [`RunContext`] carries: [`get`](Self::get) is [`None`] for every type and
/// [`is_empty`](Self::is_empty) is `true`. The accessor signatures are unchanged
/// from the T16 seam, so every existing T16/T20/T24 caller keeps compiling and
/// passing.
#[derive(Clone, Default)]
pub struct ResourceRegistry {
    // Type-keyed store of long-lived clients. `Arc` so a clone is a cheap shared
    // handle (immutable, shared for the whole run — not a per-clone copy), and so
    // the registry stays `Send + Sync`. `Arc<dyn Any + Send + Sync>` erases the
    // concrete type behind the `TypeId` key; the downcast on `get` is infallible
    // by construction (the key is the value's own `TypeId`).
    resources: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl ResourceRegistry {
    /// Begin building a registry in the developer's own code. Register each
    /// resource with [`ResourceRegistryBuilder::register`], then
    /// [`build`](ResourceRegistryBuilder::build) the immutable registry.
    #[must_use]
    pub fn builder() -> ResourceRegistryBuilder {
        ResourceRegistryBuilder::default()
    }

    /// Retrieve the registered resource of type `R`, or [`None`] if no resource of
    /// that type was registered. Type-keyed: **no string lookup**, and the
    /// downcast is infallible by construction (the store keys each value under its
    /// own [`TypeId`]), so there is no runtime type check on the happy path.
    #[must_use]
    pub fn get<R: Any + Send + Sync>(&self) -> Option<&R> {
        // The store keys each value under its own `TypeId`, so when the key
        // `TypeId::of::<R>()` is present the value **is** an `R` and the downcast
        // succeeds — the single erasure boundary is infallible by construction.
        // `and_then` keeps `get` panic-free: the `downcast_ref` branch cannot be
        // `None` here, so no runtime type check can fail on the happy path.
        self.resources
            .get(&TypeId::of::<R>())
            .and_then(|erased| erased.downcast_ref::<R>())
    }

    /// Whether the registry holds no resources. The [default](Self::default) and a
    /// zero-registration [`build`](ResourceRegistryBuilder::build) are both empty
    /// (the T16-compatible honest-empty registry).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// The number of distinct resource types registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// **Bootstrap validation** (arch.md `### C9`): check this registry against the
    /// per-node declared [resource requirements](ResourceRequirements), *before*
    /// any node executes.
    ///
    /// `declarations` is the set of `(node, requirements)` pairs the pipeline
    /// declared (surfaced from C8 / T16). Validation collects **every** declared
    /// requirement whose resource type is **not** registered and, for each such
    /// missing type, **every** node that declared a requirement on it. If any
    /// requirement is unmet the result is [`Err`] carrying a [`BootstrapFailure`]
    /// — the **bootstrap-failure artifact**, distinct from an assembly failure,
    /// naming the missing resource and its requiring nodes, with **zero attempts
    /// recorded**. If every declared requirement is satisfied the result is
    /// [`Ok`] and execution may proceed.
    ///
    /// This produces the failure *value*; wiring it into the run/artifact emission
    /// (C20 / C22) and the driver bootstrap phase is a later ticket's — this
    /// ticket only produces it and asserts it is produced.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapFailure`] when at least one declared resource type is not
    /// registered.
    pub fn validate_requirements(
        &self,
        declarations: &[(NodeId, ResourceRequirements)],
    ) -> Result<(), BootstrapFailure> {
        // For each missing resource type, accumulate the requiring nodes in
        // declaration order (dedup-stable). Keyed by TypeId, ordered for a stable,
        // renderable error list.
        let mut missing: BTreeMap<TypeId, MissingResourceError> = BTreeMap::new();
        for (node, requirements) in declarations {
            for req in requirements.iter() {
                if self.resources.contains_key(&req.type_id()) {
                    continue;
                }
                let entry = missing
                    .entry(req.type_id())
                    .or_insert_with(|| MissingResourceError {
                        resource_type_name: req.type_name(),
                        requiring_nodes: Vec::new(),
                    });
                if !entry.requiring_nodes.contains(node) {
                    entry.requiring_nodes.push(*node);
                }
            }
        }

        if missing.is_empty() {
            return Ok(());
        }
        Err(BootstrapFailure {
            errors: missing.into_values().collect(),
        })
    }
}

// A hand-written `Debug` that never renders resource *values* (they are erased,
// and one may be a `Secret` with no Debug path anyway). Only the count and the
// registered type-ids' presence are shown — no framework-controlled path can
// leak a secret's bytes through the registry's own `Debug`.
impl std::fmt::Debug for ResourceRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceRegistry")
            .field("resource_count", &self.resources.len())
            .finish_non_exhaustive()
    }
}

/// The builder for a [`ResourceRegistry`] (arch.md `### C9`) — the **only** place
/// a resource is added, used once in the developer's `main`.
///
/// Each [`register`](Self::register) either accepts the resource (returning the
/// builder to chain) or rejects it as an **ambiguous duplicate**
/// ([`RegistryError::Duplicate`]) — the error path yields **no** builder, so a
/// rejected duplicate can never silently replace the first resource or leave a
/// half-built registry the caller proceeds with. [`build`](Self::build) consumes
/// the builder and freezes the immutable registry, after which there is no
/// mutation path.
#[derive(Default)]
pub struct ResourceRegistryBuilder {
    resources: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

// A hand-written `Debug` mirroring the registry's: never renders resource values
// (they are erased and one may be a `Secret`). This also lets `register`'s
// `Result<Self, _>` be `.expect`/`.expect_err`-ed in tests without leaking bytes.
impl std::fmt::Debug for ResourceRegistryBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceRegistryBuilder")
            .field("resource_count", &self.resources.len())
            .finish_non_exhaustive()
    }
}

impl ResourceRegistryBuilder {
    /// Register `resource`, keyed by its concrete type `R`.
    ///
    /// `R` must be **`Send + Sync + 'static`** (the stored-resource bound — a
    /// non-thread-safe client uses the owning-worker pattern documented on
    /// [`ResourceRegistry`], it is not registered directly).
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Duplicate`] if a resource of the **identical** type
    /// `R` was already registered — an ambiguity, since a type-keyed `get::<R>()`
    /// could not choose between them. The first resource is left untouched and the
    /// builder is consumed by the error, so the caller cannot proceed with an
    /// ambiguous registry.
    pub fn register<R: Any + Send + Sync>(mut self, resource: R) -> Result<Self, RegistryError> {
        let type_id = TypeId::of::<R>();
        if self.resources.contains_key(&type_id) {
            return Err(RegistryError::Duplicate {
                type_name: type_name::<R>(),
            });
        }
        self.resources.insert(type_id, Arc::new(resource));
        Ok(self)
    }

    /// Freeze the registered resources into the immutable [`ResourceRegistry`].
    /// After this there is no mutation path; the registry is shared read-only for
    /// the whole run.
    #[must_use]
    pub fn build(self) -> ResourceRegistry {
        ResourceRegistry {
            resources: Arc::new(self.resources),
        }
    }
}

/// The overall outcome a bootstrap phase records (arch.md `### C9`; the run's
/// shape). Distinct from an **assembly** failure: assembly is the pure pass (C7 /
/// T14), bootstrap is the fail-fast startup phase that validates the registry
/// against declared requirements *after* assembly and *before* any node runs.
///
/// This is the outcome the downstream artifact emitter (C20 / C22) renders; this
/// ticket produces the value and asserts it is produced, it does not render it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BootstrapOutcome {
    /// Bootstrap validation passed — every declared resource requirement is
    /// satisfied; execution may proceed.
    Succeeded,
    /// Bootstrap failed a fail-fast check (here: a missing declared resource)
    /// before any node executed. **Distinct** from an assembly failure.
    BootstrapFailed,
}

/// One missing declared resource, for the [bootstrap-failure
/// artifact](BootstrapFailure) (arch.md `### C9`): the resource type that was
/// declared but never registered, and **every** node that declared a requirement
/// on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingResourceError {
    resource_type_name: &'static str,
    requiring_nodes: Vec<NodeId>,
}

impl MissingResourceError {
    /// The name of the missing (declared-but-unregistered) resource type.
    #[must_use]
    pub fn resource_type_name(&self) -> &'static str {
        self.resource_type_name
    }

    /// Every node that declared a requirement on the missing resource — the exact
    /// set an operator needs to fix the run.
    #[must_use]
    pub fn requiring_nodes(&self) -> &[NodeId] {
        &self.requiring_nodes
    }
}

impl std::fmt::Display for MissingResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "resource `{}` is required but was never registered; required by {} node(s): {:?}",
            self.resource_type_name,
            self.requiring_nodes.len(),
            self.requiring_nodes
        )
    }
}

/// The **bootstrap-failure artifact** produced when bootstrap validation fails
/// (arch.md `### C9`): the fail-fast startup outcome, distinct from an assembly
/// failure, that names every missing resource and its requiring nodes and records
/// that **zero attempts** ran — no node executed. A downstream emitter (C20 /
/// C22) renders it; this ticket produces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapFailure {
    errors: Vec<MissingResourceError>,
}

impl BootstrapFailure {
    /// The bootstrap outcome — always [`BootstrapOutcome::BootstrapFailed`] for a
    /// failure value; distinct from an assembly failure.
    #[must_use]
    pub fn outcome(&self) -> BootstrapOutcome {
        BootstrapOutcome::BootstrapFailed
    }

    /// The resource-validation errors, one per missing resource type, each naming
    /// its requiring nodes.
    #[must_use]
    pub fn errors(&self) -> &[MissingResourceError] {
        &self.errors
    }

    /// The number of attempts recorded — **always zero** for a bootstrap failure,
    /// because bootstrap fails *before any node executes* (arch.md C9: never a
    /// mid-run surprise). The run also never hangs: this is a synchronous,
    /// terminating check.
    #[must_use]
    #[allow(
        clippy::unused_self,
        reason = "zero-attempts is a property of the bootstrap-failure artifact instance"
    )]
    pub fn attempts_recorded(&self) -> usize {
        0
    }
}

impl std::fmt::Display for BootstrapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bootstrap failed: {} unmet resource requirement(s)",
            self.errors.len()
        )?;
        for err in &self.errors {
            write!(f, "; {err}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BootstrapFailure {}

/// **Surface** the declared resource requirements as flat `(node, type-name)`
/// pairs (arch.md `### C9`, "Declared requirements appear in the graph artifact").
///
/// This exposes every declared requirement in a stable, renderable form so a
/// downstream **graph-artifact** test (C20) can assert they appear — independent
/// of whether they are satisfied. This ticket only *surfaces* them; rendering the
/// artifact is C20 / C22.
#[must_use]
pub fn surface_requirements(
    declarations: &[(NodeId, ResourceRequirements)],
) -> Vec<(NodeId, &'static str)> {
    let mut surfaced = Vec::new();
    for (node, requirements) in declarations {
        for req in requirements.iter() {
            surfaced.push((*node, req.type_name()));
        }
    }
    surfaced
}

// The **durable scratch store** (C18) and its error surface now live in
// [`crate::scratch`], landed by T53. They are re-exported here so the C8 context
// seam — `RunContext::scratch` returning `&ScratchStore` — keeps the exact type
// path every existing caller uses (`dagr_core::context::ScratchStore` /
// `ScratchError`), unchanged from the T16 seam.
pub use crate::scratch::{ScratchError, ScratchStore};

/// A node's **terminal state**, from arch.md's normative taxonomy (Vocabulary —
/// "Terminal states"). Every node ends a run in exactly one of these.
///
/// This ticket needs the taxonomy for the [teardown extension](CoveredNodeStates):
/// a teardown node reads the terminal states of the nodes it covers so cleanup
/// can no-op when setup never ran (C17). The names are the exact canonical ones;
/// the readiness tracker, failure policy, and run artifact (C11 / C15 / C22) that
/// *assign* these states are later tickets — this enum only carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TerminalState {
    /// The task returned a value; the slot was filled. *(success-like)*
    Succeeded,
    /// Permanent failure, retries exhausted, or a caught panic. *(failure-like)*
    Failed,
    /// The final attempt exceeded its per-attempt timeout. *(failure-like)*
    TimedOut,
    /// The task itself returned a deliberate skip (an *originated* skip).
    /// *(skip-like)*
    Skipped,
    /// Never ran because an upstream skip propagated to it. *(skip-like)*
    UpstreamSkipped,
    /// Never ran because its trigger rule can no longer be satisfied due to an
    /// upstream failure. *(failure-like)*
    UpstreamFailed,
    /// Observed the cancellation signal and returned promptly, or was never
    /// admitted after cancellation began. *(stop-like)*
    Cancelled,
    /// Was asked to cancel and never returned within the grace period; its thread
    /// was left behind. *(failure-like)*
    Abandoned,
    /// Not executed in this run; resume (C27) carried its prior success forward.
    /// *(success-like)*
    SatisfiedFromPrior,
}

/// The **teardown-only** view of covered nodes' terminal states (arch.md
/// `### C8`, `### C17`).
///
/// A teardown node's context additionally exposes the terminal states of the
/// nodes it covers, so cleanup can **no-op when setup never ran**. This type
/// defines the *shape* of that extension and is hand-constructable for tests;
/// the **runtime-side population** of covered states — teardown ordering, the
/// fresh uncancelled signal, the teardown deadline — is completed under **C17 /
/// T52**. A **non-teardown** context carries no [`CoveredNodeStates`] at all
/// ([`RunContext::covered_terminal_states`] returns [`None`]), which is how the
/// absence of a covered set is represented.
#[derive(Debug, Clone, Default)]
pub struct CoveredNodeStates {
    // Keyed by NodeId (Eq + Hash, not Ord — a HashMap, not a BTreeMap): this is a
    // keyed lookup a teardown does ("what state is the node I cover in?"), not a
    // rendered, order-sensitive collection.
    states: HashMap<NodeId, TerminalState>,
}

impl CoveredNodeStates {
    /// An empty covered-states set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a covered node's terminal state (builder-style). Used by C17 / T52
    /// to populate the set from the runtime, and by tests to hand-construct one.
    #[must_use]
    pub fn with(mut self, node: NodeId, state: TerminalState) -> Self {
        self.states.insert(node, state);
        self
    }

    /// The terminal state of a covered node, or [`None`] if this teardown does
    /// not cover that node (so cleanup can no-op — e.g. setup never ran).
    #[must_use]
    pub fn get(&self, node: NodeId) -> Option<TerminalState> {
        self.states.get(&node).copied()
    }

    /// The number of covered nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Whether no nodes are covered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
}

/// One declared resource requirement: a node's dependency on a resource *type*
/// (arch.md `### C9`). Carries the type's identity for validation and its
/// author-declared type name for rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceRequirement {
    type_id: TypeId,
    type_name: &'static str,
}

impl ResourceRequirement {
    /// The requirement for resource type `R`.
    #[must_use]
    pub fn of<R: Any>() -> Self {
        Self {
            type_id: TypeId::of::<R>(),
            type_name: type_name::<R>(),
        }
    }

    /// The required type's identity — what bootstrap (T30) keys registry
    /// validation on.
    #[must_use]
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// The required type's name, for rendering into the graph artifact (C20 /
    /// T30). Informational only — identity is [`type_id`](Self::type_id).
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }
}

/// The **resource-requirement declaration plumbing** (arch.md `### C9`): the set
/// of resource types a node declares it requires at registration.
///
/// This is the mechanism a node uses to record its required resource types so
/// **bootstrap (T30)** can validate the registry against the declared
/// requirements — a missing resource is a startup failure, never a mid-run
/// surprise — and so those declarations can later surface in the **graph artifact
/// (C20)**. This ticket lands only the *declaration* and its queryable form; the
/// registry itself, and the bootstrap validation against it, are **T30**.
///
/// A node declaring nothing reports an [empty](Self::is_empty) requirement set;
/// declarations are additive and do not affect a context's other fields.
#[derive(Debug, Clone, Default)]
pub struct ResourceRequirements {
    // Keyed by TypeId so declaring the same type twice is idempotent (a node
    // requiring a type "twice" requires it once). Ordered for stable rendering.
    required: BTreeMap<TypeId, ResourceRequirement>,
}

impl ResourceRequirements {
    /// An empty requirement set — the default for a node that declares nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare that the node requires resource type `R` (builder-style).
    /// Idempotent: declaring the same type twice records it once.
    #[must_use]
    pub fn require<R: Any>(mut self) -> Self {
        let req = ResourceRequirement::of::<R>();
        self.required.insert(req.type_id(), req);
        self
    }

    /// Whether the node declares it requires resource type `R`.
    #[must_use]
    pub fn requires<R: Any>(&self) -> bool {
        self.required.contains_key(&TypeId::of::<R>())
    }

    /// The number of distinct declared requirements.
    #[must_use]
    pub fn len(&self) -> usize {
        self.required.len()
    }

    /// Whether the node declares no requirements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.required.is_empty()
    }

    /// The declared requirements, in a stable order — the form bootstrap (T30)
    /// validates and a graph artifact (C20) renders.
    pub fn iter(&self) -> impl Iterator<Item = &ResourceRequirement> {
        self.required.values()
    }
}

/// The **read-only** handle every task invocation is told about the run it is
/// part of (arch.md `### C8 · Run context`).
///
/// See the [module docs](self) for the full contract: it carries run / pipeline /
/// node identity, the current attempt and the maximum, the run's parameters, an
/// optional [data interval](DataInterval), a [cancellation signal](CancellationSignal),
/// a [logging span](LogSpan), and the [registry](ResourceRegistry) /
/// [scratch](ScratchStore) accessors — and it exposes **only reads**, with no
/// route back to the scheduler. Build one by hand with [`RunContext::builder`] or
/// [`RunContext::for_test`].
#[derive(Debug, Clone)]
pub struct RunContext {
    run: RunId,
    pipeline: PipelineId,
    node: NodeId,
    attempt: u32,
    max_attempts: u32,
    parameters: Option<Arc<dyn Any + Send + Sync>>,
    data_interval: Option<DataInterval>,
    cancellation: CancellationSignal,
    span: LogSpan,
    resources: ResourceRegistry,
    scratch: ScratchStore,
    covered_terminal_states: Option<CoveredNodeStates>,
    temp_dir: Option<std::path::PathBuf>,
}

impl RunContext {
    /// Begin hand-constructing a context with the required identity fields. The
    /// remaining fields take sensible, spec-consistent defaults (attempt 1, max 1,
    /// no parameters, no data interval, a fresh uncancelled signal, empty seams,
    /// non-teardown) until set on the returned [`RunContextBuilder`].
    ///
    /// This is the C8 hand-construction path — **no runtime, no store, no
    /// registry, no clock, no network** — that feeds the single-task test kit
    /// (C28 / T60). The runtime constructs and threads the *real* context (T20 /
    /// C14); that is out of scope here.
    #[must_use]
    pub fn builder(run: RunId, pipeline: PipelineId, node: NodeId) -> RunContextBuilder {
        RunContextBuilder::new(run, pipeline, node)
    }

    /// A fully-populated context for exercising a single task in isolation, with
    /// **no arguments** and **no runtime running** (arch.md C8 / C28). Every field
    /// is present: recognizable placeholder identities, attempt 1 of 1, no
    /// parameters, no data interval, a fresh uncancelled signal, the honest
    /// registry/scratch seams, and no covered-states set (non-teardown).
    ///
    /// This is the seam T9's task tests already call and T60 builds on. For a
    /// context with specific field values, use [`RunContext::builder`].
    #[must_use]
    pub fn for_test() -> Self {
        Self::builder(
            RunId::new("test-run"),
            PipelineId::new("test-pipeline"),
            NodeId::from_name("test-node"),
        )
        .build()
    }

    /// The run's identity.
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run
    }

    /// The pipeline's identity.
    #[must_use]
    pub fn pipeline_id(&self) -> &PipelineId {
        &self.pipeline
    }

    /// This node's identity.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node
    }

    /// The current attempt number — carries the retry count in a form logs and
    /// artifacts consume (arch.md C8; it increments across retries, driven by the
    /// runner, C14 / T22). It is **not** fixed or defaulted-away: every
    /// invocation, including the first attempt of the first node, carries it.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The configured maximum number of attempts for this node.
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// The run's parameters, downcast to the caller's parameter type `P`, or
    /// [`None`] if no parameters were supplied or the requested type does not
    /// match what was supplied.
    ///
    /// Parameters are carried **opaquely**: the framework does not interpret them
    /// (they are parsed at bootstrap, after the pure assembly phase — C7 / C26)
    /// and this accessor only hands the task back the value it was given, by type.
    #[must_use]
    pub fn parameters<P: Any>(&self) -> Option<&P> {
        self.parameters.as_ref().and_then(|p| p.downcast_ref::<P>())
    }

    /// The run's optional [data interval](DataInterval), or [`None`] when none was
    /// supplied. **Caller-supplied and tool-opaque** — returned exactly as
    /// supplied; no framework code path interprets its contents (arch.md C8).
    #[must_use]
    pub fn data_interval(&self) -> Option<&DataInterval> {
        self.data_interval.as_ref()
    }

    /// The **observe-only** [cancellation signal](CancellationSignal). A task may
    /// observe it and return promptly; there is no lever here to cancel the run
    /// (arch.md C8: no route back to the scheduler).
    #[must_use]
    pub fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// The [logging span](LogSpan) this attempt runs inside (arch.md C8 / C25).
    #[must_use]
    pub fn span(&self) -> &LogSpan {
        &self.span
    }

    /// The [resource-registry accessor](ResourceRegistry) — a **stable seam**;
    /// the concrete registry (C9) lands with **T30** (see [`ResourceRegistry`]).
    #[must_use]
    pub fn resources(&self) -> &ResourceRegistry {
        &self.resources
    }

    /// The node's [durable scratch store](ScratchStore) (C18 / T53): a per-run,
    /// per-node key-value store of opaque bytes under the run store, with enforced
    /// cross-node isolation, atomic crash-safe writes, and a success-time cleanup
    /// hook. A value written on one attempt is readable on the next. A context
    /// built with **no run store** carries an honestly-unwired store that never
    /// pretends to persist (see [`ScratchStore`]).
    #[must_use]
    pub fn scratch(&self) -> &ScratchStore {
        &self.scratch
    }

    /// The terminal states of the nodes a **teardown** node covers, or [`None`]
    /// for a non-teardown context (arch.md C8 / C17). A teardown reads these so
    /// cleanup can no-op when setup never ran; the runtime-side population is
    /// finished under **C17 / T52** (see [`CoveredNodeStates`]).
    #[must_use]
    pub fn covered_terminal_states(&self) -> Option<&CoveredNodeStates> {
        self.covered_terminal_states.as_ref()
    }

    /// The run's **per-run temp directory** (arch.md `### C16`; C16/T36), or
    /// [`None`] when the context was hand-built with no run store (the C8 test
    /// path).
    ///
    /// Everything a task writes **locally** — scratch files, intermediates it
    /// materializes on the local filesystem before persisting a durable reference
    /// (C2 output ownership) — goes under this directory. The convention confines a
    /// run's local debris so it can be reclaimed: a cooperative task that observes
    /// cancellation within grace removes what it wrote here, and the whole directory
    /// is removed by the run's end or by the **next** invocation regardless of how
    /// the prior process ended (arch.md C16). The directory lives under the run-store
    /// base at `<base>/<pipeline>/<run-id>/tmp/`; the runtime creates it at bootstrap
    /// and threads it here. This is the *path*, not a handle — a task uses ordinary
    /// filesystem operations under it.
    #[must_use]
    pub fn temp_dir(&self) -> Option<&std::path::Path> {
        self.temp_dir.as_deref()
    }
}

/// The hand-construction builder for a [`RunContext`] (arch.md C8 / C28).
///
/// Obtained from [`RunContext::builder`]. Fields not set take sensible,
/// spec-consistent defaults; [`build`](Self::build) yields the immutable context.
/// This is the **no-runtime** path — nothing here touches the filesystem, the
/// clock, the network, or a registry — that a plain unit test and the single-task
/// test kit (T60) use to exercise a task in isolation.
#[derive(Debug, Clone)]
pub struct RunContextBuilder {
    run: RunId,
    pipeline: PipelineId,
    node: NodeId,
    attempt: u32,
    max_attempts: u32,
    parameters: Option<Arc<dyn Any + Send + Sync>>,
    data_interval: Option<DataInterval>,
    cancellation: Option<CancellationSignal>,
    resources: ResourceRegistry,
    scratch_root: Option<std::path::PathBuf>,
    covered_terminal_states: Option<CoveredNodeStates>,
    temp_dir: Option<std::path::PathBuf>,
}

impl RunContextBuilder {
    fn new(run: RunId, pipeline: PipelineId, node: NodeId) -> Self {
        Self {
            run,
            pipeline,
            node,
            attempt: 1,
            max_attempts: 1,
            parameters: None,
            data_interval: None,
            cancellation: None,
            resources: ResourceRegistry::default(),
            scratch_root: None,
            covered_terminal_states: None,
            temp_dir: None,
        }
    }

    /// Set the current attempt number (default 1).
    #[must_use]
    pub fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    /// Set the configured maximum number of attempts (default 1).
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Supply the run's parameters, carried opaquely and read back by type via
    /// [`RunContext::parameters`]. The value must be `Send + Sync + 'static` so
    /// the context can be shared with the worker driving the attempt.
    #[must_use]
    pub fn parameters(mut self, parameters: Arc<dyn Any + Send + Sync>) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Supply the run's opaque [data interval](DataInterval). Omit it for a run
    /// with no interval (the default), which [`RunContext::data_interval`] reports
    /// as [`None`].
    #[must_use]
    pub fn data_interval(mut self, interval: DataInterval) -> Self {
        self.data_interval = Some(interval);
        self
    }

    /// Supply the [cancellation signal](CancellationSignal) a task observes,
    /// obtained from a [`CancellationSource`] the caller (runtime or test) holds.
    /// Omit it for a fresh, never-cancelled signal (the default).
    #[must_use]
    pub fn cancellation(mut self, signal: CancellationSignal) -> Self {
        self.cancellation = Some(signal);
        self
    }

    /// Supply the [resource registry](ResourceRegistry) (C9) this run shares with
    /// every task. Omit it for the honest-empty registry (the default), which
    /// [`RunContext::resources`] reports as [empty](ResourceRegistry::is_empty).
    /// The runtime threads the real registry here at bootstrap (T-later); a test
    /// hands in one built by [`ResourceRegistry::builder`], which is how a task is
    /// exercised against a fake resource with no change to the task code.
    #[must_use]
    pub fn resources(mut self, resources: ResourceRegistry) -> Self {
        self.resources = resources;
        self
    }

    /// Supply the **run-store base** under which this context's node reaches its
    /// [durable scratch store](ScratchStore) (C18 / T53). The store resolves to
    /// `<base>/<pipeline>/<run-id>/scratch/<node>/` from the run / pipeline / node
    /// identity this context already carries (T0.6 §3, §9).
    ///
    /// The runtime threads the resolved run-store base here at bootstrap; a test
    /// hands in a temp base to exercise the real store with **no runtime running**
    /// (the C8 single-task path). Omit it for the honestly-unwired store (the
    /// default), whose reads report absent-of-store and whose writes report a
    /// retry-eligible fault — it never pretends to persist.
    #[must_use]
    pub fn scratch_root(mut self, base: std::path::PathBuf) -> Self {
        self.scratch_root = Some(base);
        self
    }

    /// Mark this as a **teardown** context by supplying the terminal states of the
    /// nodes it covers (arch.md C17). Omit it for a non-teardown context, which
    /// [`RunContext::covered_terminal_states`] reports as [`None`].
    #[must_use]
    pub fn covered_terminal_states(mut self, covered: CoveredNodeStates) -> Self {
        self.covered_terminal_states = Some(covered);
        self
    }

    /// Supply the run's **per-run temp directory** (arch.md C16), reachable by a
    /// task through [`RunContext::temp_dir`]. The runtime threads the real
    /// `<base>/<pipeline>/<run-id>/tmp/` path here at bootstrap (C16 / T36); omit it
    /// for the no-run-store hand-built context (the C8 test path), which reports
    /// [`None`].
    #[must_use]
    pub fn temp_dir(mut self, temp_dir: std::path::PathBuf) -> Self {
        self.temp_dir = Some(temp_dir);
        self
    }

    /// Build the immutable [`RunContext`]. Every field is populated: the required
    /// identities, the attempt/max, and — for any field not explicitly set — its
    /// spec-consistent default (no parameters, no data interval, a fresh
    /// uncancelled signal, honest empty seams, non-teardown). The span is derived
    /// from the run/node/attempt identity.
    #[must_use]
    pub fn build(self) -> RunContext {
        let cancellation = self
            .cancellation
            .unwrap_or_else(|| CancellationSource::new().signal());
        let span = LogSpan {
            run: self.run.clone(),
            node: self.node,
            attempt: self.attempt,
        };
        // Resolve the node's durable scratch store from the run/pipeline/node
        // identity and the supplied run-store base (T0.6 §3, §9). With no base
        // (the C8 hand-built path) the store is honestly unwired — it never
        // pretends to persist.
        let scratch = match &self.scratch_root {
            Some(base) => ScratchStore::for_node(base, &self.pipeline, &self.run, self.node),
            None => ScratchStore::unwired(),
        };
        RunContext {
            run: self.run,
            pipeline: self.pipeline,
            node: self.node,
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            parameters: self.parameters,
            data_interval: self.data_interval,
            cancellation,
            span,
            resources: self.resources,
            scratch,
            covered_terminal_states: self.covered_terminal_states,
            temp_dir: self.temp_dir,
        }
    }
}
