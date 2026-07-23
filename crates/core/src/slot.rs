//! The C10 **output slot** — where a node's produced value lives between its
//! production and its last consumption (arch.md `### C10 · Output slot`).
//!
//! Each node owns **exactly one** slot, typed to that node's output, empty until
//! the node succeeds. Downstream consumers hold a **direct, typed reference** to
//! that slot ([`SlotRef<T>`]), established at assembly time (from T14), so a read
//! is a direct access with **no map lookup and no runtime type check**. This
//! module is the storage substrate the attempt runner (T20) fills and the
//! bounded-memory chain test (T26) exercises; it is a typed container plus a
//! delivery discipline, **not** a scheduler.
//!
//! # Type-erasure strategy (the Open question, resolved)
//!
//! Heterogeneous slots — one per node, each a different output type — must be
//! storable together (keyed by node) while every read stays lookup-free and
//! type-check-free. The strategy, inherited from the **T0.2 output-ownership
//! ADR** (§1: *"wrap once, hand out cheap clones"*) and the dagx erasure-boundary
//! prior art, is:
//!
//! - A produced value is **`Arc`-wrapped exactly once** at fill time and stored
//!   behind the single crate-internal erasure boundary as
//!   `Arc<dyn Any + Send + Sync>`. That is the *only* place a type is erased, so
//!   heterogeneous slots (`Slot<Payload>`, `Slot<Counter>`, …) can be held in one
//!   homogeneous collection keyed by [`NodeId`] by a later runner.
//! - Every **consumer reference is typed** ([`SlotRef<T>`]): it can be minted
//!   **only** from a [`Slot<T>`] of the *same* `T` (via [`Slot::shared_ref`] and
//!   friends), so the concrete output type is recovered by construction. A read
//!   through a `SlotRef<T>` performs the one `downcast` the erasure boundary
//!   requires, but that downcast is **infallible by construction**: the reference
//!   type parameter *is* the stored type, proven when the reference was minted.
//!   There is no name/index/string-key lookup on the read path (the reference is
//!   a direct `Arc` link), and no *fallible* runtime type-tag branch a consumer
//!   could ever hit — a **mismatched-type wiring is impossible to construct**,
//!   not caught at read time.
//!
//! ## Safety argument for the downcast
//!
//! A `SlotRef<T>` is produced only by `Slot::<T>::*_ref`, which stamps the same
//! `T`. The value behind the erasure boundary was `Arc`-wrapped from a `T` in
//! `Slot::<T>::fill`. Therefore the `TypeId` of the stored value always equals
//! `TypeId::of::<T>()` for the reference's `T`, and the downcast is a total
//! function that never returns `None` for a filled slot. The single `expect` on
//! the downcast is thus a *framework-defect* assertion (it can only fire if this
//! module's own invariant were violated), in exactly the same spirit as the
//! loud read-before-fill defect below — never a task-visible error and never a
//! path a correctly-wired graph reaches. No `unsafe` is used anywhere.
//!
//! # Delivery: the three T0.2 modes
//!
//! Delivery to a consumer is one of the three modes the T0.2 ADR locked, decided
//! per edge at assembly and recorded on the [`ReceiveMode`](crate::binding::ReceiveMode)
//! this module *honours* (it does not adjudicate — that is assembly's job, T14):
//!
//! - **sole-consumer-owns** ([`Slot::owned_ref`] → [`ConsumerLease::take`]): the
//!   value is **moved out** of the slot; after the move the framework has no copy
//!   left. Works on a **non-`Clone`** output.
//! - **multi-consumer-shared-read** ([`Slot::shared_ref`] → [`SlotRef::read`] /
//!   [`ConsumerLease::read`]): each consumer receives an `Arc<T>` clone (O(1))
//!   giving `&T` read access for the duration of its attempt; no consumer can
//!   move or mutate the value. Works on a **non-`Clone`** output.
//! - **per-edge clone-on-read** ([`Slot::clone_on_read_ref`] →
//!   [`SlotRef::clone_value`]): each attempt receives a **fresh** `T` via
//!   `T::clone`; the only mode that demands `T: Clone`.
//!
//! # Release discipline (zombie-aware)
//!
//! Each slot knows how many consumers remain. The value is released only when
//! **every consumer has reached a terminal state AND every consumer's closure
//! has actually returned** — *not* when the last one has read it, because a
//! shared consumer that read the value and then failed a retry-eligible attempt
//! must find its input still there next time. Release is gated on the **closure
//! return** (the [`ConsumerLease`] guard dropping), not on the terminal-state
//! decision ([`ConsumerLease::mark_terminal`]): an **abandoned-but-running
//! (zombie)** consumer keeps its read access and its counted residency until its
//! closure returns (arch.md C10; T0.2 ADR §7). A **retained** node keeps its
//! value until run end regardless of consumers, redeemable afterward via
//! [`RedemptionHandle::redeem`].
//!
//! # Residency accounting (single-count, honest)
//!
//! A slot's value is counted **once** against the memory pool — the producer's
//! declared **output residency** transfers from the producing attempt into the
//! slot at fill time and is released **exactly once** at *actual* slot release
//! (which waits for zombies), never once per consumer. The [`ResidencyLedger`]
//! is the accounting hook the memory pool (C12) and the run artifact (C23)
//! consume, including [peak](ResidencyLedger::peak) measured residency. "Memory
//! reclaimed" means returned to the allocator (the stored `Arc` is dropped), not
//! necessarily to the OS.
//!
//! # What lives elsewhere
//!
//! - **Filling the slot from a real attempt outcome** and emitting attempt
//!   records is the runner (T20 / C14); this module exposes the fill/read/release
//!   surface it drives.
//! - **Timeout classification, abandonment decisions, and permit-release timing**
//!   are C14/C12 (T21, the T0.3 spike); this module only honours the
//!   residency/reachability pinning a zombie consumer imposes.
//! - **Memory-pool capacity, admission ordering**, and the **run artifact's
//!   rendered numbers** are C12/C23; this module only exposes the accounting
//!   hooks they consume.
//! - **Durable/addressable outputs and rehydration** are C27; in-memory slots
//!   deliberately cannot be rehydrated, and nothing here adds durability.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::handle::NodeId;

/// The **residency accounting hook** the memory pool (C12) and run artifact (C23)
/// consume: the current counted output residency and the peak observed over the
/// run (arch.md C10 *Memory accounting*).
///
/// Output residency is counted **once per value** (not per consumer) — a slot's
/// declared residency is added when the slot is filled and removed exactly once
/// when the slot actually releases (after every consumer is terminal-and-returned,
/// or at run end for a retained value). Because a zombie consumer holds release
/// open, the ledger never regains capacity for bytes a leftover thread still
/// pins.
///
/// Shared across every slot in a run via an [`Arc`]; hand one to each slot at
/// construction. It is internally synchronized (atomics), so slots on different
/// threads charge it safely.
#[derive(Debug)]
pub struct ResidencyLedger {
    current: AtomicU64,
    peak: AtomicU64,
}

impl ResidencyLedger {
    /// A fresh ledger with zero current and zero peak residency, ready to share
    /// across a run's slots.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    /// The current counted output residency in bytes — the sum of the declared
    /// residency of every slot that is filled-and-not-yet-released. This is the
    /// live figure the admission controller (C12) reads.
    #[must_use]
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }

    /// The **peak** measured output residency in bytes — the maximum concurrent
    /// counted residency observed since this ledger was created. This is the
    /// figure the run artifact (C23) folds as *peak measured slot residency*.
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::SeqCst)
    }

    /// Charge `bytes` of residency (a slot was filled). Updates the peak.
    ///
    /// `pub(crate)` so the C12 admission controller ([`crate::admission`]) can
    /// mint an output-residency **slot lease** against the same shared ledger a
    /// slot fills through (the working-vs-residency split, C12/C10): the transfer
    /// of residency from the producing attempt to the output slot charges this
    /// ledger, and the pool's counted memory folds it in. Slots charge it from
    /// `Slot::fill`; the admission controller charges it at the residency transfer.
    pub(crate) fn charge(&self, bytes: u64) {
        let new = self.current.fetch_add(bytes, Ordering::SeqCst) + bytes;
        // Raise the peak to at least the new current (monotone, race-safe).
        self.peak.fetch_max(new, Ordering::SeqCst);
    }

    /// Release `bytes` of residency (a slot actually released). Idempotence is the
    /// caller's (a slot releases its residency exactly once).
    ///
    /// `pub(crate)` for the same reason as [`charge`](Self::charge): the admission
    /// controller's slot lease drops when the slot actually releases (per C10,
    /// after every consumer — including a zombie consumer — has returned), which
    /// returns those bytes to the memory pool.
    pub(crate) fn release(&self, bytes: u64) {
        self.current.fetch_sub(bytes, Ordering::SeqCst);
    }
}

/// The refusal returned when a [`Slot`] that is already filled is filled again —
/// the **once-writable** invariant (arch.md C10). It carries the **rejected**
/// value back so the caller can discard it; the slot's original value is
/// unchanged.
#[derive(Debug)]
pub struct FillError<T> {
    rejected: T,
}

impl<T> FillError<T> {
    /// The value whose fill was refused, handed back so the caller owns it. The
    /// slot's original value is untouched.
    pub fn rejected(self) -> T {
        self.rejected
    }
}

impl<T> std::fmt::Display for FillError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "output slot is already filled; a slot is once-writable (the second fill was refused)"
        )
    }
}

impl<T: std::fmt::Debug> std::error::Error for FillError<T> {}

/// Why a post-run [redemption](RedemptionHandle::redeem) found no value —
/// distinguishing a **retained** value (redeemable) from the two non-redeemable
/// cases (arch.md C10: *"released ones are not"* redeemable).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedeemError {
    /// The node was **not retained** and its value was **released** after its
    /// last consumer returned. Released values are not redeemable — distinct from
    /// a value that was never produced.
    Released,
    /// The node's slot was **never filled** (the producing attempt never
    /// succeeded), so there is no value to redeem. Distinct from a *released*
    /// value and from the read-before-fill defect (which is a framework defect
    /// that panics; this is an ordinary post-run query result).
    NeverFilled,
}

impl std::fmt::Display for RedeemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Released => write!(
                f,
                "no value to redeem: the node was not retained and its value was released"
            ),
            Self::NeverFilled => write!(
                f,
                "no value to redeem: the node's slot was never filled (it did not succeed)"
            ),
        }
    }
}

impl std::error::Error for RedeemError {}

/// What the slot currently holds behind the type-erasure boundary. The value is
/// `Arc`-wrapped exactly once (T0.2 ADR §1) so shared-read fan-out is O(1) and
/// the same value is stored regardless of consumer count (single-count
/// residency).
enum Content {
    /// Never filled: reading is a framework defect (read-before-fill).
    Empty,
    /// Filled: the value, erased once behind `Arc<dyn Any + Send + Sync>`.
    Filled(Arc<dyn Any + Send + Sync>),
    /// Released: the value was reclaimed (non-retained, last consumer returned).
    /// Distinct from `Empty` so redemption can tell "released" from "never
    /// filled".
    Released,
}

/// The mutable interior of a slot, shared between the [`Slot`] handle, every
/// [`SlotRef`], and the [`RedemptionHandle`] via an [`Arc`]. Guarded by one
/// [`Mutex`]: slot operations are infrequent, coarse, and correctness-critical,
/// so a single lock is the honest, deadlock-free choice (no lock is held across
/// user code — a lease holds no lock, only a count).
struct Inner {
    /// The offending node's identity — named in the read-before-fill defect.
    node: NodeId,
    /// The offending node's registration name — the human-facing identity the
    /// read-before-fill defect message must contain.
    name: String,
    /// Whether this node is `retained`: its value survives to run end and is
    /// redeemable afterward, never released by consumer completion.
    retained: bool,
    /// The declared output residency in bytes, charged once at fill.
    residency: u64,
    /// The shared run-wide residency ledger (C12/C23 accounting hook).
    ledger: Arc<ResidencyLedger>,
    /// What the slot holds.
    content: Content,
    /// How many consumer closures have **not yet returned** (leases still open).
    /// Release is gated on this reaching zero *and* every consumer being terminal.
    open_leases: u32,
    /// How many consumers have reached a terminal state. Release requires this to
    /// reach the total consumer count.
    terminal_count: u32,
    /// How many consumers have returned (leases opened and then dropped). Release
    /// requires this to reach the total consumer count — the closure-return gate.
    returned_count: u32,
    /// The exact total consumer count (from T14). Release needs both
    /// `terminal_count` and `returned_count` to reach this.
    total_consumers: u32,
    /// Whether the residency has already been released (release-exactly-once).
    residency_released: bool,
}

impl Inner {
    /// Attempt to release the slot's value and residency if the discipline
    /// allows: **every** consumer terminal AND **every** consumer's closure
    /// returned, and the node is not retained. Idempotent and release-once.
    fn try_release(&mut self) {
        if self.retained {
            // A retained value survives to run end; consumer completion never
            // releases it. Residency is counted through run end.
            return;
        }
        if !matches!(self.content, Content::Filled(_)) {
            return;
        }
        let all_terminal = self.terminal_count >= self.total_consumers;
        let all_returned = self.returned_count >= self.total_consumers && self.open_leases == 0;
        if all_terminal && all_returned {
            self.reclaim();
        }
    }

    /// Reclaim the value (drop the stored `Arc`) and release its residency once.
    fn reclaim(&mut self) {
        self.content = Content::Released;
        if !self.residency_released {
            self.ledger.release(self.residency);
            self.residency_released = true;
        }
    }
}

/// A typed, once-writable **output slot** for one node's output (arch.md C10).
///
/// Created for a node with its [identity](NodeId), name, exact consumer count
/// (from T14), `retained` flag, declared output residency, and the shared
/// [`ResidencyLedger`]. Empty until [`fill`](Slot::fill)ed; a second fill is
/// refused. Consumers are wired at assembly time by minting a typed
/// [`SlotRef<T>`] with [`shared_ref`](Slot::shared_ref) /
/// [`owned_ref`](Slot::owned_ref) / [`clone_on_read_ref`](Slot::clone_on_read_ref).
///
/// The `Slot<T>` handle, its refs, and its redemption handle share one interior
/// via [`Arc`], so any of them observes fills and releases.
pub struct Slot<T> {
    inner: Arc<Mutex<Inner>>,
    _ty: std::marker::PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> Slot<T> {
    /// Create the single output slot for a node.
    ///
    /// - `node` / `name`: the node's identity and registration name (the name is
    ///   what the read-before-fill defect message contains).
    /// - `consumers`: the **exact** consumer count precomputed at assembly (T14).
    ///   Release waits until this many consumers are terminal-and-returned.
    /// - `retained`: whether the node's output is retained until run end (C5/C10).
    /// - `residency`: the declared output residency in bytes, charged once at
    ///   fill and released once at actual release.
    /// - `ledger`: the shared run-wide residency accounting hook (C12/C23).
    #[must_use]
    pub fn new(
        node: NodeId,
        name: impl Into<String>,
        consumers: u32,
        retained: bool,
        residency: u64,
        ledger: Arc<ResidencyLedger>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                node,
                name: name.into(),
                retained,
                residency,
                ledger,
                content: Content::Empty,
                open_leases: 0,
                terminal_count: 0,
                returned_count: 0,
                total_consumers: consumers,
                residency_released: false,
            })),
            _ty: std::marker::PhantomData,
        }
    }

    /// The identity of the node that owns this slot.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.lock().node
    }

    /// **Fill** the slot with the produced value — the once-writable write the
    /// runner (T20) performs on a node's success. The value is `Arc`-wrapped once
    /// and its declared residency is charged to the ledger (single-count).
    ///
    /// # Errors
    ///
    /// Returns [`FillError`] if the slot is already filled (the once-writable
    /// invariant); the rejected value is handed back and the original is
    /// unchanged. Filling a released slot is likewise refused.
    pub fn fill(&self, value: T) -> Result<(), FillError<T>> {
        let mut inner = self.lock();
        match inner.content {
            Content::Empty => {
                inner.content = Content::Filled(Arc::new(value));
                let residency = inner.residency;
                inner.ledger.charge(residency);
                Ok(())
            }
            Content::Filled(_) | Content::Released => Err(FillError { rejected: value }),
        }
    }

    /// Whether the slot currently **holds a value** (filled and not yet
    /// released). `false` before the first fill and after release.
    #[must_use]
    pub fn is_filled(&self) -> bool {
        matches!(self.lock().content, Content::Filled(_))
    }

    /// Mint a **shared-read** consumer reference (T0.2 multi-consumer-shared-read):
    /// the consumer reads the value as an `Arc<T>` for the duration of its
    /// attempt. The reference is a **direct typed link** — no lookup.
    #[must_use]
    pub fn shared_ref(&self) -> SlotRef<T> {
        SlotRef {
            inner: Arc::clone(&self.inner),
            mode: DeliveryMode::Shared,
            _ty: std::marker::PhantomData,
        }
    }

    /// Mint a **sole-consumer-owns** consumer reference (T0.2 sole-consumer-owns):
    /// the consumer takes the value by move via [`ConsumerLease::take`]. Legal
    /// only when this node has exactly one consumer (assembly enforces that, T14);
    /// this module honours the mode.
    #[must_use]
    pub fn owned_ref(&self) -> SlotRef<T> {
        SlotRef {
            inner: Arc::clone(&self.inner),
            mode: DeliveryMode::Owned,
            _ty: std::marker::PhantomData,
        }
    }

    /// Mint a **clone-on-read** consumer reference (T0.2 per-edge clone-on-read):
    /// each attempt receives a fresh `T` via [`SlotRef::clone_value`]. Requires
    /// `T: Clone` (the only mode that does).
    #[must_use]
    pub fn clone_on_read_ref(&self) -> SlotRef<T> {
        SlotRef {
            inner: Arc::clone(&self.inner),
            mode: DeliveryMode::CloneOnRead,
            _ty: std::marker::PhantomData,
        }
    }

    /// Mint the **redemption handle** for a `retained` node: exchanged for the
    /// value once the run has ended (arch.md C10). For a non-retained node the
    /// handle's [`redeem`](RedemptionHandle::redeem) reports the value as
    /// [`Released`](RedeemError::Released) (or [`NeverFilled`](RedeemError::NeverFilled)).
    #[must_use]
    pub fn redemption_handle(&self) -> RedemptionHandle<T> {
        RedemptionHandle {
            inner: Arc::clone(&self.inner),
            _ty: std::marker::PhantomData,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Which T0.2 delivery mode a [`SlotRef`] was minted for — the
/// [`ReceiveMode`](crate::binding::ReceiveMode) its edge declared, carried on the
/// reference so the runner (T20) knows which delivery method to call
/// ([`read`](SlotRef::read) / [`enter`](SlotRef::enter)+[`take`](ConsumerLease::take)
/// / [`clone_value`](SlotRef::clone_value)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMode {
    /// Sole-consumer-owns: the value is moved out via [`ConsumerLease::take`].
    Owned,
    /// Multi-consumer-shared-read: an `Arc<T>` clone via [`SlotRef::read`].
    Shared,
    /// Per-edge clone-on-read: a fresh `T` per attempt via
    /// [`SlotRef::clone_value`].
    CloneOnRead,
}

/// A **typed consumer reference** to an upstream node's output slot, established
/// at assembly time (arch.md C10; from T14). It carries the concrete output type
/// `T`, so a read is a **direct access with no map lookup and no runtime type
/// check** — a mismatched-type wiring is impossible to construct (see the [module
/// docs](self)).
///
/// A `SlotRef<T>` can be minted **only** from a [`Slot<T>`] of the same `T`, via
/// [`Slot::shared_ref`] / [`Slot::owned_ref`] / [`Slot::clone_on_read_ref`], so
/// the downcast at the erasure boundary is infallible by construction.
pub struct SlotRef<T> {
    inner: Arc<Mutex<Inner>>,
    mode: DeliveryMode,
    _ty: std::marker::PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> SlotRef<T> {
    /// The [`DeliveryMode`] this reference was minted for — the T0.2 receive mode
    /// its edge declared. The runner (T20) reads it to know whether to deliver by
    /// [`read`](SlotRef::read) (shared), [`take`](ConsumerLease::take) (owned), or
    /// [`clone_value`](SlotRef::clone_value) (clone-on-read).
    #[must_use]
    pub fn delivery_mode(&self) -> DeliveryMode {
        self.mode
    }

    /// Read the value as a shared `Arc<T>` **outside** a consumer lease — the
    /// direct read a shared consumer performs. Cheap (`Arc` clone), lookup-free.
    ///
    /// # Panics
    ///
    /// Panics loudly — a **framework defect** naming the offending node — if the
    /// slot has not been filled (read-before-fill; arch.md C10). This is never a
    /// task error and never a path a correctly-wired graph reaches: a data edge
    /// implies upstream success, so the slot is filled before any consumer reads.
    #[must_use]
    pub fn read(&self) -> Arc<T> {
        let inner = lock(&self.inner);
        read_arc::<T>(&inner)
    }

    /// Read the value as a fresh clone (clone-on-read mode). Each call yields an
    /// independent `T`; requires `T: Clone`.
    ///
    /// # Panics
    ///
    /// Panics loudly (framework defect, naming the node) if the slot is unfilled.
    #[must_use]
    pub fn clone_value(&self) -> T
    where
        T: Clone,
    {
        let inner = lock(&self.inner);
        (*read_arc::<T>(&inner)).clone()
    }

    /// **Enter** the consumer's attempt: open a lease that pins the value for the
    /// duration of this closure and, on drop, records the closure return. The
    /// returned [`ConsumerLease`] is how release is gated on the closure actually
    /// returning (zombie-aware): while the lease is alive the value stays
    /// reachable and its residency stays counted, even after
    /// [`mark_terminal`](ConsumerLease::mark_terminal).
    #[must_use]
    pub fn enter(&self) -> ConsumerLease<T> {
        {
            let mut inner = lock(&self.inner);
            inner.open_leases += 1;
        }
        ConsumerLease {
            inner: Arc::clone(&self.inner),
            marked_terminal: false,
            _ty: std::marker::PhantomData,
        }
    }
}

/// A **consumer lease** over an upstream slot for the duration of one consuming
/// attempt's closure (arch.md C10). Holding it keeps the value reachable and its
/// residency counted; dropping it records the **closure return** — the second,
/// zombie-critical half of the release gate.
///
/// A lease delivers the value through exactly the T0.2 mode its edge declared:
/// [`read`](ConsumerLease::read) (shared), [`take`](ConsumerLease::take) (owned
/// move), or a clone via the reference's [`clone_value`](SlotRef::clone_value).
/// [`mark_terminal`](ConsumerLease::mark_terminal) records the terminal-state
/// decision **without** releasing the value — release still waits for the drop.
pub struct ConsumerLease<T> {
    inner: Arc<Mutex<Inner>>,
    marked_terminal: bool,
    _ty: std::marker::PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> ConsumerLease<T> {
    /// Read the leased value as a shared `Arc<T>` (shared-read mode). Lookup-free.
    ///
    /// # Panics
    ///
    /// Panics loudly (framework defect, naming the node) on read-before-fill.
    #[must_use]
    pub fn read(&self) -> Arc<T> {
        let inner = lock(&self.inner);
        read_arc::<T>(&inner)
    }

    /// **Take** the value by move (sole-consumer-owns mode): the value leaves the
    /// slot and the framework keeps no copy. Consumes the lease (a value can be
    /// moved out only once). The slot's content becomes released; residency is
    /// released when this lease's return completes.
    ///
    /// # Panics
    ///
    /// Panics loudly (framework defect, naming the node) on take-before-fill, or
    /// if the value was already moved out (a sole owner takes exactly once).
    #[must_use]
    pub fn take(mut self) -> T {
        let value: T;
        {
            let mut inner = lock(&self.inner);
            let taken = std::mem::replace(&mut inner.content, Content::Released);
            match taken {
                Content::Filled(arc) => {
                    // Recover the concrete `Arc<T>` at the single erasure boundary
                    // — infallible by construction (the reference's `T` is the
                    // stored type) — then move the value out. The sole owner is
                    // provably the only strong holder (owned mode is legal only for
                    // a single-consumer slot, T14), so `try_unwrap` succeeds.
                    let typed: Arc<T> = arc.downcast::<T>().unwrap_or_else(|_| {
                        unreachable!(
                            "output slot type-erasure invariant violated on take (framework \
                             defect): the stored value's type did not match the consumer \
                             reference's type"
                        )
                    });
                    value = Arc::try_unwrap(typed).unwrap_or_else(|_| {
                        unreachable!(
                            "sole-consumer-owns slot had more than one strong holder; this is a \
                             framework defect (owned mode requires exactly one consumer, \
                             enforced at assembly)"
                        )
                    });
                    // Owned delivery reclaims residency once the closure returns.
                    if !inner.residency_released {
                        inner.ledger.release(inner.residency);
                        inner.residency_released = true;
                    }
                }
                Content::Empty => panic!(
                    "read-before-fill (framework defect): node `{}` output slot was taken \
                     before it was filled",
                    inner.name
                ),
                Content::Released => panic!(
                    "output slot for node `{}` was already consumed (framework defect: a \
                     sole owner takes its value exactly once)",
                    inner.name
                ),
            }
            // Taking the value IS this consumer's terminal decision; record it in
            // the interior so the count is honest. The closure-return half is
            // recorded when the lease `drop`s (it is consumed by this method).
            if !self.marked_terminal {
                inner.terminal_count += 1;
            }
        }
        // The value left the slot; suppress the Drop's default terminal mark
        // (already recorded above) — the drop still records the closure return.
        self.marked_terminal = true;
        value
    }

    /// Record that this consumer reached a **terminal state** — the first half of
    /// the release gate. This does **not** release the value: release still waits
    /// for the closure to return (the lease drop). Marking terminal while the
    /// closure runs is exactly the **zombie** case (abandoned-but-running): the
    /// value stays reachable and its residency stays counted until the drop.
    pub fn mark_terminal(&mut self) {
        if !self.marked_terminal {
            self.marked_terminal = true;
            let mut inner = lock(&self.inner);
            inner.terminal_count += 1;
        }
    }
}

impl<T> Drop for ConsumerLease<T> {
    fn drop(&mut self) {
        let mut inner = lock(&self.inner);
        // The closure returned: one fewer open lease, one more returned.
        inner.open_leases = inner.open_leases.saturating_sub(1);
        inner.returned_count += 1;
        // A consumer that returned without an explicit terminal mark is treated
        // as terminal-and-returned (the common success/failure case where the
        // runner did not separately signal the terminal decision).
        if !self.marked_terminal {
            inner.terminal_count += 1;
        }
        inner.try_release();
    }
}

/// The **post-run redemption handle** for a `retained` node: exchanged for the
/// value once the run has ended (arch.md C10 — *"the handle can be exchanged for
/// the value once the run has ended"*). Minted with [`Slot::redemption_handle`].
///
/// For a retained node whose slot was filled, [`redeem`](RedemptionHandle::redeem)
/// returns the value (and releases its through-run-end residency exactly once).
/// For a non-retained node it reports [`Released`](RedeemError::Released); for a
/// slot that never filled it reports [`NeverFilled`](RedeemError::NeverFilled) —
/// both **distinct** from the read-before-fill framework defect.
pub struct RedemptionHandle<T> {
    inner: Arc<Mutex<Inner>>,
    _ty: std::marker::PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> RedemptionHandle<T> {
    /// Redeem the retained value after the run has ended.
    ///
    /// # Errors
    ///
    /// Returns [`RedeemError::Released`] if the node was not retained and its
    /// value was released, or [`RedeemError::NeverFilled`] if the slot never
    /// filled. A retained, filled slot yields the value and releases its
    /// residency exactly once.
    pub fn redeem(&self) -> Result<T, RedeemError> {
        let mut inner = lock(&self.inner);
        let taken = std::mem::replace(&mut inner.content, Content::Released);
        match taken {
            Content::Filled(arc) => {
                // Recover the concrete `Arc<T>` at the single erasure boundary
                // (infallible by construction), then move the value out. A
                // retained value is redeemed only after the run has ended — after
                // every consumer returned — so no read clone outlives it and
                // `try_unwrap` succeeds.
                let typed: Arc<T> = arc.downcast::<T>().unwrap_or_else(|_| {
                    unreachable!(
                        "output slot type-erasure invariant violated on redeem (framework \
                         defect)"
                    )
                });
                let value = Arc::try_unwrap(typed).unwrap_or_else(|_| {
                    unreachable!(
                        "retained value redeemed while a consumer still held a read clone \
                         (framework defect: redemption happens after the run has ended)"
                    )
                });
                if !inner.residency_released {
                    inner.ledger.release(inner.residency);
                    inner.residency_released = true;
                }
                Ok(value)
            }
            Content::Empty => {
                // Restore Empty (redemption did not consume anything).
                inner.content = Content::Empty;
                Err(RedeemError::NeverFilled)
            }
            Content::Released => Err(RedeemError::Released),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared read/lock helpers.
// ---------------------------------------------------------------------------

/// Lock the interior, recovering from poisoning (a panicking consumer must not
/// wedge the whole slot machinery — the ledger and release discipline stay
/// correct).
fn lock(inner: &Arc<Mutex<Inner>>) -> std::sync::MutexGuard<'_, Inner> {
    inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Read the filled value as a typed `Arc<T>`, or panic loudly naming the node on
/// read-before-fill. The downcast is infallible by construction (see the module
/// docs' safety argument); its `expect` is a framework-defect assertion.
fn read_arc<T: Send + Sync + 'static>(inner: &Inner) -> Arc<T> {
    match &inner.content {
        Content::Filled(arc) => Arc::clone(arc).downcast::<T>().unwrap_or_else(|_| {
            unreachable!(
                "output slot type-erasure invariant violated on read (framework defect): the \
                 stored value's type did not match the consumer reference's type for node `{}`",
                inner.name
            )
        }),
        Content::Empty => panic!(
            "read-before-fill (framework defect): node `{}` output slot was read before it was \
             filled",
            inner.name
        ),
        Content::Released => panic!(
            "read-after-release (framework defect): node `{}` output slot was read after its \
             value was released; a consumer must not read past its own terminal-and-returned \
             point",
            inner.name
        ),
    }
}
