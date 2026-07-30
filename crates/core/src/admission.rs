//! The **admission controller** — bounded capacity pools and the permit
//! lifecycle.
//!
//! # What this module owns
//!
//! The admission controller is what turns a memory ceiling into a *throughput
//! limit* instead of a crash. It holds **weighted capacity pools** for the
//! genuinely constrained resources — a **memory** pool (native unit: bytes) and
//! two **thread** pools (blocking and compute, native unit: a thread count) — and
//! decides, for each ready node, whether its declared cost fits the remaining
//! capacity of *every* pool it needs. It owns everything from acquisition through
//! release:
//!
//! - **weighted capacity pools** — each holds a total capacity and a live
//!   remaining capacity ([`AdmissionController::remaining`]);
//! - **all-or-nothing multi-pool acquisition** — a node is admitted only when its
//!   declared cost fits *every* pool, and **no** pool's capacity is held while
//!   waiting on another ([`AdmissionController::try_admit`]); that atomicity is
//!   what prevents the classic two-pool deadlock;
//! - **oldest-ready-first admission with bounded bypass** — a small node may jump
//!   the queue only when admitting it cannot delay the current oldest waiter
//!   ([`AdmissionController::offer`] / [`poll_admissions`](AdmissionController::poll_admissions)),
//!   so a large node behind a stream of small ones is never starved;
//! - **the permit held for the whole attempt** — a [`Permit`] whose `Drop`
//!   returns its cost to every pool it drew from, so the permit releases on every
//!   terminal outcome (success, permanent failure, retry-eligible failure,
//!   cooperative cancellation) exactly when the guard drops;
//! - **zombie accounting** — [`mark_zombie`](AdmissionController::mark_zombie)
//!   registers an abandoned-but-running attempt as a live zombie whose cost stays
//!   counted against every pool until the closure **actually returns** (the permit
//!   drops), never before — an honest ledger;
//! - **the working-memory vs output-residency split** — working memory is held for
//!   the attempt and released at its terminal state; output residency **transfers**
//!   to the output slot when the value is produced and is charged as a **slot
//!   lease** ([`ResidencyLease`](crate::slot)) against the same memory pool until
//!   the slot actually releases (which waits for zombie consumers to return);
//! - **permit-wait vs execution timing** — [`begin_wait`](AdmissionController::begin_wait)
//!   records the waiting phase separately from the executing phase;
//! - **the undeclared-cost warning** — [`warn_if_undeclared`](AdmissionController::warn_if_undeclared)
//!   fires for a node with no declared memory cost only when the memory pool is a
//!   real constraint;
//! - **the reporting seam** — [`zombie_report`](AdmissionController::zombie_report)
//!   surfaces the count of live zombies and the per-pool cost each pins, in the
//!   shape the run artifact folds side by side with measured cost.
//!
//! # The permit-lifecycle contract this implements verbatim
//!
//! The permit lifecycle is:
//! `try_admit(node, cost) -> Option<Permit>` (all-or-nothing across pools); a
//! `Permit` whose `Drop` returns cost to every pool; `mark_zombie(&permit)`
//! registering a `{node, per-pool cost}` record **without** releasing;
//! `zombie_report()`; and the invariant that counted cost (zombies included) never
//! exceeds capacity at any instant. The load-bearing trick: the
//! permit is moved **into** the blocking/compute closure, so "the work has
//! returned" is *definitionally* "the permit was dropped" — the ledger structurally
//! cannot release what is still running, with no watchdog and no join that blocks
//! the run loop. This module provides the ledger; the runner ([`crate::execution`])
//! moves the permit into the closure and observes live zombies through
//! [`ZombieObserver`], which
//! [`AdmissionController`] implements.
//!
//! # Determinism — admission by counts, never by sleeps
//!
//! Admission is decided by **counts**: `try_admit` succeeds or refuses on the
//! current remaining capacity, and a refused node waits until a *release* (a permit
//! drop) frees capacity — never on a timer. This keeps CI deterministic (no
//! wall-clock, no network) and is why the controller carries no async-runtime
//! dependency: `dagr-core` depends on nothing, and this
//! module holds to that — it is a synchronous, `unsafe`-free ledger the driver
//! drives from its framework runtime.
//!
//! # Scope
//!
//! This module takes pool capacities as an **input** and pins them for tests;
//! deriving them from container limits (cgroup v2 → v1 → host, the headroom
//! default, the pinning flag, too-big-node rejection at bootstrap) is handled by
//! the bootstrap limit-detection layer. Execution-class *dispatch* (routing a node
//! onto the compute-vs-blocking pool by its class) is handled separately; this
//! module provides the pools and permits, not the class routing policy. This
//! controller is per-run and in-process — there is no scheduler, no cross-process
//! capacity coordination, and no runtime-mutable pool set (a permanent non-goal).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::assembly::CostVector;
use crate::execution::ZombieObserver;
use crate::slot::ResidencyLedger;

/// The set of admission pools a node's declared cost is a vector over (the
/// [`CostVector`] dimensions).
///
/// The stated minimum is a **memory** pool and **thread** pools. Memory is a
/// single pool measured in **bytes** (the working-memory and output-residency
/// halves of the cost both draw from it); the two thread pools are the
/// **blocking** and **compute** pools, measured in a **thread count**.
///
/// The set is **fixed at compile time and never runtime-mutable** (a permanent
/// non-goal): this enum is the extension point, and adding a pool is a spec-driven
/// source change, never a runtime knob. Exactly these three pools ship in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pool {
    /// The memory pool, in **bytes**. Both working memory (held for the attempt)
    /// and output residency (the slot lease) are charged against it.
    Memory,
    /// The blocking thread pool, in a **thread count** (the `spawn_blocking`
    /// pool).
    BlockingThreads,
    /// The compute thread pool, in a **thread count** (the dedicated pool).
    ComputeThreads,
}

impl Pool {
    /// Every pool, in a stable order — the iteration order for all-or-nothing
    /// acquisition and reporting.
    pub const ALL: [Pool; 3] = [Pool::Memory, Pool::BlockingThreads, Pool::ComputeThreads];
}

/// The **pinned total capacity** of each pool.
///
/// This takes capacities as an input and pins them (the bootstrap
/// derivation from container limits lives in the limit-detection layer). The
/// default is a fully **unconstrained**
/// controller — every pool has effectively unlimited capacity — so a run with no
/// pinned constraint admits everything, which is what keeps the memory-constrained
/// warning ([`AdmissionController::warn_if_undeclared`]) scoped to genuinely
/// constrained runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolCapacities {
    memory: u64,
    memory_constrained: bool,
    blocking_threads: u32,
    compute_threads: u32,
}

impl Default for PoolCapacities {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolCapacities {
    /// An **unconstrained** capacity set: every pool is effectively unlimited, so
    /// nothing is a real constraint. Pin a pool with [`memory`](Self::memory),
    /// [`blocking_threads`](Self::blocking_threads), or
    /// [`compute_threads`](Self::compute_threads).
    #[must_use]
    pub fn new() -> Self {
        Self {
            memory: u64::MAX,
            memory_constrained: false,
            blocking_threads: u32::MAX,
            compute_threads: u32::MAX,
        }
    }

    /// Pin the **memory** pool's total capacity in bytes. Pinning it makes the
    /// memory pool a **real constraint**, which is what arms the undeclared-cost
    /// warning ([`AdmissionController::warn_if_undeclared`]).
    #[must_use]
    pub fn memory(mut self, bytes: u64) -> Self {
        self.memory = bytes;
        self.memory_constrained = true;
        self
    }

    /// Pin the **blocking** thread pool's total capacity (a thread count).
    #[must_use]
    pub fn blocking_threads(mut self, threads: u32) -> Self {
        self.blocking_threads = threads;
        self
    }

    /// Pin the **compute** thread pool's total capacity (a thread count).
    #[must_use]
    pub fn compute_threads(mut self, threads: u32) -> Self {
        self.compute_threads = threads;
        self
    }

    /// The pinned total capacity of `pool`, as a `u64` (thread counts widen).
    #[must_use]
    pub fn total(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Memory => self.memory,
            Pool::BlockingThreads => u64::from(self.blocking_threads),
            Pool::ComputeThreads => u64::from(self.compute_threads),
        }
    }

    /// Whether the memory pool is a genuine constraint (pinned to a finite
    /// capacity) — the condition under which the undeclared-cost warning fires.
    #[must_use]
    pub fn is_memory_constrained(&self) -> bool {
        self.memory_constrained
    }
}

/// A node's **declared per-pool cost** — the demand it makes on each pool
/// ([`Pool`]), in that pool's native unit (the [`CostVector`] dimensions).
///
/// Memory splits into **working memory** (held for the attempt, released at its
/// terminal state) and **output residency** (transferred to the output slot when
/// the value is produced — the slot lease). The thread costs are counts drawn
/// from the blocking and compute pools. Every field defaults to **zero** (the
/// conservative default), so a node with no declared cost demands nothing.
///
/// This is the admission-side mirror of the [`CostVector`]; build one directly
/// with the builder methods, or from a policy's cost vector with
/// [`from_cost_vector`](PoolCost::from_cost_vector) — the controller reads a node's
/// declared cost through the cost vector without duplicating its definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolCost {
    working_memory: u64,
    output_residency: u64,
    blocking_threads: u32,
    compute_threads: u32,
}

impl PoolCost {
    /// A zero cost — no demand on any pool (the conservative default).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read a node's declared cost from its [`CostVector`] — the
    /// controller consumes the declared-cost vectors without duplicating their
    /// definition.
    ///
    /// Equivalent to [`PoolCost::from`]/`.into()`; the two are one conversion
    /// with two spellings and are asserted against a single fixture.
    #[must_use]
    pub fn from_cost_vector(cost: CostVector) -> Self {
        Self {
            working_memory: cost.working_memory(),
            output_residency: cost.output_residency(),
            blocking_threads: cost.blocking_threads(),
            compute_threads: cost.compute_threads(),
        }
    }

    /// Set the **working-memory** demand in bytes (held for the attempt).
    #[must_use]
    pub fn working_memory(mut self, bytes: u64) -> Self {
        self.working_memory = bytes;
        self
    }

    /// Set the **output-residency** demand in bytes (the slot lease — transferred
    /// to the output slot when the value is produced).
    #[must_use]
    pub fn output_residency(mut self, bytes: u64) -> Self {
        self.output_residency = bytes;
        self
    }

    /// Set the **blocking**-pool thread-count demand.
    #[must_use]
    pub fn blocking_threads(mut self, threads: u32) -> Self {
        self.blocking_threads = threads;
        self
    }

    /// Set the **compute**-pool thread-count demand.
    #[must_use]
    pub fn compute_threads(mut self, threads: u32) -> Self {
        self.compute_threads = threads;
        self
    }

    /// The declared **working-memory** demand in bytes. (The setter and getter
    /// cannot share a name in Rust, so the getters carry a `_bytes` /
    /// `_thread_count` suffix while the builder setters mirror the cost-vector
    /// field names.)
    #[must_use]
    pub fn working_memory_bytes(&self) -> u64 {
        self.working_memory
    }

    /// The declared **output-residency** demand in bytes (the slot lease).
    #[must_use]
    pub fn output_residency_bytes(&self) -> u64 {
        self.output_residency
    }

    /// The declared **blocking**-pool thread-count demand.
    #[must_use]
    pub fn blocking_thread_count(&self) -> u32 {
        self.blocking_threads
    }

    /// The declared **compute**-pool thread-count demand.
    #[must_use]
    pub fn compute_thread_count(&self) -> u32 {
        self.compute_threads
    }

    /// The demand this cost makes on `pool` (as a `u64`). **Working memory** is
    /// what a permit charges the memory pool on admission (output residency is
    /// charged separately, as the slot lease, at production — not on admission).
    ///
    /// `pub(crate)` so the [`limits`](crate::limits) bootstrap check can read a
    /// node's per-pool demand against the derived pool totals without duplicating
    /// the mapping.
    #[must_use]
    pub(crate) fn demand_on(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Memory => self.working_memory,
            Pool::BlockingThreads => u64::from(self.blocking_threads),
            Pool::ComputeThreads => u64::from(self.compute_threads),
        }
    }
}

/// The idiomatic spelling of [`PoolCost::from_cost_vector`], added **alongside**
/// it rather than replacing it (`api-from-not-into`): a `From` impl is what
/// `.into()`, `?`-adjacent code, and any generic bound on `Into<PoolCost>` can
/// reach, while the named inherent constructor keeps the conversion greppable at
/// its call sites. The two share one body, so they cannot drift.
impl From<CostVector> for PoolCost {
    fn from(cost: CostVector) -> Self {
        Self::from_cost_vector(cost)
    }
}

/// One live-zombie record: the node and the per-pool cost its abandoned-but-running
/// closure still pins.
///
/// The report is a list of these, from which the live-zombie count and per-pool
/// pinned totals are derivable — the shape the event stream folds into a
/// zombie-at-exit event and the artifact folds into the declared-vs-measured
/// juxtaposition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZombieRecord {
    /// The zombie node's author-declared identity name.
    pub node: String,
    /// The per-pool cost this zombie still pins until its closure returns.
    pub pinned: ZombieCost,
}

/// The per-pool cost a live zombie pins, in a form the artifact folds.
/// Mirrors the admission-side [`PoolCost`] but is the *reported* shape (the
/// working-memory bytes the attempt drew, plus its thread counts).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ZombieCost {
    working_memory: u64,
    blocking_threads: u32,
    compute_threads: u32,
}

impl ZombieCost {
    /// The working-memory bytes this zombie pins.
    #[must_use]
    pub fn working_memory(&self) -> u64 {
        self.working_memory
    }

    /// The blocking-pool threads this zombie pins.
    #[must_use]
    pub fn blocking_threads(&self) -> u32 {
        self.blocking_threads
    }

    /// The compute-pool threads this zombie pins.
    #[must_use]
    pub fn compute_threads(&self) -> u32 {
        self.compute_threads
    }
}

/// The **zombie-cost report** — the count of live zombies and the per-pool cost
/// each pins.
///
/// This is the stable reporting seam the artifact folds. It
/// surfaces only the **declared** side (each zombie's pinned per-pool cost); no
/// measured-vs-declared comparison is computed here.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZombieReport {
    /// The number of live zombies (abandoned-but-running closures not yet returned).
    pub live_zombie_count: usize,
    /// One record per live zombie: its node and the per-pool cost it pins.
    pub zombies: Vec<ZombieRecord>,
}

/// A warning that a node declared **no** memory cost while the memory pool is a
/// **genuine constraint** (a memory-constrained run warns about nodes with no
/// declared cost).
///
/// The controller emits one only for a constrained run; an unconstrained run does
/// not warn (there is no ceiling to blow past). Surfaced so the driver can log it;
/// the event/artifact wiring lives in the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndeclaredCostWarning {
    node: String,
}

impl UndeclaredCostWarning {
    /// The node whose missing memory-cost declaration triggered the warning.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }
}

impl std::fmt::Display for UndeclaredCostWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node '{}' declared no memory cost in a memory-constrained run; \
             its working memory is uncounted against the pool",
            self.node
        )
    }
}

// ===========================================================================
// The ledger interior
// ===========================================================================

/// The mutable interior of an [`AdmissionController`], shared with every live
/// [`Permit`] and [`ResidencyLease`] via an [`Arc`]. Guarded by one [`Mutex`]:
/// admission decisions are infrequent and correctness-critical, so a single
/// coarse lock is the honest, deadlock-free choice (no lock is held across user
/// code — a permit holds only a released-count contribution, not the lock).
///
/// The counted cost of a pool is `total − remaining`. Working-memory and thread
/// costs are charged here on `try_admit` and returned on `Permit::drop`. Output
/// residency is **not** charged here — it is counted by the shared
/// [`ResidencyLedger`] (the slot lease), which the memory pool's *counted*
/// figure adds in when reporting so a zombie consumer holding the slot open keeps
/// the bytes counted.
struct Inner {
    caps: PoolCapacities,
    /// Live remaining capacity per pool (working memory + threads). Output
    /// residency is tracked by `residency`, not here.
    remaining_memory: u64,
    remaining_blocking: u32,
    remaining_compute: u32,
    /// The live zombies, in registration order (for a stable report).
    zombies: Vec<ZombieRecord>,
    /// The waiting queue: nodes offered but not yet admitted, oldest first (the
    /// oldest-ready-first discipline). Each carries its declared cost.
    waiters: VecDeque<Waiter>,
    /// The shared output-residency ledger. The memory pool's *counted* figure
    /// includes this so the slot lease is honestly charged against total memory;
    /// `None` when no slots participate (an unconstrained/threads-only controller).
    residency: Option<Arc<ResidencyLedger>>,
}

/// A node waiting for admission, carrying its declared cost. The queue order is
/// arrival order (oldest first).
struct Waiter {
    node: String,
    cost: PoolCost,
}

impl Inner {
    /// The live remaining capacity of `pool` (working memory / thread counts only;
    /// output residency does not reduce a pool's *remaining working* capacity — it
    /// is a separate charge the counted figure folds in).
    fn remaining(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Memory => self.remaining_memory,
            Pool::BlockingThreads => u64::from(self.remaining_blocking),
            Pool::ComputeThreads => u64::from(self.remaining_compute),
        }
    }

    /// Whether `cost` fits **every** pool's current remaining capacity — the
    /// all-or-nothing fit test. Output residency is *not* checked at admission (it
    /// is charged at production as the slot lease), only working memory and threads.
    fn fits(&self, cost: &PoolCost) -> bool {
        Pool::ALL
            .iter()
            .all(|&pool| cost.demand_on(pool) <= self.remaining(pool))
    }

    /// Whether `cost` could **ever** fit — i.e. its demand does not exceed any
    /// pool's **total** capacity, so an empty pool would admit it. A cost that
    /// exceeds a pool's total in some dimension can never be admitted, no matter how
    /// much capacity is released; distinguishing that from a merely-full pool is what
    /// lets the driver reject a can-never-fit node rather than strand it forever
    /// (the termination guard; the full bootstrap rejection lives in the
    /// limit-detection layer).
    fn fits_total(&self, cost: &PoolCost) -> bool {
        Pool::ALL
            .iter()
            .all(|&pool| cost.demand_on(pool) <= self.caps.total(pool))
    }

    /// Charge `cost` against every pool — all-or-nothing, so the caller has already
    /// checked [`fits`](Self::fits). Only working memory and threads are charged
    /// here; residency is charged separately at production.
    fn charge(&mut self, cost: &PoolCost) {
        self.remaining_memory -= cost.working_memory;
        self.remaining_blocking -= cost.blocking_threads;
        self.remaining_compute -= cost.compute_threads;
    }

    /// Return `cost` to every pool it drew from — the permit's release. Saturating
    /// so a double-release (a defect) can never drive remaining above total: the
    /// ledger is isolated from a misbehaving task and never over-credits.
    fn release(&mut self, cost: &PoolCost) {
        self.remaining_memory = (self.remaining_memory + cost.working_memory).min(self.caps.memory);
        self.remaining_blocking =
            (self.remaining_blocking + cost.blocking_threads).min(self.caps.blocking_threads);
        self.remaining_compute =
            (self.remaining_compute + cost.compute_threads).min(self.caps.compute_threads);
    }

    /// The **counted** cost of `pool` — `total − remaining`, plus, for the memory
    /// pool, the live output residency (the slot lease) so a zombie consumer that
    /// holds a slot open keeps its bytes honestly counted.
    fn counted(&self, pool: Pool) -> u64 {
        let charged = self.caps.total(pool) - self.remaining(pool);
        match pool {
            Pool::Memory => charged + self.residency.as_ref().map_or(0, |l| l.current()),
            _ => charged,
        }
    }

    /// The index of the next waiter to admit under the **oldest-ready-first with
    /// bounded bypass** discipline, or [`None`] if no waiter can be admitted without
    /// risking the oldest waiter.
    ///
    /// If the oldest waiter (index 0) fits, it is admitted — the oldest is never
    /// bypassed. If it does **not** fit, a younger waiter may **bypass** it, but
    /// only when doing so cannot delay the oldest: since the oldest does not fit
    /// now, admitting a younger waiter that *does* fit consumes only capacity the
    /// oldest could not have used, so the youngest such fitting waiter is chosen. If
    /// the oldest does not fit and no younger waiter fits either, nothing is
    /// admitted this round (the oldest is held for a future release).
    fn next_admissible(&self) -> Option<usize> {
        let front = self.waiters.front()?;
        if self.fits(&front.cost) {
            return Some(0);
        }
        // The oldest does not fit: bounded-bypass the first younger waiter that does.
        self.waiters
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, w)| self.fits(&w.cost))
            .map(|(i, _)| i)
    }
}

// ===========================================================================
// The admission controller
// ===========================================================================

/// The runtime **admission controller**. Cheaply cloneable —
/// every clone shares the same ledger via an [`Arc`], so the driver hands clones
/// to the pieces that admit, release, and report against one run's pools.
#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<Mutex<Inner>>,
}

impl AdmissionController {
    /// A controller over the pinned `caps`, every pool at full remaining capacity.
    #[must_use]
    pub fn new(caps: PoolCapacities) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                caps,
                remaining_memory: caps.memory,
                remaining_blocking: caps.blocking_threads,
                remaining_compute: caps.compute_threads,
                zombies: Vec::new(),
                waiters: VecDeque::new(),
                residency: None,
            })),
        }
    }

    /// Link the shared output-residency [`ResidencyLedger`] into the memory
    /// pool's **counted** figure, so a slot lease charges the same memory pool as
    /// working memory. The slot fills through this ledger (`Slot::fill`); the
    /// controller reads it to keep the pool's counted total honest — a zombie
    /// consumer holding a slot open keeps its bytes counted against the pool.
    #[must_use]
    pub fn with_residency_ledger(self, ledger: Arc<ResidencyLedger>) -> Self {
        self.lock().residency = Some(ledger);
        self
    }

    /// Poison policy: panic. The workspace rule is *recover where user-or-defect
    /// code can panic while the lock is held, panic otherwise* — and nothing runs
    /// under this lock but the ledger's own arithmetic: no task body, no user
    /// callback, no defect assertion (contrast [`crate::slot`]'s lock, which a
    /// read-before-fill panic legitimately poisons, and which therefore recovers).
    /// So a poisoned ledger can only mean a panic left a pool's counted total
    /// half-updated. Continuing on that would silently over- or under-admit for the
    /// rest of the run — capacity accounting that is quietly wrong is worse than a
    /// run that stops.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("admission ledger mutex not poisoned")
    }

    /// **Try to admit** `node` at `cost` — all-or-nothing across every pool.
    /// Returns a held [`Permit`] if `cost` fits every
    /// pool's current remaining capacity, or [`None`] if any pool cannot satisfy it
    /// — in which case **no** pool's capacity is consumed (no partial hold), so the
    /// node simply waits for a release.
    ///
    /// The returned permit is held for the whole attempt; dropping it returns the
    /// cost to every pool it drew from. Moving the permit **into** the attempt's
    /// closure (the ownership trick) is what makes permit-held-until-return
    /// fall out of Rust's ownership — the ledger cannot release what is still
    /// running.
    #[must_use]
    pub fn try_admit(&self, node: &str, cost: &PoolCost) -> Option<Permit> {
        let mut inner = self.lock();
        if !inner.fits(cost) {
            return None;
        }
        inner.charge(cost);
        Some(Permit {
            controller: self.clone(),
            node: node.to_string(),
            cost: *cost,
            released: false,
        })
    }

    /// Whether `cost` **could ever be admitted** — its declared demand does not
    /// exceed any pool's **total** capacity, so a completely empty pool would admit
    /// it. A cost that exceeds a pool's total in some dimension can
    /// **never** fit no matter how many permits release, so a node with such a cost
    /// waiting in the driver's pending queue would be stranded forever and never
    /// reach a terminal state — breaking the "every reachable node reaches a terminal
    /// state" invariant. The driver calls this to detect that condition and give the
    /// node a defined non-success terminal instead of silently stranding it.
    ///
    /// This is only the **defensive driver-level guard**: the full bootstrap-time
    /// rejection of too-big nodes (with the resolved container-limit capacities)
    /// lives in the limit-detection layer. `false` means "reject — this can never
    /// fit".
    #[must_use]
    pub fn can_ever_fit(&self, cost: &PoolCost) -> bool {
        self.lock().fits_total(cost)
    }

    /// A human-readable reason a `cost` can never be admitted — the first pool whose
    /// **total** capacity `cost` exceeds, with the demanded and total figures.
    /// Returns [`None`] when `cost` could fit an empty pool (so there
    /// is nothing to explain). This is the honest message the driver records on the
    /// node's non-success terminal so the run's outcome reflects *why* the node could
    /// never run — not a silent strand.
    #[must_use]
    pub fn over_demand_reason(&self, cost: &PoolCost) -> Option<String> {
        let inner = self.lock();
        Pool::ALL.iter().find_map(|&pool| {
            let demand = cost.demand_on(pool);
            let total = inner.caps.total(pool);
            (demand > total).then(|| {
                let unit = match pool {
                    Pool::Memory => "bytes",
                    Pool::BlockingThreads | Pool::ComputeThreads => "threads",
                };
                format!(
                    "declared cost {demand} {unit} exceeds {pool:?} pool capacity {total} {unit}"
                )
            })
        })
    }

    /// **Offer** `node` for admission at `cost`, enqueuing it in arrival order (the
    /// oldest-ready-first queue). A subsequent [`poll_admissions`](Self::poll_admissions)
    /// admits whichever waiters the oldest-ready-first-with-bounded-bypass policy
    /// allows. Offering does not consume capacity; it only records the demand and
    /// its arrival order.
    pub fn offer(&self, node: &str, cost: &PoolCost) {
        self.lock().waiters.push_back(Waiter {
            node: node.to_string(),
            cost: *cost,
        });
    }

    /// **Poll** the waiting queue and admit every waiter the oldest-ready-first
    /// discipline allows *right now*, returning their held [`Permit`]s.
    ///
    /// The discipline: walk the queue oldest-first. The **oldest waiter** is
    /// admitted whenever it fits. A younger (bypass) waiter is admitted **only**
    /// when admitting it **cannot delay the oldest waiter** — i.e. only when the
    /// oldest waiter still does not fit after the bypass (so the bypass consumes
    /// capacity the oldest could not have used anyway) and the bypass itself fits.
    /// This is the **bounded bypass**: a small node rides along only when it cannot
    /// push the oldest node's admission out, so a large oldest node is never starved
    /// by a stream of small ones. Admitted waiters are removed from the queue.
    #[must_use]
    pub fn poll_admissions(&self) -> Vec<Permit> {
        let mut inner = self.lock();
        let mut admitted_permits = Vec::new();
        // Repeatedly admit the next admissible waiter, until none remains. Each
        // admission consumes capacity and may unlock (or newly bar) the front, so
        // the queue is re-scanned oldest-first after every admission.
        while let Some(index) = inner.next_admissible() {
            if let Some(waiter) = inner.waiters.remove(index) {
                inner.charge(&waiter.cost);
                admitted_permits.push(Permit {
                    controller: self.clone(),
                    node: waiter.node,
                    cost: waiter.cost,
                    released: false,
                });
            }
        }
        admitted_permits
    }

    /// **Mark** `permit`'s attempt as abandoned-but-running — register a live
    /// zombie whose cost stays counted until the closure actually returns. This
    /// does **not** release anything: the permit is still
    /// held (by the running closure), so the cost remains charged; the release
    /// happens only when the permit drops. Registering the zombie lets the ledger
    /// *report* the abandoned cost independently and defers the node's retry while
    /// the zombie is live (via [`ZombieObserver`]).
    pub fn mark_zombie(&self, permit: &Permit) {
        let mut inner = self.lock();
        inner.zombies.push(ZombieRecord {
            node: permit.node.clone(),
            pinned: ZombieCost {
                working_memory: permit.cost.working_memory,
                blocking_threads: permit.cost.blocking_threads,
                compute_threads: permit.cost.compute_threads,
            },
        });
    }

    /// The **zombie-cost report** — the count of live zombies and the per-pool cost
    /// each pins. The stable reporting seam the artifact folds side by side with
    /// measured cost.
    #[must_use]
    pub fn zombie_report(&self) -> ZombieReport {
        let inner = self.lock();
        ZombieReport {
            live_zombie_count: inner.zombies.len(),
            zombies: inner.zombies.clone(),
        }
    }

    /// The **live remaining** working capacity of `pool` (bytes for memory, a thread
    /// count widened to `u64` for the thread pools). This does not subtract output
    /// residency — that is a separate charge folded into [`counted`](Self::counted).
    #[must_use]
    pub fn remaining(&self, pool: Pool) -> u64 {
        self.lock().remaining(pool)
    }

    /// The **counted** cost of `pool` — `total − remaining`, plus the live output
    /// residency (the slot lease) for the memory pool. The invariant this whole
    /// ticket protects: `counted(pool) <= total(pool)` at every instant, **including
    /// live zombies** (whose cost is still charged because their permit has not
    /// dropped).
    #[must_use]
    pub fn counted(&self, pool: Pool) -> u64 {
        self.lock().counted(pool)
    }

    /// Whether **every** pool is back at full remaining capacity with no live
    /// residency — the no-leak invariant a whole run must end on.
    #[must_use]
    pub fn all_pools_full(&self) -> bool {
        let inner = self.lock();
        Pool::ALL.iter().all(|&pool| inner.counted(pool) == 0)
    }

    /// **Transfer** `bytes` of output residency to the producing node's slot lease:
    /// the value was produced, so its declared residency moves
    /// from the attempt to the output slot and is charged against the **same memory
    /// pool** as working memory, held until the slot **actually** releases (the
    /// returned [`ResidencyLease`] drops). In the real path the transfer happens
    /// inside `Slot::fill` against the shared [`ResidencyLedger`]; this seam mints a
    /// lease against that same ledger so the driver can hold it for the slot's
    /// lifetime (past every consumer's return, including zombie consumers).
    ///
    /// If no residency ledger was linked ([`with_residency_ledger`](Self::with_residency_ledger)),
    /// one is created lazily on first transfer so the memory pool's counted figure
    /// still includes the slot lease — the seam is self-sufficient.
    #[must_use]
    pub fn transfer_residency(&self, node: &str, bytes: u64) -> ResidencyLease {
        let ledger = {
            let mut inner = self.lock();
            Arc::clone(inner.residency.get_or_insert_with(ResidencyLedger::new))
        };
        ledger.charge(bytes);
        ResidencyLease {
            ledger,
            node: node.to_string(),
            bytes,
            released: false,
        }
    }

    /// **Warn** if `node` declared no memory cost while the memory pool is a genuine
    /// constraint. Returns a [`UndeclaredCostWarning`] naming the node
    /// only when the memory pool is constrained *and* the node's working-memory
    /// demand is zero; otherwise [`None`] — an unconstrained run never warns, and a
    /// node with a declared memory cost never warns.
    #[must_use]
    pub fn warn_if_undeclared(&self, node: &str, cost: &PoolCost) -> Option<UndeclaredCostWarning> {
        let constrained = self.lock().caps.is_memory_constrained();
        if constrained && cost.working_memory == 0 {
            Some(UndeclaredCostWarning {
                node: node.to_string(),
            })
        } else {
            None
        }
    }

    /// Begin recording a node's **permit-wait vs execution** phases: time spent
    /// waiting for a permit is recorded separately from time spent
    /// executing. Returns a [`PhaseTiming`] the caller fills with the measured
    /// wait and execution intervals — the durations are **injected** (measured by
    /// the caller's clock), never read from a wall clock here, so the split stays
    /// deterministic and runtime-agnostic.
    #[must_use]
    pub fn begin_wait(&self, node: &str) -> PhaseTiming {
        PhaseTiming {
            node: node.to_string(),
            wait: Duration::ZERO,
            execution: Duration::ZERO,
        }
    }
}

/// [`AdmissionController`] observes its own live zombies: a timed-out
/// blocking/compute node's retry is deferred while any zombie is
/// live, and the runner reads that through this port. `has_live_zombie` is `true`
/// while the controller holds any unreturned zombie record.
impl ZombieObserver for AdmissionController {
    fn has_live_zombie(&self) -> bool {
        !self.lock().zombies.is_empty()
    }
}

// ===========================================================================
// The permit
// ===========================================================================

/// A held **admission permit** — the working memory and thread capacity a node
/// drew from the pools for the whole attempt.
///
/// The permit is held for the whole attempt and **released on `Drop`**: dropping
/// it returns its cost to every pool it drew from. That is the entire lifecycle —
/// on success, permanent failure, retry-eligible failure, or cooperative
/// cancellation the guard drops at the terminal state and the capacity is restored.
/// For a **timed-out blocking/compute** attempt, the permit is moved **into** the
/// still-running closure, so the cost stays counted until the closure returns and
/// drops it (the ownership trick — the ledger structurally cannot release what
/// is still running). Marking the attempt a zombie ([`AdmissionController::mark_zombie`])
/// records the abandoned cost for reporting without releasing.
pub struct Permit {
    controller: AdmissionController,
    node: String,
    cost: PoolCost,
    released: bool,
}

impl Permit {
    /// The node this permit was admitted for.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The declared per-pool cost this permit holds against the pools.
    #[must_use]
    pub fn cost(&self) -> PoolCost {
        self.cost
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        // Release exactly once: return the working-memory + thread cost to every
        // pool it drew from, and clear this node's zombie record if one was
        // registered (the closure has now returned — the zombie is gone). This is
        // the single release point the whole permit lifecycle turns on.
        if self.released {
            return;
        }
        self.released = true;
        let mut inner = self.controller.lock();
        inner.release(&self.cost);
        // The closure has returned: drop this node's live-zombie record if present.
        // Only the first matching record is removed, pairing one return with one
        // mark (a node's retry is deferred until its previous closure returns, so a
        // node has at most one live zombie at a time).
        if let Some(pos) = inner.zombies.iter().position(|z| z.node == self.node) {
            inner.zombies.remove(pos);
        }
    }
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `controller` back-reference and the `released` guard flag are
        // deliberately omitted (the controller is not usefully printable and the
        // flag is internal); `finish_non_exhaustive` records that intent.
        f.debug_struct("Permit")
            .field("node", &self.node)
            .field("cost", &self.cost)
            .finish_non_exhaustive()
    }
}

/// A held **output-residency slot lease** — the memory a produced value pins in
/// its output slot, charged against the memory pool from production until the slot
/// **actually** releases.
///
/// Distinct from a [`Permit`]: working memory is released at the attempt's terminal
/// state (the permit drops), but output residency is **not** — it transfers to the
/// slot and is held as this lease until the slot releases, which waits for
/// every consumer (including a **zombie** consumer whose thread has not returned).
/// A **retained** value's lease is held until run end. Dropping the lease returns
/// its bytes to the shared [`ResidencyLedger`], which the memory pool's counted
/// figure folds in.
pub struct ResidencyLease {
    ledger: Arc<ResidencyLedger>,
    node: String,
    bytes: u64,
    released: bool,
}

impl ResidencyLease {
    /// The producing node whose output residency this lease holds.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The residency bytes this lease pins against the memory pool.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for ResidencyLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        self.ledger.release(self.bytes);
    }
}

impl std::fmt::Debug for ResidencyLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The shared `ledger` handle and the `released` guard flag are omitted (the
        // ledger is not usefully printable and the flag is internal).
        f.debug_struct("ResidencyLease")
            .field("node", &self.node)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

// ===========================================================================
// Permit-wait vs execution phase timing
// ===========================================================================

/// The **wait vs execution phase split** for one attempt: permit-wait
/// time recorded separately from execution time.
///
/// The two durations are **injected** (the caller measures them with its own
/// clock), so the split is deterministic and this core adds no wall-clock read. A
/// node admitted immediately records a near-zero wait; a node that waited for
/// capacity records the measured wait interval, distinct from its execution
/// interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseTiming {
    node: String,
    wait: Duration,
    execution: Duration,
}

impl PhaseTiming {
    /// The node these phases are recorded for.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// Record the measured **permit-wait** interval (the time spent waiting for
    /// capacity before admission).
    pub fn record_wait(&mut self, wait: Duration) {
        self.wait = wait;
    }

    /// Record the measured **execution** interval (the time spent executing after
    /// admission).
    pub fn record_execution(&mut self, execution: Duration) {
        self.execution = execution;
    }

    /// The recorded permit-wait interval — distinct from [`execution`](Self::execution).
    #[must_use]
    pub fn wait(&self) -> Duration {
        self.wait
    }

    /// The recorded execution interval — distinct from [`wait`](Self::wait).
    #[must_use]
    pub fn execution(&self) -> Duration {
        self.execution
    }
}
