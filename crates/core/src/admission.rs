//! The C12 **admission controller** — bounded capacity pools and the permit
//! lifecycle (arch.md `### C12 · Admission controller`; ticket T31).
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
//!   drops), never before — the honest ledger the T0.3 ADR mandates;
//! - **the working-memory vs output-residency split** — working memory is held for
//!   the attempt and released at its terminal state; output residency **transfers**
//!   to the output slot when the value is produced and is charged as a **slot
//!   lease** ([`ResidencyLease`](crate::slot)) against the same memory pool until
//!   the slot actually releases (which, per C10, waits for zombie consumers to
//!   return);
//! - **permit-wait vs execution timing** — [`begin_wait`](AdmissionController::begin_wait)
//!   records the waiting phase separately from the executing phase;
//! - **the undeclared-cost warning** — [`warn_if_undeclared`](AdmissionController::warn_if_undeclared)
//!   fires for a node with no declared memory cost only when the memory pool is a
//!   real constraint;
//! - **the reporting seam** — [`zombie_report`](AdmissionController::zombie_report)
//!   surfaces the count of live zombies and the per-pool cost each pins, in the
//!   shape T42/C23 folds side by side with measured cost.
//!
//! # The T0.3 ADR contract this implements verbatim
//!
//! The permit lifecycle is exactly the one the T0.3 ADR (009) §2, §3, §9 fixed:
//! `try_admit(node, cost) -> Option<Permit>` (all-or-nothing across pools); a
//! `Permit` whose `Drop` returns cost to every pool; `mark_zombie(&permit)`
//! registering a `{node, per-pool cost}` record **without** releasing;
//! `zombie_report()`; and the invariant that counted cost (zombies included) never
//! exceeds capacity at any instant. The load-bearing trick is the ADR's own: the
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
//! dependency: `dagr-core` depends on nothing (the workspace ADR T1), and this
//! module holds to that — it is a synchronous, `unsafe`-free ledger the driver
//! (T24) drives from its framework runtime.
//!
//! # Scope
//!
//! This ticket takes pool capacities as an **input** and pins them for tests;
//! deriving them from container limits (cgroup v2 → v1 → host, the headroom
//! default, the pinning flag, too-big-node rejection at bootstrap) is **T32**.
//! Execution-class *dispatch* (routing a node onto the compute-vs-blocking pool by
//! its class) is **T33**; this module provides the pools and permits, not the class
//! routing policy. The exhaustive permit-release outcome matrix is **T37**; the
//! event/artifact fold of the declared-vs-measured cost is **T42/C23**. This
//! controller is per-run and in-process — there is no scheduler, no cross-process
//! capacity coordination, and no runtime-mutable pool set (a permanent non-goal).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::assembly::CostVector;
use crate::execution::ZombieObserver;
use crate::slot::ResidencyLedger;

/// The set of admission pools a node's declared cost is a vector over (arch.md
/// C12; the C5 [`CostVector`] dimensions).
///
/// The stated minimum is a **memory** pool and **thread** pools. Memory is a
/// single pool measured in **bytes** (the working-memory and output-residency
/// halves of the C5 cost both draw from it); the two thread pools are the
/// **blocking** and **compute** pools from T2, measured in a **thread count**.
///
/// The set is **fixed at compile time and never runtime-mutable** (a permanent
/// non-goal): this enum is the extension point, and adding a pool is a spec-driven
/// source change, never a runtime knob. Resolving T31's open question toward the
/// stated minimum, exactly these three pools ship in v1 (see the ticket's Open
/// questions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pool {
    /// The memory pool, in **bytes**. Both working memory (held for the attempt)
    /// and output residency (the slot lease) are charged against it.
    Memory,
    /// The blocking thread pool, in a **thread count** (T2 `spawn_blocking`).
    BlockingThreads,
    /// The compute thread pool, in a **thread count** (T2 the dedicated pool).
    ComputeThreads,
}

impl Pool {
    /// Every pool, in a stable order — the iteration order for all-or-nothing
    /// acquisition and reporting.
    pub const ALL: [Pool; 3] = [Pool::Memory, Pool::BlockingThreads, Pool::ComputeThreads];
}

/// The **pinned total capacity** of each pool (arch.md C12).
///
/// This ticket takes capacities as an input and pins them (the bootstrap
/// derivation from container limits is T32). The default is a fully **unconstrained**
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
/// ([`Pool`]), in that pool's native unit (arch.md C12; the C5 [`CostVector`]).
///
/// Memory splits into **working memory** (held for the attempt, released at its
/// terminal state) and **output residency** (transferred to the output slot when
/// the value is produced — the slot lease, C10). The thread costs are counts drawn
/// from the blocking and compute pools. Every field defaults to **zero** (the
/// conservative C5 default), so a node with no declared cost demands nothing.
///
/// This is the admission-side mirror of the C5 [`CostVector`]; build one directly
/// with the builder methods, or from a policy's cost vector with
/// [`from_cost_vector`](PoolCost::from_cost_vector) — the controller reads a node's
/// declared cost through C5 without duplicating its definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolCost {
    working_memory: u64,
    output_residency: u64,
    blocking_threads: u32,
    compute_threads: u32,
}

impl PoolCost {
    /// A zero cost — no demand on any pool (the conservative C5 default).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read a node's declared cost from its C5 [`CostVector`] (T29) — the
    /// controller consumes the declared-cost vectors without duplicating their
    /// definition.
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
    /// to the output slot when the value is produced, C10).
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
    /// `_thread_count` suffix while the builder setters mirror the C5 field names.)
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
    #[must_use]
    fn demand_on(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Memory => self.working_memory,
            Pool::BlockingThreads => u64::from(self.blocking_threads),
            Pool::ComputeThreads => u64::from(self.compute_threads),
        }
    }
}

/// One live-zombie record: the node and the per-pool cost its abandoned-but-running
/// closure still pins (arch.md C12; T0.3 ADR §7).
///
/// The report is a list of these, from which the live-zombie count and per-pool
/// pinned totals are derivable — the shape C19 folds into a zombie-at-exit event
/// and C22/C23 fold into the declared-vs-measured juxtaposition (T42).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZombieRecord {
    /// The zombie node's author-declared identity name.
    pub node: String,
    /// The per-pool cost this zombie still pins until its closure returns.
    pub pinned: ZombieCost,
}

/// The per-pool cost a live zombie pins, in a form the artifact folds (T42/C23).
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
/// each pins (arch.md C12; T0.3 ADR §7).
///
/// This is the stable reporting seam T37 asserts against and T42/C23 fold. It
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
/// **genuine constraint** (arch.md C12: "a memory-constrained run warns about
/// nodes with no declared cost").
///
/// The controller emits one only for a constrained run; an unconstrained run does
/// not warn (there is no ceiling to blow past). Surfaced so the driver can log it;
/// the event/artifact wiring is C19/C23.
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
/// [`ResidencyLedger`] (the slot lease, C10), which the memory pool's *counted*
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
    /// The shared output-residency ledger (C10). The memory pool's *counted* figure
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
    /// holds a slot open keeps its bytes honestly counted (C12/C10).
    fn counted(&self, pool: Pool) -> u64 {
        let charged = self.caps.total(pool) - self.remaining(pool);
        match pool {
            Pool::Memory => charged + self.residency.as_ref().map_or(0, |l| l.current()),
            _ => charged,
        }
    }

    /// The index of the next waiter to admit under the **oldest-ready-first with
    /// bounded bypass** discipline, or [`None`] if no waiter can be admitted without
    /// risking the oldest waiter (arch.md C12).
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

/// The runtime **admission controller** (arch.md C12; T31). Cheaply cloneable —
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

    /// Link the shared output-residency [`ResidencyLedger`] (C10) into the memory
    /// pool's **counted** figure, so a slot lease charges the same memory pool as
    /// working memory. The slot fills through this ledger (`Slot::fill`); the
    /// controller reads it to keep the pool's counted total honest — a zombie
    /// consumer holding a slot open keeps its bytes counted against the pool.
    #[must_use]
    pub fn with_residency_ledger(self, ledger: Arc<ResidencyLedger>) -> Self {
        self.lock().residency = Some(ledger);
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("admission ledger mutex not poisoned")
    }

    /// **Try to admit** `node` at `cost` — all-or-nothing across every pool
    /// (arch.md C12; T0.3 ADR §9). Returns a held [`Permit`] if `cost` fits every
    /// pool's current remaining capacity, or [`None`] if any pool cannot satisfy it
    /// — in which case **no** pool's capacity is consumed (no partial hold), so the
    /// node simply waits for a release.
    ///
    /// The returned permit is held for the whole attempt; dropping it returns the
    /// cost to every pool it drew from. Moving the permit **into** the attempt's
    /// closure (the T0.3 ownership trick) is what makes permit-held-until-return
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
    /// discipline allows *right now*, returning their held [`Permit`]s (arch.md
    /// C12).
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
    /// zombie whose cost stays counted until the closure actually returns (arch.md
    /// C12; T0.3 ADR §2). This does **not** release anything: the permit is still
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
    /// each pins (arch.md C12; T0.3 ADR §7). The stable reporting seam T37 asserts
    /// against and T42/C23 fold side by side with measured cost.
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

    /// **Transfer** `bytes` of output residency to the producing node's slot lease
    /// (arch.md C12/C10): the value was produced, so its declared residency moves
    /// from the attempt to the output slot and is charged against the **same memory
    /// pool** as working memory, held until the slot **actually** releases (the
    /// returned [`ResidencyLease`] drops). In the real path the transfer happens
    /// inside `Slot::fill` against the shared [`ResidencyLedger`]; this seam mints a
    /// lease against that same ledger so the driver can hold it for the slot's
    /// lifetime (per C10, past every consumer's return, including zombie consumers).
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
    /// constraint (arch.md C12). Returns a [`UndeclaredCostWarning`] naming the node
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

    /// Begin recording a node's **permit-wait vs execution** phases (arch.md C12:
    /// "Time spent waiting for a permit is recorded separately from time spent
    /// executing"). Returns a [`PhaseTiming`] the caller fills with the measured
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

/// [`AdmissionController`] observes its own live zombies (arch.md C12; T0.3 ADR
/// §5): a timed-out blocking/compute node's retry is deferred while any zombie is
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
/// drew from the pools for the whole attempt (arch.md C12; T0.3 ADR §2, §9).
///
/// The permit is held for the whole attempt and **released on `Drop`**: dropping
/// it returns its cost to every pool it drew from. That is the entire lifecycle —
/// on success, permanent failure, retry-eligible failure, or cooperative
/// cancellation the guard drops at the terminal state and the capacity is restored.
/// For a **timed-out blocking/compute** attempt, the permit is moved **into** the
/// still-running closure, so the cost stays counted until the closure returns and
/// drops it (the T0.3 ownership trick — the ledger structurally cannot release what
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
        // node has at most one live zombie at a time — T0.3 ADR §5).
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
/// **actually** releases (arch.md C12/C10; T0.3 ADR §4).
///
/// Distinct from a [`Permit`]: working memory is released at the attempt's terminal
/// state (the permit drops), but output residency is **not** — it transfers to the
/// slot and is held as this lease until the slot releases, which per C10 waits for
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

/// The **wait vs execution phase split** for one attempt (arch.md C12: permit-wait
/// time recorded separately from execution time).
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
