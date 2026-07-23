//! The C12 **container limit detection** probe — bootstrap sizing of the
//! admission pools from the machine's actual limits (arch.md `### C12`, line 279;
//! ticket T32).
//!
//! # What this module owns
//!
//! T31 built the admission pools and the permit lifecycle but *took* their
//! capacities as an input. This module is the **sizing half** of C12: the
//! bootstrap probe that discovers the real ceiling of the machine a run executes
//! on and turns it into the default [`PoolCapacities`] T31 admits against. It
//! owns:
//!
//! - **the ordered probe** — memory and CPU limits are detected by trying sources
//!   in strict order: **cgroup v2** first, then **cgroup v1**, then **host
//!   resources** when neither cgroup source is present (the dev-machine / macOS
//!   case);
//! - **per-dimension unlimited-sentinel fallback** — a cgroup source that reports
//!   the unlimited sentinel (`max`, or the cgroup-v1 absurdly-large representation)
//!   for a dimension is treated as "no limit here" and that dimension falls back to
//!   host resources, independently of the other dimension;
//! - **the headroom fraction** — pools are sized to the detected limit minus a
//!   headroom fraction that defaults to [`HEADROOM_DEFAULT`] (20%);
//! - **the at-least-one-unit floor** — every pool receives at least one unit after
//!   headroom and rounding; in particular the compute/thread pool gets at least one
//!   thread even under a fractional CPU quota;
//! - **the pinning flag** — an operator override in the library-reserved `dagr.`
//!   namespace ([`PinnedPools`]) that pins any pool's capacity outright, overriding
//!   both cgroup detection and host fallback; this is also the mechanism CI uses to
//!   make capacity deterministic;
//! - **the too-big-node bootstrap rejection** — [`detect_capacities`] rejects, at
//!   bootstrap and before any node executes, any node whose declared cost for any
//!   pool exceeds that pool's total capacity, producing the
//!   [`CapacityBootstrapFailure`] artifact (`bootstrap-failed`, distinct from
//!   `assembly-failed`, complete error list, zero attempts).
//!
//! # Determinism and platform (the load-bearing part)
//!
//! cgroup detection is Linux-specific. To keep `dagr-core` **dependency-free** (the
//! workspace ADR T1) and every test **deterministic on both macOS and Linux CI**,
//! the probe never reads the real `/sys` or `/proc` from a unit test: it reads
//! cgroup / proc values from a **supplied root path** ([`ContainerLimitProbe::from_root`])
//! and takes the host core count as an **injected value**
//! ([`ContainerLimitProbe::with_host_cores`]). Tests feed a temp directory
//! mimicking a cgroup v2 / v1 / unlimited / malformed tree and assert the exact
//! derived [`PoolCapacities`]. The production entry point
//! ([`ContainerLimitProbe::from_host`]) roots at `/` and reads the host core count
//! from `std::thread::available_parallelism` — the only live-host read, and it is
//! `std`, so no dependency is added.
//!
//! The platform-conditional nature is documented here for the T70 coverage matrix:
//! cgroup v2 / v1 detection is **Tier-1 Linux** (arch.md lines 629–633); on macOS
//! (and any host without a cgroup hierarchy) the probe falls back to host resources
//! by design, which is the correct dev-machine behaviour.
//!
//! # Scope
//!
//! This module only **sizes** the pools T31 built and feeds them to it. It does not
//! touch T31's permit mechanics, the class dispatch (T33), the overcommit demo
//! (T38), or the platform-matrix CI (T70) — it supplies the pinning flag and the
//! documented fallback those depend on. Runtime resizing of pools is a permanent
//! non-goal: sizing happens once, at bootstrap.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::admission::{Pool, PoolCapacities, PoolCost};
use crate::context::BootstrapOutcome;

/// The default **headroom fraction** applied to every detected pool total: pools
/// are sized to the detected limit minus this fraction (arch.md C12, line 279).
/// 20% — enough slack for the framework's own machinery and allocator overhead so
/// the container's OOM killer is not the thing that enforces the ceiling.
pub const HEADROOM_DEFAULT: f64 = 0.20;

/// The reserved key prefix a pinning flag must carry (the library-reserved `dagr.`
/// namespace — arch.md line 498). A key outside this namespace is rejected so a
/// task cannot smuggle a capacity override.
const RESERVED_PREFIX: &str = "dagr.pool.";

/// The reserved pinning-flag keys, one per pool.
const KEY_MEMORY: &str = "dagr.pool.memory";
const KEY_BLOCKING: &str = "dagr.pool.blocking-threads";
const KEY_COMPUTE: &str = "dagr.pool.compute-threads";

// ===========================================================================
// The pinning flag — operator override in the reserved namespace
// ===========================================================================

/// The **operator pinning flags** — explicit per-pool capacity overrides in the
/// library-reserved `dagr.` namespace (arch.md C12, line 289).
///
/// A pinned pool's total is exactly the flag's value, overriding **both** cgroup
/// detection and host fallback. This is also the mechanism CI uses to make capacity
/// deterministic (the T38 overcommit demo pins capacity through this flag). Build
/// one with the typed builder ([`memory`](Self::memory) …) or from a raw
/// `dagr.`-prefixed key/value pair ([`set_flag`](Self::set_flag), the CLI-flag
/// path). Unset pools derive from detection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PinnedPools {
    memory: Option<u64>,
    blocking_threads: Option<u32>,
    compute_threads: Option<u32>,
}

impl PinnedPools {
    /// No pins — every pool derives from detection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin the **memory** pool's total capacity in bytes, overriding detection.
    #[must_use]
    pub fn memory(mut self, bytes: u64) -> Self {
        self.memory = Some(bytes);
        self
    }

    /// Pin the **blocking** thread pool's total capacity (a thread count).
    #[must_use]
    pub fn blocking_threads(mut self, threads: u32) -> Self {
        self.blocking_threads = Some(threads);
        self
    }

    /// Pin the **compute** thread pool's total capacity (a thread count).
    #[must_use]
    pub fn compute_threads(mut self, threads: u32) -> Self {
        self.compute_threads = Some(threads);
        self
    }

    /// Set a pin from a raw `key`/`value` pair — the CLI-flag path. `key` **must**
    /// live in the library-reserved `dagr.pool.` namespace; a key outside it is
    /// rejected so a task cannot smuggle a capacity override.
    ///
    /// Recognized keys: `dagr.pool.memory` (bytes), `dagr.pool.blocking-threads`,
    /// `dagr.pool.compute-threads` (thread counts).
    ///
    /// # Errors
    ///
    /// Returns an error string when `key` is not in the reserved namespace, is not
    /// a recognized pool key, or `value` does not parse as a non-negative integer.
    pub fn set_flag(&mut self, key: &str, value: &str) -> Result<(), String> {
        if !key.starts_with(RESERVED_PREFIX) {
            return Err(format!(
                "pinning key `{key}` is outside the reserved `{RESERVED_PREFIX}` namespace; \
                 only framework-reserved `dagr.`-prefixed pool keys may pin capacity"
            ));
        }
        match key {
            KEY_MEMORY => {
                self.memory = Some(parse_u64(key, value)?);
                Ok(())
            }
            KEY_BLOCKING => {
                self.blocking_threads = Some(parse_u32(key, value)?);
                Ok(())
            }
            KEY_COMPUTE => {
                self.compute_threads = Some(parse_u32(key, value)?);
                Ok(())
            }
            other => Err(format!(
                "pinning key `{other}` is in the reserved namespace but is not a known pool key \
                 ({KEY_MEMORY}, {KEY_BLOCKING}, {KEY_COMPUTE})"
            )),
        }
    }

    /// The pinned memory-pool total, if any.
    #[must_use]
    pub fn memory_pin(&self) -> Option<u64> {
        self.memory
    }

    /// The pinned blocking-thread-pool total, if any.
    #[must_use]
    pub fn blocking_threads_pin(&self) -> Option<u32> {
        self.blocking_threads
    }

    /// The pinned compute-thread-pool total, if any.
    #[must_use]
    pub fn compute_threads_pin(&self) -> Option<u32> {
        self.compute_threads
    }
}

fn parse_u64(key: &str, value: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("pinning value for `{key}` is not a non-negative integer: `{value}`"))
}

fn parse_u32(key: &str, value: &str) -> Result<u32, String> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("pinning value for `{key}` is not a non-negative integer: `{value}`"))
}

// ===========================================================================
// The raw detected limits (pre-headroom, pre-pin)
// ===========================================================================

/// The raw limits detected for one dimension, before headroom or pinning: the
/// memory ceiling in bytes and the CPU allocation as a thread count. Each is
/// resolved independently through the cgroup-v2 → cgroup-v1 → host precedence, so a
/// dimension whose cgroup value is an unlimited sentinel falls back to host on its
/// own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawLimits {
    /// The detected memory ceiling in bytes.
    memory_bytes: u64,
    /// The detected CPU allocation as a whole thread count (a fractional quota
    /// rounds toward the floor here; the at-least-one floor is applied later).
    cpu_threads: u32,
}

// ===========================================================================
// The probe
// ===========================================================================

/// The **container-limit probe** — reads cgroup / proc values from an injected
/// root path and derives the default [`PoolCapacities`] (arch.md C12; T32).
///
/// Construct it with [`from_root`](Self::from_root) (tests: a fixture tree under a
/// temp dir) or [`from_host`](Self::from_host) (production: rooted at `/`, host
/// cores from `available_parallelism`). Attach optional [pins](Self::with_pins) and
/// a non-default [headroom](Self::with_headroom), then call [`detect`](Self::detect).
#[derive(Debug, Clone)]
pub struct ContainerLimitProbe {
    root: PathBuf,
    host_cores: u32,
    headroom: f64,
    pins: PinnedPools,
}

impl ContainerLimitProbe {
    /// A probe rooted at `root`, reading cgroup / proc files **relative to it** (so
    /// a test feeds a temp directory mimicking `/sys/fs/cgroup` and `/proc`). The
    /// host core count defaults to 1 until [`with_host_cores`](Self::with_host_cores)
    /// supplies an injected value; the headroom defaults to [`HEADROOM_DEFAULT`].
    ///
    /// This never reads the real host `/sys` or `/proc`, so a scenario built on it
    /// is deterministic on any CI runner and on macOS.
    #[must_use]
    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            host_cores: 1,
            headroom: HEADROOM_DEFAULT,
            pins: PinnedPools::new(),
        }
    }

    /// The production probe: rooted at `/`, host cores read from
    /// `std::thread::available_parallelism` (the only live-host read — `std`, so no
    /// dependency is added). On a Linux container it finds the cgroup hierarchy; on
    /// macOS or a bare host it falls back to host resources by design.
    #[must_use]
    pub fn from_host() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        Self {
            root: PathBuf::from("/"),
            host_cores: u32::try_from(cores).unwrap_or(u32::MAX),
            headroom: HEADROOM_DEFAULT,
            pins: PinnedPools::new(),
        }
    }

    /// Inject the host core count used for the host-fallback CPU dimension, so the
    /// host-fallback path is deterministic in a test (never the real
    /// `available_parallelism`).
    #[must_use]
    pub fn with_host_cores(mut self, cores: u32) -> Self {
        self.host_cores = cores;
        self
    }

    /// Override the default 20% headroom fraction (0.0 ..= 1.0). Values outside the
    /// range are clamped; a headroom of 1.0 still floors every pool at one unit.
    #[must_use]
    pub fn with_headroom(mut self, fraction: f64) -> Self {
        self.headroom = fraction.clamp(0.0, 1.0);
        self
    }

    /// Attach operator [pins](PinnedPools): a pinned pool's total is exactly the
    /// pin, overriding both cgroup detection and host fallback.
    #[must_use]
    pub fn with_pins(mut self, pins: PinnedPools) -> Self {
        self.pins = pins;
        self
    }

    /// Run the probe and derive the [`PoolCapacities`] (arch.md C12; T32).
    ///
    /// Memory is sized from the memory dimension; **both** thread pools (blocking
    /// and compute) are sized from the CPU dimension — routing a node onto one
    /// versus the other is T33's class dispatch, not this sizing pass. Each pool is
    /// the detected raw limit minus headroom, floored at one unit; a pinned pool is
    /// the pin verbatim.
    ///
    /// # Errors
    ///
    /// This never fails — sizing always yields a capacity set (a hostile or absent
    /// cgroup tree falls back to host, and host defaults are always available). The
    /// too-big-node rejection that *can* fail bootstrap is [`detect_capacities`],
    /// which consumes the derived capacities against the declared node costs.
    pub fn detect(&self) -> Result<PoolCapacities, CapacityBootstrapFailure> {
        let raw = self.raw_limits();

        let memory = self
            .pins
            .memory
            .unwrap_or_else(|| apply_headroom_u64(raw.memory_bytes, self.headroom));
        let compute = self
            .pins
            .compute_threads
            .unwrap_or_else(|| apply_headroom_u32(raw.cpu_threads, self.headroom));
        let blocking = self
            .pins
            .blocking_threads
            .unwrap_or_else(|| apply_headroom_u32(raw.cpu_threads, self.headroom));

        Ok(PoolCapacities::new()
            .memory(memory)
            .compute_threads(compute)
            .blocking_threads(blocking))
    }

    /// Resolve the raw per-dimension limits through the cgroup-v2 → cgroup-v1 →
    /// host precedence, each dimension falling back independently on an unlimited
    /// sentinel or a malformed / absent value.
    fn raw_limits(&self) -> RawLimits {
        let memory_bytes = self
            .cgroup_v2_memory()
            .or_else(|| self.cgroup_v1_memory())
            .unwrap_or_else(|| self.host_memory());
        let cpu_threads = self
            .cgroup_v2_cpu()
            .or_else(|| self.cgroup_v1_cpu())
            .unwrap_or(self.host_cores);
        RawLimits {
            memory_bytes,
            cpu_threads,
        }
    }

    // --- cgroup v2 (the unified hierarchy) ---------------------------------

    /// The cgroup v2 memory ceiling (`memory.max`), or `None` when the file is
    /// absent, the unlimited sentinel `max`, or unparseable (→ fall back).
    fn cgroup_v2_memory(&self) -> Option<u64> {
        let raw = self.read("sys/fs/cgroup/memory.max")?;
        parse_cgroup_v2_max(&raw)
    }

    /// The cgroup v2 CPU allocation as a thread count from `cpu.max`
    /// (`"<quota> <period>"`), or `None` when absent, the `max` sentinel, or
    /// unparseable. `quota/period` rounds toward the floor; the at-least-one floor
    /// is applied later so a fractional quota never yields zero.
    fn cgroup_v2_cpu(&self) -> Option<u32> {
        let raw = self.read("sys/fs/cgroup/cpu.max")?;
        let mut parts = raw.split_whitespace();
        let quota = parts.next()?;
        if quota == "max" {
            return None; // unlimited → fall back to host cores
        }
        let quota: u64 = quota.parse().ok()?;
        let period: u64 = parts.next()?.parse().ok()?;
        threads_from_quota(quota, period)
    }

    // --- cgroup v1 (the legacy split controllers) --------------------------

    /// The cgroup v1 memory ceiling (`memory/memory.limit_in_bytes`), or `None`
    /// when absent, the absurdly-large "no limit" sentinel, or unparseable.
    fn cgroup_v1_memory(&self) -> Option<u64> {
        let raw = self.read("sys/fs/cgroup/memory/memory.limit_in_bytes")?;
        let bytes: u64 = raw.trim().parse().ok()?;
        // cgroup v1 encodes "no limit" as a value near u64::MAX (page-aligned), far
        // larger than any real machine — treat anything at/above this as unlimited.
        if bytes >= CGROUP_V1_UNLIMITED_THRESHOLD {
            return None;
        }
        Some(bytes)
    }

    /// The cgroup v1 CPU allocation as a thread count from
    /// `cpu/cpu.cfs_quota_us` / `cpu/cpu.cfs_period_us`, or `None` when absent, the
    /// `-1` "no limit" quota, or unparseable.
    fn cgroup_v1_cpu(&self) -> Option<u32> {
        let quota_raw = self.read("sys/fs/cgroup/cpu/cpu.cfs_quota_us")?;
        let quota: i64 = quota_raw.trim().parse().ok()?;
        if quota < 0 {
            return None; // -1 → unlimited → fall back to host cores
        }
        let period_raw = self.read("sys/fs/cgroup/cpu/cpu.cfs_period_us")?;
        let period: u64 = period_raw.trim().parse().ok()?;
        threads_from_quota(u64::try_from(quota).ok()?, period)
    }

    // --- host resources ----------------------------------------------------

    /// The host memory ceiling from `proc/meminfo` (`MemTotal: <kib> kB`), or a
    /// conservative fallback when the file is absent or unparseable (macOS has no
    /// `/proc/meminfo`; the production probe still sizes a memory pool so the memory
    /// dimension is never left unset).
    fn host_memory(&self) -> u64 {
        self.read("proc/meminfo")
            .and_then(|raw| parse_meminfo_total_bytes(&raw))
            .unwrap_or(HOST_MEMORY_FALLBACK_BYTES)
    }

    /// Read `rel` under the probe root as a trimmed string, or `None` when it is
    /// absent or unreadable — the seam that makes the probe read fixtures, never the
    /// real host, in a unit test.
    fn read(&self, rel: &str) -> Option<String> {
        std::fs::read_to_string(self.root.join(rel))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

/// The cgroup-v1 "no limit" sentinel threshold: v1 encodes unlimited memory as a
/// page-aligned value close to `u64::MAX`, far above any real machine. Anything at
/// or above this is treated as unlimited (→ host fallback).
const CGROUP_V1_UNLIMITED_THRESHOLD: u64 = u64::MAX / 4096 * 4096 - 4_294_967_296;

/// The conservative host-memory fallback (1 GiB) used only when `proc/meminfo` is
/// absent/unparseable and no cgroup limit constrains memory (e.g. a bare macOS host
/// with no injected fixture). A real production probe finds a real value; this only
/// guarantees the memory pool is never left unset.
const HOST_MEMORY_FALLBACK_BYTES: u64 = 1_073_741_824;

/// Parse a cgroup v2 `memory.max` value: the literal `max` is the unlimited
/// sentinel (→ `None`, fall back to host); otherwise a byte count.
fn parse_cgroup_v2_max(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    raw.parse().ok()
}

/// Turn a CFS `quota`/`period` into a whole thread count, rounding toward the
/// floor. A zero period is nonsensical (→ `None`, fall back). The at-least-one
/// floor is applied by headroom, so a sub-one-core quota is allowed to floor to
/// zero here and is lifted to one later.
fn threads_from_quota(quota: u64, period: u64) -> Option<u32> {
    if period == 0 {
        return None;
    }
    u32::try_from(quota / period).ok()
}

/// Parse `proc/meminfo`'s `MemTotal: <kib> kB` line into a byte count.
fn parse_meminfo_total_bytes(raw: &str) -> Option<u64> {
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return kib.checked_mul(1024);
        }
    }
    None
}

/// Apply the headroom fraction to a byte count, flooring at one unit: `floor(raw *
/// (1 - headroom))`, then `max(1)`. Every pool gets at least one unit after
/// headroom and rounding (arch.md C12, line 279).
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn apply_headroom_u64(raw: u64, headroom: f64) -> u64 {
    let kept = (raw as f64 * (1.0 - headroom)).floor();
    (kept as u64).max(1)
}

/// Apply the headroom fraction to a thread count, flooring at one thread — the
/// compute pool has at least one thread even under a fractional CPU quota.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn apply_headroom_u32(raw: u32, headroom: f64) -> u32 {
    let kept = (f64::from(raw) * (1.0 - headroom)).floor();
    (kept as u32).max(1)
}

// ===========================================================================
// Too-big-node bootstrap rejection — the bootstrap-failed artifact
// ===========================================================================

/// **Bootstrap-time capacity validation** (arch.md C12, lines 285/289; T32).
///
/// Given the derived `caps` and the pipeline's declared node costs (each a
/// `(node, cost)` pair, surfaced from C5), reject **at bootstrap and before any
/// node executes** every node whose declared cost for any pool exceeds that pool's
/// **total** capacity — such a node can never be admitted no matter how much
/// capacity releases, so it must fail fast rather than wedge at admission time.
///
/// Returns [`Ok`] when every node fits every pool (a node exactly at capacity fits
/// — the rule is strictly "exceeds"). Returns [`Err`] carrying the complete
/// [`CapacityBootstrapFailure`] — one [`CapacityError`] per offending
/// `(node, pool)`, in declaration order — the `bootstrap-failed` artifact distinct
/// from an assembly failure, with zero attempts recorded.
///
/// # Errors
///
/// Returns [`CapacityBootstrapFailure`] when at least one declared node cost
/// exceeds a pool's total capacity.
pub fn detect_capacities(
    caps: &PoolCapacities,
    node_costs: &[(String, PoolCost)],
) -> Result<(), CapacityBootstrapFailure> {
    let mut errors = Vec::new();
    for (node, cost) in node_costs {
        for &pool in &Pool::ALL {
            let demand = cost.demand_on(pool);
            let total = caps.total(pool);
            if demand > total {
                errors.push(CapacityError {
                    node: node.clone(),
                    pool,
                    declared_cost: demand,
                    capacity: total,
                });
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CapacityBootstrapFailure { errors })
    }
}

/// One too-big-node capacity error, for the [bootstrap-failure
/// artifact](CapacityBootstrapFailure): the offending node, the pool it overran,
/// its declared cost, and that pool's total capacity — the exact four facts an
/// operator needs to fix the run (arch.md C12, line 285).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityError {
    node: String,
    pool: Pool,
    declared_cost: u64,
    capacity: u64,
}

impl CapacityError {
    /// The offending node's author-declared identity name.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The pool whose total capacity the node's declared cost exceeded.
    #[must_use]
    pub fn pool(&self) -> Pool {
        self.pool
    }

    /// The node's declared cost on the overran pool (bytes for memory, a thread
    /// count for the thread pools).
    #[must_use]
    pub fn declared_cost(&self) -> u64 {
        self.declared_cost
    }

    /// The overran pool's total capacity — the ceiling the declared cost exceeded.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

impl std::fmt::Display for CapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let unit = match self.pool {
            Pool::Memory => "bytes",
            Pool::BlockingThreads | Pool::ComputeThreads => "threads",
        };
        write!(
            f,
            "node `{}` declares {} {unit} for the {:?} pool, which exceeds its total capacity {} {unit}",
            self.node, self.declared_cost, self.pool, self.capacity
        )
    }
}

/// The **bootstrap-failure artifact** produced when a node's declared cost exceeds
/// a pool's total capacity (arch.md C12, lines 285/475; T32).
///
/// The fail-fast startup outcome, **distinct from an assembly failure** and from
/// T31's admission-time can-never-fit guard: it names every offending node, the
/// pool, the declared cost, and the pool capacity, records that **zero attempts**
/// ran (no node executed), and never hangs (a synchronous, terminating check). It
/// mirrors T30's resource-check [`BootstrapFailure`](crate::context::BootstrapFailure)
/// shape so the downstream artifact emitter (C20 / C22) folds both bootstrap
/// failures the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityBootstrapFailure {
    errors: Vec<CapacityError>,
}

impl CapacityBootstrapFailure {
    /// The bootstrap outcome — always [`BootstrapOutcome::BootstrapFailed`];
    /// distinct from an assembly failure.
    #[must_use]
    pub fn outcome(&self) -> BootstrapOutcome {
        BootstrapOutcome::BootstrapFailed
    }

    /// The capacity errors, one per offending `(node, pool)`, in declaration order —
    /// the complete error list (never short-circuited on the first offender).
    #[must_use]
    pub fn errors(&self) -> &[CapacityError] {
        &self.errors
    }

    /// The number of attempts recorded — **always zero** for a bootstrap failure,
    /// because bootstrap fails *before any node executes* (arch.md C12: never a
    /// mid-run surprise).
    #[must_use]
    #[allow(
        clippy::unused_self,
        reason = "zero-attempts is a property of the bootstrap-failure artifact instance"
    )]
    pub fn attempts_recorded(&self) -> usize {
        0
    }

    /// Group the errors by offending node — the shape a per-node artifact fold
    /// (C22) reads, so a node with several overrun pools renders once.
    #[must_use]
    pub fn errors_by_node(&self) -> BTreeMap<&str, Vec<&CapacityError>> {
        let mut by_node: BTreeMap<&str, Vec<&CapacityError>> = BTreeMap::new();
        for err in &self.errors {
            by_node.entry(err.node.as_str()).or_default().push(err);
        }
        by_node
    }
}

impl std::fmt::Display for CapacityBootstrapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bootstrap failed: {} node(s) declare a cost exceeding pool capacity",
            self.errors.len()
        )?;
        for err in &self.errors {
            write!(f, "; {err}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CapacityBootstrapFailure {}
