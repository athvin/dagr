//! C12 · **Container limit detection** — the bootstrap probe that sizes the
//! admission pools from the container's actual limits (ticket T32, 042). Written
//! first, TDD.
//!
//! These are the acceptance tests for the ordered limit probe: cgroup v2 first,
//! then cgroup v1, then host resources when neither cgroup source exists (the
//! dev-machine / macOS case). Detection is driven entirely off an **injected
//! probe root** (a temp directory mimicking `/sys/fs/cgroup` and `/proc`) so
//! every scenario is deterministic on any CI runner and on macOS — the real host
//! `/sys` and `/proc` are never read here. The derived [`PoolCapacities`] are
//! asserted exactly.
//!
//! The mapped headline facet for this ticket is
//! [`container_limits_size_the_admission_pools_from_cgroup_v2`] (cgroup v2 is
//! preferred when present); its siblings cover v1 fallback, host fallback, the
//! per-dimension unlimited-sentinel fallback, the 20% headroom default, the
//! at-least-one-unit floor, the pinning flag overriding both detection sources,
//! and the too-big-node bootstrap rejection with its `bootstrap-failed` artifact.

use std::fs;
use std::path::{Path, PathBuf};

use dagr_core::admission::{Pool, PoolCost};
use dagr_core::limits::{
    detect_capacities, CapacityBootstrapFailure, ContainerLimitProbe, PinnedPools, HEADROOM_DEFAULT,
};
use dagr_core::BootstrapOutcome;

// ===========================================================================
// Fixture cgroup / proc trees under a temp probe root
// ===========================================================================

/// A throwaway probe root under the crate's target dir, wiped and recreated per
/// call so each test starts from a clean tree. Deterministic and host-independent:
/// nothing under the real `/sys` or `/proc` is ever touched.
struct FixtureRoot {
    root: PathBuf,
}

impl FixtureRoot {
    fn new(tag: &str) -> Self {
        let root = std::env::temp_dir().join(format!("dagr-t32-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create probe root");
        Self { root }
    }

    fn path(&self) -> &Path {
        &self.root
    }

    /// Write `contents` to `rel` under the probe root, creating parent dirs.
    fn write(&self, rel: &str, contents: &str) {
        let p = self.root.join(rel);
        fs::create_dir_all(p.parent().expect("has parent")).expect("mkdir");
        fs::write(&p, contents).expect("write fixture file");
    }

    /// Seed a cgroup **v2** memory + cpu limit (the unified hierarchy layout the
    /// probe reads: `sys/fs/cgroup/memory.max` and `.../cpu.max`).
    fn cgroup_v2(&self, memory_max: &str, cpu_max: &str) {
        self.write("sys/fs/cgroup/memory.max", memory_max);
        self.write("sys/fs/cgroup/cpu.max", cpu_max);
    }

    /// Seed a cgroup **v1** memory + cpu limit (the legacy split-controller
    /// layout: `.../memory/memory.limit_in_bytes`, `.../cpu/cpu.cfs_quota_us`,
    /// `.../cpu/cpu.cfs_period_us`).
    fn cgroup_v1(&self, mem_limit: &str, cfs_quota_us: &str, cfs_period_us: &str) {
        self.write("sys/fs/cgroup/memory/memory.limit_in_bytes", mem_limit);
        self.write("sys/fs/cgroup/cpu/cpu.cfs_quota_us", cfs_quota_us);
        self.write("sys/fs/cgroup/cpu/cpu.cfs_period_us", cfs_period_us);
    }

    /// Seed host resources: `proc/meminfo` MemTotal (kibibytes) and a host core
    /// count the probe uses when no cgroup source constrains a dimension.
    fn host(&self, mem_total_kib: u64, host_cores: u32) {
        self.write(
            "proc/meminfo",
            &format!("MemTotal:       {mem_total_kib} kB\nMemFree:        1024 kB\n"),
        );
        // The probe reads the host core count from an injected value (never the
        // real `available_parallelism`) so the host-fallback path is deterministic.
        self.write("host_cores", &host_cores.to_string());
    }

    /// The probe over this fixture tree, with the injected host-core count read
    /// from the seeded `host_cores` file (or 1 when absent), so no test depends on
    /// the CI runner's real parallelism.
    fn probe(&self) -> ContainerLimitProbe {
        let cores = fs::read_to_string(self.root.join("host_cores"))
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(1);
        ContainerLimitProbe::from_root(&self.root).with_host_cores(cores)
    }
}

/// Apply the default 20% headroom to a raw limit the way the probe does (floor,
/// then floor at 1), so the tests state the expected total in one place.
fn with_headroom(raw: u64) -> u64 {
    let kept = (raw as f64 * (1.0 - HEADROOM_DEFAULT)).floor() as u64;
    kept.max(1)
}

// ===========================================================================
// Detection precedence — the mapped headline facet
// ===========================================================================

/// **cgroup v2 is preferred when present.** A tree exposing a cgroup v2 memory
/// limit and CPU quota, *and* a cgroup v1 source with different numbers, *and* a
/// host with different numbers again, sizes the pools from the **v2** numbers
/// (minus 20% headroom); the v1 and host values are ignored.
#[test]
fn container_limits_size_the_admission_pools_from_cgroup_v2() {
    let fx = FixtureRoot::new("v2-preferred");
    // v2: 8 GiB memory, 4 cores (400000/100000).
    fx.cgroup_v2("8589934592", "400000 100000");
    // v1: different numbers that MUST be ignored.
    fx.cgroup_v1("2147483648", "200000", "100000");
    // host: different numbers again that MUST be ignored.
    fx.host(1_048_576, 16);

    let caps = fx.probe().detect().expect("v2 limits fit");

    assert_eq!(caps.total(Pool::Memory), with_headroom(8_589_934_592));
    // 4 cores → 4 threads on each thread pool, minus headroom → floor(4*0.8)=3.
    assert_eq!(caps.total(Pool::ComputeThreads), with_headroom(4));
    assert_eq!(caps.total(Pool::BlockingThreads), with_headroom(4));
}

/// **cgroup v1 fallback when v2 absent.** No cgroup v2 source, a valid cgroup v1
/// memory limit and CPU quota, and differing host numbers → the pools derive from
/// the **v1** numbers; the host numbers are ignored.
#[test]
fn cgroup_v1_is_used_when_v2_is_absent() {
    let fx = FixtureRoot::new("v1-fallback");
    // v1: 2 GiB memory, 2 cores (200000/100000). No v2 files written.
    fx.cgroup_v1("2147483648", "200000", "100000");
    fx.host(1_048_576, 16);

    let caps = fx.probe().detect().expect("v1 limits fit");

    assert_eq!(caps.total(Pool::Memory), with_headroom(2_147_483_648));
    assert_eq!(caps.total(Pool::ComputeThreads), with_headroom(2));
    assert_eq!(caps.total(Pool::BlockingThreads), with_headroom(2));
}

/// **host fallback when no cgroup exists.** Neither cgroup v2 nor v1 (the
/// dev-machine / macOS case), only host memory and CPU → the pools derive from
/// host resources, minus headroom.
#[test]
fn host_resources_are_used_when_no_cgroup_exists() {
    let fx = FixtureRoot::new("host-fallback");
    // Only host: MemTotal 1 GiB (1048576 KiB), 8 cores.
    fx.host(1_048_576, 8);

    let caps = fx.probe().detect().expect("host limits fit");

    assert_eq!(caps.total(Pool::Memory), with_headroom(1_048_576 * 1024));
    assert_eq!(caps.total(Pool::ComputeThreads), with_headroom(8));
    assert_eq!(caps.total(Pool::BlockingThreads), with_headroom(8));
}

/// **unlimited sentinel falls back to host per dimension.** A cgroup source that
/// reports the unlimited sentinel for memory (`max`) but a real CPU quota, with a
/// distinct host memory → the memory pool is sized from **host** memory (the
/// sentinel is "no limit here"), while the thread pool is still sized from the
/// **cgroup** CPU quota. The fallback is per dimension, not all-or-nothing.
#[test]
fn an_unlimited_sentinel_falls_back_to_host_per_dimension() {
    let fx = FixtureRoot::new("sentinel");
    // v2 memory is unlimited ("max"), but CPU quota is a real 2 cores.
    fx.cgroup_v2("max", "200000 100000");
    // host memory 4 GiB, host cores 32 (the cores MUST be ignored — cgroup CPU won).
    fx.host(4 * 1_048_576, 32);

    let caps = fx.probe().detect().expect("mixed limits fit");

    // Memory from host (sentinel → host), threads from the cgroup CPU quota.
    assert_eq!(caps.total(Pool::Memory), with_headroom(4 * 1_048_576 * 1024));
    assert_eq!(caps.total(Pool::ComputeThreads), with_headroom(2));
    assert_eq!(caps.total(Pool::BlockingThreads), with_headroom(2));
}

// ===========================================================================
// Headroom and the at-least-one-unit floor
// ===========================================================================

/// **20% headroom is the default.** A round, known cgroup memory limit and CPU
/// quota with no pinning flag → each pool total equals the detected limit reduced
/// by the 20% headroom fraction, demonstrably less than the raw limit.
#[test]
fn the_headroom_fraction_defaults_to_twenty_percent() {
    let fx = FixtureRoot::new("headroom");
    // 10 GiB, 10 cores — round numbers so the 20% cut is exact.
    fx.cgroup_v2("10737418240", "1000000 100000");
    fx.host(1, 1);

    let caps = fx.probe().detect().expect("fits");

    // 20% headroom → 80% kept.
    assert_eq!(caps.total(Pool::Memory), 10_737_418_240 / 5 * 4);
    assert!(caps.total(Pool::Memory) < 10_737_418_240);
    // 10 cores → floor(10 * 0.8) = 8 threads.
    assert_eq!(caps.total(Pool::ComputeThreads), 8);
    assert!(caps.total(Pool::ComputeThreads) < 10);
}

/// **at least one unit per pool under a fractional quota.** A sub-one-CPU quota
/// (half a core) and a very small memory limit → the thread pool still has at
/// least one thread and the memory pool at least one unit, even though the
/// headroom-adjusted arithmetic would otherwise round to zero.
#[test]
fn every_pool_gets_at_least_one_unit_under_a_fractional_quota() {
    let fx = FixtureRoot::new("floor");
    // Half a core (50000/100000) and a tiny 3-byte memory limit.
    fx.cgroup_v2("3", "50000 100000");
    fx.host(1, 1);

    let caps = fx.probe().detect().expect("fits with the floor");

    // floor(0.5 * 0.8) = 0 → floored up to 1 thread.
    assert_eq!(caps.total(Pool::ComputeThreads), 1);
    assert_eq!(caps.total(Pool::BlockingThreads), 1);
    // floor(3 * 0.8) = 2, still ≥ 1; a 1-byte limit would floor up to 1.
    assert!(caps.total(Pool::Memory) >= 1);
}

// ===========================================================================
// The pinning flag — overrides BOTH detection sources
// ===========================================================================

/// **pinning flag overrides cgroup detection.** A cgroup source with real limits
/// plus the operator flag pinning the memory pool (and the compute pool) to an
/// explicit value → the pinned pools equal the flag's value regardless of what
/// cgroup detection found; unpinned pools still derive from detection.
#[test]
fn the_pinning_flag_overrides_cgroup_detection() {
    let fx = FixtureRoot::new("pin-cgroup");
    fx.cgroup_v2("8589934592", "800000 100000"); // 8 GiB, 8 cores
    fx.host(1, 1);

    let pins = PinnedPools::new()
        .memory(1_000)
        .compute_threads(2);
    let caps = fx
        .probe()
        .with_pins(pins)
        .detect()
        .expect("pinned fits");

    // Pinned pools are exactly the flag values, ignoring detection + headroom.
    assert_eq!(caps.total(Pool::Memory), 1_000);
    assert_eq!(caps.total(Pool::ComputeThreads), 2);
    // The unpinned blocking pool still derives from the cgroup CPU quota.
    assert_eq!(caps.total(Pool::BlockingThreads), with_headroom(8));
}

/// **pinning flag overrides host fallback.** No cgroup source (the host-fallback
/// path) plus the pinning flag → the pinned pool equals the flag value, not the
/// host-derived value; the flag beats both detection sources C12 names.
#[test]
fn the_pinning_flag_overrides_host_fallback() {
    let fx = FixtureRoot::new("pin-host");
    fx.host(4 * 1_048_576, 8); // no cgroup — host only

    let pins = PinnedPools::new().memory(2_048);
    let caps = fx
        .probe()
        .with_pins(pins)
        .detect()
        .expect("pinned fits");

    // Pinned to the flag, not the host-derived value.
    assert_eq!(caps.total(Pool::Memory), 2_048);
    // The unpinned thread pools still derive from the host core count.
    assert_eq!(caps.total(Pool::ComputeThreads), with_headroom(8));
}

/// **The pinning flag lives in the `dagr.`-reserved namespace.** The operator
/// pins pools by the library-reserved key convention (`dagr.pool.memory`,
/// `dagr.pool.compute-threads`, `dagr.pool.blocking-threads`); a key outside the
/// reserved namespace is rejected, so a task cannot smuggle a capacity override.
#[test]
fn pinning_keys_are_in_the_reserved_namespace() {
    let mut pins = PinnedPools::new();
    pins.set_flag("dagr.pool.memory", "4096")
        .expect("reserved key accepted");
    assert_eq!(pins.memory_pin(), Some(4_096));

    let err = pins
        .set_flag("app.pool.memory", "1")
        .expect_err("non-reserved key rejected");
    assert!(err.contains("dagr."), "the error names the reserved prefix");
}

// ===========================================================================
// Too-big-node rejection at bootstrap — the bootstrap-failed artifact
// ===========================================================================

/// **too-big node rejected at bootstrap, not at admission.** A node whose declared
/// cost for some pool exceeds that pool's headroom-adjusted total (capacity pinned
/// to a known small value) → bootstrap fails fast; the error names the offending
/// node, the pool, the declared cost, and the pool capacity.
#[test]
fn a_too_big_node_is_rejected_at_bootstrap_not_at_admission() {
    // Capacity pinned to a known small value (CI-deterministic).
    let fx = FixtureRoot::new("too-big");
    fx.host(1, 1);
    let caps = fx
        .probe()
        .with_pins(PinnedPools::new().memory(1_000).compute_threads(2))
        .detect()
        .expect("caps derived");

    // A node demanding 5000 bytes against a 1000-byte pool: can NEVER fit.
    let nodes = vec![
        ("small".to_string(), PoolCost::new().working_memory(500)),
        (
            "hog".to_string(),
            PoolCost::new().working_memory(5_000),
        ),
    ];

    let failure = detect_capacities(&caps, &nodes)
        .expect_err("bootstrap rejects the too-big node before admission");

    // The bootstrap-failed outcome, distinct from an assembly failure.
    assert_eq!(failure.outcome(), BootstrapOutcome::BootstrapFailed);
    // The complete error list names the offending node, pool, cost, and capacity.
    assert_eq!(failure.errors().len(), 1);
    let e = &failure.errors()[0];
    assert_eq!(e.node(), "hog");
    assert_eq!(e.pool(), Pool::Memory);
    assert_eq!(e.declared_cost(), 5_000);
    assert_eq!(e.capacity(), 1_000);
    let rendered = failure.to_string();
    assert!(rendered.contains("hog"));
    assert!(rendered.contains("5000"));
    assert!(rendered.contains("1000"));
}

/// **bootstrap-failure artifact is asserted, not assumed.** The too-big-node
/// scenario yields exactly one bootstrap-failure artifact with outcome
/// `bootstrap-failed` (distinct from assembly-failed), carrying the complete error
/// list and **zero attempts** — the same assertion T30's resource-check test makes.
#[test]
fn the_too_big_rejection_produces_the_bootstrap_failure_artifact() {
    let caps = dagr_core::admission::PoolCapacities::new().compute_threads(2);
    let nodes = vec![(
        "needs-four".to_string(),
        PoolCost::new().compute_threads(4),
    )];

    let failure =
        detect_capacities(&caps, &nodes).expect_err("bootstrap fails on the too-big node");

    assert_eq!(failure.outcome(), BootstrapOutcome::BootstrapFailed);
    assert_ne!(failure.outcome(), BootstrapOutcome::Succeeded);
    // Zero attempts recorded — nothing executed.
    assert_eq!(failure.attempts_recorded(), 0);
    // The complete error list is present.
    assert_eq!(failure.errors().len(), 1);
    assert_eq!(failure.errors()[0].node(), "needs-four");
    assert_eq!(failure.errors()[0].pool(), Pool::ComputeThreads);
}

/// **a node exactly at capacity is admitted, not rejected.** A node whose declared
/// cost for every pool exactly equals that pool's headroom-adjusted total → bootstrap
/// succeeds and the node is a candidate for admission (the rule is strictly
/// "exceeds", so the boundary case fits).
#[test]
fn a_node_exactly_at_capacity_is_admitted_not_rejected() {
    let caps = dagr_core::admission::PoolCapacities::new()
        .memory(1_000)
        .compute_threads(3);
    let nodes = vec![(
        "exact".to_string(),
        PoolCost::new().working_memory(1_000).compute_threads(3),
    )];

    // Exactly at capacity fits — no bootstrap failure.
    let ok = detect_capacities(&caps, &nodes);
    assert!(ok.is_ok(), "a node exactly at capacity is admitted");
}

/// **every too-big node is reported, not just the first.** Bootstrap collects the
/// complete error list across all nodes (the "complete error report" C12/T14
/// discipline), not short-circuiting on the first offender.
#[test]
fn bootstrap_reports_every_too_big_node() {
    let caps = dagr_core::admission::PoolCapacities::new().memory(100);
    let nodes = vec![
        ("a".to_string(), PoolCost::new().working_memory(200)),
        ("ok".to_string(), PoolCost::new().working_memory(50)),
        ("b".to_string(), PoolCost::new().working_memory(300)),
    ];

    let failure = detect_capacities(&caps, &nodes).expect_err("two nodes over capacity");
    assert_eq!(failure.errors().len(), 2);
    let named: Vec<&str> = failure.errors().iter().map(CapacityError::node).collect();
    assert!(named.contains(&"a"));
    assert!(named.contains(&"b"));
    assert!(!named.contains(&"ok"));
}

use dagr_core::limits::CapacityError;

// ===========================================================================
// Malformed cgroup values fall back rather than panicking
// ===========================================================================

/// **malformed cgroup values fall back to host.** A cgroup file with garbage
/// (unparseable) content is treated as "no usable limit here" and the probe falls
/// back to host resources for that dimension — detection never panics on a
/// hostile `/sys`.
#[test]
fn malformed_cgroup_values_fall_back_to_host() {
    let fx = FixtureRoot::new("malformed");
    // Garbage in the v2 memory and cpu files.
    fx.cgroup_v2("not-a-number", "garbage");
    fx.host(2 * 1_048_576, 4);

    let caps = fx.probe().detect().expect("falls back cleanly");

    // Both dimensions fell back to host (malformed → host).
    assert_eq!(caps.total(Pool::Memory), with_headroom(2 * 1_048_576 * 1024));
    assert_eq!(caps.total(Pool::ComputeThreads), with_headroom(4));
}

// Silence an unused-import lint when the CapacityBootstrapFailure type alias is
// referenced only through `detect_capacities`' return type in some builds.
#[allow(dead_code)]
fn _assert_failure_type(f: CapacityBootstrapFailure) -> BootstrapOutcome {
    f.outcome()
}
