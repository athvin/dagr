//! The C23 **node metrics** facility (arch.md `### C23 · Node metrics`).
//!
//! Every attempt reports what it measured — rows read, bytes spilled, and the
//! like — while the framework simultaneously contributes what only it can know:
//! allocator-attributed peak memory, permit sizes, and phase timings. This
//! module is that facility, kept **dependency-free** (it uses only `std`, so
//! `dagr-core`'s review-gated dependency set is untouched — arch.md
//! "Stability").
//!
//! # What it is
//!
//! [`AttemptMetrics`] is an **open, unschematized** per-attempt metric set:
//!
//! - **Open by design.** A task attaches a named numeric measurement with
//!   [`AttemptMetrics::attach`] — no framework enum, registry, or release is
//!   required to accept a novel name (arch.md C23 "adding a new measurement must
//!   never require a framework release").
//! - **Numeric only.** A metric value is a [`MetricValue`] (a wrapped `f64`);
//!   the attach API takes `impl Into<MetricValue>`, and only the numeric
//!   primitives implement it — a `&str` or `bool` does not, so a non-numeric
//!   value **fails to compile**.
//! - **Units in the name.** Values carry their unit as a **name suffix**
//!   (`rows_read`, `bytes_spilled`, `..._ns`), per the documented convention in
//!   [`docs/conventions/metric-naming.md`](../../../../docs/conventions/metric-naming.md);
//!   every built-in metric this module emits follows it.
//! - **Reserved prefix.** The [`RESERVED_PREFIX`] (`dagr.`) is reserved for
//!   framework metrics: a task attaching under it fails **loudly and
//!   immediately** with a [`MetricError::ReservedPrefix`] naming the offending
//!   metric, and the value is not recorded. A name that merely *contains* the
//!   prefix mid-string (`my_dagr.metric`) is accepted.
//! - **Hard caps with deterministic recorded truncation.** At most
//!   [`MAX_ENTRIES`] (128) task entries and [`MAX_ENCODED_BYTES`] (16 KiB)
//!   encoded survive; overflow is truncated by a **deterministic, order-
//!   independent** rule (keep the lexicographically-smallest names), and the
//!   truncation's occurrence and extent are themselves recorded as framework
//!   metrics under the reserved prefix — without re-triggering a cap violation.
//! - **Framework measurements always present.** Peak memory, permit sizes, and
//!   phase timings are populated on every attempt regardless of what the task
//!   attaches (populated by the runtime; hand-set in tests).
//!
//! # Determinism
//!
//! The metric **names** and the **truncation** rule are deterministic — the same
//! inputs always yield the same survivors and the same dropped set, independent
//! of attach order. Timing and peak-memory **values** are *observational*: they
//! reflect a real run and are not deterministic across runs. They are reported
//! in the run artifact but are **never** baked into a structural or policy
//! fingerprint (C21) — the fingerprint is over the *graph and policy*, not over
//! an execution's measurements. This module produces no fingerprint input.
//!
//! # Peak memory: the attributing allocator
//!
//! [`AttributingAllocator`] is a `#[global_allocator]`-installable allocator
//! that attributes each allocation to the **running attempt** via **task-local
//! (thread-local) state**. Under concurrent nodes in one process it records what
//! the *attempt* allocated — the honest per-node number that belongs next to a
//! declared cost — **not** process RSS. An attempt scope is entered with
//! [`AttributingAllocator::enter_attempt`]; the returned guard tracks that
//! attempt's live and high-water bytes until it is dropped. Allocations made
//! with **no** attempt current are unattributed (they touch no attempt's peak),
//! and the allocator behaves correctly (never panics) in that case. Installing
//! the allocator is the binary's choice (a single `#[global_allocator]` static);
//! this module ships the type, and the run loop / tests install it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

// === Caps and the reserved prefix ==========================================

/// The reserved framework-metric name prefix (arch.md C23). A **task** attaching
/// a metric whose name **starts with** this prefix fails at attach time
/// ([`MetricError::ReservedPrefix`]); the framework's own metrics all live under
/// it.
pub const RESERVED_PREFIX: &str = "dagr.";

/// The per-attempt cap on the number of distinct **task** measurements
/// (arch.md C23). Task entries past this cap are truncated deterministically;
/// framework metrics (under [`RESERVED_PREFIX`]) are added on top and do not
/// count against it.
pub const MAX_ENTRIES: usize = 128;

/// The per-attempt cap on the **encoded** size of the metric set, in bytes
/// (arch.md C23: 16 KiB). "Encoded size" is a deterministic, serialization-
/// independent proxy: the sum over surviving entries of `name.len()` plus
/// [`VALUE_ENCODED_BYTES`] (the UTF-8 name bytes plus a fixed budget for the
/// numeric value). See [`AttemptMetrics::encoded_size`].
pub const MAX_ENCODED_BYTES: usize = 16 * 1024;

/// The fixed per-entry byte budget attributed to a metric's numeric value in the
/// [`encoded_size`](AttemptMetrics::encoded_size) accounting — one IEEE-754
/// `f64` is 8 bytes. Using a fixed budget (rather than a formatted-number width)
/// keeps the cap a deterministic function of the *names* present, independent of
/// how a value happens to print.
pub const VALUE_ENCODED_BYTES: usize = 8;

// === Built-in metric names (units in the name; all under the reserved prefix) ==

/// Allocator-attributed peak memory for the attempt, in bytes (arch.md C23).
pub const METRIC_PEAK_MEMORY_BYTES: &str = "dagr.peak_memory_bytes";

/// The framework flag recording that this attempt's task metrics were truncated
/// (1 when any cap fired, else 0). A count so it validates as a number.
pub const METRIC_TRUNCATED: &str = "dagr.metrics.truncated_count";
/// The number of task entries dropped by the entry-count cap (arch.md C23).
pub const METRIC_TRUNCATED_DROPPED_ENTRIES: &str = "dagr.metrics.dropped_entries_count";
/// The number of encoded bytes dropped by the byte-size cap (arch.md C23).
pub const METRIC_TRUNCATED_DROPPED_BYTES: &str = "dagr.metrics.dropped_bytes_count";

/// The `dagr.permit.` namespace prefix under which per-pool admission-permit
/// sizes are recorded (arch.md C23). Each entry is `dagr.permit.<pool_unit>`
/// (e.g. `dagr.permit.memory_bytes`, `dagr.permit.compute_threads`).
pub const PERMIT_PREFIX: &str = "dagr.permit.";

/// The `dagr.phase.` namespace prefix under which per-phase timings are recorded
/// (arch.md C23). Each entry is `dagr.phase.<phase>_ns` (e.g.
/// `dagr.phase.executing_ns`).
pub const PHASE_PREFIX: &str = "dagr.phase.";

// === Errors ================================================================

/// The error [`AttemptMetrics::attach`] reports (arch.md C23).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricError {
    /// A **task** attempted to attach a metric whose name begins with the
    /// reserved [`RESERVED_PREFIX`]. The attach failed loudly and immediately,
    /// the value was **not** recorded, and this carries the offending name so the
    /// error can name it (arch.md C23: "fails loudly at attach time, naming the
    /// offending metric").
    ReservedPrefix {
        /// The offending metric name (starts with [`RESERVED_PREFIX`]).
        metric: String,
    },
}

impl std::fmt::Display for MetricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricError::ReservedPrefix { metric } => write!(
                f,
                "metric `{metric}` uses the reserved `{RESERVED_PREFIX}` prefix, which is for \
                 framework metrics only — rename it without that prefix"
            ),
        }
    }
}

impl std::error::Error for MetricError {}

// === MetricValue (numeric only) ============================================

/// A metric **value** — numeric only (arch.md C23). Wraps an `f64`; the attach
/// API takes `impl Into<MetricValue>`, and only the numeric primitives implement
/// [`From`] into it, so a `&str` or `bool` value **fails to compile**. Integer
/// inputs (`u64`, `i64`, `u32`, …) are carried losslessly within `f64`'s exact-
/// integer range and serialize as JSON numbers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricValue(f64);

impl MetricValue {
    /// The value as an `f64` — the form the artifact's open numeric map
    /// (`schemas/run/v1.schema.json`, `additionalProperties: {"type":"number"}`)
    /// carries.
    #[must_use]
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

macro_rules! metric_value_from {
    ($($t:ty),* $(,)?) => {
        $(
            impl From<$t> for MetricValue {
                #[allow(clippy::cast_lossless, clippy::cast_precision_loss)]
                fn from(v: $t) -> Self {
                    MetricValue(v as f64)
                }
            }
        )*
    };
}
// Only numeric primitives convert — deliberately excluding bool/&str/char so a
// non-numeric attach fails to compile (the "numeric-only" type-surface rule).
metric_value_from!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64);

// === AttemptMetrics ========================================================

/// The **open, unschematized per-attempt metric set** (arch.md C23).
///
/// A task attaches numeric measurements with [`attach`](Self::attach); the
/// runtime contributes framework measurements ([`set_peak_memory_bytes`](Self::set_peak_memory_bytes),
/// [`set_permit_sizes`](Self::set_permit_sizes), [`set_phase_timings`](Self::set_phase_timings));
/// [`finalize_task_metrics`](Self::finalize_task_metrics) applies the caps with
/// deterministic recorded truncation. The collected set is read with
/// [`collected`](Self::collected) and threaded into the attempt record so T42's
/// fold carries it to the run artifact unmodified.
///
/// Hand-constructable for tests: `AttemptMetrics::new()`, attach, set framework
/// fields, finalize, read.
#[derive(Debug, Clone, Default)]
pub struct AttemptMetrics {
    // Task-attached metrics, keyed by name for a deterministic (name-ordered)
    // truncation and stable output. A BTreeMap makes attach-order irrelevant.
    task: BTreeMap<String, f64>,
    // Framework metrics, all under RESERVED_PREFIX, kept separate so they never
    // count against the task caps and are never truncated.
    framework: BTreeMap<String, f64>,
    // Set once finalize applies the caps, so encoded_size/collected report the
    // truncated task set.
    finalized: bool,
}

impl AttemptMetrics {
    /// A fresh, empty metric set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a task metric — an open, unschematized named numeric measurement
    /// (arch.md C23). Re-attaching the same name overwrites (a metric is a
    /// single measurement, not an event log).
    ///
    /// # Errors
    ///
    /// Returns [`MetricError::ReservedPrefix`] if `name` **starts with** the
    /// reserved [`RESERVED_PREFIX`] — the attach fails loudly and immediately and
    /// the value is not recorded. A name that merely contains the prefix
    /// mid-string is accepted.
    pub fn attach(
        &mut self,
        name: impl Into<String>,
        value: impl Into<MetricValue>,
    ) -> Result<(), MetricError> {
        let name = name.into();
        if name.starts_with(RESERVED_PREFIX) {
            return Err(MetricError::ReservedPrefix { metric: name });
        }
        self.task.insert(name, value.into().as_f64());
        Ok(())
    }

    /// Record the allocator-attributed **peak memory** for the attempt, in bytes
    /// — a framework metric under [`METRIC_PEAK_MEMORY_BYTES`]. Populated by the
    /// runtime from [`AttributingAllocator::attempt_peak_bytes`]; hand-set in
    /// tests.
    pub fn set_peak_memory_bytes(&mut self, bytes: u64) {
        self.framework
            .insert(METRIC_PEAK_MEMORY_BYTES.to_string(), numeric(bytes));
    }

    /// Record the attempt's granted **admission-permit sizes** (arch.md C23) —
    /// one framework metric per pool under [`PERMIT_PREFIX`], keyed
    /// `dagr.permit.<pool_unit>`. `pool_unit` carries its unit by convention
    /// (`memory_bytes`, `compute_threads`). This only *reads and reports* the
    /// sizes an attempt was granted; it does not size pools (C12/C5).
    pub fn set_permit_sizes(&mut self, sizes: &[(&str, u64)]) {
        for (pool_unit, size) in sizes {
            self.framework
                .insert(format!("{PERMIT_PREFIX}{pool_unit}"), numeric(*size));
        }
    }

    /// Record the attempt's **phase timings** (arch.md C23) — one framework
    /// metric per phase under [`PHASE_PREFIX`], keyed `dagr.phase.<phase>_ns`.
    /// `phase` carries its `_ns` unit by convention. Observational (wall-derived
    /// durations); never a fingerprint input.
    pub fn set_phase_timings(&mut self, phases: &[(&str, u64)]) {
        for (phase, ns) in phases {
            self.framework
                .insert(format!("{PHASE_PREFIX}{phase}"), numeric(*ns));
        }
    }

    /// Apply the entry-count and byte-size caps to the **task** metrics with a
    /// deterministic, order-independent truncation, and record the truncation as
    /// framework metrics under the reserved prefix (arch.md C23).
    ///
    /// The survivor rule is **keep the lexicographically-smallest names** up to
    /// the caps — a pure function of the *set* of names, so the same inputs
    /// attached in any order yield the same survivors and the same dropped set.
    /// The recorded truncation figures ([`METRIC_TRUNCATED`],
    /// [`METRIC_TRUNCATED_DROPPED_ENTRIES`], [`METRIC_TRUNCATED_DROPPED_BYTES`])
    /// are framework metrics, so adding them never re-triggers a task-cap
    /// violation (there is no cap-violation feedback loop). Idempotent: a second
    /// call is a no-op.
    pub fn finalize_task_metrics(&mut self) {
        if self.finalized {
            return;
        }
        self.finalized = true;

        // Enforce the entry-count cap first: keep the smallest MAX_ENTRIES names.
        // BTreeMap iterates in ascending key order, so this is deterministic and
        // order-independent.
        let original_entries = self.task.len();
        let mut kept: BTreeMap<String, f64> = BTreeMap::new();
        let mut encoded = 0usize;
        let mut dropped_entries = 0usize;
        let mut dropped_bytes = 0usize;

        for (name, value) in std::mem::take(&mut self.task) {
            let cost = entry_encoded_size(&name);
            let over_entry_cap = kept.len() >= MAX_ENTRIES;
            let over_byte_cap = encoded + cost > MAX_ENCODED_BYTES;
            if over_entry_cap || over_byte_cap {
                dropped_entries += 1;
                dropped_bytes += cost;
                continue;
            }
            encoded += cost;
            kept.insert(name, value);
        }
        let _ = original_entries; // (documented invariant: kept + dropped == original)

        self.task = kept;

        let truncated = dropped_entries > 0;
        self.framework
            .insert(METRIC_TRUNCATED.to_string(), f64::from(u8::from(truncated)));
        self.framework.insert(
            METRIC_TRUNCATED_DROPPED_ENTRIES.to_string(),
            numeric(dropped_entries),
        );
        self.framework.insert(
            METRIC_TRUNCATED_DROPPED_BYTES.to_string(),
            numeric(dropped_bytes),
        );
    }

    /// The **encoded size** of the current task metric set, in bytes — the sum
    /// over task entries of `name.len()` plus [`VALUE_ENCODED_BYTES`]. This is the
    /// deterministic proxy the byte-size cap ([`MAX_ENCODED_BYTES`]) is enforced
    /// against; after [`finalize_task_metrics`](Self::finalize_task_metrics) it
    /// is at or under the cap. Framework metrics do not count against it.
    #[must_use]
    pub fn encoded_size(&self) -> usize {
        self.task.keys().map(|n| entry_encoded_size(n)).sum()
    }

    /// The number of framework metrics currently present — the always-present
    /// truncation records plus any peak/permit/phase entries set. Used to reason
    /// about the total collected size relative to the task cap.
    #[must_use]
    pub fn framework_metric_count(&self) -> usize {
        self.framework.len()
    }

    /// The complete collected metric set — **task and framework entries both**,
    /// as `(name, value)` pairs in ascending name order (deterministic). This is
    /// the form threaded into the attempt record; the driver renders it into the
    /// `attempt-outcome` record's open numeric `metrics` map, and T42's fold
    /// carries it to the run artifact **unmodified**.
    #[must_use]
    pub fn collected(&self) -> Vec<(String, f64)> {
        // Merge two ascending-ordered maps into one ascending-ordered vec. Names
        // never collide (framework names are all under RESERVED_PREFIX, task
        // names never start with it), so the union is well-defined.
        let mut out: Vec<(String, f64)> =
            Vec::with_capacity(self.task.len() + self.framework.len());
        out.extend(self.task.iter().map(|(k, v)| (k.clone(), *v)));
        out.extend(self.framework.iter().map(|(k, v)| (k.clone(), *v)));
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

/// The encoded byte cost attributed to one metric entry: the UTF-8 name bytes
/// plus the fixed numeric-value budget ([`VALUE_ENCODED_BYTES`]).
fn entry_encoded_size(name: &str) -> usize {
    name.len() + VALUE_ENCODED_BYTES
}

/// Convert any numeric primitive to the stored `f64` through [`MetricValue`],
/// the single place the (justified) integer-to-float conversion lives — so no
/// raw `as f64` cast (which the pedantic `cast_precision_loss` lint denies)
/// appears at the framework-metric call sites. dagr's metric values are within
/// `f64`'s exact-integer range in practice (byte/ns/count magnitudes), and the
/// artifact carries them as JSON numbers regardless.
fn numeric(v: impl Into<MetricValue>) -> f64 {
    v.into().as_f64()
}

// === The attributing global allocator ======================================

// Per-attempt attribution lives in a thread-local: the *current* attempt's live
// and high-water byte counts, plus a depth so nested `enter_attempt` scopes on
// one thread compose. When no attempt is current the allocator does not attribute
// (the counters are untouched). Process-wide there is a small always-on live
// counter used only to snapshot a baseline; the load-bearing figures are the
// per-attempt thread-locals.
thread_local! {
    // (live_bytes, peak_bytes, depth). `depth == 0` means no attempt is current
    // on this thread, so allocations are unattributed.
    static ATTEMPT: Cell<(u64, u64, u32)> = const { Cell::new((0, 0, 0)) };
}

/// A process-wide count of live allocated bytes, maintained across *all*
/// threads; used so the allocator is always well-formed even before any attempt
/// scope exists. Not the per-attempt figure (that is the thread-local).
static PROCESS_LIVE: AtomicU64 = AtomicU64::new(0);

/// A `#[global_allocator]`-installable allocator that attributes allocations to
/// the **running attempt** via task-local (thread-local) state, yielding a
/// per-node peak (arch.md C23).
///
/// It forwards every call unchanged to the [`System`] allocator and updates the
/// current attempt's live/peak byte counts around it. Under concurrent nodes in
/// one process each attempt runs on its own worker thread inside its own
/// [`enter_attempt`](Self::enter_attempt) scope, so one attempt's live
/// allocation never inflates another's peak — the figure is what the *attempt*
/// allocated, not process RSS. With **no** attempt current the counters are left
/// untouched (unattributed) and the allocator still behaves correctly.
///
/// Install it once, as the binary's single global allocator:
///
/// ```ignore
/// use dagr_core::metrics::AttributingAllocator;
/// #[global_allocator]
/// static ALLOC: AttributingAllocator = AttributingAllocator::new();
/// ```
pub struct AttributingAllocator;

impl AttributingAllocator {
    /// Construct the allocator (a zero-sized handle). `const` so it can back a
    /// `#[global_allocator]` static.
    #[must_use]
    pub const fn new() -> Self {
        AttributingAllocator
    }

    /// Enter an **attempt scope** on the current thread: allocations made while
    /// the returned [`AttemptScope`] guard is live are attributed to *this*
    /// attempt's live/peak counts (arch.md C23). The scope's live and peak start
    /// at zero and track only allocations made under it. Dropping the guard
    /// leaves the attempt scope; nested scopes on one thread compose (the peak of
    /// an outer scope still reflects the inner scope's high-water while it was
    /// live).
    ///
    /// Under concurrency each worker thread enters its own scope, so peaks never
    /// cross-contaminate — the state is thread-local.
    #[must_use]
    pub fn enter_attempt() -> AttemptScope {
        ATTEMPT.with(|c| {
            let (live, peak, depth) = c.get();
            if depth == 0 {
                // Fresh top-level attempt scope: reset live/peak to zero so the
                // attempt measures only its own allocations, not any residual.
                c.set((0, 0, 1));
            } else {
                c.set((live, peak, depth + 1));
            }
        });
        AttemptScope { _priv: () }
    }

    /// The current attempt's **peak** (high-water) live bytes on this thread —
    /// the number to record as [`METRIC_PEAK_MEMORY_BYTES`]. Zero when no attempt
    /// is current. Reflects the highest point reached during the attempt, not the
    /// residual at the end.
    #[must_use]
    pub fn attempt_peak_bytes() -> u64 {
        ATTEMPT.with(|c| c.get().1)
    }

    /// The current attempt's **live** bytes on this thread — bytes allocated and
    /// not yet freed under the current attempt scope. Zero when no attempt is
    /// current.
    #[must_use]
    pub fn attempt_live_bytes() -> u64 {
        ATTEMPT.with(|c| c.get().0)
    }

    /// The process-wide live allocated bytes (all threads) — a well-formedness
    /// witness, **not** the per-attempt figure. Never used as the C23 peak.
    #[must_use]
    pub fn process_live_bytes() -> u64 {
        PROCESS_LIVE.load(Ordering::SeqCst)
    }
}

impl Default for AttributingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// The guard returned by [`AttributingAllocator::enter_attempt`]. While it is
/// live, allocations on this thread are attributed to the attempt; dropping it
/// leaves the attempt scope (decrementing the nesting depth). Not `Send`: an
/// attempt scope is bound to the thread that entered it, which is exactly the
/// task-local attribution C23 requires.
#[derive(Debug)]
pub struct AttemptScope {
    // Not constructible outside this module; not Send/Sync (Cell is !Sync, and
    // the raw unit keeps it thread-bound in spirit even though the field is ()).
    _priv: (),
}

impl Drop for AttemptScope {
    fn drop(&mut self) {
        ATTEMPT.with(|c| {
            let (live, peak, depth) = c.get();
            if depth <= 1 {
                // Leaving the top-level scope: clear attribution so later
                // unattributed allocations on this thread do not accrue.
                c.set((0, 0, 0));
            } else {
                c.set((live, peak, depth - 1));
            }
        });
    }
}

/// Attribute `size` newly-allocated bytes to the current attempt (if any) and to
/// the process-wide witness. Raises the attempt peak to the new live figure.
fn on_alloc(size: usize) {
    let size64 = size as u64;
    PROCESS_LIVE.fetch_add(size64, Ordering::SeqCst);
    ATTEMPT.with(|c| {
        let (live, peak, depth) = c.get();
        if depth == 0 {
            return; // no attempt current — unattributed
        }
        let new_live = live.saturating_add(size64);
        let new_peak = peak.max(new_live);
        c.set((new_live, new_peak, depth));
    });
}

/// Attribute `size` freed bytes: decrement the current attempt's live figure and
/// the process-wide witness. The peak is a high-water mark and is never lowered.
fn on_dealloc(size: usize) {
    let size64 = size as u64;
    PROCESS_LIVE.fetch_sub(size64, Ordering::SeqCst);
    ATTEMPT.with(|c| {
        let (live, peak, depth) = c.get();
        if depth == 0 {
            return; // no attempt current — unattributed
        }
        let new_live = live.saturating_sub(size64);
        c.set((new_live, peak, depth)); // peak unchanged (high-water mark)
    });
}

// SAFETY: `AttributingAllocator` forwards every call unchanged to the `System`
// allocator and only updates atomics / a thread-local `Cell` around it. It adds
// no unsafety of its own beyond the forwarding the `GlobalAlloc` contract
// already requires of `System`, and it never allocates on the accounting path
// (the thread-local access and atomic ops do not allocate), so it cannot
// re-enter itself.
//
// The workspace lint policy is `unsafe_code = "warn"` under `-D warnings`
// (docs/lint-policy.md: "`unsafe` is not forbidden outright but every use is
// surfaced for review"). Implementing `GlobalAlloc` is *inherently* `unsafe` —
// the C23 attributing allocator cannot be expressed in safe Rust — so this one
// impl block carries a scoped, justified `allow`, the only `unsafe` in the
// module.
#[allow(
    unsafe_code,
    reason = "GlobalAlloc is an inherently-unsafe trait; the C23 attributing allocator (arch.md C23) must implement it — it only forwards to System and updates atomics/thread-local"
)]
unsafe impl GlobalAlloc for AttributingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        on_dealloc(layout.size());
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // realloc changes the live figure by the delta between old and new.
            let old = layout.size();
            if new_size >= old {
                on_alloc(new_size - old);
            } else {
                on_dealloc(old - new_size);
            }
        }
        new_ptr
    }
}
