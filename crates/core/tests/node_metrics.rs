//! C23 · Node metrics — ticket T44 (055). Written first, TDD.
//!
//! Exercises the open, unschematized per-attempt metrics facility that
//! `dagr_core::metrics` provides (arch.md `### C23 · Node metrics`): the attach
//! API on an attempt's metric set, numeric-only values, the reserved `dagr.`
//! prefix, the entry-count and byte-size caps with deterministic recorded
//! truncation, the framework-contributed measurements (peak memory, permit
//! sizes, phase timings), and the instrumented per-attempt allocator whose
//! peak is attributed via task-local state (not process RSS).
//!
//! # `unsafe` note
//!
//! The single `#![allow(unsafe_code)]` below is for installing the metrics
//! crate's own [`AttributingAllocator`] as this **test binary's**
//! `#[global_allocator]` — implementing/installing a `GlobalAlloc` is inherently
//! `unsafe`. The allocator forwards every call to the system allocator and only
//! updates atomics + a thread-local; it adds no unsafety of its own.
#![allow(unsafe_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use dagr_core::metrics::{
    AttemptMetrics, AttributingAllocator, MetricError, MAX_ENCODED_BYTES, MAX_ENTRIES,
    METRIC_PEAK_MEMORY_BYTES, METRIC_TRUNCATED_DROPPED_BYTES, METRIC_TRUNCATED_DROPPED_ENTRIES,
    RESERVED_PREFIX,
};

// The metrics allocator installed as this test binary's global allocator so the
// per-attempt peak-memory tests measure the real, attributing allocator.
#[global_allocator]
static ALLOC: AttributingAllocator = AttributingAllocator::new();

// === Open, unschematized attach ============================================

#[test]
fn a_task_attaches_a_novel_metric_with_no_framework_change() {
    // The name is not known to the framework and never was — no enum, no
    // registry edit is required to accept it.
    let mut m = AttemptMetrics::new();
    m.attach("widgets_frobnicated", 7u64).expect("novel metric accepted");

    let entries = m.collected();
    let got = entries
        .iter()
        .find(|(k, _)| k == "widgets_frobnicated")
        .expect("the novel metric is present");
    assert_eq!(got.1, 7.0, "the exact numeric value survives");
}

#[test]
fn numeric_value_shapes_are_all_accepted_and_stored_as_numbers() {
    let mut m = AttemptMetrics::new();
    m.attach("rows_read", 42u64).unwrap();
    m.attach("bytes_spilled", 1_234i64).unwrap();
    m.attach("ratio_fraction", 0.5f64).unwrap();
    m.attach("count_small", 3u32).unwrap();

    let v = |name: &str| m.collected().into_iter().find(|(k, _)| k == name).unwrap().1;
    assert_eq!(v("rows_read"), 42.0);
    assert_eq!(v("bytes_spilled"), 1234.0);
    assert_eq!(v("ratio_fraction"), 0.5);
    assert_eq!(v("count_small"), 3.0);
    // The API surface only accepts `Into<MetricValue>` (numeric); a &str or a
    // bool does not implement it, so a non-numeric attach fails to COMPILE. That
    // is asserted by the fact this file compiles at all with numeric-only calls.
}

// === Reserved prefix ========================================================

#[test]
fn reserved_prefix_is_rejected_at_attach_time_naming_the_metric() {
    let mut m = AttemptMetrics::new();
    let name = format!("{RESERVED_PREFIX}sneaky");
    let err = m.attach(&name, 1u64).expect_err("reserved-prefix attach must fail");
    match err {
        MetricError::ReservedPrefix { metric } => {
            assert_eq!(metric, name, "the error names the offending metric");
        }
        other => panic!("expected ReservedPrefix, got {other:?}"),
    }
    // The reserved-prefixed value is NOT present in the collected set.
    assert!(
        m.collected().iter().all(|(k, _)| k != &name),
        "the rejected metric never entered the set"
    );
}

#[test]
fn a_name_that_merely_contains_but_does_not_start_with_the_prefix_is_accepted() {
    let mut m = AttemptMetrics::new();
    // Contains "dagr." mid-string but does not start with it.
    m.attach("my_dagr.metric", 5u64)
        .expect("a boundary name that does not START with the prefix is accepted");
    assert!(m.collected().iter().any(|(k, _)| k == "my_dagr.metric"));
}

// === Caps and deterministic truncation ======================================

#[test]
fn entry_count_cap_truncates_deterministically_and_records_it() {
    // Attach more than the cap of distinct measurements, in one order.
    let mut a = AttemptMetrics::new();
    for i in 0..(MAX_ENTRIES + 50) {
        a.attach(&format!("m_{i:04}"), i as u64).unwrap();
    }
    a.finalize_task_metrics();

    // The same inputs attached in a DIFFERENT order must yield the same survivors.
    let mut b = AttemptMetrics::new();
    for i in (0..(MAX_ENTRIES + 50)).rev() {
        b.attach(&format!("m_{i:04}"), i as u64).unwrap();
    }
    b.finalize_task_metrics();

    let names = |m: &AttemptMetrics| -> Vec<String> {
        m.collected()
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| !k.starts_with(RESERVED_PREFIX))
            .collect()
    };
    let sa = names(&a);
    let sb = names(&b);
    assert_eq!(sa.len(), MAX_ENTRIES, "exactly the cap's worth of task metrics survive");
    assert_eq!(sa, sb, "survivors are order-independent (deterministic rule)");

    // Truncation recorded as a framework metric under the reserved prefix.
    let dropped = a
        .collected()
        .into_iter()
        .find(|(k, _)| k == METRIC_TRUNCATED_DROPPED_ENTRIES)
        .expect("dropped-entries framework metric recorded");
    assert_eq!(dropped.1, 50.0, "records how many entries were dropped");
}

#[test]
fn byte_size_cap_truncates_deterministically_and_records_it() {
    // Stay well under the entry-count cap, but exceed the byte cap with long
    // names.
    let mut m = AttemptMetrics::new();
    let big_name = "x".repeat(1000);
    let mut n = 0;
    while n < 100 {
        m.attach(&format!("{big_name}_{n:03}"), n as u64).unwrap();
        n += 1;
    }
    m.finalize_task_metrics();

    assert!(m.encoded_size() <= MAX_ENCODED_BYTES, "encoded set held at or under the byte cap");
    let dropped_bytes = m
        .collected()
        .into_iter()
        .find(|(k, _)| k == METRIC_TRUNCATED_DROPPED_BYTES)
        .expect("dropped-bytes framework metric recorded");
    assert!(dropped_bytes.1 > 0.0, "records that bytes were dropped");
}

#[test]
fn recording_truncation_does_not_re_trigger_a_cap_violation() {
    // Fill exactly to the entry cap, then force overflow so truncation fires.
    let mut m = AttemptMetrics::new();
    for i in 0..(MAX_ENTRIES + 10) {
        m.attach(&format!("k_{i:04}"), i as u64).unwrap();
    }
    m.finalize_task_metrics();

    // Adding the framework truncation records must not itself push the set back
    // over the caps.
    assert!(m.collected().len() <= MAX_ENTRIES + m.framework_metric_count());
    assert!(m.encoded_size() <= MAX_ENCODED_BYTES);
    // And the recorded figure is consistent with what was dropped.
    let dropped = m
        .collected()
        .into_iter()
        .find(|(k, _)| k == METRIC_TRUNCATED_DROPPED_ENTRIES)
        .unwrap();
    assert_eq!(dropped.1, 10.0);
}

// === Framework metrics present with no task metrics =========================

#[test]
fn framework_metrics_present_when_the_task_attaches_nothing() {
    let mut m = AttemptMetrics::new();
    // The task attaches nothing. Populate framework measurements as the runtime
    // would at the attempt's terminal.
    m.set_permit_sizes(&[("memory_bytes", 1024), ("compute_threads", 2)]);
    m.set_phase_timings(&[("executing_ns", 5_000), ("permit_wait_ns", 10)]);
    m.set_peak_memory_bytes(4096);
    m.finalize_task_metrics();

    let has = |name: &str| m.collected().iter().any(|(k, _)| k == name);
    assert!(has(METRIC_PEAK_MEMORY_BYTES), "peak memory present under dagr.");
    assert!(
        m.collected().iter().any(|(k, _)| k.starts_with("dagr.permit.")),
        "permit sizes present under dagr."
    );
    assert!(
        m.collected().iter().any(|(k, _)| k.starts_with("dagr.phase.")),
        "phase timings present under dagr."
    );
    // Every framework metric is under the reserved prefix.
    for (k, _) in m.collected() {
        if k == METRIC_PEAK_MEMORY_BYTES
            || k.starts_with("dagr.permit.")
            || k.starts_with("dagr.phase.")
        {
            assert!(k.starts_with(RESERVED_PREFIX));
        }
    }
}

#[test]
fn every_builtin_metric_name_follows_the_units_in_name_convention() {
    let mut m = AttemptMetrics::new();
    m.set_permit_sizes(&[("memory_bytes", 1), ("compute_threads", 1)]);
    m.set_phase_timings(&[("executing_ns", 1)]);
    m.set_peak_memory_bytes(1);
    m.finalize_task_metrics();

    // Documented unit suffixes; every built-in (dagr.-prefixed non-flag) name
    // carries one of them, per docs/conventions/metric-naming.md.
    let unit_suffixes = ["_bytes", "_ns", "_threads", "_count", "_entries"];
    for (k, _) in m.collected() {
        if !k.starts_with(RESERVED_PREFIX) {
            continue;
        }
        // The truncation "occurred" markers are counts/entries; every built-in
        // carries a documented unit suffix.
        assert!(
            unit_suffixes.iter().any(|s| k.ends_with(s)),
            "built-in `{k}` must carry a documented unit suffix"
        );
    }
}

// === Per-attempt peak memory via the attributing allocator ==================

#[test]
fn peak_memory_is_per_attempt_not_process_wide_under_concurrency() {
    let barrier = Arc::new(Barrier::new(2));
    let big = 4 * 1024 * 1024; // A's large, held allocation.
    let small = 64 * 1024; // B's small allocation.

    let b_a = Arc::clone(&barrier);
    let a = thread::spawn(move || {
        let _g = AttributingAllocator::enter_attempt();
        let held: Vec<u8> = vec![0xAB; big];
        b_a.wait(); // hold while B allocates its small amount
        let peak = AttributingAllocator::attempt_peak_bytes();
        b_a.wait();
        drop(held);
        peak
    });
    let b_b = Arc::clone(&barrier);
    let b = thread::spawn(move || {
        let _g = AttributingAllocator::enter_attempt();
        let held: Vec<u8> = vec![0xCD; small];
        b_b.wait();
        b_b.wait();
        let peak = AttributingAllocator::attempt_peak_bytes();
        drop(held);
        peak
    });

    let peak_a = a.join().unwrap();
    let peak_b = b.join().unwrap();

    // A's peak reflects roughly A's own allocation; B's reflects B's — neither is
    // inflated by the other's live allocation.
    assert!(peak_a >= big, "A's peak covers its own large allocation ({peak_a} >= {big})");
    assert!(peak_b < big / 2, "B's peak is NOT inflated by A's live allocation ({peak_b})");
    assert!(peak_b >= small, "B's peak covers its own small allocation ({peak_b} >= {small})");
}

#[test]
fn peak_memory_tracks_the_high_water_mark_not_the_residual() {
    let _g = AttributingAllocator::enter_attempt();
    let high = 2 * 1024 * 1024;
    {
        let _big: Vec<u8> = vec![7u8; high];
        // high-water reached here
    } // freed
    let _small: Vec<u8> = vec![1u8; 4096]; // smaller residual
    let peak = AttributingAllocator::attempt_peak_bytes();
    assert!(peak >= high, "peak reflects the high-water point ({peak} >= {high}), not the residual");
}

#[test]
fn allocations_outside_any_attempt_are_unattributed_and_the_allocator_is_correct() {
    // Allocate with no attempt in task-local state.
    let outside: Vec<u8> = vec![0u8; 3 * 1024 * 1024];
    // Force a read/write so the allocation is not optimized away.
    let sum: usize = outside.iter().map(|&b| b as usize).sum();
    assert_eq!(sum, 0);

    // A subsequent, freshly-entered attempt does not see the outside bytes.
    let peak = {
        let _g = AttributingAllocator::enter_attempt();
        let small: Vec<u8> = vec![9u8; 8192];
        let p = AttributingAllocator::attempt_peak_bytes();
        drop(small);
        p
    };
    drop(outside);
    assert!(
        peak < 3 * 1024 * 1024,
        "outside allocations do not appear in the attempt's peak ({peak})"
    );
    // The allocator behaved correctly (no panic) with no attempt current — the
    // outside Vec was allocated, read, and freed above without incident.
    let _witness = AtomicUsize::new(0);
    _witness.fetch_add(1, Ordering::SeqCst);
}
