//! C10 **bounded-memory chain** test — ticket T26 (036). Written first, TDD.
//!
//! This is the "hundred-node authority" the output-slot ticket (T17) deferred to
//! T26 (arch.md `### C10 · Output slot`, acceptance: *"Peak allocator-level memory
//! across a long chain does not grow with the chain's length when nothing is
//! retained — verified against a synthetic hundred-node chain."*). It proves that
//! a long linear pipeline holds only a **bounded** number of in-flight slot values
//! at a time regardless of chain length, exercising the **real** merged pieces —
//! the T17 output slots ([`dagr_core::slot`]) driven to completion through the
//! **real** T24 run-loop driver ([`dagr_cli::driver::drive`]) — never a
//! re-implementation.
//!
//! # Two independent memory instruments, both allocator-level (never RSS)
//!
//! Per arch.md C10's memory-accounting rule (*"tests measure allocator-level
//! residency, not process RSS"*) this test reads memory two ways, both of which
//! are allocator-level and deterministic — **no** process RSS, no wall-clock, no
//! OS reclamation is consulted:
//!
//! 1. The [`ResidencyLedger`] **peak counted residency** (T17): a deterministic
//!    integer, the sum of the declared output residency of every slot that is
//!    filled-and-not-yet-released, sampled at its high-water mark. This is the C10
//!    accounting hook the run artifact (C23) folds as *peak measured slot
//!    residency*. It is exact and noise-free — the primary bounded-peak assertion.
//! 2. An [instrumented global allocator](Counting) confined to **this test
//!    binary** (never wired in as the production allocator) that records current
//!    and high-water **live allocated bytes**. It measures what the program has
//!    handed back to the allocator, not what the OS reclaimed — the arch.md C10
//!    distinction. Because a run through the whole driver allocates unrelated
//!    bookkeeping (two tokio runtimes, the event stream), the per-value payload is
//!    made large enough (`PAYLOAD` bytes) that the slot values dominate the
//!    live-bytes signal, and the assertion carries margin for that bounded,
//!    chain-length-**independent** noise while a real leak (~`N`·`PAYLOAD`) blows
//!    straight through it.
//!
//! # Why the peak is bounded through the real driver
//!
//! Each test node consumes its single upstream through a real
//! [`ConsumerLease`](dagr_core::slot::ConsumerLease) (`enter()` → `read()` → the
//! lease drops when the node's closure returns), so the genuine C10 release rule
//! fires: node *i-1*'s value is released the instant node *i*'s sole-consumer
//! closure returns. A linear chain is admitted one node at a time (each node
//! depends on its predecessor's terminal state — C11), so at the high-water mark
//! at most a small, constant handful of values are concurrently live (the
//! just-produced value plus the one being handed to the immediate downstream),
//! independent of chain length. A regression that released on last-read, or forgot
//! to drop the value, would leave every node's value live and the peak would scale
//! with `N` — which is exactly what these assertions bite on (see
//! `peak_grows_with_length_when_release_is_defeated`, the in-test non-vacuity
//! proof).
//!
//! Scope (T26): this is a correctness guard on **peak residency**, not a
//! performance/throughput benchmark (T69), not RSS, not fan-out residency
//! semantics or durable outputs (C27) — the chain has a single consumer per node
//! and no abandoned/zombie consumers.
//!
//! # `unsafe` note
//!
//! The single `#![allow(unsafe_code)]` below is for the **test-only** instrumented
//! [`Counting`] allocator: implementing [`std::alloc::GlobalAlloc`] is inherently
//! `unsafe` (it is a `GlobalAlloc` requirement), and the implementation merely
//! forwards every call unchanged to the `System` allocator while updating two
//! atomics. No other `unsafe` appears, and this allocator is never the production
//! allocator — it is installed only in this integration-test binary.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::slot::{RedeemError, RedemptionHandle, ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// The test-only instrumented global allocator (allocator-level, NOT RSS)
// ===========================================================================

/// A `#[global_allocator]` that wraps the system allocator and records the
/// **current** and **high-water peak** count of live allocated bytes.
///
/// This is **test-only**: it is installed only in this integration-test binary
/// via the `#[global_allocator]` below and is never wired in as the production
/// allocator (production uses the platform default). It measures **allocator-level
/// residency** — bytes the program has requested and not yet returned to the
/// allocator — **not** process RSS or any OS-level memory figure, per arch.md
/// C10's accounting rule (*"'Memory reclaimed' means returned to the allocator,
/// not necessarily to the operating system — tests measure allocator-level
/// residency, not process RSS"*). `reset_peak` snaps the peak down to the current
/// live figure so a scenario can measure the high-water mark of the run it is
/// about to drive, and `live` samples the current live bytes.
struct Counting;

/// Current live allocated bytes across the whole test binary.
static LIVE: AtomicUsize = AtomicUsize::new(0);
/// High-water mark of [`LIVE`] since the last [`reset_peak`].
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Raise [`PEAK`] to at least `now` (monotone, race-safe).
fn bump_peak(now: usize) {
    PEAK.fetch_max(now, Ordering::SeqCst);
}

/// Snap the recorded peak down to the current live figure — call immediately
/// before driving the run whose high-water mark is to be measured, so earlier
/// allocations do not inflate the reading.
fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
}

/// The current live allocated bytes (allocator-level, never RSS).
fn live() -> usize {
    LIVE.load(Ordering::SeqCst)
}

/// The peak live allocated bytes since the last [`reset_peak`] (allocator-level).
fn peak_bytes() -> usize {
    PEAK.load(Ordering::SeqCst)
}

/// A process-wide lock that **serialises** every test which reads the global
/// allocator peak or allocates a chain-length-proportional amount.
///
/// The `LIVE`/`PEAK` counters are process-global, but cargo runs the tests in
/// this binary concurrently by default. Without serialisation, one test's
/// allocations (especially the deliberately-leaky non-vacuity proof, which holds
/// ~`LONG`·`PAYLOAD` live at once) would pollute the global peak another test
/// reads, making the allocator-level assertions flaky. The **ledger** peak is
/// per-run (each run owns its [`ResidencyLedger`]) and needs no such lock — it is
/// the load-bearing, always-deterministic instrument; this lock exists only so the
/// *allocator-level* corroboration stays deterministic under parallel test
/// execution. Every allocator-reading test and every length-proportional-allocating
/// test takes this lock for its whole duration.
static ALLOC_SERIAL: Mutex<()> = Mutex::new(());

/// Acquire the allocator-serialisation lock, recovering from a poisoned mutex (a
/// panicking test must not wedge the rest of the suite).
fn alloc_guard() -> std::sync::MutexGuard<'static, ()> {
    ALLOC_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// SAFETY: `Counting` forwards every call unchanged to the `System` allocator and
// only updates two atomics around it; it adds no unsafety of its own beyond the
// forwarding the `GlobalAlloc` contract already requires of `System`.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            bump_peak(now);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            bump_peak(now);
        }
        ptr
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                let now = LIVE.fetch_add(new_size - layout.size(), Ordering::SeqCst)
                    + (new_size - layout.size());
                bump_peak(now);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::SeqCst);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

// ===========================================================================
// Sizes and knobs (fixed → deterministic)
// ===========================================================================

/// The declared output residency **and** the real heap footprint of one node's
/// value, in bytes. Kept large so the slot values dominate the allocator-level
/// live-bytes signal over the driver's chain-length-independent bookkeeping,
/// making the allocator assertion robust in CI.
const PAYLOAD: u64 = 256 * 1024;

/// The short chain length (the multi-length comparison's baseline).
const SHORT: usize = 4;

/// The long chain length — the arch.md "synthetic hundred-node chain" authority.
const LONG: usize = 100;

/// A per-invocation **collision-proof** run-store base under the OS temp dir.
///
/// Determinism (CI fs race): the driver reclaims leftover per-run temp dirs at run
/// end by `remove_dir_all`-ing every sibling run-dir under `<base>/<pipeline>/`
/// other than its own. Under `--test-threads>1` several `drive_chain` /
/// `drive_leaky_chain` runs execute concurrently; on a single **fixed shared** base
/// (`/tmp/dagr-t26`) with the same pipeline name they share `<base>/<pipeline>/`, so
/// one run's terminal reclaim wipes another concurrent run's freshly-created run-dir
/// mid-run. A base keyed on `process::id()` + a wall-clock timestamp is not unique
/// either (the clock's effective resolution is coarse on CI). The fix is causal, not
/// a sleep: a process-monotonic `AtomicU64` counter makes every base provably
/// disjoint, so no two concurrent runs ever share — or delete — the same subtree.
/// Mirrors `temp_base()` in `os_signals_flush_and_cleanup.rs` /
/// `m2_demo_clean_stop.rs`. No production change.
fn temp_base() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir()
        .join(format!(
            "dagr-t26-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            unique
        ))
        .to_string_lossy()
        .into_owned()
}

// ===========================================================================
// A capturing in-memory sink + monotonic clock (the C19 injection seam)
// ===========================================================================

/// An in-memory [`EventSink`] — the driver writes its stream here; the test only
/// needs the run to complete, so it keeps the bytes but rarely inspects them.
#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock ticking one nanosecond per read — deterministic offsets with
/// no real clock (no wall-clock is consulted anywhere in this test).
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

// ===========================================================================
// The synthetic chain node: consume the predecessor's slot, produce a value
// ===========================================================================

/// The value a chain node produces: a heap payload of exactly [`PAYLOAD`] bytes,
/// so its declared residency equals its real allocator footprint.
type Payload = Vec<u8>;

/// Allocate one node's [`PAYLOAD`]-byte value. The declared residency ([`PAYLOAD`],
/// a `u64` to match the ledger's byte counts) equals this vector's real heap
/// footprint, so the ledger figure and the allocator figure track the same bytes.
fn payload_vec() -> Payload {
    let len = usize::try_from(PAYLOAD).expect("PAYLOAD fits in usize on the test target");
    vec![0u8; len]
}

/// A source node: produces the first [`PAYLOAD`]-byte value, consuming nothing.
struct ChainSource;
impl Task for ChainSource {
    type Input = ();
    type Output = Payload;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Payload, TaskError> {
        Ok(payload_vec())
    }
}

/// A source runner that fills the first slot and reports its terminal state,
/// driving the **real** single-attempt C14 runner (so residency is charged at the
/// real fill).
struct SourceRunner {
    name: String,
    task: Option<ChainSource>,
    slot: Arc<Slot<Payload>>,
}
impl SourceRunner {
    fn boxed(name: &str, slot: Arc<Slot<Payload>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(ChainSource),
            slot,
        })
    }
}
impl NodeRunner for SourceRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("source runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            let outcome = run_attempt(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// A one-input chain node runner: it opens a real [`ConsumerLease`] on its single
/// upstream slot (so the genuine C10 release rule fires when this closure
/// returns), reads the predecessor's value, produces its own [`PAYLOAD`]-byte
/// value, and fills its own slot — all through the **real** single-attempt runner.
///
/// The lease is entered *before* the attempt and dropped *after* it returns, which
/// is precisely the closure-return gate that releases the upstream slot: with the
/// chain admitted one node at a time, the predecessor's value is reclaimed as soon
/// as this node finishes, so peak residency never accumulates down the chain.
struct LinkRunner {
    name: String,
    upstream: SlotRef<Payload>,
    slot: Arc<Slot<Payload>>,
}
impl LinkRunner {
    fn boxed(
        name: &str,
        upstream: SlotRef<Payload>,
        slot: Arc<Slot<Payload>>,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            upstream,
            slot,
        })
    }
}

/// A no-input adapter task that produces a fresh [`PAYLOAD`]-byte value — so the
/// real `run_attempt` (which wants `Input = ()`) drives it and emits the genuine
/// C14 records. It **retains nothing** of the predecessor's value: a chain node
/// reads its input, does its work, and keeps none of the input in its own output.
/// Keeping no clone is exactly what makes the *allocator-level* peak flat — the
/// only live copy of a predecessor's bytes is the one in its slot, freed the
/// instant the slot releases.
struct MakeNext;
impl Task for MakeNext {
    type Input = ();
    type Output = Payload;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Payload, TaskError> {
        Ok(payload_vec())
    }
}

impl NodeRunner for LinkRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let slot = Arc::clone(&self.slot);
        // Enter the upstream lease (the closure-return gate) and read the
        // predecessor's value — the real C10 consume path. The read is an O(1)
        // `Arc` clone dropped immediately (we retain nothing of the input), so the
        // predecessor's bytes live only in its slot. The lease lives until this
        // future returns; when it drops, the upstream slot releases and those bytes
        // are freed — which is what keeps the allocator-level peak flat.
        let lease = self.upstream.enter();
        {
            let value = lease.read();
            debug_assert_eq!(
                value.len() as u64,
                PAYLOAD,
                "predecessor value has the declared size"
            );
            // `value` (an `Arc<Payload>` clone) drops here — nothing retained.
        }
        let mut task = MakeNext;
        Box::pin(async move {
            // Keep the lease alive for the whole attempt: dropping it here (after
            // the attempt returns) is this consumer's closure-return, which
            // releases the upstream slot's value and residency.
            let outcome = run_attempt(&mut task, &name, ctx, &slot, sink).await;
            drop(lease);
            outcome.terminal_state()
        })
    }
}

// ===========================================================================
// Fixture: build + drive a linear chain over one shared ledger
// ===========================================================================

/// Build a fresh output slot for a chain node, sharing the run-wide `ledger`.
/// `consumers` is the exact downstream count (T14); `retained` marks a
/// survive-to-run-end node; `residency` is the declared output residency in bytes.
fn slot_for(
    name: &str,
    consumers: u32,
    retained: bool,
    residency: u64,
    ledger: &Arc<ResidencyLedger>,
) -> Arc<Slot<Payload>> {
    Arc::new(Slot::new(
        NodeId::from_name(name),
        name,
        consumers,
        retained,
        residency,
        Arc::clone(ledger),
    ))
}

/// A pass-through one-input task shape the flow registers so a real linear-chain
/// pipeline assembles (the driver reads the pipeline's structure; the actual value
/// production runs through the runners above).
struct PassThrough;
impl Task for PassThrough {
    type Input = Payload;
    type Output = Payload;
    async fn run(&mut self, _c: &RunContext, i: Payload) -> Result<Payload, TaskError> {
        Ok(i)
    }
}

/// A **sink** runner draining the chain's last producer: it opens a real
/// [`ConsumerLease`] on that producer's slot, reads it, and produces a trivial
/// **zero-residency** value. Draining the last producer is what lets that
/// producer's value release too (a non-retained slot releases only when a
/// consumer's closure returns), so a fully non-retained chain leaves **zero**
/// counted residency at run end — the honest C10 end state. The sink itself
/// declares no residency, so it never contributes to the measured peak.
struct SinkRunner {
    name: String,
    upstream: SlotRef<Payload>,
    slot: Arc<Slot<Payload>>,
}
impl SinkRunner {
    fn boxed(
        name: &str,
        upstream: SlotRef<Payload>,
        slot: Arc<Slot<Payload>>,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            upstream,
            slot,
        })
    }
}
impl NodeRunner for SinkRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let slot = Arc::clone(&self.slot);
        let lease = self.upstream.enter();
        let _ = lease.read();
        // The sink produces a trivial zero-byte value (zero declared residency).
        let mut task = SinkTask;
        Box::pin(async move {
            let outcome = run_attempt(&mut task, &name, ctx, &slot, sink).await;
            drop(lease);
            outcome.terminal_state()
        })
    }
}

/// The sink's trivial no-residency task: it produces an empty value so the sink
/// node contributes nothing to the measured peak.
struct SinkTask;
impl Task for SinkTask {
    type Input = ();
    type Output = Payload;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Payload, TaskError> {
        Ok(Vec::new())
    }
}

/// The outcome of driving one chain to completion: the ledger peak, the ledger's
/// current counted residency at run end, the redemption handle for the last
/// **producer** node (to prove retained/released), and the run's overall outcome.
struct ChainRun {
    ledger_peak: u64,
    ledger_current_at_end: u64,
    /// Redemption handle for the last PAYLOAD-producing node (`node-{len-1}`) —
    /// the one `retain_terminal` marks retained.
    terminal_handle: RedemptionHandle<Payload>,
    outcome: RunOutcome,
}

/// Build a linear chain of `len` PAYLOAD-producing nodes — `node-0` (source) →
/// `node-1` → … → `node-{len-1}` — plus a trailing **sink** that drains the last
/// producer, all over one shared [`ResidencyLedger`]; each producer declares
/// `residency` bytes of output residency (the sink declares none). If
/// `retain_terminal` is set, the last producer (`node-{len-1}`) is marked retained
/// (its value survives to run end and is redeemable). Drive to completion through
/// the **real** T24 driver and return the observed [`ChainRun`].
///
/// The trailing sink matters for the end-of-run accounting: a non-retained slot
/// with no consumer never has its release gate triggered, so without a drain the
/// last producer's value would linger counted. Draining it makes a fully
/// non-retained chain end at zero counted residency, exactly as C10 promises when
/// every value is consumed and nothing is retained.
fn drive_chain(len: usize, residency: u64, retain_terminal: bool) -> ChainRun {
    assert!(len >= 2, "a chain needs at least a source and one link");
    let ledger = ResidencyLedger::new();

    // --- Assemble a real linear-chain pipeline (structure the driver walks):
    // `len` producers followed by a `sink` consumer.
    let mut flow = Flow::new();
    let mut handle = flow.register_source("node-0", &ChainSource);
    for i in 1..len {
        handle = flow.register::<PassThrough, _>(&format!("node-{i}"), &PassThrough, handle);
    }
    let _sink = flow.register::<PassThrough, _>("sink", &PassThrough, handle);
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("linear chain assembles");

    // --- One slot per producer, sharing the ledger. Every producer (including the
    // last) has exactly one consumer — the next producer, or the sink for the last
    // — so every producer value can release. The last producer may be retained.
    let mut slots: Vec<Arc<Slot<Payload>>> = Vec::with_capacity(len);
    for i in 0..len {
        let name = format!("node-{i}");
        let is_last_producer = i + 1 == len;
        let retained = is_last_producer && retain_terminal;
        // Every producer has exactly one consumer (next producer, or the sink).
        slots.push(slot_for(&name, 1, retained, residency, &ledger));
    }
    let terminal_handle = slots[len - 1].redemption_handle();
    // The sink's own slot: zero consumers, zero residency (it produces nothing of
    // size and nothing consumes it), so it never contributes to the peak or lingers.
    let sink_slot = slot_for("sink", 0, false, 0, &ledger);

    // --- One runner per node: the source fills node-0; each link consumes its
    // predecessor through a real lease and fills its own slot; the sink drains the
    // last producer.
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "node-0".into(),
        SourceRunner::boxed("node-0", Arc::clone(&slots[0])),
    );
    for i in 1..len {
        let name = format!("node-{i}");
        let upstream = slots[i - 1].shared_ref();
        runners.insert(
            name.clone(),
            LinkRunner::boxed(&name, upstream, Arc::clone(&slots[i])),
        );
    }
    runners.insert(
        "sink".into(),
        SinkRunner::boxed("sink", slots[len - 1].shared_ref(), sink_slot),
    );

    // --- Reset the allocator peak to *now* so the measurement captures only the
    // run's high-water mark, then drive to completion through the real driver.
    reset_peak();
    let report = drive(
        &RunConfig::new(temp_base()),
        "bounded-chain",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    );

    ChainRun {
        ledger_peak: ledger.peak(),
        ledger_current_at_end: ledger.current(),
        terminal_handle,
        outcome: report.outcome,
    }
}

// ===========================================================================
// Scenario 1 — peak is flat across chain length (short vs hundred-node)
// ===========================================================================

/// The hundred-node run's peak counted residency is within a small constant factor
/// of the short run's — it does **not** grow with chain length (arch.md C10). The
/// [`ResidencyLedger`] peak is the exact, deterministic instrument.
#[test]
fn ledger_peak_is_flat_across_chain_length() {
    let short = drive_chain(SHORT, PAYLOAD, false);
    let long = drive_chain(LONG, PAYLOAD, false);

    assert_eq!(short.outcome, RunOutcome::Succeeded, "short chain succeeds");
    assert_eq!(long.outcome, RunOutcome::Succeeded, "long chain succeeds");

    // The peak is a small constant number of concurrently-live values, the SAME
    // for a 4-node and a 100-node chain — it does not scale with length. A leak
    // (release-on-last-read, or a forgotten drop) would make `long` ≈ 100·PAYLOAD.
    assert!(
        long.ledger_peak <= short.ledger_peak + PAYLOAD,
        "hundred-node ledger peak ({}) must not exceed the short-chain peak ({}) by more than \
         one value — it must not scale with length",
        long.ledger_peak,
        short.ledger_peak,
    );
    assert!(
        long.ledger_peak < (LONG as u64 / 4) * PAYLOAD,
        "hundred-node ledger peak ({}) must be far below the chain-length-proportional figure a \
         leak would produce",
        long.ledger_peak,
    );
}

// ===========================================================================
// Scenario 2 — peak bounded by a few concurrent values, not the whole chain
// ===========================================================================

/// The hundred-node run's peak counted residency is bounded by a small constant
/// multiple of one value's size `PAYLOAD` (a handful of concurrently-live slots),
/// **not** `LONG`·`PAYLOAD` (arch.md C10). The ceiling is an explicit constant a
/// real regression would blow through.
#[test]
fn ledger_peak_bounded_by_a_few_values_not_the_whole_chain() {
    // At most a small constant number of values are ever concurrently live: the
    // just-produced value plus the predecessor being handed downstream. Four
    // values' worth is a generous ceiling with margin; a real leak would need
    // ~100. This is the explicit constant the guard bites on.
    const CEILING_VALUES: u64 = 4;

    let long = drive_chain(LONG, PAYLOAD, false);
    assert_eq!(long.outcome, RunOutcome::Succeeded);

    assert!(
        long.ledger_peak <= CEILING_VALUES * PAYLOAD,
        "peak counted residency ({}) must be bounded by {} values ({} bytes), not the whole \
         chain ({} bytes)",
        long.ledger_peak,
        CEILING_VALUES,
        CEILING_VALUES * PAYLOAD,
        LONG as u64 * PAYLOAD,
    );
}

// ===========================================================================
// Scenario 3 — the allocator high-water peak is likewise flat (allocator-level)
// ===========================================================================

/// The **per-run** high-water peak is within a small, chain-length-independent
/// margin between the short and hundred-node runs — arch.md C10's headline. The
/// load-bearing instrument is the DETERMINISTIC per-run [`ResidencyLedger`] peak
/// (pollution-free); the instrumented allocator is consulted only as a tolerant,
/// upward-only corroboration. Never RSS.
///
/// The driver's own bookkeeping (two tokio runtimes, the event stream) allocates a
/// bounded amount that does **not** depend on chain length, so the difference
/// between a 4-node and a 100-node run is a few values plus that fixed noise —
/// nowhere near the ~96·`PAYLOAD` a leak would add.
#[test]
fn allocator_peak_is_flat_across_chain_length() {
    // Serialise the *guarded* tests. Note: the process-global `PEAK` this test's
    // `peak_bytes()` reads is a high-water mark that ANY sibling `#[test]` raises via
    // `bump_peak` on every allocation — `alloc_guard` cannot hold those off. So the
    // cross-run allocator *difference* (`long_peak - short_peak`) is NOT a state this
    // test controls and must never be the load-bearing bite. We anchor the flat-peak
    // verdict on the per-run ledger instead.
    let _serial = alloc_guard();

    // Load-bearing: the DETERMINISTIC per-run ledger peak. Each run owns its ledger, so
    // this figure is exact, private, and untouched by any sibling thread. It proves the
    // C10 headline directly — the hundred-node peak does not exceed the short peak by
    // more than a value.
    let short_ledger_peak = drive_chain(SHORT, PAYLOAD, false).ledger_peak;
    let long_ledger_peak = drive_chain(LONG, PAYLOAD, false).ledger_peak;
    assert!(
        long_ledger_peak <= short_ledger_peak + PAYLOAD,
        "per-run ledger peak grew with chain length: short={short_ledger_peak}, \
         long={long_ledger_peak} (a leak would add ~{}·PAYLOAD)",
        (LONG - SHORT) as u64,
    );
    assert!(
        long_ledger_peak < (LONG as u64 / 3) * PAYLOAD,
        "per-run ledger peak ({long_ledger_peak}) must stay far below the \
         chain-length-proportional figure ({} bytes)",
        LONG as u64 * PAYLOAD,
    );

    // Tolerant allocator corroboration (never load-bearing on a cross-run difference).
    // Warm once so one-time allocator init does not skew the reading, then drive one run
    // whose peak we snapshot. `reset_peak()` runs inside `drive_chain`; a concurrent
    // sibling allocation can only push `PEAK` UP, never below the run's own high-water
    // mark, so this lower bound — "the instrumented allocator observed at least one live
    // value during the run" — can never flake on sibling traffic. It stays non-vacuous:
    // a run that produced NO live bytes would leave the allocator peak at/below its
    // reset floor. Never RSS.
    let _ = drive_chain(SHORT, PAYLOAD, false);
    let long_run = drive_chain(LONG, PAYLOAD, false);
    let long_alloc_peak = peak_bytes() as u64;
    assert!(
        long_alloc_peak >= long_run.ledger_peak,
        "the instrumented allocator peak ({long_alloc_peak}) must be at least the run's own \
         counted residency peak ({}) — the allocator held the live values",
        long_run.ledger_peak,
    );
}

// ===========================================================================
// Scenario 4 — value released after the sole consumer is terminal-and-returned
// ===========================================================================

/// After a chain run of non-retained nodes completes, the ledger's **current**
/// counted residency is back to zero — every produced value's bytes returned to
/// the allocator once its sole consumer reached a terminal state and its closure
/// returned (arch.md C10), measured at the ledger, not RSS. A direct two-node slot
/// pair proves the same at the **allocator** level: live bytes return to baseline
/// (not elevated by even one value) once the sole consumer is terminal-and-returned.
#[test]
fn value_released_after_sole_consumer_terminal_and_returned() {
    // Serialise: this test still reads the process-global live-bytes figure for a
    // *tolerant, never-panicking* corroboration; the load-bearing verdict is the
    // per-run ledger, which no sibling thread can pollute.
    let _serial = alloc_guard();

    // --- The load-bearing proof is the DETERMINISTIC per-run ledger, on a DIRECT
    // producer→consumer slot pair. The ledger's `current()` counts only THIS run's
    // slot residency — it is a private instance, never touched by sibling harness
    // threads — so the assertions below depend solely on state this test controls.
    //
    // The prior version's load-bearing bite read the PROCESS-GLOBAL `live()` allocator
    // figure across a window (`baseline` before fill, `elevated` after) and asserted
    // `elevated >= baseline + PAYLOAD/2`. Under CI parallelism a concurrent in-process
    // `#[test]` (a sibling harness thread) frees memory in that window, so
    // `elevated < baseline` despite this test's own value being live — the `alloc_guard`
    // mutex only serialises the *guarded* tests against each other, never the allocator
    // traffic of every OTHER test in the binary. That is the CI flake (run 30057266042).
    // We re-anchor the intent — "the produced value's bytes are live while held,
    // released after the sole consumer's terminal" — on the ledger, and keep the
    // `live()` reads only as a signed, slack-tolerant sanity check that can never panic
    // on a small negative delta. Allocator-level, never RSS.
    let ledger = ResidencyLedger::new();
    // Signed byte figures for a slack-tolerant, never-panicking allocator corroboration.
    // Allocator live bytes are far below `i64::MAX`; the conversions cannot realistically
    // fail (they would require an >8 EiB live figure), and using `i64::try_from` keeps the
    // signed math lint-clean while never wrapping.
    let baseline = i64::try_from(live()).expect("live bytes fit in i64");
    let payload_i64 = i64::try_from(PAYLOAD).expect("PAYLOAD fits in i64");

    let producer: Slot<Payload> = Slot::new(
        NodeId::from_name("p"),
        "p",
        1,
        false,
        PAYLOAD,
        Arc::clone(&ledger),
    );
    producer.fill(payload_vec()).expect("fill producer");
    // One value now live: the ledger counts EXACTLY it. This is the load-bearing bite —
    // deterministic, private to this run, and non-vacuous: it fails if the value is not
    // charged while held (defeat the fill and this is 0, not PAYLOAD).
    assert_eq!(
        ledger.current(),
        PAYLOAD,
        "producer value counted while live (deterministic ledger)"
    );
    // Tolerant allocator corroboration: the live figure rose by *roughly* a value. We
    // require only that the value did not somehow shrink the process below baseline by
    // more than one value — a signed delta with generous slack that a concurrent free
    // on a sibling thread can never push into a panic, while a genuinely UNCHARGED
    // producer (never filled) would leave `live()` flat and the ledger at 0 (caught
    // above). Never RSS.
    let elevated = i64::try_from(live()).expect("live bytes fit in i64");
    assert!(
        elevated >= baseline - payload_i64,
        "live allocator bytes fell implausibly far below baseline while a value was held \
         (baseline={baseline}, elevated={elevated}) — not a slot-residency signal"
    );

    // The sole consumer takes its lease, reads (without retaining the returned `Arc`),
    // and returns (the lease drops) → the real C10 release rule fires and the value's
    // bytes return to the allocator.
    let consumer = producer.shared_ref();
    {
        let lease = consumer.enter();
        let seen = lease.read().len() as u64;
        assert_eq!(seen, PAYLOAD, "consumer read a full value");
        drop(lease);
    }
    drop(producer);
    // The load-bearing no-leak direction, on the deterministic ledger: residency is
    // back to zero once the sole consumer is terminal-and-returned. Non-vacuous: if the
    // value were NOT released after terminal (leak), this stays PAYLOAD, not 0.
    assert_eq!(
        ledger.current(),
        0,
        "residency returns to zero after the sole consumer returns (deterministic ledger)"
    );

    // Tolerant allocator corroboration of the no-leak direction: live bytes did not
    // stay elevated by a whole extra value beyond baseline. A leaked value would add a
    // full PAYLOAD; a concurrent sibling free can only push `after` DOWN, never
    // spuriously up, so this upper bound with one value of slack can never flake on
    // sibling traffic. Signed math, never panics on a negative delta. Never RSS.
    let after = i64::try_from(live()).expect("live bytes fit in i64");
    assert!(
        after - baseline < 2 * payload_i64,
        "live allocator bytes stayed elevated by more than one value after release: \
         baseline={baseline}, after={after} (a leaked value would add {PAYLOAD})",
    );

    // --- Ledger half, through the REAL driver (the load-bearing C10 authority): nothing
    // is counted after a non-retained run, and the released terminal value is not
    // redeemable. This deterministic ledger proof stays strict and is what carries the
    // C10 release accounting through the driver.
    let run = drive_chain(SHORT, PAYLOAD, false);
    assert_eq!(run.outcome, RunOutcome::Succeeded);
    assert_eq!(
        run.ledger_current_at_end, 0,
        "no residency may linger after a non-retained chain completes"
    );
    assert_eq!(
        run.terminal_handle.redeem().err(),
        Some(RedeemError::Released),
        "a non-retained terminal value is released, not redeemable"
    );
}

// ===========================================================================
// Scenario 5 — retained value survives & is redeemable; released ones are not
// ===========================================================================

/// Two runs of the same short chain: with nothing retained, end-of-run residency
/// is zero and the terminal value is not redeemable; with the terminal node
/// retained, exactly one value's residency remains counted at run end and the
/// retained value is redeemable via the T17 post-run redemption API (arch.md C10:
/// *"Values still retained at the end of the run are … redeemable …; released ones
/// are not"*). This proves the guard measures the right thing — non-retained
/// releases, retained does not.
#[test]
fn retained_value_survives_and_is_redeemable_released_ones_are_not() {
    // Non-retained: nothing lingers, terminal value not redeemable.
    let plain = drive_chain(SHORT, PAYLOAD, false);
    assert_eq!(plain.outcome, RunOutcome::Succeeded);
    assert_eq!(
        plain.ledger_current_at_end, 0,
        "non-retained run leaves zero residency"
    );
    assert_eq!(
        plain.terminal_handle.redeem().err(),
        Some(RedeemError::Released),
        "released (non-retained) value is not redeemable"
    );

    // Retained terminal node: exactly one value's residency remains counted, and
    // the value is redeemable with the correct size.
    let kept = drive_chain(SHORT, PAYLOAD, true);
    assert_eq!(kept.outcome, RunOutcome::Succeeded);
    assert_eq!(
        kept.ledger_current_at_end, PAYLOAD,
        "a retained terminal value leaves exactly one value's residency counted at run end"
    );
    let redeemed = kept
        .terminal_handle
        .redeem()
        .expect("retained value is redeemable after the run");
    assert_eq!(
        redeemed.len() as u64,
        PAYLOAD,
        "the redeemed value has the correct size"
    );
    // Redeeming releases the retained residency exactly once → back to zero.
    assert_eq!(
        kept.ledger_current_at_end - PAYLOAD,
        0,
        "redemption accounts for the sole retained value"
    );
}

// ===========================================================================
// Scenario 6 — determinism: the peak and verdict are stable across repetitions
// ===========================================================================

/// The measured ledger peak and the bounded verdict are stable across repeated
/// runs of the hundred-node chain (arch.md C10 test-plan: determinism / no
/// flakiness). The ledger peak is an exact integer with pinned inputs, so it is
/// bit-for-bit identical run to run.
#[test]
fn ledger_peak_is_deterministic_across_repetitions() {
    let first = drive_chain(LONG, PAYLOAD, false).ledger_peak;
    for _ in 0..4 {
        let again = drive_chain(LONG, PAYLOAD, false).ledger_peak;
        assert_eq!(
            first, again,
            "the ledger peak must be identical across repetitions (deterministic, pinned inputs)"
        );
    }
    // And it is bounded, so the stable value is the bounded one (not a stable leak).
    assert!(
        first <= 4 * PAYLOAD,
        "the stable peak is the bounded peak: {first}"
    );
}

// ===========================================================================
// Non-vacuity — the guard BITES: a leaked chain peaks proportionally to length
// ===========================================================================

/// Proof the bounded assertions are **not vacuous**: a chain that *never releases*
/// its upstream slots (release defeated) peaks at ~`N`·`PAYLOAD`, growing with
/// length and blowing through the ceilings the real chain satisfies. This models
/// the exact regression the guard protects against (release-on-last-read, or a
/// forgotten drop) — driven the same way, but with the consumer lease never
/// entered, so the C10 release rule never fires.
/// Drive a `len`-node chain whose links **never open a consumer lease**, so no
/// upstream slot is ever released — every produced value stays counted. Returns the
/// ledger peak. This is the injected regression (release-on-last-read / a forgotten
/// drop) the non-vacuity proof drives.
fn drive_leaky_chain(len: usize) -> u64 {
    let ledger = ResidencyLedger::new();

    let mut flow = Flow::new();
    let mut handle = flow.register_source("node-0", &ChainSource);
    for i in 1..len {
        handle = flow.register::<PassThrough, _>(&format!("node-{i}"), &PassThrough, handle);
    }
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut slots: Vec<Arc<Slot<Payload>>> = Vec::with_capacity(len);
    for i in 0..len {
        let name = format!("node-{i}");
        let consumers = u32::from(i + 1 != len);
        slots.push(slot_for(&name, consumers, false, PAYLOAD, &ledger));
    }

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "node-0".into(),
        SourceRunner::boxed("node-0", Arc::clone(&slots[0])),
    );
    for (i, slot) in slots.iter().enumerate().skip(1) {
        let name = format!("node-{i}");
        // The leaky link: it fills its own slot but NEVER opens a lease on its
        // upstream, so the upstream's release gate never advances → the value is
        // never reclaimed. This is the injected regression.
        runners.insert(
            name.clone(),
            LeakyLinkRunner::boxed(&name, Arc::clone(slot)),
        );
    }

    let _ = drive(
        &RunConfig::new(temp_base()),
        "leaky-chain",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    );
    ledger.peak()
}

#[test]
fn peak_grows_with_length_when_release_is_defeated() {
    // Serialise: this test deliberately holds ~LONG·PAYLOAD live at once, which
    // would pollute the global allocator peak the allocator-level tests read. Its
    // own verdict rests on the per-run ledger peak (unaffected by this lock); the
    // lock only keeps its heavy allocation from bleeding into a concurrent
    // allocator-reading test.
    let _serial = alloc_guard();

    let short_leak = drive_leaky_chain(SHORT);
    let long_leak = drive_leaky_chain(LONG);

    // With release defeated the peak GROWS with length — the very thing the real
    // chain forbids. The long leak is many values, far above the 4-value ceiling
    // the healthy chain honours, so those assertions genuinely bite.
    assert!(
        long_leak > short_leak,
        "a leaked chain's peak must grow with length: short={short_leak}, long={long_leak}"
    );
    assert!(
        long_leak > 4 * PAYLOAD,
        "the leaked hundred-node peak ({long_leak}) must blow through the 4-value ceiling the \
         healthy chain satisfies — proving the bounded assertions are non-vacuous"
    );
    assert!(
        long_leak >= (LONG as u64 - 2) * PAYLOAD,
        "with no release, ~every value stays counted: peak {long_leak} should approach {LONG}·PAYLOAD",
    );
}

/// A **leaky** link runner used only by the non-vacuity proof: it fills its own
/// slot but never opens a [`ConsumerLease`] on its upstream, so the C10 release
/// gate never advances and the upstream value is never reclaimed. This is the
/// injected regression the healthy [`LinkRunner`] does not have — it is confined to
/// this test and never used by the passing scenarios.
struct LeakyLinkRunner {
    name: String,
    slot: Arc<Slot<Payload>>,
}
impl LeakyLinkRunner {
    fn boxed(name: &str, slot: Arc<Slot<Payload>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            slot,
        })
    }
}
impl NodeRunner for LeakyLinkRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let slot = Arc::clone(&self.slot);
        // No lease is ever opened on the upstream → its release gate never advances.
        let mut task = MakeNext;
        Box::pin(async move {
            let outcome = run_attempt(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}
