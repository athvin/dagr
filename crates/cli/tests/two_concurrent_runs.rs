//! Two-concurrent-runs disjointness test — ticket T67 (048). Written first, TDD.
//!
//! These exercise the **already-merged** system under test: the M1 run-loop
//! driver [`dagr_cli::driver::drive`] (T24), the run-store contract (T0.6, the
//! `<base>/<pipeline>/<run-id>/` layout), and the C19 event-stream writer (T19).
//! No production behaviour is defined or changed here — this ticket **consumes**
//! those components and proves one property against them:
//!
//! > Two simultaneous runs of one dagr binary on one machine coexist cleanly —
//! > disjoint run identities, disjoint run-store directories, no shared or
//! > colliding file, and two event streams that are each independently valid,
//! > gapless, and safely concatenable-and-partitionable by run identity
//! > (arch.md `### C19`, `## Operational model`).
//!
//! ## Why this is non-vacuous
//!
//! Each [`drive`] call is handed its **own** injected [`EventSink`] and its own
//! [`RunConfig`] (own base / own run id). The disjointness the test asserts is a
//! real per-run-store-partition guarantee, not an artefact of the harness: a
//! setup that shared one sink or one run id between the two runs would fail the
//! stream-identity and directory-disjointness assertions. Nothing in the tool
//! coordinates between the two runs (the deliberate scope boundary — "that road
//! ends in building a scheduler"); the guarantee holds because every record
//! carries its run identity and every run writes under its own id-scoped
//! directory, *not* because the two processes cooperate.
//!
//! ## Genuine, deterministic simultaneity (no race flake)
//!
//! Overlap is forced by an **observable rendezvous**, never by a sleep and never
//! by assuming one run out-races the other. A `gate` source task in each run
//! arrives at a shared [`Barrier`] of width 2 and blocks until *both* runs'
//! gate tasks have arrived — so both runs are provably mid-run, actively
//! emitting attempt events, at the same wall-clock instant, before either can
//! proceed. Both runs then complete independently. The assertions never depend
//! on interleaving order (that is the point: partition-by-identity is order
//! independent), so there is no ordering assumption to flake — the classic
//! cancellation-test race (assuming one run beats the other) is structurally
//! impossible here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt, run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// A capturing in-memory run-store sink + monotonic clock (C19 injection seam)
// ===========================================================================

/// An in-memory [`EventSink`] capturing every appended line, so a test can parse
/// the real event stream one run wrote. Each run is handed its **own**
/// `MemorySink`, so the two byte buffers are the two runs' independent streams —
/// which is exactly the disjointness under test (a shared sink would fail).
#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}

impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().clone()
    }
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

/// A monotonic clock ticking one nanosecond per read — distinct, non-decreasing
/// offsets with no real clock. Each run gets its **own** clock, so no shared
/// mutable timing state leaks between the two runs.
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
// Parsed event-stream helpers (over the real tolerant reader)
// ===========================================================================

/// A minimal parsed view of one record: its `event` kind, its `run_id`, its
/// `seq`, and its `schema_version` — the envelope fields C19 requires on every
/// record.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    kind: String,
    run_id: String,
    seq: u64,
    schema_version: String,
}

/// Parse a stream's bytes into records, asserting each envelope is well-formed
/// (every record carries a `run_id`, a `seq`, a `schema_version`, and an
/// `event` kind). Panics on a malformed record so a partial/foreign write is a
/// hard failure, not a silent skip.
fn parse_records(bytes: &[u8]) -> Vec<Record> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    assert!(
        !stream.trailing_partial_discarded,
        "a cleanly-finished run's stream has no trailing partial record"
    );
    stream
        .records
        .iter()
        .map(|rec| Record {
            kind: rec
                .get("kind")
                .and_then(|v| v.as_str())
                .expect("every record carries an event kind")
                .to_string(),
            run_id: rec
                .get("run_id")
                .and_then(|v| v.as_str())
                .expect("every record carries a run identity")
                .to_string(),
            seq: rec
                .get("seq")
                .and_then(serde_json::Value::as_u64)
                .expect("every record carries a sequence number"),
            schema_version: rec
                .get("schema_version")
                .and_then(|v| v.as_str())
                .expect("every record carries a schema version")
                .to_string(),
        })
        .collect()
}

/// Assert a single run's stream is independently valid per C19: it parses, every
/// record carries the expected identity and the schema version, it opens with a
/// `run-started` and closes with a `run-finished`, and its sequence numbers are
/// gapless and strictly increasing from `0`.
fn assert_stream_is_valid(records: &[Record], expected_run_id: &str) {
    assert!(!records.is_empty(), "a completed run's stream is non-empty");

    // Run-started opens, run-finished closes (C19: both a run-started and a
    // run-finished record present, in that framing).
    assert_eq!(
        records.first().map(|r| r.kind.as_str()),
        Some("run-started"),
        "the stream opens with run-started"
    );
    assert_eq!(
        records.last().map(|r| r.kind.as_str()),
        Some("run-finished"),
        "the stream closes with run-finished"
    );

    // Exactly one schema version across the stream, and it is a real (non-empty)
    // version — every record carries the schema version (C19).
    let schema_versions: BTreeSet<&str> =
        records.iter().map(|r| r.schema_version.as_str()).collect();
    assert_eq!(
        schema_versions.len(),
        1,
        "one schema version across the whole stream, found {schema_versions:?}"
    );
    assert!(
        schema_versions
            .iter()
            .all(|v| !v.is_empty() && v.starts_with("dagr.event-stream@")),
        "the schema version is the real C19 event-stream schema id, found {schema_versions:?}"
    );

    // Every record carries THIS run's identity (C19), and no foreign identity.
    for rec in records {
        assert_eq!(
            rec.run_id, expected_run_id,
            "record {rec:?} must carry its own run identity {expected_run_id}"
        );
    }

    // Sequence numbers are gapless and strictly increasing from 0 within the run
    // (C19). Records are in stored order, so seq must be exactly 0, 1, 2, ….
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(
            rec.seq, i as u64,
            "sequence must be gapless and strictly increasing from 0; record {i} has seq {}",
            rec.seq
        );
    }
}

// ===========================================================================
// Test tasks
// ===========================================================================

/// A one-input pass-through that returns its input unchanged.
struct PassThrough;
impl Task for PassThrough {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
        Ok(i)
    }
}

/// A source task that **rendezvouses** at a shared [`Barrier`] before it
/// succeeds. When both runs' gate tasks reach the barrier, the two runs are
/// provably mid-run and actively emitting attempt events at the same instant —
/// this is the observable-signal synchronisation that forces a genuinely
/// overlapping write window without a sleep and without assuming an order.
struct GateThenSucceed {
    barrier: Arc<Barrier>,
    value: u64,
}
impl Task for GateThenSucceed {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // Block until BOTH runs have arrived. `Barrier::wait` is a blocking call;
        // the gate node runs on the driver's blocking/async task surface and the
        // driver's framework runtime is isolated, so this cannot wedge the loop.
        self.barrier.wait();
        Ok(self.value)
    }
}

// ===========================================================================
// Type-erased node runners built on the real C14 attempt path
// ===========================================================================

/// A no-input source runner: runs its task's single attempt through the real
/// caught runner and reports the terminal state (the genuine C14 records).
struct SourceRunner<T: Task<Input = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<T::Output>>,
}
impl<T: Task<Input = ()>> SourceRunner<T> {
    fn boxed(name: &str, task: T, slot: Arc<Slot<T::Output>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot,
        })
    }
}
impl<T: Task<Input = ()>> NodeRunner for SourceRunner<T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("source runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            let outcome = run_attempt_caught(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// A one-input data runner: reads its upstream slot, binds the value into an
/// owned no-input adapter, and drives the real single-attempt runner over it.
struct MapRunner<U: Send + Sync + 'static, T: Task<Input = U>> {
    name: String,
    task: Option<T>,
    upstream: SlotRef<U>,
    slot: Arc<Slot<T::Output>>,
}
impl<U: Send + Sync + Clone + 'static, T: Task<Input = U>> MapRunner<U, T> {
    fn boxed(
        name: &str,
        task: T,
        upstream: SlotRef<U>,
        slot: Arc<Slot<T::Output>>,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            upstream,
            slot,
        })
    }
}
impl<U: Send + Sync + Clone + 'static, T: Task<Input = U>> NodeRunner for MapRunner<U, T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("map runner runs once");
        let slot = Arc::clone(&self.slot);
        let input = (*self.upstream.read()).clone();
        let mut bound = Bound {
            inner: task,
            input: Some(input),
        };
        Box::pin(async move {
            let outcome = run_attempt(&mut bound, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// An owned no-input adapter over a one-input task (so the real single-attempt
/// runner drives it and emits genuine C14 records).
struct Bound<U, T> {
    inner: T,
    input: Option<U>,
}
impl<U: Send + 'static, T: Task<Input = U>> Task for Bound<U, T> {
    type Input = ();
    type Output = T::Output;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, TaskError> {
        let input = self.input.take().expect("bound input consumed once");
        self.inner.run(ctx, input).await
    }
}

// ===========================================================================
// Pipeline + plan builders
// ===========================================================================

fn ledger() -> Arc<ResidencyLedger> {
    ResidencyLedger::new()
}

/// A fresh output slot for a node. Every slot is per-run (built here, owned by
/// its run's runner) — slot residency is per-run, never shared between runs.
fn slot_for<T: Send + Sync + 'static>(name: &str, consumers: u32) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        consumers,
        false,
        0,
        ledger(),
    ))
}

/// Build a two-node plan `gate → tail`: the `gate` source rendezvouses at the
/// shared barrier before succeeding, and `tail` passes its value through. Each
/// call builds a fresh, independent set of slots and runners — nothing is shared
/// between two builds except the barrier (the deliberate rendezvous seam).
fn gated_chain_plan(barrier: Arc<Barrier>, value: u64) -> (Pipeline, RunPlan) {
    // Register the same task type that executes (`GateThenSucceed`), so the
    // assembled graph's stable task name matches the runner — the registration
    // instance is metadata-only (the executed runner is the separate type-erased
    // `SourceRunner` below), so its barrier/value are never consulted at assembly.
    let mut flow = Flow::new();
    let gate = flow.register_source(
        "gate",
        &GateThenSucceed {
            barrier: Arc::clone(&barrier),
            value,
        },
    );
    let _tail = flow.register::<PassThrough, _>("tail", &PassThrough, gate);
    let pipeline = flow.finish();

    let gate_slot = slot_for::<u64>("gate", 1);
    let tail_slot = slot_for::<u64>("tail", 0);

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "gate".into(),
        SourceRunner::boxed(
            "gate",
            GateThenSucceed { barrier, value },
            Arc::clone(&gate_slot),
        ),
    );
    runners.insert(
        "tail".into(),
        MapRunner::boxed("tail", PassThrough, gate_slot.shared_ref(), tail_slot),
    );
    let plan = RunPlan::new(pipeline.clone(), runners);
    (pipeline, plan)
}

/// A per-process-unique run-store base under the system temp dir, so the test is
/// hermetic (no reliance on a fixed path, no external services) and two test
/// runs of the suite never collide.
fn temp_base() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "dagr-t67-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ))
}

/// The launch inputs for one concurrent run: its config, its plan, and the sink
/// it will write into (kept so the caller can read the finished stream).
struct RunLaunch {
    config: RunConfig,
    sink: MemorySink,
}

/// Launch two runs **concurrently**, each on its own OS thread, each with its
/// own injected sink + clock and its own `RunConfig` (own base / own id). The
/// two `gate` tasks rendezvous at the shared barrier, so both runs are mid-run
/// at the same instant (genuine overlap). Returns each run's report and the
/// bytes its stream captured.
fn drive_two_concurrently(
    a: RunLaunch,
    b: RunLaunch,
    barrier: &Arc<Barrier>,
) -> (
    (dagr_cli::driver::RunReport, Vec<u8>),
    (dagr_cli::driver::RunReport, Vec<u8>),
) {
    let spawn = move |launch: RunLaunch, barrier: Arc<Barrier>, value: u64| {
        std::thread::spawn(move || {
            let (pipeline, plan) = gated_chain_plan(barrier, value);
            let report = drive(
                &launch.config,
                "concurrent-pipe",
                Ok(plan),
                &[],
                launch.sink.clone(),
                TickClock::default(),
            );
            let _ = pipeline;
            (report, launch.sink.bytes())
        })
    };

    let ta = spawn(a, Arc::clone(barrier), 11);
    let tb = spawn(b, Arc::clone(barrier), 22);
    let ra = ta.join().expect("run A thread does not panic");
    let rb = tb.join().expect("run B thread does not panic");
    (ra, rb)
}

// ===========================================================================
// The tests
// ===========================================================================

/// **Two concurrent auto-minted runs stay fully disjoint.** Both runs are driven
/// concurrently against distinct bases with auto-minted `UUIDv7` identities; they
/// rendezvous mid-run so their write windows genuinely overlap. Asserted: the two
/// run identities differ; each stream carries only its own identity and the
/// schema version; each stream is independently valid, gapless, and framed by
/// run-started/run-finished; and both reach the correct terminal outcome. This is
/// the headline disjointness proof (C19: two simultaneous runs write disjoint
/// files and both produce valid streams).
#[test]
fn two_concurrent_auto_minted_runs_stay_disjoint() {
    let barrier = Arc::new(Barrier::new(2));
    let a = RunLaunch {
        config: RunConfig::new(temp_base().to_str().unwrap()),
        sink: MemorySink::default(),
    };
    let b = RunLaunch {
        config: RunConfig::new(temp_base().to_str().unwrap()),
        sink: MemorySink::default(),
    };

    let ((report_a, bytes_a), (report_b, bytes_b)) = drive_two_concurrently(a, b, &barrier);

    // Distinct run identities (auto-minted UUIDv7s are unique).
    assert_ne!(
        report_a.run_id, report_b.run_id,
        "two concurrent runs mint distinct identities"
    );

    // Both reached the correct terminal outcome — neither run was disturbed by the
    // other (each is a two-node success).
    assert_eq!(report_a.outcome, RunOutcome::Succeeded, "run A succeeds");
    assert_eq!(report_b.outcome, RunOutcome::Succeeded, "run B succeeds");
    for report in [&report_a, &report_b] {
        assert_eq!(
            report.terminal_states.get("gate").copied(),
            Some(TerminalState::Succeeded),
            "the gate node succeeded"
        );
        assert_eq!(
            report.terminal_states.get("tail").copied(),
            Some(TerminalState::Succeeded),
            "the tail node succeeded"
        );
    }

    // Each stream is independently valid and carries ONLY its own identity.
    let records_a = parse_records(&bytes_a);
    let records_b = parse_records(&bytes_b);
    assert_stream_is_valid(&records_a, &report_a.run_id);
    assert_stream_is_valid(&records_b, &report_b.run_id);

    // No cross-contamination: neither stream carries a single record of the other
    // run's identity (the negative half of the identity assertion).
    assert!(
        records_a.iter().all(|r| r.run_id != report_b.run_id),
        "run A's stream carries no record of run B's identity"
    );
    assert!(
        records_b.iter().all(|r| r.run_id != report_a.run_id),
        "run B's stream carries no record of run A's identity"
    );
}

/// **The two streams concatenate-then-partition losslessly and safely.** The raw
/// bytes of both concurrent streams are concatenated into one buffer, parsed, and
/// partitioned by run identity. Asserted: partitioning yields exactly two groups
/// matching the two identities; each group, read in stored order, equals that
/// run's original stream record-for-record; and the split is order-independent
/// because identity travels on every record (C19: records from concurrent runs
/// can be concatenated and partitioned safely).
#[test]
fn concurrent_streams_concatenate_and_partition_losslessly() {
    let barrier = Arc::new(Barrier::new(2));
    let a = RunLaunch {
        config: RunConfig::new(temp_base().to_str().unwrap()),
        sink: MemorySink::default(),
    };
    let b = RunLaunch {
        config: RunConfig::new(temp_base().to_str().unwrap()),
        sink: MemorySink::default(),
    };

    let ((report_a, bytes_a), (report_b, bytes_b)) = drive_two_concurrently(a, b, &barrier);

    let records_a = parse_records(&bytes_a);
    let records_b = parse_records(&bytes_b);

    // Concatenate the two raw streams (B then A — deliberately NOT the launch
    // order, to prove the partition does not depend on interleaving/order).
    let mut concatenated = bytes_b.clone();
    concatenated.extend_from_slice(&bytes_a);
    let combined = parse_records(&concatenated);
    assert_eq!(
        combined.len(),
        records_a.len() + records_b.len(),
        "concatenation loses no record"
    );

    // Partition by run identity.
    let identities: BTreeSet<String> = combined.iter().map(|r| r.run_id.clone()).collect();
    assert_eq!(
        identities,
        BTreeSet::from([report_a.run_id.clone(), report_b.run_id.clone()]),
        "partitioning yields exactly the two run identities"
    );

    let partition_a: Vec<Record> = combined
        .iter()
        .filter(|r| r.run_id == report_a.run_id)
        .cloned()
        .collect();
    let partition_b: Vec<Record> = combined
        .iter()
        .filter(|r| r.run_id == report_b.run_id)
        .cloned()
        .collect();

    // Each partition, in stored order, reproduces that run's original stream
    // record-for-record — lossless and exact, regardless of concatenation order.
    assert_eq!(
        partition_a, records_a,
        "run A's partition reproduces run A's stream exactly"
    );
    assert_eq!(
        partition_b, records_b,
        "run B's partition reproduces run B's stream exactly"
    );
}

/// **Two concurrent runs write disjoint run-store directories under a shared
/// base.** Both runs are launched against the **same** run-store base with
/// distinct operator-overridden identities; each `drive` creates its real per-run
/// directory `<base>/<pipeline>/<run-id>/…` on disk. Asserted: exactly two run
/// directories exist under `<base>/<pipeline>/`, their run-id segments differ, the
/// reported stream paths differ and each embeds its own id, and neither directory
/// path is a prefix of the other (disjoint subtrees). This is the store-layout
/// disjointness guarantee (C19: two simultaneous runs write disjoint files),
/// proven with operator-supplied ids so it is not an artefact of `UUIDv7`
/// monotonicity.
#[test]
fn concurrent_runs_write_disjoint_directories_under_a_shared_base() {
    let base = temp_base();
    let base_str = base.to_str().unwrap().to_string();
    let pipeline = "concurrent-pipe";
    let barrier = Arc::new(Barrier::new(2));

    // A SHARED base, distinct operator-supplied ids — the store-layout property,
    // not a UUIDv7 artefact.
    let a = RunLaunch {
        config: RunConfig::new(base_str.clone()).run_id("operator-run-alpha"),
        sink: MemorySink::default(),
    };
    let b = RunLaunch {
        config: RunConfig::new(base_str.clone()).run_id("operator-run-beta"),
        sink: MemorySink::default(),
    };

    let ((report_a, bytes_a), (report_b, bytes_b)) = drive_two_concurrently(a, b, &barrier);

    // Directories are named by the supplied identities (verbatim), and differ.
    assert_eq!(report_a.run_id, "operator-run-alpha");
    assert_eq!(report_b.run_id, "operator-run-beta");
    assert_ne!(report_a.run_id, report_b.run_id);

    // The two reported stream paths differ and each embeds its own run id under the
    // shared base — the C19 `<base>/<pipeline>/<run-id>/…` layout.
    assert_ne!(
        report_a.stream_path, report_b.stream_path,
        "two concurrent runs report disjoint stream paths"
    );
    assert!(report_a.stream_path.contains("operator-run-alpha"));
    assert!(report_b.stream_path.contains("operator-run-beta"));

    // On disk: exactly the two per-run directories exist under <base>/<pipeline>/,
    // named by the two ids. `drive` creates each run's <run-id>/tmp/ at bootstrap,
    // so the real directories exist and can be enumerated.
    let pipeline_dir = base.join(pipeline);
    let mut run_dirs: BTreeSet<String> = BTreeSet::new();
    for entry in std::fs::read_dir(&pipeline_dir).expect("pipeline directory exists on disk") {
        let entry = entry.expect("readable dir entry");
        if entry.file_type().expect("file type").is_dir() {
            run_dirs.insert(entry.file_name().to_string_lossy().into_owned());
        }
    }
    assert_eq!(
        run_dirs,
        BTreeSet::from([
            "operator-run-alpha".to_string(),
            "operator-run-beta".to_string()
        ]),
        "exactly the two per-run directories exist under the shared base, found {run_dirs:?}"
    );

    // The two run directories are disjoint subtrees: neither is a prefix of the
    // other, so no file either run wrote can live under the other's directory
    // (no shared, overwritten, or collided file between the two runs).
    let dir_a = pipeline_dir.join("operator-run-alpha");
    let dir_b = pipeline_dir.join("operator-run-beta");
    assert!(!dir_a.starts_with(&dir_b) && !dir_b.starts_with(&dir_a));

    // Both streams are still individually valid and carry their own operator id —
    // the disjointness holds for operator-overridden identities too.
    let records_a = parse_records(&bytes_a);
    let records_b = parse_records(&bytes_b);
    assert_stream_is_valid(&records_a, "operator-run-alpha");
    assert_stream_is_valid(&records_b, "operator-run-beta");

    // And they still partition cleanly, confirming the layout guarantee is
    // identity-carried, not id-scheme-dependent.
    let mut concatenated = bytes_a.clone();
    concatenated.extend_from_slice(&bytes_b);
    let combined = parse_records(&concatenated);
    let identities: BTreeSet<String> = combined.iter().map(|r| r.run_id.clone()).collect();
    assert_eq!(
        identities,
        BTreeSet::from([
            "operator-run-alpha".to_string(),
            "operator-run-beta".to_string()
        ]),
        "operator-identified streams partition cleanly by identity"
    );

    let _ = std::fs::remove_dir_all(&base);
}
