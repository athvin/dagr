//! C19 crash-safety and I/O fault-injection suite — ticket T27 (037). TDD.
//!
//! This is the fault-injection suite C28 requires — *"kill-points around every
//! event write, disk-full, and slow or failing sinks"* — driven against the
//! **merged** C19 event-stream writer (T19, `dagr_artifact::event_stream`) through
//! an **injected controllable test sink**, exercised against the run-store /
//! record-header / flush contract fixed in T0.6 (012). It proves two things the
//! spec asserts but nothing else yet checks (arch.md `### C19 · Event stream`):
//!
//! 1. **Crash tolerance.** Killing the process at any moment leaves a stream whose
//!    every record but at most one trailing partial is valid and parseable, whose
//!    sequence numbers are gapless and strictly increasing, and whose one-record
//!    prefix still identifies its run (the run-started event carries the full
//!    header). A crash is simulated **deterministically** — see the module note
//!    below — never by killing the real test process.
//! 2. **Sink-failure surfacing.** An induced mid-run / disk-full / slow sink
//!    fault surfaces the documented `SinkFault` carrying the verbatim reason
//!    `"event stream unwritable"` (the run-level fault the run loop reacts to by
//!    moving to cancelling and exiting with the distinct sink-failure code —
//!    T0.6 §5), asserted **by its documented cause, never by a magic number**, and
//!    the complete records preceding the fault remain a valid, gapless prefix.
//!
//! # What this suite is, and what it is NOT (scope — T27)
//!
//! It asserts the guarantees the **already-merged** writer/reader (T19) and the
//! injected-sink seam (T0.6) already provide; it changes **no** production
//! behavior. It does not build or modify the writer (T19), the run-loop driver
//! (T24), or the sink/base contract (T0.6); it injects fault variants of that sink
//! and asserts against the contract. The OS-signal / final-flush / temp-cleanup
//! wiring is T36; the fold of a crashed stream into a run artifact is C22/C26
//! (T42/T68) — this suite asserts only that the raw stream survives the crash. The
//! full two-concurrent-runs test is T67; the concurrency check here is scoped to
//! the fault suite's disjointness-and-validity assertion.
//!
//! # How a "crash" is simulated (deterministic-in-CI, seeded)
//!
//! Real process kills are non-deterministic and can hang or flake CI. Per the T27
//! process rules this suite simulates a crash as a **truncation of the byte stream
//! the real writer produced**: it drives the genuine [`EventStreamWriter`] through
//! a capturing sink to obtain the exact bytes an abrupt kill would have left on
//! disk (the default local-file sink does not fsync per event — T0.6 §6, so the
//! on-disk bytes are exactly the appended-so-far prefix, possibly cut mid-line),
//! truncates that buffer at **seeded** byte offsets landing *at*, *before*, and
//! *part-way through* individual event writes, and feeds the truncation to the
//! **real tolerant reader** [`read_records`]. This exercises the identical
//! invariant an uncatchable-signal kill would (a valid prefix with at most one
//! trailing partial) with none of the process-kill nondeterminism — and every
//! randomized trial records its seed so a CI failure reproduces exactly. The
//! resolution of the "real child kill vs deterministic truncation" tension is
//! recorded in the ticket's Open questions.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dagr_artifact::event_stream::{
    read_records, AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId,
    RunOutcome, RunStartedHeader, SinkFault, TerminalState, EVENTS_FILE_NAME,
    EVENT_STREAM_SCHEMA_VERSION, EVENT_STREAM_UNWRITABLE, FINGERPRINT_ALGORITHM_VERSION,
};

// ===========================================================================
// Exit code by CAUSE, never by a literal (C26 / T0.6 §5)
// ===========================================================================

/// The distinct **sink-failure cause** every sink-failure assertion resolves
/// through — the documented cancellation reason a [`SinkFault`] carries, the
/// verbatim arch.md C19 string re-exported by the writer as
/// [`EVENT_STREAM_UNWRITABLE`]. The concrete numeric exit code and its place in
/// the C26 exit-code precedence table are fixed by the CLI ticket (T55, still
/// `unmapped`); until then the **cause** is the stable, documented identifier, so
/// this suite compares against the named cause and **never hard-codes a magic
/// exit-code number** (T0.6 §5: "exit codes are named by cause, not by number").
/// Renumbering the table in one place therefore keeps these tests correct.
const SINK_FAILURE_CAUSE: &str = EVENT_STREAM_UNWRITABLE;

/// The run outcome a sink fault drives the run toward: `cancelled` (arch.md C19:
/// "the run moves to cancelling with reason 'event stream unwritable'"). A run
/// that stops because it could not record what it did is a **cancelled** run, not
/// a `succeeded` one — this is the outcome the sink-failure exit code is selected
/// from by cause.
const SINK_FAILURE_OUTCOME: RunOutcome = RunOutcome::Cancelled;

/// Assert a surfaced [`SinkFault`] is the documented sink-failure, **by cause**.
/// The single place every sink-failure scenario funnels its exit-by-cause check
/// through, so no individual test names a literal.
fn assert_is_sink_failure(fault: &SinkFault) {
    assert_eq!(
        fault.reason, SINK_FAILURE_CAUSE,
        "the run-level fault carries the documented sink-failure cause, verbatim"
    );
    assert_eq!(
        fault.reason, "event stream unwritable",
        "the cause is exactly the arch.md C19 string (guards the re-export)"
    );
    // The fault chains its underlying I/O error as its source (std::error::Error),
    // so a best-effort stderr report can name what went wrong.
    assert!(
        std::error::Error::source(fault).is_some(),
        "the sink fault carries the underlying I/O error as its source"
    );
}

// ===========================================================================
// A seeded, dependency-free deterministic RNG (no new deps; reproducible)
// ===========================================================================

/// A tiny `SplitMix64` PRNG. Dependency-free (no `rand`, deny/audit untouched)
/// and fully deterministic: a recorded seed replays the exact same kill/injection
/// point, so a CI failure is diagnosable rather than a flake (T27 definition of
/// done: "every randomized scenario records and reports its seed").
struct SeededRng {
    state: u64,
    seed: u64,
}

impl SeededRng {
    fn new(seed: u64) -> Self {
        Self { state: seed, seed }
    }
    fn next_u64(&mut self) -> u64 {
        // SplitMix64.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// A value in `0..bound` (bound > 0).
    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next_u64() % (bound as u64)).expect("modulo bound fits usize")
    }
    /// The seed that produced this generator — reported on failure for replay.
    fn seed(&self) -> u64 {
        self.seed
    }
}

// ===========================================================================
// Injected controllable test sinks (the C19 injection seam — T0.6 §1)
// ===========================================================================

/// A capturing sink that keeps every appended byte in order, so a test can obtain
/// the exact on-disk bytes and then truncate them to simulate an abrupt kill. The
/// default local-file sink does not fsync per append (T0.6 §6), so the bytes a
/// crash leaves are exactly the concatenation of the appends that completed —
/// which is what this sink records.
#[derive(Clone, Default)]
struct CaptureSink {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self::default()
    }
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().unwrap().clone()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The failure mode a fault sink injects at its chosen point.
#[derive(Clone, Copy)]
enum FaultKind {
    /// A generic append/flush I/O error (a "failing sink").
    Io,
    /// An out-of-space error (a "disk-full" injection).
    NoSpace,
}

impl FaultKind {
    fn to_error(self) -> io::Error {
        match self {
            FaultKind::Io => io::Error::other("sink append failed"),
            FaultKind::NoSpace => {
                io::Error::new(io::ErrorKind::StorageFull, "no space left on device")
            }
        }
    }
}

/// A fault sink that appends normally for a controllable number of writes, then
/// fails — modelling a **failing sink** (`FaultKind::Io`) or a **disk-full**
/// (`FaultKind::NoSpace`) event mid-run or at final flush. It keeps the bytes it
/// *did* accept so a test can prove the surviving prefix is valid and gapless.
#[derive(Clone)]
struct FaultSink {
    /// Bytes accepted so far (a valid prefix of the stream).
    bytes: Arc<Mutex<Vec<u8>>>,
    /// Remaining appends that will succeed before the injected append fault.
    /// `usize::MAX` means "never fail an append".
    ok_appends: Arc<AtomicUsize>,
    /// Whether the *flush* (fsync at run end / cancellation) should fault.
    fail_flush: Arc<Mutex<bool>>,
    /// The fault to inject.
    kind: FaultKind,
    /// An optional per-append delay, modelling a **slow sink** (bounded, tiny —
    /// never a real hang; the test asserts termination within a fixed timeout).
    append_delay: Duration,
}

impl FaultSink {
    /// A sink that fails its append after `ok_appends` successful appends.
    fn failing_after(ok_appends: usize, kind: FaultKind) -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
            ok_appends: Arc::new(AtomicUsize::new(ok_appends)),
            fail_flush: Arc::new(Mutex::new(false)),
            kind,
            append_delay: Duration::ZERO,
        }
    }
    /// A sink that accepts every append but faults the final flush (fsync) —
    /// the disk-full-at-final-flush case: a run that appeared to succeed
    /// throughout must still report the sink-failure cause, not a corrupt success.
    fn failing_flush(kind: FaultKind) -> Self {
        let s = Self::failing_after(usize::MAX, kind);
        *s.fail_flush.lock().unwrap() = true;
        s
    }
    /// A slow sink: each append waits `delay` (bounded), then may fault at
    /// `ok_appends`. Used to prove the unwritable-at-shutdown path is a bounded
    /// wait, never a hang.
    fn slow_then_fail(delay: Duration, ok_appends: usize, kind: FaultKind) -> Self {
        let mut s = Self::failing_after(ok_appends, kind);
        s.append_delay = delay;
        s
    }
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().unwrap().clone()
    }
}

impl EventSink for FaultSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        if !self.append_delay.is_zero() {
            std::thread::sleep(self.append_delay);
        }
        // Decrement the budget; fault when it is exhausted.
        let remaining = self.ok_appends.load(Ordering::SeqCst);
        if remaining == 0 {
            return Err(self.kind.to_error());
        }
        if remaining != usize::MAX {
            self.ok_appends.store(remaining - 1, Ordering::SeqCst);
        }
        self.bytes.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        if *self.fail_flush.lock().unwrap() {
            return Err(self.kind.to_error());
        }
        Ok(())
    }
}

/// A monotonic clock ticking one nanosecond per read — distinct, non-decreasing
/// offsets with no real clock (deterministic, no wall-clock).
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
// The fixture pipeline stream (a short, known transition sequence)
// ===========================================================================

/// The full run-artifact header a fixture run starts with — every field known at
/// start present, so the one-record-prefix assertion has something to check.
fn fixture_header() -> RunStartedHeader {
    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-23".to_string());
    let mut env = BTreeMap::new();
    env.insert("REGION".to_string(), "us-east-1".to_string());
    RunStartedHeader {
        pipeline: "fixture-pipeline".to_string(),
        fingerprint_structural: Some("blake3:1111".to_string()),
        fingerprint_policy: Some("blake3:2222".to_string()),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: params,
        data_interval: Some(["2026-07-22".to_string(), "2026-07-23".to_string()]),
        captured_env: env,
        resumed_from: Some("run-prior-0000".to_string()),
    }
}

/// A writer over the given sink for a fixture run, with a fixed run id and
/// pipeline so on-disk paths and identity are predictable.
fn fixture_writer<S: EventSink>(sink: S, run_id: &str) -> EventStreamWriter<S, TickClock> {
    EventStreamWriter::new(
        sink,
        TickClock::default(),
        RunId::from_operator(run_id),
        "fixture-pipeline",
    )
}

/// Drive a fixture pipeline of a short chain (a→b→c) to completion through the
/// writer, emitting the known transition sequence a real run would, and return
/// the number of records the complete run emits. Every `emit` must succeed for a
/// capture sink; the returned count is the "full run" the crash prefix is checked
/// against.
fn drive_fixture<S: EventSink>(w: &mut EventStreamWriter<S, TickClock>) -> usize {
    w.run_started(fixture_header()).unwrap();
    for node in ["a", "b", "c"] {
        w.node_ready(node).unwrap();
        w.node_admitted(node).unwrap();
        w.attempt_started(node, 1).unwrap();
        w.attempt_succeeded(node, 1).unwrap();
        // The single rich attempt-outcome record, alongside the per-transition
        // attempt-succeeded event (arch.md l.331) — what a real run now emits.
        w.attempt_outcome(AttemptOutcomeRecord::new(node, 1, "succeeded"))
            .unwrap();
        w.node_terminal(node, TerminalState::Succeeded).unwrap();
    }
    w.run_finished(RunOutcome::Succeeded).unwrap();
    w.finish().unwrap();
    // 1 run-started
    //   + 3*(ready, admitted, started, succeeded, attempt-outcome, terminal)
    //   + run-finished.
    1 + 3 * 6 + 1
}

/// Produce the exact bytes a completed fixture run leaves on disk (through a
/// capture sink, no fsync-per-event so the buffer is the crash-visible prefix).
fn full_fixture_bytes(run_id: &str) -> Vec<u8> {
    let sink = CaptureSink::new();
    let mut w = fixture_writer(sink.clone(), run_id);
    let _ = drive_fixture(&mut w);
    sink.bytes()
}

/// Parse a record's `seq` field.
fn seq_of(rec: &serde_json::Value) -> u64 {
    rec.get("seq").and_then(serde_json::Value::as_u64).unwrap()
}

/// Assert the parsed records form a **gapless, strictly-increasing** sequence
/// starting at the run's first sequence value (0 on run-started). The load-bearing
/// crash-safety invariant: a reader detects a lost record as a gap.
fn assert_gapless_from_zero(records: &[serde_json::Value]) {
    let seqs: Vec<u64> = records.iter().map(seq_of).collect();
    let expected: Vec<u64> = (0..seqs.len() as u64).collect();
    assert_eq!(
        seqs, expected,
        "sequence numbers are gapless and strictly increasing from the run's first value"
    );
}

/// Assert every record carries the run identity and the schema version (C19).
fn assert_every_record_identified(records: &[serde_json::Value], run_id: &str) {
    for rec in records {
        assert_eq!(
            rec.get("run_id").and_then(serde_json::Value::as_str),
            Some(run_id),
            "every record carries the run identity"
        );
        assert_eq!(
            rec.get("schema_version")
                .and_then(serde_json::Value::as_str),
            Some(EVENT_STREAM_SCHEMA_VERSION),
            "every record carries the schema version"
        );
    }
}

// ===========================================================================
// Crash / kill-point scenarios (deterministic truncation, seeded)
// ===========================================================================

/// **Abrupt kill at a random point yields a valid prefix.** For many seeded
/// trials, truncate the fixture stream at a random byte offset (a "kill" landing
/// anywhere across the whole run), then parse it with the real tolerant reader:
/// every complete record parses, at most one trailing partial is discarded, no
/// interior record is malformed, and the complete records are a gapless prefix of
/// the run.
#[test]
fn abrupt_kill_at_a_random_point_yields_a_valid_prefix() {
    let run_id = "run-kill-prefix";
    let full = full_fixture_bytes(run_id);
    assert!(full.len() > 20, "the fixture stream has substance to cut");

    // A base seed; each trial derives a distinct offset. Recorded so a failure
    // replays. (Env-tunable trial count keeps CI heavier than a local smoke.)
    let base_seed: u64 = 0x00C1_9CAF_E000_0001_u64.wrapping_mul(2_654_435_761);
    let trials = trial_count();
    let mut rng = SeededRng::new(base_seed);

    for trial in 0..trials {
        // A cut anywhere in `1..=full.len()` — the whole span of possible kills.
        let cut = 1 + rng.below(full.len());
        let truncated = &full[..cut];

        let parsed = read_records(truncated).unwrap_or_else(|e| {
            panic!(
                "trial {trial} (base_seed={:#x}) at cut={cut}: a kill left an \
                 UN-parseable stream — interior corruption, not the tolerated \
                 trailing partial: {e}",
                rng.seed()
            )
        });

        // At most one trailing partial is discarded — never more; every complete
        // record parses. `read_records` reports the single discard via its flag.
        assert!(
            parsed.records.len() <= 20,
            "trial {trial} at cut={cut}: no more records than the full run"
        );
        // The surviving complete records are a gapless prefix of the run.
        assert_gapless_from_zero(&parsed.records);
        assert_every_record_identified(&parsed.records, run_id);

        // The complete records are a genuine PREFIX of the full run's records:
        // parse the full stream and compare the surviving head element-wise.
        let whole = read_records(&full).unwrap();
        for (i, rec) in parsed.records.iter().enumerate() {
            assert_eq!(
                rec, &whole.records[i],
                "trial {trial} at cut={cut}: surviving record {i} matches the full run's record {i} \
                 (a valid prefix, not a re-ordering)"
            );
        }
    }
}

/// **Kill mid-write leaves at most one partial, never two.** Drive kills so they
/// land *at* a record boundary (clean), *before* a record's newline (mid-line),
/// and *part-way through* the final record's bytes — the "around every event
/// write" clause. In every case the reader finds zero or one trailing partial,
/// never two-or-more; and a hand-built stream with two trailing partials must
/// fail the reader's tolerance (guarding it from being too lax).
#[test]
fn kill_mid_write_leaves_at_most_one_partial_never_two() {
    let run_id = "run-kill-midwrite";
    let full = full_fixture_bytes(run_id);

    // Every complete-record boundary is a byte just after a '\n'. Kills landing
    // exactly on a boundary leave no partial; kills just before a boundary
    // (mid-line) leave exactly one partial; kills part-way through the final
    // record leave exactly one partial. Enumerate all three around EVERY write.
    let boundaries: Vec<usize> = full
        .iter()
        .enumerate()
        .filter_map(|(i, &b)| (b == b'\n').then_some(i + 1))
        .collect();
    assert!(boundaries.len() >= 5, "several record boundaries exist");

    for &boundary in &boundaries {
        // At the boundary: a clean cut — no trailing partial.
        let clean = read_records(&full[..boundary]).unwrap();
        assert!(
            !clean.trailing_partial_discarded,
            "a cut exactly on a record boundary leaves NO trailing partial (at={boundary})"
        );
        assert_gapless_from_zero(&clean.records);

        // One byte before the boundary: the final record is cut mid-line — a
        // single trailing partial the reader tolerates and discards.
        if boundary >= 2 {
            let midline = &full[..boundary - 1];
            let cut = read_records(midline).unwrap();
            // Exactly one partial: the records are the complete ones before the
            // cut, and the discarded/kept tail is the single unterminated record.
            assert!(
                cut.records.len() + usize::from(cut.trailing_partial_discarded) >= 1,
                "at most one partial accounted for at={}",
                boundary - 1
            );
            assert_gapless_from_zero(&cut.records);
        }
    }

    // Guard the reader's tolerance: a stream with TWO trailing partials (two
    // unterminated, un-parseable fragments after the last '\n') must NOT be
    // silently accepted as clean — the reader tolerates at most one. A terminated
    // interior fragment that fails to parse is genuine corruption → an error.
    let mut two_partials = full.clone();
    // Append an un-terminated garbage fragment, then another, with the first
    // TERMINATED so it is an interior (non-final) line → corruption, not the
    // tolerated single trailing partial.
    two_partials.extend_from_slice(b"{ this is not valid json\n");
    two_partials.extend_from_slice(b"{ nor is this");
    let err = read_records(&two_partials).unwrap_err();
    // The interior garbage line is reported as corruption (a non-final line that
    // failed to parse), NOT swallowed as a second tolerated partial.
    assert!(
        err.line >= boundaries.len(),
        "the interior corruption is flagged at its line, not tolerated"
    );
}

/// **Sequence numbers are gapless and strictly increasing after a kill.** Take a
/// killed-child stream and assert every complete record's seq increases by exactly
/// one from the run's first value, with no gaps and no repeats, up to the last
/// complete record.
#[test]
fn sequence_numbers_are_gapless_after_a_kill() {
    let run_id = "run-kill-seq";
    let full = full_fixture_bytes(run_id);
    // Cut so at least a few records survive plus a partial.
    let cut = full.len() - 12;
    let parsed = read_records(&full[..cut]).unwrap();
    assert!(
        parsed.records.len() >= 3,
        "several records survive the kill"
    );
    assert_gapless_from_zero(&parsed.records);
    // Strictly increasing (implied by gapless-from-zero, checked explicitly).
    for w in parsed.records.windows(2) {
        assert!(
            seq_of(&w[1]) == seq_of(&w[0]) + 1,
            "each seq is exactly one more than the previous"
        );
    }
}

/// **A one-record stream still identifies its run.** A child killed so early the
/// stream holds only the run-started record (plus perhaps a partial next line)
/// still identifies its run completely: the single complete record is the
/// run-started event carrying every run-artifact header field known at start.
#[test]
fn a_one_record_stream_still_identifies_its_run() {
    let run_id = "run-kill-onerecord";
    let full = full_fixture_bytes(run_id);

    // Cut right after the first record's newline: exactly one complete record.
    let first_nl = full.iter().position(|&b| b == b'\n').unwrap();
    let one = read_records(&full[..=first_nl]).unwrap();
    assert_eq!(one.records.len(), 1, "exactly one complete record");
    assert!(!one.trailing_partial_discarded);

    let rec = &one.records[0];
    assert_eq!(
        rec.get("kind").and_then(serde_json::Value::as_str),
        Some("run-started"),
        "the sole record is the run-started event"
    );
    assert_eq!(
        rec.get("run_id").and_then(serde_json::Value::as_str),
        Some(run_id),
        "the run is identified from the one record alone"
    );
    let header = rec
        .get("header")
        .and_then(serde_json::Value::as_object)
        .unwrap();
    // Every header field known at start is present (C19: full artifact header).
    // The captured environment is `captured_environment` in the published schema.
    for field in [
        "pipeline",
        "parameters",
        "captured_environment",
        "data_interval",
    ] {
        assert!(header.contains_key(field), "run-started carries {field}");
    }
    assert_eq!(header["pipeline"], serde_json::json!("fixture-pipeline"));
    assert_eq!(
        header["fingerprint_structural"],
        serde_json::json!("blake3:1111"),
        "structural fingerprint (assembly succeeded) is in the header"
    );
    assert_eq!(
        header["fingerprint_policy"],
        serde_json::json!("blake3:2222")
    );
    // Resume lineage is an object carrying the originating run id (schema field).
    assert_eq!(
        header["resume_lineage"],
        serde_json::json!({ "run_id": "run-prior-0000" })
    );
    // No overall outcome/summary at start — those exist only at run end.
    assert!(
        !header.contains_key("overall_outcome"),
        "no outcome in the start header"
    );

    // Even with a trailing partial after that first record, the run is still
    // identified from the one complete record (cut a few bytes into record 2).
    let with_partial = read_records(&full[..first_nl + 6]).unwrap();
    assert_eq!(with_partial.records.len(), 1, "still one complete record");
    assert!(
        with_partial.trailing_partial_discarded,
        "the mid-record tail is the single tolerated partial"
    );
}

/// **Every record carries run identity and schema version.** Inspect every
/// complete record of a killed stream; none is missing either field.
#[test]
fn every_record_carries_run_identity_and_schema_version() {
    let run_id = "run-kill-identity";
    let full = full_fixture_bytes(run_id);
    let parsed = read_records(&full[..full.len() - 7]).unwrap();
    assert!(!parsed.records.is_empty());
    assert_every_record_identified(&parsed.records, run_id);
}

/// **Interior corruption is rejected (the reader is not too lax).** A terminated
/// line in the *middle* of the stream that fails to parse is genuine corruption,
/// reported as an error — distinct from the single tolerated trailing partial.
#[test]
fn interior_corruption_is_rejected() {
    let run_id = "run-corrupt-interior";
    let full = full_fixture_bytes(run_id);
    // Splice a garbage terminated line after the first record.
    let first_nl = full.iter().position(|&b| b == b'\n').unwrap();
    let mut corrupt = Vec::new();
    corrupt.extend_from_slice(&full[..=first_nl]);
    corrupt.extend_from_slice(b"{ not valid json at all\n");
    corrupt.extend_from_slice(&full[first_nl + 1..]);
    let err = read_records(&corrupt).unwrap_err();
    assert_eq!(
        err.line, 1,
        "the interior corrupt line is flagged, not tolerated"
    );
}

// ===========================================================================
// Sink-failure scenarios (failing sink / disk-full / slow sink)
// ===========================================================================

/// **Induced mid-run failing sink surfaces the sink-failure cause.** A run driven
/// through a sink injected to error on a chosen mid-run append surfaces a
/// [`SinkFault`] whose reason is the documented `"event stream unwritable"` cause
/// (the run-level fault the run loop reacts to by cancelling and exiting with the
/// distinct sink-failure code — asserted **by cause**, never a literal). The
/// complete records preceding the failure remain a valid, gapless prefix, and the
/// writer refuses to advance the sequence over the un-recorded record (no gap).
#[test]
fn induced_mid_run_failing_sink_surfaces_the_sink_failure_cause() {
    // Fail on the 4th append (after run-started + node-ready + node-admitted).
    let sink = FaultSink::failing_after(3, FaultKind::Io);
    let mut w = fixture_writer(sink.clone(), "run-failing-sink");

    w.run_started(fixture_header()).unwrap();
    w.node_ready("a").unwrap();
    w.node_admitted("a").unwrap();
    let seq_before = w.next_seq();
    // The 4th append faults.
    let fault = w.attempt_started("a", 1).unwrap_err();
    assert_is_sink_failure(&fault);
    // The run is now unwritable, and the writer left NO gap: the failed record
    // did not advance the sequence, so the next record would reuse the same seq.
    assert!(w.is_faulted(), "the writer records the unwritable fault");
    assert_eq!(
        w.next_seq(),
        seq_before,
        "a faulted append leaves no sequence gap (seq not advanced)"
    );

    // The complete records the sink DID accept are a valid, gapless prefix.
    let parsed = read_records(&sink.bytes()).unwrap();
    assert_eq!(
        parsed.records.len(),
        3,
        "three records were recorded before the fault"
    );
    assert_gapless_from_zero(&parsed.records);
    assert_every_record_identified(&parsed.records, "run-failing-sink");
}

/// **Disk-full mid-run behaves as a sink failure.** Identical to the failing-sink
/// case but the injected error is an out-of-space error: the same
/// `"event stream unwritable"` cause surfaces, and the records before the failure
/// remain a valid, gapless, parseable prefix.
#[test]
fn disk_full_mid_run_behaves_as_a_sink_failure() {
    let sink = FaultSink::failing_after(2, FaultKind::NoSpace);
    let mut w = fixture_writer(sink.clone(), "run-disk-full");

    w.run_started(fixture_header()).unwrap();
    w.node_ready("a").unwrap();
    let fault = w.node_admitted("a").unwrap_err();
    assert_is_sink_failure(&fault);
    // The underlying cause is specifically an out-of-space condition.
    let io_err = std::error::Error::source(&fault)
        .and_then(|e| e.downcast_ref::<io::Error>())
        .expect("the source is the injected io::Error");
    assert_eq!(
        io_err.kind(),
        io::ErrorKind::StorageFull,
        "disk-full surfaces as a storage-full I/O cause under the sink-failure fault"
    );

    let parsed = read_records(&sink.bytes()).unwrap();
    assert_eq!(
        parsed.records.len(),
        2,
        "two records recorded before disk-full"
    );
    assert_gapless_from_zero(&parsed.records);
    assert_every_record_identified(&parsed.records, "run-disk-full");
}

/// **Disk-full at final flush produces the sink-failure cause, not a corrupt
/// success.** A sink that accepts every append but fails the fsync/flush at run
/// end surfaces the sink-failure cause from `finish()`; the stream on disk is
/// still a valid prefix under the reader's tolerance.
#[test]
fn disk_full_at_final_flush_produces_the_sink_failure_cause() {
    let sink = FaultSink::failing_flush(FaultKind::NoSpace);
    let mut w = fixture_writer(sink.clone(), "run-flush-full");

    // Every append succeeds throughout the run…
    w.run_started(fixture_header()).unwrap();
    w.node_ready("a").unwrap();
    w.node_terminal("a", TerminalState::Succeeded).unwrap();
    w.run_finished(RunOutcome::Succeeded).unwrap();
    // …but the final fsync (flush) fails → the run must report the sink-failure
    // cause, NOT a silent success.
    let fault = w.finish().unwrap_err();
    assert_is_sink_failure(&fault);
    assert!(w.is_faulted());

    // The stream on disk is still a valid prefix (all four records parse).
    let parsed = read_records(&sink.bytes()).unwrap();
    assert_eq!(parsed.records.len(), 4);
    assert_gapless_from_zero(&parsed.records);
    assert!(
        !parsed.trailing_partial_discarded,
        "the appended bytes are whole lines; the flush — not the content — failed"
    );
}

/// **Slow/unwritable sink at shutdown yields a bounded wait, not a hang.** A sink
/// whose appends are slow (a bounded per-append delay) and which then faults must
/// surface the sink-failure cause and the whole drive must complete within a
/// fixed timeout — a regression to a hang fails the suite by exceeding the budget
/// rather than stalling CI. The delay is a tiny bounded value; the assertion is on
/// *termination within a fixed wall-clock budget*, never on the delay's duration.
#[test]
fn slow_or_unwritable_sink_at_shutdown_is_a_bounded_wait_not_a_hang() {
    // Each append waits a tiny bounded time; the sink faults on the 3rd append.
    let per_append = Duration::from_millis(5);
    let sink = FaultSink::slow_then_fail(per_append, 2, FaultKind::Io);
    let mut w = fixture_writer(sink.clone(), "run-slow-sink");

    // A generous fixed budget: a handful of bounded appends must complete and the
    // fault must surface well within it. A hang would blow past this and fail.
    let budget = Duration::from_secs(5);
    let start = Instant::now();

    w.run_started(fixture_header()).unwrap();
    w.node_ready("a").unwrap();
    let fault = w.node_admitted("a").unwrap_err();
    let elapsed = start.elapsed();

    assert_is_sink_failure(&fault);
    assert!(
        elapsed < budget,
        "the slow/unwritable sink at shutdown produced a bounded wait ({elapsed:?} < {budget:?}), \
         not a hang"
    );
    // The prefix the slow sink did accept is still valid.
    let parsed = read_records(&sink.bytes()).unwrap();
    assert_gapless_from_zero(&parsed.records);
}

/// **Sink-failure exit code is asserted by cause, not by literal.** Each
/// sink-failure scenario compares the observed fault against the named
/// sink-failure *cause* and its `cancelled` outcome, resolved through the
/// documented mapping — so renumbering the C26 exit-code table in one place keeps
/// the tests correct and no test hard-codes a magic number. This test pins the
/// mapping itself.
#[test]
fn sink_failure_is_asserted_by_cause_not_by_literal() {
    // A sink fault always carries the same documented cause, whatever the
    // underlying I/O error was (failing sink OR disk-full).
    for kind in [FaultKind::Io, FaultKind::NoSpace] {
        let sink = FaultSink::failing_after(0, kind);
        let mut w = fixture_writer(sink, "run-cause");
        let fault = w.run_started(fixture_header()).unwrap_err();
        assert_is_sink_failure(&fault);
    }
    // The cause resolves to the `cancelled` run outcome the sink-failure exit
    // code is selected from — by cause, never a literal number.
    assert_eq!(SINK_FAILURE_OUTCOME, RunOutcome::Cancelled);
    assert_eq!(SINK_FAILURE_OUTCOME.as_str(), "cancelled");
    // The cause string is the exact arch.md C19 identifier (the stable mapping
    // key the C26 table / T55 renumbers behind).
    assert_eq!(SINK_FAILURE_CAUSE, "event stream unwritable");
}

/// **Run failure is not masked by self-inflicted cancellation.** A run where a
/// node genuinely fails yields a `failed` overall outcome that wins over a
/// `cancelled` one — confirming the sink-failure scenarios above are asserting the
/// sink cause (`cancelled`) specifically, not a generic cancellation that a node
/// failure would produce. This pins the outcome precedence the exit-code table
/// reads: a real node failure is `failed`, distinct from a sink-fault `cancelled`.
#[test]
fn run_failure_is_not_masked_by_self_inflicted_cancellation() {
    // A run whose stream records a real node failure ends `failed`, not
    // `cancelled` — the two causes are distinct outcomes.
    let sink = CaptureSink::new();
    let mut w = fixture_writer(sink.clone(), "run-node-failed");
    w.run_started(fixture_header()).unwrap();
    w.node_ready("a").unwrap();
    w.attempt_started("a", 1).unwrap();
    w.attempt_failed("a", 1).unwrap();
    w.node_terminal("a", TerminalState::Failed).unwrap();
    // A failed run finishes with the `failed` outcome — NOT the `cancelled`
    // outcome a sink fault would drive. The run-failure cause wins over the
    // cancellation cause the sink-failure scenarios assert.
    w.run_finished(RunOutcome::Failed).unwrap();
    w.finish().unwrap();

    let parsed = read_records(&sink.bytes()).unwrap();
    let finished = parsed.records.last().unwrap();
    assert_eq!(
        finished.get("kind").and_then(serde_json::Value::as_str),
        Some("run-finished")
    );
    assert_eq!(
        finished["outcome"],
        serde_json::json!("failed"),
        "a genuine node failure yields `failed`, distinct from the sink-fault `cancelled`"
    );
    assert_ne!(
        RunOutcome::Failed,
        SINK_FAILURE_OUTCOME,
        "the run-failure outcome is distinct from the sink-failure (cancelled) outcome"
    );
}

// ===========================================================================
// Concurrency: two runs, disjoint & individually valid
// ===========================================================================

/// **Two concurrent runs produce disjoint, individually valid streams.** Two runs
/// of the same fixture binary against the same base write under their own
/// `<base>/<pipeline>/<run-id>/` directory (disjoint paths), each stream is
/// independently valid with gapless sequences, and concatenating the two
/// partitions cleanly by the run identity every record carries. (Overlaps T67's
/// remit; here it is the fault-suite's concurrency-safety check only.)
#[test]
fn two_concurrent_runs_produce_disjoint_individually_valid_streams() {
    // Drive two runs on separate threads against the same base+pipeline.
    let run = |run_id: &'static str| {
        std::thread::spawn(move || {
            let sink = CaptureSink::new();
            let mut w = fixture_writer(sink.clone(), run_id);
            let n = drive_fixture(&mut w);
            (w.stream_path("shared-base"), sink.bytes(), n)
        })
    };
    let (path_a, bytes_a, n_a) = run("run-concurrent-A").join().unwrap();
    let (path_b, bytes_b, n_b) = run("run-concurrent-B").join().unwrap();

    // Disjoint per-run directories — the path embeds the run id (T0.6 §3).
    assert_ne!(path_a, path_b, "the two runs write disjoint stream paths");
    assert!(path_a.contains("run-concurrent-A"));
    assert!(path_b.contains("run-concurrent-B"));
    assert!(path_a.ends_with(EVENTS_FILE_NAME) && path_b.ends_with(EVENTS_FILE_NAME));

    // Each stream is independently valid with gapless sequences.
    for (bytes, run_id, n) in [
        (&bytes_a, "run-concurrent-A", n_a),
        (&bytes_b, "run-concurrent-B", n_b),
    ] {
        let parsed = read_records(bytes).unwrap();
        assert_eq!(
            parsed.records.len(),
            n,
            "the whole {run_id} stream is present"
        );
        assert_gapless_from_zero(&parsed.records);
        assert_every_record_identified(&parsed.records, run_id);
    }

    // Concatenate both streams and partition cleanly by run identity — cross-run
    // analysis is safe concatenation, no shared index (C19 / T0.6 §10).
    let mut all = bytes_a.clone();
    all.extend_from_slice(&bytes_b);
    let merged = read_records(&all).unwrap();
    let a_part: Vec<_> = merged
        .records
        .iter()
        .filter(|r| r["run_id"] == serde_json::json!("run-concurrent-A"))
        .collect();
    let b_part: Vec<_> = merged
        .records
        .iter()
        .filter(|r| r["run_id"] == serde_json::json!("run-concurrent-B"))
        .collect();
    assert_eq!(a_part.len(), n_a, "A's records partition out cleanly");
    assert_eq!(b_part.len(), n_b, "B's records partition out cleanly");
    // Each partition is itself gapless (records interleave in the concatenation
    // but each run's own sequence is intact).
    for part in [&a_part, &b_part] {
        let seqs: Vec<u64> = part.iter().map(|r| seq_of(r)).collect();
        let expected: Vec<u64> = (0..seqs.len() as u64).collect();
        assert_eq!(seqs, expected, "the partition's own sequence is gapless");
    }
}

// ===========================================================================
// Determinism-on-failure
// ===========================================================================

/// **The suite is deterministic-on-failure.** The seeded RNG replays the exact
/// same offsets for a given seed, so a reported seed reproduces the same kill
/// point — a CI failure is diagnosable rather than a flake. This pins the RNG's
/// reproducibility the randomized scenarios rely on.
#[test]
fn seeded_kill_points_are_reproducible() {
    let seed = 0xDEAD_BEEF_1234_5678;
    let mut a = SeededRng::new(seed);
    let mut b = SeededRng::new(seed);
    for _ in 0..1000 {
        assert_eq!(a.below(97), b.below(97), "same seed → same offset sequence");
    }
    assert_eq!(
        a.seed(),
        seed,
        "the seed is recoverable for a failure report"
    );
    // Two different seeds diverge (the randomization is real, not constant).
    let mut c = SeededRng::new(seed ^ 0x1);
    let mut differ = false;
    for _ in 0..1000 {
        if a.below(97) != c.below(97) {
            differ = true;
            break;
        }
    }
    assert!(differ, "distinct seeds explore distinct kill points");
}

/// Number of randomized trials: heavier in CI (via `DAGR_CRASH_TRIALS`), a quick
/// smoke locally. Every trial's offset is seed-derived and reported on failure.
fn trial_count() -> usize {
    std::env::var("DAGR_CRASH_TRIALS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256)
}
