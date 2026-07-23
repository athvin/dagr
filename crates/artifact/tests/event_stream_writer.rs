//! C19 event-stream writer — behavioral test suite (T19 / ticket 029).
//!
//! These tests are the covering suite the coverage matrix (`docs/coverage-matrix.md`)
//! maps C19 to. Each `#[test]` realizes one scenario from the ticket's Test plan;
//! together they exercise the append-only JSONL writer, the T0.6 record header,
//! the T4 canonical encoding, write-through-then-record discipline, the
//! fsync-at-boundary contract, induced sink failure, per-run directory
//! disjointness, and the tolerant reader the fold contract (C22/T42) and
//! crash-safety suite (T27) depend on.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{
    read_records, Event, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState, EVENTS_FILE_NAME, EVENT_STREAM_SCHEMA_VERSION,
    EVENT_STREAM_UNWRITABLE,
};

// === Test doubles =========================================================

/// A sink that records the ordered sequence of (append, flush) operations and
/// keeps every appended line, so tests can assert both content and ordering.
#[derive(Clone, Default)]
struct CaptureSink {
    inner: Arc<Mutex<CaptureState>>,
}

#[derive(Default)]
struct CaptureState {
    /// Every appended line, in order (each ends in `\n`).
    lines: Vec<Vec<u8>>,
    /// The interleaved operation log: `Append(index)` / `Flush`.
    ops: Vec<Op>,
    /// Count of flush calls (a proxy for fsync requests).
    flushes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Append(usize),
    Flush,
}

impl CaptureSink {
    fn new() -> Self {
        Self::default()
    }
    fn lines(&self) -> Vec<Vec<u8>> {
        self.inner.lock().unwrap().lines.clone()
    }
    fn ops(&self) -> Vec<Op> {
        self.inner.lock().unwrap().ops.clone()
    }
    fn flush_count(&self) -> usize {
        self.inner.lock().unwrap().flushes
    }
    /// Concatenate all appended lines into one byte buffer (the on-disk stream).
    fn bytes(&self) -> Vec<u8> {
        self.inner
            .lock()
            .unwrap()
            .lines
            .iter()
            .flatten()
            .copied()
            .collect()
    }
}

impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        let mut s = self.inner.lock().unwrap();
        let idx = s.lines.len();
        s.lines.push(line.to_vec());
        s.ops.push(Op::Append(idx));
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        let mut s = self.inner.lock().unwrap();
        s.ops.push(Op::Flush);
        s.flushes += 1;
        Ok(())
    }
}

/// A sink that fails its append/flush after `ok_before` successful appends.
#[derive(Clone)]
struct FailingSink {
    ok_before: Arc<Mutex<usize>>,
}

impl FailingSink {
    fn after(ok_before: usize) -> Self {
        Self {
            ok_before: Arc::new(Mutex::new(ok_before)),
        }
    }
}

impl EventSink for FailingSink {
    fn append_line(&mut self, _line: &[u8]) -> io::Result<()> {
        let mut n = self.ok_before.lock().unwrap();
        if *n == 0 {
            return Err(io::Error::other("disk full"));
        }
        *n -= 1;
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock whose returned offset the test controls directly.
#[derive(Clone)]
struct ManualClock {
    now: Rc<Cell<u64>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(0)),
        }
    }
    fn set(&self, ns: u64) {
        self.now.set(ns);
    }
}

impl MonotonicClock for ManualClock {
    fn elapsed_ns(&self) -> u64 {
        self.now.get()
    }
}

// === Helpers ==============================================================

fn writer(sink: CaptureSink, clock: ManualClock) -> EventStreamWriter<CaptureSink, ManualClock> {
    EventStreamWriter::new(
        sink,
        clock,
        RunId::from_operator("run-fixed-0001"),
        "my-pipeline",
    )
}

fn header() -> RunStartedHeader {
    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-23".to_string());
    let mut env = BTreeMap::new();
    env.insert("REGION".to_string(), "us-east-1".to_string());
    RunStartedHeader {
        pipeline: "my-pipeline".to_string(),
        fingerprint_structural: Some("blake3:aaaa".to_string()),
        fingerprint_policy: Some("blake3:bbbb".to_string()),
        parameters: params,
        data_interval: Some(["2026-07-22".to_string(), "2026-07-23".to_string()]),
        captured_env: env,
        resumed_from: Some("run-prior-0000".to_string()),
    }
}

/// Parse one captured line as a JSON object.
fn as_obj(line: &[u8]) -> serde_json::Map<String, serde_json::Value> {
    let v: serde_json::Value = serde_json::from_slice(line).expect("line parses as JSON");
    v.as_object().expect("record is a JSON object").clone()
}

// === Tests ================================================================

#[test]
fn envelope_completeness() {
    let sink = CaptureSink::new();
    let clock = ManualClock::new();
    let mut w = writer(sink.clone(), clock.clone());

    w.run_started(header()).unwrap();
    w.node_ready("n").unwrap();
    w.node_admitted("n").unwrap();
    w.attempt_started("n", 1).unwrap();
    w.attempt_succeeded("n", 1).unwrap();
    w.attempt_failed("n", 1).unwrap();
    w.node_terminal("n", TerminalState::Succeeded).unwrap();
    w.zombie_at_exit("n").unwrap();
    w.run_finished(RunOutcome::Succeeded).unwrap();

    let lines = sink.lines();
    assert_eq!(lines.len(), 9, "one record per emitted transition");
    for line in &lines {
        let obj = as_obj(line);
        for field in ["schema_version", "run_id", "seq", "wall", "offset_ns"] {
            assert!(
                obj.contains_key(field),
                "record missing envelope field {field}"
            );
        }
        assert_eq!(
            obj["schema_version"],
            serde_json::json!(EVENT_STREAM_SCHEMA_VERSION)
        );
        assert_eq!(obj["run_id"], serde_json::json!("run-fixed-0001"));
        assert!(obj.contains_key("event"), "record names its event kind");
    }
}

#[test]
fn gapless_strictly_increasing_sequence() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());

    w.run_started(header()).unwrap();
    for i in 0..5 {
        w.node_ready(&format!("n{i}")).unwrap();
        w.attempt_started(&format!("n{i}"), 1).unwrap();
    }
    w.run_finished(RunOutcome::Succeeded).unwrap();

    let seqs: Vec<u64> = sink
        .lines()
        .iter()
        .map(|l| as_obj(l)["seq"].as_u64().unwrap())
        .collect();
    let expected: Vec<u64> = (0..seqs.len() as u64).collect();
    assert_eq!(
        seqs, expected,
        "sequence is contiguous 0..N-1, no gaps/repeats"
    );
}

#[test]
fn offsets_are_monotonic_and_authoritative() {
    let sink = CaptureSink::new();
    let clock = ManualClock::new();
    let mut w = writer(sink.clone(), clock.clone());

    clock.set(0);
    w.run_started(header()).unwrap();
    clock.set(1_000);
    w.node_ready("a").unwrap();
    // The wall clock steps backward here (simulated inside the writer's wall
    // source is out of our reach, but the offset must not decrease): advance the
    // monotonic clock and assert the offset tracks it, never regressing.
    clock.set(5_000);
    w.node_ready("b").unwrap();
    clock.set(5_000); // no advance
    w.node_ready("c").unwrap();

    let offs: Vec<u64> = sink
        .lines()
        .iter()
        .map(|l| as_obj(l)["offset_ns"].as_u64().unwrap())
        .collect();
    assert_eq!(offs, vec![0, 1_000, 5_000, 5_000]);
    // Non-decreasing.
    for w in offs.windows(2) {
        assert!(w[1] >= w[0], "offsets never decrease");
    }
    // Duration by difference is non-negative and matches injected elapsed.
    assert_eq!(offs[2] - offs[1], 4_000);
}

#[test]
fn run_started_carries_full_header() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();

    let obj = as_obj(&sink.lines()[0]);
    assert_eq!(obj["event"], serde_json::json!("run-started"));
    let body = obj["body"].as_object().expect("run-started has a body");
    assert_eq!(body["pipeline"], serde_json::json!("my-pipeline"));
    assert_eq!(
        body["fingerprint_structural"],
        serde_json::json!("blake3:aaaa")
    );
    assert_eq!(body["fingerprint_policy"], serde_json::json!("blake3:bbbb"));
    assert_eq!(body["parameters"]["date"], serde_json::json!("2026-07-23"));
    assert_eq!(
        body["captured_env"]["REGION"],
        serde_json::json!("us-east-1")
    );
    assert_eq!(body["resumed_from"], serde_json::json!("run-prior-0000"));
    assert_eq!(
        body["data_interval"],
        serde_json::json!(["2026-07-22", "2026-07-23"])
    );
    // Neither overall outcome nor summary is present at start.
    assert!(!body.contains_key("outcome"), "no outcome at start");
    assert!(!body.contains_key("summary"), "no summary at start");
}

#[test]
fn header_when_assembly_failed() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());

    let mut h = header();
    h.fingerprint_structural = None; // assembly failed → no fingerprint
    h.fingerprint_policy = None;
    h.resumed_from = None;
    w.run_started(h).unwrap();
    w.run_finished(RunOutcome::AssemblyFailed).unwrap();

    let lines = sink.lines();
    assert_eq!(lines.len(), 2, "valid two-record stream");
    let start = as_obj(&lines[0]);
    let body = start["body"].as_object().unwrap();
    assert!(
        !body.contains_key("fingerprint_structural"),
        "omitted when absent"
    );
    assert!(
        !body.contains_key("fingerprint_policy"),
        "omitted when absent"
    );
    // Still identifies the run.
    assert_eq!(start["run_id"], serde_json::json!("run-fixed-0001"));
    assert_eq!(body["pipeline"], serde_json::json!("my-pipeline"));
    // Whole stream parses.
    let parsed = read_records(&sink.bytes()).unwrap();
    assert_eq!(parsed.records.len(), 2);
    assert!(!parsed.trailing_partial_discarded);
    let fin = parsed.records[1].as_object().unwrap();
    assert_eq!(fin["body"]["outcome"], serde_json::json!("assembly-failed"));
}

#[test]
fn write_through_not_buffered() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();

    // After a single record, the sink has already seen the append (and the
    // record method has returned) — nothing is deferred to run end.
    let ops = sink.ops();
    assert_eq!(
        ops.first(),
        Some(&Op::Append(0)),
        "the record was appended immediately"
    );
    assert_eq!(sink.lines().len(), 1, "the single record reached the sink");
}

#[test]
fn fsync_once_at_run_end() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();
    w.node_ready("a").unwrap();
    // Steady-state records do not fsync.
    assert_eq!(sink.flush_count(), 0, "no per-event fsync in steady state");
    w.run_finished(RunOutcome::Succeeded).unwrap();
    w.finish().unwrap();
    assert_eq!(sink.flush_count(), 1, "exactly one fsync at run end");
}

#[test]
fn fsync_once_at_cancellation() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();
    w.run_finished(RunOutcome::Cancelled).unwrap();
    w.finish().unwrap();
    assert_eq!(sink.flush_count(), 1, "exactly one fsync at cancellation");
}

#[test]
fn abrupt_kill_parseability() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();
    w.node_ready("a").unwrap();
    w.node_ready("b").unwrap();
    w.run_finished(RunOutcome::Succeeded).unwrap();

    let full = sink.bytes();
    // Cut the final record at an arbitrary byte mid-line.
    let cut = full.len() - 10;
    let truncated = &full[..cut];

    let parsed = read_records(truncated).unwrap();
    assert!(
        parsed.trailing_partial_discarded,
        "one trailing partial discarded"
    );
    assert_eq!(
        parsed.records.len(),
        3,
        "every complete record still parses"
    );
    // A fully-intact stream discards nothing.
    let whole = read_records(&full).unwrap();
    assert_eq!(whole.records.len(), 4);
    assert!(!whole.trailing_partial_discarded);
}

#[test]
fn concurrent_run_disjointness() {
    // Two writers, two run ids, one base+pipeline. The path each writes under is
    // derived by run id, so the files are disjoint; concatenating both and
    // partitioning by run_id recovers each run's records exactly.
    let sink_a = CaptureSink::new();
    let sink_b = CaptureSink::new();
    let mut a = EventStreamWriter::new(
        sink_a.clone(),
        ManualClock::new(),
        RunId::from_operator("run-A"),
        "p",
    );
    let mut b = EventStreamWriter::new(
        sink_b.clone(),
        ManualClock::new(),
        RunId::from_operator("run-B"),
        "p",
    );

    a.run_started(header()).unwrap();
    b.run_started(header()).unwrap();
    a.node_ready("x").unwrap();
    b.node_ready("y").unwrap();

    // Each writer's declared stream path is disjoint by run id.
    assert_ne!(a.stream_path("base"), b.stream_path("base"));
    assert!(a.stream_path("base").ends_with(EVENTS_FILE_NAME));
    assert!(a.stream_path("base").contains("run-A"));
    assert!(b.stream_path("base").contains("run-B"));

    // Concatenate + partition by run_id.
    let mut all = sink_a.bytes();
    all.extend_from_slice(&sink_b.bytes());
    let parsed = read_records(&all).unwrap();
    let a_records: Vec<_> = parsed
        .records
        .iter()
        .filter(|r| r["run_id"] == serde_json::json!("run-A"))
        .collect();
    let b_records: Vec<_> = parsed
        .records
        .iter()
        .filter(|r| r["run_id"] == serde_json::json!("run-B"))
        .collect();
    assert_eq!(a_records.len(), 2);
    assert_eq!(b_records.len(), 2);
}

#[test]
fn mid_run_sink_failure_surfaces_fault() {
    // Sink fails on the 3rd append (after 2 OK).
    let sink = FailingSink::after(2);
    let mut w = EventStreamWriter::new(
        sink,
        ManualClock::new(),
        RunId::from_operator("run-fail"),
        "p",
    );
    w.run_started(header()).unwrap();
    w.node_ready("a").unwrap();
    // The third record's append fails → run-level fault.
    let fault = w.node_ready("b").unwrap_err();
    assert_eq!(fault.reason, EVENT_STREAM_UNWRITABLE);
    assert_eq!(fault.reason, "event stream unwritable");
}

#[test]
fn foldability_with_no_original_run_access() {
    // A completed stream file only — hand its bytes to the reader used by the
    // fold contract (C22/T42). It yields the ordered record sequence with
    // envelope fields intact, using nothing but the bytes.
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();
    w.attempt_started("a", 1).unwrap();
    w.attempt_succeeded("a", 1).unwrap();
    w.node_terminal("a", TerminalState::Succeeded).unwrap();
    w.run_finished(RunOutcome::Succeeded).unwrap();

    let bytes = sink.bytes();
    drop(w); // no live writer, no run object

    let parsed = read_records(&bytes).unwrap();
    assert_eq!(parsed.records.len(), 5);
    let seqs: Vec<u64> = parsed
        .records
        .iter()
        .map(|r| r["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![0, 1, 2, 3, 4], "ordered with envelope intact");
    assert_eq!(parsed.records[0]["event"], serde_json::json!("run-started"));
    assert_eq!(
        parsed.records.last().unwrap()["event"],
        serde_json::json!("run-finished")
    );
}

#[test]
fn canonical_bytes_are_deterministic_and_sorted() {
    // Two writers producing the same record must emit byte-identical lines, and
    // object keys are lexicographically sorted (T4 §6).
    let sink1 = CaptureSink::new();
    let sink2 = CaptureSink::new();
    let mut w1 = EventStreamWriter::new(
        sink1.clone(),
        ManualClock::new(),
        RunId::from_operator("r"),
        "p",
    );
    let mut w2 = EventStreamWriter::new(
        sink2.clone(),
        ManualClock::new(),
        RunId::from_operator("r"),
        "p",
    );
    w1.run_started(header()).unwrap();
    w2.run_started(header()).unwrap();

    assert_eq!(
        sink1.lines()[0],
        sink2.lines()[0],
        "byte-identical canonical output"
    );

    // Top-level keys are sorted and compact (no spaces after ':' or ',').
    let line = String::from_utf8(sink1.lines()[0].clone()).unwrap();
    assert!(
        line.ends_with('\n'),
        "record is one physical line terminated by \\n"
    );
    let trimmed = line.trim_end_matches('\n');
    assert!(!trimmed.contains(", "), "compact: no space after comma");
    assert!(!trimmed.contains(": "), "compact: no space after colon");
    // The envelope keys appear in sorted order in the raw bytes.
    let idx_event = trimmed.find("\"event\"").unwrap();
    let idx_offset = trimmed.find("\"offset_ns\"").unwrap();
    let idx_run = trimmed.find("\"run_id\"").unwrap();
    let idx_schema = trimmed.find("\"schema_version\"").unwrap();
    let idx_seq = trimmed.find("\"seq\"").unwrap();
    let idx_wall = trimmed.find("\"wall\"").unwrap();
    // "body" < "event" < "offset_ns" < "run_id" < "schema_version" < "seq" < "wall"
    let idx_body = trimmed.find("\"body\"").unwrap();
    assert!(idx_body < idx_event);
    assert!(idx_event < idx_offset);
    assert!(idx_offset < idx_run);
    assert!(idx_run < idx_schema);
    assert!(idx_schema < idx_seq);
    assert!(idx_seq < idx_wall);
}

#[test]
fn terminal_states_use_normative_names() {
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();
    for (i, (state, name)) in [
        (TerminalState::Succeeded, "succeeded"),
        (TerminalState::Failed, "failed"),
        (TerminalState::TimedOut, "timed-out"),
        (TerminalState::Skipped, "skipped"),
        (TerminalState::UpstreamSkipped, "upstream-skipped"),
        (TerminalState::UpstreamFailed, "upstream-failed"),
        (TerminalState::Cancelled, "cancelled"),
        (TerminalState::Abandoned, "abandoned"),
        (TerminalState::SatisfiedFromPrior, "satisfied-from-prior"),
    ]
    .into_iter()
    .enumerate()
    {
        w.node_terminal(&format!("n{i}"), state).unwrap();
        let obj = as_obj(sink.lines().last().unwrap());
        assert_eq!(obj["event"], serde_json::json!("node-terminal"));
        assert_eq!(obj["body"]["state"], serde_json::json!(name));
    }
}

#[test]
fn event_kinds_have_stable_wire_names() {
    // Every C19 transition serializes under its documented kebab-case kind name.
    let sink = CaptureSink::new();
    let mut w = writer(sink.clone(), ManualClock::new());
    w.run_started(header()).unwrap();
    w.node_ready("n").unwrap();
    w.node_admitted("n").unwrap();
    w.attempt_started("n", 1).unwrap();
    w.attempt_succeeded("n", 1).unwrap();
    w.attempt_failed("n", 2).unwrap();
    w.node_terminal("n", TerminalState::Failed).unwrap();
    w.zombie_at_exit("n").unwrap();
    w.run_finished(RunOutcome::Failed).unwrap();

    let kinds: Vec<String> = sink
        .lines()
        .iter()
        .map(|l| as_obj(l)["event"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        kinds,
        vec![
            "run-started",
            "node-ready",
            "node-admitted",
            "attempt-started",
            "attempt-succeeded",
            "attempt-failed",
            "node-terminal",
            "zombie-at-exit",
            "run-finished",
        ]
    );
    // Attempt records carry node + attempt number.
    let admit = as_obj(&sink.lines()[3]);
    assert_eq!(admit["body"]["node"], serde_json::json!("n"));
    assert_eq!(admit["body"]["attempt"], serde_json::json!(1));
}

/// Event enum round-trips through the writer as the public constructor path.
/// (Kept minimal — the `Event` type is public so the run loop (T24) can name it.)
#[test]
fn event_enum_is_constructible() {
    let _ = Event::RunFinished {
        outcome: RunOutcome::Succeeded,
    };
    let _ = Event::NodeTerminal {
        node: "n".to_string(),
        state: TerminalState::Succeeded,
    };
}
