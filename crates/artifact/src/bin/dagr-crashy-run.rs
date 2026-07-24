//! `dagr-crashy-run` — a **test-support** pipeline-run harness for T68 (ticket
//! 060): the crashed-run finalize path.
//!
//! # Why this exists
//!
//! T68 proves system criterion 3's *crash clause* (arch.md `### C22 · Run
//! artifact`, acceptance: "a crashed run's stream folds into an artifact, marked
//! interrupted, containing everything up to the crash — produced by a later
//! invocation of the binary"). The honest way to test that is to launch a **real**
//! run as a **separate OS process** writing its C19 event stream continuously to a
//! real on-disk `events.jsonl`, then kill that process **abruptly with an
//! uncatchable signal** (so no exit handler runs — "the dominant failure mode in a
//! container is an abrupt kill with no chance to run an exit handler", arch.md
//! C19) and fold the surviving bytes with the standalone T42 fold. This binary is
//! the run under kill.
//!
//! The M1 driver writes its stream through an **injected** [`EventSink`] (T0.6),
//! and the production local-file sink (T0.6 / C18) is not yet wired into a
//! runnable `dagr` verb (that is T55). So this harness drives the **real** merged
//! C19 [`EventStreamWriter`] directly through a minimal append-only file sink and
//! emits exactly the transition sequence a real run would (`run-started`,
//! `node-ready`/`node-admitted`/`attempt-started`/`attempt-succeeded`/
//! `attempt-outcome`/`node-terminal` per node, and `run-finished` only when it is
//! allowed to finish). It is **not** production code and ships in no released
//! binary — it is checked-in, reusable scaffolding the T68 integration test (and
//! T49, the M3 demo) launch and kill.
//!
//! # Determinism contract (no fixed sleeps)
//!
//! The harness synchronises with its killer through **observable on-disk state**,
//! never a fixed-duration sleep. It:
//!   1. writes each event through the real write-through writer (so the on-disk
//!      `events.jsonl` grows as the run progresses — no buffering to exit);
//!   2. at the requested **kill checkpoint**, creates an on-disk `ready` marker
//!      file, so the parent test can `SIGKILL` exactly once the run has reached a
//!      known mid-run state (at least one attempt recorded, at least one node
//!      still pending);
//!   3. then spins forever awaiting the kill — it never exits on its own, so a
//!      surviving `run-finished` can only appear on the explicit "finish" mode
//!      (the negative control).
//!
//! # Usage
//!
//! ```text
//! dagr-crashy-run <run-store-base> <run-id> <checkpoint> <ready-marker-path>
//! ```
//! `checkpoint` selects the observable point at which the `ready` marker is
//! written (and thus where the parent kills): `after-run-started`,
//! `after-first-attempt-started`, `after-node-terminal`, or `finish` (the
//! negative control — runs to a clean `run-finished` and exits `0`).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState, EVENTS_FILE_NAME, FINGERPRINT_ALGORITHM_VERSION,
};

/// A minimal append-only local-file [`EventSink`]: it appends each complete line
/// to the run's `events.jsonl` and flushes to the OS on `flush`. It models the
/// default local-file sink's crash-relevant property — it does **not** fsync per
/// append (T0.6 §6), so the bytes an abrupt kill leaves on disk are exactly the
/// appends that reached the file, possibly cut mid-line. That is precisely the
/// crash surface T68 folds.
struct FileSink {
    file: File,
}

impl FileSink {
    fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }
}

impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        // Write the whole line, then flush to the OS so the bytes are visible on
        // disk to a concurrent reader (the parent) — but do NOT fsync per append,
        // matching the default sink's crash surface.
        self.file.write_all(line)?;
        self.file.flush()
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A monotonic clock advanced by hand, one fixed step per event, so offsets are
/// deterministic, distinct, and non-zero regardless of wall-clock timing.
struct StepClock {
    now: std::cell::Cell<u64>,
}

impl StepClock {
    fn new() -> Self {
        Self {
            now: std::cell::Cell::new(0),
        }
    }
    fn advance(&self, by: u64) {
        self.now.set(self.now.get() + by);
    }
}

impl MonotonicClock for StepClock {
    fn elapsed_ns(&self) -> u64 {
        self.now.get()
    }
}

/// The full run-artifact header known at start — every field populated, so the
/// folded artifact's header is complete from `run-started` alone (C19).
fn header() -> RunStartedHeader {
    let mut parameters = std::collections::BTreeMap::new();
    parameters.insert("date".to_string(), "2026-07-23".to_string());
    let mut captured_env = std::collections::BTreeMap::new();
    captured_env.insert("DAGR_REGION".to_string(), "us-east-1".to_string());
    RunStartedHeader {
        pipeline: "crashy-pipeline".to_string(),
        fingerprint_structural: Some(
            "blake3:1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        fingerprint_policy: Some(
            "blake3:2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters,
        data_interval: Some([
            "2026-07-23T00:00:00Z".to_string(),
            "2026-07-24T00:00:00Z".to_string(),
        ]),
        captured_env,
        resumed_from: None,
    }
}

/// The observable checkpoint at which the harness signals "ready to be killed"
/// (or, for [`Finish`](Checkpoint::Finish), runs to a clean end — the negative
/// control).
#[derive(Clone, Copy)]
enum Checkpoint {
    /// Signal ready right after `run-started` and one following event — the
    /// header-complete-despite-crash case (kill as early as possible).
    AfterRunStarted,
    /// Signal ready right after node `a`'s first `attempt-started` — a node is
    /// executing with node `b` still pending.
    AfterFirstAttemptStarted,
    /// Signal ready right after node `a` reached a terminal state, with node `b`
    /// still pending — the "a node terminal, others pending" checkpoint.
    AfterNodeTerminal,
    /// After node `a`'s terminal, append a **byte-truncated (unterminated)**
    /// fragment of the next record directly to the file — modelling a kill the OS
    /// accepted only *part-way through* the sink's append (the realistic outcome
    /// of an abrupt kill mid-write). This deterministically leaves the surviving
    /// stream ending mid-record, so the fold's single-trailing-partial discard is
    /// exercised over a real killed process, not just a hand-built stream.
    PartialTail,
    /// Do not stop: run to a clean `run-finished` and exit — the negative control
    /// (a finished run must NOT fold to an interrupted artifact).
    Finish,
}

impl Checkpoint {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "after-run-started" => Some(Checkpoint::AfterRunStarted),
            "after-first-attempt-started" => Some(Checkpoint::AfterFirstAttemptStarted),
            "after-node-terminal" => Some(Checkpoint::AfterNodeTerminal),
            "partial-tail" => Some(Checkpoint::PartialTail),
            "finish" => Some(Checkpoint::Finish),
            _ => None,
        }
    }
}

/// Write the on-disk `ready` marker (atomically, via write + rename) so the
/// parent observes a complete file the instant it appears.
fn signal_ready(marker: &Path) -> io::Result<()> {
    let tmp = marker.with_extension("tmp");
    std::fs::write(&tmp, b"ready")?;
    std::fs::rename(&tmp, marker)
}

/// Spin forever, awaiting the abrupt kill. The harness never returns from here on
/// a crash checkpoint — so a `run-finished` never lands, and the only way the
/// process ends is the parent's uncatchable signal.
fn spin_until_killed() -> ! {
    loop {
        std::hint::spin_loop();
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, base, run_id, checkpoint, marker] = args.as_slice() else {
        eprintln!(
            "usage: dagr-crashy-run <run-store-base> <run-id> <checkpoint> <ready-marker-path>"
        );
        return ExitCode::from(2);
    };
    let Some(checkpoint) = Checkpoint::parse(checkpoint) else {
        eprintln!("unknown checkpoint `{checkpoint}`");
        return ExitCode::from(2);
    };
    let marker = Path::new(marker);

    // The run store path this run writes under: <base>/<pipeline>/<run-id>/events.jsonl
    // (T0.6 §3) — the same layout the writer's `stream_path` computes.
    let stream_path = PathBuf::from(format!(
        "{base}/crashy-pipeline/{run_id}/{EVENTS_FILE_NAME}"
    ));
    let sink = match FileSink::create(&stream_path) {
        Ok(sink) => sink,
        Err(e) => {
            eprintln!("cannot open event stream: {e}");
            return ExitCode::from(2);
        }
    };
    let clock = StepClock::new();
    let mut writer = EventStreamWriter::new(
        sink,
        clock_ref(&clock),
        RunId::from_operator(run_id),
        "crashy-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    // run-started (seq 0) — carries the full header known at start.
    if writer.run_started(header()).is_err() {
        return ExitCode::from(2);
    }

    // node `a` becomes ready; `b` stays pending the whole run.
    clock.advance(100);
    let _ = writer.node_ready("a");
    if matches!(checkpoint, Checkpoint::AfterRunStarted) {
        // Header + one following event are on disk — kill as early as possible.
        let _ = signal_ready(marker);
        spin_until_killed();
    }

    clock.advance(100);
    let _ = writer.node_admitted("a");
    clock.advance(100);
    let _ = writer.attempt_started("a", 1);
    if matches!(checkpoint, Checkpoint::AfterFirstAttemptStarted) {
        // A node is executing (attempt started) with `b` still pending.
        let _ = signal_ready(marker);
        spin_until_killed();
    }

    clock.advance(400);
    let _ = writer.attempt_succeeded("a", 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: "a".into(),
        attempt: 1,
        status: TerminalState::Succeeded.as_str().into(),
        worker: Some("compute#1".into()),
        metrics: Some(serde_json::json!({ "rows_read": 1000 })),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal("a", TerminalState::Succeeded);
    if matches!(checkpoint, Checkpoint::AfterNodeTerminal) {
        // Node `a` reached a terminal state; `b` is still pending.
        let _ = signal_ready(marker);
        spin_until_killed();
    }
    if matches!(checkpoint, Checkpoint::PartialTail) {
        // Model a kill accepted only PART-WAY through the sink's next append: put
        // a byte-truncated (unterminated) fragment of a would-be next record onto
        // the on-disk stream, so the surviving file ends mid-record. The fold must
        // tolerate and discard exactly this one trailing partial (C19), raising no
        // error. Written by re-opening the same file so it is a genuine on-disk
        // byte-truncation, not an in-memory splice.
        if let Ok(mut f) = OpenOptions::new().append(true).open(&stream_path) {
            // A prefix of a plausible next record, deliberately UNTERMINATED (no
            // trailing '\n') and cut mid-JSON so it does not parse.
            let _ =
                f.write_all(br#"{"schema_version":"dagr.event-stream@1","seq":8,"kind":"node-rea"#);
            let _ = f.flush();
        }
        let _ = signal_ready(marker);
        spin_until_killed();
    }

    // The negative control (Checkpoint::Finish): run node `b` to success and
    // finish cleanly. A clean run must NOT fold to an interrupted artifact.
    clock.advance(100);
    let _ = writer.node_ready("b");
    clock.advance(100);
    let _ = writer.node_admitted("b");
    clock.advance(100);
    let _ = writer.attempt_started("b", 1);
    clock.advance(400);
    let _ = writer.attempt_succeeded("b", 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord::new(
        "b",
        1,
        TerminalState::Succeeded.as_str(),
    ));
    let _ = writer.node_terminal("b", TerminalState::Succeeded);
    clock.advance(100);
    let _ = writer.run_finished(RunOutcome::Succeeded);
    let _ = writer.finish();
    // Signal ready last, so the parent (for the negative control) waits for a
    // fully finished stream before reading it.
    let _ = signal_ready(marker);
    ExitCode::SUCCESS
}

/// Borrow the [`StepClock`] as a [`MonotonicClock`] the writer can own by
/// reference — the writer takes the clock by value, so hand it a thin reference
/// wrapper and keep the real clock live in `main` for `advance`.
fn clock_ref(clock: &StepClock) -> ClockRef<'_> {
    ClockRef { clock }
}

/// A by-reference [`MonotonicClock`] adapter so `main` retains ownership of the
/// [`StepClock`] (to call `advance`) while the writer holds a reference to it.
struct ClockRef<'a> {
    clock: &'a StepClock,
}

impl MonotonicClock for ClockRef<'_> {
    fn elapsed_ns(&self) -> u64 {
        self.clock.elapsed_ns()
    }
}
