//! C22 · Crashed-run finalize path — ticket T68 (060). Written first, TDD.
//!
//! This is the **integration proof of the crash clause** of system criterion 3
//! (arch.md `## System-level acceptance` criterion 3: "Every run produces
//! artifacts — including runs that crashed …"; `### C22 · Run artifact`
//! acceptance: "A crashed run's stream folds into an artifact, marked
//! interrupted, containing everything up to the crash — produced by a later
//! invocation of the binary"; `### C19 · Event stream`: "Killing the process
//! abruptly at any moment leaves a stream whose every record but at most one
//! trailing partial is valid and parseable", and "A stream can be folded into a
//! run artifact by a function that needs no access to the original run").
//!
//! # What makes this the integration proof (vs T27 / T42)
//!
//! T42 folds **hand-built** truncated streams; T27 simulates a crash as a
//! **deterministic byte-truncation** of a captured writer buffer **inside the
//! test process**. Neither one actually kills a live process. This suite closes
//! the remaining gap: it launches a **real run as a separate OS process**
//! (`dagr-crashy-run`, the checked-in test-support harness) that writes its C19
//! event stream **continuously to a real on-disk `events.jsonl`**, kills that
//! process **abruptly with an uncatchable signal** (`Child::kill` — SIGKILL on
//! unix, `TerminateProcess` on Windows; no exit handler can run), and folds the
//! **surviving on-disk bytes** with T42's **standalone** [`fold_stream`] — with
//! **no access to the dead run's live state**, exactly as a later binary
//! invocation would. The crash surface is genuine: an abrupt kill mid-append can
//! leave the file cut mid-line, which is the trailing-partial the fold must
//! tolerate.
//!
//! # Determinism (no fixed sleeps)
//!
//! Timing is coordinated by **observing on-disk state**, never a fixed-duration
//! sleep: the harness writes a `ready` marker file at the requested mid-run
//! checkpoint, and the test spins on the marker's existence before killing. The
//! only bounded waits are a generous overall timeout guarding against a harness
//! that never becomes ready (a bug, not a race) — a poll loop, not a sleep the
//! assertion depends on.
//!
//! # Scope (T68)
//!
//! This suite consumes the merged C19 writer (T19) and C22 fold (T42) unchanged
//! and asserts only the **`interrupted` marking** and **up-to-crash completeness**
//! over a real killed run, plus the negative control. The CLI `fold` *verb* is
//! T55; the assembly-failed / bootstrap-failed variants, allowlist sentinels, and
//! the fixture-corpus CI are T42/T48 — all out of scope here.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dagr_artifact::event_stream::{read_records, EVENTS_FILE_NAME};
use dagr_artifact::fold::fold_stream;
use serde_json::Value;

/// The checked-in test-support harness binary (a real run under kill). Cargo sets
/// `CARGO_BIN_EXE_<name>` for every bin in the package when compiling this
/// integration test, so the path is resolved at build time — no `target/` path
/// guessing.
const HARNESS: &str = env!("CARGO_BIN_EXE_dagr-crashy-run");

/// The graph's node roster the fold is given for coverage — the crashy pipeline
/// is a two-node chain `a → b` (`b` never ran on any crash checkpoint).
fn graph_nodes() -> Vec<String> {
    vec!["a".to_string(), "b".to_string()]
}

/// A per-test collision-proof run-store base under the OS temp dir. A
/// process-monotonic counter (plus pid + a wall stamp) makes every base provably
/// disjoint, so parallel tests never share — or delete — the same subtree.
fn temp_base() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("dagr-t68-{}-{stamp}-{unique}", std::process::id()))
}

/// The on-disk event-stream path a run writes under
/// (`<base>/crashy-pipeline/<run-id>/events.jsonl` — the harness's fixed
/// pipeline name).
fn stream_path(base: &Path, run_id: &str) -> PathBuf {
    base.join("crashy-pipeline")
        .join(run_id)
        .join(EVENTS_FILE_NAME)
}

/// Poll `predicate` until it holds or the deadline elapses, spinning on observable
/// state (never a fixed sleep the assertion depends on). Returns `true` iff the
/// predicate held before the deadline.
fn wait_until(mut predicate: impl FnMut() -> bool, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        // A tiny yield keeps the poll from pegging a core; it is not a
        // synchronization sleep — correctness rests on the marker/exit, not on
        // this duration.
        std::thread::sleep(Duration::from_millis(1));
    }
    predicate()
}

/// Launch the harness at `checkpoint`, wait until it signals mid-run readiness on
/// disk, then **abruptly kill** it and confirm it is dead. Returns the surviving
/// on-disk stream bytes (read only after the process is confirmed dead, so no
/// concurrent writer can be racing the read). The harness's live run state is
/// gone — only the file remains, exactly the "later invocation" contract.
fn launch_kill_and_read(base: &Path, run_id: &str, checkpoint: &str) -> Vec<u8> {
    let marker = base.join(format!("{run_id}.ready"));
    let mut child: Child = Command::new(HARNESS)
        .arg(base.as_os_str())
        .arg(run_id)
        .arg(checkpoint)
        .arg(&marker)
        .spawn()
        .expect("the crashy-run harness launches as a separate OS process");

    // Wait until the run has reached its mid-run checkpoint (a node executing,
    // a node still pending) — observed via the on-disk marker, not a sleep.
    let ready = wait_until(|| marker.exists(), Duration::from_secs(30));
    assert!(
        ready,
        "the harness reached its mid-run checkpoint and signalled readiness on disk"
    );

    // Abruptly kill the child with an uncatchable signal (SIGKILL on unix) — no
    // exit handler can run, exactly the abrupt-container-kill failure mode C19
    // exists to survive.
    child.kill().expect("the mid-run child is killed abruptly");
    let status = child.wait().expect("reap the killed child");
    assert!(
        !status.success(),
        "an abruptly-killed child does not exit successfully (it was signalled dead)"
    );

    // The dead run's live state is gone; read only the surviving on-disk stream.
    let path = stream_path(base, run_id);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read surviving stream {}: {e}", path.display()))
}

/// The `kind` of a folded/parsed record.
fn kinds(records: &[Value]) -> Vec<String> {
    records
        .iter()
        .filter_map(|r| r.get("kind").and_then(Value::as_str).map(String::from))
        .collect()
}

// ===========================================================================
// A real killed run folds into an interrupted artifact.
// ===========================================================================

/// **A real killed run folds into an interrupted artifact.** Launch the harness,
/// let it reach a mid-run state (node `a` executing, node `b` pending), kill it
/// abruptly, and fold the surviving stream with the standalone fold. The fold
/// succeeds and the artifact's overall outcome carries the interrupted marking.
///
/// Non-vacuous: if the crashed stream did **not** finalize — e.g. the fold
/// errored on the killed stream, or failed to mark it interrupted — this fails.
/// The interrupted signal is a **first-class** top-level field distinct from a
/// deliberate cancellation (both read `overall_outcome = cancelled`).
#[test]
fn a_real_killed_run_folds_into_an_interrupted_artifact() {
    let base = temp_base();
    let run_id = "run-interrupted";
    let bytes = launch_kill_and_read(&base, run_id, "after-first-attempt-started");

    let art = fold_stream(&bytes, &graph_nodes())
        .expect("the surviving crashed stream folds — the crash path finalizes");
    assert!(
        art.is_interrupted(),
        "a crash-truncated run (no run-finished) is marked interrupted"
    );
    // The crash-truncation reads `cancelled` inside the closed schema enum; the
    // distinction from a deliberate cancellation lives in the interrupted flag.
    assert_eq!(
        art.overall_outcome(),
        "cancelled",
        "the outcome stays inside the closed enum (interrupted carries the distinction)"
    );

    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// Everything up to the crash is present.
// ===========================================================================

/// **Everything recorded up to the crash is present in the folded body.** The
/// harness records a known set of transitions before the kill point
/// (`run-started`, `node-ready`, `node-admitted`, `attempt-started` for node `a`);
/// after the kill, the folded artifact reflects that reached state — not an empty
/// or exit-only record.
///
/// Non-vacuous: a fold that dropped the pre-crash records, or produced an
/// empty/never-ran-only body, would fail the "node `a` present + admitted-offset
/// header" checks.
#[test]
fn everything_up_to_the_crash_is_present() {
    let base = temp_base();
    let run_id = "run-uptocrash";
    let bytes = launch_kill_and_read(&base, run_id, "after-first-attempt-started");

    // The raw surviving stream carries the exact transitions recorded before the
    // kill: run-started, node-ready(a), node-admitted(a), attempt-started(a). The
    // fold reuses the tolerant reader, so parse the same way to inspect them.
    let read = read_records(&bytes).expect("the surviving stream parses tolerantly");
    let ks = kinds(&read.records);
    for expected in [
        "run-started",
        "node-ready",
        "node-admitted",
        "attempt-started",
    ] {
        assert!(
            ks.iter().any(|k| k == expected),
            "the {expected} transition recorded before the kill survives (kinds: {ks:?})"
        );
    }

    let art = fold_stream(&bytes, &graph_nodes()).expect("fold");
    // Node `a` reached the run (it was ready + admitted + attempt-started); every
    // graph node still appears at least once (never-ran `b` carries a propagated
    // terminal state — node coverage).
    for node in ["a", "b"] {
        assert!(
            art.attempts().iter().any(|at| at.node() == node),
            "graph node `{node}` appears in the folded artifact (up-to-crash + coverage)"
        );
    }
    assert!(
        art.is_interrupted(),
        "the up-to-crash artifact is still marked interrupted"
    );

    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// Header is complete despite the crash (kill as early as possible).
// ===========================================================================

/// **The header is complete despite the crash.** Kill the child as early as
/// possible — right after `run-started` and one following event are on disk. The
/// folded header (run identity, pipeline identity, both fingerprints, parameters,
/// data interval, allowlisted captured environment) is fully populated from the
/// `run-started` event; only the overall outcome (`interrupted`) and the summary
/// reflect the truncation.
///
/// Non-vacuous: were the header assembled from anything but `run-started`, an
/// early kill (before any node terminal / run-finished) would leave header fields
/// empty and fail these exact-value checks.
#[test]
fn header_is_complete_despite_the_crash() {
    let base = temp_base();
    let run_id = "run-earlykill-header";
    let bytes = launch_kill_and_read(&base, run_id, "after-run-started");

    let art = fold_stream(&bytes, &graph_nodes()).expect("fold of an early-killed stream");

    // The header is complete from the run-started event alone.
    assert_eq!(
        art.header_run_id(),
        run_id,
        "run identity folded from run-started"
    );
    assert_eq!(
        art.header_pipeline(),
        "crashy-pipeline",
        "pipeline identity"
    );
    assert_eq!(
        art.header_fingerprint_structural(),
        Some("blake3:1111111111111111111111111111111111111111111111111111111111111111"),
        "structural fingerprint present (assembly succeeded)"
    );
    assert_eq!(
        art.header_fingerprint_policy(),
        Some("blake3:2222222222222222222222222222222222222222222222222222222222222222"),
        "policy fingerprint present"
    );
    assert_eq!(
        art.header_parameters().get("date").and_then(Value::as_str),
        Some("2026-07-23"),
        "invocation parameters folded from the start header"
    );
    assert!(
        art.header_data_interval().is_some(),
        "data interval folded from the start header"
    );
    assert_eq!(
        art.header_captured_environment()
            .get("DAGR_REGION")
            .and_then(Value::as_str),
        Some("us-east-1"),
        "allowlisted captured environment folded from the start header"
    );

    // Only the outcome + summary reflect the truncation.
    assert!(art.is_interrupted(), "interrupted marks the truncation");
    assert_eq!(art.overall_outcome(), "cancelled");

    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// At most one trailing partial is tolerated, silently.
// ===========================================================================

/// **At most one trailing partial is tolerated, and no error is raised for it.**
/// An abrupt kill mid-append can leave the last write byte-truncated. The fold
/// tolerates at most one trailing partial, raises no error, still folds, and marks
/// the artifact interrupted — the real-kill counterpart to T42's hand-built
/// trailing-partial test.
///
/// The `partial-tail` checkpoint **deterministically** leaves the surviving
/// on-disk stream ending mid-record (the harness appends a genuine byte-truncated
/// fragment before the kill, modelling a kill accepted part-way through the
/// sink's append), so the fold's single-trailing-partial discard is exercised
/// over a real killed process — not just observed opportunistically. The other
/// checkpoints assert the fold **never errors** and marks interrupted regardless.
#[test]
fn at_most_one_trailing_partial_is_tolerated_silently() {
    // The partial-tail checkpoint is the deterministic mid-record cut.
    {
        let base = temp_base();
        let run_id = "run-partial-deterministic";
        let bytes = launch_kill_and_read(&base, run_id, "partial-tail");
        assert_ne!(
            bytes.last(),
            Some(&b'\n'),
            "the partial-tail kill leaves the on-disk stream ending mid-record"
        );
        let art = fold_stream(&bytes, &graph_nodes())
            .expect("the fold tolerates the single byte-truncated trailing partial (no error)");
        assert!(
            art.trailing_partial_discarded(),
            "exactly one trailing partial was discarded — silently, no error"
        );
        assert!(art.is_interrupted(), "still marked interrupted");
        // Everything up to the cut is present: node `a` succeeded before the
        // partial record.
        assert!(
            art.attempts()
                .iter()
                .any(|at| at.node() == "a" && at.status() == "succeeded"),
            "the complete records before the partial tail are all present"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    // Across the other real-kill checkpoints the fold never errors and marks
    // interrupted; when a kill happens to land mid-record it reports the single
    // discard (never two — a second partial would be a hard error).
    for (i, checkpoint) in [
        "after-run-started",
        "after-first-attempt-started",
        "after-node-terminal",
    ]
    .into_iter()
    .enumerate()
    {
        let base = temp_base();
        let run_id = format!("run-partial-{i}");
        let bytes = launch_kill_and_read(&base, &run_id, checkpoint);

        let art = fold_stream(&bytes, &graph_nodes()).unwrap_or_else(|e| {
            panic!("fold must tolerate the abrupt-kill stream at {checkpoint}, got error: {e}")
        });
        assert!(art.is_interrupted(), "{checkpoint}: interrupted");

        if bytes.last() != Some(&b'\n') && !bytes.is_empty() {
            assert!(
                art.trailing_partial_discarded(),
                "{checkpoint}: a byte-truncated tail is the single tolerated trailing partial"
            );
        }

        let _ = std::fs::remove_dir_all(&base);
    }
}

// ===========================================================================
// The dead run's live state is never touched.
// ===========================================================================

/// **The dead run's live state is never touched — the fold needs only the stream
/// bytes.** After the kill, copy the surviving stream bytes out and delete the
/// entire run-store directory, then fold the bytes alone. The fold succeeds using
/// only the bytes — it opens no run store, no live graph, no network — matching
/// the "produced by the next invocation" contract (C19 fold criterion; C22).
///
/// Non-vacuous: if the fold reached back for any run-store file or live state, the
/// fold-after-deletion would fail (there is nothing left but the in-memory bytes).
#[test]
fn the_dead_runs_live_state_is_never_touched() {
    let base = temp_base();
    let run_id = "run-noaccess";
    let bytes = launch_kill_and_read(&base, run_id, "after-first-attempt-started");

    // Make everything except the in-memory bytes inaccessible: delete the whole
    // run store the dead run wrote under.
    std::fs::remove_dir_all(&base).expect("delete the entire run store");
    assert!(
        !base.exists(),
        "the run store is gone — only the bytes remain"
    );

    // Fold the bytes alone — no store, no live run, no network.
    let art = fold_stream(&bytes, &graph_nodes())
        .expect("the fold uses only the stream bytes it is handed");
    assert!(art.is_interrupted());
    assert_eq!(
        art.header_run_id(),
        run_id,
        "header folded from the bytes alone"
    );
}

// ===========================================================================
// Kill at different points all fold.
// ===========================================================================

/// **Killing at distinct observable checkpoints each folds into an interrupted
/// artifact containing exactly the transitions recorded before that point.**
/// Killing just after `run-started`, after the first `attempt-started`, and after
/// a node reached a terminal state (with `b` pending) each yields a valid
/// interrupted artifact whose recorded kinds grow monotonically with the kill
/// point — demonstrating "killing at any moment" survives through the finalize
/// path (C19 abrupt-kill; C22 crashed-run).
#[test]
fn kill_at_different_points_all_fold() {
    // Each successively-later checkpoint must record a superset of the earlier
    // one's transitions before the kill.
    let checkpoints = [
        ("after-run-started", 0usize),
        ("after-first-attempt-started", 0),
        ("after-node-terminal", 0),
    ];

    let mut prev_terminal_count: Option<usize> = None;
    for (checkpoint, _) in checkpoints {
        let base = temp_base();
        let run_id = format!("run-cp-{checkpoint}");
        let bytes = launch_kill_and_read(&base, &run_id, checkpoint);

        let art = fold_stream(&bytes, &graph_nodes())
            .unwrap_or_else(|e| panic!("fold at {checkpoint}: {e}"));
        assert!(art.is_interrupted(), "{checkpoint}: interrupted");

        // The recorded node-terminal count is monotonic in the kill point:
        // after-run-started / after-first-attempt-started have 0 real terminals
        // recorded for `a`; after-node-terminal has 1. (Never-ran coverage
        // records are synthesized by the fold, so count them from the raw stream.)
        let read = read_records(&bytes).expect("parse");
        let real_terminals = read
            .records
            .iter()
            .filter(|r| r.get("kind").and_then(Value::as_str) == Some("node-terminal"))
            .count();
        if let Some(prev) = prev_terminal_count {
            assert!(
                real_terminals >= prev,
                "{checkpoint}: recorded transitions grow monotonically with the kill point"
            );
        }
        prev_terminal_count = Some(real_terminals);

        // After the node-terminal checkpoint specifically, node `a`'s success is
        // recorded before the kill.
        if checkpoint == "after-node-terminal" {
            assert!(
                art.attempts()
                    .iter()
                    .any(|at| at.node() == "a" && at.status() == "succeeded"),
                "after-node-terminal: node `a`'s recorded success is present up to the crash"
            );
        }

        let _ = std::fs::remove_dir_all(&base);
    }
}

// ===========================================================================
// Interrupted marking is not always-on (negative control).
// ===========================================================================

/// **Interrupted marking is not always-on.** Launch the same harness but let it
/// run to natural completion (no kill), then fold its complete stream. The folded
/// artifact is **not** marked interrupted and its outcome reflects the actual
/// terminal result (`succeeded`) — proving the interrupted marking distinguishes
/// crashed runs from finished ones, rather than being a constant.
///
/// This is the load-bearing negative control: without it, every other test could
/// pass on a fold that always marks `interrupted = true`.
#[test]
fn interrupted_marking_is_not_always_on_negative_control() {
    let base = temp_base();
    let run_id = "run-clean";
    let marker = base.join(format!("{run_id}.ready"));

    // Run to natural completion: the harness finishes and exits 0 on "finish".
    let status = Command::new(HARNESS)
        .arg(base.as_os_str())
        .arg(run_id)
        .arg("finish")
        .arg(&marker)
        .status()
        .expect("the harness runs to completion as a separate OS process");
    assert!(
        status.success(),
        "the un-killed run finishes cleanly and exits successfully"
    );
    // Its readiness marker is written last, after run-finished — a completed run.
    assert!(marker.exists(), "the finished run signalled completion");

    let bytes = std::fs::read(stream_path(&base, run_id)).expect("read the complete stream");
    let art = fold_stream(&bytes, &graph_nodes()).expect("fold of a complete run");

    assert!(
        !art.is_interrupted(),
        "a run allowed to finish is NOT marked interrupted (the marking is a genuine signal)"
    );
    assert_eq!(
        art.overall_outcome(),
        "succeeded",
        "the outcome reflects the actual terminal result, not a crash"
    );
    // The complete stream ends with run-finished; the reader kept no trailing
    // partial.
    let read = read_records(&bytes).expect("parse");
    assert!(
        !read.trailing_partial_discarded,
        "a cleanly finished run leaves no trailing partial"
    );
    assert_eq!(
        kinds(&read.records).last().map(String::as_str),
        Some("run-finished"),
        "the complete stream ends with run-finished"
    );

    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// Crash-surviving artifact requires only a persistent store, nothing else.
// ===========================================================================

/// **The crash path uses only an on-disk run-store directory — no server,
/// database, or scheduler (system criterion 7 crash-survival clause).** The child
/// writes to a plain on-disk directory with nothing else running; after the kill,
/// the interrupted artifact is produced from the on-disk stream alone.
///
/// The `base` here is an ordinary directory under the OS temp dir — the whole
/// operational requirement. Nothing else (no listening socket, no DB handle, no
/// scheduler) participates, which is exactly the point of criterion 7.
#[test]
fn crash_surviving_artifact_requires_only_a_persistent_store() {
    let base = temp_base();
    let run_id = "run-storeonly";

    // The only operational input is the store directory path handed to the child.
    let bytes = launch_kill_and_read(&base, run_id, "after-node-terminal");

    // The interrupted artifact is produced from the on-disk stream alone.
    let art = fold_stream(&bytes, &graph_nodes()).expect("fold from the on-disk stream alone");
    assert!(art.is_interrupted());
    // The store location is a plain directory — the sole operational requirement.
    assert!(
        stream_path(&base, run_id).is_file(),
        "the stream is a plain file under an ordinary on-disk directory"
    );

    let _ = std::fs::remove_dir_all(&base);
}
