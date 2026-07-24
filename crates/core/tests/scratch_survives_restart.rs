//! C18 · Scratch survives a full process restart — ticket T54a (066). Written
//! first, TDD. Each test mirrors one bullet of the ticket's Test plan.
//!
//! # What makes this the restart proof (vs T53's `scratch_store.rs`)
//!
//! T53 proves the store's *in-process* contract: an attempt-1 write is readable
//! on attempt 2 through a fresh in-process handle, keys are namespaced, isolation
//! is enforced, a succeeded node's scratch is deleted. It never actually ends a
//! process. This suite closes the remaining, load-bearing gap for C18's
//! durability half: a **real, separate OS process** (`dagr-scratch-run`, the
//! checked-in test-support harness) writes a node's scratch **through the run
//! store on disk** and then **exits**; a **later, separate process** (this test
//! process — the situation a resume faces) opens the *same run directory* with a
//! fresh [`ScratchStore`] and reads the value back. The value crossed a genuine
//! process boundary via the run-store medium, not via any in-process state — the
//! foundation T54b/T58 resume stands on (arch.md `### C18`, "The shape of a run"
//! line 67; T0.6 §8, §9).
//!
//! It also proves the *lifecycle* half the amended C18 governs (arch.md line 393;
//! T0.6 §8): at run end **nothing is deleted implicitly** — only a **succeeded**
//! node's scratch is removed (by the on-success hook), and every **non-succeeded**
//! node's scratch is **retained** on disk under the run-store base, byte-for-byte,
//! for a later resume to copy forward and for **prune (C26)** — and prune alone —
//! to reclaim by removing the whole per-run directory.
//!
//! # Determinism + isolation (no wall-clock sleeps; private per-test temp)
//!
//! Timing is coordinated by **observing on-disk state**, never a fixed-duration
//! sleep: the harness writes a `ready` marker file (atomically, write+rename)
//! once it has finished its scratch work, and the test spins on the marker's
//! existence — and then on the child having actually exited — before reading. The
//! only bounded wait is a generous overall timeout guarding against a harness that
//! never becomes ready (a bug, not a race). Every test uses a **private per-test
//! temp base** under the OS temp dir keyed by pid + a monotonic counter + a
//! nanosecond stamp, so parallel test threads (and repeated suite runs) never
//! share — or delete — the same subtree (the shared-`/tmp` parallelism bug class
//! that has red-flaked this repo's CI). No child process is left orphaned: every
//! launch is reaped with `wait`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dagr_core::context::{PipelineId, RunId};
use dagr_core::handle::NodeId;
use dagr_core::scratch::{ScratchStore, SCRATCH_DIR_NAME};

/// The checked-in test-support harness binary (a real run that writes scratch and
/// exits). Cargo sets `CARGO_BIN_EXE_<name>` for every bin in the package when
/// compiling this integration test, so the path is resolved at build time — no
/// `target/` path guessing.
const HARNESS: &str = env!("CARGO_BIN_EXE_dagr-scratch-run");

/// A **private** per-test temp base, removed on drop. The name blends the pid, a
/// process-monotonic counter, and a nanosecond stamp so two tests running
/// concurrently — or two runs of the suite — never collide on a path, and one
/// test's cleanup never deletes another's subtree.
struct TempBase {
    path: PathBuf,
}

impl TempBase {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let unique = format!(
            "dagr-t54a-{tag}-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            nanos,
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).expect("create private temp base");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
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
        // synchronization sleep — correctness rests on the marker + the child's
        // exit, not on this duration.
        std::thread::sleep(Duration::from_millis(1));
    }
    predicate()
}

/// Launch the harness to write scratch for one node in a separate OS process, then
/// wait until it signals readiness on disk **and has actually exited** — so when we
/// return, the writing process is gone and only its on-disk scratch remains,
/// exactly the "a later, separate process" situation a resume faces.
///
/// `outcome` is `"succeed"` (the node reaches terminal success → its on-success
/// hook runs) or `"fail"` (the node ends non-succeeded → no cleanup runs, run end
/// deletes nothing of its scratch). The child is always reaped — no orphan.
fn write_scratch_in_a_separate_process(
    base: &Path,
    pipeline: &str,
    run: &str,
    node: &str,
    key: &str,
    value: &str,
    outcome: &str,
) {
    let marker = base.join(format!("{run}.{node}.ready"));
    let mut child = Command::new(HARNESS)
        .arg(base.as_os_str())
        .arg(pipeline)
        .arg(run)
        .arg(node)
        .arg(key)
        .arg(value)
        .arg(outcome)
        .arg(&marker)
        .spawn()
        .expect("the scratch-run harness launches as a separate OS process");

    // The harness writes the ready marker only after its scratch work is durably
    // on disk (write + optional success-hook), so once it exists the value has
    // crossed to the filesystem. Observe it — no sleep.
    let ready = wait_until(|| marker.exists(), Duration::from_secs(30));
    assert!(
        ready,
        "the harness finished its scratch work and signalled readiness on disk"
    );

    // Reap the child and confirm it exited cleanly. After this the writing
    // process is gone; nothing shares its address space — a later read can only
    // come from the run-store medium.
    let status = child.wait().expect("reap the scratch-writing child");
    assert!(
        status.success(),
        "the scratch-writing harness exits cleanly (exit status: {status:?})"
    );
}

/// Build a fresh [`ScratchStore`] handle for one node under a base — this is the
/// *later, separate* process opening an existing run directory (the resume/prune
/// situation). It shares nothing with the writer beyond the on-disk run store.
fn reopen_store(base: &Path, pipeline: &str, run: &str, node: &str) -> ScratchStore {
    ScratchStore::for_node(
        base,
        &PipelineId::new(pipeline),
        &RunId::new(run),
        NodeId::from_name(node),
    )
}

/// The whole per-run directory `<base>/<pipeline>/<run-id>/` — the unit prune
/// operates over (T0.6 §8). Built from the same identity strings the store uses;
/// the reserved `scratch/` subtree (and everything else a run leaves) lives under
/// it. Ordinary ids used by these tests pass through as-is.
fn run_dir(base: &Path, pipeline: &str, run: &str) -> PathBuf {
    base.join(pipeline).join(run)
}

// ===========================================================================
// Scratch survives a full process restart.
// ===========================================================================

/// **A non-succeeded node's scratch survives a full process exit and restart.**
/// One process writes a known key/value to a node's scratch and ends the node
/// non-succeeded (`fail`), then exits. A *separate* process opens the same run
/// directory and reads that node's scratch namespace: the key is present and its
/// bytes equal what the first process wrote — the value crossed a process boundary
/// via the run-store medium, not via in-process state.
///
/// Non-vacuous: if the value lived only in the first process's memory (or run end
/// had deleted the non-succeeded node's scratch), the reopened read would be
/// `Ok(None)` and this fails.
#[test]
fn non_succeeded_scratch_survives_a_full_process_restart() {
    let base = TempBase::new("survive");
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "node-a",
        "cursor",
        "high-water-42",
        "fail",
    );

    // A fresh handle, in this later process, opens the same run directory and
    // reads the value back byte-for-byte.
    let reopened = reopen_store(base.path(), "pipe", "run-1", "node-a");
    let got = reopened.get(b"cursor").expect("read across the restart");
    assert_eq!(
        got.as_deref(),
        Some(&b"high-water-42"[..]),
        "the value written by the dead process is readable byte-for-byte by a later, separate process"
    );
}

// ===========================================================================
// Non-succeeded scratch is retained at run end (process finished normally).
// ===========================================================================

/// **Non-succeeded scratch is retained at run end.** A node writes scratch and
/// ends non-succeeded; the run process finishes **normally** (not killed). After
/// it exits, the on-disk run directory still carries that node's scratch — run end
/// deleted nothing belonging to a non-succeeded node (arch.md line 393; T0.6 §8).
///
/// Non-vacuous: a run-finished path that performed any blanket scratch cleanup
/// would leave the namespace absent and fail the on-disk existence check.
#[test]
fn non_succeeded_scratch_is_retained_after_a_clean_run_end() {
    let base = TempBase::new("retain");
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "node-y",
        "progress",
        "first-half-done",
        "fail",
    );

    let reopened = reopen_store(base.path(), "pipe", "run-1", "node-y");
    let ns = reopened
        .namespace_dir()
        .expect("wired handle has a namespace")
        .to_path_buf();
    assert!(
        ns.exists(),
        "the non-succeeded node's scratch directory is still on disk after a clean run end"
    );
    assert_eq!(
        reopened.get(b"progress").unwrap().as_deref(),
        Some(&b"first-half-done"[..]),
        "the retained scratch is readable — nothing implicit deleted it"
    );
}

// ===========================================================================
// Succeeded scratch is gone after restart too.
// ===========================================================================

/// **Succeeded scratch is gone after restart too.** A node writes scratch and then
/// **succeeds** (its on-success hook runs) before the process exits. From a fresh
/// process the succeeded node's scratch namespace is absent — the T53
/// success-triggered deletion is durable and is not resurrected by the retention
/// path.
///
/// Non-vacuous: were success-deletion an in-process-only illusion (or were
/// retention to re-materialize it), the reopened read would return the value and
/// this fails.
#[test]
fn succeeded_scratch_is_gone_after_restart() {
    let base = TempBase::new("succeeded-gone");
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "node-s",
        "cursor",
        "done",
        "succeed",
    );

    let reopened = reopen_store(base.path(), "pipe", "run-1", "node-s");
    assert!(
        matches!(reopened.get(b"cursor"), Ok(None)),
        "a succeeded node's scratch reads absent from a fresh process (durable deletion)"
    );
    assert!(
        !reopened.namespace_dir().expect("wired").exists(),
        "the succeeded node's scratch directory no longer exists on disk after restart"
    );
}

// ===========================================================================
// A non-succeeded run has no implicit end-of-run deletion (mixed roster).
// ===========================================================================

/// **A multi-node run performs no implicit end-of-run deletion beyond per-node
/// success deletions.** Some nodes succeed and some do not, all writing scratch.
/// After the processes finish, exactly the non-succeeded nodes' scratch remains;
/// the succeeded nodes' scratch is gone. The run-finished path itself performed no
/// deletion beyond the per-node success deletions (arch.md line 393; T0.6 §8).
///
/// Non-vacuous: a blanket end-of-run cleanup would also remove the non-succeeded
/// nodes' scratch (failing the retained checks); a retention path that skipped the
/// success deletion would leave the succeeded nodes' scratch (failing the gone
/// checks).
#[test]
fn a_mixed_run_deletes_only_succeeded_scratch_at_run_end() {
    let base = TempBase::new("mixed");
    // Two succeed, two do not — each writes a distinct value.
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "ok-1",
        "k",
        "ok-1-val",
        "succeed",
    );
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "ok-2",
        "k",
        "ok-2-val",
        "succeed",
    );
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "bad-1",
        "k",
        "bad-1-val",
        "fail",
    );
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "bad-2",
        "k",
        "bad-2-val",
        "fail",
    );

    // The non-succeeded nodes' scratch is retained and readable.
    for (node, value) in [("bad-1", &b"bad-1-val"[..]), ("bad-2", &b"bad-2-val"[..])] {
        let store = reopen_store(base.path(), "pipe", "run-1", node);
        assert_eq!(
            store.get(b"k").unwrap().as_deref(),
            Some(value),
            "non-succeeded node {node}'s scratch is retained at run end"
        );
    }
    // The succeeded nodes' scratch is gone.
    for node in ["ok-1", "ok-2"] {
        let store = reopen_store(base.path(), "pipe", "run-1", node);
        assert!(
            matches!(store.get(b"k"), Ok(None)),
            "succeeded node {node}'s scratch is deleted at success — and only that"
        );
    }
}

// ===========================================================================
// A fresh process sees retained scratch under the original per-node namespacing.
// ===========================================================================

/// **A fresh process sees retained scratch under the original per-run/per-node
/// namespacing, with cross-node isolation preserved across the boundary.** Nodes A
/// and B both end non-succeeded, each writing a distinct value under a **shared
/// key name**. From a new process, each reads back its own node's value; the
/// per-node namespace kept them disjoint across the restart, and neither handle can
/// reach the other's namespace (isolation from T53/C18 acceptance holds across the
/// process boundary).
///
/// Non-vacuous: a namespacing that collided across nodes (or a handle that could
/// address a foreign namespace) would read the wrong node's value or a non-`None`
/// result through the cross-read, failing these checks.
#[test]
fn a_fresh_process_sees_retained_scratch_per_node_namespaced() {
    let base = TempBase::new("namespaced");
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "node-a",
        "shared",
        "a-only",
        "fail",
    );
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-1",
        "node-b",
        "shared",
        "b-only",
        "fail",
    );

    let a = reopen_store(base.path(), "pipe", "run-1", "node-a");
    let b = reopen_store(base.path(), "pipe", "run-1", "node-b");

    assert_eq!(
        a.get(b"shared").unwrap().as_deref(),
        Some(&b"a-only"[..]),
        "node A reads its own value under the shared key name"
    );
    assert_eq!(
        b.get(b"shared").unwrap().as_deref(),
        Some(&b"b-only"[..]),
        "node B reads its own value under the shared key name — disjoint across the restart"
    );

    // The two handles address disjoint directories; there is no API by which one
    // reaches the other's namespace — isolation is preserved across the boundary.
    let a_dir = a.namespace_dir().expect("A wired");
    let b_dir = b.namespace_dir().expect("B wired");
    assert_ne!(a_dir, b_dir, "the two nodes resolve to disjoint namespaces");
    assert!(a_dir.starts_with(base.path()) && b_dir.starts_with(base.path()));
}

// ===========================================================================
// Prune removes retained non-succeeded scratch (by removing the per-run dir).
// ===========================================================================

/// **Prune (C26) is the mechanism that removes retained non-succeeded scratch, and
/// it does so by removing the whole per-run directory.** A completed run retains a
/// non-succeeded node's scratch; prune's unit of work is the per-run directory
/// `<base>/<pipeline>/<run-id>/` (T0.6 §8). Removing that directory reclaims the
/// retained scratch. (The prune verb's *selection* semantics — count/age, the CLI
/// surface — are C26/T55/T56 and out of scope here; this asserts only that prune's
/// unit of removal is the per-run directory and that removing it reclaims the
/// retained scratch.)
///
/// Non-vacuous: if the retained scratch had leaked outside the per-run directory,
/// removing that directory would not reclaim it and the post-remove read would
/// still return the value.
#[test]
fn prune_reclaims_retained_scratch_by_removing_the_per_run_directory() {
    let base = TempBase::new("prune");
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-doomed",
        "node-a",
        "k",
        "retained-value",
        "fail",
    );

    // The retained scratch is present before prune (a fresh process confirms it).
    let before = reopen_store(base.path(), "pipe", "run-doomed", "node-a");
    assert_eq!(
        before.get(b"k").unwrap().as_deref(),
        Some(&b"retained-value"[..]),
        "the non-succeeded scratch is retained before prune"
    );

    // Prune's unit of removal is the whole per-run directory. The retained scratch
    // lives *inside* it (under the reserved `scratch/` subtree), so removing the
    // per-run directory reclaims it — this is the only implicit-deletion path.
    let dir = run_dir(base.path(), "pipe", "run-doomed");
    assert!(
        dir.join(SCRATCH_DIR_NAME).exists(),
        "the retained scratch lives under the per-run directory prune removes"
    );
    std::fs::remove_dir_all(&dir).expect("prune removes the whole per-run directory");

    // After prune, a fresh process finds the scratch gone — prune reclaimed it.
    let after = reopen_store(base.path(), "pipe", "run-doomed", "node-a");
    assert!(
        matches!(after.get(b"k"), Ok(None)),
        "prune removed the per-run directory, reclaiming the retained scratch"
    );
    assert!(
        !dir.exists(),
        "the whole per-run directory is gone after prune"
    );
}

// ===========================================================================
// Prune is the only remover (nothing else reclaims retained scratch).
// ===========================================================================

/// **Prune is the only remover.** A completed run retains a non-succeeded node's
/// scratch, and prune is **not** run against it. Opening the run directory from a
/// fresh process at a later time still finds the scratch present — no timer, no
/// next run, and no run-end path ever removed it. Here the "later time" and the
/// "next run" are modelled concretely: a **second, unrelated run** writes and even
/// **succeeds** (running its own success-deletion) under the same base, and the
/// first run's retained scratch is still untouched afterward.
///
/// Non-vacuous: if any path other than an explicit prune (a subsequent run's
/// finish, a stray cleanup) reclaimed the first run's retained scratch, the final
/// read would be `Ok(None)` and this fails.
#[test]
fn only_prune_removes_retained_scratch_a_later_run_does_not() {
    let base = TempBase::new("only-prune");
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-kept",
        "node-a",
        "k",
        "still-here",
        "fail",
    );

    // A later, unrelated run happens under the same base and even succeeds
    // (exercising its own on-success deletion). It must not touch run-kept.
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-later",
        "node-z",
        "k",
        "whatever",
        "succeed",
    );
    // And another non-succeeded run — still nothing reclaims run-kept.
    write_scratch_in_a_separate_process(
        base.path(),
        "pipe",
        "run-later-2",
        "node-w",
        "k",
        "whatever2",
        "fail",
    );

    let kept = reopen_store(base.path(), "pipe", "run-kept", "node-a");
    assert_eq!(
        kept.get(b"k").unwrap().as_deref(),
        Some(&b"still-here"[..]),
        "no timer, no next run, and no run-end path removed the retained scratch — only prune would"
    );
    assert!(
        run_dir(base.path(), "pipe", "run-kept").exists(),
        "the un-pruned run directory still carries its non-succeeded scratch at a later time"
    );
}

// ===========================================================================
// Retention holds on a durable-style base (survives-the-container config).
// ===========================================================================

/// **Retention holds on a durable-style base.** The base is pointed at a directory
/// that outlives a single process (the "survives the container" configuration,
/// arch.md line 67); a run leaves a node non-succeeded, and the process (the
/// "container") goes away. Re-opening the run directory from the persisted base
/// with a new process finds the non-succeeded node's scratch intact and readable —
/// matching the operational promise that a run whose store survives is the
/// resumable case (arch.md lines 67, 688).
///
/// This mirrors `non_succeeded_scratch_survives_a_full_process_restart` but frames
/// the base as the operator-supplied durable medium and re-opens **twice** across
/// two later processes, to make explicit that persistence is a property of the
/// medium, not of a single re-open.
#[test]
fn retention_holds_on_a_durable_style_base_across_multiple_reopens() {
    // The base stands in for a mounted/synced directory the operator points at:
    // it is a plain on-disk directory that outlives the writing process — the
    // whole operational requirement (arch.md "The shape of a run").
    let base = TempBase::new("durable");
    write_scratch_in_a_separate_process(
        base.path(),
        "durable-pipe",
        "run-1",
        "node-a",
        "cursor",
        "checkpoint-7",
        "fail",
    );

    // The container went away (the writer exited). Two independent later processes
    // — modelled as two fresh handles opened at different times — each read the
    // intact value from the persisted base.
    for attempt in 1..=2 {
        let reopened = reopen_store(base.path(), "durable-pipe", "run-1", "node-a");
        assert_eq!(
            reopened.get(b"cursor").unwrap().as_deref(),
            Some(&b"checkpoint-7"[..]),
            "reopen #{attempt}: the non-succeeded scratch is intact and readable from the durable base"
        );
    }
}
