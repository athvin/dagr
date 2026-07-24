//! C18 · Resume **scratch carry-forward** — ticket T54b (071). Written first, TDD.
//! Each test mirrors one bullet of the ticket's Test plan.
//!
//! # What this proves (vs T54a's `scratch_survives_restart.rs`)
//!
//! T54a proves the durability *half*: a non-succeeded node's scratch **survives**
//! a full process exit and is readable, byte-for-byte, from a *later, separate*
//! process opening the same prior run directory (arch.md `### C18`; "The shape of
//! a run" line 67). That is the checkpoint sitting on disk. This suite proves the
//! resume *transfer*: on resume, for exactly the nodes T58's plan marks for
//! re-execution (its `must_run` set), each node's **retained prior scratch is
//! copied forward** into the **resumed run's** per-node namespace, so the
//! re-executing node reads its checkpoint through the ordinary C18 context with no
//! awareness that a copy happened, and no path to the prior run's directory
//! (arch.md line 391; C18 acceptance "a resumed run's re-executing nodes see the
//! prior run's scratch values", line 399).
//!
//! The copy is **copy, not move**: the prior run's scratch is *retained*, not
//! consumed (T54a) — prune (C26) alone reclaims it. Only re-executing nodes are
//! copied; a `satisfied-from-prior` node never runs and never reads scratch, so
//! nothing is copied for it. Cross-node **isolation** survives the copy: each
//! node's carried-forward scratch lands only in its own resumed namespace, keyed
//! by that node's own identity fingerprint — one node can never receive another's.
//!
//! # Determinism + isolation (no wall-clock sleeps; private per-test temp)
//!
//! Carry-forward is a pure, single-process filesystem copy — no sleep, no clock.
//! Every test uses a **private per-test temp base** under the OS temp dir keyed by
//! pid + a monotonic counter + a nanosecond stamp, so parallel test threads (and
//! repeated suite runs) never share — or delete — the same subtree (the shared-
//! `/tmp` parallelism bug class that has red-flaked this repo's CI).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_core::context::{PipelineId, RunId};
use dagr_core::handle::NodeId;
use dagr_core::scratch::ScratchStore;

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
            "dagr-t54b-{tag}-{}-{}-{}",
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

/// Resolve a node's scratch store under one run — the ordinary production path a
/// live run wires (`<base>/<pipeline>/<run-id>/scratch/<node>/`). Used both to
/// seed the prior run's retained scratch and to read a resumed node's scratch back
/// through the very same context API a task would.
fn store_for(base: &Path, pipeline: &str, run: &str, node: &str) -> ScratchStore {
    ScratchStore::for_node(
        base,
        &PipelineId::new(pipeline),
        &RunId::new(run),
        NodeId::from_name(node),
    )
}

/// Seed a node's **retained prior-run scratch** by writing a key/value directly
/// through its prior-run store handle (a non-succeeded node's scratch is retained
/// on disk, T54a — the write leaves it there, no success-hook deletion).
fn seed_prior_scratch(base: &Path, pipeline: &str, prior_run: &str, node: &str, key: &[u8], value: &[u8]) {
    let prior = store_for(base, pipeline, prior_run, node);
    prior.put(key, value).expect("seed prior retained scratch");
}

/// Carry one re-executing node's retained prior scratch forward into the resumed
/// run's namespace — the operation under test (T54b). A thin wrapper over the new
/// [`ScratchStore::carry_forward`] API so each test reads at the resume level.
fn carry_forward(base: &Path, pipeline: &str, prior_run: &str, resumed_run: &str, node: &str) -> Result<(), dagr_core::scratch::ScratchError> {
    ScratchStore::carry_forward(
        base,
        &PipelineId::new(pipeline),
        &RunId::new(prior_run),
        &RunId::new(resumed_run),
        NodeId::from_name(node),
    )
}

const PIPE: &str = "pipe";
const PRIOR: &str = "run-prior";
const RESUMED: &str = "run-resumed";

// ===========================================================================
// A re-executing node sees the prior run's scratch value.
// ===========================================================================

/// **A re-executing node reads back the exact bytes its counterpart wrote in the
/// linked prior run.** The prior run left a node non-succeeded with a retained
/// key/value; resume carries that node's scratch forward; the resumed node reads
/// the key through the ordinary context API and gets the exact prior bytes — the
/// checkpoint crossed from the prior namespace into the resumed one (arch.md line
/// 391, 399).
///
/// Non-vacuous: without the copy, the resumed node's namespace is empty and the
/// read is `Ok(None)`.
#[test]
fn a_re_executing_node_sees_the_prior_runs_scratch_value() {
    let base = TempBase::new("sees-prior");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"cursor", b"high-water-42");

    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-a").expect("carry forward node-a");

    let resumed = store_for(base.path(), PIPE, RESUMED, "node-a");
    assert_eq!(
        resumed.get(b"cursor").expect("read resumed scratch").as_deref(),
        Some(&b"high-water-42"[..]),
        "the re-executing node reads the exact prior-run bytes through the ordinary C18 context"
    );
}

// ===========================================================================
// The value arrives in the resumed run's OWN namespace, not the prior one.
// ===========================================================================

/// **The carried-forward value lands under the resumed run id's per-node
/// namespace — the resumed node has no path to the prior run's directory.** After
/// the copy, the resumed run directory's scratch for that node holds the key; the
/// value was read through the resumed handle, whose namespace is under the resumed
/// run id, never the prior one (arch.md T53/T54a per-run/per-node namespacing).
///
/// Non-vacuous: a copy that wrote into the prior namespace (or read through the
/// prior handle) would leave the resumed namespace empty here.
#[test]
fn the_value_arrives_in_the_resumed_namespace_not_the_prior_one() {
    let base = TempBase::new("resumed-ns");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"k", b"v");

    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-a").expect("carry forward");

    let resumed = store_for(base.path(), PIPE, RESUMED, "node-a");
    let resumed_ns = resumed.namespace_dir().expect("resumed handle is wired").to_path_buf();
    // The resumed namespace is under the RESUMED run id, and the file is there.
    assert!(
        resumed_ns.starts_with(base.path().join(PIPE).join(RESUMED)),
        "the carried value lives under the resumed run id's directory, not the prior run's"
    );
    assert!(
        !resumed_ns.starts_with(base.path().join(PIPE).join(PRIOR)),
        "the resumed namespace is not the prior run's namespace"
    );
    assert_eq!(
        resumed.get(b"k").unwrap().as_deref(),
        Some(&b"v"[..]),
        "the resumed node reads it through its own namespace"
    );
}

// ===========================================================================
// A continued node resumes from its checkpoint, not from zero.
// ===========================================================================

/// **A continued node resumes from its high-water mark, not from the beginning.**
/// The prior run recorded a high-water mark ("finished item K"); after carry-
/// forward the re-executing node reads that mark and would do only the remaining
/// work — it did not restart from zero, which is the point of checkpoints (arch.md
/// line 391).
///
/// Non-vacuous: without carry-forward the mark is absent and the node would start
/// over from the beginning.
#[test]
fn a_continued_node_resumes_from_its_checkpoint_not_from_zero() {
    let base = TempBase::new("continue");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "worker", b"high-water", b"finished-item-K");

    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "worker").expect("carry forward worker");

    let resumed = store_for(base.path(), PIPE, RESUMED, "worker");
    let mark = resumed.get(b"high-water").unwrap();
    assert_eq!(
        mark.as_deref(),
        Some(&b"finished-item-K"[..]),
        "the re-executing worker observes the prior high-water mark and continues, not restarts"
    );
}

// ===========================================================================
// A satisfied-from-prior node has nothing carried forward.
// ===========================================================================

/// **A `satisfied-from-prior` node has no scratch copied forward.** The copy set
/// is the re-execution set only: a node T58 leaves outside `must_run` is never
/// carried forward, so its resumed scratch namespace is empty — it never runs and
/// never reads scratch (arch.md lines 35, 391). This is modelled by simply **not**
/// calling carry-forward for that node (the driver only carries the `must_run`
/// set), even though it *had* retained prior scratch.
///
/// Non-vacuous: if carry-forward were driven over all nodes rather than the
/// must-run set, the satisfied node's namespace would be non-empty here.
#[test]
fn a_satisfied_from_prior_node_has_nothing_carried_forward() {
    let base = TempBase::new("satisfied");
    // The satisfied node DID leave retained prior scratch, but it is not in the
    // re-execution set, so the driver does not carry it forward.
    seed_prior_scratch(base.path(), PIPE, PRIOR, "satisfied-node", b"k", b"leftover");

    // Only a re-executing node is carried; the satisfied node is deliberately not.
    seed_prior_scratch(base.path(), PIPE, PRIOR, "reexec-node", b"k", b"carried");
    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "reexec-node").expect("carry the re-exec node");

    let satisfied = store_for(base.path(), PIPE, RESUMED, "satisfied-node");
    assert!(
        matches!(satisfied.get(b"k"), Ok(None)),
        "the satisfied-from-prior node's resumed namespace is empty — nothing was carried for it"
    );
    assert!(
        !satisfied.namespace_dir().expect("wired").exists(),
        "no resumed namespace directory was even created for the satisfied node"
    );
}

// ===========================================================================
// A re-executing node with no prior scratch starts empty (not an error).
// ===========================================================================

/// **A re-executing node whose prior scratch is absent resumes with an empty
/// namespace — absence is not an error.** The node ended non-succeeded but never
/// wrote any retained scratch; carry-forward is a clean empty carry (`Ok`), and
/// the resumed node reads an empty namespace and proceeds (arch.md: missing prior
/// scratch is a clean start, not a resume failure).
///
/// Non-vacuous: if carry-forward treated a missing prior namespace as an error,
/// this would return `Err` and fail.
#[test]
fn a_re_executing_node_with_no_prior_scratch_starts_empty() {
    let base = TempBase::new("empty-prior");
    // No prior scratch is seeded for this node at all.
    let result = carry_forward(base.path(), PIPE, PRIOR, RESUMED, "never-wrote");
    assert!(
        result.is_ok(),
        "carrying forward a node with no retained prior scratch is a clean empty carry, not an error"
    );

    let resumed = store_for(base.path(), PIPE, RESUMED, "never-wrote");
    assert!(
        matches!(resumed.get(b"anything"), Ok(None)),
        "the re-executing node with no prior scratch sees an empty namespace and proceeds"
    );
}

// ===========================================================================
// Cross-node isolation survives the copy.
// ===========================================================================

/// **Cross-node isolation survives the copy.** Two re-executing nodes A and B each
/// wrote a distinct value under the **same key name** in the prior run. After
/// carrying both forward, A reads A's value and B reads B's; neither can read the
/// other's — the per-run/per-node namespacing kept the two carried-forward sets
/// disjoint (C18 isolation criterion, arch.md line 399).
///
/// Non-vacuous: a copy that mixed the two nodes' namespaces (or keyed by anything
/// other than each node's own identity) would let one read the other's value.
#[test]
fn cross_node_isolation_survives_the_copy() {
    let base = TempBase::new("isolation");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"shared", b"a-only");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-b", b"shared", b"b-only");

    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-a").expect("carry a");
    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-b").expect("carry b");

    let a = store_for(base.path(), PIPE, RESUMED, "node-a");
    let b = store_for(base.path(), PIPE, RESUMED, "node-b");
    assert_eq!(
        a.get(b"shared").unwrap().as_deref(),
        Some(&b"a-only"[..]),
        "node A reads its own carried-forward value under the shared key name"
    );
    assert_eq!(
        b.get(b"shared").unwrap().as_deref(),
        Some(&b"b-only"[..]),
        "node B reads its own carried-forward value — disjoint from A's"
    );
    // The two resumed namespaces are distinct directories; there is no handle by
    // which one reaches the other's carried-forward scratch.
    let a_dir = a.namespace_dir().expect("A wired");
    let b_dir = b.namespace_dir().expect("B wired");
    assert_ne!(a_dir, b_dir, "the two nodes resolve to disjoint resumed namespaces");
}

// ===========================================================================
// The copy is COPY, not MOVE: the prior run's retained scratch survives.
// ===========================================================================

/// **Carry-forward is a copy, not a move: the prior run's retained scratch is
/// still there afterward.** The prior scratch is retained (T54a) and reclaimed
/// only by prune (C26) — carry-forward must not consume it. After the copy, both
/// the prior namespace and the resumed namespace hold the value.
///
/// Non-vacuous: a move (or a copy that deleted the source) would leave the prior
/// namespace empty and fail the prior-side read.
#[test]
fn carry_forward_is_a_copy_the_prior_scratch_is_retained() {
    let base = TempBase::new("copy-not-move");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"cursor", b"keep-me");

    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-a").expect("carry forward");

    // The PRIOR run's scratch is still present (retained, not consumed).
    let prior = store_for(base.path(), PIPE, PRIOR, "node-a");
    assert_eq!(
        prior.get(b"cursor").unwrap().as_deref(),
        Some(&b"keep-me"[..]),
        "the prior run's retained scratch survives the copy — it is copied forward, not moved"
    );
    // And the RESUMED run's scratch has its own copy.
    let resumed = store_for(base.path(), PIPE, RESUMED, "node-a");
    assert_eq!(
        resumed.get(b"cursor").unwrap().as_deref(),
        Some(&b"keep-me"[..]),
        "the resumed run has its own copy of the value"
    );
}

// ===========================================================================
// Only re-executing nodes are copied (the copy set = re-exec ∩ had-scratch).
// ===========================================================================

/// **Only the re-executing nodes that had retained prior scratch are carried
/// forward.** A mixed roster: two re-executing nodes (one wrote scratch, one did
/// not) and one satisfied node (wrote scratch but is not re-executed). Driving
/// carry-forward over the re-execution set only, exactly the re-executing node
/// that had scratch ends up with carried-forward scratch; the satisfied node has
/// none; the re-executing node that wrote nothing has an empty namespace (arch.md
/// line 391).
#[test]
fn only_re_executing_nodes_with_prior_scratch_are_copied() {
    let base = TempBase::new("only-reexec");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "reexec-wrote", b"k", b"carried");
    // reexec-empty ended non-succeeded but wrote no scratch.
    seed_prior_scratch(base.path(), PIPE, PRIOR, "satisfied-wrote", b"k", b"leftover");

    // The driver carries only the re-execution set: {reexec-wrote, reexec-empty}.
    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "reexec-wrote").expect("carry reexec-wrote");
    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "reexec-empty").expect("carry reexec-empty");

    // Re-executing node that had scratch: carried.
    let wrote = store_for(base.path(), PIPE, RESUMED, "reexec-wrote");
    assert_eq!(wrote.get(b"k").unwrap().as_deref(), Some(&b"carried"[..]));
    // Re-executing node that had none: empty, clean.
    let empty = store_for(base.path(), PIPE, RESUMED, "reexec-empty");
    assert!(matches!(empty.get(b"k"), Ok(None)));
    // Satisfied node (not carried): nothing.
    let satisfied = store_for(base.path(), PIPE, RESUMED, "satisfied-wrote");
    assert!(matches!(satisfied.get(b"k"), Ok(None)));
}

// ===========================================================================
// Multiple keys are all carried forward for one node.
// ===========================================================================

/// **Every key a node retained is carried forward, not just one.** A node's prior
/// scratch namespace holds several keys; carry-forward copies the whole namespace,
/// so the resumed node reads every one back.
///
/// Non-vacuous: a carry that copied only a single key would leave the others
/// absent in the resumed namespace.
#[test]
fn all_of_a_nodes_retained_keys_are_carried_forward() {
    let base = TempBase::new("multi-key");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"cursor", b"42");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"stage", b"reduce");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"", b"empty-key-value");

    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-a").expect("carry forward");

    let resumed = store_for(base.path(), PIPE, RESUMED, "node-a");
    assert_eq!(resumed.get(b"cursor").unwrap().as_deref(), Some(&b"42"[..]));
    assert_eq!(resumed.get(b"stage").unwrap().as_deref(), Some(&b"reduce"[..]));
    assert_eq!(
        resumed.get(b"").unwrap().as_deref(),
        Some(&b"empty-key-value"[..]),
        "even the empty-key value is carried forward"
    );
}

// ===========================================================================
// Carry-forward I/O failure is retry-eligible, attributed to the node.
// ===========================================================================

/// **A carry-forward write failure is a retry-eligible task failure attributed to
/// the affected node — not a silent skip, not a whole-resume abort.** The resumed
/// run's scratch destination for one node is made unwritable (a plain file sits
/// where its namespace directory must be created), so the copy's write fails. The
/// carry surfaces a [`ScratchError`] (which converts to a retry-eligible
/// [`TaskError`], C4 / arch.md line 393); it does not silently succeed, and it
/// leaves other nodes' carries unaffected.
///
/// Non-vacuous: a carry-forward that swallowed the error (silent skip) would return
/// `Ok`, and this fails.
#[test]
fn carry_forward_io_failure_is_retry_eligible_against_the_node() {
    let base = TempBase::new("io-fail");
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-a", b"cursor", b"v");

    // Block the resumed node's scratch destination: create a plain FILE exactly
    // where its namespace DIRECTORY must be created, so the copy's create/write
    // fails at the store level. Build the destination namespace path via a store
    // handle (its own dir), then plant a file at it.
    let dest = store_for(base.path(), PIPE, RESUMED, "node-a");
    let dest_ns = dest.namespace_dir().expect("wired").to_path_buf();
    std::fs::create_dir_all(dest_ns.parent().expect("scratch parent")).expect("mk scratch dir");
    std::fs::write(&dest_ns, b"i am a file, not a directory").expect("plant blocking file");

    let result = carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-a");
    assert!(
        result.is_err(),
        "a carry-forward write failure surfaces as an error, not a silent skip"
    );
    // It converts to a retry-eligible task failure attributed to the node (C4).
    let task_err: dagr_core::error::TaskError = result.unwrap_err().into();
    assert!(
        task_err.is_retryable(),
        "a scratch carry-forward I/O failure is classified retry-eligible (arch.md line 393)"
    );

    // Another node's carry is unaffected by that node's isolated failure.
    seed_prior_scratch(base.path(), PIPE, PRIOR, "node-b", b"k", b"ok");
    carry_forward(base.path(), PIPE, PRIOR, RESUMED, "node-b")
        .expect("an unrelated node's carry-forward is unaffected by node-a's isolated failure");
    let b = store_for(base.path(), PIPE, RESUMED, "node-b");
    assert_eq!(b.get(b"k").unwrap().as_deref(), Some(&b"ok"[..]));
}
