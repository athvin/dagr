//! The **gate demo** — kill, resume, and review. Written first, TDD.
//!
//! This is the operability gate: the spec fixes the *"It is operable"* done
//! when *"a pipeline killed mid-run resumes and skips completed durable work, and a
//! structural change to the graph is caught in code review."* These tests prove
//! those two guarantees **end-to-end as CI tests**, composing the already-merged
//! operability machinery through the **shipped** driver — not at node grain:
//!
//! - **Kill** — the `dagr-t63-demo-run` binary drives the reference pipeline through
//!   the **real** `drive()` loop, writing a real on-disk event stream; once the
//!   durable stage boundary has succeeded (recording its reference) and the
//!   scratch-checkpointing node has written its checkpoint, it is **abruptly killed**
//!   (SIGKILL — no exit handler, the container-kill failure mode). The surviving
//!   stream folds into a complete, interrupted, resumable artifact.
//! - **Resume** — the same binary reads the killed run's folded artifact, runs the
//!   **real** `resume_verb`, carries scratch forward (the **real** `carry_forward`),
//!   and drives the **must-run subset** through the **real** `drive()` loop with the
//!   satisfied-from-prior nodes pre-seeded and the demanded durable producer
//!   **rehydrated** into its consumer's slot. Every "It is operable" done-when is
//!   asserted from what the shipped driver actually did.
//! - **Review** — the reference pipeline's canonical structure fixture (produced
//!   through the blessed `bless_structure` flow) is checked against a rewiring
//!   and a group rename, each of which fails review with a structural diff, while a
//!   clean rebuild does not and the group rename leaves the fingerprint byte-identical.
//!
//! # Not a tautology (load-bearing)
//!
//! Every asserted value is traced to the prior run through the real machinery: the
//! consumer's received value is `Blob::rehydrate(reference)` of the reference the
//! prior durable producer **serialized and the driver recorded**; the carried-forward
//! checkpoint is the byte string the prior checkpoint node **wrote through its own
//! durable store** and the real `carry_forward` copied. Never a constant next to the
//! assertion.
//!
//! # Determinism + isolation
//!
//! The kill synchronises on an **observable on-disk marker** the checkpoint node
//! writes (no wall-clock sleep), under a **private per-test** run-store base (pid + a
//! process-monotonic counter + a nanosecond stamp), removed on drop. The killed child
//! is reaped. The kill is portable (`Child::kill`); nothing here is unix-gated.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;

use dagr_artifact::event_stream::EVENTS_FILE_NAME;
use dagr_artifact::fold::fold_stream;
use dagr_cli::structure_snapshot::{assert_structure, bless_structure, StructureAssertError};
use dagr_cli::t63_demo::{assemble, base_groups, ConsumerFrom, PIPELINE, STAGE_VALUE};

/// Cargo sets `CARGO_BIN_EXE_<name>` for every bin in the package when compiling an
/// integration test — the demo binary's path, resolved at build time.
const DEMO: &str = env!("CARGO_BIN_EXE_dagr-t63-demo-run");

// ===========================================================================
// Private per-test run-store base (never shared /tmp; removed on drop).
// ===========================================================================

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
            "dagr-t63-{tag}-{}-{}-{}",
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
    fn str(&self) -> &str {
        self.path.to_str().expect("utf-8 temp path")
    }
}
impl Drop for TempBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Poll `predicate` until it holds or the deadline elapses — spinning on observable
/// state, never a fixed sleep the assertion depends on.
fn wait_until(mut predicate: impl FnMut() -> bool, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    predicate()
}

fn stream_path(base: &Path, run_id: &str) -> PathBuf {
    base.join(PIPELINE).join(run_id).join(EVENTS_FILE_NAME)
}

/// Launch the demo in `run` mode, wait for its mid-run readiness marker, then
/// **abruptly kill** it (SIGKILL, portable via `Child::kill`) and reap it. Returns
/// the surviving on-disk stream bytes (read only after the process is confirmed
/// dead, so no concurrent writer races the read).
fn launch_kill_and_read(base: &TempBase, run_id: &str) -> Vec<u8> {
    let marker = base.path().join(format!("{run_id}.ready"));
    let mut child: Child = Command::new(DEMO)
        .arg("run")
        .arg(base.str())
        .arg(run_id)
        .arg(&marker)
        .spawn()
        .expect("the demo binary launches as a separate OS process");

    let ready = wait_until(|| marker.exists(), Duration::from_secs(30));
    assert!(
        ready,
        "the demo reached its mid-run checkpoint (durable stage succeeded + scratch \
         written) and signalled readiness on disk"
    );

    child.kill().expect("the mid-run child is killed abruptly");
    let status = child.wait().expect("reap the killed child");
    assert!(
        !status.success(),
        "an abruptly-killed child does not exit successfully"
    );

    let path = stream_path(base.path(), run_id);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read surviving stream {}: {e}", path.display()))
}

/// Run the demo in `resume` mode and return the parsed result JSON it wrote.
fn resume(base: &TempBase, prior: &str, new: &str) -> Value {
    let result_path = base.path().join(format!("{new}.result.json"));
    let status = Command::new(DEMO)
        .arg("resume")
        .arg(base.str())
        .arg(prior)
        .arg(new)
        .arg(&result_path)
        .status()
        .expect("the demo binary resumes");
    assert!(status.success(), "the resume mode exits cleanly");
    let bytes = std::fs::read(&result_path).expect("read the resume result json");
    serde_json::from_slice(&bytes).expect("the resume result parses")
}

fn parse_records(stream: &[u8]) -> Vec<Value> {
    dagr_artifact::event_stream::read_records(stream)
        .expect("the surviving stream parses")
        .records
}

fn terminal_of(records: &[Value], node: &str) -> Option<String> {
    records.iter().find_map(|r| {
        (r.get("kind").and_then(Value::as_str) == Some("node-terminal")
            && r.get("node").and_then(Value::as_str) == Some(node))
        .then(|| r.get("state").and_then(Value::as_str).map(str::to_string))
        .flatten()
    })
}

// ===========================================================================
// Scenario 1 — Kill mid-run flushes a complete, resumable artifact.
// ===========================================================================

/// **A run killed mid-run leaves a complete, gapless stream that folds into an
/// interrupted, resumable artifact — with the durable stage boundary's reference
/// recorded.** The demo runs the real pipeline through the shipped `drive()` loop;
/// once `expensive` (durable) succeeded and `checkpoint` wrote its scratch, it is
/// killed with `SIGKILL`. The surviving stream is gapless, folds without error, is
/// marked interrupted, records `expensive`'s durable reference, and the run directory
/// is intact (so the run is resumable).
#[test]
fn kill_mid_run_flushes_a_complete_resumable_artifact() {
    let base = TempBase::new("kill");
    let stream = launch_kill_and_read(&base, "run-killed");
    let records = parse_records(&stream);

    // The stream is gapless (seq 0..N contiguous) — a complete write up to the kill.
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(
            rec.get("seq").and_then(Value::as_u64),
            Some(i as u64),
            "gapless sequence at record {i}"
        );
    }

    // The durable stage boundary succeeded and recorded its reference — through the
    // REAL driver's durable-reference seam.
    assert_eq!(
        terminal_of(&records, "expensive").as_deref(),
        Some("succeeded"),
        "the durable stage boundary succeeded before the kill"
    );
    let expensive_ref = records.iter().find_map(|r| {
        (r.get("kind").and_then(Value::as_str) == Some("attempt-outcome")
            && r.get("node").and_then(Value::as_str) == Some("expensive"))
        .then(|| {
            r.get("durable_reference")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .flatten()
    });
    assert_eq!(
        expensive_ref.as_deref(),
        Some("blob://STAGE-OUTPUT"),
        "the killed run recorded the durable stage boundary's reference in its attempt record"
    );

    // The checkpoint node was in-flight at the kill (ready, no terminal) — it is the
    // must-run seed on resume, and its scratch is retained on disk.
    assert!(
        terminal_of(&records, "checkpoint").is_none(),
        "the checkpoint node was still in flight at the kill (non-succeeded)"
    );
    let scratch_dir = base
        .path()
        .join(PIPELINE)
        .join("run-killed")
        .join("scratch");
    assert!(
        scratch_dir.exists(),
        "the checkpoint node's retained scratch survives the kill under the run directory"
    );

    // The surviving stream folds into an interrupted (crash-truncated) artifact —
    // resumable, with the run directory intact (it was not deleted).
    let node_roster: Vec<String> = assemble(base_groups, ConsumerFrom::Expensive)
        .nodes()
        .map(|n| n.name().to_string())
        .collect();
    let artifact = fold_stream(&stream, &node_roster).expect("the killed stream folds");
    assert!(
        artifact.is_interrupted(),
        "a crash-truncated run (no run-finished) folds to an interrupted artifact"
    );
    assert!(
        stream_path(base.path(), "run-killed").exists(),
        "the run directory is left intact in the run store (resumable)"
    );
}

// ===========================================================================
// Scenario 2 — Resume skips completed durable work, rehydrates, and carries
// scratch forward — all through the shipped driver.
// ===========================================================================

/// **Resuming the killed run through the real `resume_verb` + `drive()` skips the
/// durable stage boundary, rehydrates its value into the re-executing consumer,
/// carries the checkpoint's scratch forward, re-runs the teardown-covered producer,
/// and completes successfully — the "It is operable" done-when, end-to-end through
/// the shipped driver.**
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "this is the single end-to-end done-when assertion — one killed run resumed \
              once, then every teardown, scratch, and resume outcome checked against what the \
              shipped driver actually did; splitting it would re-run the (process-spawning) kill \
              several times and scatter the one coherent picture"
)]
fn resume_skips_durable_work_rehydrates_and_carries_scratch_forward() {
    let base = TempBase::new("resume");
    let _stream = launch_kill_and_read(&base, "run-killed");
    let result = resume(&base, "run-killed", "run-resumed");

    // The resumed run completed successfully.
    assert_eq!(
        result["overall_outcome"].as_str(),
        Some("Succeeded"),
        "the resumed run completes successfully"
    );

    let terminals = &result["terminals"];

    // The durable stage boundary is satisfied-from-prior — NOT re-executed.
    assert_eq!(
        terminals["expensive"].as_str(),
        Some("SatisfiedFromPrior"),
        "the durable stage boundary is satisfied-from-prior (skipped, not re-run)"
    );
    let satisfied: Vec<&str> = result["satisfied_from_prior"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        satisfied.contains(&"expensive"),
        "the resume plan left the durable stage boundary satisfied-from-prior"
    );

    // The demanded durable producer is rehydrated, and the re-executing
    // consumer receives the EXACT value the prior producer serialized — traced through
    // the real reference, not a constant.
    assert_eq!(
        result["rehydrate"]["expensive"].as_str(),
        Some("blob://STAGE-OUTPUT"),
        "the demanded durable producer is scheduled for rehydration from its reference"
    );
    assert_eq!(
        result["consumer_received"].as_str(),
        Some(STAGE_VALUE),
        "the re-executing consumer received the exact value the prior durable producer wrote, \
         rehydrated (not recomputed)"
    );
    assert_eq!(
        terminals["consumer"].as_str(),
        Some("Succeeded"),
        "the re-executing consumer succeeds over the rehydrated value"
    );

    // The must-run checkpoint node re-executes and observes its prior-run
    // scratch carried forward into the resumed namespace — it continued from the
    // checkpoint, not from zero.
    let must_run: Vec<&str> = result["must_run"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        must_run.contains(&"checkpoint"),
        "the non-succeeded checkpoint node is in the must-run set (re-executes)"
    );
    assert_eq!(
        terminals["checkpoint"].as_str(),
        Some("Succeeded"),
        "the checkpoint node re-executes to success on resume"
    );
    assert_eq!(
        result["carried_forward_checkpoint"].as_str(),
        Some("finished-item-K"),
        "the re-executing checkpoint node observed the exact high-water mark its prior \
         counterpart wrote — carried forward, continuing from the checkpoint"
    );

    // The teardown-covered producer is re-executed, never satisfied-from-prior.
    assert!(
        !satisfied.contains(&"inmem"),
        "the teardown-covered producer is never satisfied-from-prior"
    );
    assert_eq!(
        terminals["inmem"].as_str(),
        Some("Succeeded"),
        "the teardown-covered producer is re-executed on resume"
    );
    assert_eq!(
        terminals["teardown"].as_str(),
        Some("Succeeded"),
        "the teardown runs on resume under its fresh signal"
    );

    // The cleanup-after-publish shape resumes correctly: publish is
    // satisfied-from-prior (undemanded, ordering-only), cleanup re-runs and its rule
    // fires on the success-like satisfied upstream.
    assert!(
        satisfied.contains(&"publish"),
        "the ordering-only publish is satisfied-from-prior even though it is not durable"
    );
    assert!(must_run.contains(&"cleanup"), "the cleanup node re-runs");
    assert_eq!(
        terminals["cleanup"].as_str(),
        Some("Succeeded"),
        "cleanup's rule fires on the satisfied publish and it succeeds"
    );

    // The resumed run produces its own artifact, linked to parent + lineage
    // root, with the durable reference copied forward (self-contained).
    let artifact = &result["resumed_artifact"];
    assert_eq!(artifact["header"]["run_id"].as_str(), Some("run-resumed"));
    assert_eq!(
        artifact["header"]["resume_lineage"]["parent_run_id"].as_str(),
        Some("run-killed"),
        "the resumed artifact links its immediate parent"
    );
    assert_eq!(
        artifact["header"]["resume_lineage"]["lineage_root_run_id"].as_str(),
        Some("run-killed"),
        "and the lineage root"
    );
    let expensive_attempt = artifact["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["node"].as_str() == Some("expensive"))
        .expect("expensive recorded in the resumed artifact");
    assert_eq!(
        expensive_attempt["durable_reference"].as_str(),
        Some("blob://STAGE-OUTPUT"),
        "the durable reference is copied forward so the resumed artifact is self-contained"
    );
    assert_eq!(
        expensive_attempt["satisfied_from_run"].as_str(),
        Some("run-killed"),
        "the satisfied node carries its originating run identity"
    );
}

/// **A multi-generation resume links to its immediate parent and the original
/// lineage root.** Resuming the killed run, then resuming the resumed run, produces a
/// third-generation artifact naming run 2 as parent and run 1 as the lineage root,
/// with the durable reference copied forward.
#[test]
fn multi_generation_resume_links_parent_and_lineage_root() {
    let base = TempBase::new("lineage");
    let _stream = launch_kill_and_read(&base, "run-killed");
    // First resume: run-killed -> run-gen2 (still leaves nothing to re-run? no — the
    // resumed run succeeds fully, so a second resume of it is a no-op that is still
    // lineage-linked). We instead resume the ORIGINAL killed run twice to prove the
    // lineage-linking across two generations from the same intact store.
    let gen2 = resume(&base, "run-killed", "run-gen2");
    assert_eq!(gen2["overall_outcome"].as_str(), Some("Succeeded"));

    // Resume the gen2 run (whose artifact was written to the store): its parent is
    // run-gen2 and its lineage root stays run-killed.
    let gen3 = resume(&base, "run-gen2", "run-gen3");
    let artifact = &gen3["resumed_artifact"];
    assert_eq!(
        artifact["header"]["resume_lineage"]["parent_run_id"].as_str(),
        Some("run-gen2"),
        "the immediate parent is the prior resumed run"
    );
    assert_eq!(
        artifact["header"]["resume_lineage"]["lineage_root_run_id"].as_str(),
        Some("run-killed"),
        "the lineage root stays the original killed run across generations"
    );
}

// ===========================================================================
// Scenario 3 — Review: a structural change is caught by the structure fixture.
// ===========================================================================

/// Bless the reference pipeline's canonical structure fixture under a private path
/// (the blessed update flow), returning it.
fn bless_reference_fixture(base: &TempBase) -> PathBuf {
    let fixture = base.path().join("reference.snapshot.json");
    bless_structure(
        &assemble(base_groups, ConsumerFrom::Expensive),
        PIPELINE,
        &fixture,
    )
    .expect("bless writes the reference structure fixture");
    fixture
}

/// **A rewiring is caught by the structure fixture with a structural diff.**
/// Redirecting the `consumer`'s input from the durable `expensive` to the in-memory
/// `inmem` producer — a pure edge rewire over an unchanged node set — fails the
/// structure test with a diff naming the changed edge, while a clean rebuild does not.
#[test]
fn a_rewiring_is_caught_by_the_structure_fixture() {
    let base = TempBase::new("review-rewire");
    let fixture = bless_reference_fixture(&base);

    // A clean rebuild of the reference pipeline does NOT fail (volatile header
    // excluded, semantic comparison).
    assert_structure(
        &assemble(base_groups, ConsumerFrom::Expensive),
        PIPELINE,
        &fixture,
    )
    .expect("a clean rebuild does not fail the structure test");

    // The rewired variant (consumer demands `inmem` instead of `expensive`) fails.
    let err = assert_structure(
        &assemble(base_groups, ConsumerFrom::InMemory),
        PIPELINE,
        &fixture,
    )
    .expect_err("a rewiring must fail the structure test");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("a rewiring is a structural mismatch, not an I/O error");
    };
    let report = diff.to_string();
    assert!(
        report.contains("expensive") && report.contains("inmem") && report.contains("consumer"),
        "the diff names the rewired edge (consumer's input moved expensive -> inmem): {report}"
    );
    // A pure rewire: no node added or removed.
    assert!(
        !report.contains("added node") && !report.contains("removed node"),
        "a pure rewire reports no node add/remove: {report}"
    );
}

/// **A group rename is review-visible but does not touch the structural
/// fingerprint.** Renaming the `publish` group leaves the graph identical except the
/// group label; the structure test fails with a diff (review-visible), while the
/// structural fingerprint is byte-identical across the rename — so resume across the
/// rename would still be permitted.
#[test]
fn a_group_rename_is_review_visible_but_fingerprint_neutral() {
    let base = TempBase::new("review-group");
    let fixture = bless_reference_fixture(&base);

    // A group layout that renames `publish` -> `shipping` (label only).
    let renamed = |name: &str| -> Option<&'static str> {
        match base_groups(name) {
            Some("publish") => Some("shipping"),
            other => other,
        }
    };

    let err = assert_structure(
        &assemble(renamed, ConsumerFrom::Expensive),
        PIPELINE,
        &fixture,
    )
    .expect_err("a group rename must fail the structure test (review-visible)");
    let StructureAssertError::Mismatch(diff) = err else {
        panic!("a group rename is a structural-snapshot mismatch");
    };
    let report = diff.to_string();
    assert!(
        report.contains("group") && (report.contains("publish") || report.contains("shipping")),
        "the diff reports the group rename (publish -> shipping): {report}"
    );

    // Companion: the structural fingerprint is byte-identical across the rename —
    // the label is excluded from identity, so resume is still permitted.
    let base_fp = dagr_cli::structure_snapshot::StructureSnapshot::from_pipeline(
        &assemble(base_groups, ConsumerFrom::Expensive),
        PIPELINE,
    )
    .unwrap()
    .structural_fingerprint();
    let renamed_fp = dagr_cli::structure_snapshot::StructureSnapshot::from_pipeline(
        &assemble(renamed, ConsumerFrom::Expensive),
        PIPELINE,
    )
    .unwrap()
    .structural_fingerprint();
    assert_eq!(
        base_fp, renamed_fp,
        "a group rename leaves the structural fingerprint byte-identical (excluded from identity)"
    );
}

/// **The fixture-update flow is a single blessed command exercised on a deliberate
/// change.** Blessing the reference fixture, then a rewired variant, rewrites the
/// canonical fixture; after the deliberate re-bless the structure test passes, and a
/// second bless is idempotent (byte-identical) — proving the blessed flow, not a
/// silent auto-rewrite.
#[test]
fn the_fixture_update_flow_is_a_single_blessed_command() {
    let base = TempBase::new("review-bless");
    let fixture = bless_reference_fixture(&base);

    // A deliberate structural change (the rewired variant) no longer matches.
    assert!(
        assert_structure(
            &assemble(base_groups, ConsumerFrom::InMemory),
            PIPELINE,
            &fixture,
        )
        .is_err(),
        "the stale fixture no longer matches the deliberately-changed structure"
    );

    // Re-bless deliberately to the new structure; the assertion then passes.
    bless_structure(
        &assemble(base_groups, ConsumerFrom::InMemory),
        PIPELINE,
        &fixture,
    )
    .expect("re-bless rewrites the canonical fixture");
    let after_first = std::fs::read_to_string(&fixture).unwrap();
    assert_structure(
        &assemble(base_groups, ConsumerFrom::InMemory),
        PIPELINE,
        &fixture,
    )
    .expect("after re-blessing, the structure test passes");

    // Idempotent: a second bless is byte-identical.
    bless_structure(
        &assemble(base_groups, ConsumerFrom::InMemory),
        PIPELINE,
        &fixture,
    )
    .expect("re-bless again");
    let after_second = std::fs::read_to_string(&fixture).unwrap();
    assert_eq!(
        after_first, after_second,
        "re-running the bless command is idempotent (byte-identical output)"
    );
}
