//! **M3 gate demo — explain a run from artifacts** — ticket T49 (061). Written
//! first, TDD. **This is the M3 done-when, executed in CI.**
//!
//! arch.md's **Build order** states M3 is *done when … a run produces both
//! artifacts, the rendered diagram is reviewable, and "which node was slowest, and
//! was it waiting or working?" is answerable from the artifacts without reading a
//! single log line.* This file is that proof: it runs the **real** M3 producers
//! (via the checked-in `dagr-m3-demo-run` reference-pipeline harness), then a
//! programmatic **explainer** reads **only the emitted artifacts** — the graph
//! artifact (C20), the folded run artifact (C22), and the run-overlaid diagram
//! (C24) — and answers the M3 question mechanically, with **zero** reads of the
//! event-log/stdout stream and **zero** access to the producing binary.
//!
//! # This demo composes merged components — it adds no capability
//!
//! This is a **feature (demo)** ticket. It drives already-merged M3 components and
//! adds **zero** engine capability, **no** CLI verb (that is T55), and reaches into
//! **no** T64/T65 scope:
//!
//! - **T40 emit / T41 fingerprints** — the real graph artifact and its computed
//!   C21 structural fingerprint (`dagr_cli::graph::emit_graph`).
//! - **T42 fold / T43 summary+critical-path** — the real fold
//!   (`dagr_artifact::fold::fold_stream`) over the real on-disk event stream, and
//!   the summary's total-elapsed vs critical-path numbers.
//! - **T44 node metrics** — the real C23 metrics facility (`dagr_core::metrics`),
//!   whose task + framework measurements reach the run artifact unmodified.
//! - **T47 overlay** — the real run overlay
//!   (`dagr_render::overlay::{render_dot_overlay, render_mermaid_overlay}`).
//! - **T48 validation** — the real published-schema validator
//!   (`dagr_artifact::schema`), gated behind `schema-validation` like T40's suite.
//! - **T68 crashed-run finalize** — reused unchanged; this demo asserts the
//!   full-run **happy path only** (the crash variant is T68's).
//!
//! # Real artifacts, explained from artifacts alone
//!
//! The reference pipeline is run by a **separate producer** — the real T40 emitter
//! and the real merged C19 [`EventStreamWriter`] leave `graph.json` and
//! `events.jsonl` on disk. Every explainer assertion below reads **only those two
//! files** (and diagrams derived purely from them); it never touches the live run,
//! the producing binary, or — for the "waiting vs working / slowest" answers — the
//! event-log stream. The `critical_path_ns` summary number is consumed as an
//! **upper bound**, never an exact value, per `docs/adr/0001-critical-path-definition.md`.
//!
//! # Determinism
//!
//! The producer stamps a hand-stepped monotonic clock, so every phase duration,
//! total elapsed, critical path, and terminal state is a fixed number; two runs
//! leave identical verdicts and identical fingerprints (generation time aside), so
//! the demo can gate the milestone without flaking.
//!
//! # Scope (T49 — integration demo only)
//!
//! Adds no framework surface. It does not implement the run/fold CLI verbs (T55),
//! the crashed/replay artifact variants beyond the happy path (T68/M4), the
//! cookbook prose (T64), or the system acceptance gate (T65). It composes merged
//! components over local artifact bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::EVENTS_FILE_NAME;
use dagr_artifact::fold::{
    fold_stream, RunArtifact, PHASE_EXECUTING, PHASE_PERMIT_WAIT, PHASE_READY_WAIT,
};
use serde_json::Value;

/// The checked-in reference-pipeline producer (a real run leaving real artifacts).
/// Cargo sets `CARGO_BIN_EXE_<name>` for every bin in the package when compiling
/// this integration test, so the path is resolved at build time.
const PRODUCER: &str = env!("CARGO_BIN_EXE_dagr-m3-demo-run");

/// The producer's fixed pipeline identity and reserved artifact file names.
const PIPELINE: &str = "m3-demo-pipeline";
const GRAPH_FILE_NAME: &str = "graph.json";

/// The declared-allowlist and the deliberately-not-allowlisted sentinel env names.
const ALLOWLISTED_ENV: &str = "DAGR_REGION";
const SENTINEL_ENV: &str = "DAGR_M3_DEMO_SENTINEL";
/// The sentinel value the demo plants — it must appear NOWHERE in the artifacts.
const SENTINEL_VALUE: &str = "SECRET-NOT-ALLOWLISTED-9f2c";

/// The reference pipeline's full node roster (the C20 node set), used for the
/// fold's node-coverage roster and coverage assertions.
const GRAPH_NODES: [&str; 7] = [
    "load",
    "transform",
    "publish",
    "slow-compute",
    "queue-limited",
    "decide-skip",
    "skipped-consumer",
];

/// The node the reference pipeline is designed to make the unambiguous slowest,
/// compute-bound (working) bottleneck.
const DESIGNED_BOTTLENECK: &str = "slow-compute";
/// The node the reference pipeline is designed to make queue/permit-limited
/// (waiting).
const DESIGNED_WAITER: &str = "queue-limited";
/// The never-ran node carrying a propagated terminal state (node coverage).
const NEVER_RAN: &str = "skipped-consumer";

/// A per-invocation collision-proof run-store base under the OS temp dir, so
/// parallel test binaries never share — or delete — the same subtree.
fn temp_base() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("dagr-t49-{}-{stamp}-{unique}", std::process::id()))
}

/// The on-disk artifact paths a producer run leaves under
/// `<base>/m3-demo-pipeline/<run-id>/`.
struct Artifacts {
    base: PathBuf,
    run_dir: PathBuf,
}

impl Artifacts {
    fn graph_path(&self) -> PathBuf {
        self.run_dir.join(GRAPH_FILE_NAME)
    }
    fn stream_path(&self) -> PathBuf {
        self.run_dir.join(EVENTS_FILE_NAME)
    }
    /// Read the graph artifact JSON bytes (C20).
    fn graph_bytes(&self) -> Vec<u8> {
        std::fs::read(self.graph_path()).expect("graph artifact exists")
    }
    /// Read the event-stream bytes (C19) — used ONLY to fold into the run artifact
    /// (and, in the no-log-line test, deliberately made inaccessible).
    fn stream_bytes(&self) -> Vec<u8> {
        std::fs::read(self.stream_path()).expect("event stream exists")
    }
}

/// Run the reference-pipeline producer once, planting the not-allowlisted sentinel
/// and the allowlisted region in its environment, and return the artifact paths it
/// left on disk. The producer is a **separate OS process**, so the demo that reads
/// the artifacts afterward has no access to the producer's live state.
fn produce(run_id: &str) -> Artifacts {
    let base = temp_base();
    let status = Command::new(PRODUCER)
        .arg(base.as_os_str())
        .arg(run_id)
        // Plant a sentinel NOT on the allowlist (must never reach the artifact) and
        // the allowlisted region (which the header captures).
        .env(SENTINEL_ENV, SENTINEL_VALUE)
        .env(ALLOWLISTED_ENV, "us-east-1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("the m3-demo producer launches as a separate OS process");
    assert!(status.success(), "the producer run completed successfully");
    let run_dir = base.join(PIPELINE).join(run_id);
    Artifacts { base, run_dir }
}

/// Fold the on-disk event stream into a run artifact (C22) — the ONLY use of the
/// stream, exactly as a later invocation folds it. Everything the explainer
/// answers is read off the returned artifact, never the stream.
fn fold_artifacts(a: &Artifacts) -> RunArtifact {
    fold_stream(&a.stream_bytes(), &roster()).expect("the on-disk stream folds into a run artifact")
}

fn roster() -> Vec<String> {
    GRAPH_NODES.iter().map(|s| (*s).to_string()).collect()
}

// ===========================================================================
// The programmatic explainer — reads ONLY the run artifact (no stream, no binary)
// ===========================================================================

/// The explainer's mechanical read of one node's attempt phases: its total elapsed
/// and the split into waiting (ready-wait + permit-wait) vs working (executing),
/// summed across the node's attempts. A pure function of the run artifact's
/// per-attempt monotonic-offset phase durations — no stream, no clock, no binary.
struct NodeProfile {
    node: String,
    total_ns: u64,
    waiting_ns: u64,
    working_ns: u64,
}

impl NodeProfile {
    /// The classification the M3 question asks for: dominated by working
    /// (executing) or by waiting (ready-wait + permit-wait). A mechanical
    /// comparison, not a heuristic.
    fn verdict(&self) -> &'static str {
        if self.working_ns >= self.waiting_ns {
            "working"
        } else {
            "waiting"
        }
    }
}

/// Rank every node that ran by total attempt-elapsed time, descending, reading ONLY
/// the run artifact (C22, system criterion 5). Never-ran nodes (zero total) sort
/// last; ties break by node name for determinism.
fn rank_by_total(run: &RunArtifact) -> Vec<NodeProfile> {
    let mut by_node: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
    for att in run.attempts() {
        let phases = att.phase_durations_ns();
        let executing = phases.get(PHASE_EXECUTING).copied().unwrap_or(0);
        let waiting = phases.get(PHASE_READY_WAIT).copied().unwrap_or(0)
            + phases.get(PHASE_PERMIT_WAIT).copied().unwrap_or(0);
        let total = att.total_elapsed_ns();
        let entry = by_node.entry(att.node().to_string()).or_insert((0, 0, 0));
        entry.0 += total;
        entry.1 += waiting;
        entry.2 += executing;
    }
    let mut profiles: Vec<NodeProfile> = by_node
        .into_iter()
        .map(|(node, (total, waiting, working))| NodeProfile {
            node,
            total_ns: total,
            waiting_ns: waiting,
            working_ns: working,
        })
        .collect();
    // Descending by total; ties by name (ascending) for a deterministic order.
    profiles.sort_by(|a, b| b.total_ns.cmp(&a.total_ns).then(a.node.cmp(&b.node)));
    profiles
}

/// The slowest node by attempt-elapsed time, read from the run artifact alone.
fn slowest_node(run: &RunArtifact) -> NodeProfile {
    rank_by_total(run)
        .into_iter()
        .next()
        .expect("the run artifact has at least one attempt to rank")
}

/// One node's profile, by name, from the run artifact alone.
fn profile_of(run: &RunArtifact, node: &str) -> NodeProfile {
    rank_by_total(run)
        .into_iter()
        .find(|p| p.node == node)
        .unwrap_or_else(|| panic!("node {node} has a profile in the run artifact"))
}

// ===========================================================================
// Scenario 1 — both artifacts are produced by one run
// ===========================================================================

/// **Both artifacts are produced by one run.** The producer emits a graph artifact
/// (C20) and, from a real event-stream run, a folded run artifact (C22) in the
/// temp run store; both parse, and the run outcome is the successful full-run
/// outcome (skips-among-successes is still a successful run).
#[test]
fn both_artifacts_are_produced_by_one_run() {
    let a = produce("run-both-artifacts");
    assert!(
        a.graph_path().is_file(),
        "graph artifact written to the run store"
    );
    assert!(
        a.stream_path().is_file(),
        "event stream written to the run store"
    );

    let graph: Value = serde_json::from_slice(&a.graph_bytes()).expect("graph artifact parses");
    assert_eq!(graph["header"]["pipeline"].as_str(), Some(PIPELINE));

    let run = fold_artifacts(&a);
    assert_eq!(
        run.overall_outcome(),
        "succeeded",
        "the full-run happy path completes successfully (skips are success-like)"
    );
    assert!(!run.is_interrupted(), "the full run is not interrupted");

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 2 — artifacts are joinable (matching structural fingerprint)
// ===========================================================================

/// **Artifacts are joinable.** The run artifact's structural fingerprint equals the
/// graph artifact's fingerprint from the same build (C21/C22 fingerprint-match).
#[test]
fn artifacts_join_on_the_same_structural_fingerprint() {
    let a = produce("run-join");
    let graph: Value = serde_json::from_slice(&a.graph_bytes()).unwrap();
    let graph_fp = graph["header"]["fingerprint_structural"]
        .as_str()
        .expect("graph carries a structural fingerprint");

    let run = fold_artifacts(&a);
    let run_fp = run
        .header_fingerprint_structural()
        .expect("run artifact carries a structural fingerprint");
    assert_eq!(
        run_fp, graph_fp,
        "the run artifact's structural fingerprint equals the graph artifact's (C22 join)"
    );

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 3 — node coverage (including never-ran propagated nodes)
// ===========================================================================

/// **Node coverage.** Every node in the graph artifact appears at least once in the
/// run artifact, including the never-ran node carrying its propagated terminal
/// state (C22 node-coverage criterion).
#[test]
fn every_graph_node_is_covered_by_the_run_artifact() {
    let a = produce("run-coverage");
    let graph: Value = serde_json::from_slice(&a.graph_bytes()).unwrap();
    let graph_names: BTreeSet<String> = graph["nodes"]
        .as_array()
        .expect("graph has nodes")
        .iter()
        .map(|n| n["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        graph_names,
        GRAPH_NODES
            .iter()
            .map(|s| (*s).to_string())
            .collect::<BTreeSet<_>>(),
        "the graph artifact carries exactly the reference roster"
    );

    let run = fold_artifacts(&a);
    let covered: BTreeSet<String> = run
        .attempts()
        .iter()
        .map(|at| at.node().to_string())
        .collect();
    for node in &graph_names {
        assert!(
            covered.contains(node),
            "graph node `{node}` appears in the run artifact"
        );
    }

    // The never-ran node carries a propagated terminal state (upstream-skipped),
    // distinct from an originated skip (`decide-skip` is `skipped`).
    let never = run
        .attempts()
        .iter()
        .find(|at| at.node() == NEVER_RAN)
        .expect("the never-ran node is covered");
    assert_eq!(
        never.status(),
        "upstream-skipped",
        "the never-ran node carries the PROPAGATED skip state"
    );
    let originator = run
        .attempts()
        .iter()
        .find(|at| at.node() == "decide-skip")
        .expect("the skip originator is covered");
    assert_eq!(
        originator.status(),
        "skipped",
        "the originator carries the ORIGINATED skip state (distinct from propagated)"
    );

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 4 — phase durations sum exactly to the attempt total
// ===========================================================================

/// **Phase durations sum exactly.** For every attempt record, the named phase
/// durations sum bit-exactly to the attempt total (both derive from monotonic
/// offsets) — no floating-point slack (C22).
#[test]
fn phase_durations_sum_exactly_to_the_attempt_total() {
    let a = produce("run-phases");
    let run = fold_artifacts(&a);
    assert!(!run.attempts().is_empty(), "there are attempts to check");
    for att in run.attempts() {
        let sum: u64 = att.phase_durations_ns().values().copied().sum();
        assert_eq!(
            sum,
            att.total_elapsed_ns(),
            "node `{}` attempt {} phases sum exactly to the attempt total",
            att.node(),
            att.attempt_number()
        );
    }
    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 5 — slowest node identifiable from artifacts alone
// ===========================================================================

/// **Slowest node is identifiable from artifacts alone.** The explainer ranks
/// attempts by total elapsed and names the designed bottleneck — reading ONLY the
/// run artifact, with the event stream not consulted (C22, system criterion 5).
#[test]
fn slowest_node_is_identifiable_from_the_run_artifact_alone() {
    let a = produce("run-slowest");
    let run = fold_artifacts(&a);
    let slowest = slowest_node(&run);
    assert_eq!(
        slowest.node, DESIGNED_BOTTLENECK,
        "the explainer names the designed bottleneck as slowest (by attempt elapsed)"
    );
    // Non-vacuity: the bottleneck is genuinely slower than the next-slowest node.
    let ranked = rank_by_total(&run);
    assert!(
        ranked.len() >= 2 && ranked[0].total_ns > ranked[1].total_ns,
        "the bottleneck's total strictly exceeds the runner-up's (unambiguous)"
    );
    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 6 — "waiting or working" answerable from artifacts alone
// ===========================================================================

/// **"Waiting or working" is answerable from artifacts alone.** For the slowest
/// node the explainer compares waiting (ready-wait + permit-wait) vs working
/// (executing) and classifies the compute-bound bottleneck as "working"; for the
/// queue/permit-limited node it classifies "waiting" — each a mechanical,
/// reproducible verdict, read only from the run artifact's phase durations (C22).
#[test]
fn waiting_vs_working_is_answerable_from_the_run_artifact_alone() {
    let a = produce("run-waiting-working");
    let run = fold_artifacts(&a);

    let bottleneck = profile_of(&run, DESIGNED_BOTTLENECK);
    assert_eq!(
        bottleneck.verdict(),
        "working",
        "the compute-bound bottleneck's executing dominates its waiting (working); \
         working={} waiting={}",
        bottleneck.working_ns,
        bottleneck.waiting_ns
    );

    let waiter = profile_of(&run, DESIGNED_WAITER);
    assert_eq!(
        waiter.verdict(),
        "waiting",
        "the queue/permit-limited node's waiting dominates its executing (waiting); \
         working={} waiting={}",
        waiter.working_ns,
        waiter.waiting_ns
    );
    // Non-vacuity: the two verdicts are genuinely opposite, not both defaulting.
    assert_ne!(bottleneck.verdict(), waiter.verdict());

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 7 — structure-limited vs resource-limited at the summary
// ===========================================================================

/// **Structure-limited vs resource-limited is distinguishable at the summary.** The
/// demo compares the summary's total-elapsed against its critical-path time,
/// consuming `critical_path_ns` strictly as an UPPER BOUND (per the T43 ADR). For
/// this pipeline — parallel independent branches whose queue-limited node inflates
/// total elapsed well above the true dependency chain — total elapsed exceeds the
/// (over-attributing) critical-path upper bound, which reads as resource-limited
/// (C22, T43, system criterion 5).
#[test]
fn structure_vs_resource_limited_distinguishable_at_the_summary() {
    let a = produce("run-summary");
    let run = fold_artifacts(&a);
    let total = run.summary_total_elapsed_ns();
    let critical_upper_bound = run.summary_critical_path_ns();

    assert!(total > 0, "the run has a positive total elapsed");
    assert!(
        critical_upper_bound > 0,
        "the run has a positive critical-path bound"
    );

    // `critical_path_ns` is an UPPER BOUND on the true dependency chain (T43 ADR:
    // it can only over-attribute). So the true critical path is ≤ this number, and
    // total elapsed exceeding even the OVER-attributing bound is a sound, robust
    // "resource-limited" signal: idle/queue time pushed the wall past the longest
    // dependency chain.
    assert!(
        total > critical_upper_bound,
        "total elapsed ({total}) exceeds the critical-path UPPER BOUND ({critical_upper_bound}) \
         — resource-limited (the true critical path is ≤ the bound, so this is robust)"
    );

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 8 — metrics reached the artifact unmodified
// ===========================================================================

/// **Metrics reached the artifact unmodified.** A node attaches a task metric and
/// relies on framework-contributed metrics; the explainer reads that node's attempt
/// record and finds the task metric with its declared value and unit-suffixed name
/// AND the framework metrics (peak memory, phase timings) present too (C23).
#[test]
fn metrics_reached_the_run_artifact_unmodified() {
    let a = produce("run-metrics");
    let run = fold_artifacts(&a);
    let load = run
        .attempts()
        .iter()
        .find(|at| at.node() == "load")
        .expect("the load node has an attempt record");
    let metrics = load.metrics().as_object().expect("metrics is an object");

    // The task-attached, unit-in-the-name measurement with its declared value.
    assert_eq!(
        metrics.get("rows_read").and_then(Value::as_f64),
        Some(1000.0),
        "the task metric `rows_read` reached the artifact with its declared value"
    );
    // Framework-contributed measurements, present even though the task attached one.
    assert!(
        metrics.contains_key("dagr.peak_memory_bytes"),
        "framework peak-memory metric is present (C23)"
    );
    assert!(
        metrics.keys().any(|k| k.starts_with("dagr.phase.")),
        "framework phase-timing metrics are present (C23)"
    );
    // The reserved-prefix guard is real: a task attaching under `dagr.` fails at
    // attach time (the metrics that reached the artifact were built through that
    // guarded facility).
    let mut probe = dagr_core::metrics::AttemptMetrics::new();
    assert!(
        probe.attach("dagr.sneaky", 1u64).is_err(),
        "a task metric under the reserved prefix fails loudly at attach time (C23)"
    );

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 9 — overlay renders from artifacts only (structurally sound)
// ===========================================================================

/// **Overlay renders from artifacts only.** From the graph and run artifacts (no
/// running pipeline, no producing binary), the run-overlaid diagram renders in DOT
/// and Mermaid; every node appears, and terminal states map to documented distinct
/// styles with originated skips distinguishable from propagated ones (C24/C47). The
/// reference-tool acceptance (`dot` parses, Mermaid parser accepts) runs in CI
/// under `DAGR_REQUIRE_RENDER_TOOLS=1`; absent locally, it skips (mirroring T46/T47).
#[test]
fn overlay_renders_from_artifacts_only_and_is_structurally_sound() {
    let a = produce("run-overlay");
    // The overlay consumes ONLY the two published artifacts — parsed through the
    // render crate's own readers, which cannot even reach `dagr-core` (the C24
    // crate-graph boundary).
    let graph = dagr_render::GraphArtifact::from_json_str(
        &String::from_utf8(a.graph_bytes()).expect("graph is UTF-8"),
    )
    .expect("graph artifact parses for the renderer");
    // The run artifact the overlay reads is the folded artifact serialized to its
    // published JSON form — artifacts only, no live run.
    let run_json = fold_artifacts(&a).to_canonical_json();
    let run_overlay =
        dagr_render::overlay::RunArtifact::from_json_str(&run_json).expect("run artifact parses");

    let dot = dagr_render::overlay::render_dot_overlay(&graph, &run_overlay);
    let mermaid = dagr_render::overlay::render_mermaid_overlay(&graph, &run_overlay);

    // Every node appears in both outputs (structural soundness).
    for node in GRAPH_NODES {
        assert!(dot.contains(node), "DOT overlay contains node `{node}`");
        assert!(
            mermaid.contains(node),
            "Mermaid overlay contains node `{node}`"
        );
    }
    // Originated vs propagated skip are distinguishable: the overlay tags each node
    // with its state, so `skipped` (originated) and `upstream-skipped` (propagated)
    // both appear as distinct textual tags.
    assert!(dot.contains("skipped"), "originated skip tag present (DOT)");
    assert!(
        dot.contains("upstream-skipped"),
        "propagated skip tag present (DOT)"
    );
    assert!(
        mermaid.contains("skipped"),
        "originated skip tag present (Mermaid)"
    );
    assert!(
        mermaid.contains("upstream-skipped"),
        "propagated skip tag present (Mermaid)"
    );

    // Reference-tool acceptance in CI (dot parses; Mermaid parser accepts).
    reference_tools::assert_dot_accepted(&dot);
    reference_tools::assert_mermaid_accepted(&mermaid);

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 10 — no log line was consulted (the M3 claim, enforced)
// ===========================================================================

/// **No log line was consulted.** With the event stream / log output made
/// inaccessible for the duration of the explain step (the stream bytes are folded
/// once into the run artifact, then the on-disk stream is DELETED before the
/// explainer runs), the explainer still produces answers 5, 6, and 7 — proving the
/// M3 claim that the questions are answerable "without reading a single log line",
/// and with no access to the producing binary (C22, system criterion 5).
#[test]
fn the_explainer_consults_no_log_line_and_no_binary() {
    let a = produce("run-no-log-line");
    // Fold ONCE, then make the event stream (the only log/stdout stream) and the
    // producer's live state provably inaccessible: delete the entire run store.
    let run = fold_artifacts(&a);
    std::fs::remove_dir_all(&a.base)
        .expect("delete the run store — nothing but the artifact remains");
    assert!(!a.stream_path().exists(), "the event stream is gone");
    assert!(!a.graph_path().exists(), "even the graph file is gone");

    // Answers 5, 6, 7 are produced from the in-memory run artifact ALONE — no
    // stream, no file, no binary.
    let slowest = slowest_node(&run);
    assert_eq!(
        slowest.node, DESIGNED_BOTTLENECK,
        "answer 5 from the artifact alone"
    );
    assert_eq!(
        profile_of(&run, DESIGNED_BOTTLENECK).verdict(),
        "working",
        "answer 6 (bottleneck) from the artifact alone"
    );
    assert_eq!(
        profile_of(&run, DESIGNED_WAITER).verdict(),
        "waiting",
        "answer 6 (waiter) from the artifact alone"
    );
    assert!(
        run.summary_total_elapsed_ns() > run.summary_critical_path_ns(),
        "answer 7 (resource-limited) from the artifact alone, critical path as an upper bound"
    );
}

// ===========================================================================
// Scenario 11 — no non-allowlisted environment leaks (sentinel)
// ===========================================================================

/// **No non-allowlisted environment leaks (sentinel).** A sentinel env var set but
/// not on the pipeline's declared allowlist appears NOWHERE in the emitted
/// artifacts; the allowlisted value IS captured (C22 allowlist criterion).
#[test]
fn no_non_allowlisted_environment_leaks_into_the_artifacts() {
    let a = produce("run-sentinel");
    let graph_bytes = a.graph_bytes();
    let stream_bytes = a.stream_bytes();
    let run_bytes = fold_artifacts(&a).to_canonical_json().into_bytes();

    for (label, bytes) in [
        ("graph artifact", &graph_bytes),
        ("event stream", &stream_bytes),
        ("run artifact", &run_bytes),
    ] {
        assert!(
            !contains_bytes(bytes, SENTINEL_VALUE.as_bytes()),
            "the not-allowlisted sentinel value must appear nowhere in the {label}"
        );
    }

    // Non-vacuity: the allowlisted value WAS captured into the run artifact header,
    // so the allowlist genuinely admits declared names (not a blanket drop).
    let run = fold_artifacts(&a);
    assert_eq!(
        run.header_captured_environment()
            .get(ALLOWLISTED_ENV)
            .and_then(Value::as_str),
        Some("us-east-1"),
        "the allowlisted env value IS captured (the allowlist is not a blanket drop)"
    );

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Scenario 12 — determinism of the demo
// ===========================================================================

/// **Determinism of the demo.** Two runs of the reference pipeline yield identical
/// slowest-node and waiting-vs-working verdicts and identical structural
/// fingerprints (generation time aside) — the demo does not flake, so it can gate
/// the milestone.
#[test]
fn the_demo_is_deterministic_across_runs() {
    let a1 = produce("run-determinism-1");
    let a2 = produce("run-determinism-2");
    let r1 = fold_artifacts(&a1);
    let r2 = fold_artifacts(&a2);

    assert_eq!(
        slowest_node(&r1).node,
        slowest_node(&r2).node,
        "the slowest-node verdict is stable across runs"
    );
    assert_eq!(
        profile_of(&r1, DESIGNED_BOTTLENECK).verdict(),
        profile_of(&r2, DESIGNED_BOTTLENECK).verdict(),
        "the bottleneck's waiting-vs-working verdict is stable"
    );
    assert_eq!(
        profile_of(&r1, DESIGNED_WAITER).verdict(),
        profile_of(&r2, DESIGNED_WAITER).verdict(),
        "the waiter's waiting-vs-working verdict is stable"
    );
    assert_eq!(
        r1.header_fingerprint_structural(),
        r2.header_fingerprint_structural(),
        "the structural fingerprint is identical across runs"
    );

    // The graph artifacts are byte-identical outside the generation-time field.
    let g1 = mask_generated_at(serde_json::from_slice(&a1.graph_bytes()).unwrap());
    let g2 = mask_generated_at(serde_json::from_slice(&a2.graph_bytes()).unwrap());
    assert_eq!(
        g1, g2,
        "graph artifacts are byte-identical outside generation time"
    );

    let _ = std::fs::remove_dir_all(&a1.base);
    let _ = std::fs::remove_dir_all(&a2.base);
}

// ===========================================================================
// Scenario 13 — criteria-matrix wiring
// ===========================================================================

/// **Criteria-matrix wiring.** The M3 done-when and system criterion 5 map to this
/// demo in the checked-in coverage matrix, and the matrix-coverage CI check passes.
/// This test runs the real verifier against the real matrix and suite (the same
/// verifier `SL8machine` names), so this demo's registration is self-consistent.
#[test]
fn criteria_matrix_coverage_check_passes() {
    let root = repo_root();
    let verifier = root.join("scripts/check-coverage-matrix.sh");
    assert!(verifier.is_file(), "coverage-matrix verifier exists");
    let output = Command::new("bash")
        .arg(&verifier)
        .current_dir(&root)
        .output()
        .expect("run the coverage-matrix verifier");
    assert!(
        output.status.success(),
        "coverage-matrix verifier passes (M3 done-when + SL5 registration is sound)\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    // This demo's own test id is named in the SL5 row's additive note.
    let matrix = std::fs::read_to_string(root.join("docs/coverage-matrix.md"))
        .expect("read the coverage matrix");
    assert!(
        matrix.contains("m3_demo_explain_a_run"),
        "the SL5 row names this demo file as the M3 done-when machine test"
    );
}

// ===========================================================================
// Scenario 1 (schema half) — both artifacts validate against their schemas
// ===========================================================================

/// **Both artifacts validate against their published schemas.** The real emitted
/// graph artifact validates against `schemas/graph/v1.schema.json` and the real
/// folded run artifact validates against `schemas/run/v1.schema.json` (C20/C22),
/// via the T39 validator. Gated behind `schema-validation` (default OFF, pulling
/// the CI-/dev-scoped `jsonschema` validator), mirroring T40's graph round-trip
/// suite; CI runs it with the feature ON.
#[cfg(feature = "schema-validation")]
#[test]
fn both_artifacts_validate_against_their_published_schemas() {
    use dagr_artifact::schema::{validate_value, ArtifactKind};

    let a = produce("run-schema-valid");
    let graph: Value = serde_json::from_slice(&a.graph_bytes()).unwrap();
    validate_value(ArtifactKind::Graph, 1, &graph)
        .expect("the emitted graph artifact validates against its published schema");

    let run_value = fold_artifacts(&a).to_value();
    validate_value(ArtifactKind::Run, 1, &run_value)
        .expect("the folded run artifact validates against its published schema");

    let _ = std::fs::remove_dir_all(&a.base);
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Locate the repository root from this crate's manifest directory (`crates/cli`).
fn repo_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// Whether `haystack` contains the byte subsequence `needle`.
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Blank the graph header's generation-time field so two artifacts compare for
/// byte-identity outside it (C20: generation time is the only field allowed to
/// vary).
fn mask_generated_at(mut artifact: Value) -> Value {
    if let Some(header) = artifact.get_mut("header").and_then(Value::as_object_mut) {
        header.insert("generated_at".into(), Value::from(""));
    }
    artifact
}

// ===========================================================================
// Reference-tool acceptance (dot / Mermaid) — CI gate, local skip
// ===========================================================================
//
// The rendered DOT/Mermaid must be accepted by their reference tools in CI (C24
// line 520). These external programs may be absent locally, so an absent tool
// SKIPS with a printed notice (keeping `cargo test --workspace` green on a machine
// without Graphviz/Node); in CI `DAGR_REQUIRE_RENDER_TOOLS=1` turns an absent tool
// into a hard failure. This mirrors `crates/render/tests/reference_tools.rs` (the
// module is test-private there, so the small harness is duplicated here for the
// integration demo).
mod reference_tools {
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    fn tools_required() -> bool {
        std::env::var("DAGR_REQUIRE_RENDER_TOOLS")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    }

    fn tool_available(program: &str, probe_args: &[&str]) -> bool {
        Command::new(program)
            .args(probe_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }

    fn null_device() -> &'static str {
        if cfg!(windows) {
            "NUL"
        } else {
            "/dev/null"
        }
    }

    struct ToolRun {
        accepted: bool,
        stderr: String,
    }

    fn accepts_on_stdin(program: &str, args: &[&str], input: &str) -> ToolRun {
        let child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn();
        let Ok(mut child) = child else {
            return ToolRun {
                accepted: false,
                stderr: format!("failed to spawn `{program}`"),
            };
        };
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(input.as_bytes());
        }
        match child.wait_with_output() {
            Ok(out) => ToolRun {
                accepted: out.status.success(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            },
            Err(e) => ToolRun {
                accepted: false,
                stderr: format!("`{program}` did not complete: {e}"),
            },
        }
    }

    /// Assert `dot` accepts the overlaid DOT (CI gate; skips if absent locally).
    pub fn assert_dot_accepted(dot: &str) {
        if !tool_available("dot", &["-V"]) {
            assert!(
                !tools_required(),
                "`dot` (Graphviz) is required in CI (DAGR_REQUIRE_RENDER_TOOLS=1) but was not found"
            );
            eprintln!("SKIP: `dot` (Graphviz) not installed; skipping the DOT reference-tool gate");
            return;
        }
        let good = accepts_on_stdin("dot", &["-Tcanon", "-o", null_device()], dot);
        assert!(
            good.accepted,
            "`dot` must accept the overlaid DOT output; dot stderr:\n{}",
            good.stderr
        );
    }

    /// Assert Mermaid's browserless parser accepts the overlaid Mermaid (CI gate;
    /// skips if absent locally). Mirrors `crates/render/tests/reference_tools.rs`.
    pub fn assert_mermaid_accepted(mermaid: &str) {
        let Some(parse_dir) = mermaid_parse_dir() else {
            assert!(
                !tools_required(),
                "Mermaid's parser (mermaid.parse via Node) is required in CI \
                 (DAGR_REQUIRE_RENDER_TOOLS=1) but DAGR_MERMAID_PARSE_DIR/node was not usable"
            );
            eprintln!(
                "SKIP: Mermaid parser not available (set DAGR_MERMAID_PARSE_DIR to a dir with \
                 `mermaid` + `jsdom` in node_modules); skipping the Mermaid reference-tool gate"
            );
            return;
        };
        let good = mermaid_parser_accepts(&parse_dir, mermaid);
        assert!(
            good.accepted,
            "Mermaid's parser must accept the overlaid Mermaid output; parser stderr:\n{}",
            good.stderr
        );
    }

    fn mermaid_parse_dir() -> Option<PathBuf> {
        let dir = PathBuf::from(std::env::var("DAGR_MERMAID_PARSE_DIR").ok()?);
        if !dir.is_dir() {
            return None;
        }
        if !tool_available("node", &["--version"]) {
            return None;
        }
        Some(dir)
    }

    fn mermaid_parser_accepts(parse_dir: &Path, input: &str) -> ToolRun {
        const PARSE_JS: &str = r"import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';
const dom = new JSDOM('<!DOCTYPE html><html><body></body></html>', { pretendToBeVisual: true });
globalThis.window = dom.window;
globalThis.document = dom.window.document;
const mermaid = (await import('mermaid')).default;
const src = readFileSync(process.argv[2], 'utf8');
try {
  await mermaid.parse(src);
} catch (e) {
  console.error('MERMAID_PARSE_REJECTED: ' + String((e && e.message) || e).split('\n')[0]);
  process.exit(1);
}
";
        let token = rand_token();
        let script_path = parse_dir.join(format!("dagr-t49-mermaid-parse-{token}.mjs"));
        let in_path = parse_dir.join(format!("dagr-t49-mermaid-in-{token}.mmd"));
        let cleanup = |script: &Path, mmd: &Path| {
            let _ = std::fs::remove_file(script);
            let _ = std::fs::remove_file(mmd);
        };
        if let Err(e) = std::fs::write(&script_path, PARSE_JS) {
            return ToolRun {
                accepted: false,
                stderr: format!("failed to write parse helper: {e}"),
            };
        }
        if let Err(e) = std::fs::write(&in_path, input) {
            cleanup(&script_path, &in_path);
            return ToolRun {
                accepted: false,
                stderr: format!("failed to write Mermaid input: {e}"),
            };
        }
        let output = Command::new("node")
            .arg(&script_path)
            .arg(&in_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output();
        cleanup(&script_path, &in_path);
        match output {
            Ok(out) => ToolRun {
                accepted: out.status.success(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            },
            Err(e) => ToolRun {
                accepted: false,
                stderr: format!("`node` did not complete: {e}"),
            },
        }
    }

    fn rand_token() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    }
}
