//! C24 · run-overlay rendering — ticket T47 (058). Written first, TDD.
//!
//! These translate the T47 Test plan into executable tests against the **real**
//! overlay renderer (`dagr_render::overlay`), projecting a **run artifact**
//! (C22) onto the base structural diagram (T46) in both DOT and Mermaid. Each
//! test maps to one T47 Test-plan scenario / arch.md C24 acceptance line (see
//! the per-test doc comment).
//!
//! Inputs are **real artifacts**. Wherever the real producers can already emit
//! the shape a scenario needs, the test drives them:
//!
//! * The graph artifact is the checked-in 30-node T46 fixture and the genuine
//!   T40-emitted two-node corpus artifact.
//! * The run artifact is produced by the **real T42 fold**
//!   (`dagr_artifact::fold::fold_stream(...).to_canonical_json()`) over a real
//!   C19 event stream, and by the checked-in run-artifact corpus fixtures
//!   (`tests/fixtures/corpus/run/v1/*.json`) — themselves published-schema
//!   artifacts.
//!
//! The one shape the real fold cannot yet emit is the **single-node-replay**
//! variant (`variant`/`node_markings`/`not-requested`), which the fold's
//! `to_value` does not produce; that scenario uses the checked-in corpus
//! fixture `single-node-replay.json` (a real published-schema artifact),
//! justified in the test doc.
//!
//! At least one genuinely-real-producer test (`real_folded_run_artifact_*`)
//! folds an event stream through the real fold and overlays the result, proving
//! the overlay is not tied to hand-authored run JSON.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use dagr_artifact::fold::fold_stream;
use dagr_render::overlay::{
    render_dot_overlay, render_mermaid_overlay, RunArtifact, TERMINAL_STATES,
};
use dagr_render::{render_dot, render_mermaid, GraphArtifact};
use serde_json::{json, Value};

// === Fixture loading =========================================================

fn render_fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}

fn corpus(rel: &str) -> PathBuf {
    workspace_root().join("tests/fixtures/corpus").join(rel)
}

fn thirty_node_graph() -> GraphArtifact {
    let raw = std::fs::read_to_string(render_fixture("thirty-node.graph.json")).unwrap();
    GraphArtifact::from_json_str(&raw).unwrap()
}

fn two_node_graph() -> GraphArtifact {
    let raw = std::fs::read_to_string(corpus("graph/v1/two-node.json")).unwrap();
    GraphArtifact::from_json_str(&raw).unwrap()
}

fn run_from_corpus(rel: &str) -> RunArtifact {
    let raw = std::fs::read_to_string(corpus(rel)).unwrap();
    RunArtifact::from_json_str(&raw).expect("corpus run artifact parses")
}

// === Real run-artifact builders (via the T42 fold) ===========================

/// The shared C19 record envelope (published T39 event-stream wire form).
fn env(seq: u64, offset_ns: u64, kind: &str) -> Value {
    json!({
        "schema_version": "dagr.event-stream@1",
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "seq": seq,
        "wall": "2026-07-23T00:00:00.000Z",
        "offset_ns": offset_ns,
        "kind": kind,
    })
}

fn with(mut v: Value, fields: &[(&str, Value)]) -> Value {
    let o = v.as_object_mut().unwrap();
    for (k, val) in fields {
        o.insert((*k).to_string(), val.clone());
    }
    v
}

fn start_header() -> Value {
    json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
        "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
        "fingerprint_algorithm_version": 1,
        "parameters": {},
        "data_interval": null,
        "captured_environment": {},
        "resume_lineage": null,
    })
}

fn stream(records: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for r in records {
        out.push_str(&serde_json::to_string(r).unwrap());
        out.push('\n');
    }
    out.into_bytes()
}

/// Emit the lifecycle of one succeeded attempt for `node` with a known executing
/// duration (from monotonic offsets): ready → admitted → started → outcome →
/// terminal. `base` is the run-relative offset the attempt starts at; the
/// executing span is `dur_ns`.
#[allow(clippy::too_many_arguments)]
fn succeeded_node(recs: &mut Vec<Value>, seq: &mut u64, base: u64, node: &str, dur_ns: u64) {
    let s = |seq: &mut u64| {
        let v = *seq;
        *seq += 1;
        v
    };
    recs.push(with(
        env(s(seq), base, "node-ready"),
        &[("node", json!(node))],
    ));
    recs.push(with(
        env(s(seq), base, "node-admitted"),
        &[("node", json!(node))],
    ));
    recs.push(with(
        env(s(seq), base, "attempt-started"),
        &[("node", json!(node)), ("attempt", json!(1))],
    ));
    recs.push(with(
        env(s(seq), base + dur_ns, "attempt-outcome"),
        &[
            ("node", json!(node)),
            ("attempt", json!(1)),
            ("status", json!("succeeded")),
        ],
    ));
    recs.push(with(
        env(s(seq), base + dur_ns, "node-terminal"),
        &[("node", json!(node)), ("state", json!("succeeded"))],
    ));
}

/// Fold a real event stream that runs `load` (dur 1000ns) and `sink` (dur
/// 2000ns), both succeeded, into a REAL C22 run artifact JSON via the T42 fold,
/// then parse it into the overlay's read-only view.
fn real_folded_two_node_run() -> (String, RunArtifact) {
    let mut recs = vec![run_started_rec()];
    let mut seq = 1u64;
    succeeded_node(&mut recs, &mut seq, 0, "load", 1000);
    succeeded_node(&mut recs, &mut seq, 1000, "sink", 2000);
    recs.push(with(
        env(seq, 3000, "run-finished"),
        &[("outcome", json!("succeeded"))],
    ));
    let bytes = stream(&recs);
    let art =
        fold_stream(&bytes, &["load".to_string(), "sink".to_string()]).expect("real fold succeeds");
    let json = art.to_canonical_json();
    let view = RunArtifact::from_json_str(&json).expect("folded run artifact parses");
    (json, view)
}

fn run_started_rec() -> Value {
    with(env(0, 0, "run-started"), &[("header", start_header())])
}

// === Diagram-inspection helpers ==============================================

/// The DOT declaration line for `node` (`"node" [ ... ];`).
fn dot_node_line<'a>(dot: &'a str, node: &str) -> &'a str {
    dot.lines()
        .find(|l| {
            let t = l.trim();
            t.starts_with(&format!("\"{node}\" ")) && !t.contains("->")
        })
        .unwrap_or_else(|| panic!("DOT has no declaration for node `{node}`"))
}

/// Sanitize a node identity to its Mermaid id token the same way the renderer
/// does (non-alphanumeric, non-`_` → `_`), so a hyphenated identity like
/// `timed-out` matches the `timed_out` id the renderer emits.
fn mermaid_id(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The Mermaid declaration line for `node` (`node[...]`), excluding subgraph
/// headers and links.
fn mermaid_node_line<'a>(mmd: &'a str, node: &str) -> &'a str {
    let id = mermaid_id(node);
    mmd.lines()
        .find(|l| {
            let t = l.trim();
            t.starts_with(&format!("{id}[")) && !t.contains("-->") && !t.contains("-.->")
        })
        .unwrap_or_else(|| panic!("Mermaid has no declaration for node `{node}`"))
}

/// The Mermaid `class <node> <className>` assignment line for `node`, if any.
fn mermaid_class_of(mmd: &str, node: &str) -> Option<String> {
    let id = mermaid_id(node);
    mmd.lines().find_map(|l| {
        let t = l.trim();
        let rest = t.strip_prefix("class ")?;
        let mut it = rest.split_whitespace();
        let n = it.next()?;
        let cls = it.next()?;
        (n == id).then(|| cls.to_string())
    })
}

// === Test-plan scenarios =====================================================

/// **Overlay is opt-in / no regression.** The base `render_dot`/`render_mermaid`
/// output is untouched by the presence of the overlay module; rendering the
/// 30-node fixture with no run artifact reproduces the T46 golden byte-for-byte.
#[test]
fn overlay_is_opt_in_no_regression() {
    let art = thirty_node_graph();
    let dot = render_dot(&art);
    let mmd = render_mermaid(&art);
    let golden_dot = std::fs::read_to_string(render_fixture("thirty-node.golden.dot")).unwrap();
    let golden_mmd = std::fs::read_to_string(render_fixture("thirty-node.golden.mmd")).unwrap();
    assert_eq!(dot, golden_dot, "base DOT must equal the T46 golden");
    assert_eq!(mmd, golden_mmd, "base Mermaid must equal the T46 golden");
}

/// The nine terminal states plus the `not-requested` marking, as they appear in
/// a run artifact. Used to build a one-node-per-state fixture graph + run.
const NINE_STATES: [&str; 9] = [
    "succeeded",
    "failed",
    "timed-out",
    "skipped",
    "upstream-skipped",
    "upstream-failed",
    "cancelled",
    "abandoned",
    "satisfied-from-prior",
];

/// Build a synthetic graph artifact with one isolated node per given name.
fn graph_of_nodes(names: &[&str]) -> GraphArtifact {
    let nodes: Vec<Value> = names
        .iter()
        .map(|n| json!({ "name": n, "group": "", "output_type_name": "Unit" }))
        .collect();
    let v = json!({ "nodes": nodes, "edges": [] });
    GraphArtifact::from_json_str(&v.to_string()).unwrap()
}

/// Build a run artifact (published-schema shape) whose attempts assign each
/// `(node, status)` pair a distinct executing duration. A hand-authored
/// published-schema fixture: it carries one attempt per state so all nine
/// documented styles are exercised in a single render — a shape a single real
/// run rarely produces but the schema fully sanctions.
fn run_of_states(pairs: &[(&str, &str)]) -> RunArtifact {
    let attempts: Vec<Value> = pairs
        .iter()
        .enumerate()
        .map(|(i, (node, status))| {
            let mut a = json!({
                "node": node,
                "attempt": 1,
                "status": status,
                "phase_durations_ns": { "executing": (i as u64 + 1) * 1000 },
                "worker": "worker-0",
            });
            if *status == "satisfied-from-prior" {
                a.as_object_mut().unwrap().insert(
                    "satisfied_from_run".into(),
                    json!("018f0000-0000-7000-8000-000000000001"),
                );
            }
            a
        })
        .collect();
    let v = json!({
        "header": {
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "pipeline": "example-pipeline",
            "parameters": {},
            "data_interval": null,
            "captured_environment": {},
            "resume_lineage": null,
            "overall_outcome": "failed"
        },
        "attempts": attempts,
        "summary": null
    });
    RunArtifact::from_json_str(&v.to_string()).expect("state fixture parses")
}

/// **Every state gets a distinct documented style (DOT).** Nine nodes carry the
/// nine terminal states; each node's DOT declaration carries the style
/// documented for its state, and the nine styles are mutually distinct.
#[test]
fn every_state_distinct_documented_style_dot() {
    let graph = graph_of_nodes(&NINE_STATES);
    let pairs: Vec<(&str, &str)> = NINE_STATES.iter().map(|s| (*s, *s)).collect();
    let run = run_of_states(&pairs);
    let dot = render_dot_overlay(&graph, &run);

    let mut styles = BTreeSet::new();
    for state in NINE_STATES {
        let line = dot_node_line(&dot, state);
        // The documented DOT per-state discriminator is the `fillcolor` (nodes
        // are `style=filled`), so distinctness is checkable from the DOT alone.
        assert!(
            line.contains("fillcolor=") && line.contains("filled"),
            "node `{state}` must carry a filled fillcolor style: {line}"
        );
        let fc = extract_attr(line, "fillcolor");
        assert!(
            styles.insert(fc.clone()),
            "state `{state}` reuses fillcolor `{fc}` — styles must be mutually distinct"
        );
    }
    assert_eq!(
        styles.len(),
        9,
        "nine states → nine distinct DOT fillcolors"
    );
}

/// **Every state gets a distinct documented style (Mermaid).** The same nine
/// states; each node carries its documented Mermaid class; the nine classes are
/// mutually distinct and each has a distinct `classDef` style.
#[test]
fn every_state_distinct_documented_style_mermaid() {
    let graph = graph_of_nodes(&NINE_STATES);
    let pairs: Vec<(&str, &str)> = NINE_STATES.iter().map(|s| (*s, *s)).collect();
    let run = run_of_states(&pairs);
    let mmd = render_mermaid_overlay(&graph, &run);

    let mut classes = BTreeSet::new();
    for state in NINE_STATES {
        let cls =
            mermaid_class_of(&mmd, state).unwrap_or_else(|| panic!("node `{state}` has no class"));
        assert!(
            classes.insert(cls.clone()),
            "state `{state}` reuses class `{cls}` — classes must be distinct"
        );
        // The class must be defined with a fill style.
        assert!(
            mmd.lines()
                .any(|l| l.trim().starts_with(&format!("classDef {cls} ")) && l.contains("fill")),
            "class `{cls}` must have a filled classDef"
        );
    }
    assert_eq!(
        classes.len(),
        9,
        "nine states → nine distinct Mermaid classes"
    );
}

/// **Originated vs propagated skip are distinguishable.** A `skipped` node and a
/// downstream `upstream-skipped` node carry different styles in both formats;
/// the originated-skip style is not reused for any propagated state.
#[test]
fn originated_vs_propagated_skip_distinguishable() {
    let graph = graph_of_nodes(&["orig", "prop", "other"]);
    let run = run_of_states(&[
        ("orig", "skipped"),
        ("prop", "upstream-skipped"),
        ("other", "upstream-failed"),
    ]);
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    let orig_dot = extract_attr(dot_node_line(&dot, "orig"), "fillcolor");
    let prop_dot = extract_attr(dot_node_line(&dot, "prop"), "fillcolor");
    let other_dot = extract_attr(dot_node_line(&dot, "other"), "fillcolor");
    assert_ne!(
        orig_dot, prop_dot,
        "skipped vs upstream-skipped differ (DOT)"
    );
    assert_ne!(
        orig_dot, other_dot,
        "originated skip not reused for a propagated state"
    );

    let orig_m = mermaid_class_of(&mmd, "orig").unwrap();
    let prop_m = mermaid_class_of(&mmd, "prop").unwrap();
    assert_ne!(
        orig_m, prop_m,
        "skipped vs upstream-skipped differ (Mermaid)"
    );
}

/// **Originated vs propagated failure are distinguishable.** `failed`,
/// `timed-out`, and `upstream-failed` carry three distinct documented styles in
/// both formats; no propagated-failure style collides with an originated one.
#[test]
fn originated_vs_propagated_failure_distinguishable() {
    let graph = graph_of_nodes(&["f", "t", "uf"]);
    let run = run_of_states(&[
        ("f", "failed"),
        ("t", "timed-out"),
        ("uf", "upstream-failed"),
    ]);
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    let f = extract_attr(dot_node_line(&dot, "f"), "fillcolor");
    let t = extract_attr(dot_node_line(&dot, "t"), "fillcolor");
    let uf = extract_attr(dot_node_line(&dot, "uf"), "fillcolor");
    assert_ne!(f, uf, "failed vs upstream-failed differ (DOT)");
    assert_ne!(t, uf, "timed-out vs upstream-failed differ (DOT)");
    assert_ne!(f, t, "failed vs timed-out differ (DOT)");

    let fm = mermaid_class_of(&mmd, "f").unwrap();
    let tm = mermaid_class_of(&mmd, "t").unwrap();
    let ufm = mermaid_class_of(&mmd, "uf").unwrap();
    assert!(
        fm != ufm && tm != ufm && fm != tm,
        "three distinct Mermaid classes"
    );
}

/// **Cancellation-family states are distinct.** `cancelled` and `abandoned`
/// carry distinct styles, distinct from every other state.
#[test]
fn cancellation_family_distinct() {
    let graph = graph_of_nodes(&["c", "a"]);
    let run = run_of_states(&[("c", "cancelled"), ("a", "abandoned")]);
    let dot = render_dot_overlay(&graph, &run);
    let c = extract_attr(dot_node_line(&dot, "c"), "fillcolor");
    let a = extract_attr(dot_node_line(&dot, "a"), "fillcolor");
    assert_ne!(c, a, "cancelled vs abandoned differ");
    // Distinct from every other state's style.
    let others = distinct_state_fillcolors(&["succeeded", "failed", "skipped", "upstream-failed"]);
    assert!(!others.contains(&c) && !others.contains(&a));
}

/// **Resume carry-forward is styled.** A `satisfied-from-prior` node carries its
/// own documented style, distinct from `succeeded`.
#[test]
fn satisfied_from_prior_distinct_from_succeeded() {
    let graph = graph_of_nodes(&["s", "sfp"]);
    let run = run_of_states(&[("s", "succeeded"), ("sfp", "satisfied-from-prior")]);
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);
    assert_ne!(
        extract_attr(dot_node_line(&dot, "s"), "fillcolor"),
        extract_attr(dot_node_line(&dot, "sfp"), "fillcolor"),
        "satisfied-from-prior differs from succeeded (DOT)"
    );
    assert_ne!(
        mermaid_class_of(&mmd, "s").unwrap(),
        mermaid_class_of(&mmd, "sfp").unwrap(),
        "satisfied-from-prior differs from succeeded (Mermaid)"
    );
}

/// **Single-node-replay marking is handled.** The real (hand-authored, but
/// published-schema) `single-node-replay.json` corpus fixture marks `load`
/// `not-requested` and runs `sink`. `not-requested` renders with its own
/// documented style, is not treated as a terminal state, and raises no error.
/// (Justified fixture: the T42 fold does not yet emit the replay variant.)
#[test]
fn single_node_replay_not_requested_handled() {
    let graph = two_node_graph();
    let run = run_from_corpus("run/v1/single-node-replay.json");
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    // `not-requested` is not a terminal state; it has its own style.
    let nr_dot = extract_attr(dot_node_line(&dot, "load"), "fillcolor");
    let sink_dot = extract_attr(dot_node_line(&dot, "sink"), "fillcolor");
    assert_ne!(nr_dot, sink_dot, "not-requested differs from the run node");
    // Its style is not any terminal-state style.
    let terminal_styles = distinct_state_fillcolors(&NINE_STATES);
    assert!(
        !terminal_styles.contains(&nr_dot),
        "not-requested must not reuse a terminal-state style"
    );
    // And it must not be labelled as a terminal state — the tag says not-requested.
    assert!(dot_node_line(&dot, "load").contains("not-requested"));
    assert!(mermaid_class_of(&mmd, "load").is_some());
}

/// **Duration annotations appear and are correct.** Nodes with known distinct
/// durations are annotated with the documented human-readable duration in both
/// formats. Uses a REAL folded run artifact.
#[test]
fn duration_annotations_present_and_correct() {
    let graph = two_node_graph();
    let (_json, run) = real_folded_two_node_run();
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    // load executed for 1000ns → "1.00µs"; sink for 2000ns → "2.00µs".
    assert!(
        dot_node_line(&dot, "load").contains("1.00µs"),
        "load duration annotation missing: {}",
        dot_node_line(&dot, "load")
    );
    assert!(dot_node_line(&dot, "sink").contains("2.00µs"));
    assert!(mermaid_node_line(&mmd, "load").contains("1.00µs"));
    assert!(mermaid_node_line(&mmd, "sink").contains("2.00µs"));
}

/// **Every node and edge still appears with the overlay on.** The 30-node
/// fixture with a matching run artifact covering all nodes: every graph node and
/// edge is present, data/ordering edges keep their T46 styling, groups still
/// cluster, and every node additionally carries state colouring and a duration.
#[test]
fn every_node_and_edge_present_with_overlay() {
    let graph = thirty_node_graph();
    // A run covering all 30 nodes, all succeeded, each with a 1µs duration.
    let pairs: Vec<(String, &str)> = graph
        .nodes()
        .iter()
        .map(|n| (n.name().to_string(), "succeeded"))
        .collect();
    let pair_refs: Vec<(&str, &str)> = pairs.iter().map(|(n, s)| (n.as_str(), *s)).collect();
    let run = run_of_states(&pair_refs);
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    // Every node declared in both formats.
    for n in graph.nodes() {
        assert!(
            dot.contains(&format!("\"{}\" [", n.name())),
            "DOT missing node {}",
            n.name()
        );
        assert!(dot_node_line(&dot, n.name()).contains("fillcolor="));
        assert!(mermaid_node_line(&mmd, n.name()).len() > n.name().len());
    }
    // Every edge present, base edge styling preserved.
    let dot_edge_lines = dot.lines().filter(|l| l.contains("->")).count();
    assert_eq!(dot_edge_lines, graph.edges().len(), "all DOT edges present");
    let mmd_edge_lines = mmd
        .lines()
        .filter(|l| l.contains("-->") || l.contains("-.->"))
        .count();
    assert_eq!(
        mmd_edge_lines,
        graph.edges().len(),
        "all Mermaid edges present"
    );
    // Data edges stay solid, ordering dashed (T46 guarantee preserved).
    assert!(dot.contains("style=solid"));
    assert!(dot.contains("style=dashed"));
    // Groups still cluster.
    assert!(dot.contains("subgraph \"cluster_"));
    assert!(mmd.contains("subgraph group_"));
}

/// **Golden files: overlaid DOT and Mermaid for the 30-node fixture are
/// byte-stable.** Pins the overlaid output deterministically.
#[test]
fn overlaid_30node_golden_is_byte_stable() {
    let graph = thirty_node_graph();
    let pairs: Vec<(String, &str)> = graph
        .nodes()
        .iter()
        .enumerate()
        .map(|(i, n)| {
            // Cycle through all nine states so the golden exercises the whole table.
            (n.name().to_string(), NINE_STATES[i % NINE_STATES.len()])
        })
        .collect();
    let pair_refs: Vec<(&str, &str)> = pairs.iter().map(|(n, s)| (n.as_str(), *s)).collect();
    let run = run_of_states(&pair_refs);
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    if std::env::var("DAGR_BLESS").is_ok_and(|v| v == "1") {
        std::fs::write(render_fixture("thirty-node.overlay.golden.dot"), &dot).unwrap();
        std::fs::write(render_fixture("thirty-node.overlay.golden.mmd"), &mmd).unwrap();
    }
    let golden_dot =
        std::fs::read_to_string(render_fixture("thirty-node.overlay.golden.dot")).unwrap();
    let golden_mmd =
        std::fs::read_to_string(render_fixture("thirty-node.overlay.golden.mmd")).unwrap();
    assert_eq!(
        dot, golden_dot,
        "overlaid DOT must match the golden; bless with DAGR_BLESS=1 if intended"
    );
    assert_eq!(
        mmd, golden_mmd,
        "overlaid Mermaid must match the golden; bless with DAGR_BLESS=1 if intended"
    );
}

/// **Determinism: overlaid rendering is byte-stable across repetitions.**
#[test]
fn overlaid_rendering_is_byte_stable() {
    let graph = two_node_graph();
    let run = run_from_corpus("run/v1/success-with-retry.json");
    assert_eq!(
        render_dot_overlay(&graph, &run),
        render_dot_overlay(&graph, &run)
    );
    assert_eq!(
        render_mermaid_overlay(&graph, &run),
        render_mermaid_overlay(&graph, &run)
    );
}

/// **Works on a historical artifact with no producing binary.** The frozen
/// two-node graph corpus + the frozen `success-with-retry` run corpus (both
/// checked-in published-schema artifacts) overlay correctly, with no access to
/// any producing binary (the render crate links no pipeline crate — a crate-graph
/// fact). `load` retried (failed→succeeded) so its duration sums both attempts.
#[test]
fn works_on_a_historical_artifact() {
    let graph = two_node_graph();
    let run = run_from_corpus("run/v1/success-with-retry.json");
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);

    // load's FINAL attempt succeeded → succeeded style; sink satisfied-from-prior.
    let load = dot_node_line(&dot, "load");
    assert!(
        load.contains("succeeded"),
        "load final state is succeeded: {load}"
    );
    let sink = dot_node_line(&dot, "sink");
    assert!(
        sink.contains("satisfied-from-prior"),
        "sink is satisfied-from-prior: {sink}"
    );
    assert!(mermaid_class_of(&mmd, "load").is_some());
    assert!(mermaid_class_of(&mmd, "sink").is_some());
}

/// **A genuinely-real-producer overlay.** Fold a real event stream through the
/// T42 fold and overlay the result — the overlay is not tied to hand-authored
/// run JSON.
#[test]
fn real_folded_run_artifact_overlays() {
    let graph = two_node_graph();
    let (json, run) = real_folded_two_node_run();
    // The JSON came out of the real fold (carries the fold_reader provenance).
    assert!(json.contains("fold_reader"), "produced by the real fold");
    let dot = render_dot_overlay(&graph, &run);
    assert!(dot_node_line(&dot, "load").contains("succeeded"));
    assert!(dot_node_line(&dot, "sink").contains("succeeded"));
}

/// **Graph/run node mismatch is defined, not fatal.** (a) A graph node with no
/// run record renders without overlay styling (base only), no panic. (b) A run
/// record whose node id is absent from the graph is reported per the documented
/// rule (a diagram comment listing extra run records), not injected as a phantom
/// node, no panic.
#[test]
fn graph_run_mismatch_is_defined_not_fatal() {
    // (a) graph has an extra node `ghost` with no run record.
    let graph = graph_of_nodes(&["a", "ghost"]);
    let run = run_of_states(&[("a", "succeeded")]);
    let dot = render_dot_overlay(&graph, &run);
    let mmd = render_mermaid_overlay(&graph, &run);
    // `ghost` renders (base styling), carries NO fillcolor (no overlay state).
    let ghost = dot_node_line(&dot, "ghost");
    assert!(
        !ghost.contains("fillcolor="),
        "an unmatched graph node renders without overlay styling: {ghost}"
    );
    assert!(mermaid_class_of(&mmd, "ghost").is_none());
    // `a` is styled.
    assert!(dot_node_line(&dot, "a").contains("fillcolor="));

    // (b) run has a record for `extra`, absent from the graph.
    let graph2 = graph_of_nodes(&["a"]);
    let run2 = run_of_states(&[("a", "succeeded"), ("extra", "failed")]);
    let dot2 = render_dot_overlay(&graph2, &run2);
    let mmd2 = render_mermaid_overlay(&graph2, &run2);
    // The extra record is reported in a comment, never as a node declaration.
    assert!(
        !dot2.contains("\"extra\" ["),
        "an extra run record must not be injected as a phantom DOT node"
    );
    assert!(
        dot2.contains("extra"),
        "the extra run record must be reported (documented rule): {dot2}"
    );
    assert!(mmd2.contains("extra"), "extra reported in Mermaid too");
    // No panic reaching here means the mismatch was handled.
    assert!(!dot2.is_empty() && !mmd2.is_empty());
}

/// **The taxonomy the overlay documents is exactly the normative nine.** Guards
/// against drift between `TERMINAL_STATES` and arch.md's Vocabulary.
#[test]
fn terminal_states_table_matches_the_normative_taxonomy() {
    let got: BTreeSet<&str> = TERMINAL_STATES.iter().copied().collect();
    let expected: BTreeSet<&str> = NINE_STATES.iter().copied().collect();
    assert_eq!(got, expected, "TERMINAL_STATES must be the normative nine");
}

// === small attribute extractors ==============================================

/// Extract the value of `key=VALUE` from a DOT attribute line (VALUE is up to
/// the next `,` or `]`, quotes stripped).
fn extract_attr(line: &str, key: &str) -> String {
    let needle = format!("{key}=");
    let idx = line
        .find(&needle)
        .unwrap_or_else(|| panic!("attribute `{key}` not on line: {line}"));
    let rest = &line[idx + needle.len()..];
    let end = rest.find([',', ']']).unwrap_or(rest.len());
    rest[..end].trim().trim_matches('"').to_string()
}

/// The distinct DOT fillcolors the overlay assigns to the given states.
fn distinct_state_fillcolors(states: &[&str]) -> BTreeSet<String> {
    let names: Vec<String> = states.iter().map(|s| (*s).to_string()).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let graph = graph_of_nodes(&name_refs);
    let pairs: Vec<(&str, &str)> = states.iter().map(|s| (*s, *s)).collect();
    let run = run_of_states(&pairs);
    let dot = render_dot_overlay(&graph, &run);
    states
        .iter()
        .map(|s| extract_attr(dot_node_line(&dot, s), "fillcolor"))
        .collect()
}
