//! C24 · Diagram renderer — ticket T46. Written first, TDD.
//!
//! These translate the T46 Test plan into executable, structurally-checkable
//! tests against the **real** renderer (`dagr_render`) reading a **real**
//! published C20 graph artifact (the checked-in 30-node fixture, plus the genuine
//! T40-emitted corpus fixture). Each test maps to one T46 Test-plan scenario /
//! arch.md C24 acceptance line (see the per-test doc comment).
//!
//! The renderer is a pure reader over the artifact schema: it deserializes the
//! artifact JSON into typed structs and emits Graphviz DOT and Mermaid source. It
//! has **no** dependency on `dagr-core` — that independence is a crate-graph fact
//! (arch.md C24 "rendering requires no access to the binary that produced the
//! artifacts"), asserted structurally by this crate's manifest, and exercised
//! here by rendering artifacts alone with no producing binary linked.
//!
//! The schema-validity of the 30-node fixture (that it conforms to the published
//! `schemas/graph/v1.schema.json`) is proven by the sibling
//! `fixture_schema_valid.rs`, gated behind the `schema-validation` feature so the
//! CI-/dev-scoped `jsonschema` validator is pulled only by CI's dedicated step.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use dagr_render::{render_dot, render_mermaid, GraphArtifact, RenderError};

// === Fixture loading =========================================================

/// Repo-root-relative path to a checked-in render fixture.
fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

/// The genuine T40-emitted graph artifact from the shared corpus
/// (`tests/fixtures/corpus/graph/v1/t40-three-node.json`, produced by the real
/// C20 emitter and kept equal to a live emission by T40's own suite). Rendering
/// this proves the renderer consumes **real emitter output**, not a hand-faked
/// artifact.
fn t40_corpus_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(Path::parent) // <workspace root>
        .unwrap()
        .join("tests/fixtures/corpus/graph/v1/t40-three-node.json")
}

fn load(name: &str) -> GraphArtifact {
    let raw = std::fs::read_to_string(fixture_path(name)).expect("fixture readable");
    GraphArtifact::from_json_str(&raw).expect("fixture is a schema-shaped graph artifact")
}

/// The checked-in 30-node fixture: multiple groups, an ungrouped node, data and
/// ordering edges, and a node carrying both a data dependency and an ordering
/// edge.
fn thirty_node() -> GraphArtifact {
    load("thirty-node.graph.json")
}

// === Structural parsing helpers over the emitted diagram source ==============

/// Collect DOT node-declaration identities (the quoted label on each declared
/// node line). A node declaration in our DOT is a line of the form
/// `    "name" [label="name", ...];` inside or outside a cluster. We identify a
/// declaration line by the presence of a `label=` attribute (edges never carry a
/// `label=` unless they are data edges, which are always `->` lines).
fn dot_node_labels(dot: &str) -> Vec<String> {
    dot.lines()
        .filter(|l| !l.contains("->"))
        .filter_map(|l| {
            let l = l.trim();
            // A node line: `"id" [ ... label="LABEL" ... ];`
            let label_key = "label=\"";
            let idx = l.find(label_key)?;
            // Only lines that also start with a quoted id are node declarations.
            if !l.starts_with('"') {
                return None;
            }
            let rest = &l[idx + label_key.len()..];
            let end = rest.find('"')?;
            Some(rest[..end].to_string())
        })
        .collect()
}

/// Collect DOT edge lines (`"from" -> "to" [ ... ];`) as `(from, to, attrs)`.
fn dot_edges(dot: &str) -> Vec<(String, String, String)> {
    dot.lines()
        .filter(|l| l.contains("->"))
        .filter_map(|l| {
            let l = l.trim();
            let arrow = l.find("->")?;
            let from = l[..arrow].trim().trim_matches('"').to_string();
            let after = &l[arrow + 2..];
            // target is the next quoted token
            let after = after.trim_start();
            if !after.starts_with('"') {
                return None;
            }
            let rest = &after[1..];
            let end = rest.find('"')?;
            let to = rest[..end].to_string();
            let attrs = rest[end + 1..].to_string();
            Some((from, to, attrs))
        })
        .collect()
}

/// Collect Mermaid node-declaration identities. In our Mermaid a node is declared
/// as `    id["name"]` (a bracketed node with an escaped label). We collect the
/// quoted label text, deduplicated in first-seen order.
fn mermaid_node_labels(mmd: &str) -> Vec<String> {
    let mut seen = Vec::new();
    for l in mmd.lines() {
        let l = l.trim();
        // A node declaration line: `id["LABEL"]` with no arrow.
        if l.contains("-->") || l.contains("-.->") {
            continue;
        }
        if let Some(open) = l.find("[\"") {
            let rest = &l[open + 2..];
            if let Some(end) = rest.find("\"]") {
                let label = rest[..end].to_string();
                if !seen.contains(&label) {
                    seen.push(label);
                }
            }
        }
    }
    seen
}

// === Test plan ===============================================================

/// **Every node appears (DOT).** The DOT output declares exactly one node for
/// each of the 30 artifact nodes, each carrying its stable declared name; none
/// missing, none invented — verified structurally, not by string matching.
#[test]
fn every_node_appears_in_dot() {
    let art = thirty_node();
    let dot = render_dot(&art);

    let declared: Vec<String> = dot_node_labels(&dot);
    let declared_set: BTreeSet<&str> = declared.iter().map(String::as_str).collect();
    let expected: BTreeSet<&str> = art.nodes().iter().map(|n| n.name()).collect();

    assert_eq!(
        declared.len(),
        art.nodes().len(),
        "expected exactly one DOT node declaration per artifact node"
    );
    assert_eq!(
        declared_set, expected,
        "DOT node identities must equal the artifact node set one-to-one"
    );
}

/// **Every node appears (Mermaid).** Exactly one Mermaid node declaration per
/// artifact node, its stable declared name; count and identity match one-to-one.
#[test]
fn every_node_appears_in_mermaid() {
    let art = thirty_node();
    let mmd = render_mermaid(&art);

    let declared = mermaid_node_labels(&mmd);
    let declared_set: BTreeSet<&str> = declared.iter().map(String::as_str).collect();
    let expected: BTreeSet<&str> = art.nodes().iter().map(|n| n.name()).collect();

    assert_eq!(
        declared.len(),
        art.nodes().len(),
        "expected exactly one Mermaid node declaration per artifact node"
    );
    assert_eq!(declared_set, expected);
}

/// **Every edge appears (both formats).** Each output contains exactly one edge
/// per artifact edge, connecting the correct source and target; edge count equals
/// the artifact edge count in both formats.
#[test]
fn every_edge_appears_in_both_formats() {
    let art = thirty_node();
    let dot = render_dot(&art);
    let mmd = render_mermaid(&art);

    let dot_es = dot_edges(&dot);
    assert_eq!(
        dot_es.len(),
        art.edges().len(),
        "DOT edge count must equal the artifact edge count"
    );
    let dot_pairs: BTreeSet<(String, String)> = dot_es
        .iter()
        .map(|(f, t, _)| (f.clone(), t.clone()))
        .collect();
    let expected_pairs: BTreeSet<(String, String)> = art
        .edges()
        .iter()
        .map(|e| (e.from().to_string(), e.to().to_string()))
        .collect();
    assert_eq!(
        dot_pairs, expected_pairs,
        "DOT edges must connect the same node pairs as the artifact"
    );

    // Mermaid edge count: count arrow-bearing lines.
    let mmd_edge_lines = mmd
        .lines()
        .filter(|l| l.contains("-->") || l.contains("-.->"))
        .count();
    assert_eq!(
        mmd_edge_lines,
        art.edges().len(),
        "Mermaid edge count must equal the artifact edge count"
    );
}

/// **Data vs ordering edges are styled distinctly (DOT).** Every data edge
/// carries one documented style and every ordering edge a different one; the two
/// style sets are disjoint; a node bearing both shows each edge in its own style.
#[test]
fn data_and_ordering_edges_styled_distinctly_in_dot() {
    let art = thirty_node();
    let dot = render_dot(&art);
    let dot_es = dot_edges(&dot);

    // Map artifact edges by (from,to) → kind. Note the 30-node fixture has no
    // parallel edges of different kinds between the same pair.
    let mut data_pairs = BTreeSet::new();
    let mut ordering_pairs = BTreeSet::new();
    for e in art.edges() {
        match e.kind() {
            dagr_render::EdgeKind::Data => {
                data_pairs.insert((e.from().to_string(), e.to().to_string()));
            }
            dagr_render::EdgeKind::Ordering => {
                ordering_pairs.insert((e.from().to_string(), e.to().to_string()));
            }
        }
    }

    let mut data_styles = BTreeSet::new();
    let mut ordering_styles = BTreeSet::new();
    for (f, t, attrs) in &dot_es {
        let key = (f.clone(), t.clone());
        // The documented DOT style discriminator is the `style=` attribute:
        // data edges are solid, ordering edges are dashed.
        let style = if attrs.contains("style=solid") {
            "solid"
        } else if attrs.contains("style=dashed") {
            "dashed"
        } else {
            panic!("edge {f}->{t} carries neither documented style: {attrs}");
        };
        if data_pairs.contains(&key) {
            data_styles.insert(style);
        } else if ordering_pairs.contains(&key) {
            ordering_styles.insert(style);
        }
    }

    assert_eq!(
        data_styles,
        BTreeSet::from(["solid"]),
        "every data edge must be solid"
    );
    assert_eq!(
        ordering_styles,
        BTreeSet::from(["dashed"]),
        "every ordering edge must be dashed"
    );
    assert!(
        data_styles.is_disjoint(&ordering_styles),
        "data and ordering DOT styles must be disjoint"
    );

    // The both-edges node: publish_05 has data edges in and an ordering edge in.
    let p5_data: Vec<_> = dot_es
        .iter()
        .filter(|(_, t, _)| t == "publish_05")
        .filter(|(f, t, _)| data_pairs.contains(&(f.clone(), t.clone())))
        .collect();
    let p5_ord: Vec<_> = dot_es
        .iter()
        .filter(|(_, t, _)| t == "publish_05")
        .filter(|(f, t, _)| ordering_pairs.contains(&(f.clone(), t.clone())))
        .collect();
    assert!(!p5_data.is_empty(), "publish_05 must have a data edge in DOT");
    assert!(
        !p5_ord.is_empty(),
        "publish_05 must have an ordering edge in DOT"
    );
    assert!(p5_data.iter().all(|(_, _, a)| a.contains("style=solid")));
    assert!(p5_ord.iter().all(|(_, _, a)| a.contains("style=dashed")));
}

/// **Data vs ordering edges are styled distinctly (Mermaid).** Data and ordering
/// edges use distinct, documented Mermaid link forms; the two forms are disjoint,
/// each edge uses the form matching its recorded kind.
#[test]
fn data_and_ordering_edges_styled_distinctly_in_mermaid() {
    let art = thirty_node();
    let mmd = render_mermaid(&art);

    let data_count = art
        .edges()
        .iter()
        .filter(|e| e.kind() == dagr_render::EdgeKind::Data)
        .count();
    let ordering_count = art
        .edges()
        .iter()
        .filter(|e| e.kind() == dagr_render::EdgeKind::Ordering)
        .count();

    // Documented Mermaid forms: data edges are SOLID (`-->`, possibly labelled);
    // ordering edges are DASHED (`-.->`). The two forms are lexically disjoint.
    let dashed_lines: Vec<&str> = mmd.lines().filter(|l| l.contains("-.->")).collect();
    // A solid data-edge line contains `-->` but NOT `-.->`.
    let solid_lines: Vec<&str> = mmd
        .lines()
        .filter(|l| l.contains("-->") && !l.contains("-.->"))
        .collect();

    assert_eq!(
        dashed_lines.len(),
        ordering_count,
        "every ordering edge must render as a dashed Mermaid link (`-.->`)"
    );
    assert_eq!(
        solid_lines.len(),
        data_count,
        "every data edge must render as a solid Mermaid link (`-->`)"
    );

    // publish_05: a data link in AND a dashed ordering link in.
    let p5_dashed = dashed_lines.iter().any(|l| l.contains("publish_05"));
    let p5_solid = solid_lines
        .iter()
        .any(|l| l.contains("--> publish_05") || l.contains("--> publish_05"));
    assert!(
        p5_dashed,
        "publish_05 must have a dashed ordering link in Mermaid"
    );
    assert!(p5_solid, "publish_05 must have a solid data link in Mermaid");
}

/// **Carried type name on data edges.** Each data edge is labelled with the
/// carried stable type name from the artifact; ordering edges carry no type
/// label. Checked in both formats.
#[test]
fn data_edges_labelled_with_carried_type_ordering_unlabelled() {
    let art = thirty_node();
    let dot = render_dot(&art);
    let mmd = render_mermaid(&art);

    for e in art.edges() {
        match e.kind() {
            dagr_render::EdgeKind::Data => {
                let ty = e.type_name().expect("data edge carries a type name");
                // DOT: the data edge line carries `label="<ty>"`.
                let dot_line = dot
                    .lines()
                    .find(|l| {
                        l.contains(&format!("\"{}\" -> \"{}\"", e.from(), e.to()))
                    })
                    .unwrap_or_else(|| panic!("DOT missing data edge {}->{}", e.from(), e.to()));
                assert!(
                    dot_line.contains(&format!("label=\"{ty}\"")),
                    "DOT data edge {}->{} must be labelled with `{ty}`: {dot_line}",
                    e.from(),
                    e.to()
                );
                // Mermaid: the link carries the type text.
                let mmd_line = mmd
                    .lines()
                    .find(|l| {
                        l.contains(&format!("{} --", e.from())) && l.contains(e.to())
                    })
                    .unwrap_or_else(|| {
                        panic!("Mermaid missing data edge {}->{}", e.from(), e.to())
                    });
                assert!(
                    mmd_line.contains(ty),
                    "Mermaid data edge {}->{} must carry `{ty}`: {mmd_line}",
                    e.from(),
                    e.to()
                );
            }
            dagr_render::EdgeKind::Ordering => {
                assert!(
                    e.type_name().is_none(),
                    "an ordering edge carries no type name"
                );
                // DOT: ordering edge line carries no `label=`.
                let dot_line = dot
                    .lines()
                    .find(|l| l.contains(&format!("\"{}\" -> \"{}\"", e.from(), e.to())))
                    .unwrap_or_else(|| panic!("DOT missing ordering edge {}->{}", e.from(), e.to()));
                assert!(
                    !dot_line.contains("label="),
                    "DOT ordering edge {}->{} must carry no label: {dot_line}",
                    e.from(),
                    e.to()
                );
            }
        }
    }
}

/// **Groups render as clusters (DOT).** Each group renders as a subgraph cluster
/// containing exactly the nodes labelled with that group; the ungrouped nodes sit
/// outside every cluster; groups do not nest.
#[test]
fn groups_render_as_clusters_in_dot() {
    let art = thirty_node();
    let dot = render_dot(&art);

    // Expected group → member set from the artifact.
    let mut groups: std::collections::BTreeMap<String, BTreeSet<String>> = Default::default();
    let mut ungrouped: BTreeSet<String> = BTreeSet::new();
    for n in art.nodes() {
        if n.group().is_empty() {
            ungrouped.insert(n.name().to_string());
        } else {
            groups
                .entry(n.group().to_string())
                .or_default()
                .insert(n.name().to_string());
        }
    }
    assert!(groups.len() >= 3, "fixture must have at least three groups");
    assert!(!ungrouped.is_empty(), "fixture must have an ungrouped node");

    // Parse clusters: `subgraph "cluster_<group>" { ... }`. We walk lines,
    // tracking cluster nesting depth. Any node declared at depth>0 belongs to the
    // innermost open cluster; groups must NOT nest (max depth 1).
    let mut depth: usize = 0;
    let mut max_depth: usize = 0;
    let mut current_cluster: Vec<String> = Vec::new();
    let mut cluster_members: std::collections::BTreeMap<String, BTreeSet<String>> =
        Default::default();
    let mut nodes_in_a_cluster: BTreeSet<String> = BTreeSet::new();

    for raw in dot.lines() {
        let l = raw.trim();
        if let Some(rest) = l.strip_prefix("subgraph ") {
            depth += 1;
            max_depth = max_depth.max(depth);
            // cluster id: `"cluster_<group>" {`
            let id = rest.trim_end_matches('{').trim().trim_matches('"');
            let group = id.strip_prefix("cluster_").unwrap_or(id).to_string();
            current_cluster.push(group);
            continue;
        }
        if l == "}" && depth > 0 {
            current_cluster.pop();
            depth -= 1;
            continue;
        }
        // A node declaration line inside a cluster.
        if depth > 0 && l.starts_with('"') && l.contains("label=\"") && !l.contains("->") {
            // extract the node id (first quoted token)
            let rest = &l[1..];
            if let Some(end) = rest.find('"') {
                let id = rest[..end].to_string();
                if let Some(group) = current_cluster.last() {
                    cluster_members
                        .entry(group.clone())
                        .or_default()
                        .insert(id.clone());
                    nodes_in_a_cluster.insert(id);
                }
            }
        }
    }

    assert_eq!(max_depth, 1, "DOT groups must not nest (C6)");
    assert_eq!(
        cluster_members, groups,
        "each DOT cluster must contain exactly its group's members"
    );
    // Ungrouped nodes are outside every cluster.
    for u in &ungrouped {
        assert!(
            !nodes_in_a_cluster.contains(u),
            "ungrouped node {u} must sit outside every DOT cluster"
        );
    }
}

/// **Groups render as clusters (Mermaid).** Each group renders as a Mermaid
/// subgraph containing exactly its members; the ungrouped node is outside all
/// subgraphs.
#[test]
fn groups_render_as_clusters_in_mermaid() {
    let art = thirty_node();
    let mmd = render_mermaid(&art);

    let mut groups: std::collections::BTreeMap<String, BTreeSet<String>> = Default::default();
    let mut ungrouped: BTreeSet<String> = BTreeSet::new();
    for n in art.nodes() {
        if n.group().is_empty() {
            ungrouped.insert(n.name().to_string());
        } else {
            groups
                .entry(n.group().to_string())
                .or_default()
                .insert(n.name().to_string());
        }
    }

    // Walk `subgraph <id>[ ... ]` ... `end` blocks. Members are node declarations
    // (`id["label"]`) inside a block. Mermaid subgraphs must not nest here.
    let mut depth: usize = 0;
    let mut max_depth: usize = 0;
    let mut stack: Vec<String> = Vec::new();
    let mut members: std::collections::BTreeMap<String, BTreeSet<String>> = Default::default();
    let mut nodes_in_a_subgraph: BTreeSet<String> = BTreeSet::new();

    for raw in mmd.lines() {
        let l = raw.trim();
        if let Some(rest) = l.strip_prefix("subgraph ") {
            depth += 1;
            max_depth = max_depth.max(depth);
            // id up to `[` or whitespace
            let id: String = rest
                .chars()
                .take_while(|c| *c != '[' && !c.is_whitespace())
                .collect();
            let group = id.strip_prefix("group_").unwrap_or(&id).to_string();
            stack.push(group);
            continue;
        }
        if l == "end" && depth > 0 {
            stack.pop();
            depth -= 1;
            continue;
        }
        if depth > 0 && !l.contains("-->") && !l.contains("-.->") {
            if let Some(open) = l.find("[\"") {
                let id: String = l[..open].trim().to_string();
                if !id.is_empty() {
                    if let Some(group) = stack.last() {
                        members.entry(group.clone()).or_default().insert(id.clone());
                        nodes_in_a_subgraph.insert(id);
                    }
                }
            }
        }
    }

    assert_eq!(max_depth, 1, "Mermaid subgraphs must not nest (C6)");
    assert_eq!(
        members, groups,
        "each Mermaid subgraph must contain exactly its group's members"
    );
    for u in &ungrouped {
        assert!(
            !nodes_in_a_subgraph.contains(u),
            "ungrouped node {u} must sit outside every Mermaid subgraph"
        );
    }
}

/// **Golden DOT is stable.** The 30-node fixture renders byte-identically to the
/// checked-in golden `.dot`.
#[test]
fn golden_dot_is_byte_stable() {
    let art = thirty_node();
    let dot = render_dot(&art);
    let golden = std::fs::read_to_string(fixture_path("thirty-node.golden.dot"))
        .expect("golden .dot exists");
    assert_eq!(
        dot, golden,
        "rendered DOT must be byte-identical to the golden; bless with \
         DAGR_BLESS=1 if the change is intended"
    );
}

/// **Golden Mermaid is stable.** The 30-node fixture renders byte-identically to
/// the checked-in golden `.mmd`.
#[test]
fn golden_mermaid_is_byte_stable() {
    let art = thirty_node();
    let mmd = render_mermaid(&art);
    let golden = std::fs::read_to_string(fixture_path("thirty-node.golden.mmd"))
        .expect("golden .mmd exists");
    assert_eq!(
        mmd, golden,
        "rendered Mermaid must be byte-identical to the golden; bless with \
         DAGR_BLESS=1 if the change is intended"
    );
}

/// **Determinism: rendering is idempotent and byte-stable.** Rendering the same
/// artifact twice yields byte-identical output (stable node/edge/cluster
/// ordering), independent of artifact node/edge input order.
#[test]
fn rendering_is_byte_stable_across_repetitions() {
    let art = thirty_node();
    assert_eq!(render_dot(&art), render_dot(&art));
    assert_eq!(render_mermaid(&art), render_mermaid(&art));
}

/// **Renders a historical artifact with no producing binary present.** Handing
/// the renderer a fixture artifact alone succeeds; the crate links no pipeline
/// crate (a crate-graph fact). Uses the genuine T40-emitted corpus artifact.
#[test]
fn renders_a_real_t40_emitted_artifact_with_no_producing_binary() {
    let raw = std::fs::read_to_string(t40_corpus_path()).expect("t40 corpus fixture readable");
    let art = GraphArtifact::from_json_str(&raw).expect("t40 corpus is a valid graph artifact");

    // It is a REAL emission (3 nodes, 2 data edges) from the C20 emitter.
    assert_eq!(art.nodes().len(), 3);
    assert_eq!(art.edges().len(), 2);

    let dot = render_dot(&art);
    let mmd = render_mermaid(&art);

    // Every real node appears in both outputs.
    for n in art.nodes() {
        assert!(dot_node_labels(&dot).iter().any(|l| l == n.name()));
        assert!(mermaid_node_labels(&mmd).iter().any(|l| l == n.name()));
    }
    // Both data edges appear.
    assert_eq!(dot_edges(&dot).len(), 2);
}

/// **Rejects a schema-invalid artifact.** An artifact missing a required field
/// (the node's `output_type_name`) is refused with a diagnostic naming the
/// problem, rather than producing partial or misleading diagram source.
#[test]
fn rejects_a_schema_invalid_artifact() {
    let raw = std::fs::read_to_string(fixture_path("schema-invalid.graph.json"))
        .expect("invalid fixture readable");
    let err = GraphArtifact::from_json_str(&raw)
        .expect_err("a schema-invalid artifact must be rejected");
    match err {
        RenderError::Malformed(msg) => {
            assert!(
                msg.contains("output_type_name"),
                "the diagnostic must name the missing field, got: {msg}"
            );
        }
        other => panic!("expected a Malformed diagnostic, got {other:?}"),
    }
}

/// **Stable declared names only.** A node whose informational `type_name` debug
/// field differs from its stable declared name renders with the stable declared
/// name; the unstable `type_name` never appears as a node identity or label.
#[test]
fn renders_stable_declared_names_never_type_name() {
    let art = load("stable-names.graph.json");
    let dot = render_dot(&art);
    let mmd = render_mermaid(&art);

    // The unstable debug type name string must never appear anywhere in either
    // output (not as an id, not as a label).
    let unstable = "UnstableWidgetImpl";
    assert!(
        !dot.contains(unstable),
        "the unstable type_name must not appear in DOT"
    );
    assert!(
        !mmd.contains(unstable),
        "the unstable type_name must not appear in Mermaid"
    );

    // The stable declared node names DO appear.
    for n in art.nodes() {
        assert!(dot.contains(&format!("label=\"{}\"", n.name())));
        assert!(mmd.contains(&format!("[\"{}\"]", n.name())));
    }
}
