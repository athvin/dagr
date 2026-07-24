//! Graphviz **DOT** emission for a C20 graph artifact (arch.md `### C24 ·
//! Renderers`; the `dot` reference tool gates this format in CI).
//!
//! The output is a single `digraph` with:
//!
//! * one `node` declaration per artifact node, drawn with its **stable declared
//!   name** as both id and `label` (never the informational `type_name`, C20);
//! * one edge per artifact edge, `"from" -> "to"`, in canonical
//!   `(from, to, kind)` order;
//! * each **group** rendered as a **DOT subgraph cluster** (`subgraph
//!   "cluster_<group>"`) containing exactly its member nodes; **ungrouped** nodes
//!   (empty group label) declared at the top level, outside every cluster; groups
//!   never nest (C6).
//!
//! # Documented, disjoint edge styling (C4 line 143 / C24 line 521)
//!
//! The two edge kinds carry **disjoint** `style` attributes, so a reader — and
//! the structural tests — can tell them apart from the DOT alone:
//!
//! | kind     | `style`  | arrowhead | label                       |
//! |----------|----------|-----------|-----------------------------|
//! | data     | `solid`  | `normal`  | the carried stable type name |
//! | ordering | `dashed` | `empty`   | *(none — carries no value)* |
//!
//! A data edge is labelled with the stable name of the type it carries; an
//! ordering edge carries **no** label (C4 line 144). Downstream T47 (run overlay)
//! layers colour/annotation on top of this base and relies on these edge-kind
//! and cluster distinctions staying fixed.
//!
//! # Determinism (C24 golden files)
//!
//! Output is byte-stable and independent of the artifact's node/edge input order:
//! clusters are emitted in group-name order, nodes within a cluster and the
//! ungrouped nodes in identity-name order, and edges in `(from, to, kind)` order.

use std::fmt::Write as _;

use crate::model::{Edge, EdgeKind, GraphArtifact, Node};

/// Render `artifact` to Graphviz DOT source (arch.md C24). Deterministic and
/// byte-stable; parseable by the `dot` reference tool.
#[must_use]
pub fn render(artifact: &GraphArtifact) -> String {
    let mut out = String::new();

    // A stable, human-legible header comment; then the digraph and its
    // rank/spacing defaults (layout is left to `dot` — no hand-layout, C24).
    out.push_str("// Rendered by dagr-render (C24) from a graph artifact.\n");
    out.push_str("// Data edges: solid, labelled with the carried type. ");
    out.push_str("Ordering edges: dashed, unlabelled.\n");
    out.push_str("digraph pipeline {\n");
    out.push_str("  rankdir=TB;\n");
    out.push_str("  node [shape=box];\n");

    // Partition nodes into groups (sorted by group name) and ungrouped, both in
    // identity-name order for determinism.
    let mut nodes: Vec<&Node> = artifact.nodes().iter().collect();
    nodes.sort_by(|a, b| a.name().cmp(b.name()));

    // Group name -> members (already name-sorted because `nodes` is sorted).
    let mut group_names: Vec<&str> = nodes
        .iter()
        .map(|n| n.group())
        .filter(|g| !g.is_empty())
        .collect();
    group_names.sort_unstable();
    group_names.dedup();

    // Emit one cluster per group, members in name order.
    for group in &group_names {
        // A cluster id must be stable and unique; `cluster_` prefix makes `dot`
        // draw the bounding box.
        let _ = writeln!(out, "  subgraph \"cluster_{}\" {{", escape_dot_id(group));
        let _ = writeln!(out, "    label=\"{}\";", escape_dot_string(group));
        for node in nodes.iter().filter(|n| n.group() == *group) {
            emit_node(&mut out, node, 4);
        }
        out.push_str("  }\n");
    }

    // Ungrouped nodes at the top level, outside every cluster.
    for node in nodes.iter().filter(|n| n.group().is_empty()) {
        emit_node(&mut out, node, 2);
    }

    // Edges in canonical (from, to, kind) order.
    let mut edges: Vec<&Edge> = artifact.edges().iter().collect();
    edges.sort_by(|a, b| {
        a.from()
            .cmp(b.from())
            .then_with(|| a.to().cmp(b.to()))
            .then_with(|| kind_ord(a.kind()).cmp(&kind_ord(b.kind())))
    });
    for edge in &edges {
        emit_edge(&mut out, edge);
    }

    out.push_str("}\n");
    out
}

/// Emit one node declaration at the given indent (spaces): `"id" [label="id"];`.
/// The id and label are both the node's stable identity name (never `type_name`).
fn emit_node(out: &mut String, node: &Node, indent: usize) {
    let pad = " ".repeat(indent);
    let id = escape_dot_string(node.name());
    let _ = writeln!(out, "{pad}\"{id}\" [label=\"{id}\"];");
}

/// Emit one edge line with its kind's documented, disjoint styling.
fn emit_edge(out: &mut String, edge: &Edge) {
    let from = escape_dot_string(edge.from());
    let to = escape_dot_string(edge.to());
    match edge.kind() {
        EdgeKind::Data => {
            // A data edge carries the stable name of the type it carries as its
            // label (C4 line 144). The schema requires the type name on a data
            // edge; a defensively-absent one degrades to no label rather than
            // panicking (the reject-path in the model already refuses a malformed
            // artifact).
            let label = edge.type_name().map(escape_dot_string).unwrap_or_default();
            let _ = writeln!(
                out,
                "  \"{from}\" -> \"{to}\" [style=solid, arrowhead=normal, label=\"{label}\"];"
            );
        }
        EdgeKind::Ordering => {
            // An ordering edge carries no value and no type label (C4 line 144).
            let _ = writeln!(
                out,
                "  \"{from}\" -> \"{to}\" [style=dashed, arrowhead=empty];"
            );
        }
    }
}

/// A total order over edge kinds for deterministic edge sorting.
fn kind_ord(kind: EdgeKind) -> u8 {
    match kind {
        EdgeKind::Data => 0,
        EdgeKind::Ordering => 1,
    }
}

/// Escape a string for use inside a double-quoted DOT string literal: backslash
/// and double-quote are escaped. Stable declared names are constrained to a safe
/// character set (T0.7), so this is belt-and-braces, but it keeps the output
/// well-formed for any schema-valid input.
fn escape_dot_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Escape a group name for use inside a DOT cluster id. Identical rules to
/// [`escape_dot_string`]; kept separate for intent at the call sites.
fn escape_dot_id(s: &str) -> String {
    escape_dot_string(s)
}
