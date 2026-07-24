//! **Mermaid** (flowchart) emission for a C20 graph artifact (arch.md `### C24 ·
//! Renderers`; Mermaid's parser gates this format in CI).
//!
//! The output is a single `flowchart TB` with:
//!
//! * one node declaration per artifact node, `id["name"]`, drawn with its
//!   **stable declared name** as both id and bracketed label (never the
//!   informational `type_name`, C20);
//! * one link per artifact edge, in canonical `(from, to, kind)` order;
//! * each **group** rendered as a **Mermaid subgraph** (`subgraph group_<group>`)
//!   containing exactly its member nodes; **ungrouped** nodes declared at the top
//!   level, outside every subgraph; groups never nest (C6).
//!
//! # Documented, disjoint edge styling (C4 line 143 / C24 line 521)
//!
//! The two edge kinds use **disjoint** Mermaid link forms:
//!
//! | kind     | Mermaid link form           | label                        |
//! |----------|-----------------------------|------------------------------|
//! | data     | solid arrow `-- "T" -->`    | the carried stable type name |
//! | ordering | dashed arrow `-.->`         | *(none — carries no value)*  |
//!
//! A solid arrow (`-->`) and a dashed arrow (`-.->`) are lexically disjoint link
//! forms — a reader (and the structural tests) tell them apart from the Mermaid
//! alone. A data link is labelled with the carried stable type name; an ordering
//! link carries **no** label (C4 line 144).
//!
//! # Determinism (C24 golden files)
//!
//! Output is byte-stable and independent of the artifact's node/edge input order:
//! subgraphs in group-name order, nodes within a subgraph and the ungrouped nodes
//! in identity-name order, and links in `(from, to, kind)` order.

use std::fmt::Write as _;

use crate::model::{Edge, EdgeKind, GraphArtifact, Node};
use crate::overlay::{all_classes, Overlay};

/// Render `artifact` to Mermaid flowchart source (arch.md C24). Deterministic and
/// byte-stable; accepted by Mermaid's parser.
#[must_use]
pub fn render(artifact: &GraphArtifact) -> String {
    render_with_overlay(artifact, None)
}

/// Render `artifact` to Mermaid, optionally applying a **run overlay** (T47):
/// when `overlay` is `Some`, a per-state `classDef` prelude is emitted and each
/// joined node gets a `class <node> <state>` assignment plus a duration in its
/// label; the structure is identical to the base render. With `None` the output
/// is byte-for-byte the base structural diagram.
pub(crate) fn render_with_overlay(artifact: &GraphArtifact, overlay: Option<&Overlay>) -> String {
    let mut out = String::new();

    out.push_str("%% Rendered by dagr-render (C24) from a graph artifact.\n");
    out.push_str("%% Data edges: solid, labelled with the carried type. ");
    out.push_str("Ordering edges: dashed, unlabelled.\n");
    if overlay.is_some() {
        out.push_str("%% Run overlay: nodes classed by terminal state, labelled with duration.\n");
    }
    out.push_str("flowchart TB\n");

    // Overlay style prelude: one `classDef` per documented state, in a fixed
    // order (deterministic). Emitted only under an overlay so the base render is
    // byte-identical.
    if overlay.is_some() {
        for (class, style) in all_classes() {
            let _ = writeln!(
                out,
                "  classDef {class} fill:{},color:{},stroke:#333,stroke-width:1px",
                style.fillcolor, style.fontcolor
            );
        }
    }

    let mut nodes: Vec<&Node> = artifact.nodes().iter().collect();
    nodes.sort_by(|a, b| a.name().cmp(b.name()));

    let mut group_names: Vec<&str> = nodes
        .iter()
        .map(|n| n.group())
        .filter(|g| !g.is_empty())
        .collect();
    group_names.sort_unstable();
    group_names.dedup();

    // One subgraph per group, members in name order. Mermaid subgraph syntax:
    // `subgraph <id>["<title>"]` … `end`.
    for group in &group_names {
        let _ = writeln!(
            out,
            "  subgraph group_{}[\"{}\"]",
            sanitize_id(group),
            escape_mermaid_text(group)
        );
        for node in nodes.iter().filter(|n| n.group() == *group) {
            emit_node(&mut out, node, 4, overlay);
        }
        out.push_str("  end\n");
    }

    // Ungrouped nodes at the top level, outside every subgraph.
    for node in nodes.iter().filter(|n| n.group().is_empty()) {
        emit_node(&mut out, node, 2, overlay);
    }

    // Links in canonical (from, to, kind) order.
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

    // Overlay class assignments (`class <node> <state>`), in node-name order for
    // determinism; then the extra-run-record report as a trailing comment.
    if let Some(ov) = overlay {
        for (node, dec) in &ov.by_node {
            let _ = writeln!(out, "  class {} {}", sanitize_id(node), dec.class);
        }
        if !ov.extra_run_nodes.is_empty() {
            let _ = writeln!(
                out,
                "%% extra run records not in graph: {}",
                ov.extra_run_nodes.join(", ")
            );
        }
    }

    out
}

/// Emit one node declaration at the given indent: `id["name"]`. The id is the
/// sanitized identity name; the bracketed label is the exact stable identity name
/// (never `type_name`). Under an overlay with a joined state, the label also
/// carries the state tag and (when the node ran) its duration.
fn emit_node(out: &mut String, node: &Node, indent: usize, overlay: Option<&Overlay>) {
    let pad = " ".repeat(indent);
    let id = sanitize_id(node.name());
    let mut label = escape_mermaid_text(node.name());
    if let Some(dec) = overlay.and_then(|ov| ov.by_node.get(node.name())) {
        // `<br/>` is Mermaid's in-label line break; append the state tag and
        // duration below the identity name.
        label.push_str("<br/>");
        label.push_str(&escape_mermaid_text(&dec.state_tag));
        if let Some(dur) = &dec.duration {
            label.push_str("<br/>");
            label.push_str(&escape_mermaid_text(dur));
        }
    }
    let _ = writeln!(out, "{pad}{id}[\"{label}\"]");
}

/// Emit one link with its kind's documented, disjoint form.
fn emit_edge(out: &mut String, edge: &Edge) {
    let from = sanitize_id(edge.from());
    let to = sanitize_id(edge.to());
    match edge.kind() {
        EdgeKind::Data => {
            // A solid, labelled link: `from -- "Type" --> to`. The carried stable
            // type name is the link text (C4 line 144).
            let ty = edge
                .type_name()
                .map(escape_mermaid_text)
                .unwrap_or_default();
            let _ = writeln!(out, "  {from} -- \"{ty}\" --> {to}");
        }
        EdgeKind::Ordering => {
            // A dashed link, unlabelled: `from -.-> to`. Ordering edges carry no
            // value and no label (C4 line 144).
            let _ = writeln!(out, "  {from} -.-> {to}");
        }
    }
}

/// A total order over edge kinds for deterministic link sorting.
fn kind_ord(kind: EdgeKind) -> u8 {
    match kind {
        EdgeKind::Data => 0,
        EdgeKind::Ordering => 1,
    }
}

/// Sanitize an identity name into a Mermaid node id. Mermaid ids may contain only
/// a restricted character set without quoting; stable declared names (T0.7) use
/// ASCII letters, digits, and `_ - . :`, of which `-`, `.`, and `:` can confuse
/// Mermaid's id grammar, so they are mapped to `_`. The bracketed **label**
/// carries the exact original name, so the drawn identity is always the stable
/// declared name; only the internal id token is normalized. The mapping is
/// injective enough for the stable-name character set (no two distinct valid
/// names collide, because the label — not the id — is the drawn identity, and
/// links reference ids consistently).
fn sanitize_id(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

/// Escape text for a double-quoted Mermaid label/link string. Mermaid uses `#`
/// HTML-entity escapes inside quoted strings; a literal double-quote is escaped
/// as `#quot;`, and a `#` as `#35;`, so the label round-trips through Mermaid's
/// parser. Backslashes are left as-is (stable names never contain them).
fn escape_mermaid_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '#' => out.push_str("#35;"),
            '"' => out.push_str("#quot;"),
            other => out.push(other),
        }
    }
    out
}
