//! `dagr-render` — dagr's diagram renderer (arch.md `### C24 · Renderers`).
//!
//! Given **one graph artifact** (C20, produced by T40, schematized by T39), this
//! crate emits diagram source a human can read without hand-layout: **Graphviz
//! DOT** ([`render_dot`]) and **Mermaid** ([`render_mermaid`]). Both outputs
//! include every node and every edge, style **data** edges distinctly from
//! **ordering** edges, label data edges with the carried stable type name, and
//! cluster nodes by group (C6).
//!
//! This is the **base** renderer (T46). The run-artifact **overlay** — colouring
//! nodes by terminal state, distinguishing originated from propagated skips, and
//! annotating durations — is a separate concern layered on top by T47; it is not
//! part of this crate's surface.
//!
//! # Artifacts only — no access to the producing binary (C24)
//!
//! `dagr-render` depends on [`dagr-artifact`](../dagr_artifact/index.html) and
//! the sanctioned `serde`/`serde_json` reader stack, and on **nothing else** in
//! the workspace — in particular it has **no** dependency edge onto `dagr-core`,
//! the live-pipeline surface. Because that edge does not exist, no code here
//! *can* reference a live-pipeline type, so "rendering requires no access to the
//! binary that produced the artifacts" (arch.md C24 line 523) is a property of
//! the crate graph rather than a convention. A renderer therefore works equally
//! on a historical run from three months ago: it reads the published artifact
//! schema and nothing else — no network, no credentials, no filesystem access
//! beyond the artifact it is handed.
//!
//! # Reading an artifact
//!
//! [`GraphArtifact::from_json_str`] parses a published C20 graph-artifact JSON
//! document into the read-only [`GraphArtifact`] view. The required fields the
//! diagram depends on are *required* on the parsed structs, so an artifact that
//! fails the schema — e.g. a node missing its required `output_type_name` — is
//! **rejected** with a [`RenderError`] naming the problem, rather than producing
//! partial or misleading diagram source (arch.md C24). Unknown future fields are
//! ignored (additive-only schema evolution, T0.10), so a newer artifact still
//! renders.
//!
//! # The documented, disjoint style contract (C4 / C24)
//!
//! The two edge kinds and the group clustering are drawn with a **fixed,
//! documented** treatment, so downstream consumers (the T47 overlay, T51 groups,
//! and T55's `render` verb) can rely on it:
//!
//! | element         | DOT                                   | Mermaid                    |
//! |-----------------|---------------------------------------|----------------------------|
//! | data edge       | `style=solid, arrowhead=normal`, `label` = carried type | `-- "Type" -->` (solid) |
//! | ordering edge   | `style=dashed, arrowhead=empty`, no label | `-.->` (dashed), no label |
//! | group           | `subgraph "cluster_<group>"`          | `subgraph group_<group>`   |
//! | ungrouped node  | top-level, outside every cluster      | top-level, outside every subgraph |
//!
//! The data-edge and ordering-edge style sets are **disjoint** in both formats
//! (solid vs dashed), and an ordering edge carries no value label (C4 line 144).
//! Groups do not nest (C6 line 170). Full per-format details are in the
//! [`dot`] and [`mermaid`] module docs.
//!
//! # Determinism
//!
//! Both renderers are **deterministic and byte-stable**, independent of the
//! artifact's node/edge input order: clusters/subgraphs are emitted in
//! group-name order, nodes in identity-name order, and edges in canonical
//! `(from, to, kind)` order. Byte-identity is pinned by golden-file tests, and
//! the two output formats are accepted by their reference tools (`dot`, Mermaid's
//! parser) in CI.

use std::fmt;

pub mod dot;
pub mod mermaid;
pub mod model;
pub mod overlay;

pub use model::{Edge, EdgeKind, GraphArtifact, Node};

/// Render a [`GraphArtifact`] to **Graphviz DOT** source (arch.md C24).
/// Deterministic and byte-stable; parseable by the `dot` reference tool. See the
/// [`dot`] module for the exact format and the documented edge/cluster styling.
#[must_use]
pub fn render_dot(artifact: &GraphArtifact) -> String {
    dot::render(artifact)
}

/// Render a [`GraphArtifact`] to **Mermaid** flowchart source (arch.md C24).
/// Deterministic and byte-stable; accepted by Mermaid's parser. See the
/// [`mermaid`] module for the exact format and the documented link/subgraph
/// styling.
#[must_use]
pub fn render_mermaid(artifact: &GraphArtifact) -> String {
    mermaid::render(artifact)
}

impl GraphArtifact {
    /// Parse a published **C20 graph-artifact** JSON document (T39 schema) into a
    /// read-only [`GraphArtifact`] (arch.md C24 — the renderer consumes the
    /// published artifact and nothing else).
    ///
    /// This is the renderer's schema gate: the fields the diagram depends on are
    /// required, so an artifact that fails the schema (e.g. a node missing its
    /// required `output_type_name`, or an edge missing `from`/`to`/`kind`) is
    /// refused with a diagnostic naming the problem, rather than rendering
    /// partially. Unknown future fields are ignored (additive-only evolution).
    ///
    /// # Errors
    ///
    /// Returns [`RenderError::Malformed`] if the input is not valid JSON, or does
    /// not match the graph-artifact shape (a missing required field, or a field
    /// of the wrong type). The message names the offending field/reason.
    pub fn from_json_str(json: &str) -> Result<Self, RenderError> {
        serde_json::from_str(json).map_err(|e| RenderError::Malformed(e.to_string()))
    }
}

/// A failure to read or render a graph artifact (arch.md C24).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderError {
    /// The input is not a schema-shaped C20 graph artifact — not valid JSON, or a
    /// required field is missing or of the wrong type. The wrapped message names
    /// the field/reason (from the deserializer), so a schema-invalid artifact is
    /// rejected with an actionable diagnostic rather than rendered partially.
    Malformed(String),
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(msg) => write!(
                f,
                "not a valid C20 graph artifact (does not conform to the published \
                 schemas/graph/v1.schema.json): {msg}"
            ),
        }
    }
}

impl std::error::Error for RenderError {}
