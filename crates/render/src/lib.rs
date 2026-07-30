#![doc = include_str!("../README.md")]
//!
//! # Module index
//!
//! The orientation above comes from the crate's `README.md`, inlined here so the
//! crates.io landing page and this front page are one file. What follows is the
//! map of where each piece lives.
//!
//! The two entry points are [`render_dot`] and [`render_mermaid`]; the
//! run-artifact **overlay** — colouring nodes by terminal state, distinguishing
//! originated from propagated skips, and annotating durations — is layered on top
//! by the [`overlay`] module.
//!
//! # Reading an artifact
//!
//! [`GraphArtifact::from_json_str`] parses a published graph-artifact JSON
//! document into the read-only [`GraphArtifact`] view. The required fields the
//! diagram depends on are *required* on the parsed structs, so an artifact that
//! fails the schema — e.g. a node missing its required `output_type_name` — is
//! **rejected** with a [`RenderError`] naming the problem, rather than producing
//! partial or misleading diagram source. Unknown future fields are ignored
//! (additive-only schema evolution), so a newer artifact still renders.
//!
//! # The documented, disjoint style contract
//!
//! The two edge kinds and the group clustering are drawn with a **fixed,
//! documented** treatment, so downstream consumers (the run overlay, group
//! clustering, and the `render` verb) can rely on it:
//!
//! | element         | DOT                                   | Mermaid                    |
//! |-----------------|---------------------------------------|----------------------------|
//! | data edge       | `style=solid, arrowhead=normal`, `label` = carried type | `-- "Type" -->` (solid) |
//! | ordering edge   | `style=dashed, arrowhead=empty`, no label | `-.->` (dashed), no label |
//! | group           | `subgraph "cluster_<group>"`          | `subgraph group_<group>`   |
//! | ungrouped node  | top-level, outside every cluster      | top-level, outside every subgraph |
//!
//! The data-edge and ordering-edge style sets are **disjoint** in both formats
//! (solid vs dashed), and an ordering edge carries no value label. Groups do not
//! nest. Full per-format details are in the [`dot`] and [`mermaid`] module docs.
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

/// Render a [`GraphArtifact`] to **Graphviz DOT** source.
/// Deterministic and byte-stable; parseable by the `dot` reference tool. See the
/// [`dot`] module for the exact format and the documented edge/cluster styling.
#[must_use]
pub fn render_dot(artifact: &GraphArtifact) -> String {
    dot::render(artifact)
}

/// Render a [`GraphArtifact`] to **Mermaid** flowchart source.
/// Deterministic and byte-stable; accepted by Mermaid's parser. See the
/// [`mermaid`] module for the exact format and the documented link/subgraph
/// styling.
#[must_use]
pub fn render_mermaid(artifact: &GraphArtifact) -> String {
    mermaid::render(artifact)
}

impl GraphArtifact {
    /// Parse a published **graph-artifact** JSON document into a read-only
    /// [`GraphArtifact`] — the renderer consumes the published artifact and
    /// nothing else.
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
        serde_json::from_str(json).map_err(RenderError::Malformed)
    }
}

/// The idiomatic spelling of [`GraphArtifact::from_json_str`], added
/// **alongside** it rather than replacing it (`conv-tryfrom-fallible`,
/// `api-from-not-into`): parsing from a string is fallible, so `TryFrom<&str>` is
/// the conversion trait that fits, and it is what `.try_into()` and any generic
/// bound on `TryFrom<&str>` can reach. The named inherent reader stays, so no
/// call site changes and the parse remains greppable. Both spellings share one
/// body, so they cannot drift.
impl TryFrom<&str> for GraphArtifact {
    type Error = RenderError;

    /// # Errors
    ///
    /// Returns [`RenderError::Malformed`] exactly as
    /// [`GraphArtifact::from_json_str`] does — same input, same diagnostic.
    fn try_from(json: &str) -> Result<Self, Self::Error> {
        Self::from_json_str(json)
    }
}

/// A failure to read or render a graph artifact.
///
/// The variant **carries the deserializer's own error**, not a string copy of it.
/// That is deliberate and load-bearing: `serde_json::Error` knows the offending
/// **line and column**, and `to_string()` is a lossy projection of it. Stringifying
/// at construction destroyed the one piece of information an operator needs to fix
/// a hand-edited or truncated artifact, and destroyed it *before* any caller could
/// choose otherwise. Carrying the error costs this type its `Clone`/`PartialEq`
/// derives (`serde_json::Error` has neither) — a structural consequence of holding
/// a real cause, the same trade `io::Error`-carrying types in this workspace make.
#[derive(Debug)]
pub enum RenderError {
    /// The input is not a schema-shaped graph artifact — not valid JSON, or a
    /// required field is missing or of the wrong type. The wrapped
    /// [`serde_json::Error`] names the field/reason and its position, so a
    /// schema-invalid artifact is rejected with an actionable diagnostic rather
    /// than rendered partially.
    Malformed(serde_json::Error),
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(e) => write!(
                f,
                "not a valid C20 graph artifact (does not conform to the published \
                 schemas/graph/v1.schema.json): {e}"
            ),
        }
    }
}

impl std::error::Error for RenderError {
    /// Exposes the deserializer error the variant carries, so a caller that walks
    /// the chain reaches the line/column the `Display` form only summarizes.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Malformed(e) => Some(e),
        }
    }
}
