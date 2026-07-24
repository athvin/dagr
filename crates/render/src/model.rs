//! The typed, read-only view of a **C20 graph artifact** the renderer consumes
//! (arch.md `### C20 · Graph artifact`; the published schema
//! `schemas/graph/v1.schema.json`, T39).
//!
//! The renderer is a pure *reader*: it deserializes the published artifact JSON
//! into these structs and emits diagram source. It never re-serializes the
//! artifact and never mutates it. Deserialization is the renderer's schema gate —
//! the required fields the diagram depends on (a node's stable `name`, its
//! `group`, its `output_type_name`; an edge's `from`, `to`, `kind`, and, for a
//! data edge, its carried `type_name`) are *required* on these structs, so an
//! artifact missing one is refused with a diagnostic naming the field
//! ([`GraphArtifact::from_json_str`]) rather than producing partial or misleading
//! diagram source (arch.md C24 "reject … with a clear diagnostic"). Unknown
//! future fields are ignored (schema evolution is additive-only within a version,
//! T0.10), so a newer artifact still renders.
//!
//! # Only artifact fields the diagram needs
//!
//! These structs deliberately model **only** the fields the base renderer draws:
//! node identity + group, and edge endpoints + kind + carried type. The full
//! policy, resources, fingerprints, and provenance the artifact also carries are
//! not modelled here — they are not part of the C24 base diagram (the run overlay
//! that colours by terminal state and annotates duration is T47, a separate
//! artifact and a separate concern). `serde`'s default "ignore unknown fields"
//! posture makes that omission safe and forward-compatible.

use serde::Deserialize;

/// A parsed, read-only **C20 graph artifact** — the renderer's sole input
/// (arch.md C24 "renderers consume artifacts only").
///
/// Obtain one with [`GraphArtifact::from_json_str`]. It exposes only what the
/// diagram draws: the node set (identity + group) and the edge set (endpoints,
/// kind, carried type).
#[derive(Debug, Clone, Deserialize)]
pub struct GraphArtifact {
    #[serde(default)]
    nodes: Vec<Node>,
    #[serde(default)]
    edges: Vec<Edge>,
}

impl GraphArtifact {
    /// The artifact's nodes, in the artifact's own (already canonical, T40)
    /// order. The renderer sorts by identity name itself, so callers need not
    /// rely on input order.
    #[must_use]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The artifact's edges, in the artifact's own order. The renderer sorts by
    /// `(from, to, kind)` itself.
    #[must_use]
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }
}

/// One node of the graph artifact — its stable identity `name`, its `group`
/// label (the empty string when ungrouped, C6), and its stable declared
/// `output_type_name` (C20 identity, never the informational `type_name`).
///
/// The base renderer draws the identity name and the group association; the
/// declared type/task names and policy the artifact also carries are not part of
/// the base diagram.
#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    name: String,
    #[serde(default)]
    group: String,
    /// The stable declared output type name (required by the schema — its
    /// presence is what makes a node emittable). Held so the renderer can never
    /// mistake the informational `type_name` debug field for identity; the base
    /// diagram labels nodes by `name`, not by this.
    output_type_name: String,
}

impl Node {
    /// The node's stable identity name — the label the diagram draws (never the
    /// informational `type_name` debug field, arch.md C20 line 439).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The node's group label, or the empty string when it is ungrouped (C6). An
    /// ungrouped node is drawn outside every cluster.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The node's stable declared output type name (C20). Not drawn by the base
    /// diagram, but held so identity is always the stable name.
    #[must_use]
    pub fn output_type_name(&self) -> &str {
        &self.output_type_name
    }
}

/// The kind of a graph edge: a **data** dependency (a value flows along it, C3)
/// or an **ordering** dependency (sequence only, no value, C4). The two are
/// recorded distinctly in the artifact and drawn distinctly in the diagram
/// (arch.md C4 line 143).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeKind {
    /// A data dependency — carries the stable name of the type it carries.
    Data,
    /// An ordering dependency — carries no value and no type label (C4 line 144).
    Ordering,
}

/// One edge of the graph artifact: its `from` (source) and `to` (target)
/// identity names, its [`EdgeKind`], and, for a data edge only, the stable name
/// of the type it carries (`type_name`).
#[derive(Debug, Clone, Deserialize)]
pub struct Edge {
    from: String,
    to: String,
    kind: EdgeKind,
    /// The carried stable type name — present (and required by the schema) for a
    /// data edge, absent for an ordering edge.
    #[serde(default)]
    type_name: Option<String>,
}

impl Edge {
    /// The source (producer) node identity name.
    #[must_use]
    pub fn from(&self) -> &str {
        &self.from
    }

    /// The target (consumer / ordered-after) node identity name.
    #[must_use]
    pub fn to(&self) -> &str {
        &self.to
    }

    /// The edge kind (data vs ordering).
    #[must_use]
    pub fn kind(&self) -> EdgeKind {
        self.kind
    }

    /// The carried stable type name for a data edge; `None` for an ordering edge
    /// (which carries no value, C4 line 144).
    #[must_use]
    pub fn type_name(&self) -> Option<&str> {
        self.type_name.as_deref()
    }
}
