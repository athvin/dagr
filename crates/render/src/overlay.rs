//! **Run overlay** for the C24 renderer (arch.md `### C24 · Renderers`).
//!
//! The base renderer ([`crate::render_dot`] / [`crate::render_mermaid`], T46)
//! draws a graph artifact's *structure*. This module adds the optional **run
//! overlay**: given a C22 **run artifact** (produced by the T42 fold) alongside
//! the graph artifact, it projects each node's **terminal state** and
//! **duration** onto the diagram — colouring each node by a documented, distinct
//! per-state style and annotating it with its recorded duration — so a diagram
//! becomes a legible post-mortem.
//!
//! The overlay is a **pure function** of `(graph artifact, run artifact) →
//! diagram text`: [`render_dot_overlay`] and [`render_mermaid_overlay`]. It
//! reads artifacts only (no run store, no live graph, no network, no producing
//! binary — the same crate-graph guarantee as the base renderer, since
//! `dagr-render` never depends on `dagr-core`), so it works identically on a run
//! captured three months ago.
//!
//! # The documented state → style mapping (auditable from source)
//!
//! Every entry in the normative **Terminal states** table (arch.md Vocabulary),
//! plus the `not-requested` single-node-replay artifact marking (C22/C26 — an
//! artifact marking, **not** a terminal state), maps to a **distinct** style. In
//! DOT the discriminator is the node `fillcolor` (nodes are `style="filled"`);
//! in Mermaid it is a per-state `classDef`/`class`. All fill colours are
//! mutually distinct, so `skipped` (originated) is separable from
//! `upstream-skipped` (propagated), and `failed`/`timed-out` (originated) from
//! `upstream-failed` (propagated) — the arch.md C24 requirement. Each node's
//! label additionally carries its **state tag** and **duration**, so the
//! distinction is textual as well as chromatic.
//!
//! | state (or marking) | class | DOT `fillcolor` |
//! |---|---|---|
//! | `succeeded` | `succeeded` | `#2e7d32` (green) |
//! | `satisfied-from-prior` | `satisfiedFromPrior` | `#a5d6a7` (light green) |
//! | `skipped` (originated) | `skipped` | `#fdd835` (yellow) |
//! | `upstream-skipped` (propagated) | `upstreamSkipped` | `#fff59d` (pale yellow) |
//! | `failed` (originated) | `failed` | `#c62828` (red) |
//! | `timed-out` (originated) | `timedOut` | `#ef6c00` (orange) |
//! | `upstream-failed` (propagated) | `upstreamFailed` | `#ef9a9a` (light red) |
//! | `cancelled` | `cancelled` | `#455a64` (slate) |
//! | `abandoned` | `abandoned` | `#212121` (near-black) |
//! | `not-requested` (marking) | `notRequested` | `#eceff1` (very light grey) |
//!
//! An unrecognized status string (a future terminal state the schema's open
//! taxonomy might one day carry) maps to `unknown` / `#ffffff` rather than
//! panicking, and is labelled with the verbatim status.
//!
//! # Node-identity join (defined, non-panicking)
//!
//! Graph nodes are joined to run records by **node identity** (the stable
//! name). Two mismatch cases have documented, non-panicking behaviour:
//!
//! * A graph node **absent from the run artifact** renders with its base T46
//!   styling and **no** overlay colouring or duration (there is nothing to
//!   overlay). This is the ordinary case for a partial single-node replay's
//!   out-of-request nodes that carry no marking either.
//! * A run record whose node id is **absent from the graph** is **not** injected
//!   as a phantom node; the extra ids are reported in a trailing diagram
//!   **comment** (`// extra run records not in graph: …` / `%% …`), so the
//!   diagram stays a faithful drawing of the graph while the mismatch remains
//!   visible.
//!
//! A node with several attempt records (retries) is coloured by its **final**
//! attempt's status (highest attempt number) and annotated with the **sum** of
//! all its attempts' durations — retries are real re-work, the same convention
//! the critical-path summary uses (C22 · T43). A `not-requested` marking wins
//! over any (absent) attempt record for that node.
//!
//! # Determinism
//!
//! The overlay adds only per-node attributes and a fixed style prelude/suffix to
//! the deterministic base layout, so the output stays byte-stable and
//! golden-pinned, and the overlaid DOT/Mermaid remain accepted by their
//! reference tools (`dot`, the browserless Mermaid parser).

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::model::GraphArtifact;
use crate::{dot, mermaid};

/// The nine normative **terminal states** (arch.md Vocabulary). `not-requested`
/// is deliberately **absent** — it is an artifact marking, never a terminal
/// state.
pub const TERMINAL_STATES: [&str; 9] = [
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

/// A read-only view of a **C22 run artifact** (the published
/// `schemas/run/v1.schema.json` shape, produced by the T42 fold). The overlay
/// reads only the fields it projects: each attempt's node identity, terminal
/// status, and phase durations, plus the single-node-replay `node_markings`.
///
/// Obtain one with [`RunArtifact::from_json_str`]. Unknown fields are ignored
/// (additive-only schema evolution), so a newer run artifact still overlays.
#[derive(Debug, Clone, Deserialize)]
pub struct RunArtifact {
    #[serde(default)]
    attempts: Vec<Attempt>,
    /// Single-node-replay per-node markings (C26): maps a node id to `requested`
    /// / `not-requested`. Absent on a full run.
    #[serde(default)]
    node_markings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
struct Attempt {
    node: String,
    #[serde(rename = "attempt", default = "one")]
    number: u64,
    status: String,
    #[serde(default)]
    phase_durations_ns: BTreeMap<String, u64>,
}

fn one() -> u64 {
    1
}

impl RunArtifact {
    /// Parse a published **C22 run-artifact** JSON document into the read-only
    /// overlay view (arch.md C24 — the overlay consumes the published artifact
    /// and nothing else).
    ///
    /// # Errors
    ///
    /// Returns a diagnostic message if the input is not valid JSON or does not
    /// match the run-artifact shape (e.g. an attempt missing its required
    /// `node`/`status`). The message names the offending field/reason.
    pub fn from_json_str(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| e.to_string())
    }

    /// The per-node overlay outcome, joined by node identity: for each node that
    /// has an attempt record and/or a marking, its final state and summed
    /// duration.
    fn outcomes(&self) -> BTreeMap<String, NodeOutcome> {
        let mut out: BTreeMap<String, NodeOutcome> = BTreeMap::new();

        // Attempts: final status wins (highest attempt number); durations sum.
        for a in &self.attempts {
            let dur: u64 = a.phase_durations_ns.values().copied().sum();
            let entry = out.entry(a.node.clone()).or_insert(NodeOutcome {
                state: NodeState::from_status(&a.status),
                final_attempt: a.number,
                total_ns: 0,
            });
            entry.total_ns = entry.total_ns.saturating_add(dur);
            if a.number >= entry.final_attempt {
                entry.final_attempt = a.number;
                entry.state = NodeState::from_status(&a.status);
            }
        }

        // Markings: `not-requested` is an artifact marking that wins over any
        // (usually absent) attempt for that node; `requested` is not projected
        // (the node's own terminal state already covers it).
        for (node, mark) in &self.node_markings {
            if mark == "not-requested" {
                out.insert(
                    node.clone(),
                    NodeOutcome {
                        state: NodeState::NotRequested,
                        final_attempt: 0,
                        total_ns: 0,
                    },
                );
            }
        }

        out
    }
}

/// One node's projected overlay outcome.
#[derive(Debug, Clone)]
struct NodeOutcome {
    state: NodeState,
    final_attempt: u64,
    total_ns: u64,
}

/// The style + label decoration the overlay applies to one node.
#[derive(Debug, Clone)]
pub(crate) struct NodeDecoration {
    /// The DOT `fillcolor` (nodes are `style="filled"`).
    pub(crate) fillcolor: &'static str,
    /// The DOT `fontcolor` (white on dark fills for legibility).
    pub(crate) fontcolor: &'static str,
    /// The Mermaid class name / DOT-independent state class.
    pub(crate) class: &'static str,
    /// The state (or marking) tag drawn in the label.
    pub(crate) state_tag: String,
    /// The human-readable duration drawn in the label, if any.
    pub(crate) duration: Option<String>,
}

/// The overlay decorations, keyed by node identity, plus the extra-run-record
/// ids absent from the graph (reported, never drawn).
pub(crate) struct Overlay {
    pub(crate) by_node: BTreeMap<String, NodeDecoration>,
    pub(crate) extra_run_nodes: Vec<String>,
}

impl Overlay {
    /// Build the overlay by joining the run artifact's outcomes to the graph's
    /// node roster.
    fn build(graph: &GraphArtifact, run: &RunArtifact) -> Self {
        let outcomes = run.outcomes();
        let graph_names: std::collections::BTreeSet<&str> =
            graph.nodes().iter().map(crate::model::Node::name).collect();

        let mut by_node = BTreeMap::new();
        for (node, oc) in &outcomes {
            if !graph_names.contains(node.as_str()) {
                continue; // reported separately, not drawn
            }
            let style = oc.state.style();
            by_node.insert(
                node.clone(),
                NodeDecoration {
                    fillcolor: style.fillcolor,
                    fontcolor: style.fontcolor,
                    class: style.class,
                    state_tag: oc.state.tag().to_string(),
                    // A never-ran / not-requested node carries no meaningful
                    // duration; annotate one only when the node actually ran.
                    duration: if oc.state.ran() {
                        Some(format_duration_ns(oc.total_ns))
                    } else {
                        None
                    },
                },
            );
        }

        // Run records whose node id is absent from the graph: report, sorted for
        // determinism.
        let mut extra_run_nodes: Vec<String> = outcomes
            .keys()
            .filter(|n| !graph_names.contains(n.as_str()))
            .cloned()
            .collect();
        extra_run_nodes.sort();
        extra_run_nodes.dedup();

        Overlay {
            by_node,
            extra_run_nodes,
        }
    }
}

/// A node's projected state — the nine normative terminal states, the
/// `not-requested` artifact marking, and an `Unknown` fallback for a status the
/// closed taxonomy does not (yet) name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeState {
    Succeeded,
    Failed,
    TimedOut,
    Skipped,
    UpstreamSkipped,
    UpstreamFailed,
    Cancelled,
    Abandoned,
    SatisfiedFromPrior,
    NotRequested,
    Unknown,
}

/// The documented style for one state.
struct StateStyle {
    fillcolor: &'static str,
    fontcolor: &'static str,
    class: &'static str,
}

impl NodeState {
    fn from_status(status: &str) -> Self {
        match status {
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "timed-out" => Self::TimedOut,
            "skipped" => Self::Skipped,
            "upstream-skipped" => Self::UpstreamSkipped,
            "upstream-failed" => Self::UpstreamFailed,
            "cancelled" => Self::Cancelled,
            "abandoned" => Self::Abandoned,
            "satisfied-from-prior" => Self::SatisfiedFromPrior,
            "not-requested" => Self::NotRequested,
            _ => Self::Unknown,
        }
    }

    /// The documented, distinct style for this state. See the module-level
    /// mapping table.
    fn style(self) -> StateStyle {
        // Dark fills use a white font; light fills a black one, for legibility.
        match self {
            Self::Succeeded => StateStyle {
                fillcolor: "#2e7d32",
                fontcolor: "#ffffff",
                class: "succeeded",
            },
            Self::SatisfiedFromPrior => StateStyle {
                fillcolor: "#a5d6a7",
                fontcolor: "#000000",
                class: "satisfiedFromPrior",
            },
            Self::Skipped => StateStyle {
                fillcolor: "#fdd835",
                fontcolor: "#000000",
                class: "skipped",
            },
            Self::UpstreamSkipped => StateStyle {
                fillcolor: "#fff59d",
                fontcolor: "#000000",
                class: "upstreamSkipped",
            },
            Self::Failed => StateStyle {
                fillcolor: "#c62828",
                fontcolor: "#ffffff",
                class: "failed",
            },
            Self::TimedOut => StateStyle {
                fillcolor: "#ef6c00",
                fontcolor: "#ffffff",
                class: "timedOut",
            },
            Self::UpstreamFailed => StateStyle {
                fillcolor: "#ef9a9a",
                fontcolor: "#000000",
                class: "upstreamFailed",
            },
            Self::Cancelled => StateStyle {
                fillcolor: "#455a64",
                fontcolor: "#ffffff",
                class: "cancelled",
            },
            Self::Abandoned => StateStyle {
                fillcolor: "#212121",
                fontcolor: "#ffffff",
                class: "abandoned",
            },
            Self::NotRequested => StateStyle {
                fillcolor: "#eceff1",
                fontcolor: "#000000",
                class: "notRequested",
            },
            Self::Unknown => StateStyle {
                fillcolor: "#ffffff",
                fontcolor: "#000000",
                class: "unknown",
            },
        }
    }

    /// The state tag drawn in the node label (the verbatim taxonomy name).
    fn tag(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed-out",
            Self::Skipped => "skipped",
            Self::UpstreamSkipped => "upstream-skipped",
            Self::UpstreamFailed => "upstream-failed",
            Self::Cancelled => "cancelled",
            Self::Abandoned => "abandoned",
            Self::SatisfiedFromPrior => "satisfied-from-prior",
            Self::NotRequested => "not-requested",
            Self::Unknown => "unknown",
        }
    }

    /// Whether the node actually executed in this run (so a duration is
    /// meaningful). Never-ran propagated states, the resume carry-forward, and
    /// the not-requested marking did not execute here.
    fn ran(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::TimedOut)
    }
}

/// The distinct classes the overlay uses, in a fixed order, so the Mermaid
/// `classDef` prelude is deterministic. Includes `unknown` (fallback) and the
/// `not-requested` marking; the nine terminal states come first.
pub(crate) fn all_classes() -> Vec<(&'static str, StateStyleRef)> {
    [
        NodeState::Succeeded,
        NodeState::Failed,
        NodeState::TimedOut,
        NodeState::Skipped,
        NodeState::UpstreamSkipped,
        NodeState::UpstreamFailed,
        NodeState::Cancelled,
        NodeState::Abandoned,
        NodeState::SatisfiedFromPrior,
        NodeState::NotRequested,
        NodeState::Unknown,
    ]
    .into_iter()
    .map(|s| {
        let st = s.style();
        (
            st.class,
            StateStyleRef {
                fillcolor: st.fillcolor,
                fontcolor: st.fontcolor,
            },
        )
    })
    .collect()
}

/// A style pair for a Mermaid `classDef` line.
pub(crate) struct StateStyleRef {
    pub(crate) fillcolor: &'static str,
    pub(crate) fontcolor: &'static str,
}

/// Format a nanosecond duration into a deterministic, human-readable string with
/// two fractional digits and a unit chosen by magnitude (`ns`, `µs`, `ms`, `s`).
///
/// Computed with **integer arithmetic only** (no floating point), so it is
/// byte-stable across platforms — no locale, no float-rounding drift. The two
/// fractional digits are the hundredths of the chosen unit, truncated (not
/// rounded) toward zero.
fn format_duration_ns(ns: u64) -> String {
    // (divisor, unit) for each SI step; below 1µs we print bare nanoseconds.
    let (divisor, unit) = if ns < 1_000 {
        return format!("{ns}ns");
    } else if ns < 1_000_000 {
        (1_000u64, "µs")
    } else if ns < 1_000_000_000 {
        (1_000_000u64, "ms")
    } else {
        (1_000_000_000u64, "s")
    };
    let whole = ns / divisor;
    // Hundredths of the unit: (remainder * 100) / divisor, truncated.
    let hundredths = (ns % divisor) * 100 / divisor;
    format!("{whole}.{hundredths:02}{unit}")
}

/// Render `graph` to Graphviz DOT source with a **run overlay** projected from
/// `run` (arch.md C24). Each node joined to the run artifact is coloured by its
/// terminal state and annotated with its duration; the base structure (nodes,
/// edges, clusters) is drawn exactly as [`crate::render_dot`]. Deterministic and
/// byte-stable; accepted by the `dot` reference tool.
///
/// See the [module documentation](self) for the state → style mapping and the
/// node-identity join rules.
#[must_use]
pub fn render_dot_overlay(graph: &GraphArtifact, run: &RunArtifact) -> String {
    let overlay = Overlay::build(graph, run);
    dot::render_with_overlay(graph, Some(&overlay))
}

/// Render `graph` to Mermaid flowchart source with a **run overlay** projected
/// from `run` (arch.md C24). Each node joined to the run artifact carries its
/// documented per-state `class` and a duration annotation; the base structure is
/// drawn exactly as [`crate::render_mermaid`]. Deterministic and byte-stable;
/// accepted by the browserless Mermaid parser.
///
/// See the [module documentation](self) for the state → style mapping and the
/// node-identity join rules.
#[must_use]
pub fn render_mermaid_overlay(graph: &GraphArtifact, run: &RunArtifact) -> String {
    let overlay = Overlay::build(graph, run);
    mermaid::render_with_overlay(graph, Some(&overlay))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_formatting_is_deterministic_and_unit_scaled() {
        assert_eq!(format_duration_ns(0), "0ns");
        assert_eq!(format_duration_ns(999), "999ns");
        assert_eq!(format_duration_ns(1_000), "1.00µs");
        assert_eq!(format_duration_ns(1_500), "1.50µs");
        assert_eq!(format_duration_ns(2_000_000), "2.00ms");
        assert_eq!(format_duration_ns(3_500_000_000), "3.50s");
    }

    #[test]
    fn every_terminal_state_and_the_marking_have_distinct_fillcolors() {
        let mut seen = std::collections::BTreeSet::new();
        for status in TERMINAL_STATES.iter().chain(["not-requested"].iter()) {
            let fc = NodeState::from_status(status).style().fillcolor;
            assert!(seen.insert(fc), "state `{status}` reuses fillcolor `{fc}`");
        }
        assert_eq!(
            seen.len(),
            10,
            "ten distinct styles (nine states + marking)"
        );
    }

    #[test]
    fn final_attempt_status_wins_and_durations_sum() {
        // load: attempt 1 failed (400ns), attempt 2 succeeded (900ns).
        let run = RunArtifact::from_json_str(
            r#"{"attempts":[
                {"node":"load","attempt":1,"status":"failed","phase_durations_ns":{"executing":400}},
                {"node":"load","attempt":2,"status":"succeeded","phase_durations_ns":{"executing":900}}
            ]}"#,
        )
        .unwrap();
        let oc = run.outcomes();
        let load = &oc["load"];
        assert_eq!(load.state, NodeState::Succeeded, "final attempt wins");
        assert_eq!(load.total_ns, 1300, "durations sum across attempts");
    }
}
