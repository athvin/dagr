//! Run-overlay rendering (arch.md `### C24 · Renderers` — the run-artifact
//! overlay). PLACEHOLDER — filled in by the T47 implementation commits.

use crate::model::GraphArtifact;

/// The nine normative terminal states (arch.md Vocabulary).
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

/// A read-only view of a C22 run artifact (placeholder).
#[derive(Debug, Clone)]
pub struct RunArtifact;

impl RunArtifact {
    /// Parse a run artifact JSON (placeholder).
    ///
    /// # Errors
    /// Never, in the placeholder.
    pub fn from_json_str(_json: &str) -> Result<Self, String> {
        Ok(RunArtifact)
    }
}

/// Render DOT with a run overlay (placeholder).
#[must_use]
pub fn render_dot_overlay(_graph: &GraphArtifact, _run: &RunArtifact) -> String {
    String::new()
}

/// Render Mermaid with a run overlay (placeholder).
#[must_use]
pub fn render_mermaid_overlay(_graph: &GraphArtifact, _run: &RunArtifact) -> String {
    String::new()
}
