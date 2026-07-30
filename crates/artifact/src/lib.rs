#![doc = include_str!("../README.md")]
//!
//! # Module index
//!
//! The orientation above comes from the crate's `README.md`, inlined here so the
//! crates.io landing page and this front page are one file. What follows is the
//! map of where each piece lives.
//!
//! The **event-stream writer** lives in [`event_stream`]. The published
//! versioned schemas and their validation helper live in the `schema` module —
//! compiled only when the `schema-validation` feature is enabled, since its
//! `jsonschema` dependency is CI-/dev-scoped. The **run-artifact fold** — the
//! standalone reader that folds an event stream into a run artifact — lives in
//! the [`fold`] module. The graph artifact emitter lands elsewhere.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod canonical;
pub mod event_stream;
pub mod fold;

/// The published-artifact-schema validation helper.
///
/// Behind the `schema-validation` cargo feature (default OFF) because its
/// `jsonschema` dependency is CI-/dev-scoped; the runtime writers never pull it.
/// The published schema documents themselves live at the repo root under
/// `schemas/<kind>/v<version>.schema.json`.
#[cfg(feature = "schema-validation")]
pub mod schema;

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph — every member crate is discoverable and testable.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
