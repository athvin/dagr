//! `dagr-artifact` — dagr's artifact types.
//!
//! This crate defines the serializable records a run leaves behind — the
//! graph artifact, the run artifact, and the event-record shapes they are
//! derived from — together with their versioned schemas.
//!
//! It is a deliberate boundary: a renderer consumes an artifact and nothing
//! else, so this crate is the *only* thing the
//! [`dagr-render`](../dagr_render/index.html) crate is allowed to depend on.
//! Because `artifact` depends on no other workspace crate, it can never drag in
//! the live-pipeline surface, and rendering stays "no access to the binary that
//! produced the artifacts."
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
