//! `dagr-artifact` — dagr's artifact types (placeholder skeleton).
//!
//! This crate will define the serializable records a run leaves behind — the
//! graph artifact (arch.md C20), the run artifact (C22), and the event-record
//! shapes they are derived from (C19) — together with their versioned schemas.
//!
//! It is the deliberate boundary named by arch.md C24 · Renderers: a renderer
//! consumes an artifact and nothing else, so this crate is the *only* thing the
//! [`dagr-render`](../dagr_render/index.html) crate is allowed to depend on.
//! Because `artifact` depends on no other workspace crate, it can never drag in
//! the live-pipeline surface, and rendering stays "no access to the binary that
//! produced the artifacts."
//!
//! The first concrete artifact code lands with ticket T19 (029): the C19
//! **event-stream writer** in [`event_stream`]. The published versioned schemas
//! (T39) and their validation helper live in the `schema` module — compiled only
//! when the `schema-validation` feature is enabled, since its `jsonschema`
//! dependency is CI-/dev-scoped (T4 ADR 017 §4). The graph artifact emitter
//! (C20 / T40) and run artifact fold (C22 / T42) still land later.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod canonical;
pub mod event_stream;

/// The T39 published-artifact-schema validation helper (arch.md C19/C20/C22).
///
/// Behind the `schema-validation` cargo feature (default OFF) because its
/// `jsonschema` dependency is CI-/dev-scoped per the T4 ADR (017 §4); the
/// runtime writers never pull it. The published schema documents themselves live
/// at the repo root under `schemas/<kind>/v<version>.schema.json`.
#[cfg(feature = "schema-validation")]
pub mod schema;

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph (T1 Test plan: "every member crate is discoverable and
    /// testable"). Real tests arrive with the artifact types in later tickets.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
