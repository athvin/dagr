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
//! **event-stream writer** in [`event_stream`]. The graph artifact (C20 / T40),
//! run artifact (C22 / T42), and versioned schemas (T39) still land later.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod event_stream;

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph (T1 Test plan: "every member crate is discoverable and
    /// testable"). Real tests arrive with the artifact types in later tickets.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
