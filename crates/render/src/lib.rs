//! `dagr-render` — dagr's diagram renderer (placeholder skeleton).
//!
//! This crate will read a graph artifact (optionally overlaid with a run
//! artifact) and emit diagram source in Graphviz DOT and Mermaid, distinguishing
//! data edges from ordering edges and clustering nodes by group (arch.md
//! C24 · Renderers).
//!
//! # Renderer independence (C24)
//!
//! `render` depends on [`dagr-artifact`](../dagr_artifact/index.html) and
//! nothing else in the workspace — in particular it has **no** dependency edge
//! onto `dagr-core`, the live-pipeline surface. Because that edge does not
//! exist, no code here *can* reference a live-pipeline type, so "rendering
//! requires no access to the binary that produced the artifacts" is a property
//! of the crate graph rather than a convention. A renderer therefore works
//! equally on a historical artifact from three months ago.
//!
//! The crate is a library first: the same rendering is driven both from the
//! pipeline binary's `render` subcommand (hosted in `dagr-cli`) and from the
//! standalone renderer binary in this crate's `src/main.rs`.
//!
//! At this milestone the crate is an empty, compiling placeholder created by
//! ticket T1. The DOT/Mermaid emission, styling, overlay, and golden-file tests
//! belong to C24's own implementation tickets (T46, T47).
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph (T1 Test plan: "every member crate is discoverable and
    /// testable"). Real tests arrive with the renderer in T46/T47.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
