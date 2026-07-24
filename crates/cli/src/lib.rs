//! `dagr-cli` — the pipeline binary's command-line contract (placeholder
//! skeleton).
//!
//! This crate will supply the standard verbs every dagr pipeline binary shares
//! — emit the graph, validate, render, run, run a single node, resume, fold an
//! event stream into a run artifact, and prune — along with the typed-parameter
//! plumbing around them (arch.md C26 · Command-line contract).
//!
//! It is the one place the three other crates meet: it depends on
//! [`dagr-core`](../dagr_core/index.html) (the live pipeline),
//! [`dagr-artifact`](../dagr_artifact/index.html) (the records), and
//! [`dagr-render`](../dagr_render/index.html) (diagram source). Invoking
//! rendering here as the pipeline binary's `render` subcommand still consumes
//! artifacts only, so it does not weaken the C24 renderer-independence
//! guarantee that the crate graph enforces.
//!
//! The first concrete code lands with ticket T24 (034): the M1 **run-loop
//! driver** in [`driver`], the component that orchestrates one complete run from
//! an assembled pipeline to a truthful end. The verb implementations and
//! exit-code contract still land in later tickets (T55, T56).
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod contract;
pub(crate) mod dispatch;
pub mod driver;
#[cfg(feature = "test-kit")]
pub mod full_pipeline;
pub mod graph;
pub mod logging;
pub mod scale_bench;
pub mod signals;
pub mod structure_snapshot;
pub mod temp;

pub use graph::{
    emit_graph, graph_verb, BuildProvenance, GraphEmitError, GraphVerbError, GRAPH_SCHEMA_MAJOR,
    GRAPH_SCHEMA_VERSION,
};
pub use structure_snapshot::{
    assert_structure, bless_structure, StructureAssertError, StructureDiff, StructureSnapshot,
};

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph (T1 Test plan: "every member crate is discoverable and
    /// testable"). Real tests arrive with the CLI contract in T55/T56.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
