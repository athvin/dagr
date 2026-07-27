//! `dagr-cli` — the pipeline binary's command-line contract (placeholder
//! skeleton).
//!
//! This crate will supply the standard verbs every dagr pipeline binary shares
//! — emit the graph, validate, render, run, run a single node, resume, fold an
//! event stream into a run artifact, and prune — along with the typed-parameter
//! plumbing around them (the command-line contract).
//!
//! It is the one place the three other crates meet: it depends on
//! [`dagr-core`](../dagr_core/index.html) (the live pipeline),
//! [`dagr-artifact`](../dagr_artifact/index.html) (the records), and
//! [`dagr-render`](../dagr_render/index.html) (diagram source). Invoking
//! rendering here as the pipeline binary's `render` subcommand still consumes
//! artifacts only, so it does not weaken the renderer-independence guarantee
//! that the crate graph enforces.
//!
//! The first concrete code is the **run-loop driver** in [`driver`], the
//! component that orchestrates one complete run from an assembled pipeline to a
//! truthful end. The verb implementations and exit-code contract land alongside
//! it.
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

pub mod config;
pub mod contract;
pub(crate) mod dispatch;
pub mod driver;
pub mod flow_builder;
#[cfg(feature = "test-kit")]
pub mod full_pipeline;
pub mod graph;
pub mod logging;
pub mod prelude;
pub mod registry;
/// The `inventory`-backed DAG auto-discovery entrypoint (M6, ADR 092). Gated behind
/// the default-on `dag` feature so `--no-default-features` drops the `inventory`
/// runtime dependency edge entirely (`dagr-core` never sees it).
#[cfg(feature = "dag")]
pub mod run;
pub mod run_flow;
pub mod scale_bench;
pub mod signals;
pub mod structure_snapshot;
#[cfg(feature = "test-kit")]
pub mod t63_demo;
pub mod temp;

/// The `#[dag]` attribute macro (M6, T80), re-exported here — the layer that already
/// re-exports at the CLI boundary — behind the default-on `dag` feature, exactly as
/// `dagr-core` re-exports `#[task]` behind its `macros` feature. Placing the re-export
/// in `dagr-cli` (not `dagr-core`) keeps `dagr-core`'s zero-runtime-dependency
/// guarantee untouched and introduces no `core → cli` cycle. It expands to
/// `::dagr_cli::…` / `::inventory::…`, so a DAG-hosting binary depends on both
/// `dagr-cli` and `inventory` (ADR 092). Absent under `--no-default-features`, which
/// also drops the `inventory` edge the expansion targets.
#[cfg(feature = "dag")]
pub use dagr_macros::dag;
pub use flow_builder::FlowBuilder;
pub use graph::{
    emit_graph, graph_verb, BuildProvenance, GraphEmitError, GraphVerbError, GRAPH_SCHEMA_MAJOR,
    GRAPH_SCHEMA_VERSION,
};
/// The DAG auto-discovery surface (M6, ADR 092), re-exported at the crate root under
/// the default-on `dag` feature: [`run()`] (the one-call entrypoint a DAG-hosting
/// binary's `main` delegates to) and [`DagRegistration`] (the record a binary submits
/// per DAG). Absent under `--no-default-features`.
#[cfg(feature = "dag")]
pub use run::{run, DagRegistration};
pub use structure_snapshot::{
    assert_structure, bless_structure, StructureAssertError, StructureDiff, StructureSnapshot,
};

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph — every member crate is discoverable and testable. Real
    /// tests arrive with the CLI contract.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
