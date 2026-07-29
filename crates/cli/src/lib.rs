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
/// The `dagr metastore init` verb (M7, T83, ADR 097). Gated behind the default-off
/// `metastore` feature so `--no-default-features` (and any default build) drops the
/// `dagr-metastore`/`libsql` edge entirely — `dagr-core` never sees it.
#[cfg(feature = "metastore")]
pub mod metastore;
/// The run-sink **tee** composing the on-disk `events.jsonl` sink with the
/// guaranteed live [`dagr_metastore::MetastoreSink`] (M7, T86, ADR 097). Gated
/// behind the default-off `metastore` feature so a default build (and
/// `--no-default-features`) omits the tee wiring and the `dagr-metastore`/`libsql`
/// edge entirely.
#[cfg(feature = "metastore")]
pub mod metastore_tee;
pub mod prelude;
pub mod registry;
/// The `inventory`-backed DAG auto-discovery entrypoint (M6, ADR 092). Gated behind
/// the default-on `dag` feature so `--no-default-features` drops the `inventory`
/// runtime dependency edge entirely (`dagr-core` never sees it).
#[cfg(feature = "dag")]
pub mod run;
pub mod run_flow;
pub mod run_store;
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
pub use flow_builder::{FlowBuilder, NodeBuilder};
pub use graph::{
    BuildProvenance, GRAPH_SCHEMA_MAJOR, GRAPH_SCHEMA_VERSION, GraphEmitError, GraphVerbError,
    emit_graph, graph_verb,
};
/// The DAG auto-discovery surface (M6, ADR 092), re-exported at the crate root under
/// the default-on `dag` feature: [`run()`] (the one-call entrypoint a DAG-hosting
/// binary's `main` delegates to) and [`DagRegistration`] (the record a binary submits
/// per DAG). Absent under `--no-default-features`.
#[cfg(feature = "dag")]
pub use run::{DagRegistration, run};
pub use run_flow::RunToStoreError;
/// The run-store defaults (the local-file event sink, the wall-clock and deterministic
/// clocks, the default store base, and the run-id minter) — reusable so a hand-written
/// driver, the registry, the `#[dag]` run path, and the one-call
/// [`RunnableFlow::run_to_store`](run_flow::RunnableFlow::run_to_store) all share one
/// implementation and no one hand-writes a `FileSink` again.
pub use run_store::{DEFAULT_STORE_BASE, FileSink, SystemClock, TickClock, mint_run_id};
pub use structure_snapshot::{
    StructureAssertError, StructureDiff, StructureSnapshot, assert_structure, bless_structure,
};

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph — every member crate is discoverable and testable. Real
    /// tests arrive with the CLI contract.
    #[test]
    fn crate_is_in_the_build_graph() {}
}
