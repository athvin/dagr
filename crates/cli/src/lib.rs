#![doc = include_str!("../README.md")]
//!
//! # Module index
//!
//! The orientation above comes from the crate's `README.md`, inlined here so the
//! crates.io landing page and this front page are one file. What follows is the
//! map of where each piece lives.
//!
//! The **run-loop driver** is [`driver`] — the component that orchestrates one
//! complete run from an assembled pipeline to a truthful end. The one-call
//! run-a-flow seam is [`run_flow`]; the per-invocation choice of *which* executor
//! runs a node attempt is [`executor`]; the command-line contract and exit-code
//! table are [`contract`]; many-DAGs-in-one-binary is [`registry`] (hand-wired)
//! and [`run`](mod@run) (auto-discovered); the structure fixture is
//! [`structure_snapshot`]; the C28 fakes harness is [`full_pipeline`].
//!
//! Lint posture is inherited from `[workspace.lints]`; this crate adds no
//! crate-level lint attributes.

/// **Orphan adoption, tombstones and ownership revocation** (M10, T109,
/// ADR 115 §5): the startup pass that reclaims the pods a dead orchestrator left
/// running, revokes the duplicates, and hands the survivors to
/// [`k8s_runner`](mod@k8s_runner). Behind the default-off `k8s` feature; the
/// decisions it acts on are `dagr_k8s::adoption`'s, for ADR 004's reason.
#[cfg(feature = "k8s")]
pub mod adoption;
/// The **blob-backed durable-output bridge** (M10, T104, ADR 115 §8): `Blob<T>`,
/// a `DurableOutput` over any `dagr_core::Payload`, stored through the
/// `dagr_blob::BlobStore` port. Gated behind the default-off `blob` feature so a
/// default build (and `--no-default-features`) drops both the bridge and the
/// `dagr-blob` edge — `dagr-core` never sees either, and the port itself keeps no
/// edge onto `dagr-core`.
#[cfg(feature = "blob")]
pub mod blob_bridge;
/// **Intermediate-blob garbage collection** (M10, T110): `prune`'s blob half,
/// which reclaims a blob exactly when no retained run artifact references it.
/// Content addressing makes the same value produced by two runs one blob, so age
/// is never the criterion — reachability is. Gated behind the same default-off
/// `blob` feature the port is.
#[cfg(feature = "blob")]
pub mod blob_gc;
/// The **HTTPS client** the object-store backend's sans-IO protocol is executed
/// over (M10, T110). Gated behind the default-off `blob-s3` feature — the whole
/// of M10's network dependency — so `cargo build --all`, `--no-default-features`
/// and even `--features blob` compile no HTTP or TLS crate.
#[cfg(feature = "blob-s3")]
pub mod blob_s3;
pub mod config;
pub mod contract;
pub(crate) mod dispatch;
pub mod driver;
/// The pod-side **`exec-node`** verb (M10, T106, ADR 115 §3): one attempt of one
/// node, rehydrated from durable references and reported through an attempt shard.
/// Gated behind the default-off `blob` feature, because the references it reads and
/// the shard it writes both live in the blob store the feature provides.
#[cfg(feature = "blob")]
pub mod exec_node;
/// The reference pipeline the `exec-node` suite drives as a subprocess (T106). Gated
/// behind `test-kit` *and* `blob`: it ships in no released binary and exists to prove
/// the pod-side verb end to end without a cluster.
#[cfg(all(feature = "test-kit", feature = "blob"))]
pub mod exec_node_demo;
pub mod executor;
pub mod flow_builder;
#[cfg(feature = "test-kit")]
pub mod full_pipeline;
pub mod graph;
/// The **shared pod observer**'s library half (M10, T107, ADR 115 §2/§4),
/// re-exported here behind the default-off `k8s` feature: the deterministic
/// reconnect discipline, the `PodApi` port, the label/annotation identity
/// encoding, and the cluster-access decision. That crate names no async runtime;
/// the task that drives it is [`pod_observer`] in this crate, which is where ADR
/// 004 places one. Absent from a default build and from `--no-default-features`,
/// which compile no HTTP or TLS crate at all.
#[cfg(feature = "k8s")]
pub use dagr_k8s;
/// The **Kubernetes node runner** (M10, T108, ADR 115): one more implementation of
/// [`driver::NodeRunner`], which happens to submit a pod — record, submit, await,
/// read the attempt shard, replay it, return a `TerminalState`. Gated behind the
/// default-off `k8s` feature (which turns on `blob`, because the shard's address is
/// a blob-container path). No driver code path is special-cased for remoteness.
#[cfg(feature = "k8s")]
pub mod k8s_runner;
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
/// The **pod-observer task** (M10, T107, ADR 115 §2): the runtime half of the
/// shared observer — one spawned task owning one watch, its stall/backoff timer,
/// and the per-attempt waiters it demultiplexes to. Gated behind the default-off
/// `k8s` feature, and living here rather than in [`dagr_k8s`] because ADR 004
/// places the async runtime in the crate that owns the run loop, and "one watch
/// per orchestrator *process*" is a property of this process.
#[cfg(feature = "k8s")]
pub mod pod_observer;
pub mod prelude;
pub mod registry;
/// The **remote executor, wired into the run path** (M10, T112, ADR 115): the
/// cluster target an operator states, the two ports, and the startup pass that
/// reclaims a killed orchestrator's pods before anything is submitted. Behind the
/// default-off `k8s` feature, which is where the executor itself lives.
#[cfg(feature = "k8s")]
pub mod remote;
pub mod remote_guard;
/// The `inventory`-backed DAG auto-discovery entrypoint (M6, ADR 092). Gated behind
/// the default-on `dag` feature so `--no-default-features` drops the `inventory`
/// runtime dependency edge entirely (`dagr-core` never sees it).
#[cfg(feature = "dag")]
pub mod run;
pub mod run_flow;
pub mod run_store;
pub mod scale_bench;
/// The **attempt shard** (M10, T106, ADR 115 §3): one attempt's slice of the event
/// stream plus the outcome an orchestrator stamps from it. Gated behind the
/// default-off `blob` feature — the shard's address is a blob-container path.
#[cfg(feature = "blob")]
pub mod shard;
pub mod signals;
pub mod structure_snapshot;
/// The **submission log** (M10, T108, ADR 115 §9): the run's sequence authority, so
/// a write-ahead `attempt-submitted` record can be made durable *before* the pod
/// exists without giving the run a second event-stream writer. Gated behind the
/// default-off `k8s` feature and installed only under a remote executor, which is
/// the other half of why a local run's stream stays byte-identical to a pre-M10 one.
#[cfg(feature = "k8s")]
pub mod submission_log;
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
