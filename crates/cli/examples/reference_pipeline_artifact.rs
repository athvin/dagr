//! Cross-toolchain **structural-determinism** fixture (arch.md system-level
//! acceptance criterion 4(a); ticket T65 / `SL4a`).
//!
//! Prints, for the checked-in reference pipeline, the **masked graph artifact**
//! (the full C20 artifact with the sole generation-time header field blanked) as
//! canonical JSON on one line, followed by the two C21 fingerprint strings:
//!
//! ```text
//! artifact={ ... canonical JSON, generation-time masked ... }
//! structural=fnv1a-64:v1:<hex>
//! policy=fnv1a-64:v1:<hex>
//! ```
//!
//! CI builds and runs this example under **two different toolchains** and diffs
//! the output byte-for-byte (`.github/workflows/ci.yml`, the
//! `structural-determinism` job). A divergence fails the job — that is the
//! executable form of `SL4a`'s "two builds of the same source produce identical
//! fingerprints AND byte-identical graph artifacts (generation time aside), on
//! different toolchains" guarantee. It extends the fingerprint-only
//! `cross-toolchain-fingerprint` job to the full graph artifact.
//!
//! Every hashed and emitted input is author-declared and the digest is pure
//! integer arithmetic (FNV-1a), and the generation-time field — the only
//! byte-varying field in a repeated emission — is masked to a fixed sentinel by
//! `mask_generated_at`, so the two toolchains must agree byte-for-byte.
//!
//! This example depends only on the public `dagr_cli::graph` surface and
//! `dagr_core`; it opens no run store, reads no environment, and takes no
//! parameters (C7 / C20 pure assembly). The reference pipeline it builds is the
//! same shape the `crates/cli/tests/system_acceptance_gate.rs` structural-
//! determinism test drives, kept in sync by review.

use dagr_cli::graph::{
    emit_graph, format_fingerprint_policy, format_fingerprint_structural, mask_generated_at,
    BuildProvenance,
};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};

// Author-declared stable names — the identity the artifact and fingerprint record.
struct Loaded;
impl StableName for Loaded {
    const STABLE_NAME: &'static str = "Loaded";
}
struct Transformed;
impl StableName for Transformed {
    const STABLE_NAME: &'static str = "Transformed";
}
struct Published;
impl StableName for Published {
    const STABLE_NAME: &'static str = "Published";
}

struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "load";
}
impl Task for Load {
    type Input = ();
    type Output = Loaded;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Loaded, TaskError> {
        Ok(Loaded)
    }
}

struct Transform;
impl StableName for Transform {
    const STABLE_NAME: &'static str = "transform";
}
impl Task for Transform {
    type Input = Loaded;
    type Output = Transformed;
    async fn run(&mut self, _c: &RunContext, _i: Loaded) -> Result<Transformed, TaskError> {
        Ok(Transformed)
    }
}

struct Publish;
impl StableName for Publish {
    const STABLE_NAME: &'static str = "publish";
}
impl Task for Publish {
    type Input = Transformed;
    type Output = Published;
    async fn run(&mut self, _c: &RunContext, _i: Transformed) -> Result<Published, TaskError> {
        Ok(Published)
    }
}

/// The checked-in reference pipeline: `load → transform → publish`, with a
/// non-default policy on `load` so the policy hash is a non-trivial value. Kept
/// in sync (by review) with the reference pipeline in
/// `crates/cli/tests/system_acceptance_gate.rs`.
fn reference_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    let loaded = flow.register_source_named(
        "load",
        &Load,
        Some("ingest"),
        NodePolicy::new().retries(2).working_memory(4096),
    );
    let transformed = flow.register_named(
        "transform",
        &Transform,
        loaded,
        None::<String>,
        NodePolicy::new(),
    );
    let _published = flow.register_named(
        "publish",
        &Publish,
        transformed,
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

fn main() {
    let pipeline = reference_pipeline();

    // Fixed provenance so the ONLY byte-varying field is generation time, which we
    // mask. Any generation-time value works: the mask blanks it before printing.
    let provenance = BuildProvenance::new("dagr-reference", "0000000", "0000000000000000");
    let json = emit_graph(
        &pipeline,
        "t65-reference",
        "1970-01-01T00:00:00Z",
        &provenance,
    )
    .expect("the reference pipeline emits a graph artifact");
    let value: serde_json::Value =
        serde_json::from_str(&json).expect("the emitted artifact is valid JSON");
    let masked = mask_generated_at(value);
    let canonical = dagr_artifact::canonical::to_canonical_string(&masked);
    println!("artifact={canonical}");

    let slot = pipeline.fingerprint();
    println!("structural={}", format_fingerprint_structural(&slot));
    println!("policy={}", format_fingerprint_policy(&slot));
}
