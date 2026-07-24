//! Cross-toolchain fingerprint stability fixture (arch.md `### C21 · Graph
//! fingerprint`; ticket T41 / `SL4a`).
//!
//! Prints the two C21 fingerprints — the structural fingerprint and the policy
//! hash, each as its version-prefixed header string — for a small, fixed fixture
//! pipeline, one per line:
//!
//! ```text
//! structural=fnv1a-64:v1:<hex>
//! policy=fnv1a-64:v1:<hex>
//! ```
//!
//! CI builds and runs this example under **two different toolchains** and
//! compares the output byte-for-byte (`.github/workflows/ci.yml`, the
//! `cross-toolchain-fingerprint` job). A divergence fails the job — that is the
//! executable form of C21's "two builds of unchanged source, on different
//! toolchains, produce the same fingerprint" guarantee (T0.7 §5). Because every
//! hashed input is author-declared and the digest is pure integer arithmetic, the
//! two runs must agree.
//!
//! This example depends only on the public `dagr_cli::graph` surface and
//! `dagr_core`; it opens no run store, reads no environment, and takes no
//! parameters (C7 / C20 pure assembly).

use dagr_cli::graph::{format_fingerprint_policy, format_fingerprint_structural};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};

// Author-declared stable names — the only inputs the fingerprint hashes.
struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
struct Schema;
impl StableName for Schema {
    const STABLE_NAME: &'static str = "Schema";
}
struct Report;
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

struct MakeRows;
impl StableName for MakeRows {
    const STABLE_NAME: &'static str = "make-rows";
}
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

struct MakeSchema;
impl StableName for MakeSchema {
    const STABLE_NAME: &'static str = "make-schema";
}
impl Task for MakeSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}

struct BuildReport;
impl StableName for BuildReport {
    const STABLE_NAME: &'static str = "build-report";
}
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// A three-node fixture with a non-default policy and a group label (exercising
/// both hashes and the group exclusion): two sources feeding a two-input report.
fn fixture() -> Pipeline {
    let mut f = Flow::new();
    let rows = f.register_source_named(
        "load",
        &MakeRows,
        Some("ingest"),
        NodePolicy::new()
            .retries(2)
            .working_memory(4096)
            .compute_threads(3),
    );
    let schema = f.register_source_named("schema", &MakeSchema, None::<String>, NodePolicy::new());
    let _ = f.register_named(
        "report",
        &BuildReport,
        (rows, schema),
        None::<String>,
        NodePolicy::new(),
    );
    f.finish()
}

fn main() {
    let slot = fixture().fingerprint();
    println!("structural={}", format_fingerprint_structural(&slot));
    println!("policy={}", format_fingerprint_policy(&slot));
}
