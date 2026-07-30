//! **M9 behavioural-identity probe** (ticket 114 · T99).
//!
//! **CI fixture — do not change its printed output.** The M9 acceptance gate
//! (`crates/cli/tests/m9_acceptance_gate.rs`) runs this example on the current
//! head and diffs its output byte-for-byte against
//! `crates/cli/tests/fixtures/m9-baseline/reference.snapshot.txt`, which was
//! produced by running **this same file, unmodified** against the pre-M9 tree.
//! Changing what it prints re-bases the comparison and destroys the gate.
//!
//! # What it prints
//!
//! A single deterministic snapshot of dagr's *observable* behaviour — the four
//! surfaces the M9 gate claims are unchanged, namely artifacts (graph and run),
//! fingerprints, terminal-state transitions, and the event stream — as
//! `key=value` lines:
//!
//! - `graph-artifact=` — the reference pipeline's full graph artifact, canonical
//!   JSON, with the sole generation-time header field masked.
//! - `fingerprint-structural=` / `fingerprint-policy=` — the structural
//!   fingerprint and policy hash of that same pipeline. The edition bump (T94)
//!   must not have altered a hashed input.
//! - `overall-outcome=` and `terminal-state:<node>=` — the terminal-state
//!   transitions of a scripted run driven through the **real** scheduler.
//! - `run-artifact=` — the folded run artifact of that run, normalized by the
//!   harness's own volatile-field masking.
//! - `event:` — every record of that run's event stream, normalized (see
//!   [`VOLATILE_KEYS`]) and printed one per line in stream order.
//!
//! Those `key=value` lines, and nothing else, are the snapshot: the gate captures
//! this program's **stdout** only. Anything the engine writes to stderr — notably
//! the driver's worst-case shutdown-budget arithmetic, which `driver.rs` emits
//! with `eprintln!` at startup — is neither in the fixture nor compared by the
//! gate. Behaviour reachable only through stderr is outside what this probe
//! pins.
//!
//! # Why it is written the way it is
//!
//! Two constraints shape this file, and both are load-bearing:
//!
//! 1. **It must compile against the pre-M9 tree.** The baseline was captured by
//!    copying this file into a worktree at the pre-M9 commit and running it
//!    there, so it uses only public API that existed then and no edition-2024
//!    syntax. It has no dependency beyond what `dagr-cli` already carries.
//! 2. **Its output must be a pure function of the engine.** The run is scripted
//!    through a *linear* chain, so no two nodes are ever in flight together and
//!    the stream order is fixed; identity, parameters, and the data interval are
//!    pinned; and every clock-derived or host-derived field is masked. What
//!    survives the masking is exactly the interpretive content the milestone
//!    promised not to change.

use dagr_cli::full_pipeline::{FullPipelineTest, Outcome};
use dagr_cli::graph::{
    BuildProvenance, emit_graph, format_fingerprint_policy, format_fingerprint_structural,
    mask_generated_at,
};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};

// ---------------------------------------------------------------------------
// The reference pipeline — the same `load -> transform -> publish` shape the
// `structural-determinism` CI job and the T65 acceptance suite drive, so the M9
// gate compares the artifact surface the rest of the repo already treats as the
// reference. Kept in sync with `examples/reference_pipeline_artifact.rs` and
// `tests/system_acceptance_gate.rs` by review.
// ---------------------------------------------------------------------------

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

/// The checked-in reference pipeline: `load -> transform -> publish`, with a
/// non-default policy on `load` so the policy hash is a non-trivial value.
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

// ---------------------------------------------------------------------------
// Event-stream normalization.
// ---------------------------------------------------------------------------

/// The record fields that legitimately vary between two runs of the same script:
/// the wall clock, the monotonic offsets, the minted run identity, the worker
/// that happened to pick the attempt up, and the measured (as opposed to
/// declared) cost and metrics. Everything else — sequence numbers, event kinds,
/// node identity, attempt numbers, terminal states, propagation origins, the
/// declared cost, the artifact header's fingerprints — is interpretive content
/// and is compared verbatim.
///
/// The set is deliberately SHORT. Every key added here is a byte the gate stops
/// checking, so a field is masked only when it is genuinely clock-, host-, or
/// identity-derived.
const VOLATILE_KEYS: &[&str] = &[
    "wall",
    "offset_ns",
    "produced_at_offset_ns",
    "run_id",
    "worker",
    "metrics",
    "cost_measured",
    "generated_at",
];

/// Recursively replace every [`VOLATILE_KEYS`] value with a fixed sentinel,
/// in place, preserving the key so its *presence* is still compared.
fn mask_volatile(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, slot) in map.iter_mut() {
                if VOLATILE_KEYS.contains(&key.as_str()) {
                    *slot = serde_json::Value::from("<masked>");
                } else {
                    mask_volatile(slot);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                mask_volatile(item);
            }
        }
        _ => {}
    }
}

fn main() {
    // --- 0. The scripted run, FIRST ------------------------------------------
    // Driven before anything is printed, so no output the engine produces can
    // interleave with this file's own lines. The driver's startup line — its
    // worst-case shutdown-budget arithmetic — goes to stderr, and the gate reads
    // stdout only, so it is absent from the snapshot and is not one of the
    // observables this probe compares.
    //
    // A LINEAR chain: `load` succeeds, `transform` fails once and retries to
    // success, `publish` consumes it. Nothing is ever concurrent, so the event
    // stream's order is a property of the engine rather than of the scheduler's
    // interleaving.
    let run = FullPipelineTest::new("t99-reference")
        .run_id("t99-fixed-run-id")
        .parameter("window", "P1D")
        .data_interval("2026-07-24T00:00:00Z", "2026-07-25T00:00:00Z")
        .source("load", Outcome::succeed())
        .node("transform", &["load"], Outcome::fail_then_succeed(1))
        .node("publish", &["transform"], Outcome::succeed())
        .run();

    // --- 1. The graph artifact and the two fingerprints ---------------------
    let pipeline = reference_pipeline();
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
    println!(
        "graph-artifact={}",
        dagr_artifact::canonical::to_canonical_string(&masked)
    );

    let slot = pipeline.fingerprint();
    println!(
        "fingerprint-structural={}",
        format_fingerprint_structural(&slot)
    );
    println!("fingerprint-policy={}", format_fingerprint_policy(&slot));

    // --- 2. The scripted run's terminal-state transitions -------------------
    println!("overall-outcome={}", run.overall_outcome());
    for node in ["load", "transform", "publish"] {
        println!("terminal-state:{}={:?}", node, run.terminal_state(node));
    }

    // --- 3. The folded run artifact (harness-normalized) --------------------
    let artifact: serde_json::Value = serde_json::from_str(&run.interpretive_artifact_json())
        .expect("the folded run artifact is valid JSON");
    println!(
        "run-artifact={}",
        dagr_artifact::canonical::to_canonical_string(&artifact)
    );

    // --- 4. The event stream, record by record ------------------------------
    let stream = String::from_utf8(run.event_stream().to_vec())
        .expect("the event stream is valid UTF-8 JSONL");
    for line in stream.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut record: serde_json::Value =
            serde_json::from_str(line).expect("every event-stream record is valid JSON");
        mask_volatile(&mut record);
        println!(
            "event={}",
            dagr_artifact::canonical::to_canonical_string(&record)
        );
    }
}
