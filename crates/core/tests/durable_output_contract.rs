//! C27 · Durable-output **reference contract** — ticket T57 (067). Written
//! first, TDD.
//!
//! T0.8 (ADR 014) *decided* the contract; T14/T29 landed the assembly-witness
//! marker `DurableOutput` and the durability policy flag. T57 **supersedes that
//! marker with the full trait pair** (T0.8 ADR §4): a durable node's OUTPUT TYPE
//! serializes a self-describing reference to where the value durably lives and
//! rehydrates the typed value from that reference later. This suite exercises the
//! contract's *shape* and its **serialize + rehydrate round-trip** — the "serialize
//! side" this ticket owns. The existence-`probe` classification and the
//! demand-driven resume that consume references are **T58**'s (out of scope here).
//!
//! Core stays dependency-free: the reference is an owned UTF-8 `String` the task's
//! own output type produces (a JSON blob, a storage key, a URL — the task's
//! choice), trivially serde-serializable downstream and round-tripping through the
//! artifact schema's opaque `durable_reference` slot. Recorded per ticket-067
//! "Open questions".

use dagr_core::assembly::{DurableOutput, NodePolicy, ProblemKind};
use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{RehydrateError, RunContext, TaskError};

// A fake external durable store: a value written under a key, so `rehydrate` can
// read it back exactly like a real resume/replay reads from durable storage.
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

fn store() -> &'static Mutex<HashMap<String, String>> {
    static STORE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A durable output type whose value lives at a key in the fake external store.
/// Its reference is the key (a self-describing string), and `rehydrate` reads the
/// value back from the store using only that key — no live handle, no producing
/// process.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Snapshot {
    key: String,
    payload: String,
}

impl Snapshot {
    /// Write `payload` to the fake store under `key` and return the durable value.
    fn write(key: &str, payload: &str) -> Self {
        store()
            .lock()
            .unwrap()
            .insert(key.to_string(), payload.to_string());
        Self {
            key: key.to_string(),
            payload: payload.to_string(),
        }
    }
}

impl DurableOutput for Snapshot {
    fn serialize_reference(&self) -> String {
        // A self-describing reference: just the key here (a real task might embed a
        // URL + content hash). Infallible — the value is already written.
        self.key.clone()
    }

    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        // Reconstruct the typed value from the reference alone, reading the fake
        // external store — no dependency on the producing process.
        match store().lock().unwrap().get(reference) {
            Some(payload) => Ok(Self {
                key: reference.to_string(),
                payload: payload.clone(),
            }),
            None => Err(RehydrateError::absent(format!(
                "referent `{reference}` is gone"
            ))),
        }
    }
}

/// A source task producing a durable `Snapshot`.
struct MakeSnapshot;
impl Task for MakeSnapshot {
    type Input = ();
    type Output = Snapshot;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Snapshot, TaskError> {
        Ok(Snapshot::write("snap-node/output", "the-produced-bytes"))
    }
}

/// A non-durable in-memory output type — does NOT implement the contract.
struct Rows;
struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

// ---------------------------------------------------------------------------
// Serialize side of the contract, end to end: produce → serialize → rehydrate
// yields an EQUAL typed value (T57 test plan: "Serialize side of the contract is
// exercised end to end").
// ---------------------------------------------------------------------------

#[test]
fn serialize_then_rehydrate_round_trips_to_an_equal_value() {
    // A value written to a known durable location.
    let produced = Snapshot::write("round-trip/key", "hello durable world");

    // Serialize the reference (the string that lands in the artifact) …
    let reference = produced.serialize_reference();
    assert_eq!(
        reference, "round-trip/key",
        "the reference identifies the durable location"
    );

    // … serialize/deserialize the reference itself (it is a plain owned String, so
    // it round-trips through any serde carrier — here the identity is enough to
    // prove self-containment: no live handle travels with it).
    let carried: String = reference.clone();

    // … and rehydrate the typed value from that same reference, with NO access to
    // the producing value — only the string.
    let rehydrated = Snapshot::rehydrate(&carried).expect("rehydrate a live referent");
    assert_eq!(
        rehydrated, produced,
        "rehydrating from the serialized reference yields an equal typed value"
    );
}

#[test]
fn rehydrate_of_a_dangling_reference_is_a_typed_absent_error() {
    // A reference whose referent was never written / has been deleted.
    let err = Snapshot::rehydrate("never/written").expect_err("dangling reference must fail");
    assert!(
        err.is_absent(),
        "a gone referent classifies as absent, not a transient/corruption error"
    );
    // The error is a plain typed value carrying the reference in its message — feeds
    // T58's plan-time refusal and C26's single-node-replay refusal.
    assert!(
        err.to_string().contains("never/written"),
        "the error names the offending reference"
    );
}

// ---------------------------------------------------------------------------
// The contract is on the OUTPUT TYPE, not the task: a value can be rehydrated
// from its reference WITHOUT the producing task (T0.8 ADR §4 rationale — what
// makes single-node replay / resume able to rebuild an input it never produced).
// ---------------------------------------------------------------------------

#[test]
fn rehydrate_needs_only_the_reference_not_the_producing_task() {
    let produced = Snapshot::write("independent/key", "produced-once");
    let reference = produced.serialize_reference();
    drop(produced); // the producing value (and, conceptually, its task) is gone.

    // A brand-new value is reconstructed from the reference string alone.
    let reconstructed = Snapshot::rehydrate(&reference).expect("rehydrate without the producer");
    assert_eq!(reconstructed.payload, "produced-once");
    assert_eq!(reconstructed.key, "independent/key");
}

// ---------------------------------------------------------------------------
// The enriched contract still arms assembly's durable-without-contract check
// (T57 supersedes the marker without regressing the T14 enforcement seam).
// ---------------------------------------------------------------------------

#[test]
fn durable_with_the_full_contract_assembles() {
    let mut flow = Flow::new();
    let _ = flow.register_source_durable("snap", &MakeSnapshot, NodePolicy::new());
    let pipeline = flow.finish();
    // Durability is recorded in the effective policy in the graph artifact.
    assert!(
        pipeline
            .node(dagr_core::handle::NodeId::from_name("snap"))
            .expect("node present")
            .effective_policy()
            .is_durable(),
        "durability is recorded in the effective policy"
    );
    pipeline
        .assemble()
        .expect("a durable node whose output implements the full contract assembles");
}

#[test]
fn durable_without_the_contract_still_fails_assembly() {
    let mut flow = Flow::new();
    // `Rows` does not implement DurableOutput; marking the node durable fails.
    let _ = flow.register_source_with("snap", &MakeRows, NodePolicy::new().durable(true));
    let err = flow
        .finish()
        .assemble()
        .expect_err("durable without the contract must fail assembly");
    assert!(
        err.problems()
            .iter()
            .any(|p| p.kind() == ProblemKind::DurableWithoutContract),
        "the durable-without-contract problem is reported"
    );
}
