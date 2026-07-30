//! Common-trait and additive-conversion tests for `dagr-core` — ticket 111 (T96),
//! written first, TDD.
//!
//! Three families, all API-surface rather than behavior:
//!
//! * **`Debug` gaps** (`api-common-traits`). `Handle<T>` and the four slot
//!   capability types (`Slot<T>`, `SlotRef<T>`, `ConsumerLease<T>`,
//!   `RedemptionHandle<T>`) implemented no `Debug` at all, so none could appear
//!   in a `{:?}` diagnostic. The same module family already shows the right
//!   pattern: `Permit` and `ResidencyLease` hand-write `Debug` with
//!   `finish_non_exhaustive()` to omit a back-reference that is not usefully
//!   printable. These assert the pattern, not just the presence of the trait.
//! * **Freely-derivable traits** on plain-data types that lacked them.
//! * **Additive conversions** (`api-from-not-into`): a `From` impl that parallels
//!   an existing inherent constructor must agree with it on the same fixture, so
//!   the two paths cannot drift.
//!
//! And the `get_`-prefix rename on `DurableReferenceMeta`'s read accessors
//! (`name-no-get-prefix`), asserted through the new names.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use dagr_core::TaskError;
use dagr_core::admission::PoolCost;
use dagr_core::assembly::{DurableReferenceMeta, NodePolicy};
use dagr_core::context::{PipelineId, RunContext, RunId, ScratchStore};
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::limits::ContainerLimitProbe;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;

/// A deliberately un-`Debug` payload: the slot types' `Debug` must not require
/// `T: Debug` (a derive would have, wrongly — the same trap `Handle`'s manual
/// `Clone`/`Copy` already avoids).
struct Opaque;

/// The smallest possible source task — a handle is unforgeable, so minting one
/// means registering a real node.
struct Loader;

impl Task for Loader {
    type Input = ();
    type Output = Opaque;

    async fn run(&mut self, _ctx: &RunContext, (): ()) -> Result<Opaque, TaskError> {
        Ok(Opaque)
    }
}

fn hash_of<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

// ---------------------------------------------------------------------------
// `Debug` on the handle and the four slot capability types.
// ---------------------------------------------------------------------------

#[test]
fn handle_formats_under_debug_without_requiring_debug_on_its_value_type() {
    // A handle is only obtainable from registration, so go through a flow.
    let mut flow = Flow::new();
    let handle = flow.register_source("loader", &Loader);
    let rendered = format!("{handle:?}");
    assert!(
        rendered.starts_with("Handle"),
        "a handle names its own type in a diagnostic: {rendered}"
    );
    assert!(
        rendered.contains(&format!("{:?}", handle.id())),
        "a handle's Debug carries the node identity it refers to: {rendered}"
    );
}

#[test]
fn the_four_slot_capability_types_format_under_debug_non_exhaustively() {
    let ledger = ResidencyLedger::new();
    let slot: Slot<Opaque> = Slot::new(
        NodeId::from_name("producer"),
        "producer",
        1,
        true,
        4096,
        Arc::clone(&ledger),
    );
    let slot_ref = slot.shared_ref();
    let lease = slot_ref.enter();
    let redemption = slot.redemption_handle();

    for (label, rendered) in [
        ("Slot", format!("{slot:?}")),
        ("SlotRef", format!("{slot_ref:?}")),
        ("ConsumerLease", format!("{lease:?}")),
        ("RedemptionHandle", format!("{redemption:?}")),
    ] {
        assert!(
            rendered.starts_with(label),
            "{label} names itself in a diagnostic: {rendered}"
        );
        assert!(
            rendered.contains(".."),
            "{label} follows the Permit/ResidencyLease precedent and omits its \
             unprintable interior with finish_non_exhaustive(): {rendered}"
        );
    }

    // The node's registration name is what makes a slot diagnostic actionable —
    // the same field `Permit`'s Debug prints.
    assert!(
        format!("{slot:?}").contains("producer"),
        "a slot's Debug names the node whose output it holds"
    );
}

/// The lesser `Debug` case in this crate: the kit owns the task under test and a
/// registry of erased fakes, neither of which is `Debug`, so it follows the same
/// precedent — the configured context identity shown, the un-printable interior
/// recorded as omitted.
#[test]
fn single_task_test_formats_under_debug_non_exhaustively() {
    let kit = dagr_core::test_kit::SingleTaskTest::new(Loader).node("loader");
    let rendered = format!("{kit:?}");
    assert!(
        rendered.starts_with("SingleTaskTest"),
        "the kit names itself in a diagnostic: {rendered}"
    );
    assert!(
        rendered.contains("loader"),
        "the kit's Debug carries the node identity it configured: {rendered}"
    );
    assert!(
        rendered.contains(".."),
        "the task and the erased fake registry are omitted with \
         finish_non_exhaustive(): {rendered}"
    );
}

// ---------------------------------------------------------------------------
// Freely-derivable traits on plain-data types.
// ---------------------------------------------------------------------------

#[test]
fn log_span_compares_and_hashes_by_identity() {
    let ctx = RunContext::for_test();
    let span = ctx.span().clone();
    let same = span.clone();
    assert_eq!(span, same, "a LogSpan is its run/node/attempt identity");
    assert_eq!(
        hash_of(&span),
        hash_of(&same),
        "equal spans hash equally, so a span can key a map"
    );
}

#[test]
fn scratch_store_compares_and_hashes_by_namespace() {
    // TEMP-BASE-EXEMPT: a value-identity test over `ScratchStore` handles. Nothing
    // is created, opened, or written — the path is only a component of the
    // namespace whose equality and hashing are under test.
    let base = std::path::Path::new("/tmp/t96-scratch");
    let pipeline = PipelineId::new("p");
    let run = RunId::new("r");
    let a = ScratchStore::for_node(base, &pipeline, &run, NodeId::from_name("n"));
    let b = ScratchStore::for_node(base, &pipeline, &run, NodeId::from_name("n"));
    let other = ScratchStore::for_node(base, &pipeline, &run, NodeId::from_name("m"));
    assert_eq!(a, b, "two handles on one namespace are the same store");
    assert_ne!(a, other, "a different node is a different namespace");
    assert_eq!(hash_of(&a), hash_of(&b));
}

#[test]
fn container_limit_probe_compares_by_configuration() {
    let a = ContainerLimitProbe::from_root("/fixture").with_host_cores(4);
    let b = ContainerLimitProbe::from_root("/fixture").with_host_cores(4);
    let c = ContainerLimitProbe::from_root("/fixture").with_host_cores(8);
    assert_eq!(a, b, "two identically-configured probes are equal");
    assert_ne!(a, c, "a different host-core count is a different probe");
}

// ---------------------------------------------------------------------------
// Additive conversions: the `From` impl agrees with the inherent constructor.
// ---------------------------------------------------------------------------

#[test]
fn from_cost_vector_agrees_with_the_inherent_constructor() {
    // One fixture, both paths — so the two cannot drift.
    let cost = NodePolicy::new()
        .working_memory(8 * 1024)
        .output_residency(2 * 1024)
        .blocking_threads(3)
        .compute_threads(5)
        .cost();

    let inherent = PoolCost::from_cost_vector(cost);
    let converted: PoolCost = PoolCost::from(cost);
    assert_eq!(
        inherent, converted,
        "`From<CostVector> for PoolCost` and `PoolCost::from_cost_vector` are one \
         conversion with two spellings"
    );
    assert_eq!(converted.working_memory_bytes(), 8 * 1024);
    assert_eq!(converted.output_residency_bytes(), 2 * 1024);
    assert_eq!(converted.blocking_thread_count(), 3);
    assert_eq!(converted.compute_thread_count(), 5);

    // `.into()` at a call site resolves to the same value.
    let inferred: PoolCost = cost.into();
    assert_eq!(inferred, inherent);
}

// ---------------------------------------------------------------------------
// The `get_`-prefix rename (`name-no-get-prefix`).
// ---------------------------------------------------------------------------

#[test]
fn durable_reference_meta_read_accessors_carry_no_get_prefix() {
    let meta = DurableReferenceMeta::new()
        .content_hash("sha256:abc")
        .size_bytes(4096)
        .scheme("file")
        .produced_at_offset_ns(4242);

    assert_eq!(meta.recorded_content_hash(), Some("sha256:abc"));
    assert_eq!(meta.recorded_size_bytes(), Some(4096));
    assert_eq!(meta.recorded_scheme(), Some("file"));
    assert_eq!(meta.recorded_produced_at_offset_ns(), Some(4242));

    let empty = DurableReferenceMeta::new();
    assert!(empty.is_empty());
    assert_eq!(empty.recorded_content_hash(), None);
    assert_eq!(empty.recorded_size_bytes(), None);
    assert_eq!(empty.recorded_scheme(), None);
    assert_eq!(empty.recorded_produced_at_offset_ns(), None);
}
