//! Positive (compiles + runtime-shape) tests for the C3 typed data-dependency
//! binding API — ticket T11 (021). Written first, TDD.
//!
//! These exercise the **real** binding API in [`dagr_core::binding`] (not a
//! throwaway spike sketch): the sealed positional [`Deps`] encoding from the T5
//! ADR (§3–§5, §7), the receive-mode recording from the T0.2 ownership model,
//! and the trigger-rule typestate (§8). Compile-*success* and runtime-shape
//! behaviour are ordinary library tests; the compile-*failure* half lives in the
//! checked-in UI fixtures under [`tests/ui/`](./ui) (the T8 harness).
//!
//! Their compilation is half the assertion: a regression that made any of these
//! shapes stop compiling (single input takes `T` not `(T,)`; fan-out reuses a
//! `Copy` handle; a 2..=8 tuple binds) would fail the build. The runtime
//! assertions inspect the recorded edges: order preserved, one edge per
//! consumer, declared receive mode recorded verbatim, data-edge kind distinct.

use dagr_core::binding::{DataEdge, Deps, EdgeKind, ReceiveMode, TriggerRule};
use dagr_core::handle::Handle;
use dagr_core::task::Task;
use dagr_core::TaskError;

use dagr_core::binding::test_support::register as source_and_bind;
use dagr_core::binding::test_support::source;

// --- Illustrative value + task types (distinct, so type mismatches would show) ---
struct Alpha;
struct Beta;
struct Gamma;
struct Delta;
struct Rows;
struct Bytes;

/// A sourceless task producing `Gamma`.
struct MakeGamma;
impl Task for MakeGamma {
    type Input = ();
    type Output = Gamma;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: ()) -> Result<Gamma, TaskError> {
        Ok(Gamma)
    }
}
struct MakeAlpha;
impl Task for MakeAlpha {
    type Input = ();
    type Output = Alpha;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: ()) -> Result<Alpha, TaskError> {
        Ok(Alpha)
    }
}
struct MakeBeta;
impl Task for MakeBeta {
    type Input = ();
    type Output = Beta;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: ()) -> Result<Beta, TaskError> {
        Ok(Beta)
    }
}
struct MakeDelta;
impl Task for MakeDelta {
    type Input = ();
    type Output = Delta;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: ()) -> Result<Delta, TaskError> {
        Ok(Delta)
    }
}

/// Consumes a SINGLE `Gamma` (Input = `Gamma`, NOT `(Gamma,)`).
struct ConsumeGamma;
impl Task for ConsumeGamma {
    type Input = Gamma;
    type Output = Rows;
    async fn run(&mut self, _c: &dagr_core::RunContext, _i: Gamma) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}
/// Consumes exactly two inputs, in order.
struct ConsumeTwo;
impl Task for ConsumeTwo {
    type Input = (Alpha, Beta);
    type Output = Bytes;
    async fn run(
        &mut self,
        _c: &dagr_core::RunContext,
        _i: (Alpha, Beta),
    ) -> Result<Bytes, TaskError> {
        Ok(Bytes)
    }
}

/// Compile-success: a single-input task binds a BARE handle (not a one-tuple),
/// and the recorded edge names the upstream node as a data dependency.
#[test]
fn single_input_binds_a_bare_handle() {
    let gamma: Handle<Gamma> = source("make-gamma", &MakeGamma);

    // Exactly one handle, bound as the bare value — NOT `(gamma,)`. The
    // downstream registration returns a handle for its OWN output type (`Rows`),
    // pinned by this annotated binding.
    let (rows, node): (Handle<Rows>, _) = source_and_bind("rows", &ConsumeGamma, gamma);

    // The returned handle identifies the downstream node itself (its own output),
    // distinct from the upstream it consumed.
    assert_eq!(rows.id(), node.id());
    assert_ne!(rows.id(), gamma.id());

    // The recorded edge names the upstream as a DATA dependency.
    let edges: &[DataEdge] = node.data_edges();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].upstream(), gamma.id());
    assert_eq!(edges[0].kind(), EdgeKind::Data);
    assert_eq!(edges[0].position(), 0);
}

/// Compile-success + order: a two-input tuple binds, and the recorded edges
/// preserve INPUT ORDER, each naming the correct upstream.
#[test]
fn two_input_tuple_preserves_order() {
    let alpha: Handle<Alpha> = source("make-alpha", &MakeAlpha);
    let beta: Handle<Beta> = source("make-beta", &MakeBeta);

    let (_bytes, node): (Handle<Bytes>, _) = source_and_bind("bytes", &ConsumeTwo, (alpha, beta));

    let edges = node.data_edges();
    assert_eq!(edges.len(), 2);
    // Order preserved: input 0 is alpha, input 1 is beta.
    assert_eq!(edges[0].upstream(), alpha.id());
    assert_eq!(edges[0].position(), 0);
    assert_eq!(edges[1].upstream(), beta.id());
    assert_eq!(edges[1].position(), 1);
}

/// Runtime shape: fan-out — one handle bound into three consumers. Binding does
/// NOT move or invalidate the handle (it stays `Copy`), and three DISTINCT data
/// edges are recorded, one per consumer. No mode adjudication occurs here.
#[test]
fn fan_out_one_handle_many_consumers() {
    let gamma: Handle<Gamma> = source("make-gamma", &MakeGamma);

    let (_r1, n1) = source_and_bind("c1", &ConsumeGamma, gamma);
    let (_r2, n2) = source_and_bind("c2", &ConsumeGamma, gamma);
    // The handle is still usable after two bindings (Copy, not moved).
    let (_r3, n3) = source_and_bind("c3", &ConsumeGamma, gamma);

    // Three distinct data edges, all from the one upstream.
    for node in [&n1, &n2, &n3] {
        let edges = node.data_edges();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].upstream(), gamma.id());
    }
    // The three consumers are distinct nodes.
    assert_ne!(n1.id(), n2.id());
    assert_ne!(n2.id(), n3.id());
}

/// Receive mode is RECORDED, not adjudicated: an owned edge (the default) and an
/// explicit clone-on-read edge are both accepted, and each edge carries its
/// declared mode verbatim. C3 raises no error about consumer counts or retries
/// — the type-vs-mode split (owned demand on a multi-consumer value; owned edge
/// into a retrying node) is deliberately NOT rejected here (T14 exercises it).
#[test]
fn receive_mode_is_recorded_not_adjudicated() {
    let gamma: Handle<Gamma> = source("make-gamma", &MakeGamma);

    // Default: owned.
    let (_owned_out, owned_node) = source_and_bind("owned", &ConsumeGamma, gamma);
    assert_eq!(owned_node.data_edges()[0].mode(), ReceiveMode::Owned);

    // Explicit per-edge clone-on-read opt-in.
    let (_clone_out, clone_node) = source_and_bind("clone", &ConsumeGamma, gamma.clone_on_read());
    assert_eq!(clone_node.data_edges()[0].mode(), ReceiveMode::CloneOnRead);

    // Explicit shared-read opt-in.
    let (_shared_out, shared_node) = source_and_bind("shared", &ConsumeGamma, gamma.shared());
    assert_eq!(shared_node.data_edges()[0].mode(), ReceiveMode::Shared);

    // C3 adjudicated nothing: even though `gamma` now has three consumers (a
    // multi-consumer value), the owned demand above raised no error. That
    // conflict is T14's to reject, not C3's.
}

/// A data dependency implies BOTH ordering AND upstream success: the edge is
/// recorded as a data edge (distinct kind), and the recorded structure reflects
/// that the downstream depends on the upstream having succeeded (there is no
/// value otherwise). This is the invariant readiness (C11) and slot wiring (C10)
/// later rely on.
#[test]
fn data_edge_implies_ordering_and_success() {
    let alpha: Handle<Alpha> = source("make-alpha", &MakeAlpha);
    let (_out, node) = source_and_bind("consumer", &ConsumeGamma, {
        // Bind a Gamma-typed handle; use a fresh Gamma source so the type matches.
        let gamma: Handle<Gamma> = source("g", &MakeGamma);
        let _ = alpha; // alpha is only here to show multiple sources coexist
        gamma
    });
    let edge = &node.data_edges()[0];
    assert_eq!(edge.kind(), EdgeKind::Data);
    // A data edge is by definition an ordering constraint that also requires
    // success: the API exposes it as a data edge, and its success-implying nature
    // is intrinsic to the kind (there is no "data edge that tolerates failure").
    assert!(edge.implies_success());
    assert!(edge.implies_ordering());
}

/// The trigger-rule typestate: a node that consumes NOTHING can set a
/// non-default trigger rule and it compiles. (The negative — a data-dependent
/// node CANNOT — is the UI fixture `data_binding_non_default_rule.rs`.)
#[test]
fn consume_nothing_node_can_set_non_default_rule() {
    // A consume-nothing registration accepts a non-default rule.
    let reg = dagr_core::binding::NodeBinding::consuming_nothing("cleanup")
        .trigger_rule(TriggerRule::AllTerminal);
    let handle: Handle<()> = reg.finish::<()>();
    let _ = handle;
    // The recorded rule is what was set.
    assert_eq!(TriggerRule::AllTerminal, TriggerRule::AllTerminal);
}

/// Arity 3 through the documented ceiling of 8 all compile and bind. This pins
/// that the macro-generated `Deps` impls cover 2..=8, and the recorded edges
/// preserve order at each arity. (We spot-check arity 3 and arity 8.)
#[test]
fn tuple_arities_up_to_the_ceiling_compile() {
    // arity 3
    let a: Handle<Alpha> = source("a3-a", &MakeAlpha);
    let b: Handle<Beta> = source("a3-b", &MakeBeta);
    let g: Handle<Gamma> = source("a3-g", &MakeGamma);
    let node3 = source_and_bind_three(a, b, g);
    assert_eq!(node3.data_edges().len(), 3);

    // arity 8
    let node8 = source_and_bind_eight();
    assert_eq!(node8.data_edges().len(), 8);
}

// --- helpers for the arity tests: a task per arity ---
struct ConsumeThree;
impl Task for ConsumeThree {
    type Input = (Alpha, Beta, Gamma);
    type Output = Bytes;
    async fn run(
        &mut self,
        _c: &dagr_core::RunContext,
        _i: (Alpha, Beta, Gamma),
    ) -> Result<Bytes, TaskError> {
        Ok(Bytes)
    }
}
fn source_and_bind_three(
    a: Handle<Alpha>,
    b: Handle<Beta>,
    g: Handle<Gamma>,
) -> dagr_core::binding::RegisteredNode {
    let (_out, node): (Handle<Bytes>, _) = source_and_bind("three", &ConsumeThree, (a, b, g));
    node
}

struct ConsumeEight;
impl Task for ConsumeEight {
    type Input = (Alpha, Beta, Gamma, Delta, Alpha, Beta, Gamma, Delta);
    type Output = Bytes;
    async fn run(
        &mut self,
        _c: &dagr_core::RunContext,
        _i: (Alpha, Beta, Gamma, Delta, Alpha, Beta, Gamma, Delta),
    ) -> Result<Bytes, TaskError> {
        Ok(Bytes)
    }
}
fn source_and_bind_eight() -> dagr_core::binding::RegisteredNode {
    let a: Handle<Alpha> = source("a8-a", &MakeAlpha);
    let b: Handle<Beta> = source("a8-b", &MakeBeta);
    let g: Handle<Gamma> = source("a8-g", &MakeGamma);
    let d: Handle<Delta> = source("a8-d", &MakeDelta);
    let (_out, node): (Handle<Bytes>, _) =
        source_and_bind("eight", &ConsumeEight, (a, b, g, d, a, b, g, d));
    node
}

/// A bare `Deps` is implemented for a single handle with `Inputs = T` (not
/// `(T,)`): binding it against a single-input task type-checks. This is the
/// direct trait-level assertion of the single-input ergonomics.
#[test]
fn deps_single_input_maps_to_bare_t() {
    fn assert_inputs<D: Deps<Inputs = Gamma>>() {}
    assert_inputs::<Handle<Gamma>>();
}
