// UI compile-failure fixture — case `ordering_edge_self_cycle`.
//
// PROVES: an ordering-edge cycle to SELF is inexpressible by CONSTRUCTION —
// structural, enforced by the type system, never a runtime cycle-detection pass.
// This is the ordering-edge half of the acceptance criterion "an attempt to
// express a cycle — through data edges or ORDERING edges — fails to compile,
// demonstrated by a checked-in compile-failure test." It is wired to the same
// UI harness (crates/core/tests/ui.rs) that the full wiring compile-fail suite
// reuses: a sibling `.stderr` names the substrings the diagnostic must contain,
// and the harness asserts this sample FAILS to compile under the pinned toolchain.
//
// This is a THROWAWAY, intentionally NON-COMPILING SKETCH — NOT a use of dagr's
// real authoring API (handles, the flow builder, ordering-edge declaration),
// which lands in the real builder (node identity) and ordering-edge surface. It
// models only the settled registration-time backward-reference discipline:
//
//   * a `Handle` is obtainable ONLY by registering a node, and
//   * `register` takes already-existing `Handle`s as ordering upstreams.
//
// Consequently a node cannot name its OWN handle among its OWN ordering
// upstreams: the handle binding does not exist yet at the point of the argument
// expression, so the back-edge to self cannot be written. The real wiring
// cases replay this same structural argument against the real API.

#[derive(Clone, Copy)]
struct Handle;

struct Flow;
impl Flow {
    // An ordering edge is declared at the DOWNSTREAM node's registration, taking
    // already-existing handles as ordering upstreams. The returned handle is the
    // ONLY way to refer to this node afterward (no lookup by name/index/key).
    fn register(&mut self, _ordering_upstreams: &[Handle]) -> Handle {
        Handle
    }
}

fn main() {
    let mut flow = Flow;

    // Attempt a self-cycle: reference `a`'s handle among `a`'s OWN ordering
    // upstreams. `a` is not yet bound where the argument is evaluated, so there
    // is no handle to pass — a use of an undeclared binding. The cycle is
    // therefore inexpressible; no runtime check is involved.
    let a = flow.register(&[a]);

    let _ = a;
}
