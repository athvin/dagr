// UI compile-failure fixture — ticket T0.9 (015), case `ordering_edge_back_edge`.
//
// PROVES: an ordering-edge cycle BACK to a descendant is inexpressible by
// CONSTRUCTION — structural per arch.md C2 — because there is NO after-the-fact
// "add edge" API to reach back to an already-closed registration (C4 acceptance
// criterion: "no API exists to add an edge between two existing nodes
// afterward"). Wired to the same T8 UI harness (crates/core/tests/ui.rs) that
// T12 reuses; the sibling `.stderr` names the substrings the diagnostic must
// contain, and the harness asserts this sample FAILS to compile under the pinned
// toolchain.
//
// THROWAWAY, intentionally NON-COMPILING SKETCH — NOT dagr's real authoring API
// (that lands in T13 / T50). It models the settled C4 discipline: A registers
// first, then B registers an ordering edge on A; the only edge-declaring entry
// point is `register`, taken at a node's OWN registration. To make A order-after
// B afterward one would need a `Flow::add_ordering_edge(existing, existing)`
// method — which the surface deliberately DOES NOT offer. Attempting to call it
// is a compile error, so the back-edge is inexpressible; no runtime check exists.

#[derive(Clone, Copy)]
struct Handle;

struct Flow;
impl Flow {
    fn register(&mut self, _ordering_upstreams: &[Handle]) -> Handle {
        Handle
    }
    // There is deliberately NO `add_ordering_edge(existing, existing)` method:
    // an ordering edge is only ever declared at a node's own registration, and
    // a registration cannot be reopened. The absence is the guarantee.
}

fn main() {
    let mut flow = Flow;

    let handle_a = flow.register(&[]);
    let handle_b = flow.register(&[handle_a]);

    // Attempt to add a back-edge (A order-after B) AFTER THE FACT. No such API
    // exists; the only edge-declaring entry point (`register`) was already used
    // for A and cannot be reopened. This fails to compile.
    flow.add_ordering_edge(handle_a, handle_b);

    let _ = handle_b;
}
