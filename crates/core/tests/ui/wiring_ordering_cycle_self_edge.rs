// UI compile-failure fixture — ticket T50 (062), case `wiring_ordering_cycle_self_edge`.
//
// PROVES (C2/C4; arch.md `### C2 · Handle`, `### C4 · Ordering dependency`): an
// ORDERING-edge cycle is INEXPRESSIBLE by CONSTRUCTION — structural, never a
// runtime or later cycle-detection pass. This is the REAL authoring API
// (`dagr_core::flow::Flow`, `Flow::register_source_ordered_after`, and the
// type-erased `Handle::ordering()`), not the throwaway T0.9 sketch: a node's
// output handle is the return value of its own registration, so it does not exist
// where its own ordering-edge argument is evaluated. A node therefore cannot name
// its OWN not-yet-returned handle among its OWN ordering upstreams — a use of an
// undeclared binding (E0425), and the self-cycle cannot be written.
//
// This is the ordering-edge half of C2's acceptance criterion "an attempt to
// express a cycle — through data edges or ordering edges — fails to compile,
// demonstrated by a checked-in compile-failure test"; the data-edge half is
// `wiring_data_cycle_self_edge`, and the same backward-reference discipline holds
// across BOTH edge kinds (C2/C4). The T0.9 `ordering_edge_self_cycle` sketch is
// superseded for the REAL API by this fixture.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28). The positive
// counterpart (an ordering edge against an EXISTING upstream that DOES compile)
// lives in crates/core/tests/ordering_edges.rs.

use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

// A sourceless effect-only task (the cleanup/notify shape ordering edges order).
struct Effect;
impl Task for Effect {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

fn main() {
    let mut flow = Flow::new();
    // Attempt a self-cycle: node "a" is ordered after its OWN handle `a`. `a` is
    // the return value of THIS registration, so it is not yet bound where the
    // ordering argument is evaluated — a use of an undeclared binding. The cycle
    // cannot be EXPRESSED; there is no runtime cycle-detection pass.
    let a = flow.register_source_ordered_after("a", &Effect, &[a.ordering()]);
    let _ = a;
}
