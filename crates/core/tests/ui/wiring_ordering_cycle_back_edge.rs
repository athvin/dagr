// UI compile-failure fixture — ticket T50 (062), case `wiring_ordering_cycle_back_edge`.
//
// PROVES (C2/C4; arch.md `### C2 · Handle`, `### C4 · Ordering dependency`): an
// ORDERING-edge cycle cannot be closed AFTER THE FACT either — a back-edge is as
// inexpressible as a self-edge. This is the REAL authoring API
// (`dagr_core::flow::Flow`, `Flow::register_source_ordered_after`,
// `Handle::ordering()`). Ordering node "a" after node "b", when "b" is registered
// AFTER "a", requires naming `b`'s handle inside "a"'s ordering argument — but "b"
// does not exist there yet (it is the return value of a LATER registration), and
// there is NO after-the-fact edge API to add an ordering edge once "a" is
// registered (arch.md §141: "no API exists to add an edge between two existing
// nodes afterward"). The forward reference is a use of an undeclared binding
// (E0425), so the back-edge cannot be written. The cycle guarantee extends across
// BOTH edge kinds (C2/C4); the data-edge counterpart is `wiring_data_cycle_back_edge`.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

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
    // Attempt a back-edge: node "a" is ordered after `b`, but "b" is registered
    // AFTER "a". `b` is the return value of the LATER registration, so it is not
    // yet bound where "a"'s ordering argument is evaluated — a forward reference to
    // an undeclared binding. "a"'s registration is closed once written and there is
    // no API to add an `a -> b` ordering edge afterward, so the cycle cannot be
    // EXPRESSED.
    let a = flow.register_source_ordered_after("a", &Effect, &[b.ordering()]);
    let b = flow.register_source_ordered_after("b", &Effect, &[a.ordering()]);
    let _ = (a, b);
}
