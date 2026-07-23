// UI compile-failure fixture — ticket T12 (024), case `wiring_data_cycle_back_edge`.
//
// PROVES (C2; arch.md `### C2 · Handle`): a cycle via a DATA edge cannot be
// closed AFTER THE FACT either — a back-edge is as inexpressible as a self-edge.
// This is the REAL authoring API (`dagr_core::flow::Flow`, `Flow::register`).
// Registering node "a" that binds node "b", when "b" is registered AFTER "a",
// requires naming `b`'s handle inside `a`'s binding expression — but `b` does
// not exist there yet (it is the return value of a LATER `register` call), and
// there is no after-the-fact edge API to add `a -> b` once "a" is registered.
// The forward reference is a use of an undeclared binding (E0425), so the
// back-edge cannot be written. This is the same backward-reference discipline
// the T0.9 ordering-edge fixtures prove, now shown against the real data-binding
// surface — the cycle guarantee extends across BOTH edge kinds (C2/C4).
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28). The positive
// counterpart (a forward-only A->B->C chain that DOES compile) lives in
// crates/core/tests/flow_builder.rs.

use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct V;

// A source producing `V`, and a data-dependent task consuming one `V`.
struct Src;
impl Task for Src {
    type Input = ();
    type Output = V;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<V, TaskError> {
        Ok(V)
    }
}
struct Consume;
impl Task for Consume {
    type Input = V;
    type Output = V;
    async fn run(&mut self, _c: &RunContext, _i: V) -> Result<V, TaskError> {
        Ok(V)
    }
}

fn main() {
    let mut flow = Flow::new();
    let _seed = flow.register_source("seed", &Src);
    // Attempt a back-edge: node "a" binds `b`, but "b" is registered AFTER "a".
    // `b` is the return value of the LATER `register` call, so it is not yet
    // bound where "a"'s argument is evaluated — a forward reference to an
    // undeclared binding. "a"'s registration is closed once written and there is
    // no API to add an `a -> b` edge afterward, so the cycle cannot be EXPRESSED.
    let a = flow.register("a", &Consume, b);
    let b = flow.register("b", &Consume, a);
    let _ = (a, b);
}
