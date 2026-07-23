// UI compile-failure fixture — ticket T12 (024), case `wiring_data_cycle_self_edge`.
//
// PROVES (C2; arch.md `### C2 · Handle`): a cycle via a DATA edge is
// INEXPRESSIBLE by CONSTRUCTION — structural, never a later or runtime
// validation pass. This is the REAL authoring API (`dagr_core::flow::Flow`,
// `Flow::register`), not the throwaway T5 sketch: a node's output handle is the
// return value of its own `register` call, so it does not exist at the point its
// own binding expression is evaluated. A node therefore cannot bind its OWN
// not-yet-returned handle as a data input — it is a use of an undeclared binding
// (E0425), and the self-cycle cannot be written.
//
// This is the data-edge half of C2's acceptance criterion "an attempt to express
// a cycle — through data edges or ordering edges — fails to compile, demonstrated
// by a checked-in compile-failure test"; the ordering-edge half is the T0.9
// `ordering_edge_*` fixtures, folded into this same suite.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28). The positive
// counterpart (an acyclic data chain that DOES compile) lives in
// crates/core/tests/flow_builder.rs, so a regression that loosened this
// guarantee would fail review.

use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct V;

// A data-dependent task consuming one `V` and producing one `V`.
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
    // Attempt a self-cycle: node "a" binds its OWN handle `a` as its data input.
    // `a` is the return value of THIS `register` call, so it is not yet bound
    // where the argument is evaluated — a use of an undeclared binding. The
    // cycle cannot be EXPRESSED; there is no runtime cycle-detection pass.
    let a = flow.register("a", &Consume, a);
    let _ = a;
}
