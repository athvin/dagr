// UI compile-failure fixture — ticket T13 (023), case
// `flow_finish_consumes_builder`.
//
// PROVES (C7; arch.md "Flow assembly"): finalization CONSUMES the builder by
// value — "a finalization step that consumes the builder and yields an immutable
// pipeline." Once `finish()` has been called, the builder is moved out and no
// further registration through it is possible; using the builder afterward is a
// use-of-moved-value COMPILE error, not a runtime check. This is the REAL flow
// builder API (dagr_core::flow), not a throwaway sketch.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::flow::Flow;
use dagr_core::handle::Handle;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct Rows;

struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

fn main() {
    let mut flow = Flow::new();
    let _rows: Handle<Rows> = flow.register_source("rows", &MakeRows);

    // Finalization consumes the builder by value.
    let _pipeline = flow.finish();

    // The builder was moved into `finish`; registering again through it is a
    // use-of-moved-value compile error — there is no route by which the graph
    // shape could change after finalization.
    let _late: Handle<Rows> = flow.register_source("late", &MakeRows);
}
