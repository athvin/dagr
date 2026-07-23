// UI compile-failure fixture — ticket T13 (023), case `flow_pipeline_immutable`.
//
// PROVES (C7; arch.md "Flow assembly"): finalization CONSUMES the builder and
// yields an IMMUTABLE pipeline — "once produced, no further registration or
// mutation is possible." Mutation-after-finalize is not a runtime check; it is
// INEXPRESSIBLE. The finalized `Pipeline` exposes only read access to its node
// set, so it has no `register`/`register_source` method: a call to one is a
// compile error ("no method named ..."). This is the REAL flow builder API
// (dagr_core::flow), not a throwaway sketch.
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
    let pipeline = flow.finish();

    // The finalized pipeline is immutable: it has no `register_source` method —
    // registration is impossible once the builder has been consumed. This is a
    // compile error ("no method named `register_source`"), never a runtime one.
    let _more: Handle<Rows> = pipeline.register_source("more-rows", &MakeRows);
}
