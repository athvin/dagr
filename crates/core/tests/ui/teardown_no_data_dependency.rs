// UI compile-failure fixture — ticket T52 (064), case
// `teardown_no_data_dependency`.
//
// PROVES (C17; C3; arch.md §52, §126, §371): a teardown node NEVER has data
// dependencies, and this is a COMPILE error, not a runtime or assembly check. A
// teardown fires on the non-default `all-terminal` rule, which C3's builder
// typestate makes inexpressible on any node that consumes data — so the
// `register_teardown` seam only accepts a consume-nothing task (`Task<Input =
// ()>`). Passing a task that consumes a value is a trait-bound error: `Input = ()`
// is not satisfied. A data-dependent teardown therefore cannot even be spelled.
//
// This is the REAL authoring API (dagr_core::flow::Flow::register_teardown), not a
// throwaway sketch. Wired to the T8 UI harness (crates/core/tests/ui.rs); the
// sibling `.stderr` names the substrings the diagnostic must contain, and the
// harness asserts this sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct Rows;

// A task that CONSUMES a value — `Input = u64`, not `()`.
struct ConsumesData;
impl Task for ConsumesData {
    type Input = u64;
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: u64) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

// A plain consume-nothing task to cover.
struct MakeCount;
impl Task for MakeCount {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}

fn main() {
    let mut flow = Flow::new();
    let count = flow.register_source("count", &MakeCount);
    // `register_teardown` requires a consume-nothing task (`Task<Input = ()>`),
    // because a teardown fires on `all-terminal` and a data-consuming node cannot
    // carry a non-default rule (C3/C4). Passing a data-consuming task is a
    // compile error — a teardown can never have a data dependency.
    let _t = flow.register_teardown("cleanup", &ConsumesData, &[count.ordering()]);
}
