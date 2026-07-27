// UI compile-failure sample — the task-type bound.
//
// THE most common first-hour error: capturing a
// non-`Send` value in a task. The rustdoc on the real `Task` trait carries this
// same worked example, and the cookbook's "non-`Send` capture" entry reproduces
// this exact broken form; its FIXES (capture an `Arc`, or build the value inside
// `run`) compile and are exercised by the compiling cookbook tests.
//
// This is the REAL authoring API: `dagr_core::flow::Flow::register_source`, whose
// `T: Task` bound (and `Task: Send + 'static`) is what the framework requires
// before moving a task value to a worker thread. Capturing an `Rc` (deliberately
// `!Send`) violates it. The UI harness links this sample against the built
// `dagr_core` rlib (crates/core/tests/ui.rs) and asserts only that it FAILS to
// compile under the pinned toolchain, with the snapshot substrings present.

use std::rc::Rc;

use dagr_core::flow::Flow;
use dagr_core::task::{RunContext, Task};
use dagr_core::TaskError;

/// A task that captures a non-`Send` value (`Rc`) in its configuration. Because
/// `Rc<u32>` is deliberately `!Send`, this task struct is `!Send` — and a task
/// must be `Send` to be moved onto a worker thread. The fix is to capture an
/// `Arc` instead (or build the value inside `run`), not to weaken the bound.
struct NonSendTask {
    shared: Rc<u32>,
}

impl Task for NonSendTask {
    type Input = ();
    type Output = u32;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u32, TaskError> {
        Ok(*self.shared)
    }
}

fn main() {
    let task = NonSendTask { shared: Rc::new(1) };
    // Registering the task moves it toward a worker thread and demands `Send`; the
    // captured `Rc<u32>` makes `NonSendTask` non-`Send`, so this fails to compile
    // with an E0277 naming both `Rc` and `NonSendTask`.
    let mut flow = Flow::new();
    let _ = flow.register_source("non-send", &task);
}
