// UI compile-failure sample — ticket T9 (019), C1 task-type bound.
//
// THE most common first-hour error (arch.md C1; ticket DoD): capturing a
// non-`Send` value in a task. The rustdoc on the real `Task` trait carries this
// same worked example; this file is its compile-fail mirror.
//
// The T8 UI harness compiles each `tests/ui/*.rs` sample STANDALONE with the
// pinned `rustc` and NO `--extern dagr_core`, so this sample cannot name the
// real `Task` trait. It reproduces the exact bound the real trait imposes — a
// task value must be `Send + 'static` — with a local `assert_task_bound` whose
// `Send` bound is what the framework requires before moving a task to a worker
// thread. Capturing an `Rc` (deliberately `!Send`) violates it.
//
// The diagnostic names two distinct types the snapshot keys on: `Rc` (the
// non-`Send` captured value's type) and `NonSendTask` (the offending task type).

use std::rc::Rc;

/// A task value the framework must be able to move to a worker thread: the real
/// `Task` supertrait bound is `Send + 'static`. Mirrored here locally.
struct NonSendTask {
    // `Rc<u32>` is deliberately NOT `Send` — the adversarial captured value.
    shared: Rc<u32>,
}

/// Stand-in for the framework's requirement that a registered task be `Send`.
fn assert_task_bound<T: Send + 'static>(_task: T) {}

fn main() {
    let task = NonSendTask { shared: Rc::new(1) };
    // Registering / moving the task to a worker thread requires `Send`; the
    // captured `Rc<u32>` makes `NonSendTask` non-`Send`, so this fails to
    // compile with an E0277 naming both `Rc` and `NonSendTask`.
    assert_task_bound(task);
}
