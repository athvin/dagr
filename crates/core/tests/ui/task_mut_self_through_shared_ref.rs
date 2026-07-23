// UI compile-failure sample — ticket T9 (019), C1 exclusive `&mut self` work
// signature.
//
// The work takes the task EXCLUSIVELY (`&mut self`), which is what makes
// sequential attempts safe without any synchronization written by the author
// (arch.md C1; T0.2 ADR). Invoking the work through a SHARED reference must
// therefore fail to compile — the mirror of the behavioral test that invokes
// the work twice through `&mut self` and observes the mutation.
//
// The T8 harness compiles this STANDALONE with no `--extern dagr_core`, so the
// real `Task` trait is unavailable; the `&mut self` work signature is
// reproduced locally on a `TaskWork` trait (mirroring `Task::run`) implemented
// by `MutableTask`. Calling that `&mut self` work through a `&MutableTask`
// fails to compile — the shared reference cannot supply the exclusive access
// the work demands.
//
// The diagnostic names two distinct types the snapshot keys on: `TaskWork` (the
// work trait whose `&mut self` method is being called) and `MutableTask` (the
// task type behind the shared reference), plus the mutability wording.

/// The exclusive-access work trait, mirroring the real `Task::run(&mut self)`.
trait TaskWork {
    fn run(&mut self) -> u32;
}

/// A task whose work mutates a captured field and therefore takes `&mut self`.
struct MutableTask {
    seen: u32,
}

impl TaskWork for MutableTask {
    fn run(&mut self) -> u32 {
        self.seen += 1;
        self.seen
    }
}

fn call_through_shared(task: &MutableTask) -> u32 {
    // `TaskWork::run` takes `&mut self`, but `task` is a shared `&MutableTask`;
    // supplying a shared reference where exclusive access is required fails to
    // compile (types differ in mutability).
    TaskWork::run(task)
}

fn main() {
    let task = MutableTask { seen: 0 };
    let _ = call_through_shared(&task);
}
