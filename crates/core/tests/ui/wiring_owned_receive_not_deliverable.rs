// UI compile-failure fixture — ticket T12 (024),
// case `wiring_owned_receive_not_deliverable`.
//
// PROVES (C3; arch.md `### C3 · Data dependency`, C1 receive-mode): the TYPE
// system alone rejects an OWNED receive of a value that is not movable into
// owned delivery. This is the REAL `dagr_core::task::Task`. A data input is
// delivered by OWNED move (the default receive mode — a bare `Handle<T>`), which
// requires the value to cross the framework's send boundary: `Task::run` returns
// a `Send` future, and an owned `!Send` input held across an await point makes
// that future `!Send`. A task declaring `type Input = Rc<Data>` and receiving it
// owned therefore cannot compile — an owned receive of an un-deliverable value
// is a compile error, naming `Rc<Data>` and the `Send` bound.
//
// SCOPE (explicit): this is ONLY the TYPE-LEVEL slice of the ownership model.
// The whole-graph multi-consumer ownership CONFLICT — the same value demanded
// `owned` by two consumers, or an owned edge into a retrying node without
// clone-on-read — is an ASSEMBLY error naming both consumers, NOT a compile
// error, and is asserted by T14 (assembly validation), never here. This fixture
// keeps only the type-system-decidable half of the model honest.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};
use std::rc::Rc;

struct Data;

// A consumer that RECEIVES an owned `Rc<Data>` — a `!Send + !Sync` value. Owned
// delivery moves the value into the consumer's `run` future; holding it across
// an await point makes that future `!Send`, violating the `run` future's `Send`
// bound. The owned receive of an un-deliverable value cannot be written.
struct ConsumeOwnedRc;
impl Task for ConsumeOwnedRc {
    type Input = Rc<Data>;
    type Output = ();
    async fn run(&mut self, _c: &RunContext, input: Rc<Data>) -> Result<(), TaskError> {
        // Hold the owned `Rc<Data>` across an await point: the future captures a
        // `!Send` value, so it cannot be the `Send` future `Task::run` demands.
        std::future::ready(()).await;
        drop(input);
        Ok(())
    }
}

fn main() {}
