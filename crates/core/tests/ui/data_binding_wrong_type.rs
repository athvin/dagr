// UI compile-failure fixture — ticket T11 (021), case `data_binding_wrong_type`.
//
// PROVES (C3; arch.md §124): binding a handle whose VALUE TYPE does not exactly
// match the consuming task's declared input is a COMPILE error whose message
// names BOTH the expected and the supplied type. This is the REAL binding API
// (dagr_core::binding), not a throwaway sketch: the sealed positional `Deps`
// trait's `Inputs = T::Input` bound is unsatisfied when a `Handle<Beta>` is
// bound to a task declaring `type Input = Alpha`.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28) — asserting only that
// both type names appear, never prose quality (C3 message-quality clause).

use dagr_core::binding::test_support::{register, source};
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct Alpha;
struct Beta;
struct Rows;

struct MakeBeta;
impl Task for MakeBeta {
    type Input = ();
    type Output = Beta;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Beta, TaskError> {
        Ok(Beta)
    }
}

// Declares it consumes an `Alpha`.
struct ConsumeAlpha;
impl Task for ConsumeAlpha {
    type Input = Alpha;
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: Alpha) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

fn main() {
    let beta = source("make-beta", &MakeBeta);
    // Bind a `Handle<Beta>` where `Handle<Alpha>` is required: the `Deps<Inputs =
    // Alpha>` bound is unsatisfied — a wrong-type binding is a compile error
    // naming BOTH `Alpha` (expected) and `Beta` (supplied).
    let _bad = register("rows", &ConsumeAlpha, beta);
}
