// UI compile-failure fixture — ticket T11 (021),
// case `data_binding_wrong_arity_too_few`.
//
// PROVES (C3; arch.md §125): binding FEWER handles than the task declares is a
// COMPILE error. A task declaring `type Input = (Alpha, Beta)` (exactly two
// inputs) is bound ONE handle; the sealed `Deps` trait's `Inputs = T::Input`
// bound (`<Handle<Alpha> as Deps>::Inputs == (Alpha, Beta)`) is unsatisfied.
// This is the REAL binding API (dagr_core::binding), not a throwaway sketch.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::binding::test_support::{register, source};
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct Alpha;
struct Beta;
struct Bytes;

struct MakeAlpha;
impl Task for MakeAlpha {
    type Input = ();
    type Output = Alpha;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Alpha, TaskError> {
        Ok(Alpha)
    }
}

// Declares it consumes EXACTLY TWO inputs.
struct ConsumeTwo;
impl Task for ConsumeTwo {
    type Input = (Alpha, Beta);
    type Output = Bytes;
    async fn run(&mut self, _c: &RunContext, _i: (Alpha, Beta)) -> Result<Bytes, TaskError> {
        Ok(Bytes)
    }
}

fn main() {
    let alpha = source("make-alpha", &MakeAlpha);
    // Bind ONE handle where TWO are declared: `<Handle<Alpha> as Deps>::Inputs`
    // is `Alpha`, not `(Alpha, Beta)` — a wrong-arity binding is a compile error.
    let _bad = register("bytes", &ConsumeTwo, alpha);
}
