// UI compile-failure fixture — ticket T11 (021),
// case `data_binding_wrong_arity_too_many`.
//
// PROVES (C3; arch.md §125): binding MORE handles than the task declares is a
// COMPILE error. A task declaring `type Input = (Alpha, Beta)` (exactly two
// inputs) is bound a THREE-tuple; the sealed `Deps` impl for a 3-tuple has
// `Inputs = (Alpha, Beta, Gamma)`, which does not equal the task's `(Alpha,
// Beta)`, so the `Inputs = T::Input` bound is unsatisfied. This is the REAL
// binding API (dagr_core::binding), distinct from the too-few case and from the
// arity-CEILING case (which fires the curated on_unimplemented message instead).
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::binding::test_support::{register, source};
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct Alpha;
struct Beta;
struct Gamma;
struct Bytes;

struct MakeAlpha;
impl Task for MakeAlpha {
    type Input = ();
    type Output = Alpha;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Alpha, TaskError> {
        Ok(Alpha)
    }
}
struct MakeBeta;
impl Task for MakeBeta {
    type Input = ();
    type Output = Beta;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Beta, TaskError> {
        Ok(Beta)
    }
}
struct MakeGamma;
impl Task for MakeGamma {
    type Input = ();
    type Output = Gamma;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Gamma, TaskError> {
        Ok(Gamma)
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
    let beta = source("make-beta", &MakeBeta);
    let gamma = source("make-gamma", &MakeGamma);
    // Bind THREE handles where TWO are declared: the 3-tuple's `Inputs` is
    // `(Alpha, Beta, Gamma)`, not the declared `(Alpha, Beta)` — a compile error.
    let _bad = register("bytes", &ConsumeTwo, (alpha, beta, gamma));
}
