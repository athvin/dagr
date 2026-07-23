// UI compile-failure fixture — ticket T11 (021),
// case `data_binding_arity_ceiling`.
//
// PROVES (C3; arch.md §121, §128; T5 ADR §4): binding MORE than the documented
// maximum arity (8) yields the single CURATED `#[diagnostic::on_unimplemented]`
// message that names the ceiling and directs the author to aggregate the
// upstream values into a struct produced by an intermediate node — NOT a wall of
// raw trait errors. There is no `Deps` impl beyond arity 8, so a 9-tuple of
// handles hits the on_unimplemented diagnostic on the `Deps` bound.
//
// This is the REAL binding API (dagr_core::binding). Wired to the T8 UI harness
// (crates/core/tests/ui.rs); the sibling `.stderr` names the substrings the
// curated message must contain (the ceiling number and the aggregate-into-a-
// struct remedy), and the harness asserts this sample FAILS to compile under the
// pinned toolchain (C28).

use dagr_core::binding::test_support::{register, source};
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct V;
struct Bytes;

struct MakeV;
impl Task for MakeV {
    type Input = ();
    type Output = V;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<V, TaskError> {
        Ok(V)
    }
}

// A task whose declared input is a 9-tuple — beyond the arity ceiling of 8. No
// `Deps` impl exists for a 9-tuple of handles, so binding one hits the curated
// on_unimplemented diagnostic rather than a trait-error cascade.
struct ConsumeNine;
impl Task for ConsumeNine {
    type Input = (V, V, V, V, V, V, V, V, V);
    type Output = Bytes;
    async fn run(
        &mut self,
        _c: &RunContext,
        _i: (V, V, V, V, V, V, V, V, V),
    ) -> Result<Bytes, TaskError> {
        Ok(Bytes)
    }
}

fn main() {
    let v: dagr_core::handle::Handle<V> = source("make-v", &MakeV);
    // Bind NINE handles — past the ceiling of 8. `Deps` is implemented only for
    // arity 1..=8, so this fires the curated on_unimplemented message.
    let _bad = register(
        "bytes",
        &ConsumeNine,
        (v, v, v, v, v, v, v, v, v),
    );
}
