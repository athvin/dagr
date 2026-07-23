// UI compile-failure fixture — ticket T11 (021),
// case `data_binding_non_default_rule`.
//
// PROVES (C3; arch.md §126, §52; Vocabulary): a node that carries a DATA
// dependency cannot be given any trigger rule other than `all-succeeded` — the
// builder TYPESTATE makes it INEXPRESSIBLE, a COMPILE error rather than a runtime
// check. `NodeBinding` starts in the `ConsumesNothing` state where
// `.trigger_rule(..)` IS offered; binding a data dependency transitions it to the
// `ConsumesData` state, which offers NO `trigger_rule` method. Calling it there
// is a "no method in this state" error (E0599).
//
// This is the REAL binding API (dagr_core::binding), not a throwaway sketch.
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::binding::test_support::source;
use dagr_core::binding::{NodeBinding, TriggerRule};
use dagr_core::task::Task;
use dagr_core::{RunContext, TaskError};

struct Gamma;
struct Rows;

struct MakeGamma;
impl Task for MakeGamma {
    type Input = ();
    type Output = Gamma;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Gamma, TaskError> {
        Ok(Gamma)
    }
}
struct ConsumeGamma;
impl Task for ConsumeGamma {
    type Input = Gamma;
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: Gamma) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

fn main() {
    let gamma = source("make-gamma", &MakeGamma);
    // After `.depends_on`, the builder is `NodeBinding<ConsumesData>`, a typestate
    // that offers no `trigger_rule` method — setting `AllTerminal` on a
    // data-dependent node is a compile error, not a runtime check.
    let _node = NodeBinding::consuming_nothing("consumer")
        .depends_on::<ConsumeGamma, _>(&ConsumeGamma, gamma)
        .trigger_rule(TriggerRule::AllTerminal);
}
