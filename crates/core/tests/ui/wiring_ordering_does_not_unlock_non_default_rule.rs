// UI compile-failure fixture — ticket T50 (062),
// case `wiring_ordering_does_not_unlock_non_default_rule`.
//
// PROVES (C3/C4, Vocabulary; arch.md §52, §134): a *non-default* trigger rule
// (`all-terminal` / `any-failed`) is expressible ONLY on a node that consumes
// NOTHING — and adding an ORDERING edge to a DATA-consuming node does NOT unlock
// it. This is enforced at COMPILE time by the shape of the real authoring API
// (`dagr_core::flow::Flow`), not a runtime check.
//
// The only registrar that accepts a trigger rule alongside ordering edges is
// `register_source_ordered_after_with_trigger`, whose bound is `T: Task<Input =
// ()>` — a CONSUME-NOTHING task. A data-consuming task (`Input = Rows`, NOT `()`)
// cannot satisfy that bound, so trying to give it a non-default rule fails to
// compile with a trait-bound error (E0271: the associated `Input` type is `Rows`,
// not `()`). There is deliberately no `..._ordered_after_with_trigger` variant on
// the DATA registrars (`register` / `register_ordered_after`), so a data node has
// no path to a non-default rule at all — an ordering edge on it (via
// `register_ordered_after`) still leaves it locked to `all-succeeded` (C3).
//
// This is the ordering-edge-aware counterpart of the T5 typestate fixture
// `typed_handle_non_default_rule_on_data_node`, now shown against the real API.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::binding::TriggerRule;
use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::{NodePolicy, RunContext, TaskError};

struct Rows;

// A sourceless task producing `Rows` — used only to mint an ordering upstream.
struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

// A DATA-consuming task: its `Input` is `Rows`, NOT `()`. It can never be a
// consume-nothing node, so a non-default trigger rule must stay inexpressible on it.
struct Consume;
impl Task for Consume {
    type Input = Rows;
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<(), TaskError> {
        Ok(())
    }
}

fn main() {
    let mut flow = Flow::new();
    let up = flow.register_source("up", &MakeRows);

    // Attempt to give a DATA-consuming task (`Consume`, `Input = Rows`) a
    // non-default trigger rule via the ordering-edge-with-trigger registrar. That
    // registrar requires `T: Task<Input = ()>`; `Consume::Input` is `Rows`, not
    // `()`, so the bound is unsatisfied and this does not compile — an ordering
    // edge does NOT unlock a non-default rule on a data-consuming node.
    let _bad = flow.register_source_ordered_after_with_trigger(
        "bad",
        &Consume,
        &[up.ordering()],
        NodePolicy::new(),
        TriggerRule::AllTerminal,
    );
}
