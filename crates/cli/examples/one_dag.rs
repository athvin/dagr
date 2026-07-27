//! A **leaf binary** declaring exactly **one** DAG by hand-written
//! `inventory::submit!` and dispatching it through `dagr_cli::run`.
//!
//! This is the single-flow ergonomic corpus for T79: with exactly one discovered
//! DAG, `run` builds the registry through `FlowRegistry::single_flow`, so the DAG's
//! name may be **omitted** on the command line (`run` / `graph` / `validate` with no
//! name dispatch the sole DAG) — parity with the one-flow ergonomic default the
//! existing registry already offers.

use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::DagRegistration;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A row-count payload with a declared stable name.
#[derive(Clone)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// The sole DAG's single source node.
struct Extract {
    rows: u64,
}
impl StableName for Extract {
    const STABLE_NAME: &'static str = "Extract";
}
impl Task for Extract {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows(self.rows))
    }
}

/// Build the sole DAG: a single graph-emittable source node.
fn build_only() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _rows = flow.register_source_named("extract", Extract { rows: 5 });
    flow
}

inventory::submit! { DagRegistration { name: "only", factory: build_only } }

fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os())
}
