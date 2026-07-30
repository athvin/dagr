//! A **leaf binary** declaring exactly **one** DAG by hand-written
//! `inventory::submit!` and dispatching it through `dagr_cli::run`.
//!
//! Test discovery corpus (drives `tests/dag_auto_discovery.rs`), not a tutorial —
//! see `examples/README.md`. It pins the single-DAG ergonomic: with exactly one
//! discovered DAG, `dagr_cli::run` registers it under its **own declared name**
//! (`only`), so `list` prints `only` and its stream lives under `<base>/only/…`,
//! while the name stays **omittable** on the command line (`run` / `graph` /
//! `validate` with no name dispatch the sole DAG, since it is the only flow).

use dagr_cli::DagRegistration;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::TaskError;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;

/// A row-count payload with a declared stable name. This example asserts on the run
/// outcome / store, not the produced value, so the wrapped count is unread.
#[derive(Clone)]
#[allow(dead_code)]
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
    dagr_cli::run(std::env::args_os()).into()
}
