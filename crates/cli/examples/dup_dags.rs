//! A **leaf binary** declaring **two DAGs with the same name** by hand-written
//! `inventory::submit!`, to prove `dagr_cli::run` rejects a duplicate name.
//!
//! Per T79 / ADR 092: `FlowRegistry::add` does not reject duplicate names, and
//! `inventory::iter` order is unspecified, so `run` must fail fast with
//! `ExitCode::InvalidUsage` and a diagnostic **naming the duplicated DAG**, *before*
//! any flow is built. This corpus submits two records both named `dup` so any `run`
//! invocation over it takes the duplicate-rejection path.

use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::DagRegistration;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A payload with a declared stable name. The duplicate is rejected before any flow
/// is built, so this value is never produced or read.
#[derive(Clone)]
#[allow(dead_code)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// The DAGs' single source node.
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

/// A factory that would build a DAG if the duplicate were ever built (it is not:
/// duplicate rejection happens before any flow is built).
fn build_first() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _rows = flow.register_source_named("extract", Extract { rows: 1 });
    flow
}

/// A second factory under the same name — the duplicate.
fn build_second() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _rows = flow.register_source_named("extract", Extract { rows: 2 });
    flow
}

inventory::submit! { DagRegistration { name: "dup", factory: build_first } }
inventory::submit! { DagRegistration { name: "dup", factory: build_second } }

fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
