//! A **leaf binary** that declares three DAGs by hand-written `inventory::submit!`
//! of `DagRegistration` and dispatches them through `dagr_cli::run`.
//!
//! This is the discovery corpus for the `dagr_cli::run` auto-discovery entrypoint
//! (T79): the `#[dag]` macro does not exist yet (T80), so the submissions are
//! written directly against the collected record type. The submissions live in the
//! **binary/example crate**, per ADR 092's leaf-binary contract — `inventory`
//! reliably collects a submission only when it is compiled into the final linked
//! binary, so a discovery corpus must be the binary itself, never a dependency lib.
//!
//! The three records are submitted **out of alphabetical order** (`gamma`, `alpha`,
//! `beta`) on purpose: `inventory::iter` order is unspecified, so `run` must sort by
//! name and produce a deterministic `list` / dispatch regardless of submission order.

use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::DagRegistration;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A row-count payload with a declared stable name, so a graph edge carrying it
/// records that name.
#[derive(Clone)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// A report payload with a declared stable name. These examples assert on graph
/// structure / run outcome, not the produced value, so the wrapped total is unread.
#[derive(Clone)]
#[allow(dead_code)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// A two-node source: extracts some rows. Stable-named, so it is graph-emittable.
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

/// The sink node: consumes rows and produces a report.
struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "Load";
}
impl Task for Load {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, input: Rows) -> Result<Report, TaskError> {
        Ok(Report(input.0 * 2))
    }
}

/// A single-node source with a distinct stable name, so its graph shape differs.
struct Aggregate {
    seed: u64,
}
impl StableName for Aggregate {
    const STABLE_NAME: &'static str = "Aggregate";
}
impl Task for Aggregate {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Ok(Report(self.seed))
    }
}

/// Build the `alpha` DAG: a two-node `extract -> load` flow (graph-emittable).
fn build_alpha() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let rows = flow.register_source_named("extract", Extract { rows: 3 });
    let _report = flow.register_named::<Load, _>("load", Load, rows);
    flow
}

/// Build the `beta` DAG: a single-node `aggregate` flow.
fn build_beta() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _report = flow.register_source_named("aggregate", Aggregate { seed: 7 });
    flow
}

/// Build the `gamma` DAG: a single-node `aggregate` flow with a different seed.
fn build_gamma() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _report = flow.register_source_named("aggregate", Aggregate { seed: 11 });
    flow
}

// The submissions, deliberately out of alphabetical order to prove `run` sorts.
inventory::submit! { DagRegistration { name: "gamma", factory: build_gamma } }
inventory::submit! { DagRegistration { name: "alpha", factory: build_alpha } }
inventory::submit! { DagRegistration { name: "beta", factory: build_beta } }

fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
