//! **One binary, many named flows** — the copyable authoring pattern for the
//! `FlowRegistry` + `run_registry` seam.
//!
//! A single dagr pipeline binary usually carries exactly one flow. This example
//! shows the *many-flows* option: it registers **two** independent flows — `etl`
//! and `analytics` — under one binary and lets the operator select one per
//! invocation by name. It is the direct counterpart to
//! [`quickstart.rs`](../examples/quickstart.rs) (which runs a single flow in one
//! call): here the whole `main` is *just* a [`FlowRegistry`] of named factories and
//! a single [`run_registry`] dispatch — no hand-written verb dispatch, no per-verb
//! plumbing.
//!
//! The library owns every verb, so all of these work against **either** flow with
//! no extra code:
//!
//! ```text
//! cargo run --example multi_flow -- list                       # etl, analytics
//! cargo run --example multi_flow -- graph etl                  # etl's graph artifact
//! cargo run --example multi_flow -- graph analytics            # analytics' graph
//! cargo run --example multi_flow -- validate etl               # assembly only, every problem
//! cargo run --example multi_flow -- run etl --store ./runs     # drive etl (its own run + store)
//! cargo run --example multi_flow -- run analytics --store ./runs
//! ```
//!
//! ## Why factories, not stored flows
//!
//! [`RunnableFlow::run`](dagr_cli::run_flow::RunnableFlow::run) / `into_pipeline`
//! **consume** the flow, and [`RunnableFlow`] is not `Clone`, so one instance can
//! answer at most one verb. The registry therefore stores a *re-invokable factory*
//! `fn() -> RunnableFlow` and calls it **once per invocation**: `graph etl` builds a
//! fresh flow to emit the artifact, and a later `run etl` builds another to drive —
//! each `run` being its own independent run with its own run identity and store.
//!
//! ## Graph-emittable registration
//!
//! `graph <flow>` emits the graph artifact, which records each node's
//! author-declared **stable names**. So each flow registers its nodes through the
//! stable-name-aware
//! [`register_source_named`](dagr_cli::run_flow::RunnableFlow::register_source_named)
//! / [`register_named`](dagr_cli::run_flow::RunnableFlow::register_named) surface
//! (the tasks and payloads implement [`StableName`]). The type-erased `register` /
//! `register_source` still run fine, but a flow built with them is not
//! graph-emittable.

use std::process::ExitCode;

use dagr_cli::registry::{FlowRegistry, run_registry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::TaskError;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;

// --- Flow 1: `etl` — a two-node `extract -> load` pipeline. --------------------

/// The rows an extract produced — a payload whose declared [`StableName`] is what
/// the graph artifact records for the edge that carries it.
#[derive(Clone)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// A loaded report. The wrapped total is the terminal value each flow produces;
/// this example inspects only the *structure* (via `graph`/`validate`) and the run
/// outcome (via `run`), not the produced value, so the field is not read back here
/// — a real pipeline would read it off the run report (see `quickstart.rs`).
#[derive(Clone)]
#[allow(dead_code)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// The `etl` source: reads some rows (here, a fixed count).
struct Extract {
    rows: u64,
}
impl StableName for Extract {
    const STABLE_NAME: &'static str = "Extract";
}
impl Task for Extract {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _ctx: &RunContext, _input: ()) -> Result<Rows, TaskError> {
        Ok(Rows(self.rows))
    }
}

/// The `etl` sink: loads the rows into a report.
struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "Load";
}
impl Task for Load {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _ctx: &RunContext, input: Rows) -> Result<Report, TaskError> {
        Ok(Report(input.0 * 2))
    }
}

/// Build the `etl` flow (a **fresh** one each call — the registry re-invokes this
/// factory once per invocation). Registers through the stable-name-aware surface so
/// `graph etl` can emit its graph artifact.
fn build_etl() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let rows = flow.register_source_named("extract", Extract { rows: 100 });
    let _report = flow.register_named::<Load, _>("load", Load, rows);
    flow
}

// --- Flow 2: `analytics` — a distinct single-node pipeline. --------------------

/// The `analytics` source: a single aggregation node, a different graph shape.
struct Aggregate {
    seed: u64,
}
impl StableName for Aggregate {
    const STABLE_NAME: &'static str = "Aggregate";
}
impl Task for Aggregate {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _ctx: &RunContext, _input: ()) -> Result<Report, TaskError> {
        Ok(Report(self.seed))
    }
}

/// Build the `analytics` flow (a fresh one each call).
fn build_analytics() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _report = flow.register_source_named("aggregate", Aggregate { seed: 42 });
    flow
}

// --- The whole binary: a registry of named factories + one dispatch. -----------

fn main() -> ExitCode {
    // Register both flows by name. `add` takes any `fn() -> RunnableFlow`, so the
    // binary hosts many flows and selects one per invocation. `run_registry` owns
    // every verb — `list`, `graph <flow>`, `validate <flow>`, `run <flow>` — so
    // there is no hand-written verb dispatch here.
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("analytics", build_analytics);
    ExitCode::from(run_registry(&registry, std::env::args_os()).as_u8())
}
