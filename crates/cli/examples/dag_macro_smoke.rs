//! A **leaf binary** that declares its DAGs with the `#[dag]` attribute (T80) and
//! dispatches them through `dagr_cli::run` — the whole authoring story end-to-end:
//! `#[task]`-declared tasks, `#[dag]`-declared flows, a one-line `main`.
//!
//! It is the expansion-and-discovery corpus for the `#[dag]` macro: the macro keeps
//! each user fn, generates a `fn() -> RunnableFlow` factory, and emits an
//! `inventory::submit!` of a `DagRegistration`, so `dagr_cli::run` discovers every
//! DAG by name with no registry edit. Per ADR 092's leaf-binary contract, the
//! `#[dag]`s (and the submissions they emit) live in the **binary/example crate**,
//! never a dependency lib, so `inventory` reliably collects them.
//!
//! It exercises three properties the T80 test plan pins:
//! - `#[dag] fn etl(..)` is discovered under the fn name `etl` (default name).
//! - `#[dag(name = "nightly")] fn foo(..)` is discovered as `nightly`, not `foo`.
//! - Two `#[dag]`s in the **same module** (`etl`, `foo`) both expand without an
//!   item-name collision and both are discovered.

use dagr_cli::prelude::*;

/// A row-count payload with a declared stable name, so the graph-emittable `source`
/// registrar can record it. The examples assert on discovery, not the value, so the
/// wrapped count is unread.
#[derive(Clone)]
#[allow(dead_code)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// A report payload with a declared stable name.
#[derive(Clone)]
#[allow(dead_code)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// The `etl` DAG's source node: extracts some rows.
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

/// The `etl` DAG's sink node: loads the rows into a report.
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

/// The `nightly` DAG's single source node.
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

/// `#[dag]` with no argument: the DAG name defaults to the fn name (`etl`). The
/// macro keeps this fn, generates a factory that drives it through a `FlowBuilder`,
/// and submits the registration for discovery.
#[dag]
fn etl(f: &mut FlowBuilder) {
    let rows = f.source("extract", Extract { rows: 5 });
    // Wrong wiring here would be a COMPILE error — the façade returns the real
    // `Handle<T>` and the `Deps` bound is exact-typed. The sink handle is unused
    // (nothing consumes it), so bind it to `_` to satisfy `#[must_use]`.
    let _report = f.node("load", Load, rows);
}

/// `#[dag(name = "nightly")]` on a fn named `foo`: the registered DAG name is
/// `nightly`, not `foo`. This lives in the **same module** as `etl`, so both must
/// expand without an item-level name collision.
#[dag(name = "nightly")]
fn foo(f: &mut FlowBuilder) {
    let _report = f.source("aggregate", Aggregate { seed: 42 });
}

fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
