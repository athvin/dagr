//! **One binary, many `#[dag]`-declared flows** — the copyable authoring pattern for
//! the M6 declarative-DAG layer (ADR 092).
//!
//! This is the `#[dag]` counterpart to [`multi_flow.rs`](../examples/multi_flow.rs)
//! (which hand-wires the same shape into a [`FlowRegistry`]): here each DAG is
//! declared with the [`#[dag]`](dagr_cli::dag) attribute over the [`FlowBuilder`]
//! façade, auto-registered for discovery, and the **whole `main` is one line** —
//! [`dagr_cli::run`] finds every declared DAG, builds the registry, and delegates to
//! the existing `run_registry`. Adding a DAG here needs **no registry edit**: write
//! another `#[dag]` fn.
//!
//! The library owns every verb, so all of these work against **any** discovered DAG
//! with no extra code:
//!
//! ```text
//! cargo run --example many_dags -- list                       # alpha, beta, gamma (sorted)
//! cargo run --example many_dags -- graph alpha                # alpha's graph artifact (stdout)
//! cargo run --example many_dags -- graph beta                 # beta's graph
//! cargo run --example many_dags -- validate alpha             # assembly only, every problem
//! cargo run --example many_dags -- run alpha --store ./runs   # drive alpha (its own run + store)
//! cargo run --example many_dags -- run beta --store ./runs
//! ```
//!
//! # The leaf-binary contract (ADR 092)
//!
//! Discovery is `inventory`-backed: `#[dag]` emits an `inventory::submit!` and
//! [`dagr_cli::run`] iterates `inventory::iter` at link time. `inventory` registers
//! life-before-`main` constructors in a linker section, so a submission is **reliably
//! collected only when it is compiled into the final linked binary** — the `#[dag]`s
//! therefore live in this **binary/example crate** (across as many modules as you
//! like), never a dependency library. Cross-crate DAG libraries are out of scope for
//! this milestone. An app crate hosting `#[dag]`s depends on `inventory = "0.3"` (the
//! `$crate`-based `submit!` offers no path override) — this example inherits it
//! through `dagr-cli`'s default-on `dag` feature.
//!
//! # Graph-emittable declaration
//!
//! `graph <dag>` / `validate <dag>` record each node's author-declared **stable
//! names**, so each DAG declares its nodes through the graph-emittable
//! [`FlowBuilder::source`] / [`FlowBuilder::node`] pair (the tasks and payloads
//! implement [`StableName`](dagr_core::stable_name::StableName)). The type-erased
//! [`FlowBuilder::source_erased`] / [`FlowBuilder::node_erased`] still run fine, but a
//! DAG built with them is not graph-emittable.

use dagr_cli::prelude::*;

/// The rows an extract produced — a payload whose declared
/// [`StableName`](dagr_core::stable_name::StableName) is what the graph artifact
/// records for the edge that carries it.
#[derive(Clone)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// A loaded report. These DAGs are inspected by structure (`graph`/`validate`) and by
/// run outcome (`run`), not by the produced value, so the wrapped total is unread.
#[derive(Clone)]
#[allow(dead_code)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// A source node: extracts some rows. Stable-named, so it is graph-emittable.
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

/// A sink node: consumes rows and produces a report.
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

/// A single-node aggregation source with a distinct stable name, so its graph shape
/// differs from `alpha`'s two-node shape.
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

/// The `alpha` DAG — a two-node `extract -> load` flow. `#[dag]` with no argument
/// registers it under the **fn name** (`alpha`). Nodes are declared through the
/// graph-emittable `source` / `node` pair, so `graph alpha` emits its artifact.
///
/// Wrong wiring here would be a **compile** error — `FlowBuilder::node` returns the
/// real `Handle<T>` and the `Deps` bound is exact-typed. The sink handle is unused
/// (nothing consumes it), so bind it to `_` to satisfy `#[must_use]`.
#[dag]
fn alpha(f: &mut FlowBuilder) {
    let rows = f.source("extract", Extract { rows: 3 });
    let _report = f.node("load", Load, rows);
}

/// The `beta` DAG — a single-node aggregation, a different graph shape. `#[dag]`
/// defaults the name to the fn name (`beta`).
#[dag]
fn beta(f: &mut FlowBuilder) {
    let _report = f.source("aggregate", Aggregate { seed: 7 });
}

/// The `gamma` DAG — another single-node aggregation with a different seed, declared
/// in the **same module** as `alpha`/`beta` (all three expand without an item-name
/// collision and all three are discovered). `#[dag]` defaults the name to `gamma`.
#[dag]
fn gamma(f: &mut FlowBuilder) {
    let _report = f.source("aggregate", Aggregate { seed: 11 });
}

/// The whole binary: one line. `dagr_cli::run` discovers `alpha` / `beta` / `gamma`
/// (sorted, deduped by name), builds the registry, and delegates to `run_registry`.
/// A caller who prefers the short `dagr::run` spelling writes `use dagr_cli as dagr;`
/// (there is no facade crate — ADR 092).
fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
