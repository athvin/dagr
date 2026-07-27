//! Compile-pass: the **headline composition** the derive exists for — a `#[task]`
//! task and its `#[derive(StableName)]` payloads flow through the graph-emittable
//! `FlowBuilder::source` / `node` registrars (which require `StableName` on every
//! task and payload) with **no hand-written trait bodies**. Before the derive, this
//! required an `impl StableName` per type; now it is one derive line each.

use dagr_cli::prelude::*;

/// A payload: `#[derive(StableName)]` names it, `Clone` lets it wire as a node input.
#[derive(Clone, StableName)]
struct Rows(u64);

/// Another payload.
#[derive(Clone, StableName)]
struct Report(u64);

/// A source task — `#[task]` on the impl, `#[derive(StableName)]` on the struct.
#[derive(StableName)]
struct Extract {
    rows: u64,
}

#[task]
impl Extract {
    async fn run(&mut self, _input: ()) -> Result<Rows, TaskError> {
        Ok(Rows(self.rows))
    }
}

/// A data-dependent task consuming `Rows`, producing `Report`.
#[derive(StableName)]
struct Load;

#[task]
impl Load {
    async fn run(&mut self, rows: Rows) -> Result<Report, TaskError> {
        Ok(Report(rows.0 * 2))
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    {
        let mut f = FlowBuilder::new(&mut flow);
        // Graph-emittable registrars: they compile only because every task and
        // payload carries `StableName` — supplied entirely by the derive.
        let rows = f.source("extract", Extract { rows: 3 });
        let _report = f.node("load", Load, rows);
    }
    let _ = flow;
}
