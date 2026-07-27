//! The **README quickstart**, compiled and run verbatim in CI.
//!
//! The anchored region below (`// ANCHOR: quickstart` … `// ANCHOR_END: quickstart`)
//! is byte-identical to the README's single fenced `rust` block, enforced by
//! [`crates/cli/tests/readme_quickstart.rs`](../../tests/readme_quickstart.rs) — so
//! the code a reader copies is exactly what the build compiles and CI runs.
//!
//! It is the whole authoring story in ~25 lines: two `#[task]` tasks, a
//! `#[derive(StableName)]` payload, one `#[dag]` fn grouping them, and a one-line
//! `main`. `dagr_cli::run` supplies the run store, event sink, clock, run id, and the
//! standard verbs (`run` / `graph` / `validate` / `list`) — there is nothing else to
//! write. Run it with `cargo run --example quickstart -- run --store <dir>`.

// ANCHOR: quickstart
use dagr_cli::prelude::*;

// The one payload the two tasks pass along. `#[derive(StableName)]` gives it the
// author-declared name the graph artifact records — one line, no trait body.
#[derive(Clone, StableName)]
struct Number(u64);

// --- Author two tasks. A task is a value holding its configuration plus one
// `async fn run` body — that is the whole authoring surface. `#[task]` reads the
// `run` signature and generates the four things you declare (input type, output type,
// execution class, and the work), so you write business logic only — no scheduling,
// retry, or permit code. `#[derive(StableName)]` names the task struct so the DAG can
// graph it.

/// The source: consumes nothing (an `()` input) and produces a starting number.
#[derive(StableName)]
struct Count {
    up_to: u64,
}

#[task]
impl Count {
    async fn run(&mut self, _input: ()) -> Result<Number, TaskError> {
        Ok(Number(self.up_to))
    }
}

/// The sink: consumes the source's number and produces its double.
#[derive(StableName)]
struct Double;

#[task]
impl Double {
    async fn run(&mut self, input: Number) -> Result<Number, TaskError> {
        Ok(Number(input.0 * 2))
    }
}

// --- Declare the DAG. `#[dag]` groups the tasks into a named flow. `f.source` is a
// root (no upstream); `f.task(..).depends_on(..)` makes a node downstream of another —
// this reads "double depends on count", and the handle threads the edge. A wrong-typed
// binding is a *compile* error, not a runtime surprise. This is the whole graph — two
// nodes, one edge — and the flow's name defaults to the fn name (`quickstart`).
#[dag]
fn quickstart(f: &mut FlowBuilder) {
    let count = f.source("count", Count { up_to: 21 });
    let _double = f.task("double", Double).depends_on(count);
}

// --- The whole binary is one line. `dagr_cli::run` discovers every `#[dag]`, builds
// the run store, event sink, clock, and run id for you, and dispatches the verbs every
// pipeline binary shares: `run` drives the flow, `graph` emits the DAG, `list` names
// the flows. No server, no database, no scheduler — the binary and a run-store
// directory are all you need.
fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os()).into()
}
// ANCHOR_END: quickstart
