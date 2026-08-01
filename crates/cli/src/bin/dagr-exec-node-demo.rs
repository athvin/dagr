//! The **`exec-node` reference pipeline binary** — the process the T106 suite
//! launches as a subprocess in place of a pod.
//!
//! It is one line of dispatch over a single-flow registry, exactly as a real
//! pipeline binary is: the point of the design is that the pod re-enters *the same
//! binary* and rebuilds the graph, the tasks, the resources, and the codec from the
//! same code, so nothing here is special-cased for remote execution. Every standard
//! verb — `run`, `graph`, `validate`, and now `exec-node` — comes from the registry
//! entrypoint for free.
//!
//! It ships behind `test-kit` + `blob` and is in no released artifact.

use dagr_cli::exec_node_demo::build_exec_node_demo_flow;
use dagr_cli::registry::{FlowRegistry, run_registry};

fn main() -> std::process::ExitCode {
    let registry = FlowRegistry::single_flow(build_exec_node_demo_flow);
    run_registry(&registry, std::env::args_os()).into()
}
