# One binary, many named flows — the flow registry

> **The ergonomic path is `#[dag]` auto-discovery** — write `#[dag]` fns and let
> `dagr_cli::run` discover them (see the [cookbook](cookbook.md#declaring-dags-with-dag-and-running-them-with-one-line)
> and [`crates/cli/examples/many_dags.rs`](../crates/cli/examples/many_dags.rs)). This
> guide covers the explicit **`FlowRegistry`** fallback you hand-wire when you want the
> registry spelled out in `main` (a computed flow set, a non-`#[dag]` factory, or no
> `inventory` dependency). Both produce the same `list` / `graph` / `validate` / `run`.

> **Status:** documentation deliverable, authored by ticket **T75** (ticket 088,
> [`docs/implementation/088-T75-registry-graph-validate-and-example.md`](implementation/088-T75-registry-graph-validate-and-example.md)),
> over the [ADR 086](implementation/086-flow-registry-adr.md) decision and the T74
> registry core. The whole pattern is backed by **compiled, run code** — the
> example [`crates/cli/examples/multi_flow.rs`](../crates/cli/examples/multi_flow.rs)
> and the tests in
> [`crates/cli/tests/registry_graph_validate.rs`](../crates/cli/tests/registry_graph_validate.rs) —
> so nothing here claims a behaviour the shipped API does not have.

A dagr pipeline binary usually carries exactly **one** flow: you write your tasks
and a flow, hand it to the one-call
[`RunnableFlow`](../crates/cli/src/run_flow.rs) seam (see the
[README quickstart](../README.md#quickstart)), and the binary runs that flow. This
guide covers the *other* option (arch.md **C26** "Many flows per binary"; ADR 086):
hosting **many named flows** under one binary and selecting one per invocation by
name — `dagr run etl` versus `dagr run analytics`, `dagr graph etl`, `dagr list`.

It is **name-based selection over the existing single-run engine** — each
`dagr run <flow>` is still its own independent run with its own run identity and
store. It is *not* concurrent orchestration of many flows and *not* sub-DAG
composition (both are permanent non-goals).

## The whole thing

The entire many-flows binary is a [`FlowRegistry`](../crates/cli/src/registry.rs)
of named factories plus a single
[`run_registry`](../crates/cli/src/registry.rs) dispatch — no hand-written verb
dispatch:

```rust
use dagr_cli::registry::{run_registry, FlowRegistry};
use dagr_cli::run_flow::RunnableFlow;

fn build_etl() -> RunnableFlow { /* register tasks + wire the flow */ todo!() }
fn build_analytics() -> RunnableFlow { todo!() }

fn main() -> std::process::ExitCode {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("analytics", build_analytics);
    std::process::ExitCode::from(run_registry(&registry, std::env::args_os()).as_u8())
}
```

The library owns every verb, so all of these work against **either** flow with no
extra code:

```text
cargo run --example multi_flow -- list                    # etl, analytics
cargo run --example multi_flow -- graph etl               # etl's C20 graph artifact (stdout)
cargo run --example multi_flow -- graph analytics         # analytics' graph
cargo run --example multi_flow -- validate etl            # assembly only, prints every problem
cargo run --example multi_flow -- run etl --store ./runs  # drive etl (its own run + store)
cargo run --example multi_flow -- run analytics --store ./runs
```

Copy [`crates/cli/examples/multi_flow.rs`](../crates/cli/examples/multi_flow.rs)
whole; it is the compiled reference for this pattern.

## The builder API

[`FlowRegistry`](../crates/cli/src/registry.rs) maps a flow **name → a
re-invokable factory** `Fn() -> RunnableFlow`:

- `FlowRegistry::new()` — an empty registry.
- `.add(name, factory)` — register a named factory; returns the registry for
  chaining. `factory` is any `Fn() -> RunnableFlow` (a plain `fn` or a closure).
- `FlowRegistry::single_flow(factory)` — the **one-flow ergonomic** constructor,
  whose single flow's name may be **omitted** on the command line (see below).
- `run_registry(&registry, argv) -> ExitCode` — dispatch a command line over the
  registry and return the C26 exit code. Convert with `.as_u8()` /
  `std::process::ExitCode::from(...)`.

### Why factories, not stored flows

[`RunnableFlow::run`](../crates/cli/src/run_flow.rs) (and `into_pipeline`, which the
inspection verbs use) **consume** the flow, and `RunnableFlow` is not `Clone`, so
one instance can answer at most one verb. The registry therefore stores a factory
and calls it **once per invocation** — a fresh flow each time. So `graph etl` builds
one flow to emit the artifact and a later `run etl` builds another to drive; two
`run`s are two independent runs with their own identities and stores.

## Registering graph-emittable flows

`graph <flow>` emits the **C20 graph artifact**, which records each node's
author-declared **stable names**. So a flow you intend to inspect with `graph`
registers its nodes through the *stable-name-aware* surface — the tasks and payload
types implement [`StableName`](../crates/core/src/stable_name.rs), and you register
with
[`register_source_named`](../crates/cli/src/run_flow.rs) /
[`register_named`](../crates/cli/src/run_flow.rs):

```rust
use dagr_cli::run_flow::RunnableFlow;
# use dagr_core::context::RunContext;
# use dagr_core::stable_name::StableName;
# use dagr_core::task::Task;
# use dagr_core::TaskError;
# #[derive(Clone)] struct Rows(u64);
# impl StableName for Rows { const STABLE_NAME: &'static str = "Rows"; }
# struct Extract; impl StableName for Extract { const STABLE_NAME: &'static str = "Extract"; }
# impl Task for Extract { type Input = (); type Output = Rows;
#   async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> { Ok(Rows(1)) } }
# struct Load; impl StableName for Load { const STABLE_NAME: &'static str = "Load"; }
# impl Task for Load { type Input = Rows; type Output = ();
#   async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<(), TaskError> { Ok(()) } }
fn build_etl() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let rows = flow.register_source_named("extract", Extract);
    let _out = flow.register_named::<Load, _>("load", Load, rows);
    flow
}
```

The type-erased [`register`](../crates/cli/src/run_flow.rs) /
[`register_source`](../crates/cli/src/run_flow.rs) still **run** fine, but a flow
built with them carries no stable names and is **not** graph-emittable — `graph`
reports the offending node instead. `validate` (assembly only) works either way.

## The contract extension: an optional `flow_name` positional

Carrying a flow name adds **one optional positional** to the C26 command surface
(ADR 086, recorded in arch.md's C26 section). The parsed
[`Cli`](../crates/cli/src/contract.rs) gains a `flow_name: Option<String>` — the
first *positional* token after the verb (e.g. `etl` in `dagr run etl --store DIR`),
or `None` when absent. It is **backward-compatible**:

- Every existing verb, flag, and single-flow binary parses unchanged.
- A leading `--flag` is never mistaken for a flow name (a value-taking flag such as
  `--store DIR` consumes its own value).
- Library-owned flags live in a reserved namespace, so a pipeline parameter can
  never collide with one.

## Single-flow ergonomics

`FlowRegistry::single_flow(factory)` builds a registry whose one flow's name may be
**omitted**. On such a registry the operator types no name:

```text
dagr graph          # emits the sole flow's graph
dagr validate       # assembles the sole flow
dagr run --store D   # drives the sole flow
```

A **multi-flow** registry, by contrast, requires a name and refuses a missing or
unknown one:

- no name on a multi-flow registry → `InvalidUsage` (exit `2`) with a
  `name required (etl, analytics)` message listing the available flows;
- an unknown name → `InvalidUsage` (exit `2`) with a message listing the available
  flows.

Selection fails **before** the flow is ever built, so a bad name never reaches your
task code.

## Exit codes are per-verb

Each verb maps its **own** outcome to its C26 exit code
([the exit-code table](../crates/cli/src/contract.rs)):

| Verb | Outcome → exit code |
|---|---|
| `list` | always `Success` (0) |
| `run <flow>` | the run's `RunReport` → its C26 code (success / run-failure / cancelled / …) |
| `graph <flow>` | `Success` (0) on a clean emit; `AssemblyFailure` (3) if the flow cannot be emitted (a node without stable names) |
| `validate <flow>` | `Success` (0) on a clean assembly; `AssemblyFailure` (3) otherwise, printing **every** problem |
| bad/absent/unknown flow name | `InvalidUsage` (2), before the flow is built |

`graph` and `validate` return their own codes **directly** — they do not go through
the completed-run exit-code path.

## What the registry does not route

`single-node` (replay from a prior run, C27) and `prune` (run-store retention)
*select* a flow but need per-invocation store / parameter / rehydration plumbing the
registry entrypoint does not own; `run_registry` recognizes them, applies the same
selection rules, and points at the pipeline-specific verb wiring (the reference
sample binary `dagr-t56-alpha` wires those directly). The artifact-only `render` /
`fold` carry no flow and are dispatched by the binary directly, not through the
registry.
