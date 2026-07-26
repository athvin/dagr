# 086 · Flow registry ADR — one binary hosts many named flows

> **Date:** 2026-07-26 · **Status:** accepted (operator-approved framework feature) · **Type:** decision · **Components:** C7, C26
> **Branch:** `feat/flow-registry-adr` · **Relates to:** T13 (C7 flow), T55 (C26 CLI contract), ADR 081 (run-a-flow) · **Unblocks:** T74 (registry + run/list), T75 (graph/validate routing + example)

## Why / context

Today a dagr *pipeline binary* carries exactly one flow. The reference driver (`crates/cli/src/main.rs`) confirms it: the pipeline-bound verbs (`graph`, `validate`, `run`, `single-node`, `prune`) report "needs a pipeline-specific binary", and a real pipeline crate wires each verb to its single assembled pipeline. There is no built-in way for one binary to offer `dagr run etl` versus `dagr run nightly` and select between several flows by name.

An operator asked for exactly that — define **many** named flows and pick one per invocation, with each `dagr run <flow>` being its own independent run (its own run-id and store). This is *not* concurrent orchestration and *not* sub-DAG composition; it is name-based selection over the existing single-run engine. This ADR records a small, additive registry that provides it. (A binary that prefers one flow per executable already works and is untouched; the registry only adds the many-flows-per-binary option.)

## Decision

Add `dagr_cli::registry::FlowRegistry`, a builder mapping a flow **name → a re-invokable factory `Fn() -> RunnableFlow`**, plus a dispatch entrypoint `dagr_cli::run_registry(&registry, argv) -> ExitCode` that a pipeline binary calls instead of hand-dispatching verbs:

```rust
let registry = FlowRegistry::new()
    .add("etl", build_etl_flow)          // fn() -> RunnableFlow
    .add("nightly", build_nightly_flow);
std::process::exit(run_registry(&registry, std::env::args_os()).into());
```

`run_registry` serves `dagr list` (print the registered names), and routes `run` / `graph` / `validate` to the selected flow. An unknown name exits `InvalidUsage` (2) with a message listing the available flows; a registry with a single flow lets the name be omitted, while a multi-flow registry with no name given fails with the same helpful "name required (etl, nightly)" message.

### Why factories, not stored flows (the crux)

`RunnableFlow::run(self, …)` **consumes** the flow (`crates/cli/src/run_flow.rs`) and the type is not `Clone`; a single instance cannot serve two verbs. Meanwhile `graph_verb` and `validate_verb` need a live `&Pipeline` (`crates/cli/src/graph.rs`, `crates/cli/src/contract.rs`), which is only obtainable by assembling a flow. Storing a built `RunnableFlow` would therefore let a binary answer at most one verb. Storing a **factory closure** and calling it **once per verb** resolves this cleanly: `graph <flow>` builds a fresh flow, assembles it, and emits the artifact; `run <flow>` builds another and drives it. Factories are the only pattern consistent with `run(self)` consuming the flow.

### The CLI-contract touch (honest scope)

Carrying a flow name is *not* purely additive at the type level. Today `Cli { verb: Verb }` (`crates/cli/src/contract.rs`) and each subcommand collects trailing tokens into an undifferentiated `args` vector (`trailing_var_arg`); the first positional is never extracted. This ADR adds an **optional** `flow_name: Option<String>` to `Cli` and extracts the first positional in/around `parse_cli`. Behaviour stays backward-compatible — every existing verb, flag, and single-flow binary works unchanged — but `Cli` is a public C26 type, so the field addition is recorded here and noted in arch.md's C26 section as an operator-approved M5 addition.

### Per-verb exit-code fidelity

`run_registry` maps each verb's own outcome to its C26 code, not through a single path: `run` → `RunReport` via the existing `exit_code_for_run` (`contract.rs`); `graph` / `validate` → their own `ExitCode`/error types; the artifact-only `render` / `fold` are unchanged and need no flow; `single-node` / `prune` remain pipeline-bound and route to the selected flow. The reference `main.rs` is updated to delegate to `run_registry` so it stops being a misleading verb-only dispatcher.

## Consequences

- **Additive and opt-in.** One-flow-per-binary still works; the registry is the many-flows option. No engine or run semantics change.
- **Each run stays independent.** Selection picks *which* flow to build; the driver still executes one run with its own identity and store — matching the operator's "each invocation its own thing".
- **`Cli` gains a public field** — a small, documented C26 surface change (arch.md note).
- **A clear authoring pattern.** `examples/multi_flow.rs` (T75) shows a two-flow binary a pipeline author can copy.

## Rejected alternatives

- **Storing built `RunnableFlow` instances in the registry.** Impossible: `run(self)` consumes the flow and it is not `Clone`, so an instance serves one verb only. Rejected in favour of `Fn() -> RunnableFlow` factories called once per verb.
- **Concurrent in-process orchestration of many flows (shared pools).** A far larger change to the one-run-per-`drive` model (cross-flow permit governance, merged event streams, cross-flow cancellation) and explicitly not what was asked. Rejected as out of scope.
- **Making `parse_cli` a full flag parser to own the positional.** The C26 design keeps verb parsing library-owned and parameter parsing pipeline-specific; the minimal `flow_name` extraction preserves that boundary. Rejected the larger contract rewrite.
- **Sub-DAG composition (a flow reused as a node).** A distinct feature orthogonal to selection; can layer on later without this ADR. Deferred.
