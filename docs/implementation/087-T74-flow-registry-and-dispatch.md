# 087 · T74 — FlowRegistry + Cli.flow_name + run_registry (run/list)

> **Milestone:** M5 · **Size:** M · **Type:** feature · **Components:** C7, C26
> **Branch:** `feat/t74-flow-registry` · **Depends on:** T13, T24, T55 · **Blocks:** T75

## Why / context
Today every dagr pipeline binary carries exactly **one** flow: the reference driver (`crates/cli/src/main.rs`) reports "needs a pipeline-specific binary" for the pipeline-bound verbs, and a real pipeline crate wires each verb to its single assembled pipeline. There is no built-in way for one binary to offer `dagr run etl` versus `dagr run nightly` and select a flow by name. ADR 086 records the fix: a small, additive `dagr_cli::registry::FlowRegistry` mapping a flow **name → a re-invokable factory `Fn() -> RunnableFlow`**, plus a dispatch entrypoint `run_registry(&registry, argv) -> ExitCode` a binary calls instead of hand-dispatching. Factories (not stored flows) are load-bearing: `RunnableFlow::run(self, …)` **consumes** the flow (`crates/cli/src/run_flow.rs`) and `RunnableFlow` is not `Clone`, so one instance can serve at most one verb; storing a `Fn() -> RunnableFlow` and calling it once per invocation lets each `run <flow>` build a fresh flow with its own run identity and store — matching the operator's "each invocation its own thing". This slice ships the registry type, the C26 contract extension (an optional `flow_name` on `Cli` extracted in/around `parse_cli`), and the two verbs `run_registry` needs to route here: `list` and `run <flow>`, each mapped to its C26 exit code through the existing `exit_code_for_run` (`crates/cli/src/contract.rs`). It is the first half of ADR 086; T75 adds the remaining verb routing and the example binary.

## Objective
Provide the many-flows-per-binary registry and the `run`/`list` dispatch over it, purely additively over the C26 surface.

- Add `dagr_cli::registry::FlowRegistry` — a builder mapping a flow name to a re-invokable factory `Fn() -> RunnableFlow` (stored boxed, because `RunnableFlow::run(self)` consumes the flow and it is not `Clone`):
  - `FlowRegistry::new()` — an empty registry.
  - `add(name, factory)` — register a named factory; returns the registry for chaining (`FlowRegistry::new().add("etl", build_etl).add("nightly", build_nightly)`).
  - `single_flow(factory)` — the one-flow ergonomic constructor (its name may be omitted at the command line).
- Extend the C26 contract additively (`crates/cli/src/contract.rs`): add `flow_name: Option<String>` to `Cli`, and extract the first positional token in/around `parse_cli`. This is additive — every existing verb, flag, and single-flow binary parses unchanged; the new field is `None` when no positional is present. The subcommand already collects trailing tokens into an undifferentiated `args` vector (`trailing_var_arg`); the first of those becomes the flow name, the rest stay as `args` for the pipeline binary.
- Implement `dagr_cli::run_registry(&registry, argv) -> ExitCode`:
  - `list` — print the registered flow names (in a deterministic order) and exit `ExitCode::Success`.
  - `run <flow>` — resolve the name to its factory, call the factory to build a fresh `RunnableFlow`, drive it, and map the resulting `RunReport` to its exit code via `exit_code_for_run`.
  - A single-flow registry lets the name be **omitted** on `run` — the one flow is dispatched.
  - A multi-flow registry with **no** name on `run` → `ExitCode::InvalidUsage` (2) with a "name required (etl, nightly)" message listing the available flows.
  - An **unknown** name → `ExitCode::InvalidUsage` (2) with a message listing the available flows.
- Update `crates/cli/src/main.rs` to delegate to `run_registry` (over a single-flow registry) so the reference driver stops being a misleading verb-only dispatcher.
- Record the `Cli.flow_name` addition as an operator-approved M5 C26 surface change in arch.md's C26 section (per ADR 086).

## Test plan (write these first — TDD)

**Registry & selection**
- Given a registry with flows `etl` and `nightly`, when argv `run etl --store DIR` is parsed, then the resulting `Cli` carries `verb = Verb::Run` and `flow_name = Some("etl")`, and `run_registry` builds the `etl` flow via its factory and drives it.
- Given a single-flow registry (`FlowRegistry::single_flow(f)`), when `run` is invoked with no name, then the one flow is built and dispatched (no name required).
- Given a multi-flow registry, when `run` is invoked with no name, then `run_registry` returns `ExitCode::InvalidUsage` and its message lists the registered names.
- Given `run unknown` on a multi-flow registry with flows `etl` and `nightly`, when `run_registry` runs, then it returns `ExitCode::InvalidUsage` and the message lists the available flows (`etl`, `nightly`).

**list**
- Given a registry with flows `a` and `b`, when `list` runs, then `run_registry` prints both `a` and `b` and exits `ExitCode::Success`.

**Independence & factories**
- Given the same flow run twice through `run_registry`, when each invocation completes, then each produced its own run identity and its own store (no shared state between them), demonstrating the factory is re-invoked once per invocation rather than a single flow being reused.

**Backward compatibility**
- Given an existing single-flow binary and every existing verb/flag, when each is parsed through `parse_cli`, then behaviour is unchanged and `Cli.flow_name` is `None` whenever no positional flow name is supplied — the new positional is optional and non-breaking.

## Definition of done
- [ ] `FlowRegistry::new`, `add(name, factory)`, and `single_flow(factory)` exist; factories are stored as `Fn() -> RunnableFlow` (boxed) and re-invoked once per invocation, because `RunnableFlow::run(self)` consumes the flow and the type is not `Clone`.
- [ ] `Cli` gains an optional `flow_name: Option<String>`, and `parse_cli` extracts the first positional token additively — every existing verb/flag/single-flow binary parses unchanged and `flow_name` is `None` when no positional is present.
- [ ] `run_registry` routes `list` (print names, exit `Success`) and `run <flow>` (build the selected flow via its factory, drive it, map the `RunReport` through `exit_code_for_run`) with the correct C26 exit codes.
- [ ] An unknown flow name → `ExitCode::InvalidUsage` (2) with a message listing the available flows; a multi-flow registry with no name on `run` → the same code with a "name required (…)" message listing the names.
- [ ] A single-flow registry serves `run` with the name omitted (the ergonomic default for the common one-flow binary).
- [ ] `crates/cli/src/main.rs` delegates to `run_registry` instead of hand-dispatching verbs.
- [ ] The `Cli.flow_name` addition is documented as an operator-approved M5 C26 surface change in arch.md's C26 section (per ADR 086).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Routing `graph` / `validate` / `single-node` / `prune` to the selected flow, the `examples/multi_flow.rs` two-flow example binary, and the usage guide — those are **T75**; this ticket ships only the registry type, the `flow_name` contract extension, and the `list` / `run <flow>` routing.
- Concurrent in-process orchestration of many flows (shared pools, merged event streams, cross-flow cancellation) — a far larger change to the one-run-per-`drive` model, explicitly not what was asked (ADR 086 rejected alternative). Permanent scope boundary.
- Sub-DAG composition (a flow reused as a node) — a distinct feature orthogonal to name-based selection, deferred by ADR 086.
