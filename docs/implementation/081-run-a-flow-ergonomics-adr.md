# 081 · Run-a-Flow ergonomics ADR

> **Date:** 2026-07-24 · **Status:** accepted (operator-approved framework feature) · **Type:** decision + implementation · **Components:** C1, C7, C10, C14
> **Branch:** `feat/run-a-flow-ergonomics` · **Relates to:** T24 (driver), T13 (flow), T20/T22 (attempt/retry), T63 (scratch/resume) · **Unblocks:** T64 (quickstart), T65 (M1 acceptance gate)

## Why / context

arch.md **C1** states the load-bearing authoring contract: *a task author writes tasks + a flow and **never** writes scheduling or permit plumbing.* Yet until now every milestone demo (m1/m2/m3/t56/t63) hand-wrote ~150 lines of type-erased `NodeRunner` impls per pipeline to actually *run* a flow — a `Pin<Box<dyn Future<Output = TerminalState> + Send>>` per node that reads its input `SlotRef`s, calls `run_attempt`/`run_with_retries_caught`, and fills its output `Slot`. That is exactly the scheduling plumbing C1 says the author must not write, and it blocked the T64 quickstart (a task author cannot be shown "register tasks, run the flow" if running the flow means hand-writing runners) and T65.

The operator approved adding a **public run-a-Flow convenience** so a user runs a `Flow` of real `Task`s in **one call**, with **no** hand-written `NodeRunner`. This ADR records that decision and its implementation.

## Decision

Add a purely **additive** public seam, `dagr_cli::run_flow`, comprising:

- **`RunnableFlow`** — wraps a `dagr_core::flow::Flow` and, alongside each registration, captures a **type-erased runner factory** at registration time (where the concrete `Task` + `Input`/`Output` types are known). Registration mirrors `Flow`: `register_source` (consume-nothing node), `register`/`register_with` (data-dependent node, optional `NodePolicy`). Each returns the node's typed `Handle`.
- **`RunnableFlow::run(pipeline_name, &RunConfig, sink, clock) -> Result<RunReport, AssemblyError>`** — assembles the pipeline, mints each node's typed output `Slot` with its assembly-precomputed consumer count, builds every node's generic `NodeRunner` (wiring producer→consumer slots exactly as the demos did by hand), assembles a `RunPlan`, and drives it through the **real** `dagr_cli::driver::drive` loop.
- **`run_flow::RunReport`** — wraps the driver's `RunReport` and additionally retains the run's output slots, so a caller reads a node's produced value by its `Handle` (`report.output(handle) -> Option<T>`), plus `outcome()`, `terminal_state(node)`, and `run_id()`.

### The generic adapter (the crux)

A pipeline's nodes have heterogeneous input/output types, so the driver's `NodeRunner` is type-erased and the run loop cannot be generic over one `T`. The knowledge needed to build a node's runner — its concrete `Task` type and the concrete types of its upstreams' output slots — exists **only at the registration call site**. So `RunnableFlow` captures, per registration, a boxed `FnOnce(&SlotRegistry) -> Box<dyn NodeRunner>` that:

1. downcasts each upstream slot (stored in a run-wide **slot registry keyed by `NodeId`**) back to its concrete `Arc<Slot<Upstream>>` — infallible by construction, because the slot was stored under that id as exactly that type, and the consumer's captured upstream type is proven equal to the producer's output by the `Deps<Inputs = T::Input>` bound the `Flow` registration enforces;
2. reads the wired input value **lazily, inside the runner's `run()`** (never at plan-assembly, when the upstream slots are still empty), honouring the declared `ReceiveMode` (clone-on-read clones per attempt; shared/owned read the shared value);
3. builds one generic `GenericNodeRunner<T>` that drives the node through the **same** real attempt path the hand-wired demos use: a single caught attempt (`run_attempt_caught`) on the driver's own per-attempt `RunContext` when the node does not retry, or the real bounded-retry loop (`run_with_retries_caught`) when its policy grants retries.

The adapter is **genuinely generic over `Task` `Input`/`Output`**, not a per-type shim: `GenericNodeRunner<T>`, `BoundTask<T>` (a consume-nothing adapter binding the read input for the attempt runner), and the `InputWiring`/`InputReader` seam are each written once, generic over the value type. Input arity is handled through `InputWiring`, a blanket impl over `Deps` reading the framework's own `Deps::into_edges` — the current registration surface exercises single-input nodes (the M1 chain shape); multi-input tuple arities extend the same seam with one small per-arity reader, no per-value-type code.

### `dagr run` / demo path

`dagr-run-a-flow-demo` (a checked-in reference driver) runs the M1 chain (`source → transform → sink`, the middle node flaky with one retry) through `RunnableFlow::run` — **no** hand-written `NodeRunner`, no manual slot wiring, no `RunPlan` assembly — writing a real on-disk C19 `events.jsonl`. It is the executable demonstration that a task author runs a real flow without scheduler plumbing.

## Consequences

- **The existing path is untouched.** The public `NodeRunner` / `RunPlan::new` surface, the milestone demos, and the `full_pipeline` fake harness all still hand-write runners and compile — the new path is additive.
- **T63 preserved.** The generic runners are ordinary `NodeRunner`s driven by the real `drive` loop, so the T63 `scratch_root`/`temp_dir`/`cancellation` wiring and the resume seam apply to them unchanged: a single-attempt node reaches its real per-node durable scratch namespace through the driver's per-attempt context (proven by the scratch test).
- **`dagr-core` stays dependency-free.** The adapter lives entirely in `dagr-cli` (which already depends on both crates); no `dagr-core` dependency was added and no schema was edited.
- **Fidelity.** The auto-adapter reproduces, per node, the exact ordered event-stream transition shape and per-node terminal states the hand-wired M1 demo produces — including `transform`'s two attempt cycles — verified by `fidelity_auto_adapter_matches_hand_wired`.

## Rejected alternatives

- **Reading inputs eagerly in the factory (at plan-assembly).** Rejected: the upstream slots are empty until the driver admits the consumer, so an eager read is a read-before-fill defect. Inputs are read lazily inside `run()`.
- **Keying the slot registry by node name.** Rejected: an edge yields a `NodeId`, and `NodeId` has no reverse mapping to a name (identity is a one-way FNV hash — C2). Keying the registry by `NodeId` lets an edge resolve its upstream slot directly with no impossible id→name lookup.
- **A per-type runner shim.** Rejected outright by the operator constraint: the adapter must generalize over `Task` `Input`/`Output`, which `GenericNodeRunner<T>` + `InputWiring` do.
