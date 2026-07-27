# 093 · T78 — `FlowBuilder` declaration façade over `RunnableFlow`

> **Milestone:** M6 · **Size:** S · **Type:** feature · **Components:** C7
> **Branch:** `feat/t78-flowbuilder-facade` · **Depends on:** T74 · **Blocks:** T80

## Why / context

ADR 092 adds a `#[dag]` attribute over a flow-builder fn whose body stays real Rust, so wrong-type/arity wiring remains a **compile error**. The macro (T80) needs a small, curated surface to hand the author — one that offers *declaration* verbs (`source`, `node`) but does **not** leak the *execution* seam (`run`, `into_pipeline`, which consume the flow) into a DAG body. This ticket delivers that surface as a standalone library type, before any macro or `inventory` plumbing exists, so it can be built and tested on its own.

Today a flow is declared directly against `RunnableFlow` (`crates/cli/src/run_flow.rs`): `register_source_named` / `register_named` are the **graph-emittable** registrars (they carry `StableName` bounds so `dagr graph <flow>` / `validate <flow>` record author-declared stable names — see `crates/cli/examples/multi_flow.rs:106-135`), and `register_source` / `register` are the type-erased ones (a flow built with them is not graph-emittable). `FlowBuilder` is a thin newtype over `&mut RunnableFlow` that names the graph-emittable pair `source` / `node` (the right default for a framework where `graph`/`validate` are expected to work) and offers `source_erased` / `node_erased` as escape hatches. Each method forwards to the underlying registrar and returns the **real** `Handle<T>`, so the existing `Deps<Inputs = T::Input>` bound keeps mis-wiring a compile error.

Making `FlowBuilder` a newtype (not a type alias for `RunnableFlow`) is deliberate: an alias would expose the full `RunnableFlow` surface — including `run`/`into_pipeline` — inside a DAG body, letting a declaration consume itself. The newtype exposes exactly four methods and nothing else.

## Objective

Add a minimal, graph-emittable-by-default declaration façade over `RunnableFlow`, with real `Handle<T>` returns, and no new dependencies.

- Add `pub struct FlowBuilder<'a>(&'a mut RunnableFlow)` (and a private constructor the factory in T80 will use) to `crates/cli` — a re-exported public type at the `dagr_cli` crate root and in a new `dagr_cli::prelude`.
- `FlowBuilder::source<T>(&mut self, name, task) -> Handle<T::Output>` forwards to `RunnableFlow::register_source_named` (bounds: `T: Task<Input = ()> + StableName`, `T::Output: StableName`).
- `FlowBuilder::node<T, D>(&mut self, name, task, deps) -> Handle<T::Output>` forwards to `RunnableFlow::register_named` (bounds: `T: Task + StableName`, `T::Input: StableInputNames + Clone + Send`, `T::Output: StableName`, `D: Deps<Inputs = T::Input> + InputWiring + Clone`).
- `FlowBuilder::source_erased<T>` / `node_erased<T, D>` forward to the type-erased `register_source` / `register`; documented as **not** graph-emittable (same wording as `multi_flow.rs`).
- Do **not** expose `run`, `into_pipeline`, `register_with`, or any consuming method on `FlowBuilder`.
- No `inventory`, no `#[dag]` macro, no `run` entrypoint in this ticket — those are T79/T80.

## Test plan (write these first — TDD)

**Surface & forwarding**
- Given a `FlowBuilder` wrapping a fresh `RunnableFlow`, when `source` and `node` are called to build a two-node `extract -> load` DAG, then assembling the resulting flow yields a pipeline with the same two nodes and one data edge as the equivalent `register_source_named` / `register_named` calls (structure-snapshot parity, via the T61 structure-snapshot kit).
- Given a DAG built through `FlowBuilder::source` / `node`, when `into_pipeline` is called on the underlying flow and the graph artifact is emitted, then it is **graph-emittable** and records the author-declared stable names (not `AssemblyFailure`).

**Compile-time safety preserved**
- Given `node` fed a `Handle<T>` of the wrong type for the task's declared `Input`, when compiled, then it is a compile error (the `Deps<Inputs = T::Input>` bound), proven by a `trybuild` compile-fail case.
- Given a task **without** `StableName` passed to `source`, when compiled, then it fails to compile; and given the same task passed to `source_erased`, then it compiles and yields a flow documented as not graph-emittable.

**Encapsulation**
- Given a `FlowBuilder`, when a caller attempts `f.run(...)` or `f.into_pipeline()`, then it does not compile — the façade exposes no consuming/execution method.

## Definition of done

- [ ] `pub struct FlowBuilder<'a>` exists in `crates/cli`, re-exported at the `dagr_cli` root and from a new `dagr_cli::prelude` module.
- [ ] `source` / `node` forward to `register_source_named` / `register_named` with the correct `StableName` / `StableInputNames` bounds and return the true `Handle<T>`.
- [ ] `source_erased` / `node_erased` forward to the type-erased registrars and are documented as not graph-emittable.
- [ ] No consuming/execution method (`run`, `into_pipeline`, `register_with`) is reachable through `FlowBuilder`.
- [ ] A structure-snapshot test proves a `FlowBuilder`-built DAG is identical to the hand-written `register_*_named` form; a `trybuild` case proves wrong-type wiring is a compile error.
- [ ] No new runtime dependency is introduced (this ticket is pure library surface; `inventory` arrives in T79).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The `inventory` dependency, `DagRegistration` type, `collect!`, and the `dagr_cli::run` entrypoint — those are **T79**.
- The `#[dag]` attribute macro that constructs a `FlowBuilder` and submits a factory — that is **T80**.
- The many-dags example, cookbook rewrite, and `#[dag]` trybuild corpus — those are **T81**.
- Any `register_with`-style per-node `NodePolicy` (retries/durability) surface on `FlowBuilder` — deferred; a DAG needing custom policy uses the underlying `RunnableFlow` directly for now.
- Any change to `RunnableFlow`, the engine, or `dagr-core` — this ticket only adds a wrapper type.
