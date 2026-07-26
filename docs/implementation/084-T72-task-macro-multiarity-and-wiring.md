# 084 · T72 — `#[task]` multi-arity, ctx, ExecutionClass + tuple InputWiring

> **Milestone:** M5 · **Size:** M · **Type:** feature · **Components:** C1, C7, C10
> **Branch:** `feat/t72-task-macro-multiarity` · **Depends on:** T71 · **Blocks:** T73

## Why / context
T71 scaffolds `dagr-macros` and lands `#[task]` for the shapes an author most often writes first: a zero-input source and a single by-value input. This ticket completes the macro per ADR 082 so **multi-input** tasks are both *authorable* and *runnable*, which ADR 082 §"The wiring prerequisite" flags as two halves that must land together. Half one is codegen: parse 2..=8 by-value dep args into `type Input = (A, B, …)`, emit `let (a, b, …) = input;` at the top of the generated `run` body (the author writes one non-destructured `input: (A, B)` parameter, per `binding.rs:476` where dagr delivers the input *by value* and arity-1 stays the bare `T`), and set the impl-level `EXECUTION_CLASS` const from the attribute argument (`#[task]`/`#[task()]` → `AwaitBound`, `#[task(blocking)]` → `Blocking`, `#[task(compute)]` → `Compute`; never inferred from the body). Half two is the engine prerequisite: the `InputWiring` seam introduced by ADR 081 in `crates/cli/src/run_flow.rs` is today single-arity only — its blanket `impl<D: Deps> InputWiring for D` asserts `edges.len() == 1` and a multi-input `register`/`register_with` call panics at wiring time. This ticket extends `InputWiring` with per-arity tuple readers for 2..=8 that mirror the existing `SingleReader`: downcast each upstream output slot, honour that edge's `ReceiveMode`, read lazily *inside* `run()` (never at plan-assembly, where slots are still empty — `run_flow.rs:475`), and assemble the `Deps::into_edges` positional order (position 0..N, `binding.rs:547-553`) into the declared tuple. Without half two, a multi-input `#[task]` node compiles but cannot run through `RunnableFlow`.

## Objective
Finish `#[task]` for the full input-arity range and make multi-input nodes actually run through `RunnableFlow`.

- Parse 2..=8 by-value dep args from the author's `run` signature into `type Input = (A, B, …)`; the author writes one non-destructured tuple parameter (`input: (A, B)`), and the macro emits `let (a, b, …) = input;` as the first statement of the generated `Task::run` body so the author's body sees the individual bindings.
- Preserve the T71 arity rules unchanged: zero dep args → `type Input = ()`; **one dep arg `x: T` → bare `T`** (never `(T,)`; the arity-1 blanket `Deps` delivers the bare value, `binding.rs:476`).
- Support the attribute-argument execution class: `#[task]` and `#[task()]` → `const EXECUTION_CLASS = ExecutionClass::AwaitBound`; `#[task(blocking)]` → `Blocking`; `#[task(compute)]` → `Compute`. Emit it as the impl-level associated const; do not infer it from the body.
- Keep T71's `ctx: &RunContext` detection (by type) and its `Result<T, TaskError>` return-type handling untouched; a bare `-> T` still yields the T71 `compile_error!`.
- Extend the `InputWiring` seam in `crates/cli/src/run_flow.rs` with tuple readers for arities 2..=8 mirroring `SingleReader`/the single-arity blanket impl: for each upstream `(id, mode)` pair from `Deps::into_edges`, downcast that node's output slot (`downcast_slot`), read through the declared `ReceiveMode` (via `EdgeRead::read`), and assemble the positional tuple; the read stays deferred to attempt time. Replace or supersede the single-arity `edges.len() == 1` assertion so a 2..=8-input node wires instead of panicking, and keep the arity-1 bare-`T` path intact.
- Document the arity ceiling: binding more than 8 inputs is already a `Deps` `on_unimplemented` compile error (`binding.rs`, `MAX_INPUT_ARITY = 8`); state in the macro's docs whether a 9-arg `run` is rejected at the macro level or falls through to that existing deps cliff.

## Test plan (write these first — TDD)

**Multi-arity expansion**
- Given `async fn run(&mut self, input: (u64, String)) -> Result<bool, TaskError>` under `#[task]`, when the impl compiles, then the generated `impl Task` has `type Input = (u64, String)`, `type Output = bool`, and the body begins by destructuring the tuple so the author's statements observe the two named bindings.
- Given a task whose `run` takes an 8-element tuple input (the `MAX_INPUT_ARITY` ceiling), when it compiles, then `type Input` is the 8-tuple and the generated body destructures all eight positions in order.
- Given a task whose `run` takes a 9-element tuple input, when it compiles, then a helpful compile error appears — either a macro-level `compile_error!` naming the ceiling or the existing `Deps` arity cliff surfaced at the registration site — and the ticket documents which of the two it is.

**Execution class**
- Given `#[task(blocking)]`, when the impl compiles, then `const EXECUTION_CLASS` equals `ExecutionClass::Blocking`.
- Given `#[task(compute)]`, when the impl compiles, then `const EXECUTION_CLASS` equals `ExecutionClass::Compute`.
- Given `#[task]` or `#[task()]` (no argument), when the impl compiles, then `const EXECUTION_CLASS` equals `ExecutionClass::AwaitBound`, and the class is taken from the attribute rather than any property of the body.

**Runtime wiring**
- Given a two-input node registered on a `RunnableFlow` with two upstream handles bound in declared order, when the flow runs to completion, then the node receives both upstream values in that exact order and produces the expected output — proving the tuple `InputWiring` reader assembles `Deps::into_edges` positionally.
- Given a two-input node one of whose upstreams is bound via `handle.shared()`, when the flow runs, then that edge's shared receive mode is honoured by the reader (the mode comes from the registration-site binding, not from the macro), while the other edge reads under its own declared mode.
- Given a multi-input flow, when it runs, then the node's inputs are read *inside* `run()` (after the driver admits the node and its upstreams have succeeded), not at plan-assembly, so no read-before-fill occurs.

**No regression**
- Given the T71 zero-input source and single-input map cases, when re-run through the extended macro and the extended `InputWiring`, then they still expand identically (`type Input = ()` / bare `T`) and still run end-to-end through `RunnableFlow`.

## Definition of done
- [ ] `#[task(blocking|compute)]` is parsed and emits the correct impl-level `EXECUTION_CLASS` const (`Blocking`/`Compute`), and bare `#[task]`/`#[task()]` emits `AwaitBound`; the class is never inferred from the body.
- [ ] The macro handles input arities 2..=8: it maps the by-value dep args to `type Input = (A, B, …)` and emits `let (a, b, …) = input;` as the first statement of the generated `run`, from the author's single non-destructured tuple parameter.
- [ ] The T71 rules are preserved unchanged: zero args → `()`, one arg → bare `T` (never `(T,)`), `ctx: &RunContext` detection by type, and the `Result<T, TaskError>` return-type requirement with its bare-`-> T` compile error.
- [ ] `InputWiring` impls are added for tuple arities 2..=8 in `crates/cli/src/run_flow.rs` (mirroring `SingleReader` and the single-arity blanket impl: per-edge downcast, `ReceiveMode`-honouring read via `EdgeRead::read`, deferred to attempt time, positional assembly), and the prior single-arity `edges.len() == 1` assertion no longer rejects a valid multi-input node.
- [ ] A multi-input flow (including at least one `handle.shared()` edge) runs end-to-end through `RunnableFlow`, delivering upstream values in declared order under their declared receive modes.
- [ ] An input arity greater than 8 yields a documented compile error (macro-level or the existing `MAX_INPUT_ARITY`/`Deps` `on_unimplemented` cliff), and the macro docs state which.
- [ ] Existing single-input and zero-input tests (T71's and the run-a-flow suite's) still pass unchanged.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The quickstart rewrite onto `#[task]` and the `trybuild` diagnostics corpus — those are T73; this ticket ships only the unit/expansion and run-a-flow behaviour tests its own TDD requires.
- Any durability, trigger-rule, or other opt-in marker on the attribute — ADR 082 leaves room for these but T72 adds none; the attribute grammar this ticket parses is exactly `blocking`/`compute`/empty.
- Any change to `ReceiveMode` semantics or to where receive mode is declared — it stays at the registration site (`binding.rs`), never in the macro or the task body; the tuple readers only *honour* the mode the edge already carries.
- Custom task error types — the return type remains `Result<T, TaskError>` (permanent M5 scope boundary per ADR 082).
