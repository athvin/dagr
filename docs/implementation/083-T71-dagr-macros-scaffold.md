# 083 · T71 — dagr-macros crate scaffold + zero/single-input `#[task]`

> **Milestone:** M5 · **Size:** M · **Type:** feature · **Components:** C1, C7
> **Branch:** `feat/t71-dagr-macros-scaffold` · **Depends on:** T9, T13 · **Blocks:** T72

## Why / context
This ticket lays the first slice of the ergonomic authoring layer that ADR 082 decided on: a new **optional** proc-macro crate, `crates/macros` (`dagr-macros`, `proc-macro = true`), whose *only* dependencies are the build-time `syn` / `quote` / `proc-macro2` and which links **zero runtime deps** into any shipped binary. It exports one attribute, `#[task]`, applied to an inherent `impl` block, that expands to the exact `impl Task for Foo { … }` an author writes by hand today (`crates/core/src/task.rs:182`, and the repeated scaffolding in `crates/cli/examples/quickstart.rs:46-65`). `dagr-core` re-exports the attribute behind a **default-on `macros` feature** (`#[cfg(feature = "macros")] pub use dagr_macros::task;` in `crates/core/src/lib.rs`), so `use dagr_core::task;` resolves it and `--no-default-features` turns it off — preserving ADR 081's zero-runtime-dependency guarantee for `dagr-core`, proved by a `cargo build --all --no-default-features` gate. This slice covers **only** zero-input (`Input = ()`) and single-input (`Input = T`, delivered *bare* by value per `crates/core/src/binding.rs`) tasks, `AwaitBound` execution class only, an optional `ctx: &RunContext` parameter, and enforcement that `run` returns `Result<T, TaskError>`. Multi-arity, execution-class arguments, and tuple `InputWiring` are all deferred to T72, which this ticket blocks.

## Objective
Deliver the macro crate scaffold and a `#[task]` that expands zero- and single-input tasks, additively, with no runtime dependency added to `dagr-core`.

- Create `crates/macros/Cargo.toml` (`[lib] proc-macro = true`; `syn` / `quote` / `proc-macro2` as build-time dependencies; **no** runtime dependencies; workspace lints applied) and `crates/macros/src/lib.rs` exporting the `#[proc_macro_attribute] pub fn task`.
- Add `crates/macros` as a member of the workspace `Cargo.toml`.
- `#[task]` applied to an inherent `impl` block generates an `impl Task` for the same type: it reads the user's `async fn run` signature and emits the trait impl deterministically.
- Infer `type Input`: **zero** dep args → `()`; **one** dep arg `x: T` → bare `T` (never `(T,)`; the arity-1 blanket `Deps` impl delivers the bare value, `binding.rs:476`). Arguments are taken **by value**.
- Infer `type Output = T` from the user's `-> Result<T, TaskError>` return type.
- Always emit the trait's `&mut self` receiver and the trait's `ctx` parameter; thread the user's `ctx: &RunContext` into the body **only when the user's `run` declares it**, and leave it unused (no `unused` warning) when absent.
- Emit `const EXECUTION_CLASS: ExecutionClass = ExecutionClass::AwaitBound;` (this slice sets `AwaitBound` unconditionally; the attribute takes no argument yet).
- Reject a `run` that does not return `Result<_, TaskError>` with a `compile_error!` naming the required shape.
- Add a **default-on `macros` feature** to `crates/core/Cargo.toml` (adding `dagr-macros` as an optional workspace path dependency) and, in `crates/core/src/lib.rs`, `#[cfg(feature = "macros")] pub use dagr_macros::task;`.
- Keep every hand-written `impl Task` (including the quickstart tasks) compiling unchanged — the macro is purely additive.

## Test plan (write these first — TDD)

**Crate boundary & feature wiring**
- Given the `dagr-macros` crate, when it is built, then it is a `proc-macro = true` crate whose only manifest dependencies are `syn` / `quote` / `proc-macro2` and it exposes no runtime symbols (nothing but the `#[proc_macro_attribute]`).
- Given `dagr-core`, when it is built with `--no-default-features`, then the `pub use dagr_macros::task` re-export is absent and core compiles with **no** dependency edge onto `dagr-macros` (`cargo build --all --no-default-features` passes).
- Given `dagr-core` built with default features, when a user writes `use dagr_core::task;`, then the `#[task]` attribute resolves and is applicable to an inherent `impl` block.

**Zero/single-input expansion**
- Given a struct with `async fn run(&mut self, _input: ()) -> Result<u64, TaskError>` annotated `#[task]`, when it is compiled, then the generated `impl Task` has `type Input = ()`, `type Output = u64`, `EXECUTION_CLASS = ExecutionClass::AwaitBound`, and its `run` invokes the user's body.
- Given `async fn run(&mut self, input: u64) -> Result<String, TaskError>` annotated `#[task]`, when it is compiled, then `type Input = u64` (bare, **not** `(u64,)`) and `type Output = String`.
- Given a `run` fn that omits the `ctx` parameter, when it is compiled, then the generated `run` still type-checks and produces no `unused` warning for the trait-supplied `ctx`.
- Given a `run` fn that declares `ctx: &RunContext`, when it is compiled, then `ctx` is threaded into the user's body and is usable there.

**Output enforcement**
- Given a `run` written with a bare `-> u64` (no `Result`), when it is compiled, then compilation fails with a clear `compile_error!` stating the return type must be `Result<T, TaskError>`.

**Compatibility**
- Given the existing hand-written quickstart tasks (`crates/cli/examples/quickstart.rs`), when the `macros` feature is enabled (default), then they still compile unchanged — the macro adds a new path and removes none.

## Definition of done
- [ ] `crates/macros/Cargo.toml` exists with `[lib] proc-macro = true`, `syn` / `quote` / `proc-macro2` as build-time dependencies, **zero runtime dependencies**, and `[lints] workspace = true`.
- [ ] `crates/macros/src/lib.rs` implements `#[task]` for zero-input (`Input = ()`) and single-input (bare `Input = T`) tasks, emits `EXECUTION_CLASS = AwaitBound`, threads an optional `ctx: &RunContext` only when declared, and rejects a non-`Result<_, TaskError>` return with a naming `compile_error!`.
- [ ] The workspace `Cargo.toml` lists `crates/macros` as a member.
- [ ] `crates/core` gains a default-on `macros` feature and a conditional `#[cfg(feature = "macros")] pub use dagr_macros::task;` in `crates/core/src/lib.rs`.
- [ ] Both `cargo build --all` and `cargo build --all --no-default-features` pass, the latter with no `dagr-macros` dependency in core's build graph.
- [ ] Doctests demonstrate zero-input and single-input `#[task]` usage that compiles and runs.
- [ ] Every existing hand-written `impl Task` (including the quickstart) compiles unchanged.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Multi-arity tasks (2..=8 dep args) and tuple `InputWiring` on `RunnableFlow` (`crates/cli/src/run_flow.rs`) — those are **T72**; this slice handles only zero and single input.
- `#[task(blocking)]` / `#[task(compute)]` execution-class arguments — **T72**; this slice emits `AwaitBound` unconditionally and the attribute takes no argument.
- The quickstart/cookbook rewrite to `#[task]` and the `trybuild` diagnostic corpus — **T73**; this ticket ships only the doctests its own TDD requires.
- Custom task error types — permanently out of scope for M5; `run` must return `Result<T, TaskError>`.
- Any change to engine behaviour, scheduling, or `dagr-core`'s runtime dependency graph — the macro is pure sugar over the existing hand-written `impl Task`, which remains the first-class, zero-dependency escape hatch.
