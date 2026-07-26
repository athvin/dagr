# 082 · Task-authoring macro ADR — `#[task]`

> **Date:** 2026-07-26 · **Status:** accepted (operator-approved framework feature) · **Type:** decision · **Components:** C1, C7, C10
> **Branch:** `feat/task-macro-adr` · **Relates to:** T9 (C1 task), T13 (C7 flow), T5 (typed handle/deps encoding), ADR 081 (run-a-flow) · **Unblocks:** T71 (macro scaffold), T72 (multi-arity + wiring), T73 (quickstart + trybuild)

## Why / context

arch.md **C1** says a task author declares *exactly four things* — the consumed `Input`, the produced `Output`, the `EXECUTION_CLASS`, and the `run` work — and writes *no* scheduling, retry, or permit code. dagr already delivers that: `impl Task for Foo { type Input; type Output; async fn run(&mut self, ctx, input) }` (`crates/core/src/task.rs:182`), plus a fully typed `Flow`/`RunnableFlow` builder with `Handle<T>` and compile-time binding (`Deps<Inputs = T::Input>`). What it does *not* have is the ergonomic top layer the `dagx` crate popularised: an attribute that lets an author write only the `run` fn and have the `Task` impl generated. Today every task repeats the `type Input` / `type Output` / `&mut self, ctx, input` scaffolding by hand (see `crates/cli/examples/quickstart.rs:46-65`).

An operator comparison against `dagx` asked for the same one-fn authoring ergonomics **without** compromising dagr's zero-*runtime*-dependency guarantee for `dagr-core` (the property ADR 081 explicitly preserved) and without changing any engine behaviour. This ADR records the decision to add that layer as a purely additive, opt-in macro.

## Decision

Add a new **optional** proc-macro crate, `crates/macros` (`dagr-macros`, `proc-macro = true`), whose only dependencies are the build-time `syn` / `quote` / `proc-macro2`. It exports one attribute, `#[task]`, applied to an inherent `impl` block:

```rust
#[task]
impl Double {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> { Ok(input * 2) }
}
```

which expands to the exact `impl Task for Double { … }` an author writes today. `dagr-core` re-exports the attribute behind a **default-on `macros` feature** (`#[cfg(feature = "macros")] pub use dagr_macros::task;`), so `use dagr_core::task;` resolves the macro; `--no-default-features` turns it off. Hand-written `impl Task` remains a first-class, zero-dependency path and the sole escape hatch for anything the macro cannot express.

### The generated impl (the crux)

The macro reads the user's `async fn run` signature and emits the trait impl deterministically (rules verified against `task.rs` and `crates/core/src/binding.rs`):

- **Receiver.** `run` may be written stateless, `&self`, or `&mut self`; the generated `Task::run` always takes `&mut self` (the exclusive receiver C1 mandates for safe sequential retries) and calls through.
- **Inputs → `type Input`.** Zero dep args → `()`; **one dep arg `x: T` → bare `T`** (never `(T,)`; the arity-1 blanket `Deps` impl delivers the bare value — `binding.rs:476,524`); 2..=8 dep args → the tuple `(A, B, …)`. Arguments are taken **by value** — dagr delivers an input into `run` by value ("the bare value `T`, never a reference", `binding.rs:476`), so the macro does *not* adopt `dagx`'s `&T` convention. Authors write one non-destructured tuple parameter (`input: (A, B)`) and the macro emits `let (a, b) = input;`.
- **`RunContext`.** An optional `ctx: &RunContext` parameter is detected by type; the generated `run` (which always carries the trait's `ctx`) threads it into the body when present and ignores it when absent.
- **Output.** `run` must return `Result<T, TaskError>` (a task must be able to fail with a classified error). `type Output = T`; a bare `-> T` is rejected with a `compile_error!` naming the fix. Custom error types are out of scope for M5.
- **Execution class.** Set by the attribute, emitted as the impl-level associated const — `#[task]` → `AwaitBound`, `#[task(blocking)]` → `Blocking`, `#[task(compute)]` → `Compute`. It is *not* inferred from the body.
- **`async fn` → `impl Future + Send`.** The body is wrapped so the returned future is `Send` (as C1 requires); mis-captures (e.g. a non-`Send` value held across `.await`) remain natural borrow-checker errors, unchanged from the hand-written path.

### The wiring prerequisite (not just codegen)

The macro makes multi-input tasks *authorable*, but they only *run* through `RunnableFlow` once the `InputWiring` seam from ADR 081 — currently single-arity (`crates/cli/src/run_flow.rs`) — is extended to tuple arities 2..=8. That extension is engine work, folded into **T72**, not something the macro can supply on its own.

### Feature wiring & the zero-dependency guarantee

`dagr-macros` is a proc-macro crate: `syn`/`quote` run inside the compiler and are **never linked into the shipped binary**. `dagr-core` depends on `dagr-macros` only behind the `macros` feature, and the expansion references only existing `dagr-core` items, so the produced program's runtime dependency graph is byte-for-byte unchanged. `cargo build --all --no-default-features` (a DoD check in T71) proves core still builds with no macro dependency at all.

## Consequences

- **Additive.** Every existing `impl Task`, demo, and test compiles unchanged; the macro is pure sugar.
- **`dagr-core` stays runtime-dependency-free** (ADR 081's guarantee preserved); the only new dependency is a build-time, opt-in proc-macro crate.
- **One authoring style to teach.** The quickstart and cookbook move to `#[task]` (T73) while documenting the hand-written form as the fallback.
- **Extensible.** The attribute-argument syntax (`#[task(compute)]`) leaves room for future opt-in markers (e.g. durability) without breaking existing usage.
- **Known limitation.** Proc-macro diagnostics can point at the `#[task]` site rather than the offending line; mitigated with a `trybuild` corpus (T73) and a cookbook "common mistakes" section.

## Rejected alternatives

- **A declarative `macro_rules!` `task!`.** Zero build dependencies, but it cannot cleanly infer the input tuple from a natural `run` signature, handles receiver state and generics poorly, and yields worse errors. Rejected: the ergonomic ceiling is exactly what this ADR exists to raise.
- **Putting `#[task]` in `dagr-core` directly (making core a proc-macro crate).** A crate cannot be both a normal library and a `proc-macro` crate, and it would drag `syn` into core's build graph unconditionally. Rejected in favour of a separate, optional crate re-exported behind a feature.
- **Mirroring `dagx`'s `&T` dep arguments.** dagr delivers inputs by value and expresses receive mode (owned / shared / clone-on-read) at the *registration* site, not in the task body (`binding.rs:158-173`). By-value arguments match the real delivery and avoid forcing `Clone`. Rejected as a false match to a different execution model.
- **Auto-wrapping a bare `-> T` return into `Result<T, TaskError>`.** Hides the failure channel and surprises readers. Rejected in favour of requiring the explicit `Result`.
