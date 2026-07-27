# 095 · T80 — `#[dag]` attribute macro (keep fn, generate factory, submit)

> **Milestone:** M6 · **Size:** M · **Type:** feature · **Components:** C1, C7
> **Branch:** `feat/t80-dag-attribute-macro` · **Depends on:** T78, T79 · **Blocks:** T81

## Why / context

This ticket delivers the authoring surface ADR 092 decided on: a `#[dag]` attribute — a sibling to `#[task]` — applied to a flow-builder fn. It ties together the `FlowBuilder` façade (T78) and the `inventory`-backed discovery (`DagRegistration` + `dagr_cli::run`, T79): the macro keeps the user's fn, generates a `fn() -> RunnableFlow` factory that drives that fn through a `FlowBuilder`, and emits an `inventory::submit!` so `run` discovers it. With `#[dag]` in place, adding a DAG needs no registry edit and `main` is a one-liner.

`#[dag]` lives in `crates/macros` (`dagr-macros`), the same build-time-only, zero-runtime-dependency proc-macro crate as `#[task]`, and follows its discipline exactly: the macro is never linked into a binary, and its expansion references only existing items in the user's dependency graph. Unlike `#[task]` (whose expansion points only at `::dagr_core::…`, which every user already depends on), `#[dag]`'s expansion points at `::dagr_cli::…` **and** `::inventory::…` — so, per ADR 092, the app crate depends on both `dagr-cli` and `inventory = "0.3"` (the `$crate`-based `inventory::submit!` offers no path override). The `#[cfg(feature = "dag")] pub use dagr_macros::dag;` re-export lives in `dagr-cli` (which already re-exports at that layer), **not** `dagr-core`, so no `dagr-core → dagr-cli` cycle is introduced and core's zero-runtime-dep guarantee is untouched.

The `inventory::submit!` must be emitted at **module (item) scope** — as a sibling item next to the kept fn — not inside the fn body. The attribute grammar is `#[dag]` (name = fn name) or `#[dag(name = "…")]`; any other argument is a spanned `compile_error!`, mirroring `#[task]`'s `parse_exec_class` (`crates/macros/src/lib.rs:130`).

## Objective

Add a `#[dag]` attribute that keeps the user fn, generates a discovery-registered factory, and rejects bad grammar — additively, with no runtime dependency added to `dagr-core`.

- Add `#[proc_macro_attribute] pub fn dag(attr, item)` to `crates/macros/src/lib.rs`, applied to a fn `fn NAME(f: &mut FlowBuilder) { … }`.
- Parse the optional attribute: empty → DAG name = fn name; `name = "…"` → the given name; anything else → a spanned `compile_error!` naming the accepted grammar.
- Keep the user's fn verbatim, and emit two **sibling items** at module scope:
  - a factory `fn __dag_factory_<name>() -> ::dagr_cli::run_flow::RunnableFlow` that news a `RunnableFlow`, wraps a `&mut` of it in a `FlowBuilder`, calls the user's fn, and returns the flow;
  - an `::inventory::submit!{ ::dagr_cli::DagRegistration { name: <name>, factory: __dag_factory_<name> } }`.
- Generate hygienic, collision-free item names (derive from the DAG name / fn ident) so multiple `#[dag]`s in one module do not clash at the Rust-item level. (Duplicate *DAG names* are a runtime concern already handled in T79's `run`.)
- Re-export the attribute from `crates/cli/src/lib.rs`: `#[cfg(feature = "dag")] pub use dagr_macros::dag;`, and include it in the `dagr_cli::prelude`.
- Add `dagr-macros` as a dependency of `dagr-cli` (a second edge onto the leaf proc-macro crate; no cycle is possible).
- Keep `dagr-core`'s build graph and every existing `#[task]` / `impl Task` unchanged — `#[dag]` adds a new path and removes none.

## Test plan (write these first — TDD)

**Expansion & discovery** (in a test/example **binary**, per the leaf-binary contract)
- Given `#[dag] fn etl(f: &mut FlowBuilder) { … }` compiled into a binary that calls `dagr_cli::run`, when `list` runs, then `etl` is discovered by name (the macro emitted a working factory + `submit!`).
- Given `#[dag(name = "nightly")]` on a fn named `foo`, when discovered, then the registered DAG name is `nightly`, not `foo`.
- Given two `#[dag]` fns in the **same module**, when compiled, then both expand without an item-name collision and both are discovered.

**Grammar enforcement**
- Given `#[dag(bogus)]` (or `#[dag(name = 42)]`), when compiled, then a spanned `compile_error!` names the accepted `name = "…"` grammar (mirrors `#[task]`'s attribute rejection).

**Additivity & zero-core-dep**
- Given the existing `#[task]` tasks and hand-written `impl Task`s, when the workspace is built with `#[dag]` present, then they compile unchanged.
- Given `cargo build --all --no-default-features`, when compiled, then it still builds: the `dag` re-export and the `inventory` edge are gated off cleanly and `dagr-core` gains no dependency on `dagr-macros`'s new attribute at runtime.

## Definition of done

- [ ] `#[proc_macro_attribute] pub fn dag` exists in `crates/macros/src/lib.rs`; it keeps the user fn and emits, at item scope, a `fn() -> RunnableFlow` factory plus an `inventory::submit!` of a `DagRegistration`.
- [ ] `#[dag(name = "…")]` overrides the default fn-name; empty attribute uses the fn name; any other grammar is a spanned `compile_error!` naming the accepted form.
- [ ] The `submit!` is emitted at module (item) scope, not inside the fn; expansion references only `::dagr_cli::…` / `::inventory::…`.
- [ ] Generated item names are hygienic/collision-free, so multiple `#[dag]`s per module compile.
- [ ] `crates/cli/src/lib.rs` re-exports `dag` behind `#[cfg(feature = "dag")]` and includes it in `dagr_cli::prelude`; `dagr-cli` depends on `dagr-macros` with no cycle.
- [ ] `dagr-core`'s zero-runtime-dependency guarantee is untouched, and `cargo build --all --no-default-features` still builds.
- [ ] Every existing `#[task]` / hand-written `impl Task` compiles unchanged.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The `FlowBuilder` façade (**T78**) and the `DagRegistration` / `collect!` / `run` discovery machinery (**T79**) — dependencies of this ticket, not delivered here.
- The many-dags example, cookbook rewrite, and the full `#[dag]` trybuild UI-snapshot corpus — those are **T81**; this ticket ships only the compile-fail cases its own TDD requires.
- Any per-node `NodePolicy` / execution-class argument on `#[dag]` — `#[dag]` declares graph structure; per-task execution class stays on `#[task]`. Out of scope for M6.
- Cross-crate DAG-library discovery and the `inventory`-hiding wrapper macro (Option B) — deferred per ADR 092.
- Any change to engine behaviour, scheduling, or `dagr-core`'s runtime dependency graph — `#[dag]` is pure sugar over the `FlowBuilder` + registry surfaces, and hand-wiring a `FlowRegistry` remains the first-class fallback.
