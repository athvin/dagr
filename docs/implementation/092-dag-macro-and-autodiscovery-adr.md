# 092 · Declarative-DAG ADR — `#[dag]` + auto-discovered many-dags-per-binary

> **Date:** 2026-07-26 · **Status:** accepted (operator-approved framework feature) · **Type:** decision · **Components:** C1, C7, C26
> **Branch:** `feat/dag-macro-adr` · **Relates to:** T13 (C7 flow), T55 (C26 CLI contract), ADR 081 (run-a-flow), ADR 082 (`#[task]` macro), ADR 086 (flow registry), T74/T75 (registry) · **Unblocks:** T78 (FlowBuilder façade), T79 (registration + entrypoint), T80 (`#[dag]` macro), T81 (example + docs)

## Why / context

Two ergonomic layers already exist over the finished engine. ADR 082 added `#[task]`, so an author writes only `async fn run` and the `impl Task` is generated (`crates/macros/src/lib.rs`). ADR 086 added `dagr_cli::registry::FlowRegistry` + `run_registry`, so **one binary hosts many named flows** and selects one per invocation — `dagr run etl`, `dagr run nightly`, `dagr list`, `dagr graph <flow>`, `dagr validate <flow>` all work today (`crates/cli/src/registry.rs`, `crates/cli/examples/multi_flow.rs`). The runtime for "many DAGs in one binary" is therefore **already shipped**.

What is still missing is the *declaration + discovery* layer an operator asked for after comparing against the `dagx` crate. A flow today is a hand-written factory `fn build_etl() -> RunnableFlow { … }` (`crates/cli/examples/multi_flow.rs:106`), and every flow must be hand-wired into `FlowRegistry::new().add("etl", …).add(…)` in `main`. Adding a DAG means editing the central registry. The operator wants a `#[dag]` attribute — a sibling to `#[task]` — so a DAG is declared with less boilerplate over the real, type-checked builder, and is **auto-registered** so `main` is a one-liner and adding a DAG needs no registry edit. (`dagx` itself has no DAG-level macro; it keeps DAG construction imperative. This layer goes one step beyond it while preserving dagr's compile-time wiring guarantees.)

This ADR records the decision to add that layer as a purely additive, opt-in feature that changes **no** engine behaviour and keeps `dagr-core` runtime-dependency-free.

## Decision

Add three pieces, all at the CLI/authoring layer, none touching `dagr-core`:

1. **`FlowBuilder`** — a thin newtype declaration façade over `RunnableFlow` (`crates/cli`), exposing a curated `source` / `node` / `source_erased` / `node_erased` surface that returns the real `Handle<T>`.
2. **`#[dag]`** — a new attribute in `dagr-macros` (build-time only, zero runtime deps, same discipline as `#[task]`) that keeps the user's flow-builder fn, generates a `fn() -> RunnableFlow` factory, and registers it for discovery.
3. **`dagr_cli::run(argv)`** — an auto-discovery entrypoint that finds every declared DAG, builds a `FlowRegistry`, and delegates to the existing `run_registry`.

```rust
use dagr_cli::prelude::*;   // #[task], #[dag], FlowBuilder, run

#[dag]                       // name defaults to the fn name ("etl"); #[dag(name = "etl")] overrides
fn etl(f: &mut FlowBuilder) {
    let rows = f.source("extract", Extract { rows: 100 });   // rows: Handle<Rows>
    f.node("load", Load, rows);                              // wrong wiring => COMPILE error
}

#[dag(name = "analytics")]
fn analytics(f: &mut FlowBuilder) {
    f.source("aggregate", Aggregate { seed: 42 });
}

fn main() -> std::process::ExitCode {
    dagr_cli::run(std::env::args_os())   // discovers etl + analytics, delegates to run_registry
}
```

### The crux: `inventory` collects reliably only in the leaf binary

Auto-discovery uses the `inventory` crate: `#[dag]` emits an `inventory::submit!` of a `{ name, factory: fn() -> RunnableFlow }` record, and `run` iterates `inventory::iter` to build the registry. `inventory` works by registering life-before-`main` constructors in a linker section. The load-bearing consequence (dtolnay/inventory#7) is that **a submission is reliably collected only when it is compiled into the final linked binary**; a `#[dag]` placed in a *dependency library crate* that the binary does not otherwise reference is dropped by linker dead-code elimination, and the binary sees zero DAGs. Submissions in the leaf binary crate itself are always collected.

Therefore the reliable, documented contract is: **`#[dag]` declarations live in the binary crate that calls `dagr_cli::run`** (across as many modules of that crate as you like). This fully satisfies "many DAGs in one binary." Cross-crate DAG *libraries* are **out of scope** for this milestone; the escape hatch (a `use dag_lib::_force_link;` reference from the binary) is documented but not a supported surface. This constraint is the single most important thing this ADR pins down.

### Why factories, not stored flows (unchanged from ADR 086)

`RunnableFlow::run(self, …)` / `into_pipeline(self)` **consume** the flow and the type is not `Clone` (`crates/cli/src/run_flow.rs`), so one instance serves at most one verb. `DagRegistration` therefore stores a `factory: fn() -> RunnableFlow` (a plain fn pointer — the generated factory), and `run` builds a `FlowRegistry` of those factories, reusing ADR 086's every-verb-its-own-fresh-flow guarantee verbatim.

### Path resolution: the user depends on `inventory`

`inventory::submit!` / `collect!` expand through `$crate`, resolving `inventory` by the caller's extern-prelude name with **no path override**. So the `#[dag]` expansion emits `::inventory::submit!{ … }` and the app crate must depend on `inventory` directly — exactly as it already depends on `dagr-core` for `#[task]`. `dagr-cli` defines the collected type and calls `inventory::collect!(DagRegistration)` exactly once (a `collect!` must live in the crate that defines the type).

### Determinism: sort + dedup by name in the entrypoint

`inventory::iter` order is unspecified, and `FlowRegistry::add` does not reject duplicate names (`crates/cli/src/registry.rs:140`). So `run` **sorts discovered records by name and fails fast on a duplicate name** (`ExitCode::InvalidUsage`, before any flow is built), giving deterministic `list` output and a deterministic "available flows" diagnostic — matching the registration-order invariant ADR 086 documents.

### Feature wiring & the zero-dependency guarantee

`inventory` is a runtime dependency; it lands in **`dagr-cli`**, never `dagr-core`. `dagr-cli` gains a **default-on `dag` feature** that pulls `inventory` as an *optional* dep (`dag = ["dep:inventory"]`) and gates the `DagRegistration` type, the `collect!`, the `run` entrypoint, and the `#[cfg(feature = "dag")] pub use dagr_macros::dag;` re-export. `--no-default-features` builds `dagr-cli` with no `inventory` edge. The `#[dag]` proc-macro is added to `dagr-macros` unconditionally (build-time only, never linked), and its expansion references only `::dagr_cli::…` / `::inventory::…` items, so `dagr-core`'s zero-runtime-dependency guarantee (ADR 081/082) is untouched. `inventory` is `MIT OR Apache-2.0` (MIT-resolved) with no transitive runtime deps, so no `deny.toml` change is expected — a `cargo deny` green check is a DoD line because it is the first runtime dependency added outside the already-vetted clap/tokio/rayon/tracing set.

## Consequences

- **Additive and opt-in.** Every existing `impl Task`, `RunnableFlow`, `FlowRegistry`, demo, and test compiles unchanged. Hand-wired registries (`multi_flow.rs`) still work; `#[dag]` + `run` is the auto-discovery option.
- **`dagr-core` stays runtime-dependency-free.** `inventory` is confined to `dagr-cli` behind an optional feature; core never sees it.
- **One authoring story to teach.** Declare tasks with `#[task]`, declare DAGs with `#[dag]`, run them with a one-line `main`. The quickstart/cookbook gain the pattern (T81) while the hand-wired `FlowRegistry` remains documented as the explicit fallback.
- **A known, documented constraint.** DAGs must live in the binary crate; cross-crate DAG libraries are unsupported for now. This is a real limitation of the `inventory` approach and is stated wherever `#[dag]` is taught.
- **A small public surface addition.** `dagr_cli` gains `FlowBuilder`, `DagRegistration`, `run`, a `prelude`, and the `dag` re-export — recorded here as an operator-approved M6 addition to C7/C26.

## Rejected alternatives

- **A declarative node/edge DSL inside `#[dag] { … }`.** Most concise, but it re-derives types the compiler already knows, fights type inference, and points errors at the `#[dag]` site rather than the offending edge — the same fragility ADR 082 rejected for a declarative `task!`. Rejected in favour of an attribute over a real, type-checked builder body where mis-wiring stays a compile error.
- **A new `crates/dagr` facade crate** (to get the `dagr::run` / `use dagr::prelude::*` spelling). It would expand the workspace skeleton the T1 crate-layout ADR pins and that `scripts/check-workspace-skeleton.sh` enumerates (`core/artifact/render/cli`), a much larger change than the naming buys. The entrypoint is `dagr_cli::run`; a caller who wants the short spelling writes `use dagr_cli as dagr;` at zero cost. A facade is a strictly-separable later ticket.
- **`inventory` (or the `#[dag]` plumbing) in `dagr-core`.** Violates the zero-runtime-dependency invariant (ADR 081/082) and would drag a runtime dep into core's build graph unconditionally. Rejected; it lives in `dagr-cli` behind an optional feature.
- **`linkme` instead of `inventory`.** Same distributed-slice / dead-code-elimination class, no ergonomic win, and a second link-section mechanism to reason about. Rejected in favour of the more widely used `inventory`.
- **A `#[dag]` that *returns* a `RunnableFlow` value instead of registering.** That is just today's `fn build_etl() -> RunnableFlow` with extra syntax — it loses the auto-discovery that is the whole point (`main` would still hand-wire the registry). Rejected; the value-returning path already exists as the hand-wired `FlowRegistry` fallback.
- **Making the user's `Cargo.toml` `inventory`-free via a `dagr-cli`-owned wrapper `submit!` macro (Option B).** Works (a `macro_rules!` in `dagr-cli` where `$crate` resolves correctly, calling the real `inventory::submit!`), but adds a macro-in-a-macro layer for a one-line manifest saving. Deferred as a self-contained follow-up if the operator wants the clean-manifest property; M6 ships the direct `::inventory::submit!` (the user adds `inventory = "0.3"`).
- **Cross-crate DAG-library discovery.** Unreliable by inventory's design (dead-code elimination drops unreferenced dependency submissions). Deferred; M6's contract is DAGs-in-the-binary-crate.
