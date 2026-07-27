# 094 · T79 — `DagRegistration` inventory type + `dagr_cli::run` auto-discovery entrypoint

> **Milestone:** M6 · **Size:** M · **Type:** feature · **Components:** C7, C26
> **Branch:** `feat/t79-dag-registration-and-run-entrypoint` · **Depends on:** T74, T75 · **Blocks:** T80

## Why / context

ADR 092 decides that DAGs are discovered at link time via the `inventory` crate and dispatched through the **existing** `run_registry` (`crates/cli/src/registry.rs`). This ticket adds the runtime half of that decision — the collected record type, the one `inventory::collect!`, the `dag` feature that pulls `inventory`, and the `dagr_cli::run(argv)` entrypoint — **before** the `#[dag]` macro exists, so the discovery mechanism is built and tested against hand-written `inventory::submit!`s. T80 then makes `#[dag]` emit those submissions.

The entrypoint must neutralise two `inventory` realities the ADR calls out. First, `inventory::iter` order is **unspecified**, and `FlowRegistry::add` does not reject duplicate names (`registry.rs:140`), so `run` sorts discovered records by name and fails fast on a duplicate (before any flow is built). Second, `inventory` is a runtime dependency and must never reach `dagr-core`; it is confined to `dagr-cli` behind a default-on `dag` feature (`dag = ["dep:inventory"]`), so `--no-default-features` builds `dagr-cli` with no `inventory` edge, mirroring how `--no-default-features` drops `dagr-macros` from core.

`run` is a thin layer: it discovers records, sorts + dedups, builds a `FlowRegistry` (using `single_flow` when exactly one DAG is present, so the name may be omitted — parity with the one-flow ergonomic default), and delegates to `run_registry`, which already owns list/run/graph/validate dispatch, per-verb exit codes, store handling, run-id minting, and diagnostics.

## Objective

Deliver `inventory`-backed DAG discovery and a one-call entrypoint that reuses the existing registry dispatch.

- Add a **default-on `dag` feature** to `crates/cli/Cargo.toml` with `inventory = { version = "0.3", optional = true }` and `dag = ["dep:inventory"]`; gate all items below behind `#[cfg(feature = "dag")]`.
- Define `pub struct DagRegistration { pub name: &'static str, pub factory: fn() -> RunnableFlow }` and call `inventory::collect!(DagRegistration)` **exactly once** in `dagr-cli` (a `collect!` must live in the crate that defines the type).
- Add `pub fn run<I, T>(argv: I) -> ExitCode` (same `I: IntoIterator<Item = T>, T: Into<OsString> + Clone` shape as `run_registry`) that: iterates `inventory::iter::<DagRegistration>()`, sorts records by `name`, rejects a duplicate name with a clear `ExitCode::InvalidUsage` diagnostic naming the duplicate (before building any flow), builds a `FlowRegistry` in sorted order (via `single_flow` when exactly one DAG, else `add` per record), and delegates to `run_registry`.
- Re-export `run` and `DagRegistration` at the `dagr_cli` root and from the `dagr_cli::prelude` (added in T78), all under `#[cfg(feature = "dag")]`.
- Confirm `inventory` resolves to `MIT` under `deny.toml` with no `deny.toml` change; if the resolver disagrees, record the minimal change and its justification.

## Test plan (write these first — TDD)

**Discovery, ordering, dedup** (submissions from a test/example **binary**, per the leaf-binary contract)
- Given three `DagRegistration`s submitted in an arbitrary order, when `run(["prog", "list"])` runs, then the names print **sorted** and stably across repeated runs (inventory's unspecified order neutralised).
- Given two submitted records with the **same** name, when `run` builds the registry, then it exits `InvalidUsage` with a message naming the duplicated DAG, **before** any flow is built.

**Delegation parity with the existing registry**
- Given exactly one submitted DAG, when `run(["prog", "run", "--store", <dir>])` runs with the name omitted, then the single DAG is dispatched (single-flow default), producing the same run outcome/exit code as the equivalent hand-wired `FlowRegistry::single_flow` + `run_registry`.
- Given two DAGs and `run(["prog", "graph", "<name>"])` / `run(["prog", "validate", "<name>"])`, when dispatched, then each verb produces the same artifact/exit code it would through a hand-wired `FlowRegistry` (delegation is transparent).

**Feature gating**
- Given `dagr-cli` built with `--no-default-features`, when the crate is compiled, then there is **no** `inventory` dependency edge and the `DagRegistration` / `run` items are absent; given default features, they are present.

## Definition of done

- [ ] `crates/cli/Cargo.toml` gains a default-on `dag` feature = `["dep:inventory"]` with `inventory` as an optional dependency; `cargo build -p dagr-cli --no-default-features` has no `inventory` edge.
- [ ] `DagRegistration { name, factory }` is defined and `inventory::collect!(DagRegistration)` appears exactly once in `dagr-cli`.
- [ ] `dagr_cli::run(argv)` sorts discovered records by name, rejects duplicate names with an `InvalidUsage` diagnostic before dispatch, builds a `FlowRegistry` (single-flow default for exactly one DAG), and delegates to `run_registry`.
- [ ] `list` output is sorted by name and stable across runs regardless of submission order.
- [ ] `run` / `DagRegistration` are re-exported at the `dagr_cli` root and prelude under `#[cfg(feature = "dag")]`.
- [ ] `cargo deny` / `cargo audit` are green with `inventory` in the tree; no `deny.toml` change (MIT-resolved) — or the change is recorded with justification.
- [ ] A discovery test exercising `list` / `run` / `graph` / `validate` through submitted records passes on both `ubuntu-latest` and `macos-latest`, with the submissions declared in the **binary/example** crate (not a dependency lib), per the ADR's leaf-binary contract.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (The user-manifest `inventory` dependency and the leaf-binary contract are decided in ADR 092; this ticket implements the `dagr-cli` side only.)

## Out of scope
- The `#[dag]` attribute macro that emits `inventory::submit!` — that is **T80**; this ticket tests discovery against hand-written `submit!`s.
- The `FlowBuilder` façade — delivered by **T78** (a dependency for T80, not this ticket).
- The many-dags example, cookbook rewrite, and trybuild corpus — those are **T81**.
- Cross-crate DAG-library discovery — out of scope for M6 per ADR 092 (inventory dead-code-elimination; DAGs live in the binary crate).
- A `dagr_cli`-owned wrapper `submit!` that hides `inventory` from the user manifest (Option B in ADR 092) — a possible later ticket; M6 uses the direct `::inventory::submit!`.
