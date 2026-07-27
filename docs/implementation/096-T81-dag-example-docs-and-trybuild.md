# 096 · T81 — many-dags example, cookbook, and `#[dag]` trybuild corpus

> **Milestone:** M6 · **Size:** M · **Type:** feature (tests/docs) · **Components:** C7, C26
> **Branch:** `feat/t81-dag-example-docs-and-trybuild` · **Depends on:** T80 · **Blocks:** —

## Why / context

T78–T80 deliver the `#[dag]` authoring surface and `inventory`-backed discovery. This ticket makes the feature *teachable and provably correct*: a copyable example binary that declares several `#[dag]`s and runs with a one-line `main`, a cookbook/README section that teaches the pattern and its constraints, and a `trybuild` diagnostic corpus that pins `#[dag]`'s compile-error UX. It is the M6 counterpart to `crates/cli/examples/multi_flow.rs` (the hand-wired `FlowRegistry` example) and to T73's `#[task]` trybuild suite.

The example is deliberately a **binary/example target, not a library** — ADR 092's crux is that `inventory` reliably collects only submissions compiled into the leaf binary (dtolnay/inventory#7). Running `list` on the example binary on **both** CI OSes is the real cross-platform proof that discovery works. The docs must state the two operator-facing obligations ADR 092 records: the app crate depends on `inventory = "0.3"` (the `$crate`-based `submit!` gives no path override), and `#[dag]` declarations live in the binary crate (cross-crate DAG libraries are unsupported for now).

## Objective

Ship a copyable example, docs, and a diagnostic corpus that make `#[dag]` + auto-discovery usable and regression-guarded.

- Add `crates/cli/examples/many_dags.rs`: two (or more) `#[dag]` fns over `FlowBuilder`, and `fn main() -> ExitCode { dagr_cli::run(std::env::args_os()) }`. Mirror `multi_flow.rs`'s module docs, exercising `list` / `graph <dag>` / `validate <dag>` / `run <dag> --store` from the header.
- Add a `#[dag]` `trybuild` corpus (alongside the `#[task]` suite) covering the diagnostics: malformed attribute grammar (`#[dag(bogus)]`, `#[dag(name = 42)]`), and a wrong-shaped fn signature. Note in-corpus that duplicate *DAG names* surface at **runtime** (`InvalidUsage` from `run`, T79), not at compile time.
- Update the cookbook/README (the T64 quickstart/cookbook home) with a `#[dag]` section: the `#[dag]` + `FlowBuilder` + one-line `main` pattern, the required `inventory = "0.3"` Cargo.toml line, and the leaf-binary contract (with cross-crate DAG libraries called out as out of scope). Keep the hand-wired `FlowRegistry` documented as the explicit fallback.
- Ensure the example runs end-to-end in CI on both `ubuntu-latest` and `macos-latest`.

## Test plan (write these first — TDD)

**Cross-platform discovery (the load-bearing proof)**
- Given `many_dags` built as an example binary, when `list` runs on `ubuntu-latest` and on `macos-latest`, then both DAGs are discovered (sorted, stable) on both platforms.
- Given `run <dag> --store <dir>`, when the example is driven, then the selected DAG runs to a real on-disk event stream and exits with the run's own code (delegation through `run_registry`), on both OSes.

**Graph-emittability through the sugar**
- Given `graph <dag>` for a DAG whose nodes were declared via `FlowBuilder::source` / `node` (the graph-emittable variants), when emitted, then a real graph artifact is produced (records the author-declared stable names), **not** `AssemblyFailure`.

**Diagnostic stability**
- Given each `#[dag]` misuse case, when compiled under `trybuild`, then the `.stderr` UI snapshot matches byte-for-byte on the pinned toolchain (snapshots frozen; `DAGR_BLESS`/blessing disabled in CI, matching the T8/T73 posture).

**Docs are runnable**
- Given the cookbook `#[dag]` snippet, when built as a doctest or as the `many_dags` example, then it compiles and runs (docs stay executable, matching the quickstart-is-the-example discipline).

## Definition of done

- [ ] `crates/cli/examples/many_dags.rs` declares ≥ 2 `#[dag]`s and a one-line `dagr_cli::run` `main`; its module docs show `list` / `graph` / `validate` / `run --store` invocations, mirroring `multi_flow.rs`.
- [ ] The example runs `list` / `graph <dag>` / `validate <dag>` / `run <dag> --store` end-to-end on both `ubuntu-latest` and `macos-latest`.
- [ ] A `#[dag]` `trybuild` corpus pins the malformed-attribute and bad-signature diagnostics with stable `.stderr` snapshots on the pinned toolchain.
- [ ] The example/cookbook are **binary/example** targets (not a lib), so submissions are reliably collected; the docs state this leaf-binary contract explicitly.
- [ ] The cookbook/README `#[dag]` section documents the `inventory = "0.3"` Cargo.toml requirement, the leaf-binary contract, and cross-crate DAG libraries as out of scope, and keeps the hand-wired `FlowRegistry` as the documented fallback.
- [ ] A DAG registered via `FlowBuilder::source` / `node` graph-emits (not `AssemblyFailure`), proven end-to-end.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured), on the platform matrix.

## Open questions
None.

## Out of scope
- The `FlowBuilder` façade (**T78**), the discovery machinery (**T79**), and the `#[dag]` macro (**T80**) — dependencies, delivered earlier in M6.
- Cross-crate DAG-library discovery and the `inventory`-hiding wrapper macro (Option B) — deferred per ADR 092; the docs describe the current contract, not these.
- A `crates/dagr` facade crate for the `dagr::run` spelling — a strictly-separable later ticket per ADR 092; the docs use `dagr_cli::run` (noting `use dagr_cli as dagr;` for the short spelling).
- Any change to engine behaviour or `dagr-core` — this ticket is example/tests/docs only.
