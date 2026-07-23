# 003 · T1 — Crate layout and workspace skeleton

> **Milestone:** M0 · **Size:** S · **Type:** setup · **Components:** structural (whole product; C24)
> **Branch:** `chore/t1-crate-layout-and-workspace-skeleton` · **Depends on:** T0.0a · **Blocks:** T2, T5, T7, T8, T9, T13

## Why / context
Every later ticket needs a place to land code, and that place has to reflect the product's real module boundaries rather than be reshuffled mid-project. This ticket commits an Architecture Decision Record (ADR) that resolves the facade-vs-workspace question and the renderer-independence question, then builds the compiling Cargo workspace it describes. The governing spec is `docs/arch.md` — "What this is" (the single-binary framing and the permanent non-goals), **C24 · Renderers** (renderers consume artifacts only and require no access to the pipeline binary), and **Stability** (the core crate holds a minimal, review-gated dependency set; MSRV is pinned in the workspace; supply-chain checks run in CI). The ADR's central decision is a multi-crate workspace with a separable renderer, locked here because T2, T5, T7, T8, T9, and T13 all assume these crate boundaries exist.

## Objective
Decide the crate topology and produce the empty-but-compiling skeleton it prescribes.

- Write and commit an ADR (in the repo's ADR location) that records: single facade crate vs multi-crate workspace, the chosen answer (multi-crate workspace), and the rationale tied to C24 and Stability; and whether the renderer must be a separate binary given C24's no-access-to-the-pipeline-binary requirement, with the chosen answer and rationale.
- Create a Cargo workspace with member crates `core`, `artifact`, `render`, and `cli`, each with a placeholder `lib` target (and a placeholder `bin` target where the ADR determines one is needed, e.g. the standalone renderer).
- Establish the dependency direction between crates so that `render` depends only on `artifact` (never on `core`'s live-pipeline surface), consistent with C24's artifact-only consumption.
- Pin the MSRV and toolchain at the workspace level and reference them from the README, per Stability.
- Wire the workspace so `cargo build`, `cargo test`, `cargo fmt --check`, and `cargo clippy` all succeed on the empty skeleton, giving every downstream ticket a green baseline.

## Test plan (write these first — TDD)
- **Workspace builds clean.** Setup: check out the ticket branch with the skeleton in place. Action: run `cargo build --workspace`. Expected: the build succeeds with zero errors and zero warnings across all four member crates.
- **Every member crate is discoverable and testable.** Setup: the workspace manifest lists `core`, `artifact`, `render`, and `cli`. Action: run `cargo test --workspace`. Expected: each crate is compiled and its (possibly trivial placeholder) test target runs to a clean pass; no member is silently excluded from the build graph.
- **Renderer independence is structurally enforced.** Setup: the `render` crate's manifest declares its dependencies. Action: inspect `render`'s dependency set (and attempt a throwaway edit that makes `render` reference a `core` live-pipeline type). Expected: `render` depends on `artifact` only; the throwaway edit either fails to compile or is impossible because no such dependency edge exists — demonstrating C24's "no access to the binary that produced the artifacts" at the crate-graph level. Revert the throwaway edit.
- **Renderer target matches the ADR decision.** Setup: the ADR states whether the renderer is a separate binary. Action: confirm the workspace's targets against the ADR — if the ADR says separate binary, a `render` (or `cli`-hosted) `bin` target exists and builds standalone; if it says a subcommand of the pipeline binary, the reasoning that still satisfies "no access to the pipeline binary" is recorded and the targets match. Expected: targets and ADR agree exactly, with no undocumented target.
- **MSRV is pinned and honored.** Setup: the workspace pins an MSRV/toolchain and the README names it. Action: read the pinned version from the workspace manifest and the README. Expected: both name the same version, and building under that pinned toolchain succeeds.
- **Core dependency set is minimal.** Setup: the `core` crate manifest. Action: list `core`'s direct dependencies. Expected: the set is empty or minimal and each entry is justified in the ADR, matching Stability's "core crate holds a minimal dependency set" commitment.
- **Formatting and lint baseline is green.** Setup: the empty skeleton. Action: run `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`. Expected: both pass with no diffs and no warnings.
- **Layout is documented.** Setup: the ADR and README. Action: read the described crate roles and dependency direction. Expected: each crate's purpose (`core`, `artifact`, `render`, `cli`) and the allowed dependency edges are written down and match the actual manifests.

## Definition of done
- [ ] An ADR is committed that records the single-facade-vs-multi-crate decision (chosen: multi-crate workspace) with rationale referencing C24 and Stability.
- [ ] The ADR records whether the renderer is a separate binary, answering the C24 requirement that rendering needs no access to the pipeline binary, with rationale.
- [ ] A Cargo workspace exists with member crates `core`, `artifact`, `render`, and `cli`, each carrying a placeholder `lib` target that compiles.
- [ ] Any `bin` target required by the ADR (e.g. the standalone renderer) exists and builds standalone.
- [ ] The `render` crate depends on `artifact` only and has no dependency edge onto `core`'s live-pipeline surface, so renderers consume artifacts and never a live pipeline (C24).
- [ ] `render`'s dependency direction makes it structurally incapable of reaching into the pipeline binary, satisfying C24's "rendering requires no access to the binary that produced the artifacts."
- [ ] MSRV/toolchain is pinned at the workspace level and named in the README (Stability).
- [ ] The `core` crate's direct dependency set is empty or minimal, with each entry justified, per Stability's minimal-core commitment.
- [ ] Crate roles and the allowed inter-crate dependency edges are documented (ADR and/or README) and match the actual manifests.
- [ ] `cargo build --workspace` and `cargo test --workspace` succeed with no warnings, giving downstream tickets (T2, T5, T7, T8, T9, T13) a green landing baseline.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Single facade crate vs multi-crate workspace: confirmed as multi-crate in the ADR, or does a concrete constraint favor a single facade re-exporting internal modules?
- Must the renderer be a separate binary, given C24 requires rendering with no access to the pipeline binary? (Resolve in the ADR: a separate binary is the obvious satisfier, but a subcommand that only ever reads artifacts may also satisfy the letter of C24 — the ADR must state which and why.)

## Out of scope
- Any actual renderer logic, DOT/Mermaid emission, or golden-file tests — that is C24's own implementation ticket, not this skeleton.
- Any task, handle, binding, assembly, runtime, artifact-serialization, or CLI-verb logic — those land in later crates via T9, T5, T13, and the M1+ tickets; here the crates are placeholders only.
- The CI pipeline definition and the acceptance-criteria coverage matrix themselves (T7), the compile-failure harness (T8), and the async-runtime/tokio decision (T2) — this ticket only guarantees the skeleton those tickets will extend, and must not pre-decide their content.
- Fingerprint composition, build-provenance embedding (C20/C21), and dependency-set enforcement tooling beyond declaring the minimal `core` set — do not build provenance capture or a dependency-policy gate here.
- Reintroducing any scope-boundary temptation: no scheduler, distributed execution, metadata store, web interface, DSL, or backfill orchestrator scaffolding, and no crate whose existence presupposes a runtime-mutable graph shape.
