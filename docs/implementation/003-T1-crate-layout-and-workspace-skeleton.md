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

---

# ADR: crate layout and workspace skeleton

> The repo keeps ADRs inside their implementation-ticket file (see the T2, T0.6,
> T3, T4, and T0.7 ADR tickets, which all embed the ADR at the ticket's own
> `path`). This ADR is committed here, at
> `docs/implementation/003-T1-crate-layout-and-workspace-skeleton.md`, the ADR
> location for ticket T1.

## Status

Accepted (2026-07-23). Implemented by the workspace this ticket ships.

## Context

`docs/arch.md` frames dagr as a single binary that *is* the pipeline, with a
short list of permanent non-goals ("What this is"). Two structural questions
must be settled before any code lands, because T2, T5, T7, T8, T9, and T13 all
assume the answers:

1. **Single facade crate vs multi-crate workspace.** Later tickets need stable
   module boundaries that will not be reshuffled mid-project.
2. **Renderer independence (C24).** arch.md **C24 · Renderers** requires that a
   renderer "consume artifacts only, never a live pipeline" and that "rendering
   requires no access to the binary that produced the artifacts." The question
   is whether that forces the renderer to be a separate binary.

The governing constraints are **C24** (artifact-only consumption; no access to
the pipeline binary) and **Stability** (the core crate holds a minimal,
review-gated dependency set; MSRV is pinned in the workspace).

## Decision

### 1. Multi-crate workspace (not a single facade crate)

dagr is a Cargo **workspace** with four member crates under `crates/`:

| Crate | Role | Depends on (workspace) |
|---|---|---|
| `core` (`dagr-core`) | The authoring surface and execution core — the code that *is* a running pipeline (the "live-pipeline surface"). | *(nothing)* |
| `artifact` (`dagr-artifact`) | The serializable records a run leaves behind — graph artifact (C20), run artifact (C22), event records (C19) — and their schemas. The C24 consumption boundary. | *(nothing)* |
| `render` (`dagr-render`) | The diagram renderer: reads an artifact and emits DOT / Mermaid (C24). A library, plus a standalone renderer binary. | `artifact` **only** |
| `cli` (`dagr-cli`) | The pipeline binary and its command-line contract (C26): the standard verbs and typed parameters. | `core`, `artifact`, `render` |

Allowed dependency edges, and only these: `cli → {core, artifact, render}`,
`render → artifact`, `core → ∅`, `artifact → ∅`. Nothing depends on `cli`.
`render` has **no** edge onto `core`.

**Rationale (C24 + Stability).** A single facade crate re-exporting internal
modules cannot make renderer independence *structural*: with everything in one
crate, nothing stops rendering code from reaching a live-pipeline type, so C24
would rest on discipline. Separate crates with no `render → core` edge make the
reach *inexpressible* — the import does not resolve (verified: a throwaway
`use dagr_core` in `render` fails with `E0432 unresolved import`). Separate
crates also serve Stability's minimal-`core` commitment: `core` can hold its own
tight dependency set instead of a facade transitively pulling in everything the
renderer and CLI need. The lockfile confirms `core` and `artifact` have zero
external dependencies today.

### 2. The renderer is a library, exposed as a standalone binary *and* usable as a CLI subcommand

The renderer is realized as the `render` **library** crate that depends on
`artifact` only. C24's guarantee — "no access to the binary that produced the
artifacts" — is satisfied **structurally by the crate graph** (the missing
`render → core` edge), independently of how rendering is invoked. On top of that
library:

- A **standalone renderer binary** ships in the `render` crate
  (`crates/render/src/main.rs`, bin name `dagr-render`). It builds and links with
  no access to `core` or `cli`, which is the concrete proof of C24 renderer
  independence and the answer to the ticket's "must the renderer be a separate
  binary?" question.
- Rendering is *also* reachable as the pipeline binary's `render` subcommand
  (hosted in `cli`, per C26). Because that subcommand drives the same
  artifact-only `render` library, it consumes artifacts only and does **not**
  weaken C24 — the subcommand path satisfies the letter of C24 for the same
  structural reason the standalone binary does.

So the answer is *both, and it does not matter which*: the artifact-only crate
edge is what makes rendering independent of the pipeline binary, whether run
standalone or as a subcommand. A separate binary is the obvious satisfier and is
provided; the structural guarantee is what actually enforces the requirement.

### 3. MSRV / toolchain pinned at the workspace level

`[workspace.package].rust-version = "1.95.0"` pins the MSRV at the workspace
level (Stability), inherited by every member via `rust-version.workspace = true`.
It matches `rust-toolchain.toml`'s `channel = "1.95.0"` and the README's "MSRV"
line verbatim; drift is checked by `scripts/check-workspace-skeleton.sh` and by
T0.0a's hygiene check.

### 4. Lint policy inherited, not reinvented

`lints.toml` (T0.0a) is the single source of truth for the warnings-denied
posture. Its `[rust]`, `[clippy]`, and `[rustdoc]` tables are copied verbatim
into `[workspace.lints.*]` in the root manifest, and each member opts in with
`[lints] workspace = true`. No crate invents its own lint attributes. In
particular `unsafe_code` stays `warn` (never `forbid`), honoring
`docs/lint-policy.md`'s recorded exception.

## Consequences

- T2, T5, T7, T8, T9, and T13 land against fixed crate boundaries; the graph and
  the four crate roles will not be reshuffled.
- C24 renderer independence is a compile-time property from day one: a
  `render → core` reference does not compile. C24's own implementation ticket
  (T46/T47) fills the `render` crate without touching this guarantee.
- `core`'s dependency set is empty and every future addition is an API decision
  (Stability). The committed `Cargo.lock` gives `cargo audit` (T7) a target.
- The renderer is diagram-source-only and artifact-driven, so it works on a
  historical artifact with no live pipeline present.
- The workspace is an empty-but-green baseline: `cargo build --workspace`,
  `cargo test --workspace`, `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps` all pass.

## Rejected alternatives

- **Single facade crate re-exporting internal modules.** Rejected: it cannot
  make C24 renderer independence structural (nothing stops rendering code from
  reaching a live-pipeline type within one crate), and it works against
  Stability's minimal-`core` commitment by forcing one dependency set to cover
  the pipeline, the renderer, and the CLI at once.
- **Renderer as a pipeline-binary subcommand *only* (no separate crate/binary).**
  Rejected as the sole form: a subcommand that only reads artifacts satisfies the
  *letter* of C24, but keeping rendering in the same crate as the live pipeline
  would leave the "no access to the pipeline binary" guarantee resting on
  discipline. We keep the subcommand (it is convenient, per C26) but back it with
  a separate `render` crate whose missing `core` edge makes the guarantee
  structural, and we additionally ship the standalone binary as explicit proof.
- **Merging `artifact` into `core`.** Rejected: `render` must depend on the
  artifact types without depending on the live pipeline. If artifacts lived in
  `core`, `render → artifact` would drag in `core`, destroying C24 independence.
  A standalone `artifact` crate is what keeps the consumption boundary clean.
