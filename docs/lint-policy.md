# Lint and supply-chain policy

This document is the human-readable companion to [`lints.toml`](../lints.toml)
at the repository root. It states dagr's warnings-denied posture, justifies
every `allow` exception, and records the license-metadata target the
supply-chain check (`cargo deny`) will validate against. It is authored by
ticket 001 (T0.0a); it is **applied** by later tickets, which this document
names so no downstream ticket has to invent a location.

## Warnings-denied posture

dagr's entire pitch is compile-time confidence (arch.md "What this is",
"Stability"). "Clippy with warnings denied" is therefore a **shared contract**,
not a per-developer habit:

- Rust compiler `warnings = "deny"` — nothing ships carrying a warning.
- Clippy lint groups `all` and `pedantic` are denied at group level.
- The rustdoc lint denies broken intra-doc links and warns on missing docs on
  public items (arch.md "Documentation": rustdoc on every public item,
  enforced by lint in CI).

## Where this policy lives and who applies it

- **Source of truth:** `lints.toml` at the repository root. Its `[rust]`,
  `[clippy]`, and `[rustdoc]` tables are shaped exactly like a
  `[workspace.lints]` table so they can be copied verbatim.
- **T1 (crate layout / workspace skeleton)** wires the policy into the workspace
  manifest under `[workspace.lints]` and has each crate opt in with
  `[lints] workspace = true`, applying it workspace-wide from one place. T1 owns
  creating `Cargo.toml`; this ticket adds none.
- **T7 (CI pipeline)** enforces it: `cargo clippy --workspace --all-targets --
  -D warnings`, `cargo fmt --all --check`, `cargo doc` with `RUSTDOCFLAGS=-D
  warnings`, and the supply-chain jobs below.

## Allowed exceptions (each justified)

Every exception weakens the deny set by exactly the one lint named, with a
one-line rationale. No exception silences the compiler `warnings = "deny"` line
or the top-level clippy group denies.

| Lint | Level | Rationale |
|---|---|---|
| `clippy::module_name_repetitions` | allow | dagr's modules follow the C-numbered component names (`node`, `node_policy`); repeated stems read naturally here. |
| `clippy::missing_errors_doc` | warn (not deny) | Encouraged but non-blocking until the error taxonomy (T3) defines the canonical error docs; revisited then. |
| `rust::missing_docs` | warn (not deny) | Public-item rustdoc is enforced by the rustdoc job; kept at `warn` pre-workspace so scaffolding crates in T1 are not blocked before their public surface exists. |
| `rust::unsafe_code` | warn | dagr targets safe Rust; `unsafe` is not forbidden outright but every use is surfaced for review. |

When T1/T7 apply the policy, any change to this exception set is reviewed as an
API decision (arch.md "Stability": additions to the core dependency/lint set are
reviewed).

## Supply-chain: license metadata (target for `cargo deny`)

The project is licensed **MIT** — see [`LICENSE`](../LICENSE), which carries the
`SPDX-License-Identifier: MIT` tag.

- **Allowed license for `cargo deny` (T7):** `MIT`. When T7 authors `deny.toml`,
  its `[licenses]` allow-list target is `SPDX-License-Identifier: MIT`; every
  crate's declared license must resolve to an SPDX identifier in that allow set.
- Each shipped crate (created by T1) declares `license = "MIT"` in its package
  metadata so `cargo deny check licenses` has an unambiguous, machine-readable
  target.
- `cargo audit` (advisories) and `cargo deny` (licenses, sources, advisories)
  run in CI per arch.md "Stability": Supply chain. This ticket only records the
  license target; wiring the jobs is T7.
