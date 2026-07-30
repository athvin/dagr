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
- Broken intra-doc links and missing docs on public items are **denied** — the
  latter promoted from `warn` by T96, see the additions table below (arch.md
  "Documentation": rustdoc on every public item, enforced by lint in CI).

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
| `clippy::collapsible_if` | allow | Under edition 2024 (T94) the lint's fix for `if let A { if let B { … } }` is a **let-chain** (`if let A && let B`), so denying it would force let-chain adoption at 25 sites — which T94's Out of scope defers to ordinary future work (see `pat-if-let-chains` in [`rust-skills-register.md`](rust-skills-register.md)). Re-denying the lint is how the adopting ticket finds its work list. |
| `rust::unsafe_code` | warn | dagr targets safe Rust; `unsafe` is not forbidden outright but every use is surfaced for review. |

## Additions beyond the deny groups

A lint added here **strengthens** the deny set; it is listed for the same reason
the exceptions are — so the deviation from the plain group posture is reviewable.

| Lint | Level | Rationale |
|---|---|---|
| `clippy::undocumented_unsafe_blocks` | deny | Added by T95. `unsafe_code` above surfaces each use for review; this makes the *justification* mandatory rather than customary — every `unsafe` block and `unsafe impl` must carry a `// SAFETY:` comment. dagr's whole `unsafe` surface is one `GlobalAlloc` impl whose blocks T94 had already commented, so this cost a single new comment and denies the next uncommented one. |
| `rust::missing_docs` | deny | Promoted from `warn` by T96. It is allow-by-default in rustc, so `warn` is what switched it on and `warnings = "deny"` has been promoting it ever since — it was already *effectively* denied. Writing the level out makes the intent readable at the setting and retires the previous rationale ("kept at `warn` pre-workspace so scaffolding crates in T1 are not blocked before their public surface exists"), which stopped being true when T1 shipped. This is a **declarative ratchet, not a bug fix**: clippy was already clean. |
| `clippy::missing_errors_doc` | deny | Promoted from `warn` by T96, for the same reason and with the same effect. Its previous rationale deferred to the error taxonomy — "encouraged but non-blocking until T3 defines the canonical error docs; revisited then" — and T3 shipped long ago; this is that revisit. T95's audit found **zero** missing `# Errors` sections, so the promotion costs nothing today and is what keeps that true. |

When T1/T7 apply the policy, any change to this exception set is reviewed as an
API decision (arch.md "Stability": additions to the core dependency/lint set are
reviewed).

## The two files agree, and that is checked

The policy is written down twice on purpose — `lints.toml` is the source of truth
and `Cargo.toml`'s `[workspace.lints]` is the applied form Cargo actually reads —
and two files carrying one decision drift.
[`scripts/check-lint-parity.sh`](../scripts/check-lint-parity.sh) asserts they
agree field for field, that every member opts in with `[lints] workspace = true`,
that no crate root carries a blanket `#![allow]`, and that the two ratcheted
lints above are `deny` in both. It proves those scans non-vacuous against a
fixture whose two files disagree.

## Suppressions expire: `#[expect]`, not `#[allow]`

Every suppression in production `src/` is an `#[expect(…, reason = "…")]`, not an
`#[allow]`. `#[expect]` warns when the suppressed lint **stops** firing, so a
suppression that outlives its cause becomes visible instead of accumulating
silently; an `#[allow]` never expires. The same parity check enforces both halves
— the form, and that each one states a reason.

Converting the workspace's suppressions found **five stale** ones, deleted rather
than converted: `Handle::for_registration`'s `dead_code` (the flow builder
consumes it now), three `clippy::unused_self` suppressions on exported methods
(the lint does not fire on an exported API), and one
`clippy::cast_precision_loss` on `apply_headroom_u32` (`f64::from(u32)` is exact,
so nothing was lost to suppress).

Exactly **one** suppression cannot take this form and says so at the site, marked
`EXPECT-EXEMPT:`: the numeric `From` impls in `crates/core/src/metrics.rs` are
generated by a `macro_rules!`, so all twelve expansions share one attribute span,
and neither covered lint fires for every expansion — `cast_lossless` fires for the
narrow integers and `cast_precision_loss` for the wide ones. An `#[expect]` there
would report "unfulfilled" on every build for whichever half did not fire.

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
