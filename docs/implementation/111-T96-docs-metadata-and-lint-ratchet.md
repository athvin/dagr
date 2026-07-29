# 111 · T96 — crate metadata, per-crate docs, and the lint ratchet

> **Milestone:** M9 · **Size:** M · **Type:** feature (docs) · **Components:** Documentation, Stability
> **Branch:** `feat/t96-docs-metadata-and-lint-ratchet` · **Depends on:** T95 · **Blocks:** T98

## Why / context

Three separate gaps, all in the "publishable, self-describing crate" family.

**Crate metadata is absent.** No member declares `description`, `keywords`,
`categories`, `readme`, `documentation`, or `homepage`, and none sets
`publish = false`. As configured, all six crates are nominally publishable to
crates.io and all six would be **rejected**, because `description` is mandatory
(`lint-cargo-metadata`, `doc-cargo-metadata`). That is a live foot-gun: the
failure mode is discovering it during a release, not before.

**The lint deferrals have expired.** `docs/lint-policy.md` justifies
`missing_docs = "warn"` as "kept at `warn` pre-workspace so scaffolding crates in
T1 are not blocked before their public surface exists" and
`clippy::missing_errors_doc = "warn"` as "encouraged, not blocking, until the
error taxonomy (T3) defines the canonical error docs; revisited then." T1 and T3
both shipped long ago; the file still says "revisited then." Being honest about
the current state: `warnings = "deny"` already promotes every warn-level lint,
so both are *effectively* denied today and clippy is clean. **This change is a
declarative ratchet, not a bug fix** — it makes the intent readable at the
setting and removes documentation that has quietly become false. It is worth
doing for that reason and should not be sold as more.

**Suppressions can go stale.** There are 23 `#[allow]` attributes in production
`src/`, roughly ten of which carry no `reason`. `#[expect]` (stable since 1.81)
warns when the suppressed lint *stops* firing, so a suppression that outlives its
cause becomes visible instead of accumulating silently.

## Objective

- **Crate metadata.** Add `description` (all six), plus `keywords`, `categories`,
  `documentation`, and `readme` where meaningful. Set `publish = false` on the
  crates that are not intended for release — decide this per crate and state the
  reasoning in the manifest comment, in the style the existing manifests use.
- **Per-crate `README.md` + `#![doc = include_str!("../README.md")]`**
  (`doc-crate-readme`). Each `lib.rs` already carries a substantial hand-written
  `//!` header (`crates/core/src/lib.rs` alone is ~280 lines of module index).
  Reconcile rather than duplicate: the README carries the orientation a crates.io
  visitor needs, the `//!` header keeps the module index. Do not let the two
  drift — that is what `include_str!` is for.
- **Lint ratchet.** Promote `missing_docs` and `clippy::missing_errors_doc` from
  `warn` to `deny` in **both** `lints.toml` and `[workspace.lints]` (they are a
  documented pair and must not diverge), and rewrite the two now-false rationale
  rows in `docs/lint-policy.md` to state the current reason.
- **`#[allow]` → `#[expect(…, reason = "…")]`** across the production sites, each
  with a reason. Where `#[expect]` immediately fires "this lint is not fulfilled",
  that suppression was already stale — delete it rather than convert it. Report
  how many were stale.
- **Naming.** `crates/core/src/assembly.rs:285-303` carries the workspace's only
  naming violation (`name-no-get-prefix`): `get_content_hash`, `get_size_bytes`,
  `get_scheme`, `get_produced_at_offset_ns` on `DurableReferenceMeta`. Drop the
  `get_` prefix. This is a public rename on the authoring API — the crate is at
  version `0.0.0` with no released semver commitment and these are the only four
  in 37k lines, so it is taken now rather than becoming permanent. **Flag it
  prominently in the PR description.**
- **Doctests that actually run.** The audit found the real gap is not *count* but
  *execution*: in `dagr-cli`, **8 of 9** doc examples are ```` ```no_run ```` or
  ```` ```ignore ```` — including the primary user-facing ones
  (`registry.rs:101,207`, `run.rs:15,78,107`, `structure_snapshot.rs:62`, and
  `flow_builder.rs:122,214` which are not even compiled). Only
  `full_pipeline.rs:86` is executed by `cargo test --doc`. An `ignore`d example is
  documentation that cannot rot loudly. Convert what can run to a real doctest;
  where an example genuinely cannot execute in a test harness, downgrade `ignore`
  → `no_run` so it is at least compile-checked, and say why at the fence.
  `dagr-metastore` has **zero** examples on `MetaStore::open` / `with_write_txn`;
  add one. Use `?` rather than `.unwrap()` in every example added or touched
  (`doc-question-mark`) — there are four executed doctests using `.unwrap()`
  today (`crates/core/src/test_kit.rs:69`, `crates/core/src/context.rs:371,480,481`).
  This is deliberately *not* a blanket sweep: arch.md's "runnable examples
  covering each layer" is already served by the nine programs in
  `crates/cli/examples/`.
- **Fill the `Debug` gaps** (`api-common-traits`). `Handle<T>`
  (`crates/core/src/handle.rs:169`), and `Slot<T>` / `SlotRef<T>` /
  `ConsumerLease<T>` / `RedemptionHandle<T>` (`crates/core/src/slot.rs:331,490,566,688`)
  implement **no `Debug` at all**, so none can appear in a `{:?}` diagnostic. The
  same module family already shows the right pattern: `Permit` and
  `ResidencyLease` (`crates/core/src/admission.rs:880,934`) hand-write `Debug`
  with `finish_non_exhaustive()` to omit an unprintable back-reference. Apply that
  pattern here. `EventStreamWriter` and `SingleTaskTest` are lesser cases of the
  same gap.
- **Add the freely-derivable traits** on plain-data types that lack them:
  `LogSpan` (`context.rs:309`, whose three fields are all `Copy + Eq + Hash`),
  `ScratchStore` (`scratch.rs:203`), `ContainerLimitProbe` (`limits.rs:232`),
  `SchemaValidationError` (`schema.rs:152`), `ReadError`, `FoldError`. Types that
  carry `Box<dyn Error>` or `io::Error` correctly cannot derive `Clone`/`PartialEq`
  and stay as they are — the split is structural, and only these six are
  unjustified.
- **Additive conversions** (`conv-tryfrom-fallible`, `conv-fromstr-parsing`,
  `api-from-not-into`). Add `impl From<CostVector> for PoolCost` alongside
  `PoolCost::from_cost_vector` (`crates/core/src/admission.rs:232`), and
  `TryFrom<&str>` alongside `GraphArtifact::from_json_str`
  (`crates/render/src/lib.rs:105`) and `RunArtifact::from_json_str`
  (`crates/render/src/overlay.rs:138`). **Additive only** — the inherent methods
  stay, so no call site changes and no API is redesigned.
  `NodeId::from_name` is deliberately *not* included: its doc comment argues
  explicitly against a `From` impl, because identity-minting must not read as an
  implicit drive-by conversion. Record that as a correct decline, not an oversight.

## Test plan (write these first — TDD)

**Metadata**
- Given each member manifest, then `description` is non-empty; where
  `publish = false` is absent, the crate carries the full metadata set a
  crates.io release needs. Assert this mechanically — a shell check in the style
  of `scripts/check-hygiene.sh`, so the next crate added cannot skip it.
- Given `cargo package --list` (or an equivalent dry run) for each publishable
  crate, then it does not fail for missing required metadata.

**README / rustdoc unification**
- Given each crate with a README, then `cargo doc` renders it as the crate root
  documentation and the rustdoc job stays green with `-D warnings` (a README with
  a broken intra-doc link now fails the docs build — that is the point).
- Given each per-crate README, then it states what the crate is and its place in
  the dependency direction, consistent with the top-level README's architecture
  section.

**Lint ratchet**
- Given the promoted levels, then `cargo clippy --workspace --all-targets -- -D warnings`
  is green with no new suppressions added to achieve it. If a promotion requires
  adding an `#[allow]`, the promotion is wrong — report it instead.
- Given `lints.toml` and `[workspace.lints]`, then the two agree field for field.
  Assert mechanically; they are a documented pair and have no other guard.

**`#[expect]` migration**
- Given the migrated attributes, then the build is green, every one carries a
  `reason`, and the count of stale suppressions removed is reported in the PR.

**Naming, traits, and doctests**
- Given the renamed accessors, then all call sites and the metastore lineage
  projection that reads them still compile and pass.
- Given the added examples, then `cargo test --doc --workspace` passes, the count
  of *executed* doctests in `dagr-cli` is reported before and after, and no
  example anywhere uses `.unwrap()`.
- Given `Handle<T>`, `Slot<T>`, `SlotRef<T>`, `ConsumerLease<T>`, and
  `RedemptionHandle<T>`, then each formats under `{:?}` without exposing an
  unprintable internal, matching the `Permit`/`ResidencyLease` precedent.
- Given the added `From`/`TryFrom` impls, then they agree with the inherent
  methods they parallel on the same input — assert both paths against one fixture,
  so they cannot drift.

## Definition of done

- [ ] All six manifests declare `description`; publishable crates carry the full metadata set; `publish = false` is set where release is not intended, with the reasoning in a manifest comment.
- [ ] A mechanical check asserts the metadata invariant for every current and future member.
- [ ] Each crate has a `README.md` inlined via `#![doc = include_str!("../README.md")]`, reconciled with its existing `//!` header; the rustdoc job is green.
- [ ] `missing_docs` and `clippy::missing_errors_doc` are `deny` in both `lints.toml` and `[workspace.lints]`; a check asserts the two files agree; `docs/lint-policy.md`'s two stale rationale rows are rewritten to the current reason.
- [ ] Production `#[allow]` attributes are `#[expect(…, reason = "…")]`, stale ones deleted, count reported.
- [ ] The four `get_`-prefixed accessors on `DurableReferenceMeta` are renamed, flagged in the PR description.
- [ ] `dagr-cli`'s doc examples are executed where they can be and `no_run` (never `ignore`) where they cannot, with the reason at the fence; `dagr-metastore` gains at least one example; the executed-doctest count is reported before and after.
- [ ] `Handle<T>`, `Slot<T>`, `SlotRef<T>`, `ConsumerLease<T>`, `RedemptionHandle<T>` implement `Debug` following the `Permit`/`ResidencyLease` `finish_non_exhaustive()` precedent.
- [ ] `LogSpan`, `ScratchStore`, `ContainerLimitProbe`, `SchemaValidationError`, `ReadError`, `FoldError` derive the traits their fields freely allow.
- [ ] `From<CostVector> for PoolCost` and `TryFrom<&str>` for both artifact readers exist alongside (not replacing) the inherent methods, each tested against the same fixture as its inherent twin.
- [ ] `cargo test --doc --workspace` passes; no doctest uses `.unwrap()`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

Whether `dagr-macros` and `dagr-metastore` should be `publish = false`. Both are
reachable only through feature-gated edges and neither is independently useful,
but a published `dagr-core` with the `macros` feature on requires a published
`dagr-macros`. Decide in the PR and record the reasoning; the answer changes only
which crates need the full metadata set.

## Out of scope

- A blanket doctest sweep across every public item. Scoped above, and recorded in
  the register as partially adopted with that reason.
- `#![doc = include_str!]` for the binary crates' `main.rs` — the rule targets
  library crate roots.
- Any change to `docs/coverage-matrix.md`; M9 adds no arch.md acceptance criterion.
