# 109 · T94 — edition 2024 migration and MSRV bump

> **Milestone:** M9 · **Size:** L · **Type:** setup · **Components:** system-level, Stability
> **Branch:** `chore/t94-edition-2024-and-msrv-bump` · **Depends on:** T93 · **Blocks:** T95, T96, T97, T98

## Why / context

The workspace is pinned to **Rust 1.95.0 / edition 2021**. The rust-skills guidance
targets **edition 2024**, and a meaningful set of its rules is unreachable until
the edition moves: `pat-if-let-chains` (let-chains are edition-gated),
`unsafe-extern-block`, `unsafe-no-mangle-unsafe`, and the MSRV-aware dependency
resolver (`resolver = "3"`, `proj-msrv-declare`). Edition 2024 also makes
`unsafe_op_in_unsafe_fn` deny-by-default, which is the compiler enforcing
`unsafe-minimize-scope` — a rule dagr wants and currently satisfies only by
convention.

Raising the MSRV is not a side effect here; arch.md "Stability" makes it a minor
version bump that updates the manifest, the toolchain pin, and the README
**together**, called out in release notes. This ticket is that event.

One consequence to state plainly, because it is a real commitment and not an
oversight: `rust-toolchain.toml`'s pinned channel *is* dagr's MSRV by design (the
pin is what makes trybuild `.stderr` snapshots byte-reproducible across machines).
Moving the pin to the newest stable therefore sets the declared minimum to the
newest stable. That is consistent with how this repo has always worked, and it is
recorded here so the next MSRV conversation starts from it.

## Objective

Move the workspace to edition 2024 on the newest stable toolchain, in one
reviewable change.

- **Six pin sites must move together**, or the consistency checks fail:
  `Cargo.toml` (`edition`, `rust-version`, and `resolver = "3"`),
  `rust-toolchain.toml` (`channel`), `rustfmt.toml` (`edition`), `README.md`
  ("MSRV" line), `scripts/check-stability-and-criteria.sh` (asserts a single
  concrete MSRV is named), and the toolchain reference in
  `crates/core/tests/ui.rs`. The stale `1.95.0` mentions in `.github/workflows/ci.yml`
  comments move too.
- **Migrate the `unsafe` surface.** `unsafe_op_in_unsafe_fn` is deny-by-default in
  2024: the `GlobalAlloc` implementation in `crates/core/src/metrics.rs` and the two
  test allocators (`crates/core/tests/node_metrics.rs`,
  `crates/cli/tests/bounded_memory_chain.rs`) need each unsafe operation wrapped in
  its own `unsafe { }` block with a `// SAFETY:` comment stating why *that
  operation* is sound — not one blanket block per function
  (`unsafe-minimize-scope`, `unsafe-safety-comment`).
- **`gen` is a reserved keyword in 2024.** Rename the parameter at
  `crates/cli/tests/fingerprint_artifact.rs` (rename it meaningfully; do not
  reach for `r#gen`).
- **Review the RPIT sites.** Edition 2024 changes what lifetimes `-> impl Trait`
  captures. Walk each `-> impl ...` return in the workspace and confirm the new
  capture rules do not widen a returned type's borrow beyond what the caller
  expects; add `+ use<>` bounds where the old behaviour was the intended one.
- **Regenerate and review the trybuild snapshots.** There are **47** `.stderr`
  files across `crates/core/tests/ui/`, `crates/cli/tests/ui/`, and
  `crates/macros/`. Regenerating them is mechanical; **reviewing** them is not,
  and is the substance of this ticket — see the test plan.

## Test plan (write these first — TDD)

**Snapshot review is the gate, not the regeneration**
- Given each of the 47 regenerated `.stderr` snapshots, when its diff is
  inspected, then the *diagnostic being pinned is still the same diagnostic*.
  A snapshot whose wording changed is a rebase; a snapshot that now reports a
  **different error**, reports it at a **different span**, or that newly
  *compiles* is a **regression** and blocks the ticket. The PR description
  classifies every one of the 47 as wording-only or behaviour-changed, and
  behaviour-changed count must be zero at merge.
- Given `cargo test -p dagr-core --test ui` and the `dagr-cli` / `dagr-macros`
  trybuild corpora, then all pass under the new pinned toolchain.

**Determinism across the edition boundary**
- Given the migrated workspace, when `reference_pipeline_artifact` and
  `fingerprint_fixture` run under the new pinned toolchain and under `+stable`,
  then output is byte-identical across toolchains **and** the structural
  fingerprint is unchanged from before the migration. A fingerprint change would
  mean the edition altered a hashed input, which must be understood before merge,
  not absorbed.

**Unsafe migration**
- Given the migrated allocators, then every unsafe operation sits in its own
  `unsafe { }` block with a `// SAFETY:` comment, and the allocator-attribution
  tests (`node_metrics`, `bounded_memory_chain`) still pass — the accounting
  behaviour is unchanged by the migration.

**Pin consistency**
- Given the six pin sites, when `scripts/check-stability-and-criteria.sh` runs,
  then it passes, and no site still names the old version. Assert this
  mechanically across the repo (excluding `docs/implementation/*.md`, which are
  historical records of what was true when each ticket shipped and are **not**
  rewritten).

**Cross-platform**
- Tests pass on `ubuntu-latest` and `macos-latest`.

## Definition of done

- [ ] All six pin sites name the new toolchain/edition consistently; `resolver = "3"` is set; `scripts/check-stability-and-criteria.sh` passes.
- [ ] `docs/implementation/*.md` historical tickets are **not** rewritten to the new version.
- [ ] The `GlobalAlloc` impl and both test allocators use per-operation `unsafe { }` blocks with `// SAFETY:` comments; allocator-attribution tests pass.
- [ ] The `gen` identifier is renamed; every RPIT site has been reviewed and any needed `use<>` bound added.
- [ ] All 47 trybuild snapshots regenerated, each classified wording-only vs behaviour-changed in the PR, with zero behaviour-changed at merge.
- [ ] Determinism jobs byte-match across toolchains and the structural fingerprint is unchanged from pre-migration.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] The README MSRV section states the new minimum and the release-notes implication (arch.md "Stability": raising the MSRV is a minor version bump).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

None blocking. If a trybuild snapshot turns out to be **behaviour**-changed —
most plausibly one of the `task_non_send_capture` or arity-mismatch fixtures,
where 2024's capture and match-ergonomics changes could alter which error the
compiler reaches first — that is a finding to report and resolve in this ticket,
not to absorb into the snapshot.

### Resolutions recorded at implementation (2026-07-29)

The migration surfaced four decisions the ticket did not pre-answer. Each is
resolved here rather than left implicit in a diff.

1. **The snapshot outcome.** Nothing was behaviour-changed and nothing needed
   regenerating: **47** `.stderr` files exist on disk, of which **44** are the
   tracked, reviewable corpus (30 `crates/core/tests/ui/`, 4
   `crates/cli/tests/ui/flow_builder/fail/`, 10
   `crates/macros/tests/expand/fail/`) and **3** are stale, git-ignored
   `crates/macros/wip/` trybuild scratch left by a run in July — not corpus, and
   untouched by this ticket. All 44 pass **unchanged** under 1.97.1 / edition
   2024: the 14 trybuild snapshots match byte-exactly (and the generated trybuild
   project reports `edition = "2024"`, so the corpus really is exercised under the
   new edition), and all 30 core substring snapshots are satisfied by the current
   diagnostics. Classification: **44 reviewed / 44 wording-only-or-unchanged / 0
   behaviour-changed.** No fixture newly compiles — both harnesses hard-assert
   compile failure, so a lost compile-time guarantee could not pass silently.
2. **`DAGR_BLESS` was deliberately not used on the core corpus.** Its blessing
   path derives a snapshot's substring list from the first ``expected `` `` /
   ``found `` `` markers in the diagnostic, which only exist for `E0308`. Run, it
   *panics* on the first sample (a trait-bound error with no such markers) rather
   than writing anything — confirmed, no file changed. For this corpus
   "regenerate" is not a meaningful operation: the substring lists are
   hand-curated, blessing would narrow them, and the frozen check is the real
   gate. The frozen check passes.
3. **`scripts/check-stability-and-criteria.sh` could not take the new literal.**
   Its MSRV assertion is scoped to the ADR embedded in ticket 005 — a historical
   record this ticket's DoD forbids rewriting — so a new hardcoded version would
   have forced that rewrite. The assertion is now *structural* (the ADR names
   exactly **one** concrete version, which is what T0.10's test plan asked:
   "stated and singular"), plus two new assertions on the **live** pin read from
   `rust-toolchain.toml`. No version literal remains in the script to go stale.
4. **`clippy::collapsible_if` is moved to `allow`.** Under edition 2024 its fix
   for `if let A { if let B { … } }` *is* a let-chain, so denying it would force
   let-chain adoption at 25 sites — precisely what Out of scope defers. Recorded
   in `lints.toml`, the manifest, and `docs/lint-policy.md`'s exception table;
   re-denying that one lint is how the adopting ticket finds its work list. Chosen
   over 25 scattered `#[allow]`s (one reviewable decision, not ~125 lines of
   boilerplate) and over adopting let-chains (out of scope).

Two smaller findings, for the record: the ticket names "two test allocators", but
`crates/core/tests/node_metrics.rs` has no `unsafe fn` of its own — it installs
`dagr-core`'s allocator, so the only two `GlobalAlloc` impls needing migration are
in `crates/core/src/metrics.rs` and `crates/cli/tests/bounded_memory_chain.rs`.
And the unsafe surface was **larger** than the ticket anticipated: edition 2024
also makes `std::env::set_var`/`remove_var` `unsafe fn`s, adding 44 call sites
across four files.

## Out of scope

- Adopting let-chains, `unsafe extern` blocks, or `#[unsafe(no_mangle)]` at call
  sites. This ticket makes them *reachable*; using them is ordinary future work,
  and the register records them as available-from-T94 rather than adopted.
- Any behavioural change to the engine. This is a migration: if a test's meaning
  changes, that is a defect to investigate, never a snapshot to bless.
