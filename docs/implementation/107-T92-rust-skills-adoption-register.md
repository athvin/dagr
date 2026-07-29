# 107 · T92 — rust-skills adoption register (decision)

> **Milestone:** M9 · **Size:** S · **Type:** decision · **Components:** system-level
> **Branch:** `adr/t92-rust-skills-adoption-register` · **Depends on:** — · **Blocks:** T93, T94, T95, T96, T97, T98, T99

## Why / context

The repo carries an agent skill at `.claude/skills/rust-skills` — **265 Rust
best-practice rules across 26 categories**, written against Rust 1.96 / edition
2024. An audit against dagr found the codebase already meets most of that bar
(clippy `all` + `pedantic` denied workspace-wide, zero crate-level `#![allow]`
escapes, hand-written error types with real `source()` chains, 223
`BTreeMap`/`BTreeSet` uses against 8 `HashMap` — all lookup tables never iterated
into output, 11 `as` casts in all of production, zero files missing `//!` docs).

The risk is therefore **not** under-application; it is *blind* application. Several
rules are actively wrong for dagr: `err-thiserror-lib` would add a runtime
dependency to `dagr-core`, whose zero-runtime-dependency guarantee is an
architectural commitment (arch.md "Stability", ADR 081/082); `perf-release-profile`
recommends `panic = "abort"`, which `execution::check_panic_strategy` *refuses to
run under* because panic containment needs unwinding; `perf-ahash` would change a
hash iteration order the determinism jobs byte-diff.

Without a record, every future contributor re-derives these conclusions — or
worse, "fixes" one. This ticket writes the disposition down once, in a file CI
verifies, exactly as `docs/coverage-matrix.md` pins acceptance-criteria coverage.

## Objective

Record the disposition of every rule and make the record machine-checked.

- Commit `.claude/skills/rust-skills/` (currently untracked). The register
  references rule ids by path; an untracked source makes the check unrunnable on
  a fresh clone and the milestone unreviewable.
- Author `docs/rust-skills-register.md`: one row per rule id, with a `Disposition`
  of exactly one of `satisfied` / `adopt` / `n-a` / `declined`, the owning M9
  ticket where `adopt`, and a one-line **reason** for every `n-a` and `declined`
  (no bare verdicts — the reason is the whole point of the file).
- Author `scripts/check-rust-skills-adoption.sh`, which fails when: a rule id in
  `.claude/skills/rust-skills/rules/*.md` is **absent** from the register; a rule
  id appears on **more than one** row; a row names a rule id that **does not
  exist** (a dangling reference); an `n-a`/`declined` row carries **no reason**;
  or an `adopt` row names a ticket id that is not an M9 ticket.
- Wire the script into `.github/workflows/ci.yml` as its own job.

The four dispositions carry fixed meanings, stated in the register's header:
`satisfied` — dagr already complies, no work; `adopt` — a named M9 ticket applies
it; `n-a` — structurally inapplicable (no such construct, or an architectural
invariant forbids it); `declined` — applicable but deliberately not taken, with
the trade-off named.

## Test plan (write these first — TDD)

**Verifier correctness (fixture-driven, mirroring `check-coverage-verifier-selftest.sh`)**
- Given a fixture register missing one rule id present in `rules/`, when the
  verifier runs, then it fails naming that id.
- Given a fixture register listing one rule id on two rows, then it fails naming
  the duplicate.
- Given a fixture register naming a rule id with no corresponding `rules/*.md`,
  then it fails naming the dangling id.
- Given a fixture row dispositioned `n-a` or `declined` with an empty reason,
  then it fails naming that row.
- Given a fixture row dispositioned `adopt` naming a ticket outside M9, then it
  fails naming that row.
- Given a complete, well-formed fixture register, then it passes silently.

**The real register**
- Given the checked-in `docs/rust-skills-register.md`, when the verifier runs
  against the real `rules/` directory, then it passes and the row count equals
  the rule-file count (265 at time of writing — the verifier derives the count,
  never hard-codes it).
- Given the register, then every rule whose disposition is `adopt` names a ticket
  that exists under `docs/implementation/`.

## Definition of done

- [ ] `.claude/skills/rust-skills/` is committed, so the verifier runs on a fresh clone.
- [ ] `docs/rust-skills-register.md` covers every rule id exactly once, with a reason on every `n-a` and `declined` row.
- [ ] The four architectural non-adoptions are recorded with their reasons: `err-thiserror-lib`/`err-anyhow-app`/`obs-tracing-over-log`/`mem-smallvec`/`perf-ahash` as `n-a` for `dagr-core` (zero-runtime-dependency, ADR 081/082); `panic = "abort"` within `perf-release-profile`/`opt-lto-release` as `n-a` (refused by `execution::check_panic_strategy`); `test-criterion-bench` and `test-proptest-properties` as `declined` (the scale budget and the termination property are deliberately *deterministic* harnesses); `proj-mod-by-feature` as `declined` (file splitting is an API/structure refactor, out of scope for M9).
- [ ] Rules requiring edition 2024 are dispositioned against the post-T94 state, not the current one.
- [ ] `scripts/check-rust-skills-adoption.sh` implements all five failure modes and has fixture self-tests covering each.
- [ ] The verifier runs as a CI job and is green.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

None. The disposition vocabulary is fixed above; individual verdicts are the
reviewable content of the PR.

## Out of scope

- Applying any rule. This ticket records intent; T93–T98 do the work.
- Amending `docs/lint-policy.md` or `lints.toml` — that is T96, which owns the
  lint-policy pair.
- Any change to `docs/coverage-matrix.md`. M9 adds no arch.md acceptance
  criterion, so the partition and the coverage matrix are untouched.
