# 113 · T98 — CI coverage and test-isolation hygiene

> **Milestone:** M9 · **Size:** M · **Type:** feature (tests) · **Components:** C28, system-level
> **Branch:** `feat/t98-ci-and-test-hygiene` · **Depends on:** T96 · **Blocks:** T99

## Why / context

Two families of gap, both about checks that exist but are not *wired*.

**CI does not cover the feature matrix.** The workspace's features are the
mechanism behind three architectural guarantees — `dagr-core` stays
zero-runtime-dependency under `--no-default-features`, `metastore` is default-off,
`dag` confines `inventory` to `dagr-cli`. Yet no CI job runs
`--no-default-features` or `--all-features` across the workspace; the only
coverage is inside two metastore-specific shell scripts. `proj-feature-additive`
wants features proved additive, and today a feature combination could break
without any job noticing. Relatedly, **16 of the 20 `scripts/check-*.sh`
invariant checkers never run in CI** — they were written as ticket-time gates and
are now dormant, so the structural invariants they encode (the workspace crate
graph, repo hygiene, the ADR content contracts) are unguarded against drift.

**Test isolation is inconsistent.** `crates/core/tests/scratch_survives_restart.rs:63`
has the right pattern — a `TempBase::new(tag)` helper combining
`std::env::temp_dir()`, the process id, an atomic counter, and a nanosecond
timestamp. Much of `crates/cli/tests/` instead hardcodes a shared literal base
path reused across every test in the file: `crates/cli/tests/run_loop_driver.rs`
uses `/tmp/dagr-test` across **15** separate tests, and the same shape appears in
`env_fallbacks_and_headroom.rs`, `os_signals_flush_and_cleanup.rs`,
`execution_class_dispatch.rs`, `cancellation_core_and_drain.rs`, and a dozen more
files. Collisions are avoided today only because the run store namespaces by
`<base>/<pipeline>/<run-id>` — an *implicit* invariant, holding as long as no two
tests pick the same pipeline name. This repo has already paid for exactly this
class of flake once, in the T35 cancellation-ordering investigation; leaving the
implicit version in place invites the recurrence.

## Objective

- **Feature-matrix job.** Add a CI job running `cargo build --workspace --no-default-features`,
  `cargo build --workspace --all-features`, and `cargo test --workspace --all-features`.
  The no-default leg is the standing proof of `dagr-core`'s zero-dependency
  guarantee — today that claim rests on two shell scripts scoped to the metastore.
- **Wire the dormant checkers.** Run all 20 `scripts/check-*.sh` in CI. If a
  checker no longer passes, that is a finding: either the invariant drifted (fix
  it) or the checker outlived its ticket (delete it deliberately, with the reason
  in the PR). Do not wire a checker by weakening it.
  **Two are already known-broken, verified against clean `main`:**
  `scripts/check-hygiene.sh` reports `HYGIENE=FAIL` on
  `FAIL test8: crate artifact leaked in: ./Cargo.toml` — a false positive, since
  it greps for a leaked crate name and the root manifest legitimately discusses
  `crates/artifact` in its dependency-direction comment. And
  `scripts/check-coverage-matrix.sh` takes over two minutes locally (it resolves
  test ids against the suite), so wiring it needs either a faster resolution path
  or its own job rather than being bolted onto an existing one. Both are precisely
  the drift that lets a dormant checker rot — fix the predicate in the first case,
  do not relax the invariant.
- **`unexpected_cfgs` + `check-cfg`** (`lint-cfg-check`). No custom cfg names
  exist today, so this changes nothing now — which is the point: it is insurance
  against a future `#[cfg(feature = "metastor")]` typo compiling silently into
  dead code. Cheap, and only cheap before it is needed.
- **Miri.** Add a `cargo miri test` job scoped to the crates containing `unsafe`.
  The workspace's only production `unsafe` is the `GlobalAlloc` in
  `crates/core/src/metrics.rs`, which miri **cannot** meaningfully exercise (it
  supplies its own allocator). So: run miri over the crates whose *tests* can
  benefit and record honestly in the register that the allocator itself is out of
  miri's reach, rather than claiming `unsafe-miri-ci` is satisfied when it is not.
  If miri cannot run usefully at all here, record that verdict with its reason
  instead of adding a job that proves nothing.
- **Unify the temp-directory idiom.** Promote `core`'s `TempBase` pattern into a
  shared test-support helper and adopt it across `crates/cli/tests/`, so isolation
  is structural rather than dependent on distinct pipeline names. Prefer promoting
  the existing in-repo helper to adding a `tempfile` dev-dependency — but if
  `tempfile` turns out materially better, it is dev-only and permitted under M9's
  dependency rule with a `deny.toml` justification. Decide in the PR.
- **Fix the dangling references.** `.github/workflows/ci.yml:9` and
  `docs/coverage-matrix.md:47` both cite a `docs/quality-gates.md` and
  `scripts/run_gate.sh` that **do not exist**. Either write them or drop the
  citations; a merge-gate document that references a phantom gate is worse than
  one that does not mention it.
- **Housekeeping.** Delete the leftover empty `layer-c-runs/` run-output tree, and
  bring the 30 trybuild fixtures under `crates/core/tests/ui/` onto the `//!`
  header convention that the otherwise-identical fixture directories in
  `crates/cli/tests/ui/` and `crates/macros/tests/expand/` already follow.

## Test plan (write these first — TDD)

**Feature matrix**
- Given `cargo build --workspace --no-default-features`, then it succeeds and
  `dagr-core` resolves with **no** dependency edge onto `dagr-macros`,
  `inventory`, `libsql`, or `dagr-metastore`. Assert the resolved graph, not just
  that the build passed.
- Given `cargo test --workspace --all-features`, then it passes — including the
  `metastore` + `schema-validation` + `dag` combination, which no job runs today.

**Dormant checkers**
- Given each of the 20 `scripts/check-*.sh`, when run from a clean checkout, then
  it passes. Report any that had to be repaired or retired, and why.

**Test isolation**
- Given the whole `dagr-cli` test suite run with high parallelism, then no two
  tests share a filesystem path. Prove it by construction — every base path
  contains the pid and a per-test unique component — rather than by observing that
  a run happened to pass.
- Given a repeated local run, then tests do not accumulate stale directories, or
  the cleanup story is documented.

**Cfg typo insurance**
- Given a deliberately misspelled `#[cfg(feature = "…")]` in a scratch commit,
  then the build warns. Verify the guard actually bites before relying on it.

## Definition of done

- [ ] A feature-matrix CI job runs `--no-default-features` and `--all-features` across the workspace, asserting the resolved dependency graph for the no-default leg.
- [ ] All 20 `scripts/check-*.sh` run in CI; any repaired or retired checker is called out with its reason. `check-hygiene.sh`'s `test8` false positive is fixed (by correcting the predicate, not by relaxing the invariant) and `check-coverage-matrix.sh`'s runtime is addressed.
- [ ] `unexpected_cfgs` + `check-cfg` are configured, and the guard is verified to fire on a deliberate typo.
- [ ] A miri job exists scoped to where miri can help, **or** the register records why miri cannot usefully run here — not a job that proves nothing.
- [ ] `crates/cli/tests/` uses a shared unique-temp-base helper; isolation is structural, not dependent on distinct pipeline names.
- [ ] The `docs/quality-gates.md` / `scripts/run_gate.sh` references are resolved (written or removed).
- [ ] `layer-c-runs/` is deleted; `crates/core/tests/ui/*.rs` use `//!` headers.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

Whether every one of the 16 dormant checkers still *should* run. Several are ADR
content checks that assert a decision record contains particular sections — those
have no drift risk once merged and arguably belong in the ticket's own gate, not
in every CI run. The PR proposes a split (standing invariants vs one-time
acceptance) and records the reasoning; the default is to wire them all and only
carve out with a stated reason.

## Out of scope

- `loom` model-checking for the hand-rolled `AdmissionController`
  (`test-loom-concurrency`). It is a genuine fit — a `std::sync::Mutex`-guarded
  concurrency structure is exactly loom's target — but adopting it means
  restructuring the type to use loom's primitives under `cfg(loom)`, which is a
  design change, not hardening. Record as `declined — needs a cfg(loom) port`,
  with the recommendation that it be revisited on its own.
- Code-coverage instrumentation (`llvm-cov`/`tarpaulin`). The repo's
  `coverage-matrix` job verifies *acceptance-criterion* coverage, which is a
  stronger and more deliberate guarantee than line coverage; adding a line-coverage
  number invites optimizing for it.
- Converting the hand-rolled golden-file harness to `insta`. The existing
  checked-in goldens plus the `bless` helper in `structure_snapshot.rs` are
  functionally equivalent and dependency-free. Record as `declined`.
