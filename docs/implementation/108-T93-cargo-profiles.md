# 108 · T93 — Cargo release/bench/dev profiles

> **Milestone:** M9 · **Size:** S · **Type:** setup · **Components:** system-level, Performance envelope
> **Branch:** `chore/t93-cargo-profiles` · **Depends on:** T92 · **Blocks:** T94

## Why / context

The workspace defines **no `[profile.*]` section anywhere** — not in the root
manifest, not in any member, and there is no `.cargo/config.toml`. Every build
runs at Cargo's defaults, which means release builds ship with `lto = false` and
`codegen-units = 16`: the cross-crate inlining that a six-crate workspace with a
hot per-node scheduling path most benefits from is simply switched off
(`opt-lto-release`, `opt-codegen-units`, `perf-release-profile`). arch.md's
"Performance envelope" budgets framework overhead per node at **under one
millisecond** and holds it with a CI benchmark; that budget is currently being
met without the compiler settings the budget assumes.

Two deviations from the skill's recommended profile are load-bearing and are the
reason this is a ticket rather than a one-line edit:

- **`panic = "abort"` must not be set.** `crates/core/src/execution.rs` refuses to
  start a run under it (`check_panic_strategy`: *"a task panic would abort the
  whole process uncontained"*), because dagr contains task panics with
  `catch_unwind` and reports them as attempt outcomes. Setting it would turn every
  task panic into a process abort and every run into a bootstrap refusal.
- **`strip` must stay off.** dagr's pitch is explaining a run after the fact, and
  it attributes panics through a panic hook; stripping symbols trades that away
  for binary size dagr does not compete on.

## Objective

Add the profiles, with the two deviations commented in place so a future reader
sees the reason at the setting rather than in a changelog.

- `[profile.release]`: `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, and an
  **explicit `panic = "unwind"`** carrying a comment naming `check_panic_strategy`
  — explicit rather than defaulted, so a consumer inheriting these settings cannot
  silently flip it.
- `[profile.bench]`: `inherits = "release"`, `debug = true`, `strip = false` — a
  profiled binary needs symbols.
- `[profile.dev.package."*"]`: `opt-level = 3` — optimize *dependencies* in dev
  and test builds while leaving first-party crates unoptimized and fast to
  rebuild.
- Record the `strip` and `panic` deviations in T92's register against
  `perf-release-profile` / `opt-lto-release`.

The last item has a measurable consequence and is why the test plan leads with
it: the scale benchmark runs under `cargo test`, i.e. the **dev** profile. Making
dependencies optimized changes what that benchmark measures.

## Test plan (write these first — TDD)

**The performance budget still holds**
- Given the new profiles, when `cargo test -p dagr-cli --test scale_benchmark`
  runs, then the 1000-node no-op graph still meets its per-node budget. Record
  the before and after per-node figures in the PR description — the budget is a
  ceiling, and a change in either direction is information the reviewer needs.

**Determinism is untouched**
- Given the new profiles, when the `reference_pipeline_artifact` and
  `fingerprint_fixture` examples run under the pinned toolchain and under
  `+stable`, then both still produce byte-identical output across toolchains.
  Compiler settings must not reach emitted artifacts; if they do, that is a bug
  this ticket surfaced, not a reason to skip the profile.

**The panic-containment guarantee survives**
- Given a release-profile build, when a task panics, then it is still contained
  and reported as a panicked attempt rather than aborting the process — the
  existing panic-containment tests pass under `--release`.
- Given the workspace manifest, then no profile anywhere sets `panic = "abort"`;
  assert this mechanically so a later edit cannot reintroduce it silently.

## Definition of done

- [ ] `[profile.release]`, `[profile.bench]`, and `[profile.dev.package."*"]` exist in the root manifest with the values above.
- [ ] `panic = "unwind"` is set explicitly in `[profile.release]` and commented with the `check_panic_strategy` rationale; a mechanical check asserts no profile sets `panic = "abort"`.
- [ ] `strip` is not enabled anywhere; the deviation from the skill's recommendation is recorded in `docs/rust-skills-register.md`.
- [ ] The scale benchmark passes, with before/after per-node figures in the PR description.
- [ ] Both determinism jobs still byte-match across toolchains.
- [ ] Panic-containment tests pass under `--release`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

None. `lto = "fat"` over `"thin"` is chosen deliberately: this is a leaf binary
built rarely and run repeatedly, so compile time is the cheap side of the trade.
If CI wall-clock becomes the binding constraint, `"thin"` is the documented
fallback — a follow-up, not a blocker.

## Out of scope

- PGO and `target-cpu=native` (`opt-pgo-profile`, `opt-target-cpu`). dagr ships
  portable container builds; pinning to a build host's microarchitecture would
  trade a supported deployment story for single-digit percentages. Record both as
  `declined` in the register.
- Any source-level optimization. `perf-profile-first` applies: no hot-path edit
  lands in this ticket without a profile showing it matters.
