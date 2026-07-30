# 091 · T77 — Wire DAGR_* env fallbacks into RunConfig + expose headroom

> **Milestone:** M5 · **Size:** M · **Type:** feature · **Components:** C12, C26
> **Branch:** `feat/t77-env-fallbacks-headroom` · **Depends on:** T76, T31, T32 · **Blocks:** —

## Why / context
This ticket applies the T76 precedence helper so every runtime knob honours `flag > env > default`, and exposes the headroom fraction that has been hardcoded at `crates/core/src/limits.rs:73` (`HEADROOM_DEFAULT = 0.20`) since C12 shipped, all while keeping `dagr-core` environment-free per ADR 089's load-bearing decision: the CLI parses `DAGR_*` and passes already-parsed values inward, `dagr-core` never reads the environment. Today only the startup banner honours an env var (`DAGR_NO_BANNER`); `RunConfig::new()` (`crates/cli/src/driver.rs`) is infallible and env-free, its `grace`/`teardown_deadline`/`failure_mode` knobs are flag-only, and `ContainerLimitProbe::with_headroom(f64)` (`crates/core/src/limits.rs:286`) exists but no flag or env reaches it. Building on T76's `dagr_cli::config::resolve<T>` helper, its duration/`FailureMode` parsers, and its extended `reserved_flag_names()`, this ticket adds opt-in fallible env-fallback builder methods to `RunConfig`, wires `DAGR_POOL_*` at the CLI pool-pinning layer that feeds `PinnedPools`/`ContainerLimitProbe` (`crates/cli/src/driver.rs`, `crates/core/src/limits.rs`), adds the `--dagr.headroom-fraction` / `DAGR_HEADROOM` knob that reaches `with_headroom`, and documents the whole `DAGR_*` table with a real binary-wiring pattern in the quickstart/example (`crates/cli/examples/quickstart.rs`) and an arch.md C26 subsection. It completes ADR 089 (the wiring half; T76 was the helper half) and leaves no `Blocks`.

## Objective
Apply the T76 helper to give every runtime knob the standard `flag > env > default` behaviour, expose the headroom fraction, and document the env surface — without letting `dagr-core` read the environment.

- Add opt-in, **fallible** `RunConfig` env-fallback builder methods for the three driver knobs — grace, teardown-deadline, failure-mode — each taking the already-parsed flag `Option<T>`, calling T76's `resolve()` against the corresponding `DAGR_*` key, and returning the `Result` so a bad env value surfaces; `RunConfig::new()` stays infallible and env-free.
- Wire `DAGR_POOL_COMPUTE_THREADS` / `DAGR_POOL_BLOCKING_THREADS` / `DAGR_POOL_MEMORY` at the **CLI pool-pinning layer** (`crates/cli/src/driver.rs`): the CLI resolves each with `resolve()`, then passes the parsed capacity into `PinnedPools` / `ContainerLimitProbe`; `crates/core/src/limits.rs` still performs no env read.
- Add `--dagr.headroom-fraction` / `DAGR_HEADROOM` (default `0.20`, validated `0.0..=1.0`), resolved with the helper and passed to `ContainerLimitProbe::with_headroom(f64)`; the existing at-least-one-unit floor is unchanged so extreme fractions never zero a pool.
- Map failure causes to the correct exit code per ADR 089: an unparseable env value → `InvalidUsage`; an out-of-range (validated) value → `BootstrapFailure`; each error names the offending variable.
- Document the `DAGR_*` table alongside `DAGR_NO_BANNER` and a **binary-wiring pattern** in the quickstart/example (`crates/cli/examples/quickstart.rs`) showing how a pipeline binary folds env fallback into `RunConfig` and the pool pins; add an arch.md C26 subsection carrying the same table.

## Test plan (write these first — TDD)

**Precedence per knob**
- Given both `--grace` and `DAGR_GRACE` are set, when the `RunConfig` is built via the env-fallback method, then the flag value wins and the env value is ignored.
- Given only `DAGR_GRACE` is set (no flag), when the config is built, then the parsed env value is used.
- Given neither the flag nor `DAGR_GRACE` is set, when the config is built, then the effective grace is the 10 s default.
- Given the same three cases for teardown-deadline (`DAGR_TEARDOWN_DEADLINE`, default 15 s) and for failure-mode (`DAGR_FAILURE_MODE`, default continue-independent), when each config is built, then flag beats env beats default identically for every knob.

**Pools**
- Given `DAGR_POOL_COMPUTE_THREADS`, `DAGR_POOL_BLOCKING_THREADS`, and `DAGR_POOL_MEMORY` are set and no matching flag, when the CLI pins the pools, then the resulting `PinnedPools` / capacities reflect exactly the env values.
- Given both a `--dagr.pool.compute-threads` flag and `DAGR_POOL_COMPUTE_THREADS`, when the CLI pins the pools, then the flag capacity wins.
- Given any of the `DAGR_POOL_*` variables set, when `ContainerLimitProbe`/`PinnedPools` size the pools, then `crates/core/src/limits.rs` performs no environment read (the env is resolved in `dagr-cli` and passed in as a parsed value).

**Headroom**
- Given `DAGR_HEADROOM=0.5` and no flag, when the pools are sized from a known detected total, then a 50% headroom is applied and every pool still floors at one unit.
- Given `--dagr.headroom-fraction 0.1` together with `DAGR_HEADROOM=0.5`, when the pools are sized, then the flag's 10% headroom wins over the env's 50%.
- Given `DAGR_HEADROOM=1.5` (out of the `0.0..=1.0` range), when the binary bootstraps, then the run fails with a `BootstrapFailure` naming `DAGR_HEADROOM`, before any node executes.

**Loud failure**
- Given `DAGR_GRACE=notaduration`, when the binary bootstraps, then it fails with `InvalidUsage` naming `DAGR_GRACE`, and no attempt is recorded.
- Given a bad env value for any knob, when resolution runs, then the value is never silently ignored or clamped — the run fails with the variable named.

**End-to-end**
- Given a small pipeline run with several `DAGR_*` variables set (grace, failure-mode, a pool pin, and headroom) and no overriding flags, when it runs to completion, then the effective grace/failure-mode/capacities/headroom observed by the run reflect the env values.
- Given the same pipeline run twice — once with the knobs supplied via `DAGR_*` and once via the equivalent flags — when both complete, then the terminal node states and the run's exit code match (the env path and the flag path are behaviourally identical).

## Definition of done
- [ ] `RunConfig` gains opt-in, fallible env-fallback builder methods for grace, teardown-deadline, and failure-mode, each folding the parsed flag `Option<T>` with T76's `resolve()` against the matching `DAGR_*` key and returning a `Result`; `RunConfig::new()` remains infallible and reads no environment.
- [ ] `DAGR_POOL_COMPUTE_THREADS` / `DAGR_POOL_BLOCKING_THREADS` / `DAGR_POOL_MEMORY` are wired at the `dagr-cli` pool-pinning layer, with `crates/core/src/limits.rs` still performing zero env reads (the CLI passes parsed values to `PinnedPools` / `ContainerLimitProbe`).
- [ ] `--dagr.headroom-fraction` / `DAGR_HEADROOM` is exposed (default `0.20`), validated to `0.0..=1.0`, resolved via the helper, and passed to `ContainerLimitProbe::with_headroom(f64)`; the at-least-one-unit floor is preserved (`headroom = 1.0` still yields one unit per pool).
- [ ] `flag > env > default` is proven for **every** knob — grace, teardown-deadline, failure-mode, each `DAGR_POOL_*`, and headroom — via an explicit test matrix.
- [ ] Bad env values fail loudly with the right exit code: an unparseable value → `InvalidUsage`, an out-of-range value → `BootstrapFailure`, each naming the offending variable; no value is ever silently ignored or clamped.
- [ ] The `DAGR_*` table (alongside `DAGR_NO_BANNER`) and the binary-wiring pattern are documented in the quickstart/example (`crates/cli/examples/quickstart.rs`) and mirrored in a new arch.md C26 subsection.
- [ ] An end-to-end run test with several `DAGR_*` variables set proves the effective values reflect the env, and that the env path's terminal states match the flag-driven baseline.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None in the ticket; `docs/tasks.md` carries no `T77` entry (an M5 addition), so no `Q:` items to resolve. One design decision surfaced during implementation and is recorded here:

- **Where the `0.0..=1.0` headroom range is enforced.** `dagr-core`'s `ContainerLimitProbe::with_headroom(f64)` already *clamps* out-of-range fractions (a defensive floor that keeps the core total). The DoD requires an out-of-range value to **fail** with `BootstrapFailure`, and the core must stay env-free — so validation lives in the CLI, in `dagr_cli::config::resolve_headroom`, which rejects anything outside `0.0..=1.0` (naming `DAGR_HEADROOM` or `--dagr.headroom-fraction`) **before** the value ever reaches `with_headroom`. The core clamp is left intact as a belt-and-braces floor; it is unreachable via the CLI path (the CLI has already rejected out-of-range values), so no behaviour is duplicated and `dagr-core` is untouched.

## Out of scope
- The `resolve<T>` helper itself, the duration/`FailureMode` parsers, `EnvParseError`, and the extended reserved-flag list — those are **T76**; this ticket only applies them.
- Any config-file or DSL surface — a permanent scope boundary (ADR 089 rejected alternatives; arch.md: dagr decides neither *when* to run nor via a config surface).
  **[Superseded (in part) by ADR 128 (T113), 2026-07-29 — the config-file half only.]** A bootstrap-read file of
  runtime knobs with named profiles is permitted as a fourth precedence tier; the **DSL** half stands, and so does
  the prohibition on a configuration file **describing the graph**. This ticket's own scope is unchanged.
- A `dagr config` printer that dumps effective knob values — an optional future affordance, not this ticket.
- Cross-process coordination, scheduling, or advancing a data interval — the CLI never decides *when* a pipeline runs; env fallbacks only change how a single invocation is configured.
