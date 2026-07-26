# 090 · T76 — flag>env>default precedence helper + parsers

> **Milestone:** M5 · **Size:** M · **Type:** feature · **Components:** C12, C26
> **Branch:** `feat/t76-config-precedence-helper` · **Depends on:** T55 · **Blocks:** T77

## Why / context
ADR 089 records the operator-requested `flag > env > default` story for every runtime knob and, having read the code, refutes the assumption that a single flag-parsing choke point exists: `parse_cli` returns only the verb (`crates/cli/src/contract.rs`), runtime-flag parsing is ad-hoc per pipeline binary, `RunConfig::new()` is infallible and env-free (`crates/cli/src/driver.rs`), and `crates/core/src/limits.rs` deliberately reads no environment (the headroom default lives at `limits.rs:73` as `HEADROOM_DEFAULT = 0.20`). The ADR's answer is reusable **cli-level** pieces wired at the binary layer, keeping `dagr-core` environment-free. This ticket ships the isolated building blocks of that answer: a resolver helper, the parsers it needs, a strict never-silent error type, the `DAGR_*` name constants, and the extended reserved-flag namespace — all tested standalone in a new `crates/cli/src/config.rs`. It does **not** wire any of this into `RunConfig` or expose the headroom knob; that is T77, which consumes exactly the surface this ticket lands. Splitting the slice this way lets the helper and its error/exit-code mapping be proven in isolation before any binary-layer plumbing (`main.rs`, the sample driver's hand-rolled flag parse) depends on it.

## Objective
- Add `crates/cli/src/config.rs` (a new public module of `dagr-cli`) owning the precedence machinery below; wire it into the crate root so `dagr_cli::config::*` is reachable.
- Implement `resolve<T: FromStr>(flag: Option<T>, env_key: &str, default: T) -> Result<T, EnvParseError>` applying `flag > env > default`: a present flag wins outright (env never read); with no flag, the env var is read and parsed via `T::FromStr`; with neither, `default` is returned.
- Implement a **duration parser** for the bare `10`, `10s`, and `10ms` forms (no `FromStr` exists for these on `std::time::Duration`), returning a `std::time::Duration`.
- Implement a `FromStr` (or a parse fn) for `FailureMode` (`crates/core/src/flow.rs`) accepting `continue-independent` and `stop-on-first-failure`; any other token is a parse error.
- Add `EnvParseError` carrying the offending variable name and mapping to an exit code: a **parse failure** → `ExitCode::InvalidUsage`, an **out-of-range** value → `ExitCode::BootstrapFailure` (reusing the C26 table from `contract.rs`); its `Display` names the variable.
- Add public `DAGR_*` env-name constants for every knob in ADR 089's table (`DAGR_GRACE`, `DAGR_TEARDOWN_DEADLINE`, `DAGR_FAILURE_MODE`, `DAGR_POOL_COMPUTE_THREADS`, `DAGR_POOL_BLOCKING_THREADS`, `DAGR_POOL_MEMORY`, `DAGR_HEADROOM`), alongside the existing `DAGR_NO_BANNER` (`contract.rs`).
- Extend `reserved_flag_names()` (`crates/cli/src/contract.rs`) to list `grace`, `teardown-deadline`, `failure-mode`, `dagr.pool.compute-threads`, `dagr.pool.blocking-threads`, `dagr.pool.memory`, and `dagr.headroom-fraction`, **replacing** the generic `pool` entry; keep `help`, `version`, `run-id`, `store`, `data-interval`, `force`, `run`, and `no-banner`.

## Test plan (write these first — TDD)

**Precedence**
- Given a flag value is present, when `resolve` is called while a valid env var is also set, then the flag value is returned and the env var is not consulted.
- Given no flag but a valid env var, when `resolve` is called, then the env value is parsed via `T::FromStr` and returned.
- Given neither a flag nor an env var, when `resolve` is called, then the supplied `default` is returned unchanged.

**Parsing & errors**
- Given an env var set to a value that fails `T::FromStr`, when `resolve` is called with no flag, then it returns an `EnvParseError` whose `Display` names the variable and whose exit-code mapping is `ExitCode::InvalidUsage`.
- Given an env var set to a syntactically valid but out-of-range value (e.g. a headroom of `1.5` against the `0.0..=1.0` bound), when it is resolved, then the resulting `EnvParseError` maps to `ExitCode::BootstrapFailure` and names the variable.
- Given the duration strings `10`, `10s`, and `10ms`, when each is parsed, then they yield `Duration::from_secs(10)`, `Duration::from_secs(10)`, and `Duration::from_millis(10)` respectively.
- Given `DAGR_FAILURE_MODE=stop-on-first-failure`, when the `FailureMode` parser runs, then it returns `FailureMode::StopOnFirstFailure`; and given `continue-independent`, then `FailureMode::ContinueIndependent`.
- Given an unknown failure-mode token, when the `FailureMode` parser runs, then it returns a parse error (surfaced as an `EnvParseError` → `ExitCode::InvalidUsage` when reached through `resolve`).

**Reserved flags**
- Given a pipeline parameter whose flag name equals any newly reserved library flag (`grace`, `teardown-deadline`, `failure-mode`, `dagr.pool.compute-threads`, `dagr.pool.blocking-threads`, `dagr.pool.memory`, or `dagr.headroom-fraction`), when `check_reserved_collision` runs, then the collision is rejected as a `LibraryFlagCollision` naming that exact flag.
- Given the generic `pool` name that used to be reserved, when a pipeline declares it as a parameter, then it no longer collides (it was replaced by the specific `dagr.pool.*` entries).

## Definition of done
- [ ] `crates/cli/src/config.rs` exists and exposes `resolve<T: FromStr>(flag: Option<T>, env_key: &str, default: T) -> Result<T, EnvParseError>` implementing `flag > env > default`, with the flag path never reading the environment.
- [ ] A duration parser accepts `10` / `10s` / `10ms` and returns the correct `Duration`; a `FailureMode` parser accepts `continue-independent` / `stop-on-first-failure` and rejects anything else.
- [ ] `EnvParseError` carries the offending variable name, its `Display` names it, and it maps a parse failure to `ExitCode::InvalidUsage` and an out-of-range value to `ExitCode::BootstrapFailure` (reusing the C26 table in `contract.rs`).
- [ ] Public `DAGR_*` name constants exist for every ADR-089 knob (`DAGR_GRACE`, `DAGR_TEARDOWN_DEADLINE`, `DAGR_FAILURE_MODE`, `DAGR_POOL_COMPUTE_THREADS`, `DAGR_POOL_BLOCKING_THREADS`, `DAGR_POOL_MEMORY`, `DAGR_HEADROOM`).
- [ ] `reserved_flag_names()` is extended with exactly `grace`, `teardown-deadline`, `failure-mode`, `dagr.pool.compute-threads`, `dagr.pool.blocking-threads`, `dagr.pool.memory`, and `dagr.headroom-fraction`, and the generic `pool` entry is removed.
- [ ] Unit tests cover flag-only, env-only, both-present, and neither-present resolution, plus bad-value (unparseable and out-of-range) cases, all in isolation with no `RunConfig` involvement.
- [ ] `dagr-core` is untouched: no env read is added to `limits.rs`, `flow.rs`, or anywhere in the core crate (env access lives only in `dagr-cli`).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Wiring the env fallback into `RunConfig`, adding the opt-in env-fallback builder methods, and exposing the headroom knob via `--dagr.headroom-fraction` / `ContainerLimitProbe::with_headroom` — T77 owns all of it and consumes this ticket's helper, parsers, error type, and constants.
- Any env read inside `dagr-core` (`crates/core/src/limits.rs`, `crates/core/src/flow.rs`) — a permanent scope boundary: the core reads the host once and is injectable for tests; the CLI parses env and passes already-parsed values inward.
- Threading env resolution into `main.rs` or the sample driver's hand-rolled flag parse, and any actual reading of the `DAGR_*` variables at run time — the constants are declared here but consumed by T77.
