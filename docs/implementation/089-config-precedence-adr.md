# 089 · Runtime knob precedence ADR — `flag > env > default`

> **Date:** 2026-07-26 · **Status:** accepted (operator-approved framework feature) · **Type:** decision · **Components:** C12, C26
> **Branch:** `feat/config-precedence-adr` · **Relates to:** T32 (C12 container-limit detection), T55 (C26 CLI contract), T31 (C12 admission pools) · **Unblocks:** T76 (precedence helper), T77 (env wiring + headroom)

## Why / context

dagr already chooses sensible defaults and exposes many as flags: the admission pools size themselves from cgroup/host detection with a **hardcoded 20% headroom** (`crates/core/src/limits.rs:73`), overridable by `--dagr.pool.compute-threads` / `--dagr.pool.blocking-threads` / `--dagr.pool.memory`; the driver defaults grace to 10 s (`--grace`), teardown to 15 s (`--teardown-deadline`), and failure mode to continue-independent (`--failure-mode`). But **only the startup banner honours an environment variable** (`DAGR_NO_BANNER`, via `banner_suppressed_by_env`); no runtime knob has an env fallback, and the headroom fraction has no override at all.

An operator asked for the standard `flag > env var > default` story across every runtime knob, so a knob can be set once in an orchestrator's environment and still be overridden per-invocation on the command line. This ADR records how to provide it.

### What the code actually allows (the design constraint)

An earlier framing assumed a single flag-parsing choke point in which env fallback could be inserted. Reading the code refutes it: `parse_cli` returns only the verb (`crates/cli/src/contract.rs`); real runtime-flag parsing is **ad-hoc per pipeline binary** (e.g. the sample driver's hand-rolled `Flags::parse` over the post-verb argv); `RunConfig::new()` is infallible and reads no environment (`crates/cli/src/driver.rs`); and `crates/core/src/limits.rs` deliberately reads no environment at all (it takes the host core count once and is injectable for tests). There is no library layer that already sees every flag value, so precedence cannot be resolved "in one place inside `parse_cli`."

## Decision

Deliver precedence as **reusable library pieces wired at the binary layer**, keeping `dagr-core` environment-free:

- **A resolver helper in `dagr-cli` (never core):** `dagr_cli::config::resolve<T: FromStr>(flag: Option<T>, env_key: &str, default: T) -> Result<T, EnvParseError>` implementing `flag > env > default` once. A pipeline binary parses its flag as it does today, then calls `resolve` to fold in the env fallback before constructing `RunConfig` / pool pins.
- **Parsers the resolver needs:** a duration helper for the `10` / `10s` / `10ms` forms (no `FromStr` exists for them) and a `FromStr` for `FailureMode`.
- **Strict, never-silent errors:** `EnvParseError` maps a bad value to an exit code — a parse failure to `InvalidUsage`, an out-of-range value to `BootstrapFailure` — and names the offending variable; env values are never silently ignored.
- **Opt-in `RunConfig` env-fallback builder methods** so a binary that wants the standard behaviour gets it explicitly, while `RunConfig::new()` stays infallible and env-free. `dagr-core` never reads env; the CLI parses env and passes already-parsed values in (the headroom fraction is handed to the existing `ContainerLimitProbe::with_headroom(f64)`).
- **A new knob:** expose the hardcoded headroom as `--dagr.headroom-fraction` / `DAGR_HEADROOM` (default 0.20, validated `0.0..=1.0`). The existing at-least-one-unit floor still applies, so extreme values never zero a pool; document that `headroom = 1.0` still yields one unit per pool.
- **Reserved-flag coverage:** extend `reserved_flag_names()` (`contract.rs`) to list the exact library flags (`grace`, `teardown-deadline`, `failure-mode`, `dagr.pool.compute-threads|blocking-threads|memory`, `dagr.headroom-fraction`) instead of the generic `pool` entry, so a pipeline parameter can never shadow one.

### The env namespace

`DAGR_*`, snake-case (env convention; flags stay kebab), documented alongside `DAGR_NO_BANNER`:

| Flag | Env var | Default | Type | Validation |
|---|---|---|---|---|
| `--grace` | `DAGR_GRACE` | 10s | Duration | `10` / `10s` / `10ms`; ≥ 1 ms |
| `--teardown-deadline` | `DAGR_TEARDOWN_DEADLINE` | 15s | Duration | ≥ 1 s |
| `--failure-mode` | `DAGR_FAILURE_MODE` | continue-independent | enum | `continue-independent` \| `stop-on-first-failure` |
| `--dagr.pool.compute-threads` | `DAGR_POOL_COMPUTE_THREADS` | detected | u32 | ≥ 1 |
| `--dagr.pool.blocking-threads` | `DAGR_POOL_BLOCKING_THREADS` | detected | u32 | ≥ 1 |
| `--dagr.pool.memory` | `DAGR_POOL_MEMORY` | detected | u64 | ≥ 1 byte |
| `--dagr.headroom-fraction` | `DAGR_HEADROOM` | 0.20 | f64 | `0.0..=1.0` |

## Consequences

- **`flag > env > default` everywhere**, resolved by one shared helper — no per-knob duplication.
- **`dagr-core` stays environment-free**; env reading lives entirely in `dagr-cli`, preserving the "reads the host once, injectable for tests" property of C12.
- **Bad env values fail loudly** at bootstrap with a named variable and a specific exit code.
- **The headroom becomes tunable** for the first time, with the safety floor intact.
- **A documented wiring pattern** (T77) shows a pipeline binary how to fold env fallback into `RunConfig` — the helper assists callers rather than hiding parsing inside the library.

## Rejected alternatives

- **Resolving precedence inside `parse_cli`.** No such choke point exists — `parse_cli` returns only the verb and never sees runtime-flag values; turning it into a full flag parser would break the C26 "verbs library-owned, parameters pipeline-specific" boundary. Rejected for the binary-layer helper.
- **Reading `DAGR_*` inside `dagr-core` (`limits.rs`, `RunConfig`).** Would put environment access in the dependency-lean core and defeat its testable, inject-once probe design. Rejected: the CLI parses env and passes parsed values inward.
- **Clamping out-of-range env values silently.** Hides operator mistakes. Rejected in favour of a named `EnvParseError` with a distinct exit code.
- **A config file / DSL.** Out of the permanent scope boundary (arch.md: dagr decides neither *when* to run nor via a config surface). Rejected; flags + env + defaults are the whole model.
  **[Superseded (in part) by ADR 128 (T113), 2026-07-29 — the *config file* half only.]** ADR 128 permits a
  **file of runtime knobs, read at bootstrap, with named profiles**, as a fourth precedence tier beneath
  the environment (`flag > env > file(profile) > default`). Two corrections it records: this bullet's
  parenthetical **misattributes** its rationale — arch.md contains no sentence saying dagr decides nothing
  "via a config surface"; arch.md C26 says the *opposite polarity*, calling the flag/env layer "a config
  *surface* only" and permitting it. And the genuine spec-level prohibition is narrower than this bullet
  reads: arch.md forbids a configuration file **describing the graph**, which stays verbatim. **The DSL half
  of this rejection stands unchanged**, as does every other clause of this ADR — the resolver, the `DAGR_*`
  namespace, the strict never-silent errors, the opt-in builders, and the zero-env-in-core boundary.
