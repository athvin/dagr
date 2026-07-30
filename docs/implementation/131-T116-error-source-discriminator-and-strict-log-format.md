# 131 · T116 — a source discriminator for config errors, and strict `DAGR_LOG_FORMAT`

> **Milestone:** M11 · **Size:** M · **Type:** feature · **Components:** C25, C26
> **Branch:** `feat/t116-error-source-discriminator-and-strict-log-format` · **Depends on:** T115 · **Blocks:** T118

## Why / context

Two honesty defects in the configuration layer, both surfaced by adding a third source
of values.

**1. `EnvParseError` can only describe an environment variable.** Its field is named
`variable`, and its `Display` hardcodes the phrase:

> `environment variable \`{}\` = \`{}\` {}: {} (arch.md C26 / ADR 089 — bad env values fail loudly and are never silently ignored)`

A value that came from `dagr.toml` resolved through this type produces a **factually
wrong diagnostic** — it tells the operator to look at an environment variable that may
not be set, for a value that came from a file, and it cites a rationale about env
values. T115 has to route file values through this type, so this is the ticket that
makes the message true. A configuration error's whole job is to say *where the bad
value came from*; getting that wrong is worse than a generic message.

**2. `DAGR_LOG_FORMAT` silently swallows bad values.** `OutputMode::from_env_value`
maps anything unrecognized to `Structured`. So `DAGR_LOG_FORMAT=humann` produces
structured logs and no complaint. This directly contradicts the rule stated in
`arch.md` C26 and in `config.rs`'s own module docs — "a bad env value **fails loudly**
and is never silently ignored or clamped." The M10 truth pass had to *document the
exception* to stop `arch.md` from lying; this ticket removes the exception so the
documentation can go back to being unconditional.

`DAGR_LOG_FORMAT` is also the only knob with **no flag at all**, which makes it
unreachable per-invocation and absent from the reserved namespace — inconsistent with
the invariant that every out-of-band knob has a reserved flag it cannot be shadowed by.

## Objective

Make a configuration diagnostic name its own source, and bring the last knob into the
strict regime.

- Replace `EnvParseError`'s env-only shape with a **source discriminator** covering
  **flag**, **environment variable**, and **file** (carrying path, profile, and key
  path). Keep the type's name and its `Parse` / `OutOfRange` → exit-code split; this is
  a widening, not a redesign.
- Make `Display` render each source correctly and specifically: a flag by its
  `--flag` spelling, an env var by its name, a file value by **path, profile, and key**
  — so an operator is pointed at the line they must edit.
- Route T115's file values and T114's flag validations through it, so the existing
  `validate_headroom` no longer has to pass a flag name into a field called `variable`.
- Bring **`DAGR_LOG_FORMAT`** into strict resolution: an unrecognized value is a loud
  `Parse` failure naming the variable and listing the accepted values; unset or empty
  still resolves to `structured`.
- Give it a **flag** (`--dagr.log-format`) and reserve it, so it stops being the one
  env-only knob, and add it to the `arch.md` C26 table as a full row.
- Remove the exception sentence the truth pass added to `arch.md`, restoring the
  never-silent rule as unconditional.

## Test plan (write these first — TDD)

**Diagnostics name their source**
- Given a bad value from a **flag**, then the message names the `--flag` spelling and
  does **not** say "environment variable".
- Given a bad value from an **environment variable**, then the message is unchanged
  from today (a regression guard on the existing wording).
- Given a bad value from a **file**, then the message names the file path, the profile,
  and the key path — and does **not** say "environment variable".
- Given each source, then the `Parse` → `InvalidUsage` and `OutOfRange` →
  `BootstrapFailure` mapping is unchanged.
- Given a file value that is out of range (a headroom of `1.5` in `dagr.toml`), then it
  exits `BootstrapFailure` naming the file, profile, and key.

**`DAGR_LOG_FORMAT` is strict**
- Given `DAGR_LOG_FORMAT=humann`, then the process fails loudly naming the variable and
  the accepted values — it does **not** produce structured logs silently. This fails
  today.
- Given `DAGR_LOG_FORMAT=human` and `=structured`, then each selects its mode.
- Given it unset or empty, then `structured` applies (unchanged).
- Given `--dagr.log-format human` and `DAGR_LOG_FORMAT=structured`, then the flag wins.
- Given a `dagr.toml` setting the log format, then it applies at the `file` tier and is
  beaten by both env and flag.
- Given a pipeline parameter named `dagr.log-format`, then it is a hard
  `LibraryFlagCollision`.

**Documentation is true again**
- A docs test (in the style of the existing docs-claims tests) asserts `arch.md` no
  longer records an exception to the never-silent rule, and that the C26 table's row
  count matches the number of resolved knobs.

**No regression**
- Given no configuration at all, then logging behaviour and every event stream are
  byte-identical to before this change.

## Definition of done

- [ ] `EnvParseError` carries a source discriminator for flag / env / file (with path,
      profile, key) and renders each correctly; the `Parse` / `OutOfRange` exit-code
      split is unchanged.
- [ ] File and flag values are routed through it; `validate_headroom` no longer passes a
      flag name as a `variable`.
- [ ] `DAGR_LOG_FORMAT` fails loudly on an unrecognized value, listing the accepted
      set; unset/empty still yields `structured`.
- [ ] `--dagr.log-format` exists, is reserved, is in `flag_takes_value`, and is a full
      row in the `arch.md` C26 table.
- [ ] The never-silent rule in `arch.md` is unconditional again, and its exception
      sentence is removed.
- [ ] With no configuration set, behaviour and event streams are byte-identical.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Is renaming `EnvParseError` worth the churn?** The name becomes inaccurate once it
  covers three sources. It is a public type in `dagr-cli`, so renaming is a visible API
  change for a pre-1.0 crate — cheap now, and the alternative is a permanently misnamed
  type. Default: widen in place and keep the name this ticket, with the rename recorded
  as a follow-up if the inaccuracy grates; decided in-PR.
- **Does making `DAGR_LOG_FORMAT` strict break anyone?** It converts a silently-ignored
  typo into a startup failure — which is the point, and matches every other knob. Any
  CI or deployment currently passing a bad value would begin failing loudly, which is
  the correct outcome; called out in the PR body as a behaviour change.

## Out of scope

- The loader, discovery, and profile layering — **T115**.
- The env↔key mapping table — **T117**.
- The acceptance gate — **T118**.
- Any other knob's validation semantics; ADR 089's duration bounds are **T114**'s.
- Changing what the log formats *are* (C25's two modes stay two modes) or adding a
  third.
- Scope boundary restated: better diagnostics and one strict knob add no capability;
  dagr remains not a scheduler, a *distributed* execution system beyond ADR 115's
  carve-out, a *coordinating* metadata store, a web interface, a DSL, or a backfill
  orchestrator, and the graph's shape never changes at runtime.
