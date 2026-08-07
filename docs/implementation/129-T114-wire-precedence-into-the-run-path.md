# 129 · T114 — wire the existing precedence into the run path

> **Milestone:** M11 · **Size:** M · **Type:** feature · **Components:** C12, C26
> **Branch:** `feat/t114-wire-precedence-into-the-run-path` · **Depends on:** T113 · **Blocks:** T115

## Why / context

ADR 089 specified `flag > env > default` across every runtime knob and shipped the
machinery to do it. **Nothing on the shipped run path calls it.**
`crates/cli/src/registry.rs` builds `RunConfig::new(base).run_id(run_id)` and chains
none of the env-fallback builders; there are **zero** non-test callers of
`grace_from_env`, `teardown_deadline_from_env`, `failure_mode_from_env`,
`resolve_pool_pins`, or `resolve_headroom`. Every one is exercised only by
`crates/cli/tests/env_fallbacks_and_headroom.rs`.

This is not a bug in ADR 089 — the builders are deliberately opt-in so
`RunConfig::new` stays infallible and environment-free. But it means the reference
binary honours **none** of the documented knobs: setting `DAGR_GRACE=30s` and running
`dagr run etl` changes nothing. The M10 truth pass corrects `arch.md`'s claim about
this; **this ticket makes the original claim true instead.**

It lands before the file loader (T115) for a concrete reason from ADR 128: a file tier
inserted beneath a tier nothing consults would resolve nothing. Wiring first means the
loader is added to a path that actually reads its own configuration.

Three smaller defects found by the same audit are folded in here, because they are all
in the same two files and all block a coherent config story:

- **`--store` has no environment fallback**, yet `arch.md` claimed the store base is
  "supplied by flag or environment variable". `DAGR_STORE` does not exist anywhere.
  The store base is the one piece of infrastructure the spec asks an operator to
  supply, so it is the knob that most needs to be settable once in an environment.
- **`flag_takes_value` omits `dagr.metastore-store`**, which does take a path. So
  `dagr run --dagr.metastore-store ./x.db etl` treats `./x.db` as the flow name and
  fails confusingly.
- **ADR 089's table documents validations that were never implemented** — grace
  "≥ 1 ms", teardown-deadline "≥ 1 s". `EnvDuration::from_str` accepts `0`.

## Objective

Make the reference binary honour the precedence it documents.

- Wire resolution into the **run path**: `run_selected_flow` (`registry.rs`) and
  `RunnableFlow::run_to_store` resolve grace, teardown deadline, failure mode, the
  three pool pins, and the headroom fraction through the existing helpers, and pass
  the results into `RunConfig` and the limit probe. `RunConfig::new` **stays
  infallible and env-free**; the resolution happens at the call site, in bootstrap.
- Parse the reserved runtime flags that currently have no parser at all
  (`--grace`, `--teardown-deadline`, `--failure-mode`, `--dagr.pool.*`,
  `--dagr.headroom-fraction`) so the *flag* tier of `flag > env > default` exists on
  the run path, not just the env tier.
- Add **`DAGR_STORE`** as `--store`'s environment fallback, resolved with the same
  helper, and restore the `arch.md` sentence the truth pass had to delete.
- Add `dagr.metastore-store` to **`flag_takes_value`**, with a regression test that
  `dagr run --dagr.metastore-store <path> <flow>` selects `<flow>`.
- **Settle ADR 089's duration bounds**: implement `≥ 1 ms` for grace and `≥ 1 s` for
  teardown-deadline as `OutOfRange` errors, or strike them from both tables. Record
  which, and why, in-PR.
- Add `DAGR_METASTORE` to `config.rs`'s env-name constant test, which omits it.
- A **bad value fails loudly** at bootstrap with the offending variable named, using
  the existing `EnvParseError` exit-code split.

## Test plan (write these first — TDD)

**The knobs actually work now — each of these fails today**
- Given `DAGR_GRACE=30s` and a run through `dagr run <flow>`, then the run's shutdown
  budget reflects 30s (observable in the startup banner and the budget arithmetic).
- Given `--grace 5s` **and** `DAGR_GRACE=30s`, then the flag wins and the env is not
  read.
- Given `DAGR_TEARDOWN_DEADLINE`, then the teardown phase is bounded by it.
- Given `DAGR_FAILURE_MODE=stop-on-first-failure`, then a failing node stops the run,
  observable in the terminal states and cancellation origin.
- Given `DAGR_POOL_MEMORY`, then the admission ledger's memory capacity matches it
  rather than the detected value; given no pin, then detection still applies (the
  tri-state is preserved).
- Given `DAGR_HEADROOM=0.5`, then pools are sized to half the detected limit, with the
  at-least-one-unit floor intact.
- Given `DAGR_STORE=./elsewhere`, then the run store is created there; given `--store`
  as well, then the flag wins.

**Loud failures**
- Given `DAGR_GRACE=nonsense`, then the run exits `InvalidUsage` naming `DAGR_GRACE`
  and its value — not a default silently applied.
- Given `DAGR_HEADROOM=1.5`, then it exits `BootstrapFailure` naming the variable.
- Given the chosen resolution of the duration bounds: either `DAGR_GRACE=0` exits
  `BootstrapFailure` naming the bound, or the bound is absent from both tables and `0`
  is accepted — the test matches whichever was decided, and the tables match the test.

**The parsing bug**
- Given `dagr run --dagr.metastore-store ./x.db <flow>`, then `<flow>` is selected and
  `./x.db` is not mistaken for it.

**No behaviour change when nothing is set**
- Given no flags and an empty environment, then a run's event stream is
  **byte-identical** to before this change, and the terminal states match.
- Given the existing examples and the quickstart, then their output is unchanged.

**Boundaries**
- `dagr-core` still reads no environment; `cargo tree` shows its runtime dependency
  set is still empty.
- `config.rs`'s env-name constant test covers all knobs including `DAGR_METASTORE`.

## Definition of done

- [ ] The run path resolves grace, teardown deadline, failure mode, the three pool
      pins, and headroom through the existing helpers; `RunConfig::new` stays
      infallible and env-free.
- [ ] The reserved runtime flags are parsed on the run path, so the flag tier exists
      and beats the environment.
- [ ] `DAGR_STORE` exists as `--store`'s fallback and `arch.md`'s store sentence is
      restored to match.
- [ ] `flag_takes_value` includes `dagr.metastore-store`, with a regression test.
- [ ] ADR 089's duration bounds are implemented or struck, consistently in code,
      `arch.md`'s table, and ADR 089's table; the decision is recorded in-PR.
- [ ] `DAGR_METASTORE` is in the env-name constant test.
- [ ] Bad values fail loudly at bootstrap with the variable named and the correct exit
      code.
- [ ] With nothing set, event streams are byte-identical to before this change.
- [ ] `dagr-core` reads no environment; its runtime dependency set is still empty.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Where does flag parsing live?** ADR 089 established that there is no single
  flag-parsing choke point — `parse_cli` returns only the verb, and runtime flags are
  parsed ad-hoc per binary. This ticket needs the *library* run path to parse the
  reserved flags without turning `parse_cli` into a full parser (which ADR 089
  rejected, and which would breach C26's "verbs library-owned, parameters
  pipeline-specific" split). The likely shape is a small reserved-flag scan next to
  the existing `store_base` scan in `registry.rs`, reusing `flag_takes_value`;
  recorded in-PR.
  - **RESOLVED (this PR): the reserved-flag scan lives in `config.rs`.** Each
    runtime flag gets a `parse_*_flag(argv)` scanner over the shared
    `parse_valued_flag` body (the exact shape `--dagr.executor` / `--dagr.max-pods`
    already use), and `run_selected_flow` calls them next to its existing `--store`
    scan. `parse_cli` still returns only the verb — no choke point was created —
    and `flag_takes_value` already listed every flag this ticket parses, so
    `first_positional` needed no change beyond the `dagr.metastore-store` fix the
    ticket names.
- **Do the pool pins belong on the run path at all?** They are pinning knobs an
  operator uses to split a host between concurrent runs. Wiring them is consistent
  with ADR 089's table; if it turns out the probe is constructed too early in
  bootstrap to accept them, the ordering fix is recorded in-PR rather than silently
  skipping the knob.
  - **RESOLVED (this PR): yes — and the probe is engaged only when a pool knob is
    supplied.** No ordering problem existed: the run path sizes capacities before
    `RunConfig` is built, so `resolve_pool_pins` + `resolve_headroom` feed
    `ContainerLimitProbe::from_host().with_pins(..).with_headroom(..)` directly.
    The probe runs **iff** at least one pin or an explicit headroom was supplied
    (flag or env): a pinned pool is the pin verbatim and every unpinned pool derives
    from detection (the `resolve_opt` tri-state, preserved exactly per ADR 128 §2).
    With **no** pool knob supplied the capacities stay the historical unconstrained
    set — the ticket's own byte-identical requirement and its "no default value
    changes" boundary both forbid silently switching the no-knob default from
    unconstrained to detection-sized; that switch, if ever wanted, is its own
    decision.
- **ADR 089's duration bounds (from the Objective): IMPLEMENTED, not struck.**
  Grace `≥ 1 ms` and teardown-deadline `≥ 1 s` are enforced in
  `grace_from_env` / `teardown_deadline_from_env` as `OutOfRange` →
  `BootstrapFailure`, naming the offending source (`DAGR_GRACE` / `--grace`,
  `DAGR_TEARDOWN_DEADLINE` / `--teardown-deadline`) — and `arch.md`'s C26 table now
  carries the same bounds as ADR 089's. Why implement rather than strike: a zero
  grace makes the printed shutdown budget a lie (no drain window at all), a zero
  teardown deadline guarantees every teardown attempt is killed before it runs, and
  both are operator typos far more often than intent — exactly the never-silent
  posture every other validated knob (headroom's `0.0..=1.0`) already commits to.
  This ticket's whole theme is making the documented claim true rather than
  deleting it.
- **`docs/tasks.md` carries no T114 entry** (the tasks file ends at the original
  M0–M4 set; the M9–M11 tickets exist only as ticket files), so there are no
  `Q:` items beyond the two above.

## Out of scope

- The configuration file, profiles, and the `file` tier — **T115**.
- `EnvParseError`'s source discriminator and `DAGR_LOG_FORMAT` — **T116**.
- The env↔key mapping table — **T117**.
- New knobs of any kind. This ticket wires the knobs that already exist and adds
  exactly one documented-but-missing variable (`DAGR_STORE`).
- Changing the precedence order, the reserved-flag namespace, or any default value.
- Scope boundary restated: honouring documented flags and environment variables
  configures how one invocation runs and nothing else; dagr remains not a scheduler, a
  *distributed* execution system beyond ADR 115's carve-out, a *coordinating* metadata
  store, a web interface, a DSL, or a backfill orchestrator, and the graph's shape
  never changes at runtime.
