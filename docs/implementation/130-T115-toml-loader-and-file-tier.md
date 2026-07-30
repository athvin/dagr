# 130 · T115 — the TOML loader, profile layering, and the `file` precedence tier

> **Milestone:** M11 · **Size:** M · **Type:** feature · **Components:** C7, C20, C26
> **Branch:** `feat/t115-toml-loader-and-file-tier` · **Depends on:** T114 · **Blocks:** T116, T117

## Why / context

This is the ticket the milestone exists for: `dagr.toml`, named profiles, and the
fourth precedence tier ADR 128 §2 decided — `flag > env > file(profile) > default`.
T114 made the run path actually resolve its knobs, so the tier is being added to
something that consults it.

The design is settled by ADR 128; this ticket implements it. Two constraints are worth
restating because they are the ones that can be got quietly wrong:

**The file is read at bootstrap, never during assembly (ADR 128 §5).** This is
machine-checked in a way that is easy to trip: `crates/core/tests/determinism_and_purity.rs`
runs a real assembly in a child process with the environment cleared and the working
directory pointed at an empty temp dir, and C20's criterion requires a graph artifact to
be producible "in an empty environment with no configuration present." A loader that
searches for `dagr.toml` from the current directory during assembly breaks both, and —
worse — makes the graph depend on ambient state. **Assembly must behave identically
whether or not a `dagr.toml` exists**, and this ticket adds the test that proves it.

**The pool pins are tri-state (ADR 128 §2).** `resolve_opt` distinguishes *pinned* from
*absent, so detect from the host*. A file that mentions no pool must leave it
**unpinned**, not pin it to a default. Collapsing that is a silent behaviour change on
every machine whose detected capacity differs from the default.

## Objective

Add the loader, the profile model, and the tier.

- Add a **TOML parser to `dagr-cli` only** (never `dagr-core`), and extend `deny.toml`
  for its licences.
- Implement **discovery**: `--dagr.config <path>` when given (a missing explicit path
  is a hard error, never a silent fallback); otherwise `./dagr.toml` if present;
  otherwise no file. Any user-level fallback is decided here and must be inert during
  assembly. **No file present is not an error** — the default path stays
  zero-configuration.
- Implement **profile selection**: `--dagr.profile <name>` / `DAGR_PROFILE`, defaulting
  to the `default` table alone. A named profile **layers over `default` key by key**,
  so a profile states only its differences. An **unknown profile name is a loud
  failure**, never a silent fallback.
- Insert the **`file` tier** beneath the environment in `resolve`, preserving both the
  flag-wins-without-reading-the-env property and `resolve_opt`'s tri-state.
- Read the file **at bootstrap only**, and add the purity test that proves assembly is
  unaffected by a `dagr.toml` present in the working directory.
- **Loud, specific failures**: an unparseable file, an unknown key, a wrong-typed
  value, or an unknown profile fails at bootstrap naming the **file, profile, and key**.
- Register `dagr.profile` and `dagr.config` in the reserved flag namespace and in
  `flag_takes_value`.
- Keep every knob's reserved flag (ADR 128 §6): **no file-only knob is introduced.**

## Test plan (write these first — TDD)

**Precedence, all four tiers**
- Given a file setting grace and nothing else, then a run uses it.
- Given the file **and** `DAGR_GRACE`, then the environment wins.
- Given the file, the environment, **and** `--grace`, then the flag wins.
- Given none of them, then the compiled default applies and the event stream is
  byte-identical to a pre-M11 run.

**Profiles**
- Given `[default]` and `[prod]` where `prod` overrides one key, then
  `--dagr.profile prod` yields prod's value for that key and default's for every other
  (layering, not replacement).
- Given `--dagr.profile nope`, then it fails loudly naming the profile and listing the
  profiles the file defines — not a silent fallback to `default`.
- Given `DAGR_PROFILE=prod` and `--dagr.profile dev`, then the flag wins.
- Given a file with only `[default]` and no profile selected, then it applies.

**Discovery**
- Given no `dagr.toml` anywhere, then everything behaves exactly as before this ticket
  (asserted by a byte-identical event stream).
- Given `--dagr.config ./missing.toml`, then it is a **hard error** naming the path.
- Given `--dagr.config <path>` and a `./dagr.toml` that would also match, then the
  explicit path wins.

**The tri-state — the subtle one**
- Given a file that sets `pool.memory` but not the thread pools, then memory is pinned
  and both thread pools are still **detected**, not defaulted.
- Given a file that sets no pools at all, then the admission ledger is identical to a
  run with no file.

**Purity — the constraint that can be silently broken**
- Given a `dagr.toml` **present** in the working directory,
  `crates/core/tests/determinism_and_purity.rs` still passes: assembly succeeds and the
  graph artifact is byte-identical to assembly with no file present.
- Given a `dagr.toml` present, the **graph fingerprint** is unchanged — the file cannot
  reach graph identity.
- Given assembly, then no file read occurs (asserted by pointing discovery at a path
  that would error if opened during assembly).

**Loud failures**
- Given malformed TOML, an unknown key, and a wrong-typed value, then each fails at
  bootstrap naming the file, the profile, and the key, with the `EnvParseError` exit-code
  split honoured.

**Boundaries**
- `cargo tree -p dagr-core -e normal --no-default-features` shows an empty runtime
  dependency set; no TOML parser is reachable from `dagr-core`.
- `cargo deny check licenses` passes with the new dependency.
- A test asserts the file **cannot** select a flow (there is no key for it) and cannot
  alter any node's policy hash.

## Definition of done

- [ ] A TOML parser lives in `dagr-cli` only; `deny.toml` covers it; `dagr-core`'s
      runtime dependency set is still empty.
- [ ] Discovery works: explicit path (missing ⇒ hard error) > `./dagr.toml` > none;
      no file is not an error.
- [ ] Profile selection works via flag > env > `default`; a named profile layers over
      `default`; an unknown name fails loudly listing the available profiles.
- [ ] The `file` tier sits beneath the environment; the flag-wins-without-reading-env
      property and `resolve_opt`'s tri-state are both preserved.
- [ ] The file is read at bootstrap only; a `dagr.toml` present in the assembly cwd
      leaves the graph artifact and fingerprint byte-identical, and the purity test
      proves it.
- [ ] Malformed input fails at bootstrap naming file, profile, and key.
- [ ] `dagr.profile` and `dagr.config` are reserved and in `flag_takes_value`; no
      file-only knob exists.
- [ ] With no file present, event streams are byte-identical to a pre-M11 run.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Is there a user-level fallback path?** A `$XDG_CONFIG_HOME/dagr/config.toml` tier
  is convenient for a developer and a liability for reproducibility — a run could pick
  up configuration from outside the repository, which is exactly what makes "it works
  on my machine" bugs. The default answer is **no user-level path in the first cut**
  (working directory or explicit flag only); if it is added, it must be inert during
  assembly and recorded in-PR with the reproducibility trade-off stated.
- **Does discovery walk parent directories?** Cargo-style upward search is familiar but
  makes the effective configuration depend on where the binary was invoked from. Default
  answer: no walk. Recorded in-PR.
- **Which TOML crate?** Decided in-PR against the dependency and licence budget, noting
  whether it pulls `serde` derive.

## Out of scope

- Wiring the env tier and the reserved-flag parsing — **T114** (done first).
- `EnvParseError`'s source discriminator, so a file diagnostic reads correctly rather
  than saying "environment variable" — **T116**. Until then this ticket's diagnostics
  name the file/profile/key in the `detail`.
- The env↔key mapping table and its totality assertion — **T117**.
- The acceptance gate — **T118**.
- **Any key that describes the graph** — flow selection, node/edge declaration, node
  policy or placement overrides, conditionals. Permanently excluded by ADR 128, not
  deferred.
- Secrets in the file; credentials stay in the ambient environment.
- Scope boundary restated: a bootstrap-read file of runtime knobs configures how one
  invocation runs and describes no graph; dagr remains not a scheduler, a *distributed*
  execution system beyond ADR 115's carve-out, a *coordinating* metadata store, a web
  interface, a DSL, or a backfill orchestrator, and the graph's shape never changes at
  runtime.
