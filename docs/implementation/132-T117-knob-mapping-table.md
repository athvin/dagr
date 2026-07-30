# 132 · T117 — the knob mapping table and its totality assertion

> **Milestone:** M11 · **Size:** S · **Type:** feature (tests) · **Components:** C26
> **Branch:** `feat/t117-knob-mapping-table` · **Depends on:** T115 · **Blocks:** T118

## Why / context

ADR 128 §4 decided that the nine shipped `DAGR_*` environment variables keep their
exact spellings, and that each maps to one file key path through **a documented table
CI proves total and injective**. That table is the whole reason the decision was
affordable: the alternative was deriving `DAGR__SECTION__KEY` names mechanically from
the file structure, which would have given every existing knob a second spelling and a
deprecation cycle. A checked table buys the same predictability with no migration.

It has to be *checked* rather than merely written, because the failure mode is silent
and this repo has already been bitten by it. The audit found that `DAGR_METASTORE` is
missing from **both** knob tables (`arch.md`'s C26 table and ADR 089's), and that
`config.rs`'s own env-name constant test omits it — so a knob shipped, was documented
nowhere, and no test noticed. With three tiers and a file, the same drift produces a
worse symptom: a key that an operator writes in `dagr.toml`, that parses fine, and that
nothing ever reads.

The invariant is three-way, and every leg needs asserting:

- **Total** — every knob the binary resolves appears in the table, in the `arch.md`
  table, and (where it has one) in `reserved_flag_names`.
- **Injective** — no two knobs share an environment variable name, a file key path, or
  a flag.
- **Live** — every table row corresponds to a knob something actually resolves. A row
  for a knob nobody reads is the same defect as a knob with no row, and is what
  eventually produces dead configuration that looks supported.

## Objective

Make the knob set a single checked source of truth.

- Introduce **one canonical table** of knobs in `dagr-cli`, each entry carrying: the
  reserved flag name (or explicitly none), the `DAGR_*` variable name, the file key
  path, the type, the default (or "detected"), and the validation.
- Derive the resolution sites from it, or assert against it — whichever keeps the table
  from becoming a parallel truth. Prefer derivation: the table should be the thing the
  resolver consults, not a description of it.
- Add CI assertions for all three legs: **totality**, **injectivity**, and
  **liveness**.
- Assert the table agrees with `arch.md`'s C26 table **row for row** — the same
  discipline `scripts/check-edition-and-msrv-pins.sh` applies to the MSRV pins and
  `scripts/check-stability-and-criteria.sh` applies to the criteria matrix.
- Document the table once, in `arch.md` C26, and have the cookbook link it rather than
  restate it.

## Test plan (write these first — TDD)

**Totality**
- Given the canonical table, then every knob the run path resolves has an entry — a new
  knob added without a row fails the build.
- Given the table, then every entry with a flag appears in `reserved_flag_names`, and
  every value-taking flag appears in `flag_takes_value`. **This fails today** for
  `dagr.metastore-store`, which is absent from `flag_takes_value` (T114 fixes it; this
  ticket makes the absence impossible to reintroduce).
- Given the table, then every entry appears in `arch.md`'s C26 table with matching
  default and validation, and the two row counts are equal. **This fails today** for
  `DAGR_METASTORE` and `DAGR_LOG_FORMAT`.

**Injectivity**
- Given the table, then no two entries share a `DAGR_*` name, a file key path, or a
  flag name.
- Given a deliberately duplicated key path in a test fixture, then the assertion fails
  with both offending entries named.

**Liveness**
- Given the table, then every entry is resolved by some code path — a row for a knob
  nothing reads fails the build.

**Behaviour is unchanged**
- Given every knob set via each of the three tiers in turn, then the resolved values
  are identical to before this ticket (a pure refactor at the resolution sites).
- Given nothing set, then event streams are byte-identical.

**The regression this ticket exists to prevent**
- Given a new knob added to the resolver but not the table (simulated in a test), then
  CI fails naming the knob — the `DAGR_METASTORE` drift cannot recur.

## Definition of done

- [ ] One canonical knob table exists in `dagr-cli`, carrying flag, env var, file key
      path, type, default, and validation for every knob.
- [ ] Resolution is derived from the table (or asserted against it with a liveness
      check that makes divergence impossible).
- [ ] CI asserts totality, injectivity, and liveness, each with a failing fixture
      proving the assertion bites.
- [ ] CI asserts the table matches `arch.md`'s C26 table row for row, including row
      count.
- [ ] Every flag in the table is in `reserved_flag_names`; every value-taking flag is in
      `flag_takes_value`.
- [ ] Resolved values are unchanged across all tiers; event streams are byte-identical
      with nothing set.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **A Rust table or a checked-in data file?** A data file would be loadable at runtime
  and the repo has a standing rejection of exactly that shape — ADR 005 refused a
  machine-readable criteria matrix because "a data file invites a consumer to load it at
  runtime and grow it into a registry." The same reasoning applies here, so the table
  should be **Rust** (a `const` slice the resolver consults), with the `arch.md` table as
  its reviewed human mirror and CI asserting they agree. Recorded in-PR.
- **Is derivation or assertion the right coupling?** Derivation makes drift impossible
  but may not fit every knob (the pool pins are tri-state; the banner toggle reads two
  variables including a non-`DAGR_` one). The likely answer is derivation for the
  uniform knobs and an explicit, commented exception list for the two irregular ones,
  with liveness covering both. Decided in-PR.

## Out of scope

- Adding, removing, or renaming any knob — this ticket describes what exists.
- Adding `DAGR_STORE`, fixing `flag_takes_value`, or the duration bounds — **T114**.
- The loader and the `file` tier — **T115**; the error discriminator — **T116**.
- The acceptance gate — **T118**.
- A runtime-loadable configuration registry, which is what ADR 005 rejected and what a
  data-file table would drift toward.
- Scope boundary restated: a checked table of existing knobs adds no capability and no
  coordination; dagr remains not a scheduler, a *distributed* execution system beyond
  ADR 115's carve-out, a *coordinating* metadata store, a web interface, a DSL, or a
  backfill orchestrator, and the graph's shape never changes at runtime.
