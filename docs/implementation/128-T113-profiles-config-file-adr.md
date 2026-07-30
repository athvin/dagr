# 128 · T113 — ADR: named-profile configuration file and the `file` precedence tier

> **Milestone:** M11 · **Size:** S · **Type:** decision · **Components:** C12, C26
> **Branch:** `adr/t113-profiles-config-file-adr` · **Depends on:** — · **Blocks:** T114

## Why / context

M10 gave dagr two executors. The operator's reason for wanting them is a workflow:
**iterate locally at full speed until the pipeline is provably correct, then give
each task the infrastructure its work actually needs.** Today that switch is a
scatter of individual flags and environment variables with no way to name a coherent
set of them, so "run this the way we run it in production" is something an operator
reconstructs by hand — or gets wrong.

The ask is the shape dbt and Airflow both settled on: a **configuration file with
named profiles** (`dev`, `prod`), whose values any environment variable can override,
whose overrides any command-line flag can override. dbt's `profiles.yml` supplies the
naming model; `airflow.cfg` plus its env-var override convention supplies the
precedence model. dagr already has the top two tiers (`flag > env > default`,
ADR 089); this adds one tier underneath and a way to name a set.

**This crosses a stated boundary, and the boundary turns out to be thinner than the
repo's own boilerplate implies.** An audit of every site found:

- The **spec-level** prohibition is *narrow and survives*: `arch.md` and both crate
  READMEs forbid "no configuration file **describing the graph** / the shape." The
  permanent non-goals list (`arch.md` "What this is") contains **"a domain-specific
  language"** and **no mention of a configuration file at all**.
- The **unqualified** prohibition exists in exactly two places, both descending from
  ADR 089: `arch.md` C26's closing clause "there is no config file or DSL (a permanent
  scope boundary)", and ADR 089's rejected alternative. `ADR 091` restates the latter.
- **ADR 089's rejection misquotes its own authority.** It reads: "**A config file /
  DSL.** Out of the permanent scope boundary (arch.md: dagr decides neither *when* to
  run nor via a config surface)." **`arch.md` contains no such sentence.** What
  `arch.md` C26 actually says is the *opposite polarity* — it calls the existing
  flag/env layer "a config *surface* only: it changes how a single invocation is
  configured, never *when* a pipeline runs." The real cited boundary is "Something
  outside this tool decides *when* a pipeline runs," which a file of runtime knobs
  does not approach.

So the decision to make is narrow: a file that configures **how one invocation runs**
is inside the spec boundary and was excluded by an over-broad ADR sentence resting on
a misattribution. A file that describes **what the graph is** stays permanently
excluded. **This ticket owns that amendment; it ships no code.**

One more finding shapes the milestone rather than the ADR: **the existing env tier is
inert on the shipped path.** `crates/cli/src/registry.rs` builds
`RunConfig::new(base).run_id(run_id)` and calls none of the env-fallback builders;
there are **zero** non-test callers of `grace_from_env`,
`teardown_deadline_from_env`, `failure_mode_from_env`, `resolve_pool_pins`, or
`resolve_headroom`. A file tier added beneath a tier nothing consults would do
nothing at all — so wiring resolution into the run path is **T114**, and it lands
before the loader.

## Objective

Produce the ADR (written into this ticket file per ticket-conventions §6), amend the
two superseded sentences, and record these decisions:

- **Scope amendment.** Supersede ADR 089's "A config file / DSL" rejected
  alternative, `arch.md` C26's "there is no config file or DSL" clause, and ADR 091's
  restatement — for the **config-file half only**. "No DSL" stays. "No configuration
  file **describing the graph**" stays, verbatim, in all three places it appears.
- **A fourth precedence tier:** `flag > env > file(profile) > default`.
- **Named profiles**, selected by `--dagr.profile` / `DAGR_PROFILE`, with a `default`
  profile every other profile layers over.
- **Env names do not change.** The nine shipped `DAGR_*` variables keep their exact
  spellings; each maps to one file key path through a documented table CI proves total
  and collision-free. No `DAGR__SECTION__KEY`, nothing deprecated.
- **Profiles reach run-level knobs only** — never which flow runs, never a node's
  policy. The graph stays code.
- **Read at bootstrap, never during assembly**, so the purity guarantees hold.
- **`dagr-core` never reads the file**, exactly as it never reads the environment.
- **TOML**, and the parser lives in `dagr-cli` only.

## Test plan (write these first — TDD)

Decision ticket: the "tests" are mechanical file/content assertions.

- **ADR completeness.** All five sections — Status, Context, Decision, Consequences,
  Rejected alternatives — present, with Status `Accepted` and citing the dated operator
  acceptance recorded in Open questions.
- **Supersession recorded, narrowly.** ADR 089 and ADR 091 carry
  "Superseded (in part) by ADR 128 (T113)" notes naming the config-file clause
  **only**; no other text in either changes.
- **The narrow prohibitions survive, verifiably.** A grep confirms "describing the
  graph" / "describing the shape" still appear in `arch.md`, `README.md`, and
  `crates/core/README.md`, and that "domain-specific language" is still in the
  permanent non-goals list.
- **The misattribution is recorded**, so nobody re-derives the broad reading from
  ADR 089's text later.
- **The purity constraint is stated as binding**, naming
  `crates/core/tests/determinism_and_purity.rs` and criterion C20's
  "empty environment with no configuration present".
- **No code.** The diff touches only `docs/**`; no `crates/**`, no `Cargo.lock`.

## Definition of done

- [ ] This file contains an ADR with Status / Context / Decision / Consequences /
      Rejected alternatives capturing the decisions in Objective, with Status
      `Accepted` and the dated operator acceptance recorded.
- [ ] ADR 089 and ADR 091 are marked superseded-in-part for the config-file clause
      only; no other text in either file changes.
- [ ] `arch.md` C26's "there is no config file or DSL" clause is amended to permit a
      runtime-knob file and to keep "no DSL" and "no file describing the graph".
- [ ] The ADR records the four-tier precedence, profile selection and layering, the
      unchanged env spellings plus the mapping-table obligation, the run-level-only
      reach, the bootstrap-only read, the zero-dep-core boundary, and TOML.
- [ ] The ADR records the inert-env-tier finding and names **T114** as the fix that
      must land first.
- [ ] The diff is docs-only (no `crates/**`, no `Cargo.lock`).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Operator sign-off — RESOLVED, accepted (recorded per §5).** `arch.md` C26's
  clause self-describes as "a permanent scope boundary", so moving it would normally be
  a STOP under ticket-conventions §8/§10. The operator proposed this file and settled
  its design on 2026-07-29 (four-tier precedence, unchanged env spellings,
  run-level-only reach, dbt-style profiles with Airflow-style env override). Asked
  explicitly to accept the **boundary amendment itself** — this ADR's narrowing of the
  config-file prohibition plus the partial supersessions of ADRs 089 and 091 — the
  operator **accepted on 2026-07-29** ("ya I accept those"). The ADR is `Accepted`; the
  loop may ship it without halting. No other contested decisions.
- **File name and discovery order.** `dagr.toml` in the working directory is the
  obvious primary; whether a user-level fallback (`$XDG_CONFIG_HOME/dagr/config.toml`)
  and an explicit `--dagr.config <path>` are in the first cut is **T115**'s to settle
  against the purity constraint. Any search path must be inert during assembly.

## Out of scope

- Wiring the existing env tier into the run path, `DAGR_STORE`, the
  `flag_takes_value` fix, and ADR 089's unimplemented duration bounds — **T114**.
- The TOML loader, profile selection and layering, and the `file` tier — **T115**.
- `EnvParseError`'s source discriminator and strict `DAGR_LOG_FORMAT` — **T116**.
- The env↔key mapping table and its CI assertion — **T117**.
- The acceptance gate — **T118**.
- **Anything that would let the file describe the graph** — selecting a flow,
  declaring nodes or edges, overriding node policy or placement, or expressing
  conditionals. Permanently excluded, not deferred.
- Secret material in the file: credentials come from the ambient environment
  (ADR 115 §8). The file is expected to be committed to a repository.
- Scope boundary restated: a file of runtime knobs read at bootstrap configures **how
  one invocation runs** and nothing else — it coordinates nothing, hosts nothing, and
  describes no graph. dagr remains not a scheduler, a *distributed* execution system
  beyond ADR 115's carve-out, a *coordinating* metadata store, a web interface, a DSL,
  or a backfill orchestrator, and the graph's shape never changes at runtime.

---

# ADR: named-profile configuration file and the `file` precedence tier

> This repo keeps each ADR inside its own implementation-ticket file. This ADR is
> committed here, at
> `docs/implementation/128-T113-profiles-config-file-adr.md`, the ADR location for
> ticket T113 — satisfying ticket-conventions §6 with zero deviation. It amends
> `docs/arch.md` C26 and marks ADRs 089 and 091 superseded-in-part for one clause
> each; it ships **no code**.

## Status

**Accepted (2026-07-29).** A **decision** ticket: it moves a clause that calls itself
a permanent scope boundary, and ships **no production code**. The only artifacts are
this ADR, the `arch.md` C26 amendment, and two partial-supersession notes.

**Operator acceptance is recorded, not pending.** `arch.md` C26 says "there is no
config file or DSL (**a permanent scope boundary**)", so moving it would normally be a
STOP under ticket-conventions §8/§10. The operator asked for this file, settled its
design on 2026-07-29 (four-tier precedence, unchanged `DAGR_*` spellings, dbt-style
named profiles with Airflow-style environment override, run-level-only reach), and —
presented with the explicit statement that this ADR and ADR 115 each awaited a dated
acceptance line for the boundary it moves — **explicitly accepted both on
2026-07-29** ("ya I accept those"), recorded in this ticket's `## Open questions` per
ticket-conventions §5. The acceptance covers the `arch.md` C26 amendment and the
partial supersessions of ADRs 089 and 091. This ADR is therefore **Accepted** and
ships through the normal branch/PR/merge flow without halting.

**No spike.** Nothing here needs measuring. The one real risk — that a file read at a
conventional path breaks the empty-environment purity guarantees — is addressed by
**decision** (§5: bootstrap-only) and proven by an existing test the milestone must
keep green, not by new exploration.

## Context

ADR 089 gave dagr `flag > env > default` across its runtime knobs and, in its rejected
alternatives, closed the door on a configuration file:

> **A config file / DSL.** Out of the permanent scope boundary (arch.md: dagr decides
> neither *when* to run nor via a config surface). Rejected; flags + env + defaults are
> the whole model.

`arch.md` C26 mirrors it: "there is no config file or DSL (a permanent scope
boundary)." ADR 091 restates it. Those three sentences are the entire prohibition.

**Two things about that text matter.**

First, **it misquotes its own authority.** The parenthetical attributes to `arch.md`
the claim that dagr decides nothing "via a config surface". No such sentence exists.
What `arch.md` C26 says — in the same paragraph the clause ends — is the opposite
polarity: the flag/env layer **is** "a config *surface* only: it changes how a single
invocation is configured, never *when* a pipeline runs." The genuine cited boundary is
"Something outside this tool decides *when* a pipeline runs," which a file of runtime
knobs does not touch: the file cannot start a run, schedule one, or advance an
interval.

Second, **the spec-level boundary is narrower and unaffected.** Everywhere the
*specification* forbids a config file, it is qualified — "no configuration file
**describing the graph**" (`arch.md`), "no config file **describing the graph**"
(`README.md`), "no config file **describing the shape**" (`crates/core/README.md`) —
and the permanent non-goals list names "a domain-specific language" while never
mentioning a configuration file. The qualifier is doing real work: it already
anticipates that a *non-graph* file might exist.

So the distinction that makes this carve-out sound is the same shape ADR 097 and ADR
115 used, applied to a third term: **what the file describes.** A file that describes
the **graph** — nodes, edges, conditionals, which tasks exist — would be the DSL the
non-goals permanently exclude, and would destroy the property the whole product rests
on (the compiler has already checked the graph). A file that describes **how one
invocation runs** — pool sizes, executor choice, store paths, blob backend — is the
same category of thing as the flags and environment variables that already exist. It
adds no expressive power; it names a set of values an operator can already supply one
at a time.

M10 is what makes this urgent rather than cosmetic. There are now enough
environment-shaped knobs — executor, max-pods, blob backend, bucket, endpoint,
namespace, pool pins, headroom — that "run it the way production runs it" is a dozen
flags an operator retypes and eventually mistypes.

### One finding that reorders the milestone

The env tier ADR 089 specified is **not wired into the shipped run path.**
`registry.rs` constructs `RunConfig::new(base).run_id(run_id)` and calls none of the
opt-in env-fallback builders; there are **zero** non-test callers of
`grace_from_env`, `teardown_deadline_from_env`, `failure_mode_from_env`,
`resolve_pool_pins`, or `resolve_headroom`. This is consistent with ADR 089's design
(the builders are deliberately opt-in so `RunConfig::new` stays infallible and
env-free) but it means `arch.md`'s "**every** runtime knob honours the standard
precedence" describes an *available surface*, not the reference binary's behaviour —
a claim the M10 truth pass corrects.

The consequence for this milestone is concrete: **a file tier beneath a tier nothing
consults would resolve nothing.** Wiring resolution into the run path is **T114** and
lands before the loader.

## Decision

Six decisions.

### 1. Scope amendment (arch.md C26 amended; ADRs 089 and 091 superseded in part)

`arch.md` C26's clause is amended to permit **a configuration file of runtime knobs,
read at bootstrap, that configures how a single invocation runs**. The rest of the
clause stands: **"no DSL"** is unchanged, and **"no configuration file describing the
graph"** is unchanged everywhere it appears. ADR 089's "A config file / DSL" rejected
alternative and ADR 091's restatement each carry a "Superseded (in part) by ADR 128
(T113)" note covering the config-file half only; the DSL half of both stands, and no
other text in either file changes.

### 2. Four tiers: `flag > env > file(profile) > default`

One tier is inserted beneath the environment. A flag still wins outright; an
environment variable still beats the file; the file beats the compiled default. The
existing `resolve` helper grows the tier; its flag-wins-without-reading-the-env
property is preserved.

`resolve_opt`'s **tri-state** is preserved exactly: the three pool pins must keep
distinguishing *pinned* from *absent, so detect from the host*. A file that supplies
no value for a pool must leave it unpinned, not pin it to a default.

### 3. Named profiles, with a `default` every profile layers over

A profile is a named table of knob values. `--dagr.profile <name>` selects one;
`DAGR_PROFILE` is its environment fallback; absent, the `default` profile is used
alone. A named profile **layers over** `default` key by key, so a profile states only
its differences.

```toml
[default.pool]
memory = "8GiB"
headroom-fraction = 0.2

[dev]
executor = "local"

[prod]
executor = "k8s"
max-pods = 50
```

Profile *names* are operator-chosen and carry no semantics — `dev` and `prod` are
conventions, not keywords. An unknown profile name is a **loud failure**, never a
silent fallback to `default`.

### 4. The environment spellings do not change

The nine shipped `DAGR_*` variables keep their exact names. Each maps to exactly one
file key path through a **documented table** that CI asserts is total (every knob
appears) and injective (no two knobs share a variable or a key path) — **T117**.

Rejected: deriving `DAGR__SECTION__KEY` from the key path. It would give every
existing knob a second spelling and a deprecation cycle, for a mechanical convenience
that a checked table provides without breaking anything.

### 5. Read at bootstrap; never during assembly

The file is read in the **bootstrap** phase — the one place `arch.md` designates as
deliberately impure, where "everything the runtime needs from the actual machine and
the actual invocation happens." It is **never** read during assembly.

This is a hard constraint with a test behind it, not a stylistic preference.
`crates/core/tests/determinism_and_purity.rs` runs a real assembly in a child process
with the environment cleared and the working directory pointed at an empty temp dir,
asserting it succeeds; C20's acceptance criterion requires a graph artifact to be
"produced in an empty environment with **no configuration present**." A file consulted
at a conventional path during assembly breaks both, and would make the graph depend on
ambient state — the precise failure the purity test exists to catch. **Assembly must
behave identically whether or not a `dagr.toml` exists.**

### 6. Boundaries kept

- **`dagr-core` never reads the file**, exactly as it never reads the environment. The
  CLI parses and passes already-parsed values inward; core's runtime dependency set
  stays empty and its "reads the host once, injectable for tests" property is intact.
- **TOML**, parsed in `dagr-cli` only. TOML because the workspace is already
  TOML-configured and the format is unambiguous about types.
- **Run-level knobs only.** A profile may not select which flow runs (argv owns that),
  may not declare or alter nodes, edges, or node policy, and may not override
  placement. Placement stays typed Rust that the policy hash observes; a profile that
  could change it would make the policy hash depend on an untracked file and turn an
  edited TOML into a resume diff.
- **Every knob keeps its reserved flag.** The invariant that a knob with an
  environment fallback also has a reserved library flag extends to the file: a
  file-only knob is not introduced.
- **Loud failures.** An unparseable file, an unknown profile, an unknown key, or a
  bad value fails at bootstrap naming the file, the profile, and the key — the same
  never-silent posture the environment tier already commits to.

## Consequences

- **The boundary is now open — and only this far.** A file may set the values an
  invocation runs with. It may not describe the graph, and no later ticket may grow it
  toward that without its own decision ticket.
- **Each M11 ticket inherits a named seam:** **T114** (wire the existing env tier into
  the run path, add `DAGR_STORE`, fix `flag_takes_value`, settle ADR 089's
  unimplemented duration bounds); **T115** (the loader, discovery, profile layering,
  the `file` tier, §2/§3/§5); **T116** (`EnvParseError`'s source discriminator and
  strict `DAGR_LOG_FORMAT`, §6); **T117** (the mapping table and its CI assertion,
  §4); **T118** (the acceptance gate).
- **`arch.md`'s precedence claim becomes true for the first time.** T114 wires
  resolution into the run path, so "every runtime knob honours the standard
  precedence" stops being a statement about an unused library surface.
- **A diagnostic type has to grow.** `EnvParseError` names an environment variable in
  its field and hardcodes "environment variable" in its `Display`. A file value
  resolved through it would produce a factually wrong message, so it gains a source
  discriminator (flag / env / file+profile+key) — **T116**.
- **The purity tests gain a new job.** They must additionally prove assembly is
  unaffected by a `dagr.toml` **present** in the working directory. That is a
  strengthening of an existing guarantee, and the gate asserts it (**T118**).
- **One new dependency, in the CLI only.** A TOML parser enters `dagr-cli`'s tree, not
  `dagr-core`'s. `deny.toml` grows accordingly.
- **Profiles are a documentation surface too.** The dev/prod pair is how the two
  executors get explained to an operator, so the cookbook gains a worked example
  rather than only a reference table.
- **Reopen condition.** If the file cannot be read at bootstrap without perturbing
  assembly — i.e. the purity guarantee and the file cannot coexist — then §5 reopens
  **here**, not locally. A local workaround that reads the file during assembly is a
  defect, not a fix.

## Rejected alternatives

- **A config file that can describe the graph** (nodes, edges, conditionals, or which
  flow to run). **Rejected on the permanent boundary, unchanged.** This is the DSL the
  non-goals exclude and the thing "no configuration file describing the graph" names.
  dagr's entire premise is that the compiler has already checked the graph; a file
  that shapes it would move graph errors from compile time to run time. Not a later
  ticket.
- **Keeping ADR 089's blanket rejection.** **Rejected as over-broad and
  mis-founded:** its stated rationale attributes to `arch.md` a claim `arch.md` does
  not make, and the sentence it *does* make ("a config *surface* only… never *when* a
  pipeline runs") is satisfied by a runtime-knob file. The narrow, spec-level
  prohibition it should have cited — no file describing the graph — is kept verbatim.
- **`DAGR__SECTION__KEY` environment names derived from the file structure**
  (Airflow's convention). **Rejected on compatibility cost:** it would give all nine
  shipped variables a second spelling plus a deprecation window, to save a lookup that
  a CI-checked mapping table provides for free.
- **YAML** (dbt's format). **Rejected on ecosystem fit:** the workspace is already
  TOML-configured, TOML is unambiguous about types where YAML is famously not, and a
  YAML parser is a heavier dependency for a file of scalars.
- **Reading the file during assembly**, so a flow could consult configuration as it is
  built. **Rejected on purity, which is machine-checked:** assembly is pure — no
  network, no filesystem, no clock, no credentials — and C20 requires artifact
  emission in an empty environment with no configuration present. This would also make
  the graph depend on ambient state, defeating the determinism guarantees.
- **Letting a profile select the flow, or override node placement.** **Rejected on
  "the graph is code":** flow selection is argv's, and placement is node policy the
  policy hash observes. A file-driven placement override would make an edited TOML
  produce a resume policy diff, and would split node policy across two sources of
  truth.
- **A file-only knob with no flag and no environment variable.** **Rejected on the
  reserved-namespace invariant:** every knob that can be set out-of-band has a
  reserved library flag a pipeline parameter cannot shadow. A file-only knob would be
  the first exception and would silently weaken that guarantee.
- **Secrets in the file.** **Rejected:** the file is expected to live in version
  control. Credentials come from the ambient environment (ADR 115 §8), and the file
  references buckets and endpoints, never tokens.

*(Operator acceptance of the boundary amendment is RECORDED — dated 2026-07-29 in
§Status and this ticket's §Open questions, per ticket-conventions §5. Reopen condition
stated in §Consequences.)*
