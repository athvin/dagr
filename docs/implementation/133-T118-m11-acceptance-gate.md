# 133 · T118 — M11 acceptance gate: purity, zero-dep core, and graph-is-code

> **Milestone:** M11 · **Size:** M · **Type:** feature (gate) · **Components:** system-level
> **Branch:** `feat/t118-m11-acceptance-gate` · **Depends on:** T113–T117 · **Blocks:** —

## Why / context

M11 added a configuration file to a project whose specification says the graph is code
and whose acceptance criteria include producing artifacts "in an empty environment with
**no configuration present**." The carve-out ADR 128 obtained is narrow — a file that
configures *how one invocation runs* — and the only thing that keeps it narrow after
the ADR merges is a test that fails when it widens. That is this ticket, in the same
role T88 played for the metastore and T112 for remote execution.

Three invariants are load-bearing, and each has a specific way of being broken quietly:

- **Purity.** A loader that searches for `dagr.toml` from the current directory is one
  refactor away from being called during assembly, at which point the graph depends on
  ambient state and `crates/core/tests/determinism_and_purity.rs` and C20 both become
  false. The gate must prove assembly is *indifferent* to the file, not merely that the
  purity test still passes on a machine with no file.
- **Zero-dep core.** A TOML parser in `dagr-cli` is fine; the same parser reachable from
  `dagr-core` breaks the commitment that additions to core's dependency set are reviewed
  as API decisions.
- **Graph-is-code.** The file must have no key that selects a flow, declares a node or
  edge, or alters node policy. This is the one that would be *convenient* to violate —
  a `flow = "nightly"` key is an obvious ergonomic win and the exact first step toward
  the DSL the non-goals permanently exclude.

The gate also owes a debt from M10: the test named
`no_server_database_or_scheduler_is_required` now reads as narrower than the criterion
it covers, since ADR 115 amended criterion 7 to scope the "requires" half to the
default executor. Renaming it belongs with the gate that owns it.

## Objective

Prove M11's invariants structurally, and finish the documentation surface.

- Add an **M11 acceptance-gate script** in the style of
  `scripts/check-metastore-acceptance-boundary.sh`, asserting every invariant below,
  wired into CI.
- **Purity, actively:** assert assembly is indifferent to the file — a `dagr.toml`
  present in the working directory leaves the graph artifact byte-identical and the
  graph fingerprint unchanged, and no file read occurs during assembly.
- **Zero-dep core:** assert via `cargo tree` that no TOML parser is reachable from
  `dagr-core`, and that its `--no-default-features` runtime dependency set is empty.
- **Graph-is-code:** assert the file has **no** key that selects a flow or reaches node
  policy, and that a `dagr.toml` cannot change any node's policy hash or the structural
  fingerprint.
- **Default path unchanged:** assert that with no file and no environment, a run's event
  stream is byte-identical to a pre-M11 run.
- Rename `no_server_database_or_scheduler_is_required` to match the amended criterion 7,
  updating the coverage-matrix row that names it.
- Document the file in the **cookbook** with a worked `dev` / `prod` example that is the
  place the two executors get explained to an operator — the motivation `arch.md` now
  states, made concrete — plus a docs-claims test in the style of
  `crates/cli/tests/metastore_docs_claims.rs`.

## Test plan (write these first — TDD)

**Purity — assembly is indifferent to the file**
- Given a `dagr.toml` present in the working directory, then
  `crates/core/tests/determinism_and_purity.rs` passes and the graph artifact is
  **byte-identical** to one produced with no file present.
- Given a `dagr.toml` present, then the **structural fingerprint** and every node's
  **policy hash** are unchanged.
- Given assembly, then no configuration file is opened (asserted by pointing discovery
  at a path whose read would fail).
- Given a graph artifact emitted with a file present, then C20's "empty environment"
  criterion still holds.

**Zero-dep core**
- `cargo tree -p dagr-core -e normal --no-default-features` shows an **empty** runtime
  dependency set.
- `cargo tree -i <toml crate>` shows no path to `dagr-core`.
- `cargo build --all` and `--no-default-features` succeed.

**Graph-is-code — the convenient violation**
- Given a `dagr.toml` containing a `flow` key, then it is **rejected as an unknown key**
  — the file cannot select which DAG runs.
- Given a file attempting to set a node's placement or any policy field, then it is
  rejected as an unknown key.
- Given any valid file, then the set of nodes and edges in the graph artifact is
  identical to a run with no file.
- A grep-style assertion confirms no key in the canonical knob table (T117) reaches node
  policy or flow selection.

**Precedence end to end**
- Given all four tiers supplying the same knob, then flag > env > file > default holds,
  asserted for at least one knob of each type (duration, enum, u64, f64, bool, path).
- Given a profile layering over `default`, then unspecified keys fall through.
- Given an unknown profile, then it fails loudly.

**Default path unchanged**
- Given no file and no environment variables, then a run's event stream is
  byte-identical to a pre-M11 run, and the quickstart and examples are unchanged.

**Docs are true**
- The cookbook's `dev`/`prod` example is a compiled or CI-executed artifact, not prose.
- A docs-claims test asserts the cookbook's and `arch.md`'s claims about defaults,
  discovery, precedence, and the absence of graph keys.
- The coverage matrix's SL7 row names the renamed test, and no row count changes.

## Definition of done

- [ ] An M11 acceptance-gate script asserts purity, zero-dep core, graph-is-code, and
      the unchanged default path, and runs in CI.
- [ ] Assembly is proven **indifferent** to a present `dagr.toml`: identical artifact,
      identical fingerprints, no file read.
- [ ] No TOML parser is reachable from `dagr-core`; its runtime dependency set is empty.
- [ ] A `flow` key and any policy/placement key are rejected as unknown; nodes and edges
      are unaffected by any valid file.
- [ ] Four-tier precedence is asserted end to end across every value type; profile
      layering and unknown-profile failure are asserted.
- [ ] With no configuration, event streams are byte-identical to pre-M11.
- [ ] `no_server_database_or_scheduler_is_required` is renamed to match criterion 7, and
      the coverage-matrix row that names it is updated; no row count changes.
- [ ] The cookbook documents the file with a CI-exercised `dev`/`prod` example, and a
      docs-claims test guards its claims.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **How is "no file read during assembly" asserted?** Options: point discovery at a path
  that is a directory (so opening errors), use a permission-denied path, or assert via a
  counter in a test build. The requirement is that the assertion **fails** if a future
  refactor moves the read into assembly; the mechanism is decided in-PR against whatever
  T115's discovery actually does.
- **Does the gate need a new numbered `arch.md` criterion?** M11 amends C26 prose and
  adds no new component. If the gate's purity-indifference assertion is worth a numbered
  criterion, it owes a criteria-matrix and coverage-matrix row; the default is **no new
  criterion** (it strengthens C20 and C7's existing purity criteria rather than adding
  one), recorded in-PR either way so the matrices never drift silently again.

## Out of scope

- Any mechanism work — T113–T117 own it. A gate failure is fixed in the owning ticket,
  or reopens ADR 128 if it contradicts a decision.
- New knobs, new tiers, or a user-level configuration path beyond what T115 shipped.
- Anything that would let the file describe the graph — the gate's job is to make that
  impossible, not to scope it.
- Re-litigating ADR 128's carve-out width.
- Scope boundary restated: the gate exists to hold the carve-out at exactly the width
  ADR 128 granted — a bootstrap-read file of runtime knobs, no graph keys, no runtime
  registry, core reading nothing. dagr remains not a scheduler, a *distributed* execution
  system beyond ADR 115's carve-out, a *coordinating* metadata store, a web interface, a
  DSL, or a backfill orchestrator, and the graph's shape never changes at runtime.
