# Quality gates by era

Read by the orchestrator once per session and by every implementer/fixer.
The scripts implement these definitions; this file is the rationale and the
recipes the scripts cannot encode.

## Contents

1. [Era table](#1-era-table)
2. [Gate commands per era](#2-gate-commands-per-era)
3. [Criteria-matrix duty (era ci)](#3-criteria-matrix-duty-era-ci)
4. [CI polling recipes](#4-ci-polling-recipes)
5. [Required tools](#5-required-tools)

## 1. Era table

| Era | Tickets | Repo state | Gate |
|---|---|---|---|
| `pre-workspace` | 001–002 | No `Cargo.toml` anywhere (ticket 001 forbids creating crates) | Scripted file/content assertions from the tickets' Test plans; no cargo commands exist |
| `pre-ci` | 003–005 | Workspace exists, no `.github/workflows/` | Full local cargo substitute under the pinned toolchain |
| `ci` | 006+ | CI workflow exists (or the current PR adds it) | Local gate is a dry run; real CI on the PR is authoritative |

- Boundaries are **static by NNN**. This is sound because ticket numbering is a
  topological order and the loop ships tickets serially — the repo state each
  era assumes is guaranteed by the tickets before it.
- `run_gate.sh` asserts the observed repo state matches the declared era at
  gate time and exits with `ANOMALY` on mismatch (hand-edited README, wrong
  branch). An ANOMALY is a stop, not a retry.
- In `pre-workspace`, every ticket's "CI is green" Definition-of-done line is
  satisfied vacuously through its own "where configured" clause — there is
  nothing to configure yet, and no cargo command can run.
- Ticket **003** creates the workspace mid-ticket: its `pre-ci` gate applies to
  the branch **end state** (Cargo.toml must exist when the gate runs).
- Ticket **006** is polled as era `ci`: `pull_request` events run workflow
  files from the PR head, so 006's own PR exercises the CI it adds.

## 2. Gate commands per era

`pre-workspace` (`run_gate.sh pre-workspace <nnn>`): mechanical assertions
derived from tickets 001/002 — no crate leaked (`Cargo.toml`/`Cargo.lock`/
`*.rs` absent), hygiene files exist (`rust-toolchain.toml` with rustfmt+clippy
components, `rustfmt.toml`, `LICENSE`, `.editorconfig`), README carries the
MSRV line and the permanent non-goals, `git check-ignore` probes for generated
paths, and (002) `CONTRIBUTING.md` contract, PR template, `CODEOWNERS`,
README cross-links.

`pre-ci` and `ci` (`run_gate.sh <era>`), in order:

```
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps   # rustdoc lint default
cargo audit        # only if cargo-audit and Cargo.lock exist, else SKIP:named-reason
cargo deny check   # only if cargo-deny and deny.toml exist, else SKIP:named-reason
```

**Hand-off rule:** these are defaults. Once `CONTRIBUTING.md` or the CI
workflow defines an exact command (the rustdoc lint especially), the repo
definition is authoritative — if `run_gate.sh` diverges from it, fix the
skill's script to match the repo, never the other way around.

## 3. Criteria-matrix duty (era ci)

- Ticket 006 lands a checked-in acceptance-criteria coverage matrix and a
  verification script. From then on, every component ticket must map each
  newly-satisfied arch.md criterion to its test id in the matrix.
- `unmapped` is allowed only until the mapped test ships.
- Run the matrix-verification script locally before pushing — `run_gate.sh ci`
  probes conventional locations (`scripts/`, `ci/`) and prints
  `CHECK:criteria-matrix=SKIP:...` loudly if it cannot find one.
- Skipped matrix updates compound silently and explode at ticket 080 (the
  gate requires every machine criterion mapped to an existing, passing test).
  Pay the debt per ticket.

## 4. CI polling recipes

`ci_status.sh <pr> [--wait <seconds>] [--grace <seconds>]` prints `KEY=VALUE`
lines and a final `VERDICT=` line. Exit codes: 0 `PASS` · 1 `FAIL` ·
2 `PENDING` · 3 `NO_CHECKS` · 4 `ANOMALY`.

- `NO_CHECKS` is **normal** before ticket 006 lands — the local gate is the
  merge gate in `pre-workspace`/`pre-ci`. In era `ci`, `NO_CHECKS` after the
  grace window means the workflow is broken: run the diagnosis recipe once
  (`gh run list --branch <branch>`, `gh workflow list`, check the workflow's
  triggers/paths/YAML), then stop.
- Registration grace: `statusCheckRollup` can be empty for a couple of minutes
  after a push while GitHub registers the run. Use `--grace 300` for ordinary
  era-`ci` PRs and `--grace 600` for ticket 006's PR (the first-ever workflow
  run registers slowest).
- Wait budget: `--wait 1800` (post-006 CI is a multi-job cargo matrix; 30
  minutes covers a cold-cache run). On `PENDING` at the cap, wait **one**
  additional 1800s cycle before stopping. The 3-OS matrix from ticket 077 and
  the benchmarks from 076 may need a bigger budget — revisit the numbers when
  those tickets land.
- On `FAIL` the script prints the `FAILING=` check names and a `RUN_ID=`. Hand
  both to the fixer; the fixer fetches logs itself with
  `gh run view <run-id> --log-failed` — logs never transit the orchestrator.
- **Failure fingerprint** = the sorted `FAILING=` names plus the first error
  line of each failing check's log. An identical fingerprint on two
  consecutive fix rounds = thrash = stop.

## 5. Required tools

- Always: `git`, `gh` (authenticated; the `workflow` OAuth scope must be
  present before ticket 006 — pushing `.github/workflows/` files is rejected
  without it), `python3`.
- From era `pre-ci` onward: `rustup` (the pinned toolchain from
  `rust-toolchain.toml` is picked up automatically), plus the `cargo-audit`
  and `cargo-deny` binaries — install locally with
  `cargo install --locked cargo-audit cargo-deny` if missing.
- Later tickets add their own tools (trybuild via Cargo at 007, possibly
  tarpaulin per CI definitions) — the ticket text governs.
- Never install globally beyond cargo's own user directories.
