# Contributing to dagr

This document is the shared **process contract** every dagr change ships under:
one branch and one pull request per implementation ticket, tests written first,
and a fixed merge gate of CI checks that must be green before anything lands.
The rule is written down here — version-controlled and reviewed like code — so
it is enforced socially and mechanically, not by folklore.

It is the human-readable half of the process. The machine half — the GitHub
Actions workflows and the acceptance-criteria coverage matrix that enforce the
merge gate — is authored by ticket **T7**
([`docs/implementation/006-T7-ci-pipeline-and-criteria-matrix.md`](docs/implementation/006-T7-ci-pipeline-and-criteria-matrix.md)).
Until then, run the checks locally before opening a PR.

If you are new here, start from the [`README`](README.md), read
[`docs/arch.md`](docs/arch.md) for what dagr is (and, permanently, is not), and
pick a ticket from [`docs/implementation/README.md`](docs/implementation/README.md).

## Branch per ticket

Every implementation ticket lives at
`docs/implementation/NNN-TID-slug.md` and carries a two-line header. The second
line has a **`Branch:`** field. That is the branch you work on — and there is
**exactly one branch per ticket**.

- The branch name is copied **verbatim** from the ticket header's `Branch:`
  field: never re-slugged, re-prefixed, or normalized. Prefix conventions
  (`chore/`, `adr/`, `feat/`) are already baked into the ticket, so you copy,
  you do not re-derive.
- **Worked example.** This very ticket — T0.0b, at
  [`docs/implementation/002-T0.0b-contributor-and-branch-workflow.md`](docs/implementation/002-T0.0b-contributor-and-branch-workflow.md)
  — has `Branch: chore/t0.0b-contributor-and-branch-workflow`, so its one branch
  is `chore/t0.0b-contributor-and-branch-workflow`, character for character.

Do not open a second branch for the same ticket, and do not fold two tickets
into one branch. One ticket, one branch.

## One PR per ticket

Each branch produces **exactly one pull request**, and that PR maps to **one
implementation ticket**.

- The PR **links its ticket** two ways: by **`tid`** (the task id from
  [`docs/tasks.md`](docs/tasks.md), e.g. `T0.0b`, `T7`, `T54a`) and by its path
  under `docs/implementation/` (e.g.
  `docs/implementation/002-T0.0b-contributor-and-branch-workflow.md`). Both,
  because the `tid` and the `NNN` file number are different namespaces and a
  reviewer needs to reach the ticket unambiguously.
- The PR **restates the acceptance criteria** it claims to satisfy (from the
  ticket's Definition of done) and shows the tests that exercise them.
- The PR is **not merged until review has happened and CI is green** — every
  check in the merge gate below must pass first.

## Tests first (non-negotiable)

dagr is written test-first, and this is a **hard rule**, not an aspiration:

- The ticket's **Test plan** — and the failing tests it describes — is **written
  before** any implementation. The failing-tests commit lands first; you watch
  the tests fail, then implement until they are green in later commits.
- **Every PR demonstrates tests exercising the acceptance criteria it claims to
  satisfy.** A PR that adds behavior without a test that pins it does not merge.
- This applies to documentation and scaffolding tickets too: their "tests" are
  concrete, independently-checkable assertions about the files that must exist
  and what they must contain (see this ticket's own
  [`scripts/check-contributor-workflow.sh`](scripts/check-contributor-workflow.sh)
  and T0.0a's [`scripts/check-hygiene.sh`](scripts/check-hygiene.sh)).

Skipping tests-first, or writing tests after the fact to match what the code
happened to do, defeats the entire point of a compile-time-confident tool. It is
**mandatory**.

## The merge gate (CI checks that must be green)

No PR merges until **all** of the following are green. This list matches the
posture in [`docs/arch.md`](docs/arch.md) ("Stability": supply chain;
"Documentation": rustdoc in CI) and the lint policy in
[`docs/lint-policy.md`](docs/lint-policy.md). **T7** implements these as GitHub
Actions checks; the same commands reproduce the gate locally today.

| Check | Command | What it enforces |
|---|---|---|
| Formatting | `cargo fmt --check` (`cargo fmt --all --check`) | Code matches [`rustfmt.toml`](rustfmt.toml). |
| Lints | `cargo clippy` **with warnings denied** (`cargo clippy --workspace --all-targets -- -D warnings`) | The warnings-denied deny set in [`docs/lint-policy.md`](docs/lint-policy.md); nothing ships carrying a warning. |
| Test suite | `cargo test` (`cargo test --workspace`) | The tests, including the ones this PR added, pass. |
| Rustdoc lint | `cargo doc` with `RUSTDOCFLAGS="-D warnings"` (`--workspace --no-deps`) | Rustdoc on public items, no broken intra-doc links (arch.md "Documentation"). |
| Supply chain | `cargo audit` and `cargo deny` **where configured** | Advisories, and licenses/sources/advisories, per arch.md "Stability": Supply chain. `cargo deny`'s license allow-list target is `MIT` (see [`docs/lint-policy.md`](docs/lint-policy.md)). |

`cargo audit` and `cargo deny` run where they are configured — they need a
`Cargo.lock` and a `deny.toml`, which arrive with the workspace (T1) and the CI
pipeline (T7). Until a check's tooling exists it is skipped, loudly, never
silently dropped. When the compile-failure / UI-test harness (T8) and its
pinned-toolchain policy exist, those checks **join this gate** too.

The wording of "CI green" here is identical to every ticket's Definition-of-done
final line: *fmt, clippy with warnings denied, tests, rustdoc lint, and
cargo-audit/deny where configured.*

## Review and merge

Every PR requires review before merge. Review ownership is assigned in
[`.github/CODEOWNERS`](.github/CODEOWNERS), which covers the whole repository so
any PR — code or docs — reaches an owner. Branch-protection rules that *enforce*
required reviews are configured in the GitHub UI/API by the operator, outside
this repository; see
[`docs/implementation/DEVIATIONS.md`](docs/implementation/DEVIATIONS.md) for the
recorded standing exception where the operator runs the loop with autonomous
merge.

Open your PR with the template at
[`.github/pull_request_template.md`](.github/pull_request_template.md); it
carries the ticket link, the restated criteria, and the Definition-of-done
checklist.

## Scope boundary

dagr is, **permanently**, **not** a scheduler, a distributed execution system, a
metadata store, a web interface, a domain-specific language, or a backfill
orchestrator — and **the graph's shape never changes at runtime**. This process
contract governs contribution only; it does not — and no contribution may —
reintroduce any of those as capability. Every ticket restates this boundary in
its Out of scope section, and diffs that cross it fail review by design. See
[`docs/arch.md`](docs/arch.md) for the full component specification and the
normative Vocabulary (terminal states, state classes, trigger rules).
