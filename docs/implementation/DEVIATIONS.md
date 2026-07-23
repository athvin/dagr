# Deviations

Deliberate departures from a ticket's Definition of done are recorded here, one
entry each, with: date, ticket, the quoted DoD line, the deviation, its
rationale, and the operator decision it traces to. A matching note goes in the
PR body. Merged decision text elsewhere is never rewritten; this file is the
audit trail for where reality diverges from a DoD line on purpose.

---

## 2026-07-23 · 002 (T0.0b) — autonomous merge vs "every PR requires review"

**Quoted DoD line.** *"A `CODEOWNERS` file exists at a GitHub-honoured location
and assigns review ownership such that every PR requires review before merge
(satisfying the arch.md commitment that the criteria matrix and release
checklist are reviewed like code, and that core-crate dependency additions are
reviewed as API decisions)."*

**Deviation.** `.github/CODEOWNERS` and `CONTRIBUTING.md` are authored exactly as
ticket 002 specifies — a repo-wide owner is assigned and the process contract
states that every PR requires review before merge. However, the mechanism that
would *enforce* required Code-Owner review — GitHub branch protection with
"Require review from Code Owners" — is **not enabled**, and PRs on the ticket
loop are **squash-merged autonomously** by the orchestrator without a
second-party human review.

**Rationale.** Ticket 002 explicitly scopes *"Branch-protection rules configured
in the GitHub UI/API"* out as an operator action outside the repository (its Out
of scope list). CODEOWNERS assigns ownership; only branch protection turns that
into a hard requirement. With enforcement off, the CODEOWNERS assignment is the
recorded intent, and the autonomous squash-merge is the operating reality. The
written contract (review-before-merge) is preserved as the documented norm for
human contributors; the loop is the exception, not the rule.

**Operator decision.** The dagr ticket-loop is run unattended with autonomous
squash-merge per operator policy (the `shipping-dagr-tickets` skill's settled
autonomous-merge decision). This entry is the standing record referenced by the
ticket-conventions §10 "known standing case."

---

## 2026-07-23 · 042 (T32) — supersedes T31's driver-guard over-demand test

**Affected artifact.** `crates/cli/tests/admission_driver.rs`, the T31 (041)
test formerly named `an_over_demand_node_is_failed_terminally_not_silently_stranded`.

**Change.** T31 shipped a *defensive* driver-level guard that caught a
can-never-fit node (declared cost exceeding a pool's total capacity) inside the
run loop and folded it to a `Failed` terminal, because — by T31's own comments —
"the full bootstrap-time rejection of too-big nodes is deferred to T32". T32
implements that authoritative rejection: a too-big node now fails the run at
**bootstrap, before any node executes**, with the distinct `bootstrap-failed`
outcome (arch.md C12 acceptance: "fails at bootstrap, not at admission time").
The bootstrap check therefore intercepts the over-demand node before the loop's
guard is reached. The T31 test's expectation was updated to the T32 behaviour
(renamed to `an_over_demand_node_is_rejected_at_bootstrap_not_silently_stranded`,
now asserting `RunOutcome::BootstrapFailed` and that nothing executed).

**Rationale.** This is a ticket-conventions §10 **supersession**, not a DoD
deviation: T32 owns the "too-big rejection" behaviour, and arch.md's C12
acceptance criterion mandates the bootstrap-time outcome the test now asserts.
The T31 *permit mechanics* (`admission.rs`) and the T31 driver guard code
(`can_ever_fit` / `reject_over_demand`) are **unchanged** — the guard is retained
as a defensive backstop, merely unreached on the default drive path. No test id
referenced by `docs/coverage-matrix.md` was renamed (the matrix maps T31's driver
integration to `a_pinned_pool_admits_one_node_at_a_time_and_the_run_still_completes`,
which is untouched).

**Operator decision.** Traces to the arch.md C12 acceptance criterion and the
T32 ticket DoD, which the loop implements autonomously.
