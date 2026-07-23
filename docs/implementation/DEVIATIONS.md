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
