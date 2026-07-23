# Implementer prompt template

The verbatim prompt the orchestrator hands to every per-ticket subagent.
**ALWAYS use this exact structure; fill placeholders only from
`next_ticket.py` JSON and `ci_status.sh` output.** Never paraphrase, never
add context the placeholders don't carry.

## Contents

1. [Placeholders](#1-placeholders)
2. [The template](#2-the-template)
3. [Mode deltas](#3-mode-deltas)

## 1. Placeholders

| Placeholder | Source |
|---|---|
| `{MODE}` | orchestrator: `implement` \| `continue` \| `ci-fix` |
| `{NNN}` `{TID}` `{TITLE}` `{TICKET_PATH}` `{BRANCH}` `{ERA}` `{SIZE}` `{COMPONENTS}` | `next_ticket.py` JSON (`nnn`, `tid`, `title`, `path`, `branch`, `era`, `size`, `components`) |
| `{DEP_TIDS}` | `next_ticket.py` JSON `depends_on` |
| `{FAILURE_CONTEXT}` | ci-fix only: `FAILING=` names + `RUN_ID=` from `ci_status.sh`, plus prior fingerprints from the PR-body ledger |

## 2. The template

```text
You are implementing dagr ticket {NNN} ({TID} — {TITLE}) in MODE={MODE}.
Branch: {BRANCH} · Era: {ERA} · Size: {SIZE}

READ, in this order, before writing anything:
1. {TICKET_PATH} — the ticket, IN FULL (header, Objective, Test plan,
   Definition of done, Open questions, Out of scope).
2. docs/arch.md — the sections named in "{COMPONENTS}", plus the Vocabulary
   section (terminal states, state classes, trigger rules are normative).
3. docs/tasks.md — the entry for {TID}; it carries Q: items the ticket omits.
4. The ADRs/outputs of the dependency tickets: {DEP_TIDS}.
5. .claude/skills/shipping-dagr-tickets/references/ticket-conventions.md.
6. The routed section of
   .claude/skills/shipping-dagr-tickets/references/dagx-prior-art.md
   (see its routing table; skip if no section routes to {TID}).
7. CONTRIBUTING.md and the criteria coverage matrix — once they exist.

PROCESS (hard rules, no deviation):
- Work ONLY on branch {BRANCH}. Verify first:
  `git rev-parse --abbrev-ref HEAD`. Wrong branch = stop and report.
- TDD is non-negotiable: translate the ticket's Test plan into failing
  tests, commit them FIRST with message
  `test({TID}): failing tests for <short description>`, watch them fail,
  then implement until green in separate commits.
- Stage explicit paths only. NEVER `git add -A`, `git add .`, or `-a`.
- NEVER push. The orchestrator owns push, PR, and merge.
- Before declaring done, run
  `.claude/skills/shipping-dagr-tickets/scripts/run_gate.sh {ERA} {NNN}`
  from the repo root and iterate until it prints GATE=PASS.
- Resolve and RECORD every open question (the ticket's section AND the
  tasks.md Q: items) per ticket-conventions.md — a silent pick violates
  the ticket's own Definition of done. A genuinely contested decision:
  stop and report BLOCKED_ON instead of choosing.
- The ticket's Out of scope list and arch.md's permanent non-goals are
  hard constraints. Deferred seams belong to the ticket named in Out of
  scope — do not fill them early.
- Spike code (decision-spike tickets) is quarantined or deleted before
  the PR, never promoted into shipping crates.
- Do not touch docs/implementation/README.md — the orchestrator owns it.

RETURN FORMAT (strict, ≤20 lines total):
STATUS=complete|blocked|needs-another-round
GATE=PASS|FAIL
COMMITS=<one line per commit: short-sha + subject>
TESTS_FIRST_SHA=<sha of the failing-tests commit>
OPEN_QUESTIONS=<each resolution + where recorded, or none>
DEVIATIONS=none|<description + DEVIATIONS.md pointer>
NOTES=<≤3 lines>
BLOCKED_ON=<one line — only if STATUS=blocked>
```

## 3. Mode deltas

Append the matching block to the template.

**`continue`** (branch already has commits; do not redo committed work):

```text
MODE=continue: this branch already has work. First run
`git log --oneline main..HEAD` and diff the branch state against the
ticket's Definition of done. Report one extra line:
ASSESSMENT=<what is done / what remains>
Then finish only the remaining work under the same rules.
```

**`ci-fix`** (an open PR has failing checks):

```text
MODE=ci-fix: CI failed on the open PR.
{FAILURE_CONTEXT}
Fetch the logs yourself: `gh run view <RUN_ID> --log-failed`.
Make the smallest change that turns the named checks green without
violating the ticket or its Out of scope list. Commit with message
`fix({TID}): <failing-check>: <cause>`. Report one extra line:
FINGERPRINT=<sorted failing check names + first error line of each>
If the fingerprint matches a prior one from {FAILURE_CONTEXT}, do not
attempt the same fix again — report STATUS=blocked with the reason.
```
