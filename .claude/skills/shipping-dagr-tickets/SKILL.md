---
name: shipping-dagr-tickets
description: Ships dagr implementation tickets autonomously in a continuous loop. Selects the next eligible ticket from docs/implementation/README.md, delegates TDD implementation to a fresh subagent, runs era-appropriate quality gates, opens a PR, polls CI with gh until green, squash-merges, verifies the merge landed on main, and flips the ticket checkbox before repeating until no eligible ticket remains. Use when asked to ship, implement, or burn down dagr tickets, run or continue the ticket loop, pick the next ticket, or resume an interrupted ticket that is mid-implementation, on an open PR, or merged but not yet checked off.
---

# Shipping dagr tickets

Run the dagr ticket loop: one ticket per iteration, from selection through
squash-merge and bookkeeping, repeating until `ALL_DONE`. The operator has
chosen **fully autonomous** operation: every ticket — including ADR/decision
tickets — merges once its gate/CI is green, with decisions recorded in ADR
files and deviations in `docs/implementation/DEVIATIONS.md`.

## Terminology

| Term | Meaning |
|---|---|
| ticket | one `docs/implementation/NNN-*.md` file (NNN = 001–080) |
| tid | the T-identifier (T0.2, T7, T54a) — a DIFFERENT namespace from NNN |
| box | the ticket's checkbox line in `docs/implementation/README.md` — the sole done-tracker |
| gate | the era-appropriate check set run by `scripts/run_gate.sh` |
| era | `pre-workspace` (001–002) · `pre-ci` (003–005) · `ci` (006+) |
| orchestrator | this session: runs scripts and git/gh commands, delegates everything else |
| implementer / fixer | fresh per-ticket subagents that read, write code, and commit |
| ledger | the `<!-- dagr-loop: ... -->` marker line in the PR body (durable attempt counters) |

## Hard rules — NEVER

1. NEVER use `gh pr checks` — it conflates "failed" with "no checks", and its
   `cancel` bucket is not a pass. `scripts/ci_status.sh` is the only CI probe.
2. NEVER merge on an `ANOMALY` verdict, with pending checks, or without the
   pre-merge invariants in `references/pr-and-merge-playbook.md`.
3. NEVER use `gh pr merge --admin`, enable/disable branch protection, or
   change repo settings. If merge is blocked by protection: STOP.
4. NEVER reopen or recreate a PR a human closed without merging — that is a
   veto. STOP and report.
5. NEVER `git add -A`, `-a`, or `.` — stage explicit paths only. Untracked
   files must never leak into commits or wedge the loop.
6. NEVER edit checkbox states inside ticket files; NEVER flip the README box
   before the PR is verified `MERGED`.
7. NEVER read arch.md, ticket bodies, tasks.md, ADRs, dagx material, or CI
   logs in the orchestrator — delegate to subagents; run scripts for state.
8. NEVER let a subagent's code cross the permanent scope boundary (no
   scheduler, distributed execution, metadata store, web UI, DSL, backfill
   orchestrator, runtime graph mutation) or another ticket's Out-of-scope
   seam — that is what the scope check exists to catch.
9. NEVER silently resolve a genuinely contested open question or rewrite a
   merged ADR — record, supersede, or STOP per
   `references/ticket-conventions.md`.
10. NEVER force-push except `--force-with-lease` after a deliberate rebase.

## Scripts

All under `.claude/skills/shipping-dagr-tickets/scripts/`. Run them; do not
reimplement their logic inline. Each prints `KEY=VALUE` lines and a final
verdict line.

| Script | Invocation | Output |
|---|---|---|
| `next_ticket.py` | `python3 …/next_ticket.py` (also `--status`, `--ticket NNN`) | `VERDICT=SELECTED\|ALL_DONE\|NO_ELIGIBLE\|PARSE_ERROR` + ticket JSON (nnn, tid, branch, era, pr_title, components, depends_on, size, path) |
| `preflight.sh` | `…/preflight.sh` (repo mode) | `PREFLIGHT=PASS\|FAIL` + BRANCH/CLEAN/SYNCED/GH_AUTH/WORKFLOW_SCOPE/MAIN_CI |
| `preflight.sh` | `…/preflight.sh <branch> <nnn>` | `ROUTE=FRESH\|CONTINUE_IMPL\|POLL_CI\|FLIP_BOX\|DONE\|STOP_AMBIGUOUS`; leaves checkout on the branch for resume routes |
| `run_gate.sh` | `…/run_gate.sh <era> [nnn]` | `CHECK:<name>=PASS\|FAIL\|SKIP:<reason>` lines + `GATE=PASS\|FAIL` (40-line tail on failures) |
| `ci_status.sh` | `…/ci_status.sh <pr> [--wait s] [--grace s]` | `VERDICT=PASS\|FAIL\|PENDING\|NO_CHECKS\|ANOMALY` + HEAD_SHA/FAILING/RUN_ID (exit 0/1/2/3/4) |
| `flip_box.py` | `python3 …/flip_box.py <NNN>` | `VERDICT=FLIPPED\|ALREADY_CHECKED\|ERROR` (idempotent) |

## Session startup

1. Run `preflight.sh` (repo mode). `PREFLIGHT=FAIL` → STOP with the failing
   keys. `MAIN_CI=red` → STOP: never cut a branch from red main.
   `WORKFLOW_SCOPE=no` → note it; STOP before starting ticket 006.
2. Read `references/quality-gates.md` and
   `references/pr-and-merge-playbook.md` once. Do not read the other
   references — they are for subagents.
3. Keep an in-session ledger: one line per finished ticket
   (`NNN TID merged PR#n`). Retain nothing else between tickets.

## The loop

Repeat until `ALL_DONE`. Every step is idempotent; on any interruption,
re-invoking this skill re-enters at step 1 and step 2 routes to the right
place.

**1 · SELECT** — `python3 scripts/next_ticket.py`.
`ALL_DONE` → final report (ledger + `--status` output), end.
`NO_ELIGIBLE` / `PARSE_ERROR` → STOP (authoritative state is corrupted; never
guess around it). `SELECTED` → keep the JSON; call its fields
`{NNN} {TID} {BRANCH} {ERA} {SIZE} {PR_TITLE}` below.

**2 · ROUTE** — `scripts/preflight.sh {BRANCH} {NNN}`.
`FRESH` → step 3 · `CONTINUE_IMPL` → step 4 with MODE=continue ·
`POLL_CI` → step 7 (re-read the ledger from the PR body first) ·
`FLIP_BOX` → step 9 · `DONE` or `STOP_AMBIGUOUS` → STOP (a selected ticket
cannot already be done; something is inconsistent).

**3 · BRANCH** — `git switch -c {BRANCH} && git push -u origin {BRANCH}`.

**4 · IMPLEMENT** — fill the template in
`references/implementer-prompt.md` (MODE=implement, or continue when
routed/resuming) strictly from the step-1 JSON; launch a fresh subagent.
On return:
- `STATUS=blocked` → STOP with its `BLOCKED_ON`.
- `STATUS=needs-another-round` → relaunch MODE=continue. Max **3 rounds**
  per ticket per session; exceeded → STOP.
- `STATUS=complete` with `GATE=PASS` → `git push`, step 5.
- `GATE=FAIL` claimed complete → one continue round to fix; recurrence → STOP.

**5 · SCOPE CHECK** — only for `feature*` tickets sized M or L: launch a
one-shot reviewer subagent with ONLY: `git diff main...HEAD --stat`, the
diff of key files, the ticket's Out of scope section, and the permanent
non-goals list; it returns `IN_SCOPE` or `VIOLATION:<what>`.
`VIOLATION` → one continue round to remove it, re-check once; recurrence →
STOP. (Cheap insurance against a subagent "helpfully" implementing another
ticket's scope.)

**6 · PR** — if none exists: create per
`references/pr-and-merge-playbook.md` §1 (title = `{PR_TITLE}` verbatim,
body template, ledger marker as last line).

**7 · CI** — by era:
- `pre-workspace` / `pre-ci`: `scripts/ci_status.sh <pr>` once. Expected
  `NO_CHECKS` (normal — CI does not exist yet; the local gate already
  passed). If real checks appear, treat as era `ci` from here on.
- `ci`: `scripts/ci_status.sh <pr> --wait 1800 --grace 300` (grace 600 for
  ticket 006 — the first-ever workflow run registers slowly).
  - `PASS` → step 8.
  - `PENDING` at cap → one more `--wait 1800` cycle; still pending → STOP
    with the run URL.
  - `FAIL` → update the ledger (increment `attempts_ci`, append the
    fingerprint per `references/quality-gates.md` §4). Same fingerprint
    twice OR `attempts_ci` > **3** → STOP (thrash guard). Otherwise launch
    a fixer (MODE=ci-fix with `FAILING=`, `RUN_ID=`, prior fingerprints),
    push its commits, repeat step 7.
  - `NO_CHECKS` after grace → run the diagnosis recipe in
    `references/quality-gates.md` §4 once; unresolved → STOP.
  - `ANOMALY` → STOP.

**8 · MERGE** — follow `references/pr-and-merge-playbook.md` §3 exactly:
freshness invariant (`git merge-base --is-ancestor origin/main {BRANCH}`
after fetch; stale → rebase → re-gate → `--force-with-lease` → back to
step 7), `MERGEABLE=UNKNOWN` re-poll, then
`gh pr merge <N> --squash --delete-branch --match-head-commit <HEAD_SHA>
--subject "{PR_TITLE} (#<N>)"`, one 15s retry on the transient
not-mergeable race. Anything else → STOP.

**9 · VERIFY + FLIP** — playbook §4: confirm `MERGED`; `git switch main &&
git fetch origin && git merge --ff-only origin/main`;
`python3 scripts/flip_box.py {NNN}`; stage
`docs/implementation/README.md` explicitly; commit
`docs: mark {NNN} {TID} done`; push (on rejection `git pull --rebase`,
retry ×2). Append the ledger line. → step 1.

## Caps

| Limit | Value | On breach |
|---|---|---|
| implement/continue rounds per ticket per session | 3 | STOP |
| CI-fix rounds per ticket (durable in PR ledger) | 3 | STOP |
| identical CI failure fingerprint | 2 | STOP |
| CI wait cycles (1800s each) | 2 | STOP |
| merge transient retry | 1 | STOP |
| box-flip push rebase retry | 2 | STOP |

STOP always emits the report template in
`references/pr-and-merge-playbook.md` §6 — the state is always resumable by
re-invoking this skill.

## References

| File | Who reads it | When |
|---|---|---|
| `references/quality-gates.md` | orchestrator + subagents | session start / per ticket |
| `references/pr-and-merge-playbook.md` | orchestrator | session start |
| `references/ticket-conventions.md` | implementer | every ticket |
| `references/dagx-prior-art.md` | implementer | only the section routed by its table |
| `references/implementer-prompt.md` | orchestrator (as template) | every delegation |

## Operator notes

An unattended run needs these allowlisted (documented here, never
self-applied): `git switch/push/fetch/rebase/merge`, `gh pr
create/view/edit/list/merge/comment`, `gh run list/view`, `cargo *`,
`python3 .claude/skills/shipping-dagr-tickets/scripts/*`, and the scripts
themselves. Tools required: `git`, `gh` (with the `workflow` OAuth scope
before ticket 006), `python3`; from era `pre-ci` onward `cargo-audit` and
`cargo-deny` (`cargo install --locked …` if missing).
