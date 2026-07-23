# PR and merge playbook

Read by the orchestrator once per session. Exact command sequences for the
fragile PR/merge/bookkeeping steps; the loop follows these verbatim.

## Contents

1. [PR creation](#1-pr-creation)
2. [Loop-state ledger](#2-loop-state-ledger)
3. [Merge sequence](#3-merge-sequence)
4. [Post-merge](#4-post-merge)
5. [Conflict recipe](#5-conflict-recipe)
6. [Stop-report template](#6-stop-report-template)

## 1. PR creation

- Title: the `pr_title` field from `next_ticket.py` JSON **verbatim**
  (format `NNN · TID — Title`). It becomes the squash subject — never compose
  it by hand.
- Body (strict skeleton, flexible prose inside each section):

```markdown
Ticket: {TID} — docs/implementation/{file}

## Summary
<what shipped, 2-5 lines>

## Tests-first
Confirmed — failing tests committed first in <TESTS_FIRST_SHA>.

## Definition of done
<the ticket's DoD restated as a checklist, one status per item;
 the ticket file's own boxes are LEFT UNTOUCHED — the README box
 is the sole done-tracker>

## Open questions resolved
<each resolution + where it is recorded, or "None.">

## Deviations
<"None." | pointer to the docs/implementation/DEVIATIONS.md entry>

<!-- dagr-loop: phase=ci attempts_impl=1 attempts_ci=0 fingerprints=[] -->
```

- Command: `gh pr create --title "<pr_title>" --body-file <tmpfile>`.

## 2. Loop-state ledger

Durable per-ticket state lives in an HTML comment as the **last line of the
PR body** (session context dies; the PR body does not):

```
<!-- dagr-loop: phase=<implement|ci|merge> attempts_impl=N attempts_ci=N fingerprints=["..."] -->
```

- Update: `gh pr view <N> --json body -q .body` → rewrite **only** the marker
  line → `gh pr edit <N> --body-file <tmpfile>`.
- On resume, re-read the marker before doing anything else: counters only
  ever increment, never reset — this is what keeps attempt caps monotonic
  across session deaths.

## 3. Merge sequence

Pre-merge invariants, all required:

1. CI verdict `PASS` — or `NO_CHECKS` in `pre-workspace`/`pre-ci` eras with
   `GATE=PASS` (see quality-gates.md).
2. Base freshness after a fresh `git fetch origin`:
   `git merge-base --is-ancestor origin/main <branch>`. If main moved:
   rebase → re-run the gate → `git push --force-with-lease` → re-poll CI →
   retry the merge. A stale-but-textually-clean base is the classic
   semantic-conflict window; never merge across it.
3. `MERGEABLE=UNKNOWN` (GitHub computes mergeability asynchronously):
   re-poll `gh pr view <N> --json mergeable` every 15s, up to 60s, before
   deciding anything.

Then, in one command:

```
gh pr merge <N> --squash --delete-branch \
  --match-head-commit <HEAD_SHA from ci_status.sh> \
  --subject "<pr_title> (#<N>)"
```

- `--match-head-commit` closes the race between the green verdict and the
  merge call — the merge fails rather than merging a head that was never
  verified.
- Explicit `--subject` because GitHub uses the single commit's message, not
  the PR title, for one-commit squashes — without it, main's history drifts.
- Transient failure whose message contains "not mergeable" or
  "clean status": retry **once** after 15s.
- Review-required or branch-protection errors: **STOP** — operating
  assumptions changed. Never `--admin`, never enable or disable protection.

## 4. Post-merge

```
gh pr view <N> --json state,mergedAt      # must show MERGED
git switch main && git fetch origin && git merge --ff-only origin/main
python3 .claude/skills/shipping-dagr-tickets/scripts/flip_box.py <NNN>
git add docs/implementation/README.md     # explicit path, nothing else
git commit -m "docs: mark <NNN> <TID> done"
git push
```

- On push rejection (concurrent push to main): `git pull --rebase` and retry,
  twice max — the flip touches one line of one file, so the rebase is clean.

## 5. Conflict recipe

On the ticket branch: `git fetch origin && git rebase origin/main`.

- Conflicts confined to `docs/implementation/README.md` box lines: take both
  flips and `git rebase --continue`.
- Any other conflict: **one** fixer-subagent attempt with the ticket context;
  if it cannot resolve, `git rebase --abort` and STOP.
- Force-push only ever with `--force-with-lease`.

## 6. Stop-report template

Every stop emits exactly this block (one line per key):

```
TICKET=<NNN>
TID=<tid>
PHASE=<select|implement|gate|pr|ci|merge|flip>
BRANCH=<branch or ->
PR=<number or ->
VERDICT=<last script verdict>
FAILING=<check names or ->
WHY_STOPPED=<one sentence>
HOW_TO_RESUME=re-invoke the skill — preflight routes automatically; <plus any decision only the operator can make>
```
