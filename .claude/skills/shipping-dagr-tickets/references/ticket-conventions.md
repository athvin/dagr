# Ticket conventions

Everything an implementer needs about dagr's ticket anatomy and repo process.
Durable conventions only — live state (which boxes are checked, what the next
ticket is) always comes from `scripts/next_ticket.py` at run time.

## Contents

1. [§1 Ticket anatomy](#§1-ticket-anatomy)
2. [§2 tid vs NNN](#§2-tid-vs-nnn)
3. [§3 Branch naming](#§3-branch-naming)
4. [§4 Ticket-type handling matrix](#§4-ticket-type-handling-matrix)
5. [§5 Open-questions duty](#§5-open-questions-duty)
6. [§6 Literal-DoD-first rule](#§6-literal-dod-first-rule)
7. [§7 Stubs and seams](#§7-stubs-and-seams)
8. [§8 Scope boundary](#§8-scope-boundary)
9. [§9 Normative vocabulary](#§9-normative-vocabulary)
10. [§10 ADR supersession and DEVIATIONS.md](#§10-adr-supersession-and-deviationsmd)

## §1 Ticket anatomy

Ticket files live at `docs/implementation/NNN-TID-slug.md`. Every file has the
same shape:

- Line 1: H1 `# NNN · TID — Title` (UTF-8 middle dot and em dash).
- A 2-line blockquote header:
  - `> **Milestone:** M0–M4 · **Size:** S|M|L · **Type:** <type> · **Components:** <C-ids or system-level>`
  - `> **Branch:** ` `` `branch-name` `` ` · **Depends on:** <tids or —> · **Blocks:** <tids or —>`
- Six sections, always in this order:
  1. `## Why / context` — governing arch.md sections and upstream decisions.
  2. `## Objective` — bulleted concrete work.
  3. `## Test plan (write these first — TDD)` — plain-English Setup/Action/Expected
     scenarios that must be written and failing BEFORE implementation.
  4. `## Definition of done` — `- [ ]` checklist, always ending with the CI-green
     boilerplate ("fmt, clippy with warnings denied, tests, rustdoc lint, and
     cargo-audit/deny where configured").
  5. `## Open questions` — "None." or genuine questions the ticket must resolve
     and record.
  6. `## Out of scope` — deferred work with the owning ticket named, ending with
     a scope-boundary restatement.

Sizes: S is under a day, M is 1–3 days, L is up to a week. Expect M/L tickets to
span multiple sessions; the loop is designed to resume mid-ticket.

## §2 tid vs NNN

Two namespaces identify a ticket and they diverge:

- **NNN** — the 3-digit file/sequence number (001–080).
- **tid** — the task id from `docs/tasks.md` (T0.2, T7, T54a …).

Divergence examples: T7 = ticket 006, T67 = ticket 048, T55 = ticket 068,
T54a/T54b = tickets 066/071. Dependencies in ticket headers and README `— after`
lists use tids; resolve them to NNN through `docs/implementation/README.md`
(or `scripts/next_ticket.py`, which does it for you). Ticket numbering is a
valid topological order: every dependency has a strictly lower NNN, so serial
lowest-first execution always satisfies dependencies.

## §3 Branch naming

The branch name is copied **verbatim** from the ticket header's `Branch:` field
— never normalized, re-slugged, or re-prefixed. Pattern:
`<prefix>/t<tid-lowercase>-<slug>` with prefix by type:

- `chore/` — setup tickets (e.g. `chore/t0.0a-repo-init-and-hygiene`)
- `adr/` — decision tickets (e.g. `adr/t0.2-output-ownership-adr-spike`)
- `feat/` — feature, demo, and gate tickets (e.g. `feat/t9-task-abstraction-and-errors`)

Exactly one branch and one PR per ticket. The PR links its ticket by tid and by
`docs/implementation/` path.

## §4 Ticket-type handling matrix

| Type | How to work it |
|---|---|
| setup | Deliverables are files; the "tests" are the Test plan's file/content assertions, checked mechanically before authoring, then made true. |
| decision | Deliverable is an ADR with **status / context / decision / consequences / rejected alternatives** sections, committed where the repo keeps ADRs. |
| decision (spike) | ADR plus throwaway prototype. Spike code is quarantined (outside the workspace) or deleted before the PR — **never promoted into crates**. Reopen conditions are hard stops: if the prototype defeats the design (e.g. ticket 008's borrow-checker clause), the spec decision REOPENS; do not paper over it. |
| feature | Strict TDD: the failing tests from the Test plan land in a commit **before** implementation commits. |
| feature (tests) | The tests are the deliverable; they exercise already-merged components. |
| feature (demo) | Integration proof of a milestone's done-when. Adds **zero capability**: any missing behavior belongs to the owning component ticket (which means STOP, not scope creep). Respects milestone boundaries — the M1 demo has no artifacts, no admission control, no CLI. |
| feature (bench) | Benchmark deliverable; wire into CI only as the ticket specifies. |
| feature (ci) | CI configuration; the workflow must run green on its own PR. |
| feature (docs) | Documentation deliverable; claims must not exceed shipped behavior. |
| feature (gate) | Ticket 080 asserts existing behavior and adds nothing. |

## §5 Open-questions duty

Before writing code, check **both** sources of open questions:

1. The ticket's `## Open questions` section (e.g. 003's facade-vs-workspace and
   renderer-binary questions; 019's trait-vs-struct-vs-closure expression form).
2. The ticket's tid entry in `docs/tasks.md`, which carries `Q:` items the
   ticket files omit (e.g. T42's phase list, T43's critical-path definition —
   some marked "escalate to a short ADR before implementing").

Every question must be **resolved and recorded** — in the ticket file or a short
ADR inside the PR. Silently picking an answer violates the ticket's own DoD.
A genuinely contested decision (no defensible default, or it moves a merged
decision the ticket does not own) is a STOP for the loop, never a silent pick.

## §6 Literal-DoD-first rule

When a DoD line looks odd, prefer the literal reading if a repo-relative
interpretation exists. Example: ticket 008's DoD names a machine-absolute path
that points at the ticket file itself — writing the ADR sections **into that
ticket file** satisfies it literally with zero deviation. Deviate (and record it
per §10) only when literal satisfaction is impossible.

## §7 Stubs and seams

`## Out of scope` names the owning ticket for every deferred piece. Respect the
seams: implementing deferred scope "while you're in there" steals another
ticket's scope and breaks the one-branch-one-PR contract. Canonical example:
ticket 068 (T55) ships the `resume` CLI verb as a **recognized stub** with a
reserved exit code — the real resume algorithm belongs to T58 (ticket 070).

## §8 Scope boundary

arch.md's permanent non-goals, restated in every ticket's Out of scope, are a
hard boundary: dagr is **not** a scheduler, distributed execution system,
metadata store, web interface, DSL, or backfill orchestrator — and the graph's
shape never changes at runtime (no dynamic fan-out). Adding "helpful" capability
beyond the boundary (group-level concurrency, a push exporter, runtime graph
mutation…) fails review by design. The orchestrator runs an independent scope
check on M/L feature diffs; do not rely on it — stay inside the ticket.

## §9 Normative vocabulary

arch.md's **Vocabulary** section is load-bearing, not flavor: 9 terminal states
(succeeded, failed, timed-out, skipped, upstream-skipped, upstream-failed,
cancelled, abandoned, satisfied-from-prior), 4 state classes (success-like,
skip-like, failure-like, stop-like), and a closed trigger-rule set
(all-succeeded default, all-terminal, any-failed). Enum-shaped tickets
(010/T0.4 onward) must use the exact canonical names. Tickets cross-reference
arch.md by section anchor; read the referenced sections, not just the ticket.

## §10 ADR supersession and DEVIATIONS.md

- **Supersession:** a later ticket whose decision contradicts a merged ADR marks
  the older ADR "Superseded by <new ADR>" in the same PR — merged decision text
  is never rewritten. If a ticket's governing arch.md section conflicts with a
  merged dependency ADR and the ticket does not own that decision, STOP and
  report a spec conflict instead of picking a side.
- **DEVIATIONS.md:** any deliberate departure from a DoD line is recorded in
  `docs/implementation/DEVIATIONS.md` (create it on first use) with: date,
  ticket, quoted DoD line, the deviation, rationale, and the operator decision
  it traces to — plus a note in the PR body. Known standing case: ticket 002's
  "every PR requires review before merge" intent vs the operator's
  autonomous-merge decision. Author CONTRIBUTING.md, CODEOWNERS, and the PR
  template exactly as ticket 002 specifies (002 itself scopes branch-protection
  configuration out as an operator action); enforcement stays off, and the
  deviation entry records that self-merge is per operator policy.
