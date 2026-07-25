# dagr release checklist

> **Status:** the version-controlled **release checklist** arch.md system-level
> acceptance **criterion 8** names — *"The human-classed criteria … are on the
> release checklist, which is version-controlled and reviewed like code."* A
> **checked-in, review-owned document**, never a runtime input. Edited only by
> pull request, reviewed like code (like [`docs/criteria-matrix.md`](criteria-matrix.md)
> and [`docs/coverage-matrix.md`](coverage-matrix.md)).

Every **human**-classed acceptance criterion in the criteria partition
([`docs/criteria-matrix.md`](criteria-matrix.md)) is a **judgment** that no test
can settle honestly (readability, documentation quality, a time-to-complete
goal). Criterion 8 does not leave those unenforced: it requires each to be a
line on **this** checklist, and the T65 system-acceptance gate
([`scripts/check-coverage-matrix.sh`](../scripts/check-coverage-matrix.sh),
`--checklist` binding) **fails CI** if any human criterion in the partition has
no matching line here. A machine criterion is covered by a test; a human
criterion is covered by a checked, reviewed line below.

The **disclaimer** criterion (SL4c — task-side external effects) is *not* on this
checklist and is *not* required to be: it is carried in the partition as a
disclaimer, deliberately unenforced by either a test or a checklist line.

## How the gate binds this file

The gate reads the partition, and for every criterion classed `human` it
requires a checklist item below whose leading `[SLUG]` tag matches the
criterion's partition id. A human criterion with no `[<id>]`-tagged line here
fails CI, naming the empty slot (Test-plan scenario 7). The audit boxes are
reviewed and ticked each release; a stale tick is a review finding, not a CI
signal — that is the point of a *human* criterion.

## The checklist — one item per human criterion

Each item is tagged with the partition criterion id it discharges. Tick the box
during the release review; the tick records a human judgment made, not a test
run.

- [ ] **[C1]** *Types are readable from the declaration.* Read three
  representative task `impl`s from the cookbook and confirm a reader can name
  each task's inputs and output from its `Input`/`Output` associated types alone,
  with no scheduling or wiring code in view (arch.md C1; criterion 8 names this
  as a judgment). C1's mechanical sub-criteria are separately machine-tested
  (T9/T29/T14) — this item is the judgment half only.

- [ ] **[C21]** *The graph-fingerprint internal-logic limitation is documented at
  the point of use.* Confirm the `FingerprintSlot` / fingerprint public surface
  still documents, in prose a reader meets when they reach for it, that the
  fingerprint tracks the graph's declared shape and **not** a task's internal
  logic — so a reviewer knows a same-interface logic change needs a
  hand-maintained version marker (arch.md C21; criterion 8's
  documentation-at-point-of-use).

- [ ] **[C24]** *The rendered diagrams are readable with no manual layout.*
  Render the reference / cookbook pipelines to DOT and Mermaid and confirm the
  output reads clearly without hand-tuning: node/edge labels legible, data vs
  ordering edges visually distinct, group clusters grouped, per-state styling
  distinguishes originated from propagated skips (arch.md C24). The mechanical
  proxies (`dot` parses, Mermaid's parser accepts, structural + golden coverage)
  are machine-tested in CI (T46/T47); this item is the readability judgment.

- [ ] **[SL8human]** *The human-classed criteria are on this checklist.* Confirm
  this checklist still carries a line for **every** human criterion in the
  partition (C1, C21, C24, and this item), and that the gate's `--checklist`
  binding is wired in CI so a newly-added human criterion cannot ship without a
  checklist line (arch.md system-level criterion 8, human half).

- [ ] **[SL1]** *The README quickstart stays completable in under thirty minutes.*
  Walk the README quickstart end to end as a developer comfortable with Rust and
  cargo but new to dagr — empty directory to a compiled, run, artifact-inspected
  two-node pipeline — and confirm it stays under thirty minutes (arch.md
  system-level criterion 1, human part). This is a design-goal audit **each
  release, not a timer in CI** (the machine half — the quickstart compiles and
  runs verbatim — is the mapped `SL1` test). SL1's partition class is `machine`
  (the verbatim-compile half governs), so this checklist line is the audit of its
  human facet; the gate does not require it, but the release review does.
