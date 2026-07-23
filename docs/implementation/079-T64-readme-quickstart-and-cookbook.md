# 079 · T64 — README, quickstart, and cookbook

> **Milestone:** M4 · **Size:** L · **Type:** feature (docs) · **Components:** Documentation
> **Branch:** `feat/t64-readme-quickstart-and-cookbook` · **Depends on:** T49, T55 · **Blocks:** T65

## Why / context
The **Documentation** deliverables are the human-facing half of the product: a README quickstart, rustdoc on every public item, runnable per-layer examples, and a cookbook of the patterns the design forces on authors (arch.md *Documentation*). These are held to in CI where a machine can check them, and they are named directly by system-level acceptance criteria 1 and 6 (*System-level acceptance*). This ticket depends on the M3 explain-a-run demo (T49) for the artifact-inspection step of the quickstart and on the C26 CLI contract (T55) for the verbs the quickstart and cookbook invoke. It also carries the *When not to use this* honesty note into the README. This ticket is the last content gate before the T65 system-acceptance job, which is why every claim here must be CI-verified rather than prose-only.

## Objective
Produce the documentation set that a new user reads first and that CI protects from rot, without adding any framework behavior.

- Write the **README quickstart**: empty directory to a compiled, run, artifact-inspected two-node pipeline, targeted at a developer comfortable with Rust and cargo but with no async experience, and structure it so its code blocks are extracted and run **verbatim** in CI (system criterion 1).
- Wire the **rustdoc-on-public-items lint** so that any public item lacking documentation fails CI.
- Author **runnable examples covering each layer** (authoring, execution core, artifacts/observability, developer/operator surface) so no example drifts from the current API.
- Write the **cookbook**, one entry per pattern the design forces: fan-out inside one node (with the declared-cost rule for internal parallelism), fan-in, branch-in-task (`self-skip` versus `succeed-with-empty` for joins), incremental cursors via the scratch store (C18), durable stage boundaries and what they buy at resume (C10/C27), the non-`Send` capture error and its fixes, and two same-typed resources distinguished by newtypes (C9).
- Add the README **"When not to use this"** section stating plainly that below the adoption triggers, plain tokio is the honest recommendation, and document the **MSRV** as pinned in the workspace (Stability).
- Add the **criterion-6 locality check**: a structure-diff over a reference pipeline proving that adding a node touches only the task's own module, the assembly site, the structure fixture, and — when a new resource is introduced — the registry construction in `main`.
- Register every doc-verifiable artifact of this ticket in the criteria matrix so T65 can map criteria 1 and 6 to passing tests.

## Test plan (write these first — TDD)
Each scenario is CI-runnable and independently checkable. Where a scenario reads "the quickstart," it means the exact code blocks extracted from the README, not a hand-maintained copy.

- **Quickstart compiles verbatim.** Setup: a fresh temporary directory with only what the README's first block tells the reader to create. Action: extract each fenced Rust/TOML/shell block from the README quickstart in order and execute them exactly as written. Expected: the project compiles with no edits to the extracted text; a divergence between README text and what compiles fails the test.

- **Quickstart runs end to end.** Setup: the compiled two-node quickstart pipeline from the previous scenario. Action: run the pipeline via the C26 `run` verb with the quickstart's stated arguments. Expected: the process exits with the success code, and the two nodes both reach `succeeded`.

- **Quickstart artifact inspection matches the prose.** Setup: the completed quickstart run and its run-store directory. Action: perform the exact artifact-inspection step the quickstart instructs (open the run artifact / render, per T49). Expected: the values the quickstart tells the reader they will see (node names, terminal states, the two-node shape) are present in the artifact; if the prose promises a number or a node label, the test asserts that same value.

- **Quickstart needs no async knowledge and no running server.** Setup: an environment with no database, scheduler, or network service. Action: run the full quickstart flow. Expected: it completes using only the binary and its arguments (system criterion 7 boundary), and no quickstart step requires the reader to write `async`/`await` reasoning to succeed.

- **Rustdoc lint fails on an undocumented public item.** Setup: a throwaway branch that removes the doc comment from one public item in a core crate. Action: run the documentation lint step. Expected: CI fails and names the undocumented item; restoring the doc comment makes it pass.

- **Every public item is documented at head.** Setup: the crates as they ship on the ticket branch. Action: run the rustdoc-on-public-items lint. Expected: it passes with zero missing-docs findings.

- **Each per-layer example compiles and runs.** Setup: the examples directory. Action: build and execute each layer example under `cargo` as CI does. Expected: each example compiles and runs to a successful exit; an example referencing a renamed or removed API fails the build, catching drift.

- **Fan-out cookbook entry demonstrates the declared-cost rule.** Setup: the fan-out-inside-one-node example the entry references. Action: run it and read its declared versus measured cost from the run artifact (C23). Expected: the internal parallelism inside the single node is bounded by that node's **declared cost** vector (C5), and the entry's text matches what the artifact shows — establishing that fan-out is a within-node concern, not a graph-shape change.

- **Fan-in cookbook entry wires many upstreams into one node.** Setup: the fan-in example. Action: run it. Expected: the joining node consumes multiple upstream handles as a tuple and succeeds only when all upstreams succeeded (C3 default rule), matching the entry's description.

- **Branch-in-task entry shows both join disciplines.** Setup: the branch-in-task example with one branch that yields `self-skip` and one that yields `succeed-with-empty`. Action: run it. Expected: the downstream join behaves as the entry documents for each case — a skipping branch propagates a skip under the default rule, while a succeed-with-empty branch keeps the join alive with an empty value — demonstrating why an author picks one over the other.

- **Incremental-cursor entry checkpoints via scratch.** Setup: the scratch-cursor example. Action: run an attempt that writes a cursor to scratch, force a retry-eligible failure, then let the retry read it. Expected: attempt two reads the cursor written on attempt one (C18) and resumes from it rather than starting over; the entry's text matches this observable behavior.

- **Durable-stage-boundary entry pays off at resume.** Setup: the durable-boundary example with a node marked durable whose output type implements the reference contract. Action: run it, kill after the durable node succeeds, then resume (C27). Expected: the durable node is `satisfied-from-prior` and its value is rehydrated rather than recomputed, while an in-memory sibling documented in the same entry re-executes when demanded — the entry states exactly this trade.

- **Non-`Send` capture entry reproduces the error and each fix.** Setup: the non-`Send` capture example, plus compile-fail fixtures pinned to the workspace toolchain (C28). Action: attempt to compile the broken form and each documented fix. Expected: the broken form fails to compile with the non-`Send` diagnostic, and each fix the entry prescribes compiles and runs; the compile-fail fixture asserts the error is the one the entry describes.

- **Same-typed-resources entry uses newtypes.** Setup: the resource example registering two resources of the same underlying type behind distinct newtypes. Action: build and run it; also attempt a variant that registers two resources of the literally identical type. Expected: the newtype variant compiles and each node retrieves the intended resource by type (C9), while the identical-type variant fails registry construction as ambiguous — both outcomes as the entry documents.

- **Criterion-6 locality structure-diff.** Setup: a reference pipeline with a checked-in structure fixture (C28), plus a recorded set of files. Action: add one new node — placing its task in its own module, wiring it at the assembly site, updating the structure fixture, and (for the resource-introducing variant) editing the registry construction in `main` — then run the structure test and a file-touch check. Expected: the structure diff shows exactly the new node and edges, and no file **outside** that permitted set changed; a change that leaks outside those files fails the check (system criterion 6).

- **"When not to use this" and MSRV are present and truthful.** Setup: the README. Action: assert the README contains the adoption-triggers guidance recommending plain tokio below them, and that the documented MSRV equals the workspace-pinned MSRV. Expected: both assertions pass; a drift between documented and pinned MSRV fails.

- **Criteria-matrix wiring.** Setup: the checked-in criteria matrix (feeds T65). Action: confirm system criteria 1 and 6, and the machine-checkable Documentation deliverables, each map to one of the tests above. Expected: every such criterion has a mapped passing test and appears exactly once in the matrix; a criterion absent from the matrix fails CI.

## Definition of done
- [ ] The README quickstart takes a reader from empty directory to a compiled, run, artifact-inspected two-node pipeline, targeted at a Rust/cargo developer with no async experience (system criterion 1; *Documentation*).
- [ ] The quickstart's code blocks compile and run **verbatim** in CI, extracted from the README rather than maintained separately (system criterion 1).
- [ ] The quickstart's artifact-inspection step reflects real artifact contents produced by T49's explain-a-run path, and every value the prose promises is asserted.
- [ ] The quickstart requires no running server, database, or scheduler — binary and arguments are sufficient (system criterion 7 boundary).
- [ ] Rustdoc is present on **every public item**, enforced by a CI lint that fails on any missing-docs finding (*Documentation*).
- [ ] **Runnable examples cover each layer** (A authoring, B execution core, C artifacts/observability, D developer/operator surface), each built and run in CI so API drift is caught (*Documentation*).
- [ ] Cookbook entry: **fan-out inside one node**, showing the declared-cost rule bounds internal parallelism (C5/C12/C23) and that fan-out is not a graph-shape change.
- [ ] Cookbook entry: **fan-in**, many upstream handles joined into one node under the default `all-succeeded` rule (C3).
- [ ] Cookbook entry: **branch-in-task**, contrasting `self-skip` and `succeed-with-empty` for joins and when to choose each.
- [ ] Cookbook entry: **incremental cursors via scratch**, a cursor written on one attempt and read on the next (C18).
- [ ] Cookbook entry: **durable stage boundaries**, what they buy at resume — satisfied-from-prior with rehydration versus in-memory re-execution when demanded (C10/C27).
- [ ] Cookbook entry: **non-`Send` capture error and its fixes**, the broken form failing to compile with the expected diagnostic and each fix compiling and running, pinned by a compile-fail fixture (C28).
- [ ] Cookbook entry: **two same-typed resources via newtypes**, retrieval by type succeeding and the identical-type registration failing as ambiguous (C9).
- [ ] README **"When not to use this"** section states the adoption triggers and recommends plain tokio below them (*When not to use this*).
- [ ] README documents the workspace-pinned **MSRV**, and CI asserts documented MSRV equals the pinned value (Stability).
- [ ] **Criterion-6 locality** is proven by a structure-diff on a reference pipeline: adding a node changes only the task's module, the assembly site, the structure fixture, and — for a new resource — the registry construction in `main`, with a check that no other file changed (system criterion 6).
- [ ] System criteria 1 and 6 and the machine-checkable Documentation deliverables are mapped in the checked-in criteria matrix to the passing tests above, each appearing exactly once, ready for the T65 gate (system criterion 8).
- [ ] No new framework behavior, verb, or public API is introduced by this ticket; it is documentation and its CI verification only.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **No new runtime or CLI behavior.** The quickstart and cookbook only exercise verbs and APIs already delivered by T55 (C26) and earlier tickets; if a pattern seems to want a new knob, it belongs to that component's ticket, not here.
- **The T65 system-acceptance gate itself** — this ticket supplies documentation content and its per-item CI checks and registers them in the criteria matrix; assembling the full matrix-to-test mapping and the cross-toolchain determinism checks is T65's job.
- **The structure-fixture and structure-diff machinery** (C28, T61) and the resume/durable mechanics (C27, T57/T58) — this ticket *uses* them for the locality and durable-boundary demonstrations but does not build or modify them.
- **A tutorial site, a book, generated API-doc hosting, or any web interface** — out of the product's permanent scope; the deliverables are the in-repo README, rustdoc, examples, and cookbook only.
- **A DSL, macro sugar, or config format to shorten the examples** — the examples show the real authoring surface; inventing a shorthand would cross the "not a DSL" boundary.
- **Documenting hypothetical distributed, scheduled, or backfill-orchestrator usage** — the "When not to use this" and operational-model boundaries are stated as boundaries, never as a roadmap.
