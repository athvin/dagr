# 068 · T55 — C26: CLI contract

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C26
> **Branch:** `feat/t55-cli-contract` · **Depends on:** T34, T36, T40, T42, T46, T57, T0.6 · **Blocks:** T56, T58, T63, T64

## Why / context
Every dagr binary must expose the same command surface so operators learn it once and orchestrators (cron, a Kubernetes Job, a CI step, systemd) get truthful exit codes. This ticket implements C26 · Command-line contract (arch.md §541): the library-supplied verbs, the typed-parameter/derived-parsing seam, the reserved library-flag namespace, and the exhaustive exit-code table with its precedence rules. It wires together already-built machinery — the graph artifact (C20/T40), the diagram renderer (C24/T46), the stream-fold function (C22/T42), the failure/propagation taxonomy (C15/T34), signal-driven shutdown and the sink-failure exit path (C16/T36), durable-reference rehydration for single-node replay (C27/T57), and the run store (C18/C19/T0.6). It defines the operational contract that T56 (acceptance tests), T58 (resume core, which replaces the stubbed `resume` verb), and the M4 demo (T63) all build on; the run-failure-beats-cancellation precedence here is the load-bearing decision this ticket locks.

## Objective
Provide the standard, library-owned command-line contract that every pipeline binary inherits unchanged.

- Implement the library-supplied verbs, each behaving identically across every pipeline built with the library:
  - `graph` — emit the graph artifact (C20) for this binary; no store required.
  - `validate` — run assembly (C7) only, exit non-zero on any assembly failure, print every problem found.
  - `render` — produce a diagram (C24) from a graph artifact, optionally overlaying a run artifact; no live pipeline needed.
  - `run` — mint run identity and open the store/stream before assembly (per bootstrap, arch.md §63), then execute.
  - `single-node` — replay node N from a prior run R: rehydrate that node's inputs from the durable references recorded in R's artifact (C27/T57); refuse with a clear message naming the offending input when an input is not durable; allow a consume-nothing node to run standalone with no prior run; mark unselected nodes `not-requested` in the resulting artifact variant.
  - `resume` — stubbed: recognized in the verb table and help, gated behind the same fingerprint/store preconditions surface, but returns a "not yet implemented" outcome (its real algorithm lands in T58); a replay/resume refusal shares the resume-refusal exit code.
  - `fold` — wire the standalone stream-fold function (C22/T42) to produce the interrupted run artifact from a crashed run's event stream.
  - `prune` — delete old runs from the run store by count or by age; nothing is deleted implicitly by any other verb.
- Define a typed parameter struct the pipeline declares once, from which the library derives argument parsing; parameters are validated at bootstrap (after assembly, which never sees them — C7) and carried in the context thereafter.
- Reserve a library-flag namespace so pipeline parameters can never collide with or shadow library-owned flags; collision is a hard, named error.
- Implement the exit-code table by cause with precedence, documented in exactly one place and stable within a major version.
- Print the available verbs and exit cleanly when invoked with no arguments.

## Test plan (write these first — TDD)

**Verb-parity and inspection verbs**
- Given two distinct pipelines both built with the library, when each is invoked with `graph`, then both emit a well-formed graph artifact via the identical code path and the verb table is identical between them.
- Given a pipeline whose assembly succeeds, when `validate` runs, then it exits with the success code and prints no problems.
- Given a pipeline with two independent assembly failures, when `validate` runs, then it exits with the assembly-failure code and prints both problems (not just the first).
- Given a previously emitted graph artifact, when `render` runs with no live binary state, then it produces diagram source; and when `render` is given a run artifact to overlay, then nodes are coloured by terminal state — verifying the renderer (C24) is reachable purely from artifacts.

**Parameters and reserved namespace**
- Given a pipeline declaring a typed parameter struct, when `run` is invoked with a value that fails validation, then the process exits with the invalid-usage code before any node executes (assert via the event stream that no attempt was recorded).
- Given a pipeline that attempts to declare a parameter whose flag name lands in the reserved library-flag namespace, when the binary is built or bootstrapped, then a named collision error is produced and the run does not proceed.
- Given valid typed parameters and a data interval supplied on the command line, when `run` completes, then those exact parameter values and the verbatim interval appear in the run artifact header (C22) and were visible in the context, and assembly never observed them.

**Exit-code table and precedence**
- Given a run in which one non-teardown node ends `failed` and no external signal arrives, when `run` completes, then the process exits with the run-failure code.
- Given a run in which a non-teardown node `timed-out`, when `run` completes, then the exit code is the same run-failure code (that state also counts as run failure).
- Given a run under stop-on-first-failure where one node fails and the failure triggers self-inflicted cancellation of pending nodes, when `run` completes, then the exit code is the run-failure code (the consequent cancellation does not mask it) — the precedence assertion.
- Given a run with no run failure that receives an external SIGTERM and drains within budget (C16/T36), when the process exits, then the exit code is the cancellation code, distinct from run failure.
- Given a skip-only run in which every node ends in a skip-family state and none `failed`/`timed-out`, when `run` completes, then the exit code is the success code.
- Given assembly failure, bootstrap failure (e.g., a declared cost that cannot fit, per §63), and sink failure at shutdown (§358) respectively, when each is provoked, then each produces its own distinct exit code, and every code in the table has at least one test — the table is exhaustive over verbs and causes.
- Given a `single-node` replay whose requested input is not durable, when `single-node` runs, then it refuses with a message naming which input and why, and exits with the resume-refusal code (shared with resume refusal).

**Single-node replay**
- Given a prior run R whose node N recorded durable references for its inputs (C27/T57), when `single-node` replays N from R, then N's inputs are rehydrated from those references, N executes, and the produced artifact marks every unselected node `not-requested`.
- Given a node that consumes nothing, when `single-node` targets it with no prior run supplied, then it runs standalone and succeeds.

**Fold and no-arg help**
- Given the event stream of a run that was killed mid-execution (crash clause of system criterion 3), when `fold` is invoked on that stream, then it produces the interrupted run artifact matching the standalone-function output (T42/T68), and the verb exits cleanly.
- Given the binary invoked with no arguments, when it runs, then it prints the available verbs and exits with the success code.
- Given the `resume` verb (stubbed) invoked, when it runs, then it is recognized, reports "not yet implemented," and exits with a defined code — so T58 can replace the body without changing the surface.

## Definition of done
- [ ] Every C26 verb (`graph`, `validate`, `render`, `run`, `single-node`, `resume` (stubbed), `fold`, `prune`) is supplied by the library and behaves identically across all pipelines built with it.
- [ ] `validate` exits non-zero on any assembly failure and prints every problem found (not only the first).
- [ ] `run` exits with the run-failure code whenever any non-teardown node ended `failed` or `timed-out`, including under stop-on-first-failure where self-inflicted cancellation must not mask the failure.
- [ ] Run failure beats consequent cancellation; cancellation is reported only for externally originated termination with no run failure (`abandoned` attributes to cancellation, never to run failure).
- [ ] The exit-code table is exhaustive over verbs and causes, documented in exactly one place, stable within a major version, with distinct codes for: success (including skip-only runs), run failure, assembly failure, bootstrap failure, cancellation, resume refusal (also used by a single-node replay refused for a non-durable input), sink failure, and invalid usage — and each code has a test.
- [ ] Invalid parameters are rejected at bootstrap, before any node executes; assembly never observes parameters (C7).
- [ ] The pipeline's typed parameter struct drives library-derived parsing; parameters and the verbatim data interval are carried in the context and recorded in the run artifact header (C22).
- [ ] A reserved library-flag namespace prevents any pipeline parameter from shadowing or colliding with a library flag; a collision is a named, hard error.
- [ ] `single-node` replays node N from run R by rehydrating inputs from durable references (C27/T57); a non-durable input refuses with a message naming which input and why; a consume-nothing node runs standalone with no prior run; unselected nodes are marked `not-requested` in the artifact variant.
- [ ] `fold` wires the standalone C22/T42 function and produces the interrupted artifact from a crashed run's event stream.
- [ ] `prune` removes old runs from the run store by count or age; no other verb deletes runs implicitly.
- [ ] `render` reaches the C24 renderer from artifacts alone, with optional run-overlay, requiring no live pipeline.
- [ ] `resume` is present as a recognized, help-listed, stubbed verb that returns a defined "not yet implemented" outcome, leaving a stable seam for T58.
- [ ] Running the binary with no arguments prints the available verbs and exits cleanly.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The resume algorithm itself (seed/closure/demand, fingerprint gating, parameter derivation with the force flag) — that is T58/C27; this ticket only stubs the `resume` verb and reserves its refusal exit code.
- CLI acceptance tests as a suite — T56 owns the exhaustive black-box coverage; this ticket ships the unit/behaviour tests its own TDD requires but does not build the acceptance harness.
- The durable-output reference contract, its assembly-time rejection, and reference recording — those are T57/C27; this ticket only consumes recorded references for replay.
- The renderer internals and golden-file corpus — T46/C24; this ticket only wires the `render` verb to them.
- Cross-process coordination, scheduling, backfill interval computation, a config DSL, or any web/metadata surface — permanent scope boundary; the CLI never decides *when* a pipeline runs, never advances a data interval, and never coordinates between concurrent runs (that is the operator's call via pool-pinning flags).
