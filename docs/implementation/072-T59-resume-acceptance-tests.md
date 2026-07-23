# 072 · T59 — C27: resume acceptance tests

> **Milestone:** M4 · **Size:** M · **Type:** feature (tests) · **Components:** C27
> **Branch:** `feat/t59-resume-acceptance-tests` · **Depends on:** T58, T54b · **Blocks:** T63

## Why / context
Resume (C27) is the operability payoff: after an interruption, expensive work already done is carried forward instead of repeated. T58 built the resume core (fingerprint gate, parameter derivation, reference existence check, the seed/closure/demand algorithm, lineage linkage) and T54b added scratch carry-forward for re-executing nodes. This ticket is the black-box acceptance suite that pins every C27 acceptance criterion — plus the scratch behaviour from C18 that resume triggers — against real sample pipelines run through the resume verb, so the guarantees are executable and regression-proof. It governs by the acceptance criteria in arch.md `C27 · Resume` and the `satisfied-from-prior` and success-like definitions in the Vocabulary, the artifact lineage/reference-copy-forward behaviour in `C22`, and the scratch carry-forward rule in `C18`. This suite is a gate for the M4 demo (T63).

## Objective
Prove, through end-to-end tests over sample pipelines, that resume behaves exactly as C27 specifies: it refuses when it must, carries prior success forward when it can, re-runs demand-driven when it should, and produces a self-contained, correctly-linked artifact. Concretely, this ticket delivers acceptance tests for each of:

- **Fingerprint refusal with diff** — a structural-fingerprint mismatch refuses and prints the structural difference.
- **Policy-change proceed-with-diff** — a policy-only change proceeds and prints the per-node policy diff.
- **Durable-success satisfied** — a durable prior success is not re-executed; a re-executing consumer that demands its value receives the rehydrated value.
- **In-memory success re-run only when demanded** — an in-memory prior success re-executes when, and only when, a re-executing consumer demands its value.
- **Undemanded non-durable success satisfied-from-prior** — the cleanup-after-publish shape: an ordering-only prior success whose value nothing demands is `satisfied-from-prior` even though it is not durable, and the re-running downstream's trigger rule fires on the satisfied (success-like) upstream.
- **Full-success no-op** — resuming a fully successful run exits successfully with an empty seed and re-executes nothing.
- **Dangling-reference plan failure** — a durable reference to a deleted object fails the resume plan before any node executes.
- **Multi-generation resume** — a resume of a resume: the newest artifact links to its immediate parent and to the lineage root, and durable references are copied forward so the artifact is self-contained.
- **Scratch carry-forward observed by a re-executing node** — a re-executing node sees the scratch its prior-run counterpart wrote, carried forward from the linked prior run.
- **Parameter-conflict refusal with force-flag recording** — conflicting parameters refuse with a diff; the force flag overrides and is recorded in the resumed artifact.

Use the C28 full-pipeline testing surface (fakes, tiny fixtures, real scheduler); do not build a bespoke harness. Each sample pipeline is the smallest shape that exercises its scenario. Scenarios must be independent — each stages its own prior run (or runs) in a fresh run-store directory, then invokes resume against it.

## Test plan (write these first — TDD)

Each scenario stages a prior run into a temporary run store, mutates the world as described, invokes the resume verb, and asserts on observable output: the resumed run's artifact, the resume verb's stdout/stderr, its exit code, and side effects a fake resource recorded (which nodes' task bodies actually executed).

1. **Fingerprint refusal prints the structural diff.**
   Setup: complete a prior run of a sample pipeline; keep its run-store directory. Point the resume at a variant binary/pipeline whose node or edge set differs so the structural fingerprint no longer matches.
   Action: invoke resume against the prior run directory.
   Expected: resume refuses with the resume-refusal exit code, prints a structural diff naming what changed, executes no task body, and writes no successful resumed artifact.

2. **Policy-only change proceeds and prints the per-node policy diff.**
   Setup: complete a prior run; construct a variant that changes only policy values (for example a raised timeout or retry count) so the structural fingerprint is unchanged and only the policy hash diverges.
   Action: invoke resume.
   Expected: resume proceeds (does not refuse), prints a per-node policy diff identifying the changed nodes and values, and completes the run.

3. **Durable prior success is satisfied and rehydrated on demand.**
   Setup: a two-node pipeline where the upstream is durable and the downstream consumes its value; complete a prior run where upstream succeeded and downstream did not (arrange downstream to have not-succeeded so it is in the seed).
   Action: invoke resume.
   Expected: upstream is marked `satisfied-from-prior` and its task body does not execute; downstream re-runs, its demanded input is filled by rehydration from the durable reference, and it receives the same value the prior run produced.

4. **In-memory prior success re-runs only when demanded.**
   Two sub-cases, staged independently.
   - Setup A: a producer with an in-memory (non-durable) output feeding a consumer; complete a prior run where producer succeeded and consumer did not. Action: resume. Expected: because the re-running consumer demands the value and it cannot be rehydrated, the producer re-executes (its task body runs), and its own upstream demands cascade as specified.
   - Setup B: the same producer's value is *not* demanded by any node that re-runs in the resumed plan (nothing downstream re-runs, or the only demander is itself satisfied). Action: resume. Expected: the in-memory producer is `satisfied-from-prior` and its task body does not execute.

5. **Cleanup-after-publish: undemanded non-durable success is satisfied-from-prior and the downstream rule fires.**
   Setup: a pipeline where `publish` succeeds with an ordering-only edge to `cleanup` (nothing demands publish's value, and publish is not durable), and `cleanup` did not succeed in the prior run (so it is in the seed). Complete that prior run.
   Action: invoke resume.
   Expected: `publish` is `satisfied-from-prior` (not re-executed) even though it is not durable; `cleanup` re-runs; `cleanup`'s trigger rule evaluates its upstream as success-like and fires, and `cleanup` reaches a success terminal state.

6. **Full-success resume is a no-op.**
   Setup: complete a prior run in which every node succeeded.
   Action: invoke resume against it.
   Expected: resume exits successfully, executes no task body (empty seed), and every node in the resumed artifact is `satisfied-from-prior`, each carrying the originating run identity.

7. **Dangling durable reference fails the resume plan before execution.**
   Setup: complete a prior run whose durable upstream feeds a not-yet-succeeded downstream; then delete the durable object the reference points to, leaving the reference recorded but unresolvable.
   Action: invoke resume.
   Expected: resume fails the plan up front with a message identifying the missing reference, executes no task body (in particular it does not fail partway through the resumed run), and no node re-executes.

8. **Multi-generation resume links parent and lineage root and copies references forward.**
   Setup: run 1 completes partially; resume it into run 2, still leaving some node not-succeeded; resume run 2 into run 3.
   Action: after run 3 completes, inspect its artifact.
   Expected: run 3's artifact header names run 2 as immediate parent and run 1 as lineage root; every durable reference needed to make run 3 self-contained is present in run 3's artifact (copied forward), so run 3's artifact can be read without run 1 or run 2 present.

9. **Scratch carry-forward is observed by a re-executing node.**
   Setup: a node that writes an identifiable value into its scratch on a first attempt and, on re-execution, continues from that scratch (a checkpoint shape). Complete a prior run in which that node wrote scratch but did not reach success, so it is in the seed.
   Action: invoke resume.
   Expected: the re-executing node observes the scratch value its prior-run counterpart wrote (scratch was copied forward from the linked prior run into the new run's namespace), demonstrated by the node reporting it continued from the checkpoint rather than starting over.

10. **Parameter conflict refuses with a diff; force overrides and is recorded.**
    Two sub-cases, staged independently.
    - Setup A: complete a prior run with a known parameter set; invoke resume supplying a parameter value that conflicts with the prior run's derived value, without the force flag. Action: resume. Expected: refusal with the resume-refusal exit code and a diff showing prior-versus-supplied values; nothing re-executes.
    - Setup B: the same conflict, now with the force flag. Action: resume. Expected: resume proceeds using the overriding value, and the resumed artifact records that the force flag was used (and the overriding value it was invoked with).

## Definition of done
- [ ] A resume against a mismatched structural fingerprint refuses and prints the structural difference (test 1).
- [ ] A policy-only change proceeds and prints the per-node policy diff (test 2).
- [ ] A `satisfied-from-prior` node is not re-executed, and a re-executing consumer that demands its value receives the rehydrated value (tests 3, 6).
- [ ] A node that succeeded with an in-memory output is re-executed when, and only when, a re-executing consumer demands its value (test 4, both sub-cases).
- [ ] A prior success whose value nothing demands is `satisfied-from-prior` even when not durable, verified with the cleanup-after-publish shape: ordering upstream succeeded, downstream re-runs, its rule fires (test 5).
- [ ] A dangling durable reference fails the resume plan before execution begins, with no node re-executing (test 7).
- [ ] Resuming a fully successful run is a no-op that exits successfully, with every node marked `satisfied-from-prior` carrying its originating run identity (test 6).
- [ ] Supplying parameters that conflict with the prior run refuses with a diff; the force flag overrides and is recorded in the resumed artifact (test 10, both sub-cases).
- [ ] A resumed run produces its own artifact, linked to both its immediate parent and its lineage root, with durable references copied forward so the artifact is self-contained (test 8).
- [ ] Scratch is copied forward from the linked prior run for re-executing nodes and is observable by the re-executing node (test 9; C18 behaviour that T54b delivers).
- [ ] Every scenario is independent: it stages its own prior run(s) in a fresh run-store directory and cleans up, and assertions are on observable output only (resumed artifact, resume stdout/stderr, exit code, recorded task-body executions) — not on internal state.
- [ ] Refusal scenarios assert the resume-refusal exit code and confirm no task body executed; proceed scenarios assert the expected terminal state per node from the normative taxonomy.
- [ ] Tests use the C28 full-pipeline testing surface (fakes, tiny fixtures, real scheduler) rather than a bespoke harness, and the full suite completes in seconds.
- [ ] Sample pipelines added for this ticket are the smallest shape that exercises their scenario and are documented in-line with the scenario they serve.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The resume core itself — the fingerprint gate, parameter derivation, reference existence check, the seed/closure/demand algorithm, and lineage linkage — is T58; this ticket only tests it and must not reimplement or alter it.
- Scratch carry-forward mechanics are T54b (C18); this ticket only observes the behaviour, it does not implement copy-forward.
- Cross-tool-version resume: v1 makes no such promise (C27 refuses across tool versions); do not add or test a cross-version resume path here beyond confirming, if convenient, that the documented refusal message stands. The algorithm-version "cannot compare" and tool-version refusals are covered by T58's own tests, not re-proven here.
- Single-node replay artifacts and their `not-requested` marking are the CLI-contract suite (C26 / T56), not this ticket.
- No scheduler, distributed execution, metadata store, web interface, DSL, or backfill orchestration — resume is demand-driven replay of a *fixed* graph from a prior run's recorded outputs; the graph shape never changes at runtime, and no scenario here may introduce runtime graph mutation, cross-process coordination, or external state stores.
