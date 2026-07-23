# 067 · T57 — C27: durable-output declaration and recording

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C27
> **Branch:** `feat/t57-durable-output-declaration-recording` · **Depends on:** T42, T0.8 · **Blocks:** T55, T58

## Why / context
Resume (C27) can only skip completed work if a completed node's value can be re-obtained without re-running the node. This ticket lands the foundation that makes that possible: the per-node durability flag on policy (C5), the serialize-reference / rehydrate reference contract decided in T0.8, the assembly-time rejection of a durable-marked node whose output type lacks the contract, and the per-attempt recording of the produced reference into the run artifact (C22, produced by T42's fold). It builds directly on the durability decision (T0.8) and the fold function (T42) and is governed by arch.md sections `C27 · Resume` (the durable-output contract paragraph and its acceptance criteria) and `C5 · Node policy`. This ticket deliberately stops at *declare, enforce, record* — the demand-driven resume algorithm and the existence check that consume these references are T58's job.

## Objective
Make durability a first-class, enforced, recorded property of a node, without implementing resume itself.

- Add the durability flag to node policy (C5): declared per node at registration, defaulting to *not durable*, changeable without touching task code, and surfaced in the graph artifact like every other effective policy value.
- Provide the reference contract (per T0.8): the pair of operations that serialize a durable reference to where a produced value lives and rehydrate the typed value from such a reference later, as a trait a durable node's output type must satisfy. (The existence check and rehydration *call sites* belong to T58 — here only the contract's shape and its serialize side are exercised.)
- Enforce the contract at assembly (C7 surface, per T0.8): a node marked durable whose output type does not implement the reference contract is an assembly error, reported alongside all other assembly problems, naming the offending node and the missing contract.
- Record the reference per attempt: when a durable node produces its output successfully, the serialized reference is captured and lands in that attempt's record in the run artifact (C22), with non-durable and failed attempts recording no reference.
- Ensure the recorded reference is self-contained in the artifact (a reference value, not a live handle) so a later run can read it without the producing run's process.

## Test plan (write these first — TDD)

**Policy default.** Setup: register a node with no policy stated, and an otherwise-identical node with every policy value written out explicitly including durability set to its default. Action: assemble both and read their effective policy from the graph artifact. Expected: both report *not durable*, and the two effective-policy representations are identical (so the defaulted node hashes identically under the policy hash, C21).

**Durability is declarable and appears in the graph artifact.** Setup: register a node whose output type implements the reference contract and mark it durable in policy. Action: assemble and read the graph artifact. Expected: that node's effective policy shows durability enabled; no other node's durability is affected; the task's code was not touched to set the flag.

**Assembly rejects durable-without-contract.** Setup: register a node marked durable whose output type does *not* implement the reference contract. Action: assemble. Expected: assembly fails; the reported problem names the offending node and states that its output type lacks the durable reference contract; the failure is one entry in the full problem list, not a panic and not an early abort that hides other problems.

**Assembly reports durable-without-contract alongside other problems.** Setup: build a pipeline containing at least one durable-without-contract node and one unrelated independent assembly problem (for example a duplicate node name). Action: assemble. Expected: both problems appear in the single reported problem list; neither masks the other.

**A durable node with the contract passes assembly.** Setup: register a durable node whose output type implements the reference contract, wired into an otherwise valid pipeline. Action: assemble. Expected: assembly succeeds with no durability-related problem.

**Reference is recorded on successful durable output.** Setup: a pipeline with one durable node whose output type implements the contract; run it to a successful terminal state (or fold a stream that carries its successful attempt). Action: fold the event stream into the run artifact (T42) and inspect the durable node's attempt record. Expected: the attempt record carries the serialized durable reference for the value the node produced, and that reference is a plain serialized value with no dependency on the producing process.

**Non-durable success records no reference.** Setup: a pipeline with a non-durable node that succeeds. Action: fold and inspect its attempt record. Expected: the attempt record carries no durable reference (the reference slot is absent/empty), while status, phases, and cost are recorded as usual.

**Failed or retried attempts record no reference.** Setup: a durable node whose first attempt fails and whose second attempt succeeds. Action: fold and inspect both attempt records. Expected: the failed attempt carries no durable reference; the succeeding attempt carries exactly one reference; there is one record per attempt (retries are not collapsed).

**Recorded reference round-trips through the schema.** Setup: an artifact containing a durable node's recorded reference. Action: serialize the artifact to its on-disk form and re-read it with the published schema/validation helper (T39). Expected: the reference field validates against the schema and deserializes back to the same reference value (self-contained, T39's durable-reference field is populated and well-formed).

**Serialize side of the contract is exercised end to end.** Setup: a durable output type implementing the contract, given a value that lives at a known durable location. Action: produce the value in a run and observe the serialized reference reaching the attribute record. Expected: the reference identifies the durable location; rehydrating from that same reference (contract's rehydrate operation, exercised in isolation here as a contract-level unit check, not through the resume planner) yields back an equal typed value.

## Definition of done
- [ ] Node policy (C5) carries a durability flag, attached at registration and never inside the task; its default is *not durable*, documented and applied uniformly.
- [ ] Changing durability requires no change to task code.
- [ ] A node's effective durability appears in the graph artifact, including when it holds its default value.
- [ ] A node with no stated policy behaves identically to one with durability written out at its default, including under the policy hash (C21).
- [ ] The reference contract (per T0.8) exists as the trait a durable node's output type must implement, expressing serialize-reference and rehydrate; a durable output type's bound (`Send + Sync + 'static` plus the reference contract, per C10) is enforced by the type system for durable nodes.
- [ ] Assembly rejects a durable-marked node whose output type lacks the reference contract, reporting a problem that names the node and the missing contract.
- [ ] The durable-without-contract problem is reported within assembly's full problem list, not via panic or early abort, and does not mask unrelated assembly problems.
- [ ] A durable node whose output type implements the contract passes assembly with no durability-related problem.
- [ ] On a successful attempt of a durable node, the serialized reference is recorded in that attempt's record in the run artifact (C22, via T42's fold).
- [ ] Non-durable successful attempts record no durable reference; failed and timed-out attempts of a durable node record no reference.
- [ ] There is one record per attempt; a durable node that failed then succeeded records the reference only on the succeeding attempt.
- [ ] The recorded reference is a self-contained serialized value (not a live handle) and validates against and round-trips through the published artifact schema (T39, durable-reference field).
- [ ] Rustdoc on the public durability flag and reference-contract items explains declare/serialize/rehydrate and states that outputs a teardown deletes are not resume-safe (cross-reference C17), and that in-memory outputs cannot be rehydrated (cross-reference C27 authoring pressure).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The resume algorithm itself — the must-run seed, downward closure, upward demand resolution, `satisfied-from-prior` marking, and slot-filling by rehydration are T58 (C27). This ticket only declares, enforces, and records.
- The reference *existence check* at resume time and dangling-reference plan failure — T58.
- Copying durable references forward into a resumed artifact and lineage linkage — T58.
- Scratch-store carry-forward (C18) — T53/T54b; durability of the output value is distinct from the durable scratch store.
- CLI wiring of single-node replay and resume verbs that rehydrate inputs from these references — T55/T58.
- Retention/release semantics of the output slot (C10) beyond noting that a durable output additionally requires the contract — retention is its own policy field, governed elsewhere.
- Any move toward a metadata store, an external artifact catalog, or cross-tool-version reference portability — v1 makes no cross-version promise and this ticket adds no such surface (permanent scope boundary: dagr is not a metadata store).
