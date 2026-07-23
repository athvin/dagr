# 039 · T29 — C5: node policy

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C5
> **Branch:** `feat/t29-node-policy` · **Depends on:** T14, T22, T0.4, T0.5 · **Blocks:** T31, T33, T34, T40

## Why / context

This ticket lands C5 (`### C5 · Node policy`, arch.md), the per-node operational knobs kept out of the task's logic: retries, backoff, timeout, declared cost, trigger rule, execution-class override, group, retention, and durability. It is the M2 keystone that later capacity (T31), execution-class dispatch (T33), failure/trigger runtime (T34), and fingerprint (T40) tickets read their inputs from, so the struct's field set, defaults, and hashing behaviour must be exactly right here. It consumes the decision tables T0.4 (the closed trigger-rule set and terminal-state classes) and T0.5 (the cost model — working vs. output residency, per-pool vectors), attaches at the assembly surface built by T14, and absorbs the interim M1 retry knob shipped in T22 so retry configuration now lives in one policy home. The governing rule for this ticket: a node with no stated policy must behave identically to one with every default written out, *including under the C21 policy hash*.

## Objective

Define the immutable per-node policy value, its conservative defaults, and its attach-at-registration surface, wired into assembly so that its validation runs there and its effective values reach the graph artifact and the policy hash.

- Model the policy fields: retry count and backoff shape (attempt cap, base, cap, jitter — the shape migrated from T22's interim knob), per-attempt timeout, declared cost vector, trigger rule, execution-class override, group label, output retention flag, durability flag.
- Model the declared cost vector per T0.5: one entry per admission pool in that pool's native unit (bytes for memory, thread count for thread pools), with memory split into *working memory* and *output residency*.
- Give every field a single documented default, applied uniformly: no retries, no timeout, zero declared cost, `all-succeeded`, execution class as declared by the task, no group, release-once-consumed, not durable.
- Restrict the trigger rule to the closed set from T0.4 (`all-succeeded`, `all-terminal`, `any-failed`) and keep the compile-time constraint that non-default rules are expressible only on consume-nothing nodes (already enforced upstream — this ticket must not weaken it).
- Add the execution-class override with its constraint: synchronous work may move between the blocking and compute classes; await-bound work may not be forced to a synchronous class. An invalid override is an assembly error, reported through T14's all-problems assembly path and naming the offending node.
- Migrate the T22 interim M1 retry knob into this policy struct so retry configuration has exactly one home; the attempt runner reads it from policy.
- Expose the attach-at-registration API so setting any policy value requires no change to task code, and the full effective policy (defaulted values included) is emitted into the graph artifact.
- Ensure policy participates in hashing correctly: policy values (retries, timeouts, costs, classes, retention, durability) feed the C21 policy hash; the trigger rule feeds the structural fingerprint; the group label feeds neither. Defaulted and written-out-default policies must hash identically.

## Test plan (write these first — TDD)

- **Every field has a documented default, applied uniformly.** Setup: register a node giving no policy. Action: inspect its effective policy. Expected: retry count is zero, timeout is absent, declared cost is zero on every pool entry (working and output residency both zero), trigger rule is `all-succeeded`, execution class equals the task's declared class, group is absent, retention is release-once-consumed, durability is off — each matching the documented default exactly.

- **All-defaults node equals no-policy node, behaviourally.** Setup: register the same task twice — once with no policy, once with every default written out explicitly. Action: assemble and compare the two nodes' effective policies. Expected: the two effective policies are field-for-field equal.

- **All-defaults node equals no-policy node, under the policy hash.** Setup: as above, two pipelines identical except one node states every default explicitly and the other states none. Action: assemble both and read the C21 policy hash of each. Expected: the policy hashes are byte-identical (and the structural fingerprints match too).

- **A single changed policy value changes only the policy hash.** Setup: two pipelines identical except one node's retry count differs. Action: assemble both, read both hashes. Expected: structural fingerprints match; policy hashes differ.

- **Trigger rule feeds the structural fingerprint, not the policy hash.** Setup: two pipelines identical except one consume-nothing node's trigger rule differs (`all-succeeded` vs. `all-terminal`). Action: assemble both, read both hashes. Expected: structural fingerprints differ; comparison of the remaining policy values yields no policy-hash-only divergence attributable to the rule.

- **Group is in neither hash.** Setup: two pipelines identical except one node carries a group label and the other does not (and a third variant renames the group). Action: assemble all, read both hashes. Expected: structural fingerprint and policy hash are identical across all variants; the group difference is visible only as artifact/diagram organization.

- **Valid execution-class override on synchronous work assembles.** Setup: register a task whose declared work shape is synchronous, override its class from blocking to compute (and the reverse in a second case). Action: assemble. Expected: assembly succeeds and the effective class reflects the override.

- **Invalid execution-class override on await-bound work fails assembly.** Setup: register an await-bound task and override its class to a synchronous class. Action: assemble. Expected: assembly fails, the error names the offending node and states the incompatible-work-shape reason, and — combined with an unrelated duplicate-name node — assembly still reports both problems (the invalid override does not short-circuit T14's all-problems reporting).

- **Declared cost vector carries per-pool native units with the memory split.** Setup: register a node declaring memory working memory, memory output residency, and a thread count. Action: read the node's effective declared cost from the graph artifact. Expected: the memory pool entry shows distinct working and output-residency values, the thread pool entry shows the thread count, and unspecified pool entries are zero.

- **Setting any policy value requires no task-code change.** Setup: one task value; register it twice with different retry counts and timeouts. Action: assemble. Expected: both nodes exist with their distinct effective policies and the task's own code is byte-identical between registrations (the difference lives entirely in the attach-time policy).

- **Full effective policy, including defaults, appears in the graph artifact.** Setup: register one node with a partial policy (only a timeout set) and one with none. Action: assemble and read the graph artifact. Expected: both nodes' entries carry the *complete* effective policy with every defaulted field written out, not just the fields the author set.

- **Retention flag is honoured as policy, not as task behaviour.** Setup: register a node marked retained and an identical one not retained. Action: read effective policies and the policy hash. Expected: the retained flag is present in the effective policy of the first and absent in the second, and it contributes to the policy hash (the two hashes differ).

- **Durability flag is present in policy and gates the assembly contract check.** Setup: register a durable-marked node whose output type does implement the reference contract, and a second durable-marked node whose output type does not. Action: assemble. Expected: the first node carries `durable` in its effective policy and assembles; the second fails assembly through the existing durable-without-contract check (this ticket supplies the flag that arms that check; it does not re-implement the check).

- **Migrated retry knob is read from policy by the attempt runner.** Setup: a node with a retry count and a backoff shape set via policy, driving a task that fails retry-eligibly. Action: run the node. Expected: the attempt runner performs the configured number of attempts with the configured backoff shape, and no separate M1 retry knob remains reachable (the interim knob is gone from the public surface).

## Definition of done

- [ ] Every policy field — retry count, backoff shape, per-attempt timeout, declared cost vector, trigger rule, execution-class override, group, retention, durability — has a single documented default, applied uniformly.
- [ ] Documented defaults are exactly: no retries, no timeout, zero declared cost, `all-succeeded`, execution class as declared by the task (await-bound if the task left it unspecified), no group, release the output once consumed, not durable.
- [ ] Every node's *full effective policy* — including defaulted values written out — appears in the graph artifact.
- [ ] Changing any policy value requires no change to task code; policy is attached at registration, never inside the task.
- [ ] A node with no stated policy behaves identically to one with every default written out, including under the C21 policy hash (defaulted values hash identically to written-out defaults).
- [ ] The declared cost is a vector with one entry per admission pool in that pool's native unit (bytes for memory, thread count for thread pools), and the memory entry splits into working memory and output residency per T0.5.
- [ ] The trigger rule is drawn only from the closed T0.4 set (`all-succeeded`, `all-terminal`, `any-failed`); the default is `all-succeeded`; the compile-time restriction that non-default rules apply only to consume-nothing nodes is preserved, not weakened.
- [ ] The execution-class override is constrained per C13: synchronous work may move between blocking and compute; await-bound work may not be forced to a synchronous class.
- [ ] An execution-class override incompatible with the task's declared work shape fails assembly, names the offending node, and is reported through T14's all-problems path (it does not short-circuit reporting of other problems).
- [ ] Policy values (retries, timeouts, costs, classes, retention, durability) feed the C21 policy hash; the trigger rule feeds the structural fingerprint; the group label feeds neither hash.
- [ ] The durability flag lives in policy and arms the existing assembly durable-without-contract check (T14/T0.8); this ticket supplies the flag, not a second implementation of the check.
- [ ] The retention flag lives in policy and marks a node's output to be kept until run end (C10), redeemable by the embedding program after the run.
- [ ] The T22 interim M1 retry knob is migrated into this policy struct; retry/backoff configuration has exactly one home and the attempt runner reads it from policy; the interim knob is removed from the public surface.
- [ ] The policy value is immutable once assembled and produced through the attach-at-registration API.
- [ ] Rustdoc on the policy type and the attach API documents each field, its default, and which hash it participates in.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

None were stated (this section and the `docs/tasks.md` T29 entry carry no `Q:`
items). Three design decisions the objective left implicit are recorded here for
the record:

- **Trigger rule and group live *beside* the policy value, not as settable
  `NodePolicy` fields.** The objective lists the trigger rule and group among the
  fields C5 owns, but it also mandates keeping the compile-time constraint that a
  non-default trigger rule is expressible *only* on a consume-nothing node — a
  constraint enforced upstream by the binding typestate (`ConsumesNothing` vs.
  `ConsumesData`). Adding a settable `trigger_rule` to `NodePolicy` (which any node
  carries) would weaken that guarantee. Resolution: the trigger rule stays sourced
  from the binding (a new `Flow::register_source_with_trigger` exposes it for
  sources — themselves consume-nothing — without offering it to data nodes), and
  the group stays the `register_*_in_group` presentation seam (C6/T51). Both
  surface on the resolved `EffectivePolicy` (the artifact-facing, defaults-written-out
  view), which is where "full effective policy in the graph artifact" is satisfied.

- **`RetryConfig` becomes the runner's *derived input*, not a second authoring
  knob.** "Retry configuration has exactly one home" and "the interim knob is
  removed from the public surface" are reconciled by making the C5 policy the sole
  *authoring* home: `NodePolicy` carries the retry count and full `Backoff` shape,
  and `NodePolicy::retry_config()` derives the `RetryConfig` the attempt runner
  (`run_with_retries`) reads (`retries(n)` → `n+1` total attempts). `RetryConfig` /
  `Backoff` remain the runner's *parameter types* (the runner signature and its T20/
  T21/T22 tests depend on them), but there is no longer an independent registration
  path that authors retries outside policy.

- **Backoff equality is over the growth factor's bit pattern.** `NodePolicy` must be
  `Eq`/`Hash` (it lives inside the `Eq`-deriving `PipelineNode`/`AssemblyArtifact`),
  but `Backoff` holds an `f64` factor. Resolution: `Backoff` gets a manual
  `PartialEq`/`Eq`/`Hash` comparing the factor by `to_bits()` — total, deterministic,
  and consistent with the canonical policy-hash encoding (which also hashes the raw
  bits), so two policies stating the same backoff compare and hash identically.

## Out of scope

- The runtime *consumption* of these knobs: admission/capacity checks against the cost vector (T31/C12), execution-class dispatch onto pools (T33/C13), and failure/propagation/trigger-rule runtime evaluation (T34/C15) are downstream tickets — this ticket only defines the values they read.
- Computing the fingerprint and policy hash themselves (T40/C21); this ticket ensures policy contributes the right inputs, but the hashing algorithm and its artifact fields land in T40.
- The durable-output contract itself and the durable-without-contract assembly check (T0.8/T14/C27); the durability flag is arming an existing check, not defining the contract or resume behaviour.
- Bootstrap-time capacity feasibility rejection of a cost no pool can satisfy (C7/C12 at bootstrap) — the cost is declared here, checked there.
- Group semantics beyond a presentation label (C6): no group-level concurrency limit and no group-level failure handling — resisting that is the point.
- Any suggestion that policy could reshape the graph at runtime, add scheduling, or introduce a policy DSL — dagr is not a scheduler, a DSL, or a runtime-mutable graph, and node policy stays a static, author-declared value.
