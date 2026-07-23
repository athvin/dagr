#!/usr/bin/env bash
# Bootstrap-phase-interface & cost-model ADR acceptance checks for ticket 011
# (T0.5).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/011-T0.5-bootstrap-phase-and-cost-model.md). T0.5 is a
# DECISION ticket whose durable deliverable is an ADR that locks the
# assembly -> bootstrap -> execution seam as an INTERFACE (what assembly
# produces, what bootstrap consumes, where the two pre-execution failure modes
# land) and the per-pool cost model (bytes for memory, thread counts for thread
# pools, working-memory vs output-residency, the zero default, the declared-vs-
# measured honesty rule). Its "tests" are documentary completeness and
# internal-consistency checks against the recorded ADR: that every seam element
# and cost-type choice the ticket is chartered to fix is present, in the exact
# normative vocabulary of arch.md ("The shape of a run", C5, C7, C12, C10, C22,
# C26), so no seam or cost-type choice is left open to T14/T29/T31/T32.
#
# The load-bearing assertions are COMPLETENESS (every named seam element and
# cost-type is recorded) and INTERNAL CONSISTENCY (the compile/assembly/bootstrap
# partition table names each check on the correct side, and cost-fit rejection
# sits at bootstrap while the invalid execution-class override stays at
# assembly). Authored FIRST as the acceptance gate, it fails on the ticket file
# as it stands before the ADR is written into it, and passes once the ADR records
# every element the ticket is chartered to lock.
#
# This is a DOC-ONLY decision (the cost model is a type-shape choice fully
# decidable from arch.md; no spike is required), so the script does NOT build or
# run any prototype and asserts no production code: the shipping crates and
# Cargo.lock are untouched, and the only committed artifacts are the embedded ADR
# and this mechanical acceptance script.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/011-T0.5-bootstrap-phase-and-cost-model.md"
archmd="docs/arch.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.3/T0.4/T0.7 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already names bootstrap, cost,
# residency, etc. — so a whole-file grep would pass content checks the ADR itself
# has not yet made. We therefore scope every content assertion to the ADR BODY
# only: the slice of the file from the first 'ADR:' heading line to EOF. The
# ticket's own H1 title ('# 011 · T0.5 — …') is deliberately NOT matched (no
# 'ADR:' with a colon), so before the embedded ADR is authored the slice is empty
# and every content check fails — exactly the tests-first behaviour we want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

# Case-insensitive extended-regex grep, scoped to the ADR body only.
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }

# --- ADR skeleton (ticket-conventions §4: decision tickets need status /
# context / decision / consequences / rejected alternatives) ------------------
if printf '%s' "$adr_body" | grep -qiE '^#+[[:space:]]+ADR:'; then
  pass "adr: an embedded 'ADR:' heading is present in the ticket file"
else
  bad "adr: no embedded 'ADR:' heading found (expected '# ADR: …')"
fi
for sect in Status Context Decision Consequences 'Rejected alternative'; do
  if printf '%s' "$adr_body" | grep -qiE "^#+[[:space:]]+$sect"; then
    pass "adr: section '$sect' heading present"
  else
    bad "adr: required section '$sect' heading missing"
  fi
done

# --- Accepted status (ticket-conventions: decision ADRs are Accepted) --------
if has '^#+[[:space:]]+Status' && has 'accepted'; then
  pass "status: the ADR is in an Accepted status (not draft/proposed)"
else
  bad "status: the ADR must be recorded in an Accepted status"
fi
if has 'draft|proposed'; then
  bad "status: the ADR must not be draft/proposed"
else
  pass "status: no draft/proposed status leaked in"
fi

# --- 1. Assembly-output interface (DoD 1; Test plan constructible-in-empty-env)
# The immutable, machine-independent assembly artifact must carry: graph
# structure, per-node effective policy including the declared cost vector,
# consumer counts, remaining-dependency counts, execution order, a fingerprint
# slot — and be constructible with every external resource absent.
if has 'assembly' && (has 'immutable' || has 'machine-independent' || has 'machine independent'); then
  pass "assembly-artifact: recorded as an immutable, machine-independent value"
else
  bad "assembly-artifact: the immutable/machine-independent property is missing"
fi
if has 'graph structure' || has 'graph .* structure'; then
  pass "assembly-artifact: carries the graph structure"
else
  bad "assembly-artifact: the graph-structure field is missing"
fi
if (has 'effective policy' || has 'per-node .* policy') && (has 'declared cost' && (has 'vector' || has 'cost vector')); then
  pass "assembly-artifact: carries per-node effective policy incl. the declared cost vector"
else
  bad "assembly-artifact: the per-node-effective-policy-incl-cost-vector field is missing"
fi
if has 'consumer count'; then
  pass "assembly-artifact: carries precomputed consumer counts"
else
  bad "assembly-artifact: the consumer-counts field is missing"
fi
if has 'remaining-dependency' || has 'remaining dependency' || has 'dependency count'; then
  pass "assembly-artifact: carries remaining-dependency counts"
else
  bad "assembly-artifact: the remaining-dependency-counts field is missing"
fi
if has 'execution order'; then
  pass "assembly-artifact: carries the execution order"
else
  bad "assembly-artifact: the execution-order field is missing"
fi
if has 'fingerprint' && (has 'slot' || has 'fingerprint slot'); then
  pass "assembly-artifact: carries the graph fingerprint slot"
else
  bad "assembly-artifact: the fingerprint-slot field is missing"
fi
if (has 'empty environment' || has 'every external resource absent' || has 'external resource absent') && has 'construct'; then
  pass "assembly-artifact: provably constructible with every external resource absent (C7)"
else
  bad "assembly-artifact: the constructible-in-an-empty-environment property is missing"
fi

# --- 2. Assembly purity + no parameter reachable (DoD 2; Test plan) ----------
if has 'assembly is pure' || (has 'assembly' && has 'pure'); then
  pass "purity: the ADR records that assembly is pure"
else
  bad "purity: the assembly-is-pure statement is missing"
fi
for r in network filesystem clock credential; do
  if has "$r"; then :; else bad "purity: assembly must touch no $r (missing from the ADR)"; fi
done
if has 'no parameter value' || (has 'parameter' && (has 'not reachable' || has 'no path' || has 'unreachable' || has 'never sees')); then
  pass "purity: no parameter value is reachable from the assembly-output type (C7)"
else
  bad "purity: the no-parameter-value-reachable property is missing"
fi

# --- 3. Bootstrap inputs/outputs + ordering (DoD 3; Test plan ordered steps) --
if has 'bootstrap' && (has 'consumes' || has 'input') && has 'assembly artifact' && has 'parameter' && has 'environment' && (has 'container limit' || has 'container-limit') && (has 'resource registry' || has 'registry'); then
  pass "bootstrap-inputs: assembly artifact plus invocation (params, env, container limits, registry)"
else
  bad "bootstrap-inputs: the ordered bootstrap inputs are incomplete"
fi
if has 'bootstrap' && (has 'validated run context' || has 'run context') && (has 'bootstrap failure' || has 'bootstrap-fail'); then
  pass "bootstrap-outputs: a validated run context, or a bootstrap failure"
else
  bad "bootstrap-outputs: the run-context-or-bootstrap-failure output is missing"
fi
if (has 'identity' || has 'run identity') && (has 'store' && has 'stream') && (has 'before assembly' || has 'before .* assembly executes'); then
  pass "ordering: identity minted and store/stream opened before assembly runs (run verbs)"
else
  bad "ordering: the identity/store/stream-before-assembly ordering is missing"
fi
if (has 'inspection verb' || has 'validate' || has 'graph' || has 'render') && (has 'no store' || has 'without a store' || has 'without opening a store'); then
  pass "ordering: inspection verbs (validate/graph/render) run assembly with no store"
else
  bad "ordering: the inspection-verbs-run-with-no-store record is missing"
fi
# The ordered bootstrap-step sequence must be recorded (never a partial hang).
if (has 'pool sizing' || has 'size the .* pool' || has 'sizing') && has 'cost-fit' && (has 'parameter parse' || has 'parse .* parameter' || has 'parse and validate') && (has 'environment capture' || has 'capture .* environment' || has 'allowlisted environment'); then
  pass "ordering: the ordered bootstrap steps (registry -> pool sizing -> cost-fit -> param parse -> env capture) are recorded"
else
  bad "ordering: the ordered bootstrap-step sequence is incomplete"
fi
if (has 'fail fast' || has 'fails fast') && (has 'never hang' || has 'no wait' || has 'no wedge' || has 'never wedge'); then
  pass "ordering: bootstrap fails fast and never hangs"
else
  bad "ordering: the fail-fast/never-hang property is missing"
fi

# --- 4. Two distinct pre-execution failure variants (DoD 4; Test plan) -------
if has 'assembly-failed' && has 'bootstrap-failed'; then
  pass "variants: both assembly-failed and bootstrap-failed variants are named"
else
  bad "variants: the two distinct pre-execution failure variants are not both named"
fi
if has 'assembly-failed' && (has 'fingerprint absent' || has 'fingerprint is absent' || has 'no fingerprint' || has 'fingerprint .* absent') && (has 'complete problem list' || has 'problem list' || has 'all problems') && (has 'zero attempt' || has 'no attempt'); then
  pass "variants: assembly-failed = fingerprint absent, complete problem list, zero attempts"
else
  bad "variants: the assembly-failed contents (fingerprint absent / problem list / zero attempts) are incomplete"
fi
if has 'bootstrap-failed' && (has 'fingerprint present' || has 'fingerprint is present' || has 'fingerprint .* present'); then
  pass "variants: bootstrap-failed carries a present fingerprint (assembly succeeded)"
else
  bad "variants: the bootstrap-failed fingerprint-present property is missing"
fi
if has 'distinct' && has 'exit code'; then
  pass "variants: each variant has its own distinct exit code (C26)"
else
  bad "variants: the distinct-exit-code-per-variant record is missing"
fi
if (has 'predictable run-store' || has 'predictable .* run store' || has 'predictable location' || has 'run-store location') && has 'artifact'; then
  pass "variants: each produces an artifact at the predictable run-store location (C22)"
else
  bad "variants: the artifact-at-a-predictable-run-store-location record is missing"
fi

# --- 5. Store-open-failure case (DoD 5; Test plan) ---------------------------
if (has 'store .* cannot be opened' || has 'opening the store .* fail' || has 'store-open' || has 'store open .* fail' || has 'nowhere to write') && has 'stderr' && (has 'sink-failure' || has 'sink failure'); then
  pass "store-open-failure: no artifact possible, error to stderr with the sink-failure exit code"
else
  bad "store-open-failure: the store-open-failure -> stderr/sink-failure-code case is missing"
fi

# --- 6. Per-pool cost-vector types (DoD 6; Test plan native units) -----------
if (has 'one entry per .* pool' || has 'one entry per admission pool' || has 'per admission pool') && has 'native unit'; then
  pass "cost-vector: one entry per admission pool, in that pool's native unit (C5)"
else
  bad "cost-vector: the one-entry-per-pool-in-native-units record is missing"
fi
if has 'byte' && (has 'memory pool' || has 'memory'); then
  pass "cost-vector: bytes for the memory pool"
else
  bad "cost-vector: the bytes-for-memory-pool unit is missing"
fi
if has 'thread count' && (has 'thread pool' || has 'thread'); then
  pass "cost-vector: a thread count for each thread pool"
else
  bad "cost-vector: the thread-count-for-thread-pools unit is missing"
fi

# --- 7. Memory split: working memory vs output residency (DoD 7; Test plan) --
if has 'working memory' || has 'working-memory'; then
  pass "memory-split: the working-memory component is named"
else
  bad "memory-split: the working-memory component is missing"
fi
if has 'output residency' || has 'output-residency'; then
  pass "memory-split: the output-residency component is named"
else
  bad "memory-split: the output-residency component is missing"
fi
if (has 'working memory' || has 'working-memory') && (has 'held for the attempt' || has 'held .* attempt') && (has 'released at .* terminal' || has 'terminal state'); then
  pass "memory-split: working memory held for the attempt, released at its terminal state"
else
  bad "memory-split: the working-memory release-at-terminal-state timing is missing"
fi
if (has 'output residency' || has 'output-residency') && (has 'transferred to the .* slot' || has 'output slot' || has 'transfer .* slot') && (has 'last consumer' && has 'terminal'); then
  pass "memory-split: output residency transfers to the slot, released when the last consumer is terminal (C10)"
else
  bad "memory-split: the output-residency transfer/release-with-last-consumer timing is missing"
fi

# --- 8. Zero default applied uniformly (DoD 8; Test plan) --------------------
if (has 'zero declared cost' || has 'zero .* cost' || has 'default .* zero') && has 'default'; then
  pass "zero-default: the conservative default of zero declared cost is recorded"
else
  bad "zero-default: the zero-declared-cost default is missing"
fi
if (has 'no stated .* cost' || has 'no stated policy' || has 'no stated cost') && (has 'identical' || has 'behaves identically' || has 'all-zero'); then
  pass "zero-default: a node with no stated cost behaves identically to an all-zero cost written out"
else
  bad "zero-default: the no-stated-cost-equals-all-zero equivalence is missing"
fi
if (has 'defaulted' || has 'including defaulted' || has 'default') && (has 'graph artifact' || has 'assembly artifact' || has 'assembly/graph artifact') && (has 'every node' || has 'full effective') && has 'cost'; then
  pass "zero-default: every node's effective cost appears in the assembly/graph artifact incl. defaulted values"
else
  bad "zero-default: the effective-cost-incl-defaults-in-the-artifact record is missing"
fi

# --- 9. Declared-vs-measured honesty rule (DoD 9; Test plan) -----------------
if (has 'declared' && has 'measured') && (has 'juxtapose' || has 'juxtaposes' || has 'side by side' || has 'juxtaposition') && (has 'dishonest' || has 'honest' || has 'visible'); then
  pass "honesty: the run artifact juxtaposes declared against measured cost so dishonest declarations are visible (C23)"
else
  bad "honesty: the declared-vs-measured juxtaposition/honesty rule is missing"
fi

# --- 10. Cost-fit & resource/parameter rejection at bootstrap (DoD 10) -------
if (has 'cost .* no pool can .* satisfy' || has 'exceeds .* pool.* .* capacity' || has 'exceeds .* total capacity' || has 'no pool can satisfy') && (has 'reject' || has 'rejected') && has 'bootstrap'; then
  pass "cost-fit: a declared cost no pool can satisfy is rejected at bootstrap (C12)"
else
  bad "cost-fit: the cost-no-pool-can-satisfy bootstrap rejection is missing"
fi
if (has 'missing .* resource' || has 'missing declared resource') && has 'bootstrap'; then
  pass "cost-fit: a missing declared resource is rejected at bootstrap"
else
  bad "cost-fit: the missing-declared-resource bootstrap rejection is missing"
fi
if (has 'invalid parameter' || has 'parameter .* invalid') && has 'bootstrap'; then
  pass "cost-fit: an invalid parameter is rejected at bootstrap"
else
  bad "cost-fit: the invalid-parameter bootstrap rejection is missing"
fi
if (has 'before any node executes' || has 'before .* node executes' || has 'before admission') && (has 'distinct' || has 'complete error report' || has 'complete .* report'); then
  pass "cost-fit: each rejection is before any node executes, with a distinct complete error report + the bootstrap-failure artifact"
else
  bad "cost-fit: the before-execution/distinct-complete-report property is missing"
fi
# The invalid execution-class override STAYS an assembly check (consistency).
if (has 'execution-class override' || has 'execution class override' || has 'class override') && has 'assembly'; then
  pass "cost-fit: the invalid execution-class override stays an assembly failure (machine not needed)"
else
  bad "cost-fit: the invalid-execution-class-override-stays-at-assembly record is missing"
fi

# --- 11. Compile / assembly / bootstrap partition table (DoD 11) -------------
if has 'partition' && (has 'compile' && has 'assembly' && has 'bootstrap'); then
  pass "partition: the compile-time / assembly-time / bootstrap-time partition table is present"
else
  bad "partition: the compile/assembly/bootstrap partition table is missing"
fi
# Assembly-side entries the table must list.
if has 'duplicate' && (has 'name' || has 'node name'); then
  pass "partition: duplicate names listed as an assembly failure"
else
  bad "partition: the duplicate-names assembly entry is missing"
fi
if has 'empty pipeline'; then
  pass "partition: empty pipeline listed as an assembly failure"
else
  bad "partition: the empty-pipeline assembly entry is missing"
fi
if has 'durable' && (has 'contract' || has 'durable-without-contract' || has 'without .* contract'); then
  pass "partition: durable-without-contract listed as an assembly failure"
else
  bad "partition: the durable-without-contract assembly entry is missing"
fi
# Bootstrap-side entries the table must list (cost / resource / parameter).
if has 'bootstrap' && (has 'cost' && (has 'satisfy' || has 'capacity')) && (has 'missing .* resource' || has 'resource') && (has 'invalid parameter' || has 'parameter'); then
  pass "partition: the three bootstrap failures (cost / missing resource / invalid parameter) are listed"
else
  bad "partition: the bootstrap-side entries (cost/resource/parameter) are incomplete"
fi

# --- 12. Rejected alternatives (DoD 12) --------------------------------------
if (has 'single scalar cost' || has 'scalar cost' || has 'single scalar') && (has 'independently constrained' || has 'all-or-nothing' || has 'per pool' || has 'per-pool'); then
  pass "rejected: a single scalar cost is rejected (pools independently constrained, all-or-nothing acquisition)"
else
  bad "rejected: the single-scalar-cost rejected alternative is missing"
fi
if (has 'undifferentiated memory' || has 'undifferentiated' || has 'without the working' || has 'without the split') && (has 'C10' || has 'transfer-to-slot' || has 'last consumer'); then
  pass "rejected: undifferentiated memory without the working/residency split is rejected (cannot express C10 accounting)"
else
  bad "rejected: the undifferentiated-memory rejected alternative is missing"
fi
if has 'reopen' || has 'reopen condition'; then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen-condition statement is missing"
fi

# --- Component attributions the DoD demands (C5, C7, C12, C10, C22, C26) ------
for c in C5 C7 C12 C10 C22 C26; do
  if has "$c"; then :; else bad "component: '$c' is not referenced in the ADR"; fi
done
if has 'C5' && has 'C7' && has 'C12' && has 'C10' && has 'C22' && has 'C26'; then
  pass "component: C5, C7, C12, C10, C22, C26 all referenced"
fi

# --- Downstream hand-off: the four blocked tickets inherit the seam ----------
for t in T14 T29 T31 T32; do
  if has "$t"; then :; else bad "handoff: blocked ticket '$t' is not named in the hand-off"; fi
done
if has 'T14' && has 'T29' && has 'T31' && has 'T32'; then
  pass "handoff: all four blocked tickets named (T14, T29, T31, T32)"
fi

# --- Open questions resolved (ticket says None; tasks.md carries no Q:) -------
if has 'open question' && (has 'none' || has 'no open question' || has 'no unresolved'); then
  pass "questions: the ADR records that there are no open questions (ticket + tasks.md)"
else
  bad "questions: the ADR must record the open-questions disposition (none)"
fi

# --- No coverage-matrix change (decision ticket owes no covering test) --------
# C5/C7/C12 remain unmapped/deferred to T29/T13/T31 in docs/coverage-matrix.md;
# this ADR must state it makes no matrix change so the boundary is visible.
if has 'coverage-matrix' || has 'coverage matrix'; then
  pass "coverage: the ADR states its (no-)coverage-matrix disposition"
else
  bad "coverage: the ADR must record that it makes no coverage-matrix change"
fi

# --- Cross-reference against arch.md (COMPLETENESS/no-invention guard) --------
# Every canonical seam term the ADR fixes must be one arch.md actually defines —
# no invention of a fourth phase, a new failure variant, or a new pool unit.
arch_has() { grep -qiE "$1" "$archmd"; }
invented=0
for term in 'assembly-failed' 'bootstrap-failed' 'working memory' 'output residency' 'sink-failure'; do
  arch_has "$term" || { bad "cross-ref: ADR term '$term' is not present in arch.md"; invented=1; }
done
[ "$invented" -eq 0 ] && pass "cross-ref: every seam/cost term the ADR fixes is grounded in arch.md (no invention)"

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
