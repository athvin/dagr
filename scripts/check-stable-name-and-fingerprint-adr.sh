#!/usr/bin/env bash
# Stable-name-trait and fingerprint-composition ADR acceptance checks for ticket
# 013 (T0.7).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/013-T0.7-stable-name-and-fingerprint-adr.md). T0.7 is a
# DECISION ticket whose durable deliverable is an ADR that locks the identity /
# caching / provenance contract every downstream identity/caching consumer binds
# against: the author-declared stable-name trait/derive on both task and payload
# (input/output) types; the C20 per-node and per-edge field lists the graph
# artifact carries; the exact split of fields between the STRUCTURAL FINGERPRINT
# (node set by stable name, edge set with carried-type stable names and edge
# kind, per-node trigger rule) and the POLICY HASH (retries/backoff, timeout,
# declared cost, execution class, retention, durability); the exclusions (group
# labels in NEITHER hash and not in identity; everything environmental excluded
# from both); the single deterministic registration-order-independent
# canonicalization (ordering + byte-encoding + hash function); the binding to the
# T0.10 algorithm-version identifier ("cannot compare" is T0.10's, deferred, not
# re-decided here); the point-of-use documentation requirement for the C21
# internal-logic limitation (on the resume verb C27 and the structure-assertion
# API C28, naming the hand-maintained task version marker); and the per-ticket
# downstream hand-off (T13, T40, T41, T58, T61). Its "tests" are documentary
# completeness and internal-consistency checks against the recorded ADR: that
# every seam element the ticket is chartered to fix is present, in the exact
# normative vocabulary of arch.md (C20, C21, C5, C6, C7, C27, C28, "Vocabulary"),
# so no seam is left open to T13/T40/T41/T58/T61.
#
# The load-bearing assertions are COMPLETENESS (every named seam element is
# recorded) and INTERNAL CONSISTENCY (the stored name is the AUTHOR-DECLARED name
# and never std::any::type_name in identity or either hash; the two field lists
# PARTITION the effective policy with no overlap; group labels are in NEITHER
# hash; defaulted policy hashes IDENTICALLY to written-out defaults; the
# canonicalization is registration-order-independent; the composition is fixed by
# construction, never runtime-configurable, and node identity never depends on
# task internal logic via automatic content hashing). Authored FIRST as the
# acceptance gate, it fails on the ticket file as it stands before the ADR is
# written into it, and passes once the ADR records every element the ticket is
# chartered to lock.
#
# This is a DOC-ONLY decision (the stable-name contract and the fingerprint
# composition are fully decidable from arch.md; the TRAIT and fingerprint TYPES
# are IMPLEMENTED by T13/T40/T41, not here), so the script does NOT build or run
# any prototype and asserts no production code: the shipping crates and
# Cargo.lock are untouched, and the only committed artifacts are the embedded ADR
# and this mechanical acceptance script.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/013-T0.7-stable-name-and-fingerprint-adr.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.3/T0.4/T0.5/T0.6 precedent: '# ADR: <title>'). The
# ticket prose above it (Objective, Test plan, DoD) already names the trait,
# fingerprint, canonicalization, etc. — so a whole-file grep would pass content
# checks the ADR itself has not yet made. We therefore scope every content
# assertion to the ADR BODY only: the slice of the file from the first 'ADR:'
# heading line to EOF. The ticket's own H1 title ('# 013 · T0.7 — …') is
# deliberately NOT matched (no 'ADR:' with a colon), so before the embedded ADR
# is authored the slice is empty and every content check fails — exactly the
# tests-first behaviour we want.
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

# --- 1. Stable-name trait / derive (DoD 1; Test plan trait-covers-both) ------
# An author-declared constant on BOTH task and payload (input/output) types,
# with a one-line derive for the common case; the stored name is the DECLARED
# name of the compiler-enforced type.
if (has 'trait') && (has 'stable.?name' || has 'stable name') && (has 'author-declared' || has 'author declared' || has 'declared .* constant' || has 'constant'); then
  pass "trait: a stable-name trait carrying an author-declared constant is defined"
else
  bad "trait: the author-declared stable-name trait/constant is missing"
fi
if (has 'task') && (has 'payload' || (has 'input' && has 'output')); then
  pass "trait: the trait is implemented by both task and payload (input/output) types"
else
  bad "trait: coverage of both task and payload (input/output) types is missing"
fi
if (has 'derive') && (has 'one[ -]line' || has 'one line' || has 'common case'); then
  pass "trait: a one-line derive for the common case is stated"
else
  bad "trait: the one-line derive for the common case is missing"
fi
if (has 'declared name' || has 'the .* declared name' || has 'declared .* name') && (has 'compiler-enforced' || has 'compiler enforced' || has 'the type the compiler'); then
  pass "trait: the stored name is the declared name of the compiler-enforced type"
else
  bad "trait: the stored-name-is-the-declared-name property is missing"
fi

# --- 2. Never std::any::type_name (DoD 2; Test plan type_name) ---------------
if (has 'type_name') && (has 'unstable') && (has 'compiler version' || has 'across compiler' || has 'across toolchain' || has 'toolchain'); then
  pass "type_name: type_name is recorded as unstable across compiler versions"
else
  bad "type_name: the type_name-is-unstable record is missing"
fi
if (has 'type_name') && (has 'debug' && (has 'informational' || has 'never .* identity' || has 'never .* hash')); then
  pass "type_name: type_name may appear only as an informational debug field, never as identity, never in either hash"
else
  bad "type_name: the debug-only / never-identity / never-in-hash restriction is missing"
fi

# --- 3. Well-formedness + uniqueness scope (DoD 3; Test plan well-formedness) -
if (has 'non-empty' || has 'nonempty' || has 'not empty') && (has 'character set' || has 'char set' || has 'shape' || has 'fixed .* set'); then
  pass "wellformed: a well-formedness rule (non-empty, fixed shape/character set) is stated"
else
  bad "wellformed: the well-formedness rule (non-empty, fixed shape/character set) is missing"
fi
if (has 'duplicate') && (has 'assembly') && (has 'not .* compile' || has 'not a compile' || has 'whole-pipeline' || has 'whole pipeline' || has 'assembly failure' || has 'assembly-time'); then
  pass "uniqueness: duplicate declared names fail assembly (not compilation), whole-pipeline scoped"
else
  bad "uniqueness: the duplicate-names-fail-assembly (whole-pipeline) rule is missing"
fi
if (has 'both declaration' || has 'names both' || has 'name both' || has 'both declared'); then
  pass "uniqueness: the duplicate error names both declarations"
else
  bad "uniqueness: the names-both-declarations requirement is missing"
fi

# --- 4. C20 per-node field list (DoD 4; Test plan per-node field list) --------
# name, group, stable task name, stable input/output type names, execution
# class, complete effective policy incl. defaulted values, declared resource
# requirements, dependency lists.
node_ok=1
for term in 'name' 'group' 'stable task' 'execution class' 'effective policy' 'defaulted' 'resource' 'dependency'; do
  has "$term" || node_ok=0
done
if [ "$node_ok" = 1 ] && (has 'stable input' || has 'input .* type name' || has 'input/output type' || has 'input and output type'); then
  pass "c20-node: the per-node field list is complete (incl. defaulted policy + declared resources + dependency lists)"
else
  bad "c20-node: the per-node C20 field list is incomplete"
fi

# --- 5. C20 per-edge field list (DoD 5; Test plan per-edge field list) --------
if (has 'edge') && (has 'data' && (has 'ordering' || has 'order-only' || has 'ordering-only')) && (has 'carr' && has 'type'); then
  pass "c20-edge: per-edge kind (data vs ordering) and carried-type stable name are recorded"
else
  bad "c20-edge: the per-edge field list (kind + carried type) is incomplete"
fi

# --- 6. Structural-fingerprint field list (DoD 6; Test plan structural) -------
# EXACTLY node set (stable names), edge set (carried-type names + edge kind),
# trigger rules — and nothing else.
if (has 'structural fingerprint' || has 'structural-fingerprint') && (has 'node set') && (has 'edge set' || has 'edge .* set') && (has 'trigger rule'); then
  pass "structural: the structural fingerprint covers node set, edge set, and trigger rules"
else
  bad "structural: the structural-fingerprint field list is incomplete"
fi
if (has 'structural') && (has 'nothing else' || has 'and no' || has 'exactly' || has 'no policy' || has 'no group' || has 'no environmental'); then
  pass "structural: the structural fingerprint is stated to cover exactly the shape inputs (nothing else)"
else
  bad "structural: the exactly-shape-inputs (nothing else) statement is missing"
fi

# --- 7. Policy-hash field list (DoD 7; Test plan policy hash) ----------------
# retries/backoff, timeout, declared cost, execution class, retention,
# durability — the residual effective policy; defaulted == written-out defaults.
policy_ok=1
for term in 'retr' 'timeout' 'cost' 'execution class' 'retention' 'durab'; do
  has "$term" || policy_ok=0
done
if [ "$policy_ok" = 1 ] && (has 'policy hash' || has 'policy-hash'); then
  pass "policy: the policy hash covers exactly the residual policy (retries/backoff, timeout, cost, class, retention, durability)"
else
  bad "policy: the policy-hash field list is incomplete"
fi
if (has 'no .* policy' || has 'no stated policy' || has 'without .* policy' || has 'defaulted') && (has 'written out' || has 'written-out' || has 'every default') && (has 'identical' || has 'identically'); then
  pass "policy: a node with no stated policy hashes identically to one with every default written out (C5)"
else
  bad "policy: the defaulted-hashes-identically-to-written-out-defaults property is missing"
fi

# --- 8. Group exclusion (DoD 8; Test plan group labels) ----------------------
if (has 'group') && (has 'neither hash' || has 'neither .* hash' || (has 'not .* structural' && has 'not .* policy'))  && (has 'not .* identity' || has 'not part of .* identity'); then
  pass "group: group labels are in neither hash and not part of node identity"
else
  bad "group: the group-in-neither-hash / not-in-identity exclusion is missing"
fi
if (has 'group') && (has 'rename') && (has 'structure diff' || has 'structure-diff' || has 'C28' || has 'structure snapshot' || has 'structure-assertion'); then
  pass "group: a group rename surfaces only in the C28 structure diff, never breaks resume"
else
  bad "group: the group-rename-surfaces-only-in-the-structure-diff record is missing"
fi

# --- 9. Environmental exclusions (DoD 9; Test plan environmental) ------------
env_ok=1
for term in 'timestamp' 'hostname' 'compiler' 'generation time' 'git commit' 'lockfile'; do
  has "$term" || env_ok=0
done
if [ "$env_ok" = 1 ] && (has 'exclude' || has 'excluded' || has 'not .* included'); then
  pass "environmental: both hashes exclude timestamps, hostnames, compiler/tool versions, generation time, git commit, lockfile hash"
else
  bad "environmental: the environmental-exclusion list is incomplete"
fi
if (has 'different .* toolchain' || has 'cross-toolchain' || has 'different machine' || has 'two toolchains' || has 'different toolchains') && (has 'identical' || has 'same .* fingerprint' || has 'same structural' || has 'hash identically'); then
  pass "environmental: unchanged source on a different toolchain hashes identically"
else
  bad "environmental: the cross-toolchain-identical-hash property is missing"
fi

# --- 10. Canonicalization (DoD 10; Test plan canonicalization) ---------------
if (has 'canonical' && (has 'order' || has 'ordering')) && (has 'registration-order' || has 'registration order' || has 'independent of registration' || has 'reorder'); then
  pass "canon: a deterministic, registration-order-independent canonical ordering is fixed"
else
  bad "canon: the registration-order-independent canonical ordering is missing"
fi
if (has 'byte' && (has 'encoding' || has 'encode')) && (has 'hash function' || has 'hash algorithm' || has 'BLAKE' || has 'SHA'); then
  pass "canon: a single byte-encoding and a single named hash function are fixed"
else
  bad "canon: the single byte-encoding / named hash function is missing"
fi
if (has 'byte-identical' || has 'byte identical' || has 'identical bytes') && (has 'generation time' || has 'generation-time'); then
  pass "canon: assembling the same pipeline twice yields byte-identical artifacts outside the generation-time field (C7)"
else
  bad "canon: the assemble-twice-byte-identical (generation-time aside) property is missing"
fi

# --- 11. Change matrix (DoD 11; Test plan change matrix) ---------------------
if (has 'add' && has 'remove' && has 'rename') && (has 'rewire' || has 're-wire' || has 'edge') && (has 'trigger') && (has 'carried' || has 'carried-type' || has 'carried type') && (has 'structural fingerprint' || has 'structural-fingerprint'); then
  pass "matrix: every structural change (node add/remove/rename, rewire, trigger-rule change, carried-type change) moves the structural fingerprint"
else
  bad "matrix: the structural-change row of the change matrix is incomplete"
fi
if (has 'policy-only' || has 'policy only' || has 'only the policy') && (has 'only the policy hash' || has 'policy hash .* only' || has 'moves only'); then
  pass "matrix: a policy-only change moves only the policy hash"
else
  bad "matrix: the policy-only-moves-only-the-policy-hash row is missing"
fi
if (has 'group rename' || has 'group .* rename' || has 'rename .* group') && (has 'neither' || has 'moves neither' || has 'changes neither'); then
  pass "matrix: a group rename moves neither hash"
else
  bad "matrix: the group-rename-moves-neither row is missing"
fi

# --- 12. Algorithm-version binding, deferred to T0.10 (DoD 12; Test plan) -----
if (has 'T0\.10' || has 'T0.10') && (has 'algorithm.?version' || has 'algorithm version' || has 'versioned algorithm' || has 'version identifier'); then
  pass "version: each hash carries the T0.10 versioned algorithm identifier"
else
  bad "version: the T0.10 algorithm-version-identifier binding is missing"
fi
if (has 'cannot compare') && (has 'topology differ' || has 'topology diff' || has 'differs' || has 'topology'); then
  pass "version: a version mismatch reads as 'cannot compare', distinct from 'topology differs'"
else
  bad "version: the cannot-compare-vs-topology-differs distinction is missing"
fi
if (has 'algorithm-version bump' || has 'algorithm version bump' || has 'version bump' || has 'bump') && (has 'defer' || has 'owner' || has 'owned by' || has 'T0.10' || has 'not re-decid' || has 'not re-decide'); then
  pass "version: changing the composition/canonicalization is a version bump; the versioning policy is deferred to T0.10"
else
  bad "version: the version-bump / defer-to-T0.10 record is missing"
fi

# --- 13. Both hashes + version in graph AND run artifacts (DoD 13) -----------
if (has 'graph artifact') && (has 'run artifact') && (has 'both hash' || has 'both fingerprint' || (has 'structural' && has 'policy hash')); then
  pass "artifacts: both hashes (and the algorithm version) appear in the graph artifact and in every run artifact"
else
  bad "artifacts: the both-hashes-in-graph-and-run-artifacts record is missing"
fi

# --- 14. Emission needs no environment (DoD 14; Test plan emission) ----------
if (has 'no credential' || has 'no credentials') && (has 'no network') && (has 'no database'); then
  pass "emission: producing the artifact and computing the fingerprints needs no credentials, no network, no database (C7)"
else
  bad "emission: the no-credentials/no-network/no-database emission constraint is missing"
fi

# --- 15. Internal-logic limitation documented at point of use (DoD 15) -------
if (has 'internal logic' || has 'internal-logic') && (has 'does not change' || has 'not change the fingerprint' || has "doesn't change" || has 'without changing its interface'); then
  pass "limitation: the internal-logic limitation (interface unchanged => fingerprint unchanged) is recorded"
else
  bad "limitation: the internal-logic-limitation record is missing"
fi
if (has 'resume verb' || has 'C27') && (has 'structure-assertion' || has 'structure assertion' || has 'C28'); then
  pass "limitation: the limitation is required at the point of use — the resume verb (C27) and the structure-assertion API (C28)"
else
  bad "limitation: the point-of-use placement (C27 resume verb + C28 structure API) is missing"
fi
if (has 'task version marker' || has 'version marker' || has 'manual .* marker') && (has 'manual' || has 'hand-maintained' || has 'hand maintained' || has 'honest'); then
  pass "limitation: the hand-maintained task version marker is named as the honest manual answer"
else
  bad "limitation: the hand-maintained-task-version-marker naming is missing"
fi
if (has 'not .* buried' || has 'not buried' || has 'rather than .* buried' || has 'not .* only in the ADR' || has 'point of use' || has 'point-of-use'); then
  pass "limitation: the requirement is at the point of use, not buried only in the ADR"
else
  bad "limitation: the not-buried-in-the-ADR requirement is missing"
fi

# --- 16. Downstream hand-off (DoD 16; Test plan hand-off) --------------------
handoff_ok=1
for t in 'T13' 'T40' 'T41' 'T58' 'T61'; do
  has "$t" || handoff_ok=0
done
if [ "$handoff_ok" = 1 ]; then
  pass "handoff: all five blocked tickets named (T13, T40, T41, T58, T61)"
else
  bad "handoff: not every blocked ticket (T13, T40, T41, T58, T61) is named"
fi
if (has 'T13') && (has 'trait' || has 'derive') && (has 'uniqueness' || has 'unique' || has 'node identity'); then
  pass "handoff: T13 consumes the trait/derive and the whole-pipeline uniqueness rule"
else
  bad "handoff: the T13 hand-off (trait/derive + uniqueness) is missing"
fi
if (has 'T40') && (has 'field list' || has 'per-node' || has 'per-edge') && (has 'byte-identity' || has 'byte identity' || has 'canonicalization' || has 'byte-identical'); then
  pass "handoff: T40 consumes the field list and the byte-identity/canonicalization rules"
else
  bad "handoff: the T40 hand-off (field list + byte-identity/canonicalization) is missing"
fi
if (has 'T41') && (has 'two field list' || has 'both field list' || (has 'ordering' && has 'exclusion')) ; then
  pass "handoff: T41 consumes the two field lists, canonical ordering, and exclusion"
else
  bad "handoff: the T41 hand-off (two field lists + ordering + exclusion) is missing"
fi
if (has 'T58') && (has 'record' || has 'run artifact') && (has 'hash' || has 'fingerprint'); then
  pass "handoff: T58 consumes the fingerprint hashes it records into run artifacts"
else
  bad "handoff: the T58 hand-off (recorded hashes) is missing"
fi
if (has 'T61') && (has 'structural fingerprint' || has 'structure-snapshot' || has 'structure snapshot' || has 'diff'); then
  pass "handoff: T61 consumes the structural fingerprint the structure-snapshot assertion diffs against"
else
  bad "handoff: the T61 hand-off (structural fingerprint to diff) is missing"
fi

# --- Rejected alternatives (ticket-conventions §4; Out of scope) -------------
# type_name-as-identity, automatic content hashing of internal logic, a single
# combined fingerprint, runtime-configurable composition.
if (has 'type_name') && (has 'reject' || has 'rejected'); then
  pass "rejected: type_name-as-identity is named and rejected"
else
  bad "rejected: the type_name-as-identity rejected alternative is missing"
fi
if (has 'content hash' || has 'content-hash' || has 'automatic .* hash' || has 'hashing .* internal' || has 'internal logic') && (has 'reject' || has 'rejected' || has 'under-detect' || has 'under detect'); then
  pass "rejected: automatic content hashing of task internal logic is named and rejected"
else
  bad "rejected: the automatic-content-hashing rejected alternative is missing"
fi
if (has 'single .* hash' || has 'one hash' || has 'combined .* hash' || has 'single fingerprint' || has 'one fingerprint') && (has 'reject' || has 'rejected'); then
  pass "rejected: a single combined fingerprint (instead of the two-hash split) is named and rejected"
else
  bad "rejected: the single-combined-fingerprint rejected alternative is missing"
fi
if (has 'runtime-configurable' || has 'runtime configurable' || has 'configurable .* runtime' || has 'runtime') && (has 'reject' || has 'rejected' || has 'fixed by construction' || has 'never .* configurable'); then
  pass "rejected: a runtime-configurable field split/canonicalization is named and rejected"
else
  bad "rejected: the runtime-configurable-composition rejected alternative is missing"
fi
if (has 'reopen'); then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen condition is missing"
fi

# --- Components referenced ----------------------------------------------------
comp_ok=1
for c in 'C20' 'C21' 'C5' 'C6' 'C7'; do
  has "$c" || comp_ok=0
done
if [ "$comp_ok" = 1 ]; then
  pass "component: C20, C21, C5, C6, C7 all referenced"
else
  bad "component: not every governing/related component (C20, C21, C5, C6, C7) is referenced"
fi

# --- Open questions discharged (ticket §Open questions = None; tasks.md T0.7) -
if (has 'open question' || has 'no open question') && (has 'none' || has 'no .* question'); then
  pass "questions: the ADR records that there are no open questions (ticket + tasks.md)"
else
  bad "questions: the ADR does not state its open-questions disposition"
fi

# --- Coverage-matrix disposition (decision ticket owes no covering test) ------
if (has 'coverage matrix' || has 'coverage-matrix') && (has 'no change' || has 'no edit' || has 'unmapped' || has 'no .* change'); then
  pass "coverage: the ADR states its (no-)coverage-matrix disposition"
else
  bad "coverage: the ADR does not state its coverage-matrix disposition"
fi

# --- C20 provenance deferral consistency (must AGREE with the coverage matrix) -
# The coverage matrix defers C20 build-provenance to T40; the ADR must not
# claim provenance is decided/emitted here.
if (has 'provenance') && (has 'T40' || has 'deferred' || has 'defer'); then
  pass "provenance: build-provenance coverage is stated as deferred to T40 (agrees with the coverage matrix)"
else
  bad "provenance: the ADR must record that C20 build-provenance is deferred to T40"
fi

# --- Scope boundary (permanent non-goals): composition fixed by construction --
if (has 'fixed by construction' || has 'never runtime-configurable' || has 'never runtime configurable' || has 'not runtime-configurable') && (has 'metadata store' || has 'scheduler' || has 'DSL' || has 'never change.* at runtime'); then
  pass "scope: the ADR restates the fixed-by-construction / permanent-non-goals boundary"
else
  bad "scope: the ADR does not restate the fixed-by-construction / non-goals boundary"
fi

# --- No production code was added (DOC-ONLY decision) ------------------------
# The ADR ships no crate change; assert the ADR SAYS so (the tree-clean check is
# the gate/porcelain's job, not this script's).
if (has 'no .* code' || has 'no production code' || has 'doc-only' || has 'doc only' || has 'shipping crates .* unchanged' || has 'crates .* unchanged'); then
  pass "doc-only: the ADR records that it ships no production code (trait/types are T13/T40/T41)"
else
  bad "doc-only: the ADR does not record its doc-only disposition"
fi

if [ "$fail" = 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
