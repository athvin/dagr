#!/usr/bin/env bash
# Durable-output-contract ADR acceptance checks for ticket 014 (T0.8).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/014-T0.8-durable-output-contract.md). T0.8 is a DECISION
# ticket whose durable deliverable is an ADR that locks the durable-output
# contract every downstream resume/schema/assembly consumer binds against: the
# per-node DURABILITY FLAG in policy (C5, default not-durable, hashes into the
# POLICY HASH not the structural fingerprint per C21); the REFERENCE TRAIT PAIR
# on a durable node's OUTPUT TYPE (serialize-reference producing a
# serde-serializable, self-describing reference; rehydrate reconstructing the
# typed value from it later) with fixed error typing and fallibility/async-ness;
# the cheap EXISTENCE-CHECK operation with a present / absent / cannot-determine
# classification that does NOT fetch the value; the ASSEMBLY-ENFORCEMENT seam
# (C7) that rejects a durable-marked node whose output type lacks the contract,
# naming the offending node, as one of the all-problems-reported assembly errors
# (feeds T14); the SCHEMA FIELD recorded per attempt in the artifact and copied
# forward on resume so artifacts stay self-contained (C22, feeds T39); the rule
# that a dangling reference fails the resume PLAN before execution begins (C27);
# the record that in-memory (non-durable) outputs cannot be rehydrated and
# re-execute on demand at resume; and the per-ticket downstream hand-off (T14,
# T39, T57, T58).
#
# The load-bearing assertions are COMPLETENESS (every named seam element is
# recorded) and INTERNAL CONSISTENCY with the already-merged M0 ADRs it composes
# with: T0.2 (008 — the reference IS the node's output, so the contract sits on
# the OUTPUT TYPE), T0.6 (012 — durability is LOCAL and file-based; no networked
# or distributed store), and T0.7 (013 — the durability flag is in the POLICY
# HASH and EXCLUDED from the structural fingerprint, and defaulted policy hashes
# identically to the written-out default). Authored FIRST as the acceptance gate,
# it fails on the ticket file as it stands before the ADR is written into it, and
# passes once the ADR records every element the ticket is chartered to lock.
#
# This is a DOC-ONLY decision (the durable-output contract is fully decidable
# from arch.md; the durability FLAG, the reference TRAIT PAIR, the assembly CHECK,
# and the schema FIELD are IMPLEMENTED by T14/T39/T57/T58, not here), so the
# script does NOT build or run any prototype and asserts no production code: the
# shipping crates and Cargo.lock are untouched, and the only committed artifacts
# are the embedded ADR and this mechanical acceptance script.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/014-T0.8-durable-output-contract.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.3/T0.4/T0.5/T0.6/T0.7 precedent: '# ADR: <title>').
# The ticket prose above it (Objective, Test plan, DoD) already names the flag,
# trait pair, existence check, etc. — so a whole-file grep would pass content
# checks the ADR itself has not yet made. We therefore scope every content
# assertion to the ADR BODY only: the slice of the file from the first 'ADR:'
# heading line to EOF. The ticket's own H1 title ('# 014 · T0.8 — …') is
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

# --- 1. Durability flag is a per-node policy field (DoD 1; Test plan) ---------
# Per-node policy field (C5), attached at registration, default not durable,
# changing it requires no change to task code.
if (has 'durab') && (has 'policy') && (has 'per-node' || has 'per node' || has 'node .* policy') && (has 'C5'); then
  pass "flag: the durability flag is a per-node policy field (C5)"
else
  bad "flag: the durability-flag-is-a-per-node-policy-field (C5) record is missing"
fi
if (has 'registration' || has 'registered' || has 'attached at') && (has 'no .* task code' || has 'no change to task' || has 'without .* task code' || has 'no task-code'); then
  pass "flag: attached at registration; changing it requires no task-code change"
else
  bad "flag: the attached-at-registration / no-task-code-change record is missing"
fi
if (has 'default') && (has 'not durable' || has 'not-durable' || has 'non-durable'); then
  pass "flag: the documented default is not durable"
else
  bad "flag: the default-is-not-durable record is missing"
fi

# --- 2. Flag in effective policy incl. defaulted; defaulted == written-out ----
# (DoD 2; C5/C21) In every node's full effective policy in the graph artifact,
# including defaulted; defaulted behaves identically and hashes identically.
if (has 'effective policy') && (has 'defaulted' || has 'including .* default' || has 'even when defaulted') && (has 'graph artifact' || has 'artifact'); then
  pass "policy-emit: the flag appears in every node's full effective policy, including when defaulted"
else
  bad "policy-emit: the flag-in-full-effective-policy-incl-defaulted record is missing"
fi
if (has 'defaulted' || has 'no stated' || has 'default') && (has 'written out' || has 'written-out') && (has 'identical' || has 'identically'); then
  pass "policy-emit: a defaulted value behaves and hashes identically to the written-out default (C5/C21)"
else
  bad "policy-emit: the defaulted-equals-written-out (identical hash) record is missing"
fi

# --- 3. Flag in POLICY HASH, excluded from structural fingerprint (DoD 3) -----
# (C21/C27) A policy-only durability change PROCEEDS at resume.
if (has 'policy hash' || has 'policy-hash') && (has 'durab'); then
  pass "hash: the durability flag participates in the policy hash"
else
  bad "hash: the durability-flag-in-the-policy-hash record is missing"
fi
if (has 'exclud' || has 'not .* structural fingerprint' || has 'not in the structural') && (has 'structural fingerprint' || has 'structural-fingerprint'); then
  pass "hash: the durability flag is excluded from the structural fingerprint"
else
  bad "hash: the excluded-from-the-structural-fingerprint record is missing"
fi
if (has 'policy-only' || has 'policy only' || has 'proceed') && (has 'resume') && (has 'C21' || has 'C27'); then
  pass "hash: a policy-only durability change proceeds at resume (C21/C27)"
else
  bad "hash: the policy-only-durability-change-proceeds-at-resume record is missing"
fi

# --- 4. Reference trait pair on the OUTPUT TYPE (DoD 4; Test plan round-trip) --
# Two operations: serialize a self-describing, serde-serializable reference;
# rehydrate the typed value from it later. Sits on the OUTPUT TYPE. Error typing
# and fallibility/async fixed.
if (has 'trait') && (has 'output type' || has 'output .* type') && (has 'serialize.?reference' || has 'serialize-reference' || has 'serialize a reference' || has 'serialize .* reference') && (has 'rehydrat'); then
  pass "trait: the reference trait pair (serialize-reference + rehydrate) sits on the node's OUTPUT TYPE"
else
  bad "trait: the reference-trait-pair-on-the-output-type record is missing"
fi
if (has 'self-describing' || has 'self describing') && (has 'serde' || has 'serializable' || has 'serde-serializable'); then
  pass "trait: the reference is a self-describing, serde-serializable value"
else
  bad "trait: the self-describing / serde-serializable reference record is missing"
fi
if (has 'error type' || has 'error typing' || has 'Result<' || has 'fallible') && (has 'fallib' || has 'async' || has 'infallib' || has 'synchronous'); then
  pass "trait: error typing and fallibility/async-ness for both operations are fixed"
else
  bad "trait: the error-typing / fallibility-and-async record is missing"
fi
# Contract is on the output type (not the task) so any durable node's output is
# reconstructable regardless of producer — needed for single-node replay (C26).
if (has 'output type' || has 'not .* task' || has 'not the task') && (has 'single-node replay' || has 'single node replay' || has 'C26' || has 'regardless of .* produced' || has 'regardless of which node'); then
  pass "trait: the contract is on the output type (not the task) so replay works regardless of producer (C26/T57)"
else
  bad "trait: the on-the-output-type-not-the-task rationale (single-node replay) is missing"
fi
# Round-trip: serialize-reference then rehydrate is lossless.
if (has 'round-trip' || has 'round trip' || has 'lossless') && (has 'rehydrat') && (has 'equal' || has 'same value' || has 'original'); then
  pass "trait: serialize-reference then rehydrate composes into a lossless round-trip"
else
  bad "trait: the lossless serialize-then-rehydrate round-trip record is missing"
fi

# --- 5. Existence-check operation (DoD 5; Test plan existence check) ----------
# A cheap probe that does NOT fetch the value; present / absent /
# cannot-determine classification; a dangling reference fails the resume PLAN
# before execution begins.
if (has 'existence.?check' || has 'existence check') && (has 'cheap' || has 'without fetching' || has 'does not fetch' || has 'not fetch' || has "doesn't fetch"); then
  pass "existence: a cheap existence check that does not fetch the value is defined"
else
  bad "existence: the cheap-probe-does-not-fetch record is missing"
fi
if (has 'present') && (has 'absent') && (has 'cannot-determine' || has 'cannot determine' || has 'cannot-determin' || has 'indeterminate'); then
  pass "existence: the present / absent / cannot-determine classification is fixed"
else
  bad "existence: the present/absent/cannot-determine classification is missing"
fi
if (has 'dangling' || has 'deleted' || has 'absent') && (has 'resume plan' || has 'resume-plan' || has 'the plan') && (has 'before .* execution' || has 'before execution begins' || has 'up front' || has 'not the eleventh' || has 'not mid-run' || has 'not mid run'); then
  pass "existence: a dangling reference fails the resume plan before execution begins (C27)"
else
  bad "existence: the dangling-fails-the-plan-before-execution record is missing"
fi

# --- 6. Assembly-enforcement seam (DoD 6; Test plan reject-at-assembly) -------
# (C7) Assembly proves, from the flag + output type, that a durable-marked node
# implements the contract; violation names the offending node; part of the
# all-problems-reported pass. Feeds T14.
if (has 'assembly') && (has 'reject' || has 'fail') && (has 'durable-marked' || has 'durable marked' || has 'marked durable' || has 'durable node') && (has 'lack' || has 'does not implement' || has "doesn't implement" || has 'without .* contract'); then
  pass "assembly: assembly rejects a durable-marked node whose output type lacks the contract"
else
  bad "assembly: the reject-durable-marked-node-lacking-the-contract record is missing"
fi
if (has 'name' && (has 'offending node' || has 'the node' || has 'naming .* node')); then
  pass "assembly: the assembly error names the offending node"
else
  bad "assembly: the names-the-offending-node requirement is missing"
fi
if (has 'all-problems' || has 'all problems' || has 'every .* problem' || has 'all .* problem') && (has 'C7'); then
  pass "assembly: it is one of the all-problems-reported assembly errors (C7)"
else
  bad "assembly: the all-problems-reported-pass (C7) record is missing"
fi
# Non-durable node with the same non-implementing output type assembles cleanly.
if (has 'non-durable' || has 'not durable' || has 'not-durable' || has 'default') && (has 'assemble' || has 'assembly succeeds' || has 'assembles' || has 'no error') && (has 'only .* durable' || has 'only when .* durab' || has 'required only' || has 'same .* output type' || has 'nothing'); then
  pass "assembly: a non-durable node with the same non-implementing output type assembles without error"
else
  bad "assembly: the non-durable-node-needs-nothing record is missing"
fi

# --- 7. Schema field per attempt, copied forward on resume (DoD 8; T39) -------
# (C22/C27) The durable reference recorded per attempt in the artifact and
# copied forward on resume so artifacts stay self-contained. Feeds T39.
if (has 'per attempt' || has 'per-attempt' || has 'attempt record' || has 'attempt-record') && (has 'reference') && (has 'artifact'); then
  pass "schema: the durable reference is recorded per attempt in the artifact (C22)"
else
  bad "schema: the reference-recorded-per-attempt record is missing"
fi
if (has 'copied forward' || has 'copy forward' || has 'copy-forward' || has 'carried forward') && (has 'self-contained' || has 'self contained' || has 'resume'); then
  pass "schema: the reference is copied forward on resume so artifacts stay self-contained (C22/C27)"
else
  bad "schema: the copied-forward-on-resume / self-contained record is missing"
fi
# Attempt-record field is serializable / round-trips through the artifact schema.
if (has 'serializable' || has 'serde' || has 'round-trip' || has 'round trip') && (has 'attempt record' || has 'artifact schema' || has 'schema field' || has 'attempt-record'); then
  pass "schema: the attempt-record reference field is serializable / round-trips through the schema (feeds T39)"
else
  bad "schema: the serializable-attempt-record-field record is missing"
fi

# --- 8. In-memory (non-durable) outputs re-execute on demand at resume (DoD 9) -
# (C27) Cannot be rehydrated; re-execute on demand; contract applies only to
# durable-marked nodes.
if (has 'in-memory' || has 'in memory') && (has 'cannot .* rehydrat' || has 'not .* rehydrat' || has 'never .* rehydrat') && (has 're-execut' || has 're-run' || has 'rerun' || has 're-executes on demand' || has 'on demand'); then
  pass "in-memory: in-memory (non-durable) outputs cannot be rehydrated and re-execute on demand at resume (C27)"
else
  bad "in-memory: the in-memory-outputs-re-execute-on-demand record is missing"
fi
if (has 'only .* durable' || has 'only to durable' || has 'deliberately applies only' || has 'applies only to durable'); then
  pass "in-memory: the contract deliberately applies only to durable-marked nodes"
else
  bad "in-memory: the applies-only-to-durable-marked-nodes record is missing"
fi

# --- 9. Downstream hand-off (DoD 10; Test plan cross-references) --------------
handoff_ok=1
for t in 'T14' 'T39' 'T57' 'T58'; do
  has "$t" || handoff_ok=0
done
if [ "$handoff_ok" = 1 ]; then
  pass "handoff: all four blocked tickets named (T14, T39, T57, T58)"
else
  bad "handoff: not every blocked ticket (T14, T39, T57, T58) is named"
fi
if (has 'T14') && (has 'assembly'); then
  pass "handoff: T14 consumes the assembly-enforcement check"
else
  bad "handoff: the T14 hand-off (assembly check) is missing"
fi
if (has 'T39') && (has 'schema' || has 'reference field' || has 'schemas'); then
  pass "handoff: T39 consumes the durable-reference schema field"
else
  bad "handoff: the T39 hand-off (schema field) is missing"
fi
if (has 'T57') && (has 'declar' || has 'record' || has 'flag' || has 'reference'); then
  pass "handoff: T57 consumes the declaration/recording of the flag + reference"
else
  bad "handoff: the T57 hand-off (declaration/recording) is missing"
fi
if (has 'T58') && (has 'resume' || has 'existence' || has 'rehydrat'); then
  pass "handoff: T58 consumes the resume-core contract (existence check + rehydration)"
else
  bad "handoff: the T58 hand-off (resume core) is missing"
fi

# --- Consistency with already-merged M0 dependency ADRs (T0.2, T0.6, T0.7) ----
# T0.2 (008): the reference IS the node's output, so the contract is on the
# output type; large values live in external storage, not in the run store.
if (has 'T0\.2' || has 'T0.2' || has '008') && (has 'reference .* output' || has 'output .* reference' || has 'the reference is the .* output' || has 'reference as the .* output'); then
  pass "consistency: composes with T0.2 (008) — the reference is the node's output"
else
  bad "consistency: the T0.2 (reference-is-the-output) consistency record is missing"
fi
# T0.6 (012): durability is LOCAL and file-based; resume requires a surviving
# store; the reference is over the task's OWN output type, not a dagr backend.
if (has 'T0\.6' || has 'T0.6' || has '012') && (has 'local' || has 'file-based' || has 'run store' || has 'surviving store'); then
  pass "consistency: composes with T0.6 (012) — durability is local / file-based"
else
  bad "consistency: the T0.6 (local run store) consistency record is missing"
fi
# T0.7 (013): the durability flag is in the POLICY HASH and EXCLUDED from the
# structural fingerprint; defaulted hashes identically.
if (has 'T0\.7' || has 'T0.7' || has '013') && (has 'policy hash' || has 'structural fingerprint' || has 'policy-hash'); then
  pass "consistency: composes with T0.7 (013) — flag in the policy hash, out of the structural fingerprint"
else
  bad "consistency: the T0.7 (policy-hash / fingerprint) consistency record is missing"
fi

# --- Rejected alternatives (ticket-conventions §4; Out of scope) -------------
# contract-on-the-task (not the output type); a built-in object-store / metadata
# backend; eager existence check that fetches the value; dangling detected
# mid-run instead of at plan time.
if (has 'on the task' || has 'contract on the task' || has 'task .* implement' || has 'per-task contract' || has 'per task') && (has 'reject' || has 'rejected'); then
  pass "rejected: a contract on the task (not the output type) is named and rejected"
else
  bad "rejected: the contract-on-the-task rejected alternative is missing"
fi
if (has 'object.?store' || has 'object store' || has 'built-in .* backend' || has 'built-in .* store' || has 'metadata store' || has 'storage layer' || has 'storage backend') && (has 'reject' || has 'rejected'); then
  pass "rejected: a built-in object-store / metadata-store backend is named and rejected"
else
  bad "rejected: the built-in-object-store rejected alternative is missing"
fi
if (has 'fetch' || has 'eager' || has 'download') && (has 'reject' || has 'rejected'); then
  pass "rejected: an existence check that fetches the value is named and rejected"
else
  bad "rejected: the fetch-on-existence-check rejected alternative is missing"
fi
if (has 'mid-run' || has 'mid run' || has 'eleventh node' || has 'lazy .* check') && (has 'reject' || has 'rejected' || has 'not .* mid-run' || has 'not mid-run'); then
  pass "rejected: detecting a dangling reference mid-run (not at plan time) is named and rejected"
else
  bad "rejected: the dangling-detected-mid-run rejected alternative is missing"
fi
if (has 'reopen'); then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen condition is missing"
fi

# --- Components referenced ----------------------------------------------------
comp_ok=1
for c in 'C27' 'C5' 'C7' 'C22' 'C21'; do
  has "$c" || comp_ok=0
done
if [ "$comp_ok" = 1 ]; then
  pass "component: C27, C5, C7, C22, C21 all referenced"
else
  bad "component: not every governing/related component (C27, C5, C7, C22, C21) is referenced"
fi

# --- Open questions discharged (ticket §Open questions = None; tasks.md T0.8) -
if (has 'open question' || has 'no open question') && (has 'none' || has 'no .* question'); then
  pass "questions: the ADR records that there are no open questions (ticket + tasks.md)"
else
  bad "questions: the ADR does not state its open-questions disposition"
fi

# --- Coverage-matrix disposition (decision ticket owes no covering test) ------
# C27's covering test lands with T57/T58/T59; this ADR makes NO matrix edit.
if (has 'coverage matrix' || has 'coverage-matrix') && (has 'no change' || has 'no edit' || has 'unmapped' || has 'no .* change'); then
  pass "coverage: the ADR states its (no-)coverage-matrix disposition"
else
  bad "coverage: the ADR does not state its coverage-matrix disposition"
fi

# --- Scope boundary (permanent non-goals): durability stays LOCAL -------------
if (has 'local' || has 'file-based') && (has 'metadata store' || has 'scheduler' || has 'distributed' || has 'object-store abstraction' || has 'networked' || has 'no built-in .* backend'); then
  pass "scope: the ADR restates the local-durability / permanent-non-goals boundary"
else
  bad "scope: the ADR does not restate the local-durability / non-goals boundary"
fi

# --- No production code was added (DOC-ONLY decision) ------------------------
# The ADR ships no crate change; assert the ADR SAYS so (the tree-clean check is
# the gate/porcelain's job, not this script's).
if (has 'no .* code' || has 'no production code' || has 'doc-only' || has 'doc only' || has 'shipping crates .* unchanged' || has 'crates .* unchanged'); then
  pass "doc-only: the ADR records that it ships no production code (flag/trait/check/field are T14/T39/T57/T58)"
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
