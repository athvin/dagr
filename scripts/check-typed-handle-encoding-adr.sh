#!/usr/bin/env bash
# C2/C3 typed-handle + dependency-encoding ADR acceptance checks for ticket 018
# (T5).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/018-T5-typed-handle-encoding-spike.md). T5 is a DESIGN
# SPIKE whose durable deliverables are (a) an ADR that RESOLVES and RECORDS the
# typed-handle representation, the dependency/binding encoding, and the three
# open decisions (the exact input-arity ceiling, single-input-`T` ergonomics,
# and the trigger-rule typestate), with EVIDENCE quoted from a throwaway spike
# (real rustc error codes), and (b) skeletal compile-fail / positive fixtures
# wired to the existing T8 UI harness (crates/core/tests/ui.rs) with NO harness
# change — the fixtures T8/T10/T11/T12 adopt.
#
# The ADR fixes: the handle carries node identity plus the value's type and is
# freely COPYABLE (PhantomData<fn() -> T> keeps it Copy/Send/Sync regardless of
# T); a handle is obtainable ONLY by registering a node — no fabrication, no
# lookup by name/index/string key (C2); a cycle (data OR ordering edge) is
# INEXPRESSIBLE by the backward-reference registration discipline, structural
# per C2, never a later/runtime validation pass; a wrong-VALUE-TYPE binding is a
# compile error naming BOTH the expected and supplied type (C3, verified against
# the pinned toolchain per C28); a wrong-ARITY binding is a compile error; the
# maximum input arity is FIXED at 8, with a curated
# #[diagnostic::on_unimplemented] at the cliff (one message, not a trait-error
# cascade); a single-input task consumes `T`, never a one-tuple `(T,)`; one
# handle fans out to any number of consumers; and a non-default trigger rule on
# a data-dependent node is INEXPRESSIBLE via builder typestate (a compile error,
# not a runtime check).
#
# The load-bearing assertions are COMPLETENESS (every seam element and every
# named compile-fail / positive case the ticket is chartered to lock is
# recorded) and INTERNAL CONSISTENCY (identity from the name not registration
# order; the handle is unforgeable and Copy; the cycle guarantee is structural,
# not a runtime pass; wrong type and wrong arity are compile errors; the ceiling
# is 8 with the curated diagnostic; single input is `T` not `(T,)`; the
# typestate makes the non-default rule inexpressible). Authored FIRST as the
# acceptance gate, it fails on the ticket file as it stands before the ADR is
# written into it, and passes once the ADR records every element and the
# fixtures are in place.
#
# This spike ships NO shipping-crate change: the shipping crates (core,
# artifact, render, cli) and Cargo.lock are untouched. The exploratory spike
# code was built OUTSIDE the workspace (in /tmp) and DELETED before finish. The
# only committed artifacts are the embedded ADR, this mechanical acceptance
# script, and the skeletal compile-fail / positive fixtures under
# crates/core/tests/ (wired to the existing T8 UI harness, adding no harness
# change) — exactly as T0.9 (015) did.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = a required file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/018-T5-typed-handle-encoding-spike.md"
ui_dir="crates/core/tests/ui"
positive_test="crates/core/tests/typed_handle_positive.rs"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1..T0.9 precedent). Scope every content assertion to the ADR
# BODY only: the slice from the first 'ADR:' heading line to EOF. The ticket's
# own H1 ('# 018 · T5 — …') is deliberately NOT matched (no 'ADR:' colon), so
# before the embedded ADR is authored the slice is empty and every content
# check fails — exactly the tests-first behaviour.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }

# --- ADR skeleton (ticket-conventions §4) ------------------------------------
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

# --- Accepted status ---------------------------------------------------------
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

# --- 1. Handle carries identity + value type and is freely COPYABLE (DoD 1) --
if (has 'handle') && (has 'identity' || has 'node identity') \
   && (has 'value.?type' || has 'type of the value' || has "value's type"); then
  pass "handle: the handle carries node identity PLUS the value's type"
else
  bad "handle: the identity-plus-value-type property is missing"
fi
if (has 'copy' || has 'copyable' || has 'freely copyable') \
   && (has 'phantomdata' || has 'phantom' || has 'fn() -> t' || has 'fn pointer' || has 'fn-pointer'); then
  pass "handle: the handle is freely Copy via a fn-pointer PhantomData (Copy/Send/Sync regardless of T)"
else
  bad "handle: the freely-copyable / PhantomData<fn()->T> encoding is missing"
fi

# --- 2. Handle is UNFORGEABLE: only by registration; no key lookup (DoD 2,3) -
if (has 'only .* register' || has 'only be obtained' || has 'only obtainable' || has 'obtain.* by register' || has 'produced by registering') \
   && (has 'no .* fabricate' || has 'unforgeable' || has 'no .* constructor' || has 'no escape hatch' || has 'no public constructor' || has 'no other constructor'); then
  pass "unforgeable: a handle is produced ONLY by registering a node (no fabrication)"
else
  bad "unforgeable: the handle-only-by-registration / no-fabrication property is missing"
fi
if (has 'no api' || has 'no lookup' || has 'no method' || has 'no way to' || has 'never') \
   && (has 'name' && has 'index' && (has 'string key' || has 'string')); then
  pass "no-lookup: no API retrieves an output by name, index, or string key (C2)"
else
  bad "no-lookup: the no-lookup-by-name/index/string-key rule is missing"
fi

# --- 3. Identity from the NAME, not registration order (DoD 5) ---------------
if (has 'identity') \
   && (has 'from the name' || has 'from its name' || has 'declared name' || has 'stable name' || has 'name, never' || has 'name — never') \
   && (has 'rename' || has 'renaming'); then
  pass "identity: identity comes from the name (renaming changes identity)"
else
  bad "identity: the identity-from-name / rename-changes-identity property is missing"
fi
if (has 'reorder' || has 'reordering' || has 'registration order') \
   && (has 'changes nothing' || has 'never from registration order' || has 'not .* registration order' || has 'unchanged' || has 'no change'); then
  pass "identity: reordering registrations changes nothing (identity not from order)"
else
  bad "identity: the reorder-changes-nothing property is missing"
fi
if (has 'T0\.7' || has 'T0.7') && (has 'T10' || has 'T13'); then
  pass "identity: the identity-from-name decision is traced to T0.7 and feeds T10/T13"
else
  bad "identity: the ADR does not trace identity to T0.7 / feed T10/T13"
fi

# --- 4. Cycle inexpressible (data AND ordering), structural not runtime (DoD 4) -
if (has 'inexpressible' || has 'cannot be expressed' || has 'cannot be written' || has 'unrepresentable') \
   && (has 'cycle') \
   && (has 'data' && (has 'ordering'  || has 'ordering edge' || has 'ordering-edge')); then
  pass "cycle: a cycle via data edges OR ordering edges is inexpressible"
else
  bad "cycle: the data-and-ordering cycle-inexpressibility statement is missing"
fi
if (has 'backward.?reference' || has 'backward reference' || has 'already-registered' || has 'already registered' || has 'already exist') \
   && (has 'register'); then
  pass "cycle: the guarantee rests on the backward-reference registration discipline"
else
  bad "cycle: the backward-reference registration discipline is missing"
fi
if (has 'structural' || has 'by construction') \
   && (has 'not a .* validation pass' || has 'not a runtime' || has 'no runtime' || has 'not a later validation' || has 'never a .* validation' || has 'never a runtime'); then
  pass "cycle: the guarantee is structural (C2), not a later/runtime validation pass"
else
  bad "cycle: the structural-not-runtime-pass property is missing"
fi
if (has 'checked-in compile-fail' || has 'checked-in compile.?fail' || has 'compile-failure test' || has 'compile-fail fixture' || has 'compile failure test'); then
  pass "cycle: the guarantee is demonstrated by a checked-in compile-failure fixture"
else
  bad "cycle: the checked-in-compile-failure demonstration is missing"
fi

# --- 5. Wrong-VALUE-TYPE binding is a compile error naming BOTH types (DoD 6) -
if (has 'wrong.?type' || has 'wrong value type' || has 'type mismatch' || has 'mismatched type') \
   && (has 'compile error' || has 'compile-error' || has 'fails to compile' || has 'compile-fail') \
   && (has 'both' && (has 'expected' && (has 'supplied' || has 'found' || has 'actual'))); then
  pass "wrong-type: a wrong-value-type binding is a compile error naming BOTH expected and supplied types"
else
  bad "wrong-type: the wrong-type-names-both-types property is missing"
fi
# Real rustc evidence must be quoted (E-code).
if (has 'E0271' || has 'E0308'); then
  pass "wrong-type: a real rustc error code (E0271 / E0308) from the spike is quoted"
else
  bad "wrong-type: no rustc error-code evidence for the wrong-type case is quoted"
fi
if (has 'C28') && (has 'pinned' || has 'workspace toolchain'); then
  pass "wrong-type: the message assertion is pinned to the workspace toolchain per C28"
else
  bad "wrong-type: the C28 pinned-toolchain note is missing"
fi

# --- 6. Wrong-ARITY binding is a compile error (DoD 7) -----------------------
if (has 'wrong.?arity' || has 'arity mismatch' || has 'different number of handles' || has 'wrong number') \
   && (has 'compile error' || has 'compile-error' || has 'fails to compile' || has 'compile-fail'); then
  pass "wrong-arity: a wrong-arity binding is a compile error"
else
  bad "wrong-arity: the wrong-arity-is-a-compile-error property is missing"
fi

# --- 7. Arity CEILING fixed at 8 with curated on_unimplemented (DoD 9, OQ) ----
if (has 'arity') && (has 'ceiling' || has 'maximum' || has 'ceiling' || has 'cliff') \
   && (has '\b8\b' || has 'eight'); then
  pass "arity: the maximum input arity ceiling is fixed at 8"
else
  bad "arity: the arity ceiling (8) is not fixed/recorded"
fi
if (has 'on_unimplemented' || has 'on-unimplemented' || has 'diagnostic::on') \
   && (has 'curated' || has 'one .* message' || has 'single .* message' || has 'not a .* cascade' || has 'rather than a .* cascade' || has 'wall of trait'); then
  pass "arity: a curated #[diagnostic::on_unimplemented] message sits at the cliff (one message, not a cascade)"
else
  bad "arity: the curated on_unimplemented arity-cliff message is missing"
fi
if (has 'aggregate' && has 'struct') && (has 'intermediate node' || has 'intermediate'); then
  pass "arity: the cliff message points at the struct-aggregation remedy"
else
  bad "arity: the struct-aggregation remedy is not recorded"
fi
if (has 'E0277'); then
  pass "arity: the real rustc E0277 curated-diagnostic evidence is quoted"
else
  bad "arity: no E0277 evidence for the arity cliff is quoted"
fi

# --- 8. Single-input ergonomics take T, not (T,) (DoD 10, OQ) -----------------
if (has 'single.?input' || has 'single input' || has 'one input') \
   && (has 'consume.* `?t`?' || has 'take.* `?t`?' || has 'plain `t`' || has '`t` directly' || has 'the value directly') \
   && (has '\(t,\)' || has 'one-tuple' || has 'one tuple' || has 'tuple wrapping'); then
  pass "single-input: a single-input task consumes T, never a one-tuple (T,)"
else
  bad "single-input: the single-input-T (not (T,)) ergonomics decision is missing"
fi

# --- 9. Fan-out: one handle -> many consumers (DoD 8) ------------------------
if (has 'fan.?out' || has 'fan out') \
   && (has 'many' || has 'any number' || has 'multiple' || has 'several') \
   && (has 'consumer' || has 'downstream'); then
  pass "fan-out: one handle can be bound to any number of downstream consumers"
else
  bad "fan-out: the fan-out (one handle, many consumers) property is missing"
fi

# --- 10. Non-default trigger rule on a data node is INEXPRESSIBLE (DoD 8/typestate) -
if (has 'typestate' || has 'type-state' || has 'type state') \
   && (has 'non-default' || has 'non default' || has 'all-terminal' || has 'any-failed') \
   && (has 'inexpressible' || has 'not offered' || has 'no such method' || has 'no method' || has 'compile error' || has 'compile-error'); then
  pass "typestate: a non-default trigger rule on a data-dependent node is inexpressible (typestate, not runtime)"
else
  bad "typestate: the trigger-rule typestate restriction is missing"
fi
if (has 'all-succeeded' || has 'all succeeded') && (has 'default'); then
  pass "typestate: a default-rule data node still assembles as all-succeeded"
else
  bad "typestate: the default-rule (all-succeeded) data-node record is missing"
fi
if (has 'E0599'); then
  pass "typestate: the real rustc E0599 evidence for the missing method is quoted"
else
  bad "typestate: no E0599 evidence for the typestate case is quoted"
fi

# --- 11. Open questions discharged (ticket §OQ + tasks.md T5 Q:) --------------
if (has 'open question' || has 'open-question') \
   && (has 'arity ceiling' || has 'ceiling') && (has 'single.?input' || has 'ergonomics'); then
  pass "questions: both open questions (arity ceiling; single-input ergonomics) are recorded resolved"
else
  bad "questions: the open-questions resolutions are not both recorded"
fi

# --- 12. Skeletal fixtures wired to the T8 harness (DoD 11,12) ----------------
# Compile-fail fixtures live in crates/core/tests/ui/ with sibling .stderr;
# positive fixtures live in a compiled integration test.
for f in typed_handle_wrong_arity typed_handle_unforgeable typed_handle_data_cycle typed_handle_non_default_rule_on_data_node; do
  if [ -f "$ui_dir/$f.rs" ] && [ -f "$ui_dir/$f.stderr" ]; then
    pass "fixture: $f.rs + .stderr present in the UI harness dir"
  else
    bad "fixture: $f.{rs,stderr} missing from $ui_dir"
  fi
done
# The pre-existing T8 wrong-type seed is reused as the C3 wrong-type case.
if [ -f "$ui_dir/wrong_type_binding.rs" ] && [ -f "$ui_dir/wrong_type_binding.stderr" ]; then
  pass "fixture: the T8 wrong_type_binding seed is present and reused for the C3 wrong-type case"
else
  bad "fixture: the wrong_type_binding seed is missing from $ui_dir"
fi
# The positive fixtures (compile == the assertion): Copy/fan-out/single-input-T.
if [ -f "$positive_test" ] \
   && grep -q 'single_input_takes_t_not_one_tuple' "$positive_test" 2>/dev/null \
   && grep -q 'fan_out_one_handle_many_consumers' "$positive_test" 2>/dev/null \
   && grep -q 'handles_are_freely_copyable' "$positive_test" 2>/dev/null; then
  pass "fixture: the positive (compiles) fixtures — Copy, fan-out, single-input-T — present in $positive_test"
else
  bad "fixture: the positive Copy/fan-out/single-input-T fixtures are missing from $positive_test"
fi
# The ADR must name the harness the fixtures are wired to.
if (has 'ui.rs' || has 'tests/ui' || has 'T8' || has 'UI harness' || has 'UI-test harness'); then
  pass "fixture: the ADR names the T8 UI harness the fixtures are wired to"
else
  bad "fixture: the ADR does not name the harness the fixtures are wired to"
fi
# The fixtures are pinned-toolchain and regenerated deliberately on a bump (C28).
if (has 'C28') && (has 'toolchain bump' || has 'bump' || has 'regenerat') && (has 'pinned' || has 'deliberate'); then
  pass "fixture: the ADR notes the pinned-toolchain dependency + deliberate regeneration on a bump (C28)"
else
  bad "fixture: the pinned-toolchain / deliberate-regeneration note is missing"
fi

# --- 13. Throwaway prototype disposition (spike-code quarantine) --------------
if (has 'throwaway' || has 'thrown away' || has 'disposable') \
   && (has '/tmp' || has 'outside the workspace' || has 'outside the repo') \
   && (has 'delete' || has 'deleted'); then
  pass "spike: the prototype was built outside the workspace (/tmp) and deleted"
else
  bad "spike: the spike-code disposition (built in /tmp, deleted) is missing"
fi
if (has 'T10' && has 'T11') && (has 'real' || has 'ship' || has 'implement' || has 'production'); then
  pass "spike: the ADR states the real C2/C3 implementation is T10/T11, not this ticket"
else
  bad "spike: the ADR does not point at T10/T11 for the real implementation"
fi

# --- 14. Downstream hand-off (Consequences) ----------------------------------
if (has 'T10') && (has 'T11') && (has 'T12'); then
  pass "handoff: the blocked consumers (T10, T11, T12) are named"
else
  bad "handoff: the T10/T11/T12 downstream hand-off is missing"
fi

# --- Consistency with the already-merged M0 ADRs (T1, T0.2, T0.7, T0.9) -------
if (has 'T1\b' || has 'T1,' || has 'T1 ') && (has 'T0\.2' || has 'T0.2') \
   && (has 'T0\.7' || has 'T0.7') && (has 'T0\.9' || has 'T0.9'); then
  pass "consistency: the ADR states consistency with T1, T0.2, T0.7, and T0.9"
else
  bad "consistency: the ADR does not state consistency with the merged M0 ADRs (T1/T0.2/T0.7/T0.9)"
fi
# Ownership model (T0.2): types are compile-time (C3), modes assembly-time (C1).
if (has 'T0\.2' || has 'T0.2') \
   && (has 'type.* compile' || has 'compile.* type' || has 'value type' ) \
   && (has 'mode' && (has 'assembly' || has 'assembly-time' || has 'assembly time')); then
  pass "consistency: the ADR keeps T0.2's type-at-compile-time / mode-at-assembly split"
else
  bad "consistency: the ADR does not keep T0.2's type-vs-mode partition"
fi

# --- Rejected alternatives (ticket-conventions §4) ---------------------------
# PhantomData<T> (over-constrains); runtime cycle detection; name/index lookup
# registry; one-tuple single input; runtime trigger-rule check.
if (has 'phantomdata<t>' || has 'phantomdata< *t *>' || has 'phantom.* of t' || has 'naive phantom' || has 'phantomdata of the value') && (has 'reject' || has 'rejected'); then
  pass "rejected: the naive PhantomData<T> phantom is named and rejected"
else
  bad "rejected: the PhantomData<T>-over-constrains rejected alternative is missing"
fi
if (has 'runtime cycle' || has 'runtime .* detection' || has 'validation pass') && (has 'reject' || has 'rejected'); then
  pass "rejected: a runtime cycle-detection / validation pass is named and rejected"
else
  bad "rejected: the runtime-cycle-detection rejected alternative is missing"
fi
if (has 'lookup' || has 'registry' || has 'name-keyed' || has 'name key' || has 'string key') && (has 'reject' || has 'rejected'); then
  pass "rejected: a name/index/string-key lookup registry is named and rejected"
else
  bad "rejected: the lookup-registry rejected alternative is missing"
fi
if (has '\(t,\)' || has 'one-tuple' || has 'one tuple') && (has 'reject' || has 'rejected' || has 'unnecessary'); then
  pass "rejected: the one-tuple (T,) single-input form is named and rejected/unnecessary"
else
  bad "rejected: the one-tuple single-input rejected alternative is missing"
fi
if (has 'runtime' && has 'trigger rule') && (has 'reject' || has 'rejected'); then
  pass "rejected: a runtime trigger-rule check (vs typestate) is named and rejected"
else
  bad "rejected: the runtime-trigger-rule-check rejected alternative is missing"
fi
if (has 'reopen'); then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen condition is missing"
fi

# --- Components referenced ----------------------------------------------------
comp_ok=1
for c in 'C2' 'C3'; do
  has "$c" || comp_ok=0
done
if [ "$comp_ok" = 1 ]; then
  pass "component: C2 and C3 (the ticket's governing components) are referenced"
else
  bad "component: the governing components (C2, C3) are not both referenced"
fi

# --- Coverage-matrix disposition (decision ticket owes no covering test) ------
if (has 'coverage matrix' || has 'coverage-matrix') && (has 'no change' || has 'no edit' || has 'unmapped'); then
  pass "coverage: the ADR states its (no-)coverage-matrix disposition (C2/C3 owed by T10/T12)"
else
  bad "coverage: the ADR does not state its coverage-matrix disposition"
fi

# --- Scope boundary (permanent non-goals) ------------------------------------
if (has 'scope' || has 'non-goal' || has 'boundary') \
   && (has 'never change.* at runtime' || has 'shape .* fixed at compile' || has 'no runtime' || has 'not a scheduler' || has 'no .* dsl' || has 'no lookup registry' || has 'runtime-mutable'); then
  pass "scope: the ADR restates the permanent scope boundary (compile-time-fixed graph, no DSL/registry)"
else
  bad "scope: the ADR does not restate the permanent scope boundary"
fi

# --- No production code was added (spike quarantine) -------------------------
if (has 'no .* production code' || has 'no production code' || has 'shipping crates .* unchanged' || has 'crates .* unchanged' || has 'crates/\*/src .* unchanged' || has 'no shipping-crate'); then
  pass "no-prod: the ADR records that no shipping-crate code changed (real impl is T10/T11)"
else
  bad "no-prod: the ADR does not record its no-shipping-code disposition"
fi

if [ "$fail" = 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
