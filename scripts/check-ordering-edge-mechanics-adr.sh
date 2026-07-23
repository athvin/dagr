#!/usr/bin/env bash
# C4 ordering-edge-mechanics ADR acceptance checks for ticket 015 (T0.9).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/015-T0.9-ordering-edge-mechanics.md). T0.9 is a DECISION
# ticket whose durable deliverable is an ADR that locks the C4 ordering-edge API
# shape every downstream ordering-edge consumer binds against, plus the exact
# compile-fail contract the wiring compile-failure suite (T12) will assert for
# ordering-edge cycles, before either the ordering-edge implementation (T50) or
# the compile-fail suite (T12) is written. The ADR fixes: the registration-time
# API shape (an ordering edge is declared ONLY at the downstream node's
# registration, against ALREADY-REGISTERED upstream handles); the type-erasure
# rule (an ordering edge ignores the handle's value type — any `T` is an
# acceptable ordering upstream, mixing types is legal); that a node may carry
# BOTH data dependencies and additional ordering edges, and that an
# ordering-only node RECEIVES NO VALUE; the constructive cycle-inexpressibility
# argument (structural per C2, never a runtime cycle-detection pass); and the
# compile-fail assertion table for T12 (per case: name, misuse, observable
# expectation), covering at minimum ordering_edge_self_cycle,
# ordering_edge_back_edge, and a positive ordering_edge_any_value_type_ok.
#
# Its "tests" are documentary completeness and internal-consistency checks
# against the recorded ADR: that every seam element the ticket is chartered to
# fix is present, in the exact normative vocabulary of arch.md (C2, C3, C4, C5,
# C28, "Vocabulary"), so no seam is left open to T12/T50. Two skeletal
# compile-fail fixtures for the ordering-edge cycle cases, plus the positive
# type-erasure fixture and the data-plus-ordering coexistence fixture, are wired
# into the same harness T12 uses (the T8 UI harness, crates/core/tests/ui.rs)
# and asserted present here; the T8 harness itself proves they compile-fail (or,
# for the positive fixtures, compile) under the pinned toolchain.
#
# The load-bearing assertions are COMPLETENESS (every named seam element and
# every named compile-fail case is recorded) and INTERNAL CONSISTENCY (an
# ordering edge is declared only at registration against existing handles; NO
# API accepts an ordering upstream by name/index/string key; NO after-the-fact
# edge API exists; the cycle guarantee is STRUCTURAL, not a runtime pass;
# ordering upstreams are TYPE-ERASED; an ordering-only node receives NO VALUE;
# and the non-default-trigger-rule restriction is explicitly EXCLUDED as
# C5/T50 scope). Authored FIRST as the acceptance gate, it fails on the ticket
# file as it stands before the ADR is written into it, and passes once the ADR
# records every element the ticket is chartered to lock.
#
# This is a DOC-ONLY decision (the ordering-edge mechanics are fully decidable
# from arch.md's already-settled Option A; the ordering-edge machinery is
# IMPLEMENTED by T50 and the compile-fail suite by T12, not here), so the script
# builds no shipping prototype and asserts no production code: the shipping
# crates and Cargo.lock are untouched, and the only committed artifacts are the
# embedded ADR, this mechanical acceptance script, and the skeletal
# compile-fail / positive fixtures under crates/core/tests/ui/ (wired to the
# existing T8 harness, adding no harness change).
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = a required file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/015-T0.9-ordering-edge-mechanics.md"
ui_dir="crates/core/tests/ui"
positive_test="crates/core/tests/ordering_edge_positive.rs"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2..T0.8 precedent: '# ADR: <title>'). The ticket prose
# above it (Objective, Test plan, DoD) already names the API shape, cycles,
# type-erasure, etc. — so a whole-file grep would pass content checks the ADR
# itself has not yet made. We therefore scope every content assertion to the
# ADR BODY only: the slice from the first 'ADR:' heading line to EOF. The
# ticket's own H1 title ('# 015 · T0.9 — …') is deliberately NOT matched (no
# 'ADR:' with a colon), so before the embedded ADR is authored the slice is
# empty and every content check fails — exactly the tests-first behaviour.
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

# --- 1. Registration-time API shape (DoD 1; Test plan) -----------------------
# An ordering edge is declared ONLY at the downstream node's registration,
# against ALREADY-REGISTERED upstream handles.
if (has 'ordering edge' || has 'ordering-edge') \
   && (has 'registration' || has 'registered') \
   && (has 'downstream') \
   && (has 'already-registered' || has 'already registered' || has 'existing handle' || has 'already exist'); then
  pass "api: an ordering edge is declared only at the downstream node's registration against already-registered handles"
else
  bad "api: the registration-time backward-reference API shape is missing"
fi
if (has 'handle') && (has 'only .* register' || has 'obtain.* by register' || has 'only way' || has 'only be obtained' || has 'only obtainable'); then
  pass "api: a handle is obtainable only by registering a node"
else
  bad "api: the handle-only-by-registration property is missing"
fi

# --- 2. No forward reference / no lookup by name/index/string key (DoD 2) -----
# Mirrors C2's "no lookup by name/index/string key."
if (has 'no api' || has 'no method' || has 'no entry point' || has 'none accepts' || has 'no argument') \
   && (has 'name' && has 'index' && (has 'string key' || has 'string' )); then
  pass "no-forward-ref: no API accepts an ordering upstream by node name, index, or string key"
else
  bad "no-forward-ref: the no-name/index/string-key rule is missing"
fi
if (has 'not-yet-registered' || has 'not yet registered' || has 'unregistered' || has 'not yet .* return' || has 'no expression can name'); then
  pass "no-forward-ref: no expression can name a not-yet-registered node"
else
  bad "no-forward-ref: the cannot-name-an-unregistered-node property is missing"
fi

# --- 3. No after-the-fact edge API (DoD 2; Test plan; C4 acceptance) ---------
if (has 'after the fact' || has 'after-the-fact' || has 'afterward' || has 'post-hoc' || has 'post hoc') \
   && (has 'no api' || has 'no method' || has 'none exists' || has 'no .* add' || has 'does not offer'); then
  pass "no-afterward: no API adds an ordering edge between two already-registered nodes after the fact (C4)"
else
  bad "no-afterward: the no-after-the-fact-edge-API rule is missing"
fi
if (has 'add.?ordering.?edge' || has 'add_ordering_edge' || has 'add an edge' || has 'add.* edge'); then
  pass "no-afterward: the absent add-edge method is named (mirrors the back-edge fixture)"
else
  bad "no-afterward: the ADR does not name the deliberately-absent add-edge method"
fi

# --- 4. Cycle inexpressibility is STRUCTURAL, not a runtime pass (DoD 3) ------
if (has 'inexpressible' || has 'cannot be expressed' || has 'cannot be written' || has 'cannot express') \
   && (has 'self' || has 'back.?edge' || has 'descendant') \
   && (has 'cycle'); then
  pass "cycle: an ordering-edge cycle (self or back to a descendant) is inexpressible by construction"
else
  bad "cycle: the cycle-inexpressibility statement (self / descendant) is missing"
fi
if (has 'structural' || has 'by construction') \
   && (has 'not a .* validation pass' || has 'not a runtime' || has 'no runtime' || has 'not a later validation' || has 'never a runtime'); then
  pass "cycle: the guarantee is structural (C2), not a later/runtime validation pass"
else
  bad "cycle: the structural-not-runtime-pass property is missing"
fi
if (has 'checked-in compile-fail' || has 'checked-in compile.?fail' || has 'compile-failure test' || has 'compile-fail test' || has 'compile failure test'); then
  pass "cycle: the guarantee is demonstrated by a checked-in compile-failure test"
else
  bad "cycle: the checked-in-compile-failure-test demonstration is missing"
fi

# --- 5. Type-erasure: any value type is an acceptable ordering upstream (DoD 4) -
if (has 'type-eras' || has 'type eras' || has 'ignores the .* value type' || has 'value type does not' || has 'value type is ignored' || has 'irrespective of .* value type') \
   && (has 'any .* value type' || has 'any `t`' || has 'any type' || has 'regardless of .* value type' || has 'handle of any'); then
  pass "type-erasure: an ordering edge attaches to any node regardless of value type (type-erased)"
else
  bad "type-erasure: the type-erasure / any-value-type property is missing"
fi
if (has 'mixing' || has 'mix ') && (has 'differ' || has 'different value type' || has 'distinct .* type') && (has 'legal' || has 'compiles' || has 'allowed' || has 'is fine'); then
  pass "type-erasure: mixing ordering upstreams of differing value types is legal"
else
  bad "type-erasure: the mixing-different-value-types-is-legal property is missing"
fi

# --- 6. Data + ordering coexist (DoD 5) --------------------------------------
if (has 'both' && has 'data') && (has 'ordering edge' || has 'ordering-edge' || has 'additional ordering') && (has 'carry' || has 'coexist' || has 'may .* both' || has 'may carry'); then
  pass "coexist: a node may carry both data dependencies and additional ordering edges (C4)"
else
  bad "coexist: the data-plus-ordering coexistence property is missing"
fi

# --- 7. Ordering-only node receives no value (DoD 6) -------------------------
if (has 'only .* ordering' || has 'ordering-only' || has 'ordering only' || has 'attached only by ordering') \
   && (has 'no value' || has 'receives no' || has 'no bound' || has 'no input value'); then
  pass "no-value: a node attached only by ordering edges receives no value"
else
  bad "no-value: the ordering-only-receives-no-value property is missing"
fi
if (has 'T50') && (has 'enforce' || has 'implement' || has 'owns' || has 'structural'); then
  pass "no-value: the ADR points at where T50 enforces the no-value shape"
else
  bad "no-value: the ADR does not point at where T50 enforces the no-value shape"
fi

# --- 8. Non-default-trigger-rule restriction is EXCLUDED (DoD 7) -------------
# It belongs to C5/vocabulary and T50; this ticket does not conflate it.
if (has 'non-default' || has 'non default') && (has 'trigger rule' || has 'trigger-rule') \
   && (has 'out of scope' || has 'excluded' || has 'not .* here' || has 'C5' || has 'T50' || has 'vocabulary' || has 'consume nothing' || has 'consume-nothing'); then
  pass "trigger-scope: the non-default-trigger-rule restriction is explicitly excluded (C5/vocabulary/T50)"
else
  bad "trigger-scope: the ADR does not explicitly exclude the non-default-trigger-rule restriction"
fi

# --- 9. Default-rule propagation across ordering edges (C4; Vocabulary) ------
if (has 'ordering upstream') && (has 'propagat' || has 'fail' || has 'skip') && (has 'default' || has 'all-succeeded' || has 'all succeeded'); then
  pass "propagation: under the default rule an ordering upstream's failure/skip propagates like a data upstream's"
else
  bad "propagation: the default-rule ordering-edge propagation record is missing"
fi

# --- 10. Compile-fail assertion table for T12 (DoD 8, 9) ---------------------
# Per case: case name, misuse demonstrated, observable expectation.
if (has 'compile-fail' || has 'compile fail' || has 'assertion table' || has 'compile-failure') \
   && (has 'T12'); then
  pass "table: the ADR carries a compile-fail assertion table for T12"
else
  bad "table: the compile-fail assertion table for T12 is missing"
fi
if (has 'case name' || has 'case') && (has 'misuse') && (has 'observable' || has 'expectation' || has 'fails to compile' || has 'compile-fail'); then
  pass "table: the table states per case the case name, the misuse, and the observable expectation"
else
  bad "table: the per-case (name / misuse / observable) columns are missing"
fi
# The three minimum named cases.
for case_name in 'ordering_edge_self_cycle' 'ordering_edge_back_edge' 'ordering_edge_any_value_type_ok'; do
  if has "$case_name"; then
    pass "table: case '$case_name' is named in the ADR"
  else
    bad "table: required case '$case_name' is not named in the ADR"
  fi
done
# Message-assertion pinning per C28.
if (has 'C28') && (has 'pinned' || has 'pinned .* toolchain' || has 'workspace toolchain'); then
  pass "table: message assertions are pinned to the workspace toolchain per C28"
else
  bad "table: the C28 pinned-toolchain message-assertion note is missing"
fi
# Cross-reference to the C2/C4 acceptance criteria.
if (has 'C2') && (has 'C4') && (has 'cross-ref' || has 'cross ref' || has 'acceptance criteri' || has 'maps to' || has 'satisfi'); then
  pass "table: each case is cross-referenced to the C2/C4 acceptance criterion it satisfies"
else
  bad "table: the cross-reference to the C2/C4 acceptance criteria is missing"
fi

# --- 11. Skeletal compile-fail fixtures wired to the T8/T12 harness (DoD 10) --
# The fixtures live in crates/core/tests/ui/, discovered by the T8 harness
# (crates/core/tests/ui.rs) which T12 reuses. Each compile-fail fixture has a
# sibling .stderr snapshot; the positive fixtures live in a compiled test.
if [ -f "$ui_dir/ordering_edge_self_cycle.rs" ] && [ -f "$ui_dir/ordering_edge_self_cycle.stderr" ]; then
  pass "fixture: ordering_edge_self_cycle.rs + .stderr present in the UI harness dir"
else
  bad "fixture: ordering_edge_self_cycle.{rs,stderr} missing from $ui_dir"
fi
if [ -f "$ui_dir/ordering_edge_back_edge.rs" ] && [ -f "$ui_dir/ordering_edge_back_edge.stderr" ]; then
  pass "fixture: ordering_edge_back_edge.rs + .stderr present in the UI harness dir"
else
  bad "fixture: ordering_edge_back_edge.{rs,stderr} missing from $ui_dir"
fi
# The ADR must reference the harness the fixtures are wired to.
if (has 'ui.rs' || has 'tests/ui' || has 'T8' || has 'trybuild' || has 'UI harness' || has 'UI-test harness'); then
  pass "fixture: the ADR names the harness the fixtures are wired to (the T8 UI harness / trybuild-style)"
else
  bad "fixture: the ADR does not name the harness the cycle fixtures are wired to"
fi
# The positive fixtures are compiled — a compiled test proving the positive
# shapes compile IS the executable regression guard the ticket asks for. They
# live in a normal integration test (compiled by `cargo test --workspace`), NOT
# in tests/ui/, because the T8 harness asserts every tests/ui/*.rs sample
# FAILS to compile; a positive sample there would break the harness.
if [ -f "$positive_test" ] \
   && grep -q 'ordering_edge_any_value_type_ok' "$positive_test" 2>/dev/null; then
  pass "fixture: the positive ordering_edge_any_value_type_ok fixture (compiles) present in $positive_test"
else
  bad "fixture: the positive ordering_edge_any_value_type_ok fixture is missing from $positive_test"
fi
# The data-plus-ordering coexistence / ordering-only-no-value positive fixture.
if [ -f "$positive_test" ] \
   && grep -qE 'data_plus_ordering|ordering_only' "$positive_test" 2>/dev/null; then
  pass "fixture: the data-plus-ordering / ordering-only-no-value positive fixture present in $positive_test"
else
  bad "fixture: the data-plus-ordering / ordering-only-no-value positive fixture is missing from $positive_test"
fi

# --- 12. Linked from T12 and T50 as the source of their contract (DoD 11) -----
if (has 'T12') && (has 'T50'); then
  pass "handoff: both blocked tickets (T12, T50) are named as consumers of this contract"
else
  bad "handoff: the T12 / T50 downstream hand-off is missing"
fi
if (has 'T12') && (has 'cycle' || has 'compile-fail' || has 'compile fail' || has 'assertion'); then
  pass "handoff: T12 consumes the ordering-edge cycle compile-fail contract"
else
  bad "handoff: the T12 hand-off (compile-fail cycle contract) is missing"
fi
if (has 'T50') && (has 'implement' || has 'record' || has 'render' || has 'propagat' || has 'machinery'); then
  pass "handoff: T50 consumes the API shape it implements (recording/rendering/propagation)"
else
  bad "handoff: the T50 hand-off (implementation of the API shape) is missing"
fi

# --- Consistency with the already-merged M0 ADRs (T0.2, T0.4, T0.7) ----------
if (has 'T0\.2' || has 'T0.2') && (has 'T0\.4' || has 'T0.4') && (has 'T0\.7' || has 'T0.7'); then
  pass "consistency: the ADR states consistency with T0.2, T0.4, and T0.7"
else
  bad "consistency: the ADR does not state consistency with the merged M0 ADRs (T0.2/T0.4/T0.7)"
fi

# --- Rejected alternatives (ticket-conventions §4; Out of scope) -------------
# A runtime cycle-detection pass; an after-the-fact add-edge API; coupling the
# value type into the ordering edge.
if (has 'runtime cycle' || has 'runtime .* detection' || has 'validation pass' || has 'graph-validation') && (has 'reject' || has 'rejected'); then
  pass "rejected: a runtime cycle-detection / graph-validation pass is named and rejected"
else
  bad "rejected: the runtime-cycle-detection rejected alternative is missing"
fi
if (has 'add.?edge' || has 'add an edge' || has 'after-the-fact' || has 'after the fact' || has 'mutable graph' || has 'post-hoc') && (has 'reject' || has 'rejected'); then
  pass "rejected: an after-the-fact add-edge API is named and rejected"
else
  bad "rejected: the after-the-fact-add-edge rejected alternative is missing"
fi
if (has 'value type' || has 'typed ordering' || has 'coupl') && (has 'reject' || has 'rejected'); then
  pass "rejected: coupling the value type into the ordering edge is named and rejected"
else
  bad "rejected: the value-type-coupling rejected alternative is missing"
fi
if (has 'reopen'); then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen condition is missing"
fi

# --- Components referenced ----------------------------------------------------
comp_ok=1
for c in 'C2' 'C4'; do
  has "$c" || comp_ok=0
done
if [ "$comp_ok" = 1 ]; then
  pass "component: C2 and C4 (the ticket's governing components) are referenced"
else
  bad "component: the governing components (C2, C4) are not both referenced"
fi

# --- Open questions discharged (ticket §Open questions = None; tasks.md T0.9) -
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

# --- Scope boundary (permanent non-goals) ------------------------------------
if (has 'scope' || has 'non-goal' || has 'boundary') \
   && (has 'never change.* at runtime' || has 'shape never changes' || has 'no runtime' || has 'not a scheduler' || has 'not a metadata store' || has 'dynamic'); then
  pass "scope: the ADR restates the permanent scope boundary (graph shape never changes at runtime)"
else
  bad "scope: the ADR does not restate the permanent scope boundary"
fi

# --- No production code was added (DOC-ONLY decision) ------------------------
# The ADR ships no shipping-crate change; assert the ADR SAYS so (the tree-clean
# check is the gate/porcelain's job, not this script's). Test fixtures under
# crates/core/tests/ui/ are test-only, not shipping code.
if (has 'no .* production code' || has 'no production code' || has 'doc-only' || has 'doc only' || has 'shipping crates .* unchanged' || has 'crates .* unchanged'); then
  pass "doc-only: the ADR records that it ships no production code (machinery is T12/T50)"
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
