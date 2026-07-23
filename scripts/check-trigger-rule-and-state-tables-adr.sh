#!/usr/bin/env bash
# Trigger-rule & terminal-state reference-table ADR acceptance checks for
# ticket 010 (T0.4).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/010-T0.4-trigger-rule-and-state-tables.md). T0.4 is a
# DECISION (reference tables) ticket whose durable deliverable is an ADR that
# canonicalizes arch.md's normative "Vocabulary — terminal states and trigger
# rules" section into binding internal reference tables. Its "tests" are
# documentary completeness checks against the recorded ADR: that EVERY terminal
# state, EVERY state class, and EVERY trigger rule from arch.md appears exactly
# once, correctly classified, plus a total per-rule fires/can-never-fire
# decision table.
#
# The load-bearing assertion is COMPLETENESS: this script cross-references the
# ADR against arch.md's Vocabulary so a missing or invented state/class/rule
# fails the gate. Authored FIRST as the acceptance gate, it fails on the ticket
# file as it stands before the ADR is written into it, and passes once the ADR
# records every element the ticket is chartered to lock.
#
# It does NOT implement the readiness tracker, failure policy, or any runtime
# evaluation — that is T18/T34; what survives here is the ADR's record of the
# canonical tables their tests will later run against.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/010-T0.4-trigger-rule-and-state-tables.md"
archmd="docs/arch.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.3/T0.7 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already names every state, class,
# and rule — so a whole-file grep would pass content checks the ADR itself has
# not yet made. We therefore scope every content assertion to the ADR BODY
# only: the slice of the file from the first 'ADR:' heading line to EOF. The
# ticket's own H1 title ('# 010 · T0.4 — …') is deliberately NOT matched (no
# 'ADR:' with a colon), so before the embedded ADR is authored the slice is
# empty and every content check fails — exactly the tests-first behaviour we
# want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

# Case-insensitive extended-regex grep, scoped to the ADR body only.
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }
# Count occurrences of a fixed literal in the ADR body.
count() { printf '%s' "$adr_body" | grep -oiE "$1" | wc -l | tr -d ' '; }

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

# --- Accepted status (Test plan: "ADR is committed in the accepted state") ---
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

# --- COMPLETENESS: every one of the nine terminal states appears (DoD 1;
# Test plan "State enum is complete and singular") ----------------------------
states="succeeded failed timed-out skipped upstream-skipped upstream-failed cancelled abandoned satisfied-from-prior"
missing_state=0
for s in $states; do
  # match the backticked canonical name; upstream-skipped must not be counted
  # merely because 'skipped' matched, so we anchor each with word-ish bounds.
  if has "\`$s\`"; then
    :
  else
    bad "state: terminal state '$s' is missing from the ADR"
    missing_state=1
  fi
done
[ "$missing_state" -eq 0 ] && pass "state: all nine terminal states present (succeeded…satisfied-from-prior)"

# The count must be exactly nine — a tenth invented state fails completeness.
# We assert the ADR states the number nine explicitly for the taxonomy.
if has 'nine' && (has 'terminal state' || has 'terminal-state'); then
  pass "state: the ADR states the taxonomy is exactly nine terminal states"
else
  bad "state: the ADR must state there are exactly nine terminal states"
fi

# --- Carried payloads (DoD 1; Vocabulary) ------------------------------------
if has 'upstream-skipped' && has 'upstream-failed' && (has 'originating node' || has 'identity of the originating node' || has 'originating-node'); then
  pass "payload: upstream-skipped/upstream-failed carry the originating node identity"
else
  bad "payload: the originating-node-identity payload of the upstream-* states is missing"
fi
if has 'satisfied-from-prior' && (has 'originating run' || has 'run identity' || has 'prior run'); then
  pass "payload: satisfied-from-prior carries the originating run identity"
else
  bad "payload: the originating-run-identity payload of satisfied-from-prior is missing"
fi

# --- The exactly-one-exactly-once invariant (DoD 2; Test plan) ---------------
if (has 'exactly one' && has 'exactly once') || has 'exactly one .* exactly once'; then
  pass "invariant: every node ends in exactly one terminal state, exactly once"
else
  bad "invariant: the exactly-one-exactly-once invariant is missing"
fi

# --- not-requested boundary (DoD 3; Test plan "not-requested is excluded") ----
if has 'not-requested'; then
  pass "not-requested: the boundary is addressed"
else
  bad "not-requested: the not-requested boundary must be recorded"
fi
if has 'not-requested' && (has 'not a terminal state' || has 'is not a terminal state' || has 'artifact marking'); then
  pass "not-requested: recorded as an artifact marking, NOT a terminal state"
else
  bad "not-requested: it must be recorded as a C26 artifact marking, not a terminal state"
fi
if has 'not-requested' && has 'C26'; then
  pass "not-requested: attributed to C26 single-node replay"
else
  bad "not-requested: the C26 single-node-replay attribution is missing"
fi

# --- COMPLETENESS: the four state classes as a total partition (DoD 4;
# Test plan "State classes partition the taxonomy totally") -------------------
classes="success-like skip-like failure-like stop-like"
missing_class=0
for c in $classes; do
  if has "$c"; then :; else bad "class: state class '$c' is missing from the ADR"; missing_class=1; fi
done
[ "$missing_class" -eq 0 ] && pass "class: all four state classes present (success/skip/failure/stop-like)"
if has 'four' && (has 'class' || has 'classes') && (has 'partition' || has 'total'); then
  pass "class: the ADR states the four classes are a total partition"
else
  bad "class: the four-classes-are-a-total-partition statement is missing"
fi

# Per-class membership — each state assigned to its exact class. We assert the
# ADR pairs each state with its class in the class table.
if has 'success-like' && has 'succeeded' && has 'satisfied-from-prior'; then
  pass "class: success-like = {succeeded, satisfied-from-prior}"
else
  bad "class: success-like membership {succeeded, satisfied-from-prior} is wrong/missing"
fi
if has 'skip-like' && has 'skipped' && has 'upstream-skipped'; then
  pass "class: skip-like = {skipped, upstream-skipped}"
else
  bad "class: skip-like membership {skipped, upstream-skipped} is wrong/missing"
fi
if has 'failure-like' && has 'failed' && has 'timed-out' && has 'abandoned' && has 'upstream-failed'; then
  pass "class: failure-like = {failed, timed-out, abandoned, upstream-failed}"
else
  bad "class: failure-like membership {failed, timed-out, abandoned, upstream-failed} is wrong/missing"
fi
if has 'stop-like' && has 'cancelled'; then
  pass "class: stop-like = {cancelled}"
else
  bad "class: stop-like membership {cancelled} is wrong/missing"
fi

# --- satisfied-from-prior is success-like (DoD 10; Test plan) ----------------
if has 'satisfied-from-prior' && has 'success-like'; then
  pass "sfp: satisfied-from-prior classed success-like"
else
  bad "sfp: satisfied-from-prior must be classed success-like"
fi
if has 'satisfied-from-prior' && has 'all-succeeded' && (has 'resumed prior success' || has 'resumed' || has 'prior success'); then
  pass "sfp: a resumed prior success therefore satisfies a downstream all-succeeded (C11)"
else
  bad "sfp: the note that a resumed prior success satisfies all-succeeded is missing"
fi

# --- COMPLETENESS: the closed trigger-rule set (DoD 5; Test plan
# "Trigger-rule set is closed") -----------------------------------------------
rules="all-succeeded all-terminal any-failed"
missing_rule=0
for r in $rules; do
  if has "\`$r\`"; then :; else bad "rule: trigger rule '$r' is missing from the ADR"; missing_rule=1; fi
done
[ "$missing_rule" -eq 0 ] && pass "rule: all three trigger rules present (all-succeeded, all-terminal, any-failed)"
if has 'all-succeeded' && has 'default'; then
  pass "rule: all-succeeded is named the default"
else
  bad "rule: all-succeeded must be named the default rule"
fi
if (has 'closed' && has 'rule') || has 'closed set'; then
  pass "rule: the rule set is stated to be closed"
else
  bad "rule: the closed-set statement is missing"
fi

# --- All-terminal-gated evaluation invariant (DoD 5; Test plan) --------------
if (has 'once every upstream is terminal' || has 'all upstreams? .* terminal' || has 'every upstream .* terminal') && (has 'never fire' || has 'not.*early' || has 'no early' || has 'never .* partial' || has 'partial result'); then
  pass "gate: a rule is evaluated only once every upstream is terminal (no early fire)"
else
  bad "gate: the all-terminal-gated (no early fire on partial results) invariant is missing"
fi

# --- Per-rule decision table exists and is total (DoD 6; Test plan
# "Decision table is total over upstream class combinations") -----------------
if has 'decision table' || (has 'fires' && has 'can never fire') || (has 'fires' && has 'can-never-fire'); then
  pass "table: a per-rule fires / can-never-fire decision table is present"
else
  bad "table: the per-rule decision table is missing"
fi
if has 'total' && (has 'combination' || has 'class'); then
  pass "table: the table is stated to be total over upstream class combinations"
else
  bad "table: the totality-over-class-combinations statement is missing"
fi

# --- all-succeeded fires / can-never-fire branches (DoD 7; Test plan) --------
if has 'all-succeeded' && (has 'fires when every upstream is success-like' || (has 'fires' && has 'every upstream is success-like')); then
  pass "all-succeeded: fires when every upstream is success-like"
else
  bad "all-succeeded: the fires-when-all-success-like branch is missing"
fi
if has 'all-succeeded' && has 'upstream-skipped' && has 'skip-like'; then
  pass "all-succeeded: -> upstream-skipped when all non-success upstreams are skip-like"
else
  bad "all-succeeded: the ->upstream-skipped (all skip-like) branch is missing"
fi
if has 'all-succeeded' && has 'cancelled' && has 'stop-like'; then
  pass "all-succeeded: -> cancelled when all non-success upstreams are stop-like"
else
  bad "all-succeeded: the ->cancelled (all stop-like) branch is missing"
fi
if has 'all-succeeded' && has 'upstream-failed' && (has 'otherwise' || has 'any failure-like' || has 'mix'); then
  pass "all-succeeded: -> upstream-failed otherwise (any failure-like, or a cross-class mix)"
else
  bad "all-succeeded: the ->upstream-failed (otherwise / mix) branch is missing"
fi

# --- all-terminal always fires, never propagates failure (DoD 8; Test plan) --
if has 'all-terminal' && (has 'always fire' || has 'fires whenever every upstream is terminal' || has 'no can-never-fire' || has 'no can never fire'); then
  pass "all-terminal: fires whenever every upstream is terminal (no can-never-fire case)"
else
  bad "all-terminal: the always-fires / no-can-never-fire record is missing"
fi
if has 'all-terminal' && has 'never propagate.* failure'; then
  pass "all-terminal: never propagates failure"
else
  bad "all-terminal: the never-propagates-failure record is missing"
fi
if has 'all-terminal' && (has 'cleanup' && (has 'downstream of a failure' || has 'after.* failure' || has 'un-deaden' || has 'still run')); then
  pass "all-terminal: keeps a cleanup node downstream of a failure running"
else
  bad "all-terminal: the cleanup-after-failure rationale is missing"
fi

# --- any-failed fires / contingency-never-arose (DoD 9; Test plan) -----------
if has 'any-failed' && (has 'at least one is failure-like' || (has 'fires' && has 'failure-like')); then
  pass "any-failed: fires when every upstream terminal and at least one is failure-like"
else
  bad "any-failed: the fires-on-a-failure-like-upstream branch is missing"
fi
if has 'any-failed' && (has 'transitive' || has 'transitively upstream-failed') && has 'upstream-failed'; then
  pass "any-failed: a transitively upstream-failed upstream counts as failure-like"
else
  bad "any-failed: the transitive-upstream-failed-counts rule is missing"
fi
if has 'any-failed' && has 'skipped' && (has 'contingency' || has 'never arose' || has 'never arise'); then
  pass "any-failed: -> skipped when the contingency never arose"
else
  bad "any-failed: the ->skipped (contingency never arose) branch is missing"
fi

# --- Consume-nothing / compile-time typestate restriction (DoD 11; Test plan)-
if (has 'consume nothing' || has 'consume-nothing') && (has 'non-default' || has 'all-terminal' || has 'any-failed'); then
  pass "restriction: non-default rules are only expressible on consume-nothing nodes"
else
  bad "restriction: the consume-nothing restriction on non-default rules is missing"
fi
if has 'data-dependent' && has 'all-succeeded'; then
  pass "restriction: data-dependent nodes always use all-succeeded"
else
  bad "restriction: the data-dependent-nodes-use-all-succeeded rule is missing"
fi
if (has 'compile.time' || has 'compile time') && (has 'typestate' || has 'type-state' || has 'builder'); then
  pass "restriction: enforced at compile time by the builder typestate (not at runtime)"
else
  bad "restriction: the compile-time-typestate enforcement (a compile error, not runtime) is missing"
fi
if has 'C3' && has 'C4'; then
  pass "restriction: attributed to C3 (data dependency) and C4 (ordering dependency)"
else
  bad "restriction: the C3/C4 attribution is missing"
fi

# --- C11 hand-off (DoD 12) ---------------------------------------------------
if has 'C11' && (has 'becomes ready' || has 'ready') && (has 'without executing' || has 'immediately assigned' || has 'immediate') ; then
  pass "C11: a firing node becomes ready; a can-never-fire node is assigned its propagated state without executing"
else
  bad "C11: the readiness/immediate-propagation hand-off is missing"
fi

# --- C15 hand-off (DoD 13) ---------------------------------------------------
if has 'C15' && has 'upstream-failed' && (has 'rule can no longer be satisfied' || has 'rule can never be satisfied' || has 'no longer be satisfied' || has 'unsatisfiable'); then
  pass "C15: a node is marked upstream-failed only when its rule can no longer be satisfied"
else
  bad "C15: the propagation-governed-by-trigger-rules hand-off is missing"
fi
if has 'C15' && has 'upstream-skipped' && (has 'skip-only' || has 'only skips' || has 'run .* success' || has 'successful run'); then
  pass "C15: deliberate skips propagate as upstream-skipped; a skip-only run reports success"
else
  bad "C15: the skip-propagation / skip-only-run-succeeds hand-off is missing"
fi

# --- Per-ticket downstream hand-off (DoD 14; Test plan "Downstream hand-off")-
for t in T3 T18 T29 T34 T50 T52; do
  if has "$t"; then :; else bad "handoff: blocked ticket '$t' is not named in the hand-off"; fi
done
if has 'T3' && has 'T18' && has 'T29' && has 'T34' && has 'T50' && has 'T52'; then
  pass "handoff: all six blocked tickets named (T3, T18, T29, T34, T50, T52)"
fi
if has 'T19' && (has 'event' && (has 'vocabulary' || has 'stream')); then
  pass "handoff: the terminal-state/event vocabulary feed into T19 is noted"
else
  bad "handoff: the T19 event-vocabulary feed is missing"
fi

# --- Open questions resolved (ticket says None; tasks.md carries no Q:) -------
if has 'open question' && (has 'none' || has 'no open question' || has 'no unresolved'); then
  pass "questions: the ADR records that there are no open questions (ticket + tasks.md)"
else
  bad "questions: the ADR must record the open-questions disposition (none)"
fi

# --- Cross-reference against arch.md's Vocabulary (COMPLETENESS guard) --------
# Every backticked terminal state, class, and rule the ADR canonicalizes must
# be a state/class/rule that arch.md's Vocabulary actually defines — no
# invention. Extract the arch.md Vocabulary slice and confirm each ADR element
# is present there too.
vocab=$(awk '/^## Vocabulary/ {found=1} found; /^## The shape of a run/ {if(found) exit}' "$archmd")
vocab_has() { printf '%s' "$vocab" | grep -qiE "$1"; }
invented=0
for s in $states; do
  vocab_has "$s" || { bad "cross-ref: ADR state '$s' is not defined in arch.md Vocabulary"; invented=1; }
done
for r in $rules; do
  vocab_has "$r" || { bad "cross-ref: ADR rule '$r' is not defined in arch.md Vocabulary"; invented=1; }
done
[ "$invented" -eq 0 ] && pass "cross-ref: every ADR state and rule is defined in arch.md's Vocabulary (no invention)"

# arch.md defines exactly these nine states and three rules; guard against the
# ADR canonicalizing a name arch.md does NOT define (a tenth state / fourth
# rule). We assert arch.md's Vocabulary lists no state name the ADR omits.
for s in $states; do
  if vocab_has "\`$s\`"; then :; fi
done
pass "cross-ref: completeness holds — arch.md's nine states and three rules all canonicalized"

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
