#!/usr/bin/env bash
# Error-taxonomy ADR acceptance checks for ticket 016 (T3).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done (docs/implementation/016-T3-error-taxonomy-adr.md). T3 is a
# DECISION ticket whose durable deliverable is an ADR that settles dagr's error
# taxonomy: the three-valued task-facing error enum (C1), the five-classification
# framework-internal runner outcome taxonomy (C14), their superset relationship,
# and the total mapping from runner outcome onto the normative terminal states
# and state classes fixed by T0.4 (010). Its "tests" are documentary
# completeness checks against the recorded ADR ("the ADR states X"), backed by
# throwaway-prototype evidence the ADR quotes.
#
# The load-bearing assertion is that the taxonomy is TOTAL and CONSISTENT with
# T0.4: every runner classification maps to exactly one terminal state, every
# runner-minted terminal state carries T0.4's state class, and all ten* terminal
# states are attributed to an owner (runner-minted vs propagation/cancellation/
# resume). (*nine terminal states plus the runner-minted `succeeded` success
# path — the ticket's "all ten terminal states" phrasing counts `succeeded`
# explicitly; T0.4's enum is nine states, of which the runner mints
# `succeeded`/`failed`/`timed-out`/`skipped`.)
#
# Authored FIRST as the acceptance gate, it fails on the ticket file as it stands
# before the ADR is written into it (the content assertions are scoped to the
# embedded ADR body only), and passes once the ADR records every element the
# ticket is chartered to lock. It implements NO error types and NO classification
# function — that is T9/C14; what survives here is the ADR's record of the
# taxonomy their code will later realize.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/016-T3-error-taxonomy-adr.md"
t04="docs/implementation/010-T0.4-trigger-rule-and-state-tables.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.3/T0.4/T0.7 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already names every enum, outcome,
# and state — so a whole-file grep would pass content checks the ADR itself has
# not yet made. We therefore scope every content assertion to the ADR BODY only:
# the slice of the file from the first 'ADR:' heading line to EOF. The ticket's
# own H1 title ('# 016 · T3 — …') is deliberately NOT matched (no 'ADR:' with a
# colon), so before the embedded ADR is authored the slice is empty and every
# content check fails — exactly the tests-first behaviour we want.
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

# --- Task-facing enum: EXACTLY three variants (DoD 1, DoD 2; Test plan
# "Three-valued author surface holds") ----------------------------------------
if (has 'task-facing' || has 'task facing' || has 'author-facing' || has 'author-returnable') \
   && has 'three' && (has 'enum' || has 'variant'); then
  pass "task-enum: the task-facing enum is fixed at exactly three variants"
else
  bad "task-enum: the ADR must fix the task-facing enum at exactly three variants"
fi
if (has 'retry-eligible' || has 'retryable') && (has 'permanent') \
   && (has 'deliberate skip' || has 'originated skip' || (has 'skip' && has 'deliberate')); then
  pass "task-enum: the three variants are retry-eligible failure, permanent failure, deliberate skip"
else
  bad "task-enum: the three variants (retry-eligible / permanent / deliberate skip) are not all named"
fi
if (has 'stays three-valued' || has 'three-valued permanently' || has 'permanently three-valued' \
    || (has 'three-valued' && has 'permanent')) ; then
  pass "task-enum: the enum is recorded to stay three-valued permanently"
else
  bad "task-enum: the 'stays three-valued permanently' statement is missing"
fi
if (has 'timeout' && has 'panic') \
   && (has 'not author-returnable' || has 'not author.returnable' || has 'cannot return' \
       || has 'no author-facing' || has 'no author facing' || has 'never.*author return'); then
  pass "task-enum: why timeout and panic are NOT author-returnable is recorded"
else
  bad "task-enum: the rationale that timeout and panic are not author-returnable is missing"
fi

# --- Runner outcome taxonomy: FIVE classifications (DoD 3) --------------------
if (has 'runner' || has 'framework-internal' || has 'framework internal') && has 'five' \
   && (has 'classification' || has 'outcome' || has 'taxonomy'); then
  pass "runner-tax: the runner outcome taxonomy has five classifications"
else
  bad "runner-tax: the five-classification runner outcome taxonomy is not stated"
fi
if has 'timeout' && has 'panic' && (has 'retry-eligible' || has 'retryable') \
   && has 'permanent' && has 'skip'; then
  pass "runner-tax: the five classifications name retry-eligible/permanent/skip + timeout + panic"
else
  bad "runner-tax: the five runner classifications are not all named"
fi

# --- Superset relationship + origins (DoD 4; Test plan "Superset mapping") ----
if has 'superset' && (has 'strictly contain' || has 'strict superset' || has 'contains the task-facing' \
                       || has 'contains the task facing'); then
  pass "superset: the runner taxonomy is recorded as a strict superset of the task-facing enum"
else
  bad "superset: the strict-superset relationship is missing"
fi
if has 'timeout' && (has 'per-attempt clock' || has 'per attempt clock' || has 'attempt clock' \
                      || has 'the clock'); then
  pass "superset: timeout originates from the per-attempt clock"
else
  bad "superset: the timeout-from-the-clock origin is missing"
fi
if has 'panic' && (has 'catch boundary' || has 'catch-boundary' || has 'catch_unwind' || has 'catch unwind'); then
  pass "superset: panic originates from the catch boundary"
else
  bad "superset: the panic-from-the-catch-boundary origin is missing"
fi
if (has 'never.*author return' || has 'never from an author' || has 'not.*author return' \
    || has 'never an author return' || has 'no author return'); then
  pass "superset: the two extra classifications never originate from an author return"
else
  bad "superset: the never-from-an-author-return statement is missing"
fi

# --- Runner-outcome -> terminal-state mapping (DoD 5; Test plan
# "Superset mapping is total and unambiguous") --------------------------------
if (has 'exhaust' && has 'failed') && (has 'permanent' && has 'failed'); then
  pass "map: exhausted-retry and permanent both -> failed"
else
  bad "map: the exhausted-retry+permanent -> failed mapping is missing"
fi
if has 'panic' && has 'failed'; then
  pass "map: panic -> failed"
else
  bad "map: the panic -> failed mapping is missing"
fi
if has 'timeout' && has 'timed-out'; then
  pass "map: timeout -> timed-out"
else
  bad "map: the timeout -> timed-out mapping is missing"
fi
if (has 'originated skip' || (has 'skip' && has 'originated')) && has 'skipped'; then
  pass "map: originated skip -> skipped"
else
  bad "map: the originated-skip -> skipped mapping is missing"
fi
if has 'total' && (has 'mapping' || has 'map ' || has 'unambiguous' || has 'exactly one terminal state'); then
  pass "map: the runner-outcome -> terminal-state mapping is stated total/unambiguous"
else
  bad "map: the total-and-unambiguous statement for the mapping is missing"
fi

# --- Timeout retry-eligibility default (DoD 6; Test plan "Timeout
# retry-eligibility default is recorded") -------------------------------------
if has 'timeout' && (has 'retry-eligible by default' || has 'retryable by default' \
                      || (has 'retry-eligible' && has 'default')) \
   && (has 'retry budget' || has 'retry-budget' || has 'node.*budget'); then
  pass "timeout-retry: timeout is retry-eligible by default, subject to the node's retry budget"
else
  bad "timeout-retry: the timeout-retry-eligible-by-default record is missing"
fi
if has 'timeout' && (has 'without adding an author-facing' || has 'no author-facing variant' \
                      || has 'not.*author-facing variant' || has 'without.*author.facing'); then
  pass "timeout-retry: recorded without adding an author-facing variant"
else
  bad "timeout-retry: the 'no author-facing variant' clause is missing"
fi

# --- Retry-eligibility is a runner concern, not a state (DoD 7; Test plan
# "Retry-eligibility is a runner concern, not a state") -----------------------
if (has 'retry-eligibility' || has 'retry eligibility' || has 'retry-eligible') \
   && (has 'not.*a terminal state' || has 'not itself a terminal state' || has 'not a state' \
       || has 'governs.*scheduling' || has 'attempt scheduling'); then
  pass "retry-eligibility: governs attempt scheduling only, is not itself a terminal state"
else
  bad "retry-eligibility: the 'runner concern, not a state' record is missing"
fi
if (has 'exhaust' && has 'same') && has 'failed' && has 'permanent'; then
  pass "retry-eligibility: once exhausted resolves to the SAME failed state as permanent"
else
  bad "retry-eligibility: the exhausted-resolves-to-same-failed-as-permanent record is missing"
fi

# --- Panic handling (DoD 8; Test plan "Panic never unwinds and always fails") -
if has 'panic' && (has 'caught' || has 'catch') && (has 'not unwound' || has 'never unwind' \
                                                     || has 'not.*unwind' || has 'without unwinding'); then
  pass "panic: caught, not unwound"
else
  bad "panic: the caught-not-unwound record is missing"
fi
if has 'panic' && (has 'permanent failure') && has 'failed'; then
  pass "panic: converted to permanent failure, resolving to failed"
else
  bad "panic: the panic->permanent-failure->failed record is missing"
fi
if has 'panic' && (has 'own node' || has 'its own node' || has 'attributed to its' \
                    || has 'panicking node only' || has 'only.*node'); then
  pass "panic: attributed to its own node only"
else
  bad "panic: the attributed-to-its-own-node-only record is missing"
fi

# --- State-class assignment matches T0.4 (DoD 9; Test plan
# "State-class assignment matches T0.4") --------------------------------------
if has 'failed' && has 'timed-out' && has 'failure-like'; then
  pass "class: failed and timed-out are failure-like"
else
  bad "class: the failed/timed-out -> failure-like assignment is missing"
fi
if has 'skipped' && has 'skip-like'; then
  pass "class: skipped is skip-like"
else
  bad "class: the skipped -> skip-like assignment is missing"
fi
if has 'succeeded' && has 'success-like'; then
  pass "class: succeeded (the success path) is success-like"
else
  bad "class: the succeeded -> success-like assignment is missing"
fi
if (has 'identical to T0.4' || has 'identical to.*T0.4' || has 'matches T0.4' || has 'per T0.4' \
    || has 'consistent with T0.4' || has 'same as T0.4') && (has 'class' || has 'partition'); then
  pass "class: the state-class assignment is stated identical to T0.4's partition"
else
  bad "class: the 'identical to T0.4's classes' statement is missing"
fi

# --- All terminal states accounted for + owner attribution (DoD 10; Test plan
# "Terminal-state table is covered") ------------------------------------------
states="succeeded failed timed-out skipped upstream-skipped upstream-failed cancelled abandoned satisfied-from-prior"
missing_state=0
for s in $states; do
  if has "\`$s\`"; then :; else bad "coverage: terminal state '$s' is not accounted for in the ADR"; missing_state=1; fi
done
[ "$missing_state" -eq 0 ] && pass "coverage: all terminal states are accounted for (succeeded…satisfied-from-prior)"

# Runner-minted set attributed to C14.
if has 'C14' && has 'succeeded' && has 'failed' && has 'timed-out' && has 'skipped'; then
  pass "coverage: the runner-minted set (succeeded/failed/timed-out/skipped) is attributed to C14"
else
  bad "coverage: the runner-minted set attribution to C14 is missing"
fi
# Non-runner-minted states attributed to their owners.
if (has 'upstream-skipped' && has 'upstream-failed') && (has 'C11' && has 'C15'); then
  pass "coverage: upstream-skipped/upstream-failed attributed to propagation (C11/C15)"
else
  bad "coverage: the upstream-* -> C11/C15 attribution is missing"
fi
if has 'cancelled' && has 'abandoned' && has 'C16'; then
  pass "coverage: cancelled/abandoned attributed to cancellation (C16)"
else
  bad "coverage: the cancelled/abandoned -> C16 attribution is missing"
fi
if has 'satisfied-from-prior' && has 'C27'; then
  pass "coverage: satisfied-from-prior attributed to resume (C27)"
else
  bad "coverage: the satisfied-from-prior -> C27 attribution is missing"
fi

# --- abandoned is NOT a runner classification (DoD 11; Test plan
# "Abandoned is not a runner classification") ---------------------------------
if has 'abandoned' && (has 'not a runner classification' || has 'not a runner outcome' \
                        || has 'excluded from the classification' || has 'not.*runner-minted' \
                        || has 'not a runner-minted'); then
  pass "abandoned: not a runner classification"
else
  bad "abandoned: the 'not a runner classification' record is missing"
fi
if has 'abandoned' && (has 'never.*second terminal state' || has 'never a second' \
                        || has 'stays.*timed-out' || has 'stays `timed-out`') ; then
  pass "abandoned: never a second terminal state after timed-out"
else
  bad "abandoned: the 'never a second state after timed-out' record is missing"
fi

# --- satisfied-from-prior not produced by classification (DoD 12) ------------
if has 'satisfied-from-prior' && (has 'not produced by the classification' \
    || has 'not.*classification path' || has 'not a runner' || has 'resume' && has 'C27'); then
  pass "sfp: satisfied-from-prior is not produced by the classification path (C27 resume)"
else
  bad "sfp: the 'satisfied-from-prior not from classification (C27)' record is missing"
fi

# --- Decision drivers, rejected alternatives, consequences, naming for T9
# (DoD 13; Test plan "ADR structure is complete") -----------------------------
if has 'decision driver' || has 'drivers'; then
  pass "drivers: decision drivers are recorded"
else
  bad "drivers: the decision-drivers section is missing"
fi
if (has 'single.*enum' || has 'unified.*enum' || has 'one enum') \
   && (has 'two-valued' || has 'two valued') \
   && (has 'author-returnable timeout' || has 'author.returnable timeout' \
       || (has 'author' && has 'timeout'))  ; then
  pass "rejected: the three named alternatives (unified enum, two-valued, author-returnable timeout) appear"
else
  bad "rejected: not all three named rejected alternatives are recorded"
fi
if (has 'T9' && (has 'naming' || has 'placement' || has 'guidance')); then
  pass "handoff: naming/placement guidance for T9 is recorded"
else
  bad "handoff: the T9 naming/placement guidance is missing"
fi

# --- T9 governance linkage (DoD 14) ------------------------------------------
if has 'T9' && (has 'governing' || has 'governs' || has 'depends on' || has 'consumes'); then
  pass "linkage: the ADR is named as the governing decision T9 consumes"
else
  bad "linkage: the T9-governing-decision linkage is missing"
fi

# --- C1 / C14 / downstream consumers named (Test plan "ADR structure is
# complete": names C1, C14, C11/C15) ------------------------------------------
if has 'C1' && has 'C14'; then
  pass "components: the ADR names the components it constrains (C1, C14)"
else
  bad "components: C1/C14 are not both named"
fi
if has 'C11' && has 'C15'; then
  pass "components: the downstream consumers C11/C15 are named"
else
  bad "components: the downstream consumers C11/C15 are not named"
fi

# --- Open questions resolved (ticket says None; tasks.md T3 carries no Q:) -----
if has 'open question' && (has 'none' || has 'no open question' || has 'no unresolved'); then
  pass "questions: the ADR records that there are no open questions (ticket + tasks.md)"
else
  bad "questions: the ADR must record the open-questions disposition (none)"
fi

# --- Cross-reference against T0.4 (010) — consistency, no invention -----------
# Every terminal state and state class the ADR attributes must be one T0.4's
# reference tables actually define. Extract T0.4's ADR body and confirm.
if [ -f "$t04" ]; then
  t04_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$t04")
  t04_has() { printf '%s' "$t04_body" | grep -qiE "$1"; }
  invented=0
  for s in $states; do
    t04_has "\`$s\`" || { bad "cross-ref: ADR state '$s' is not defined in T0.4's tables"; invented=1; }
  done
  for c in success-like skip-like failure-like stop-like; do
    if has "$c"; then
      t04_has "$c" || { bad "cross-ref: ADR class '$c' is not defined in T0.4's tables"; invented=1; }
    fi
  done
  [ "$invented" -eq 0 ] && pass "cross-ref: every ADR state and class is defined in T0.4 (no invention, consistent with 010)"
else
  bad "cross-ref: T0.4 dependency file missing ($t04)"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
