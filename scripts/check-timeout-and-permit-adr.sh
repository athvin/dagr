#!/usr/bin/env bash
# Timeout-abandonment & permit-accounting ADR acceptance checks for ticket 009
# (T0.3).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done (docs/implementation/009-T0.3-timeout-and-permit-accounting-spike.md).
# T0.3 is a DECISION (SPIKE) ticket whose durable deliverable is an ADR; its
# Test-plan scenarios are throwaway-prototype evidence whose *result* the ADR
# records, plus a decision-record completeness check ("the ADR states X").
#
# This script asserts the documentary record: that the ADR — embedded in the
# ticket file at the path the DoD names, matching the T1/T2/T0.2/T0.7 precedent
# of keeping each ADR inside its own ticket file — contains the required
# sections and the required decisions, in the exact normative vocabulary of
# arch.md (C12/C14, the terminal-state table, C16/C19/C23 scope notes). Authored
# FIRST as the acceptance gate, it fails on the ticket file as it stands before
# the ADR is written into it, and passes once the ADR records every decision the
# ticket is chartered to lock.
#
# It does NOT re-run the prototype: the prototype is throwaway spike code, built
# outside the shipping crates and deleted before the PR per the ticket-conventions
# decision(spike) rule; what survives is the ADR's record of what it proved,
# quoted as EVIDENCE lines, which is what this script checks for.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/009-T0.3-timeout-and-permit-accounting-spike.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.7 precedent: '# ADR: <title>'). The ticket prose
# above it (Objective, Test plan, DoD) already mentions timeout, zombie, permit,
# etc. — so a whole-file grep would pass content checks the ADR itself has not
# yet made. We therefore scope every content assertion to the ADR BODY only: the
# slice of the file from the first 'ADR:' heading line to EOF. The ticket's own
# H1 title ('# … spike') is deliberately NOT matched (no 'ADR:' with a colon),
# so before the embedded ADR is authored the slice is empty and every content
# check fails — exactly the tests-first behaviour we want.
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

# --- Await-bound: future dropped, permit released IMMEDIATELY (DoD 2; Test
# plan "Await-bound timeout releases the permit immediately") ------------------
if has 'await.bound' && (has 'future.drop' || has 'drop.*future' || has 'dropped'); then
  pass "await-bound: future-drop cancellation recorded"
else
  bad "await-bound: future-drop on timeout is missing"
fi
if has 'released immediately' || has 'immediate.*release' || has 'permit releases immediately'; then
  pass "await-bound: immediate permit release recorded"
else
  bad "await-bound: immediate-permit-release is missing"
fi

# --- Blocking/compute: MARKED timed out immediately, permit HELD until the
# closure returns (DoD 3; Test plan "Blocking timeout …", "Compute … identical")
if has 'blocking' && has 'compute'; then
  pass "blocking/compute: both classes named for the abandoned-but-running path"
else
  bad "blocking/compute: the ADR must name both the blocking and compute classes"
fi
if has 'cannot be killed'; then
  pass "blocking/compute: 'a thread cannot be killed' honesty recorded"
else
  bad "blocking/compute: the unkillable-thread premise is missing"
fi
if has 'marked' && (has 'timed.out immediately' || has 'immediately.*timed.out' || has 'fate.*decided' || has 'fate decided'); then
  pass "blocking/compute: marked-timed-out-immediately (fate decided) recorded"
else
  bad "blocking/compute: the mark-immediately/fate-decided rule is missing"
fi
if has 'abandoned.but.running'; then
  pass "blocking/compute: abandoned-but-running work named"
else
  bad "blocking/compute: 'abandoned-but-running' is missing"
fi
if has 'held until' && has 'returns'; then
  pass "blocking/compute: permit held until the closure actually returns"
else
  bad "blocking/compute: permit-held-until-closure-returns is missing"
fi

# --- Capacity invariant counts zombies (DoD 4; Test plan "capacity invariant
# never lies", C12 acceptance) ------------------------------------------------
if has 'never exceed' && (has 'capacity' || has 'pool'); then
  pass "capacity: counted cost (incl. zombies) never exceeds pool capacity"
else
  bad "capacity: the never-exceed-capacity invariant is missing"
fi
if has 'zombie' && has 'counted'; then
  pass "capacity: the invariant explicitly counts zombies"
else
  bad "capacity: the invariant must state that it counts abandoned-but-running (zombie) cost"
fi
if has 'ledger'; then
  pass "capacity: the ledger mechanism that guarantees the invariant is named"
else
  bad "capacity: the permit-ledger mechanism is not named"
fi

# --- Ledger observes the return (DoD 5) --------------------------------------
if has 'join' || has 'handoff' || has 'hand.off' || has 'observes.*return' || has 'closure return'; then
  pass "return: how the ledger observes the closure's return is named (join/handoff)"
else
  bad "return: the join/handoff mechanism that observes the return is missing"
fi

# --- Deferred retry (DoD 6; Test plan "Retry is deferred"; C1 exclusivity) ---
if has 'retry' && (has 'deferred' || has 'defer')  ; then
  pass "retry: deferred-until-previous-closure-returns recorded"
else
  bad "retry: deferred-retry rule is missing"
fi
if has 'exclusiv' || has 'concurrent.*zombie' || has 'its own zombie'; then
  pass "retry: preserves C1 exclusivity (never concurrent with its own zombie)"
else
  bad "retry: the C1-exclusivity rationale is missing"
fi

# --- Late-result barrier (DoD 7; Test plan "A late result … discarded"; C14) --
if has 'late.result' || has 'late result'; then
  pass "late-result: the late-result barrier is named"
else
  bad "late-result: the late-result barrier is missing"
fi
if has 'never fill' && (has 'slot' || has 'scratch'); then
  pass "late-result: never fills a slot / never writes scratch recorded"
else
  bad "late-result: the never-fill-slot / never-write-scratch rule is missing"
fi

# --- Terminal state decided exactly once (DoD 8; Test plan "Terminal state …
# exactly once"; the normative terminal-state table) --------------------------
if has 'exactly once' || has 'decided.*once'; then
  pass "terminal: terminal state decided exactly once recorded"
else
  bad "terminal: the decided-exactly-once rule is missing"
fi
if has 'stays.*timed.out' || has 'timed.out.*stays' || has 'remains.*timed.out'; then
  pass "terminal: a blocking timeout is and stays 'timed-out'"
else
  bad "terminal: the 'timed-out stays timed-out' rule is missing"
fi
if has 'abandoned' && (has 'cancellation path' || has 'C16'); then
  pass "terminal: 'abandoned' arises only on the cancellation path (C16), not after timeout"
else
  bad "terminal: the 'abandoned only on the cancellation path' distinction is missing"
fi

# --- Zombie-cost reporting shape (DoD 9; Test plan "Zombie cost is reportable")
if has 'zombie.cost' || (has 'count of' && has 'zombie') || has 'how many zombies'; then
  pass "report: zombie-cost reporting shape (count + per-pool cost) recorded"
else
  bad "report: the zombie-cost reporting shape is missing"
fi
if has 'C19' && has 'C23'; then
  pass "report: the shape feeds C19 (zombie-at-exit event) and C23 (declared-vs-measured)"
else
  bad "report: the C19/C23 downstream feed of the report shape is missing"
fi

# --- Named seams for the three consumers (DoD 10) ----------------------------
if has 'T21' && has 'T31' && has 'T37'; then
  pass "seams: the three consumer tickets (T21, T31, T37) are named"
else
  bad "seams: the ADR must name its consumers T21, T31, T37"
fi
if has 'seam'; then
  pass "seams: the ADR explicitly names the seams the consumers implement against"
else
  bad "seams: the ADR must name the concrete seams (permit-ledger ops, timeout-classification, capacity assertions)"
fi

# --- Throwaway prototype evidence + disposition (DoD 11, 12) ------------------
if has 'prototype' || has 'spike'; then
  pass "evidence: the ADR references the throwaway prototype"
else
  bad "evidence: the ADR must reference the throwaway prototype"
fi
if has 'EVIDENCE'; then
  pass "evidence: the ADR quotes the prototype's EVIDENCE lines"
else
  bad "evidence: the ADR must quote the prototype's EVIDENCE (real runtime behaviour)"
fi
if has 'tokio'; then
  pass "evidence: the prototype ran against the real tokio runtime"
else
  bad "evidence: the ADR must record that the prototype ran on the tokio runtime"
fi
if has 'deleted' || has 'quarantin' || has 'removed' || has 'disposed'; then
  pass "evidence: the ADR records the prototype's disposition (deleted/quarantined)"
else
  bad "evidence: the spike-disposition record is missing"
fi

# --- Scope note: what this ADR deliberately does NOT decide (DoD 13) ----------
if has 'C16' && has 'C19' && has 'C23'; then
  pass "scope: the scope note names C16 (cancellation-path abandoned), C19/C23 (wiring)"
else
  bad "scope: the deliberately-not-decided scope note (C16, C19/C23) is missing"
fi
if has 'T32' && (has 'container' || has 'limit detection' || has 'sizing')  ; then
  pass "scope: container-limit-detection / pool-sizing deferred to T32"
else
  bad "scope: the T32 (container limit detection / sizing) scope-out is missing"
fi

# --- No new dependency leaked into shipping crates ---------------------------
# T0.3 is a doc-only decision: the spike depends on tokio/rayon OUTSIDE the
# workspace, but the shipping crates must NOT gain a dependency (that is
# T21/T31/T33). Guard against an accidental dependency addition that would
# change Cargo.lock and give cargo-audit new surface.
if grep -qiE '^[[:space:]]*(tokio|rayon|tokio-util)[[:space:]]*=' crates/*/Cargo.toml 2>/dev/null; then
  bad "scope: no runtime dependency may be wired into a shipping crate here (that is T21/T31/T33)"
else
  pass "scope: no tokio/rayon/tokio-util dependency wired into a shipping crate (doc-only decision)"
fi

# --- No spike code leaked into the workspace ---------------------------------
if find crates -type d -name '*spike*' 2>/dev/null | grep -q .; then
  bad "scope: a spike directory leaked into crates/"
else
  pass "scope: no spike directory under crates/"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
