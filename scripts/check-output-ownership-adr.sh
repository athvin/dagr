#!/usr/bin/env bash
# Output-ownership ADR acceptance checks for ticket 008 (T0.2).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/008-T0.2-output-ownership-adr-spike.md, section "Test
# plan"). T0.2 is a DECISION (SPIKE) ticket whose deliverable is an ADR; its
# Test plan scenarios are throwaway-prototype evidence whose *result* the ADR
# records, plus documentary record checks ("the partition lists X as a compile
# error", "the rejected-alternatives section names Y").
#
# This script asserts the documentary record: that the ADR — embedded in the
# ticket file at the path the DoD names, matching the T1/T2/T0.6/T3/T4/T0.7
# precedent of keeping each ADR inside its own ticket file — contains the
# required sections and the required decisions, in the exact normative
# vocabulary of arch.md (C1/C3/C10, Vocabulary). Authored FIRST as the
# acceptance gate, it fails on the ticket file as it stands before the ADR is
# written into it, and passes once the ADR records every decision the ticket is
# chartered to lock.
#
# It does NOT re-run the prototype: the prototype is throwaway spike code, built
# outside the shipping crates (under /tmp) and deleted before the PR per the
# ticket-conventions decision(spike) rule; what survives is the ADR's record of
# what each prototype proved (the quoted EVIDENCE and compiler-error lines),
# which is what this script checks.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/008-T0.2-output-ownership-adr-spike.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.6/T3/T4/T0.7 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already mentions clone-on-read,
# Send, Sync, etc. — so a whole-file grep would pass content checks the ADR
# itself has not yet made. We therefore scope every content assertion to the ADR
# BODY only: the slice of the file from the first 'ADR:' heading line to EOF.
# The ticket's own H1 title ('# … spike') is deliberately NOT matched (no
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

# --- The three-mode model is the locked decision (DoD line 1) ----------------
if has 'sole.consumer.owns' || has 'sole owner'; then
  pass "model: sole-consumer-owns mode recorded"
else
  bad "model: the sole-consumer-owns mode is missing"
fi
if has 'shared.read' || has 'shared access'; then
  pass "model: multi-consumer-shared-read mode recorded"
else
  bad "model: the multi-consumer-shared-read mode is missing"
fi
if has 'clone.on.read'; then
  pass "model: per-edge clone-on-read opt-in mode recorded"
else
  bad "model: the per-edge clone-on-read opt-in mode is missing"
fi

# --- Delivery representation + sole-owns/shared evidence (DoD line 2) ---------
if has 'EVIDENCE sole-owns'; then
  pass "evidence: sole-consumer-owns delivery (by move, slot consumed) quoted"
else
  bad "evidence: the sole-owns delivery evidence line is missing"
fi
if has 'EVIDENCE shared-read' || has 'EVIDENCE concurrent-shared'; then
  pass "evidence: multi-consumer shared-read delivery quoted"
else
  bad "evidence: the shared-read delivery evidence line is missing"
fi

# --- Non-Clone survives owned+shared; clone-on-read rejected (DoD line 3) -----
if has 'EVIDENCE nonclone-modes'; then
  pass "evidence: non-Clone output through owned AND shared quoted"
else
  bad "evidence: the non-Clone-through-both-modes evidence line is missing"
fi
# The rejection is BOTH a static compile error AND a dynamic curated assembly
# error. The compile error must be the real E0599 (method requires Clone).
if has 'E0599' && has 'derive\(?clone'; then
  pass "evidence: static clone-on-read on non-Clone is the real E0599 compile error"
else
  bad "evidence: the E0599 (missing-Clone) compile error for static clone-on-read is missing"
fi
if has 'EVIDENCE dyn-clone-on-read' || has 'curated assembly error'; then
  pass "evidence: dynamic clone-on-read yields a curated assembly error naming the type"
else
  bad "evidence: the dynamic clone-on-read curated-assembly-error path is missing"
fi

# --- Compile-time / assembly-time PARTITION (DoD line 4) ---------------------
# The partition must exist as a table/record and classify each violation.
if has 'partition'; then
  pass "partition: the compile-time/assembly-time partition is named"
else
  bad "partition: the compile-time/assembly-time partition record is missing"
fi
# Compile errors: type mismatch, missing Clone on a static clone-on-read edge,
# missing Send / Sync.
if has 'type mismatch'; then
  pass "partition: type mismatch listed as a compile error"
else
  bad "partition: type mismatch not listed as a compile error"
fi
if has 'send' && has 'sync'; then
  pass "partition: missing Send/Sync listed as compile errors"
else
  bad "partition: missing Send/Sync not listed as compile errors"
fi
# Assembly errors: owned-on-multiply-consumed (naming both), second-consumer-on-
# sole-owner, owned-into-retrying-without-clone.
if has 'naming both' || has 'names both' || has 'both consumers'; then
  pass "partition: owned-on-multiply-consumed (naming both) listed as an assembly error"
else
  bad "partition: owned-on-multiply-consumed naming-both is missing"
fi
if has 'second consumer'; then
  pass "partition: second-consumer-on-sole-owner listed as an assembly error"
else
  bad "partition: second-consumer-on-sole-owner is missing"
fi
if has 'retr' && has 'clone.on.read'; then
  pass "partition: owned-into-retrying-node-without-clone-on-read listed as an assembly error"
else
  bad "partition: owned-into-retrying-without-clone-on-read is missing"
fi

# --- Owned-on-shared assembly error names both (DoD line 5) ------------------
if has 'EVIDENCE owned-on-shared'; then
  pass "evidence: owned demand on a multiply-consumed value fails assembly, names both"
else
  bad "evidence: the owned-on-shared (names-both) evidence line is missing"
fi
if has 'EVIDENCE second-consumer'; then
  pass "evidence: second consumer against a sole-owner value fails at the new registration"
else
  bad "evidence: the second-consumer registration-rejection evidence line is missing"
fi

# --- Retry evidence (DoD line 6) --------------------------------------------
if has 'EVIDENCE owned-into-retry'; then
  pass "evidence: owned edge into a retrying node is rejected without clone-on-read"
else
  bad "evidence: the owned-into-retrying-node rejection evidence line is missing"
fi
if has 'EVIDENCE shared-retry'; then
  pass "evidence: shared-access consumer finds its input intact on retry"
else
  bad "evidence: the shared-access-retry-finds-value-intact evidence line is missing"
fi

# --- Clone-on-read into a retrying node, fresh per attempt (DoD line 7) ------
if has 'EVIDENCE clone-on-read'; then
  pass "evidence: clone-on-read edge into a retrying node passes and delivers a fresh value"
else
  bad "evidence: the clone-on-read-fresh-per-attempt evidence line is missing"
fi

# --- Author-visible bounds fixed + first-hour error (DoD line 8) -------------
if has 'send .{0,4}. .?static' || has 'send \+ .?static' || has "send \\+ 'static"; then
  pass "bounds: task values Send + 'static recorded"
else
  bad "bounds: the task Send + 'static bound is missing"
fi
if has "send \\+ sync \\+ 'static" || has 'send . sync . .?static'; then
  pass "bounds: output values Send + Sync + 'static recorded"
else
  bad "bounds: the output Send + Sync + 'static bound is missing"
fi
if has '&mut self' || has 'mut self'; then
  pass "bounds: work takes &mut self (sequential attempts, no author sync) recorded"
else
  bad "bounds: the &mut self work-signature bound is missing"
fi
if has 'clone.on.read' && has 'clone'; then
  pass "bounds: clone-on-read edges additionally require Clone recorded"
else
  bad "bounds: the clone-on-read-requires-Clone bound is missing"
fi
# The worked first-hour error: capturing a non-Send value in a task, as the real
# E0277 compiler error.
if has 'E0277' && (has 'non.send' || has 'not.*send' || has 'cannot be sent'); then
  pass "bounds: the worked first-hour non-Send-capture error is quoted (real E0277)"
else
  bad "bounds: the worked non-Send-capture first-hour error (E0277) is missing"
fi

# --- Receive mode is in the signature, NOT in C3 type matching (DoD line 9) --
if has 'signature' && (has 'not .*type matching' || has 'not part of .*type' || has 'separate rail'); then
  pass "partition: receive mode is in the signature and NOT part of C3 type matching"
else
  bad "partition: the mode-in-signature-not-in-type-matching split is missing"
fi

# --- Residency / zombie-aware release SHAPE (DoD line 10) --------------------
if has 'EVIDENCE residency-shape'; then
  pass "evidence: the release-when-terminal-AND-returned shape is demonstrated"
else
  bad "evidence: the residency/zombie-aware-release shape evidence line is missing"
fi
if has 'terminal' && (has 'returned' || has 'return'); then
  pass "residency: release rule (every consumer terminal AND every closure returned) recorded"
else
  bad "residency: the terminal-AND-closure-returned release rule is missing"
fi
if has 'single.count' || has 'counted once' || has 'once against'; then
  pass "residency: single-count residency (counted once, not per consumer) recorded"
else
  bad "residency: the single-count residency rule is missing"
fi

# --- Rejected alternatives + reopen condition (DoD line 11) ------------------
# House style backticks type names ("Always `Arc`-wrap"), so tolerate any
# non-word characters between "always" and the type name.
if has 'always[^[:alnum:]]*arc' || has 'arc.wrap.everything' || has 'wrap everything'; then
  pass "rejected: the always-Arc-wrap-everything alternative is named"
else
  bad "rejected: the always-Arc alternative is missing"
fi
if has 'always[^[:alnum:]]*clone' || has 'clone.per.consumer'; then
  pass "rejected: the always-Clone-per-consumer alternative is named"
else
  bad "rejected: the always-Clone-per-consumer alternative is missing"
fi
if has 'reopen'; then
  pass "reopen: the spec-decision-reopens-if-prototype-fails condition is stated"
else
  bad "reopen: the reopen condition is missing"
fi

# --- Spike quarantine record (DoD line 12) ----------------------------------
if has 'prototype' || has 'spike'; then
  pass "evidence: the ADR references the throwaway prototype"
else
  bad "evidence: the ADR must reference the throwaway prototype"
fi
if has '/tmp' && (has 'deleted' || has 'quarantin' || has 'removed'); then
  pass "quarantine: spike built under /tmp and deleted before the PR recorded"
else
  bad "quarantine: the spike-built-in-/tmp-and-deleted record is missing"
fi

# --- Unblocks T5/T9/T11/T17/T26 (DoD line 13) -------------------------------
for tid in T5 T9 T11 T17 T26; do
  if has "\\b$tid\\b"; then
    pass "unblocks: downstream consumer $tid named (no choice left open)"
  else
    bad "unblocks: downstream consumer $tid not named"
  fi
done

# --- No spike code / dependency leaked into shipping crates ------------------
# T0.2 is a doc-only decision: the shipping crates must be UNCHANGED. Guard
# against a stray spike file under crates/* referencing the spike name.
if grep -rniE 'dagr-t0[._]2-spike|NonCloneOutput|clone-on-read spike' crates/ 2>/dev/null | grep -q .; then
  bad "scope: spike identifiers must NOT appear in any shipping crate"
else
  pass "scope: no spike code leaked into a shipping crate (doc-only decision)"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
