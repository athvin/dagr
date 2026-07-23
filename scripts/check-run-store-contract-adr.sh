#!/usr/bin/env bash
# Run-store-contract ADR acceptance checks for ticket 012 (T0.6).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/012-T0.6-run-store-contract-adr.md). T0.6 is a DECISION
# ticket whose durable deliverable is an ADR that locks the LOCAL run-store
# contract every downstream persistence/artifact consumer binds against: the
# two-operation injected sink (append a line, flush), the operator-supplied
# base-location surface (path default / library-flag / env, with precedence),
# the <base>/<pipeline>/<run-id>/ directory layout and reserved file names, the
# UUIDv7 run-identity scheme, the two write-failure paths (store-open vs mid-run
# sink failure), the flush semantics (no user-space buffering, fsync at
# end/cancel, per-event fsync delegated to the sink, ≤1 trailing partial record),
# the prune/retention semantics (nothing deleted implicitly at run end), the
# scratch placement/isolation/lifecycle contract, and the per-record header
# (run identity + schema version + gapless sequence + wall-clock + monotonic
# offset). Its "tests" are documentary completeness and internal-consistency
# checks against the recorded ADR: that every seam element the ticket is
# chartered to fix is present, in the exact normative vocabulary of arch.md
# ("The shape of a run", C18, C19, C26, C27, "Operational model"), so no seam is
# left open to T19/T24/T27/T36/T53/T54a/T58.
#
# The load-bearing assertions are COMPLETENESS (every named seam element is
# recorded) and INTERNAL CONSISTENCY (the sink is exactly two operations; the
# two failure paths are distinct; prune is the SOLE implicit-deletion path;
# scratch of a succeeded node is deleted while a non-succeeded node's is retained
# for resume copy-forward). Authored FIRST as the acceptance gate, it fails on
# the ticket file as it stands before the ADR is written into it, and passes once
# the ADR records every element the ticket is chartered to lock.
#
# This is a DOC-ONLY decision (the run-store contract is a LOCAL, file-based
# persistence boundary fully decidable from arch.md; no spike is required), so
# the script does NOT build or run any prototype and asserts no production code:
# the shipping crates and Cargo.lock are untouched, and the only committed
# artifacts are the embedded ADR and this mechanical acceptance script.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/012-T0.6-run-store-contract-adr.md"
archmd="docs/arch.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.3/T0.4/T0.5 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already names sink, base, layout,
# scratch, etc. — so a whole-file grep would pass content checks the ADR itself
# has not yet made. We therefore scope every content assertion to the ADR BODY
# only: the slice of the file from the first 'ADR:' heading line to EOF. The
# ticket's own H1 title ('# 012 · T0.6 — …') is deliberately NOT matched (no
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

# --- 1. The injected sink trait (DoD 1; Test plan sink-trait shape) ----------
# Exactly two operations — append a line, flush — a named default local-file
# sink under the run directory, an explicit injection seam, and the two per-
# operation guarantees stated separately from what a concrete sink may add.
if (has 'append' && has 'line') && has 'flush'; then
  pass "sink: the two sink operations (append a line, flush) are named"
else
  bad "sink: the two-operation sink (append a line, flush) is not stated"
fi
if (has 'exactly two' || has 'two operation' || has 'two-operation') && has 'sink'; then
  pass "sink: the sink is stated to be exactly two operations"
else
  bad "sink: the exactly-two-operations property is missing"
fi
if (has 'default' && has 'sink') && (has 'local file' || has 'local-file') && (has 'under the run' || has 'run directory' || has "run's directory"); then
  pass "sink: the default local-file sink under the run directory is named"
else
  bad "sink: the named default local-file sink under the run directory is missing"
fi
if (has 'inject' || has 'injected' || has 'injection') && (has 'seam' || has 'supply' || has 'supplied' || has 'alternative'); then
  pass "sink: an explicit injection seam for supplying an alternative is stated"
else
  bad "sink: the injection seam by which an operator/test supplies an alternative is missing"
fi
if (has 'append' && (has 'atomic' || has 'whole line' || has 'one line' || has 'line-atomic')) ; then
  pass "sink: the append operation's line-atomicity guarantee is stated"
else
  bad "sink: the append line-atomicity guarantee is missing"
fi
if has 'flush' && (has 'guarantee' || has 'no user-space buffer' || has 'written to the sink' || has 'delegat'); then
  pass "sink: the flush guarantee is stated separately from the concrete sink"
else
  bad "sink: the flush guarantee (vs the concrete sink) is missing"
fi

# --- 2. Base-location surface + precedence (DoD 2; Test plan base-location) ---
if (has 'local path' || has 'default .* path' || has 'path .* default') && (has 'flag' && has 'environment variable') && (has 'precedence' || has 'takes precedence' || has 'overrides'); then
  pass "base: a local-path default, a library flag, and an env var with a precedence order are named"
else
  bad "base: the base-location surface (default / flag / env) with a precedence order is incomplete"
fi
if (has 'reserved' && (has 'namespace' || has 'library-flag' || has 'library flag')) && (has 'never .* collide' || has 'cannot .* collide' || has 'cannot .* shadow' || has 'never .* shadow' || has 'pipeline parameter'); then
  pass "base: the flag lives in the reserved library-flag namespace so a pipeline parameter cannot shadow it (C26)"
else
  bad "base: the reserved-namespace / no-parameter-shadowing guarantee is missing"
fi

# --- 3. Directory layout + reserved names + disjointness (DoD 3; Test plan) --
if has '<base>/<pipeline>/<run-id>' || has 'base.*pipeline.*run-id' || has 'base./.pipeline./.run-id'; then
  pass "layout: the <base>/<pipeline>/<run-id>/ directory layout is specified"
else
  bad "layout: the <base>/<pipeline>/<run-id>/ directory layout is missing"
fi
if (has 'event stream' && has 'artifact') && (has 'reserved' && (has 'file name' || has 'filename' || has 'reserved name')); then
  pass "layout: reserved file names for the event stream and both artifacts are listed"
else
  bad "layout: the reserved file names for the event stream and artifacts are missing"
fi
if (has 'disjoint' || has 'never .* collide' || has 'never share a file' || has 'no file collision') && (has 'concurrent' || has 'simultaneous' || has 'two runs' || has 'same binary'); then
  pass "layout: the disjoint-directory guarantee for two concurrent same-binary runs is stated (C19)"
else
  bad "layout: the disjoint-directory guarantee for concurrent same-binary runs is missing"
fi

# --- 4. Run identity (DoD 4; Test plan run-identity) -------------------------
if has 'UUIDv7' || has 'UUID v7' || has 'UUID version 7'; then
  pass "identity: run identity is UUIDv7"
else
  bad "identity: the UUIDv7 run-identity choice is missing"
fi
if (has 'operator-overridable' || has 'operator overridable' || has 'operator-supplied' || has 'override') && (has 'honored verbatim' || has 'honoured verbatim' || has 'verbatim' || has 'used verbatim'); then
  pass "identity: an operator-supplied id is honored verbatim"
else
  bad "identity: the operator-overridable / honored-verbatim rule is missing"
fi
if (has 'minted' || has 'mint') && (has 'bootstrap') && (has 'before assembly' || has 'before .* assembly executes'); then
  pass "identity: identity is minted at bootstrap before assembly (run verbs)"
else
  bad "identity: the minted-at-bootstrap-before-assembly property is missing"
fi
if (has 'time-ordered' || has 'time ordered' || has 'monotonic') && (has 'natural' && has 'sort'); then
  pass "identity: the natural-sort property of time-ordered ids is called out"
else
  bad "identity: the natural-sort property of time-ordered ids is missing"
fi

# --- 5. The two write-failure paths (DoD 5; Test plan open + mid-run) --------
# Store-open failure: stderr with the sink-failure exit code, nothing more.
if (has 'open' && has 'store') && (has 'nowhere to write' || has 'store-open' || has 'opening the store .* fail' || has 'cannot .* open') && has 'stderr' && (has 'sink-failure' || has 'sink failure'); then
  pass "failure-open: store-open failure -> stderr with the sink-failure exit code, nothing more"
else
  bad "failure-open: the store-open-failure path (stderr / sink-failure code / nothing more) is missing"
fi
# Mid-run sink failure: cancelling with reason "event stream unwritable",
# best-effort stderr report, distinct sink-failure code.
if (has 'mid-run' || has 'mid run') && (has 'cancelling' || has 'cancel') && (has 'event stream unwritable') && (has 'best-effort' || has 'best effort') && (has 'sink-failure' || has 'sink failure'); then
  pass "failure-midrun: mid-run sink failure -> cancelling (event stream unwritable), best-effort stderr, distinct sink-failure code"
else
  bad "failure-midrun: the mid-run sink-failure path is incomplete"
fi
# Exit codes named by cause, not by number (C26).
if (has 'by cause' || has 'by its cause' || has 'not by number' || has 'named .* cause') && (has 'exit code' || has 'sink-failure code'); then
  pass "failure: the exit code is named by cause, not by number (C26)"
else
  bad "failure: the name-exit-code-by-cause rule is missing"
fi

# --- 6. Flush semantics (DoD 6; Test plan flush-semantics) -------------------
if (has 'no user-space buffer' || has 'no user space buffer' || has 'without user-space buffer') && (has 'before its transition' || has 'before the transition' || has 'before .* recorded' || has 'written to the sink before'); then
  pass "flush: no user-space buffering — each record written to the sink before its transition is recorded"
else
  bad "flush: the no-user-space-buffering / write-before-recorded definition is missing"
fi
if has 'fsync' && (has 'run end' || has 'end of the run' || has 'at end') && (has 'cancel' || has 'cancellation'); then
  pass "flush: fsync at run end and at cancellation"
else
  bad "flush: the fsync-at-run-end-and-cancellation rule is missing"
fi
if (has 'per-event fsync' || has 'per event fsync') && (has "sink's business" || has 'delegat' || has 'the sink') && (has 'default local-file sink does not' || has 'does not .* fsync' || has 'not fsync per event'); then
  pass "flush: per-event fsync is delegated to the sink; the default local-file sink does not do it"
else
  bad "flush: the per-event-fsync-delegated / default-does-not rule is missing"
fi
if (has 'trailing partial' || has 'partial record') && (has 'at most one' || has 'one trailing') && (has 'toleren' || has 'tolerate' || has 'discard' || has 'tolerates'); then
  pass "flush: a reader tolerates and discards at most one trailing partial record"
else
  bad "flush: the ≤1-trailing-partial-record tolerance is missing"
fi

# --- 7. Per-record header (DoD 6/DoD-record; Test plan gapless-sequence) ------
if (has 'run identity' && has 'schema version') && (has 'every record' || has 'each record' || has 'every event'); then
  pass "header: every record carries run identity and schema version"
else
  bad "header: the run-identity + schema-version per record is missing"
fi
if (has 'sequence number' || has 'sequence' ) && has 'gapless' && (has 'strictly increasing' || has 'strictly-increasing'); then
  pass "header: sequence numbers are gapless and strictly increasing within a run"
else
  bad "header: the gapless-strictly-increasing-sequence property is missing"
fi
if (has 'wall-clock' || has 'wall clock') && (has 'informational') && (has 'monotonic offset' || has 'monotonic') && (has 'authoritative'); then
  pass "header: wall-clock stamp (informational) and monotonic offset (authoritative)"
else
  bad "header: the wall-clock-informational / monotonic-offset-authoritative split is missing"
fi
if (has 'concatenat' || has 'concatenate') && (has 'partition' || has 'partitioned'); then
  pass "header: records from concurrent runs concatenate and partition safely"
else
  bad "header: the safe-concatenation/partition property is missing"
fi

# --- 8. Prune / retention semantics (DoD 7; Test plan prune) -----------------
if (has 'nothing .* deleted implicitly' || has 'nothing is deleted implicitly' || has 'no .* implicit deletion') && (has 'run end' || has 'at run end'); then
  pass "prune: nothing is deleted implicitly at run end"
else
  bad "prune: the no-implicit-deletion-at-run-end rule is missing"
fi
if (has 'prune') && (has 'sole' || has 'only') && (has 'implicit-deletion' || has 'implicit deletion' || has 'deletion path' || has 'reclaim' || has 'removes'); then
  pass "prune: the prune verb is the SOLE implicit-deletion path (C26)"
else
  bad "prune: the prune-is-the-sole-deletion-path rule is missing"
fi
if has 'prune' && (has 'by count' || has 'count') && (has 'by age' || has 'age') && (has 'per-run director' || has 'run director' || has 'whole .* director'); then
  pass "prune: prune operates over whole per-run directories by count or age"
else
  bad "prune: the prune-over-per-run-directories-by-count-or-age unit is missing"
fi
if (has 'non-succeeded' || has 'not succeed' || has 'did not succeed') && has 'scratch' && has 'prune'; then
  pass "prune: prune is the only mechanism that removes non-succeeded scratch"
else
  bad "prune: the prune-removes-non-succeeded-scratch rule is missing"
fi

# --- 9. Scratch placement / isolation / lifecycle (DoD 8; Test plan scratch) --
if has 'scratch' && (has 'under the run' || has 'run director' || has "run's director"); then
  pass "scratch: scratch lives under the run directory"
else
  bad "scratch: the scratch-under-the-run-directory placement is missing"
fi
if has 'scratch' && (has 'namespaced by run and node' || has 'namespaced by run' || (has 'run' && has 'node' && has 'namespace')) && (has 'cannot .* collide' || has 'two nodes cannot' || has 'cannot read another' || has 'one .* cannot reach' || has 'isolat'); then
  pass "scratch: namespaced by run and node so nodes cannot collide and one cannot read another's"
else
  bad "scratch: the per-run/per-node namespacing + isolation is missing"
fi
if (has 'succeeded node' || has 'node succeeds' || has 'succeeded') && (has 'scratch .* deleted' || has 'deleted when the node succeeds' || has 'is deleted' || has 'scratch is gone') && (has 'non-succeeded' || has 'not succeed' || has 'did not succeed' || has 'retained'); then
  pass "scratch: succeeded-node scratch deleted; non-succeeded-node scratch retained for resume copy-forward"
else
  bad "scratch: the succeeded-deleted / non-succeeded-retained lifecycle is missing"
fi
if (has 'copy forward' || has 'copy-forward' || has 'copied forward') && (has 'resume'); then
  pass "scratch: a non-succeeded node's scratch is retained for a later resume to copy forward"
else
  bad "scratch: the retained-for-resume-copy-forward rule is missing"
fi
if (has 'scratch') && (has 'I/O failure' || has 'read or write failure' || has 'io failure' || has 'read/write failure') && (has 'retry-eligible' || has 'retry eligible'); then
  pass "scratch: scratch I/O failure is classified retry-eligible"
else
  bad "scratch: the scratch-I/O-failure-is-retry-eligible classification is missing"
fi

# --- 10. Concurrency posture (DoD 9; Test plan concurrent-runs) --------------
if (has 'per-run director' || has 'run director') && (has 'never collide' || has 'never share' || has 'disjoint') && (has 'concurrent' || has 'simultaneous' || has 'two .* runs'); then
  pass "concurrency: two simultaneous runs are safe w.r.t. the run store — per-run directories never collide"
else
  bad "concurrency: the concurrent-runs-safe / directories-never-collide posture is missing"
fi
if (has 'one run per container' || has 'one-run-per-container') && (has 'coordinates nothing' || has 'no .* coordination' || has 'does not coordinate' || has 'coordinate nothing'); then
  pass "concurrency: one-run-per-container; the tool coordinates nothing between processes"
else
  bad "concurrency: the one-run-per-container / no-cross-process-coordination posture is missing"
fi

# --- 11. Blocked-ticket hand-off (DoD 10; Test plan blocked-seam coverage) ----
for t in T19 T24 T27 T36 T53 T54a T58; do
  if has "$t"; then :; else bad "handoff: blocked ticket '$t' is not named in the hand-off"; fi
done
if has 'T19' && has 'T24' && has 'T27' && has 'T36' && has 'T53' && has 'T54a' && has 'T58'; then
  pass "handoff: all seven blocked tickets named (T19, T24, T27, T36, T53, T54a, T58)"
fi
# Each named with a specific seam (spot-check the load-bearing ones).
if has 'T19' && (has 'record header' || has 'header') && has 'sink'; then
  pass "handoff: T19 binds against the record header and the sink"
else
  bad "handoff: T19's record-header + sink seam is not named"
fi
if has 'T24' && (has 'before assembly' || has 'identity .* before' || has 'store .* before'); then
  pass "handoff: T24 binds against identity-minted-and-store-opened-before-assembly"
else
  bad "handoff: T24's before-assembly seam is not named"
fi
if has 'T58' && (has 'copy forward' || has 'copy-forward' || has 'copied forward') && (has 'store' && has 'resume'); then
  pass "handoff: T58 binds against scratch copy-forward and store-required-for-resume"
else
  bad "handoff: T58's copy-forward / store-required-for-resume seam is not named"
fi

# --- 12. Rejected alternatives (DoD 11; Test plan rejected-alternatives) ------
if (has 'buffer-until-exit' || has 'buffer until exit' || has 'buffered until .* exit' || has 'buffer .* exit'); then
  pass "rejected: a buffer-until-exit event writer is named and rejected"
else
  bad "rejected: the buffer-until-exit rejected alternative is missing"
fi
if (has 'per-event fsync' || has 'per event fsync') && (has 'default') && (has 'reject' || has 'not a default' || has 'opts into' || has 'opt into' || has 'cost the operator'); then
  pass "rejected: a per-event-fsync default is named and rejected"
else
  bad "rejected: the per-event-fsync-default rejected alternative is missing"
fi
if (has 'cross-run' || has 'shared .* index' || has 'metadata store' || has 'shared index') && (has 'metadata store' || has 'never be' || has 'reject'); then
  pass "rejected: a shared cross-run index / metadata store is named and rejected"
else
  bad "rejected: the cross-run-index/metadata-store rejected alternative is missing"
fi
if (has 'multi-process' || has 'multi process' || has 'cross-process') && (has 'lock' || has 'coordinat') && (has 'scheduler' || has 'reject'); then
  pass "rejected: a coordinated multi-process store lock is named and rejected"
else
  bad "rejected: the coordinated-multi-process-lock rejected alternative is missing"
fi
if has 'reopen' || has 'reopen condition'; then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen-condition statement is missing"
fi

# --- Component attributions the DoD demands (C18, C19, C26, C27) -------------
for c in C18 C19 C26 C27; do
  if has "$c"; then :; else bad "component: '$c' is not referenced in the ADR"; fi
done
if has 'C18' && has 'C19' && has 'C26' && has 'C27'; then
  pass "component: C18, C19, C26, C27 all referenced"
fi

# --- Open questions resolved (ticket says None; tasks.md carries no Q:) -------
if has 'open question' && (has 'none' || has 'no open question' || has 'no unresolved'); then
  pass "questions: the ADR records that there are no open questions (ticket + tasks.md)"
else
  bad "questions: the ADR must record the open-questions disposition (none)"
fi

# --- No coverage-matrix change (decision ticket owes no covering test) --------
# C18/C19 remain unmapped/deferred to T53/T19 in docs/coverage-matrix.md; this
# ADR must state it makes no matrix change so the boundary is visible.
if has 'coverage-matrix' || has 'coverage matrix'; then
  pass "coverage: the ADR states its (no-)coverage-matrix disposition"
else
  bad "coverage: the ADR must record that it makes no coverage-matrix change"
fi

# --- Scope-boundary restatement (permanent non-goals: LOCAL store only) -------
if (has 'local' || has 'file-based' || has 'file based' || has 'embedded') && (has 'metadata store' || has 'networked' || has 'network-backed' || has 'distributed') && (has 'never' || has 'not a' || has 'boundary' || has 'reject'); then
  pass "scope: the ADR restates the local-store-only boundary (never a networked/metadata store)"
else
  bad "scope: the ADR must restate the permanent-non-goals boundary (local store only)"
fi

# --- Cross-reference against arch.md (COMPLETENESS/no-invention guard) --------
# Every canonical seam term the ADR fixes must be one arch.md actually defines —
# no invention of a networked store, a new failure variant, or a new id scheme.
arch_has() { grep -qiE "$1" "$archmd"; }
invented=0
for term in 'UUIDv7' 'event stream unwritable' 'sink-failure' 'trailing partial' '<base>/<pipeline>/<run-id>'; do
  arch_has "$term" || { bad "cross-ref: ADR term '$term' is not present in arch.md"; invented=1; }
done
[ "$invented" -eq 0 ] && pass "cross-ref: every seam term the ADR fixes is grounded in arch.md (no invention)"

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
