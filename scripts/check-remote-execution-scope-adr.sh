#!/usr/bin/env bash
# Remote-execution scope-carve-out ADR acceptance checks for ticket 115 (T100).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/115-T100-remote-execution-scope-and-substrate-adr.md).
# T100 is a DECISION ticket whose durable deliverable is an ADR that MOVES a
# boundary docs/arch.md calls PERMANENT: "distributed execution system" is
# narrowed to mean an engine that distributes the GRAPH AND ITS CONTROL, so that
# ONE orchestrator process placing individual node attempts on remote compute it
# owns for the duration of ONE run becomes permitted.
#
# A carve-out that only says what is allowed drifts. This checker therefore pins
# BOTH halves, and the second half is the load-bearing one:
#
#   PERMITTED — one orchestrator process, one graph, one event stream, one run
#   lifetime, renting compute per node attempt and releasing it.
#
#   A VIOLATION — and every one of these is asserted to be still named as
#   excluded, in the ADR and in arch.md: cooperating orchestrators, work-stealing
#   between them, cross-run queues, a control plane that outlives a run, a
#   scheduler, an inbound API or listener on the orchestrator, a served database,
#   a task workload that reads or writes the run index, a web interface, a DSL, a
#   backfill orchestrator, and runtime graph expansion (a task discovering N
#   files does not become N pods).
#
# It also pins the substrate decisions T101-T112 inherit (one watch per process,
# Kubernetes-must-not-retry, annotations-authoritative identity, the two separate
# retry budgets, placement as node POLICY rather than an ExecutionClass variant,
# the Payload + blob-backend data path, and the write-ahead `attempt-submitted`
# record that goes into the EVENT STREAM rather than straight into the index) —
# because a later ticket that quietly re-decides one of them has widened the
# carve-out without anyone amending it.
#
# The supersession checks assert the two partial supersessions are recorded AND
# that the ADR-097 notes already in those files survive — a merged decision's
# text is never rewritten (ticket-conventions §10), so the older note surviving
# is the evidence that nothing was overwritten.
#
# This is a DOC-ONLY decision (the substrate's real risks are measured by the
# T101 spike, and the acceptance gate that asserts the boundary structurally over
# shipped code is T112), so the script builds nothing and asserts no production
# code: the only artifacts the decision commits are the embedded ADR, the
# arch.md amendment, the two partial-supersession notes, the README/CONTRIBUTING
# de-staling, and the additive `@1.3` event-stream schema revision.
#
# Run from the repository root. Exit 0 = every assertion holds, 1 = a failure,
# 2 = a required file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/115-T100-remote-execution-scope-and-substrate-adr.md"
archmd="docs/arch.md"
adr012="docs/implementation/012-T0.6-run-store-contract-adr.md"
adr014="docs/implementation/014-T0.8-durable-output-contract.md"
schema="schemas/event-stream/v1.schema.json"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

for f in "$adr" "$archmd" "$adr012" "$adr014" "$schema"; do
  [ -f "$f" ] || { echo "FAIL  required file missing: $f"; exit 2; }
done

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the ADR 012 / ADR 097 precedent). The ticket prose above it already
# names Kubernetes, placement, Payload and the rest — so a whole-file grep would
# pass content checks the ADR itself has not made. Every ADR-content assertion is
# therefore scoped to the ADR BODY: the slice from the first 'ADR:' heading to
# EOF. The ticket's own H1 ('# 115 · T100 — …') carries no 'ADR:' with a colon,
# so before the ADR is authored the slice is empty and every content check fails.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

has() { printf '%s' "$adr_body" | grep -qiE "$1"; }
file_has() { grep -qiE "$2" "$1"; }

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

# --- Accepted, with the DATED operator acceptance the boundary move needs -----
# Moving a boundary arch.md calls permanent is a hard STOP under
# ticket-conventions §8/§10 unless the operator's acceptance is RECORDED. The
# date is asserted, not just the word, so a later edit cannot soften a recorded
# acceptance into an implied one.
if has '^#+[[:space:]]+Status' && has 'accepted'; then
  pass "status: the ADR is in an Accepted status (not draft/proposed)"
else
  bad "status: the ADR must be recorded in an Accepted status"
fi
# Scoped to the Status section: the ADR's prose elsewhere legitimately discusses
# an earlier draft of the design, which is not a status.
adr_status=$(printf '%s' "$adr_body" | awk '/^#+[[:space:]]+Status/{f=1;next} /^#+[[:space:]]/{f=0} f {print}')
if printf '%s' "$adr_status" | grep -qiE 'status:?[[:space:]]*\**(draft|proposed)'; then
  bad "status: the ADR must not be draft/proposed"
else
  pass "status: no draft/proposed status leaked in"
fi
if has 'accepted \(2026-07-29\)|accepted on 2026-07-29|2026-07-29'; then
  pass "status: the operator acceptance carries its date (2026-07-29)"
else
  bad "status: the dated operator acceptance (2026-07-29) is missing"
fi
if (has 'operator' && has 'accept') && (has 'open questions' || has 'ticket-conventions §5' || has 'recorded'); then
  pass "status: operator acceptance is recorded (not inferred) and points at Open questions"
else
  bad "status: the recorded-operator-acceptance statement is missing"
fi

# --- 1. What the carve-out PERMITS, stated exactly ---------------------------
if (has 'single orchestrator process' || has 'one orchestrator process') \
   && (has 'node attempt') && (has 'remote compute'); then
  pass "permitted: one orchestrator process placing node attempts on remote compute"
else
  bad "permitted: the permitted shape (one orchestrator, node attempts, remote compute) is missing"
fi
if (has 'one run' || has 'duration of one run' || has "one run's lifetime") \
   && (has 'owns the graph' || has 'owning one graph' || has 'one graph'); then
  pass "permitted: bounded to ONE graph for ONE run's lifetime"
else
  bad "permitted: the one-graph / one-run-lifetime bound is missing"
fi
if (has 'event stream') && (has 'single-writer' || has 'one writer' || has 'owns the event stream'); then
  pass "permitted: the run keeps ONE event stream with ONE writer"
else
  bad "permitted: the single-event-stream / single-writer property is missing"
fi
if has 'changes .{0,3}where.{0,3} a node runs' || has 'a node runs, never'; then
  pass "permitted: remote execution changes WHERE a node runs and nothing else"
else
  bad "permitted: the 'changes where a node runs, nothing else' framing is missing"
fi

# --- 2. What still counts as a VIOLATION — the half that keeps it narrow -----
# Each of these must remain NAMED AS EXCLUDED in the ADR. A later ticket that
# needs one of them has widened the carve-out and owes its own decision ticket.
for term in 'cooperating orchestrator' 'work-stealing' 'cross-run queue' 'control plane'; do
  if has "$term"; then
    pass "violation: '$term' is still named as excluded"
  else
    bad "violation: '$term' is no longer named as excluded — the carve-out has widened"
  fi
done
if has 'outlives a run|outliving a run|outlive .* run'; then
  pass "violation: a control plane that OUTLIVES a run is still excluded"
else
  bad "violation: the outlives-a-run exclusion is missing"
fi
if (has 'no peer' || has 'gains no peer') && has 'no election' \
   && (has 'no shared lock' || has 'shared lock') && has 'no queue' && has 'no service'; then
  pass "violation: no peer / no election / no shared lock / no queue / no service"
else
  bad "violation: the no-peer/election/lock/queue/service list is incomplete"
fi
if (has 'no inbound' || has 'no callback' || has 'nothing inbound') \
   && (has 'listener' || has 'inbound reachability' || has 'no listener'); then
  pass "violation: an inbound listener/API on the orchestrator is still excluded"
else
  bad "violation: the no-inbound-listener exclusion is missing"
fi
if (has 'pods never' || has 'never read or write the metastore' || has 'never touch the run index') \
   && (has 'metastore' || has 'run index' || has 'index'); then
  pass "violation: task pods never read or write the run index"
else
  bad "violation: the pods-never-touch-the-index rule is missing"
fi
if has 'ModeNotImplemented' && (has 'RemoteSqld' || has 'SyncedReplica') && has 'sqld'; then
  pass "violation: a served database (sqld / remote / synced-replica modes) stays unimplemented"
else
  bad "violation: the no-served-database (sqld / RemoteSqld / SyncedReplica) statement is missing"
fi
if (has 'graph expansion' || has 'N files' || has 'discovers N') \
   && (has 'still rejected' || has 'never .* how many' || has 'unchanged'); then
  pass "violation: runtime graph expansion is still rejected, unchanged"
else
  bad "violation: the runtime-graph-expansion exclusion is missing"
fi
if has 'scheduler' && (has 'still' || has 'remains' || has 'not a scheduler' || has 'unchanged'); then
  pass "violation: dagr is still not a scheduler"
else
  bad "violation: the still-not-a-scheduler restatement is missing"
fi

# --- 3. Substrate: Kubernetes pods, one watch per PROCESS --------------------
if (has 'kubernetes') && (has 'pod') && (has 'watch'); then
  pass "substrate: Kubernetes pods, submitted by the orchestrator and observed by watch"
else
  bad "substrate: the Kubernetes-pods-observed-by-watch substrate is missing"
fi
if (has 'one shared watch' || has 'one watch per' || has 'single .*watch') \
   && (has 'never one per pod' || has 'per-pod watch' || has 'per pod'); then
  pass "substrate: ONE shared watch per orchestrator process, never one per pod"
else
  bad "substrate: the one-watch-per-process rule is missing"
fi
if (has 'resourceversion') && (has 'reconnect' || has 'reconnection' || has 'expiry'); then
  pass "substrate: watch reconnection after resourceVersion expiry is named"
else
  bad "substrate: the watch-reconnection obligation is missing"
fi

# --- 4. Kubernetes must not retry — an INVARIANT, not a default --------------
if (has 'backofflimit' && has '0') \
   && (has 'invariant' || has 'refuses a configuration' || has 'not a default'); then
  pass "retry: Kubernetes must not retry (backoffLimit=0) is an invariant, not a default"
else
  bad "retry: the Kubernetes-must-not-retry invariant is missing or softened to a default"
fi
if (has 'duplicat') && (has 'same attempt' || has 'execution of the same' || has 'both retry'); then
  pass "retry: the reason is recorded — two retry loops duplicate execution of one attempt"
else
  bad "retry: the duplicate-execution rationale is missing"
fi

# --- 5. No callback: the state path is outbound-only -------------------------
if (has 'shard') && (has 'event.stream shard' || has 'attempt-scoped') && has 'blob'; then
  pass "state-path: the pod writes an attempt-scoped event-stream shard and its output blob"
else
  bad "state-path: the shard + blob write is missing"
fi
if (has 'outbound') && (has 'kubernetes api' || has 'cluster.* api' || has 'platform') ; then
  pass "state-path: terminal state is learned from the platform API — outbound only"
else
  bad "state-path: the outbound-only terminal-state path is missing"
fi
if (has 'replay') && (has 'attempteventsink' || has 'sink'); then
  pass "state-path: shards are replayed into the driver's injected sink"
else
  bad "state-path: the replay-into-the-existing-sink step is missing"
fi
if (has 'nothing to authenticate' || has 'no authn' || has 'authentication surface' || has 'no pod authentication'); then
  pass "state-path: no listener means no authentication surface in a pod running user code"
else
  bad "state-path: the no-authentication-surface consequence is missing"
fi

# --- 6. Identity: annotations authoritative, labels are selectors ------------
if (has 'annotation') && (has 'authoritative') && has 'label'; then
  pass "identity: annotations are authoritative; labels are selectors"
else
  bad "identity: the annotations-authoritative / labels-as-selectors rule is missing"
fi
if has '63' && (has 'label' ) && (has 'lossy' || has 'limit' || has 'ceiling'); then
  pass "identity: the 63-character label-value limit is the recorded reason"
else
  bad "identity: the 63-character label limit rationale is missing"
fi

# --- 7. Two retry budgets, kept separate ------------------------------------
if (has 'never started' || has 'infrastructure failure' || has 'unschedulable') \
   && (has 'separate' || has 'not .* charged' || has 'own budget' || has 'operator-set budget'); then
  pass "budgets: a pod that never started is an infrastructure failure on a separate budget"
else
  bad "budgets: the separate infrastructure-retry budget is missing"
fi
if (has 'nodepolicy::retries' || has 'nodepolicy .*retries' || has 'user-visible attempt'); then
  pass "budgets: only a container that actually ran consumes a user-visible attempt"
else
  bad "budgets: the user-visible-attempt rule is missing"
fi
# ADR §6 keeps the normative taxonomy CLOSED: OOMKilled/Evicted/etc. are
# diagnostic strings, never a tenth terminal state.
if (has 'nine-state' || has 'nine' ) && (has 'closed') && (has 'gains no member' || has 'no member' || has 'taxonomy'); then
  pass "budgets: the nine-state terminal taxonomy stays closed and gains no member"
else
  bad "budgets: the closed-taxonomy invariant is missing"
fi
if (has 'oomkilled' || has 'evicted' || has 'imagepullbackoff') && (has 'diagnostic'); then
  pass "budgets: platform failure modes are carried as diagnostic strings, not states"
else
  bad "budgets: the diagnostic-strings-not-states rule is missing"
fi

# --- 8. Placement is a NodePolicy field, NEVER an ExecutionClass variant -----
# This is the decision that keeps resume working; a later ticket adding a
# `Remote` variant to ExecutionClass refuses resume for every shipped pipeline.
if (has 'nodepolicy') && (has 'placement') \
   && (has 'never an .?executionclass' || has 'not an .?executionclass' || has 'rather than an? .?executionclass'); then
  pass "placement: placement is a NodePolicy field, never an ExecutionClass variant"
else
  bad "placement: the NodePolicy-not-ExecutionClass rule is missing"
fi
if (has 'structuralmismatch' || has 'structural mismatch') && (has 'policydiff' || has 'policy diff' || has 'policy hash'); then
  pass "placement: the reason is recorded — structural mismatch refuses resume, a policy diff does not"
else
  bad "placement: the resume-compatibility rationale is missing"
fi
if (has 'opaque string') && (has 'never learns what kubernetes is' || has 'dagr-core never learns' || has 'core .*never learns'); then
  pass "placement: the policy carries opaque strings only; dagr-core never learns Kubernetes"
else
  bad "placement: the opaque-strings-only / core-stays-ignorant rule is missing"
fi

# --- 9. Data passing: a zero-dep Payload codec plus ONE blob backend ---------
if has 'payload' && (has 'encode' && has 'decode') && (has 'dagr-core' || has 'core'); then
  pass "data: a Payload encode/decode trait lands in dagr-core"
else
  bad "data: the Payload codec trait is missing"
fi
if (has 'zero .*dependency' || has 'dependency set stays empty' || has 'zero-dep') && (has 'no serde' || has 'serde'); then
  pass "data: dagr-core's runtime dependency set stays empty (serde rejected)"
else
  bad "data: the zero-dep-core guarantee for the codec is missing"
fi
if has 'blobstore|blob store|blob backend|blob crate' && (has 'opt-in crate' || has 'new .*crate' || has 'default-off'); then
  pass "data: one blob backend lands in a new opt-in crate behind a default-off feature"
else
  bad "data: the opt-in blob-crate / default-off-feature boundary is missing"
fi
if has 'durableoutput' && (has 'blanket' || has 'bridge' || has 'bridged'); then
  pass "data: a blanket DurableOutput bridge reuses the shipped durable-reference machinery"
else
  bad "data: the DurableOutput bridge is missing"
fi
if (has 'in-memory fast path' || has 'in-memory' ) && has 'local'; then
  pass "data: local mode keeps the in-memory fast path"
else
  bad "data: the local in-memory fast path is missing"
fi
# The three local/remote boundary cases and their honest cost.
for edge in 'remote → remote' 'local → remote' 'remote → local'; do
  if has "$edge"; then
    pass "data: boundary case '$edge' is recorded"
  else
    bad "data: boundary case '$edge' is missing"
  fi
done
if (has 'contiguous subgraph') && (has 'through the orchestrator' || has 'routes payload bytes'); then
  pass "data: the alternating-placement cost and the contiguous-subgraph guidance are recorded"
else
  bad "data: the alternating-placement cost guidance is missing"
fi

# --- 10. The write-ahead submission record — an EVENT, not an index write ----
# This is the clause that keeps ADR 097's projection guarantee alive.
if has 'attempt-submitted'; then
  pass "submission: the attempt-submitted record is named"
else
  bad "submission: the attempt-submitted record is missing"
fi
if (has 'write-ahead' || has 'write ahead') && has 'before.{0,3} the remote work is created'; then
  pass "submission: it is written BEFORE the remote work is created (write-ahead)"
else
  bad "submission: the write-ahead ordering is missing"
fi
if (has 'run_id, node, attempt' || has 'identity triple') && (has 'idempotency key' || has 'idempotenc'); then
  pass "submission: the identity triple (run_id, node, attempt) is the idempotency key"
else
  bad "submission: the identity-triple idempotency key is missing"
fi
if (has 'ordered' && has 'positional') && (has 'content hash' || has 'content_hash'); then
  pass "submission: the ordered positional inputs each carry a content hash"
else
  bad "submission: the ordered-positional-inputs-with-content-hash rule is missing"
fi
if (has 'empty array') && (has 'never a null' || has 'not a null'); then
  pass "submission: a consume-nothing source encodes as an empty array, never a null"
else
  bad "submission: the empty-array-never-null encoding is missing"
fi
if (has 'opaque and non-secret' || has 'non-secret') && (has 'presigned' || has 'credential'); then
  pass "submission: recorded references must be opaque and non-secret (no presigned URLs)"
else
  bad "submission: the no-credential-in-a-record rule is missing"
fi
if (has 'event stream, not' || has 'not straight into the index' || has 'not directly into the index') \
   && (has 'adr 097' || has 'projection'); then
  pass "submission: it goes in the EVENT STREAM, not the index — ADR 097's projection guarantee"
else
  bad "submission: the event-not-index decision (and ADR 097's projection guarantee) is missing"
fi
if (has 'live-tee' || has 'live tee') && (has 'byte-identical' || has 'reconcile'); then
  pass "submission: the machine-checked live-equals-reconcile property is named as the reason"
else
  bad "submission: the live-equals-reconcile reason is missing"
fi
if has 'dagr\.event-stream@1\.3|@1\.3'; then
  pass "submission: the revision is recorded as dagr.event-stream@1.3"
else
  bad "submission: the @1.3 schema revision is not named"
fi
if (has 'new event kind' || has 'a new kind') && (has 'byte-identical' || has 'local .*unchanged' || has 'only by a remote executor'); then
  pass "submission: a new kind (not extra fields) keeps a local run's stream byte-identical"
else
  bad "submission: the new-kind / byte-identical-local-run rationale is missing"
fi

# --- 11. Containment boundaries this ADR keeps ------------------------------
if (has 'default-off' || has 'default off') && (has 'feature'); then
  pass "containment: the remote executor is behind a default-off feature"
else
  bad "containment: the default-off-feature boundary is missing"
fi
if (has 'http' || has 'tls' || has 'network stack') && (has 'quarantine' || has 'confined' || has 'containment'); then
  pass "containment: the network stack a Kubernetes client pulls in is quarantined"
else
  bad "containment: the dependency-quarantine statement is missing"
fi
if has 'check-metastore-acceptance-boundary' || (has 'passes .*unchanged' && has 'boundary'); then
  pass "containment: the shipped no-server boundary checker passes unchanged under this design"
else
  bad "containment: the no-server-checker-passes-unchanged claim is missing"
fi

# --- 12. Reopen condition + no-code posture ---------------------------------
if has 'reopen'; then
  pass "reopen: the reopen condition is stated (T101's measurements can reopen §2/§3)"
else
  bad "reopen: the reopen-condition statement is missing"
fi
if (has 'no code' || has 'no production code') ; then
  pass "no-code: the ADR records that it ships no production code"
else
  bad "no-code: the ships-no-code statement is missing"
fi
if has 't101' && (has 'gates' || has 'spike'); then
  pass "no-code: T101's spike is named as the gate on every other M10 ticket"
else
  bad "no-code: the T101-gates-the-rest statement is missing"
fi

# --- 13. Partial supersession, recorded WITHOUT rewriting merged text -------
# ticket-conventions §10: merged decision text is never rewritten. The evidence
# that nothing was overwritten is that each file's PRIOR note still stands.
if file_has "$adr012" 'Superseded \(in part\) by ADR 115 \(T100\)'; then
  pass "supersede: ADR 012 carries the partial-supersession note for ADR 115"
else
  bad "supersede: ADR 012's 'Superseded (in part) by ADR 115 (T100)' note is missing"
fi
if file_has "$adr012" 'not a distributed store|distributed store'; then
  pass "supersede: ADR 012's superseded clause is named (its distributed-store clause only)"
else
  bad "supersede: ADR 012's distributed-store clause is not named"
fi
if file_has "$adr012" 'Superseded \(in part\) by ADR 097'; then
  pass "supersede: ADR 012's earlier ADR-097 note survives — merged text was not rewritten"
else
  bad "supersede: ADR 012's earlier ADR-097 note was lost — merged decision text must never be rewritten"
fi
if file_has "$adr014" 'Superseded \(in part\) by ADR 115 \(T100\)'; then
  pass "supersede: ADR 014 carries the partial-supersession note for ADR 115"
else
  bad "supersede: ADR 014's 'Superseded (in part) by ADR 115 (T100)' note is missing"
fi
if file_has "$adr014" 'remote/object backend|object-store abstraction'; then
  pass "supersede: ADR 014's superseded clause is named (its object-backend clause only)"
else
  bad "supersede: ADR 014's object-backend clause is not named"
fi

# --- 14. arch.md: every OTHER permanent exclusion survives verbatim ---------
# The amendment moved exactly one word's meaning. If any of these disappears,
# the amendment did more than it was accepted to do.
for excl in 'scheduler' 'coordinating\** metadata store|\*coordinating\* metadata store' \
            'web interface' 'domain-specific language' 'backfill orchestrator'; do
  if grep -qiE "$excl" "$archmd"; then
    pass "arch: permanent exclusion still present — ${excl%%|*}"
  else
    bad "arch: permanent exclusion LOST — ${excl%%|*}"
  fi
done
if file_has "$archmd" "graph's shape never changes at runtime"; then
  pass "arch: 'the graph's shape never changes at runtime' is untouched"
else
  bad "arch: the no-runtime-graph-change clause is missing"
fi
if file_has "$archmd" 'ADR 115'; then
  pass "arch: the carve-out cites ADR 115"
else
  bad "arch: arch.md does not cite ADR 115"
fi
if file_has "$archmd" 'single orchestrator process placing individual node attempts on remote compute'; then
  pass "arch: arch.md states the permitted shape in full"
else
  bad "arch: arch.md's permitted-shape sentence is missing or reworded"
fi
if file_has "$archmd" 'ADR 115 \(T100\), 2026-07-29'; then
  pass "arch: the Amendment changelog carries the dated ADR 115 entry"
else
  bad "arch: the Amendment changelog entry for ADR 115 is missing"
fi
# Criterion 7: amended IN PLACE — still [machine]-classed, still one criterion,
# with the "no dagr server" half kept UNCONDITIONAL.
if grep -qE '^7\. \*\*\[machine\]\*\*' "$archmd"; then
  pass "arch: system-level criterion 7 is still present and still [machine]-classed"
else
  bad "arch: criterion 7 is missing or lost its [machine] classification"
fi
if grep -qE '^7\. .*server, database, or scheduler' "$archmd" \
   && grep -qiE '^7\. .*(default).*executor' "$archmd"; then
  pass "arch: criterion 7 scopes the server/database/scheduler clause to the DEFAULT executor"
else
  bad "arch: criterion 7's default-executor scoping is missing"
fi
if grep -qiE "^7\. .*no .*dagr.* server|^7\. .*unconditional" "$archmd"; then
  pass "arch: criterion 7 keeps the 'no dagr server' half unconditional"
else
  bad "arch: criterion 7's unconditional no-dagr-server half is missing"
fi
# Operational model and Performance envelope, amended for the executor seam and
# the local-vs-remote overhead split.
if grep -qiE 'Remote execution \(opt-in, ADR 115\)' "$archmd"; then
  pass "arch: the Operational model records the opt-in remote executor"
else
  bad "arch: the Operational model's remote-executor paragraph is missing"
fi
if grep -qiE 'under one millisecond.*local, in-process execution|local, in-process execution' "$archmd" \
   && grep -qiE 'submission-to-start latency' "$archmd"; then
  pass "arch: the Performance envelope scopes the sub-ms budget to local and reports remote latency separately"
else
  bad "arch: the Performance envelope's local/remote overhead split is missing"
fi
# The ADR's §6 invariant, asserted against the normative table itself: the
# terminal-state taxonomy is CLOSED at nine. A tenth row means a later ticket
# added a state ADR 115 said it would not.
states=$(awk '/\*\*Terminal states\.\*\*/{f=1} /\*\*State classes\.\*\*/{f=0} f && /^\| `/{n++} END{print n+0}' "$archmd")
if [ "$states" -eq 9 ]; then
  pass "arch: the terminal-state taxonomy is still closed at nine members"
else
  bad "arch: the terminal-state taxonomy has $states members, not nine — ADR 115 §6 said it gains none"
fi

# --- 15. The @1.3 schema revision is additive and safe for old readers ------
# Scoped to the `kind` ENUM, not the whole file: every kind also appears in its
# own `"const"` conditional further down, so a whole-file grep would still find a
# kind that had been dropped from the enum.
kind_enum=$(awk '/"enum": \[/{f=1} f{print} f && /\]/{exit}' "$schema")
in_enum() { printf '%s' "$kind_enum" | grep -q "\"$1\""; }
if in_enum attempt-submitted; then
  pass "schema: the kind enum carries 'attempt-submitted'"
else
  bad "schema: the attempt-submitted kind is missing from the kind enum"
fi
# Additive means every pre-@1.3 kind survives; a REPLACED kind would break every
# shipped reader and every frozen corpus fixture.
dropped=0
for kind in run-started node-ready node-admitted attempt-started attempt-succeeded \
            attempt-failed attempt-outcome output-produced node-terminal \
            zombie-at-exit run-finished; do
  in_enum "$kind" || { bad "schema: pre-@1.3 kind '$kind' left the enum — the revision is not additive"; dropped=1; }
done
[ "$dropped" -eq 0 ] && pass "schema: every pre-@1.3 kind survives in the enum — the revision is additive"
if grep -q '@1\.3' "$schema"; then
  pass "schema: the @1.3 revision is described inline in the schema's own description"
else
  bad "schema: the @1.3 revision is not described inline (the @1.1/@1.2 precedent)"
fi
if grep -q '"required": \["node", "attempt", "inputs"\]' "$schema"; then
  pass "schema: the attempt-submitted conditional requires node/attempt/inputs"
else
  bad "schema: the attempt-submitted conditional's required fields are missing"
fi
if grep -q '"maxItems": 8' "$schema"; then
  pass "schema: inputs are capped at the arity ceiling of 8"
else
  bad "schema: the inputs arity ceiling of 8 is missing"
fi
# Additive-only evolution: no object may close itself (the T48 drift guard's rule).
if grep -qE '"(additionalProperties|unevaluatedProperties)": *false' "$schema"; then
  bad "schema: an object closed itself — that is a non-additive change within @1"
else
  pass "schema: no object closes itself — evolution stays additive within @1"
fi

# --- 16. The process gates and the user-facing docs name BOTH carve-outs ----
# A scope check over an M10 diff reads these three files. If one stops naming the
# carve-out, correct M10 work starts getting rejected — or, worse, an M11 diff
# that crosses the boundary stops getting caught.
for gate in ".claude/skills/shipping-dagr-tickets/SKILL.md" \
            ".claude/skills/shipping-dagr-tickets/references/ticket-conventions.md" \
            ".github/pull_request_template.md"; do
  if [ ! -f "$gate" ]; then
    bad "gates: process gate missing: $gate"
  elif grep -qE 'ADR 115' "$gate" && grep -qE 'ADR 097' "$gate"; then
    pass "gates: $gate names both carve-outs"
  else
    bad "gates: $gate does not name both carve-outs (ADR 097 and ADR 115)"
  fi
done
for doc in README.md CONTRIBUTING.md; do
  if grep -qE 'ADR 115' "$doc" && grep -qiE 'backfill orchestrator' "$doc"; then
    pass "docs: $doc carries the amended non-goals sentence and cites ADR 115"
  else
    bad "docs: $doc does not carry the amended non-goals sentence citing ADR 115"
  fi
done

# --- 17. The matrices were reworded, not re-rowed ---------------------------
# The ADR amends criterion 7 IN PLACE, so no matrix row is added or removed and
# the mapped test id is unchanged. A new SL row here would mean a feature ticket
# smuggled a criterion into a decision ticket.
if grep -qE '^\| SL7 \| machine \|' docs/criteria-matrix.md; then
  pass "matrix: criteria-matrix SL7 keeps its row and its machine class"
else
  bad "matrix: criteria-matrix's SL7 row or its machine class changed"
fi
if grep -qE '^\| SL7 \| machine \|' docs/coverage-matrix.md \
   && grep -q 'no_server_database_or_scheduler_is_required' docs/coverage-matrix.md; then
  pass "matrix: coverage-matrix SL7 keeps its row, class, and mapped test id"
else
  bad "matrix: coverage-matrix's SL7 row, class, or mapped test id changed"
fi
for m in docs/criteria-matrix.md docs/coverage-matrix.md; do
  if grep -q 'ADR 115' "$m"; then
    pass "matrix: $m's SL7 paraphrase matches the amended criterion (cites ADR 115)"
  else
    bad "matrix: $m's SL7 paraphrase still contradicts the amended criterion"
  fi
done

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
