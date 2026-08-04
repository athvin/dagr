#!/usr/bin/env bash
# T112 — the M10 remote-execution acceptance gate's STRUCTURAL boundary
# invariants.
#
# M10 moved a boundary docs/arch.md calls PERMANENT. ADR 115 narrowed
# "distributed execution system" to mean an engine that distributes the GRAPH AND
# ITS CONTROL, and permitted ONE orchestrator process placing individual node
# attempts on remote compute it owns for the duration of ONE run. The only thing
# keeping that carve-out at exactly the width it was granted is a check that
# fails when it widens — which is what this file is, on ADR 097's precedent
# (T88 asserted its boundary with `cargo tree` and a diff-scanning script, not
# with prose).
#
# Everything here is a fact about the BUILD or about the DIFF, so none of it
# needs a cluster and none of it can be made true or false by one. The
# behavioural half of the gate is `crates/cli/tests/m10_acceptance_gate.rs`
# (which runs this script) and `crates/cli/tests/m10_remote_execution.rs`.
#
# The invariants, in the order the ticket lists them:
#
#   1. `dagr-core`'s runtime dependency set is EMPTY, at every feature setting,
#      and it reaches no M10 crate.
#   2. A build that did not ask for remote execution compiles NO HTTP/TLS stack,
#      no Kubernetes client and no object-store transport — asserted against the
#      resolved dependency list, not the manifest. Plus the non-vacuity leg:
#      `--features k8s` / `--features blob-s3` DO reach them.
#   3. The POD path links no metastore and carries no database credential: the
#      pod-side `exec-node` verb's module reaches neither, and nothing in the pod
#      spec builder plumbs an index URL or credential into a container.
#   4. `OpenMode::RemoteSqld` / `SyncedReplica` are still `ModeNotImplemented`
#      stubs — the reserved seam did not acquire a client while M10 was adding
#      one for Kubernetes.
#   5. NO LISTENER. The M9->M10 shipped-source diff introduces no network
#      listener, server framework, or `*Scheduler` type.
#   6. The vocabulary is closed: nine terminal states, three trigger rules.
#   7. Every arch.md numbered criterion has exactly one criteria-matrix row and
#      exactly one coverage-matrix row, and nothing is left `unmapped`.
#
# It COMPOSES `scripts/check-k8s-feature-gating.sh` (T107's crate-graph proofs)
# and `scripts/check-blob-feature-gating.sh` (T104's) rather than restating them,
# so M10's gating is asserted in one place and a relaxation in either is a
# failure here.
#
# `dagr-k8s` and `dagr-blob` are normal workspace members, so `cargo build --all`
# DOES build their default (client-free) surfaces — that is expected and is NOT
# what these checks forbid. What they forbid is an HTTP/TLS/Kubernetes/S3 stack
# COMPILED by a build that did not ask for one, any edge onto `dagr-core`, a
# metastore edge on the pod path, and a coordination surface crossing the
# permanent scope boundary.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure,
# 2 = a usage/setup anomaly.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

command -v cargo >/dev/null 2>&1 || {
  echo "cargo unavailable — the crate-graph proofs are load-bearing for T112"
  exit 2
}

# ---------------------------------------------------------------------------
# 0. Compose the per-crate feature-gating scripts
# ---------------------------------------------------------------------------
for gating in scripts/check-k8s-feature-gating.sh scripts/check-blob-feature-gating.sh; do
  if [ -f "$gating" ]; then
    if bash "$gating" >/dev/null 2>&1; then
      pass "composed: $(basename "$gating") passes"
    else
      bad "composed: $(basename "$gating") FAILED — rerun it directly for the detail"
    fi
  else
    bad "the composed gating script $gating is missing"
  fi
done

# ---------------------------------------------------------------------------
# 1. dagr-core's runtime dependency set is empty, at every setting
# ---------------------------------------------------------------------------
core_nd=$(cargo tree -p dagr-core --no-default-features -e normal --prefix none 2>/dev/null \
            | grep -vE '^dagr-core[[:space:]]' | grep -vE '^[[:space:]]*$')
if [ -z "$core_nd" ]; then
  pass "core(--no-default-features): runtime dependency set is EMPTY (M10 added a Kubernetes client, an object-store client and a pod-side verb; none reached the core)"
else
  bad "core(--no-default-features): runtime dependency set is NON-empty:"
  printf '%s\n' "$core_nd"
fi

core_all=$(cargo tree -p dagr-core --all-features -e normal --prefix none 2>/dev/null)
if [ -z "$core_all" ]; then
  bad "core(--all-features): cargo tree reported nothing — the assertion would be vacuous"
else
  hits=$(printf '%s\n' "$core_all" \
           | grep -EI '^(dagr-k8s|dagr-blob|dagr-metastore|kube|kube-client|k8s-openapi|rustls|hyper|ureq)[[:space:]]')
  if [ -z "$hits" ]; then
    pass "core(--all-features): reaches no M10 crate and no remote stack (the adversarial setting)"
  else
    bad "core(--all-features): reached an M10 crate or a remote stack:"
    printf '%s\n' "$hits"
  fi
fi

# ---------------------------------------------------------------------------
# 2. A build that did not ask for remote execution compiles none of it
# ---------------------------------------------------------------------------
# Every crate that constitutes an HTTP or TLS stack, a Kubernetes client, or an
# object-store transport. A default build compiling ANY of these is the M10
# containment guarantee broken, whichever crate pulled it in.
remote_stack='^(hyper|hyper-util|hyper-rustls|rustls|rustls-pki-types|rustls-webpki|rustls-native-certs|ring|aws-lc-rs|tower|tower-http|h2|tokio-rustls|ureq|kube|kube-client|kube-core|kube-runtime|k8s-openapi)[[:space:]]'

for leg in "" "--no-default-features"; do
  label=${leg:-default}
  # shellcheck disable=SC2086 # an empty $leg must expand to no argument at all
  ws=$(cargo tree --workspace -e normal --prefix none $leg 2>/dev/null)
  if [ -z "$ws" ]; then
    bad "workspace($label): cargo tree reported nothing — the assertion would be vacuous"
    continue
  fi
  hits=$(printf '%s\n' "$ws" | grep -EI "$remote_stack" | sort -u)
  if [ -z "$hits" ]; then
    pass "workspace($label): compiles NO HTTP/TLS stack, no Kubernetes client, no object-store transport"
  else
    bad "workspace($label): compiled a remote stack a build that did not ask for one must not have:"
    printf '%s\n' "$hits"
  fi
done

# Non-vacuity: the very same query DOES see the stack when a build asks for it.
# Without this control a mistyped crate name would turn the guarantee green for
# the wrong reason.
k8s_tree=$(cargo tree -p dagr-cli --features k8s -e normal --prefix none 2>/dev/null)
if printf '%s\n' "$k8s_tree" | grep -qE '^kube[[:space:]]'; then
  pass "non-vacuity: cli(--features k8s) DOES reach kube — the absence checks are looking at something"
else
  bad "non-vacuity: cli(--features k8s) did not reach kube; the absence checks above prove nothing"
fi
s3_tree=$(cargo tree -p dagr-cli --features blob-s3 -e normal --prefix none 2>/dev/null)
if printf '%s\n' "$s3_tree" | grep -qE '^ureq[[:space:]]'; then
  pass "non-vacuity: cli(--features blob-s3) DOES reach the object-store transport"
else
  bad "non-vacuity: cli(--features blob-s3) did not reach ureq; the absence checks above prove nothing"
fi

# ---------------------------------------------------------------------------
# 3. The pod path links no metastore and carries no database credential
# ---------------------------------------------------------------------------
# The pod re-enters the pipeline binary and runs ONE attempt through the
# `exec-node` verb (ADR 115 §2/§3). Its module, and the pod spec builder that
# constructs the container it runs in, are where a metastore edge or a
# credential would have to appear.
podside="crates/cli/src/exec_node.rs crates/cli/src/shard.rs crates/k8s/src/executor.rs"
missing=""
for f in $podside; do [ -f "$f" ] || missing="$missing $f"; done
if [ -n "$missing" ]; then
  bad "the pod-path sources are missing:$missing — the scan below would be vacuous"
else
  hits=$(grep -nEI 'dagr_metastore|dagr-metastore|libsql|OpenMode|MetastoreWriter|run_index' $podside)
  if [ -z "$hits" ]; then
    pass "pod path: the \`exec-node\` verb, the shard writer and the pod spec builder reach NO metastore"
  else
    bad "pod path: a metastore symbol is reachable from the pod path:"
    printf '%s\n' "$hits"
  fi

  # And no credential: nothing on the pod path names a run-index database URL, a
  # DSN, or an auth token it would have to pass into a container. Deliberately
  # narrow — `crates/k8s/src/executor.rs` carries a list of credential MARKERS it
  # refuses, and a scan that read those as leaks would be asserting the opposite
  # of the invariant.
  creds=$(grep -nEI 'DATABASE_URL|LIBSQL_URL|DAGR_METASTORE|libsql://|postgres://|\bdsn\b' $podside)
  if [ -z "$creds" ]; then
    pass "pod path: no database credential is plumbed into a pod"
  else
    bad "pod path: a database-credential symbol appears on the pod path:"
    printf '%s\n' "$creds"
  fi

  # The positive half, and the reason the scan above can be narrow: the pod spec
  # builder REFUSES a credential-bearing reference outright, so a payload URI
  # that carries one never reaches a container's argv.
  if grep -q 'reject_credential_bearing' crates/k8s/src/executor.rs; then
    pass "pod path: a credential-bearing reference is refused before it can reach a container's argv"
  else
    bad "pod path: the credential-bearing-reference refusal is gone from the pod spec builder"
  fi
fi

# The pod's own container spec must carry no environment-variable plumbing that
# could smuggle one: `PodSpec` states the image, the command, the size and the
# labels, and that is the whole of it.
if [ -f crates/k8s/src/executor.rs ]; then
  if grep -qEI 'EnvVar|env_vars|envFrom|secretKeyRef|configMapKeyRef' crates/k8s/src/executor.rs; then
    bad "pod spec: the builder grew an environment/secret plumbing surface — a pod's inputs are references it is GIVEN, never a credential"
  else
    pass "pod spec: no environment, secret or configmap plumbing (payloads travel through the blob store, never through the API server)"
  fi
fi

# ---------------------------------------------------------------------------
# 4. The reserved open modes are still stubs
# ---------------------------------------------------------------------------
store="crates/metastore/src/store.rs"
if [ -f "$store" ]; then
  if grep -q 'RemoteSqld' "$store" && grep -q 'SyncedReplica' "$store" \
     && grep -q 'ModeNotImplemented' "$store"; then
    pass "metastore: RemoteSqld / SyncedReplica are still recognized stubs (ModeNotImplemented), not a shipped server client"
  else
    bad "metastore: the reserved open-mode seam lost its ModeNotImplemented guard"
  fi
  wired=$(grep -nEI 'embedded_replica\(|sync_from_remote|Database::open_remote' "$store")
  if [ -z "$wired" ]; then
    pass "metastore: the seam acquired no real remote client call while M10 was adding one for Kubernetes"
  else
    bad "metastore: the reserved seam acquired a wired remote client call:"
    printf '%s\n' "$wired"
  fi
else
  bad "metastore: $store is missing — the reserved-seam assertion would be vacuous"
fi

# ---------------------------------------------------------------------------
# 5. NO LISTENER: the M9->M10 shipped-source diff
# ---------------------------------------------------------------------------
# The base is the last M9 commit (the T99 done-marker), an immutable ancestor of
# this branch. In CI `actions/checkout` makes a shallow clone that holds only the
# tip, so DEEPEN it until the marker is in the local object store — the same
# treatment `check-metastore-acceptance-boundary.sh` applies, and for the same
# reason: the diff base must be byte-identical to the dev box's.
m9_marker=d11de14
if ! git rev-parse --verify --quiet "${m9_marker}^{commit}" >/dev/null 2>&1 \
   && [ "$(git rev-parse --is-shallow-repository 2>/dev/null)" = "true" ]; then
  git fetch --quiet --deepen=200 origin >/dev/null 2>&1 || true
fi
m9_base=""
for cand in "$m9_marker" origin/main main; do
  if git rev-parse --verify --quiet "${cand}^{commit}" >/dev/null 2>&1; then
    m9_base="$cand"; break
  fi
done

if [ -z "$m9_base" ]; then
  bad "could not resolve an M9 base commit for the diff-surface check (neither the T99 marker nor main is reachable)"
else
  # Added lines only (`^+`), over SHIPPED source (`crates/*/src/*`) — never tests
  # or examples, because a gate's own test naturally mentions these words in
  # negation. A REAL violation is a bound socket, a served endpoint, a server
  # framework, or a scheduler type. dagr's Kubernetes client makes OUTBOUND calls
  # and holds ONE watch; it accepts nothing.
  forbidden='TcpListener|UdpSocket|::bind\(|\.serve\(|\.listen\(|axum|actix|warp::|hyper::(Server|server)|tonic::|pgwire|struct[[:space:]]+[A-Za-z_]*Scheduler|impl[[:space:]]+[A-Za-z_]*Scheduler|trait[[:space:]]+[A-Za-z_]*Scheduler'
  diff_lines=$(git diff "$m9_base"..HEAD -- 'crates/*/src/*' 2>/dev/null | grep -cE '^\+' || true)
  if [ "${diff_lines:-0}" -lt 100 ]; then
    bad "the M9->M10 shipped-source diff from ${m9_base} added only ${diff_lines:-0} lines — the scan would be near-vacuous; is the base right?"
  fi
  hits=$(git diff "$m9_base"..HEAD -- 'crates/*/src/*' 2>/dev/null \
           | grep -E '^\+' \
           | grep -vE '^\+\+\+' \
           | grep -EI "$forbidden")
  if [ -z "$hits" ]; then
    pass "M9->M10 diff (from ${m9_base}, ${diff_lines} added shipped-source lines): NO listener, no server framework, no scheduler type"
  else
    bad "M9->M10 diff adds a forbidden inbound/coordination surface in shipped source:"
    printf '%s\n' "$hits"
  fi
fi

# The unconditional half, independent of any diff base: no shipped source
# anywhere binds a socket or serves an endpoint.
listener=$(grep -rnEI 'TcpListener|UdpSocket|::bind\(|\.serve\(|\.listen\(' crates/*/src 2>/dev/null)
if [ -z "$listener" ]; then
  pass "shipped source (whole tree): binds no socket and serves no endpoint under either executor"
else
  bad "shipped source binds a socket or serves an endpoint:"
  printf '%s\n' "$listener"
fi

# ---------------------------------------------------------------------------
# 6. The vocabulary is closed
# ---------------------------------------------------------------------------
# M10 added an executor, two retry budgets, a pre-start failure class, an
# adoption refusal and an OOM diagnostic — and not one of them is a new terminal
# state. That is the whole claim: remote execution changes WHERE a node runs and
# nothing else. (The typed, exhaustive-match form of this is in
# `crates/cli/tests/m10_acceptance_gate.rs`; the count is asserted here so the
# gate script alone catches a tenth variant.)
ctx="crates/core/src/context.rs"
if [ -f "$ctx" ]; then
  n=$(awk '/^pub enum TerminalState/{f=1;next} f&&/^}/{exit} f' "$ctx" \
        | grep -cE '^[[:space:]]+[A-Z][A-Za-z]*,')
  if [ "$n" -eq 9 ]; then
    pass "vocabulary: TerminalState has exactly nine members"
  else
    bad "vocabulary: TerminalState has $n members, not nine"
  fi
else
  bad "vocabulary: $ctx is missing"
fi

asm="crates/core/src/binding.rs"
if [ -f "$asm" ]; then
  n=$(awk '/^pub enum TriggerRule/{f=1;next} f&&/^}/{exit} f' "$asm" \
        | grep -cE '^[[:space:]]+[A-Z][A-Za-z]*,')
  if [ "$n" -eq 3 ]; then
    pass "vocabulary: TriggerRule has exactly three members"
  else
    bad "vocabulary: TriggerRule has $n members, not three"
  fi
else
  bad "vocabulary: $asm is missing"
fi

# ---------------------------------------------------------------------------
# 7. The criteria and coverage matrices are complete
# ---------------------------------------------------------------------------
# ADR 115 amended C5, C12, C26, the operational model, the performance envelope
# and system-level criterion 7 IN PLACE; it introduced no new numbered criterion.
# So the duty is to prove that claim, which is false the moment somebody adds a
# C29 — precisely when a row would be owed.
arch="docs/arch.md"
crit="docs/criteria-matrix.md"
cov="docs/coverage-matrix.md"
if [ -f "$arch" ] && [ -f "$crit" ] && [ -f "$cov" ]; then
  ids=$(grep -oE '^### C[0-9]+' "$arch" | sed 's/^### C//' | sort -n | uniq)
  expected=$(seq 1 28)
  if [ "$ids" = "$expected" ]; then
    pass "criteria: arch.md still numbers exactly C1..C28 (M10 added no numbered criterion)"
  else
    bad "criteria: arch.md's numbered criterion set changed — a new id owes a matrix row in both matrices"
    printf 'got:\n%s\n' "$ids"
  fi

  missing=""
  for n in $(seq 1 28); do
    c=$(grep -cF "| C$n |" "$crit")
    v=$(grep -cF "| C$n |" "$cov")
    [ "$c" -eq 1 ] || missing="$missing C$n(criteria=$c)"
    [ "$v" -eq 1 ] || missing="$missing C$n(coverage=$v)"
  done
  for sl in SL1 SL2 SL3 SL4a SL4b SL4c SL5 SL6 SL7 SL8machine SL8human; do
    c=$(grep -cF "| $sl |" "$crit")
    v=$(grep -cF "| $sl |" "$cov")
    [ "$c" -eq 1 ] || missing="$missing $sl(criteria=$c)"
    [ "$v" -eq 1 ] || missing="$missing $sl(coverage=$v)"
  done
  if [ -z "$missing" ]; then
    pass "criteria: every criterion appears exactly once in both matrices"
  else
    bad "criteria: a criterion is missing or duplicated:$missing"
  fi

  if grep -qF '| unmapped |' "$cov"; then
    bad "coverage: a machine criterion is still \`unmapped\` at the end of M10"
  else
    pass "coverage: no machine criterion is left unmapped at the end of M10"
  fi
else
  bad "criteria: one of $arch / $crit / $cov is missing"
fi

# ---------------------------------------------------------------------------
# 8. The RBAC an operator applies ships, and stays least-privilege
# ---------------------------------------------------------------------------
rbac="crates/k8s/manifests/dagr-orchestrator-rbac.yaml"
if [ -f "$rbac" ]; then
  if grep -qE '^kind: ClusterRole' "$rbac" || grep -qE '^kind: ClusterRoleBinding' "$rbac"; then
    bad "rbac: the orchestrator manifest declares a cluster-scoped grant — M10 is single-namespace by decision"
  else
    pass "rbac: the orchestrator grant is namespaced (Role + RoleBinding), never cluster-scoped"
  fi
  widened=$(grep -nE 'deletecollection|"\*"|pods/(log|exec|attach|portforward)|configmaps|secrets|persistentvolumeclaims|jobs|cronjobs|"update"|nodes|events' "$rbac" \
              | grep -vE '^[0-9]+:#')
  if [ -z "$widened" ]; then
    pass "rbac: no widened verb or second resource in the shipped Role"
  else
    bad "rbac: the shipped Role widened beyond pods + six verbs:"
    printf '%s\n' "$widened"
  fi
else
  bad "rbac: $rbac does not ship"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL M10 REMOTE-EXECUTION ACCEPTANCE-BOUNDARY CHECKS PASSED"
else
  echo "SOME M10 REMOTE-EXECUTION ACCEPTANCE-BOUNDARY CHECKS FAILED"
fi
exit "$fail"
