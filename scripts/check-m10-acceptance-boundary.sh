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

# Every shipped file that CONSTRUCTS a Kubernetes API object, discovered rather
# than hand-listed. This is the correction to a real hole: the scans below used to
# name `crates/k8s/src/executor.rs` alone, and `crates/k8s/src/client.rs`'s
# `pod_object` — the ONLY translation of a `PodSpec` into a `Pod` the API server
# ever sees — was unscanned, so an `EnvVar` planted there kept both assertions
# green. Discovery by `k8s_openapi` reference means a NEW builder file is covered
# the day it lands rather than the day somebody remembers to add it here.
apiobjects=$(grep -rlI 'k8s_openapi' crates/*/src 2>/dev/null | sort)

# The SHIPPED half of a source file: everything above its `#[cfg(test)]` module.
# A file's own tests name the very surfaces these scans forbid — in order to assert
# their absence — so scanning them would fail the gate on its own coverage.
shipped_half() { awk '/^#\[cfg\(test\)\]/{exit} {print}' "$1"; }

# grep an extended regex over the shipped half of each file, keeping `file:line:`.
scan_shipped() {
  re=$1
  shift
  for f in "$@"; do
    shipped_half "$f" | grep -nEI "$re" | sed "s|^|$f:|"
  done
}

if [ -z "$apiobjects" ]; then
  bad "no file under crates/*/src references k8s_openapi — the API-object scans below would be vacuous"
elif ! printf '%s\n' "$apiobjects" | grep -q 'crates/k8s/src/client.rs'; then
  bad "the API-object scan did not find crates/k8s/src/client.rs, which builds the Pod — the discovery is broken"
else
  pass "pod spec: the API-object scan covers $(printf '%s\n' "$apiobjects" | wc -l | tr -d ' ') file(s), including the one that builds the Pod"
fi

missing=""
for f in $podside; do [ -f "$f" ] || missing="$missing $f"; done
if [ -n "$missing" ]; then
  bad "the pod-path sources are missing:$missing — the scan below would be vacuous"
else
  # shellcheck disable=SC2086 # both lists are whitespace-separated paths, by design
  hits=$(scan_shipped 'dagr_metastore|dagr-metastore|libsql|OpenMode|MetastoreWriter|run_index' $podside $apiobjects)
  if [ -z "$hits" ]; then
    # NARROW, and stated at the strength it is checked at. This is a scan of three
    # named files plus every API-object builder, NOT a link-graph fact: the pod
    # re-enters the SAME binary, so at link granularity a `--features metastore`
    # build genuinely does contain the metastore, and the claim can only ever be
    # about what the pod-side code PATH names. The module-edge pin below is what
    # keeps that narrowness honest.
    pass "pod path: the \`exec-node\` verb, the shard writer and every pod/API-object builder NAME no metastore symbol"
  else
    bad "pod path: a metastore symbol is named on the pod path:"
    printf '%s\n' "$hits"
  fi

  # The module-edge pin. A file scan cannot see a wrapper — `crate::index::record(
  # &cfg.endpoint, &cfg.token)` names no metastore symbol and no credential — so the
  # set of crate-internal modules the pod path reaches is pinned instead. A new edge
  # is a gate failure that a reviewer clears by re-pinning, having looked at what
  # the new module does.
  pinned_edges="config contract driver graph registry run_flow run_store shard signals"
  actual_edges=$(grep -ohE 'crate::[a-z_][a-z0-9_]*' crates/cli/src/exec_node.rs crates/cli/src/shard.rs 2>/dev/null \
                   | sed 's/^crate:://' | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//')
  if [ "$actual_edges" = "$pinned_edges" ]; then
    pass "pod path: reaches exactly the pinned crate-internal modules ($pinned_edges)"
  else
    bad "pod path: its crate-internal module edges changed — review what the new one does, then re-pin"
    printf 'pinned: %s\nactual: %s\n' "$pinned_edges" "$actual_edges"
  fi

  # And no credential: nothing on the pod path or in an API object names a run-index
  # database URL, a DSN, or an auth token it would have to pass into a container.
  # Deliberately narrow on the SYMBOLS — `crates/k8s/src/executor.rs` carries a list
  # of credential MARKERS it refuses, and a scan that read those as leaks would be
  # asserting the opposite of the invariant.
  # shellcheck disable=SC2086 # both lists are whitespace-separated paths, by design
  creds=$(scan_shipped 'DATABASE_URL|LIBSQL_URL|DAGR_METASTORE|libsql://|postgres://|\bdsn\b' $podside $apiobjects)
  if [ -z "$creds" ]; then
    pass "pod path: no database credential is plumbed into a pod (the pod path and every API-object builder)"
  else
    bad "pod path: a database-credential symbol appears on the pod path or in an API object:"
    printf '%s\n' "$creds"
  fi

  # The positive half, and the reason the scan above can be narrow: the pod spec
  # builder REFUSES a credential-bearing reference outright, so a payload URI
  # that carries one never reaches a container's argv.
  #
  # Asserted BEHAVIOURALLY. A `grep -q` for the function's name was a name-presence
  # check: inverting the body to `return Ok(())` kept it green, which is exactly the
  # regression the claim exists to catch. The suite below is `dagr-k8s`'s default
  # feature set, so it costs no HTTP/TLS tree.
  if cargo test -q -p dagr-k8s --test pod_executor -- \
       a_presigned_or_otherwise_secret_bearing_url_is_rejected_before_it_can_be_recorded \
       an_opaque_blob_reference_carries_no_credential_and_is_accepted >/dev/null 2>&1; then
    pass "pod path: a credential-bearing reference is REFUSED (behaviour, not the presence of a function name)"
  else
    bad "pod path: the credential-bearing-reference refusal does not hold — rerun \`cargo test -p dagr-k8s --test pod_executor\` for the detail"
  fi
fi

# The pod's own container spec must carry no environment-variable plumbing that
# could smuggle one: `PodSpec` states the image, the command, the size and the
# labels, and the emitted `Pod` states exactly that and nothing more.
if [ -n "$apiobjects" ]; then
  # shellcheck disable=SC2086 # a whitespace-separated path list, by design
  env_hits=$(scan_shipped 'EnvVar|env_vars|env_from|envFrom|secretKeyRef|configMapKeyRef|SecretKeySelector|ConfigMapKeySelector|volume_mounts|volumeMounts|VolumeMount|image_pull_secrets|imagePullSecrets|service_account_name|serviceAccountName' $apiobjects)
  if [ -z "$env_hits" ]; then
    pass "pod spec: no environment, secret, configmap or volume plumbing in any API-object builder (payloads travel through the blob store, never through the API server)"
  else
    bad "pod spec: an API-object builder grew an environment/secret/volume plumbing surface — a pod's inputs are references it is GIVEN, never a credential"
    printf '%s\n' "$env_hits"
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
  # libSQL's remote surface, spelled every way the crate offers it. The first list
  # named three calls and missed the API a real wiring would most likely use —
  # `libsql::Builder::new_remote(..)` — which is the whole failure mode this check
  # exists to prevent, so the builder constructors are named explicitly.
  wired=$(shipped_half "$store" | grep -nEI 'embedded_replica\(|sync_from_remote|Database::open_remote|Builder::new_remote|new_remote\(|new_remote_replica|new_synced_database|new_local_replica|open_with_remote_sync|remote_writes|SyncedDatabase|sync_interval\(')
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
# Every way a process can start accepting: the socket TYPES (so a listener built
# by `from_std`, `from_raw_fd` or any other constructor is caught, not only one
# built by `::bind(`), and the accept/serve verbs.
#
# Two alternation gaps were proven and are closed here: a Unix-domain listener
# (`tokio::net::UnixListener::from_std`) matched nothing, and a scheduler type
# whose name did not END in `Scheduler` (`struct GlobalScheduler2;`) slipped the
# suffix-bound pattern. Sockets are named by TYPE as well as by constructor, so a
# listener built by any constructor is caught.
listener_types='TcpListener|UdpSocket|UnixListener|UnixDatagram|TcpSocket|NamedPipeServer|::bind\(|\.serve\(|\.serve_|\.listen\(|\.incoming\('
forbidden_all="$listener_types"'|axum|actix|warp::|hyper::(Server|server)|tonic::|pgwire|(struct|enum|trait|impl)[[:space:]]+[A-Za-z_0-9]*Scheduler[A-Za-z_0-9]*'
# The base is the last M9 commit (the T99 done-marker), an immutable ancestor of
# this branch. In CI `actions/checkout` makes a shallow clone that holds only the
# tip, so DEEPEN it until the marker is in the local object store — the same
# treatment `check-metastore-acceptance-boundary.sh` applies, and for the same
# reason: the diff base must be byte-identical to the dev box's.
#
# There is NO fallback base, deliberately. Falling back to `origin/main` narrowed
# the scan window to whatever this branch alone added, while the non-vacuity guard
# below still passed — so the section reported a green it had not earned. An
# unreachable marker is a SETUP failure and says so.
m9_marker=d11de14
if ! git rev-parse --verify --quiet "${m9_marker}^{commit}" >/dev/null 2>&1 \
   && [ "$(git rev-parse --is-shallow-repository 2>/dev/null)" = "true" ]; then
  git fetch --quiet --deepen=500 origin >/dev/null 2>&1 || true
fi
m9_base=""
if git rev-parse --verify --quiet "${m9_marker}^{commit}" >/dev/null 2>&1; then
  m9_base=$m9_marker
fi

if [ -z "$m9_base" ]; then
  bad "the M9 marker ${m9_marker} is unreachable, so the M9->M10 diff cannot be taken. REFUSING to fall back to main: that silently narrows the scan window to this branch while every guard below still passes. Deepen the clone (\`git fetch --deepen=500 origin\`) and rerun."
else
  # Added lines only (`^+`), over SHIPPED source (`crates/*/src/*`) — never tests
  # or examples, because a gate's own test naturally mentions these words in
  # negation. A REAL violation is a bound socket, a served endpoint, a server
  # framework, or a scheduler type. dagr's Kubernetes client makes OUTBOUND calls
  # and holds ONE watch; it accepts nothing.
  #
  # The diff runs base..WORKING TREE, not base..HEAD. `..HEAD` could only ever see
  # committed content, so a violation sitting in the tree was invisible to it — a
  # gate that a developer cannot make fail before committing is a gate that teaches
  # nothing. Untracked files are still outside a `git diff`, which is why the
  # unconditional whole-tree scan below runs the SAME pattern.
  diff_lines=$(git diff "$m9_base" -- 'crates/*/src/*' 2>/dev/null | grep -cE '^\+' || true)
  if [ "${diff_lines:-0}" -lt 100 ]; then
    bad "the M9->M10 shipped-source diff from ${m9_base} added only ${diff_lines:-0} lines — the scan would be near-vacuous; is the base right?"
  fi
  hits=$(git diff "$m9_base" -- 'crates/*/src/*' 2>/dev/null \
           | grep -E '^\+' \
           | grep -vE '^\+\+\+' \
           | grep -EI "$forbidden_all")
  if [ -z "$hits" ]; then
    pass "M9->M10 diff (from ${m9_base}, ${diff_lines} added shipped-source lines): NO listener, no server framework, no scheduler type"
  else
    bad "M9->M10 diff adds a forbidden inbound/coordination surface in shipped source:"
    printf '%s\n' "$hits"
  fi
fi

# The unconditional half, independent of any diff base and of git entirely: no
# shipped source anywhere binds a socket, serves an endpoint, links a server
# framework or declares a scheduler type. It runs the SAME pattern as the diff
# above, so an untracked new file — which no `git diff` can see — is covered too.
listener=$(grep -rnEI "$forbidden_all" crates/*/src 2>/dev/null)
if [ -z "$listener" ]; then
  pass "shipped source (whole tree, tracked or not): binds no socket, serves no endpoint, links no server framework and declares no scheduler type"
else
  bad "shipped source binds a socket, serves an endpoint, or declares a coordination surface:"
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
#
# The variant pattern counts all three declaration forms — `Name,`, `Name(T),` and
# `Name {` — because the earlier unit-only form could not see a data-carrying
# variant at all: `Displaced(String),` and `AnyOf(u8),` were both invisible to it,
# which made the "the gate script alone catches a tenth variant" claim false as
# written. Doc lines (`///`) and attributes (`#[`) never start with an uppercase
# letter, so neither is counted.
variant='^[[:space:]]+[A-Z][A-Za-z0-9]*[[:space:]]*(,|\(|\{|=)'
ctx="crates/core/src/context.rs"
if [ -f "$ctx" ]; then
  n=$(awk '/^pub enum TerminalState/{f=1;next} f&&/^}/{exit} f' "$ctx" \
        | grep -cE "$variant")
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
        | grep -cE "$variant")
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
  # The manifest's comments explain what is deliberately ABSENT and therefore name
  # every forbidden string; scanning them would fail on the file's own rationale.
  # Body = every non-comment, non-blank line, which is what actually gets applied.
  rbac_body=$(grep -vE '^[[:space:]]*#' "$rbac" | grep -vE '^[[:space:]]*$')

  # `kind:` is matched WHEREVER it appears, not only at column 0. A `roleRef` names
  # its kind indented by two spaces, so the old `^kind: ClusterRole` anchor could
  # not see a binding that pointed at `ClusterRole/cluster-admin` — the single most
  # effective way to turn a least-privilege manifest into a cluster-admin grant.
  if printf '%s\n' "$rbac_body" | grep -qE '\bkind:[[:space:]]*ClusterRole'; then
    bad "rbac: the orchestrator manifest names a cluster-scoped kind (a ClusterRole object, or a roleRef pointing at one) — M10 is single-namespace by decision"
    printf '%s\n' "$rbac_body" | grep -nE '\bkind:[[:space:]]*ClusterRole'
  else
    pass "rbac: the orchestrator grant is namespaced (Role + RoleBinding), and its roleRef points at a Role"
  fi

  # A wildcard in ANY quoting. The old `"\*"` matched double quotes only, so
  # `verbs: ['*']` — valid YAML, and a grant of every verb — passed.
  if printf '%s\n' "$rbac_body" | grep -qE '\*'; then
    bad "rbac: the shipped Role contains a wildcard:"
    printf '%s\n' "$rbac_body" | grep -nE '\*'
  else
    pass "rbac: no wildcard verb, resource or apiGroup, in any quoting"
  fi

  widened=$(printf '%s\n' "$rbac_body" \
              | grep -nE "deletecollection|pods/(log|exec|attach|portforward|status|eviction|ephemeralcontainers)|configmaps|secrets|persistentvolumeclaims|serviceaccounts|endpoints|jobs|cronjobs|nodes|events|['\"](update|bind|escalate|impersonate)['\"]")
  if [ -z "$widened" ]; then
    pass "rbac: no widened verb or second resource in the shipped Role"
  else
    bad "rbac: the shipped Role widened beyond pods + six verbs:"
    printf '%s\n' "$widened"
  fi

  # An ALLOW-list, which a deny-list cannot replace: a second rule granting
  # `pods/status` plus `serviceaccounts` plus `endpoints` is refused by the list
  # above, but a second rule granting something nobody thought to forbid is not.
  # Exactly one rule, one resource list, one verb list, each spelled exactly.
  rules=$(printf '%s\n' "$rbac_body" | grep -cE '^[[:space:]]*-[[:space:]]*apiGroups:')
  resources=$(printf '%s\n' "$rbac_body" | grep -cE '^[[:space:]]*resources:')
  verbs=$(printf '%s\n' "$rbac_body" | grep -cE '^[[:space:]]*verbs:')
  exact_resources=$(printf '%s\n' "$rbac_body" | grep -cF 'resources: ["pods"]')
  exact_verbs=$(printf '%s\n' "$rbac_body" | grep -cF 'verbs: ["create", "delete", "get", "list", "patch", "watch"]')
  if [ "$rules" -eq 1 ] && [ "$resources" -eq 1 ] && [ "$verbs" -eq 1 ] \
     && [ "$exact_resources" -eq 1 ] && [ "$exact_verbs" -eq 1 ]; then
    pass "rbac: exactly one rule, granting exactly [\"pods\"] and exactly the six verbs"
  else
    bad "rbac: the Role is no longer one rule of six verbs on pods (rules=$rules resources=$resources verbs=$verbs exact_resources=$exact_resources exact_verbs=$exact_verbs)"
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
