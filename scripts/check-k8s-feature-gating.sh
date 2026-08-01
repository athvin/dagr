#!/usr/bin/env bash
# Crate-boundary and feature-gating checks for ticket 122 (T107) — the `dagr-k8s`
# crate, its default-off `client` quarantine, and the default-off `k8s` cli
# feature.
#
# These are mechanical translations of the ticket's "Configuration and
# boundaries" Test-plan scenarios into crate-graph assertions cargo can prove but
# a unit test cannot:
#
#   * `dagr-k8s` is absent from `dagr-core`'s reverse-dependency tree, and its own
#     forward tree reaches neither core, cli, render, artifact, nor blob.
#   * `cargo build --all` and `--no-default-features` pull NO HTTP/TLS stack. This
#     is the strong form, and it is why the Kubernetes client sits behind a
#     default-off feature INSIDE this crate rather than being an unconditional
#     dependency of it: `dagr-k8s`'s default resolution is `dagr-k8s` + `tokio`,
#     and every one of hyper / rustls / kube / ring is absent from the whole
#     workspace resolution until a feature asks for it.
#   * The `k8s` cli feature is DEFAULT-OFF and its dependency optional, so the
#     default and `--no-default-features` cli resolutions reach neither the crate
#     nor a client; `--features k8s` reaches both (the non-vacuity leg).
#   * `dagr-core`'s runtime dependency set is untouched at every feature setting.
#
# `dagr-k8s` is a normal workspace member, so `cargo build --all` DOES build its
# default (client-free) surface — that is expected and is NOT what these checks
# forbid. What they forbid is an HTTP/TLS stack compiled by a default build, an
# edge reachable through `dagr-cli` by default, and any edge onto `dagr-core`.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

# The crates that make up an HTTP/TLS stack. A default build compiling any one of
# these is the guarantee broken, whichever crate pulled it.
http_tls="hyper hyper-util hyper-rustls rustls rustls-pki-types rustls-webpki ring aws-lc-rs tower tower-http kube kube-client kube-core k8s-openapi h2 tokio-rustls"

# --- 1. The crate exists, is a member, and keeps its boundary ----------------
km="crates/k8s/Cargo.toml"
if [ -f "$km" ]; then
  pass "k8s: Cargo.toml exists"
  grep -qE '"crates/k8s"' Cargo.toml \
    && pass "k8s: listed in [workspace].members" \
    || bad "k8s: not listed in [workspace].members"

  if grep -qE '^[[:space:]]*dagr-core[[:space:]]*=' "$km"; then
    bad "k8s: has a dependency edge onto dagr-core (violates the T107 boundary)"
  else
    pass "k8s: has NO edge onto dagr-core (core's zero-runtime-dependency guarantee)"
  fi
  if grep -qE '^[[:space:]]*dagr-cli[[:space:]]*=' "$km"; then
    bad "k8s: has a dependency edge onto dagr-cli (the observer knows nothing about verbs or runs)"
  else
    pass "k8s: has NO edge onto dagr-cli"
  fi

  # The quarantine, at the manifest: every client-tree dependency is optional and
  # the `client` feature is not in `default`.
  for dep in kube k8s-openapi rustls futures-util; do
    if grep -qE "^[[:space:]]*${dep}[[:space:]]*=.*optional[[:space:]]*=[[:space:]]*true" "$km"; then
      pass "k8s: $dep is an OPTIONAL dependency (no default edge)"
    else
      bad "k8s: $dep is not marked optional — a default build would compile it"
    fi
  done
  k8s_default=$(grep -E '^[[:space:]]*default[[:space:]]*=' "$km" | head -1)
  if printf '%s' "$k8s_default" | grep -q 'client'; then
    bad "k8s: the client feature is in the crate's default set (must be DEFAULT-OFF)"
  else
    pass "k8s: the client feature is NOT in the crate's default set"
  fi
  # T101 build trap 1: k8s-openapi must be pinned to the minor kube depends on,
  # or both copies resolve and the un-featured one's build script panics.
  if grep -qE '^[[:space:]]*k8s-openapi[[:space:]]*=.*"0\.25"' "$km" \
     && grep -qE '^[[:space:]]*k8s-openapi[[:space:]]*=.*v1_' "$km"; then
    pass "k8s: k8s-openapi is pinned to one minor with a Kubernetes version feature selected"
  else
    bad "k8s: k8s-openapi is not pinned with a v1_* feature — the build script panics or two copies resolve"
  fi
else
  bad "k8s: crates/k8s/Cargo.toml missing"
fi

# --- 2. The RBAC manifest ships with the crate ------------------------------
rbac="crates/k8s/manifests/pod-observer-rbac.yaml"
if [ -f "$rbac" ]; then
  pass "k8s: the least-privilege RBAC manifest ships at $rbac"
  if grep -q 'kind: ClusterRole' "$rbac"; then
    bad "k8s: the RBAC manifest grants a ClusterRole — the observer reads pods in ONE namespace"
  else
    pass "k8s: the RBAC manifest grants nothing cluster-wide"
  fi
else
  bad "k8s: the RBAC manifest is missing at $rbac"
fi

# --- 3. cli has a DEFAULT-OFF k8s feature ------------------------------------
cli="crates/cli/Cargo.toml"
if grep -qE '^[[:space:]]*k8s[[:space:]]*=[[:space:]]*\[[[:space:]]*"dep:dagr-k8s"' "$cli"; then
  pass "cli: has a k8s = [\"dep:dagr-k8s\"] feature"
else
  bad "cli: no k8s feature forwarding to dep:dagr-k8s"
fi
cli_default_line=$(grep -E '^[[:space:]]*default[[:space:]]*=' "$cli" | head -1)
if printf '%s' "$cli_default_line" | grep -q 'k8s'; then
  bad "cli: k8s is in the default feature set (must be DEFAULT-OFF)"
else
  pass "cli: k8s is NOT in the default feature set (default-off)"
fi
if grep -qE '^[[:space:]]*dagr-k8s[[:space:]]*=.*optional[[:space:]]*=[[:space:]]*true' "$cli"; then
  pass "cli: dagr-k8s dependency is optional (no default edge)"
else
  bad "cli: dagr-k8s dependency is not marked optional"
fi

# --- 4. dagr-core holds no knowledge of the observer -------------------------
core_mentions=$(grep -rln 'dagr_k8s\|dagr-k8s' crates/core/src 2>/dev/null || true)
if [ -z "$core_mentions" ]; then
  pass "core: holds no knowledge of the k8s crate"
else
  bad "core: mentions the k8s crate — the observer must live outside core:"
  printf '%s\n' "$core_mentions" | sed 's/^/        /'
fi

# --- 5. crate-graph proof via `cargo tree` -----------------------------------
if command -v cargo >/dev/null 2>&1; then
  # `-e normal` is the RUNTIME graph cargo would actually build. Every query is
  # guarded against an empty result, because a query that ERRORS is otherwise
  # indistinguishable from a clean resolution and satisfies every absence check at
  # once — which would turn this whole file green for the wrong reason.
  tree() {
    pkg=$1; shift
    cargo tree -p "$pkg" -e normal --prefix none "$@" 2>/dev/null \
      | awk '{print $1}' | grep -v '^$' | LC_ALL=C sort -u
  }
  ws_tree() {
    cargo tree --workspace -e normal --prefix none "$@" 2>/dev/null \
      | awk '{print $1}' | grep -v '^$' | LC_ALL=C sort -u
  }
  offenders() { # offenders "<tree>" "<forbidden list>"
    _found=""
    for _f in $2; do
      if printf '%s\n' "$1" | grep -qx "$_f"; then _found="$_found$_f "; fi
    done
    printf '%s' "$_found"
  }

  # 5a. The k8s crate's DEFAULT resolution: itself plus tokio's minimal tree, and
  #     no HTTP/TLS crate anywhere in it.
  k8s_tree=$(tree dagr-k8s)
  if [ -z "$k8s_tree" ]; then
    bad "k8s(tree): cargo tree produced nothing — the assertion would be vacuous"
  else
    hit=$(offenders "$k8s_tree" "$http_tls")
    if [ -z "$hit" ]; then
      pass "k8s(tree, default): reaches NO HTTP/TLS crate"
    else
      bad "k8s(tree, default): reached an HTTP/TLS crate: $hit"
    fi
    bad_edge=$(printf '%s\n' "$k8s_tree" | grep -xE 'dagr-core|dagr-cli|dagr-render|dagr-artifact|dagr-blob|dagr-metastore' || true)
    if [ -z "$bad_edge" ]; then
      pass "k8s(tree): reaches no other dagr crate (no core, cli, render, artifact, blob or metastore edge)"
    else
      bad "k8s(tree): reached another dagr crate:"
      printf '%s\n' "$bad_edge" | sed 's/^/        /'
    fi
  fi

  # 5b. Non-vacuity: --features client DOES pull the stack, so the query above is
  #     looking at something.
  k8s_client=$(tree dagr-k8s --features client)
  if printf '%s\n' "$k8s_client" | grep -qx 'kube'; then
    pass "k8s(--features client): reaches kube (non-vacuous — the query does see this edge)"
  else
    bad "k8s(--features client): does not reach kube — the client feature is not wired"
  fi

  # 5c. dagr-k8s is absent from core's reverse-dependency tree.
  if cargo tree -i dagr-core --prefix none 2>/dev/null | grep -qE '^dagr-k8s([[:space:]]|$)'; then
    bad "core(reverse): dagr-k8s appears in dagr-core's reverse-dependency tree"
  else
    pass "core(reverse): dagr-k8s is absent from dagr-core's reverse-dependency tree"
  fi

  # 5d. `cargo build --all` (the DEFAULT whole-workspace resolution) and
  #     `--no-default-features` compile no HTTP/TLS stack at all. This is the
  #     ticket's literal requirement and the reason the client feature lives
  #     inside this crate rather than being unconditional.
  for leg in "default" "--no-default-features"; do
    if [ "$leg" = "default" ]; then ws=$(ws_tree); else ws=$(ws_tree --no-default-features); fi
    if [ -z "$ws" ]; then
      bad "workspace($leg): cargo tree produced nothing — the assertion would be vacuous"
    else
      hit=$(offenders "$ws" "$http_tls")
      if [ -z "$hit" ]; then
        pass "workspace($leg): the whole-workspace resolution compiles NO HTTP/TLS stack"
      else
        bad "workspace($leg): an HTTP/TLS stack entered the workspace resolution: $hit"
      fi
    fi
  done

  # 5e. The cli's default and no-default resolutions carry no dagr-k8s edge;
  #     --features k8s does.
  cli_default=$(tree dagr-cli)
  if [ -z "$cli_default" ]; then
    bad "cli(default): cargo tree produced nothing — the assertion would be vacuous"
  elif printf '%s\n' "$cli_default" | grep -qx 'dagr-k8s'; then
    bad "cli(default): the default resolution reaches dagr-k8s (the feature is default-off)"
  else
    pass "cli(default): reaches NO dagr-k8s edge"
  fi
  cli_nodefault=$(tree dagr-cli --no-default-features)
  if [ -z "$cli_nodefault" ]; then
    bad "cli(no-default): cargo tree produced nothing — the assertion would be vacuous"
  elif printf '%s\n' "$cli_nodefault" | grep -qx 'dagr-k8s'; then
    bad "cli(no-default): --no-default-features still reaches dagr-k8s"
  else
    pass "cli(no-default): drops the dagr-k8s edge"
  fi
  cli_k8s=$(tree dagr-cli --features k8s)
  if printf '%s\n' "$cli_k8s" | grep -qx 'dagr-k8s'; then
    pass "cli(--features k8s): reaches dagr-k8s (non-vacuous — the query does see this edge)"
  else
    bad "cli(--features k8s): does not reach dagr-k8s — the feature is not wired"
  fi
  if printf '%s\n' "$cli_k8s" | grep -qx 'kube'; then
    pass "cli(--features k8s): reaches the client through it (the feature forwards to dagr-k8s/client)"
  else
    bad "cli(--features k8s): does not reach kube — the cli feature does not turn the client on"
  fi

  # 5f. dagr-core reaches no runtime dependency at ANY feature setting, k8s
  #     included. --all-features is the adversarial case.
  core_all=$(tree dagr-core --all-features)
  if [ -z "$core_all" ]; then
    bad "core(all-features): cargo tree produced nothing — the assertion would be vacuous"
  elif printf '%s\n' "$core_all" | grep -qxE 'dagr-k8s|kube|rustls|hyper'; then
    bad "core(all-features): dagr-core reached the observer or its client tree"
  else
    pass "core(all-features): dagr-core still reaches neither dagr-k8s nor a client"
  fi
  core_nodefault=$(tree dagr-core --no-default-features)
  others=$(printf '%s\n' "$core_nodefault" | grep -vx 'dagr-core' || true)
  if [ -z "$core_nodefault" ]; then
    bad "core(no-default): cargo tree produced nothing — the assertion would be vacuous"
  elif [ -z "$others" ]; then
    pass "core(no-default): dagr-core's runtime dependency set is still EMPTY"
  else
    bad "core(no-default): dagr-core acquired runtime dependencies:"
    printf '%s' "$others" | sed 's/^/        /'
  fi
else
  bad "cargo unavailable — cannot run the crate-graph boundary proofs (they are load-bearing for T107)"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL K8S FEATURE-GATING CHECKS PASSED"
else
  echo "SOME K8S FEATURE-GATING CHECKS FAILED"
fi
exit "$fail"
