#!/usr/bin/env bash
# Crate-boundary and feature-gating checks for ticket 119 (T104) — the
# `dagr-blob` crate + the default-off `blob` cli feature.
#
# These are mechanical translations of the ticket's "Boundaries" Test-plan
# scenarios into crate-graph assertions cargo can prove but a unit test cannot:
#
#   * `dagr-blob` has NO dependencies at all — not on `dagr-core`, not on
#     `dagr-cli`, not on any third-party crate. That is what makes "a plain
#     `cargo build --all` compiles no storage dependency" true, and it is the
#     stronger form of the ticket's no-edge-onto-core requirement.
#   * `dagr-blob` is absent from `dagr-core`'s reverse-dependency tree.
#   * The `blob` cli feature is DEFAULT-OFF and its dependency is optional, so a
#     default build and `--no-default-features` reach neither the crate nor the
#     bridge; `--features blob` reaches both.
#   * `dagr-core`'s runtime dependency set is untouched at every feature setting.
#
# `dagr-blob` is a normal workspace member, so `cargo build --all` DOES build it
# directly — that is expected and is NOT what these checks forbid. What they
# forbid is an edge reachable through `dagr-cli` by default, and any edge onto
# `dagr-core` in either direction.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

# --- 1. dagr-blob is a member, and it depends on NOTHING ---------------------
bm="crates/blob/Cargo.toml"
if [ -f "$bm" ]; then
  pass "blob: Cargo.toml exists"
  grep -qE '"crates/blob"' Cargo.toml \
    && pass "blob: listed in [workspace].members" \
    || bad "blob: not listed in [workspace].members"

  # Any entry in a real dependency table is a violation: the crate's whole point
  # is that it compiles with no tree behind it, so this is asserted by ABSENCE of
  # entries rather than by an allow-list of expected ones.
  deps=$(awk '
    { line = $0; sub(/^[[:space:]]+/, "", line) }
    line ~ /^#/ { next }
    line ~ /^\[/ {
      s = line; sub(/^\[/, "", s); sub(/\][[:space:]]*$/, "", s); cur = s; next
    }
    cur ~ /^(dependencies|build-dependencies|target\..*\.dependencies)$/ && line ~ /=/ { print line }
  ' "$bm")
  if [ -z "$deps" ]; then
    pass "blob: declares NO dependencies (no core edge, no cli edge, no third-party crate)"
  else
    bad "blob: declares dependencies — 'cargo build --all compiles no storage dependency' is no longer true:"
    printf '%s\n' "$deps" | sed 's/^/        /'
  fi

  if grep -qE '^[[:space:]]*dagr-core[[:space:]]*=' "$bm"; then
    bad "blob: has a dependency edge onto dagr-core (violates the T104 boundary)"
  else
    pass "blob: has NO edge onto dagr-core (core's zero-runtime-dependency guarantee)"
  fi
  if grep -qE '^[[:space:]]*dagr-cli[[:space:]]*=' "$bm"; then
    bad "blob: has a dependency edge onto dagr-cli (the port knows nothing about verbs or runs)"
  else
    pass "blob: has NO edge onto dagr-cli"
  fi
else
  bad "blob: crates/blob/Cargo.toml missing"
fi

# --- 2. The bridge lives in dagr-cli, behind the feature ---------------------
# The blanket DurableOutput bridge needs BOTH DurableOutput and Payload (core's)
# and the BlobStore port, so it cannot live in dagr-blob without breaking the
# boundary above. Assert it is where it has to be, and gated.
bridge="crates/cli/src/blob_bridge.rs"
if [ -f "$bridge" ]; then
  pass "bridge: crates/cli/src/blob_bridge.rs exists (outside dagr-core, as the contract requires)"
  if grep -qE '^#\[cfg\(feature = "blob"\)\]' crates/cli/src/lib.rs \
     && grep -qE '^pub mod blob_bridge;' crates/cli/src/lib.rs; then
    pass "bridge: the module is declared behind #[cfg(feature = \"blob\")]"
  else
    bad "bridge: crates/cli/src/lib.rs does not declare blob_bridge behind the blob feature"
  fi
  grep -qE 'impl<T: Payload> DurableOutput for Blob<T>' "$bridge" \
    && pass "bridge: a generic DurableOutput impl covers every Payload" \
    || bad "bridge: no generic 'impl<T: Payload> DurableOutput for Blob<T>' found"
else
  bad "bridge: crates/cli/src/blob_bridge.rs missing"
fi

# dagr-core must hold no knowledge of the store: no mention of the blob crate
# anywhere in its sources.
core_mentions=$(grep -rln 'dagr_blob\|dagr-blob' crates/core/src 2>/dev/null || true)
if [ -z "$core_mentions" ]; then
  pass "core: holds no knowledge of the blob crate"
else
  bad "core: mentions the blob crate — the bridge must live outside core:"
  printf '%s\n' "$core_mentions" | sed 's/^/        /'
fi

# --- 3. cli has a DEFAULT-OFF blob feature -----------------------------------
cli="crates/cli/Cargo.toml"
if grep -qE '^[[:space:]]*blob[[:space:]]*=[[:space:]]*\[[[:space:]]*"dep:dagr-blob"' "$cli"; then
  pass "cli: has a blob = [\"dep:dagr-blob\"] feature"
else
  bad "cli: no blob feature forwarding to dep:dagr-blob"
fi
default_line=$(grep -E '^[[:space:]]*default[[:space:]]*=' "$cli" | head -1)
if printf '%s' "$default_line" | grep -q 'blob'; then
  bad "cli: blob is in the default feature set (must be DEFAULT-OFF)"
else
  pass "cli: blob is NOT in the default feature set (default-off)"
fi
if grep -qE '^[[:space:]]*dagr-blob[[:space:]]*=.*optional[[:space:]]*=[[:space:]]*true' "$cli"; then
  pass "cli: dagr-blob dependency is optional (no default edge)"
else
  bad "cli: dagr-blob dependency is not marked optional"
fi

# --- 4. crate-graph proof via `cargo tree` (if cargo is available) -----------
if command -v cargo >/dev/null 2>&1; then
  # `-e normal` is the RUNTIME graph cargo would actually build; an empty result
  # is guarded against everywhere below, because a query that ERRORS is otherwise
  # indistinguishable from a clean resolution and satisfies every absence check
  # at once.
  tree() {
    pkg=$1; shift
    cargo tree -p "$pkg" -e normal --prefix none "$@" 2>/dev/null \
      | awk '{print $1}' | grep -v '^$' | LC_ALL=C sort -u
  }

  # 4a. The blob crate resolves to ITSELF and nothing else.
  blob_tree=$(tree dagr-blob)
  if [ -z "$blob_tree" ]; then
    bad "blob(tree): cargo tree produced nothing — the assertion would be vacuous"
  else
    others=$(printf '%s\n' "$blob_tree" | grep -vx 'dagr-blob' || true)
    if [ -z "$others" ]; then
      pass "blob(tree): resolves to dagr-blob ALONE — it reaches neither dagr-core, dagr-cli, nor any third-party crate"
    else
      bad "blob(tree): dagr-blob acquired dependencies:"
      printf '%s' "$others" | sed 's/^/        /'
    fi
  fi

  # 4b. dagr-blob is absent from core's reverse-dependency tree.
  if cargo tree -i dagr-core -e normal --prefix none 2>/dev/null | grep -qE '^dagr-blob([[:space:]]|$)'; then
    bad "core(reverse): dagr-blob appears in dagr-core's reverse-dependency tree"
  else
    pass "core(reverse): dagr-blob is absent from dagr-core's reverse-dependency tree"
  fi

  # 4c. The default cli resolution has no dagr-blob edge; --features blob does.
  cli_default=$(tree dagr-cli)
  if [ -z "$cli_default" ]; then
    bad "cli(default): cargo tree produced nothing — the assertion would be vacuous"
  elif printf '%s\n' "$cli_default" | grep -qx 'dagr-blob'; then
    bad "cli(default): the default resolution reaches dagr-blob (the feature is default-off)"
  else
    pass "cli(default): reaches NO dagr-blob edge"
  fi
  cli_nodefault=$(tree dagr-cli --no-default-features)
  if [ -z "$cli_nodefault" ]; then
    bad "cli(no-default): cargo tree produced nothing — the assertion would be vacuous"
  elif printf '%s\n' "$cli_nodefault" | grep -qx 'dagr-blob'; then
    bad "cli(no-default): --no-default-features still reaches dagr-blob"
  else
    pass "cli(no-default): drops the dagr-blob edge"
  fi
  cli_blob=$(tree dagr-cli --features blob)
  if printf '%s\n' "$cli_blob" | grep -qx 'dagr-blob'; then
    pass "cli(--features blob): reaches dagr-blob (non-vacuous — the query does see this edge)"
  else
    bad "cli(--features blob): does not reach dagr-blob — the feature is not wired"
  fi

  # 4d. dagr-core reaches no runtime dependency at ANY feature setting, blob
  #     included. --all-features is the adversarial case.
  core_all=$(tree dagr-core --all-features)
  if [ -z "$core_all" ]; then
    bad "core(all-features): cargo tree produced nothing — the assertion would be vacuous"
  elif printf '%s\n' "$core_all" | grep -qx 'dagr-blob'; then
    bad "core(all-features): dagr-core reached dagr-blob"
  else
    pass "core(all-features): dagr-core still reaches no dagr-blob edge"
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
  pass "cargo unavailable — skipped the cargo-tree crate-graph proofs (manifest checks stand)"
fi

# --- 5. T110: the object-store client is quarantined behind `blob-s3` --------
#
# ADDED by ticket 125 (T110), which put an S3-compatible backend behind the same
# port. Nothing above is relaxed: `dagr-blob` still declares NO dependencies, and
# it still resolves to itself alone. That is possible because the backend is split
# — the PROTOCOL (canonical requests, SigV4 over the in-tree SHA-256/HMAC, status
# classification, paged listing, the bounded retry) lives in `dagr-blob` with no
# dependency table, and only the HTTPS TRANSPORT lives in `dagr-cli` behind a
# default-off feature.
#
# These checks assert the half that is new: the ticket's literal boundary
# requirement that `cargo build --all` and `--no-default-features` compile no
# HTTP/TLS stack or S3 client, that the containment survives `--features blob`
# (a pipeline on the local backend pays nothing for the object store), and that
# `--features blob-s3` DOES pull the client so none of the above is vacuous.
s3_stack="ureq ureq-proto rustls rustls-native-certs rustls-pki-types rustls-webpki ring webpki-roots hyper h2 reqwest aws-lc-rs"

# 5a. The feature is declared, default-off, and its dependencies are optional.
if grep -qE '^[[:space:]]*blob-s3[[:space:]]*=[[:space:]]*\[' "$cli"; then
  pass "cli: has a blob-s3 feature"
else
  bad "cli: no blob-s3 feature declared"
fi
if printf '%s' "$default_line" | grep -q 'blob-s3'; then
  bad "cli: blob-s3 is in the default feature set (must be DEFAULT-OFF)"
else
  pass "cli: blob-s3 is NOT in the default feature set (default-off)"
fi
# Is <dep> declared OPTIONAL in <manifest>'s `[dependencies]` table?
#
# It reads the WHOLE `<dep> = { … }` value rather than one line, because Cargo
# lets that value span lines and these three entries do (a `features = [ … ]`
# list). A same-line grep would report "not optional" for a manifest that is
# correct and merely wrapped — and, worse, would silently start passing again if
# someone reflowed it, which is a checker that tracks formatting instead of the
# invariant.
declares_optional() { # declares_optional <manifest> <dep>
  awk -v want="$2" '
    BEGIN { intable = 0; collecting = 0; buf = ""; found = 0 }
    /^[[:space:]]*\[/ {
      h = $0; sub(/^[[:space:]]*\[/, "", h); sub(/\].*$/, "", h)
      intable = (h == "dependencies"); collecting = 0; next
    }
    !intable { next }
    collecting {
      buf = buf " " $0
      if (buf ~ /}/) {
        collecting = 0
        if (buf ~ /optional[[:space:]]*=[[:space:]]*true/) { found = 1; exit }
      }
      next
    }
    {
      line = $0; sub(/^[[:space:]]+/, "", line)
      if (line ~ ("^" want "[[:space:]]*=")) {
        buf = line
        if (line ~ /\{/ && line !~ /}/) { collecting = 1; next }
        if (buf ~ /optional[[:space:]]*=[[:space:]]*true/) { found = 1; exit }
      }
    }
    END { exit(found ? 0 : 1) }
  ' "$1" 2>/dev/null
}
for dep in ureq rustls rustls-native-certs; do
  if declares_optional "$cli" "$dep"; then
    pass "cli: the $dep dependency is optional (no default edge)"
  else
    bad "cli: the $dep dependency is not marked optional — a default build would compile it"
  fi
done
# The predicate is non-vacuous, proved against planted manifests rather than by
# inspecting the real one: a multi-line optional entry must read as optional, and
# a non-optional one must not.
oprobe=$(mktemp -d 2>/dev/null) || oprobe=""
if [ -n "$oprobe" ]; then
  printf '[dependencies]\nureq = { version = "3", features = [\n  "x",\n], optional = true }\n' \
    >"$oprobe/yes.toml"
  printf '[dependencies]\nureq = { version = "3", features = [\n  "x",\n] }\n' >"$oprobe/no.toml"
  printf '[dev-dependencies]\nureq = { version = "3", optional = true }\n' >"$oprobe/dev.toml"
  if declares_optional "$oprobe/yes.toml" ureq; then
    pass "cli: the optional-dependency predicate reads a MULTI-LINE entry (it tracks the invariant, not the formatting)"
  else
    bad "cli: the optional-dependency predicate misses a multi-line optional entry — it is vacuous"
  fi
  if declares_optional "$oprobe/no.toml" ureq; then
    bad "cli: the optional-dependency predicate passes a NON-optional entry — it asserts nothing"
  else
    pass "cli: the optional-dependency predicate rejects a non-optional entry"
  fi
  if declares_optional "$oprobe/dev.toml" ureq; then
    bad "cli: the optional-dependency predicate reads the dev table (a dev edge is not the shipping edge)"
  else
    pass "cli: the optional-dependency predicate reads the [dependencies] table only"
  fi
  rm -rf "$oprobe"
fi

# 5b. `dagr-blob` still has no dependency at ANY feature setting. The manifest
#     check above reads the file; this reads the RESOLUTION, including the crate's
#     own `test-kit` feature and every other.
if command -v cargo >/dev/null 2>&1; then
  blob_all=$(tree dagr-blob --all-features)
  if [ -z "$blob_all" ]; then
    bad "blob(tree, all-features): cargo tree produced nothing — the assertion would be vacuous"
  else
    others=$(printf '%s\n' "$blob_all" | grep -vx 'dagr-blob' || true)
    if [ -z "$others" ]; then
      pass "blob(tree, all-features): STILL resolves to dagr-blob alone — the S3 protocol added no dependency"
    else
      bad "blob(tree, all-features): dagr-blob acquired dependencies:"
      printf '%s' "$others" | sed 's/^/        /'
    fi
  fi

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

  # 5c. The ticket's literal requirement, over the WHOLE workspace resolution.
  for leg in "default" "--no-default-features"; do
    if [ "$leg" = "default" ]; then ws=$(ws_tree); else ws=$(ws_tree --no-default-features); fi
    if [ -z "$ws" ]; then
      bad "workspace($leg): cargo tree produced nothing — the assertion would be vacuous"
    else
      hit=$(offenders "$ws" "$s3_stack")
      if [ -z "$hit" ]; then
        pass "workspace($leg): the whole-workspace resolution compiles NO HTTP/TLS stack or S3 client"
      else
        bad "workspace($leg): an HTTP/TLS stack entered the workspace resolution: $hit"
      fi
    fi
  done

  # 5d. `--features blob` — the local-backend path — still pulls none of it.
  cli_blob_only=$(tree dagr-cli --features blob)
  if [ -z "$cli_blob_only" ]; then
    bad "cli(--features blob): cargo tree produced nothing — the assertion would be vacuous"
  else
    hit=$(offenders "$cli_blob_only" "$s3_stack")
    if [ -z "$hit" ]; then
      pass "cli(--features blob): the local-backend path compiles NO HTTP/TLS stack (blob-s3 is a separate opt-in)"
    else
      bad "cli(--features blob): an HTTP/TLS stack rode in with the blob feature: $hit"
    fi
  fi

  # 5e. Non-vacuity: `--features blob-s3` DOES pull the client, and DOES imply
  #     `blob` (a transport with no port to serve is nothing).
  cli_s3=$(tree dagr-cli --features blob-s3)
  if printf '%s\n' "$cli_s3" | grep -qx 'ureq'; then
    pass "cli(--features blob-s3): reaches ureq (non-vacuous — the query does see this edge)"
  else
    bad "cli(--features blob-s3): does not reach ureq — the client feature is not wired"
  fi
  if printf '%s\n' "$cli_s3" | grep -qx 'dagr-blob'; then
    pass "cli(--features blob-s3): implies the blob feature (the port the transport serves)"
  else
    bad "cli(--features blob-s3): does not reach dagr-blob — a transport with no port"
  fi
  if printf '%s\n' "$cli_s3" | grep -qx 'webpki-roots'; then
    bad "cli(--features blob-s3): reached webpki-roots — its CDLA-Permissive-2.0 CA bundle is not in deny.toml's allow-list; roots come from the platform trust store"
  else
    pass "cli(--features blob-s3): does NOT reach webpki-roots (no new SPDX id; roots come from the platform trust store)"
  fi

  # 5f. core is untouched by the new feature, at every setting.
  if printf '%s\n' "$core_all" | grep -qxE 'ureq|rustls|ring'; then
    bad "core(all-features): dagr-core reached an HTTP/TLS crate"
  else
    pass "core(all-features): dagr-core still reaches no HTTP/TLS crate"
  fi
fi

# --- 6. T110: the S3 backend carries no credential surface -------------------
#
# dagr holds no credential of its own: credentials come from the ambient
# environment the platform already populated. A `--dagr.blob.*` flag or a
# `DAGR_BLOB_*` variable that named a secret would BE a credential surface, so the
# absence is asserted rather than left to review.
#
# The patterns require the name to appear as a QUOTED STRING LITERAL, because that
# is what DECLARING a flag or an environment variable looks like. Matching bare
# text would flag the comment in `crates/cli/src/config.rs` that says there is no
# such knob — a checker that reads prose has to be loosened the first time someone
# documents the invariant it exists to defend, which is exactly backwards.
cred_flags=$(grep -rEn -- '"--dagr\.blob\.[a-z.-]*(secret|key|password|token|credential)"' crates/ 2>/dev/null || true)
if [ -z "$cred_flags" ]; then
  pass "creds: no --dagr.blob.* flag names a secret (dagr adds no credential surface)"
else
  bad "creds: a --dagr.blob.* flag names a secret — dagr holds no credential of its own:"
  printf '%s\n' "$cred_flags" | sed 's/^/        /'
fi
cred_env=$(grep -rEn '"DAGR_BLOB_[A-Z_]*(SECRET|PASSWORD|TOKEN|ACCESS_KEY|CREDENTIAL)[A-Z_]*"' crates/ 2>/dev/null || true)
if [ -z "$cred_env" ]; then
  pass "creds: no DAGR_BLOB_* variable names a secret"
else
  bad "creds: a DAGR_BLOB_* variable names a secret:"
  printf '%s\n' "$cred_env" | sed 's/^/        /'
fi
# Non-vacuity: the two scans DO fire on a planted declaration, so "no output"
# means "none declared" rather than "the pattern matches nothing".
cprobe=$(mktemp -d 2>/dev/null) || cprobe=""
if [ -n "$cprobe" ]; then
  printf 'pub const F: &str = "--dagr.blob.secret-key";\n' >"$cprobe/flag.rs"
  printf 'pub const E: &str = "DAGR_BLOB_SECRET_ACCESS_KEY";\n' >"$cprobe/env.rs"
  printf '// there is no `--dagr.blob.access-key` and no `DAGR_BLOB_SECRET` here\n' >"$cprobe/prose.rs"
  hits=$(grep -rEn -- '"--dagr\.blob\.[a-z.-]*(secret|key|password|token|credential)"' "$cprobe" 2>/dev/null || true)
  hits="$hits$(grep -rEn '"DAGR_BLOB_[A-Z_]*(SECRET|PASSWORD|TOKEN|ACCESS_KEY|CREDENTIAL)[A-Z_]*"' "$cprobe" 2>/dev/null || true)"
  if printf '%s' "$hits" | grep -q 'flag.rs' && printf '%s' "$hits" | grep -q 'env.rs'; then
    pass "creds: the credential-surface scans fire on a planted declaration (non-vacuous)"
  else
    bad "creds: the credential-surface scans missed a planted declaration — they assert nothing"
  fi
  if printf '%s' "$hits" | grep -q 'prose.rs'; then
    bad "creds: the credential-surface scans flag PROSE about the invariant, not a declaration"
  else
    pass "creds: the credential-surface scans read declarations, not prose"
  fi
  rm -rf "$cprobe"
fi
# The secret is reachable through exactly one accessor, and exactly one caller
# uses it. A second call site is a review question, not a silent change.
secret_uses=$(grep -rn 'expose_secret()' crates/ 2>/dev/null | grep -v '^crates/blob/src/s3/creds.rs:' || true)
if [ "$(printf '%s\n' "$secret_uses" | grep -c 'sigv4.rs')" -le 1 ] \
   && [ -z "$(printf '%s\n' "$secret_uses" | grep -v 'sigv4.rs' | grep -v '^$')" ]; then
  pass "creds: the secret accessor has exactly one caller (the request signer)"
else
  bad "creds: the credential secret is read outside the request signer:"
  printf '%s\n' "$secret_uses" | sed 's/^/        /'
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL BLOB FEATURE-GATING CHECKS PASSED"
else
  echo "SOME BLOB FEATURE-GATING CHECKS FAILED"
fi
exit "$fail"
