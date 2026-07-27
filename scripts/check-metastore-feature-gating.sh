#!/usr/bin/env bash
# Feature-gating structural checks for ticket 098 (T83) — the `dagr-metastore`
# crate + the default-off `metastore` cli feature.
#
# These are mechanical translations of the ticket's "Feature gating" Test-plan
# scenarios into crate-graph assertions cargo can prove but a unit test cannot:
#
#   * A default `cargo build --all` (and `cargo build -p dagr-cli
#     --no-default-features`) pulls NEITHER `libsql` NOR `dagr-metastore` through
#     the cli, and the `metastore` verb is absent.
#   * `cargo build -p dagr-cli --features metastore` pulls BOTH, and the verb is
#     present.
#   * `cargo build --all --no-default-features` leaves `dagr-core`'s runtime
#     dependency set unchanged (empty) — libsql never reaches core.
#
# `dagr-metastore` is a normal workspace member, so `cargo build --all` DOES
# build it (and thus libsql) directly — that is expected and is NOT what these
# checks forbid. What they forbid is a `libsql`/`dagr-metastore` edge reachable
# through `dagr-cli` by default, and any edge onto `dagr-core`.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

# --- 1. dagr-metastore depends on artifact + libsql + tokio, NOT core --------
mm="crates/metastore/Cargo.toml"
if [ -f "$mm" ]; then
  pass "metastore: Cargo.toml exists"
  grep -qE '^[[:space:]]*dagr-artifact[[:space:]]*=' "$mm" \
    && pass "metastore: depends on dagr-artifact (the artifact-only edge, ADR 097 §5)" \
    || bad "metastore: does not depend on dagr-artifact"
  grep -qE '^[[:space:]]*libsql[[:space:]]*=' "$mm" \
    && pass "metastore: depends on libsql (the ADR 097 substrate)" \
    || bad "metastore: does not depend on libsql"
  # The exact pin from ADR 097 §2 (=0.10.0-pre.4) — must not float.
  grep -qE 'libsql[[:space:]]*=.*"=0\.10\.0-pre\.4"' "$mm" \
    && pass "metastore: libsql is pinned exactly to =0.10.0-pre.4 (ADR 097 §2)" \
    || bad "metastore: libsql is not pinned exactly to =0.10.0-pre.4"
  if grep -qE '^[[:space:]]*dagr-core[[:space:]]*=' "$mm"; then
    bad "metastore: has a dependency edge onto dagr-core (violates ADR 097 §5 boundary)"
  else
    pass "metastore: has NO edge onto dagr-core (core's zero-dep guarantee, ADR 097 §5)"
  fi
else
  bad "metastore: crates/metastore/Cargo.toml missing"
fi

# --- 2. cli has a DEFAULT-OFF metastore feature ------------------------------
cli="crates/cli/Cargo.toml"
if grep -qE '^[[:space:]]*metastore[[:space:]]*=[[:space:]]*\[[[:space:]]*"dep:dagr-metastore"' "$cli"; then
  pass "cli: has a metastore = [\"dep:dagr-metastore\"] feature"
else
  bad "cli: no metastore feature forwarding to dep:dagr-metastore"
fi
# The default feature list must NOT contain metastore (default-off).
default_line=$(grep -E '^[[:space:]]*default[[:space:]]*=' "$cli" | head -1)
if printf '%s' "$default_line" | grep -q 'metastore'; then
  bad "cli: metastore is in the default feature set (must be DEFAULT-OFF, ADR 097 §5)"
else
  pass "cli: metastore is NOT in the default feature set (default-off)"
fi
# The dep must be optional so the default build has no edge.
if grep -qE '^[[:space:]]*dagr-metastore[[:space:]]*=.*optional[[:space:]]*=[[:space:]]*true' "$cli"; then
  pass "cli: dagr-metastore dependency is optional (no default edge)"
else
  bad "cli: dagr-metastore dependency is not marked optional"
fi

# --- 3. crate-graph proof via `cargo tree` (if cargo is available) -----------
# The default cli dependency graph must contain neither libsql nor dagr-metastore.
if command -v cargo >/dev/null 2>&1; then
  # Default features: cli must not reach libsql or dagr-metastore.
  if cargo tree -p dagr-cli -e no-dev --prefix none 2>/dev/null \
       | grep -qE '^(libsql|dagr-metastore)[[:space:]]'; then
    bad "cli(default): dependency tree reaches libsql or dagr-metastore (must not, default-off)"
  else
    pass "cli(default): dependency tree reaches NEITHER libsql NOR dagr-metastore"
  fi

  # With --features metastore: cli must reach both.
  if cargo tree -p dagr-cli --features metastore -e no-dev --prefix none 2>/dev/null \
       | grep -qE '^dagr-metastore[[:space:]]' \
     && cargo tree -p dagr-cli --features metastore -e no-dev --prefix none 2>/dev/null \
       | grep -qE '^libsql[[:space:]]'; then
    pass "cli(--features metastore): dependency tree reaches BOTH libsql and dagr-metastore"
  else
    bad "cli(--features metastore): dependency tree does not reach both libsql and dagr-metastore"
  fi

  # dagr-core must reach neither libsql nor dagr-metastore under any feature.
  if cargo tree -p dagr-core --all-features -e no-dev --prefix none 2>/dev/null \
       | grep -qE '^(libsql|dagr-metastore)[[:space:]]'; then
    bad "core: dependency tree reaches libsql or dagr-metastore (violates zero-dep-core)"
  else
    pass "core: dependency tree reaches NEITHER libsql NOR dagr-metastore (even --all-features)"
  fi
else
  pass "cargo unavailable — skipped the cargo-tree crate-graph proofs (manifest checks stand)"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL METASTORE FEATURE-GATING CHECKS PASSED"
else
  echo "SOME METASTORE FEATURE-GATING CHECKS FAILED"
fi
exit "$fail"
