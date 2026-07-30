#!/usr/bin/env bash
# The feature matrix is additive, and the no-default leg proves it — ticket 113 (T98).
#
# dagr's Cargo features are not conveniences; three architectural guarantees are
# *implemented* as feature boundaries, and each one is only as true as the
# resolution that produces it:
#
#   * `dagr-core` carries an EMPTY runtime dependency set. Its single edge is a
#     build-time, optional, default-on path onto the `dagr-macros` proc-macro
#     crate, which `--no-default-features` drops outright (arch.md "Stability";
#     ADR 081/082).
#   * `metastore` is DEFAULT-OFF, so a default build pulls neither
#     `dagr-metastore` nor the heavy pre-1.0 `libsql` (ADR 097 §5).
#   * `dag` confines the runtime `inventory` dependency to `dagr-cli` behind an
#     optional feature, so it can never reach `dagr-core` (ADR 092).
#
# A BUILD SUCCEEDING PROVES NONE OF THAT. `cargo build --no-default-features`
# compiles just as happily with an unwanted edge present as without one; what
# carries the claim is the RESOLVED GRAPH. So this script asserts the graph, and
# the CI job that runs it builds both ends of the matrix around it.
#
# `cargo tree -e normal` reports the RUNTIME (normal) dependency graph — it
# excludes dev-dependencies and, being the resolution cargo would actually build,
# it accounts for feature unification rather than re-deriving it from manifests.
# The manifest-level assertions live in check-workspace-skeleton.sh and
# check-metastore-feature-gating.sh; this is the resolution-level complement.
#
# Run from the repository root. Exit 0 = the matrix holds, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if ! command -v cargo >/dev/null 2>&1; then
  echo "FAIL  cargo is not on PATH — the resolved graph cannot be asserted"
  exit 1
fi

# tree PKG [extra cargo flags...] -> the resolved runtime package names, one per line.
tree() {
  pkg=$1; shift
  cargo tree -p "$pkg" -e normal --prefix none "$@" 2>/dev/null \
    | awk '{print $1}' | grep -v '^$' | LC_ALL=C sort -u
}

# --- 1. The no-default leg: dagr-core resolves to itself, and nothing else ---
#
# This is the standing proof of the zero-runtime-dependency guarantee. Until
# T98 it rested on two shell scripts scoped to the metastore, neither of which
# looked at the no-default resolution of core itself.
core_nodefault=$(tree dagr-core --no-default-features)
if [ -z "$core_nodefault" ]; then
  bad "no-default: cargo tree produced nothing for dagr-core — the assertion is vacuous"
else
  offending=""
  for forbidden in dagr-macros inventory libsql dagr-metastore; do
    if printf '%s\n' "$core_nodefault" | grep -qx "$forbidden"; then
      offending="$offending$forbidden
"
    fi
  done
  if [ -z "$offending" ]; then
    pass "no-default: dagr-core resolves with no dagr-macros / inventory / libsql / dagr-metastore edge"
  else
    bad "no-default: dagr-core resolved onto forbidden crates:"
    printf '%s' "$offending" | sed 's/^/        /'
  fi

  # Stronger, and the actual commitment: the resolution is dagr-core ALONE.
  others=$(printf '%s\n' "$core_nodefault" | grep -vx 'dagr-core' || true)
  if [ -z "$others" ]; then
    pass "no-default: dagr-core's runtime dependency set is EMPTY (arch.md Stability)"
  else
    bad "no-default: dagr-core acquired runtime dependencies:"
    printf '%s' "$others" | sed 's/^/        /'
  fi
fi

# --- 2. The scan is non-vacuous: the default leg DOES show the macros edge ---
#
# If `cargo tree` silently reported nothing, check 1 would pass for the wrong
# reason. The default resolution of the same crate must contain the edge that
# `--no-default-features` removes, which proves the query sees edges at all.
core_default=$(tree dagr-core)
if printf '%s\n' "$core_default" | grep -qx 'dagr-macros'; then
  pass "non-vacuous: the DEFAULT resolution of dagr-core does contain dagr-macros (so the query sees edges)"
else
  bad "non-vacuous: dagr-macros is absent from the default resolution too — the query is not seeing edges"
fi

# --- 3. dagr-core never reaches the heavy crates, at ANY feature setting -----
#
# `--all-features` is the adversarial case: it is the one resolution in which
# every optional edge in the workspace is switched on at once.
core_all=$(tree dagr-core --all-features)
offending=""
for forbidden in inventory libsql dagr-metastore tokio clap rayon; do
  if printf '%s\n' "$core_all" | grep -qx "$forbidden"; then
    offending="$offending$forbidden
"
  fi
done
if [ -z "$offending" ]; then
  pass "all-features: dagr-core still reaches no runtime dependency (only the build-time proc-macro edge)"
else
  bad "all-features: dagr-core reached a runtime dependency:"
  printf '%s' "$offending" | sed 's/^/        /'
fi

# --- 4. The cli's default leg is free of the default-off metastore stack -----
cli_default=$(tree dagr-cli)
offending=""
for forbidden in libsql dagr-metastore; do
  if printf '%s\n' "$cli_default" | grep -qx "$forbidden"; then
    offending="$offending$forbidden
"
  fi
done
if [ -z "$offending" ]; then
  pass "default: dagr-cli reaches neither libsql nor dagr-metastore (metastore is default-off, ADR 097 §5)"
else
  bad "default: the default dagr-cli resolution pulled the default-off metastore stack:"
  printf '%s' "$offending" | sed 's/^/        /'
fi

# `inventory` is the inverse case: default-ON behind `dag`, and dropped by
# `--no-default-features`. Both halves are asserted so neither can silently flip.
if printf '%s\n' "$cli_default" | grep -qx 'inventory'; then
  pass "default: dagr-cli reaches inventory (the default-on dag feature, ADR 092)"
else
  bad "default: dagr-cli does not reach inventory — the default-on dag feature is not wired"
fi
cli_nodefault=$(tree dagr-cli --no-default-features)
if printf '%s\n' "$cli_nodefault" | grep -qx 'inventory'; then
  bad "no-default: dagr-cli still reaches inventory — the dag feature does not confine it"
else
  pass "no-default: dagr-cli drops inventory (the dag feature confines it, ADR 092)"
fi

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "FEATURE_MATRIX=PASS"
else
  echo "FEATURE_MATRIX=FAIL"
fi
exit "$fail"
