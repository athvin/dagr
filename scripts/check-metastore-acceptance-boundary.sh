#!/usr/bin/env bash
# T88 — the M7 metastore acceptance gate's STRUCTURAL boundary invariants.
#
# These are the "Boundary (structural, CI-enforced)" assertions of ticket 103.
# They are crate-graph / diff facts a unit test cannot reach, so they live here
# and are wired into the CI gate job. Like the whole gate ticket, this adds ZERO
# capability — it only asserts already-merged structure (T83–T87).
#
# It COMPOSES the T83 feature-gating checks (default cli reaches neither libsql
# nor dagr-metastore; --features metastore reaches both; core reaches neither
# even --all-features; libsql pinned exactly) and ADDS the T88-specific
# invariants:
#
#   * `cargo build --all` (default) and `cargo build -p dagr-cli
#     --no-default-features` pull NO libsql / dagr-metastore THROUGH THE CLI, and
#     `cargo build --all --no-default-features` leaves `dagr-core`'s runtime
#     dependency set empty/unchanged (libsql never reaches core).
#   * `dagr-metastore` has NO dependency path TO `dagr-core` (the artifact-only
#     edge of ADR 097 §5 — the same C24-style boundary as `render`).
#   * The M6->M7 diff introduces NO real scheduler/server/pgwire surface: the
#     shipped source added since the last M6 commit contains no network-listener,
#     Postgres-wire, HTTP/gRPC-server, or scheduler symbol. (The `RemoteSqld` /
#     `SyncedReplica` OpenMode variants are RECOGNIZED STUBS — ADR 097 §5, ticket
#     conventions §7 — that return `ModeNotImplemented`; they are the reserved
#     seam, not a shipped surface, so they are explicitly allowed while a real
#     wired surface is forbidden.)
#
# `dagr-metastore` is a normal workspace member, so `cargo build --all` DOES
# build it (and thus libsql) directly — that is expected and NOT what these
# checks forbid. What they forbid is a libsql / dagr-metastore edge reachable
# through `dagr-cli` by default, any edge onto `dagr-core`, and a coordination
# surface crossing the permanent scope boundary.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure,
# 2 = a usage/setup anomaly.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

# --- 0. Compose the T83 feature-gating structural checks ---------------------
# The default-off feature, the optional dep, the exact libsql pin, and the
# cli(default) / cli(--features metastore) / core(--all-features) cargo-tree
# proofs all live in the T83 script; the gate re-runs it so all of M7's gating
# is asserted in one place.
gating="scripts/check-metastore-feature-gating.sh"
if [ -f "$gating" ]; then
  if bash "$gating"; then
    pass "T83 feature-gating checks pass (default-off, optional dep, pinned libsql, crate-graph)"
  else
    bad "T83 feature-gating checks FAILED (see the output above)"
  fi
else
  bad "the T83 feature-gating script ($gating) is missing"
fi

# --- 1. dagr-metastore has NO dependency path to dagr-core -------------------
# ADR 097 §5: the only workspace edge from metastore is onto dagr-artifact; there
# must be NO path (normal, build, or dev) to dagr-core, so core's zero-runtime-
# dependency guarantee is structurally untouched. `cargo tree -i dagr-core`
# (inverse: everything that depends on dagr-core) must NOT list dagr-metastore.
if command -v cargo >/dev/null 2>&1; then
  if cargo tree -i dagr-core --prefix none 2>/dev/null \
       | grep -qE '^dagr-metastore[[:space:]]'; then
    bad "metastore: dagr-core's reverse-dep tree lists dagr-metastore (a path to core exists — violates ADR 097 §5)"
  else
    pass "metastore: no dependency path to dagr-core (dagr-core's reverse-dep tree omits dagr-metastore)"
  fi

  # And the forward view: dagr-metastore's own dependency tree (normal edges)
  # reaches neither dagr-core nor dagr-render nor dagr-cli.
  mt=$(cargo tree -p dagr-metastore -e normal --prefix none 2>/dev/null)
  if printf '%s\n' "$mt" | grep -qE '^(dagr-core|dagr-render|dagr-cli)[[:space:]]'; then
    bad "metastore: its forward dependency tree reaches dagr-core / dagr-render / dagr-cli (must reach only dagr-artifact + libsql + tokio)"
  else
    pass "metastore: forward dependency tree reaches only its allowed edges (no core / render / cli)"
  fi

  # --- 2. `cargo build --all --no-default-features`: core's runtime dep set is
  # empty/unchanged (only dagr-core itself), so libsql never reaches core even
  # under the whole-workspace no-default-features build.
  core_nd=$(cargo tree -p dagr-core --no-default-features -e no-dev --prefix none 2>/dev/null \
              | grep -vE '^dagr-core[[:space:]]' | grep -vE '^[[:space:]]*$')
  if [ -z "$core_nd" ]; then
    pass "core(--no-default-features): runtime dependency set is empty (unchanged; libsql never reaches core)"
  else
    bad "core(--no-default-features): runtime dependency set is NON-empty:"
    printf '%s\n' "$core_nd"
  fi
else
  bad "cargo unavailable — cannot run the crate-graph boundary proofs (they are load-bearing for T88)"
fi

# --- 3. The M6->M7 diff introduces no scheduler/server/pgwire surface --------
# Find the last M6 commit (the base the diff is measured from). Prefer the
# checked-in T81 done-marker commit (an immutable ancestor of this branch); if
# that SHA is not yet present in the local object store — the CI case, where
# `actions/checkout` makes a shallow (fetch-depth 1), detached-HEAD clone that
# holds only the tip commit, so neither the old marker SHA nor a local/tracking
# `main` ref exists — DEEPEN the clone by fetching exactly that pinned SHA from
# `origin`. This keeps the diff base byte-identical to the dev box (the same
# `620f511..HEAD` shipped-source diff, asserting the very same surface fact); it
# only makes the base reachable where the clone was truncated. The fetch is a
# best effort (non-fatal, quiet) so a full local clone — which already has the
# ancestor — is unaffected. `origin/main`/`main` remain secondary fallbacks for
# any environment where the marker SHA is genuinely gone. The diff is over
# SHIPPED source only (`crates/*/src`), never tests/examples (a gate's own test
# naturally mentions these words in negation).
m6_marker=620f511
if ! git rev-parse --verify --quiet "${m6_marker}^{commit}" >/dev/null 2>&1 \
   && [ "$(git rev-parse --is-shallow-repository 2>/dev/null)" = "true" ]; then
  # Shallow/CI clone: DEEPEN the already-fetched branch tip until the pinned M6
  # ancestor is in the local object store. `--deepen` extends the existing ref's
  # history (unlike a bare `fetch <sha>`, it needs no `uploadpack.allowAnySHA1InWant`
  # on the server), so it works on any host. The marker is an ancestor of HEAD a
  # few dozen commits back; 200 is generous headroom while staying far short of a
  # full `--unshallow`. Best effort (quiet, non-fatal) — a full clone never enters
  # this branch, and if it fails the `origin/main`/`main` fallbacks and the FAIL
  # below still apply.
  git fetch --quiet --deepen=200 origin >/dev/null 2>&1 || true
fi
m6_base=""
for cand in "$m6_marker" origin/main main; do
  if git rev-parse --verify --quiet "${cand}^{commit}" >/dev/null 2>&1; then
    m6_base="$cand"; break
  fi
done

if [ -n "$m6_base" ]; then
  # Added lines only (`^+`), over shipped src. A REAL coordination surface is a
  # network listener, a Postgres/pg wire protocol, an HTTP/gRPC server framework,
  # or a scheduler type — none of which M7 ships (ADR 097: native, embedded,
  # non-coordinating). The `RemoteSqld` / `SyncedReplica` reserved-seam stubs and
  # any `sqld` URL FIELD are NOT a surface (they return ModeNotImplemented), so
  # the pattern targets wired symbols, not the seam's doc/field mentions.
  forbidden='pgwire|pg_wire|postgres.*wire protocol|TcpListener|UdpSocket|::bind\(|\.serve\(|\.listen\(|axum|actix|warp::|hyper::(Server|server)|tonic::|struct[[:space:]]+[A-Za-z_]*Scheduler|impl[[:space:]]+[A-Za-z_]*Scheduler|embedded_replica\(|sync_from_remote'
  # Pathspec note: `crates/*/src/*` (trailing `/*`) is what actually matches the
  # CONTENTS of each crate's src tree — a bare `crates/*/src` matches only a path
  # element literally named `src` and so diffs NOTHING (a silently-vacuous scan).
  # This scans the real ~3k added M7 shipped-source lines (T83–T88 metastore code).
  hits=$(git diff "$m6_base"..HEAD -- 'crates/*/src/*' 2>/dev/null \
           | grep -E '^\+' \
           | grep -vE '^\+\+\+' \
           | grep -EI "$forbidden")
  if [ -z "$hits" ]; then
    pass "M6->M7 diff (from ${m6_base}) adds NO scheduler/server/pgwire surface in shipped source"
  else
    bad "M6->M7 diff adds a forbidden coordination surface (scheduler/server/pgwire) in shipped source:"
    printf '%s\n' "$hits"
  fi

  # The reserved-seam stubs, if present, must genuinely be stubs: an OpenMode that
  # is not LocalFile resolves to ModeNotImplemented (never a live server client),
  # so the seam stays unshipped. Assert the guard exists in the store.
  store="crates/metastore/src/store.rs"
  if [ -f "$store" ]; then
    if grep -q 'ModeNotImplemented' "$store"; then
      pass "metastore: the RemoteSqld/SyncedReplica seam is a recognized stub (ModeNotImplemented), not a shipped server client"
    else
      bad "metastore: no ModeNotImplemented guard found — a non-local OpenMode must not silently ship a server client"
    fi
  fi
else
  bad "could not resolve an M6 base commit for the diff-surface check (neither the T81 marker nor main is reachable)"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL M7 METASTORE ACCEPTANCE-BOUNDARY CHECKS PASSED"
else
  echo "SOME M7 METASTORE ACCEPTANCE-BOUNDARY CHECKS FAILED"
fi
exit "$fail"
