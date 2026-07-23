#!/usr/bin/env bash
# Workspace-skeleton acceptance checks for ticket 003 (T1).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/003-T1-crate-layout-and-workspace-skeleton.md, section
# "Test plan"). These are structural invariants about the Cargo workspace and
# its crate graph, not behavioural unit tests: authored FIRST as the acceptance
# gate, they fail on a tree with no workspace and pass once the four-crate
# skeleton (core, artifact, render, cli) is in place with the dependency
# direction the ADR prescribes.
#
# The per-crate `cargo test` placeholders prove every member is in the build
# graph and testable (Test plan "Every member crate is discoverable and
# testable"); this script proves the *structural* facts cargo alone cannot
# assert: crate membership, render's artifact-only dependency edge (C24),
# minimal core deps, MSRV pinning + README naming, and target/ADR agreement.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

manifest="Cargo.toml"

# --- Test: the workspace manifest exists and lists all four members ----------
# (Test plan: "Every member crate is discoverable and testable" — no member is
# silently excluded from the build graph.)
if [ -f "$manifest" ]; then
  pass "workspace: root Cargo.toml exists"
  if grep -qE '^\[workspace\]' "$manifest"; then
    pass "workspace: [workspace] table present"
  else
    bad "workspace: [workspace] table missing from root Cargo.toml"
  fi
  for member in core artifact render cli; do
    if grep -qE "\"crates/$member\"" "$manifest"; then
      pass "workspace: member '$member' listed in [workspace].members"
    else
      bad "workspace: member '$member' not listed in [workspace].members"
    fi
  done
else
  bad "workspace: root Cargo.toml missing"
fi

# --- Test: every member crate exists with a lib target -----------------------
# (Test plan: "each crate carries a placeholder lib target that compiles".)
for member in core artifact render cli; do
  crate_dir="crates/$member"
  if [ -f "$crate_dir/Cargo.toml" ]; then
    pass "crate '$member': Cargo.toml exists"
  else
    bad "crate '$member': Cargo.toml missing at $crate_dir/Cargo.toml"
    continue
  fi
  if [ -f "$crate_dir/src/lib.rs" ]; then
    pass "crate '$member': placeholder lib target (src/lib.rs) exists"
  else
    bad "crate '$member': src/lib.rs (lib target) missing"
  fi
  # Every member inherits the workspace lint policy from lints.toml
  # (era note: lints.workspace = true rather than per-crate lint attributes).
  if grep -qE '^\s*workspace\s*=\s*true' "$crate_dir/Cargo.toml" \
     && grep -qE '^\[lints\]' "$crate_dir/Cargo.toml"; then
    pass "crate '$member': inherits [workspace.lints] (lints.workspace = true)"
  else
    bad "crate '$member': does not inherit workspace lints ([lints] workspace = true)"
  fi
  # Every shipped crate declares license = "MIT" so cargo deny (T7) has an
  # unambiguous target (docs/lint-policy.md, supply-chain section).
  if grep -qE 'license' "$crate_dir/Cargo.toml"; then
    pass "crate '$member': declares package license"
  else
    bad "crate '$member': no license declared (cargo-deny target, per lint-policy.md)"
  fi
done

# --- Test: renderer independence is structurally enforced (C24) --------------
# (Test plan: "Renderer independence is structurally enforced" — render depends
# on artifact only; no dependency edge onto core exists.)
render_manifest="crates/render/Cargo.toml"
if [ -f "$render_manifest" ]; then
  if grep -qE '^[[:space:]]*dagr-artifact[[:space:]]*=' "$render_manifest"; then
    pass "render: depends on dagr-artifact (C24 artifact-only consumption)"
  else
    bad "render: does not depend on dagr-artifact"
  fi
  # No dependency edge onto core: rendering requires no access to the pipeline
  # binary (C24). This is the structural half of the throwaway-edit test.
  if grep -qE '^[[:space:]]*dagr-core[[:space:]]*=' "$render_manifest"; then
    bad "render: has a dependency edge onto dagr-core (violates C24 renderer independence)"
  else
    pass "render: has NO dependency edge onto dagr-core (C24: no access to pipeline binary)"
  fi
fi

# --- Test: renderer target matches the ADR decision --------------------------
# (Test plan: "Renderer target matches the ADR decision" — the ADR states the
# renderer is a standalone bin hosted in the render crate; that bin must exist.)
if [ -f "crates/render/src/main.rs" ] \
   || ls crates/render/src/bin/*.rs >/dev/null 2>&1; then
  pass "render: standalone renderer bin target exists (matches ADR decision)"
else
  bad "render: no standalone bin target (ADR says renderer builds standalone)"
fi

# --- Test: core dependency set is minimal ------------------------------------
# (Test plan: "Core dependency set is minimal" — empty or minimal, each entry
# justified in the ADR; Stability's minimal-core commitment.)
core_manifest="crates/core/Cargo.toml"
if [ -f "$core_manifest" ]; then
  # Extract the [dependencies] section body and count non-comment entries.
  deps=$(awk '
    /^\[dependencies\]/     {inblk=1; next}
    /^\[/                   {inblk=0}
    inblk && /^[[:space:]]*[A-Za-z0-9_-]+[[:space:]]*=/ {print}
  ' "$core_manifest" | grep -vcE '^\s*#')
  if [ "${deps:-0}" -eq 0 ]; then
    pass "core: dependency set is empty (Stability minimal-core commitment)"
  else
    bad "core: has $deps direct dependencies (expected empty/minimal per ADR)"
  fi
fi

# --- Test: MSRV is pinned and honored, and the README names it ---------------
# (Test plan: "MSRV is pinned and honored" — the pinned version and the README
# name the same version.)
pinned=""
if [ -f rust-toolchain.toml ]; then
  pinned=$(grep -E '^[[:space:]]*channel[[:space:]]*=' rust-toolchain.toml \
           | head -1 | sed -E 's/.*=[[:space:]]*"?([^"#]+)"?.*/\1/' | tr -d '[:space:]')
fi
# The workspace manifest pins rust-version to the same MSRV (workspace level).
if [ -n "$pinned" ] && grep -qE "rust-version[[:space:]]*=[[:space:]]*\"$pinned\"" "$manifest" 2>/dev/null; then
  pass "MSRV: workspace [workspace.package].rust-version matches toolchain '$pinned'"
else
  bad "MSRV: workspace rust-version does not match pinned toolchain '$pinned'"
fi
if [ -n "$pinned" ] && ls README* >/dev/null 2>&1 && grep -iE 'MSRV' README* | grep -q "$pinned"; then
  pass "MSRV: README names the pinned version '$pinned' (no drift)"
else
  bad "MSRV: README does not name the pinned version '$pinned'"
fi

# --- Test: layout is documented ----------------------------------------------
# (Test plan: "Layout is documented" — each crate's role and the allowed
# dependency edges are written down; here the ADR carries the crate-roles table.)
adr="docs/implementation/003-T1-crate-layout-and-workspace-skeleton.md"
if [ -f "$adr" ]; then
  documented=1
  for role in "core" "artifact" "render" "cli"; do
    grep -qiE "\`$role\`" "$adr" || documented=0
  done
  # The ADR must record the multi-crate decision and the renderer-binary answer.
  grep -qiE 'multi-crate|multi crate' "$adr" || documented=0
  grep -qiE 'render.*only.*artifact|artifact-only|artifact only' "$adr" || documented=0
  if [ "$documented" -eq 1 ]; then
    pass "docs: ADR documents all four crate roles + dependency direction + decisions"
  else
    bad "docs: ADR does not fully document crate roles / dependency direction / decisions"
  fi
else
  bad "docs: ticket/ADR file missing"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL WORKSPACE-SKELETON CHECKS PASSED"
else
  echo "SOME WORKSPACE-SKELETON CHECKS FAILED"
fi
exit "$fail"
