#!/usr/bin/env bash
# Per-crate publish metadata, README unification, and doc-example hygiene —
# ticket 111 (T96).
#
# Three invariants live here, all in the "publishable, self-describing crate"
# family, all mechanical so the NEXT crate added to the workspace cannot skip
# them (which is what the ticket's Test plan asks for by name).
#
#   * METADATA. Every member declares `description`. A member that does not set
#     `publish = false` is, by definition, intended for release, so it must also
#     carry the rest of the set a crates.io release needs — `readme`,
#     `documentation`, `keywords`, `categories` — and every intra-workspace path
#     dependency must carry a `version` requirement matching
#     `[workspace.package].version` (Cargo refuses to package a crate whose path
#     dependency has no version). Without this check the failure mode is
#     discovering it during a release, not before.
#   * README UNIFICATION. Every library crate has a `README.md` inlined into its
#     crate root with `#![doc = include_str!("../README.md")]`, so the crates.io
#     landing page and the rustdoc front page are one file that cannot drift.
#   * DOC-EXAMPLE HYGIENE. No ```` ```ignore ```` fence anywhere in a shipped
#     crate's `src/` (an ignored example is documentation that cannot rot
#     loudly), and no doc example uses `.unwrap()` (`doc-question-mark`: an
#     example is read as a template, so it should model `?`).
#
# The scans are non-vacuous by construction: after the real checks pass, the
# script re-invokes ITSELF in `--scan-only` mode against a throwaway fixture
# workspace that violates every rule above, and fails if that fixture is
# reported clean. A guard that cannot fail is not a guard.
#
# Usage:
#   check-crate-docs-and-metadata.sh              # full gate, from the repo root
#   check-crate-docs-and-metadata.sh --scan-only DIR
#
# `--scan-only DIR` runs ONLY the per-crate scans over DIR (which must contain a
# `Cargo.toml` and a `crates/` directory) and skips the publish-graph closure,
# the `cargo package` dry run, and the self-check. It exists so the self-check
# can drive the scans hermetically against a fixture.
#
# Exit 0 = every invariant holds, 1 = at least one failure, 2 = bad invocation.
set -u

scan_only=""
case "${1:-}" in
  --scan-only) scan_only="${2:?--scan-only needs a directory}" ;;
  -h|--help)   sed -n '1,40p' "$0"; exit 0 ;;
  "")          ;;
  *)           echo "unknown argument: $1" >&2; exit 2 ;;
esac

self=$0
case "$self" in /*) ;; *) self="$PWD/$self" ;; esac

if [ -n "$scan_only" ]; then
  [ -d "$scan_only" ] || { echo "FAIL  --scan-only: no such directory: $scan_only" >&2; exit 2; }
  root=$scan_only
else
  root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
fi
cd "$root" || { echo "cannot cd to $root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

# --- TOML helpers -------------------------------------------------------------
# Small on purpose: enough to read a scalar out of a named `[section]`. Comment
# lines are skipped, so the rationale comments these manifests carry cannot be
# misread as data.

# toml_value FILE SECTION KEY -> the scalar value, quotes and comment stripped.
toml_value() {
  awk -v want="$2" -v key="$3" '
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (line ~ /^#/ || line == "") next
      if (line ~ /^\[/) {
        s = line
        sub(/^\[/, "", s)
        sub(/\][[:space:]]*$/, "", s)
        cur = s
        next
      }
      if (cur != want || line !~ /=/) next
      k = line; sub(/=.*$/, "", k); gsub(/[[:space:]]/, "", k)
      if (k != key) next
      v = line; sub(/^[^=]*=/, "", v)
      sub(/[[:space:]]#.*$/, "", v)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
      print v
    }
  ' "$1"
}

# crate_name FILE -> the [package] name.
crate_name() { toml_value "$1" package name | tr -d '"'; }

workspace_version=$(toml_value Cargo.toml workspace.package version | tr -d '"')

# --- The per-crate scans (run in both modes) ---------------------------------
members=$(ls -d crates/*/ 2>/dev/null | sed 's:/$::')
if [ -z "$members" ]; then
  bad "layout: no crates/* directory found under $root"
fi

for dir in $members; do
  manifest="$dir/Cargo.toml"
  [ -f "$manifest" ] || { bad "layout: $dir has no Cargo.toml"; continue; }
  name=$(crate_name "$manifest")
  name=${name:-$dir}

  # --- description: mandatory for crates.io, unconditionally required here ---
  desc=$(toml_value "$manifest" package description)
  case "$desc" in
    ""|'""') bad "metadata: $name declares no \`description\` — crates.io rejects the publish, and the manifest does not say what the crate is" ;;
    *)       pass "metadata: $name declares a description" ;;
  esac

  publish=$(toml_value "$manifest" package publish)
  if [ "$publish" = "false" ]; then
    # An unpublishable crate still has to say WHY, in the style the manifests
    # already use — a bare `publish = false` is an undocumented decision.
    if grep -B8 '^[[:space:]]*publish[[:space:]]*=[[:space:]]*false' "$manifest" \
       | grep -qE '^[[:space:]]*#'; then
      pass "metadata: $name sets publish = false with a rationale comment above it"
    else
      bad "metadata: $name sets publish = false with no rationale comment above it"
    fi
    continue
  fi

  # --- publishable: the rest of the set a crates.io release needs -----------
  for key in readme documentation keywords categories; do
    v=$(toml_value "$manifest" package "$key")
    case "$v" in
      ""|'""'|"[]") bad "metadata: $name is publishable (no \`publish = false\`) but declares no \`$key\`" ;;
      *)            pass "metadata: $name declares $key" ;;
    esac
  done

  readme=$(toml_value "$manifest" package readme | tr -d '"')
  if [ "$readme" = "README.md" ] && [ -f "$dir/README.md" ]; then
    pass "readme: $name declares readme = \"README.md\" and the file exists"
  else
    bad "readme: $name declares readme = '${readme:-<unset>}' but $dir/README.md is missing or the value is not \"README.md\""
  fi

  # --- README unification: one file, three rendering targets ---------------
  if [ -f "$dir/src/lib.rs" ]; then
    if grep -q '^#!\[doc = include_str!("../README.md")\]' "$dir/src/lib.rs"; then
      pass "readme: $name inlines its README into the crate root via include_str!"
    else
      bad "readme: $dir/src/lib.rs does not carry \`#![doc = include_str!(\"../README.md\")]\` — the README and the rustdoc front page will drift"
    fi
  fi

  # --- intra-workspace path deps carry a version requirement ---------------
  # `cargo package` refuses a path dependency with no version: "all dependencies
  # must have a version requirement specified when packaging". Pinning it to the
  # workspace version keeps the six manifests from drifting apart.
  bare_paths=$(grep -nE '\{[^}]*path[[:space:]]*=[[:space:]]*"\.\./' "$manifest" \
               | grep -v 'version[[:space:]]*=' || true)
  if [ -z "$bare_paths" ]; then
    pass "publishable-deps: every intra-workspace path dependency of $name carries a version requirement"
  else
    bad "publishable-deps: $name has a path dependency with no version requirement (cargo package refuses it):"
    printf '%s\n' "$bare_paths" | sed 's/^/        /'
  fi
  wrong_version=$(grep -nE 'path[[:space:]]*=[[:space:]]*"\.\./' "$manifest" \
                  | grep 'version[[:space:]]*=' \
                  | grep -v "version = \"$workspace_version\"" || true)
  if [ -z "$wrong_version" ]; then
    pass "publishable-deps: $name's path-dependency versions match [workspace.package].version ($workspace_version)"
  else
    bad "publishable-deps: $name has a path dependency whose version does not match [workspace.package].version ($workspace_version):"
    printf '%s\n' "$wrong_version" | sed 's/^/        /'
  fi
done

# --- Doc-example hygiene ------------------------------------------------------
# An `ignore`d example is not compiled, so it cannot rot loudly; `no_run` at
# least compile-checks it. Scoped to the shipped `src/` trees.
ignored_fences=$(grep -rn '^[[:space:]]*\(///\|//!\)[[:space:]]*```ignore' crates/*/src 2>/dev/null || true)
if [ -z "$ignored_fences" ]; then
  pass "doc-examples: no \`\`\`ignore fence in any crate's src/ (an ignored example cannot rot loudly)"
else
  bad "doc-examples: an \`\`\`ignore fence is present — use \`no_run\` (compile-checked) and say why at the fence:"
  printf '%s\n' "$ignored_fences" | sed 's/^/        /'
fi

# `doc-question-mark`: a doc example is read as a template, so it models `?`
# rather than `.unwrap()`.
unwrap_examples=$(grep -rn '^[[:space:]]*\(///\|//!\).*\.unwrap()' crates/*/src 2>/dev/null || true)
if [ -z "$unwrap_examples" ]; then
  pass "doc-examples: no doc example uses .unwrap() (they model \`?\`)"
else
  bad "doc-examples: a doc example uses .unwrap() — use \`?\` so the example is a template worth copying:"
  printf '%s\n' "$unwrap_examples" | sed 's/^/        /'
fi

if [ -n "$scan_only" ]; then
  # Scan-only mode is the self-check's hermetic entry point: report and stop.
  if [ "$fail" -eq 0 ]; then echo "CRATE_DOCS_METADATA=PASS"; else echo "CRATE_DOCS_METADATA=FAIL"; fi
  exit "$fail"
fi

# --- Check: the publish graph is closed --------------------------------------
# Cargo cannot publish a crate whose dependency is not on the registry, and an
# optional or build-time dependency is still a dependency. So a `publish = false`
# member makes every member that depends on it unpublishable too — silently,
# until release day. Assert the closure instead.
unpublishable=""
for dir in $members; do
  [ -f "$dir/Cargo.toml" ] || continue
  if [ "$(toml_value "$dir/Cargo.toml" package publish)" = "false" ]; then
    unpublishable="$unpublishable $(crate_name "$dir/Cargo.toml")"
  fi
done
closure_violations=""
for dir in $members; do
  [ -f "$dir/Cargo.toml" ] || continue
  [ "$(toml_value "$dir/Cargo.toml" package publish)" = "false" ] && continue
  for u in $unpublishable; do
    # Only the real dependency sections matter; a dev-dependency is stripped at
    # publish time, so it cannot block a release.
    if awk -v dep="$u" '
        { line = $0; sub(/^[[:space:]]+/, "", line) }
        line ~ /^\[/ { s = line; sub(/^\[/, "", s); sub(/\][[:space:]]*$/, "", s); cur = s; next }
        cur ~ /^(dependencies|target\..*\.dependencies|build-dependencies)$/ && line ~ ("^" dep "[[:space:]]*=") { found = 1 }
        END { exit(found ? 0 : 1) }
      ' "$dir/Cargo.toml"; then
      closure_violations="$closure_violations$(crate_name "$dir/Cargo.toml") depends on unpublishable $u
"
    fi
  done
done
if [ -z "$closure_violations" ]; then
  pass "publish-graph: no publishable member depends on a \`publish = false\` member"
else
  bad "publish-graph: a publishable member depends on an unpublishable one, so it cannot actually be released:"
  printf '%s' "$closure_violations" | sed 's/^/        /'
fi

# --- Check: the packaging dry run does not fail for missing metadata ---------
# The ticket's Test plan asks for a real dry run per publishable crate.
# `--no-verify` skips the rebuild (this is a manifest check, not a build check);
# `--allow-dirty` lets it run on a working tree mid-ticket.
if command -v cargo >/dev/null 2>&1; then
  pkg_failures=""
  for dir in $members; do
    [ -f "$dir/Cargo.toml" ] || continue
    [ "$(toml_value "$dir/Cargo.toml" package publish)" = "false" ] && continue
    name=$(crate_name "$dir/Cargo.toml")
    out=$(cargo package -p "$name" --no-verify --allow-dirty --quiet 2>&1) || {
      pkg_failures="$pkg_failures$name: $(printf '%s' "$out" | tr '\n' ' ')
"
      continue
    }
    if printf '%s' "$out" | grep -q 'manifest has no'; then
      pkg_failures="$pkg_failures$name: $(printf '%s' "$out" | grep 'manifest has no' | tr '\n' ' ')
"
    fi
  done
  if [ -z "$pkg_failures" ]; then
    pass "package-dry-run: cargo package --no-verify succeeds for every publishable member with no missing-metadata warning"
  else
    bad "package-dry-run: cargo package reported a manifest problem:"
    printf '%s' "$pkg_failures" | sed 's/^/        /'
  fi
else
  pass "package-dry-run: SKIPPED (cargo is not on PATH)"
fi

# --- Check: the scans above are non-vacuous ----------------------------------
# Drive the scans against a fixture workspace that violates every rule. If the
# fixture comes back clean the scans are decorative.
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/crates/naked/src"
cat >"$fixture/Cargo.toml" <<'FIXTURE'
[workspace.package]
version = "0.0.0"
FIXTURE
cat >"$fixture/crates/naked/Cargo.toml" <<'FIXTURE'
[package]
name = "naked"

[dependencies]
other = { path = "../other" }
FIXTURE
cat >"$fixture/crates/naked/src/lib.rs" <<'FIXTURE'
//! No README inlined here.
/// ```ignore
/// let v = thing().unwrap();
/// ```
pub fn thing() {}
FIXTURE
if bash "$self" --scan-only "$fixture" >"$fixture/out" 2>&1; then
  bad "self-check: the scans reported a fixture with no description, no README, a bare path dep, an \`\`\`ignore fence and an .unwrap() example as clean — they are vacuous"
else
  missed=""
  grep -q 'declares no `description`'            "$fixture/out" || missed="$missed description"
  grep -q 'declares no `readme`'                 "$fixture/out" || missed="$missed readme"
  grep -q 'does not carry'                       "$fixture/out" || missed="$missed include_str"
  grep -q 'no version requirement'               "$fixture/out" || missed="$missed path-dep-version"
  grep -q 'ignore fence is present'              "$fixture/out" || missed="$missed ignore-fence"
  grep -q 'doc example uses .unwrap()'           "$fixture/out" || missed="$missed unwrap-example"
  if [ -z "$missed" ]; then
    pass "self-check: every scan rejects a fixture that violates it (non-vacuous)"
  else
    bad "self-check: the fixture failed, but these scans did not fire:$missed"
  fi
fi

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "CRATE_DOCS_METADATA=PASS"
else
  echo "CRATE_DOCS_METADATA=FAIL"
fi
exit "$fail"
