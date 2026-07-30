#!/usr/bin/env bash
# Async-runtime ADR acceptance checks for ticket 004 (T2).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/004-T2-async-runtime-adr.md, section "Test plan"). T2 is
# a DECISION ticket whose deliverable is an ADR; most of its Test plan scenarios
# are documentary record checks ("the ADR states X", "exactly one of {A,B} is
# chosen") plus throwaway-prototype evidence whose *result* the ADR records.
#
# This script asserts the documentary record: that the ADR — embedded in the
# ticket file at the path the DoD names, matching the T1/T0.6/T3/T4/T0.7
# precedent of keeping each ADR inside its own ticket file — contains the
# required sections and the required decisions, in the exact normative
# vocabulary of arch.md (C13/C14/C16, Stability, C28). Authored FIRST as the
# acceptance gate, it fails on the ticket file as it stands before the ADR is
# written into it, and passes once the ADR records every decision the ticket is
# chartered to lock.
#
# It does NOT re-run the prototypes: the prototype is throwaway spike code,
# quarantined outside the shipping crates and deleted before the PR per the
# ticket-conventions decision(spike) rule; what survives is the ADR's record of
# what each prototype proved, which is what this script checks.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/004-T2-async-runtime-adr.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T0.6/T3/T4/T0.7 precedent: '# ADR: <title>'). The ticket prose
# above it (Objective, Test plan, DoD) already mentions tokio, spawn_blocking,
# cooperative, etc. — so a whole-file grep would pass content checks the ADR
# itself has not yet made. We therefore scope every content assertion to the ADR
# BODY only: the slice of the file from the first 'ADR:' heading line to EOF.
# The ticket's own H1 title ('# … ADR') is deliberately NOT matched (no colon),
# so before the embedded ADR is authored the slice is empty and every content
# check fails — exactly the tests-first behaviour we want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

# Case-insensitive extended-regex grep, scoped to the ADR body only.
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }

# --- ADR skeleton (ticket-conventions §4: decision tickets need status /
# context / decision / consequences / rejected alternatives) ------------------
# The ADR is embedded in the ticket file, so it lives below the six ticket
# sections under its own 'ADR:' heading. We look for the ADR heading plus each
# of the five required section headings, all within the ADR body slice.
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

# --- Public-dependency record check (Test plan: "Public-dependency record
# check"; DoD lines 1-2) ------------------------------------------------------
# tokio named as the async runtime and a supported *public* dependency.
if has 'tokio' && has 'public dependency'; then
  pass "public-dep: tokio named as a supported public dependency"
else
  bad "public-dep: tokio must be named as a supported *public dependency*"
fi
# Replacing / major-bumping tokio is a major-version event.
if has 'major.version event' || has 'major version event'; then
  pass "public-dep: major-version-event rule for replacing/major-bumping tokio recorded"
else
  bad "public-dep: the 'replace/major-bump tokio = major-version event' rule is missing"
fi
# Context-exposed types are dagr-owned wherever practical.
if has 'dagr.owned' && has 'wherever practical'; then
  pass "public-dep: 'context-exposed types are dagr-owned wherever practical' recorded"
else
  bad "public-dep: the dagr-owned-context-types-wherever-practical rule is missing"
fi
# tokio types may appear in the public API only where the surface is honest.
if has 'honest'; then
  pass "public-dep: 'tokio types may appear only where the API is honest' recorded"
else
  bad "public-dep: the honest-API-surface rule for tokio types is missing"
fi

# --- Blocking-pool strategy (Test plan: "Blocking-does-not-starve",
# "Blocking abandonment"; DoD line 3) -----------------------------------------
if has 'spawn_blocking'; then
  pass "blocking-pool: blocking strategy names the tokio blocking mechanism (spawn_blocking)"
else
  bad "blocking-pool: the blocking strategy must name its mechanism (spawn_blocking)"
fi
if has 'starve' || has 'does not delay' || has "cannot delay"; then
  pass "blocking-pool: records that blocking work cannot starve/delay await-bound work"
else
  bad "blocking-pool: the no-starvation guarantee for await-bound work is missing"
fi
if has 'abandoned.but.running' && has 'cannot be killed'; then
  pass "blocking-pool: abandoned-but-running / cannot-be-killed accounting shape recorded"
else
  bad "blocking-pool: the abandoned-but-running (cannot-be-killed) permit shape is missing"
fi

# --- Compute-pool decision (Test plan: "Compute-pool decision record check",
# "Compute-pool bound evidence"; DoD line 4) ----------------------------------
# Exactly one of {dedicated compute pool (rayon), capped semaphore over
# spawn_blocking} chosen, the other named as rejected.
if has 'rayon' && has 'semaphore'; then
  pass "compute-pool: both candidate mechanisms (rayon vs capped semaphore) named"
else
  bad "compute-pool: the ADR must name BOTH candidates (dedicated pool/rayon; capped semaphore)"
fi
if has 'fixed pool size' || has 'never exceed' || has 'peak concurrency'; then
  pass "compute-pool: fixed-pool-size bound (concurrent compute never exceeds N) recorded"
else
  bad "compute-pool: the fixed-pool-size / never-exceed-N bound is missing"
fi
if has 'at least one thread' || has 'floor of.*one' || has 'at least 1 thread'; then
  pass "compute-pool: floor-of-one-thread under a fractional CPU quota recorded"
else
  bad "compute-pool: the at-least-one-thread floor under a fractional quota is missing"
fi

# --- Cancellation-token primitive (Test plan: "Await-bound cancellation",
# "Cancellation-child"; DoD line 5) -------------------------------------------
if has 'cancellationtoken' || has 'cancellation.token'; then
  pass "cancel: a run-scoped cancellation-token primitive is named"
else
  bad "cancel: the cancellation-token primitive must be named"
fi
if has 'child'; then
  pass "cancel: the per-attempt child-token relationship is recorded"
else
  bad "cancel: the per-attempt child-token relationship is missing"
fi
if has 'future.drop' || has 'drop.*future' || has 'dropping the future'; then
  pass "cancel: await-bound cancellation is by future-drop (immediate permit release)"
else
  bad "cancel: await-bound cancellation-by-future-drop is missing"
fi
if has 'cooperative'; then
  pass "cancel: blocking/compute cancellation recorded as cooperative-only (marked, not killed)"
else
  bad "cancel: blocking/compute cooperative-only (marked-not-killed) cancellation is missing"
fi

# --- Isolated framework runtime (Test plan: "Runtime-isolation evidence";
# DoD line 6) -----------------------------------------------------------------
if has 'isolated' && (has 'framework runtime' || has 'framework.machinery'); then
  pass "isolation: an isolated framework runtime is decided"
else
  bad "isolation: the isolated framework-runtime decision is missing"
fi
for rail in 'timer' 'signal' 'event.stream' 'cancellation'; do
  if has "$rail"; then
    pass "isolation: safety rail '$rail' named as running on the framework runtime"
  else
    bad "isolation: safety rail '$rail' not mentioned in the isolation decision"
  fi
done
if has 'sigterm'; then
  pass "isolation: SIGTERM-still-fires-when-workers-blocked guarantee recorded (C13)"
else
  bad "isolation: the SIGTERM-under-blocked-workforce guarantee is missing"
fi

# --- Shutdown-budget arithmetic (Test plan: "Shutdown-budget arithmetic";
# DoD line 7) -----------------------------------------------------------------
if has '10 ?s' && has '15 ?s' && has '2 ?s' && has '30 ?s'; then
  pass "budget: grace 10s + teardown 15s + flush 2s within the 30s window recorded"
else
  bad "budget: the 10s/15s/2s-within-30s shutdown-budget arithmetic is missing"
fi
if has 'operator flag' || has 'operator.configurable' || has 'operator-configurable'; then
  pass "budget: grace and teardown flagged operator-configurable"
else
  bad "budget: grace/teardown operator-configurability is missing"
fi
if has 'printed at startup' || has 'print.*at startup' || has 'startup'; then
  pass "budget: worst-case budget printed at startup recorded as a T35 obligation"
else
  bad "budget: the print-worst-case-budget-at-startup obligation is missing"
fi

# --- Test-runtime shape / C28 (Test plan: "Test-runtime shape evidence";
# DoD line 8) -----------------------------------------------------------------
if has 'no.*runtime' && has 'synchronous'; then
  pass "test-runtime: synchronous single-task tests need no runtime (C28)"
else
  bad "test-runtime: the 'sync task needs no runtime' rule (C28) is missing"
fi
if has 'test runtime' || has 'test.runtime'; then
  pass "test-runtime: a plain test runtime for await-bound task tests is specified (C28)"
else
  bad "test-runtime: the plain test-runtime for await-bound tests (C28) is missing"
fi

# --- Prototype-evidence record (DoD line 9) ----------------------------------
# The ADR must reference what each throwaway prototype proved, and record that
# the spike is quarantined/removed from the shipping crates.
if has 'prototype' || has 'spike'; then
  pass "evidence: the ADR references the throwaway prototype evidence"
else
  bad "evidence: the ADR must reference what the prototypes proved"
fi
if has 'quarantin' || has 'deleted' || has 'removed' || has 'discard'; then
  pass "evidence: the ADR records that spike code is quarantined/removed from shipping crates"
else
  bad "evidence: the spike-quarantined/removed record is missing"
fi

# --- Unblocks T9 and T33 (DoD line 10) ---------------------------------------
if has 'T9' && has 'T33'; then
  pass "unblocks: the ADR names its downstream consumers (T9, T33)"
else
  bad "unblocks: the ADR must state it leaves no choice open to T9 and T33"
fi

# --- tokio lands where the ADR says it lands ---------------------------------
#
# PREDICATE CORRECTED BY T98, and deliberately not relaxed.
#
# As authored, this asserted that tokio was NOT yet wired into any shipping
# crate, because at T2 the decision was doc-only and the wiring belonged to
# T9/T33. That is a statement about the ORDER OF WORK, not about the design, and
# it went permanently false the moment T33 shipped — after which the checker
# could only fail. Unwired in CI, nobody saw it; T98 wires every checker, so it
# has to say something true.
#
# The DESIGN invariant behind it is the durable one, and it is stronger: the ADR
# places tokio in the crate that owns the run loop, and every other crate must
# justify an edge or not have one. So the scan is re-pointed at that boundary and
# expressed as an ALLOWLIST over every workspace member, not as a scan narrowed
# to one crate. Narrowing it to `crates/core` would defend the zero-dependency
# guarantee and nothing else — a tokio edge appearing on `render`, `artifact` or
# `macros` would pass unremarked, which is the ADR's placement decision going
# unenforced everywhere except the one crate that was never going to break it.
#
# Each name on the allowlist is a decision, recorded here rather than by making
# the scan blind:
#   cli       — owns the run loop; this is the ADR's placement (T9/T33).
#   metastore — the opt-in embedded run index. `libsql`'s API is async, so the
#               crate needs a runtime to drive it; the whole crate is behind the
#               default-off `metastore` feature (ADR 097 §5), so a default build
#               resolves no tokio through it.
# Anything else is a violation, including `dagr-core`, whose runtime dependency
# set is empty by architectural commitment (arch.md "Stability"; ADR 081/082).
tokio_allowed="cli metastore"

# Does <manifest> declare a dependency on the crate <name>?
#
# The allowlist above claims that every crate other than the named ones must
# justify a tokio edge, and a grep for `^[[:space:]]*tokio[[:space:]]*=` does not
# make that claim true — it sees only the INLINE spelling. Cargo accepts several
# others, and each one is a real edge the ADR's placement decision covers:
#
#   [dependencies.tokio]              # detail-table form, at any depth
#   [dev-dependencies.tokio]          # …and the dev / build tables
#   [build-dependencies.tokio]
#   [target.'cfg(unix)'.dependencies] # …and the target-specific prefix of each,
#   tokio = "1"                       #    inline or as `….dependencies.tokio`
#   tokio.workspace = true            # dotted-key form
#   tok = { package = "tokio", … }    # renamed: the KEY is not the crate name
#
# All of them resolve tokio into the crate. So the predicate parses the manifest's
# table structure instead of pattern-matching one line shape: it tracks which
# table it is in, treats a `[…dependencies]`-suffixed header as a dependency table
# and a `[…dependencies.<name>]` header as one dependency, and reads the real
# crate name off the `package` key wherever a rename supplies one. `-` and `_` are
# folded together, since the two spellings name the same crate.
#
# TOML comments are stripped first — these manifests carry long prose comments
# that mention tokio by name, and a `#` inside a quoted string is not a comment,
# so the stripper tracks strings rather than cutting at the first `#`. Exit 0 = the
# manifest declares the dependency.
manifest_declares_dep() {
  awk -v want="$2" '
    function trim(s) { sub(/^[[:space:]]+/, "", s); sub(/[[:space:]]+$/, "", s); return s }
    function norm(s) { gsub(/_/, "-", s); return tolower(s) }
    function uncomment(s,   out, i, c, n, basic, literal, sq) {
      sq = "\047"; out = ""; n = length(s); basic = 0; literal = 0
      for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (basic) {
          if (c == "\\") { out = out c substr(s, i + 1, 1); i++; continue }
          if (c == "\"") basic = 0
        } else if (literal) {
          if (c == sq) literal = 0
        } else {
          if (c == "#") break
          if (c == "\"") basic = 1; else if (c == sq) literal = 1
        }
        out = out c
      }
      return out
    }
    function pkg_rename(s,   p) {
      if (!match(s, /package[[:space:]]*=[[:space:]]*("[^"]*"|\047[^\047]*\047)/)) return ""
      p = substr(s, RSTART, RLENGTH)
      sub(/^package[[:space:]]*=[[:space:]]*/, "", p)
      gsub(/["\047]/, "", p)
      return p
    }
    BEGIN { hit = 0; deptable = 0; detail = 0; wantn = norm(want) }
    {
      line = trim(uncomment($0))
      if (line == "") next
      if (substr(line, 1, 2) == "[[") { deptable = 0; detail = 0; next }
      if (substr(line, 1, 1) == "[") {
        h = line; sub(/^\[/, "", h); sub(/\][[:space:]]*$/, "", h); h = trim(h)
        deptable = 0; detail = 0
        if (h ~ /(^|\.)(dependencies|dev-dependencies|build-dependencies)$/) {
          deptable = 1
        } else if (h ~ /(^|\.)(dependencies|dev-dependencies|build-dependencies)\.[^.]+$/) {
          detail = 1
          name = h; sub(/^.*\./, "", name); gsub(/["\047]/, "", name)
          if (norm(name) == wantn) { hit = 1; exit }
        }
        next
      }
      if (!deptable && !detail) next
      if (line !~ /=/) next
      key = line; sub(/=.*/, "", key); key = trim(key); gsub(/["\047]/, "", key)
      val = line; sub(/^[^=]*=/, "", val)
      if (deptable) {
        head = key; sub(/\..*/, "", head)
        if (norm(head) == wantn) { hit = 1; exit }
        if (norm(pkg_rename(val)) == wantn) { hit = 1; exit }
        if (key ~ /\.package$/) {
          p = val; gsub(/[[:space:]"\047]/, "", p)
          if (norm(p) == wantn) { hit = 1; exit }
        }
      } else if (key == "package") {
        p = val; gsub(/[[:space:]"\047]/, "", p)
        if (norm(p) == wantn) { hit = 1; exit }
      }
    }
    END { exit(hit ? 0 : 1) }
  ' "$1" 2>/dev/null
}

# Emits one line per tokio edge outside the allowlist, scanning every
# `<root>/<crate>/Cargo.toml`. Emits the sentinel `__NO_MANIFESTS__` when the
# glob matched nothing, so "no output" can never be read as "no violations".
# This function IS the predicate, and the probe below drives it rather than
# re-deriving it — a probe that re-implements the rule cannot fail when the rule
# is broken, which is the failure mode this whole repair is about.
scan_tokio_edges() {
  _root=$1
  _n=0
  for _manifest in "$_root"/*/Cargo.toml; do
    [ -f "$_manifest" ] || continue
    _n=$((_n + 1))
    _crate=${_manifest%/Cargo.toml}
    _crate=${_crate##*/}
    manifest_declares_dep "$_manifest" tokio || continue
    case " $tokio_allowed " in
      *" $_crate "*) ;;
      *) printf '%s (%s)\n' "$_crate" "$_manifest";;
    esac
  done
  [ "$_n" -eq 0 ] && printf '__NO_MANIFESTS__\n'
  return 0
}

offending=$(scan_tokio_edges crates)
case "$offending" in
  __NO_MANIFESTS__*)
    bad "scope: no crates/*/Cargo.toml was scanned — the tokio-placement check is vacuous";;
  "")
    pass "scope: tokio appears only in {$tokio_allowed} across every workspace crate (ADR placement)";;
  *)
    bad "scope: tokio edge on a crate the ADR does not place a runtime in (allowed: $tokio_allowed):"
    printf '%s\n' "$offending" | sed 's/^/        /';;
esac

# The scan is non-vacuous, proved by running it against a synthetic tree rather
# than by inspecting the real one: a disallowed crate carrying tokio must be
# flagged, and an allowlisted one must not. Nothing in the working tree is
# touched.
#
# EVERY spelling of a dependency edge gets its own planted crate, because the
# whole point of parsing the table structure is that these are not one pattern:
# an inline key, a detail table, the dev and build tables, the target-specific
# prefix, and a rename that hides the crate name behind the `package` key. A
# manifest that only TALKS about tokio in a comment is planted too — a predicate
# that flags prose would fail these manifests for the wrong reason and would have
# to be loosened again.
probe=$(mktemp -d 2>/dev/null) || probe=""
if [ -n "$probe" ]; then
  mkdir -p "$probe/render" "$probe/detail" "$probe/devtable" "$probe/buildtable" \
           "$probe/targeted" "$probe/renamed" "$probe/prose" "$probe/cli"
  printf '[dependencies]\ntokio = "1"\n'                                  >"$probe/render/Cargo.toml"
  printf '[dependencies.tokio]\nversion = "1"\n'                          >"$probe/detail/Cargo.toml"
  printf '[dev-dependencies.tokio]\nversion = "1"\n'                      >"$probe/devtable/Cargo.toml"
  printf '[build-dependencies.tokio]\nversion = "1"\n'                    >"$probe/buildtable/Cargo.toml"
  printf "[target.'cfg(unix)'.dependencies.tokio]\nversion = \"1\"\n"     >"$probe/targeted/Cargo.toml"
  printf '[dependencies]\nrt = { package = "tokio", version = "1" }\n'    >"$probe/renamed/Cargo.toml"
  printf '[dependencies]\n# tokio = "1" is discussed here, not declared\nserde = "1"\n' \
                                                                          >"$probe/prose/Cargo.toml"
  printf '[dependencies]\ntokio = "1"\n'                                  >"$probe/cli/Cargo.toml"
  verdict=$(scan_tokio_edges "$probe")
  rm -rf "$probe"
  missed=""
  for expected in render detail devtable buildtable targeted renamed; do
    printf '%s\n' "$verdict" | grep -q "^$expected " || missed="$missed $expected"
  done
  if [ -z "$missed" ]; then
    pass "scope: the scan flags a tokio edge in every dependency spelling Cargo accepts (inline, detail table, dev/build tables, target-specific, renamed)"
  else
    bad "scope: the scan missed a planted tokio edge — it is vacuous for these spellings:$missed"
  fi
  if printf '%s\n' "$verdict" | grep -q '^cli '; then
    bad "scope: the scan flags the allowlisted dagr-cli — the allowlist is not being honored"
  else
    pass "scope: the scan leaves the allowlisted crate alone (it names the legitimate edges, not blindness)"
  fi
  if printf '%s\n' "$verdict" | grep -q '^prose '; then
    bad "scope: the scan flags a crate that only MENTIONS tokio in a comment — it reads prose, not dependencies"
  else
    pass "scope: the scan ignores a commented-out/discussed tokio (it reads the dependency tables, not the prose)"
  fi
fi
# The two checks below call the predicate DIRECTLY rather than through the scan,
# so the direct call shape gets its own probe: a manifest carrying tokio only in a
# detail table must read as a dependency, and one carrying only a `[package] name`
# of tokio must not. Without this, the core guard could go blind while the scan's
# probes stayed green.
dprobe=$(mktemp -d 2>/dev/null) || dprobe=""
if [ -n "$dprobe" ]; then
  printf '[package]\nname = "x"\n\n[dependencies.tokio]\nversion = "1"\n' >"$dprobe/yes.toml"
  printf '[package]\nname = "tokio"\n\n[dependencies]\nserde = "1"\n'     >"$dprobe/no.toml"
  if manifest_declares_dep "$dprobe/yes.toml" tokio; then
    pass "scope: the zero-dependency guard's predicate sees a detail-table tokio edge"
  else
    bad "scope: the zero-dependency guard's predicate misses a detail-table tokio edge — the guard is blind"
  fi
  if manifest_declares_dep "$dprobe/no.toml" tokio; then
    bad "scope: the zero-dependency guard's predicate fires on a crate merely NAMED tokio"
  else
    pass "scope: the zero-dependency guard's predicate distinguishes a dependency from a package name"
  fi
  rm -rf "$dprobe"
fi
# Called out separately from the allowlist because the message carries the
# commitment: core's failure here is not a misplacement, it is the end of the
# zero-runtime-dependency guarantee.
if manifest_declares_dep crates/core/Cargo.toml tokio; then
  bad "scope: dagr-core must NOT depend on tokio — its runtime dependency set is empty (ADR 081/082)"
else
  pass "scope: dagr-core carries no tokio edge (zero-runtime-dependency guarantee)"
fi
if manifest_declares_dep crates/cli/Cargo.toml tokio; then
  pass "scope: tokio is wired into dagr-cli, the crate the ADR places the runtime in"
else
  bad "scope: no crate wires tokio — the ADR's chosen runtime is unimplemented"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
