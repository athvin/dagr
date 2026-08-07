#!/usr/bin/env bash
# Profiles/config-file ADR acceptance checks for ticket 128 (T113).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/128-T113-profiles-config-file-adr.md).
# T113 is a DECISION ticket whose durable deliverable is an ADR that AMENDS a
# clause docs/arch.md C26 called "a permanent scope boundary": the unqualified
# "there is no config file or DSL" is narrowed — for the CONFIG-FILE half only —
# so that a file of runtime knobs with named profiles, read at bootstrap,
# configuring HOW ONE INVOCATION RUNS, becomes permitted as a fourth precedence
# tier (`flag > env > file(profile) > default`).
#
# A carve-out that only says what is allowed drifts. This checker therefore pins
# BOTH halves, and the second half is the load-bearing one:
#
#   PERMITTED — a bootstrap-read TOML file of run-level knobs with named
#   profiles, parsed in dagr-cli only, beneath the environment in precedence.
#
#   A VIOLATION — and every one of these is asserted to be still named as
#   excluded, in the ADR and in arch.md: a file that describes the GRAPH
#   (selecting a flow, declaring nodes or edges, expressing conditionals),
#   a file that reaches node policy or placement, a file read during assembly,
#   a file-only knob with no reserved flag, secrets in the file, and any DSL —
#   "no DSL" and "no configuration file describing the graph" stay verbatim.
#
# It also pins the decisions T114-T118 inherit (unchanged DAGR_* spellings plus
# the total-and-injective mapping-table obligation, the preserved tri-state of
# resolve_opt, default-profile layering, unknown-profile-is-a-loud-failure, the
# zero-dep-core boundary, and the inert-env-tier finding that puts T114 before
# the loader) — because a later ticket that quietly re-decides one of them has
# widened the carve-out without anyone amending it.
#
# The supersession checks assert the two partial supersessions are recorded AND
# that the original rejection text survives next to each note — a merged
# decision's text is never rewritten (ticket-conventions §10), so the older
# sentence surviving is the evidence that nothing was overwritten.
#
# This is a DOC-ONLY decision (the loader is T115, the wiring is T114, and the
# acceptance gate that asserts the boundary structurally over shipped code is
# T118), so the script builds nothing and asserts no production code: the only
# artifacts the decision commits are the embedded ADR, the arch.md C26
# amendment and its Amendment-changelog entry, and the two partial-supersession
# notes.
#
# Run from the repository root. Exit 0 = every assertion holds, 1 = a failure,
# 2 = a required file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/128-T113-profiles-config-file-adr.md"
archmd="docs/arch.md"
adr089="docs/implementation/089-config-precedence-adr.md"
adr091="docs/implementation/091-T77-env-fallbacks-and-headroom.md"
readme="README.md"
corereadme="crates/core/README.md"
puritytest="crates/core/tests/determinism_and_purity.rs"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

for f in "$adr" "$archmd" "$adr089" "$adr091" "$readme" "$corereadme"; do
  [ -f "$f" ] || { echo "FAIL  required file missing: $f"; exit 2; }
done

# Whitespace-flattened copies, so assertions can pin sentences that wrap across
# hard-wrapped markdown lines without depending on where the wrap falls.
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
flat089="$tmpdir/089.flat"; tr -s '[:space:]' ' ' <"$adr089" >"$flat089"
flat091="$tmpdir/091.flat"; tr -s '[:space:]' ' ' <"$adr091" >"$flat091"
flatadr="$tmpdir/128.flat"; tr -s '[:space:]' ' ' <"$adr" >"$flatadr"
flatarch="$tmpdir/arch.flat"; tr -s '[:space:]' ' ' <"$archmd" >"$flatarch"

# The ADR is embedded BELOW the six ticket sections, under its own '# ADR:'
# heading (the ADR 012 / ADR 097 / ADR 115 precedent). Section-presence checks
# are scoped to that half so a ticket-prose mention cannot satisfy them.
adrbody="$tmpdir/128.body"
sed -n '/^# ADR: named-profile configuration file/,$p' "$adr" >"$adrbody"
flatbody="$tmpdir/128.body.flat"; tr -s '[:space:]' ' ' <"$adrbody" >"$flatbody"
if [ -s "$adrbody" ]; then
  pass "adr: embedded ADR heading '# ADR: named-profile configuration file …' present"
else
  bad "adr: embedded ADR heading missing — the ticket file no longer contains the ADR"
fi

# The ticket's Open questions section (above the ADR), where the dated operator
# acceptance is recorded per ticket-conventions §5.
openq="$tmpdir/128.openq"
sed -n '/^## Open questions/,/^## Out of scope/p' "$adr" >"$openq"

# --- 1. ADR completeness (Test plan bullet 1) --------------------------------
for section in '## Status' '## Context' '## Decision' '## Consequences' \
               '## Rejected alternatives'; do
  if grep -qF "$section" "$adrbody"; then
    pass "adr: section present: $section"
  else
    bad "adr: section missing: $section"
  fi
done
if grep -qF '**Accepted (2026-07-29).**' "$adrbody"; then
  pass "adr: Status is Accepted with the 2026-07-29 date"
else
  bad "adr: Status is not 'Accepted (2026-07-29)'"
fi
if grep -qF 'accepted on 2026-07-29' "$openq" \
   && grep -qF '"ya I accept those"' "$openq"; then
  pass "adr: dated operator acceptance recorded in the ticket's Open questions"
else
  bad "adr: Open questions no longer records the dated operator acceptance"
fi

# --- 2. Supersession recorded, narrowly (Test plan bullet 2) -----------------
# Each note names the config-file half ONLY; the DSL half of each rejection is
# asserted to stand, and the ORIGINAL rejection sentence is asserted to survive
# beside the note (merged decision text is never rewritten).
if grep -qF 'Superseded (in part) by ADR 128 (T113), 2026-07-29' "$flat089" \
   && grep -qF 'the *config file* half only' "$flat089"; then
  pass "adr089: partial-supersession note present and scoped to the config-file half"
else
  bad "adr089: partial-supersession note missing or no longer scoped to the config-file half"
fi
if grep -qF 'The DSL half of this rejection stands unchanged' "$flat089"; then
  pass "adr089: the DSL half of the rejection is stated to stand"
else
  bad "adr089: the note no longer states that the DSL half stands"
fi
if grep -qF '**A config file / DSL.** Out of the permanent scope boundary' "$flat089"; then
  pass "adr089: the original rejected-alternative sentence survives verbatim"
else
  bad "adr089: the original rejected-alternative sentence was rewritten or removed"
fi
if grep -qF 'misattributes' "$flat089"; then
  pass "adr089: the note records the misattribution in the original bullet"
else
  bad "adr089: the note no longer records the misattribution"
fi
if grep -qF 'Superseded (in part) by ADR 128 (T113), 2026-07-29' "$flat091" \
   && grep -qF 'the config-file half only' "$flat091"; then
  pass "adr091: partial-supersession note present and scoped to the config-file half"
else
  bad "adr091: partial-supersession note missing or no longer scoped to the config-file half"
fi
if grep -qF 'the **DSL** half stands' "$flat091"; then
  pass "adr091: the DSL half of the restatement is stated to stand"
else
  bad "adr091: the note no longer states that the DSL half stands"
fi
if grep -qF 'Any config-file or DSL surface — a permanent scope boundary' "$flat091"; then
  pass "adr091: the original out-of-scope clause survives verbatim"
else
  bad "adr091: the original out-of-scope clause was rewritten or removed"
fi

# --- 3. The narrow prohibitions survive (Test plan bullet 3) -----------------
# The spec-level, QUALIFIED prohibitions are the permanent ones. If a later
# edit drops the qualifier — or the sentence — the boundary has silently moved.
if grep -qF 'no configuration file describing the graph' "$flatarch"; then
  pass "arch: 'no configuration file describing the graph' survives"
else
  bad "arch: 'no configuration file describing the graph' is gone"
fi
if grep -qF 'describing the graph' "$readme"; then
  pass "readme: the 'describing the graph' qualifier survives"
else
  bad "readme: the 'describing the graph' qualifier is gone"
fi
if grep -qF 'describing the shape' "$corereadme"; then
  pass "core-readme: the 'describing the shape' qualifier survives"
else
  bad "core-readme: the 'describing the shape' qualifier is gone"
fi
if grep -F 'What it is not, permanently' "$archmd" | grep -qF 'domain-specific language'; then
  pass "arch: 'a domain-specific language' still on the permanent non-goals list"
else
  bad "arch: 'a domain-specific language' left the permanent non-goals list"
fi
if grep -qF 'There is no DSL, and no configuration file describing the graph' "$flatarch"; then
  pass "arch: C26 keeps 'no DSL' and 'no configuration file describing the graph'"
else
  bad "arch: C26's amended clause lost 'no DSL' or 'no configuration file describing the graph'"
fi

# --- 4. The misattribution is recorded (Test plan bullet 4) ------------------
# ADR 089's rejection cited an arch.md sentence that does not exist. The record
# of that finding is what stops the broad reading being re-derived later.
if grep -qF 'misquotes its own authority' "$flatadr" \
   && grep -qF 'No such sentence exists' "$flatadr"; then
  pass "adr: the ADR records that ADR 089's rejection misquotes its authority"
else
  bad "adr: the misattribution finding is no longer recorded"
fi

# --- 5. The purity constraint is stated as binding (Test plan bullet 5) ------
if grep -qF 'crates/core/tests/determinism_and_purity.rs' "$adr"; then
  pass "adr: names the purity test crates/core/tests/determinism_and_purity.rs"
else
  bad "adr: no longer names the purity test file"
fi
if [ -f "$puritytest" ]; then
  pass "purity: the named test file exists"
else
  bad "purity: crates/core/tests/determinism_and_purity.rs is missing"
fi
if grep -qF 'no configuration present' "$flatadr"; then
  pass "adr: cites C20's 'no configuration present' criterion"
else
  bad "adr: no longer cites C20's 'no configuration present' criterion"
fi
if grep -qF 'empty environment with no configuration present' "$flatarch"; then
  pass "arch: C20's 'empty environment with no configuration present' criterion stands"
else
  bad "arch: C20's empty-environment criterion changed"
fi

# --- 6. The decisions the milestone inherits (DoD bullet 4) ------------------
# Pinned in the ADR and, where the amendment states them, in arch.md C26 — a
# later ticket that quietly re-decides one of these has widened the carve-out.
if grep -qF 'flag > env > file(profile) > default' "$adrbody"; then
  pass "adr: four-tier precedence 'flag > env > file(profile) > default' recorded"
else
  bad "adr: the four-tier precedence order is no longer recorded"
fi
if grep -qF 'flag > env > file(profile) > default' "$flatarch"; then
  pass "arch: C26 states the four-tier precedence"
else
  bad "arch: C26 no longer states the four-tier precedence"
fi
if grep -qF -- '--dagr.profile' "$adrbody" && grep -qF 'DAGR_PROFILE' "$adrbody"; then
  pass "adr: profile selection via --dagr.profile / DAGR_PROFILE recorded"
else
  bad "adr: profile selection flag or env fallback is no longer recorded"
fi
if grep -qF 'layers over' "$flatbody"; then
  pass "adr: named profiles layer over the default profile"
else
  bad "adr: default-profile layering is no longer recorded"
fi
if grep -qF 'loud failure' "$flatbody" && grep -qF 'never a silent fallback' "$flatbody"; then
  pass "adr: an unknown profile is a loud failure, never a silent fallback"
else
  bad "adr: the unknown-profile-fails-loudly decision is no longer recorded"
fi
if grep -qF 'keep their exact names' "$flatadr"; then
  pass "adr: the nine DAGR_* spellings are recorded as unchanged"
else
  bad "adr: the unchanged-env-spellings decision is no longer recorded"
fi
if grep -qF 'injective' "$flatadr" && grep -qF 'total' "$flatadr"; then
  pass "adr: the mapping table's total-and-injective obligation recorded (T117)"
else
  bad "adr: the mapping-table obligation is no longer recorded"
fi
if grep -qF 'tri-state' "$flatadr" && grep -qF 'resolve_opt' "$flatadr"; then
  pass "adr: resolve_opt's tri-state (pinned vs absent-so-detect) is preserved"
else
  bad "adr: the tri-state preservation for the pool pins is no longer recorded"
fi
if grep -qF 'Read at bootstrap; never during assembly' "$adrbody"; then
  pass "adr: bootstrap-only read is a decision section of its own"
else
  bad "adr: the bootstrap-only-read decision section is gone"
fi
if grep -qF 'never read during assembly' "$flatarch"; then
  pass "arch: C26 states the file is never read during assembly"
else
  bad "arch: C26 no longer states the file is never read during assembly"
fi
if grep -qF '`dagr-core` never reads the file' "$flatadr"; then
  pass "adr: dagr-core never reads the file (zero-dep-core boundary)"
else
  bad "adr: the zero-dep-core boundary is no longer recorded"
fi
if grep -qF '`dagr-core` never reads it' "$flatarch"; then
  pass "arch: C26 states dagr-core never reads the file"
else
  bad "arch: C26 no longer states dagr-core never reads the file"
fi
if grep -qF 'parsed in `dagr-cli` only' "$flatadr"; then
  pass "adr: TOML, parsed in dagr-cli only"
else
  bad "adr: the TOML-in-dagr-cli-only decision is no longer recorded"
fi
if grep -qF 'a file-only knob is not introduced' "$flatadr"; then
  pass "adr: every knob keeps its reserved flag — no file-only knob"
else
  bad "adr: the no-file-only-knob invariant is no longer recorded"
fi

# --- 7. The inert-env-tier finding, and T114 before the loader ---------------
for fn in grace_from_env teardown_deadline_from_env failure_mode_from_env \
          resolve_pool_pins resolve_headroom; do
  if grep -qF "$fn" "$adr"; then
    pass "adr: inert-env finding names $fn"
  else
    bad "adr: inert-env finding no longer names $fn"
  fi
done
if grep -qF 'lands before the loader' "$flatadr" && grep -qF 'T114' "$adr"; then
  pass "adr: T114 is named as the fix that lands before the loader"
else
  bad "adr: the T114-before-the-loader ordering is no longer recorded"
fi

# --- 8. What would count as widening — still named as excluded ---------------
# The load-bearing half. Every one of these is asserted to be REJECTED or
# EXCLUDED by name, in the ADR's Rejected alternatives / the ticket's Out of
# scope / arch.md C26, so a later edit cannot quietly flip one to permitted.
if grep -qF 'A config file that can describe the graph' "$flatbody" \
   && grep -qF 'Rejected on the permanent boundary, unchanged' "$flatbody"; then
  pass "widen: a graph-describing config file is still rejected on the permanent boundary"
else
  bad "widen: the graph-describing-file rejection was weakened or removed"
fi
if grep -qF 'Letting a profile select the flow, or override node placement' "$flatadr"; then
  pass "widen: flow selection and placement override by profile still rejected"
else
  bad "widen: the flow-selection/placement rejection was weakened or removed"
fi
if grep -qF 'Reading the file during assembly' "$flatadr" \
   && grep -qF 'Rejected on purity' "$flatadr"; then
  pass "widen: reading the file during assembly still rejected on purity"
else
  bad "widen: the assembly-read rejection was weakened or removed"
fi
if grep -qF 'DAGR__SECTION__KEY' "$adrbody"; then
  pass "widen: derived DAGR__SECTION__KEY env names still named as rejected"
else
  bad "widen: the DAGR__SECTION__KEY rejection is gone"
fi
if grep -qF 'Secrets in the file.' "$flatadr" && grep -qF 'never tokens' "$flatadr"; then
  pass "widen: secrets in the file still rejected — buckets and endpoints, never tokens"
else
  bad "widen: the no-secrets rejection was weakened or removed"
fi
if grep -qF 'Anything that would let the file describe the graph' "$flatadr" \
   && grep -qF 'Permanently excluded, not deferred' "$flatadr"; then
  pass "widen: ticket Out of scope keeps graph-describing keys permanently excluded"
else
  bad "widen: the Out-of-scope permanent exclusion was weakened or removed"
fi
if grep -qF 'cannot select which flow runs, declare nodes or edges, or reach node policy or placement' "$flatarch"; then
  pass "widen: arch C26 names flow selection, nodes/edges, policy and placement as out of the file's reach"
else
  bad "widen: arch C26 no longer names what the file cannot reach"
fi
if grep -qF 'Run-level knobs only' "$flatbody"; then
  pass "widen: run-level-only reach recorded as a kept boundary"
else
  bad "widen: the run-level-only boundary is no longer recorded"
fi

# --- 9. The seams each M11 ticket inherits, and the reopen condition ---------
for seam in T114 T115 T116 T117 T118; do
  if grep -qF "$seam" "$adrbody"; then
    pass "adr: Consequences name the $seam seam"
  else
    bad "adr: the $seam seam is no longer named"
  fi
done
for nnn in 129 130 131 132 133; do
  if ls docs/implementation/${nnn}-*.md >/dev/null 2>&1; then
    pass "seams: ticket file docs/implementation/${nnn}-*.md exists"
  else
    bad "seams: ticket file docs/implementation/${nnn}-*.md is missing"
  fi
done
if grep -qF 'Reopen condition' "$adrbody"; then
  pass "adr: the reopen condition (purity vs file cannot coexist) is recorded"
else
  bad "adr: the reopen condition is no longer recorded"
fi

# --- 10. The amendment is in arch.md's own history ---------------------------
# The Amendment changelog's boundary section self-describes as listing EACH
# boundary-moving decision "so the spec's history is readable in one place";
# ADR 097 and ADR 115 have entries, so a missing ADR 128 entry means the spec's
# history no longer covers this amendment.
changelog="$tmpdir/arch.changelog"
sed -n '/^## Amendment changelog/,$p' "$archmd" >"$changelog"
if grep -qF 'ADR 128 (T113), 2026-07-29' "$changelog"; then
  pass "arch: Amendment changelog carries the dated ADR 128 (T113) entry"
else
  bad "arch: Amendment changelog has no ADR 128 (T113) entry"
fi
flatlog="$tmpdir/arch.changelog.flat"; tr -s '[:space:]' ' ' <"$changelog" >"$flatlog"
if grep -qF 'ADR 089 superseded in part' "$flatlog" \
   && grep -qF 'ADR 091' "$flatlog"; then
  pass "arch: the changelog entry names both partial supersessions (ADR 089, ADR 091)"
else
  bad "arch: the changelog entry does not name the ADR 089 / ADR 091 partial supersessions"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
