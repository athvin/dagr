#!/usr/bin/env bash
# Artifact-serialization-format and schema-versioning ADR acceptance checks for
# ticket 017 (T4).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done
# (docs/implementation/017-T4-artifact-serialization-format-adr.md). T4 is a
# DECISION ticket whose durable deliverable is an ADR that locks the serialization
# contract shared by C19 (event stream), C20 (graph artifact), and C22 (run
# artifact): the event-stream encoding (JSON Lines, one self-contained JSON object
# per line, written through the T0.6 line-append sink); the artifact encoding
# (a single JSON document per graph/run artifact, with the canonical form
# authoritative for byte-identity and a pretty form for human reading); the
# schema-version field (name, placement on every event record and every artifact
# header, value form, and its additive-only / ignore-unknown / default-missing
# semantics under T0.10); the JSON Schema draft and Rust validation crate that
# satisfy "validates against its published schema" for C20/C22; the in-repo schema
# directory + per-kind/per-version file naming, and the sibling fixture-corpus
# layout (T0.10); and the canonicalization rules (key ordering, number/whitespace
# formatting, string escaping, generation-time exclusion) sufficient for C20's
# byte-identity criterion. Its "tests" are documentary completeness and
# internal-consistency checks against the recorded ADR, PLUS the quoted throwaway
# spike evidence that JSONL round-trips (with one-trailing-partial tolerance), that
# canonicalization is byte-deterministic outside generation time, that the chosen
# draft + crate validate a well-formed artifact and reject a malformed one, and
# that an unknown future field passes validation and is ignored by a reader.
#
# The load-bearing assertions are COMPLETENESS (every decision the ticket is
# chartered to make is recorded, each with its chosen option, a rationale, and at
# least one named rejected alternative) and INTERNAL CONSISTENCY with the merged
# dependency ADRs: T0.6 (the reserved file names events.jsonl / graph.json /
# run.json and the line-append sink), T0.10 (additive-only / ignore-unknown /
# default-missing evolution + the tests/fixtures/corpus fixture plan), T0.7 (the
# canonicalization must produce deterministic bytes so BLAKE3 v1 fingerprinting is
# reproducible), and T0.8 (the run-artifact shape reserves the optional per-attempt
# durable_reference slot). Authored FIRST as the acceptance gate, it fails on the
# ticket file as it stands before the ADR is written into it, and passes once the
# ADR records every element the ticket is chartered to lock.
#
# This is a DOC-ONLY decision (the serialization format and schema-versioning
# scheme are fully decidable from arch.md + the merged ADRs; the WRITERS are T19,
# the SCHEMA FILES and fixture corpus are T39/T48, the artifact TYPES are
# T40/T42). So the script does NOT build or run any prototype and asserts no
# production code: the shipping crates and Cargo.lock are untouched, and the only
# committed artifacts are the embedded ADR and this mechanical acceptance script.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/017-T4-artifact-serialization-format-adr.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T0.6/T0.7/T0.8/T0.10 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already names JSONL, JSON, the
# schema-version field, etc. — so a whole-file grep would pass content checks the
# ADR itself has not yet made. We therefore scope every content assertion to the
# ADR BODY only: the slice of the file from the first 'ADR:' heading line to EOF.
# The ticket's own H1 title ('# 017 · T4 — …') is deliberately NOT matched (no
# 'ADR:' with a colon), so before the embedded ADR is authored the slice is empty
# and every content check fails — exactly the tests-first behaviour we want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

# Case-insensitive extended-regex grep, scoped to the ADR body only.
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }

# --- ADR skeleton (ticket-conventions §4: decision tickets need status /
# context / decision / consequences / rejected alternatives) ------------------
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

# --- Accepted status (ticket-conventions: decision ADRs are Accepted) --------
if has '^#+[[:space:]]+Status' && has 'accepted'; then
  pass "status: the ADR is in an Accepted status (not draft/proposed)"
else
  bad "status: the ADR must be recorded in an Accepted status"
fi
# NB: scoped to a draft/proposed *status declaration* — a bare "draft" appears
# legitimately in this ADR ("JSON Schema draft 2020-12"), so we only reject
# draft/proposed as an ADR STATUS (adjacent to the Status heading / "status:"),
# not the standalone word.
if has 'status[^[:alnum:]]+(draft|proposed)' || has '(draft|proposed)[^[:alnum:]]+status'; then
  bad "status: the ADR must not be draft/proposed"
else
  pass "status: no draft/proposed status leaked in"
fi

# --- 1. Event-stream encoding: JSON Lines (DoD 1; Test plan JSONL) -----------
# One self-contained JSON object per line, written through the T0.6 line-append
# sink, into the reserved events.jsonl; a record is a single-line JSON object.
if (has 'json lines' || has 'jsonl' || has 'json-lines') && (has 'one .* per line' || has 'per line' || has 'single-line' || has 'single line' || has 'newline-delimited' || has 'newline delimited'); then
  pass "jsonl: event stream is JSON Lines — one self-contained JSON object per line"
else
  bad "jsonl: the JSON-Lines event-stream encoding decision is missing"
fi
if (has 'events\.jsonl') && (has 'T0\.6' || has 'sink' || has 'append'); then
  pass "jsonl: records are written through the T0.6 line-append sink into events.jsonl"
else
  bad "jsonl: the tie to the T0.6 sink / events.jsonl is missing"
fi
if (has 'json object' || has 'object per line' || has 'value shape') && (has 'trailing partial' || has 'trailing-partial' || has 'partial record'); then
  pass "jsonl: the record value shape (object) and trailing-partial tolerance are stated"
else
  bad "jsonl: the record value shape / trailing-partial tolerance statement is missing"
fi

# --- 2. Artifact encoding: JSON, canonical vs pretty (DoD 2; Test plan) ------
# A single JSON document per graph and per run artifact; canonical form
# authoritative for byte-identity (C20), pretty form for human reading.
if (has 'graph.json' && has 'run.json') && (has 'single .* json' || has 'one json' || has 'json document' || has 'json artifact'); then
  pass "artifact: graph and run artifacts are each a single JSON document (graph.json/run.json)"
else
  bad "artifact: the single-JSON-document-per-artifact decision (graph.json/run.json) is missing"
fi
if (has 'canonical' && (has 'authoritative' || has 'byte-identity' || has 'byte identity' || has 'byte-ident')) && (has 'pretty' || has 'human read' || has 'human-read' || has 'human reading'); then
  pass "artifact: the canonical form is authoritative for byte-identity; pretty form is for human reading"
else
  bad "artifact: the canonical-authoritative / pretty-for-humans distinction is missing"
fi

# --- 3. Schema-version field (DoD 3; Test plan schema-version semantics) ------
# field name, placement on every event record AND every artifact header, value
# form, and additive-only / ignore-unknown / default-missing semantics.
if (has 'schema_version' || has 'schema-version field' || has 'schema version field') && (has 'field'); then
  pass "schemaver: the schema-version field name is fixed"
else
  bad "schemaver: the schema-version field name is missing"
fi
if (has 'every event record' || has 'each event record' || has 'per event' || has 'on every record') && (has 'artifact header' || has 'every artifact' || has 'each artifact'); then
  pass "schemaver: placement fixed on every event record (C19) and every artifact header (C20/C22)"
else
  bad "schemaver: the schema-version-field placement (event record + artifact header) is missing"
fi
if (has 'name@version' || has '<name>@<version>' || has 'name.?@.?version' || has 'dagr\.' || has 'integer'); then
  pass "schemaver: the value form (a <name>@<version> string, or a documented integer) is stated"
else
  bad "schemaver: the schema-version value form is missing"
fi
if (has 'additive') && (has 'ignore unknown' || has 'ignore-unknown' || has 'unknown field') && (has 'default missing' || has 'default-missing' || has 'missing field' || has 'missing .* default'); then
  pass "schemaver: additive-only / ignore-unknown / default-missing evolution semantics are recorded (T0.10)"
else
  bad "schemaver: the additive-only / ignore-unknown / default-missing semantics are missing"
fi
if (has 'version bump' || has 'bump' || has 'new .* version' || has 'new schema version') && (has 'non-additive' || has 'not additive' || has 'anything else' || has 'breaking'); then
  pass "schemaver: a non-additive change requires a version bump"
else
  bad "schemaver: the version-bump-for-non-additive-change rule is missing"
fi
if (has 'fold' || has 'folding function') && (has 'which .* version' || has 'declares .* version' || has 'stream schema version' || has 'reads'); then
  pass "schemaver: the folding function declares which stream schema versions it reads (C22)"
else
  bad "schemaver: the folding-function-declares-versions record is missing"
fi

# --- 4. Schema language + validation crate (DoD 5; Open question) ------------
# a JSON Schema draft + a Rust validation crate satisfying validates-against-
# published-schema for C20/C22, weighed against the minimal-dependency posture.
if (has 'json schema' || has 'json-schema') && (has '2020-12' || has 'draft 2020' || has 'draft-2020' || has 'draft 7' || has 'draft-07' || has 'draft'); then
  pass "schema-lang: a concrete JSON Schema draft is chosen"
else
  bad "schema-lang: no concrete JSON Schema draft is chosen"
fi
if (has 'jsonschema' || has 'validation crate' || has 'validator crate' || has 'crate'); then
  pass "schema-lang: a concrete Rust validation crate is named"
else
  bad "schema-lang: no Rust validation crate is named"
fi
if (has 'minimal.?dependency' || has 'minimal dependency' || has 'supply chain' || has 'supply-chain' || has 'core crate') && (has 'dev' || has 'CI' || has 'not .* core' || has 'default feature' || has 'trim'); then
  pass "schema-lang: the crate choice is weighed against the core-crate minimal-dependency posture"
else
  bad "schema-lang: the minimal-dependency / supply-chain weighing is missing"
fi

# --- 5. In-repo schema location + naming + fixture corpus (DoD 7; Test plan) --
if (has 'schema' && (has 'directory' || has 'in-repo' || has 'in repo' || has 'location')) && (has 'per-kind' || has 'per kind' || has 'per-version' || has 'per version' || has 'filename' || has 'file name' || has 'naming'); then
  pass "location: an in-repo schema directory and a per-kind/per-version filename scheme are fixed"
else
  bad "location: the schema directory / per-kind/per-version naming scheme is missing"
fi
if (has 'fixture' && (has 'corpus' || has 'one .* per .* version' || has 'per released schema version')) && (has 'tests/fixtures/corpus' || has 'fixtures/corpus' || has 'sibling' || has 'beside'); then
  pass "location: the sibling fixture-corpus layout (one artifact per released schema version) is fixed (T0.10)"
else
  bad "location: the fixture-corpus layout is missing"
fi
if (has 'T39') && (has 'T48'); then
  pass "location: T39 (schema publication) and T48 (validation CI) get a fixed target"
else
  bad "location: the T39/T48 fixed-target hand-off is missing"
fi

# --- 6. Canonicalization rules (DoD 6; Test plan byte-identity) --------------
# key ordering, number formatting, whitespace, string escaping, and generation-
# time exclusion from byte comparison.
canon_ok=1
for term in 'key order' 'number' 'whitespace' 'escap'; do
  has "$term" || canon_ok=0
done
if [ "$canon_ok" = 1 ] || ( (has 'key ordering' || has 'sorted key' || has 'sort .* key' || has 'lexicographic') && (has 'whitespace' || has 'compact') && (has 'escap' || has 'string escap') ); then
  pass "canon: canonicalization fixes key ordering, number formatting, whitespace, and string escaping"
else
  bad "canon: the canonicalization rules (key order / number / whitespace / escaping) are incomplete"
fi
if (has 'generation.?time' || has 'generation-time' || has 'generation time' || has 'generated_at') && (has 'exclud' || has 'excepted' || has 'aside' || has 'outside'); then
  pass "canon: the generation-time field is explicitly excluded from byte comparison (C20, C21)"
else
  bad "canon: the generation-time-exclusion rule is missing"
fi
if (has 'byte-identical' || has 'byte identical' || has 'identical bytes' || has 'deterministic byte') && (has 'twice' || has 'emitting twice' || has 'emit twice' || has 'produce .* identical'); then
  pass "canon: emitting twice produces identical bytes outside the generation-time field (C20)"
else
  bad "canon: the emit-twice-byte-identical property is missing"
fi
# Consistency with T0.7: canonical bytes must feed BLAKE3 v1 fingerprinting.
if (has 'T0\.7' || has 'fingerprint' || has 'BLAKE3' || has 'blake3') && (has 'deterministic' || has 'determinism' || has 'byte-stable' || has 'reproducib'); then
  pass "canon: the deterministic bytes are consistent with T0.7's BLAKE3-v1 fingerprinting"
else
  bad "canon: the tie to T0.7 fingerprint determinism is missing"
fi

# --- 7. Trailing-partial + concurrent-run concatenation (DoD 8) --------------
if (has 'at most one' || has 'one trailing' || has 'single .* partial') && (has 'trailing partial' || has 'trailing-partial' || has 'partial record'); then
  pass "stream: at most one trailing partial record is tolerated on the event stream (C19)"
else
  bad "stream: the at-most-one-trailing-partial tolerance is missing"
fi
if (has 'concurrent' || has 'concat' || has 'partition') && (has 'run identity' || has 'run id' || has 'partitioned by run'); then
  pass "stream: concatenating concurrent-run records, partitioned by run identity, stays valid (C19)"
else
  bad "stream: the concurrent-run concatenation/partition record is missing"
fi

# --- 8. Artifact variants the schemas must accommodate (DoD 9) ---------------
# assembly-failed / bootstrap-failed and single-node not-requested, deferring
# field definitions to T39.
if (has 'assembly-failed' || has 'assembly failed') && (has 'bootstrap-failed' || has 'bootstrap failed') && (has 'not-requested' || has 'not requested'); then
  pass "variants: the assembly-failed / bootstrap-failed / not-requested artifact variants are named (C22)"
else
  bad "variants: the assembly-failed/bootstrap-failed/not-requested variant naming is missing"
fi
if (has 'defer' || has 'T39') && (has 'field' && (has 'variant' || has 'T39')); then
  pass "variants: field definitions of the variants are deferred to T39"
else
  bad "variants: the defer-variant-fields-to-T39 record is missing"
fi
# Consistency with T0.8: reserve the optional per-attempt durable_reference slot.
if (has 'durable_reference' || has 'durable reference' || has 'durable-output reference') && (has 'T0\.8' || has 'optional' || has 'reserve' || has 'per-attempt' || has 'per attempt'); then
  pass "variants: the run-artifact shape reserves the optional per-attempt durable_reference slot (T0.8/C27)"
else
  bad "variants: the durable_reference slot reservation (T0.8) is missing"
fi

# --- 9. Throwaway spike evidence captured (DoD 10; Test plan prototypes) ------
if (has 'spike' || has 'prototype' || has 'throwaway') && (has 'evidence' || has 'EVIDENCE' || has 'deleted' || has 'not merged' || has 'quoted'); then
  pass "spike: throwaway-prototype evidence is captured and the prototype is stated not-merged"
else
  bad "spike: the throwaway-prototype evidence / not-merged statement is missing"
fi
# The four specific evidence claims the Test plan requires.
if (has 'round-trip' || has 'round trip' || has 'parses' || has 'parsed') && (has 'trailing partial' || has 'trailing-partial' || has 'discard'); then
  pass "spike: JSONL round-trip / trailing-partial evidence is present"
else
  bad "spike: the JSONL round-trip / trailing-partial evidence is missing"
fi
if (has 'well-formed' || has 'wellformed' || has 'well formed') && (has 'malformed') && (has 'valid' || has 'validate' || has 'fail'); then
  pass "spike: validates-against-schema evidence (well-formed passes, malformed fails) is present"
else
  bad "spike: the validates-against-schema evidence is missing"
fi
if (has 'unknown field' || has 'future .* field' || has 'additive') && (has 'still valid' || has 'still passes' || has 'ignore' || has 'passes validation'); then
  pass "spike: additive-evolution evidence (unknown field validates and is ignored) is present"
else
  bad "spike: the additive-evolution evidence is missing"
fi

# --- Components referenced -----------------------------------------------------
comp_ok=1
for c in 'C19' 'C20' 'C22'; do
  has "$c" || comp_ok=0
done
if [ "$comp_ok" = 1 ]; then
  pass "component: C19, C20, C22 all referenced"
else
  bad "component: not every governing component (C19, C20, C22) is referenced"
fi
if (has 'stability') ; then
  pass "component: the Stability section is cross-referenced"
else
  bad "component: the Stability section cross-reference is missing"
fi

# --- Dependency ADRs cross-referenced (consistency is load-bearing) ----------
dep_ok=1
for d in 'T0\.6' 'T0\.10' 'T0\.7' 'T0\.8'; do
  has "$d" || dep_ok=0
done
if [ "$dep_ok" = 1 ]; then
  pass "deps: T0.6, T0.10, T0.7, T0.8 all cross-referenced for consistency"
else
  bad "deps: not every dependency ADR (T0.6, T0.10, T0.7, T0.8) is cross-referenced"
fi

# --- Rejected alternatives (ticket-conventions §4; Out of scope) -------------
# no binary / protobuf / Avro / columnar; each decision needs a named rejected
# alternative.
if (has 'protobuf' || has 'avro' || has 'binary' || has 'columnar') && (has 'reject' || has 'rejected'); then
  pass "rejected: a binary/protobuf/Avro/columnar format is named and rejected"
else
  bad "rejected: the binary-format rejected alternative is missing"
fi
if (has 'reopen'); then
  pass "rejected: the reopen condition is stated"
else
  bad "rejected: the reopen condition is missing"
fi

# --- Open questions discharged (ticket §Open questions; tasks.md T4 Q:) -------
if (has 'open question' || has 'no open question') && (has 'draft' || has 'crate' || has 'resolved' || has 'none'); then
  pass "questions: the ADR records its open-questions disposition (the schema-draft/crate Q:)"
else
  bad "questions: the ADR does not state its open-questions disposition"
fi

# --- Coverage-matrix disposition (decision ticket owes no covering test) ------
if (has 'coverage matrix' || has 'coverage-matrix') && (has 'no change' || has 'no edit' || has 'unmapped' || has 'no .* change'); then
  pass "coverage: the ADR states its (no-)coverage-matrix disposition"
else
  bad "coverage: the ADR does not state its coverage-matrix disposition"
fi

# --- Scope boundary (permanent non-goals) ------------------------------------
if (has 'no binary' || has 'no protobuf' || has 'no metadata' || has 'metadata store' || has 'not a metadata store' || has 'web') && (has 'scope' || has 'boundary' || has 'non-goal' || has 'permanent'); then
  pass "scope: the ADR restates the permanent-non-goals boundary (no binary formats, no metadata store/web viewer)"
else
  bad "scope: the ADR does not restate the permanent-non-goals boundary"
fi

# --- No production code was added (DOC-ONLY decision) ------------------------
if (has 'no .* code' || has 'no production code' || has 'doc-only' || has 'doc only' || has 'crates .* unchanged' || has 'writers .* T19' || has 'T19'); then
  pass "doc-only: the ADR records that it ships no production code (writers T19, schemas T39/T48)"
else
  bad "doc-only: the ADR does not record its doc-only disposition"
fi

if [ "$fail" = 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
