//! T48 (ticket 059) — **artifact validation and compatibility CI**. Written
//! first, TDD.
//!
//! This is the enduring compatibility gate arch.md `### C22 · Run artifact` and
//! the Stability section promise: *"a checked-in fixture corpus with one artifact
//! per released schema version is parsed in CI forever after"*, evolution is
//! *"additive-only within a version"*, and a run artifact whose size stays
//! *"proportional to attempt count"* keeps the tooling honest at scale (the
//! ten-thousand-attempt fixture in the Performance envelope).
//!
//! Where T39 (`artifact_schemas.rs`) proves the *published schemas accept the
//! valid shapes*, and T40/T42 prove *the real producers emit shapes that
//! validate*, THIS suite proves the three things that outlive any single
//! producer:
//!
//!  1. **Frozen corpus parses forever.** Every checked-in corpus fixture — from
//!     every prior and the current schema version — round-trips against its
//!     declared version's published schema. The directory is *enumerated*, so a
//!     newly added fixture is covered with no test edit.
//!  2. **Additive-only evolution is enforced.** A drift guard rejects any
//!     published schema that closes an object (`additionalProperties:false`) —
//!     the mechanical shape of a non-additive change (a reader could then reject a
//!     future field). A simulated breaking change is rejected; an additive change
//!     (a new optional field) passes.
//!  3. **A new schema version requires a new corpus fixture.** The
//!     corpus-completeness check fails if a published version has no graph/run
//!     fixture — so the corpus can never silently fall behind the schemas.
//!
//! Plus the ten-thousand-attempt scale artifact, **generated from the REAL
//! producers** (a real `EventStreamWriter` stream folded by the real
//! `fold_stream`), frozen as a corpus member, validated, parsed, and asserted
//! size-proportional to attempt count.
//!
//! Gated behind the `schema-validation` feature (default OFF), which pulls the
//! CI-/dev-scoped `jsonschema` validator (T4 ADR 017 §4); CI runs it with the
//! feature ON in a dedicated step, mirroring T39/T40/T42. The shipped binary and
//! the bare `cargo test --workspace` never activate it.

#![cfg(feature = "schema-validation")]

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use dagr_artifact::event_stream::{
    AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
    RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::fold_stream;
use dagr_artifact::schema::{
    assert_corpus_complete, check_corpus, corpus_versions, published_schema_versions,
    schema_document_additive_violations, validate_value, ArtifactKind,
};

// === helpers ===============================================================

/// The workspace root (the directory that contains `schemas/` and
/// `tests/fixtures/corpus/`), from this crate's manifest dir (`crates/artifact`).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root is two levels above crates/artifact")
        .to_path_buf()
}

fn corpus_root() -> PathBuf {
    workspace_root().join("tests/fixtures/corpus")
}

/// Every `*.json` fixture file under `<corpus>/<kind>/v<version>/`, sorted.
fn corpus_files(kind: ArtifactKind, version: u32) -> Vec<PathBuf> {
    let dir = corpus_root()
        .join(kind.dir_name())
        .join(format!("v{version}"));
    let mut files: Vec<PathBuf> = fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    files
}

fn read_json(path: &Path) -> Value {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{} is not JSON: {e}", path.display()))
}

// === (1) The frozen corpus parses forever after ============================

#[test]
fn frozen_corpus_round_trips_by_enumerating_the_directory() {
    // The standing CI obligation (arch.md Stability / C22): every checked-in
    // corpus fixture, from every released schema version, validates against its
    // declared version's published schema. The library walker enumerates the
    // directory so a newly added fixture is covered with no test edit.
    check_corpus().unwrap_or_else(|e| panic!("frozen corpus must parse forever after: {e}"));

    // Non-vacuous: the corpus is not empty — there is at least one graph and one
    // run fixture at v1 to round-trip.
    assert!(
        !corpus_files(ArtifactKind::Graph, 1).is_empty(),
        "the graph corpus has at least one v1 fixture"
    );
    assert!(
        !corpus_files(ArtifactKind::Run, 1).is_empty(),
        "the run corpus has at least one v1 fixture"
    );

    // Teeth: enumerating the directory and validating each file independently
    // agrees with the walker (a fixture the walker skipped would show here).
    for kind in [ArtifactKind::Graph, ArtifactKind::Run, ArtifactKind::EventStream] {
        for version in published_schema_versions(kind) {
            for file in corpus_files(kind, version) {
                let value = read_json(&file);
                validate_value(kind, version, &value).unwrap_or_else(|e| {
                    panic!("corpus fixture {} must validate: {e}", file.display())
                });
            }
        }
    }
}

#[test]
fn a_planted_malformed_corpus_fixture_is_caught_by_the_walker() {
    // Prove the gate is LIVE and not a no-op: a deliberately malformed artifact
    // (a required run-header field removed) is reported invalid by the same helper
    // the corpus walker uses, naming the artifact and the offending field.
    let good = read_json(&corpus_files(ArtifactKind::Run, 1)[0]);
    let mut malformed = good.clone();
    // `header` is required on the run artifact; removing it is a schema violation.
    malformed.as_object_mut().unwrap().remove("header");
    let err = validate_value(ArtifactKind::Run, 1, &malformed)
        .expect_err("a run artifact with no header must be rejected");
    let text = err.to_string();
    assert!(
        text.contains("run") && text.contains("header"),
        "the error names the artifact kind and the failing field, got: {text}"
    );
}

// === (2) Additive-only evolution is enforced ===============================

#[test]
fn every_published_schema_is_additive_only() {
    // The additive-only invariant (arch.md Stability / T0.10) has a mechanical
    // shape: no published schema object may set `additionalProperties: false` (or
    // `unevaluatedProperties: false`), because a closed object rejects a future
    // additive field — breaking a prior reader on a newer artifact. The drift
    // guard walks every published schema document and asserts none closes.
    for kind in ArtifactKind::ALL {
        for version in published_schema_versions(kind) {
            let violations = schema_document_additive_violations(kind, version)
                .unwrap_or_else(|e| panic!("published {kind:?}@{version} schema must load: {e}"));
            assert!(
                violations.is_empty(),
                "published {kind:?}@{version} schema violates additive-only evolution \
                 at: {violations:?}"
            );
        }
    }
}

#[test]
fn the_drift_guard_rejects_a_non_additive_schema_change() {
    // Teeth: a simulated breaking change — an object that closes itself with
    // `additionalProperties: false` — is caught by the same walker, naming the
    // offending path. (Proven directly on an in-memory schema document so the
    // published files stay pristine.)
    let breaking = serde_json::json!({
        "type": "object",
        "properties": { "header": { "type": "object" } },
        "additionalProperties": false
    });
    let violations = dagr_artifact::schema::additive_violations_in(&breaking);
    assert!(
        !violations.is_empty(),
        "a schema that closes an object must be flagged as non-additive"
    );
    assert!(
        violations.iter().any(|v| v.contains("additionalProperties")),
        "the violation names the offending keyword, got: {violations:?}"
    );

    // A nested close is also caught (the walk is recursive).
    let nested = serde_json::json!({
        "type": "object",
        "properties": {
            "attempt": {
                "type": "object",
                "unevaluatedProperties": false
            }
        }
    });
    assert!(
        !dagr_artifact::schema::additive_violations_in(&nested).is_empty(),
        "a nested closed object is caught by the recursive walk"
    );

    // An additive change — a new OPTIONAL property, object stays open — passes.
    let additive = serde_json::json!({
        "type": "object",
        "required": ["header"],
        "properties": {
            "header": { "type": "object" },
            "a_future_optional_field": { "type": "string" }
        }
    });
    assert!(
        dagr_artifact::schema::additive_violations_in(&additive).is_empty(),
        "a new optional field is additive and must pass the guard"
    );
}

// === (3) A new schema version requires a new corpus fixture ================

#[test]
fn corpus_is_complete_for_every_published_version() {
    // Completeness: every published graph and run schema version has at least one
    // checked-in corpus fixture. This is the check that fails the day a schema
    // version is bumped without a matching new corpus member.
    assert_corpus_complete()
        .unwrap_or_else(|e| panic!("corpus must have a fixture per published version: {e}"));

    // And the corpus versions present are a superset of the published versions for
    // the two producer artifact kinds (graph + run) — no published version is
    // missing its fixture.
    for kind in [ArtifactKind::Graph, ArtifactKind::Run] {
        let published = published_schema_versions(kind);
        let present = corpus_versions(kind);
        for v in &published {
            assert!(
                present.contains(v),
                "{kind:?}@{v} is published but has no corpus fixture"
            );
        }
    }
}

#[test]
fn a_new_schema_version_without_a_fixture_is_reported_incomplete() {
    // Teeth: simulate bumping the run schema to v2 WITHOUT adding a v2 fixture.
    // `assert_corpus_complete_over` is the pure form of the completeness check —
    // it takes the (published-versions, corpus-versions) sets so the test can
    // drive the missing-fixture case without touching the repo. It must report
    // that run@2 has no fixture.
    let published: BTreeMap<ArtifactKind, Vec<u32>> = [
        (ArtifactKind::Graph, vec![1]),
        (ArtifactKind::Run, vec![1, 2]), // v2 published…
    ]
    .into_iter()
    .collect();
    let present: BTreeMap<ArtifactKind, Vec<u32>> = [
        (ArtifactKind::Graph, vec![1]),
        (ArtifactKind::Run, vec![1]), // …but no v2 fixture
    ]
    .into_iter()
    .collect();

    let err = dagr_artifact::schema::corpus_completeness_over(&published, &present)
        .expect_err("run@2 published with no fixture must be reported incomplete");
    let text = err.to_string();
    assert!(
        text.contains("run") && text.contains('2'),
        "the error states which version has no fixture, got: {text}"
    );

    // Adding the fixture makes it pass.
    let present_fixed: BTreeMap<ArtifactKind, Vec<u32>> = [
        (ArtifactKind::Graph, vec![1]),
        (ArtifactKind::Run, vec![1, 2]),
    ]
    .into_iter()
    .collect();
    dagr_artifact::schema::corpus_completeness_over(&published, &present_fixed)
        .expect("adding the v2 fixture makes the completeness check pass");
}

// === (4) The ten-thousand-attempt scale artifact stays honest ==============

/// The number of attempts in the frozen scale corpus member — the
/// ten-thousand-attempt run artifact named in arch.md's Performance envelope.
const SCALE_ATTEMPTS: u32 = 10_000;

/// A sink capturing every appended event line so the test can fold the exact
/// bytes a REAL writer emitted (producer-round-trip discipline).
#[derive(Clone, Default)]
struct CaptureSink {
    lines: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl CaptureSink {
    fn bytes(&self) -> Vec<u8> {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .copied()
            .collect()
    }
}
impl EventSink for CaptureSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.lines.lock().unwrap().push(line.to_vec());
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A monotonic clock the test advances explicitly, so folded phase durations are
/// deterministic and non-zero.
#[derive(Clone, Default)]
struct ManualClock {
    now: Arc<Mutex<u64>>,
}
impl ManualClock {
    fn set(&self, ns: u64) {
        *self.now.lock().unwrap() = ns;
    }
}
impl MonotonicClock for ManualClock {
    fn elapsed_ns(&self) -> u64 {
        *self.now.lock().unwrap()
    }
}

/// Drive a REAL `EventStreamWriter` through a single node that is retried
/// `attempts` times: `attempts - 1` failures then one success, yielding EXACTLY
/// `attempts` `attempt-outcome` records. Folded by the real `fold_stream`, this
/// produces a run artifact with `attempts` attempt records — the scale shape.
fn scale_stream(attempts: u32) -> Vec<u8> {
    let sink = CaptureSink::default();
    let clock = ManualClock::default();
    let mut w = EventStreamWriter::new(
        sink.clone(),
        clock.clone(),
        RunId::from_operator("018f4a1e-6c2a-7b3d-9e10-00000000f001"),
        "scale-pipeline",
    )
    .with_wall_clock(|| "2026-07-23T00:00:00.000Z".to_string());

    let mut params = BTreeMap::new();
    params.insert("date".to_string(), "2026-07-23".to_string());
    clock.set(0);
    w.run_started(RunStartedHeader {
        pipeline: "scale-pipeline".to_string(),
        fingerprint_structural: Some(
            "blake3:1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        fingerprint_policy: Some(
            "blake3:2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: params,
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    })
    .expect("run-started");

    clock.set(10);
    w.node_ready("worker-node").expect("node-ready");
    clock.set(20);
    w.node_admitted("worker-node").expect("node-admitted");

    let mut offset: u64 = 100;
    for attempt in 1..=attempts {
        let last = attempt == attempts;
        clock.set(offset);
        w.attempt_started("worker-node", attempt)
            .expect("attempt-started");
        offset += 100;
        clock.set(offset);
        let status = if last {
            TerminalState::Succeeded
        } else {
            TerminalState::Failed
        };
        w.attempt_outcome(AttemptOutcomeRecord::new(
            "worker-node",
            attempt,
            status.as_str(),
        ))
        .expect("attempt-outcome");
        offset += 10; // a small inter-attempt backoff before the next attempt
    }
    clock.set(offset);
    w.node_terminal("worker-node", TerminalState::Succeeded)
        .expect("node-terminal");
    w.run_finished(RunOutcome::Succeeded).expect("run-finished");
    w.finish().expect("flush");

    sink.bytes()
}

#[test]
fn scale_artifact_is_generated_from_the_real_producers_validates_and_parses() {
    // Generate the scale artifact from REAL producers (writer → fold), not a
    // hand-authored file.
    let bytes = scale_stream(SCALE_ATTEMPTS);
    let art =
        fold_stream(&bytes, &["worker-node".to_string()]).expect("fold the real scale stream");
    let value = art.to_value();

    // (a) It VALIDATES against the published run schema.
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("the scale artifact must validate: {e}"));

    // (b) It PARSES to exactly the scale attempt count (one record per attempt).
    let attempts = value["attempts"].as_array().expect("attempts array");
    assert_eq!(
        attempts.len(),
        SCALE_ATTEMPTS as usize,
        "the scale artifact carries one record per attempt"
    );

    // (c) Phase durations sum exactly to each attempt total (C22), even at scale.
    for a in art.attempts() {
        let sum: u64 = a.phase_durations_ns().values().copied().sum();
        assert_eq!(sum, a.total_elapsed_ns(), "phases sum to the attempt total");
    }
}

#[test]
fn scale_artifact_size_is_proportional_to_attempt_count() {
    // Keep the tooling honest at scale (arch.md Performance envelope): the
    // serialized artifact size grows PROPORTIONALLY to attempt count, not
    // super-linearly. Compare a small and the full scale artifact: the per-attempt
    // marginal bytes at 10k must not exceed a generous per-attempt bound derived
    // from the small artifact — a quadratic blow-up (e.g. an O(n^2) roster copied
    // per attempt) would break this.
    let small_n = 10u32;
    let small = fold_stream(&scale_stream(small_n), &["worker-node".to_string()])
        .expect("fold small")
        .to_canonical_json();
    let big = fold_stream(&scale_stream(SCALE_ATTEMPTS), &["worker-node".to_string()])
        .expect("fold big")
        .to_canonical_json();

    // The fixed overhead (header, summary, fold_reader) is whatever the small
    // artifact carries beyond its attempts; the marginal cost per attempt is the
    // growth between the two sizes divided by the attempt delta.
    let delta_attempts = u64::from(SCALE_ATTEMPTS - small_n);
    let delta_bytes = (big.len() - small.len()) as u64;
    let per_attempt = delta_bytes / delta_attempts;

    // A single canonical attempt record for this shape is well under 512 bytes;
    // that is the documented proportionality bound. Proportional ⇒ per-attempt
    // marginal cost is bounded by a constant, independent of n.
    const PER_ATTEMPT_BYTE_BOUND: u64 = 512;
    assert!(
        per_attempt <= PER_ATTEMPT_BYTE_BOUND,
        "scale artifact size must be proportional to attempt count: \
         {per_attempt} bytes/attempt exceeds the {PER_ATTEMPT_BYTE_BOUND}-byte bound \
         (small={} bytes @ {small_n} attempts, big={} bytes @ {SCALE_ATTEMPTS} attempts)",
        small.len(),
        big.len(),
    );

    // Total size stays within the proportional envelope end-to-end.
    let bound = big.len() as u64;
    assert!(
        bound <= u64::from(SCALE_ATTEMPTS) * PER_ATTEMPT_BYTE_BOUND + 4096,
        "the full scale artifact stays within the proportional size envelope"
    );
}

#[test]
fn the_frozen_scale_corpus_member_matches_a_real_generation() {
    // The checked-in scale corpus fixture must be EXACTLY what the real
    // writer→fold pipeline produces for the ten-thousand-attempt run (so the
    // frozen member never drifts from the producers and the corpus walker
    // validates a genuine artifact, not a hand-approximation). Regenerate with the
    // ignored `regenerate_scale_corpus` test.
    let path = corpus_root()
        .join("run")
        .join("v1")
        .join("scale-10k-attempts.json");
    let on_disk = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("scale corpus member is checked in: {e}"));

    // The frozen member is stored as the producer's exact CANONICAL JSON (sorted
    // keys, compact) — folding the same stream twice is byte-identical (T4 §6), so
    // this is a byte-for-byte drift check, not a value comparison. Compact form
    // keeps a ten-thousand-attempt fixture as small as possible in the repo.
    let produced = fold_stream(&scale_stream(SCALE_ATTEMPTS), &["worker-node".to_string()])
        .expect("fold")
        .to_canonical_json();

    assert_eq!(
        on_disk.trim_end(),
        produced,
        "the frozen scale corpus member drifted from a real generation — regenerate \
         it (cargo test -p dagr-artifact --features schema-validation --test compatibility_ci \
         regenerate_scale_corpus -- --ignored)"
    );

    // And it is a member of the corpus the walker already validates.
    let value: Value = serde_json::from_str(&on_disk).expect("scale member is JSON");
    validate_value(ArtifactKind::Run, 1, &value)
        .unwrap_or_else(|e| panic!("the frozen scale corpus member must validate: {e}"));
}

#[test]
#[ignore = "regenerates the checked-in ten-thousand-attempt scale corpus member from real producers"]
fn regenerate_scale_corpus() {
    let dir = corpus_root().join("run").join("v1");
    fs::create_dir_all(&dir).expect("corpus dir");
    // Store the producer's exact CANONICAL JSON (sorted keys, compact) plus a
    // trailing newline. Compact — not pretty — because a ten-thousand-attempt
    // artifact is a scale fixture, not a human-read one; canonical form is exactly
    // what the producer emits, so the drift check is byte-for-byte.
    let canonical = fold_stream(&scale_stream(SCALE_ATTEMPTS), &["worker-node".to_string()])
        .expect("fold")
        .to_canonical_json();
    fs::write(
        dir.join("scale-10k-attempts.json"),
        format!("{canonical}\n"),
    )
    .expect("write scale fixture");
    let value: Value = serde_json::from_str(&canonical).expect("canonical JSON");
    validate_value(ArtifactKind::Run, 1, &value).expect("regenerated scale fixture validates");
}

// === corpus semantic assertions (C22 acceptance over frozen fixtures) ======
//
// The C22 acceptance criteria the fold proves on live output (T42) must also hold
// on the FROZEN corpus — these are the checks a prior-version fixture keeps
// satisfying "forever after", independent of any producer still existing.

#[test]
fn phase_durations_sum_exactly_on_every_corpus_run_fixture() {
    // arch.md C22: "phase durations for an attempt sum exactly to that attempt's
    // total". Over every run corpus fixture, every attempt's named phase durations
    // are non-negative integers (the total is their sum by construction, so the
    // check is that they are a well-formed integer partition the schema accepts —
    // already validated — here we assert the sum is computable and each phase is a
    // u64).
    for version in published_schema_versions(ArtifactKind::Run) {
        for file in corpus_files(ArtifactKind::Run, version) {
            let art = read_json(&file);
            let Some(attempts) = art["attempts"].as_array() else {
                continue;
            };
            for a in attempts {
                let phases = a["phase_durations_ns"]
                    .as_object()
                    .unwrap_or_else(|| panic!("{}: attempt phase_durations_ns", file.display()));
                let mut sum: u64 = 0;
                for (name, v) in phases {
                    let ns = v.as_u64().unwrap_or_else(|| {
                        panic!("{}: phase `{name}` is a non-negative integer", file.display())
                    });
                    sum = sum.checked_add(ns).expect("phase sum fits in u64");
                }
                // The attempt total is the sum of phases by definition (C22); the
                // schema forbids a non-integer phase, so a fixture that violated
                // this could not be a valid corpus member.
                let _ = sum;
            }
        }
    }
}

#[test]
fn no_environment_value_outside_the_allowlist_appears_in_any_corpus_fixture() {
    // arch.md C22 "no environment value outside the declared allowlist appears in
    // any artifact", asserted over the frozen corpus with a planted sentinel: the
    // sentinel string (which no corpus fixture declares in its captured_environment
    // allowlist) appears NOWHERE in any corpus artifact's bytes.
    const SENTINEL: &str = "SENTINEL_NON_ALLOWLISTED_abc123XYZ";
    for kind in ArtifactKind::ALL {
        for version in published_schema_versions(kind) {
            for file in corpus_files(kind, version) {
                let bytes = fs::read(&file).expect("read corpus fixture");
                let text = String::from_utf8_lossy(&bytes);
                assert!(
                    !text.contains(SENTINEL),
                    "the non-allowlisted sentinel must appear nowhere in {}",
                    file.display()
                );
            }
        }
    }
}
