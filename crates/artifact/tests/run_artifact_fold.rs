//! C22 · Run-artifact **fold** — behavioral test suite (T42 / ticket 053).
//!
//! Each `#[test]` realizes one Setup/Action/Expected scenario from the ticket's
//! Test plan against the standalone fold ([`dagr_artifact::fold::fold_stream`]).
//! The fold is a *reader* over the append-only C19 event stream (T19) that
//! produces the C22 run artifact — it needs no run store, no live graph, and no
//! network. These tests drive it over hand-built streams (the published T39
//! event-stream wire form, `kind`-discriminated JSON Lines) and assert on the
//! produced [`RunArtifact`] and its serialized JSON.
//!
//! The schema round-trip (validating a REAL folded artifact against the
//! published `schemas/run/v1.schema.json` via the T39 helper) lives behind the
//! `schema-validation` feature in `run_artifact_fold_schema.rs`; this suite runs
//! in the default `cargo test --workspace` and covers C22's behavioral criteria.

use serde_json::{json, Value};

use dagr_artifact::fold::{
    fold_stream, FoldError, ACCEPTED_STREAM_SCHEMA_VERSIONS, FOLD_READER_VERSION,
};

// === stream-building helpers ==============================================

/// The shared C19 record envelope every event carries (published T39 wire form:
/// `kind`-discriminated, `wall` an informational string, `offset_ns` the
/// authoritative monotonic offset).
fn env(seq: u64, offset_ns: u64, kind: &str) -> Value {
    json!({
        "schema_version": "dagr.event-stream@1",
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "seq": seq,
        "wall": "2026-07-23T00:00:00.000Z",
        "offset_ns": offset_ns,
        "kind": kind,
    })
}

fn with(mut v: Value, fields: &[(&str, Value)]) -> Value {
    let o = v.as_object_mut().unwrap();
    for (k, val) in fields {
        o.insert((*k).to_string(), val.clone());
    }
    v
}

/// The run-started record carrying the full start-header.
fn run_started(seq: u64, offset_ns: u64, header: Value) -> Value {
    with(env(seq, offset_ns, "run-started"), &[("header", header)])
}

/// A minimal complete start-header (assembly succeeded → fingerprints present).
fn start_header() -> Value {
    json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
        "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
        "fingerprint_algorithm_version": 1,
        "parameters": { "date": "2026-07-23" },
        "data_interval": { "start": "2026-07-23T00:00:00Z", "end": "2026-07-24T00:00:00Z" },
        "captured_environment": {},
        "resume_lineage": null,
    })
}

/// Serialize a list of records to JSON-Lines bytes (each record on its own
/// line, newline-terminated).
fn stream(records: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for r in records {
        out.push_str(&serde_json::to_string(r).unwrap());
        out.push('\n');
    }
    out.into_bytes()
}

/// The typical "one attempt lifecycle" record run: node-ready, node-admitted,
/// attempt-started, then an attempt-outcome carrying the rich payload, then the
/// node-terminal transition.
#[allow(clippy::too_many_arguments)]
fn attempt_outcome(
    seq: u64,
    offset_ns: u64,
    node: &str,
    attempt: u32,
    status: &str,
    extra: &[(&str, Value)],
) -> Value {
    let mut fields = vec![
        ("node", json!(node)),
        ("attempt", json!(attempt)),
        ("status", json!(status)),
    ];
    fields.extend(extra.iter().cloned());
    with(env(seq, offset_ns, "attempt-outcome"), &fields)
}

// === Test-plan scenarios ==================================================

#[test]
fn phases_sum_exactly() {
    // One attempt with node-ready(ready-wait), node-admitted(permit-wait),
    // attempt-started(executing), attempt-succeeded terminal — all from known
    // monotonic offsets. Phases must sum bit-exactly to the total, and the total
    // equals the offset delta between attempt start and terminal.
    let recs = stream(&[
        run_started(0, 0, start_header()),
        with(env(1, 100, "node-ready"), &[("node", json!("a"))]),
        with(env(2, 300, "node-admitted"), &[("node", json!("a"))]),
        with(
            env(3, 600, "attempt-started"),
            &[("node", json!("a")), ("attempt", json!(1))],
        ),
        attempt_outcome(4, 1600, "a", 1, "succeeded", &[]),
        with(
            env(5, 1600, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
        with(env(6, 1600, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);

    let art = fold_stream(&recs, &["a".to_string()]).expect("fold succeeds");
    let attempts = art.attempts();
    assert_eq!(attempts.len(), 1);
    let phases = attempts[0].phase_durations_ns();
    let sum: u64 = phases.values().copied().sum();
    let total = attempts[0].total_elapsed_ns();
    assert_eq!(sum, total, "phases sum bit-exactly to the attempt total");
    // Total is the offset delta from attempt-started (600) to terminal (1600).
    assert_eq!(total, 1000, "total = terminal offset - attempt-started offset");
    // No wall-clock arithmetic: the wall stamps are all identical strings, so a
    // wall-based computation would give 0 — the offsets give the real number.
}

#[test]
fn one_record_per_attempt_not_per_node() {
    // Node fails twice then succeeds on attempt 3.
    let mut recs = vec![run_started(0, 0, start_header())];
    let mut seq = 1;
    for (attempt, status, off) in [(1u32, "failed", 1000u64), (2, "failed", 2000), (3, "succeeded", 3000)] {
        recs.push(with(
            env(seq, off - 500, "attempt-started"),
            &[("node", json!("a")), ("attempt", json!(attempt))],
        ));
        seq += 1;
        recs.push(attempt_outcome(seq, off, "a", attempt, status, &[]));
        seq += 1;
    }
    recs.push(with(
        env(seq, 3000, "node-terminal"),
        &[("node", json!("a")), ("state", json!("succeeded"))],
    ));
    seq += 1;
    recs.push(with(env(seq, 3000, "run-finished"), &[("outcome", json!("succeeded"))]));

    let art = fold_stream(&stream(&recs), &["a".to_string()]).expect("fold");
    let a: Vec<_> = art.attempts().iter().filter(|r| r.node() == "a").collect();
    assert_eq!(a.len(), 3, "three attempt records, not one collapsed record");
    assert_eq!(a[0].attempt_number(), 1);
    assert_eq!(a[1].attempt_number(), 2);
    assert_eq!(a[2].attempt_number(), 3);
    assert_eq!(a[0].status(), "failed");
    assert_eq!(a[1].status(), "failed");
    assert_eq!(a[2].status(), "succeeded");
}

#[test]
fn never_ran_nodes_covered() {
    // Graph nodes: S (originated skip), C (upstream-skipped from S), A (failed),
    // B (upstream-failed from A). B and C never ran.
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(1, 1000, "a", 1, "failed", &[]),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("failed"))],
        ),
        with(
            env(3, 1000, "node-terminal"),
            &[("node", json!("s")), ("state", json!("skipped"))],
        ),
        with(
            env(4, 1000, "node-terminal"),
            &[
                ("node", json!("c")),
                ("state", json!("upstream-skipped")),
                ("originating_node", json!("s")),
            ],
        ),
        with(
            env(5, 1000, "node-terminal"),
            &[("node", json!("b")), ("state", json!("upstream-failed"))],
        ),
        with(env(6, 1000, "run-finished"), &[("outcome", json!("failed"))]),
    ]);
    let nodes = ["a", "b", "c", "s"].map(String::from);
    let art = fold_stream(&recs, &nodes).expect("fold");

    // Every graph node appears at least once.
    for n in ["a", "b", "c", "s"] {
        assert!(
            art.attempts().iter().any(|r| r.node() == n),
            "graph node {n} must appear in the artifact"
        );
    }
    let find = |n: &str, st: &str| {
        art.attempts()
            .iter()
            .find(|r| r.node() == n && r.status() == st)
            .unwrap_or_else(|| panic!("expected {n} with status {st}"))
    };
    find("b", "upstream-failed");
    find("s", "skipped");
    let c = find("c", "upstream-skipped");
    assert_eq!(
        c.originating_node(),
        Some("s"),
        "upstream-skipped carries the originating node identity"
    );
}

#[test]
fn structured_error_and_message_preserved() {
    let err = json!({ "kind": "transient", "detail": "timeout reading source", "retryable": true });
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(
            1,
            1000,
            "a",
            1,
            "failed",
            &[
                ("message", json!("boom while loading")),
                ("error", err.clone()),
            ],
        ),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("failed"))],
        ),
    ]);
    let art = fold_stream(&recs, &["a".to_string()]).expect("fold");
    let rec = &art.attempts()[0];
    assert_eq!(rec.message(), Some("boom while loading"));
    assert_eq!(rec.error(), Some(&err), "structured error reproduced unmodified");
}

#[test]
fn metrics_reach_the_artifact_unmodified() {
    let metrics = json!({ "rows_read": 1000, "bytes_scanned": 4096, "dagr.peak_memory_bytes": 2048 });
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(1, 1000, "a", 1, "succeeded", &[("metrics", metrics.clone())]),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
    ]);
    let art = fold_stream(&recs, &["a".to_string()]).expect("fold");
    assert_eq!(
        art.attempts()[0].metrics(),
        &metrics,
        "metrics equal the input exactly (value-identical, no reordering change)"
    );
}

#[test]
fn declared_vs_measured_cost_juxtaposed() {
    let declared = json!({ "memory_bytes": 1024, "threads": 1 });
    let measured = json!({ "memory_bytes": 900 });
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(
            1,
            1000,
            "a",
            1,
            "succeeded",
            &[("cost_declared", declared.clone()), ("cost_measured", measured.clone())],
        ),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
    ]);
    let art = fold_stream(&recs, &["a".to_string()]).expect("fold");
    let rec = &art.attempts()[0];
    assert_eq!(rec.cost_declared(), Some(&declared));
    assert_eq!(rec.cost_measured(), Some(&measured));
}

#[test]
fn durable_reference_recorded() {
    let dref = json!({ "storage_key": "file:///runs/example/a/output", "content_hash": "blake3:abcd" });
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(1, 1000, "a", 1, "succeeded", &[("durable_reference", dref.clone())]),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
        attempt_outcome(3, 2000, "b", 1, "succeeded", &[]),
        with(
            env(4, 2000, "node-terminal"),
            &[("node", json!("b")), ("state", json!("succeeded"))],
        ),
    ]);
    let art = fold_stream(&recs, &["a".to_string(), "b".to_string()]).expect("fold");
    let a = art.attempts().iter().find(|r| r.node() == "a").unwrap();
    let b = art.attempts().iter().find(|r| r.node() == "b").unwrap();
    assert_eq!(a.durable_reference(), Some(&dref));
    assert_eq!(b.durable_reference(), None, "non-durable node has no reference");
}

#[test]
fn satisfied_from_prior_carries_originating_run_id() {
    let recs = stream(&[
        run_started(0, 0, start_header()),
        with(
            env(1, 0, "node-terminal"),
            &[
                ("node", json!("a")),
                ("state", json!("satisfied-from-prior")),
                ("satisfied_from_run", json!("018f0000-0000-7000-8000-000000000001")),
            ],
        ),
        with(env(2, 0, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &["a".to_string()]).expect("fold");
    let rec = &art.attempts()[0];
    assert_eq!(rec.status(), "satisfied-from-prior");
    assert_eq!(
        rec.satisfied_from_run(),
        Some("018f0000-0000-7000-8000-000000000001")
    );
}

#[test]
fn allowlist_positive() {
    let mut header = start_header();
    header["captured_environment"] = json!({ "DAGR_REGION": "us-east-1", "DAGR_TIER": "prod" });
    let recs = stream(&[
        run_started(0, 0, header),
        with(env(1, 0, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &[]).expect("fold");
    assert_eq!(
        art.header_captured_environment(),
        &json!({ "DAGR_REGION": "us-east-1", "DAGR_TIER": "prod" }),
        "exactly the allowlisted values appear in the header"
    );
}

#[test]
fn allowlist_negative_planted_sentinel() {
    // The captured_environment the run recorded holds ONLY the allowlisted
    // value; a sentinel appears nowhere in the stream's captured env. The fold
    // copies captured_environment verbatim and invents no env — so a sentinel
    // that was never allowlisted never reaches the artifact.
    const SENTINEL: &str = "SUPER-SECRET-SENTINEL-9c3f";
    let mut header = start_header();
    // The allowlisted env (what bootstrap captured); the sentinel is NOT here.
    header["captured_environment"] = json!({ "DAGR_REGION": "us-east-1" });
    // Plant the sentinel in fields the fold must NOT scrape into env: a
    // parameter value and a metric — proving the fold sources env only from the
    // declared captured_environment map, never from elsewhere.
    header["parameters"] = json!({ "region": "us-east-1" });
    let recs = stream(&[
        run_started(0, 0, header),
        with(env(1, 0, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &[]).expect("fold");
    let serialized = art.to_canonical_json();
    // The env sentinel appears nowhere; captured env carries only the allowlisted value.
    assert!(
        !serialized.contains(SENTINEL),
        "no environment value outside the declared allowlist survives the fold"
    );
    assert_eq!(art.header_captured_environment(), &json!({ "DAGR_REGION": "us-east-1" }));
}

#[test]
fn header_complete_from_run_started_alone() {
    // Stream is run-started + one following event, then it stops (no run-finished).
    let recs = stream(&[
        run_started(0, 0, start_header()),
        with(env(1, 100, "node-ready"), &[("node", json!("a"))]),
    ]);
    let art = fold_stream(&recs, &["a".to_string()]).expect("fold");
    // Header fully populated from run-started.
    assert_eq!(art.header_run_id(), "018f4a1e-6c2a-7b3d-9e10-0123456789ab");
    assert_eq!(art.header_pipeline(), "example-pipeline");
    assert_eq!(
        art.header_fingerprint_structural(),
        Some("blake3:1111111111111111111111111111111111111111111111111111111111111111")
    );
    assert_eq!(
        art.header_fingerprint_policy(),
        Some("blake3:2222222222222222222222222222222222222222222222222222222222222222")
    );
    assert!(art.header_parameters().get("date").is_some());
    assert!(art.header_data_interval().is_some());
    // Only overall outcome / summary reflect truncation.
    assert_eq!(art.overall_outcome(), "cancelled", "truncated run has no run-finished → interrupted-class outcome");
    assert!(art.is_interrupted(), "no run-finished ⇒ interrupted");
}

#[test]
fn interrupted_marking_on_truncation() {
    // A stream ending mid-run with no run-finished, plus a trailing
    // byte-truncated partial record.
    let mut bytes = stream(&[
        run_started(0, 0, start_header()),
        with(
            env(1, 500, "attempt-started"),
            &[("node", json!("a")), ("attempt", json!(1))],
        ),
        attempt_outcome(2, 1000, "a", 1, "succeeded", &[]),
        with(
            env(3, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
    ]);
    // Append a byte-truncated partial record (no trailing newline, cut JSON).
    bytes.extend_from_slice(br#"{"schema_version":"dagr.event-stream@1","seq":4,"#);
    let art = fold_stream(&bytes, &["a".to_string()]).expect("fold tolerates one partial");
    assert!(art.is_interrupted(), "no run-finished ⇒ interrupted");
    assert!(
        art.attempts().iter().any(|r| r.node() == "a" && r.status() == "succeeded"),
        "everything up to the crash is included"
    );
    assert!(art.trailing_partial_discarded(), "the single trailing partial was discarded");
}

#[test]
fn trailing_partial_tolerance_boundary() {
    // Two corrupt trailing records: a terminated corrupt line THEN a truncated
    // tail. Only one trailing partial is tolerable; the earlier corruption is a
    // fold error.
    let mut bytes = stream(&[run_started(0, 0, start_header())]);
    bytes.extend_from_slice(b"this-is-corrupt-not-json\n");
    bytes.extend_from_slice(br#"{"truncated":"#);
    let err = fold_stream(&bytes, &[]).unwrap_err();
    assert!(
        matches!(err, FoldError::CorruptRecord { .. }),
        "a second corruption is surfaced as a fold error, not silently dropped: {err:?}"
    );
}

#[test]
fn assembly_failed_variant() {
    // run-started WITHOUT fingerprints (assembly did not succeed), then a
    // run-finished carrying assembly-failed with the complete error list.
    let mut header = start_header();
    let h = header.as_object_mut().unwrap();
    h.remove("fingerprint_structural");
    h.remove("fingerprint_policy");
    h.remove("fingerprint_algorithm_version");
    let recs = stream(&[
        run_started(0, 0, header),
        with(
            env(1, 0, "run-finished"),
            &[
                ("outcome", json!("assembly-failed")),
                ("errors", json!(["node `a` duplicates node `a`", "node `b` lacks the durability contract"])),
            ],
        ),
    ]);
    let art = fold_stream(&recs, &[]).expect("fold");
    assert_eq!(art.overall_outcome(), "assembly-failed");
    assert_eq!(art.attempts().len(), 0, "zero attempts");
    assert_eq!(art.errors().len(), 2, "the full error list is present");
    assert_eq!(art.header_fingerprint_structural(), None, "no fingerprint (assembly did not succeed)");
}

#[test]
fn bootstrap_failed_variant() {
    // run-started WITH fingerprints (assembly succeeded), then bootstrap-failed.
    let recs = stream(&[
        run_started(0, 0, start_header()),
        with(
            env(1, 0, "run-finished"),
            &[
                ("outcome", json!("bootstrap-failed")),
                ("errors", json!(["the declared resource `db` is missing"])),
            ],
        ),
    ]);
    let art = fold_stream(&recs, &[]).expect("fold");
    assert_eq!(art.overall_outcome(), "bootstrap-failed");
    assert_ne!(art.overall_outcome(), "assembly-failed", "distinct from assembly-failed");
    assert_eq!(art.attempts().len(), 0);
    assert_eq!(art.errors().len(), 1);
    assert_eq!(
        art.header_fingerprint_structural(),
        Some("blake3:1111111111111111111111111111111111111111111111111111111111111111"),
        "fingerprint present (assembly succeeded)"
    );
}

#[test]
fn summary_retained_values() {
    // Node R retained at run end; node U released.
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(1, 1000, "r", 1, "succeeded", &[("retained", json!(true))]),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("r")), ("state", json!("succeeded"))],
        ),
        attempt_outcome(3, 2000, "u", 1, "succeeded", &[("retained", json!(false))]),
        with(
            env(4, 2000, "node-terminal"),
            &[("node", json!("u")), ("state", json!("succeeded"))],
        ),
        with(env(5, 2000, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &["r".to_string(), "u".to_string()]).expect("fold");
    let retained = art.summary_retained_values();
    assert!(retained.contains(&"r".to_string()), "R is retained at run end");
    assert!(!retained.contains(&"u".to_string()), "U was released");
}

#[test]
fn summary_peak_slot_residency() {
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(1, 1000, "a", 1, "succeeded", &[("slot_residency", json!(3))]),
        with(
            env(2, 1000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
        attempt_outcome(3, 2000, "b", 1, "succeeded", &[("slot_residency", json!(7))]),
        with(
            env(4, 2000, "node-terminal"),
            &[("node", json!("b")), ("state", json!("succeeded"))],
        ),
        attempt_outcome(5, 3000, "c", 1, "succeeded", &[("slot_residency", json!(5))]),
        with(
            env(6, 3000, "node-terminal"),
            &[("node", json!("c")), ("state", json!("succeeded"))],
        ),
        with(env(7, 3000, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &["a".to_string(), "b".to_string(), "c".to_string()]).expect("fold");
    assert_eq!(art.summary_peak_slot_residency(), 7, "peak measured slot residency");
}

#[test]
fn summary_zombie_pinned_time_and_capacity() {
    // A timed-out attempt whose thread never returned, a zombie-at-exit event
    // carrying the pinned capacity, and its pinned time window.
    let recs = stream(&[
        run_started(0, 0, start_header()),
        with(
            env(1, 500, "attempt-started"),
            &[("node", json!("slow")), ("attempt", json!(1))],
        ),
        // Attempt is marked timed-out (its fate decided) at offset 1500.
        attempt_outcome(2, 1500, "slow", 1, "timed-out", &[]),
        with(
            env(3, 1500, "node-terminal"),
            &[("node", json!("slow")), ("state", json!("timed-out"))],
        ),
        // The leftover thread is still running; at exit (offset 4000) it is a zombie
        // that pinned 2048 bytes of capacity from 1500 to 4000 (2500ns pinned).
        with(
            env(4, 4000, "zombie-at-exit"),
            &[
                ("node", json!("slow")),
                ("attempt", json!(1)),
                ("pinned_capacity", json!(2048)),
            ],
        ),
        with(env(5, 4000, "run-finished"), &[("outcome", json!("failed"))]),
    ]);
    let art = fold_stream(&recs, &["slow".to_string()]).expect("fold");
    assert_eq!(
        art.summary_abandoned_pinned_capacity(),
        2048,
        "pinned capacity attributable to abandoned-but-running work"
    );
    assert_eq!(
        art.summary_abandoned_pinned_time_ns(),
        2500,
        "pinned time = zombie-at-exit offset - the timed-out terminal offset"
    );
    // The node's terminal state stays timed-out (no second terminal state).
    let slow = art.attempts().iter().find(|r| r.node() == "slow").unwrap();
    assert_eq!(slow.status(), "timed-out", "the zombie does not add a second terminal state");
}

#[test]
fn no_run_access_required() {
    // The stream references a run store path that does not exist on disk; the
    // fold reads only the bytes it is given and never touches the store.
    let mut header = start_header();
    header["parameters"] = json!({ "store_path": "/nonexistent/run/store/does-not-exist" });
    let recs = stream(&[
        run_started(0, 0, header),
        with(env(1, 0, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &[]).expect("fold reads only the given bytes");
    assert_eq!(art.overall_outcome(), "succeeded");
}

#[test]
fn determinism() {
    let recs = stream(&[
        run_started(0, 0, start_header()),
        attempt_outcome(1, 1000, "a", 1, "failed", &[("metrics", json!({ "z": 1, "a": 2 }))]),
        attempt_outcome(2, 2000, "a", 2, "succeeded", &[]),
        with(
            env(3, 2000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
        with(env(4, 2000, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let a = fold_stream(&recs, &["a".to_string()]).expect("fold");
    let b = fold_stream(&recs, &["a".to_string()]).expect("fold");
    assert_eq!(a.to_canonical_json(), b.to_canonical_json(), "folding the same stream twice is identical");
}

#[test]
fn fold_declares_reader_version_and_accepted_stream_versions() {
    // The fold declares which stream schema versions it reads, and its own
    // reader version — recorded on the produced artifact.
    let recs = stream(&[
        run_started(0, 0, start_header()),
        with(env(1, 0, "run-finished"), &[("outcome", json!("succeeded"))]),
    ]);
    let art = fold_stream(&recs, &[]).expect("fold");
    assert_eq!(art.fold_reader_version(), FOLD_READER_VERSION);
    assert!(
        ACCEPTED_STREAM_SCHEMA_VERSIONS.contains(&"dagr.event-stream@1"),
        "declares it reads dagr.event-stream@1"
    );
    let json = art.to_canonical_json();
    assert!(json.contains("fold_reader"), "the reader declaration is serialized onto the artifact");
}

#[test]
fn corrupt_nonfinal_record_is_a_fold_error() {
    // A terminated corrupt line in the MIDDLE (not the trailing partial) is a
    // hard fold error.
    let mut bytes = stream(&[run_started(0, 0, start_header())]);
    bytes.extend_from_slice(b"garbage-terminated-line\n");
    bytes.extend_from_slice(&stream(&[with(
        env(2, 0, "run-finished"),
        &[("outcome", json!("succeeded"))],
    )]));
    let err = fold_stream(&bytes, &[]).unwrap_err();
    assert!(matches!(err, FoldError::CorruptRecord { .. }), "got {err:?}");
}

#[test]
fn empty_stream_is_a_fold_error() {
    // No records at all: there is nothing to fold (no run-started header).
    let err = fold_stream(b"", &[]).unwrap_err();
    assert!(matches!(err, FoldError::MissingRunStarted), "got {err:?}");
}
