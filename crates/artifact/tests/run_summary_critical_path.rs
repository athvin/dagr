//! C22 · Run **summary** headline numbers — total elapsed and critical-path
//! time (ticket T43, 054). Written first, TDD.
//!
//! These fixtures build hand-crafted event streams with *known* monotonic
//! offsets and assert the two summary numbers the fold computes on top of the
//! T42 artifact. The critical-path definition is fixed by
//! `docs/adr/0001-critical-path-definition.md`: each node contributes the SUM of
//! its attempts' `executing` phase (retries collapse; ready-wait, permit-wait,
//! backoff, and zombie time are EXCLUDED), dependency predecessors are
//! reconstructed from `node-ready`/terminal timing, and critical-path time is
//! the longest such dependency-respecting chain. Total elapsed is the monotonic
//! wall (last offset minus start), never the informational `wall` stamps.
//!
//! The fold has no explicit edge list: it reconstructs the dependency partial
//! order from the fact that a node's `node-ready` offset is the instant its
//! slowest upstream reached terminal (arch.md C11). So each fixture emits the
//! full `node-ready` / `node-admitted` / `attempt-started` / `attempt-outcome` /
//! `node-terminal` lifecycle with offsets that encode the intended graph shape.

use serde_json::{json, Value};

use dagr_artifact::fold::fold_stream;

// === Fixture builders ======================================================

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

fn start_header() -> Value {
    json!({
        "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
        "pipeline": "example-pipeline",
        "fingerprint_structural": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
        "fingerprint_policy": "blake3:2222222222222222222222222222222222222222222222222222222222222222",
        "fingerprint_algorithm_version": 1,
        "parameters": {},
        "data_interval": null,
        "captured_environment": {},
        "resume_lineage": null,
    })
}

fn stream(records: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for r in records {
        out.push_str(&serde_json::to_string(r).unwrap());
        out.push('\n');
    }
    out.into_bytes()
}

/// A mutable stream-record accumulator carrying its own sequence counter, so a
/// fixture reads as the timeline it is.
struct Timeline {
    recs: Vec<Value>,
    seq: u64,
}

impl Timeline {
    fn new() -> Self {
        let mut t = Self {
            recs: Vec::new(),
            seq: 0,
        };
        t.recs.push(with(
            env(0, 0, "run-started"),
            &[("header", start_header())],
        ));
        t.seq = 1;
        t
    }

    fn push(&mut self, offset: u64, kind: &str, fields: &[(&str, Value)]) -> &mut Self {
        self.recs.push(with(env(self.seq, offset, kind), fields));
        self.seq += 1;
        self
    }

    /// Emit one node's full ready→admitted→started→outcome→terminal lifecycle.
    /// `ready`, `admitted`, `started`, `outcome` are monotonic offsets; the node
    /// executes for `outcome - started`.
    #[allow(clippy::too_many_arguments)]
    fn node_run(
        &mut self,
        node: &str,
        ready: u64,
        admitted: u64,
        started: u64,
        outcome: u64,
        status: &str,
        extra: &[(&str, Value)],
    ) -> &mut Self {
        self.push(ready, "node-ready", &[("node", json!(node))]);
        self.push(admitted, "node-admitted", &[("node", json!(node))]);
        self.push(
            started,
            "attempt-started",
            &[("node", json!(node)), ("attempt", json!(1))],
        );
        let mut oflds = vec![
            ("node", json!(node)),
            ("attempt", json!(1)),
            ("status", json!(status)),
        ];
        oflds.extend(extra.iter().cloned());
        self.push(outcome, "attempt-outcome", &oflds);
        self.push(
            outcome,
            "node-terminal",
            &[("node", json!(node)), ("state", json!(status))],
        );
        self
    }

    fn finish(&mut self, offset: u64, outcome: &str) -> Vec<u8> {
        self.push(offset, "run-finished", &[("outcome", json!(outcome))]);
        stream(&self.recs)
    }
}

// === Test-plan scenarios ===================================================

#[test]
fn total_elapsed_is_monotonic_not_wall_clock() {
    // Skew the informational wall stamps deliberately (out of order, jumping)
    // while the monotonic offsets march forward. Total elapsed must equal the
    // last offset minus the start (0), unaffected by the wall clock.
    let recs = vec![
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        // wall jumps BACKWARD then FORWARD; offsets stay monotonic.
        with(
            {
                let mut r = env(1, 1_000, "node-ready");
                r["wall"] = json!("2020-01-01T00:00:00.000Z");
                with(r, &[("node", json!("a"))])
            },
            &[],
        ),
        with(
            {
                let mut r = env(2, 2_000, "node-admitted");
                r["wall"] = json!("2099-12-31T23:59:59.000Z");
                with(r, &[("node", json!("a"))])
            },
            &[],
        ),
        with(
            env(3, 3_000, "attempt-started"),
            &[("node", json!("a")), ("attempt", json!(1))],
        ),
        {
            let mut r = env(4, 9_000, "attempt-outcome");
            r["wall"] = json!("1970-01-01T00:00:00.000Z");
            with(
                r,
                &[
                    ("node", json!("a")),
                    ("attempt", json!(1)),
                    ("status", json!("succeeded")),
                ],
            )
        },
        with(
            env(5, 9_000, "node-terminal"),
            &[("node", json!("a")), ("state", json!("succeeded"))],
        ),
        with(
            env(6, 9_000, "run-finished"),
            &[("outcome", json!("succeeded"))],
        ),
    ];
    let art = fold_stream(&stream(&recs), &["a".to_string()]).expect("fold");
    assert_eq!(
        art.summary_total_elapsed_ns(),
        9_000,
        "total elapsed = last offset - start, from monotonic offsets only"
    );
}

#[test]
fn structure_limited_run_reads_as_structure_limited() {
    // A → B → C → D chain, each admitted immediately (no permit-wait), no
    // retries. Each node executes for 1000ns. Critical path ≈ total elapsed.
    let mut t = Timeline::new();
    // node_run(node, ready, admitted, started, outcome, ...)
    t.node_run("a", 0, 0, 0, 1_000, "succeeded", &[]);
    t.node_run("b", 1_000, 1_000, 1_000, 2_000, "succeeded", &[]);
    t.node_run("c", 2_000, 2_000, 2_000, 3_000, "succeeded", &[]);
    t.node_run("d", 3_000, 3_000, 3_000, 4_000, "succeeded", &[]);
    let bytes = t.finish(
        4_000,
        "succeeded",
    );
    let art = fold_stream(
        &bytes,
        &["a".into(), "b".into(), "c".into(), "d".into()],
    )
    .expect("fold");
    let total = art.summary_total_elapsed_ns();
    let cp = art.summary_critical_path_ns();
    assert_eq!(total, 4_000, "four serial nodes, 1000ns each");
    assert_eq!(cp, 4_000, "critical path is the whole 4-node executing chain");
    // structure-limited: critical path ≈ total elapsed.
    assert_eq!(cp, total, "critical-path ≈ total elapsed ⇒ structure-limited");
}

#[test]
fn resource_limited_run_reads_as_resource_limited() {
    // Four INDEPENDENT siblings (no dependencies), all ready at offset 0, but a
    // one-permit pool serialized them: each waits behind the previous, then
    // executes 1000ns. Permit-wait is EXCLUDED from the path (ADR), so the
    // critical path is the single longest node's executing time (1000ns), while
    // total elapsed is the full serialized wall (4000ns).
    let mut t = Timeline::new();
    // All ready at 0; admitted staggered (permit-wait), execute 1000ns each.
    t.node_run("s0", 0, 0, 0, 1_000, "succeeded", &[]);
    t.node_run("s1", 0, 1_000, 1_000, 2_000, "succeeded", &[]);
    t.node_run("s2", 0, 2_000, 2_000, 3_000, "succeeded", &[]);
    t.node_run("s3", 0, 3_000, 3_000, 4_000, "succeeded", &[]);
    let bytes = t.finish(4_000, "succeeded");
    let art = fold_stream(
        &bytes,
        &["s0".into(), "s1".into(), "s2".into(), "s3".into()],
    )
    .expect("fold");
    let total = art.summary_total_elapsed_ns();
    let cp = art.summary_critical_path_ns();
    assert_eq!(total, 4_000, "serialized behind one permit");
    assert_eq!(
        cp, 1_000,
        "independent siblings ⇒ critical path is the single longest node's executing time"
    );
    assert!(
        total > cp * 3,
        "total elapsed greatly exceeds critical path ⇒ resource-limited"
    );
}

#[test]
fn critical_path_respects_dependencies_not_raw_duration() {
    // Diamond: a → b, a → c, b → d, c → d.
    //   a executes 1000.
    //   Branch b: SHORT executing (500) but a large permit-wait before it.
    //   Branch c: LONG executing (2000), admitted immediately.
    //   d executes 1000, ready only after BOTH b and c terminal.
    // The dependency-respecting longest chain is a→c→d = 1000+2000+1000 = 4000,
    // NOT the single largest node (c=2000) and NOT the sum of all nodes.
    let mut t = Timeline::new();
    t.node_run("a", 0, 0, 0, 1_000, "succeeded", &[]);
    // b: ready at 1000 (after a), but admitted at 5000 (4000ns permit-wait),
    // then executes 500 → terminal at 5500. Permit-wait is off the path.
    t.node_run("b", 1_000, 5_000, 5_000, 5_500, "succeeded", &[]);
    // c: ready at 1000 (after a), admitted immediately, executes 2000 → 3000.
    t.node_run("c", 1_000, 1_000, 1_000, 3_000, "succeeded", &[]);
    // d: ready only after BOTH b(5500) and c(3000) terminal ⇒ ready at 5500,
    // executes 1000 → terminal 6500.
    t.node_run("d", 5_500, 5_500, 5_500, 6_500, "succeeded", &[]);
    let bytes = t.finish(6_500, "succeeded");
    let art = fold_stream(
        &bytes,
        &["a".into(), "b".into(), "c".into(), "d".into()],
    )
    .expect("fold");
    let cp = art.summary_critical_path_ns();
    // Longest EXECUTING chain a→c→d = 1000 + 2000 + 1000 = 4000.
    assert_eq!(
        cp, 4_000,
        "critical path = longest dependency-respecting executing chain a→c→d"
    );
    // Not the single largest node (c = 2000) and not the sum of all executing
    // (1000+500+2000+1000 = 4500).
    assert_ne!(cp, 2_000, "not the single largest node");
    assert_ne!(cp, 4_500, "not the sum of all nodes");
}

#[test]
fn retries_collapse_per_the_adr() {
    // A single node on the (trivial) critical chain has three attempts: two
    // failed with backoff, one succeeded. Per the ADR the node contributes the
    // SUM of its attempts' `executing` phases; backoff is EXCLUDED.
    //   attempt 1: started 100, outcome 300  → executing 200
    //   attempt 2: started 500, outcome 800  → executing 300 (backoff 300..500)
    //   attempt 3: started 1200, outcome 1700 → executing 500 (backoff 800..1200)
    // Node contribution = 200 + 300 + 500 = 1000 (backoff excluded).
    let mut t = Timeline::new();
    t.push(0, "node-ready", &[("node", json!("r"))]);
    t.push(0, "node-admitted", &[("node", json!("r"))]);
    t.push(
        100,
        "attempt-started",
        &[("node", json!("r")), ("attempt", json!(1))],
    );
    t.push(
        300,
        "attempt-outcome",
        &[
            ("node", json!("r")),
            ("attempt", json!(1)),
            ("status", json!("failed")),
        ],
    );
    t.push(
        500,
        "attempt-started",
        &[("node", json!("r")), ("attempt", json!(2))],
    );
    t.push(
        800,
        "attempt-outcome",
        &[
            ("node", json!("r")),
            ("attempt", json!(2)),
            ("status", json!("failed")),
        ],
    );
    t.push(
        1_200,
        "attempt-started",
        &[("node", json!("r")), ("attempt", json!(3))],
    );
    t.push(
        1_700,
        "attempt-outcome",
        &[
            ("node", json!("r")),
            ("attempt", json!(3)),
            ("status", json!("succeeded")),
        ],
    );
    t.push(
        1_700,
        "node-terminal",
        &[("node", json!("r")), ("state", json!("succeeded"))],
    );
    let bytes = t.finish(1_700, "succeeded");
    let art = fold_stream(&bytes, &["r".to_string()]).expect("fold");
    assert_eq!(
        art.summary_critical_path_ns(),
        1_000,
        "node contributes the summed executing across attempts (backoff excluded)"
    );
}

#[test]
fn permit_wait_treatment_matches_the_adr() {
    // Two otherwise-identical chains a→b, differing ONLY in b's permit-wait: one
    // with zero permit-wait, one with a large permit-wait. Per the ADR
    // (permit-wait EXCLUDED), the two critical-path numbers are EQUAL — the added
    // permit-wait makes no difference on the path.
    let build = |b_admitted: u64, b_started: u64, b_outcome: u64, finish: u64| {
        let mut t = Timeline::new();
        t.node_run("a", 0, 0, 0, 1_000, "succeeded", &[]);
        t.node_run("b", 1_000, b_admitted, b_started, b_outcome, "succeeded", &[]);
        let bytes = t.finish(finish, "succeeded");
        fold_stream(&bytes, &["a".into(), "b".into()])
            .expect("fold")
            .summary_critical_path_ns()
    };
    // No permit-wait: b admitted+started at 1000, executes 1000 → 2000.
    let cp_no_wait = build(1_000, 1_000, 2_000, 2_000);
    // Large permit-wait: b admitted at 1000 but started at 6000 (5000ns wait),
    // executes the SAME 1000 → outcome 7000.
    let cp_big_wait = build(1_000, 6_000, 7_000, 7_000);
    assert_eq!(
        cp_no_wait, cp_big_wait,
        "ADR EXCLUDES permit-wait ⇒ the two critical-path numbers are equal"
    );
    assert_eq!(cp_no_wait, 2_000, "chain executing time a(1000)+b(1000)");
}

#[test]
fn zombie_pinned_time_is_separated_from_the_path() {
    // A node timed out at 1500 (executing 1000 from started 500), but its thread
    // ran on and was a zombie at exit (4000), pinning 2500ns. Per the ADR the
    // zombie overrun is NOT on the critical path; it stays in its own summary
    // field. Critical path = the node's executing-to-terminal (1000), not 3500.
    let mut t = Timeline::new();
    t.push(0, "node-ready", &[("node", json!("slow"))]);
    t.push(0, "node-admitted", &[("node", json!("slow"))]);
    t.push(
        500,
        "attempt-started",
        &[("node", json!("slow")), ("attempt", json!(1))],
    );
    t.push(
        1_500,
        "attempt-outcome",
        &[
            ("node", json!("slow")),
            ("attempt", json!(1)),
            ("status", json!("timed-out")),
        ],
    );
    t.push(
        1_500,
        "node-terminal",
        &[("node", json!("slow")), ("state", json!("timed-out"))],
    );
    t.push(
        4_000,
        "zombie-at-exit",
        &[
            ("node", json!("slow")),
            ("attempt", json!(1)),
            ("pinned_capacity", json!(2048)),
        ],
    );
    let bytes = t.finish(4_000, "failed");
    let art = fold_stream(&bytes, &["slow".to_string()]).expect("fold");
    assert_eq!(
        art.summary_critical_path_ns(),
        1_000,
        "critical path counts executing-to-terminal (1000), not the zombie overrun"
    );
    assert_eq!(
        art.summary_abandoned_pinned_time_ns(),
        2_500,
        "zombie-pinned time stays in its own summary field (from T42)"
    );
}

#[test]
fn pure_function_of_an_artifact() {
    // Compute the two numbers twice from the same bytes; identical, no I/O.
    let mut t = Timeline::new();
    t.node_run("a", 0, 0, 0, 1_000, "succeeded", &[]);
    t.node_run("b", 1_000, 1_000, 1_000, 3_000, "succeeded", &[]);
    let bytes = t.finish(3_000, "succeeded");
    let art1 = fold_stream(&bytes, &["a".into(), "b".into()]).expect("fold");
    let art2 = fold_stream(&bytes, &["a".into(), "b".into()]).expect("fold");
    assert_eq!(
        art1.summary_total_elapsed_ns(),
        art2.summary_total_elapsed_ns()
    );
    assert_eq!(
        art1.summary_critical_path_ns(),
        art2.summary_critical_path_ns()
    );
    // And the whole artifact is byte-identical.
    assert_eq!(art1.to_canonical_json(), art2.to_canonical_json());
}

#[test]
fn single_node_graph() {
    let mut t = Timeline::new();
    t.node_run("only", 0, 0, 0, 1_234, "succeeded", &[]);
    let bytes = t.finish(1_234, "succeeded");
    let art = fold_stream(&bytes, &["only".to_string()]).expect("fold");
    assert_eq!(
        art.summary_critical_path_ns(),
        1_234,
        "single node ⇒ critical path is that node's executing contribution"
    );
    assert_eq!(art.summary_total_elapsed_ns(), 1_234);
}

#[test]
fn never_ran_node_on_the_chain_contributes_zero() {
    // a succeeds (executing 1000), then b never ran (upstream-failed propagated,
    // no attempt records). The chain traverses b but b contributes zero executed
    // time. No panic, no negative, no NaN.
    let mut t = Timeline::new();
    t.node_run("a", 0, 0, 0, 1_000, "failed", &[]);
    // b propagated terminal only (never admitted / started).
    t.push(
        1_000,
        "node-terminal",
        &[
            ("node", json!("b")),
            ("state", json!("upstream-failed")),
            ("originating_node", json!("a")),
        ],
    );
    let bytes = t.finish(1_000, "failed");
    let art = fold_stream(&bytes, &["a".into(), "b".into()]).expect("fold");
    let cp = art.summary_critical_path_ns();
    assert_eq!(
        cp, 1_000,
        "never-ran b contributes 0; the chain is a's executing time"
    );
    // Well-defined and non-negative.
    assert!(cp <= art.summary_total_elapsed_ns());
}

#[test]
fn empty_zero_attempt_run_is_well_defined() {
    // A run with a run-started and run-finished but no attempts at all: the two
    // numbers are zero, not a panic / negative / NaN.
    let recs = vec![
        with(env(0, 0, "run-started"), &[("header", start_header())]),
        with(
            env(1, 0, "run-finished"),
            &[("outcome", json!("succeeded"))],
        ),
    ];
    let art = fold_stream(&stream(&recs), &[]).expect("fold");
    assert_eq!(art.summary_total_elapsed_ns(), 0);
    assert_eq!(art.summary_critical_path_ns(), 0);
}
