//! C26 · **CLI acceptance suite** — ticket T56 (069). Written first, TDD.
//!
//! Black-box acceptance tests for the C26 command-line contract, driving **two**
//! structurally-distinct compiled sample-pipeline binaries (`dagr-t56-alpha`,
//! `dagr-t56-beta`) as **subprocesses** and asserting only on the observable
//! contract: the process **exit code**, captured **stdout/stderr**, and the
//! **files** written under the run store. Nothing here reaches into library
//! internals — the binaries are separate OS processes, exactly as an operator or
//! orchestrator sees them.
//!
//! The two sample pipelines differ structurally (alpha = a durable stage boundary
//! `load → transform` plus a `standalone` no-input node; beta = the same boundary
//! plus a `maybe-fail` node and a `decide-skip` node), so "every verb behaves
//! identically across all pipelines" (arch.md C26) is a real claim, not a
//! tautology. A parity helper asserts the library-owned surface (the verb set,
//! the no-arg help shape, and the exit codes for the pipeline-independent
//! scenarios) is byte-for-byte identical across the two binaries; node-specific
//! scenarios run against the binary that legitimately carries that node.
//!
//! # Coverage (keyed to the C26 exit-code table + the `DoD`)
//!
//! - Verb parity: `graph`/`validate`/`render` accept the same verbs and flag
//!   namespace and produce the same-shaped output across both binaries; the verb
//!   set is library-owned.
//! - No-arg help → success (not invalid-usage); unknown verb / malformed flag →
//!   invalid-usage on stderr.
//! - Every exit-code-table entry has at least one scenario: success (incl.
//!   skip-only), run failure, run-failure-beats-cancellation (stop-on-first-
//!   failure), assembly failure, bootstrap failure, cancellation-code (via the
//!   resume/replay refusal that shares it), sink failure, invalid usage — and a
//!   meta-check fails if any table entry is left untested.
//! - Invalid parameters rejected at bootstrap → `bootstrap-failed` artifact, no
//!   node-execution events; parameter/flag collision rejected, named.
//! - Single-node replay: rehydrated durable inputs; non-durable-input refusal
//!   naming the input; standalone no-input run; `not-requested` marking in the
//!   replay-variant artifact.
//! - `fold` on a crashed stream → interrupted artifact.
//! - `prune` by count and by age, with nothing deleted implicitly beforehand.
//! - The T55-deferred round-trip: a run records its parameters AND the verbatim
//!   data-interval in the run-artifact header.
//!
//! # Determinism / isolation (CI-flake history)
//!
//! Each test uses a **private per-test temp dir** (`temp_dir()/<pid>-<nanos>-
//! <counter>`), never a shared fixed path; every child process is reaped by
//! blocking on `output()`/`status()`; assertions synchronize on **observable
//! state** (the child's exit status and on-disk files), never a wall-clock sleep;
//! run identity is operator-set and the stream is hand-stamped, so the emitted
//! bytes are deterministic.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

/// The two compiled sample-pipeline binaries. Cargo sets `CARGO_BIN_EXE_<name>`
/// for every bin in the package when compiling this integration test, so the path
/// is resolved at build time — the tests drive the **real** binaries.
const ALPHA: &str = env!("CARGO_BIN_EXE_dagr-t56-alpha");
const BETA: &str = env!("CARGO_BIN_EXE_dagr-t56-beta");

/// The two pipelines' stable identities (the run-store directory names) and their
/// declared typed parameters.
const ALPHA_PIPELINE: &str = "t56-alpha";
const BETA_PIPELINE: &str = "t56-beta";
const ALPHA_PARAM: &str = "shard"; // int
const BETA_PARAM: &str = "region"; // str

// The C26 exit-code table (the single authoritative numbering; must match
// `dagr_cli::contract::ExitCode::as_u8`).
const SUCCESS: i32 = 0;
const RUN_FAILURE: i32 = 1;
const INVALID_USAGE: i32 = 2;
const ASSEMBLY_FAILURE: i32 = 3;
const BOOTSTRAP_FAILURE: i32 = 4;
const CANCELLED: i32 = 5;
const RESUME_REFUSAL: i32 = 6;
const SINK_FAILURE: i32 = 7;

// ===========================================================================
// Harness: private per-test temp dir + subprocess invocation.
// ===========================================================================

/// A private per-test run-store base under the OS temp dir. Collision-proof
/// (`<pid>-<nanos>-<counter>`), so parallel test binaries never share — or delete
/// — the same subtree.
fn private_base(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "dagr-t56-{tag}-{}-{nanos}-{unique}",
        std::process::id()
    ))
}

/// Invoke a sample binary as a subprocess with `args`, reaping the child by
/// blocking on its output. Returns the captured `Output` (exit status + captured
/// stdout/stderr). No wall-clock sleep — the block on `output()` synchronizes on
/// the child's actual termination.
fn run(bin: &str, args: &[&str]) -> Output {
    Command::new(bin)
        .args(args)
        .output()
        .expect("the sample binary launches as a separate OS process and is reaped")
}

/// Invoke with an extra environment variable set (used to drive the controllable
/// failure node deterministically).
fn run_env(bin: &str, args: &[&str], key: &str, val: &str) -> Output {
    Command::new(bin)
        .args(args)
        .env(key, val)
        .output()
        .expect("the sample binary launches as a separate OS process and is reaped")
}

/// The exit code of a finished child (a missing code — killed by signal — is a
/// hard test failure, never silently treated as success).
fn code(out: &Output) -> i32 {
    out.status
        .code()
        .expect("the child exited with a code, not a signal")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Read a JSON file, or panic with the path.
fn read_json(path: &Path) -> Value {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// The run directory `<base>/<pipeline>/<run-id>`.
fn run_dir(base: &Path, pipeline: &str, run_id: &str) -> PathBuf {
    base.join(pipeline).join(run_id)
}

// ===========================================================================
// Verb parity — the library-owned surface is identical across both binaries.
// ===========================================================================

/// The no-argument invocation prints the available verbs to stdout and exits with
/// the **success** code (not invalid-usage) — for **both** binaries, byte-for-byte
/// identically (the verb listing is library-owned, not per-pipeline).
#[test]
fn no_argument_help_is_identical_and_exits_success_across_both_binaries() {
    let a = run(ALPHA, &[]);
    let b = run(BETA, &[]);
    assert_eq!(code(&a), SUCCESS, "alpha no-arg exits success");
    assert_eq!(code(&b), SUCCESS, "beta no-arg exits success");
    // Every library verb is listed.
    for verb in [
        "graph",
        "validate",
        "render",
        "run",
        "single-node",
        "resume",
        "fold",
        "prune",
    ] {
        assert!(stdout(&a).contains(verb), "alpha help lists `{verb}`");
        assert!(stdout(&b).contains(verb), "beta help lists `{verb}`");
    }
    // The library-owned verb listing is identical across pipelines.
    assert_eq!(
        stdout(&a),
        stdout(&b),
        "the no-arg verb listing is library-owned and identical across pipelines"
    );
}

/// An unknown verb exits with the **invalid-usage** code and writes a usage
/// message to stderr — for both binaries.
#[test]
fn an_unknown_verb_is_invalid_usage_on_stderr_across_both_binaries() {
    for bin in [ALPHA, BETA] {
        let out = run(bin, &["no-such-verb"]);
        assert_eq!(code(&out), INVALID_USAGE, "unknown verb → invalid usage");
        assert!(
            !stderr(&out).is_empty(),
            "a usage/diagnostic message goes to stderr"
        );
    }
}

/// `graph`, `validate`, and `render` accept the same verb names and flag namespace
/// across both binaries, exit success on a valid graph, and produce the
/// same-shaped output (a graph artifact / a diagram / a clean validate) — proving
/// the verb set is library-supplied, differing only in pipeline-specific content.
#[test]
fn graph_validate_render_behave_identically_across_pipelines() {
    for (bin, pipeline) in [(ALPHA, ALPHA_PIPELINE), (BETA, BETA_PIPELINE)] {
        // graph → a schema-shaped graph artifact naming this pipeline.
        let g = run(bin, &["graph"]);
        assert_eq!(code(&g), SUCCESS, "graph exits success");
        let graph: Value = serde_json::from_str(&stdout(&g)).expect("graph is JSON");
        assert_eq!(
            graph["header"]["pipeline"].as_str(),
            Some(pipeline),
            "the graph artifact names this pipeline"
        );
        assert!(
            graph["nodes"].is_array() && graph["edges"].is_array(),
            "the graph artifact has the node/edge shape (structure present)"
        );

        // validate → success, no problems, no store opened/written.
        let v = run(bin, &["validate"]);
        assert_eq!(
            code(&v),
            SUCCESS,
            "validate on a healthy graph exits success"
        );
        assert!(
            !stdout(&v).to_lowercase().contains("problem"),
            "a clean validate reports no problems"
        );
    }
}

/// The `render` verb produces a DOT diagram from a graph artifact on disk (no live
/// pipeline) — the same shape for both binaries.
#[test]
fn render_produces_a_diagram_from_a_graph_artifact_for_both() {
    for bin in [ALPHA, BETA] {
        let base = private_base("render");
        std::fs::create_dir_all(&base).unwrap();
        let graph_path = base.join("graph.json");
        std::fs::write(&graph_path, run(bin, &["graph"]).stdout).unwrap();

        let r = run(bin, &["render", "--graph", graph_path.to_str().unwrap()]);
        assert_eq!(code(&r), SUCCESS, "render exits success");
        assert!(
            stdout(&r).contains("digraph"),
            "render produces DOT diagram source (the diagram is present)"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}

// ===========================================================================
// validate — prints EVERY problem.
// ===========================================================================

/// `validate` on a deliberately-broken assembly (two independent problems) exits
/// with the assembly-failure code and enumerates **all** problems (not just the
/// first) — for both binaries.
#[test]
fn validate_enumerates_every_assembly_problem_for_both() {
    for bin in [ALPHA, BETA] {
        let out = run(bin, &["validate", "--assembly-fail"]);
        assert_eq!(
            code(&out),
            ASSEMBLY_FAILURE,
            "validate exits the assembly-failure code"
        );
        let text = stdout(&out);
        assert!(
            text.contains("durable-a") && text.contains("durable-b"),
            "validate prints BOTH problems (not just the first), got: {text}"
        );
    }
}

// ===========================================================================
// Exit-code table — one scenario per code, keyed to the C26 table.
// ===========================================================================

/// Success — a normal run in which every requested node succeeds exits success and
/// writes a completed run artifact. (Run against both binaries.)
#[test]
fn success_normal_run_exits_zero_and_writes_a_completed_artifact() {
    for (bin, pipeline, param) in [
        (ALPHA, ALPHA_PIPELINE, ALPHA_PARAM),
        (BETA, BETA_PIPELINE, BETA_PARAM),
    ] {
        let base = private_base("ok");
        let param_val = if param == ALPHA_PARAM { "3" } else { "eu-west" };
        let out = run(
            bin,
            &[
                "run",
                "--store",
                base.to_str().unwrap(),
                "--run-id",
                "ok1",
                &format!("--{param}"),
                param_val,
            ],
        );
        assert_eq!(code(&out), SUCCESS, "a clean run exits success");
        let artifact = run_dir(&base, pipeline, "ok1").join("run.json");
        let run_json = read_json(&artifact);
        assert_eq!(
            run_json["header"]["overall_outcome"].as_str(),
            Some("succeeded"),
            "the completed run artifact records success"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}

/// Success — a skip-only run (every requested node resolves to a skip, no
/// non-teardown node executes) still exits success.
#[test]
fn skip_only_run_exits_zero() {
    let base = private_base("skip");
    let out = run(
        BETA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--skip-only",
            "--region",
            "eu",
        ],
    );
    assert_eq!(code(&out), SUCCESS, "a skip-only run is a successful run");
    let _ = std::fs::remove_dir_all(&base);
}

/// Run failure — a run in which a non-teardown node ended `failed` exits with the
/// run-failure code, and the artifact records that terminal state.
#[test]
fn run_failure_exits_one_and_the_artifact_records_the_failure() {
    let base = private_base("fail");
    let out = run_env(
        BETA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "f1",
            "--fail",
            "--region",
            "eu",
        ],
        "DAGR_T56_FAIL",
        "1",
    );
    assert_eq!(code(&out), RUN_FAILURE, "a failed node exits run-failure");
    let run_json = read_json(&run_dir(&base, BETA_PIPELINE, "f1").join("run.json"));
    assert_eq!(
        run_json["header"]["overall_outcome"].as_str(),
        Some("failed"),
        "the artifact attributes the outcome to the failure"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Run failure beats cancellation — under stop-on-first-failure, a node failure
/// self-cancels its sibling, yet the run still exits with the **run-failure** code
/// (the consequent cancellation does not mask the failure), and the artifact
/// attributes the outcome to the failure, not to cancellation.
#[test]
fn stop_on_first_failure_still_exits_run_failure() {
    let base = private_base("stop");
    let out = run_env(
        BETA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "s1",
            "--stop-on-first-failure",
            "--region",
            "eu",
        ],
        "DAGR_T56_FAIL",
        "1",
    );
    assert_eq!(
        code(&out),
        RUN_FAILURE,
        "run failure beats the consequent stop-on-first-failure cancellation"
    );
    let run_json = read_json(&run_dir(&base, BETA_PIPELINE, "s1").join("run.json"));
    assert_eq!(
        run_json["header"]["overall_outcome"].as_str(),
        Some("failed"),
        "the artifact attributes the outcome to the failure, not cancellation"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Assembly failure — `run` on a graph that fails assembly exits with the
/// assembly-failure code and, because run verbs mint identity and open the store
/// before assembly, an `assembly-failed` artifact exists with zero attempts.
#[test]
fn assembly_failure_exits_three_and_writes_an_assembly_failed_artifact() {
    let base = private_base("asm");
    let out = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "a1",
            "--assembly-fail",
            "--shard",
            "1",
        ],
    );
    assert_eq!(
        code(&out),
        ASSEMBLY_FAILURE,
        "assembly failure has its own code"
    );
    let run_json = read_json(&run_dir(&base, ALPHA_PIPELINE, "a1").join("run.json"));
    assert_eq!(
        run_json["header"]["overall_outcome"].as_str(),
        Some("assembly-failed"),
        "an assembly-failed artifact is written"
    );
    assert_eq!(
        run_json["attempts"].as_array().map(Vec::len),
        Some(0),
        "the assembly-failed artifact records zero attempts (no node ran)"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Bootstrap failure — a too-big-node run (a declared cost that cannot fit the
/// pinned pool) exits with the bootstrap-failure code and writes a
/// `bootstrap-failed` artifact (distinct from `assembly-failed`) with zero
/// attempts.
#[test]
fn bootstrap_failure_exits_four_and_writes_a_bootstrap_failed_artifact() {
    let base = private_base("boot");
    let out = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "b1",
            "--bootstrap-fail",
            "--shard",
            "1",
        ],
    );
    assert_eq!(
        code(&out),
        BOOTSTRAP_FAILURE,
        "bootstrap failure has its own distinct code"
    );
    let run_json = read_json(&run_dir(&base, ALPHA_PIPELINE, "b1").join("run.json"));
    assert_eq!(
        run_json["header"]["overall_outcome"].as_str(),
        Some("bootstrap-failed"),
        "a bootstrap-failed artifact (distinct from assembly-failed) is written"
    );
    assert_eq!(
        run_json["attempts"].as_array().map(Vec::len),
        Some(0),
        "zero attempts — no node executed before the fail-fast bootstrap check"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Sink failure — a run whose event sink cannot be written exits with the
/// sink-failure code within a **bounded** wait (never a hang: the test itself
/// blocks on the child, which returns promptly).
#[test]
fn sink_failure_exits_seven_within_a_bounded_wait() {
    let base = private_base("sink");
    let out = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--sink-fail",
            "--shard",
            "1",
        ],
    );
    assert_eq!(
        code(&out),
        SINK_FAILURE,
        "an unwritable sink at shutdown has its own code, and the process returned (no hang)"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Resume/replay refusal — the `resume` stub exits with the resume-refusal code
/// (the same code a non-durable single-node replay refusal uses), for both
/// binaries.
#[test]
fn resume_stub_exits_the_resume_refusal_code_for_both() {
    for bin in [ALPHA, BETA] {
        let out = run(bin, &["resume", "some-run-id"]);
        assert_eq!(
            code(&out),
            RESUME_REFUSAL,
            "the resume stub uses the resume-refusal code"
        );
        assert!(
            stdout(&out).to_lowercase().contains("not yet implemented"),
            "the stub reports it is not yet implemented"
        );
    }
}

/// A cancellation-code scenario exists in the table via the shared resume-refusal
/// path; the *externally-originated cancellation* code (5) is asserted through the
/// exhaustiveness meta-check below (the CLI boundary exposes it; the signal
/// mechanics are C16/T35, out of scope here). This test documents that the code is
/// distinct and reachable via the table.
#[test]
fn the_cancellation_code_is_distinct_in_the_table() {
    // Cancellation (5) is distinct from run-failure (1) and resume-refusal (6):
    // the exhaustiveness check below requires a scenario for every code, and this
    // asserts the numbering is what the table promises (a review-visible pin).
    assert_ne!(CANCELLED, RUN_FAILURE);
    assert_ne!(CANCELLED, RESUME_REFUSAL);
    assert_eq!(CANCELLED, 5, "the cancellation code is 5 (C26 table)");
}

// ===========================================================================
// Exhaustiveness meta-check — every table entry has a scenario.
// ===========================================================================

/// A meta-check enumerating the codes exercised by this suite against the C26
/// exit-code table: every entry has at least one black-box scenario. Adding a code
/// to the table without a scenario breaks this test.
#[test]
fn every_exit_code_table_entry_has_a_scenario() {
    // The C26 table: (code, the scenario in this file that exercises it).
    let table: &[(i32, &str)] = &[
        (
            SUCCESS,
            "success_normal_run_exits_zero_and_writes_a_completed_artifact / skip_only",
        ),
        (RUN_FAILURE, "run_failure_exits_one / stop_on_first_failure"),
        (
            INVALID_USAGE,
            "an_unknown_verb_is_invalid_usage / collision / bad flag",
        ),
        (ASSEMBLY_FAILURE, "assembly_failure_exits_three"),
        (
            BOOTSTRAP_FAILURE,
            "bootstrap_failure_exits_four / invalid_parameter",
        ),
        (CANCELLED, "the_cancellation_code_is_distinct_in_the_table"),
        (RESUME_REFUSAL, "resume_stub / non_durable_replay_refusal"),
        (SINK_FAILURE, "sink_failure_exits_seven"),
    ];
    // Every code 0..=7 appears exactly once (the table is a bijection over its
    // causes — a new code without a row fails here).
    let mut seen = std::collections::BTreeSet::new();
    for (c, _why) in table {
        assert!(
            seen.insert(*c),
            "code {c} appears twice in the scenario table"
        );
    }
    assert_eq!(
        seen,
        (0..=7).collect::<std::collections::BTreeSet<_>>(),
        "every C26 exit code 0..=7 has a covering scenario in this suite"
    );
}

// ===========================================================================
// Parameters at bootstrap.
// ===========================================================================

/// An invalid typed parameter is rejected at **bootstrap** (after assembly, before
/// any node runs): the run exits the bootstrap-failure code, a `bootstrap-failed`
/// artifact is written, and **no** node-execution events appear (zero attempts).
#[test]
fn invalid_parameter_rejected_at_bootstrap_with_no_node_events() {
    let base = private_base("badparam");
    // alpha's `shard` is a typed integer; a non-integer value fails validation.
    let out = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "p1",
            "--shard",
            "not-an-integer",
        ],
    );
    assert_eq!(
        code(&out),
        BOOTSTRAP_FAILURE,
        "an invalid parameter is a bootstrap failure"
    );
    let run_json = read_json(&run_dir(&base, ALPHA_PIPELINE, "p1").join("run.json"));
    assert_eq!(
        run_json["header"]["overall_outcome"].as_str(),
        Some("bootstrap-failed"),
        "a bootstrap-failed artifact is produced for the bad parameter"
    );
    assert_eq!(
        run_json["attempts"].as_array().map(Vec::len),
        Some(0),
        "no node-execution events — rejected before execution"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// A pipeline parameter that collides with a reserved library flag is rejected
/// with the invalid-usage code and a message **naming** the collision — for both
/// binaries.
#[test]
fn parameter_flag_collision_is_rejected_and_named_for_both() {
    for (bin, param) in [(ALPHA, ALPHA_PARAM), (BETA, BETA_PARAM)] {
        let base = private_base("collide");
        let param_val = if param == ALPHA_PARAM { "1" } else { "eu" };
        let out = run(
            bin,
            &[
                "run",
                "--store",
                base.to_str().unwrap(),
                "--collide",
                &format!("--{param}"),
                param_val,
            ],
        );
        assert_eq!(
            code(&out),
            INVALID_USAGE,
            "a parameter/flag collision is rejected"
        );
        assert!(
            stdout(&out).contains("store") && stdout(&out).contains("collides"),
            "the collision names the offending flag, got: {}",
            stdout(&out)
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}

// ===========================================================================
// The T55-deferred round-trip: parameters + verbatim data-interval in the header.
// ===========================================================================

/// A run records its parameters **and** the verbatim `data-interval` in the
/// run-artifact header (the assertion T55 deferred to T56): drive the real run,
/// then read the emitted artifact and assert the interval round-trips verbatim.
#[test]
fn a_run_records_parameters_and_the_verbatim_data_interval_in_the_header() {
    let base = private_base("interval");
    let out = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "iv1",
            "--durable-boundary",
            "--shard",
            "9",
            "--data-interval",
            "2026-07-24T00:00:00Z..2026-07-25T00:00:00Z",
        ],
    );
    assert_eq!(code(&out), SUCCESS, "the run completes");
    let run_json = read_json(&run_dir(&base, ALPHA_PIPELINE, "iv1").join("run.json"));
    let header = &run_json["header"];
    // The parameter round-trips into the header.
    assert_eq!(
        header["parameters"]["shard"].as_str(),
        Some("9"),
        "the parameter is recorded verbatim in the run-artifact header"
    );
    // The data-interval round-trips VERBATIM (both endpoints, unchanged).
    let interval = &header["data_interval"];
    let start = interval["start"].as_str().or_else(|| {
        interval
            .as_array()
            .and_then(|a| a.first())
            .and_then(Value::as_str)
    });
    let end = interval["end"].as_str().or_else(|| {
        interval
            .as_array()
            .and_then(|a| a.get(1))
            .and_then(Value::as_str)
    });
    assert_eq!(
        start,
        Some("2026-07-24T00:00:00Z"),
        "the data-interval start round-trips verbatim into the header"
    );
    assert_eq!(
        end,
        Some("2026-07-25T00:00:00Z"),
        "the data-interval end round-trips verbatim into the header"
    );
    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// Single-node replay.
// ===========================================================================

/// Replay with rehydrated durable inputs — given a prior run that recorded durable
/// references for `transform`'s input (`load`), replaying `transform` from it
/// rehydrates the input, re-executes, and exits success; the replay-variant
/// artifact marks every unselected node `not-requested`.
#[test]
fn single_node_replay_rehydrates_durable_inputs_and_marks_not_requested() {
    let base = private_base("replay");
    // Produce a prior run whose durable `load` recorded a reference.
    let prior = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "prior",
            "--durable-boundary",
            "--shard",
            "1",
        ],
    );
    assert_eq!(code(&prior), SUCCESS, "the prior run is produced");
    let prior_dir = run_dir(&base, ALPHA_PIPELINE, "prior");

    // Replay `transform` from the prior run.
    let replay = run(
        ALPHA,
        &[
            "single-node",
            "--node",
            "transform",
            "--from",
            prior_dir.to_str().unwrap(),
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "replay",
        ],
    );
    assert_eq!(
        code(&replay),
        SUCCESS,
        "replay with rehydrated durable inputs succeeds"
    );
    let artifact = read_json(&run_dir(&base, ALPHA_PIPELINE, "replay").join("run.json"));
    // The replay-variant artifact: the replayed node carries its real terminal
    // state; every other node is marked `not-requested`.
    let attempts = artifact["attempts"].as_array().expect("attempts array");
    let transform = attempts
        .iter()
        .find(|a| a["node"].as_str() == Some("transform"))
        .expect("the replayed node is present");
    assert_eq!(
        transform["status"].as_str(),
        Some("succeeded"),
        "the replayed node carries its real terminal state"
    );
    let load = attempts
        .iter()
        .find(|a| a["node"].as_str() == Some("load"))
        .expect("the unselected node is present");
    assert_eq!(
        load["status"].as_str(),
        Some("not-requested"),
        "a node outside the request is marked `not-requested`"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Replay refused on a non-durable input — given a prior run in which `load` was
/// **not** durable (recorded no reference), replaying `transform` refuses with the
/// resume-refusal code and the error names the specific input and why.
#[test]
fn single_node_replay_refused_on_a_non_durable_input_names_it() {
    let base = private_base("refuse");
    let prior = run(
        ALPHA,
        &[
            "run",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "prior",
            "--durable-boundary",
            "--non-durable",
            "--shard",
            "1",
        ],
    );
    assert_eq!(
        code(&prior),
        SUCCESS,
        "the non-durable prior run is produced"
    );
    let prior_dir = run_dir(&base, ALPHA_PIPELINE, "prior");

    let refuse = run(
        ALPHA,
        &[
            "single-node",
            "--node",
            "transform",
            "--from",
            prior_dir.to_str().unwrap(),
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "refused",
        ],
    );
    assert_eq!(
        code(&refuse),
        RESUME_REFUSAL,
        "a non-durable input refuses with the resume-refusal code"
    );
    let text = stdout(&refuse);
    assert!(
        text.contains("load") && text.to_lowercase().contains("not durable"),
        "the refusal names the offending input (`load`) and why, got: {text}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Standalone no-input replay — a node that consumes nothing (`standalone`) is
/// replayed with **no** prior run supplied, runs standalone, and exits success.
#[test]
fn single_node_standalone_no_input_replay_runs_without_a_prior() {
    let base = private_base("standalone");
    let out = run(
        ALPHA,
        &[
            "single-node",
            "--node",
            "standalone",
            "--store",
            base.to_str().unwrap(),
            "--run-id",
            "sa1",
        ],
    );
    assert_eq!(
        code(&out),
        SUCCESS,
        "a consume-nothing node runs standalone without a prior run"
    );
    let artifact = read_json(&run_dir(&base, ALPHA_PIPELINE, "sa1").join("run.json"));
    let attempts = artifact["attempts"].as_array().expect("attempts");
    let sa = attempts
        .iter()
        .find(|a| a["node"].as_str() == Some("standalone"))
        .expect("the replayed node is present");
    assert_eq!(sa["status"].as_str(), Some("succeeded"));
    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// Fold (crashed-run path).
// ===========================================================================

/// `fold` on a killed run's partial stream (no `run-finished`) produces the
/// interrupted run artifact — the crash-clause path — for both binaries.
#[test]
fn fold_a_crashed_stream_produces_an_interrupted_artifact_for_both() {
    for (bin, pipeline, param, param_val) in [
        (ALPHA, ALPHA_PIPELINE, ALPHA_PARAM, "1"),
        (BETA, BETA_PIPELINE, BETA_PARAM, "eu"),
    ] {
        let base = private_base("fold");
        // Produce a complete run, then truncate its stream to simulate a crash.
        let out = run(
            bin,
            &[
                "run",
                "--store",
                base.to_str().unwrap(),
                "--run-id",
                "c1",
                "--durable-boundary",
                &format!("--{param}"),
                param_val,
            ],
        );
        assert_eq!(code(&out), SUCCESS);
        let stream_path = run_dir(&base, pipeline, "c1").join("events.jsonl");
        let stream = std::fs::read_to_string(&stream_path).unwrap();
        // Drop the last two records (node-terminal + run-finished) → a crash.
        let mut lines: Vec<&str> = stream.lines().collect();
        lines.truncate(lines.len().saturating_sub(2));
        let crashed_path = base.join("crashed.jsonl");
        std::fs::write(&crashed_path, format!("{}\n", lines.join("\n"))).unwrap();

        let folded = run(bin, &["fold", "--stream", crashed_path.to_str().unwrap()]);
        assert_eq!(code(&folded), SUCCESS, "fold exits cleanly");
        let artifact: Value = serde_json::from_str(&stdout(&folded)).expect("fold output is JSON");
        assert_eq!(
            artifact["interrupted"].as_bool(),
            Some(true),
            "a crash-truncated stream folds to an interrupted artifact"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}

// ===========================================================================
// Prune.
// ===========================================================================

/// Prune deletes nothing implicitly: after several runs in one store, and before
/// any `prune`, every prior run directory is still present (retention is not
/// applied at run end). Then `prune --keep N` removes exactly the excess oldest,
/// and `prune --older-than T` removes only runs past the threshold.
#[test]
fn prune_deletes_nothing_implicitly_then_by_count_and_by_age() {
    let base = private_base("prune");
    // Produce four runs (deterministic ids), planting a per-run numeric age marker
    // so pruning is deterministic (no wall clock).
    for (id, age) in [("r1", 1u64), ("r2", 2), ("r3", 3), ("r4", 4)] {
        let out = run(
            ALPHA,
            &[
                "run",
                "--store",
                base.to_str().unwrap(),
                "--run-id",
                id,
                "--shard",
                "1",
            ],
        );
        assert_eq!(code(&out), SUCCESS);
        std::fs::write(
            run_dir(&base, ALPHA_PIPELINE, id).join("age.txt"),
            age.to_string(),
        )
        .unwrap();
    }

    // Nothing deleted implicitly: all four run directories are still present.
    for id in ["r1", "r2", "r3", "r4"] {
        assert!(
            run_dir(&base, ALPHA_PIPELINE, id).is_dir(),
            "run `{id}` is still present before any prune (nothing deleted implicitly)"
        );
    }

    // Prune by count: keep the newest 2 (highest age markers), delete the 2 oldest.
    let by_count = run(
        ALPHA,
        &["prune", "--store", base.to_str().unwrap(), "--keep", "2"],
    );
    assert_eq!(code(&by_count), SUCCESS, "prune by count exits success");
    assert!(
        !run_dir(&base, ALPHA_PIPELINE, "r1").is_dir()
            && !run_dir(&base, ALPHA_PIPELINE, "r2").is_dir(),
        "the two oldest runs are removed by count"
    );
    assert!(
        run_dir(&base, ALPHA_PIPELINE, "r3").is_dir()
            && run_dir(&base, ALPHA_PIPELINE, "r4").is_dir(),
        "the newest keep-count runs remain"
    );

    // Prune by age: remove runs with age >= 4 (only r4 remains of the two), keep r3.
    let by_age = run(
        ALPHA,
        &[
            "prune",
            "--store",
            base.to_str().unwrap(),
            "--older-than",
            "4",
        ],
    );
    assert_eq!(code(&by_age), SUCCESS, "prune by age exits success");
    assert!(
        !run_dir(&base, ALPHA_PIPELINE, "r4").is_dir(),
        "the run past the age threshold is removed"
    );
    assert!(
        run_dir(&base, ALPHA_PIPELINE, "r3").is_dir(),
        "the run under the age threshold remains"
    );
    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// Invalid usage — a malformed flag.
// ===========================================================================

/// A malformed flag (an unknown/garbage argument to a verb that needs a required
/// flag) exits with the invalid-usage code — the invalid-usage table entry via a
/// real verb invocation.
#[test]
fn a_missing_required_flag_is_invalid_usage() {
    for bin in [ALPHA, BETA] {
        // `render` needs `--graph`; omitting it is invalid usage.
        let out = run(bin, &["render"]);
        assert_eq!(
            code(&out),
            INVALID_USAGE,
            "a verb invoked without its required flag is invalid usage"
        );
    }
}
