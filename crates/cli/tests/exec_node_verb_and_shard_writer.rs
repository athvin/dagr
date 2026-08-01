//! The **`exec-node` pod-side verb and the attempt shard** — written first, TDD.
//!
//! This is the pod half of remote execution, and its whole point is that it needs
//! **no cluster**: a subprocess invocation is indistinguishable from a pod
//! invocation from the verb's point of view. Every test here therefore launches the
//! reference pipeline binary as a **subprocess**, points it at a
//! **local-filesystem blob store**, and reads back what it wrote — no cluster
//! client, no pod spec, no cluster call anywhere in this file.
//!
//! What is asserted, and why each matters:
//!
//! - **One attempt, faithfully.** The verb runs the node's body exactly once
//!   through the *same* caught-attempt path a local run uses, so the shard's
//!   records are the local records — same kinds, same order, same classification.
//!   Retry is the orchestrator's, so a node whose policy grants retries still
//!   performs one attempt and emits no `BackoffStarted`.
//! - **Shard integrity.** The shard is self-identifying (run/node/attempt, both
//!   fingerprints, tool version, image digest) and verifiable, written atomically
//!   and last so a partial shard is never mistaken for a complete one, and refused
//!   by a reader when it came from a different build.
//! - **Inputs.** Missing, corrupt, and wrong-arity inputs are classified errors
//!   *distinct from a task failure*; a multi-input node rehydrates in declared
//!   order; the references the pod was actually given are recorded positionally so
//!   they can later be compared against the orchestrator's write-ahead record.
//! - **Re-entrancy.** The pod re-enters the same binary, so the resource registry
//!   is rebuilt by the binary's own flow-building path — once per invocation, which
//!   is the per-pod lifetime the design documents rather than prevents.
//! - **Cancellation.** SIGTERM yields a truthful `cancelled` shard inside the
//!   shutdown budget: a cancelled attempt is a real outcome, not a missing one.
//!
//! Nothing here asserts a constant next to itself: every expected value is derived
//! from what the subprocess actually wrote (the blob it stored, the shard it
//! emitted) or from the in-process engine run alongside it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dagr_blob::{BlobKey, BlobRef, BlobStore, LocalFsBlob};
use dagr_cli::exec_node_demo::{Counted, RESOURCE_LOG_ENV, SEED_ENV, build_exec_node_demo_flow};
use dagr_cli::shard::{AttemptShard, ShardError, shard_path};
use dagr_core::payload::Payload;
use dagr_core::test_kit::TempBase;

/// Cargo sets `CARGO_BIN_EXE_<name>` for every bin in the package when compiling an
/// integration test — the demo pipeline binary's path, resolved at build time. It is
/// the *same binary* on both sides by construction (ADR 115 §2's re-entrancy), which
/// is exactly what a pod would run.
const DEMO: &str = env!("CARGO_BIN_EXE_dagr-exec-node-demo");

/// The run identity every test uses unless it needs two.
const RUN: &str = "01931f1e-0000-7000-8000-00000000d0d0";

// ===========================================================================
// Harness — a private blob store, a subprocess invocation, a shard read-back.
// ===========================================================================

/// One `exec-node` invocation, built argument by argument so each test states only
/// what it varies.
struct Exec {
    run: String,
    node: String,
    attempt: u32,
    blob_store: PathBuf,
    inputs: Vec<String>,
    image_digest: Option<String>,
    expect_structural: Option<String>,
    env: BTreeMap<String, String>,
}

impl Exec {
    fn new(store: &Path, node: &str) -> Self {
        Self {
            run: RUN.to_string(),
            node: node.to_string(),
            attempt: 1,
            blob_store: store.to_path_buf(),
            inputs: Vec::new(),
            image_digest: None,
            expect_structural: None,
            env: BTreeMap::new(),
        }
    }

    fn run_id(mut self, run: &str) -> Self {
        self.run = run.to_string();
        self
    }

    fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    fn input(mut self, reference: impl Into<String>) -> Self {
        self.inputs.push(reference.into());
        self
    }

    fn image_digest(mut self, digest: &str) -> Self {
        self.image_digest = Some(digest.to_string());
        self
    }

    fn expect_structural(mut self, fingerprint: &str) -> Self {
        self.expect_structural = Some(fingerprint.to_string());
        self
    }

    fn env(mut self, key: &str, value: &str) -> Self {
        self.env.insert(key.to_string(), value.to_string());
        self
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(DEMO);
        cmd.arg("exec-node")
            .arg("--run")
            .arg(&self.run)
            .arg("--node")
            .arg(&self.node)
            .arg("--attempt")
            .arg(self.attempt.to_string())
            .arg("--blob-store")
            .arg(&self.blob_store);
        for reference in &self.inputs {
            cmd.arg("--input").arg(reference);
        }
        if let Some(digest) = &self.image_digest {
            cmd.arg("--image-digest").arg(digest);
        }
        if let Some(fingerprint) = &self.expect_structural {
            cmd.arg("--expect-structural").arg(fingerprint);
        }
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        cmd
    }

    /// Run the verb to completion and return its captured output.
    fn go(&self) -> Output {
        self.command()
            .output()
            .unwrap_or_else(|e| panic!("spawn {DEMO} exec-node: {e}"))
    }

    /// The shard this invocation would write, read back through the reader.
    fn shard(&self) -> Result<AttemptShard, ShardError> {
        AttemptShard::read(&self.blob_store, &self.run, &self.node, self.attempt)
    }
}

/// The process exit code, or `None` for a signal death.
fn code(out: &Output) -> Option<i32> {
    out.status.code()
}

/// Standard output + standard error joined, for diagnostics assertions.
fn said(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Store a `Counted` in `store` exactly the way the engine's bridge does — encode
/// through the codec, put the bytes — and return the self-describing reference. This
/// is how a test hands the pod an input it did not produce.
fn put_counted(store: &Path, n: u64) -> String {
    let backend = LocalFsBlob::open(store);
    let key = backend
        .put(&Counted { n }.encode_to_vec())
        .expect("put an input blob");
    backend.reference(&key).to_string()
}

/// The `Counted` a reference names, fetched and decoded the way a consumer would.
fn get_counted(reference: &str) -> Counted {
    let parsed = BlobRef::parse(reference).expect("a dagr blob reference");
    let backend = LocalFsBlob::open(parsed.container());
    let bytes = backend.get(parsed.key()).expect("fetch the output blob");
    Counted::decode(&bytes).expect("decode the output blob")
}

/// The wire `kind` of each record in the shard, in order — the shape a comparison
/// against a local in-process run is made over.
fn kinds(shard: &AttemptShard) -> Vec<String> {
    shard
        .records()
        .iter()
        .filter_map(|r| r.get("kind").and_then(|k| k.as_str()).map(str::to_string))
        .collect()
}

// ===========================================================================
// One attempt, faithfully
// ===========================================================================

/// The golden path: a node with one rehydrated input runs once, its output lands in
/// the blob store, and the shard says `succeeded`.
#[test]
fn one_attempt_stores_its_output_and_the_shard_records_succeeded() {
    let base = TempBase::new("exec-node-golden");
    let store = base.path();
    let input = put_counted(store, 21);

    let exec = Exec::new(store, "double").input(&input);
    let out = exec.go();
    assert_eq!(code(&out), Some(0), "a succeeding attempt exits 0: {}", said(&out));

    let shard = exec.shard().expect("a complete shard");
    assert_eq!(shard.terminal_state(), "succeeded");
    assert_eq!(shard.identity().node(), "double");
    assert_eq!(shard.identity().attempt(), 1);
    assert_eq!(shard.identity().run_id(), RUN);

    // The output is a real blob: named in the shard, fetchable, and the value the
    // node's body actually computed from the rehydrated input (21 doubled).
    let output = shard.output().expect("a succeeded durable output");
    assert_eq!(get_counted(output.uri()), Counted { n: 42 });
    assert_eq!(
        output.recorded_content_hash().expect("a recorded content hash"),
        BlobKey::of(&Counted { n: 42 }.encode_to_vec()).to_string(),
        "the recorded hash is the digest of the encoded bytes",
    );
    assert!(
        output.recorded_size_bytes().is_some(),
        "the reference metadata carries a size"
    );
    assert!(
        output
            .recorded_scheme()
            .is_some_and(|s| s.starts_with("dagr-blob+")),
        "the reference metadata carries the scheme: {output:?}",
    );
}

/// The shard's attempt records are **equivalent to the local in-process ones** for
/// the same outcome — same event kinds, same order, same classification. The local
/// side is a real engine run of the same flow, not a hand-written expectation.
#[test]
fn shard_records_match_the_local_in_process_records() {
    let base = TempBase::new("exec-node-equivalence");
    let store = base.path();
    let input = put_counted(store, 4);

    let exec = Exec::new(store, "double").input(&input);
    let out = exec.go();
    assert_eq!(code(&out), Some(0), "{}", said(&out));
    let shard = exec.shard().expect("a complete shard");

    let local = dagr_cli::exec_node_demo::local_attempt_kinds("double");
    assert_eq!(
        kinds(&shard),
        local,
        "the pod's records are the local records, in the local order",
    );
    assert_eq!(
        dagr_cli::exec_node_demo::local_terminal_state("double"),
        shard.terminal_state(),
        "and the same classification",
    );
}

/// Each `TaskError` class produces the matching outcome in the shard, and the exit
/// code distinguishes the run-failure family from every non-task cause.
#[test]
fn each_task_error_class_records_its_outcome_and_a_distinguishing_exit_code() {
    for (class, terminal, expected_code) in [
        ("permanent", "failed", 1),
        ("retryable", "failed", 1),
        ("skip", "skipped", 0),
    ] {
        let base = TempBase::new(&format!("exec-node-boom-{class}"));
        let store = base.path();
        let input = put_counted(store, 3);

        let exec = Exec::new(store, "boom")
            .input(&input)
            .env("DAGR_DEMO_BOOM", class);
        let out = exec.go();
        assert_eq!(
            code(&out),
            Some(expected_code),
            "a `{class}` task error exits {expected_code}: {}",
            said(&out)
        );

        let shard = exec.shard().expect("a shard is written for a failing attempt too");
        assert_eq!(shard.terminal_state(), terminal, "class `{class}`");
        assert!(
            shard.output().is_none(),
            "a non-succeeding attempt names no output"
        );
        assert!(
            shard.diagnostics().iter().any(|d| d.contains("boom")),
            "the shard carries the task's own message: {:?}",
            shard.diagnostics()
        );
    }
}

/// A panicking task is contained and attributed to the node; the process exits with
/// a code rather than aborting, and the shard records a failed attempt whose
/// diagnostics name the panic.
#[test]
fn a_panicking_task_is_contained_attributed_and_does_not_abort() {
    let base = TempBase::new("exec-node-panic");
    let store = base.path();
    let input = put_counted(store, 5);

    let exec = Exec::new(store, "panicky").input(&input);
    let out = exec.go();
    assert_eq!(
        code(&out),
        Some(1),
        "a caught panic is a run failure, not an abort: {}",
        said(&out)
    );

    let shard = exec.shard().expect("a complete shard");
    assert_eq!(shard.terminal_state(), "failed");
    assert!(
        shard
            .diagnostics()
            .iter()
            .any(|d| d.contains("panicky") || d.to_lowercase().contains("panic")),
        "the panic is attributed to the node: {:?}",
        shard.diagnostics()
    );
    assert!(
        kinds(&shard).contains(&"attempt-failed".to_string()),
        "the attempt records a failure: {:?}",
        kinds(&shard)
    );
}

/// Retry is the orchestrator's, never the pod's: a node whose policy grants retries
/// still performs **exactly one** attempt here, and emits no backoff.
#[test]
fn a_retrying_node_still_performs_exactly_one_attempt_and_emits_no_backoff() {
    let base = TempBase::new("exec-node-no-retry");
    let store = base.path();
    let input = put_counted(store, 1);

    let exec = Exec::new(store, "retrying").input(&input);
    let out = exec.go();
    assert_eq!(code(&out), Some(1), "{}", said(&out));

    let shard = exec.shard().expect("a complete shard");
    let started = shard
        .records()
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("attempt-started"))
        .count();
    assert_eq!(started, 1, "exactly one attempt: {:?}", kinds(&shard));

    // The engine's local retry loop is what would have emitted a backoff; the pod's
    // path must never reach it. `BackoffStarted` folds onto the `attempt-failed`
    // wire kind, so the count of failure records is the honest witness: one attempt
    // that failed once, never a second attempt after a wait.
    let failed = shard
        .records()
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("attempt-failed"))
        .count();
    assert_eq!(
        failed, 1,
        "no backoff record and no second attempt: {:?}",
        kinds(&shard)
    );
    assert!(
        dagr_cli::exec_node_demo::local_attempt_count("retrying") > 1,
        "the same node really does retry locally — otherwise this test proves nothing",
    );
}

// ===========================================================================
// Shard integrity
// ===========================================================================

/// The shard is self-identifying: it names the run, node, attempt, both
/// fingerprints, the tool version, and the image digest of the binary that wrote it.
#[test]
fn a_complete_shard_names_the_build_that_wrote_it() {
    let base = TempBase::new("exec-node-identity");
    let store = base.path();
    let input = put_counted(store, 2);

    let exec = Exec::new(store, "double")
        .input(&input)
        .image_digest("sha256:c0ffee");
    let out = exec.go();
    assert_eq!(code(&out), Some(0), "{}", said(&out));

    let shard = exec.shard().expect("a complete shard");
    let id = shard.identity();
    let (structural, policy) = dagr_cli::exec_node_demo::demo_fingerprints();
    assert_eq!(id.structural_fingerprint(), structural);
    assert_eq!(id.policy_hash(), policy);
    assert_eq!(id.tool_version(), dagr_cli::contract::TOOL_VERSION);
    assert_eq!(id.recorded_image_digest(), Some("sha256:c0ffee"));

    // The verification the orchestrator performs before replaying anything.
    shard
        .verify_build(&structural, dagr_cli::contract::TOOL_VERSION)
        .expect("a shard from this build verifies");
}

/// Killed mid-shard-write, a reader must not mistake the debris for a complete
/// shard. Two disciplines are asserted: the shard is written **atomically** (so an
/// interrupted write leaves no shard at the final path, and no temp debris a reader
/// would pick up), and a **truncated** shard is refused rather than half-replayed.
#[test]
fn an_incomplete_shard_is_never_mistaken_for_a_complete_one() {
    let base = TempBase::new("exec-node-incomplete");
    let store = base.path();
    let input = put_counted(store, 6);

    // Fault injection: stop before the rename. Nothing lands at the final path.
    let exec = Exec::new(store, "double")
        .input(&input)
        .env("DAGR_DEMO_SHARD_FAULT", "stop-before-rename");
    let out = exec.go();
    assert_ne!(code(&out), Some(0), "an unwritable shard is not a success");
    assert!(
        matches!(exec.shard(), Err(ShardError::Missing { .. })),
        "an interrupted write leaves no shard to read: {:?}",
        exec.shard()
    );
    let debris: Vec<PathBuf> = walk(store)
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".tmp."))
        })
        .collect();
    assert!(debris.is_empty(), "no temp debris survives: {debris:?}");

    // Truncation: a complete shard, cut short, is detected as incomplete.
    let good = TempBase::new("exec-node-truncate");
    let store2 = good.path();
    let input2 = put_counted(store2, 6);
    let exec2 = Exec::new(store2, "double").input(&input2);
    assert_eq!(code(&exec2.go()), Some(0));
    let path = shard_path(store2, RUN, "double", 1);
    let bytes = std::fs::read(&path).expect("read the complete shard");
    assert!(
        AttemptShard::parse(&bytes).is_ok(),
        "the complete shard parses"
    );
    let cut = &bytes[..bytes.len() / 2];
    assert!(
        matches!(AttemptShard::parse(cut), Err(ShardError::Incomplete { .. })),
        "a truncated shard is refused as incomplete, not read as complete",
    );
}

/// A shard written by a binary with a different structural fingerprint is refused,
/// and the refusal **names both** fingerprints so an operator can see which build
/// produced what.
#[test]
fn a_shard_from_a_different_build_is_refused_naming_both_fingerprints() {
    let base = TempBase::new("exec-node-wrong-build");
    let store = base.path();
    let input = put_counted(store, 8);
    let exec = Exec::new(store, "double").input(&input);
    assert_eq!(code(&exec.go()), Some(0));
    let shard = exec.shard().expect("a complete shard");

    let foreign = "fnv1a-64:v1:0000000000000000";
    let err = shard
        .verify_build(foreign, dagr_cli::contract::TOOL_VERSION)
        .expect_err("a foreign structural fingerprint is refused");
    let message = err.to_string();
    assert!(message.contains(foreign), "names the expected: {message}");
    assert!(
        message.contains(shard.identity().structural_fingerprint()),
        "names the found: {message}",
    );

    // The same refusal is available to the *pod*, before it does any work: an
    // orchestrator that states what it expects gets a refusal instead of a run.
    let refused = Exec::new(store, "double")
        .input(&input)
        .expect_structural(foreign)
        .go();
    assert_eq!(
        code(&refused),
        Some(6),
        "a fingerprint mismatch is a refusal, on the refusal code: {}",
        said(&refused)
    );
    let said = said(&refused);
    assert!(said.contains(foreign), "names both: {said}");
}

// ===========================================================================
// Inputs
// ===========================================================================

/// A missing input blob fails with a classified error naming the reference — and on
/// an exit code distinct from a task failure, so the orchestrator can tell "the
/// storage lost the input" from "the task said no".
#[test]
fn a_missing_input_blob_fails_distinguishably_and_names_the_reference() {
    let base = TempBase::new("exec-node-missing-input");
    let store = base.path();
    // A well-formed reference to bytes that were never stored.
    let dangling = LocalFsBlob::open(store)
        .reference(&BlobKey::of(b"never stored"))
        .to_string();

    let exec = Exec::new(store, "double").input(&dangling);
    let out = exec.go();
    assert_eq!(
        code(&out),
        Some(4),
        "a missing input is not a task failure (1): {}",
        said(&out)
    );
    let message = said(&out);
    assert!(message.contains(&dangling), "names the reference: {message}");
    assert!(
        message.contains("absent"),
        "names the classification: {message}"
    );
}

/// An input blob whose digest no longer matches is refused as **corrupt** rather
/// than decoded into a wrong value.
#[test]
fn an_input_whose_digest_no_longer_matches_fails_as_corrupt() {
    let base = TempBase::new("exec-node-corrupt-input");
    let store = base.path();
    let reference = put_counted(store, 9);

    // Overwrite the object out of band, exactly as a careless external process would.
    let parsed = BlobRef::parse(&reference).expect("a reference");
    let path = LocalFsBlob::open(store).object_path(parsed.key());
    std::fs::write(&path, Counted { n: 999 }.encode_to_vec()).expect("overwrite the blob");

    let exec = Exec::new(store, "double").input(&reference);
    let out = exec.go();
    assert_eq!(code(&out), Some(4), "corrupt is not a task failure: {}", said(&out));
    let message = said(&out);
    assert!(message.contains("corrupt"), "names corruption: {message}");
    assert!(message.contains(&reference), "names the reference: {message}");
}

/// A multi-input node rehydrates its inputs **in declared order**, and the arity is
/// checked against the node's declaration.
#[test]
fn a_multi_input_node_rehydrates_in_declared_order_and_checks_arity() {
    let base = TempBase::new("exec-node-multi-input");
    let store = base.path();
    let first = put_counted(store, 3);
    let second = put_counted(store, 7);

    // `combine` computes first*10 + second, so a swapped order is observable.
    let exec = Exec::new(store, "combine").input(&first).input(&second);
    assert_eq!(code(&exec.go()), Some(0));
    let shard = exec.shard().expect("a complete shard");
    let output = shard.output().expect("an output");
    assert_eq!(
        get_counted(output.uri()),
        Counted { n: 37 },
        "positional order is preserved",
    );

    let swapped = TempBase::new("exec-node-multi-input-swapped");
    let store2 = swapped.path();
    let a = put_counted(store2, 3);
    let b = put_counted(store2, 7);
    let exec2 = Exec::new(store2, "combine").input(&b).input(&a);
    assert_eq!(code(&exec2.go()), Some(0));
    let out2 = exec2.shard().expect("a complete shard");
    assert_eq!(
        get_counted(out2.output().expect("an output").uri()),
        Counted { n: 73 },
        "swapping the references really does change the answer",
    );

    // Wrong arity is an invalid invocation, named and refused before any work.
    let short = Exec::new(store, "combine").input(&first).go();
    assert_eq!(
        code(&short),
        Some(2),
        "too few references is invalid usage: {}",
        said(&short)
    );
    assert!(
        said(&short).contains('2') && said(&short).contains("combine"),
        "names the node and the declared arity: {}",
        said(&short)
    );
    let long = Exec::new(store, "double")
        .input(&first)
        .input(&second)
        .go();
    assert_eq!(code(&long), Some(2), "too many is invalid usage too");
}

/// The shard records the input references the pod was **actually given**, in
/// positional order with their content hashes — the fact a later comparison against
/// the orchestrator's write-ahead `attempt-submitted` record is made over.
#[test]
fn the_shard_records_the_input_references_it_was_given_positionally() {
    let base = TempBase::new("exec-node-recorded-inputs");
    let store = base.path();
    let first = put_counted(store, 11);
    let second = put_counted(store, 13);

    let exec = Exec::new(store, "combine").input(&first).input(&second);
    assert_eq!(code(&exec.go()), Some(0));
    let shard = exec.shard().expect("a complete shard");

    let recorded: Vec<&str> = shard.inputs().iter().map(|c| c.uri()).collect();
    assert_eq!(
        recorded,
        vec![first.as_str(), second.as_str()],
        "recorded in the order the pod was given them",
    );
    for (consumed, reference) in shard.inputs().iter().zip([&first, &second]) {
        let parsed = BlobRef::parse(reference).expect("a reference");
        assert_eq!(
            consumed.content_hash(),
            Some(parsed.key().to_string().as_str()),
            "every recorded reference carries its content hash",
        );
    }
}

/// A consume-nothing node attempts no input rehydration at all — and supplying it a
/// reference is refused rather than silently ignored.
#[test]
fn a_consume_nothing_node_rehydrates_nothing() {
    let base = TempBase::new("exec-node-source");
    let store = base.path();

    let exec = Exec::new(store, "seed").env(SEED_ENV, "5");
    assert_eq!(code(&exec.go()), Some(0));
    let shard = exec.shard().expect("a complete shard");
    assert_eq!(shard.terminal_state(), "succeeded");
    assert!(
        shard.inputs().is_empty(),
        "an empty array, never a null: {:?}",
        shard.inputs()
    );
    assert_eq!(
        get_counted(shard.output().expect("an output").uri()),
        Counted { n: 5 }
    );

    let spurious = put_counted(store, 1);
    let refused = Exec::new(store, "seed").input(&spurious).go();
    assert_eq!(
        code(&refused),
        Some(2),
        "a reference handed to a source is invalid usage: {}",
        said(&refused)
    );
}

// ===========================================================================
// Re-entrancy and resources
// ===========================================================================

/// The pod rebuilds the resource registry through the binary's own flow-building
/// path, and the task obtains its resource from it.
#[test]
fn a_task_obtains_its_resource_from_the_rebuilt_registry() {
    let base = TempBase::new("exec-node-resource");
    let store = base.path();
    let log = base.path().join("resource-constructions.log");

    let exec = Exec::new(store, "resourceful").env(RESOURCE_LOG_ENV, log.to_str().unwrap());
    let out = exec.go();
    assert_eq!(code(&out), Some(0), "{}", said(&out));

    let shard = exec.shard().expect("a complete shard");
    assert_eq!(shard.terminal_state(), "succeeded");
    let value = get_counted(shard.output().expect("an output").uri());
    let constructions = std::fs::read_to_string(&log).expect("the resource log");
    let first_id: u64 = constructions
        .lines()
        .next()
        .expect("one construction")
        .trim()
        .parse()
        .expect("a numeric resource id");
    assert_eq!(
        value,
        Counted { n: first_id },
        "the task read the very resource this invocation constructed",
    );
}

/// A resource that records its construction is constructed **once per `exec-node`
/// invocation** — the per-pod lifetime ADR 115 documents rather than prevents.
#[test]
fn a_resource_is_constructed_once_per_invocation() {
    let base = TempBase::new("exec-node-resource-lifetime");
    let store = base.path();
    let log = base.path().join("resource-constructions.log");

    let exec = Exec::new(store, "resourceful").env(RESOURCE_LOG_ENV, log.to_str().unwrap());
    assert_eq!(code(&exec.go()), Some(0));
    assert_eq!(
        std::fs::read_to_string(&log).unwrap().lines().count(),
        1,
        "exactly one construction per invocation",
    );

    // A second invocation is a second pod: it constructs its own.
    let again = Exec::new(store, "resourceful")
        .attempt(2)
        .env(RESOURCE_LOG_ENV, log.to_str().unwrap());
    assert_eq!(code(&again.go()), Some(0));
    assert_eq!(
        std::fs::read_to_string(&log).unwrap().lines().count(),
        2,
        "once per pod, not once per run",
    );
}

// ===========================================================================
// Cancellation
// ===========================================================================

/// SIGTERM mid-attempt: the attempt records `cancelled`, a truthful shard is
/// written, and the process exits inside the shutdown budget.
#[cfg(unix)]
#[test]
fn sigterm_mid_attempt_yields_a_truthful_cancelled_shard() {
    use std::time::{Duration, Instant};

    let base = TempBase::new("exec-node-sigterm");
    let store = base.path();
    let input = put_counted(store, 12);
    let started = base.path().join("sleeper-started");

    let exec = Exec::new(store, "sleeper")
        .input(&input)
        .env("DAGR_DEMO_SLEEPER_MARKER", started.to_str().unwrap());
    let mut child = exec.command().spawn().expect("spawn the pod");

    // Synchronise on an observable marker, never a wall-clock sleep.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !started.exists() {
        assert!(Instant::now() < deadline, "the sleeper never started");
        std::thread::sleep(Duration::from_millis(10));
    }

    let sent = Instant::now();
    // SIGTERM — pod deletion / preemption, the signal an orchestrator sends first.
    #[allow(
        unsafe_code,
        reason = "libc::kill is the only way to deliver a real SIGTERM to the child \
                  process under test — pod deletion is a real signal, and a \
                  simulated one would prove nothing about the installed handler"
    )]
    // SAFETY: `libc::kill` is an FFI call with no pointer arguments and no memory
    // obligations. The pid is the live child this test spawned and has not yet
    // reaped, so it names that process and no other, and `SIGTERM` is a valid
    // signal number the child installed a handler for before touching its marker.
    unsafe {
        libc::kill(
            i32::try_from(child.id()).expect("a pid fits in i32"),
            libc::SIGTERM,
        );
    }
    let status = child.wait().expect("reap the pod");
    let elapsed = sent.elapsed();

    assert!(
        elapsed < Duration::from_secs(25),
        "exits inside the shutdown budget, took {elapsed:?}",
    );
    assert_eq!(
        status.code(),
        Some(5),
        "externally-originated termination with no run failure is the cancellation code",
    );

    let shard = exec.shard().expect("a truthful shard is still written");
    assert_eq!(
        shard.terminal_state(),
        "cancelled",
        "a cancelled attempt is a real outcome, not a missing one",
    );
    assert!(shard.output().is_none(), "a cancelled attempt names no output");
    assert_eq!(shard.identity().node(), "sleeper");
}

// ===========================================================================
// No cluster required, and no new exit numbers
// ===========================================================================

/// The exit codes this verb reports are the **existing** table's — it invents no
/// number of its own.
#[test]
fn the_verb_reuses_the_existing_exit_code_table() {
    use dagr_cli::contract::ExitCode;
    let numbers: Vec<u8> = ExitCode::ALL.iter().map(|c| c.as_u8()).collect();
    assert_eq!(
        numbers,
        vec![0, 1, 2, 3, 4, 5, 6, 7],
        "the table is unchanged: exec-node adds no number",
    );
    // Every code this suite asserts is drawn from that table.
    for asserted in [0_u8, 1, 2, 4, 5, 6] {
        assert!(
            numbers.contains(&asserted),
            "exec-node exit {asserted} is a table code",
        );
    }
}

/// This whole suite runs against a local filesystem blob store, by subprocess, with
/// no cluster — asserted structurally over this file's own source so it cannot rot.
#[test]
fn the_suite_needs_no_cluster() {
    let source = include_str!("exec_node_verb_and_shard_writer.rs");
    // The needles are assembled at run time rather than written out, because a
    // literal here would be the very occurrence the assertion forbids — the check
    // has to be able to pass.
    let forbidden = [
        ["ku", "be"].concat(),
        ["Kuber", "netes"].concat(),
        ["k", "8s"].concat(),
        ["Pod", "Spec"].concat(),
        ["api", "Version"].concat(),
    ];
    for forbidden in &forbidden {
        assert!(
            !source.contains(forbidden.as_str()),
            "this suite must not reach for `{forbidden}` — it needs no cluster",
        );
    }
    assert!(
        source.contains("LocalFsBlob"),
        "and it does use the local filesystem blob store",
    );
}

/// An unknown node name is an assembly-level refusal, not a silent success: the pod
/// was asked to run something this build does not have.
#[test]
fn an_unknown_node_is_refused_and_named() {
    let base = TempBase::new("exec-node-unknown");
    let out = Exec::new(base.path(), "no-such-node").go();
    assert_eq!(
        code(&out),
        Some(3),
        "the graph does not contain it: {}",
        said(&out)
    );
    assert!(
        said(&out).contains("no-such-node"),
        "names the node: {}",
        said(&out)
    );
}

/// Two attempts of the same node write **distinct** shards — the attempt number is
/// part of the shard's address, so a retry never overwrites its predecessor's record.
#[test]
fn each_attempt_gets_its_own_shard() {
    let base = TempBase::new("exec-node-attempt-keyed");
    let store = base.path();
    let input = put_counted(store, 2);

    let first = Exec::new(store, "double").input(&input).attempt(1);
    let second = Exec::new(store, "double").input(&input).attempt(4);
    assert_eq!(code(&first.go()), Some(0));
    assert_eq!(code(&second.go()), Some(0));

    assert_eq!(first.shard().expect("shard 1").identity().attempt(), 1);
    assert_eq!(second.shard().expect("shard 4").identity().attempt(), 4);
    assert_ne!(
        shard_path(store, RUN, "double", 1),
        shard_path(store, RUN, "double", 4),
        "attempt is part of the shard's address",
    );

    // And so do two runs of the same node.
    let other_run = Exec::new(store, "double")
        .input(&input)
        .run_id("01931f1e-0000-7000-8000-0000000000ff");
    assert_eq!(code(&other_run.go()), Some(0));
    assert_ne!(
        shard_path(store, RUN, "double", 1),
        shard_path(
            store,
            "01931f1e-0000-7000-8000-0000000000ff",
            "double",
            1
        ),
        "run identity is part of the shard's address",
    );
}

/// Every file under `dir`, recursively — used to prove no write-temp debris survives.
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(walk(&path));
        } else {
            found.push(path);
        }
    }
    found
}

/// The in-process flow builds — a guard that the demo pipeline this suite drives is
/// a real, assembling flow rather than a fixture that only exists in argv.
#[test]
fn the_demo_flow_assembles() {
    let pipeline = build_exec_node_demo_flow().into_pipeline();
    pipeline.assemble().expect("the demo flow assembles");
    let names: Vec<&str> = pipeline.nodes().map(dagr_core::flow::PipelineNode::name).collect();
    for expected in [
        "seed",
        "double",
        "combine",
        "boom",
        "panicky",
        "sleeper",
        "resourceful",
        "retrying",
    ] {
        assert!(names.contains(&expected), "the flow declares `{expected}`");
    }
}
