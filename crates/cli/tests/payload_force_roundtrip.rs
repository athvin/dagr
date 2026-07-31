//! The `--dagr.force-roundtrip` operator toggle — the local codec check.
//!
//! # Why this suite exists
//!
//! A `Payload`-bounded value still moves through the in-memory slot locally, with
//! no encode and no decode: that is the fast path, and it is untouched. But a codec
//! bug that only shows up when bytes actually cross a boundary is a bug you find in
//! a cluster, at hour three. The toggle exists so the *same* handoff can be forced
//! through a real encode/decode locally, and CI can run the suite both ways.
//!
//! What this file pins:
//!
//! - **Default off, and off means nothing happens.** With no flag and no env var,
//!   a `Payload`-bounded pipeline performs **zero** encodes and zero decodes, and
//!   its event stream is byte-identical (modulo the informational wall stamp) to
//!   the same pipeline registered through the ordinary registrars.
//! - **On means every handoff really round-trips.** Each produced value is encoded
//!   and decoded, the run still succeeds with identical terminal states, and the
//!   stream is *still* byte-identical — the round trip adds no records.
//! - **A codec fault is loud.** A payload whose decode fails turns its node
//!   `failed` under the toggle — which is the entire point of having it. The
//!   classified `CodecError` rides on the `TaskError` (in its message and as its
//!   source); what an attempt record carries into the stream is the attempt path's
//!   existing behaviour, unchanged here.
//! - **The knob follows `flag > env > default`** and is a reserved library flag a
//!   pipeline parameter can never shadow. (The precedence resolver's own unit tests
//!   live beside the other `DAGR_*` knobs in `dagr_cli::config`.)
//! - **`dagr-core` still has an empty runtime dependency set** — the codec added no
//!   dependency.
//!
//! Determinism: an injected in-memory sink and monotonic tick clock, a fixed run id,
//! and a private temp store; no wall-clock sleep anywhere.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::config::{FORCE_ROUNDTRIP_FLAG, parse_force_roundtrip_flag};
use dagr_cli::contract::{ExitCode, reserved_flag_names};
use dagr_cli::driver::RunConfig;
use dagr_cli::registry::{FlowRegistry, run_registry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::StableName;
use dagr_core::TaskError;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::payload::{Codec, CodecError, Cursor};
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;
use std::sync::Arc;

// ===========================================================================
// Deterministic injection seams (the shape every run-flow suite uses).
// ===========================================================================

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().clone()
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// Blank the volatile `wall` stamp so two runs with an identical injected monotonic
/// clock compare byte-for-byte (the wall clock is the only non-deterministic field
/// the writer stamps).
fn strip_wall(stream: &[u8]) -> Vec<serde_json::Value> {
    let parsed = dagr_artifact::event_stream::read_records(stream).expect("stream parses");
    parsed
        .records
        .into_iter()
        .map(|mut rec| {
            if let Some(obj) = rec.as_object_mut() {
                obj.insert("wall".into(), serde_json::Value::String("<wall>".into()));
            }
            rec
        })
        .collect()
}

// ===========================================================================
// A counting payload — how "no encode call" is proved rather than asserted.
// ===========================================================================

static ENCODES: AtomicU64 = AtomicU64::new(0);
static DECODES: AtomicU64 = AtomicU64::new(0);

/// Serialize the counter-reading tests: the counters are process-global, and cargo
/// runs the tests in this binary in parallel.
fn counters() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ENCODES.store(0, Ordering::SeqCst);
    DECODES.store(0, Ordering::SeqCst);
    guard
}

/// A payload whose codec counts its own calls, so "the fast path performed no
/// encode" is a measurement, not a claim.
#[derive(Debug, Clone, PartialEq, Eq, StableName)]
struct Counted(u64);

impl Codec for Counted {
    fn encode_body(&self, out: &mut Vec<u8>) {
        ENCODES.fetch_add(1, Ordering::SeqCst);
        self.0.encode_body(out);
    }
    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        DECODES.fetch_add(1, Ordering::SeqCst);
        u64::decode_body(cursor).map(Counted)
    }
}

/// A payload that encodes fine and **never** decodes — a stand-in for the codec bug
/// the toggle exists to surface locally.
#[derive(Debug, Clone, PartialEq, Eq, StableName)]
struct Broken(u64);

impl Codec for Broken {
    fn encode_body(&self, out: &mut Vec<u8>) {
        self.0.encode_body(out);
    }
    fn decode_body(_cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        Err(CodecError::malformed(
            "Broken",
            "this payload's decoder is deliberately broken",
        ))
    }
}

// ===========================================================================
// The pipeline: source → double, both producing a payload.
// ===========================================================================

struct Produce(u64);
impl Task for Produce {
    type Input = ();
    type Output = Counted;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Counted, TaskError> {
        Ok(Counted(self.0))
    }
}

struct Double;
impl Task for Double {
    type Input = Counted;
    type Output = Counted;
    async fn run(&mut self, _c: &RunContext, input: Counted) -> Result<Counted, TaskError> {
        Ok(Counted(input.0 * 2))
    }
}

struct ProduceBroken;
impl Task for ProduceBroken {
    type Input = ();
    type Output = Broken;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Broken, TaskError> {
        Ok(Broken(1))
    }
}

const PIPE: &str = "force-roundtrip-pipe";

/// The pipeline registered through the **payload-bounded** registrars, with the
/// toggle in the given position.
fn payload_flow(force: bool) -> RunnableFlow {
    let mut flow = RunnableFlow::new().force_roundtrip(force);
    let source = flow.register_source_payload("source", Produce(21));
    let _ = flow.register_payload("double", Double, source);
    flow
}

/// The identical pipeline registered through the **ordinary** registrars — the
/// before-this-change baseline the toggled-off stream must match.
fn plain_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let source = flow.register_source("source", Produce(21));
    let _ = flow.register("double", Double, source);
    flow
}

/// Drive a flow against the deterministic seams and return its outcome, terminal
/// states, and raw stream.
fn drive(flow: RunnableFlow) -> (RunOutcome, Vec<(String, TerminalState)>, Vec<u8>) {
    let base = TempBase::new("force-roundtrip");
    let sink = MemorySink::default();
    let config = RunConfig::new(base.as_str()).run_id("fixed-run");
    let report = flow
        .run(PIPE, &config, sink.clone(), TickClock::default())
        .expect("the flow assembles");
    // The driver's map is a `BTreeMap`, so this is already node-name order.
    let terminals: Vec<(String, TerminalState)> = report
        .driver_report()
        .terminal_states
        .iter()
        .map(|(n, s)| (n.clone(), *s))
        .collect();
    (report.outcome(), terminals, sink.bytes())
}

// ===========================================================================
// Default off — the in-memory fast path is untouched
// ===========================================================================

/// With the toggle off (the default), a `Payload`-bounded pipeline performs **no**
/// encode and no decode: the value moves through the slot in memory exactly as it
/// always has.
#[test]
fn with_the_toggle_off_no_handoff_is_ever_encoded() {
    let _guard = counters();
    let (outcome, terminals, _stream) = drive(payload_flow(false));
    assert_eq!(outcome, RunOutcome::Succeeded);
    assert!(
        terminals
            .iter()
            .all(|(_, s)| *s == TerminalState::Succeeded),
        "every node succeeds: {terminals:?}"
    );
    assert_eq!(
        (
            ENCODES.load(Ordering::SeqCst),
            DECODES.load(Ordering::SeqCst)
        ),
        (0, 0),
        "the toggle is off, so the local handoff must not touch the codec"
    );
}

/// With the toggle on, **every** payload-bounded handoff is encoded and decoded, and
/// the run still succeeds with the same terminal states.
#[test]
fn with_the_toggle_on_every_payload_handoff_round_trips() {
    let (off_outcome, off_terminals) = {
        let _guard = counters();
        let (outcome, terminals, _) = drive(payload_flow(false));
        (outcome, terminals)
    };

    let _guard = counters();
    let (outcome, terminals, _stream) = drive(payload_flow(true));
    assert_eq!(outcome, off_outcome, "the run still succeeds");
    assert_eq!(
        terminals, off_terminals,
        "the terminal states are identical with the toggle on"
    );
    assert_eq!(
        ENCODES.load(Ordering::SeqCst),
        2,
        "both payload-producing nodes encoded their output"
    );
    assert_eq!(
        DECODES.load(Ordering::SeqCst),
        2,
        "and both decoded it back before it reached the slot"
    );
}

/// The toggle changes **no** observable record: the plain pipeline, the
/// payload-bounded pipeline with the toggle off, and the same with it on all produce
/// the identical event stream (modulo the informational wall stamp).
#[test]
fn the_event_stream_is_identical_plain_off_and_on() {
    let _guard = counters();
    let (_, _, plain) = drive(plain_flow());
    let (_, _, off) = drive(payload_flow(false));
    let (_, _, on) = drive(payload_flow(true));

    assert_eq!(
        strip_wall(&plain),
        strip_wall(&off),
        "a payload-bounded pipeline with the toggle off streams exactly what the \
         ordinary registration streams"
    );
    assert_eq!(
        strip_wall(&off),
        strip_wall(&on),
        "the forced round trip emits no records of its own"
    );
}

/// A payload whose decoder is broken fails its node **loudly** under the toggle —
/// and is invisible with the toggle off. This is what "catchable without a cluster"
/// means.
#[test]
fn a_codec_fault_surfaces_as_a_failed_node_only_under_the_toggle() {
    let build = |force: bool| {
        let mut flow = RunnableFlow::new().force_roundtrip(force);
        let _ = flow.register_source_payload("source", ProduceBroken);
        flow
    };

    let (clean_outcome, clean_terminals, _) = drive(build(false));
    assert_eq!(
        clean_outcome,
        RunOutcome::Succeeded,
        "with the toggle off the broken decoder is never called"
    );
    assert_eq!(clean_terminals[0].1, TerminalState::Succeeded);

    let (outcome, terminals, _) = drive(build(true));
    assert_ne!(
        outcome,
        RunOutcome::Succeeded,
        "a codec fault fails the run under the toggle"
    );
    assert_eq!(
        terminals[0].1,
        TerminalState::Failed,
        "the node whose payload cannot round-trip is `failed` — a codec defect is \
         permanent, so it is not retried into a green run"
    );
}

// ===========================================================================
// The knob itself
// ===========================================================================

/// The library-owned flag is reserved, so a pipeline parameter can never shadow it.
#[test]
fn the_flag_is_a_reserved_library_flag() {
    assert!(
        reserved_flag_names().contains(&"dagr.force-roundtrip"),
        "every runtime knob with a `DAGR_*` fallback owns a reserved flag: {:?}",
        reserved_flag_names()
    );
    assert_eq!(FORCE_ROUNDTRIP_FLAG, "--dagr.force-roundtrip");
}

/// The flag parses in its bare, `=value`, and separate-value forms, and a value that
/// is not a boolean is refused (never silently treated as off).
#[test]
fn the_flag_parses_its_accepted_forms_and_refuses_garbage() {
    let argv = |args: &[&str]| -> Vec<std::ffi::OsString> {
        args.iter().map(std::ffi::OsString::from).collect()
    };
    assert_eq!(
        parse_force_roundtrip_flag(&argv(&["dagr", "run", "etl"])).expect("absent parses"),
        None,
        "absent means fall through to the env/default"
    );
    assert_eq!(
        parse_force_roundtrip_flag(&argv(&["dagr", "run", "etl", FORCE_ROUNDTRIP_FLAG]))
            .expect("bare parses"),
        Some(true),
        "a bare flag means on"
    );
    assert_eq!(
        parse_force_roundtrip_flag(&argv(&[
            "dagr",
            "run",
            "etl",
            FORCE_ROUNDTRIP_FLAG,
            "false"
        ]))
        .expect("separate value parses"),
        Some(false)
    );
    assert_eq!(
        parse_force_roundtrip_flag(&argv(&[
            "dagr",
            "run",
            "etl",
            "--dagr.force-roundtrip=true"
        ]))
        .expect("=value parses"),
        Some(true)
    );
    assert!(
        parse_force_roundtrip_flag(&argv(&[
            "dagr",
            "run",
            "etl",
            "--dagr.force-roundtrip=maybe"
        ]))
        .is_err(),
        "a non-boolean value fails loudly"
    );
}

/// End to end through the CLI: `dagr run <flow> --dagr.force-roundtrip` really
/// round-trips the pipeline's handoffs, and a malformed value is invalid usage.
#[test]
fn the_cli_run_verb_honours_the_flag() {
    let _guard = counters();
    let base = TempBase::new("force-roundtrip-cli");
    let registry = FlowRegistry::new().add("payload", || payload_flow(false));

    let code = run_registry(
        &registry,
        [
            "dagr",
            "run",
            "payload",
            "--store",
            base.as_str(),
            FORCE_ROUNDTRIP_FLAG,
        ],
    );
    assert_eq!(code, ExitCode::Success, "the run succeeds under the toggle");
    assert!(
        ENCODES.load(Ordering::SeqCst) > 0,
        "the operator flag reached the run: the handoffs were encoded"
    );

    ENCODES.store(0, Ordering::SeqCst);
    let code = run_registry(
        &registry,
        ["dagr", "run", "payload", "--store", base.as_str()],
    );
    assert_eq!(code, ExitCode::Success);
    assert_eq!(
        ENCODES.load(Ordering::SeqCst),
        0,
        "without the flag the fast path is untouched"
    );

    let code = run_registry(
        &registry,
        [
            "dagr",
            "run",
            "payload",
            "--store",
            base.as_str(),
            "--dagr.force-roundtrip=maybe",
        ],
    );
    assert_eq!(
        code,
        ExitCode::InvalidUsage,
        "a bad toggle value fails loudly, never silently off"
    );
}

// ===========================================================================
// The authoring surface
// ===========================================================================

/// The **one authoring import** carries the codec: `use dagr_cli::prelude::*;`
/// brings both the `Payload` trait (type namespace) and its derive (macro
/// namespace) into scope, exactly as it does for `StableName`.
mod the_prelude_carries_the_codec {
    use dagr_cli::prelude::*;

    /// A payload declared with nothing but the prelude in scope.
    #[derive(StableName, Payload)]
    struct FromPrelude {
        count: u64,
    }

    #[test]
    fn payload_and_its_derive_are_re_exported_from_the_prelude() {
        let mut bytes = Vec::new();
        FromPrelude { count: 7 }.encode(&mut bytes);
        assert!(!bytes.is_empty(), "the derived encoder wrote the envelope");
        assert_eq!(
            FromPrelude::decode(&bytes)
                .expect("a prelude-declared payload round-trips")
                .count,
            7
        );
    }
}

// ===========================================================================
// The zero-dependency guarantee
// ===========================================================================

/// The codec added **no** runtime dependency: `dagr-core` still resolves alone.
#[test]
fn dagr_core_still_has_an_empty_runtime_dependency_set() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the workspace root is two levels above crates/cli")
        .to_path_buf();
    let out = std::process::Command::new(cargo)
        .args([
            "tree",
            "-p",
            "dagr-core",
            "-e",
            "normal",
            "--prefix",
            "none",
            "--no-default-features",
        ])
        .current_dir(&root)
        .output()
        .expect("cargo tree runs");
    assert!(out.status.success(), "cargo tree -p dagr-core failed");
    let packages: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect();
    assert!(
        !packages.is_empty(),
        "cargo tree reported nothing — the assertion would be vacuous"
    );
    let foreign: Vec<&String> = packages.iter().filter(|p| *p != "dagr-core").collect();
    assert!(
        foreign.is_empty(),
        "`dagr-core` must still resolve with an EMPTY runtime dependency set; it reached: {foreign:?}"
    );
}
