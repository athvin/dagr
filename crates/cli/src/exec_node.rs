//! The **`exec-node` verb** — one attempt of one node, in this process, reporting
//! through an [attempt shard](crate::shard).
//!
//! # What it is, and what it deliberately is not
//!
//! This is the pod side of ADR 115, and it does not know it is in a pod. It takes an
//! identity triple and a list of durable references on argv, rehydrates those
//! references into the node's inputs, runs **exactly one** attempt through the same
//! caught-attempt path a local run uses, stores the produced value, and writes a
//! shard. It submits nothing, watches nothing, and speaks to no cluster — which is
//! why every one of its tests runs as a plain subprocess.
//!
//! **Re-entrancy is what makes it small.** The pod runs *the same binary*, so the
//! graph, the task code, the resource registry, and the codec are identical on both
//! sides by construction. Nothing about the pipeline is transported: the wire format
//! is `{run, node, attempt, input references}` and the rest is rebuilt by calling the
//! binary's own flow factory. That is also why `RunContext`'s three
//! non-serializable pieces are not a problem — the parameters and resource registry
//! are reconstructed here, and cancellation is a signal rather than a transported
//! handle.
//!
//! # Retry is not the pod's job
//!
//! The attempt is prepared under [`AttemptDiscipline::ExactlyOnce`], so the node's
//! declared retry budget and timeout are **not** enforced here even when it has
//! them: two retry loops would duplicate an attempt, and the orchestrator owns both
//! decisions (ADR 115 §2). A node with `retries(3)` therefore emits one
//! `attempt-started` and no backoff.
//!
//! # Exit codes reuse the existing table
//!
//! No new numbers. The table already distinguishes every cause this verb has:
//!
//! | outcome | code |
//! |---|---|
//! | the attempt succeeded, or the task deliberately skipped | `0` |
//! | the task failed, timed out, or panicked | `1` |
//! | the invocation is malformed — a missing flag, a wrong input arity | `2` |
//! | this build's graph has no such node, or the flow does not assemble | `3` |
//! | an input is absent or corrupt, a codec refused it, the store is unreachable | `4` |
//! | the attempt was cancelled (SIGTERM) with no run failure | `5` |
//! | the invoker's expected fingerprint is not this build's | `6` |
//! | the shard could not be written | `7` |
//!
//! A missing input and a task failure are the pair that matters most: an
//! orchestrator must be able to tell "the storage lost the input" from "the task
//! said no", because only one of them is the pipeline's fault.

use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_blob::{BlobRef, BlobStore, LocalFsBlob};
use dagr_core::context::{
    CancellationSource, PipelineId, RunContext, RunId, TerminalState,
};
use dagr_core::execution::{AttemptEvent, AttemptEventSink};
use dagr_core::handle::NodeId;

use crate::contract::{ExitCode, TOOL_VERSION};
use crate::graph::{format_fingerprint_policy, format_fingerprint_structural};
use crate::registry::FlowFactory;
use crate::run_flow::PrepareAttemptError;
use crate::shard::{AttemptShard, ConsumedRef, ShardIdentity, ShardOutput};

/// The backend a reference must name for this build to fetch it. The object-store
/// backend lands behind the same port later (T110); until it does, a reference
/// naming it is refused honestly rather than guessed at.
const LOCAL_BACKEND: &str = "file";

/// The shutdown budget a cancelled attempt is given to return before it is recorded
/// `abandoned` and the process exits — arch.md C16's default, and the same number the
/// run verbs use.
const DEFAULT_GRACE: Duration = Duration::from_secs(10);

/// The environment variable the fault-injection test uses to stop a shard write
/// **before** its rename — the atomic-write discipline's test seam, and the only
/// thing in this module that exists for a test.
const SHARD_FAULT_ENV: &str = "DAGR_DEMO_SHARD_FAULT";

// ===========================================================================
// Arguments
// ===========================================================================

/// The parsed `exec-node` invocation.
#[derive(Debug, Clone)]
struct ExecNodeArgs {
    run: String,
    node: String,
    attempt: u32,
    blob_store: PathBuf,
    inputs: Vec<String>,
    image_digest: Option<String>,
    expect_structural: Option<String>,
    grace: Duration,
}

impl ExecNodeArgs {
    /// Parse the verb's arguments out of the raw invocation.
    ///
    /// Input references arrive as a **repeated `--input`**, which preserves their
    /// positional order and cannot be confused with the flow-name positional the
    /// command-line contract already reserves (C26). Order is load-bearing: dagr
    /// binds inputs positionally.
    fn parse(argv: &[OsString]) -> Result<Self, String> {
        let tokens: Vec<String> = argv
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let mut run = None;
        let mut node = None;
        let mut attempt = None;
        let mut blob_store = None;
        let mut inputs = Vec::new();
        let mut image_digest = None;
        let mut expect_structural = None;
        let mut grace = DEFAULT_GRACE;

        let mut index = 0;
        while index < tokens.len() {
            let token = &tokens[index];
            index += 1;
            let Some(flag) = token.strip_prefix("--") else {
                continue;
            };
            let (name, inline) = match flag.split_once('=') {
                Some((name, value)) => (name, Some(value.to_string())),
                None => (flag, None),
            };
            let mut take = |name: &str| -> Result<String, String> {
                if let Some(value) = inline.clone() {
                    return Ok(value);
                }
                let value = tokens
                    .get(index)
                    .cloned()
                    .ok_or_else(|| format!("`--{name}` needs a value"))?;
                index += 1;
                Ok(value)
            };
            match name {
                "run" => run = Some(take("run")?),
                "node" => node = Some(take("node")?),
                "attempt" => attempt = Some(take("attempt")?),
                "blob-store" => blob_store = Some(PathBuf::from(take("blob-store")?)),
                "input" => inputs.push(take("input")?),
                "image-digest" => image_digest = Some(take("image-digest")?),
                "expect-structural" => expect_structural = Some(take("expect-structural")?),
                "grace" => {
                    let raw = take("grace")?;
                    grace = crate::config::parse_duration(&raw)
                        .map_err(|e| format!("`--grace {raw}`: {e}"))?;
                }
                _ => {}
            }
        }

        let attempt = match attempt {
            Some(raw) => raw
                .parse::<u32>()
                .map_err(|_| format!("`--attempt {raw}` is not an attempt number"))?,
            None => return Err("`exec-node` needs `--attempt <n>`".to_string()),
        };
        if attempt == 0 {
            return Err("`--attempt` is 1-based; 0 is not an attempt".to_string());
        }
        Ok(Self {
            run: run.ok_or("`exec-node` needs `--run <run-id>`")?,
            node: node.ok_or("`exec-node` needs `--node <name>`")?,
            attempt,
            blob_store: blob_store
                .ok_or("`exec-node` needs `--blob-store <container>` to write its output and shard")?,
            inputs,
            image_digest,
            expect_structural,
            grace,
        })
    }
}

// ===========================================================================
// The verb body
// ===========================================================================

/// Run one attempt of one node of the selected flow, and write its shard.
///
/// This is the registry's [`FlowAction`](crate::registry) for `exec-node`: it builds
/// a **fresh** flow through the factory — the re-entrancy that makes the wire format
/// tiny — and prepares exactly one attempt of the named node.
pub(crate) fn exec_node_selected_flow<W: Write>(
    flow_name: &str,
    factory: &FlowFactory,
    argv: &[OsString],
    out: &mut W,
) -> ExitCode {
    let args = match ExecNodeArgs::parse(argv) {
        Ok(args) => args,
        Err(message) => {
            let _ = writeln!(out, "dagr exec-node: {message}");
            return ExitCode::InvalidUsage;
        }
    };
    match run_attempt(flow_name, factory, &args, out) {
        Ok(code) => code,
        Err(failure) => {
            let _ = writeln!(out, "dagr exec-node: {}", failure.message);
            failure.code
        }
    }
}

/// A refusal that happens **before** an attempt runs, or a failure that prevents one
/// being reported: it carries the diagnostic and the table code it exits on.
struct Refusal {
    code: ExitCode,
    message: String,
}

impl Refusal {
    fn new(code: ExitCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one attempt's whole life, in the order it happens: prepare, verify the \
              build, rehydrate, run, store, report. Splitting it would hide the \
              sequence that is the point of reading it."
)]
fn run_attempt<W: Write>(
    flow_name: &str,
    factory: &FlowFactory,
    args: &ExecNodeArgs,
    out: &mut W,
) -> Result<ExitCode, Refusal> {
    // Re-entrancy: the pod builds the graph, the tasks, the resources, and the codec
    // by running the binary's own flow-building code. Nothing about the pipeline
    // crossed the wire, so orchestrator-to-task skew is impossible by construction.
    let mut prepared = factory().prepare_attempt(&args.node).map_err(|err| {
        let code = match err {
            PrepareAttemptError::Assembly(_) | PrepareAttemptError::UnknownNode { .. } => {
                ExitCode::AssemblyFailure
            }
            // A node whose value has no codec is a graph this build cannot run
            // remotely at all — the graph's fault, like an unassemblable one.
            PrepareAttemptError::NotRemoteEligible { .. } => ExitCode::AssemblyFailure,
        };
        Refusal::new(code, err.to_string())
    })?;

    let fingerprint = prepared.fingerprint();
    let structural = format_fingerprint_structural(&fingerprint);
    let policy = format_fingerprint_policy(&fingerprint);

    // Refuse work launched by a different program BEFORE doing any of it. The shard
    // carries the same two values so a reader can make the same check afterwards.
    if let Some(expected) = &args.expect_structural
        && expected != &structural
    {
        return Err(Refusal::new(
            ExitCode::ResumeRefusal,
            format!(
                "refusing an attempt launched by a different build: the invoker expects \
                 structural fingerprint `{expected}`, this binary is `{structural}`"
            ),
        ));
    }

    // Arity is a property of the node's declaration, so a mismatch is a malformed
    // invocation — and it is checked before a single byte is fetched.
    let declared = prepared.input_arity();
    if args.inputs.len() != declared {
        return Err(Refusal::new(
            ExitCode::InvalidUsage,
            format!(
                "node `{}` declares {declared} input(s) and was given {}",
                args.node,
                args.inputs.len()
            ),
        ));
    }

    // Rehydrate, in positional order, recording what we were actually given.
    let mut consumed = Vec::with_capacity(args.inputs.len());
    for (index, reference) in args.inputs.iter().enumerate() {
        let parsed = BlobRef::parse(reference).map_err(|err| {
            Refusal::new(
                ExitCode::BootstrapFailure,
                format!("input {index} `{reference}` is unusable: {err}"),
            )
        })?;
        if parsed.backend() != LOCAL_BACKEND {
            return Err(Refusal::new(
                ExitCode::BootstrapFailure,
                format!(
                    "input {index} `{reference}` names the `{}` backend, which this build \
                     has no store for",
                    parsed.backend()
                ),
            ));
        }
        let store = LocalFsBlob::open(parsed.container());
        // `get` verifies the bytes against the key, so a blob overwritten out of band
        // is refused as corrupt rather than decoded into a wrong value.
        let bytes = store.get(parsed.key()).map_err(|err| {
            Refusal::new(
                ExitCode::BootstrapFailure,
                format!(
                    "input {index} `{reference}` is {}: {}",
                    err.class().as_str(),
                    err.message()
                ),
            )
        })?;
        prepared.fill_input(index, &bytes).map_err(|err| {
            Refusal::new(
                ExitCode::BootstrapFailure,
                format!(
                    "input {index} `{reference}` did not decode as `{}`: {err}",
                    prepared.input_type_name(index).unwrap_or("<unknown>")
                ),
            )
        })?;
        consumed.push(ConsumedRef::new(
            reference.clone(),
            Some(parsed.key().to_string()),
        ));
    }

    // Cancellation: pod deletion arrives as SIGTERM. The attempt observes the same
    // `CancellationSignal` a locally-run attempt observes; nothing about the task's
    // view of cancellation changes because it is running here.
    let cancellation = CancellationSource::new();
    let signal = cancellation.signal();
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = {
        let cancellation = cancellation.clone();
        let fired = Arc::clone(&fired);
        crate::signals::install_signal_handlers_with(move || {
            fired.store(true, std::sync::atomic::Ordering::SeqCst);
            cancellation.cancel();
        })
        .map_err(|err| {
            Refusal::new(
                ExitCode::BootstrapFailure,
                format!("the termination-signal handlers could not be installed: {err}"),
            )
        })?
    };

    let ctx = {
        let mut builder = RunContext::builder(
            RunId::new(args.run.clone()),
            // The pipeline identity is the selected flow's name — the same identity a
            // local run of this flow carries, so the attempt's log span reads the same
            // on both sides.
            PipelineId::new(flow_name),
            NodeId::from_name(&args.node),
        )
        .attempt(args.attempt)
        // The orchestrator owns retry, so from the attempt's point of view this is
        // the only attempt there will be. Saying otherwise would be a lie the task
        // can read off its own context.
        .max_attempts(1)
        .cancellation(signal);
        if let Some(resources) = prepared.resources() {
            builder = builder.resources(resources.clone());
        }
        builder.build()
    };

    let sink = CollectingSink::new(&args.run);
    let watchdog = Watchdog::arm(args, &structural, &policy, &fired, &sink, args.grace);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .map_err(|err| {
            Refusal::new(
                ExitCode::BootstrapFailure,
                format!("the attempt runtime could not be built: {err}"),
            )
        })?;
    let mut emitting = sink.clone();
    let terminal = runtime.block_on(prepared.run_once(&ctx, &mut emitting));
    watchdog.disarm();
    drop(guard);

    // A cancellation observed mid-attempt is a real outcome, not a failure: the
    // attempt did not decide anything, the operator did. A cancellation that arrived
    // after the attempt already succeeded leaves the success standing — that is the
    // truthful record.
    let cancelled = fired.load(std::sync::atomic::Ordering::SeqCst);
    let effective = if cancelled && terminal != TerminalState::Succeeded {
        TerminalState::Cancelled
    } else {
        terminal
    };

    // Store the produced value, last but one — the shard is written after it, so a
    // shard's existence implies its output exists.
    let mut output = None;
    let mut diagnostics = diagnostics_from(&sink.drain_snapshot(), &args.node);
    if effective != TerminalState::Succeeded {
        // The outcome, in words, attributed to the node. The abstract attempt-event
        // port carries no task message — a local attempt's records carry none either,
        // which is exactly why the shard's records stay equivalent to them — so this
        // is what the pod can say truthfully about a non-success.
        diagnostics.push(format!(
            "`{}` attempt {} ended {}",
            args.node,
            args.attempt,
            terminal_label(effective)
        ));
    }
    if effective == TerminalState::Succeeded {
        if let Some(bytes) = prepared.encode_output() {
            let store = LocalFsBlob::open(&args.blob_store);
            let key = store.put(&bytes).map_err(|err| {
                Refusal::new(
                    ExitCode::BootstrapFailure,
                    format!(
                        "the output of `{}` could not be stored: {}",
                        args.node,
                        err.message()
                    ),
                )
            })?;
            let reference = store.reference(&key);
            output = Some(
                ShardOutput::new(reference.to_string())
                    .content_hash(key.to_string())
                    .size_bytes(bytes.len() as u64)
                    .scheme(format!("dagr-blob+{}", reference.backend())),
            );
        } else {
            diagnostics.push(format!(
                "`{}` succeeded without filling its output slot, so no reference was recorded",
                args.node
            ));
        }
    }

    let shard = build_shard(
        args,
        &structural,
        &policy,
        effective,
        &consumed,
        &sink.records(),
        output,
        &diagnostics,
    );
    let path = shard
        .write_atomically(&args.blob_store, shard_fault_injected())
        .map_err(|err| Refusal::new(ExitCode::SinkFailure, err.to_string()))?;
    let _ = writeln!(
        out,
        "dagr exec-node: `{}` attempt {} ended {} — shard at {}",
        args.node,
        args.attempt,
        terminal_label(effective),
        path.display()
    );

    Ok(exit_code_for(effective))
}

/// The exit code one attempt's terminal state reports, drawn entirely from the
/// existing table.
fn exit_code_for(state: TerminalState) -> ExitCode {
    match state {
        // A skip is a deliberate task decision, and a skip-only run exits cleanly.
        TerminalState::Succeeded | TerminalState::Skipped => ExitCode::Success,
        TerminalState::Failed | TerminalState::TimedOut => ExitCode::RunFailure,
        TerminalState::Cancelled | TerminalState::Abandoned => ExitCode::Cancelled,
        // Propagated and carried-forward states are decided by the orchestrator over
        // a whole graph; a single attempt never reaches one. Reporting a run failure
        // is the conservative answer if one ever did.
        TerminalState::UpstreamSkipped
        | TerminalState::UpstreamFailed
        | TerminalState::SatisfiedFromPrior => ExitCode::RunFailure,
    }
}

/// The normative spelling of a terminal state, as the shard records it.
fn terminal_label(state: TerminalState) -> &'static str {
    match state {
        TerminalState::Succeeded => "succeeded",
        TerminalState::Failed => "failed",
        TerminalState::TimedOut => "timed-out",
        TerminalState::Skipped => "skipped",
        TerminalState::UpstreamSkipped => "upstream-skipped",
        TerminalState::UpstreamFailed => "upstream-failed",
        TerminalState::Cancelled => "cancelled",
        TerminalState::Abandoned => "abandoned",
        TerminalState::SatisfiedFromPrior => "satisfied-from-prior",
    }
}

/// Whether the atomic-write fault seam is armed for this invocation.
fn shard_fault_injected() -> bool {
    std::env::var(SHARD_FAULT_ENV).is_ok_and(|v| v == "stop-before-rename")
}

/// The diagnostic strings an attempt's records imply — a panic payload, and the node
/// attribution that makes a bare payload readable.
fn diagnostics_from(events: &[AttemptEvent], node: &str) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            AttemptEvent::AttemptPanicked { message, .. } => {
                Some(format!("`{node}` panicked: {message}"))
            }
            _ => None,
        })
        .collect()
}

/// Assemble the shard from what the attempt produced.
#[expect(
    clippy::too_many_arguments,
    reason = "the shard's own fields, passed once at the single construction site"
)]
fn build_shard(
    args: &ExecNodeArgs,
    structural: &str,
    policy: &str,
    terminal: TerminalState,
    consumed: &[ConsumedRef],
    records: &[serde_json::Value],
    output: Option<ShardOutput>,
    diagnostics: &[String],
) -> AttemptShard {
    let mut identity = ShardIdentity::new(
        args.run.clone(),
        args.node.clone(),
        args.attempt,
        structural,
        policy,
        TOOL_VERSION,
    );
    if let Some(digest) = &args.image_digest {
        identity = identity.image_digest(digest.clone());
    }
    let mut shard = AttemptShard::new(identity, terminal_label(terminal))
        .with_inputs(consumed.to_vec())
        .with_records(rewrite_terminal(records, terminal_label(terminal)));
    if let Some(output) = output {
        shard = shard.with_output(output);
    }
    for diagnostic in diagnostics {
        shard = shard.with_diagnostic(diagnostic.clone());
    }
    shard
}

/// Stamp the **effective** terminal state onto the attempt's `node-terminal` record.
///
/// The attempt runner decides a terminal state from the task's own result, which is
/// the truth in every case but one: a cancellation the operator originated is not
/// something the task decided. The shard therefore carries the effective state in
/// exactly one place, and the record set stays otherwise byte-for-byte what a local
/// attempt produced.
fn rewrite_terminal(records: &[serde_json::Value], state: &str) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|record| {
            let mut record = record.clone();
            if record.get("kind").and_then(serde_json::Value::as_str) == Some("node-terminal")
                && let Some(object) = record.as_object_mut()
            {
                object.insert("state".into(), state.into());
            }
            record
        })
        .collect()
}

// ===========================================================================
// The sink and the shutdown watchdog
// ===========================================================================

/// The attempt's event sink: it buffers the abstract [`AttemptEvent`]s and stamps
/// each into the **event-stream envelope** the shard carries, with an attempt-local
/// `seq` the orchestrator rewrites on replay.
#[derive(Clone)]
struct CollectingSink {
    run: String,
    events: Arc<Mutex<Vec<AttemptEvent>>>,
}

impl CollectingSink {
    /// A sink stamping records for `run`.
    fn new(run: &str) -> Self {
        Self {
            run: run.to_string(),
            events: Arc::default(),
        }
    }

    /// A snapshot of the abstract events, in emission order.
    fn drain_snapshot(&self) -> Vec<AttemptEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The event-stream records the shard carries.
    fn records(&self) -> Vec<serde_json::Value> {
        crate::shard::records_for(&self.run, &self.drain_snapshot())
    }
}

impl AttemptEventSink for CollectingSink {
    fn emit(&mut self, event: AttemptEvent) {
        // Poison policy: recover. The buffer holds only a record vector; dropping
        // every later record because an unrelated panic poisoned the lock would make
        // the shard less truthful than the panic did, and the panic itself is one of
        // the records we must keep.
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
    }
}

/// The **shutdown budget**, enforced.
///
/// A task that observes cancellation returns promptly and the ordinary path records
/// it. A task that does not cannot be killed — so after the grace period this thread
/// writes a truthful `abandoned` shard from the records emitted so far and exits the
/// process, exactly as arch.md C16 describes: after grace, the process exits
/// promptly and leftover threads die with it.
struct Watchdog {
    done: Arc<std::sync::atomic::AtomicBool>,
}

impl Watchdog {
    fn arm(
        args: &ExecNodeArgs,
        structural: &str,
        policy: &str,
        fired: &Arc<std::sync::atomic::AtomicBool>,
        sink: &CollectingSink,
        grace: Duration,
    ) -> Self {
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (args, structural, policy) = (
            args.clone(),
            structural.to_string(),
            policy.to_string(),
        );
        let (fired, sink, watch) = (Arc::clone(fired), sink.clone(), Arc::clone(&done));
        std::thread::spawn(move || {
            let poll = Duration::from_millis(20);
            // Wait for a signal, or for the attempt to finish first.
            loop {
                if watch.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                if fired.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(poll);
            }
            let deadline = std::time::Instant::now() + grace;
            while std::time::Instant::now() < deadline {
                if watch.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(poll);
            }
            let shard = build_shard(
                &args,
                &structural,
                &policy,
                TerminalState::Abandoned,
                &[],
                &sink.records(),
                None,
                &[format!(
                    "`{}` was asked to cancel and had not returned after {grace:?}",
                    args.node
                )],
            );
            let _ = shard.write_atomically(&args.blob_store, false);
            std::process::exit(ExitCode::Cancelled.as_u8().into());
        });
        Self { done }
    }

    fn disarm(&self) {
        self.done.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
