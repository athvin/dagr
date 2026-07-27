//! The **README quickstart**, as a compiled-and-run reference.
//!
//! This file is the *single source of truth* for the quickstart's Rust code. The
//! `## Quickstart` section of the repository `README.md` quotes the region
//! between the `ANCHOR: quickstart` / `ANCHOR_END: quickstart` markers below
//! **verbatim**, and the test `crates/cli/tests/readme_quickstart.rs` fails the
//! build if the two ever diverge — so the walkthrough a reader copies is exactly
//! the code CI compiles and runs, never a hand-maintained paraphrase that rots.
//!
//! It takes a reader from an empty directory to a compiled, run,
//! artifact-inspected **two-node pipeline** using the one-call
//! [`RunnableFlow`](dagr_cli::run_flow::RunnableFlow) seam: the author
//! writes two [`#[task]`](dagr_core::task)s and a flow, and *never* writes a
//! scheduler, a `NodeRunner`, or any slot wiring. The
//! [`#[task]`](dagr_core::task) attribute generates the
//! [`Task`](dagr_core::task::Task) impl from the `run` fn, so each node body is a
//! single function — the hand-written `impl Task` stays a first-class fallback.
//! No async experience is required — the two task bodies are ordinary code
//! returning a value; the framework runs them.
//!
//! No server, database, or scheduler is involved: the binary and its one
//! argument (a run-store directory) are sufficient. Run it
//! with `cargo run --example quickstart -- ./quickstart-runs`.

// ANCHOR: quickstart
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::task;

// --- 1. Author two tasks. A task is a value holding its configuration, plus one
// `async fn run` body — that is the whole authoring surface. `#[task]` reads the
// `run` signature and generates the four things you declare (the input type, the
// output type, the execution class, the work), so you write business logic only —
// no scheduling, retry, permit, or trait-impl scaffolding. (Prefer to write the
// `impl Task` by hand? It stays a first-class fallback — see the cookbook.)

/// The source: consumes nothing (an `()` input) and produces a starting number.
struct Count {
    up_to: u64,
}

#[task]
impl Count {
    async fn run(&mut self, _input: ()) -> Result<u64, TaskError> {
        Ok(self.up_to)
    }
}

/// The sink: consumes the source's `u64` and produces its double.
struct Double;

#[task]
impl Double {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

fn main() -> ExitCode {
    // The one thing this binary needs from you: a run-store directory. Everything
    // the run leaves behind — its event stream — lives under it. No network, no
    // database, no scheduler.
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./quickstart-runs".to_string());

    // --- 2. Wire the flow. Register the two tasks; `register` binds the sink to
    // the source's typed handle. A wrong-typed binding would be a *compile* error,
    // not a runtime surprise. This is the whole graph — two nodes, one edge.
    let mut flow = RunnableFlow::new();
    let counted = flow.register_source("count", Count { up_to: 21 });
    let doubled = flow.register::<Double, _>("double", Double, counted);

    // --- 3. Run it in one call. `run` assembles the pipeline, builds every node's
    // runner for you, and drives the whole graph to completion — writing a real
    // event stream to `<base>/quickstart/<run-id>/events.jsonl`.
    let sink = match FileSink::create(&base, "quickstart", "quickstart-run") {
        Ok(sink) => sink,
        Err(err) => {
            eprintln!("could not open the run store under {base}: {err}");
            return ExitCode::from(2);
        }
    };
    let config = RunConfig::new(base.clone()).run_id("quickstart-run");
    let report = match flow.run("quickstart", &config, sink, TickClock::default()) {
        Ok(report) => report,
        Err(err) => {
            eprintln!("the flow did not assemble: {err}");
            return ExitCode::from(2);
        }
    };

    // --- 4. Inspect the result. Read a node's produced value back by its handle,
    // and fold the event stream to confirm the two-node shape and terminal states.
    let value = report.output(doubled);
    println!("run {} finished: {:?}", report.run_id(), report.outcome());
    println!("count -> {:?}", report.terminal_state("count"));
    println!(
        "double -> {:?} (value {value:?})",
        report.terminal_state("double")
    );

    // The event stream is the crash-proof record everything else derives from.
    let stream_path = Path::new(&base)
        .join("quickstart")
        .join("quickstart-run")
        .join("events.jsonl");
    if let Ok(bytes) = std::fs::read(&stream_path) {
        let stream = read_records(&bytes).expect("the event stream parses");
        let terminals = stream
            .records
            .iter()
            .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("node-terminal"))
            .count();
        println!("the stream records {terminals} node-terminal events (one per node)");
    }

    if report.outcome() == RunOutcome::Succeeded {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// A minimal append-only event sink writing each line to `events.jsonl` under the
/// run store. dagr injects the sink so the run store is *your* one job; a real
/// deployment points the base at durable storage.
struct FileSink {
    file: File,
}

impl FileSink {
    fn create(base: &str, pipeline: &str, run_id: &str) -> io::Result<Self> {
        let dir = Path::new(base).join(pipeline).join(run_id);
        create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("events.jsonl"))?;
        Ok(Self { file })
    }
}

impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.file.write_all(line)?;
        self.file.flush()
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A monotonic clock advanced one tick per read — deterministic, wall-clock-free.
/// Durations in the artifact are computed from these monotonic offsets.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}
// ANCHOR_END: quickstart

// ===========================================================================
// The DAGR_* environment surface + the binary-wiring pattern
// ===========================================================================
//
// Every runtime knob honours the standard **`flag > env > default`** precedence:
// a value can be set once in an orchestrator's environment and still be overridden
// per-invocation on the command line. The environment surface, alongside the
// startup-banner toggle `DAGR_NO_BANNER`:
//
// | Flag                          | Env var                      | Default              | Type     | Validation                                    |
// |-------------------------------|------------------------------|----------------------|----------|-----------------------------------------------|
// | `--grace`                     | `DAGR_GRACE`                 | 10s                  | Duration | `10` / `10s` / `10ms`                          |
// | `--teardown-deadline`         | `DAGR_TEARDOWN_DEADLINE`     | 15s                  | Duration | `10` / `10s` / `10ms`                          |
// | `--failure-mode`              | `DAGR_FAILURE_MODE`          | continue-independent | enum     | `continue-independent` \| `stop-on-first-failure` |
// | `--dagr.pool.compute-threads` | `DAGR_POOL_COMPUTE_THREADS`  | detected             | u32      | ≥ 1                                            |
// | `--dagr.pool.blocking-threads`| `DAGR_POOL_BLOCKING_THREADS` | detected             | u32      | ≥ 1                                            |
// | `--dagr.pool.memory`          | `DAGR_POOL_MEMORY`           | detected             | u64      | ≥ 1 byte                                       |
// | `--dagr.headroom-fraction`    | `DAGR_HEADROOM`              | 0.20                 | f64      | `0.0..=1.0`                                    |
// | `--no-banner`                 | `DAGR_NO_BANNER` (or NO_COLOR) | banner shown       | flag     | present ⇒ suppress                             |
//
// A bad env value **fails loudly** and is never silently ignored or clamped: an
// unparseable value exits `InvalidUsage`, an out-of-range value (e.g. a headroom of
// `1.5`) exits `BootstrapFailure` — each diagnostic naming the offending variable.
//
// `dagr-core` reads **no** environment. The CLI resolves `DAGR_*` here and passes
// the already-parsed values inward (`RunConfig` builder methods and the pool pins).
// The pattern a pipeline binary folds into its own `main` — parse each flag as it
// does today, then fold in the env fallback before building `RunConfig` and the
// pool pins:

#[allow(dead_code)]
mod dagr_env_wiring {
    use std::time::Duration;

    use dagr_cli::config::{resolve_headroom, resolve_pool_pins, EnvParseError, PoolPinFlags};
    use dagr_cli::driver::RunConfig;
    use dagr_core::flow::FailureMode;
    use dagr_core::limits::ContainerLimitProbe;

    /// The runtime-flag values a binary has already parsed from its argv (each
    /// `None` when the operator supplied no flag). Your binary parses these however
    /// it likes; this function does the `flag > env > default` fold.
    #[derive(Clone, Copy, Default)]
    pub struct ParsedFlags {
        pub grace: Option<Duration>,
        pub teardown_deadline: Option<Duration>,
        pub failure_mode: Option<FailureMode>,
        pub headroom: Option<f64>,
        pub pool: PoolPinFlags,
    }

    /// Fold the parsed flags with the `DAGR_*` env fallbacks into a fully-resolved
    /// [`RunConfig`] and sized pool capacities. A bad env value surfaces as an
    /// [`EnvParseError`] whose `exit_code()` is the code to exit with (an
    /// unparseable value → `InvalidUsage`, an out-of-range headroom →
    /// `BootstrapFailure`) — the binary maps it straight to a process exit.
    pub fn resolve_config(base: &str, flags: ParsedFlags) -> Result<RunConfig, EnvParseError> {
        let ParsedFlags {
            grace,
            teardown_deadline,
            failure_mode,
            headroom,
            pool,
        } = flags;

        // 1. The pool pins (DAGR_POOL_*) and headroom (DAGR_HEADROOM), resolved in
        //    the CLI and handed to the core probe — dagr-core reads no env.
        let pins = resolve_pool_pins(pool)?;
        let headroom = resolve_headroom(headroom)?;
        let capacities = ContainerLimitProbe::from_host()
            .with_pins(pins)
            .with_headroom(headroom)
            .detect()
            .expect("sizing never fails; the too-big-node check runs at drive time");

        // 2. The driver knobs, each via the opt-in fallible env-fallback builder.
        Ok(RunConfig::new(base)
            .grace_from_env(grace)?
            .teardown_deadline_from_env(teardown_deadline)?
            .failure_mode_from_env(failure_mode)?
            .capacities(capacities))
    }
}
