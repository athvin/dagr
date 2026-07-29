//! The **run-sink tee** that composes the on-disk `events.jsonl`
//! [`FileSink`](crate::run_store::FileSink) with the guaranteed live
//! [`dagr_metastore::MetastoreSink`] (T86) — behind the default-off `metastore`
//! cargo feature.
//!
//! # Why this module exists
//!
//! dagr writes its event stream through a **single injected
//! [`EventSink`](dagr_artifact::event_stream::EventSink)**,
//! constructed once at the driver's `EventStreamWriter::new` injection point. To
//! also project a run into the metastore **as it executes**, we tee that one sink:
//! every record goes to the real on-disk `events.jsonl` (the resume source of
//! truth, unchanged) **and** to the live `MetastoreSink`, which upserts the
//! incremental rows via the T84 mapping + T83 write discipline. The JSONL stream is
//! not perturbed — the metastore is a second consumer of the same bytes, never a
//! rewriter of them.
//!
//! # The toggle (T76 precedence)
//!
//! The tee is wired only when the operator turns it on, via the standard
//! `flag > env > default` precedence
//! ([`resolve_metastore_toggle`](crate::config::resolve_metastore_toggle),
//! `--dagr.metastore` / `DAGR_METASTORE`, default **off**). Off ⇒ the run sink is a
//! plain [`FileSink`](crate::run_store::FileSink) and there is **no** `libsql`
//! activity and no behavior change.
//! The whole module is compiled only under `--features metastore`, so a default
//! build (and `--no-default-features`) omits the wiring entirely.
//!
//! # Guaranteed, not best-effort
//!
//! The `MetastoreSink` returns an [`std::io::Error`] on a durable-write failure, which
//! the event-stream writer folds into a `SinkFault` and the driver folds into its
//! shutdown exit-code precedence — so a metastore write is as durable as an
//! event-stream write, and a metastore fault surfaces as the distinct sink-failure
//! exit code (never swallowed), exactly as a `FileSink` fault does.

use std::ffi::OsString;
use std::io;
use std::path::PathBuf;

use dagr_artifact::event_stream::{EVENTS_FILE_NAME, EventSink};
use dagr_metastore::MetastoreSink;

use crate::run_store::FileSink;

/// The library-owned flag turning the live metastore tee on for a `run <flow>`
/// invocation (the reserved `--dagr.metastore`; a bare presence means "on", or an
/// explicit `--dagr.metastore=<bool>` value is honored).
const METASTORE_FLAG: &str = "--dagr.metastore";

/// The library-owned flag naming the metastore store path (the reserved
/// `--dagr.metastore-store`); default `<base>/metastore.db` under the run store.
const METASTORE_STORE_FLAG: &str = "--dagr.metastore-store";

/// The run sink the driver writes through: either the plain on-disk [`FileSink`]
/// (the toggle off, the unchanged default) or a **tee** of the `FileSink` and the
/// live [`MetastoreSink`] (the toggle on). Both are one [`EventSink`], so the
/// generic driver takes either without a signature change.
pub enum RunSink {
    /// The toggle is off: just the on-disk `events.jsonl` sink (no `libsql`).
    File(FileSink),
    /// The toggle is on: the on-disk sink teed with the live metastore projector.
    Tee {
        /// The real `events.jsonl` sink (the resume source of truth).
        file: FileSink,
        /// The guaranteed live metastore projector.
        metastore: MetastoreSink,
    },
}

impl EventSink for RunSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        match self {
            RunSink::File(f) => f.append_line(line),
            RunSink::Tee { file, metastore } => {
                // Write the on-disk stream first (the resume source of truth), then
                // project into the metastore. A metastore fault surfaces here and
                // the writer folds it into a SinkFault — guaranteed, not swallowed.
                file.append_line(line)?;
                metastore.append_line(line)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            RunSink::File(f) => f.flush(),
            RunSink::Tee { file, metastore } => {
                file.flush()?;
                metastore.flush()
            }
        }
    }
}

/// Build the run sink for a `run <flow>` invocation, honoring the metastore toggle.
///
/// `stream_path` is the run's resolved `events.jsonl` path; `base` is the run-store
/// base and `argv` the invocation's raw args (for the `--dagr.metastore` /
/// `--dagr.metastore-store` flags). When the toggle is on, the live `MetastoreSink`
/// is opened at the resolved store path (default `<base>/metastore.db`) and teed
/// with the `FileSink`; when off, a plain `FileSink` is returned and no `libsql`
/// activity occurs.
///
/// # Errors
/// - The [`io::Error`] if the on-disk stream cannot be opened for append.
/// - An [`io::Error`] wrapping the metastore open failure (an unusable store),
///   surfaced **before** the run starts — a run never begins against an index it
///   cannot write, keeping the guarantee.
/// - An [`io::Error`] (`InvalidInput`) if the `--dagr.metastore` value is not a
///   recognized boolean (a bad toggle value fails loudly, never silently off).
pub fn build_run_sink(
    stream_path: &std::path::Path,
    base: &str,
    argv: &[OsString],
) -> io::Result<RunSink> {
    let file = FileSink::create(stream_path)?;

    let flag = parse_metastore_flag(argv)
        .map_err(|msg| io::Error::new(io::ErrorKind::InvalidInput, msg))?;
    let on = crate::config::resolve_metastore_toggle(flag)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    if !on {
        return Ok(RunSink::File(file));
    }

    let store_path = resolve_store_path(base, argv);
    let events_path = stream_path.to_string_lossy().into_owned();
    let metastore = MetastoreSink::open(store_path, Some(events_path))
        .map_err(|e| io::Error::other(format!("cannot open the live metastore index: {e}")))?;
    Ok(RunSink::Tee { file, metastore })
}

/// The default metastore store path under a run store: `<base>/metastore.db` (so the
/// index sits alongside the runs it indexes), matching
/// [`crate::metastore::default_metastore_path`]'s convention.
fn resolve_store_path(base: &str, argv: &[OsString]) -> PathBuf {
    if let Some(path) = flag_value(argv, METASTORE_STORE_FLAG) {
        return PathBuf::from(path);
    }
    PathBuf::from(base).join("metastore.db")
}

/// The stream path a `run <flow>` invocation writes under:
/// `<base>/<pipeline>/<run-id>/events.jsonl`.
#[must_use]
pub fn stream_path(base: &str, pipeline: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base)
        .join(pipeline)
        .join(run_id)
        .join(EVENTS_FILE_NAME)
}

/// Parse the `--dagr.metastore` toggle flag from `argv`: absent → `None` (fall back
/// to env/default), a bare `--dagr.metastore` → `Some(true)`, and
/// `--dagr.metastore=<bool>` / `--dagr.metastore <bool>` → the parsed boolean. A
/// non-boolean value is an error (fails loudly).
fn parse_metastore_flag(argv: &[OsString]) -> Result<Option<bool>, String> {
    let mut it = argv.iter().peekable();
    while let Some(arg) = it.next() {
        let Some(s) = arg.to_str() else { continue };
        if s == METASTORE_FLAG {
            // A following value that parses as a bool consumes it; otherwise the
            // bare flag means "on".
            if let Some(next) = it.peek().and_then(|a| a.to_str()) {
                if let Ok(b) = next.parse::<crate::config::EnvBool>() {
                    return Ok(Some(b.into_inner()));
                }
            }
            return Ok(Some(true));
        }
        if let Some(value) = s.strip_prefix("--dagr.metastore=") {
            return value
                .parse::<crate::config::EnvBool>()
                .map(|b| Some(b.into_inner()))
                .map_err(|e| format!("`{METASTORE_FLAG}={value}` {e}"));
        }
    }
    Ok(None)
}

/// The value of a `--flag <value>` / `--flag=<value>` from `argv`, or `None`.
fn flag_value(argv: &[OsString], flag: &str) -> Option<String> {
    let mut it = argv.iter();
    let eq_prefix = format!("{flag}=");
    while let Some(arg) = it.next() {
        let Some(s) = arg.to_str() else { continue };
        if s == flag {
            return it.next().map(|v| v.to_string_lossy().into_owned());
        }
        if let Some(value) = s.strip_prefix(&eq_prefix) {
            return Some(value.to_string());
        }
    }
    None
}
