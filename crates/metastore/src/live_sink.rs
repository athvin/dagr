//! The **guaranteed live tee sink** (T86) — a [`dagr_artifact::event_stream::EventSink`]
//! that projects a run into the metastore **as it executes**.
//!
//! # What this is
//!
//! dagr writes its event stream through an injected [`EventSink`] (append a line,
//! flush), constructed once at the driver's `EventStreamWriter::new` injection
//! point. [`MetastoreSink`] is an `EventSink` that, alongside the real on-disk
//! `events.jsonl` (the resume source of truth, teed separately in `dagr-cli`),
//! **incrementally projects each event into the run index** via the same T84
//! mapping ([`crate::mapping::sync_run_live`]) and the same T83 write discipline
//! ([`crate::MetaStore::with_write_txn`], `BEGIN IMMEDIATE` + bounded busy retry).
//! So an operator can query in-flight state — `dag_run(state='running')` at
//! run-started, `node_attempt` rows as attempts finish, and a terminal `dag_run`
//! at run-finished — not just history after the run ends.
//!
//! # Guaranteed, not best-effort (ADR 097)
//!
//! The sink is **not** best-effort: a durable-write failure is returned as an
//! [`io::Error`] from [`append_line`](EventSink::append_line) / [`flush`](EventSink::flush),
//! which the event-stream writer folds into a `SinkFault` and the driver folds
//! into its shutdown exit-code precedence — so a metastore write is as durable as
//! an event-stream write. Once a write fails the sink is **sticky-faulted**: it
//! surfaces the same fault from every later call (including the final `flush`), so
//! the driver's bounded final flush reliably reports the sink failure and the run
//! exits with the distinct sink-failure code — the failure is never swallowed.
//!
//! # How it maps incrementally
//!
//! `EventSink` is a **synchronous** two-operation port, while [`MetaStore`] is
//! async, so the sink owns a **dedicated worker thread** running a current-thread
//! tokio runtime that holds the store connection. Each `append_line` hands the
//! worker the growing raw byte buffer; the worker folds it via `fold_stream` and
//! re-projects it via [`sync_run_live`] under one
//! write txn (an idempotent UPSERT), then reports success/failure back over a
//! channel the caller blocks on — so a write failure surfaces **synchronously**,
//! preserving the guaranteed contract. Because the projection is a pure,
//! deterministic UPSERT of the folded stream, re-projecting the stream on each
//! event converges on exactly what a post-hoc `sync` of the finished stream
//! produces (**live == reconcile**).
//!
//! # What it does not do
//!
//! It never perturbs the event stream (it only reads the bytes it is handed and
//! writes rows); JSONL remains the resume source of truth. It maps only what the
//! event stream carries today — no lineage rows, no `durable_reference_meta`
//! (M8). A stream with no `run-started` yet (nothing to fold) is a no-op success.

use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread::JoinHandle;

use dagr_artifact::event_stream::{EventSink, read_records};
use dagr_artifact::fold::fold_stream;

use crate::MetaStore;
use crate::mapping::sync_run_live;
use crate::store::{OpenError, OpenMode, RetryPolicy};

/// One projection request handed to the worker thread: the full raw event-stream
/// bytes accumulated so far, and whether the stream has already carried its
/// `run-finished` record (so the worker knows whether to stamp `running`).
struct Request {
    /// The full raw JSONL bytes of the stream so far (every line appended to date).
    bytes: Vec<u8>,
    /// `true` once the `run-finished` record has been appended — the run is no
    /// longer in flight, so the folded terminal outcome is stamped.
    finished: bool,
    /// The channel the worker reports this projection's outcome back on.
    reply: mpsc::SyncSender<Result<(), String>>,
}

/// A guaranteed live [`EventSink`] projecting a run into the metastore as it runs.
///
/// Construct it with [`MetastoreSink::open`] (opens/creates the store at a path and
/// applies migrations) and tee it beside the real `events.jsonl` sink at the
/// driver's single `EventStreamWriter::new` injection point. See the
/// [module docs](self) for the guarantee and the incremental-projection model.
pub struct MetastoreSink {
    /// The accumulated raw bytes of every line appended so far — the buffer the
    /// worker folds and projects on each event.
    buffer: Vec<u8>,
    /// Whether a `run-finished` record has been appended (drives the `running`
    /// vs terminal `dag_run.state`).
    finished: bool,
    /// The sender onto the worker thread's request queue; `None` after the worker
    /// has been shut down (the sink was faulted or dropped).
    tx: Option<mpsc::Sender<Request>>,
    /// The worker thread handle, joined on drop.
    worker: Option<JoinHandle<()>>,
    /// The sticky fault: once a durable write fails, this holds the message and the
    /// sink surfaces it from every later call so the driver's exit-code precedence
    /// reliably reports the sink failure (the failure is never swallowed).
    fault: Option<String>,
}

impl MetastoreSink {
    /// Open (creating if absent) the local-file metastore at `path`, apply the
    /// ordered idempotent migrations, and start the projection worker. The returned
    /// sink is ready to tee at the driver's injection point.
    ///
    /// The `events_path` is recorded on `dag_run.events_path` so a row points back
    /// at the stream it was folded from (the same field `sync` records); pass the
    /// run's resolved `events.jsonl` path, or `None` if unknown.
    ///
    /// # Errors
    /// Returns an [`OpenError`] if the store cannot be opened, a pragma fails, or a
    /// migration fails — surfaced at construction so the run never starts against
    /// an unusable index.
    pub fn open(path: PathBuf, events_path: Option<String>) -> Result<Self, OpenError> {
        Self::open_with_retry(path, events_path, RetryPolicy::default())
    }

    /// [`open`](Self::open) with a caller-supplied [`RetryPolicy`] for the worker's
    /// store — the same seam [`MetaStore::open_with_retry`] exposes. A test pins a
    /// tiny cap so a genuine `SQLITE_BUSY` write contention surfaces the guaranteed
    /// hard fault deterministically (rather than being absorbed by the default
    /// bounded retry). Production callers use [`open`](Self::open).
    ///
    /// # Errors
    /// See [`open`](Self::open).
    pub fn open_with_retry(
        path: PathBuf,
        events_path: Option<String>,
        retry: RetryPolicy,
    ) -> Result<Self, OpenError> {
        // A one-slot handshake so the worker can report its open result before the
        // sink is considered constructed: opening (and migrating) the store must
        // fail loudly at construction, not silently on the first event.
        let (open_tx, open_rx) = mpsc::sync_channel::<Result<(), String>>(0);
        let (tx, rx) = mpsc::channel::<Request>();
        let worker = std::thread::Builder::new()
            .name("dagr-metastore-live-sink".to_string())
            .spawn(move || worker_main(path, events_path, retry, &open_tx, &rx))
            .map_err(|e| {
                OpenError::Libsql(libsql::Error::SqliteFailure(
                    1,
                    format!("could not spawn the metastore live-sink worker thread: {e}"),
                ))
            })?;

        match open_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                buffer: Vec::new(),
                finished: false,
                tx: Some(tx),
                worker: Some(worker),
                fault: None,
            }),
            Ok(Err(msg)) => {
                let _ = worker.join();
                Err(OpenError::Libsql(libsql::Error::SqliteFailure(1, msg)))
            }
            // The worker died before reporting — surface it as an open failure.
            Err(_) => {
                let _ = worker.join();
                Err(OpenError::Libsql(libsql::Error::SqliteFailure(
                    1,
                    "the metastore live-sink worker exited before reporting readiness".to_string(),
                )))
            }
        }
    }

    /// Project the current buffer through the worker and block on the result. On a
    /// durable-write failure the sink is sticky-faulted and the error is returned
    /// (and re-returned from every later call), so the driver's exit-code
    /// precedence reliably reports the sink failure.
    fn project(&mut self) -> io::Result<()> {
        // Already faulted: keep surfacing the same fault (never swallow it, never
        // attempt a further write on a poisoned worker).
        if let Some(msg) = &self.fault {
            return Err(io::Error::other(msg.clone()));
        }
        let Some(tx) = self.tx.as_ref() else {
            return Err(io::Error::other(
                "the metastore live-sink worker is not running",
            ));
        };
        let (reply_tx, reply_rx) = mpsc::sync_channel::<Result<(), String>>(0);
        let request = Request {
            bytes: self.buffer.clone(),
            finished: self.finished,
            reply: reply_tx,
        };
        if tx.send(request).is_err() {
            return Err(self.fault_with("the metastore live-sink worker has stopped".to_string()));
        }
        match reply_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(self.fault_with(msg)),
            Err(_) => Err(self.fault_with(
                "the metastore live-sink worker dropped a projection reply".to_string(),
            )),
        }
    }

    /// Record `msg` as the sticky fault and return it as an [`io::Error`].
    fn fault_with(&mut self, msg: String) -> io::Error {
        let err = io::Error::other(msg.clone());
        if self.fault.is_none() {
            self.fault = Some(msg);
        }
        err
    }
}

impl EventSink for MetastoreSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        // Accumulate the raw line (the sink never perturbs the bytes), then note
        // whether this record finishes the run so the projection stamps the right
        // dag_run.state.
        self.buffer.extend_from_slice(line);
        if line_is_run_finished(line) {
            self.finished = true;
        }
        self.project()
    }

    fn flush(&mut self) -> io::Result<()> {
        // A sticky fault must resurface here too: the driver's bounded final flush
        // is the reliable place a mid-run metastore fault turns into the
        // sink-failure exit code, so `flush` never reports success after a fault.
        if let Some(msg) = &self.fault {
            return Err(io::Error::other(msg.clone()));
        }
        // Nothing to make durable beyond what each write txn already committed
        // (each event is its own committed txn); re-project once more so a final
        // flush with no new event is still a consistent, up-to-date row set.
        self.project()
    }
}

impl Drop for MetastoreSink {
    fn drop(&mut self) {
        // Close the request channel so the worker's loop ends, then join it so the
        // store connection is dropped on its own thread (libsql/tokio resources are
        // torn down where they were created).
        self.tx = None;
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Whether a raw appended line is a `run-finished` record. The writer emits
/// canonical single-line JSON with the `kind` discriminator; a cheap byte scan for
/// the exact `"kind":"run-finished"` token avoids parsing every line twice (the
/// worker parses the full buffer anyway). Canonical bytes have no whitespace around
/// the colon, so the token match is exact.
fn line_is_run_finished(line: &[u8]) -> bool {
    contains(line, br#""kind":"run-finished""#)
}

/// A tiny substring scan over bytes (no `str` conversion, no dependency).
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return needle.is_empty();
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// The worker thread's body: open the store on a current-thread runtime, report
/// readiness, then serve each projection request by folding the buffer and
/// upserting it via [`sync_run_live`]. The store connection lives entirely on this
/// thread, so its libsql/tokio resources are created and dropped here.
#[expect(
    clippy::needless_pass_by_value,
    reason = "path/events_path/retry are owned for the whole thread lifetime — the \
              worker is `spawn`ed with a `move` closure and holds them until the run \
              ends; a borrow would not outlive the caller that spawned the thread"
)]
fn worker_main(
    path: PathBuf,
    events_path: Option<String>,
    retry: RetryPolicy,
    open_tx: &mpsc::SyncSender<Result<(), String>>,
    rx: &mpsc::Receiver<Request>,
) {
    // A current-thread runtime is enough: the sink drives one projection at a time
    // (each `append_line` blocks on its reply), so there is no concurrency to
    // exploit — and it keeps the worker off the driver's framework runtime, so a
    // `block_on` here never nests inside the driver's own runtime.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = open_tx.send(Err(format!(
                "could not build the metastore worker runtime: {e}"
            )));
            return;
        }
    };

    let store = match rt.block_on(MetaStore::open_with_retry(OpenMode::LocalFile(path), retry)) {
        Ok(store) => store,
        Err(e) => {
            let _ = open_tx.send(Err(e.to_string()));
            return;
        }
    };
    // Ready: the store is open and migrated.
    if open_tx.send(Ok(())).is_err() {
        return;
    }

    // Serve requests until the sink drops the sender (channel closed).
    while let Ok(request) = rx.recv() {
        let result = rt.block_on(project_once(
            &store,
            &request.bytes,
            events_path.as_deref(),
            request.finished,
        ));
        // A closed reply channel means the sink gave up waiting (it faulted): stop
        // serving, the sink owns the fault.
        if request.reply.send(result).is_err() {
            break;
        }
    }
}

/// Fold the accumulated bytes and upsert them via [`sync_run_live`]. A stream with
/// no `run-started` yet (nothing to fold) is a no-op success — there is simply no
/// row to write until the run identifies itself.
async fn project_once(
    store: &MetaStore,
    bytes: &[u8],
    events_path: Option<&str>,
    finished: bool,
) -> Result<(), String> {
    // If the buffer has no complete records yet (or no run-started), there is
    // nothing to project — a no-op success, not a fault.
    match read_records(bytes) {
        Ok(read) if read.records.is_empty() => return Ok(()),
        Ok(_) => {}
        // A corrupt non-final record should never happen (we fold our own writer's
        // canonical bytes); treat it as no-op rather than faulting the run over a
        // read quirk in a partial buffer.
        Err(_) => return Ok(()),
    }
    // No run-started yet ⇒ nothing to project (a no-op success).
    let Ok(artifact) = fold_stream(bytes, &[]) else {
        return Ok(());
    };
    sync_run_live(store, &artifact, events_path, !finished)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}
