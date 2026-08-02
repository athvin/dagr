//! The **submission log** (M10, T108, ADR 115 §9): the seam that lets a
//! write-ahead record become durable *before* the work it describes exists,
//! without giving the run a second event-stream writer.
//!
//! # The problem this exists to solve
//!
//! `NodeRunner::run` emits through a **buffering** `AttemptEventSink`, and the
//! driver drains that buffer into the authoritative writer only once the attempt
//! returns. That is exactly right for an attempt's own records — it is what keeps
//! the writer single-owner while a node runs on a task worker — and exactly wrong
//! for a record whose entire value is being durable *before* a pod is created.
//! Recording after submission would lose precisely the crash window ADR 115 §9
//! introduced the record to cover.
//!
//! Writing it through a second `EventStreamWriter` is not available either: two
//! writers over one file means two sequence counters, and `seq` would stop being
//! gapless — a property C19 states as an acceptance criterion and the fold relies
//! on.
//!
//! # The resolution: the log is the run's sequence authority
//!
//! `SubmissionLog` wraps the run's `EventSink` and owns the counter. Lines the
//! driver's writer produces pass through **re-stamped** with the log's own `seq`;
//! a submission record the executor writes directly takes the next number and is
//! flushed immediately. One process, one mutex, one counter: the orchestrator is
//! still the single writer, `seq` is still gapless and strictly increasing, and the
//! driver is untouched.
//!
//! Re-stamping is a no-op when the number is unchanged — the record is re-emitted
//! through the *same* canonicalization that produced it — so a run with no
//! submissions is **byte-identical** to one written straight to the sink. That is
//! asserted, because every byte-identity guarantee in the repo would otherwise rest
//! on an unexamined round trip.
//!
//! The log is installed **only** under a remote executor. A local run never sees
//! it, which is the other half of why a local stream stays byte-identical to a
//! pre-M10 one.

use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dagr_artifact::event_stream::{
    AttemptSubmittedRecord, EventSink, EventStreamWriter, MonotonicClock, RunId,
};

/// The shared, mutex-guarded state: the real sink and the one sequence counter
/// every record in the run passes through.
struct LogState {
    sink: Box<dyn EventSink>,
    next_seq: u64,
}

impl LogState {
    /// Append `line` with `seq` re-stamped to this log's next number.
    fn append_restamped(&mut self, line: &[u8]) -> io::Result<()> {
        let seq = self.next_seq;
        let restamped = restamp_seq(line, seq)?;
        self.sink.append_line(&restamped)?;
        // Only advance after a successful append, exactly as the writer does: a
        // faulted record must leave no gap.
        self.next_seq += 1;
        Ok(())
    }
}

/// Re-serialize `line` with its `seq` set to `seq`.
///
/// The round trip goes through `dagr_artifact::canonical`, the same writer that
/// produced the line, so the output is byte-identical whenever the number is
/// unchanged — sorted keys, compact separators, identical escaping.
fn restamp_seq(line: &[u8], seq: u64) -> io::Result<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(line)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let Some(object) = value.as_object_mut() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "an event-stream record is a JSON object",
        ));
    };
    object.insert("seq".to_string(), serde_json::Value::from(seq));
    let mut out = dagr_artifact::canonical::to_canonical_string(&value).into_bytes();
    out.push(b'\n');
    Ok(out)
}

/// A plain append-and-flush file sink, for the `open` convenience constructor.
struct FileSink {
    file: std::fs::File,
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

/// The run's sequence authority: the sink the driver writes through, and the handle
/// the remote executor records its write-ahead intent through.
#[derive(Clone)]
pub struct SubmissionLog {
    state: Arc<Mutex<LogState>>,
    run_id: String,
    pipeline: String,
    started: Instant,
}

impl std::fmt::Debug for SubmissionLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmissionLog")
            .field("run_id", &self.run_id)
            .field("pipeline", &self.pipeline)
            .finish_non_exhaustive()
    }
}

impl SubmissionLog {
    /// Wrap an existing [`EventSink`].
    #[must_use]
    pub fn over<S: EventSink + 'static>(
        sink: S,
        run_id: impl Into<String>,
        pipeline: impl Into<String>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(LogState {
                sink: Box::new(sink),
                next_seq: 0,
            })),
            run_id: run_id.into(),
            pipeline: pipeline.into(),
            started: Instant::now(),
        }
    }

    /// Open (or create) the run's event-stream file and wrap it.
    ///
    /// # Errors
    ///
    /// The underlying [`io::Error`] when the path cannot be opened for appending.
    pub fn open(
        path: &Path,
        run_id: impl Into<String>,
        pipeline: impl Into<String>,
    ) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self::over(FileSink { file }, run_id, pipeline))
    }

    /// The [`EventSink`] to hand the driver. Every line it accepts is re-stamped
    /// with this log's sequence number and forwarded.
    #[must_use]
    pub fn sink(&self) -> SubmissionLogSink {
        SubmissionLogSink {
            state: Arc::clone(&self.state),
        }
    }

    /// The handle a remote executor records its write-ahead intent through.
    #[must_use]
    pub fn handle(&self) -> SubmissionHandle {
        SubmissionHandle {
            state: Arc::clone(&self.state),
            run_id: self.run_id.clone(),
            pipeline: self.pipeline.clone(),
            started: self.started,
        }
    }
}

/// The driver-facing half: an [`EventSink`] that re-stamps and forwards.
#[derive(Clone)]
pub struct SubmissionLogSink {
    state: Arc<Mutex<LogState>>,
}

impl std::fmt::Debug for SubmissionLogSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmissionLogSink").finish_non_exhaustive()
    }
}

impl EventSink for SubmissionLogSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        // Poison policy: propagate rather than panic. A poisoned log means a panic
        // left the stream half-written, and the run's sink-fault path already knows
        // how to turn an unwritable stream into a truthful cancellation.
        let mut guard = self
            .state
            .lock()
            .map_err(|_| io::Error::other("submission log mutex poisoned"))?;
        guard.append_restamped(line)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| io::Error::other("submission log mutex poisoned"))?;
        guard.sink.flush()
    }
}

/// The executor-facing half: write one `attempt-submitted` record and make it
/// durable, now.
#[derive(Clone)]
pub struct SubmissionHandle {
    state: Arc<Mutex<LogState>>,
    run_id: String,
    pipeline: String,
    started: Instant,
}

impl std::fmt::Debug for SubmissionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmissionHandle")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// A clock that reports one fixed offset — the offset captured when the record was
/// built, so the envelope's `offset_ns` is a claim about when the submission
/// happened rather than about when the line was serialized.
struct FixedOffset(u64);

impl MonotonicClock for FixedOffset {
    fn elapsed_ns(&self) -> u64 {
        self.0
    }
}

/// Collects the single line the throwaway writer produces. Shared rather than
/// owned so the line can be read back after the writer has consumed the sink.
#[derive(Clone, Default)]
struct OneLine(Arc<Mutex<Vec<u8>>>);

impl EventSink for OneLine {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("one-line sink mutex poisoned"))?
            .extend_from_slice(line);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SubmissionHandle {
    /// Write `record` into the run's stream and flush it.
    ///
    /// Returns only once the bytes have been handed to the sink and the sink has
    /// been flushed — which is what makes "durably flushed **before** the pod-create
    /// call" a fact about the ordering of two statements rather than a hope.
    ///
    /// # Errors
    ///
    /// The underlying [`io::Error`] when the record could not be serialized or the
    /// sink could not accept it.
    pub fn record(&self, record: AttemptSubmittedRecord) -> io::Result<()> {
        // Build the canonical line through the REAL writer, so the record's wire
        // form has exactly one producer and cannot drift from the published schema.
        // The `seq` it stamps is discarded; the log owns that number.
        let offset_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let capture = OneLine::default();
        let mut writer = EventStreamWriter::new(
            capture.clone(),
            FixedOffset(offset_ns),
            RunId::from_operator(self.run_id.clone()),
            self.pipeline.clone(),
        );
        writer
            .attempt_submitted(record)
            .map_err(|fault| io::Error::other(fault.reason))?;
        let line = capture
            .0
            .lock()
            .map_err(|_| io::Error::other("one-line sink mutex poisoned"))?
            .clone();

        let mut guard = self
            .state
            .lock()
            .map_err(|_| io::Error::other("submission log mutex poisoned"))?;
        guard.append_restamped(&line)?;
        guard.sink.flush()
    }
}
