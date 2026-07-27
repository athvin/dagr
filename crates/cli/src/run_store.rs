//! **Run-store defaults** — the local-file event sink, monotonic clocks, default
//! store base, and run-id minter a pipeline binary needs to record a run, promoted
//! to a reusable public surface.
//!
//! # Why this module exists
//!
//! Driving a flow to a durable event stream needs three things beyond the flow
//! itself: an [`EventSink`] to append records to, a [`MonotonicClock`] for the
//! artifact's durations, and a store path + run id to write under. Every one of
//! those has an obvious default, but until now the only implementation lived
//! **privately** inside the registry ([`crate::registry`]) and was copy-pasted into
//! the quickstart and a test. This module is the single home for those defaults, so
//! the registry, the `#[dag]` run path (the `run` module), the one-call
//! [`RunnableFlow::run_to_store`](crate::run_flow::RunnableFlow::run_to_store), and
//! any hand-written driver all agree — and no one hand-writes a `FileSink` again.
//!
//! # What it provides
//!
//! - [`FileSink`] — an append-only local-file [`EventSink`] writing a run's
//!   `events.jsonl`, with [`create_in_store`](FileSink::create_in_store) owning the
//!   `<base>/<pipeline>/<run-id>/events.jsonl` path convention in one place.
//! - [`SystemClock`] — a wall-clock-derived monotonic clock (the **default for a
//!   real run**, so artifact durations reflect real elapsed time).
//! - [`TickClock`] — a deterministic per-read counter (for reproducible byte-for-byte
//!   streams in tests and fixtures).
//! - [`DEFAULT_STORE_BASE`] — the default run-store base (`./dagr-runs`).
//! - [`mint_run_id`] — a dependency-free run-id minter guaranteeing disjoint store
//!   directories across concurrent invocations.
//!
//! It depends only on `std` and [`dagr_artifact::event_stream`], so it is present in
//! every build configuration (no `dag`/`inventory` feature gate).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, EVENTS_FILE_NAME};

/// The default run-store base when none is supplied — a relative directory under the
/// current working directory. A real deployment points the base at durable storage
/// (via `--store` or an explicit argument); this default keeps the ergonomic
/// one-flow case runnable with no flag.
pub const DEFAULT_STORE_BASE: &str = "./dagr-runs";

/// A minimal **append-only local-file event sink**: appends each complete record
/// line to the run's `events.jsonl` and flushes it to the OS.
///
/// This is the default sink every registry-driven run and the one-call
/// [`RunnableFlow::run_to_store`](crate::run_flow::RunnableFlow::run_to_store) use; a
/// hand-written driver can use it too instead of reimplementing [`EventSink`]. A real
/// deployment may substitute a sink that writes to durable or networked storage.
pub struct FileSink {
    file: File,
}

impl FileSink {
    /// Open the `events.jsonl` at `path` for append, creating its parent directories.
    ///
    /// # Errors
    /// Returns the [`io::Error`] if the parent directories cannot be created or the
    /// file cannot be opened for append (e.g. an unwritable store).
    pub fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }

    /// Open the conventional stream path
    /// `<base>/<pipeline>/<run_id>/events.jsonl`, creating its parent directories.
    ///
    /// This owns the run-store path convention in one place, so the one-call run
    /// path, the registry, and any hand-written driver agree on where a run's stream
    /// lives.
    ///
    /// # Errors
    /// Returns the [`io::Error`] if the store directory cannot be created or the
    /// stream file cannot be opened for append.
    pub fn create_in_store(base: &str, pipeline: &str, run_id: &str) -> io::Result<Self> {
        let path = Path::new(base)
            .join(pipeline)
            .join(run_id)
            .join(EVENTS_FILE_NAME);
        Self::create(&path)
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

/// A **wall-clock-derived monotonic clock**: [`elapsed_ns`](MonotonicClock::elapsed_ns)
/// returns nanoseconds since the clock was constructed, from [`Instant`].
///
/// This is the **default clock for a real run**, so the durations recorded in the
/// artifact reflect real elapsed time. [`Instant`] is monotonic (unlike
/// `SystemTime`, which NTP can step backward), so the [`MonotonicClock`]
/// non-decreasing contract holds by construction; the `as u64` truncation is safe
/// for ~584 years of runtime. For deterministic, reproducible streams use
/// [`TickClock`] instead.
pub struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// A clock whose zero is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for SystemClock {
    fn elapsed_ns(&self) -> u64 {
        // `u128` nanos to `u64`, saturating at `u64::MAX`. The saturation point is
        // ~584 years of runtime, so it never trips for a real run; saturating (rather
        // than wrapping) keeps the clock monotonic even in that impossible case.
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// A **deterministic monotonic clock** advanced one tick per read — wall-clock-free.
///
/// Every [`elapsed_ns`](MonotonicClock::elapsed_ns) returns the next integer, so a
/// run driven by it produces a byte-for-byte reproducible event stream regardless of
/// wall time. The registry drives runs with this (so its streams stay stable across
/// machines); tests and cross-toolchain fixtures use it for the same reason. A real
/// run wanting true durations uses [`SystemClock`] instead.
#[derive(Default)]
pub struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// Mint a **run identity** guaranteeing disjoint store directories across
/// invocations. A wall-clock timestamp, the process id, and a process-global
/// monotonic counter together keep every invocation's store directory disjoint —
/// even two invocations within the same process and the same clock tick get distinct
/// ids (the counter breaks the tie) — without an external UUID dependency.
#[must_use]
pub fn mint_run_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    format!("run-{}-{nanos}-{seq}", std::process::id())
}
