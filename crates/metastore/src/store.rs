//! The [`MetaStore`] connection seam and the [`MetaStore::with_write_txn`] write
//! discipline.
//!
//! `MetaStore::open(mode)` converges every open mode on one
//! [`libsql::Connection`], sets the ADR 097 §3 pragmas (`journal_mode=WAL`,
//! `synchronous=NORMAL`, a `busy_timeout`), and applies the ordered idempotent
//! [`crate::schema::migrations`]. `with_write_txn` is the one place the write
//! discipline lives: `BEGIN IMMEDIATE` + a bounded `SQLITE_BUSY` retry with
//! exponential backoff + jitter.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

/// How to open the store. Only [`OpenMode::LocalFile`] is implemented in T83; the
/// other modes are **recognized stubs** (documented, not implemented) reserved
/// behind this seam per ADR 097 §5 and ticket-conventions §7 — `open` returns a
/// clear "not implemented" error for them rather than pretending.
#[derive(Debug, Clone)]
pub enum OpenMode {
    /// The embedded, same-host local-file store — the only mode this ticket
    /// implements. Many run-processes may open the same path concurrently
    /// (libSQL multi-process WAL).
    LocalFile(PathBuf),
    /// **Recognized stub (ADR 097 §5, not implemented here).** A remote `sqld`
    /// server the store would connect to. Reserved behind this seam; `open`
    /// returns [`OpenError::ModeNotImplemented`].
    RemoteSqld {
        /// The `sqld` server URL (unused until the mode is implemented).
        url: String,
        /// The auth token (unused until the mode is implemented).
        auth_token: String,
    },
    /// **Recognized stub (ADR 097 §5, not implemented here).** An embedded
    /// replica synced against a remote primary. Reserved behind this seam; `open`
    /// returns [`OpenError::ModeNotImplemented`].
    SyncedReplica {
        /// The local replica file (unused until the mode is implemented).
        path: PathBuf,
        /// The primary's URL (unused until the mode is implemented).
        url: String,
        /// The auth token (unused until the mode is implemented).
        auth_token: String,
    },
}

/// An error opening the store or applying migrations.
#[derive(Debug)]
pub enum OpenError {
    /// A `libsql` error opening the database, setting a pragma, or applying a
    /// migration.
    Libsql(libsql::Error),
    /// The requested [`OpenMode`] is a recognized stub not implemented in T83
    /// (`RemoteSqld` / `SyncedReplica`, ADR 097 §5). Names the mode.
    ModeNotImplemented(&'static str),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Libsql(e) => write!(f, "libsql error opening the metastore: {e}"),
            OpenError::ModeNotImplemented(mode) => write!(
                f,
                "metastore open mode `{mode}` is a recognized stub reserved behind the \
                 MetaStore::open seam (ADR 097 §5) and is not implemented in this build"
            ),
        }
    }
}

impl std::error::Error for OpenError {}

impl From<libsql::Error> for OpenError {
    fn from(e: libsql::Error) -> Self {
        OpenError::Libsql(e)
    }
}

/// An error from a [`MetaStore::with_write_txn`] write transaction.
#[derive(Debug)]
pub enum WriteError {
    /// A `libsql` error the caller's closure or the commit produced that is **not**
    /// a retryable `SQLITE_BUSY`.
    Libsql(libsql::Error),
    /// The write kept hitting `SQLITE_BUSY` and the bounded retry budget was
    /// exhausted. A **hard** error (never a silent drop): carries how many
    /// attempts were made.
    BusyRetriesExhausted {
        /// The number of attempts made before giving up.
        attempts: u32,
    },
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Libsql(e) => write!(f, "metastore write transaction failed: {e}"),
            WriteError::BusyRetriesExhausted { attempts } => write!(
                f,
                "metastore write transaction still SQLITE_BUSY after {attempts} attempts \
                 (bounded retry cap exhausted)"
            ),
        }
    }
}

impl std::error::Error for WriteError {}

/// The bounded-retry schedule for [`MetaStore::with_write_txn`]: how many times a
/// `SQLITE_BUSY` write is retried and the backoff shape. Exposed so a test can
/// pin a tiny cap and prove the past-the-cap hard error.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts (including the first). `1` means no retry.
    pub max_attempts: u32,
    /// The base backoff before the first retry; doubled each retry (capped).
    pub base_backoff: Duration,
    /// The ceiling on any single backoff.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 8,
            base_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(200),
        }
    }
}

/// The busy-timeout applied on open (ADR 097 §3). Chosen conservatively; the
/// app-level bounded retry sits on top of it.
// (Wired into `MetaStore::open` in the implementation commit; unused in the
// tests-first skeleton whose bodies are `unimplemented!()`.)
#[allow(dead_code)]
const BUSY_TIMEOUT: Duration = Duration::from_millis(5_000);

/// A write-safe handle to the run index over one [`libsql::Connection`].
///
/// Open with [`MetaStore::open`]; write through [`MetaStore::with_write_txn`],
/// which is the only place `BEGIN IMMEDIATE` + the bounded `SQLITE_BUSY` retry
/// lives.
pub struct MetaStore {
    conn: libsql::Connection,
    // Kept alive so the database file handle outlives the connection.
    _db: libsql::Database,
    // Read in `with_write_txn` (implementation commit); unused in the skeleton.
    #[allow(dead_code)]
    retry: RetryPolicy,
}

impl MetaStore {
    /// Open the store for `mode`, set the ADR 097 §3 pragmas, and apply the
    /// ordered idempotent migrations. Only [`OpenMode::LocalFile`] is implemented
    /// here; the other modes return [`OpenError::ModeNotImplemented`].
    ///
    /// # Errors
    /// Returns [`OpenError`] if libSQL fails to open, a pragma fails, a migration
    /// fails, or a recognized-stub mode is requested.
    pub async fn open(mode: OpenMode) -> Result<Self, OpenError> {
        let _ = mode;
        unimplemented!("T83: MetaStore::open is not yet implemented")
    }

    /// Open the store with a caller-supplied [`RetryPolicy`] (used by tests to pin
    /// a tiny cap). Otherwise identical to [`MetaStore::open`].
    ///
    /// # Errors
    /// See [`MetaStore::open`].
    pub async fn open_with_retry(mode: OpenMode, retry: RetryPolicy) -> Result<Self, OpenError> {
        let _ = (mode, retry);
        unimplemented!("T83: MetaStore::open_with_retry is not yet implemented")
    }

    /// Run `f`'s statements inside a single write transaction opened with
    /// `BEGIN IMMEDIATE`, committing on success. On `SQLITE_BUSY`/`Busy` the
    /// whole closure is rolled back and retried with exponential backoff + jitter,
    /// up to the [`RetryPolicy`] cap; past the cap a hard
    /// [`WriteError::BusyRetriesExhausted`] is returned (never a silent drop).
    ///
    /// # Errors
    /// Returns [`WriteError`] if the closure or commit fails with a non-busy error,
    /// or the busy-retry budget is exhausted.
    pub async fn with_write_txn<F>(&self, f: F) -> Result<(), WriteError>
    where
        F: for<'a> Fn(
            &'a libsql::Connection,
        )
            -> Pin<Box<dyn Future<Output = Result<(), libsql::Error>> + 'a>>,
    {
        let _ = f;
        unimplemented!("T83: MetaStore::with_write_txn is not yet implemented")
    }

    /// The underlying connection, for read queries (the reader/writer tickets
    /// T84/T86 build on this). Writes must go through [`MetaStore::with_write_txn`].
    #[must_use]
    pub fn connection(&self) -> &libsql::Connection {
        &self.conn
    }
}
