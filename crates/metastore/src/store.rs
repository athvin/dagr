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
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The `SQLite` primary result codes that mean "retry later": `SQLITE_BUSY` (5)
/// and `SQLITE_LOCKED` (6). libSQL surfaces them as
/// [`libsql::Error::SqliteFailure`] with the primary code in the first field.
const SQLITE_BUSY: i32 = 5;
const SQLITE_LOCKED: i32 = 6;

/// Whether a [`libsql::Error`] is a retryable busy/locked failure (the only class
/// [`MetaStore::with_write_txn`] retries; everything else is surfaced immediately).
fn is_busy(err: &libsql::Error) -> bool {
    match err {
        // The primary result code is the low 8 bits; libSQL sometimes reports the
        // extended code, so mask before comparing.
        libsql::Error::SqliteFailure(code, _) => {
            let primary = code & 0xff;
            primary == SQLITE_BUSY || primary == SQLITE_LOCKED
        }
        _ => false,
    }
}

/// A tiny, dependency-free PRNG (`SplitMix64`) yielding a jitter fraction in
/// `0..=JITTER_DENOM` — the metastore crate keeps its dependency set to
/// `dagr-artifact` + `libsql` + `tokio`, so it does not pull `rand` just to jitter
/// a sleep. Seeded per call from the wall clock; jitter needs to be
/// *decorrelated*, not cryptographic. Integer-only so it triggers no float-cast
/// lints.
const JITTER_DENOM: u64 = 1 << 20;
fn jitter_numer(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Map to a fraction numerator in `0..=JITTER_DENOM` (inclusive upper bound so
    // full jitter can reach the whole capped delay).
    z % (JITTER_DENOM + 1)
}

/// A write-safe handle to the run index over one [`libsql::Connection`].
///
/// Open with [`MetaStore::open`]; write through [`MetaStore::with_write_txn`],
/// which is the only place `BEGIN IMMEDIATE` + the bounded `SQLITE_BUSY` retry
/// lives.
pub struct MetaStore {
    conn: libsql::Connection,
    // Kept alive so the database file handle outlives the connection.
    _db: libsql::Database,
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
        Self::open_with_retry(mode, RetryPolicy::default()).await
    }

    /// Open the store with a caller-supplied [`RetryPolicy`] (used by tests to pin
    /// a tiny cap). Otherwise identical to [`MetaStore::open`].
    ///
    /// # Errors
    /// See [`MetaStore::open`].
    pub async fn open_with_retry(mode: OpenMode, retry: RetryPolicy) -> Result<Self, OpenError> {
        let path = match mode {
            OpenMode::LocalFile(path) => path,
            OpenMode::RemoteSqld { .. } => {
                return Err(OpenError::ModeNotImplemented("RemoteSqld"));
            }
            OpenMode::SyncedReplica { .. } => {
                return Err(OpenError::ModeNotImplemented("SyncedReplica"));
            }
        };

        // Open the embedded local file (libSQL multi-process WAL). `new_local`
        // creates the file if it does not exist.
        let db = libsql::Builder::new_local(&path).build().await?;
        let conn = db.connect()?;

        // ADR 097 §3 pragmas, in order. `busy_timeout` is a per-connection setting
        // that the app-level bounded retry (with_write_txn) sits on top of.
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // WAL: many readers, one writer at a time; multi-process on one file.
        set_pragma(&conn, "PRAGMA journal_mode=WAL").await?;
        // synchronous=NORMAL is the WAL-recommended durability/throughput point.
        conn.execute("PRAGMA synchronous=NORMAL", ()).await?;

        let store = MetaStore {
            conn,
            _db: db,
            retry,
        };
        store.apply_migrations().await?;
        Ok(store)
    }

    /// Apply the ordered, idempotent [`crate::schema::migrations`]. Each is a
    /// `CREATE … IF NOT EXISTS`, so this is a no-op on an already-initialized store.
    /// Runs under [`MetaStore::with_write_txn`] so the whole migration set commits
    /// atomically under `BEGIN IMMEDIATE` + the bounded busy retry.
    ///
    /// After the `CREATE`s, the additive T84 columns
    /// ([`crate::schema::ADDITIVE_COLUMNS`]) are applied `ALTER TABLE … ADD COLUMN`
    /// **only when absent** (`SQLite` has no `ADD COLUMN IF NOT EXISTS`), so a
    /// pre-existing T83 store converges on the widened shape idempotently and
    /// additively.
    async fn apply_migrations(&self) -> Result<(), OpenError> {
        let statements = crate::schema::migrations();
        self.with_write_txn(move |conn| {
            let statements = statements.clone();
            Box::pin(async move {
                for stmt in &statements {
                    conn.execute(stmt, ()).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(map_migration_error)?;

        // Idempotent additive columns for a store first created under T83. On a
        // fresh store the widened `CREATE TABLE` already has them, so every
        // existence check finds the column present and no `ALTER` is issued.
        self.apply_additive_columns()
            .await
            .map_err(map_migration_error)
    }

    /// Add each of [`crate::schema::ADDITIVE_COLUMNS`] that is not already present,
    /// under one write transaction. Column presence is probed via
    /// `PRAGMA table_info`; a present column is skipped, so the whole step is a
    /// no-op on a store that already carries the T84 shape.
    async fn apply_additive_columns(&self) -> Result<(), WriteError> {
        // Compute the missing columns first (reads outside the write txn), then
        // ALTER just those inside one BEGIN IMMEDIATE.
        let mut missing: Vec<(&'static str, &'static str, &'static str)> = Vec::new();
        for &(table, column, decl) in crate::schema::ADDITIVE_COLUMNS {
            if !self.column_exists(table, column).await.unwrap_or(true) {
                missing.push((table, column, decl));
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        self.with_write_txn(move |conn| {
            let missing = missing.clone();
            Box::pin(async move {
                for (table, column, decl) in &missing {
                    conn.execute(
                        &format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"),
                        (),
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
    }

    /// Whether `column` already exists on `table` (`PRAGMA table_info` names every
    /// column in its `name` field).
    async fn column_exists(&self, table: &str, column: &str) -> Result<bool, libsql::Error> {
        let mut rows = self
            .conn
            .query(&format!("PRAGMA table_info({table})"), ())
            .await?;
        while let Some(row) = rows.next().await? {
            // table_info columns: (cid, name, type, notnull, dflt_value, pk).
            if let Ok(name) = row.get_str(1) {
                if name == column {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Run `f`'s statements inside a single write transaction opened with
    /// `BEGIN IMMEDIATE`, committing on success. On `SQLITE_BUSY`/`SQLITE_LOCKED`
    /// the whole closure is rolled back and retried with exponential backoff +
    /// jitter, up to the [`RetryPolicy`] cap; past the cap a hard
    /// [`WriteError::BusyRetriesExhausted`] is returned (never a silent drop).
    ///
    /// `BEGIN IMMEDIATE` is used for **every** write txn (never a `DEFERRED`
    /// read-txn that upgrades) because that upgrade hits an instant `SQLITE_BUSY`
    /// that `busy_timeout` will not retry (ADR 097 §3).
    ///
    /// # Errors
    /// Returns [`WriteError`] if the closure or commit fails with a non-busy error,
    /// or the busy-retry budget is exhausted.
    pub async fn with_write_txn<F>(&self, f: F) -> Result<(), WriteError>
    where
        F: for<'a> Fn(
            &'a libsql::Connection,
        ) -> Pin<Box<dyn Future<Output = Result<(), libsql::Error>> + 'a>>,
    {
        let max_attempts = self.retry.max_attempts.max(1);
        for attempt in 1..=max_attempts {
            match self.try_write_txn_once(&f).await {
                Ok(()) => return Ok(()),
                Err(err) if is_busy(&err) => {
                    // Retryable: back off (unless this was the last attempt) and try
                    // the whole closure again under a fresh BEGIN IMMEDIATE.
                    if attempt == max_attempts {
                        return Err(WriteError::BusyRetriesExhausted {
                            attempts: max_attempts,
                        });
                    }
                    self.backoff(attempt).await;
                }
                Err(err) => return Err(WriteError::Libsql(err)),
            }
        }
        // Unreachable: the loop returns on the final attempt.
        Err(WriteError::BusyRetriesExhausted {
            attempts: max_attempts,
        })
    }

    /// One attempt: `BEGIN IMMEDIATE`, run the closure, `COMMIT`. On any error,
    /// best-effort `ROLLBACK` and surface the original error so the caller decides
    /// whether it is retryable.
    async fn try_write_txn_once<F>(&self, f: &F) -> Result<(), libsql::Error>
    where
        F: for<'a> Fn(
            &'a libsql::Connection,
        ) -> Pin<Box<dyn Future<Output = Result<(), libsql::Error>> + 'a>>,
    {
        // BEGIN IMMEDIATE acquires the write lock up front — the ADR 097 §3
        // discipline. If this itself is busy, that is the retryable signal.
        self.conn.execute("BEGIN IMMEDIATE", ()).await?;

        let body = f(&self.conn).await;
        if let Err(err) = body {
            // Roll back so the connection is clean for the next attempt; ignore a
            // rollback error and surface the real one.
            let _ = self.conn.execute("ROLLBACK", ()).await;
            return Err(err);
        }

        if let Err(err) = self.conn.execute("COMMIT", ()).await {
            let _ = self.conn.execute("ROLLBACK", ()).await;
            return Err(err);
        }
        Ok(())
    }

    /// Sleep an exponential backoff for `attempt` (1-based), capped and jittered.
    async fn backoff(&self, attempt: u32) {
        // Millis are small; `Duration::as_millis` is `u128` but a base/max backoff
        // this side of a few seconds fits `u64` with room to spare.
        let base = u64::try_from(self.retry.base_backoff.as_millis()).unwrap_or(u64::MAX);
        let max = u64::try_from(self.retry.max_backoff.as_millis()).unwrap_or(u64::MAX);
        // Exponential: base * 2^(attempt-1), saturating, capped at max_backoff.
        let shift = (attempt - 1).min(20);
        let raw = base.saturating_mul(1u64 << shift);
        let capped = raw.min(max);
        // Full jitter over [0, capped]: decorrelate a fan-out of simultaneous
        // retries so they do not resynchronize (same posture as C14 backoff).
        // Integer arithmetic throughout: jittered = capped * numer / DENOM.
        let seed = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        )
        .unwrap_or(0)
        .wrapping_add(u64::from(attempt));
        let numer = jitter_numer(seed);
        let jittered = capped.saturating_mul(numer) / JITTER_DENOM;
        tokio::time::sleep(Duration::from_millis(jittered)).await;
    }

    /// The underlying connection, for read queries (the reader/writer tickets
    /// T84/T86 build on this). Writes must go through [`MetaStore::with_write_txn`].
    #[must_use]
    pub fn connection(&self) -> &libsql::Connection {
        &self.conn
    }
}

/// Map a [`WriteError`] from a migration/DDL step onto an [`OpenError`], so
/// callers of `open` see one error type. A busy-past-the-cap DDL is a real open
/// failure, mapped to a libsql busy error.
fn map_migration_error(e: WriteError) -> OpenError {
    match e {
        WriteError::Libsql(err) => OpenError::Libsql(err),
        WriteError::BusyRetriesExhausted { attempts } => OpenError::Libsql(
            libsql::Error::SqliteFailure(SQLITE_BUSY, format!("DDL still SQLITE_BUSY after {attempts} attempts")),
        ),
    }
}

/// Set a pragma that returns a result row (e.g. `PRAGMA journal_mode=WAL` echoes
/// the new mode). libSQL's `execute` rejects statements that return rows, so a
/// row-returning pragma must go through `query`.
async fn set_pragma(conn: &libsql::Connection, sql: &str) -> Result<(), libsql::Error> {
    let mut rows = conn.query(sql, ()).await?;
    // Drain the single echoed row (if any) so the statement completes.
    let _ = rows.next().await?;
    Ok(())
}
