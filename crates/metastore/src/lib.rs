//! `dagr-metastore` — the local, embedded, opt-in run index (M7, ADR 097 · T82).
//!
//! A queryable projection of dagr's JSONL event stream into a **libSQL** (the
//! `libsql` C fork) / `SQLite` file, so one many-DAG binary has a single place to
//! query cross-run state instead of scanning per-run `events.jsonl` files.
//!
//! # What this crate is — and is not
//!
//! This is the **run store's** derived, opt-in index (arch.md "The shape of a
//! run"), **not** a coordinating metadata store. It coordinates nothing: the
//! event stream stays the source of truth and the index is a guaranteed
//! projection of it. That carve-out is decided in **ADR 097 (T82)**. T83 shipped
//! the crate, schema, and write-safe connection seam; **T84** adds the
//! event→row [`mapping`] and the `sync` reconcile walk ([`mapping::sync_run_store`])
//! that folds every run under a run store and upserts it idempotently. **T86** adds
//! the guaranteed live tee sink ([`live_sink::MetastoreSink`]) that projects a run
//! into the index **as it executes**, reusing that same mapping and write
//! discipline; lineage columns are M8.
//!
//! # The concurrency recipe (ADR 097 §3)
//!
//! libSQL's WAL is **single-writer**. A `DEFERRED` read-txn that later upgrades
//! to a write hits an **instant `SQLITE_BUSY` that `busy_timeout` will not
//! retry**. So this seam encodes the verified discipline:
//!
//! - open with `PRAGMA journal_mode=WAL`, `synchronous=NORMAL`, and a
//!   `busy_timeout` ([`MetaStore::open`]);
//! - open **every write transaction** with `BEGIN IMMEDIATE`
//!   ([`MetaStore::with_write_txn`]);
//! - wrap each write txn in an **app-level bounded `SQLITE_BUSY` retry** with
//!   exponential backoff + jitter, surfacing a hard error past the cap.
//!
//! # Boundary (ADR 097 §5)
//!
//! The only workspace edge is onto [`dagr_artifact`] (the event/artifact types).
//! There is **no** path to `dagr-core`, so core's zero-runtime-dependency
//! guarantee is untouched, and the CLI reaches this crate only behind a
//! default-off `metastore` feature.

pub mod live_sink;
pub mod mapping;
pub mod schema;
pub mod store;

pub use live_sink::MetastoreSink;
pub use store::{MetaStore, OpenMode, WriteError};
