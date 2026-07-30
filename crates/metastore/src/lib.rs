#![doc = include_str!("../README.md")]
//!
//! # Module index
//!
//! The orientation above comes from the crate's `README.md`, inlined here so the
//! crates.io landing page and this front page are one file. What follows is the
//! map of where each piece lives.
//!
//! The store itself — opening, the pragmas, the migrations, and the write-txn
//! seam — is [`store`]; the event→row projection and the reconcile walk that
//! folds every run under a run store are [`mapping`]
//! ([`mapping::sync_run_store`]); the guaranteed live tee that projects a run
//! into the index **as it executes** is [`live_sink`]
//! ([`live_sink::MetastoreSink`]); the table definitions and ordered idempotent
//! migrations are [`schema`].
//!
//! The concurrency recipe the README summarises is encoded in
//! [`MetaStore::open`] (the pragmas) and [`MetaStore::with_write_txn`]
//! (`BEGIN IMMEDIATE` plus the bounded `SQLITE_BUSY` retry) — a caller does not
//! reimplement it.

pub mod live_sink;
pub mod mapping;
pub mod schema;
pub mod store;

pub use live_sink::MetastoreSink;
pub use store::{MetaStore, OpenMode, WriteError};
