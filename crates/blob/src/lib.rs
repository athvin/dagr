#![doc = include_str!("../README.md")]
//!
//! # Where the pieces are
//!
//! - [`BlobStore`] — the port: [`put`](BlobStore::put) bytes and get their content
//!   address back, [`get`](BlobStore::get) them again (verified), or
//!   [`head`](BlobStore::head) the object to learn its size and what it
//!   **actually** hashes to now.
//! - [`LocalFsBlob`] — the local filesystem backend, writing atomically under a
//!   configured root.
//! - [`BlobKey`] / [`BlobRef`] — the content address and the self-describing
//!   reference (`dagr-blob+<backend>://<container>/<algorithm>/<hex>`) a durable
//!   output serializes.
//! - [`BlobError`] / [`BlobErrorClass`] — the absent / transient / corrupt split
//!   that lets a caller tell a deleted object from an unreachable store from bad
//!   bytes.
//! - [`digest`] — the in-tree SHA-256 that makes the content address possible with
//!   no dependency.
//!
//! ```
//! use dagr_blob::{BlobRef, BlobStore, LocalFsBlob};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let root = std::env::temp_dir().join(format!("dagr-blob-doc-{}", std::process::id()));
//! let store = LocalFsBlob::open(&root);
//! let key = store.put(b"the encoded payload")?;
//!
//! // The reference is what a durable output records; it names everything needed
//! // to fetch the bytes again, in another process.
//! let reference = store.reference(&key).to_string();
//! let parsed = BlobRef::parse(&reference)?;
//! let fetched = LocalFsBlob::open(parsed.container()).get(parsed.key())?;
//! assert_eq!(fetched, b"the encoded payload");
//! # std::fs::remove_dir_all(&root).ok();
//! # Ok(())
//! # }
//! ```
//!
//! # What is deliberately absent
//!
//! There is no lifecycle here — no expiry, no replication, no bucket
//! provisioning. Reclaiming intermediate blobs *is* here, as [`BlobReclaim`], and
//! it is a **separate trait** from [`BlobStore`] because enumerating and deleting
//! are an operator's operations rather than a run's: a node runner is handed a
//! type that structurally cannot delete anything. The criterion the two
//! operations serve is **reachability, never age** — a key is the digest of its
//! bytes, so the same value produced by two runs is one blob, and reclaiming "old
//! runs' blobs" would delete blobs a newer run still references. `dagr-cli`'s
//! `prune` owns that walk.
//!
//! # The object-store backend, and where its HTTP client is not
//!
//! [`S3Blob`] implements the same port over an S3-compatible bucket, and every
//! build compiles it — because it contains **no HTTP client**. The protocol
//! (canonical requests, `SigV4` signing, status classification, paged listing, the
//! bounded retry) lives here, written against the sans-IO
//! [`HttpTransport`](s3::HttpTransport) port; the client that moves the bytes,
//! with its TLS stack and its certificate verification, lives in `dagr-cli`
//! behind a default-off feature. So this crate's dependency table stays empty,
//! `cargo build --all` compiles no HTTP or TLS crate at all, and every
//! interesting failure — an unreachable store, a 403, a 500 that clears on the
//! third try — is inducible in-process instead of raced against an endpoint.

pub mod digest;
pub mod hmac;
pub mod local;
pub mod retry;
pub mod s3;
pub mod store;

pub use local::LocalFsBlob;
pub use retry::{RetryBudget, Sleeper, ThreadSleeper};
pub use s3::{S3Blob, S3Config, S3Credentials};
pub use store::{
    BlobError, BlobErrorClass, BlobKey, BlobReclaim, BlobRef, BlobRefError, BlobStat, BlobStore,
    REFERENCE_SCHEME_PREFIX,
};
