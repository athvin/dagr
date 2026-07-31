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
//! There is no `delete`, no listing, and no lifecycle: reclaiming intermediate
//! blobs is garbage collection, it belongs with the object-store backend, and it
//! interacts with content addressing in a way worth deciding once — a purely
//! content-addressed key shared across runs cannot be reclaimed by run age, only
//! by reachability. There is also no network client here at all; the object-store
//! backend lands behind this same port, which is what keeps that dependency out of
//! a build that does not ask for it.

pub mod digest;
pub mod local;
pub mod store;

pub use local::LocalFsBlob;
pub use store::{
    BlobError, BlobErrorClass, BlobKey, BlobRef, BlobRefError, BlobStat, BlobStore,
    REFERENCE_SCHEME_PREFIX,
};
