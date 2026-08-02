//! The **blob port**: an opaque content-addressed key, a self-describing
//! reference, a three-way error classification, and the three operations a
//! backend implements.

use std::error::Error;
use std::fmt;

use crate::digest::{ALGORITHM, sha256, to_hex};

/// The scheme prefix every dagr blob reference carries, so a reference names the
/// system that wrote it before it names anything else.
pub const REFERENCE_SCHEME_PREFIX: &str = "dagr-blob+";

// ===========================================================================
// The key: content addressing.
// ===========================================================================

/// A blob's **content address** — the digest of the bytes stored under it.
///
/// Content addressing is what makes the same value written twice one blob and a
/// reference self-verifying: a reader that fetches a key's bytes can recompute the
/// digest and know whether it got what the writer wrote, with no metadata, no
/// index, and no trust in the store.
///
/// A key renders as `<algorithm>:<hex>` — `sha256:2c26b4…` — so a recorded content
/// hash carries the function that produced it and cannot be misread after an
/// algorithm change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKey {
    algorithm: String,
    hex: String,
}

impl BlobKey {
    /// The key for `bytes` — a pure function of the bytes, computable by anyone.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            algorithm: ALGORITHM.to_string(),
            hex: to_hex(&sha256(bytes)),
        }
    }

    /// The digest algorithm this key was computed with (`"sha256"`).
    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    /// The digest, lowercase hexadecimal.
    #[must_use]
    pub fn hex(&self) -> &str {
        &self.hex
    }

    /// Whether `bytes` hash to this key — the self-verification a fetched blob
    /// gets for free.
    #[must_use]
    pub fn matches(&self, bytes: &[u8]) -> bool {
        self.algorithm == ALGORITHM && self.hex == to_hex(&sha256(bytes))
    }

    /// Rebuild a key from an `<algorithm>` + `<hex>` pair a reference carried.
    ///
    /// Refuses an unknown algorithm and anything that is not lowercase hex of the
    /// right length: a key the store cannot compute is a key it cannot verify.
    #[must_use]
    pub fn from_parts(algorithm: &str, hex: &str) -> Option<Self> {
        if algorithm != ALGORITHM || hex.len() != 64 {
            return None;
        }
        if !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return None;
        }
        Some(Self {
            algorithm: algorithm.to_string(),
            hex: hex.to_string(),
        })
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.hex)
    }
}

// ===========================================================================
// The reference: `dagr-blob+<backend>://<container>/<algorithm>/<hex>`.
// ===========================================================================

/// A **self-describing reference** to a blob: which backend holds it, which
/// container within that backend, and its content address.
///
/// The grammar is uniform across backends —
/// `dagr-blob+<backend>://<container>/<algorithm>/<hex>` — so the local backend's
/// `dagr-blob+file:///var/lib/dagr/blobs/sha256/2c26b4…` and an object store's
/// `dagr-blob+s3://bucket/prefix/sha256/2c26b4…` parse through one function. The
/// `container` is the backend's own addressing unit (a filesystem root, a bucket
/// and prefix); dagr never interprets it beyond handing it back to that backend.
///
/// The reference is the string a durable output serializes, and dagr records it
/// verbatim — it interprets neither the reference nor the hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    backend: String,
    container: String,
    key: BlobKey,
}

impl BlobRef {
    /// Build a reference naming `key` inside `container` on `backend`.
    #[must_use]
    pub fn new(backend: impl Into<String>, container: impl Into<String>, key: BlobKey) -> Self {
        Self {
            backend: backend.into(),
            container: container.into(),
            key,
        }
    }

    /// Parse a reference this crate produced.
    ///
    /// # Errors
    ///
    /// [`BlobRefError`] when `text` is not a dagr blob reference: a missing or
    /// wrong scheme, an empty backend or container, a missing content address, or
    /// an algorithm/digest this build cannot verify.
    pub fn parse(text: &str) -> Result<Self, BlobRefError> {
        let after_prefix = text
            .strip_prefix(REFERENCE_SCHEME_PREFIX)
            .ok_or_else(|| BlobRefError::new(text, "it does not start with `dagr-blob+`"))?;
        let (backend, rest) = after_prefix
            .split_once("://")
            .ok_or_else(|| BlobRefError::new(text, "no `<backend>://` separator"))?;
        if backend.is_empty() {
            return Err(BlobRefError::new(text, "the backend name is empty"));
        }
        let (container, address) = rest
            .rsplit_once('/')
            .and_then(|(head, hex)| {
                let (container, algorithm) = head.rsplit_once('/')?;
                Some((container, (algorithm, hex)))
            })
            .ok_or_else(|| {
                BlobRefError::new(text, "no trailing `/<algorithm>/<hex>` content address")
            })?;
        if container.is_empty() {
            return Err(BlobRefError::new(text, "the container is empty"));
        }
        let key = BlobKey::from_parts(address.0, address.1).ok_or_else(|| {
            BlobRefError::new(
                text,
                format!(
                    "`{}/{}` is not a content address this build can verify (expected `{ALGORITHM}` and 64 hex digits)",
                    address.0, address.1
                ),
            )
        })?;
        Ok(Self {
            backend: backend.to_string(),
            container: container.to_string(),
            key,
        })
    }

    /// The backend that holds the blob (`"file"`, later `"s3"`).
    #[must_use]
    pub fn backend(&self) -> &str {
        &self.backend
    }

    /// The container within that backend — a filesystem root, a bucket + prefix.
    #[must_use]
    pub fn container(&self) -> &str {
        &self.container
    }

    /// The blob's content address.
    #[must_use]
    pub fn key(&self) -> &BlobKey {
        &self.key
    }
}

impl fmt::Display for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{REFERENCE_SCHEME_PREFIX}{}://{}/{}/{}",
            self.backend,
            self.container,
            self.key.algorithm(),
            self.key.hex()
        )
    }
}

/// A reference that could not be parsed, naming the offending text and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRefError {
    reference: String,
    reason: String,
}

impl BlobRefError {
    fn new(reference: &str, reason: impl Into<String>) -> Self {
        Self {
            reference: reference.to_string(),
            reason: reason.into(),
        }
    }

    /// The reference text that failed to parse.
    #[must_use]
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Why it failed.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for BlobRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a dagr blob reference: {}",
            self.reference, self.reason
        )
    }
}

impl Error for BlobRefError {}

// ===========================================================================
// The error classification: absent / transient / corrupt.
// ===========================================================================

/// How a blob operation failed — the **three-way split** that lets a caller
/// distinguish a deleted object from an unreachable store from bad bytes.
///
/// It is deliberately the same split the durable-output contract's rehydrate error
/// already makes, so the bridge maps one onto the other with no policy of its own:
/// absent is a dangling reference, transient is "try again", corrupt is "the
/// referent exists and is unusable".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobErrorClass {
    /// The blob is **gone** — no object under that key. This, and only this, is a
    /// dangling reference.
    Absent,
    /// The store could not be reached or read **now**; the blob may well still be
    /// there. Retry-eligible, and never evidence of deletion.
    Transient,
    /// The bytes under the key **do not hash to the key**: the object exists and
    /// is not what was written. Never returned as a value.
    Corrupt,
}

impl BlobErrorClass {
    /// The lowercase label this class renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            BlobErrorClass::Absent => "absent",
            BlobErrorClass::Transient => "transient",
            BlobErrorClass::Corrupt => "corrupt",
        }
    }
}

/// A classified blob-operation failure: a [`BlobErrorClass`], a human-readable
/// message, and the underlying cause when there was one (preserved through
/// [`Error::source`]).
#[derive(Debug)]
pub struct BlobError {
    class: BlobErrorClass,
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl BlobError {
    fn new(class: BlobErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
            source: None,
        }
    }

    /// The blob is gone — a dangling reference.
    #[must_use]
    pub fn absent(message: impl Into<String>) -> Self {
        Self::new(BlobErrorClass::Absent, message)
    }

    /// The store could not be reached or read now; the blob may still exist.
    #[must_use]
    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(BlobErrorClass::Transient, message)
    }

    /// The stored bytes do not hash to their key.
    #[must_use]
    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::new(BlobErrorClass::Corrupt, message)
    }

    /// Attach the underlying cause, preserved through [`Error::source`].
    #[must_use]
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// The failure classification.
    #[must_use]
    pub fn class(&self) -> BlobErrorClass {
        self.class
    }

    /// The human-readable message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether the blob is [absent](BlobErrorClass::Absent) — a dangling reference.
    #[must_use]
    pub fn is_absent(&self) -> bool {
        self.class == BlobErrorClass::Absent
    }

    /// Whether the failure is [transient](BlobErrorClass::Transient).
    #[must_use]
    pub fn is_transient(&self) -> bool {
        self.class == BlobErrorClass::Transient
    }

    /// Whether the stored bytes are [corrupt](BlobErrorClass::Corrupt).
    #[must_use]
    pub fn is_corrupt(&self) -> bool {
        self.class == BlobErrorClass::Corrupt
    }
}

impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "blob {}: {}", self.class.as_str(), self.message)
    }
}

impl Error for BlobError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|boxed| &**boxed as &(dyn Error + 'static))
    }
}

// ===========================================================================
// The port.
// ===========================================================================

/// What a cheap existence probe learned about a blob: how big it is and what it
/// **actually** hashes to right now.
///
/// The hash is measured, not remembered: an object overwritten out-of-band reports
/// the digest of the bytes that are there, which is exactly what lets a caller say
/// "changed" and name both hashes rather than silently reading stale bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobStat {
    size_bytes: u64,
    content_hash: String,
}

impl BlobStat {
    /// Build a stat from a measured size and content hash.
    #[must_use]
    pub fn new(size_bytes: u64, content_hash: impl Into<String>) -> Self {
        Self {
            size_bytes,
            content_hash: content_hash.into(),
        }
    }

    /// The object's size in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// The object's **actual** current content hash, in the `<algorithm>:<hex>`
    /// form a [`BlobKey`] renders as.
    #[must_use]
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }
}

/// The **blob port**: three operations over an opaque, content-addressed key.
///
/// A backend stores opaque bytes and nothing else. It coordinates nothing, watches
/// nothing, and knows nothing about runs, nodes, or graphs: `put` is a write,
/// `get` is a read, `head` is a probe. Everything above the port — which value
/// those bytes encode, which node produced them, whether a resume may trust them —
/// belongs to the caller.
///
/// Keys are **content addresses the port computes itself**, so `put` takes bytes
/// rather than a key: a caller cannot name a blob something the bytes do not
/// justify, which is what keeps a reference self-verifying.
pub trait BlobStore {
    /// The backend's short name, as it appears in a reference (`"file"`).
    fn backend(&self) -> &str;

    /// The container this store addresses within its backend — a filesystem root,
    /// a bucket and prefix.
    fn container(&self) -> String;

    /// Store `bytes` and return their content address.
    ///
    /// Idempotent by construction: the same bytes always land under the same key.
    ///
    /// # Errors
    ///
    /// A [`BlobError`] classified [`Transient`](BlobErrorClass::Transient) when the
    /// write could not be completed.
    fn put(&self, bytes: &[u8]) -> Result<BlobKey, BlobError>;

    /// Fetch the bytes stored under `key`, **verified** against it.
    ///
    /// # Errors
    ///
    /// [`Absent`](BlobErrorClass::Absent) when there is no such object,
    /// [`Transient`](BlobErrorClass::Transient) when it could not be read, and
    /// [`Corrupt`](BlobErrorClass::Corrupt) when the bytes do not hash to `key`.
    fn get(&self, key: &BlobKey) -> Result<Vec<u8>, BlobError>;

    /// Probe `key`: does it exist, how big is it, and what does it hash to now?
    ///
    /// # Errors
    ///
    /// [`Absent`](BlobErrorClass::Absent) when there is no such object and
    /// [`Transient`](BlobErrorClass::Transient) when it could not be probed. A
    /// `head` never reports corruption — reporting the **actual** hash is how a
    /// mismatch is surfaced, so the caller decides what a mismatch means.
    fn head(&self, key: &BlobKey) -> Result<BlobStat, BlobError>;

    /// The self-describing reference naming `key` in this store.
    fn reference(&self, key: &BlobKey) -> BlobRef {
        BlobRef::new(self.backend(), self.container(), key.clone())
    }
}

/// The **reclaim half** of the port: enumerate what a container holds, and remove
/// one blob from it.
///
/// It is a separate trait from [`BlobStore`] on purpose. `put` / `get` / `head`
/// are what a *run* does; enumerating and deleting are what an **operator** does,
/// through `prune`. Keeping them apart means a node runner is handed a type that
/// structurally cannot delete anything, and it keeps the trait a remote task's
/// side of the world implements as small as it was.
///
/// # The criterion these two operations exist for is reachability, never age
///
/// A key is the digest of its bytes and nothing else, so **the same value
/// produced by two runs is one blob**. Reclaiming "the blobs of runs older than
/// T" would therefore delete blobs a *newer* run still references — including one
/// a resume needs to rehydrate. The only sound criterion is that no retained run
/// artifact references the blob, and answering that needs exactly these two
/// operations: what is here, and remove this one.
pub trait BlobReclaim {
    /// Every blob this store's container holds.
    ///
    /// A container is not only blobs — attempt shards share it, and an
    /// interrupted write leaves temp debris — so an implementation enumerates
    /// **content-addressed objects only**, and never reports anything a caller
    /// would then be entitled to delete.
    ///
    /// # Errors
    ///
    /// [`Transient`](BlobErrorClass::Transient) when the container could not be
    /// enumerated. An implementation never reports an empty listing for a store
    /// it could not read: to a reaper, "nothing is stored" and "I could not look"
    /// differ by every live blob in the container.
    fn list(&self) -> Result<Vec<BlobKey>, BlobError>;

    /// Remove `key`'s object.
    ///
    /// **Idempotent:** a key that is already gone is a success, not an error. Two
    /// operators (or an interrupted reclaim and its retry) reaching the same blob
    /// both wanted it gone, and it is.
    ///
    /// # Errors
    ///
    /// [`Transient`](BlobErrorClass::Transient) when the object exists and could
    /// not be removed.
    fn delete(&self, key: &BlobKey) -> Result<(), BlobError>;
}

#[cfg(test)]
mod tests {
    use super::{BlobKey, BlobRef, BlobRefError, REFERENCE_SCHEME_PREFIX};

    #[test]
    fn a_key_renders_with_its_algorithm() {
        let key = BlobKey::of(b"x");
        assert_eq!(key.to_string(), format!("sha256:{}", key.hex()));
        assert!(key.matches(b"x"));
        assert!(!key.matches(b"y"));
    }

    #[test]
    fn from_parts_refuses_an_unverifiable_address() {
        assert!(BlobKey::from_parts("sha256", BlobKey::of(b"x").hex()).is_some());
        assert!(BlobKey::from_parts("md5", BlobKey::of(b"x").hex()).is_none());
        assert!(BlobKey::from_parts("sha256", "abc").is_none());
        // Uppercase hex is not the form this crate emits, so it is not accepted:
        // one blob must have exactly one reference text.
        assert!(BlobKey::from_parts("sha256", &BlobKey::of(b"x").hex().to_uppercase()).is_none());
    }

    #[test]
    fn a_reference_round_trips_through_its_grammar() {
        let key = BlobKey::of(b"payload");
        for (backend, container) in [
            ("file", "/var/lib/dagr/blobs"),
            ("s3", "bucket/prefix"),
            ("file", "/nested/root/with/segments"),
        ] {
            let reference = BlobRef::new(backend, container, key.clone());
            let text = reference.to_string();
            assert!(text.starts_with(REFERENCE_SCHEME_PREFIX));
            assert_eq!(BlobRef::parse(&text), Ok(reference), "round trip: {text}");
        }
    }

    #[test]
    fn a_refusal_names_the_reference_and_the_reason() {
        let err: BlobRefError = BlobRef::parse("nope").expect_err("not a reference");
        assert_eq!(err.reference(), "nope");
        assert!(err.to_string().contains("dagr-blob+"), "{err}");
    }
}
