//! The **blob-backed durable-output bridge**: [`Blob<T>`], a
//! [`DurableOutput`] over any [`Payload`], stored through the
//! [`BlobStore`](dagr_blob::BlobStore) port.
//!
//! # Why this is small, and why it is here
//!
//! [`DurableOutput`] already exists, already carries an optional
//! [`DurableReferenceMeta`] `{ content_hash, size_bytes, scheme,
//! produced_at_offset_ns }`, and resume already depends on it — including
//! out-of-band mutation detection. So a generic `DurableOutput` implementation
//! over `Payload` hands a remote output content hashes, existence probes, resume
//! rehydration, and lineage rows **for free**, through machinery that is already
//! tested. Nothing on the resume or lineage side needed inventing.
//!
//! The bridge lives in `dagr-cli` rather than in `dagr-blob` because it needs
//! **both** sides: [`DurableOutput`] and [`Payload`] are `dagr-core`'s, and
//! `dagr-blob` deliberately keeps no dependency edge onto `dagr-core` (the same
//! boundary `dagr-render` and `dagr-metastore` keep). This crate is the one place
//! both are visible — and it reaches the port only behind the default-off `blob`
//! feature, so a default build has neither the bridge nor the edge.
//!
//! `dagr-core` holds no knowledge of any of this: it never computes a hash, never
//! interprets a reference, and gains nothing from this module's existence.
//!
//! # The shape
//!
//! ```no_run
//! use dagr_blob::LocalFsBlob;
//! use dagr_cli::blob_bridge::Blob;
//! use dagr_core::{Payload, StableName};
//! use dagr_core::assembly::DurableOutput;
//!
//! #[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
//! struct Manifest { rows: u64 }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let store = LocalFsBlob::open("/mnt/dagr-blobs");
//! let published = Blob::put(&store, Manifest { rows: 4_211 })?;
//!
//! // A durable node returns this value; the framework records its reference.
//! let reference = published.serialize_reference();
//! let same = Blob::<Manifest>::rehydrate(&reference)?;
//! assert_eq!(same.value(), published.value());
//! # Ok(())
//! # }
//! ```
//!
//! # Error mapping, and why an unreadable reference is not a dangling one
//!
//! The port's absent / transient / corrupt split is the same split
//! [`RehydrateError`] makes, so the mapping is the identity — with one deliberate
//! choice at the edges: a reference this build cannot **parse**, or one naming a
//! backend it has no store for, maps to **transient**, never absent. Absent means
//! "the referent is gone", and it is the verdict that refuses a resume plan up
//! front; answering it for a reference we merely could not read would refuse a
//! resume over a value that is very probably still there. Transient says what is
//! true — this process could not complete the fetch — and lets the failure surface
//! at rehydration, named.

use std::ops::Deref;

use dagr_blob::{BlobError, BlobRef, BlobStat, BlobStore, LocalFsBlob};
use dagr_core::assembly::{DurableOutput, DurableReferenceMeta};
use dagr_core::error::RehydrateError;
use dagr_core::payload::Payload;
use dagr_core::resume::ReferenceExistence;

/// The local filesystem backend's name in a reference.
const LOCAL_BACKEND: &str = "file";

/// The object-store backend's name in a reference. A build without the
/// `blob-s3` feature has no client for it, and says so honestly — "cannot
/// determine", never "absent".
const OBJECT_BACKEND: &str = "s3";

/// A `Payload` value **and the blob it lives in** — the durable output a
/// remote-eligible node produces.
///
/// A durable node's output value *is* a reference to where the value durably
/// lives (arch.md C27), and that is exactly what this is: the value, plus the
/// content-addressed reference naming the bytes it was encoded to, plus the digest
/// and size of those bytes. [`serialize_reference`](DurableOutput::serialize_reference)
/// names the blob; [`rehydrate`](DurableOutput::rehydrate) fetches and decodes it,
/// possibly in another process.
///
/// It [`Deref`]s to the value, so a consumer reads it like the value it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob<T> {
    value: T,
    reference: BlobRef,
    size_bytes: u64,
}

impl<T: Payload> Blob<T> {
    /// Encode `value`, store the bytes, and return the durable output naming them.
    ///
    /// The key is the digest of the encoded bytes, so writing the same value twice
    /// is one blob and the reference verifies itself. The codec is deterministic
    /// (that is a property the codec's own tests pin), which is what makes that
    /// true of *values* and not merely of byte strings.
    ///
    /// # Errors
    ///
    /// The port's [`BlobError`] when the bytes could not be stored.
    pub fn put<S: BlobStore + ?Sized>(store: &S, value: T) -> Result<Self, BlobError> {
        let bytes = value.encode_to_vec();
        let key = store.put(&bytes)?;
        Ok(Self {
            value,
            reference: store.reference(&key),
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        })
    }

    /// The stored value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Take the value back out, dropping the reference.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// The blob's reference — what [`serialize_reference`](DurableOutput::serialize_reference)
    /// records.
    pub fn blob_ref(&self) -> &BlobRef {
        &self.reference
    }

    /// The content hash of the encoded bytes, in the `<algorithm>:<hex>` form the
    /// prior run records and the resume gate compares.
    pub fn content_hash(&self) -> String {
        self.reference.key().to_string()
    }

    /// The encoded size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Fetch and decode the blob `reference` names **from a store the caller
    /// supplies**, rather than from the one the reference implies.
    ///
    /// [`rehydrate`](DurableOutput::rehydrate) is a static method with no store
    /// parameter, so it opens the store the reference names, from ambient
    /// configuration. That is right for a pod rehydrating an input, and wrong for
    /// a test — which needs to drive a store it can perturb — so the store-taking
    /// form is the primitive and the ambient one is a thin wrapper over it.
    ///
    /// # Errors
    ///
    /// The same [`RehydrateError`] mapping: absent when the referent is gone,
    /// transient when the store could not be read, corruption when the bytes are
    /// not what the key names or do not decode.
    pub fn rehydrate_from<S: BlobStore + ?Sized>(
        store: &S,
        reference: &str,
    ) -> Result<Self, RehydrateError> {
        let parsed = parse_reference(reference)?;
        let bytes = store.get(parsed.key()).map_err(rehydrate_error)?;
        let value = T::decode(&bytes).map_err(|err| {
            RehydrateError::corruption(format!(
                "the blob at `{reference}` did not decode as `{}`: {err}",
                T::STABLE_NAME
            ))
            .with_source(err)
        })?;
        let size_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Ok(Self {
            value,
            reference: parsed,
            size_bytes,
        })
    }
}

impl<T> Deref for Blob<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Payload> DurableOutput for Blob<T> {
    fn serialize_reference(&self) -> String {
        self.reference.to_string()
    }

    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        let parsed = parse_reference(reference)?;
        let store = open_store(&parsed)?;
        Self::rehydrate_from(&store, reference)
    }

    fn durable_reference_meta(&self) -> Option<DurableReferenceMeta> {
        Some(
            DurableReferenceMeta::new()
                .content_hash(self.content_hash())
                .size_bytes(self.size_bytes)
                .scheme(format!("dagr-blob+{}", self.reference.backend())),
        )
    }
}

/// The **existence probe** `plan_resume` takes: is the prior run's blob still
/// there, and — when a content hash was recorded — is it still the same bytes?
///
/// It answers all four cases:
///
/// * [`Present`](ReferenceExistence::Present) — the object is there and, when a
///   hash was recorded, still matches it;
/// * [`Absent`](ReferenceExistence::Absent) — the object is gone, which fails the
///   resume plan up front as a dangling reference;
/// * [`Changed`](ReferenceExistence::Changed) — the object is there and hashes to
///   something else, carrying the **actual** hash so the refusal can name both;
/// * [`CannotDetermine`](ReferenceExistence::CannotDetermine) — the store could
///   not be reached, or the reference names a backend this build has no store for.
///   The plan proceeds and a genuine problem surfaces at rehydration, because a
///   probe that could not look is not evidence of deletion.
///
/// The `node` argument is the probe signature's, not this probe's business: which
/// node a dangling reference belongs to is the refusal's to name.
#[must_use]
pub fn reference_existence(
    node: &str,
    reference: &str,
    expected_content_hash: Option<&str>,
) -> ReferenceExistence {
    let _ = node;
    let Ok(parsed) = BlobRef::parse(reference) else {
        return ReferenceExistence::CannotDetermine;
    };
    let Ok(store) = open_store(&parsed) else {
        return ReferenceExistence::CannotDetermine;
    };
    reference_existence_in(&store, reference, expected_content_hash)
}

/// The same probe, against **a store the caller supplies**.
///
/// [`reference_existence`] opens the store the reference names from ambient
/// configuration, which is what `plan_resume` needs in production and what a test
/// cannot use — the interesting cases (unreachable, overwritten out-of-band) are
/// reachable only against a store the test controls. So this is the primitive and
/// the ambient probe is a wrapper over it.
///
/// A reference naming a **different container** than `store` addresses is
/// [`CannotDetermine`](ReferenceExistence::CannotDetermine), never `Absent`: this
/// store simply cannot see it, which is not evidence that anything was deleted.
#[must_use]
pub fn reference_existence_in<S: BlobStore + ?Sized>(
    store: &S,
    reference: &str,
    expected_content_hash: Option<&str>,
) -> ReferenceExistence {
    let Ok(parsed) = BlobRef::parse(reference) else {
        return ReferenceExistence::CannotDetermine;
    };
    if parsed.backend() != store.backend() || parsed.container() != store.container() {
        return ReferenceExistence::CannotDetermine;
    }
    match store.head(parsed.key()) {
        Ok(stat) => classify_stat(&stat, &parsed, expected_content_hash),
        Err(err) if err.is_absent() => ReferenceExistence::Absent,
        Err(_) => ReferenceExistence::CannotDetermine,
    }
}

/// Turn a measured [`BlobStat`] into the probe's verdict.
fn classify_stat(
    stat: &BlobStat,
    parsed: &BlobRef,
    expected_content_hash: Option<&str>,
) -> ReferenceExistence {
    // The recorded hash is what the prior run wrote down; the reference's own
    // content address is the same digest by construction, so an absent recorded
    // hash still leaves the key to compare against.
    let expected = expected_content_hash.map_or_else(|| parsed.key().to_string(), str::to_string);
    if stat.content_hash() == expected {
        ReferenceExistence::Present
    } else if expected_content_hash.is_some() {
        ReferenceExistence::Changed {
            actual: stat.content_hash().to_string(),
        }
    } else {
        // No hash was recorded, so `Changed` is not this probe's to return (it is
        // defined as the recorded-hash mismatch, and the refusal it drives names a
        // recorded hash there is none of). The mismatch is not lost: `get` refuses
        // these bytes as corrupt at rehydration, naming both digests.
        ReferenceExistence::CannotDetermine
    }
}

/// Convert the core durable-reference metadata a [`Blob`] supplies into the
/// **wire** metadata the event stream records (`dagr_artifact`'s).
///
/// The two types are deliberately separate — core is serde-free and the artifact
/// layer is the serialization boundary — so something has to carry the values
/// across, and it is this side, where both are already in scope. The driver stamps
/// the result onto the succeeded attempt's `attempt-outcome` record through the
/// unchanged recording path.
#[must_use]
pub fn wire_reference_meta(
    meta: &DurableReferenceMeta,
) -> dagr_artifact::event_stream::DurableReferenceMeta {
    dagr_artifact::event_stream::DurableReferenceMeta {
        content_hash: meta.recorded_content_hash().map(str::to_string),
        size_bytes: meta.recorded_size_bytes(),
        scheme: meta.recorded_scheme().map(str::to_string),
        produced_at_offset_ns: meta.recorded_produced_at_offset_ns(),
    }
}

/// Parse a reference, mapping a refusal to a **transient** rehydrate error (see
/// the module docs: unreadable is not gone).
fn parse_reference(reference: &str) -> Result<BlobRef, RehydrateError> {
    BlobRef::parse(reference).map_err(|err| {
        RehydrateError::transient(format!(
            "this build cannot read the durable reference `{reference}`: {err}"
        ))
        .with_source(err)
    })
}

/// Any store this build can open **from a reference alone** — the situation
/// [`DurableOutput::rehydrate`] is in, having no store parameter.
///
/// A reference names its backend and its container, and that is deliberately all
/// it names: an endpoint and a region are *this process's* view of how to reach a
/// bucket, and a reference that carried them would stop resolving when the network
/// changed and the storage did not. So the container comes from the reference and
/// the reach — endpoint, region, credentials — comes from the ambient
/// environment, which is also what makes a reference meaningful in a pod that
/// shares only a volume or a bucket.
enum AmbientStore {
    /// The local filesystem backend, rooted at the container.
    Local(LocalFsBlob),
    /// The object-store backend, over the real HTTP client.
    #[cfg(feature = "blob-s3")]
    Object(Box<dagr_blob::S3Blob<crate::blob_s3::HttpsTransport>>),
}

impl BlobStore for AmbientStore {
    fn backend(&self) -> &str {
        match self {
            Self::Local(store) => store.backend(),
            #[cfg(feature = "blob-s3")]
            Self::Object(store) => store.backend(),
        }
    }

    fn container(&self) -> String {
        match self {
            Self::Local(store) => store.container(),
            #[cfg(feature = "blob-s3")]
            Self::Object(store) => store.container(),
        }
    }

    fn put(&self, bytes: &[u8]) -> Result<dagr_blob::BlobKey, BlobError> {
        match self {
            Self::Local(store) => store.put(bytes),
            #[cfg(feature = "blob-s3")]
            Self::Object(store) => store.put(bytes),
        }
    }

    fn get(&self, key: &dagr_blob::BlobKey) -> Result<Vec<u8>, BlobError> {
        match self {
            Self::Local(store) => store.get(key),
            #[cfg(feature = "blob-s3")]
            Self::Object(store) => store.get(key),
        }
    }

    fn head(&self, key: &dagr_blob::BlobKey) -> Result<BlobStat, BlobError> {
        match self {
            Self::Local(store) => store.head(key),
            #[cfg(feature = "blob-s3")]
            Self::Object(store) => store.head(key),
        }
    }
}

/// Open the store a reference names, refusing a backend this build has none for.
///
/// The refusal is **transient**, never absent: a build without the object-store
/// client cannot see an `s3` blob, and "I cannot look" is not "it is gone". That
/// distinction is what keeps a resume from being refused up front over a value
/// that is very probably still there.
fn open_store(reference: &BlobRef) -> Result<AmbientStore, RehydrateError> {
    match reference.backend() {
        LOCAL_BACKEND => Ok(AmbientStore::Local(LocalFsBlob::open(
            reference.container(),
        ))),
        #[cfg(feature = "blob-s3")]
        OBJECT_BACKEND => crate::blob_s3::open_ambient(reference.container())
            .map(|store| AmbientStore::Object(Box::new(store)))
            .map_err(|err| {
                RehydrateError::transient(format!(
                    "the object store holding `{reference}` could not be opened: {err}"
                ))
            }),
        other => Err(RehydrateError::transient(format!(
            "no `{other}` blob backend is compiled into this build, so `{reference}` cannot be \
             fetched here — the value may well still exist{}",
            if other == OBJECT_BACKEND {
                " (rebuild `dagr-cli` with the `blob-s3` feature)"
            } else {
                ""
            }
        ))),
    }
}

/// Map the port's classification onto the contract's — the identity, because the
/// two splits were chosen to be the same one.
fn rehydrate_error(err: BlobError) -> RehydrateError {
    let message = err.message().to_string();
    if err.is_absent() {
        RehydrateError::absent(message).with_source(err)
    } else if err.is_corrupt() {
        RehydrateError::corruption(message).with_source(err)
    } else {
        RehydrateError::transient(message).with_source(err)
    }
}
