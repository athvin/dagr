//! The **S3-compatible object-store backend**, behind the same port as the local
//! filesystem one.
//!
//! # Why this exists, and why it is not optional in practice
//!
//! The local backend serves pod-to-pod handoff only on a **read-write-many**
//! volume mounted at the same path in every pod, and RWX is a cluster capability
//! rather than an assumption: the reference cluster the remote-execution spike
//! ran against offers exactly one storage class, and it is RWO. On such a cluster
//! the local backend cannot serve a handoff at all, which puts an object store on
//! the critical path rather than in the "later convenience" bucket.
//!
//! # The shape
//!
//! * [`S3Config`] — endpoint, bucket, prefix, region. All **operator**
//!   configuration; an S3-compatible store that is not AWS works by pointing the
//!   endpoint at it, with no code change.
//! * [`S3Credentials`] — read from the **ambient environment**, never from a flag,
//!   a reference, or anything dagr stores. Redacted in `Debug`, absent from
//!   `Display`, and reachable only by the signer.
//! * [`HttpTransport`] — the sans-IO seam. This crate builds, signs and interprets
//!   requests; something else moves the bytes. That is what keeps the whole HTTP
//!   and TLS tree out of a build that did not ask for it, and what makes "the
//!   store is unreachable" a deterministic test rather than a race.
//! * [`S3Blob`] — the backend: [`put`](crate::BlobStore::put),
//!   [`get`](crate::BlobStore::get), [`head`](crate::BlobStore::head), plus the
//!   reclaim half ([`list`](crate::BlobReclaim::list),
//!   [`delete`](crate::BlobReclaim::delete)), with the same absent / transient /
//!   corrupt classification the local backend makes and a **single** bounded
//!   retry on the engine's backoff shape.
//!
//! # Addressing
//!
//! Requests are **path-style** — `<endpoint>/<bucket>/<object-key>` — which every
//! S3-compatible implementation accepts and which is the only form that works
//! against a bare endpoint an operator supplies (`MinIO`, Ceph, a local gateway).
//! Virtual-hosted addressing is a different URL for the same object and can be
//! added behind this same config later without changing a stored reference,
//! because a reference names `<bucket>/<prefix>` and a key, never a URL.
//!
//! # This API blocks
//!
//! Every operation is a synchronous round trip, and a retrying one may sleep
//! between attempts. That matches the port (and the local backend, which pays two
//! `fsync`s on the caller's thread); a caller on an async worker treats it the way
//! it treats the scratch store.

pub mod creds;
#[cfg(feature = "test-kit")]
pub mod fake;
pub mod http;
pub mod sigv4;

use std::fmt;
use std::sync::Arc;

use crate::digest::{ALGORITHM, Sha256, to_hex};
use crate::retry::{RetryBudget, Sleeper, ThreadSleeper};
use crate::store::{BlobError, BlobKey, BlobReclaim, BlobStat, BlobStore};

pub use creds::{CredentialError, S3Credentials};
pub use http::{HttpRequest, HttpResponse, HttpTransport, TransportError};
pub use sigv4::SigningTime;

/// The backend name this store writes into a reference.
const BACKEND: &str = "s3";

/// The region assumed when an operator configures none. `us-east-1` is the
/// value every S3-compatible implementation accepts as a signing region when it
/// has no regions of its own.
pub const DEFAULT_REGION: &str = "us-east-1";

/// How many object keys one listing page asks for. The protocol maximum, so the
/// common case is one round trip.
const LIST_PAGE_SIZE: u32 = 1_000;

// ===========================================================================
// Configuration.
// ===========================================================================

/// Where the object store is, and which part of it this store addresses.
///
/// The **container** — the middle of a reference,
/// `dagr-blob+s3://<container>/<algorithm>/<hex>` — is `<bucket>` or
/// `<bucket>/<prefix>`. Endpoint and region are *this process's* view of how to
/// reach that bucket and are deliberately **not** part of the reference: the same
/// bucket is reached through different endpoints from inside and outside a
/// cluster, and a reference that hard-coded one would stop resolving when the
/// network did not change at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    endpoint: Option<String>,
    bucket: String,
    prefix: String,
    region: String,
}

impl S3Config {
    /// A config for `bucket` at the default AWS endpoint for
    /// [`DEFAULT_REGION`], with no prefix.
    #[must_use]
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            endpoint: None,
            bucket: bucket.into(),
            prefix: String::new(),
            region: DEFAULT_REGION.to_string(),
        }
    }

    /// Point at a specific endpoint (an S3-compatible store that is not AWS).
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.trim_end_matches('/').to_string())
        };
        self
    }

    /// Address a prefix within the bucket.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into().trim_matches('/').to_string();
        self
    }

    /// Set the signing region.
    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        let region = region.into();
        if !region.is_empty() {
            self.region = region;
        }
        self
    }

    /// Rebuild a config from the `<bucket>[/<prefix>]` container a reference
    /// carried, at the default endpoint and region.
    ///
    /// Returns `None` for a container that names no bucket. Endpoint and region
    /// are the *caller's* to supply — see the type docs for why a reference does
    /// not carry them.
    #[must_use]
    pub fn from_container(container: &str) -> Option<Self> {
        let container = container.trim_matches('/');
        if container.is_empty() {
            return None;
        }
        let (bucket, prefix) = container.split_once('/').unwrap_or((container, ""));
        if bucket.is_empty() {
            return None;
        }
        Some(Self::new(bucket).with_prefix(prefix))
    }

    /// The bucket.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The prefix within the bucket (empty when none).
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The signing region.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The endpoint requests are sent to — the configured one, or the AWS
    /// endpoint for the configured region.
    #[must_use]
    pub fn endpoint_url(&self) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.region))
    }

    /// The container a reference names: `<bucket>` or `<bucket>/<prefix>`.
    #[must_use]
    pub fn container(&self) -> String {
        if self.prefix.is_empty() {
            self.bucket.clone()
        } else {
            format!("{}/{}", self.bucket, self.prefix)
        }
    }

    /// The bucket-relative object key `key`'s bytes live under:
    /// `<prefix>/<algorithm>/<hex>`.
    #[must_use]
    pub fn object_key(&self, key: &BlobKey) -> String {
        if self.prefix.is_empty() {
            format!("{}/{}", key.algorithm(), key.hex())
        } else {
            format!("{}/{}/{}", self.prefix, key.algorithm(), key.hex())
        }
    }

    /// The listing prefix that selects exactly this store's blobs — and nothing
    /// else sharing the bucket or the container (an attempt shard, another
    /// tool's objects).
    #[must_use]
    pub fn listing_prefix(&self) -> String {
        if self.prefix.is_empty() {
            format!("{ALGORITHM}/")
        } else {
            format!("{}/{ALGORITHM}/", self.prefix)
        }
    }

    /// The authority (`host[:port]`) requests are signed against.
    fn host(&self) -> String {
        let url = self.endpoint_url();
        let after_scheme = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
        after_scheme
            .split_once('/')
            .map_or(after_scheme, |(host, _)| host)
            .to_string()
    }
}

// ===========================================================================
// The backend.
// ===========================================================================

/// A blob store over an S3-compatible bucket.
///
/// Generic over its [`HttpTransport`] so the protocol is testable with no socket
/// and so no HTTP or TLS crate is reachable from this one.
pub struct S3Blob<T> {
    config: S3Config,
    credentials: S3Credentials,
    transport: T,
    retry: RetryBudget,
    sleeper: Arc<dyn Sleeper>,
}

impl<T> S3Blob<T> {
    /// Open a store over `transport`, signing with `credentials`.
    ///
    /// Performs no I/O: nothing is contacted until the first operation.
    #[must_use]
    pub fn new(config: S3Config, credentials: S3Credentials, transport: T) -> Self {
        Self {
            config,
            credentials,
            transport,
            retry: RetryBudget::default(),
            sleeper: Arc::new(ThreadSleeper),
        }
    }

    /// Replace the bounded retry budget.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryBudget) -> Self {
        self.retry = retry;
        self
    }

    /// Replace how the retry waits between attempts (a test asserts the schedule
    /// rather than spending it).
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Arc<dyn Sleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// The configuration this store addresses.
    #[must_use]
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// The bucket-relative object key `key` lives under.
    #[must_use]
    pub fn object_key(&self, key: &BlobKey) -> String {
        self.config.object_key(key)
    }
}

/// Redacted, and free of a `T: Debug` bound — a store is routinely formatted into
/// a diagnostic, and it carries a credential.
impl<T> fmt::Debug for S3Blob<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Blob")
            .field("config", &self.config)
            .field("credentials", &self.credentials)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl<T: HttpTransport> S3Blob<T> {
    /// Build, sign and send a request, retrying **retryable** failures on the
    /// bounded backoff and classifying the rest.
    ///
    /// The request is rebuilt per attempt because it is signed against a
    /// timestamp: replaying a stale signature would be rejected by the store for
    /// a reason that has nothing to do with the operation.
    fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
        what: &str,
    ) -> Result<HttpResponse, BlobError> {
        let attempts = self.retry.attempts();
        let mut last: Option<BlobError> = None;
        for attempt in 0..attempts {
            let url = if query.is_empty() {
                format!("{}{path}", self.config.endpoint_url())
            } else {
                format!("{}{path}?{query}", self.config.endpoint_url())
            };
            let request = sigv4::sign(
                HttpRequest::new(method, url).with_body(body.to_vec()),
                &self.config.host(),
                &self.credentials,
                self.config.region(),
                &SigningTime::now(),
            );

            match self.transport.execute(&request) {
                Ok(response) if (200..300).contains(&response.status()) => return Ok(response),
                Ok(response) => {
                    let (error, retryable) = classify_status(&response, what);
                    if !retryable {
                        return Err(error);
                    }
                    last = Some(error);
                }
                Err(err) => {
                    last = Some(
                        BlobError::transient(format!(
                            "could not reach the object store to {what}: {err}"
                        ))
                        .with_source(err),
                    );
                }
            }

            if attempt + 1 < attempts {
                self.sleeper.sleep(self.retry.nominal_delay(attempt));
            }
        }

        Err(exhausted(last, attempts, what))
    }

    /// Fetch an object's bytes, or classify why not.
    fn fetch(&self, key: &BlobKey, what: &str) -> Result<Vec<u8>, BlobError> {
        let response = self.send("GET", &self.object_path(key), "", &[], what)?;
        Ok(response.body().to_vec())
    }

    /// The URL path of `key`'s object: `/<bucket>/<object-key>` (path-style).
    fn object_path(&self, key: &BlobKey) -> String {
        format!("/{}/{}", self.config.bucket(), self.config.object_key(key))
    }
}

impl<T: HttpTransport> BlobStore for S3Blob<T> {
    fn backend(&self) -> &str {
        BACKEND
    }

    fn container(&self) -> String {
        self.config.container()
    }

    fn put(&self, bytes: &[u8]) -> Result<BlobKey, BlobError> {
        let key = BlobKey::of(bytes);
        // Unconditional, exactly as the local backend is: content addressing makes
        // a rewrite harmless (the bytes are the same bytes), it keeps one code
        // path, and it makes a put self-healing when an object was damaged
        // out-of-band.
        self.send(
            "PUT",
            &self.object_path(&key),
            "",
            bytes,
            &format!("store `{key}`"),
        )?;
        Ok(key)
    }

    fn get(&self, key: &BlobKey) -> Result<Vec<u8>, BlobError> {
        let bytes = self.fetch(key, &format!("read `{key}`"))?;
        if !key.matches(&bytes) {
            return Err(BlobError::corrupt(format!(
                "the object stored for `{key}` hashes to `{}` — it was not written by this key, \
                 so its bytes are not returned",
                BlobKey::of(&bytes)
            )));
        }
        Ok(bytes)
    }

    /// The probe **measures** the hash rather than trusting stored metadata.
    ///
    /// An object store can answer size from a `HEAD` in one cheap round trip, and
    /// it can serve back whatever user metadata a writer attached. Neither answers
    /// the question this probe exists for. The probe's whole job is catching an
    /// **out-of-band overwrite**, and an overwrite replaces the object's metadata
    /// along with its bytes — so any predicate cheaper than reading would report
    /// `Present` for exactly the case a mutated-reference refusal exists to catch.
    /// So `head` reads the object and hashes it, and the cost is bounded by blob
    /// size.
    fn head(&self, key: &BlobKey) -> Result<BlobStat, BlobError> {
        let bytes = self.fetch(key, &format!("probe `{key}`"))?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        Ok(BlobStat::new(
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            format!("{}:{}", key.algorithm(), to_hex(&hasher.finish())),
        ))
    }
}

impl<T: HttpTransport> BlobReclaim for S3Blob<T> {
    /// List every object under this store's `<prefix>/<algorithm>/` and keep the
    /// ones whose name is a content address.
    ///
    /// Two things are load-bearing. The listing is **paged**, and every page is
    /// followed: a reader that stopped at the first would under-report, and to a
    /// reaper an under-reported listing is not "fewer blobs to delete" — it is a
    /// wrong answer in whichever direction the caller is using it. And the prefix
    /// scopes the walk to this container, so attempt shards and anything else
    /// sharing the bucket are never enumerated as blobs.
    fn list(&self) -> Result<Vec<BlobKey>, BlobError> {
        let listing_prefix = self.config.listing_prefix();
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = format!(
                "list-type=2&max-keys={LIST_PAGE_SIZE}&prefix={}",
                sigv4::uri_encode(&listing_prefix)
            );
            if let Some(token) = &token {
                query.push_str("&continuation-token=");
                query.push_str(&sigv4::uri_encode(token));
            }
            let response = match self.send(
                "GET",
                &format!("/{}", self.config.bucket()),
                &query,
                &[],
                "enumerate the container",
            ) {
                Ok(response) => response,
                // A bucket that does not exist holds nothing — the same answer the
                // local backend gives for a root that was never written to.
                Err(err) if err.is_absent() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };
            let body = String::from_utf8_lossy(response.body());
            for object_key in xml_values(&body, "Key") {
                if let Some(key) = key_from_object_key(&object_key, &listing_prefix) {
                    keys.push(key);
                }
            }
            token = xml_values(&body, "NextContinuationToken").into_iter().next();
            if token.is_none() {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    fn delete(&self, key: &BlobKey) -> Result<(), BlobError> {
        match self.send(
            "DELETE",
            &self.object_path(key),
            "",
            &[],
            &format!("reclaim `{key}`"),
        ) {
            Ok(_) => Ok(()),
            // Already gone is the outcome the caller wanted.
            Err(err) if err.is_absent() => Ok(()),
            Err(err) => Err(err),
        }
    }
}

// ===========================================================================
// Classification.
// ===========================================================================

/// Map a non-2xx response onto the port's three-way split, and say whether
/// spending another attempt on it could possibly help.
///
/// The one judgement here is that **only 404 is `Absent`**. A 403 is not evidence
/// that anything was deleted — it is evidence that this process could not look —
/// so it is `Transient`, which the existence probe turns into `CannotDetermine`
/// rather than into a resume-refusing `DanglingReference`. It is not *retried*,
/// though: a permission failure does not clear by waiting, and spending the budget
/// on it only delays the diagnosis.
fn classify_status(response: &HttpResponse, what: &str) -> (BlobError, bool) {
    let status = response.status();
    let code = xml_values(&String::from_utf8_lossy(response.body()), "Code")
        .into_iter()
        .next();
    let detail = code.map_or(String::new(), |c| format!(" ({c})"));
    match status {
        404 => (
            BlobError::absent(format!("no object to {what}: HTTP 404{detail}")),
            false,
        ),
        401 | 403 => (
            BlobError::transient(format!(
                "the object store refused the request to {what}: HTTP {status}{detail} — the \
                 object may well still be there; this is a credential or policy problem, not a \
                 deletion"
            )),
            false,
        ),
        408 | 429 | 500..=599 => (
            BlobError::transient(format!(
                "the object store could not {what} right now: HTTP {status}{detail}"
            )),
            true,
        ),
        _ => (
            BlobError::transient(format!(
                "the object store rejected the request to {what}: HTTP {status}{detail}"
            )),
            false,
        ),
    }
}

/// The error a caller sees when the retry budget is spent, naming how many
/// attempts went into it — so "it is slow" and "it is down" are distinguishable
/// from the message alone.
fn exhausted(last: Option<BlobError>, attempts: u32, what: &str) -> BlobError {
    let cause = last.map_or_else(
        || "no attempt was made".to_string(),
        |err| err.message().to_string(),
    );
    BlobError::transient(format!(
        "could not {what} after {attempts} attempts: {cause}"
    ))
}

// ===========================================================================
// The two scraps of parsing an S3 listing needs.
// ===========================================================================

/// Every `<tag>…</tag>` text value in `xml`, in document order.
///
/// A `ListObjectsV2` response is a flat, machine-generated document and the two
/// elements that matter (`Key`, `NextContinuationToken`) carry no attributes and
/// no nesting, so extracting them by tag is exact — and it keeps this crate's
/// dependency table empty, which is an asserted boundary rather than a
/// preference. Entity references are decoded for the five XML predefined
/// entities, which is all an object key can contain.
fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = rest.get(start + open.len()..).unwrap_or("");
        let Some(end) = after.find(&close) else { break };
        out.push(decode_entities(after.get(..end).unwrap_or("")));
        rest = after.get(end + close.len()..).unwrap_or("");
    }
    out
}

/// Decode the five XML predefined entities.
fn decode_entities(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Turn a listed object key back into a [`BlobKey`], or `None` when it is not one
/// of this store's blobs. Anything sharing the container that is not a content
/// address — an attempt shard, a stray upload — lands here and is skipped.
fn key_from_object_key(object_key: &str, listing_prefix: &str) -> Option<BlobKey> {
    let hex = object_key.strip_prefix(listing_prefix)?;
    if hex.contains('/') {
        return None;
    }
    BlobKey::from_parts(ALGORITHM, hex)
}

#[cfg(test)]
mod tests {
    use super::{S3Config, decode_entities, key_from_object_key, xml_values};
    use crate::store::BlobKey;

    #[test]
    fn a_container_round_trips_through_the_config() {
        let config = S3Config::new("bucket").with_prefix("blobs");
        assert_eq!(config.container(), "bucket/blobs");
        let parsed = S3Config::from_container(&config.container()).expect("parses");
        assert_eq!(parsed.bucket(), "bucket");
        assert_eq!(parsed.prefix(), "blobs");

        let bare = S3Config::from_container("bucket").expect("parses");
        assert_eq!(bare.container(), "bucket");
        assert_eq!(bare.prefix(), "");

        assert!(S3Config::from_container("").is_none());
        assert!(S3Config::from_container("/").is_none());
    }

    #[test]
    fn the_endpoint_defaults_to_the_regions_aws_host_and_is_overridable() {
        assert_eq!(
            S3Config::new("b").with_region("eu-west-2").endpoint_url(),
            "https://s3.eu-west-2.amazonaws.com"
        );
        assert_eq!(
            S3Config::new("b").with_endpoint("https://minio.internal:9000/").endpoint_url(),
            "https://minio.internal:9000"
        );
        assert_eq!(
            S3Config::new("b").with_endpoint("https://minio.internal:9000").host(),
            "minio.internal:9000"
        );
    }

    #[test]
    fn an_object_key_is_the_prefix_plus_the_content_address() {
        let key = BlobKey::of(b"x");
        assert_eq!(
            S3Config::new("b").object_key(&key),
            format!("sha256/{}", key.hex())
        );
        assert_eq!(
            S3Config::new("b").with_prefix("/blobs/").object_key(&key),
            format!("blobs/sha256/{}", key.hex())
        );
        assert_eq!(S3Config::new("b").with_prefix("blobs").listing_prefix(), "blobs/sha256/");
    }

    #[test]
    fn listed_object_keys_that_are_not_blobs_are_skipped() {
        let key = BlobKey::of(b"x");
        let prefix = "blobs/sha256/";
        assert_eq!(
            key_from_object_key(&format!("{prefix}{}", key.hex()), prefix),
            Some(key.clone())
        );
        // An attempt shard sharing the container.
        assert!(key_from_object_key("attempt-shards/run-1/aa/1.jsonl", prefix).is_none());
        // A nested key under the blob prefix is not an object this store wrote.
        assert!(key_from_object_key(&format!("{prefix}nested/{}", key.hex()), prefix).is_none());
        // A name that is not a content address.
        assert!(key_from_object_key(&format!("{prefix}README"), prefix).is_none());
    }

    #[test]
    fn the_listing_scraper_reads_every_key_and_the_continuation_token() {
        let body = "<?xml version=\"1.0\"?><ListBucketResult>\
            <IsTruncated>true</IsTruncated>\
            <Contents><Key>blobs/sha256/aa</Key><Size>1</Size></Contents>\
            <Contents><Key>blobs/sha256/bb</Key><Size>2</Size></Contents>\
            <NextContinuationToken>tok&amp;en</NextContinuationToken>\
            </ListBucketResult>";
        assert_eq!(xml_values(body, "Key"), vec!["blobs/sha256/aa", "blobs/sha256/bb"]);
        assert_eq!(xml_values(body, "NextContinuationToken"), vec!["tok&en"]);
        assert!(xml_values(body, "Absent").is_empty());
        assert_eq!(decode_entities("a&lt;b&gt;c&quot;d&apos;e&amp;f"), "a<b>c\"d'e&f");
    }
}
