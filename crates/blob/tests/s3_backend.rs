//! T110 acceptance tests: the **S3-compatible backend** behind the T104 port.
//!
//! Two things are being proved here, and they are deliberately separate:
//!
//! * **Parity.** The assertions T104's port suite makes about `LocalFsBlob` —
//!   round-trip, content addressing, deterministic keys, the three-way error
//!   split — hold *identically* for `S3Blob`. They are written once, as a
//!   backend-agnostic suite, and run against both stores, so "the same tests,
//!   both backends" is a fact about one body of code rather than two bodies that
//!   happen to agree today.
//! * **What object storage adds.** Transient-versus-absent (the distinction that
//!   protects resume from a spurious `DanglingReference`), the bounded retry, and
//!   credentials that come from the ambient environment and never appear in an
//!   error.
//!
//! No network service is involved anywhere: the backend is written against a
//! sans-IO transport port and driven here by an in-process S3-compatible fixture
//! (`dagr_blob::s3::fake::FakeS3`) that can be made unreachable, made to fail a
//! bounded number of times, or have an object overwritten out-of-band on demand.
//! That is what makes a "the store is down" test deterministic instead of a race
//! against a real endpoint.
//!
//! Written FIRST and failing before the backend lands.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_blob::retry::{RetryBudget, Sleeper};
use dagr_blob::s3::fake::FakeS3;
use dagr_blob::s3::{S3Blob, S3Config, S3Credentials};
use dagr_blob::{BlobRef, BlobStore, LocalFsBlob};

// === Test scaffolding ======================================================

/// A private per-test temp directory, created fresh and removed on drop — the
/// same discipline the T104 suite uses, so two tests running concurrently under
/// CI parallelism never see each other's blobs.
struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "dagr-s3-{label}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create the private test root");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A sleeper that records what it was asked to wait for and returns immediately,
/// so a retry test asserts the *schedule* instead of racing it.
#[derive(Debug, Default, Clone)]
struct RecordingSleeper {
    delays: Arc<Mutex<Vec<Duration>>>,
}

impl RecordingSleeper {
    fn delays(&self) -> Vec<Duration> {
        self.delays.lock().expect("the sleeper log is poisoned").clone()
    }
}

impl Sleeper for RecordingSleeper {
    fn sleep(&self, delay: Duration) {
        self.delays
            .lock()
            .expect("the sleeper log is poisoned")
            .push(delay);
    }
}

/// The credentials every fixture signs with. They are fabricated, and the
/// secret carries a sentinel so a leak into an error or a `Debug` rendering is
/// detectable by substring rather than by inspection.
const SECRET_SENTINEL: &str = "SENTINEL-b41c7e-DO-NOT-LEAK-93af2";

fn test_credentials() -> S3Credentials {
    S3Credentials::new("AKIAIOSFODNN7EXAMPLE", SECRET_SENTINEL)
}

/// An `S3Blob` over a fresh in-process fixture, with retry turned down so a test
/// that never expects a retry cannot accidentally hide one.
fn s3_store(bucket: &str) -> (S3Blob<FakeS3>, FakeS3) {
    let fake = FakeS3::new(bucket);
    let store = S3Blob::new(
        S3Config::new(bucket).region("eu-west-2"),
        test_credentials(),
        fake.clone(),
    )
    .with_retry(RetryBudget::new(1, Duration::ZERO, 2.0, Duration::ZERO));
    (store, fake)
}

// ===========================================================================
// Backend parity — the same assertions, both backends.
// ===========================================================================

/// The backend-agnostic half of T104's port suite, written once. Everything here
/// is a property of the *port*, not of a filesystem or of HTTP, so a backend that
/// fails any of it is not interchangeable with the other one.
fn port_suite(store: &dyn BlobStore, backend: &str) {
    // Round-trip.
    let bytes = b"the encoded payload for the parity suite".to_vec();
    let key = store.put(&bytes).unwrap_or_else(|e| panic!("{backend}: put: {e}"));
    assert_eq!(
        store.get(&key).unwrap_or_else(|e| panic!("{backend}: get: {e}")),
        bytes,
        "{backend}: put then get round-trips the bytes"
    );

    // Content addressing: the same value twice is one key, computable by anyone.
    let again = store.put(&bytes).expect("a second put of the same bytes");
    assert_eq!(key, again, "{backend}: the same bytes land under the same key");
    assert!(
        key.matches(&bytes),
        "{backend}: the key is the digest of the bytes"
    );

    // Deterministic and value-dependent.
    let other = store.put(b"a different value").expect("put a second value");
    assert_ne!(key, other, "{backend}: different values get different keys");

    // head reports the size and the ACTUAL current hash.
    let stat = store.head(&key).unwrap_or_else(|e| panic!("{backend}: head: {e}"));
    assert_eq!(stat.size_bytes(), bytes.len() as u64, "{backend}: head size");
    assert_eq!(
        stat.content_hash(),
        key.to_string(),
        "{backend}: head reports the measured content hash"
    );

    // A missing key is ABSENT from both get and head — never transient, never a
    // decoded value.
    let missing = dagr_blob::BlobKey::of(b"a value that was never stored anywhere");
    let get_err = store.get(&missing).expect_err("a missing key cannot be read");
    assert!(
        get_err.is_absent(),
        "{backend}: a missing key is absent from get, got {get_err}"
    );
    let head_err = store.head(&missing).expect_err("a missing key cannot be probed");
    assert!(
        head_err.is_absent(),
        "{backend}: a missing key is absent from head, got {head_err}"
    );

    // The reference names this backend and round-trips through the grammar.
    let reference = store.reference(&key);
    assert_eq!(reference.backend(), backend);
    assert_eq!(reference.container(), store.container());
    let parsed = BlobRef::parse(&reference.to_string()).expect("the reference round-trips");
    assert_eq!(&parsed, &reference, "{backend}: reference round-trip");
}

#[test]
fn the_port_suite_holds_identically_for_the_local_and_the_object_store_backends() {
    let root = TempRoot::new("parity");
    port_suite(&LocalFsBlob::open(root.path()), "file");

    let (s3, _fake) = s3_store("parity-bucket");
    port_suite(&s3, "s3");
}

#[test]
fn the_object_store_backend_names_its_bucket_and_prefix_as_the_container() {
    let fake = FakeS3::new("dagr-blobs");
    let store = S3Blob::new(
        S3Config::new("dagr-blobs").prefix("intermediates"),
        test_credentials(),
        fake,
    );
    assert_eq!(store.backend(), "s3");
    assert_eq!(store.container(), "dagr-blobs/intermediates");

    let key = store.put(b"x").expect("put");
    let reference = store.reference(&key).to_string();
    assert!(
        reference.starts_with("dagr-blob+s3://dagr-blobs/intermediates/sha256/"),
        "the reference uses the uniform grammar: {reference}"
    );
    // And it parses back into exactly the pieces a rehydrate needs.
    let parsed = BlobRef::parse(&reference).expect("parse");
    assert_eq!(parsed.backend(), "s3");
    assert_eq!(parsed.container(), "dagr-blobs/intermediates");
    assert_eq!(parsed.key(), &key);
}

#[test]
fn stored_bytes_that_no_longer_hash_to_their_key_are_corrupt_not_a_value() {
    let (store, fake) = s3_store("corrupt-bucket");
    let key = store.put(b"the original bytes").expect("put");
    fake.overwrite_object(&store.object_key(&key), b"different bytes entirely".to_vec());

    let err = store.get(&key).expect_err("corrupt bytes are never returned");
    assert!(err.is_corrupt(), "expected corrupt, got {err}");
    assert!(
        err.to_string().contains(key.hex()),
        "the refusal names the digest it wanted: {err}"
    );

    // head does NOT report corruption: it reports the actual hash and lets the
    // caller decide, which is what makes `Changed` expressible.
    let stat = store.head(&key).expect("head still succeeds");
    assert_ne!(
        stat.content_hash(),
        key.to_string(),
        "head reports the measured hash of what is actually there"
    );
}

// ===========================================================================
// Transient vs absent — the distinction that protects resume.
// ===========================================================================

#[test]
fn an_unreachable_store_is_transient_and_never_absent() {
    let (store, fake) = s3_store("unreachable-bucket");
    let key = store.put(b"stored while the store was up").expect("put");
    fake.set_unreachable(true);

    let head_err = store.head(&key).expect_err("an unreachable store cannot answer");
    assert!(
        head_err.is_transient() && !head_err.is_absent(),
        "an unreachable store is transient, never absent: {head_err}"
    );
    let get_err = store.get(&key).expect_err("an unreachable store cannot answer");
    assert!(get_err.is_transient(), "get is transient too: {get_err}");
    let put_err = store.put(b"anything").expect_err("an unreachable store cannot answer");
    assert!(put_err.is_transient(), "put is transient too: {put_err}");
}

#[test]
fn a_server_error_is_transient_and_a_not_found_is_absent() {
    let (store, fake) = s3_store("status-bucket");
    let key = store.put(b"present").expect("put");

    fake.respond_next_with_status(1, 500);
    let err = store.head(&key).expect_err("a 500 is not an answer");
    assert!(err.is_transient(), "a 5xx is transient: {err}");

    // 404 is the one status that means the referent is gone.
    let missing = dagr_blob::BlobKey::of(b"never stored");
    let err = store.head(&missing).expect_err("a missing object");
    assert!(err.is_absent(), "a 404 is absent: {err}");
}

#[test]
fn a_permission_failure_is_transient_not_absent_and_is_not_retried() {
    let (store, fake) = s3_store("forbidden-bucket");
    let key = store.put(b"present").expect("put");
    let before = fake.request_count();

    fake.respond_with_status_until_cleared(403);
    let err = store.head(&key).expect_err("a 403 is not an answer");
    assert!(
        err.is_transient() && !err.is_absent(),
        "a permission failure is never evidence of deletion: {err}"
    );
    assert_eq!(
        fake.request_count() - before,
        1,
        "a permanent failure is not retried — retrying it would only waste the budget"
    );
    assert!(
        !err.to_string().contains(SECRET_SENTINEL),
        "no credential value appears in the error: {err}"
    );
}

// ===========================================================================
// The bounded retry.
// ===========================================================================

#[test]
fn a_transient_failure_that_resolves_within_the_bound_succeeds_after_retry() {
    let fake = FakeS3::new("retry-bucket");
    let sleeper = RecordingSleeper::default();
    let store = S3Blob::new(
        S3Config::new("retry-bucket"),
        test_credentials(),
        fake.clone(),
    )
    .with_retry(RetryBudget::new(
        4,
        Duration::from_millis(10),
        2.0,
        Duration::from_secs(1),
    ))
    .with_sleeper(Arc::new(sleeper.clone()));

    fake.fail_next(2);
    let key = store
        .put(b"written on the third attempt")
        .expect("the write succeeds once the store comes back");
    assert_eq!(
        store.get(&key).expect("and it is readable"),
        b"written on the third attempt"
    );

    // Two failures ⇒ two waits, on the engine's backoff shape: base·factor^n.
    assert_eq!(
        sleeper.delays(),
        vec![Duration::from_millis(10), Duration::from_millis(20)],
        "the backoff is the engine's shape, not a second policy"
    );
}

#[test]
fn a_transient_failure_that_outlives_the_bound_surfaces_with_the_attempt_count() {
    let fake = FakeS3::new("exhausted-bucket");
    let sleeper = RecordingSleeper::default();
    let store = S3Blob::new(
        S3Config::new("exhausted-bucket"),
        test_credentials(),
        fake.clone(),
    )
    .with_retry(RetryBudget::new(
        3,
        Duration::from_millis(5),
        2.0,
        Duration::from_secs(1),
    ))
    .with_sleeper(Arc::new(sleeper.clone()));

    fake.set_unreachable(true);
    let err = store.put(b"never lands").expect_err("the bound is exhausted");
    assert!(err.is_transient(), "still transient, never absent: {err}");
    assert!(
        err.to_string().contains('3'),
        "the failure names how many attempts were spent: {err}"
    );
    assert_eq!(
        fake.request_count(),
        3,
        "exactly the budgeted number of attempts was spent"
    );
    assert_eq!(sleeper.delays().len(), 2, "one wait between each pair");
}

#[test]
fn the_retry_budget_is_the_engines_backoff_shape() {
    let budget = RetryBudget::new(5, Duration::from_millis(100), 2.0, Duration::from_secs(1));
    assert_eq!(budget.attempts(), 5);
    assert_eq!(budget.nominal_delay(0), Duration::from_millis(100));
    assert_eq!(budget.nominal_delay(1), Duration::from_millis(200));
    assert_eq!(budget.nominal_delay(2), Duration::from_millis(400));
    assert_eq!(budget.nominal_delay(3), Duration::from_millis(800));
    // Clamped by the cap, and never beyond it.
    assert_eq!(budget.nominal_delay(4), Duration::from_secs(1));
    assert_eq!(budget.nominal_delay(64), Duration::from_secs(1));

    // A budget can never be zero-attempt: that would be a store that refuses to
    // try at all.
    assert_eq!(RetryBudget::new(0, Duration::ZERO, 2.0, Duration::ZERO).attempts(), 1);
}

// ===========================================================================
// Credentials.
// ===========================================================================

#[test]
fn a_missing_credential_names_what_was_looked_for_and_is_not_a_missing_object() {
    let err = S3Credentials::from_ambient_environment_in(|_| None)
        .expect_err("no credentials are available");
    let text = err.to_string();
    assert!(
        text.contains("AWS_ACCESS_KEY_ID") && text.contains("AWS_SECRET_ACCESS_KEY"),
        "the failure names what was looked for: {text}"
    );
    // Distinguishable from a missing object: it is not a `BlobError` at all, and
    // it never renders as one.
    assert!(
        !text.contains("absent"),
        "a missing credential is not a missing object: {text}"
    );
}

#[test]
fn credentials_come_from_the_ambient_environment_in_the_documented_order() {
    let creds = S3Credentials::from_ambient_environment_in(|name| match name {
        "AWS_ACCESS_KEY_ID" => Some("AKIAIOSFODNN7EXAMPLE".to_string()),
        "AWS_SECRET_ACCESS_KEY" => Some(SECRET_SENTINEL.to_string()),
        "AWS_SESSION_TOKEN" => Some("a-projected-session-token".to_string()),
        _ => None,
    })
    .expect("the environment tier supplies them");
    assert_eq!(creds.access_key_id(), "AKIAIOSFODNN7EXAMPLE");
    assert!(creds.has_session_token(), "an injected session token is carried");
}

#[test]
fn no_credential_value_appears_in_a_debug_rendering_or_an_error() {
    let creds = test_credentials();
    let rendered = format!("{creds:?}");
    assert!(
        !rendered.contains(SECRET_SENTINEL),
        "the secret must never be rendered: {rendered}"
    );
    assert!(
        rendered.contains("redacted"),
        "and the redaction is visible rather than silent: {rendered}"
    );

    // The store carries them, so it must redact them too.
    let (store, fake) = s3_store("redaction-bucket");
    let rendered = format!("{store:?}");
    assert!(
        !rendered.contains(SECRET_SENTINEL),
        "the store's Debug must not leak the secret: {rendered}"
    );

    // And no failure path renders them either.
    fake.set_unreachable(true);
    let err = store.put(b"anything").expect_err("unreachable");
    assert!(
        !format!("{err}").contains(SECRET_SENTINEL) && !format!("{err:?}").contains(SECRET_SENTINEL),
        "the error must not leak the secret: {err}"
    );
}

#[test]
fn every_request_is_signed_and_the_signature_carries_no_secret() {
    let (store, fake) = s3_store("signing-bucket");
    store.put(b"signed").expect("put");

    let signed = fake.authorizations();
    assert!(!signed.is_empty(), "the fixture saw at least one request");
    for header in &signed {
        assert!(
            header.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"),
            "SigV4 authorization header: {header}"
        );
        assert!(
            !header.contains(SECRET_SENTINEL),
            "the signature is derived from the secret, never carries it: {header}"
        );
    }
}
