//! T104 acceptance tests: the blob port, its local backend, content addressing,
//! atomicity, and the three-way error classification.
//!
//! These are the ticket's Test-plan scenarios (port round-trip, atomicity and
//! durability, error classification, and the reference grammar the
//! `DurableOutput` bridge is built on), written FIRST and failing before the port
//! and its local implementation land.
//!
//! The bridge's own scenarios live in `crates/cli/tests/blob_bridge.rs`: they need
//! `dagr-core`'s `Payload` and `DurableOutput`, which this crate deliberately
//! cannot reach.
//!
//! Each test uses a private per-test temp directory so the suite is collision-proof
//! under CI parallelism (ubuntu + macOS).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_blob::{BlobKey, BlobRef, BlobStore, LocalFsBlob};

// === Test scaffolding ======================================================

/// A private per-test temp directory, created fresh and removed on drop. Never a
/// shared path: two tests running concurrently must not see each other's blobs.
struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "dagr-blob-t104-{tag}-{}-{nanos}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp root");
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

/// Every regular file under `dir`, recursively — how many blobs (and how much
/// debris) the store is actually holding.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

// ===========================================================================
// Port round-trip.
// ===========================================================================

#[test]
fn put_then_get_round_trips_the_bytes() {
    let root = TempRoot::new("round-trip");
    let store = LocalFsBlob::open(root.path());

    let key = store.put(b"the payload's encoded bytes").expect("put");
    let fetched = store.get(&key).expect("get");

    assert_eq!(
        fetched,
        b"the payload's encoded bytes",
        "get returns exactly the bytes put"
    );
}

#[test]
fn the_same_value_written_twice_is_one_blob_under_one_key() {
    let root = TempRoot::new("content-addressed");
    let store = LocalFsBlob::open(root.path());

    let first = store.put(b"identical bytes").expect("put 1");
    let second = store.put(b"identical bytes").expect("put 2");

    assert_eq!(
        first, second,
        "content addressing: the same bytes produce the same key"
    );
    assert_eq!(
        files_under(root.path()).len(),
        1,
        "the same value written twice is ONE blob on disk, not two"
    );
}

#[test]
fn two_different_values_get_different_keys() {
    let root = TempRoot::new("distinct");
    let store = LocalFsBlob::open(root.path());

    let a = store.put(b"value A").expect("put a");
    let b = store.put(b"value B").expect("put b");

    assert_ne!(a, b, "different values must not collide onto one key");
    assert_eq!(
        files_under(root.path()).len(),
        2,
        "two distinct values are two blobs"
    );
}

#[test]
fn a_key_is_the_digest_of_the_bytes_and_the_store_never_needed_to_be_asked() {
    // Content addressing means the key is derivable from the bytes alone — which
    // is what makes a reference self-verifying and dedup free.
    let root = TempRoot::new("derivable");
    let store = LocalFsBlob::open(root.path());

    let put_key = store.put(b"derive me").expect("put");
    let derived = BlobKey::of(b"derive me");

    assert_eq!(put_key, derived, "the key is a pure function of the bytes");
    assert_eq!(derived.algorithm(), "sha256");
    assert_eq!(
        derived.to_string(),
        format!("sha256:{}", derived.hex()),
        "a key renders as <algorithm>:<hex>, so a recorded hash carries its own algorithm"
    );
}

// ===========================================================================
// Atomicity and durability: a reader never observes a partial blob.
// ===========================================================================

#[test]
fn a_completed_put_leaves_the_object_and_no_write_temp_debris() {
    let root = TempRoot::new("no-debris");
    let store = LocalFsBlob::open(root.path());

    let key = store.put(b"durable bytes").expect("put");

    let files = files_under(root.path());
    assert_eq!(files.len(), 1, "exactly one file: the renamed object");
    assert_eq!(
        files[0],
        store.object_path(&key),
        "the surviving file is the atomically-renamed final name"
    );
    let name = files[0]
        .file_name()
        .and_then(|n| n.to_str())
        .expect("utf-8 name");
    assert!(
        !name.starts_with('.') && !name.contains(".tmp."),
        "no write-temp file survives a completed put (got `{name}`)"
    );
}

#[test]
fn write_temp_debris_from_an_interrupted_put_is_invisible_and_a_later_put_succeeds() {
    // The observable half of "interrupted before the rename": the crash leaves a
    // temp file in the object's directory and NO final name. A concurrent reader
    // sees absent (never a partial blob), and the next put still lands.
    //
    // The fault is injected at the write path itself in the crate's own unit test
    // (`crates/blob/src/local.rs`), which can reach the private step; this test
    // asserts the state that fault leaves behind is harmless.
    let root = TempRoot::new("interrupted");
    let store = LocalFsBlob::open(root.path());
    let key = BlobKey::of(b"interrupted bytes");
    let object = store.object_path(&key);
    let dir = object.parent().expect("object has a parent directory");
    std::fs::create_dir_all(dir).expect("mk object dir");

    // The debris an interrupted put leaves: a written, un-renamed temp file.
    let debris = dir.join(format!(
        ".{}.tmp.{}.0",
        object.file_name().and_then(|n| n.to_str()).expect("name"),
        std::process::id()
    ));
    std::fs::write(&debris, b"interrupted b").expect("write debris");

    let err = store.get(&key).expect_err("an un-renamed blob is not readable");
    assert!(
        err.is_absent(),
        "a reader sees ABSENT, never a partial blob: {err}"
    );
    assert!(
        store.head(&key).is_err_and(|e| e.is_absent()),
        "head agrees the object is absent"
    );

    let written = store.put(b"interrupted bytes").expect("a later put succeeds");
    assert_eq!(written, key);
    assert_eq!(
        store.get(&key).expect("readable now"),
        b"interrupted bytes",
        "the completed put is readable and complete"
    );
}

#[test]
fn a_put_overwrites_a_damaged_object_in_place_and_is_self_healing() {
    // The write is unconditional and atomic, so re-putting a value whose object
    // was damaged out-of-band repairs it rather than trusting the path's existence.
    let root = TempRoot::new("self-healing");
    let store = LocalFsBlob::open(root.path());
    let key = store.put(b"healthy bytes").expect("put");

    std::fs::write(store.object_path(&key), b"tampered").expect("tamper");
    assert!(
        store.get(&key).is_err_and(|e| e.is_corrupt()),
        "the damaged object is refused before repair"
    );

    store.put(b"healthy bytes").expect("re-put");
    assert_eq!(
        store.get(&key).expect("repaired"),
        b"healthy bytes",
        "a re-put restores the object under the same key"
    );
    assert_eq!(files_under(root.path()).len(), 1, "still one blob");
}

// ===========================================================================
// Error classification: absent vs transient vs corrupt.
// ===========================================================================

#[test]
fn a_missing_key_is_absent_from_both_get_and_head() {
    let root = TempRoot::new("missing");
    let store = LocalFsBlob::open(root.path());
    let key = BlobKey::of(b"never written");

    let get = store.get(&key).expect_err("missing key");
    assert!(get.is_absent(), "get reports absent: {get}");
    assert!(!get.is_transient() && !get.is_corrupt(), "exactly one class");
    assert!(
        get.to_string().contains("absent"),
        "the message names the class: {get}"
    );

    let head = store.head(&key).expect_err("missing key");
    assert!(head.is_absent(), "head reports absent: {head}");
}

#[test]
fn an_io_failure_that_is_not_a_missing_object_is_transient_not_absent() {
    // A missing object and an unreadable one are different facts, and only the
    // first is a dangling reference. Injected without depending on the test
    // process's privileges: the object path is a DIRECTORY, so the read fails with
    // something that is emphatically not "not found".
    let root = TempRoot::new("transient");
    let store = LocalFsBlob::open(root.path());
    let key = BlobKey::of(b"unreadable");
    std::fs::create_dir_all(store.object_path(&key)).expect("occupy the object path");

    let get = store.get(&key).expect_err("unreadable object");
    assert!(
        get.is_transient(),
        "an I/O failure that is not a missing object is TRANSIENT: {get}"
    );
    assert!(!get.is_absent(), "a transient failure is not a dangling one");
    assert!(
        std::error::Error::source(&get).is_some(),
        "the underlying io::Error is preserved as the source"
    );
}

#[test]
fn a_blob_whose_bytes_no_longer_match_its_key_is_corrupt() {
    let root = TempRoot::new("corrupt");
    let store = LocalFsBlob::open(root.path());
    let key = store.put(b"the original bytes").expect("put");

    // Overwrite the object out-of-band: it exists, it is readable, and it is wrong.
    std::fs::write(store.object_path(&key), b"someone else's bytes").expect("overwrite");

    let err = store.get(&key).expect_err("digest mismatch");
    assert!(
        err.is_corrupt(),
        "get reports CORRUPT, never a decoded value: {err}"
    );
    assert!(
        !err.is_absent() && !err.is_transient(),
        "corrupt is its own class"
    );
    assert!(
        err.to_string().contains(key.hex()),
        "the message names the expected digest: {err}"
    );
}

#[test]
fn head_reports_the_size_and_the_actual_current_hash() {
    let root = TempRoot::new("head");
    let store = LocalFsBlob::open(root.path());
    let key = store.put(b"twenty-four bytes long..").expect("put");

    let stat = store.head(&key).expect("head");
    assert_eq!(stat.size_bytes(), 24, "head reports the object's size");
    assert_eq!(
        stat.content_hash(),
        key.to_string(),
        "an intact object hashes to its own key"
    );

    // After an out-of-band overwrite `head` reports the ACTUAL hash — which is what
    // lets the resume gate say "changed", and name both hashes.
    std::fs::write(store.object_path(&key), b"different").expect("overwrite");
    let stat = store.head(&key).expect("head still succeeds — the object exists");
    assert_ne!(
        stat.content_hash(),
        key.to_string(),
        "the actual hash tracks the bytes, not the key"
    );
    assert_eq!(
        stat.content_hash(),
        BlobKey::of(b"different").to_string(),
        "the reported hash is the digest of what is actually stored"
    );
}

// ===========================================================================
// The reference grammar the durable-output bridge is built on.
// ===========================================================================

#[test]
fn a_reference_names_its_backend_container_and_key_and_round_trips() {
    let root = TempRoot::new("reference");
    let store = LocalFsBlob::open(root.path());
    let key = store.put(b"referenced bytes").expect("put");

    let reference = store.reference(&key);
    let text = reference.to_string();
    assert!(
        text.starts_with("dagr-blob+file://"),
        "the reference is self-describing about its backend: {text}"
    );
    assert!(
        text.ends_with(&format!("/sha256/{}", key.hex())),
        "and about its content address: {text}"
    );

    let parsed = BlobRef::parse(&text).expect("a reference we produced parses");
    assert_eq!(parsed, reference, "the grammar round-trips");
    assert_eq!(parsed.backend(), "file");
    assert_eq!(parsed.container(), root.path().to_string_lossy());
    assert_eq!(parsed.key(), &key);
}

#[test]
fn a_malformed_reference_is_refused_rather_than_guessed_at() {
    for bad in [
        "",
        "not-a-reference",
        "dagr-blob+file://",
        "dagr-blob+file:///root/sha256",
        "s3://bucket/sha256/abc",
        "dagr-blob+file:///root/sha256/",
    ] {
        assert!(
            BlobRef::parse(bad).is_err(),
            "`{bad}` is not a blob reference and must be refused"
        );
    }
}

#[test]
fn a_store_opened_at_the_container_a_reference_names_can_fetch_it() {
    // This is the property the bridge's `rehydrate` depends on: the reference
    // carries everything needed to reach the bytes again, in another process.
    let root = TempRoot::new("reopen");
    let key = LocalFsBlob::open(root.path())
        .put(b"cross-process bytes")
        .expect("put");
    let reference = LocalFsBlob::open(root.path()).reference(&key);

    let parsed = BlobRef::parse(&reference.to_string()).expect("parse");
    let reopened = LocalFsBlob::open(parsed.container());
    assert_eq!(
        reopened.get(parsed.key()).expect("fetch through the reference"),
        b"cross-process bytes"
    );
}
