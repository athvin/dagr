//! T110 acceptance tests: the **reclaim half of the port** — enumerate and
//! delete — for both backends.
//!
//! T104 deliberately shipped no `delete` and no listing, and recorded why:
//! reclaiming intermediate blobs is garbage collection, and a purely
//! content-addressed key shared across runs cannot be reclaimed by run age, only
//! by reachability. Reachability needs two operations the port did not have —
//! *what is in the store* and *remove this one* — and this suite pins them.
//!
//! The two hazards these tests exist for:
//!
//! * a blob store's container is **not** only blobs. Attempt shards (T106) live
//!   under the same container root in a sibling subtree, and an interrupted write
//!   leaves hidden temp debris. An enumeration that returned either would hand the
//!   reaper things it must never delete.
//! * an object store answers a listing in **pages**. A reaper that read the first
//!   page and stopped would silently under-report, which for a reclaim means
//!   leaking, and for a reachability set would mean deleting a live blob.
//!
//! Written FIRST and failing before the reclaim operations land.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_blob::s3::fake::FakeS3;
use dagr_blob::s3::{S3Blob, S3Config, S3Credentials};
use dagr_blob::{BlobKey, BlobReclaim, BlobStore, LocalFsBlob};

// === Test scaffolding ======================================================

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "dagr-reclaim-{label}-{}-{}",
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

fn credentials() -> S3Credentials {
    S3Credentials::new("AKIAIOSFODNN7EXAMPLE", "a-fabricated-secret")
}

fn sorted_hexes(keys: &[BlobKey]) -> Vec<String> {
    let mut out: Vec<String> = keys.iter().map(|k| k.hex().to_string()).collect();
    out.sort();
    out
}

// ===========================================================================
// Enumeration.
// ===========================================================================

#[test]
fn the_local_backend_enumerates_exactly_the_blobs_it_holds() {
    let root = TempRoot::new("list-local");
    let store = LocalFsBlob::open(root.path());
    assert!(
        store.list().expect("an empty store lists cleanly").is_empty(),
        "a store that was never written to holds nothing"
    );

    let a = store.put(b"alpha").expect("put");
    let b = store.put(b"beta").expect("put");
    let c = store.put(b"gamma").expect("put");

    assert_eq!(
        sorted_hexes(&store.list().expect("list")),
        sorted_hexes(&[a, b, c]),
        "every stored blob is enumerated, exactly once"
    );
}

#[test]
fn enumeration_ignores_attempt_shards_and_write_debris_in_the_same_container() {
    let root = TempRoot::new("list-siblings");
    let store = LocalFsBlob::open(root.path());
    let only = store.put(b"the one real blob").expect("put");

    // An attempt shard (T106) lives under the SAME container root, in a sibling
    // subtree. It is not a blob and a reaper must never see it as one.
    let shard_dir = root.path().join("attempt-shards").join("run-1").join("abcd");
    std::fs::create_dir_all(&shard_dir).expect("plant a shard directory");
    std::fs::write(shard_dir.join("1.jsonl"), b"{\"kind\":\"shard-header\"}\n")
        .expect("plant a shard");

    // Debris from an interrupted write: a hidden temp name beside a real object.
    let object = store.object_path(&only);
    let debris = object
        .parent()
        .expect("an object always has a parent")
        .join(format!(".{}.tmp.999.0", only.hex()));
    std::fs::write(&debris, b"a partially written blob").expect("plant debris");

    assert_eq!(
        sorted_hexes(&store.list().expect("list")),
        vec![only.hex().to_string()],
        "only content-addressed objects are enumerated"
    );
    assert!(
        shard_dir.join("1.jsonl").is_file(),
        "the shard is untouched by enumeration"
    );
}

#[test]
fn the_object_store_backend_enumerates_across_pages() {
    let fake = FakeS3::new("paged-bucket");
    // Force the fixture to answer a listing in small pages, so a reader that
    // stopped at the first one under-reports and fails this test.
    fake.set_list_page_size(2);
    let store = S3Blob::new(S3Config::new("paged-bucket"), credentials(), fake);

    let mut stored = Vec::new();
    for i in 0..7u32 {
        stored.push(store.put(format!("value-{i}").as_bytes()).expect("put"));
    }

    assert_eq!(
        sorted_hexes(&store.list().expect("list")),
        sorted_hexes(&stored),
        "every page of the listing is followed"
    );
}

#[test]
fn the_object_store_enumeration_is_scoped_to_the_configured_prefix() {
    let fake = FakeS3::new("shared-bucket");
    let mine = S3Blob::new(
        S3Config::new("shared-bucket").with_prefix("dagr"),
        credentials(),
        fake.clone(),
    );
    let key = mine.put(b"a dagr blob").expect("put");

    // Something else's object, in the same bucket, outside the prefix.
    fake.insert_object("elsewhere/sha256/deadbeef", b"not ours".to_vec());

    assert_eq!(
        sorted_hexes(&mine.list().expect("list")),
        vec![key.hex().to_string()],
        "a store enumerates its own container, not the whole bucket"
    );
}

// ===========================================================================
// Deletion.
// ===========================================================================

#[test]
fn deleting_a_blob_removes_it_from_both_backends_and_is_idempotent() {
    let root = TempRoot::new("delete-local");
    let local = LocalFsBlob::open(root.path());
    let fake = FakeS3::new("delete-bucket");
    let s3 = S3Blob::new(S3Config::new("delete-bucket"), credentials(), fake);

    for (label, store) in [
        ("file", &local as &dyn BlobReclaimAndStore),
        ("s3", &s3 as &dyn BlobReclaimAndStore),
    ] {
        let key = store.put_bytes(b"a blob about to be reclaimed").expect("put");
        store.delete_key(&key).unwrap_or_else(|e| panic!("{label}: delete: {e}"));
        assert!(
            store.head_key(&key).expect_err("it is gone").is_absent(),
            "{label}: a deleted blob is absent"
        );
        assert!(
            store.list_keys().expect("list").is_empty(),
            "{label}: and it is gone from the enumeration"
        );
        // Reclaiming twice is not an error: a reaper that raced another reaper
        // must not fail, and "already gone" is the outcome it wanted.
        store
            .delete_key(&key)
            .unwrap_or_else(|e| panic!("{label}: a second delete is a no-op, got {e}"));
    }
}

#[test]
fn deletion_does_not_disturb_the_blobs_it_was_not_asked_about() {
    let root = TempRoot::new("delete-neighbours");
    let store = LocalFsBlob::open(root.path());
    let doomed = store.put(b"reclaim me").expect("put");
    let kept = store.put(b"keep me").expect("put");

    store.delete(&doomed).expect("delete");

    assert_eq!(
        sorted_hexes(&store.list().expect("list")),
        vec![kept.hex().to_string()]
    );
    assert_eq!(store.get(&kept).expect("still readable"), b"keep me");
}

#[test]
fn an_unreachable_object_store_refuses_to_enumerate_rather_than_reporting_nothing() {
    let fake = FakeS3::new("down-bucket");
    let store = S3Blob::new(S3Config::new("down-bucket"), credentials(), fake.clone());
    store.put(b"present").expect("put");
    fake.set_unreachable(true);

    let err = store.list().expect_err("an unreachable store cannot enumerate");
    assert!(
        err.is_transient(),
        "an unreachable store is transient — an empty listing would read as \
         'nothing is stored', which is how a reaper deletes a live blob: {err}"
    );
}

// === A tiny object-safe façade so the two backends share one test body =====

/// Both halves of the port at once, object-safe, so the deletion test above runs
/// the *same* body against both backends instead of two bodies that agree today.
trait BlobReclaimAndStore {
    fn put_bytes(&self, bytes: &[u8]) -> Result<BlobKey, dagr_blob::BlobError>;
    fn head_key(&self, key: &BlobKey) -> Result<dagr_blob::BlobStat, dagr_blob::BlobError>;
    fn list_keys(&self) -> Result<Vec<BlobKey>, dagr_blob::BlobError>;
    fn delete_key(&self, key: &BlobKey) -> Result<(), dagr_blob::BlobError>;
}

impl<T: BlobStore + BlobReclaim> BlobReclaimAndStore for T {
    fn put_bytes(&self, bytes: &[u8]) -> Result<BlobKey, dagr_blob::BlobError> {
        self.put(bytes)
    }
    fn head_key(&self, key: &BlobKey) -> Result<dagr_blob::BlobStat, dagr_blob::BlobError> {
        self.head(key)
    }
    fn list_keys(&self) -> Result<Vec<BlobKey>, dagr_blob::BlobError> {
        self.list()
    }
    fn delete_key(&self, key: &BlobKey) -> Result<(), dagr_blob::BlobError> {
        self.delete(key)
    }
}
