//! The **local filesystem backend**: content-addressed blobs under a configured
//! root, written atomically.

use std::io;
use std::path::{Path, PathBuf};

use crate::digest::{Sha256, to_hex};
use crate::store::{BlobError, BlobKey, BlobStat, BlobStore};

/// The backend name this store writes into a reference.
const BACKEND: &str = "file";

/// How much of a file is read at a time when hashing it — enough that the read
/// syscalls are not the cost, small enough that probing a large blob does not
/// depend on it fitting in memory.
const HASH_CHUNK: usize = 64 * 1024;

/// A blob store over a directory tree.
///
/// # Layout
///
/// ```text
/// <root>/<algorithm>/<aa>/<bb>/<hex>
/// ```
///
/// Two levels of fan-out on the digest's leading bytes keep any one directory
/// small as blobs accumulate across runs. The **reference** a caller records names
/// the blob logically —
/// `dagr-blob+file://<root>/<algorithm>/<hex>` — and the fan-out is this store's
/// business; [`object_path`](LocalFsBlob::object_path) is the mapping, and it is
/// public because "where does this blob actually live" is an operator question.
///
/// # Durability
///
/// A write follows the discipline the durable scratch store uses: write a temp
/// file **in the same directory**, `fsync` it, atomically `rename` it into place,
/// then `fsync` the containing directory so the rename itself survives a crash. A
/// reader therefore observes either no object or the complete object — never a
/// partial blob — because only the atomically-renamed final name is ever read.
///
/// The write is **unconditional**: a `put` of bytes whose object already exists
/// rewrites it rather than trusting the path's existence, so an object damaged
/// out-of-band is repaired by the next put of the same value. Content addressing
/// makes that safe — the bytes are the same bytes — and it keeps one code path.
///
/// # ⚠ Pod-to-pod handoff needs a read-write-many volume
///
/// This store serves a handoff between two processes only when they see the same
/// directory. Across pods that means an **RWX** volume mounted at the same path in
/// every pod — a legitimate production configuration where the cluster's CSI
/// driver offers one, and **not** something to assume: a CSI driver that only
/// offers `ReadWriteOnce` cannot provision a shared root at all, and on such a
/// cluster this backend serves single-machine runs and nothing wider. Verify with
/// `kubectl get sc` and the driver's supported access modes before planning on it.
///
/// # This API blocks
///
/// Every operation is ordinary blocking file I/O, and `put` pays two `fsync`s on
/// the calling thread. That is the price of the guarantee above; a caller on an
/// async worker should treat it the way it treats the scratch store.
#[derive(Debug, Clone)]
pub struct LocalFsBlob {
    root: PathBuf,
}

impl LocalFsBlob {
    /// Open a store rooted at `root`. Performs **no** I/O: the root and its
    /// subdirectories are created lazily on the first write, so opening a store
    /// for a container that does not exist yet is not an error.
    #[must_use]
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The directory this store is rooted at.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where `key`'s object lives on disk — `<root>/<algorithm>/<aa>/<bb>/<hex>`.
    #[must_use]
    pub fn object_path(&self, key: &BlobKey) -> PathBuf {
        let hex = key.hex();
        // `from_parts`/`of` guarantee 64 hex digits, so the slices are in bounds;
        // the `get` form keeps the function total for a hand-built key anyway.
        let aa = hex.get(0..2).unwrap_or("00");
        let bb = hex.get(2..4).unwrap_or("00");
        self.root.join(key.algorithm()).join(aa).join(bb).join(hex)
    }

    /// Write `bytes` to `key`'s object path atomically.
    ///
    /// `stop_before_rename` is the **fault injection point**: with it set, the
    /// temp file is written and fsynced but never renamed, which is exactly the
    /// state a crash mid-put leaves behind. Nothing outside this module's tests
    /// passes `true` — the public [`put`](BlobStore::put) always renames.
    fn write_object(
        &self,
        key: &BlobKey,
        bytes: &[u8],
        stop_before_rename: bool,
    ) -> io::Result<()> {
        use std::io::Write as _;

        let final_path = self.object_path(key);
        let dir = final_path
            .parent()
            .ok_or_else(|| io::Error::other("a blob object path always has a parent directory"))?;
        std::fs::create_dir_all(dir)?;

        // A temp name unique to this write, in the SAME directory as the final
        // file so the rename stays on one filesystem and is therefore atomic. The
        // leading dot plus `.tmp.` marks it as debris no reader ever opens: a
        // reader only ever opens the final, hex-named path.
        let tmp_path = dir.join(format!(
            ".{}.tmp.{}.{}",
            final_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("blob"),
            std::process::id(),
            next_tmp_counter(),
        ));

        let write_result = (|| -> io::Result<()> {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(bytes)?;
            file.sync_all()?; // fsync the bytes before the rename names them
            if stop_before_rename {
                return Ok(());
            }
            std::fs::rename(&tmp_path, &final_path)?; // atomic replace
            Ok(())
        })();

        if write_result.is_err() || stop_before_rename {
            if write_result.is_err() {
                let _ = std::fs::remove_file(&tmp_path); // no debris from a failed write
            }
            return write_result;
        }

        // fsync the directory so the rename entry itself is durable across a crash.
        // A directory that cannot be opened for fsync is not fatal to the write
        // (the bytes and the rename already landed); swallow only that.
        if let Ok(handle) = std::fs::File::open(dir) {
            let _ = handle.sync_all();
        }
        Ok(())
    }

    /// Classify an I/O failure: a missing object is **absent**, everything else is
    /// **transient**. The distinction is load-bearing — only the first is a
    /// dangling reference.
    fn classify(err: io::Error, key: &BlobKey, what: &str) -> BlobError {
        if err.kind() == io::ErrorKind::NotFound {
            BlobError::absent(format!("no object under `{key}`")).with_source(err)
        } else {
            BlobError::transient(format!("could not {what} `{key}`: {err}")).with_source(err)
        }
    }
}

impl BlobStore for LocalFsBlob {
    fn backend(&self) -> &str {
        BACKEND
    }

    fn container(&self) -> String {
        self.root.to_string_lossy().into_owned()
    }

    fn put(&self, bytes: &[u8]) -> Result<BlobKey, BlobError> {
        let key = BlobKey::of(bytes);
        self.write_object(&key, bytes, false).map_err(|err| {
            BlobError::transient(format!(
                "could not write `{key}` under `{}`: {err}",
                self.root.display()
            ))
            .with_source(err)
        })?;
        Ok(key)
    }

    fn get(&self, key: &BlobKey) -> Result<Vec<u8>, BlobError> {
        let bytes =
            std::fs::read(self.object_path(key)).map_err(|err| Self::classify(err, key, "read"))?;
        if !key.matches(&bytes) {
            return Err(BlobError::corrupt(format!(
                "the object stored for `{key}` hashes to `{}` — it was not written by this key, \
                 so its bytes are not returned",
                BlobKey::of(&bytes)
            )));
        }
        Ok(bytes)
    }

    fn head(&self, key: &BlobKey) -> Result<BlobStat, BlobError> {
        use std::io::Read as _;

        let path = self.object_path(key);
        let mut file =
            std::fs::File::open(&path).map_err(|err| Self::classify(err, key, "open"))?;
        let mut hasher = Sha256::new();
        let mut size: u64 = 0;
        let mut buffer = vec![0u8; HASH_CHUNK];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|err| Self::classify(err, key, "read"))?;
            if read == 0 {
                break;
            }
            let chunk = buffer.get(..read).unwrap_or(&[]);
            hasher.update(chunk);
            size = size.saturating_add(u64::try_from(read).unwrap_or(0));
        }
        Ok(BlobStat::new(
            size,
            format!("{}:{}", key.algorithm(), to_hex(&hasher.finish())),
        ))
    }
}

/// A per-process monotonic counter so two temp files in one directory never share
/// a name within a process. Deterministic and lock-free.
fn next_tmp_counter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{LocalFsBlob, next_tmp_counter};
    use crate::store::{BlobKey, BlobStore};

    /// A private temp root for one unit test.
    fn temp_root(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "dagr-blob-unit-{tag}-{}-{nanos}-{}",
            std::process::id(),
            next_tmp_counter()
        ));
        std::fs::create_dir_all(&path).expect("create temp root");
        path
    }

    /// **Fault injection at the write path.** A put interrupted before the rename
    /// leaves the temp file and no final name, so a concurrent `get` sees absent —
    /// never a partial blob — and the next put still lands.
    ///
    /// This is the half an integration test cannot reach: it drives the private
    /// write step directly rather than simulating the state it leaves.
    #[test]
    fn a_put_interrupted_before_the_rename_is_invisible_and_recoverable() {
        let root = temp_root("interrupted");
        let store = LocalFsBlob::open(&root);
        let key = BlobKey::of(b"interrupted payload");

        store
            .write_object(&key, b"interrupted payload", true)
            .expect("the write itself succeeds; only the rename is skipped");

        // The final name does not exist, so every reader sees ABSENT.
        assert!(!store.object_path(&key).exists());
        assert!(
            store.get(&key).is_err_and(|e| e.is_absent()),
            "a reader never observes a partial blob"
        );
        assert!(store.head(&key).is_err_and(|e| e.is_absent()));

        // The temp file is there — that is what a crash leaves — and it is debris,
        // not an object: it carries the write-temp marker, so it can never be
        // mistaken for a blob (whose name is hex).
        let dir = store.object_path(&key);
        let dir = dir.parent().expect("parent");
        let leftovers: Vec<_> = std::fs::read_dir(dir)
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            leftovers.len(),
            1,
            "the interrupted temp file: {leftovers:?}"
        );
        assert!(
            leftovers[0].starts_with('.') && leftovers[0].contains(".tmp."),
            "the leftover is marked as write-temp debris: {leftovers:?}"
        );

        // And the next put still succeeds, over the top of the debris.
        let written = store
            .put(b"interrupted payload")
            .expect("a later put lands");
        assert_eq!(written, key);
        assert_eq!(store.get(&key).expect("readable"), b"interrupted payload");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// A completed put leaves the object at the hex-named final path and no debris
    /// — the observable consequence of write-temp → fsync → rename → fsync-dir.
    #[test]
    fn a_completed_put_leaves_only_the_renamed_object() {
        let root = temp_root("completed");
        let store = LocalFsBlob::open(&root);
        let key = store.put(b"complete payload").expect("put");

        let object = store.object_path(&key);
        assert!(object.is_file(), "the object is at its final path");
        let dir = object.parent().expect("parent");
        let names: Vec<_> = std::fs::read_dir(dir)
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![key.hex().to_string()], "no debris survives");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// The fan-out is derived from the digest, so two keys sharing a prefix share
    /// directories and everything else spreads out.
    #[test]
    fn the_object_path_fans_out_on_the_digest() {
        let store = LocalFsBlob::open("/blobs");
        let key = BlobKey::of(b"x");
        let hex = key.hex();
        assert_eq!(
            store.object_path(&key),
            std::path::Path::new("/blobs")
                .join("sha256")
                .join(&hex[0..2])
                .join(&hex[2..4])
                .join(hex)
        );
    }
}
