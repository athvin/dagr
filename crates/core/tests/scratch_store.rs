//! Behavioral tests for the **C18 durable scratch store** (local) — ticket T53 /
//! 065. Written first, TDD: each test mirrors one bullet of the ticket's Test
//! plan.
//!
//! Every test uses a **private per-test temp directory** under the OS temp dir,
//! keyed by process id, a monotonic counter, and a nanosecond stamp, so parallel
//! test threads never share a path (the shared-`/tmp` parallelism bug class that
//! has bitten this repo's CI). The base is removed on drop. No runtime, no
//! admission, no event stream — the C8 single-task path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_core::context::{PipelineId, RunContext, RunId};
use dagr_core::error::TaskError;
use dagr_core::handle::NodeId;
use dagr_core::scratch::{ScratchError, ScratchStore};

/// A **private** temp base unique to one test, removed on drop. The name blends
/// the pid, a per-process monotonic counter, and a nanosecond stamp so two tests
/// running concurrently — or two runs of the suite — never collide on a path.
struct TempBase {
    path: PathBuf,
}

impl TempBase {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let unique = format!(
            "dagr-scratch-test-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            nanos,
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).expect("create private temp base");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Build a store for one node directly under a base (no context needed).
fn store_for(base: &Path, pipeline: &str, run: &str, node: &str) -> ScratchStore {
    ScratchStore::for_node(
        base,
        &PipelineId::new(pipeline),
        &RunId::new(run),
        NodeId::from_name(node),
    )
}

// ---------------------------------------------------------------------------
// Write-then-read within a namespace.
// ---------------------------------------------------------------------------

#[test]
fn write_then_read_returns_exact_bytes() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");

    store
        .put(b"cursor", b"high-water-42")
        .expect("write succeeds");
    let got = store.get(b"cursor").expect("read succeeds");
    assert_eq!(
        got.as_deref(),
        Some(&b"high-water-42"[..]),
        "read returns exactly the bytes written, byte-for-byte"
    );
}

#[test]
fn opaque_bytes_round_trip_including_non_utf8_and_empty() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");

    // Non-UTF-8 value and a key with bytes that are hostile as a path.
    let key: &[u8] = b"../weird/key\x00.name";
    let value: &[u8] = &[0x00, 0xff, 0x10, b'\n', 0x80];
    store.put(key, value).expect("write opaque bytes");
    assert_eq!(store.get(key).expect("read").as_deref(), Some(value));

    // An empty value is a value (distinct from absent).
    store.put(b"empty", b"").expect("write empty value");
    assert_eq!(
        store.get(b"empty").expect("read").as_deref(),
        Some(&b""[..])
    );
}

// ---------------------------------------------------------------------------
// Absent key is distinct from failure.
// ---------------------------------------------------------------------------

#[test]
fn absent_key_is_ok_none_not_an_error() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");

    let got = store.get(b"never-written");
    assert!(
        matches!(got, Ok(None)),
        "a never-written key is the well-defined absent outcome, not an I/O error"
    );
}

// ---------------------------------------------------------------------------
// Value survives across attempts (attempt-1-write / attempt-2-read).
// ---------------------------------------------------------------------------

#[test]
fn value_written_on_attempt_one_readable_on_attempt_two_via_context() {
    let base = TempBase::new();

    // Attempt 1: reach scratch through a hand-built context configured at attempt 1.
    let attempt1 = RunContext::builder(
        RunId::new("run-x"),
        PipelineId::new("pipe"),
        NodeId::from_name("node-a"),
    )
    .attempt(1)
    .max_attempts(3)
    .scratch_root(base.path().to_path_buf())
    .build();
    assert_eq!(attempt1.attempt(), 1);
    attempt1
        .scratch()
        .put(b"progress", b"first-half-done")
        .expect("attempt 1 write succeeds");

    // Attempt 2: a fresh context for the SAME run/node at attempt 2 reads it back.
    let attempt2 = RunContext::builder(
        RunId::new("run-x"),
        PipelineId::new("pipe"),
        NodeId::from_name("node-a"),
    )
    .attempt(2)
    .max_attempts(3)
    .scratch_root(base.path().to_path_buf())
    .build();
    assert_eq!(attempt2.attempt(), 2);
    let got = attempt2.scratch().get(b"progress").expect("attempt 2 read");
    assert_eq!(
        got.as_deref(),
        Some(&b"first-half-done"[..]),
        "attempt 2 reads exactly the value attempt 1 wrote"
    );
}

// ---------------------------------------------------------------------------
// Keys are namespaced by run and node — no collision between nodes.
// ---------------------------------------------------------------------------

#[test]
fn two_nodes_same_key_do_not_collide() {
    let base = TempBase::new();
    let node_a = store_for(base.path(), "pipe", "run-1", "node-a");
    let node_b = store_for(base.path(), "pipe", "run-1", "node-b");

    node_a.put(b"k", b"value-from-a").expect("a writes");
    node_b.put(b"k", b"value-from-b").expect("b writes");

    assert_eq!(
        node_a.get(b"k").unwrap().as_deref(),
        Some(&b"value-from-a"[..])
    );
    assert_eq!(
        node_b.get(b"k").unwrap().as_deref(),
        Some(&b"value-from-b"[..])
    );
}

// ---------------------------------------------------------------------------
// Same key in different runs does not collide.
// ---------------------------------------------------------------------------

#[test]
fn same_node_different_runs_do_not_collide() {
    let base = TempBase::new();
    let run1 = store_for(base.path(), "pipe", "run-1", "node-a");
    let run2 = store_for(base.path(), "pipe", "run-2", "node-a");

    run1.put(b"k", b"run1-value").expect("run1 writes");
    run2.put(b"k", b"run2-value").expect("run2 writes");

    assert_eq!(run1.get(b"k").unwrap().as_deref(), Some(&b"run1-value"[..]));
    assert_eq!(run2.get(b"k").unwrap().as_deref(), Some(&b"run2-value"[..]));
}

// ---------------------------------------------------------------------------
// Cross-node read is impossible by construction (enforced isolation).
// ---------------------------------------------------------------------------

#[test]
fn cross_node_read_yields_absent_never_the_other_nodes_bytes() {
    let base = TempBase::new();
    let node_a = store_for(base.path(), "pipe", "run-1", "node-a");
    let node_b = store_for(base.path(), "pipe", "run-1", "node-b");

    node_a.put(b"secret", b"a-only").expect("a writes");

    // Every API surface node B is given addresses only B's namespace. There is no
    // method on the handle that takes another node, run, or absolute path — B can
    // only name its own namespace, so A's key reads absent through B.
    assert!(
        matches!(node_b.get(b"secret"), Ok(None)),
        "B cannot reach A's value; A's key is absent through B's handle"
    );

    // B's namespace dir is B's own — disjoint from A's — confirming the handle
    // cannot be pointed at A's directory.
    let a_dir = node_a.namespace_dir().expect("A has a namespace");
    let b_dir = node_b.namespace_dir().expect("B has a namespace");
    assert_ne!(a_dir, b_dir, "the two handles address disjoint directories");
    assert!(a_dir.starts_with(base.path()) && b_dir.starts_with(base.path()));
}

// ---------------------------------------------------------------------------
// Scratch of a succeeded node is deleted (on-success hook).
// ---------------------------------------------------------------------------

#[test]
fn succeeded_node_scratch_is_deleted_by_the_hook() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");

    store.put(b"k1", b"v1").expect("write");
    store.put(b"k2", b"v2").expect("write");
    let dir = store.namespace_dir().expect("wired").to_path_buf();
    assert!(dir.exists(), "namespace exists after writes");

    store.remove_on_success().expect("success hook succeeds");

    assert!(
        matches!(store.get(b"k1"), Ok(None)) && matches!(store.get(b"k2"), Ok(None)),
        "after success, the node's keys read absent"
    );
    assert!(
        !dir.exists(),
        "the node's scratch storage location no longer exists on disk"
    );
}

// ---------------------------------------------------------------------------
// Scratch of a non-succeeded node is retained (nothing deleted implicitly).
// ---------------------------------------------------------------------------

#[test]
fn non_succeeded_node_scratch_is_retained_after_run_end() {
    let base = TempBase::new();
    let succeeded = store_for(base.path(), "pipe", "run-1", "node-x");
    let retained = store_for(base.path(), "pipe", "run-1", "node-y");

    succeeded.put(b"k", b"x-progress").expect("x writes");
    retained.put(b"k", b"y-progress").expect("y writes");
    let retained_dir = retained.namespace_dir().expect("wired").to_path_buf();

    // Node X reaches success → its hook runs. Node Y reached a non-success
    // terminal state → NO hook runs for it. Then "run end": drop both stores.
    succeeded.remove_on_success().expect("x success hook");
    drop(succeeded);
    drop(retained);

    // A fresh store for Y (as a later resume/prune would open) still reads Y's
    // scratch: nothing implicit deleted it, and the directory is still on disk.
    let reopened_y = store_for(base.path(), "pipe", "run-1", "node-y");
    assert_eq!(
        reopened_y.get(b"k").unwrap().as_deref(),
        Some(&b"y-progress"[..]),
        "non-succeeded node's scratch remains readable after run end"
    );
    assert!(
        retained_dir.exists(),
        "no implicit end-of-run deletion touched the retained scratch"
    );
}

// ---------------------------------------------------------------------------
// Write failure classifies as retry-eligible.
// ---------------------------------------------------------------------------

#[test]
fn write_failure_is_retry_eligible_task_failure_not_permanent_or_panic() {
    let base = TempBase::new();
    // Make the run directory a FILE where the scratch subtree needs to be, so
    // creating the namespace directory fails deterministically (a file cannot be a
    // parent directory). This is a deterministic fault, no wall-clock race.
    let run_dir = base.path().join("pipe").join("run-1");
    std::fs::create_dir_all(&run_dir).expect("run dir");
    // Occupy the `scratch` name with a regular file → create_dir_all under it fails.
    std::fs::write(run_dir.join("scratch"), b"not a directory").expect("block scratch");

    let store = store_for(base.path(), "pipe", "run-1", "node-a");
    let err = store
        .put(b"k", b"v")
        .expect_err("write must fail when the namespace cannot be created");
    assert!(matches!(err, ScratchError::Io { .. }));

    // The failure converts to a RETRY-ELIGIBLE task failure (C4) — not permanent,
    // not a panic — and carries enough context to name the failing operation.
    let task_err: TaskError = err.into();
    assert!(
        task_err.is_retryable(),
        "scratch write failure is retry-eligible"
    );
    assert!(!task_err.is_permanent());
    assert!(
        task_err.message().contains("write"),
        "the error identifies the failing operation: {}",
        task_err.message()
    );
}

// ---------------------------------------------------------------------------
// Read failure classifies as retry-eligible (injected fault on an existing key).
// ---------------------------------------------------------------------------

#[test]
fn read_failure_is_retry_eligible_and_distinct_from_absent() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");
    store.put(b"k", b"v").expect("write an existing value");

    // Inject a read fault: replace the key's value FILE with a DIRECTORY of the
    // same name. `std::fs::read` on a directory fails with an I/O error — a
    // deterministic fault on an EXISTING key, distinct from the absent case.
    let key_file = store
        .namespace_dir()
        .expect("wired")
        .join(encoded_key(b"k"));
    std::fs::remove_file(&key_file).expect("remove the value file");
    std::fs::create_dir(&key_file).expect("occupy the key name with a directory");

    let got = store.get(b"k");
    assert!(
        matches!(got, Err(ScratchError::Io { .. })),
        "a read fault on an existing key is an error, not the absent Ok(None)"
    );
    let task_err: TaskError = got.unwrap_err().into();
    assert!(
        task_err.is_retryable(),
        "scratch read failure is retry-eligible"
    );
    assert!(task_err.message().contains("read"));
}

/// The hex encoding the store uses for a key's filename — mirrored here so the
/// read-fault test can locate the on-disk value file for an existing key.
fn encoded_key(key: &[u8]) -> String {
    let mut s = String::with_capacity(key.len() * 2);
    for byte in key {
        s.push(char::from_digit(u32::from(byte >> 4), 16).unwrap());
        s.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap());
    }
    s
}

// ---------------------------------------------------------------------------
// Physical layout is inside the run directory and namespaced.
// ---------------------------------------------------------------------------

#[test]
fn physical_layout_is_under_run_dir_and_per_node_namespaced() {
    let base = TempBase::new();
    let store = store_for(base.path(), "the-pipeline", "the-run", "node-a");
    store
        .put(b"k", b"v")
        .expect("write to materialize the layout");

    let dir = store.namespace_dir().expect("wired");
    // Under `<base>/<pipeline>/<run-id>/scratch/<node>/`.
    let expected_prefix = base
        .path()
        .join("the-pipeline")
        .join("the-run")
        .join("scratch");
    assert!(
        dir.starts_with(&expected_prefix),
        "scratch lives under <base>/<pipeline>/<run-id>/scratch/: {dir:?}"
    );
    assert!(
        dir.exists(),
        "the namespace directory was created by the write"
    );

    // Two distinct nodes resolve to distinct locations under the same run.
    let other = store_for(base.path(), "the-pipeline", "the-run", "node-b");
    assert_ne!(dir, other.namespace_dir().expect("wired"));
}

// ---------------------------------------------------------------------------
// Hand-constructed context reaches scratch with no runtime running (C8).
// ---------------------------------------------------------------------------

#[test]
fn hand_constructed_context_reaches_scratch_with_no_runtime() {
    let base = TempBase::new();
    // A context built entirely by hand — no runtime, admission, or event stream.
    let ctx = RunContext::builder(
        RunId::new("solo-run"),
        PipelineId::new("solo-pipe"),
        NodeId::from_name("solo-node"),
    )
    .scratch_root(base.path().to_path_buf())
    .build();

    ctx.scratch().put(b"k", b"round-trip").expect("write");
    assert_eq!(
        ctx.scratch().get(b"k").unwrap().as_deref(),
        Some(&b"round-trip"[..]),
        "scratch round-trips from a hand-built context with no runtime"
    );
}

// ---------------------------------------------------------------------------
// A context built with NO run store carries an honestly-unwired store.
// ---------------------------------------------------------------------------

#[test]
fn no_run_store_context_has_honestly_unwired_scratch() {
    // The default C8 test context supplies no run store.
    let ctx = RunContext::for_test();
    let scratch = ctx.scratch();

    // No namespace, and it does not pretend to persist: writes error, reads error
    // (rather than a fabricated absent) — but as a retry-eligible fault, no panic.
    assert!(scratch.namespace_dir().is_none());
    let write = scratch.put(b"k", b"v");
    assert!(
        write.is_err(),
        "an unwired store does not pretend to persist"
    );
    let read = scratch.get(b"k");
    assert!(
        read.is_err(),
        "an unwired store is honest, not silently empty"
    );
}

// ---------------------------------------------------------------------------
// Overwrite: a second write replaces the first (last write wins).
// ---------------------------------------------------------------------------

#[test]
fn second_write_overwrites_first() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");
    store.put(b"k", b"first").expect("write 1");
    store.put(b"k", b"second-longer-value").expect("write 2");
    assert_eq!(
        store.get(b"k").unwrap().as_deref(),
        Some(&b"second-longer-value"[..]),
        "the later write replaces the earlier (atomic overwrite)"
    );
}

// ---------------------------------------------------------------------------
// remove deletes a single key; removing an absent key is not an error.
// ---------------------------------------------------------------------------

#[test]
fn remove_deletes_one_key_and_is_idempotent() {
    let base = TempBase::new();
    let store = store_for(base.path(), "pipe", "run-1", "node-a");
    store.put(b"k", b"v").expect("write");
    store.remove(b"k").expect("remove present key");
    assert!(
        matches!(store.get(b"k"), Ok(None)),
        "key is gone after remove"
    );
    store
        .remove(b"k")
        .expect("removing an absent key is a no-op, not an error");
}
