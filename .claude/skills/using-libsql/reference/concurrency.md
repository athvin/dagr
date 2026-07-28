# libSQL concurrency: multi-process writes done safely

The load-bearing reference for using libSQL when many writers share one file (e.g. dagr's run metastore, where each DAG-run process writes the same `metastore.db`).

## Contents

1. [The model: WAL single-writer, multi-process OK](#1-the-model-wal-single-writer-multi-process-ok)
2. [The one footgun: DEFERRED-upgrade instant SQLITE_BUSY](#2-the-one-footgun-deferred-upgrade-instant-sqlite_busy)
3. [The 8-point recipe](#3-the-8-point-recipe)
4. [Bounded retry wrapper](#4-bounded-retry-wrapper)
5. [Local → server → replica (one seam)](#5-local--server--replica-one-seam)
6. [The Turso Database rewrite (not libSQL)](#6-the-turso-database-rewrite-not-libsql)

## 1. The model: WAL single-writer, multi-process OK

libSQL is a C fork of SQLite and keeps SQLite's file/locking model, so under WAL: **many concurrent readers + exactly one writer at a time**. Readers never block the writer and the writer never blocks readers, but a second writer that wants the write lock while another holds it gets `SQLITE_BUSY`.

Unlike the `turso` rewrite, **multiple OS processes on the same host can open and write one shared `.db` file** — the classic SQLite multi-process story, no server tier required. This is exactly the "one DAG-run process per writer" shape.

**Hard requirement:** same host, local POSIX filesystem only. WAL's shared-memory (`-shm`) index cannot cross a network filesystem — NFS/SMB will corrupt or fail. For multiple *hosts*, you need a server or embedded replicas (§5).

## 2. The one footgun: DEFERRED-upgrade instant SQLITE_BUSY

A transaction opened as `BEGIN` / `BEGIN DEFERRED` starts as a reader. If it later issues a write while another writer is active, SQLite **cannot** upgrade it (that would break serializable isolation) and returns `SQLITE_BUSY` **immediately** — *no matter how high `busy_timeout` is set*. Retrying can't help because only aborting one side resolves it.

**Fix: open every write transaction with `BEGIN IMMEDIATE`** (acquire the write lock up front, so the busy handler can legitimately wait for it). Never rely on lock upgrades.

## 3. The 8-point recipe

For many processes writing **disjoint** rows to one file:

1. `PRAGMA journal_mode = WAL` (once, e.g. at schema-create): readers never block the single writer.
2. `PRAGMA synchronous = NORMAL` under WAL (durable-enough, much cheaper fsync profile) — idiomatic for a metastore.
3. Set `busy_timeout` on **every** connection (e.g. `conn.busy_timeout(Duration::from_millis(5000))?`).
4. Start every write with **`BEGIN IMMEDIATE`** (never let a read txn upgrade to a write — §2).
5. Wrap each write transaction in an **application-level bounded retry** with exponential backoff + jitter, retrying only on `SQLITE_BUSY` / "database is locked" (§4). This covers the residual cases `busy_timeout` can't (upgrade-deadlock leftovers, WAL cleanup/recovery windows, checkpoint serialization).
6. Keep write transactions **short** — compute outside, then `BEGIN IMMEDIATE` → INSERT/UPDATE the disjoint rows → COMMIT. Short critical sections make the single-writer bottleneck effectively invisible.
7. **One connection per process**; if a process is multi-threaded, give each thread its own connection (each with `busy_timeout` set) rather than sharing.
8. **Same-host, local FS only** (§1).

Cap retries (e.g. 5–8 attempts) and surface a **hard error** after the cap so a genuinely wedged writer is visible, not silently dropped.

## 4. Bounded retry wrapper

```rust
use std::time::Duration;

fn is_busy(e: &libsql::Error) -> bool {
    matches!(e, libsql::Error::SqliteFailure(code, _) if *code == 5) // SQLITE_BUSY = 5
        || e.to_string().to_lowercase().contains("database is locked")
}

/// Runs `body` inside BEGIN IMMEDIATE with a BOUNDED retry-on-busy loop.
/// `body` must be replayable — it re-runs from scratch on each attempt.
/// (Uses explicit BEGIN IMMEDIATE / COMMIT via execute — always valid.)
async fn with_write_retry<F, Fut>(
    conn: &libsql::Connection,
    max_attempts: u32,
    mut body: F,
) -> Result<(), libsql::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), libsql::Error>>,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        let result = async {
            body().await?;                       // all statements for this txn
            conn.execute("COMMIT", ()).await.map(|_| ())
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(e) if is_busy(&e) && attempt < max_attempts => {
                let _ = conn.execute("ROLLBACK", ()).await;
                // exponential backoff + jitter to avoid livelock under contention
                let base = Duration::from_millis(1u64 << attempt.min(8));
                let jitter = Duration::from_millis(fastrand::u64(0..16)); // fastrand: a dev/runtime crate, not libsql
                tokio::time::sleep(base + jitter).await;
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e); // non-retryable, or attempts exhausted (visible wedge)
            }
        }
    }
}
```

## 5. Local → server → replica (one seam)

libSQL's `Builder::new_local` / `new_remote` / `new_remote_replica` all converge on the same async `Connection`, so depend only on `Connection` and pick the mode at construction:

- **Local file (start here):** `Builder::new_local("metastore.db").build().await?` — pure embedded, no server, `sqlite3`-compatible.
- **Promote to a server (`sqld`):** run `sqld`/`libsql-server` pointed at the file, then clients connect via `Builder::new_remote("libsql://host", token).build().await?` (also `http`/`ws` URLs). The on-disk file carries over (formats match). Needed when the store must span multiple *hosts* (WAL multi-process can't cross machines).
- **Embedded replica (local reads + remote writes):** `Builder::new_remote_replica("file:replica.db", "libsql://<db>", token).build().await?` then `conn.sync()` (or a periodic `sync_interval`). (Builder names have drifted across releases — check docs.rs for your pinned version.)

Design pattern: wrap construction in one factory (`MetaStore::open(mode)` returning a `Connection`), `mode ∈ {LocalFile, RemoteSqld, SyncedReplica}` — promotion is a config change, not a query-code rewrite.

## 6. The Turso Database rewrite (not libSQL)

`tursodatabase/turso` (crate `turso`, formerly "Limbo") is a **separate, pre-1.0, from-scratch Rust reimplementation** of SQLite. Its differences that matter here:

- It offers **row-level concurrent writes** via `PRAGMA journal_mode='mvcc'` + `BEGIN CONCURRENT` (optimistic, commit-time conflict → app must retry). libSQL has **no** such mode — it stays single-writer WAL.
- But Turso's MVCC is **experimental, single-process, and cannot use indexes** (no `CREATE INDEX` under MVCC), and Turso does **not** support multi-process file access. So for an embedded, multi-process, indexed store today, **use libSQL**; treat Turso Database as a forward-looking migration target once it reaches 1.0. (A separate `using-turso`-style guide would cover the `turso` crate's `Builder::new_local(path).build().await` / `db.connect()` API, which mirrors libSQL's but is a different crate.)
