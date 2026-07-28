---
name: using-libsql
description: >-
  Guides using the libSQL embedded database (tursodatabase/libsql — the mature
  SQLite fork) from the async `libsql` Rust crate and the sqlite3/turso CLIs.
  Covers opening file and :memory: databases, execute/query/prepared-statements/
  transactions, and the WAL single-writer + multi-process concurrency model
  (busy_timeout, BEGIN IMMEDIATE, bounded SQLITE_BUSY retry). Use when working
  with libSQL, the `libsql` Rust crate, tursodatabase/libsql, an embedded SQLite
  / sqld database, embedded replicas, or errors like SQLITE_BUSY / "database is
  locked"; also distinguishes the pre-1.0 Turso Database rewrite (`turso` crate).
---

# Using libSQL

libSQL (crate `libsql`, `tursodatabase/libsql`) is Turso's **mature, production SQLite fork**. It keeps 100% SQLite file-format + API compatibility and adds embedded replicas and remote/`sqld` sync. Its Rust API is **async-only** (needs tokio); it is not a drop-in for sync `rusqlite`.

This is **not** the same project as **Turso Database** (crate `turso`, formerly "Limbo") — a from-scratch pre-1.0 Rust rewrite whose headline `BEGIN CONCURRENT`/MVCC is single-process and index-hostile. For an embedded, multi-process, file-compatible store today, use **libSQL** (this skill). See the "Turso Database rewrite" note in `reference/concurrency.md`.

## Setup

```toml
[dependencies]
# Pin EXACTLY: libsql is 0.x, so minor bumps can break the API. rustdoc coverage
# is low (~20%), so read source/examples for less-common calls.
libsql = "=0.9.30"
# Local file + :memory: only (drops remote/replication/sync/tls):
# libsql = { version = "=0.9.30", default-features = false, features = ["core"] }
tokio = { version = "1", features = ["full"] }
```

## Quick start

```rust
use libsql::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // File-backed; use ":memory:" for in-memory (there is no new_in_memory()).
    let db = Builder::new_local("app.db").build().await?; // build() is async
    let conn = db.connect()?;                             // connect() is SYNC

    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
        (), // () = no params
    ).await?;
    conn.execute("INSERT INTO users (email) VALUES (?1)", ["alice@example.org"]).await?;

    let mut rows = conn.query("SELECT id, email FROM users", ()).await?;
    while let Some(row) = rows.next().await? {   // next() IS async
        let id: i64 = row.get(0)?;               // get()/get_value() are SYNC
        let email: String = row.get(1)?;
        println!("{id} {email}");
    }
    Ok(())
}
```

**Async-vs-sync trap (memorize):** `build()`, `execute()`, `query()`, `prepare()`, `Rows::next()`, `Transaction::commit()`/`rollback()` are **async**. `Builder::new_local()`, `Database::connect()`, `Row::get()`/`get_value()`, `Statement::reset()`, `Connection::busy_timeout()` are **sync** (no `.await`).

For prepared statements, params (`params!`/`named_params!`), the `Value` enum, transactions, and the error type, read **`reference/rust-api.md`**.

## Concurrency — the default is single-writer

libSQL inherits SQLite's WAL model: **many readers, one writer at a time**. A blocked second writer gets `SQLITE_BUSY`. Unlike the `turso` rewrite, libSQL **does** support multiple OS **processes** writing one shared file (same-host, local filesystem only).

The load-bearing footgun: a transaction that begins **`DEFERRED`** (a read) and later upgrades to a write hits an **instant `SQLITE_BUSY` that `busy_timeout` will not retry**. So **every write transaction must open with `BEGIN IMMEDIATE`**.

Minimum-viable recipe (each connection): WAL + `synchronous=NORMAL` + a `busy_timeout`, `BEGIN IMMEDIATE` for writes, short write transactions, and an app-level bounded retry on `SQLITE_BUSY`:

```rust
use std::time::Duration;
conn.execute("PRAGMA journal_mode = WAL", ()).await?;
conn.execute("PRAGMA synchronous = NORMAL", ()).await?;
conn.busy_timeout(Duration::from_millis(5000))?; // per connection (sync)

fn is_busy(e: &libsql::Error) -> bool {
    matches!(e, libsql::Error::SqliteFailure(code, _) if *code == 5) // SQLITE_BUSY = 5
        || e.to_string().to_lowercase().contains("database is locked")
}
```

MVCC / `BEGIN CONCURRENT` is a **Turso-rewrite-only** feature, not libSQL. For the full 8-point recipe, the bounded-retry wrapper, and the local→server→replica seam, read **`reference/concurrency.md`**.

## CLI access

Because a libSQL local file is byte-compatible SQLite, three tiers work:

- **Plain `sqlite3 mydb.db`** — zero new tools; `.tables`, `.schema`, `.dump`, `SELECT …`. Lowest friction for inspecting an embedded file.
- **`turso` CLI** — `brew install tursodatabase/tap/turso`; `turso db shell <file-or-url> ["SQL"]`; `turso dev --db-file local.db` runs a local server.
- **`sqld` / `libsql-server`** — the standalone server, only when promoting off a local file (Docker `ghcr.io/tursodatabase/libsql-server:latest`, or a release binary).

## Do / Don't

- **DO** pin the exact version (`libsql = "=0.9.30"`) and use `Builder::new_local(":memory:")` for in-memory.
- **DO** open every write transaction with `BEGIN IMMEDIATE`, set `busy_timeout` per connection, keep write txns short, and wrap them in a bounded `SQLITE_BUSY` retry.
- **DO** give each process/thread its own `Connection`; keep the DB on a local filesystem (never NFS/SMB — WAL shared-memory can't cross it).
- **DON'T** expect a `new_in_memory()`, or row-level `BEGIN CONCURRENT`/MVCC (that's the `turso` rewrite, single-process).
- **DON'T** rely on `busy_timeout` alone for a `DEFERRED`-then-upgrade write — it returns an instant `SQLITE_BUSY` that no timeout retries; use `BEGIN IMMEDIATE`.
