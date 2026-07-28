# libSQL Rust API (crate `libsql`, verified against 0.9.30)

Full surface beyond the SKILL.md quick start. Every signature below was checked against `docs.rs/libsql`; pin the exact version, as this is a 0.x crate.

## Contents

1. [Open a database](#1-open-a-database)
2. [execute (DDL/DML)](#2-execute-ddldml)
3. [query + reading rows](#3-query--reading-rows)
4. [The Value enum](#4-the-value-enum)
5. [Params](#5-params)
6. [Prepared statements](#6-prepared-statements)
7. [Transactions](#7-transactions)
8. [busy_timeout](#8-busy_timeout)
9. [Errors](#9-errors)

## 1. Open a database

```rust
use libsql::Builder;
// Builder::new_local(path: impl AsRef<Path>) -> Builder      (sync)
// Builder::build(self) -> Result<Database>                   (async)
// Database::connect(&self) -> Result<Connection>             (SYNC — no .await)
let db = Builder::new_local("app.db").build().await?;
let conn = db.connect()?;
let mem = Builder::new_local(":memory:").build().await?; // in-memory: use ":memory:"
```

Remote / embedded-replica constructors exist behind the default `remote`/`replication`/`sync` features (`Builder::new_remote(url, token)`, `Builder::new_remote_replica(path, url, token)`), and all converge on the same `Connection` — see `reference/concurrency.md`.

## 2. execute (DDL/DML)

```rust
// Connection::execute(sql: impl AsRef<str>, params: impl IntoParams) -> Result<u64>  (async; rows affected)
conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", ()).await?;
let affected: u64 = conn.execute("INSERT INTO t (name) VALUES (?1)", ["alice"]).await?;
let rowid = conn.last_insert_rowid();
// Connection::execute_batch(sql) -> Result<()>  (async; multiple statements)
conn.execute_batch("INSERT INTO t (name) VALUES ('bob'); INSERT INTO t (name) VALUES ('cara');").await?;
```

`()` is the empty-params form. A `[..]` array / tuple binds positionally to `?1, ?2, …`.

## 3. query + reading rows

```rust
// Connection::query(sql, params) -> Result<Rows>            (async)
// Rows::next(&mut self) -> Result<Option<Row>>              (async)
// Row::get::<T>(idx: i32) -> Result<T> where T: FromValue   (SYNC; columns are 0-indexed i32)
// Row::get_value(idx: i32) -> Result<Value>                 (SYNC)
let mut rows = conn.query("SELECT id, name FROM t WHERE id > ?1", [0_i64]).await?;
while let Some(row) = rows.next().await? {
    let id: i64 = row.get(0)?;
    let name: String = row.get(1)?;
    let raw: libsql::Value = row.get_value(1)?;
    let _ = row.column_name(0); // column metadata also on Row
}
```

`Rows::next()` returns `Result<Option<Row>>` (not `Option<Result<Row>>`), so `while let Some(row) = rows.next().await?` is correct.

## 4. The Value enum

```rust
// libsql::Value
match row.get_value(1)? {
    libsql::Value::Null      => {}
    libsql::Value::Integer(i) => { let _: i64 = i; }
    libsql::Value::Real(f)    => { let _: f64 = f; }
    libsql::Value::Text(s)    => { let _: String = s; }
    libsql::Value::Blob(b)    => { let _: Vec<u8> = b; }
}
```

`Row::get::<T>()` works for `i64`, `String`, `f64`, `Vec<u8>`, `Option<T>`, etc. (types implementing `FromValue`).

## 5. Params

```rust
use libsql::{params, named_params};
conn.execute("INSERT INTO t (id, name) VALUES (?1, ?2)", (1_i64, "alice")).await?; // tuple
conn.execute("INSERT INTO t (id, name) VALUES (?1, ?2)", params![2_i64, "bob"]).await?; // params!
// Named (:name / @name / $name):
conn.execute("UPDATE t SET name = :n WHERE id = :id", named_params! { ":n": "carol", ":id": 1_i64 }).await?;
```

`()` = no params. Tuples/arrays/`Vec` and `params![..]` bind positionally (`?1..`); `named_params!{}` binds by name.

## 6. Prepared statements

```rust
// Connection::prepare(sql) -> Result<Statement>            (async)
// Statement::execute(&mut self, params) -> Result<u64>     (async)
// Statement::query(&mut self, params) -> Result<Rows>      (async)
// Statement::reset(&self) -> ()                            (SYNC — call before reuse)
let mut ins = conn.prepare("INSERT INTO t (id, name) VALUES (?1, ?2)").await?;
ins.execute((1_i64, "alice")).await?;
ins.reset();
ins.execute((2_i64, "bob")).await?;
```

## 7. Transactions

```rust
use libsql::TransactionBehavior;
// Connection::transaction() -> Result<Transaction>                          (async; DEFERRED)
// Connection::transaction_with_behavior(TransactionBehavior) -> Result<Transaction>  (async)
// Transaction::commit(self) / rollback(self) -> Result<()>                  (async; consume self)
// For WRITES always use Immediate (see reference/concurrency.md — DEFERRED upgrade = instant SQLITE_BUSY):
let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
tx.execute("INSERT INTO t (name) VALUES (?1)", ["x"]).await?; // Transaction derefs to Connection
tx.commit().await?;                                            // or tx.rollback().await?; (drop = rollback)
```

Always-valid fallback (portable): drive transactions by hand — `conn.execute("BEGIN IMMEDIATE", ()).await?; …; conn.execute("COMMIT", ()).await?;`.

## 8. busy_timeout

```rust
use std::time::Duration;
// Connection::busy_timeout(Duration) -> Result<()>   (SYNC) — set on EVERY connection
conn.busy_timeout(Duration::from_millis(5000))?;
// Equivalent portable form (milliseconds):
conn.execute("PRAGMA busy_timeout = 5000", ()).await?;
```

`busy_timeout` is per-connection and installs SQLite's built-in busy handler (sleep-and-retry a blocked writer). It does **not** cure the `DEFERRED`-upgrade instant-`SQLITE_BUSY` (use `BEGIN IMMEDIATE`) — keep an app-level bounded retry anyway.

## 9. Errors

Calls return `Result<T, libsql::Error>`, so `?` and `async fn main() -> Result<(), Box<dyn std::error::Error>>` compose. Detect `SQLITE_BUSY` (code 5) via `libsql::Error::SqliteFailure(code, msg)` with `code == 5`, or a case-insensitive `"database is locked"` substring for portability (see the `is_busy` helper in SKILL.md / `reference/concurrency.md`). `libsql::Error` implements `std::error::Error`, so it composes with `anyhow`/`thiserror`.
