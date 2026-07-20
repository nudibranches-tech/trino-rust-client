# Migration guide

Guidance for upgrading across breaking releases. See [CHANGELOG.md](CHANGELOG.md)
for the full list of changes.

## 0.11.0 → 0.12.0

### Transactions (`TransactionId` reshaped)

`TransactionId` previously carried four fixed literals and could not represent a
transaction identifier at all, which meant transactions did not work: the
identifier Trino returned was silently discarded. It now models exactly what the
`X-Trino-Transaction-Id` header carries.

`StartTransaction`, `RollBack` and `Commit` are gone. They were SQL statements,
not header values — code that set them was sending a header Trino does not
accept, and was not in a transaction either way. Use the new `Client` methods.

**Before:**

```rust
use trino_rust_client::transaction::TransactionId;

let client = ClientBuilder::new("user", "localhost")
    .transaction_id(TransactionId::StartTransaction)
    .build()?;

// ... and there was no way to commit: the id was never captured.
```

**After:**

```rust
client.begin_transaction().await?;
client.execute("INSERT INTO t VALUES (1)").await?;
client.commit().await?;   // or client.rollback().await?
```

To inspect or adopt a transaction directly:

```rust
use trino_rust_client::transaction::TransactionId;

let id = client.transaction_id().await;       // TransactionId::Id(..) when active
client.set_transaction_id(id).await;          // adopt one started elsewhere
```

Note both accessors are `async` — the session sits behind a `tokio::sync::RwLock`.

If you match on `TransactionId`, the exhaustive set is now two variants:

**Before:**

```rust
match id {
    TransactionId::NoTransaction => { /* … */ }
    TransactionId::StartTransaction => { /* … */ }
    TransactionId::RollBack => { /* … */ }
    TransactionId::Commit => { /* … */ }
}
```

**After:**

```rust
match id {
    TransactionId::NoTransaction => { /* … */ }
    TransactionId::Id(uuid) => { /* … */ }
}
```

### Accessor renames

| Before | After | Note |
|---|---|---|
| `TransactionId::to_str(&self) -> &'static str` | `TransactionId::as_header_value(&self) -> &str` | cannot be `'static` now that a variant owns a `String` |
| `TransactionId::from_str(&str) -> Option<Self>` | `TransactionId::from_header_value(&str) -> Self` | infallible: anything other than `NONE` is an identifier |

### `TransactionId` is no longer `Copy`

It owns a `String`. It is still `Clone`, and now also `PartialEq` and `Eq`. Add
`.clone()` where you relied on implicit copies.

## 0.10.x → 0.11.0

### Error handling (restructured `Error` enum)

A Trino query failure is now a single, fully structured
`Error::Query(Box<QueryError>)` instead of a handful of data-less unit
variants. No information is lost — the query's `error_code`, `error_name`,
`error_type`, `message`, `sql_state` and `error_location` are all on the inner
`QueryError` — and every Trino error is now distinguishable (the old variants
only covered ~11 of them; everything else fell into an opaque `InternalError`).

Match on `QueryError::kind()` for the common cases, and on the raw
`error_name` / `error_code` for the long tail.

**Before:**

```rust
use trino_rust_client::error::Error;

match err {
    Error::TableNotFound => { /* … */ }
    Error::CatalogNotFound => { /* … */ }
    Error::SchemaNotFound => { /* … */ }
    _ => { /* … */ }
}
```

**After:**

```rust
use trino_rust_client::error::Error;
use trino_rust_client::models::TrinoErrorKind;

match err {
    Error::Query(q) => match q.kind() {
        TrinoErrorKind::TableNotFound => { /* … */ }
        TrinoErrorKind::CatalogNotFound => { /* … */ }
        TrinoErrorKind::SchemaNotFound => { /* … */ }
        // Full taxonomy is still reachable:
        _ => eprintln!("{} (code {}): {}", q.error_name, q.error_code, q.message),
    },
    Error::Forbidden { .. } => { /* PERMISSION_DENIED (error_code 4) */ }
    _ => { /* … */ }
}
```

Other error changes:

- **`Error` is now `#[non_exhaustive]`.** Add a `_ =>` arm to exhaustive
  matches; future variants can then be added without another breaking release.
- **`Error::EmptyData` was removed.** A zero-row query returns an empty
  `DataSet` / an empty stream, not an error.
- **`Error::InternalError(String)` was split** into typed `Error::Decode`,
  `Error::Tls` and `Error::Protocol` for decode/TLS/protocol failures.
  `Error::InternalError` remains only for genuinely internal failures.
- **`error::TrinoError` / `TrinoErrorLocation` were removed.** The single Trino
  error type is now `models::QueryError` (its `failure_info` is now optional).
- The top-level `Display` of `Error::Query` is concise
  (`query error [NAME]: message`); the full structured error is reachable via
  `std::error::Error::source()`.

### Logging → tracing

The client now emits [`tracing`](https://docs.rs/tracing) events instead of
`log` records. **If your application only installs a `log` subscriber (e.g.
`env_logger`), you will no longer see the client's logs.** Either install a
`tracing` subscriber:

```rust
tracing_subscriber::fmt().with_env_filter("trino_rust_client=debug").init();
```

or bridge tracing back into `log` with
[`tracing-log`](https://docs.rs/tracing-log). Each `get_all` / `stream` /
`execute` call is wrapped in a span carrying the `query_id`.

### Cargo features

- **The `Trino` feature was removed** (it was unused).
- **The spooling codec dependencies** (`base64`, `zstd`, `lz4`, `flate2`) are no
  longer standalone public features — enable spooling with
  `features = ["spooling"]`.
