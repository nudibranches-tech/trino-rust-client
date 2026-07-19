# Migration guide

Guidance for upgrading across breaking releases. See [CHANGELOG.md](CHANGELOG.md)
for the full list of changes.

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

### Cargo features

- **The `Trino` feature was removed** (it was unused).
- **The spooling codec dependencies** (`base64`, `zstd`, `lz4`, `flate2`) are no
  longer standalone public features — enable spooling with
  `features = ["spooling"]`.
