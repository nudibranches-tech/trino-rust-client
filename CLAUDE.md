# CLAUDE.md

## Build & Test

```bash
cargo check                      # default features
cargo check --features spooling  # with spooling protocol support
cargo test                       # run all tests (unit + integration)
cargo test --lib                 # unit tests only
```

Always verify compilation under **both** feature flag configurations (`default` and `spooling`).

## Architecture

Trino Rust client library. Core query flow:

- `Client::get_all<T>` — paginated query execution returning `DataSet<T>`
- `Client::execute` — DDL/DML execution returning `ExecuteResult`
- Pagination loop follows `next_uri` links from Trino server responses

### Data protocols

Trino can return data in two protocols, handled via `QueryResultData<T>`:

- **Direct**: inline JSON arrays (`Vec<T>`)
- **Spooled** (behind `feature = "spooling"`): compressed segments fetched from S3/MinIO

The `Accumulator` state machine in `client.rs` detects the protocol from the first data page and enforces no mixing. Key invariant: every response in the pagination loop must check `.error` before processing `.data`.

### Key types

- `DataSet<T>` (`src/types/data_set.rs`) — typed result set with column metadata
- `Row` — dynamic/untyped row (`T::ty() == TrinoTy::Unknown`), requires column metadata
- Derive macro `#[derive(Trino)]` for typed row structs

## Conventions

- Feature-gated code uses `#[cfg(feature = "spooling")]` — always test both paths
- Error types live in `src/error.rs` (client errors) and `src/models/error.rs` (Trino query errors)
- `build_dataset` handles `Row` (needs columns) vs typed `T` (infers from `T::ty()`) differently
- Tests for `client.rs` internals go in the `mod tests` block at the bottom of the file
