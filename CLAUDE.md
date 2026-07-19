# CLAUDE.md

Guidance for working in this repository.

## Build & test

```bash
cargo check                       # default features
cargo check --features spooling   # spooling protocol support
cargo test --features spooling    # unit + integration tests
cargo clippy --all-features --all-targets -- -D warnings
```

Always verify **both** feature configurations (`default` and `spooling`). The
`spooling` codec dependencies are gated behind the feature; `base64` is a core
dependency (needed by `VarBinary`).

## Workflow

- **Never commit directly to `main`.** Pre-commit hooks (`.pre-commit-config.yaml`)
  block it and run `rustfmt`, `cargo check --all-features` and
  `clippy -D warnings`. Always work on a branch and open a PR.
- Git SSH is not configured — use the `gh` CLI (or `gh auth git-credential`) for
  remote git operations.
- Keep `CHANGELOG.md` (`## [Unreleased]`) updated for user-facing changes; mark
  breaking changes **Breaking:** and add before/after notes to `MIGRATION.md`.

## Architecture

Async Trino client. Build a [`Client`] with `ClientBuilder`, then query.

### Query entry points (`src/client.rs`)
- `get_all<T>` — run a query and buffer the whole result into a `DataSet<T>`.
- `stream<T>` — lazily stream rows as a `RowStream` (`futures::Stream`), resolving
  the schema up front (`columns()`), `Send`/`Unpin`, and best-effort cancelling
  the query on early drop.
- `execute` — run a statement, returns `ExecuteResult`.
- `get` / `get_next` — low-level single-page pagination (`next_uri`).

Results are paginated: a response carries a `next_uri` to follow. Retries use
`RetryPolicy` and are **idempotency-aware**: page fetches (GET) retry on any
transient failure, query submission (POST) only when definitely not processed.

### Data protocols (`QueryResultData<T>`)
- **Direct**: inline JSON rows.
- **Spooled** (`feature = "spooling"`): compressed segments fetched from object
  storage; decoded one segment at a time.

### Type system (`src/types/`)
- The `Trino` trait maps a Rust type to a `TrinoTy` and provides a
  `DeserializeSeed`. `RawTrinoTy` (`src/models/ty.rs`) parses Trino's wire type
  strings; `TrinoTy::from_type_signature` converts them. Unmapped types produce
  `Error::UnsupportedType(name)`.
- `#[derive(Trino)]` (in `trino-rust-client-macros`, a **separately versioned**
  crate) generates the impl for a row struct. A row struct also needs
  `serde::{Deserialize, Serialize}`.
- `Row` — dynamically-typed row (`TrinoTy::Unknown`), pairs with `DataSet`.

### Errors (`src/error.rs`, `src/models/error.rs`)
- `Error` is `#[non_exhaustive]`. A query failure is `Error::Query(Box<QueryError>)`
  (structured; match on `error_code`/`error_name`, or `QueryError::kind()`).
  `Error::Decode` / `Tls` / `Protocol` cover typed failure classes;
  `InternalError` is for genuinely unexpected cases.

### Observability
- Emits `tracing` events; `get_all`/`stream`/`execute` are wrapped in a span
  carrying the `query_id`.

## Conventions

- Feature-gate spooling code with `#[cfg(feature = "spooling")]` and test both paths.
- Tests for `client.rs` internals go in its bottom `mod tests`; integration tests
  live in `tests/` (wiremock for HTTP, fixtures in `tests/data/models/`).
- Releases follow the process in the `release` skill (`.claude/skills/release/`).
