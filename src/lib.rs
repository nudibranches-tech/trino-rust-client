//! An async [Trino](https://trino.io/) client.
//!
//! Build a [`Client`] with [`ClientBuilder`],
//! then run queries. Rows deserialize into a `#[derive(Trino)]` struct for
//! statically-known schemas, or into [`Row`] when the shape is only known at
//! runtime.
//!
//! # Quickstart
//!
//! ```no_run
//! use trino_rust_client::{client::ClientBuilder, Trino};
//! use futures::StreamExt;
//! use serde::{Deserialize, Serialize};
//!
//! // A result row type needs `Trino` (column mapping) plus serde's derives.
//! #[derive(Trino, Debug, Deserialize, Serialize)]
//! struct Nation {
//!     nationkey: i64,
//!     name: String,
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ClientBuilder::new("user", "localhost")
//!     .port(8080)
//!     .catalog("tpch")
//!     .schema("sf1")
//!     .build()?;
//!
//! // Buffer the whole result set:
//! let nations = client.get_all::<Nation>("SELECT nationkey, name FROM nation").await?;
//! for n in nations.as_slice() {
//!     println!("{n:?}");
//! }
//!
//! // …or stream it lazily, without holding the whole result in memory:
//! let mut rows = client.stream::<Nation>("SELECT nationkey, name FROM nation").await?;
//! while let Some(row) = rows.next().await {
//!     println!("{:?}", row?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Result types
//!
//! - `#[derive(Trino)]` structs — decode columns by name into typed fields.
//! - [`Row`] — a dynamically-typed row when the schema is not known at compile
//!   time; pair it with [`DataSet`] to keep the column metadata.
//!
//! # Error handling
//!
//! All fallible calls return [`error::Error`]. A query failure from the
//! coordinator is [`error::Error::Query`]; match on
//! [`QueryError::kind`](models::QueryError::kind) for common cases such as
//! `TableNotFound`. See the [`error`] module for details.
//!
//! # Cargo features
//!
//! - `spooling` — support Trino's spooling protocol for large result sets
//!   (segments fetched from object storage), enabling
//!   [`ClientBuilder::spooling_encoding`](client::ClientBuilder::spooling_encoding)
//!   and related options.
//!
//! # Observability
//!
//! The client emits [`tracing`](https://docs.rs/tracing) events and wraps each
//! query in a span carrying its `query_id`. Install any `tracing` subscriber to
//! see them.
//!
//! Upgrading across a breaking release? See the [migration guide][mg].
//!
//! [mg]: https://github.com/nudibranches-tech/trino-rust-client/blob/main/MIGRATION.md
#![allow(clippy::should_implement_trait)]
#![allow(clippy::derivable_impls)]

pub mod auth;
pub mod client;
pub mod error;

mod header;
pub mod models;
#[cfg(feature = "spooling")]
pub mod spooling;

pub mod retry;
pub mod selected_role;
pub mod session;
pub mod ssl;
pub mod transaction;
pub mod tuples;
pub mod types;

pub use client::*;
pub use models::*;
pub use trino_rust_client_macros::*;
pub use types::*;
