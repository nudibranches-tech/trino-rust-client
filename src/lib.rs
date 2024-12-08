#![allow(clippy::should_implement_trait)]
#![allow(clippy::derivable_impls)]

pub mod auth;
pub mod client;
pub mod error;

mod header;
pub mod models;

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
