use reqwest::header::HeaderName;
use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;

use crate::models::QueryError;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("duplicate header")]
    DuplicateHeader(HeaderName),
    #[error("invalid empty auth")]
    EmptyAuth,
    #[error("forbidden: {message}")]
    Forbidden { message: String },
    #[error("basic auth can not be used with http")]
    BasicAuthWithHttp,
    #[error("http error, reason: {0}")]
    HttpError(#[source] Box<reqwest::Error>),
    #[error("http not ok, code: {0}, reason: {1}")]
    HttpNotOk(StatusCode, String),
    /// A query failed on the Trino coordinator. Match on the inner
    /// [`QueryError`]'s `error_code` / `error_name` / `error_type` to react to
    /// a specific failure; the full structured error is also reachable through
    /// [`std::error::Error::source`].
    #[error("query error [{}]: {}", .0.error_name, .0.message)]
    Query(#[source] Box<QueryError>),
    /// Failed to decode or deserialize a response or a spooled segment.
    #[error("decode error: {0}")]
    Decode(String),
    /// Failed to load or read a TLS certificate.
    #[error("tls error: {0}")]
    Tls(String),
    /// The server used a protocol the client cannot handle in this context
    /// (e.g. mixing the Direct and Spooled protocols across pages, or spooled
    /// data received without the `spooling` feature enabled).
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A transaction operation was attempted in a state that does not allow
    /// it — starting a transaction while one is already active, or committing
    /// or rolling back without one.
    #[error("transaction error: {0}")]
    Transaction(String),
    #[error("inconsistent data")]
    InconsistentData,
    #[error("reach max attempt: {0}")]
    ReachMaxAttempt(usize),
    #[error("invalid host: {0}")]
    InvalidHost(String),
    /// An unexpected, internal failure that callers are not expected to handle.
    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::HttpError(Box::new(err))
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        // error_code 4 is Trino's PERMISSION_DENIED.
        if err.error_code == 4 {
            Error::Forbidden {
                message: err.message,
            }
        } else {
            Error::Query(Box::new(err))
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize)]
pub struct TrinoRetryResult {
    pub id: String,
    #[serde(rename = "infoUri")]
    pub info_uri: String,
    pub stats: TrinoStats,
    pub error: Option<QueryError>,
    #[serde(rename = "updateType")]
    pub update_type: Option<String>,
    #[serde(rename = "updateCount")]
    pub update_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct TrinoStats {
    pub state: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::QueryError;

    fn query_error(error_code: i32, error_name: &str) -> QueryError {
        QueryError {
            message: "boom".into(),
            sql_state: None,
            error_code,
            error_name: error_name.into(),
            error_type: "USER_ERROR".into(),
            error_location: None,
            failure_info: None,
        }
    }

    // Both the query and execute paths funnel Trino failures through
    // `From<QueryError>`, so these two tests pin the single, shared mapping.

    #[test]
    fn permission_denied_maps_to_forbidden() {
        // error_code 4 is Trino's PERMISSION_DENIED.
        match Error::from(query_error(4, "PERMISSION_DENIED")) {
            Error::Forbidden { message } => assert_eq!(message, "boom"),
            other => panic!("expected Forbidden, got {other:?}"),
        }
    }

    #[test]
    fn other_failures_map_to_structured_query() {
        match Error::from(query_error(1, "SYNTAX_ERROR")) {
            Error::Query(q) => {
                assert_eq!(q.error_name, "SYNTAX_ERROR");
                assert_eq!(q.error_code, 1);
                assert_eq!(q.error_type, "USER_ERROR");
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn query_error_preserves_source_chain() {
        use std::error::Error as _;

        let err = Error::from(query_error(1, "SYNTAX_ERROR"));
        // Top-level Display stays concise (no failure_info dump)...
        assert_eq!(err.to_string(), "query error [SYNTAX_ERROR]: boom");
        // ...while the underlying error is reachable via the source chain, so
        // generic tooling (anyhow / eyre / tracing) can surface the cause.
        let source = err.source().expect("Query should expose a source");
        assert!(source.to_string().contains("boom"));
    }
}
