use reqwest::header::HeaderName;
use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;

use crate::models::QueryError;

#[derive(Error, Debug)]
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
    /// a specific failure.
    #[error("query error: {0}")]
    Query(Box<QueryError>),
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
