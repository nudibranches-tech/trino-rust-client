use reqwest::header::HeaderName;
use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;

use crate::models::QueryError;

#[derive(Error, Debug)]
#[allow(clippy::result_large_err)]
pub enum Error {
    #[error("invalid catalog")]
    InvalidCatalog,
    #[error("catalog not found")]
    CatalogNotFound,
    #[error("invalid schema")]
    InvalidSchema,
    #[error("schema not found")]
    SchemaNotFound,
    #[error("schema already exists")]
    SchemaAlreadyExists,
    #[error("invalid source")]
    InvalidSource,
    #[error("invalid user")]
    InvalidUser,
    #[error("invalid properties")]
    InvalidProperties,
    #[error("invalid table property: {0}")]
    InvalidTableProperty(String),
    #[error("table not found")]
    TableNotFound,
    #[error("table already exists")]
    TableAlreadyExists,
    #[error("duplicate header")]
    DuplicateHeader(HeaderName),
    #[error("invalid empty auth")]
    EmptyAuth,
    #[error("forbidden: {message}")]
    Forbidden { message: String },
    #[error("basic auth can not be used with http")]
    BasicAuthWithHttp,
    #[error("http error, reason: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("http not ok, code: {0}, reason: {1}")]
    HttpNotOk(StatusCode, String),
    #[error("query error, reason: {0}")]
    QueryError(Box<QueryError>),
    #[error("inconsistent data")]
    InconsistentData,
    #[error("empty data")]
    EmptyData,
    #[error("reach max attempt: {0}")]
    ReachMaxAttempt(usize),
    #[error("invalid host: {0}")]
    InvalidHost(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        Error::QueryError(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize)]
pub struct TrinoRetryResult {
    pub id: String,
    #[serde(rename = "infoUri")]
    pub info_uri: String,
    pub stats: TrinoStats,
    pub error: Option<TrinoError>,
    #[serde(rename = "updateType")]
    pub update_type: Option<String>,
    #[serde(rename = "updateCount")]
    pub update_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct TrinoStats {
    pub state: String,
}

#[derive(Debug, Deserialize)]
pub struct TrinoError {
    pub message: String,
    #[serde(rename = "errorCode")]
    pub error_code: i64,
    #[serde(rename = "errorName")]
    pub error_name: String,
    #[serde(rename = "errorType")]
    pub error_type: String,
    #[serde(rename = "errorLocation")]
    pub error_location: Option<TrinoErrorLocation>,
}

#[derive(Debug, Deserialize)]
pub struct TrinoErrorLocation {
    #[serde(rename = "lineNumber")]
    pub line_number: i64,
    #[serde(rename = "columnNumber")]
    pub column_number: i64,
}

impl From<TrinoError> for Error {
    fn from(error: TrinoError) -> Self {
        match error.error_name.as_str() {
            // CATALOG ERRORS
            "CATALOG_NOT_FOUND" => Error::CatalogNotFound,
            "MISSING_CATALOG_NAME" => Error::InvalidCatalog,

            // SCHEMA ERRORS
            "SCHEMA_NOT_FOUND" => Error::SchemaNotFound,
            "MISSING_SCHEMA_NAME" => Error::InvalidSchema,
            "SCHEMA_ALREADY_EXISTS" => Error::SchemaAlreadyExists,

            // TABLE ERRORS
            "INVALID_TABLE_PROPERTY" => Error::InvalidTableProperty(error.message),
            "TABLE_NOT_FOUND" => Error::TableNotFound,
            "TABLE_ALREADY_EXISTS" => Error::TableAlreadyExists,

            // OTHER ERRORS
            _ => Error::InternalError(format!(
                "Trino error: {} - {}",
                error.error_name, error.message
            )),
        }
    }
}
