use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub message: String,
    pub sql_state: Option<String>,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
    pub error_location: Option<ErrorLocation>,
    #[serde(default)]
    pub failure_info: Option<FailureInfo>,
}

/// A typed classification of the common Trino error names, so callers can
/// match on well-known failures without comparing raw strings.
///
/// This intentionally covers only frequent cases; anything else is
/// [`TrinoErrorKind::Other`] — use [`QueryError::error_name`] /
/// [`QueryError::error_code`] for the full Trino error taxonomy.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoErrorKind {
    SyntaxError,
    PermissionDenied,
    UserCanceled,
    CatalogNotFound,
    SchemaNotFound,
    SchemaAlreadyExists,
    TableNotFound,
    TableAlreadyExists,
    ColumnNotFound,
    ColumnAlreadyExists,
    FunctionNotFound,
    NotSupported,
    /// Any other Trino error — inspect the raw `error_name` / `error_code`.
    Other,
}

impl TrinoErrorKind {
    fn from_name(error_name: &str) -> Self {
        match error_name {
            "SYNTAX_ERROR" => Self::SyntaxError,
            "PERMISSION_DENIED" => Self::PermissionDenied,
            "USER_CANCELED" => Self::UserCanceled,
            "CATALOG_NOT_FOUND" => Self::CatalogNotFound,
            "SCHEMA_NOT_FOUND" => Self::SchemaNotFound,
            "SCHEMA_ALREADY_EXISTS" => Self::SchemaAlreadyExists,
            "TABLE_NOT_FOUND" => Self::TableNotFound,
            "TABLE_ALREADY_EXISTS" => Self::TableAlreadyExists,
            "COLUMN_NOT_FOUND" => Self::ColumnNotFound,
            "COLUMN_ALREADY_EXISTS" => Self::ColumnAlreadyExists,
            "FUNCTION_NOT_FOUND" => Self::FunctionNotFound,
            "NOT_SUPPORTED" => Self::NotSupported,
            _ => Self::Other,
        }
    }
}

impl QueryError {
    /// Classify this failure into a [`TrinoErrorKind`] for ergonomic matching
    /// on the common Trino error names.
    ///
    /// ```
    /// # use trino_rust_client::models::{QueryError, TrinoErrorKind};
    /// # fn handle(err: &QueryError) {
    /// match err.kind() {
    ///     TrinoErrorKind::TableNotFound => { /* create it, retry, … */ }
    ///     TrinoErrorKind::SyntaxError => { /* report err.message */ }
    ///     _ => { /* fall back to err.error_name / err.error_code */ }
    /// }
    /// # }
    /// ```
    pub fn kind(&self) -> TrinoErrorKind {
        TrinoErrorKind::from_name(&self.error_name)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorLocation {
    pub line_number: u32,
    pub column_number: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureInfo {
    #[serde(rename = "type")]
    pub ty: String,
    pub suppressed: Vec<FailureInfo>,
    pub stack: Vec<String>,
    pub message: Option<String>,
    pub cause: Option<Box<FailureInfo>>,
    pub error_location: Option<ErrorLocation>,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "message: {}", self.message)?;
        if let Some(st) = &self.sql_state {
            writeln!(f, "sql_state: {}", st)?;
        }
        writeln!(f, "error_code: {}", self.error_code)?;
        writeln!(f, "error_type: {}", self.error_name)?;
        if let Some(loc) = &self.error_location {
            writeln!(f, "error_location: {}", loc)?;
        }
        if let Some(fi) = &self.failure_info {
            writeln!(f, "failure_info: {}", fi)?;
        }
        Ok(())
    }
}

impl std::error::Error for QueryError {}

impl fmt::Display for ErrorLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.line_number, self.column_number)
    }
}

impl fmt::Display for FailureInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "ty: {}", self.ty)?;
        if let Some(msg) = &self.message {
            writeln!(f, "message: {}", msg)?;
        }
        if let Some(loc) = &self.error_location {
            writeln!(f, "loc: {}", loc)?;
        }
        writeln!(f, "stack:")?;
        for s in &self.stack {
            writeln!(f, "\ttype: {}", s)?;
        }
        if let Some(cause) = &self.cause {
            writeln!(f, "cause: {}", cause)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loc() {
        let loc = ErrorLocation {
            line_number: 100,
            column_number: 15,
        };

        assert_eq!("(100, 15)", format!("{}", loc));
    }

    #[test]
    fn test_failure() {
        let failure = FailureInfo {
            ty: "xxxty".into(),
            suppressed: vec![],
            stack: vec!["stack_1".into(), "stack_2".into(), "stack_3".into()],
            message: None,
            cause: None,
            error_location: None,
        };

        println!("{}", failure);
    }

    #[test]
    fn test_error_kind() {
        let mut err = QueryError {
            message: "boom".into(),
            sql_state: None,
            error_code: 44,
            error_name: "TABLE_NOT_FOUND".into(),
            error_type: "USER_ERROR".into(),
            error_location: None,
            failure_info: None,
        };
        assert_eq!(err.kind(), TrinoErrorKind::TableNotFound);

        err.error_name = "SOME_FUTURE_TRINO_ERROR".into();
        assert_eq!(err.kind(), TrinoErrorKind::Other);
    }
}
