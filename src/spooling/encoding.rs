use std::convert::TryFrom;
use std::fmt;

use crate::error::{Error, Result};

/// Spooling encoding format for Trino query data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpoolingEncoding {
    /// Uncompressed JSON
    Json,
    /// JSON with Zstandard compression
    JsonZstd,
    /// JSON with LZ4 compression
    JsonLz4,
}

impl SpoolingEncoding {
    /// Get the string representation of the encoding
    pub fn as_str(self) -> &'static str {
        match self {
            SpoolingEncoding::Json => "json",
            SpoolingEncoding::JsonZstd => "json+zstd",
            SpoolingEncoding::JsonLz4 => "json+lz4",
        }
    }

    /// Check if this encoding uses compression
    pub fn is_compressed(self) -> bool {
        matches!(self, SpoolingEncoding::JsonZstd | SpoolingEncoding::JsonLz4)
    }
}

// Try to convert a string to a SpoolingEncoding
impl TryFrom<&str> for SpoolingEncoding {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        match s {
            "json" => Ok(SpoolingEncoding::Json),
            "json+zstd" => Ok(SpoolingEncoding::JsonZstd),
            "json+lz4" => Ok(SpoolingEncoding::JsonLz4),
            _ => Err(Error::InternalError(format!(
                "Unsupported spooling encoding: {}. Supported values: json, json+zstd, json+lz4",
                s
            ))),
        }
    }
}

impl fmt::Display for SpoolingEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<SpoolingEncoding> for String {
    fn from(encoding: SpoolingEncoding) -> Self {
        encoding.as_str().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_encoding_try_from() {
        assert_eq!(
            SpoolingEncoding::try_from("json").unwrap(),
            SpoolingEncoding::Json
        );
        assert_eq!(
            SpoolingEncoding::try_from("json+zstd").unwrap(),
            SpoolingEncoding::JsonZstd
        );
        assert_eq!(
            SpoolingEncoding::try_from("json+lz4").unwrap(),
            SpoolingEncoding::JsonLz4
        );
        assert!(SpoolingEncoding::try_from("unknown").is_err());
    }

    #[test]
    fn test_encoding_as_str() {
        assert_eq!(SpoolingEncoding::Json.as_str(), "json");
        assert_eq!(SpoolingEncoding::JsonZstd.as_str(), "json+zstd");
        assert_eq!(SpoolingEncoding::JsonLz4.as_str(), "json+lz4");
    }

    #[test]
    fn test_encoding_display() {
        assert_eq!(SpoolingEncoding::Json.to_string(), "json");
        assert_eq!(SpoolingEncoding::JsonZstd.to_string(), "json+zstd");
        assert_eq!(SpoolingEncoding::JsonLz4.to_string(), "json+lz4");
    }

    #[test]
    fn test_encoding_is_compressed() {
        assert!(!SpoolingEncoding::Json.is_compressed());
        assert!(SpoolingEncoding::JsonZstd.is_compressed());
        assert!(SpoolingEncoding::JsonLz4.is_compressed());
    }
}
