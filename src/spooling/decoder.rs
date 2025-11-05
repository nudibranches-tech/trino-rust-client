use crate::error::Error;
use crate::spooling::SpoolingEncoding;
use base64::prelude::*;
use std::convert::TryFrom;
use std::io::Read;
use zstd::stream::Decoder;
/// Decompress already-decoded segment bytes based on encoding
pub fn decompress_segment_bytes(
    compressed_data: &[u8],
    encoding: &SpoolingEncoding,
) -> Result<String, Error> {
    decompress_bytes_internal(compressed_data, encoding)
}

/// Decode and decompress inline segment data
pub fn decode_inline_segment(encoded_data: &str, encoding: &str) -> Result<String, Error> {
    let encoding = SpoolingEncoding::try_from(encoding)?;

    let compressed_data = BASE64_STANDARD
        .decode(encoded_data)
        .map_err(|e| Error::InternalError(format!("Failed to base64 decode segment: {}", e)))?;

    decompress_bytes_internal(&compressed_data, &encoding)
}

/// Internal helper to decompress bytes with fallback logic
/// If the decompression fails, we fallback to plain JSON
/// If the fallback fails, we return the error
fn decompress_bytes_internal(
    compressed_data: &[u8],
    encoding: &SpoolingEncoding,
) -> Result<String, Error> {
    let decompressed_data = match encoding {
        SpoolingEncoding::JsonZstd => decompress_zstd(compressed_data).or_else(|e| {
            let fallback_result = String::from_utf8(compressed_data.to_vec()).map_err(|utf8_err| {
                Error::InternalError(format!(
                    "Failed to decompress zstd and plain JSON fallback also failed: {}, {}",
                    e, utf8_err
                ))
            });
            fallback_result
        })?,
        SpoolingEncoding::JsonLz4 => decompress_lz4(compressed_data).or_else(|e| {
            String::from_utf8(compressed_data.to_vec()).map_err(|utf8_err| {
                Error::InternalError(format!(
                    "Failed to decompress lz4 and plain JSON fallback also failed: {}, {}",
                    e, utf8_err
                ))
            })
        })?,
        SpoolingEncoding::Json => String::from_utf8(compressed_data.to_vec()).map_err(|e| {
            Error::InternalError(format!(
                "Failed to convert uncompressed data to UTF-8: {}",
                e
            ))
        })?,
    };

    Ok(decompressed_data)
}

/// Decompress zstd-compressed data
fn decompress_zstd(compressed_data: &[u8]) -> Result<String, Error> {
    let mut decoder = match Decoder::new(compressed_data) {
        Ok(d) => d,
        Err(e) => {
            return Err(Error::InternalError(format!(
                "Failed to create zstd decoder: {}",
                e
            )));
        }
    };

    let mut decompressed = String::new();
    match decoder.read_to_string(&mut decompressed) {
        Ok(_) => Ok(decompressed),
        Err(e) => Err(Error::InternalError(format!(
            "Failed to decompress zstd data: {}",
            e
        ))),
    }
}

/// Decompress lz4-compressed data
fn decompress_lz4(compressed_data: &[u8]) -> Result<String, Error> {
    let mut decoder = lz4::Decoder::new(compressed_data)
        .map_err(|e| Error::InternalError(format!("Failed to create lz4 decoder: {}", e)))?;

    let mut decompressed = String::new();
    decoder
        .read_to_string(&mut decompressed)
        .map_err(|e| Error::InternalError(format!("Failed to decompress lz4 data: {}", e)))?;

    Ok(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_uncompressed_json() {
        // "[[1,2],[3,4]]" as base64
        let encoded = "W1sxLDJdLFszLDRdXQ==";
        let result = decode_inline_segment(encoded, "json").unwrap();
        assert_eq!(result, "[[1,2],[3,4]]");
    }

    #[test]
    fn test_decode_zstd_compressed() {
        // Create a zstd-compressed version of "[[1,2],[3,4]]"
        let original = "[[1,2],[3,4]]";
        let compressed = zstd::encode_all(original.as_bytes(), 3).unwrap();
        let encoded = BASE64_STANDARD.encode(&compressed);

        let result = decode_inline_segment(&encoded, "json+zstd").unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decode_lz4_compressed() {
        // Create an lz4-compressed version of "[[1,2],[3,4]]"
        let original = "[[1,2],[3,4]]";
        let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
        std::io::Write::write_all(&mut encoder, original.as_bytes()).unwrap();
        let (compressed, _result) = encoder.finish();
        let encoded = BASE64_STANDARD.encode(&compressed);

        let result = decode_inline_segment(&encoded, "json+lz4").unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_invalid_base64() {
        let result = decode_inline_segment("not!valid!base64!", "json");
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_encoding() {
        let encoded = "W1sxLDJdXQ==";
        let result = decode_inline_segment(encoded, "unknown");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported spooling encoding"));
    }

    #[test]
    fn test_zstd_fallback_to_plain_json() {
        let plain_json = "[[2,\"data\"]]";
        let encoded = BASE64_STANDARD.encode(plain_json.as_bytes());
        let result = decode_inline_segment(&encoded, "json+zstd").unwrap();
        assert_eq!(result, plain_json);
    }

    #[test]
    fn test_lz4_fallback_to_plain_json() {
        let plain_json = "[[1,\"test\"]]";
        let encoded = BASE64_STANDARD.encode(plain_json.as_bytes());
        let result = decode_inline_segment(&encoded, "json+lz4").unwrap();
        assert_eq!(result, plain_json);
    }
}
