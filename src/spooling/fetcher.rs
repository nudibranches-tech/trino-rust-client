use std::collections::HashMap;
use std::io::Read;
use std::thread;

use crate::error::{Error, Result};
use crate::spooling::segment::Segment;
use crate::spooling::segment::Segment::Inlined;
use base64::{engine::general_purpose, Engine as _};
use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use reqwest::Client;

// Default maximum number of concurrent segment fetches based on CPU count
fn default_max_concurrent_segments() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(5)
        .max(1)
}

// Fetcher for segments with the spooling protocol
pub struct SegmentFetcher {
    http_client: Client,
    max_concurrent_segments: usize,
}

// Fetcher for segments
impl SegmentFetcher {
    pub fn new(http_client: Client) -> Self {
        Self {
            http_client,
            max_concurrent_segments: default_max_concurrent_segments(),
        }
    }

    /// Configure the maximum number of concurrent segment fetches
    /// Default is based on available CPU parallelism (minimum 1)
    pub fn with_max_concurrent(mut self, count: usize) -> Self {
        self.max_concurrent_segments = count.max(1);
        self
    }

    /// Fetch a single segment and return the decoded data
    pub async fn fetch_segment(&self, segment: &Segment) -> Result<Vec<u8>> {
        match segment {
            Inlined { data, .. } => self.fetch_inline_segment(data).await,
            Segment::Spooled {
                uri,
                ack_uri,
                headers,
                ..
            } => {
                let data = self.fetch_spooled_segment(uri, headers.as_ref()).await?;

                // Acknowledge the segment if ackUri is provided
                if let Some(ack) = ack_uri {
                    if let Err(e) = self.acknowledge_segment(ack, headers.as_ref()).await {
                        log::warn!("Failed to acknowledge segment {}: {}", ack, e);
                    }
                }

                Ok(data)
            }
        }
    }

    /// Fetch multiple segments with controlled concurrency
    /// Uses buffered concurrency to limit the number of concurrent fetches
    pub async fn fetch_segments(&self, segments: Vec<Segment>) -> Result<Vec<Vec<u8>>> {
        log::debug!(
            "Fetching {} segments with max concurrency of {}",
            segments.len(),
            self.max_concurrent_segments
        );

        let results: Vec<Result<Vec<u8>>> = stream::iter(segments.into_iter().enumerate())
            .map(|(idx, segment)| async move {
                self.fetch_segment(&segment).await.map_err(|e| {
                    // Add context about which segment failed
                    let segment_info = match &segment {
                        Inlined { .. } => format!("inline segment #{}", idx),
                        Segment::Spooled { uri, .. } => {
                            format!("remote segment #{} (URI: {})", idx, uri)
                        }
                    };
                    Error::InternalError(format!("Failed to fetch {}: {}", segment_info, e))
                })
            })
            .buffer_unordered(self.max_concurrent_segments)
            .collect()
            .await;

        // Collect results, returning early on first error
        results.into_iter().collect()
    }

    // Fetch an inline segment
    async fn fetch_inline_segment(&self, data: &str) -> Result<Vec<u8>> {
        general_purpose::STANDARD
            .decode(data)
            .map_err(|e| Error::InternalError(format!("Base64 decode failed: {}", e)))
    }

    // Fetch a spooled segment (official Trino format)
    // Supports optional ackUri and headers fields
    async fn fetch_spooled_segment(
        &self,
        uri: &str,
        headers: Option<&HashMap<String, Vec<String>>>,
    ) -> Result<Vec<u8>> {
        log::debug!("Fetching spooled segment from: {}", uri);

        // Build GET request with optional headers
        let mut request = self.http_client.get(uri);

        // Apply headers if provided
        if let Some(headers_map) = headers {
            for (key, values) in headers_map {
                for value in values {
                    request = request.header(key, value);
                }
            }
        }

        // Execute GET request to the signed URI
        // NOTE: For local Docker testing, the client must be able to resolve
        // the storage hostname (e.g., 'minio') used in the URI. In production,
        // clients and Trino should share the same network view of storage endpoints.
        let response = request.send().await.map_err(|e| {
            Error::InternalError(format!(
                "Failed to fetch remote segment from {}: {}",
                uri, e
            ))
        })?;

        // Check status
        if !response.status().is_success() {
            return Err(Error::HttpNotOk(
                response.status(),
                format!("Failed to fetch segment from {}", uri),
            ));
        }

        // Detect Content-Encoding from response headers
        let content_encoding = response
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "identity".to_string());

        log::debug!(
            "Remote segment Content-Encoding: {} from {}",
            content_encoding,
            uri
        );

        // Get response body
        let compressed_data = response
            .bytes()
            .await
            .map_err(|e| Error::InternalError(format!("Failed to read response body: {}", e)))?;

        // Decompress based on Content-Encoding header
        let decompressed_data = match content_encoding.to_lowercase().as_str() {
            "gzip" => {
                log::debug!("Decompressing gzip content");
                decompress_gzip(&compressed_data)?
            }
            "identity" | "" => compressed_data.to_vec(),
            other => {
                log::warn!(
                    "Unknown Content-Encoding '{}', treating as uncompressed",
                    other
                );
                compressed_data.to_vec()
            }
        };

        log::info!(
            "Successfully fetched remote spooled segment: {} bytes",
            decompressed_data.len()
        );

        Ok(decompressed_data)
    }

    /// Acknowledge a segment after successful download
    /// This is best-effort and non-fatal - failures are logged but don't prevent data retrieval
    async fn acknowledge_segment(
        &self,
        ack_uri: &str,
        headers: Option<&HashMap<String, Vec<String>>>,
    ) -> Result<()> {
        log::debug!("Acknowledging segment: {}", ack_uri);

        let mut request = self.http_client.post(ack_uri);

        // Apply headers if provided
        if let Some(headers_map) = headers {
            for (key, values) in headers_map {
                for value in values {
                    request = request.header(key, value);
                }
            }
        }

        let response = request.send().await.map_err(|e| {
            Error::InternalError(format!(
                "Failed to send acknowledgment to {}: {}",
                ack_uri, e
            ))
        })?;

        if !response.status().is_success() {
            log::warn!(
                "Acknowledgment returned non-success status: {} for {}",
                response.status(),
                ack_uri
            );
        }

        Ok(())
    }
}

/// Decompress gzip-compressed data
fn decompress_gzip(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(compressed_data);
    let mut decompressed = Vec::new();

    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| Error::InternalError(format!("Failed to decompress gzip data: {}", e)))?;

    Ok(decompressed)
}
