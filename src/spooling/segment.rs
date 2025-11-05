use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

// Data attributes for a segment
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataAttributes {
    #[serde(flatten)]
    attributes: HashMap<String, serde_json::Value>,
}

// Data attributes for a segment
impl DataAttributes {
    // Get the row offset for a segment
    pub fn row_offset(&self) -> Option<u64> {
        self.attributes.get("rowOffset")?.as_u64()
    }

    // Get the number of rows for a segment
    pub fn rows_count(&self) -> Option<u64> {
        self.attributes.get("rowsCount")?.as_u64()
    }

    // Get the size of a segment
    pub fn segment_size(&self) -> Option<u64> {
        self.attributes.get("segmentSize")?.as_u64()
    }
}

// Segment is a part of a query result when using the spooling protocol
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase", untagged)]
pub enum Segment {
    // Inlined segment
    Inlined {
        #[serde(rename = "type")]
        segment_type: String,
        data: String,
        metadata: DataAttributes,
    },
    // Spooled segment
    Spooled {
        #[serde(rename = "type")]
        segment_type: String,
        uri: String,
        #[serde(rename = "ackUri")]
        #[serde(skip_serializing_if = "Option::is_none")]
        ack_uri: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<HashMap<String, Vec<String>>>,
        metadata: DataAttributes,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_inlined_segment() {
        let json = r#"{
            "type": "inline",
            "data": "SGVsbG8gV29ybGQ=",
            "metadata": {
                "rowOffset": 0,
                "rowsCount": 1,
                "segmentSize": 1024
            }
        }"#;
        let segment: Segment = serde_json::from_str(json).unwrap();
        match segment {
            Segment::Inlined {
                segment_type,
                data,
                metadata,
            } => {
                assert_eq!(segment_type, "inline");
                assert_eq!(data, "SGVsbG8gV29ybGQ=");
                assert_eq!(metadata.row_offset(), Some(0));
                assert_eq!(metadata.rows_count(), Some(1));
                assert_eq!(metadata.segment_size(), Some(1024));
            }
            _ => panic!("Expected Inlined segment"),
        }
    }

    #[test]
    fn test_deserialize_spooled_segment_minimal() {
        let json = r#"{
            "type": "spooled",
            "uri": "http://minio:9000/bucket/segment.json?signature=abc123",
            "metadata": {
                "rowOffset": 0,
                "rowsCount": 1000,
                "segmentSize": 1048576
            }
        }"#;
        let segment: Segment = serde_json::from_str(json).unwrap();
        match segment {
            Segment::Spooled {
                segment_type,
                uri,
                ack_uri,
                headers,
                metadata,
            } => {
                assert_eq!(segment_type, "spooled");
                assert_eq!(
                    uri,
                    "http://minio:9000/bucket/segment.json?signature=abc123"
                );
                assert_eq!(ack_uri, None);
                assert_eq!(headers, None);
                assert_eq!(metadata.row_offset(), Some(0));
                assert_eq!(metadata.rows_count(), Some(1000));
                assert_eq!(metadata.segment_size(), Some(1048576));
            }
            _ => panic!("Expected Spooled segment"),
        }
    }

    #[test]
    fn test_deserialize_spooled_segment_with_ack() {
        let json = r#"{
            "type": "spooled",
            "uri": "http://minio:9000/bucket/segment.json",
            "ackUri": "http://minio:9000/bucket/segment.ack",
            "headers": {
                "X-Custom": ["value1"]
            },
            "metadata": {
                "rowOffset": 0,
                "rowsCount": 100
            }
        }"#;
        let segment: Segment = serde_json::from_str(json).unwrap();
        match segment {
            Segment::Spooled {
                segment_type,
                uri,
                ack_uri,
                headers,
                metadata,
            } => {
                assert_eq!(segment_type, "spooled");
                assert_eq!(uri, "http://minio:9000/bucket/segment.json");
                assert_eq!(
                    ack_uri,
                    Some("http://minio:9000/bucket/segment.ack".to_string())
                );
                assert!(headers.is_some());
                assert_eq!(metadata.row_offset(), Some(0));
                assert_eq!(metadata.rows_count(), Some(100));
            }
            _ => panic!("Expected Spooled segment"),
        }
    }

    #[test]
    fn test_deserialize_inlined_segment_minimal() {
        let json = r#"{
            "type": "inline",
            "data": "YWJjZGVmZw==",
            "metadata": {}
        }"#;
        let segment: Segment = serde_json::from_str(json).unwrap();
        match segment {
            Segment::Inlined {
                segment_type,
                data,
                metadata,
            } => {
                assert_eq!(segment_type, "inline");
                assert_eq!(data, "YWJjZGVmZw==");
                assert_eq!(metadata.row_offset(), None);
                assert_eq!(metadata.rows_count(), None);
                assert_eq!(metadata.segment_size(), None);
            }
            _ => panic!("Expected Inlined segment"),
        }
    }

    #[test]
    fn test_data_attributes_row_offset() {
        let json = r#"{
            "rowOffset": 42
        }"#;
        let data_attributes: DataAttributes = serde_json::from_str(json).unwrap();
        assert_eq!(data_attributes.row_offset(), Some(42));
        assert_eq!(data_attributes.rows_count(), None);
        assert_eq!(data_attributes.segment_size(), None);
    }

    #[test]
    fn test_data_attributes_rows_count() {
        let json = r#"{
            "rowsCount": 100
        }"#;
        let data_attributes: DataAttributes = serde_json::from_str(json).unwrap();
        assert_eq!(data_attributes.row_offset(), None);
        assert_eq!(data_attributes.rows_count(), Some(100));
        assert_eq!(data_attributes.segment_size(), None);
    }

    #[test]
    fn test_data_attributes_segment_size() {
        let json = r#"{
            "segmentSize": 4096
        }"#;
        let data_attributes: DataAttributes = serde_json::from_str(json).unwrap();
        assert_eq!(data_attributes.row_offset(), None);
        assert_eq!(data_attributes.rows_count(), None);
        assert_eq!(data_attributes.segment_size(), Some(4096));
    }

    #[test]
    fn test_data_attributes_all_fields() {
        let json = r#"{
            "rowOffset": 0,
            "rowsCount": 10,
            "segmentSize": 512
        }"#;
        let data_attributes: DataAttributes = serde_json::from_str(json).unwrap();
        assert_eq!(data_attributes.row_offset(), Some(0));
        assert_eq!(data_attributes.rows_count(), Some(10));
        assert_eq!(data_attributes.segment_size(), Some(512));
    }

    #[test]
    fn test_spooled_segment_with_multiple_header_values() {
        let json = r#"{
            "type": "spooled",
            "uri": "http://storage/segment.json",
            "ackUri": "http://storage/segment.ack",
            "headers": {
                "Authorization": ["Bearer token123"],
                "X-Custom": ["value1", "value2"]
            },
            "metadata": {
                "rowOffset": 100,
                "rowsCount": 50
            }
        }"#;
        let segment: Segment = serde_json::from_str(json).unwrap();

        match segment {
            Segment::Spooled {
                uri,
                ack_uri,
                headers,
                metadata,
                ..
            } => {
                assert_eq!(uri, "http://storage/segment.json");
                assert_eq!(ack_uri, Some("http://storage/segment.ack".to_string()));
                assert!(headers.is_some());
                let headers_map = headers.unwrap();
                assert_eq!(
                    headers_map.get("Authorization"),
                    Some(&vec!["Bearer token123".to_string()])
                );
                assert_eq!(
                    headers_map.get("X-Custom"),
                    Some(&vec!["value1".to_string(), "value2".to_string()])
                );
                assert_eq!(metadata.row_offset(), Some(100));
                assert_eq!(metadata.rows_count(), Some(50));
            }
            _ => panic!("Expected Spooled segment"),
        }
    }
}
