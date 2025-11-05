use serde::Deserialize;
use std::collections::HashMap;

use super::*;
#[cfg(feature = "spooling")]
use crate::error::Error;
#[cfg(feature = "spooling")]
use crate::spooling::{decode_inline_segment, Segment};
use crate::Trino;

/// Query result data can be either Direct (inline array) or Spooled (compressed segments)
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum QueryResultData<T: Trino> {
    // Spooled protocol: data is an object with encoding and segments
    Spooled(SpooledData),
    // Direct protocol: data is a simple JSON array
    #[serde(bound(deserialize = "Vec<T>: Deserialize<'de>"))]
    Direct(Vec<T>),
}

impl<T> QueryResultData<T>
where
    T: Trino,
    for<'de> T: serde::Deserialize<'de>,
{
    /// Convert into Vec for both Direct and Spooled variants
    pub fn into_vec(self) -> Vec<T> {
        match self {
            QueryResultData::Direct(data) => data,
            #[cfg(feature = "spooling")]
            QueryResultData::Spooled(spooled) => spooled.parse_segments().unwrap_or_else(|e| {
                log::error!("Failed to parse spooled segments: {}", e);
                Vec::new()
            }),
            #[cfg(not(feature = "spooling"))]
            QueryResultData::Spooled(_) => {
                panic!("Spooling feature not enabled")
            }
        }
    }
}

/// Spooled data contains encoding format and segment references
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpooledData {
    pub encoding: String,
    #[cfg(feature = "spooling")]
    pub segments: Vec<Segment>,
}

#[cfg(feature = "spooling")]
impl SpooledData {
    /// Parse all segments and return the rows
    fn parse_segments<T>(&self) -> Result<Vec<T>, Error>
    where
        for<'de> T: Trino + serde::Deserialize<'de>,
    {
        let mut all_rows = Vec::new();

        for (idx, segment) in self.segments.iter().enumerate() {
            match segment {
                Segment::Inlined { data, .. } => {
                    let decompressed = decode_inline_segment(data, &self.encoding)?;
                    let rows: Vec<T> = serde_json::from_str(&decompressed).map_err(|e| {
                        Error::InternalError(format!("Failed to parse segment {} JSON: {}", idx, e))
                    })?;
                    all_rows.reserve(rows.len());
                    for row in rows {
                        all_rows.push(row);
                    }
                }
                Segment::Spooled { .. } => {
                    return Err(Error::InternalError(
                        "Remote spooled segments not supported in this code path. Use Client::get_all() instead.".to_string(),
                    ));
                }
            }
        }

        Ok(all_rows)
    }
}

/// Metadata about spooled data segments
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataAttributes {
    pub rows_count: Option<u64>,
    pub segment_size: Option<u64>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// Trino query result
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult<T: Trino> {
    pub id: String,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,
    pub next_uri: Option<String>,

    pub columns: Option<Vec<Column>>,

    #[serde(bound(deserialize = "Option<QueryResultData<T>>: Deserialize<'de>"))]
    pub data: Option<QueryResultData<T>>,

    pub error: Option<QueryError>,

    pub stats: Stat,
    pub warnings: Vec<Warning>,

    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}

#[cfg(test)]
#[cfg(feature = "spooling")]
mod tests {
    use super::*;
    use base64::prelude::*;

    #[test]
    fn test_parse_segments_multiple_inline() {
        let rows_json1 = r#"[["alice",1],["bob",2]]"#;
        let rows_json2 = r#"[["charlie",3]]"#;

        let encoded1 = BASE64_STANDARD.encode(rows_json1.as_bytes());
        let encoded2 = BASE64_STANDARD.encode(rows_json2.as_bytes());

        let segment1_json = format!(
            r#"{{"type":"inline","data":"{}","metadata":{{}}}}"#,
            encoded1
        );
        let segment2_json = format!(
            r#"{{"type":"inline","data":"{}","metadata":{{}}}}"#,
            encoded2
        );

        let segment1: Segment = serde_json::from_str(&segment1_json).unwrap();
        let segment2: Segment = serde_json::from_str(&segment2_json).unwrap();

        let spooled = SpooledData {
            encoding: "json".to_string(),
            segments: vec![segment1, segment2],
        };

        let rows = spooled.parse_segments::<crate::Row>().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].value()[0],
            serde_json::Value::String("alice".to_string())
        );
        assert_eq!(rows[0].value()[1], serde_json::Value::Number(1.into()));
        assert_eq!(
            rows[1].value()[0],
            serde_json::Value::String("bob".to_string())
        );
        assert_eq!(rows[1].value()[1], serde_json::Value::Number(2.into()));
        assert_eq!(
            rows[2].value()[0],
            serde_json::Value::String("charlie".to_string())
        );
        assert_eq!(rows[2].value()[1], serde_json::Value::Number(3.into()));
    }
}
