use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

use super::*;
use crate::{DataSet, Trino};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult<T: Trino> {
    pub id: String,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,
    pub next_uri: Option<String>,

    #[serde(flatten)]
    #[serde(bound(deserialize = "Option<DataSet<T>>: Deserialize<'de>"))]
    pub data_set: Option<DataSet<T>>,
    pub error: Option<QueryError>,

    pub stats: Stat,
    pub warnings: Vec<Warning>,

    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EncodedQueryData {
    pub encoding: String,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    pub segments: Vec<DataSegment>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DataSegment {
    #[serde(rename_all = "camelCase")]
    Inline { data: String, metadata: SegmentMetadata },
    #[serde(rename_all = "camelCase")]
    Spooled {
        uri: String,
        #[serde(default)]
        ack_uri: Option<String>,
        #[serde(default)]
        headers: HashMap<String, Vec<String>>,
        metadata: SegmentMetadata,
    },
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SegmentMetadata {
    pub row_offset: u64,
    pub rows_count: u64,
    pub segment_size: u64,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQueryResult {
    pub id: String,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,
    pub next_uri: Option<String>,
    pub columns: Option<Vec<Column>>,
    #[serde(default)]
    pub data: Option<Value>,
    pub error: Option<QueryError>,
    pub stats: Stat,
    pub warnings: Vec<Warning>,
    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}
