use trino_rust_client::models::{QueryResult, QueryResultData};
use trino_rust_client::Trino;

#[derive(Trino, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct TestRecord {
    id: i64,
    name: String,
}

#[test]
fn test_spooled_data_deserialization() {
    let json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": {
            "encoding": "json",
            "segments": [
                {
                    "data": "W3siaWQiOjEsIm5hbWUiOiJhbGljZSJ9XQ==",
                    "metadata": {}
                },
                {
                    "data": "W3siaWQiOjIsIm5hbWUiOiJib2IifV0=",
                    "metadata": {}
                }
            ]
        },
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 2,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;
    let result: QueryResult<TestRecord> = serde_json::from_str(json).unwrap();

    assert!(matches!(result.data, Some(QueryResultData::Spooled(_))));
    assert_eq!(result.columns.unwrap().len(), 2);

    let records = result.data.unwrap().into_vec();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].id, 1);
    assert_eq!(records[0].name, "alice");
    assert_eq!(records[1].id, 2);
    assert_eq!(records[1].name, "bob");
}

#[test]
fn test_direct_protocol_fallback() {
    let json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"}
        ],
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 2,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;
    let result: QueryResult<TestRecord> = serde_json::from_str(json).unwrap();

    assert!(matches!(result.data, Some(QueryResultData::Direct(_))));
    if let Some(QueryResultData::Direct(data)) = result.data {
        assert_eq!(data.len(), 2);
        assert_eq!(data[0].id, 1);
        assert_eq!(data[0].name, "alice");
    }
}

#[test]
fn test_mixed_protocol_detection() {
    let spooled_json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": {
            "encoding": "json",
            "segments": [
                {
                    "data": "W3siaWQiOjEsIm5hbWUiOiJhbGljZSJ9XQ==",
                    "metadata": {}
                }
            ]
        },
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 1,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;
    let direct_json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": [
            {"id": 1, "name": "alice"}
        ],
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 1,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;

    let spooled: QueryResult<TestRecord> = serde_json::from_str(spooled_json).unwrap();
    let direct: QueryResult<TestRecord> = serde_json::from_str(direct_json).unwrap();

    assert!(matches!(spooled.data, Some(QueryResultData::Spooled(_))));
    assert!(matches!(direct.data, Some(QueryResultData::Direct(_))));
}
