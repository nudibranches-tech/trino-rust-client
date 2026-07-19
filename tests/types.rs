use trino_rust_client::models::{QueryResult, QueryResultData};
use trino_rust_client::{Trino, VarBinary};

// A VARBINARY column must decode end-to-end through the same path `get_all`
// uses (parsing `QueryResult<T>`), turning Trino's base64 into raw bytes.
#[derive(Trino, Debug, serde::Deserialize, serde::Serialize)]
struct Record {
    payload: VarBinary,
    n: i64,
}

#[test]
fn decodes_varbinary_column() {
    // Row is Trino's native array form; "aGVsbG8=" is base64 for "hello".
    let json = r#"{
        "id": "q",
        "infoUri": "http://localhost/q",
        "nextUri": null,
        "columns": [
            { "name": "payload", "type": "varbinary", "typeSignature": { "rawType": "varbinary", "arguments": [] } },
            { "name": "n", "type": "bigint", "typeSignature": { "rawType": "bigint", "arguments": [] } }
        ],
        "data": [ ["aGVsbG8=", 42] ],
        "stats": { "state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1,
            "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1,
            "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0, "elapsedTimeMillis": 0,
            "processedRows": 1, "processedBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0 },
        "warnings": []
    }"#;

    let result: QueryResult<Record> = serde_json::from_str(json).unwrap();
    let rows = match result.data.unwrap() {
        QueryResultData::Direct(rows) => rows,
        _ => panic!("expected direct rows"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload.0, b"hello");
    assert_eq!(rows[0].n, 42);
}

// An unsupported Trino type must fail with an error that names the type,
// rather than a generic "invalid type signature".
#[test]
fn unsupported_type_error_names_the_type() {
    use trino_rust_client::{TrinoTy, TypeSignature};

    let sig: TypeSignature =
        serde_json::from_str(r#"{ "rawType": "HyperLogLog", "arguments": [] }"#).unwrap();
    let err = TrinoTy::from_type_signature(sig).unwrap_err();
    assert!(
        err.to_string().contains("HyperLogLog"),
        "error should name the type, got: {err}"
    );
}
