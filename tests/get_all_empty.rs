use std::fs;

use trino_rust_client::{client::ClientBuilder, Row, Trino};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn read_fixture(name: &str) -> String {
    fs::read_to_string(format!("tests/data/models/{}", name)).unwrap()
}

// Matches the 6 columns in tests/data/models/query_result_empty (a-f)
#[derive(Trino, Debug, serde::Deserialize, serde::Serialize)]
struct Record {
    a: String,
    b: i32,
    c: bool,
    d: Vec<i32>,
    f: Option<String>,
}

async fn make_mock_server() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    let port: u16 = port_str.parse().unwrap();
    (server, host.to_string(), port)
}

async fn mount_empty_result_mocks(server: &MockServer) {
    let page1 = format!(
        r#"{{
            "id": "test_empty_00001",
            "infoUri": "{uri}/ui/query.html?test_empty_00001",
            "nextUri": "{uri}/v1/statement/test_empty_00001/1",
            "stats": {{
                "state": "QUEUED", "queued": true, "scheduled": false,
                "nodes": 0, "totalSplits": 0, "queuedSplits": 0,
                "runningSplits": 0, "completedSplits": 0,
                "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
                "elapsedTimeMillis": 0, "processedRows": 0, "processedBytes": 0,
                "peakMemoryBytes": 0, "spilledBytes": 0
            }},
            "warnings": []
        }}"#,
        uri = server.uri()
    );
    let page2 = read_fixture("query_result_empty");

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page1))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/statement/test_empty_00001/1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page2))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn test_get_all_empty_result_set_row_type() {
    let (server, host, port) = make_mock_server().await;
    mount_empty_result_mocks(&server).await;

    let client = ClientBuilder::new("test_user", host).port(port).build().unwrap();

    let result = client
        .get_all::<Row>("SELECT a, b, c, d, e, f FROM t WHERE 1=0".to_string())
        .await;

    assert!(
        result.is_ok(),
        "expected Ok for zero-row result (Row type), got: {:?}",
        result.err()
    );

    let dataset = result.unwrap();
    assert!(dataset.is_empty());

    let (types, rows) = dataset.split();
    assert_eq!(rows.len(), 0);
    assert_eq!(
        types.len(),
        6,
        "column metadata must be preserved for zero-row results"
    );
    assert_eq!(types[0].0, "a");
    assert_eq!(types[1].0, "b");

    server.verify().await;
}

#[tokio::test]
async fn test_get_all_empty_result_set_derived_type() {
    let (server, host, port) = make_mock_server().await;
    mount_empty_result_mocks(&server).await;

    let client = ClientBuilder::new("test_user", host).port(port).build().unwrap();

    let result = client
        .get_all::<Record>("SELECT a, b, c, d, f FROM t WHERE 1=0".to_string())
        .await;

    assert!(
        result.is_ok(),
        "expected Ok for zero-row result (derived Trino type), got: {:?}",
        result.err()
    );

    let dataset = result.unwrap();
    assert!(dataset.is_empty());
    assert_eq!(dataset.len(), 0);

    server.verify().await;
}
