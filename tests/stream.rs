use std::fs;

use futures::StreamExt;
use serde_json::{json, Value};
use trino_rust_client::{client::ClientBuilder, Row};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn read_fixture(name: &str) -> Value {
    serde_json::from_str(&fs::read_to_string(format!("tests/data/models/{}", name)).unwrap())
        .unwrap()
}

async fn make_mock_server() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    let port: u16 = port_str.parse().unwrap();
    (server, host.to_string(), port)
}

/// Mount a three-page lifecycle:
///   POST /v1/statement -> QUEUED, no data, nextUri -> /1
///   GET  /1            -> 2 rows, nextUri -> /2
///   GET  /2            -> 1 row, no nextUri (query finished)
/// Total: 3 rows streamed across 3 pages.
async fn mount_paged_result(server: &MockServer) {
    let uri = server.uri();
    let finished = read_fixture("query_result_finished");
    let columns = finished["columns"].clone();
    let row = finished["data"][0].clone();
    // Reuse a full stats block from the fixture so `Stat` deserializes.
    let stats = finished["stats"].clone();

    let page1 = json!({
        "id": "test_stream_00001",
        "infoUri": format!("{uri}/ui/query.html?test_stream_00001"),
        "nextUri": format!("{uri}/v1/statement/test_stream_00001/1"),
        "stats": stats.clone(),
        "warnings": []
    });

    let page2 = json!({
        "id": "test_stream_00001",
        "infoUri": format!("{uri}/ui/query.html?test_stream_00001"),
        "nextUri": format!("{uri}/v1/statement/test_stream_00001/2"),
        "columns": columns,
        "data": [row.clone(), row.clone()],
        "stats": stats.clone(),
        "warnings": []
    });

    let page3 = json!({
        "id": "test_stream_00001",
        "infoUri": format!("{uri}/ui/query.html?test_stream_00001"),
        "columns": finished["columns"].clone(),
        "data": [row],
        "stats": stats,
        "warnings": []
    });

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/statement/test_stream_00001/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page2))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/statement/test_stream_00001/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page3))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn test_stream_yields_rows_across_pages() {
    let (server, host, port) = make_mock_server().await;
    mount_paged_result(&server).await;

    let client = ClientBuilder::new("test_user", host)
        .port(port)
        .build()
        .unwrap();

    let stream = client.stream::<Row>("SELECT * FROM t".to_string());
    tokio::pin!(stream);

    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        rows.push(item.expect("stream item should be Ok"));
    }

    // 2 rows from page 2 + 1 row from page 3, without ever buffering the whole set.
    assert_eq!(rows.len(), 3, "expected 3 rows streamed across 3 pages");

    server.verify().await;
}

#[tokio::test]
async fn test_stream_surfaces_query_error() {
    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();

    let page1 = json!({
        "id": "test_stream_err",
        "infoUri": format!("{uri}/ui/query.html?test_stream_err"),
        "nextUri": format!("{uri}/v1/statement/test_stream_err/1"),
        "stats": read_fixture("query_result_finished")["stats"].clone(),
        "warnings": []
    });
    // Error page carried inside a 200 response (Trino reports query errors this way).
    let err_page = read_fixture("query_result_failed");

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/test_stream_err/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(err_page))
        .mount(&server)
        .await;

    let client = ClientBuilder::new("test_user", host)
        .port(port)
        .build()
        .unwrap();

    let stream = client.stream::<Row>("SELECT * FROM t".to_string());
    tokio::pin!(stream);

    let mut saw_error = false;
    while let Some(item) = stream.next().await {
        if item.is_err() {
            saw_error = true;
            break;
        }
    }
    assert!(saw_error, "a query error page must surface as an Err item");
}
