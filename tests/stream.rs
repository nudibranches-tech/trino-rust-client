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

fn client(host: String, port: u16) -> trino_rust_client::client::Client {
    ClientBuilder::new("test_user", host)
        .port(port)
        .build()
        .unwrap()
}

async fn mount(server: &MockServer, verb: &str, p: &str, body: Value) {
    let m = if verb == "POST" {
        Mock::given(method("POST")).and(path("/v1/statement".to_string()))
    } else {
        Mock::given(method("GET")).and(path(p.to_string()))
    };
    m.respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// Three-page lifecycle: QUEUED -> 2 rows -> 1 row (finished). Total 3 rows.
async fn mount_paged_result(server: &MockServer) {
    let uri = server.uri();
    let finished = read_fixture("query_result_finished");
    let columns = finished["columns"].clone();
    let row = finished["data"][0].clone();
    let stats = finished["stats"].clone();

    mount(
        server,
        "POST",
        "",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/1"),
            "stats": stats.clone(), "warnings": []
        }),
    )
    .await;
    mount(
        server,
        "GET",
        "/v1/statement/q/1",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/2"),
            "columns": columns, "data": [row.clone(), row.clone()],
            "stats": stats.clone(), "warnings": []
        }),
    )
    .await;
    mount(
        server,
        "GET",
        "/v1/statement/q/2",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "columns": finished["columns"].clone(), "data": [row],
            "stats": stats, "warnings": []
        }),
    )
    .await;
}

#[tokio::test]
async fn test_stream_yields_rows_across_pages() {
    let (server, host, port) = make_mock_server().await;
    mount_paged_result(&server).await;

    let cli = client(host, port);
    let mut stream = cli
        .stream::<Row>("SELECT * FROM t")
        .await
        .expect("stream creation should resolve the schema");

    // Schema is available up front, before any row is pulled.
    assert_eq!(stream.columns().len(), 6, "schema resolved up front");
    assert_eq!(stream.columns()[0].0, "a");

    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        rows.push(item.expect("stream item should be Ok"));
    }
    assert_eq!(rows.len(), 3, "expected 3 rows streamed across 3 pages");
}

// A RowStream must be `Send` so it can be held across `.await` inside a
// spawned task (a very common pattern in async services). This test would fail
// to compile if the inner boxed stream were not `Send`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stream_is_send_and_spawnable() {
    let (server, host, port) = make_mock_server().await;
    mount_paged_result(&server).await;

    let count = tokio::spawn(async move {
        let cli = client(host, port);
        let mut stream = cli.stream::<Row>("SELECT * FROM t").await.unwrap();
        let mut n = 0usize;
        while let Some(item) = stream.next().await {
            item.unwrap();
            n += 1;
        }
        n
    })
    .await
    .unwrap();

    assert_eq!(
        count, 3,
        "streaming inside a spawned task should yield 3 rows"
    );
}

#[tokio::test]
async fn test_stream_empty_result_set() {
    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();
    let finished = read_fixture("query_result_finished");
    let stats = finished["stats"].clone();

    mount(
        &server,
        "POST",
        "",
        json!({
            "id": "e", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/e/1"),
            "stats": stats.clone(), "warnings": []
        }),
    )
    .await;
    // Finished page with schema but zero rows and no nextUri.
    mount(
        &server,
        "GET",
        "/v1/statement/e/1",
        json!({
            "id": "e", "infoUri": format!("{uri}/ui"),
            "columns": finished["columns"].clone(),
            "stats": stats, "warnings": []
        }),
    )
    .await;

    let cli = client(host, port);
    let mut stream = cli
        .stream::<Row>("SELECT * FROM t WHERE 1=0")
        .await
        .expect("empty result must still resolve a schema");

    assert_eq!(stream.columns().len(), 6, "schema preserved for zero rows");
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.expect("no error expected");
        count += 1;
    }
    assert_eq!(count, 0, "no rows for an empty result set");
}

// The streaming + spooling combination is the highest-value path (large
// results). Exercise it end-to-end with inline segments (no object storage
// needed): the stream must resolve the schema and decode spooled rows.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn test_stream_spooled_inline_segments() {
    use trino_rust_client::Trino;

    #[derive(Trino, Debug, serde::Deserialize, serde::Serialize)]
    struct SpooledRecord {
        id: i64,
        name: String,
    }

    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();
    let stats = read_fixture("query_result_finished")["stats"].clone();

    mount(
        &server,
        "POST",
        "",
        json!({
            "id": "s", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/s/1"),
            "stats": stats.clone(), "warnings": []
        }),
    )
    .await;
    // Spooled page with two inline JSON segments in Trino's native row-array
    // format: [[1,"alice"]] and [[2,"bob"]] (base64-encoded).
    mount(
        &server,
        "GET",
        "/v1/statement/s/1",
        json!({
            "id": "s", "infoUri": format!("{uri}/ui"),
            "columns": [
                { "name": "id", "type": "bigint", "typeSignature": { "rawType": "bigint", "arguments": [] } },
                { "name": "name", "type": "varchar", "typeSignature": { "rawType": "varchar", "arguments": [] } }
            ],
            "data": {
                "encoding": "json",
                "segments": [
                    { "type": "inline", "data": "W1sxLCJhbGljZSJdXQ==", "metadata": {} },
                    { "type": "inline", "data": "W1syLCJib2IiXV0=", "metadata": {} }
                ]
            },
            "stats": stats, "warnings": []
        }),
    )
    .await;

    let cli = client(host, port);
    let mut stream = cli
        .stream::<SpooledRecord>("SELECT id, name FROM t")
        .await
        .expect("schema resolves for the spooled result");

    assert_eq!(
        stream.columns().len(),
        2,
        "spooled schema resolved up front"
    );

    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        rows.push(item.expect("spooled row should decode"));
    }
    assert_eq!(rows.len(), 2, "two spooled rows streamed");
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].name, "alice");
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].name, "bob");
}

#[tokio::test]
async fn test_stream_surfaces_mid_stream_error() {
    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();
    let finished = read_fixture("query_result_finished");
    let stats = finished["stats"].clone();
    let row = finished["data"][0].clone();

    mount(
        &server,
        "POST",
        "",
        json!({
            "id": "x", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/x/1"),
            "stats": stats.clone(), "warnings": []
        }),
    )
    .await;
    // Page with schema + 2 rows, then a link to a failing page.
    mount(
        &server,
        "GET",
        "/v1/statement/x/1",
        json!({
            "id": "x", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/x/2"),
            "columns": finished["columns"].clone(), "data": [row.clone(), row],
            "stats": stats, "warnings": []
        }),
    )
    .await;
    // Error page (carried inside a 200, as Trino does).
    mount(
        &server,
        "GET",
        "/v1/statement/x/2",
        read_fixture("query_result_failed"),
    )
    .await;

    let cli = client(host, port);
    let mut stream = cli
        .stream::<Row>("SELECT * FROM t")
        .await
        .expect("schema resolves before the failing page");

    let mut oks = 0;
    let mut saw_error = false;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => oks += 1,
            Err(_) => {
                saw_error = true;
                break;
            }
        }
    }
    assert_eq!(oks, 2, "two rows should stream before the error");
    assert!(saw_error, "the failing page must surface as an Err item");
}
