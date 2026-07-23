//! Regression test: the transaction id Trino returns must be captured and sent
//! on subsequent statements.

use trino_rust_client::client::ClientBuilder;
use trino_rust_client::transaction::TransactionId;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TX_ID: &str = "17cbc429-462a-4da3-9a06-02b6507d0d01";

async fn make_mock_server() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    let port: u16 = port_str.parse().unwrap();
    (server, host.to_string(), port)
}

/// A statement response. `next` is the path to follow, or `None` for the final
/// page.
fn page(server_uri: &str, id: &str, next: Option<&str>) -> String {
    let next_uri = match next {
        Some(p) => format!(r#""nextUri": "{server_uri}{p}","#),
        None => String::new(),
    };
    format!(
        r#"{{
            "id": "{id}",
            "infoUri": "{server_uri}/ui/query.html?{id}",
            {next_uri}
            "stats": {{
                "state": "FINISHED", "queued": false, "scheduled": false,
                "nodes": 0, "totalSplits": 0, "queuedSplits": 0,
                "runningSplits": 0, "completedSplits": 0,
                "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
                "elapsedTimeMillis": 0, "processedRows": 0, "processedBytes": 0,
                "peakMemoryBytes": 0, "spilledBytes": 0
            }},
            "warnings": []
        }}"#
    )
}

#[tokio::test]
async fn transaction_id_is_captured_sent_and_cleared() {
    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();

    // 1. START TRANSACTION: the POST is accepted, and the follow-up page
    //    carries the started-transaction header, exactly as Trino does.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "q_begin",
            Some("/v1/statement/q_begin/1"),
        )))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/statement/q_begin/1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("X-Trino-Started-Transaction-Id", TX_ID)
                .set_body_string(page(&uri, "q_begin", None)),
        )
        .mount(&server)
        .await;

    let client = ClientBuilder::new("test_user", host)
        .port(port)
        .build()
        .unwrap();

    client.begin_transaction().await.expect("begin failed");

    assert_eq!(
        client.transaction_id().await,
        TransactionId::Id(TX_ID.to_string()),
        "the UUID from X-Trino-Started-Transaction-Id must be retained"
    );

    // 2. The next statement must carry the UUID.
    client.execute("SELECT 1").await.expect("select failed");

    let requests = server.received_requests().await.unwrap();
    let select_request = requests
        .iter()
        .find(|r| r.body == b"SELECT 1")
        .expect("no request carried the SELECT body");
    assert_eq!(
        select_request
            .headers
            .get("X-Trino-Transaction-Id")
            .expect("X-Trino-Transaction-Id header missing")
            .to_str()
            .unwrap(),
        TX_ID,
        "statements must run inside the transaction, not with NONE"
    );

    // 3. COMMIT clears it.
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "q_commit",
            Some("/v1/statement/q_commit/1"),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q_commit/1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("X-Trino-Clear-Transaction-Id", "true")
                .set_body_string(page(&uri, "q_commit", None)),
        )
        .mount(&server)
        .await;

    client.commit().await.expect("commit failed");

    assert_eq!(
        client.transaction_id().await,
        TransactionId::NoTransaction,
        "X-Trino-Clear-Transaction-Id must reset the session"
    );
}

#[tokio::test]
async fn statements_send_none_when_no_transaction_is_active() {
    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "q1",
            Some("/v1/statement/q1/1"),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(&uri, "q1", None)))
        .mount(&server)
        .await;

    let client = ClientBuilder::new("test_user", host)
        .port(port)
        .build()
        .unwrap();
    client.execute("SELECT 1").await.expect("select failed");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests[0]
            .headers
            .get("X-Trino-Transaction-Id")
            .unwrap()
            .to_str()
            .unwrap(),
        "NONE"
    );
}
