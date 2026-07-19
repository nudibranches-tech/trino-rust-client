use std::fs;
use std::time::Duration;

use trino_rust_client::client::ClientBuilder;
use trino_rust_client::retry::RetryPolicy;
use trino_rust_client::Row;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn make_mock_server() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    let port: u16 = port_str.parse().unwrap();
    (server, host.to_string(), port)
}

fn fast_retry_client(host: String, port: u16) -> trino_rust_client::client::Client {
    ClientBuilder::new("test_user", host)
        .port(port)
        .retry_policy(RetryPolicy {
            max_attempts: 3,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_get_all_retries_transient_503() {
    let (server, host, port) = make_mock_server().await;
    let finished = fs::read_to_string("tests/data/models/query_result_finished").unwrap();

    // First POST fails with 503 (transient), then succeeds.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(finished))
        .mount(&server)
        .await;

    let result = fast_retry_client(host, port)
        .get_all::<Row>("SELECT * FROM t".to_string())
        .await;

    assert!(
        result.is_ok(),
        "a 503 should be retried and then succeed, got: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().len(), 1);
}

#[tokio::test]
async fn test_get_all_fails_fast_on_client_error() {
    let (server, host, port) = make_mock_server().await;

    // A 400 is terminal: it must be attempted exactly once (no retry).
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .expect(1)
        .mount(&server)
        .await;

    let result = fast_retry_client(host, port)
        .get_all::<Row>("SELECT * FROM t".to_string())
        .await;

    assert!(result.is_err(), "a 400 must fail fast");
    // `expect(1)` above is verified on drop: the request was NOT retried.
    server.verify().await;
}
